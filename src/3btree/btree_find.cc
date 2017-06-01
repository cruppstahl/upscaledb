/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

/*
 * btree searching
 */

#include "0root/root.h"

#include <string.h>

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "1base/dynamic_array.h"
#include "2page/page.h"
#include "3btree/btree_index.h"
#include "3btree/btree_cursor.h"
#include "3btree/btree_stats.h"
#include "3btree/btree_node_proxy.h"
#include "3page_manager/page_manager.h"
#include "4cursor/cursor_local.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct BtreeFindAction
{
  BtreeFindAction(BtreeIndex *btree_, Context *context_, BtreeCursor *cursor_,
                  ups_key_t *key_, ByteArray *key_arena_,
                  ups_record_t *record_, ByteArray *record_arena_,
                  uint32_t flags_)
    : btree(btree_), context(context_), cursor(cursor_), key(key_),
      record(record_), flags(flags_), key_arena(key_arena_),
      record_arena(record_arena_) {
  }

  ups_status_t run() {
    LocalEnv *env = (LocalEnv *)btree->db()->env;
    Page *page = 0;
    int slot = -1;
    BtreeNodeProxy *node = 0;

    BtreeStatistics *stats = btree->statistics();
    BtreeStatistics::FindHints hints = stats->find_hints(flags);

    if (hints.try_fast_track) {
      /*
       * see if we get a sure hit within this btree leaf; if not, revert to
       * regular scan
       *
       * As this is a speed-improvement hint re-using recent material, the
       * page should still sit in the cache, or we're using old info, which
       * should be discarded.
       */
      page = env->page_manager->fetch(context, hints.leaf_page_addr,
                                          PageManager::kOnlyFromCache
                                            | PageManager::kReadOnly);
      if (likely(page != 0)) {
        node = btree->get_node_from_page(page);
        assert(node->is_leaf());

        uint32_t is_approx_match;
        slot = find(context, page, key, flags, &is_approx_match);

        /*
         * if we didn't hit a match OR a match at either edge, FAIL.
         * A match at one of the edges is very risky, as this can also
         * signal a match far away from the current node, so we need
         * the full tree traversal then.
         */
        if (is_approx_match || slot <= 0 || slot >= (int)node->length() - 1)
          slot = -1;

        /* fall through */
      }
    }

    uint32_t is_approx_match = 0;

    if (slot == -1) {
      /* load the root page */
      page = btree->root_page(context);

      /* now traverse the root to the leaf nodes till we find a leaf */
      node = btree->get_node_from_page(page);
      while (!node->is_leaf()) {
        page = btree->find_lower_bound(context, page, key,
                              PageManager::kReadOnly, 0);
        if (unlikely(!page)) {
          stats->find_failed();
          return UPS_KEY_NOT_FOUND;
        }

        node = btree->get_node_from_page(page);
      }

      /* check the leaf page for the key (shortcut w/o approx. matching) */
      if (flags == 0 || flags == LocalCursor::kSyncDontLoadKey) {
        slot = node->find(context, key);
        if (unlikely(slot == -1)) {
          stats->find_failed();
          return UPS_KEY_NOT_FOUND;
        }

        goto return_result;
      }

      /* check the leaf page for the key (long path w/ approx. matching),
       * then fall through */
      slot = find(context, page, key, flags, &is_approx_match);
    }

    if (unlikely(slot == -1)) {
      // find the left sibling
      if (node->left_sibling() > 0) {
        page = env->page_manager->fetch(context, node->left_sibling(),
                        PageManager::kReadOnly);
        node = btree->get_node_from_page(page);
        slot = node->length() - 1;
        is_approx_match = BtreeKey::kLower;
      }
    }
    else if (unlikely(slot >= (int)node->length())) {
      // find the right sibling
      if (node->right_sibling() > 0) {
        page = env->page_manager->fetch(context, node->right_sibling(),
                        PageManager::kReadOnly);
        node = btree->get_node_from_page(page);
        slot = 0;
        is_approx_match = BtreeKey::kGreater;
      }
      else
        slot = -1;
    }

    if (unlikely(slot < 0)) {
      stats->find_failed();
      return UPS_KEY_NOT_FOUND;
    }

    assert(node->is_leaf());

return_result:
    /* set the btree cursor's position to this key */
    if (cursor)
      cursor->couple_to(page, slot, 0);

    /* approx. match: patch the key flags */
    if (is_approx_match)
      ups_key_set_intflags(key, is_approx_match);

    /* no need to load the key if we have an exact match, or if KEY_DONT_LOAD
     * is set: */
    if (key && is_approx_match && NOTSET(flags, LocalCursor::kSyncDontLoadKey))
      node->key(context, slot, key_arena, key);

    if (likely(record != 0))
      node->record(context, slot, record_arena, record, flags);

    return 0;
  }

  // Searches a leaf node for a key.
  //
  // !!!
  // only works with leaf nodes!!
  //
  // Returns the index of the key, or -1 if the key was not found, or
  // another negative status code value when an unexpected error occurred.
  int find(Context *context, Page *page, ups_key_t *key, uint32_t flags,
                  uint32_t *is_approx_match) {
    *is_approx_match = 0;

    /* ensure the approx flag is NOT set by anyone yet */
    BtreeNodeProxy *node = btree->get_node_from_page(page);
    if (unlikely(node->length() == 0))
      return -1;

    int cmp;
    int slot = node->find_lower_bound(context, key, 0, &cmp);

    /* successfull match */
    if (cmp == 0 && (flags == 0 || ISSET(flags, UPS_FIND_EQ_MATCH)))
      return slot;

    /* approx. matching: smaller key is required */
    if (ISSET(flags, UPS_FIND_LT_MATCH)) {
      if (cmp == 0 && ISSET(flags, UPS_FIND_GT_MATCH)) {
        *is_approx_match = BtreeKey::kLower;
        return slot + 1;
      }

      if (slot < 0 && ISSET(flags, UPS_FIND_GT_MATCH)) {
        *is_approx_match = BtreeKey::kGreater;
        return 0;
      }
      *is_approx_match = BtreeKey::kLower;
      return cmp <= 0 ? slot - 1 : slot;
    }

    /* approx. matching: greater key is required */
    if (ISSET(flags, UPS_FIND_GT_MATCH)) {
      *is_approx_match = BtreeKey::kGreater;
      return slot + 1;
    }

    return cmp ? -1 : slot;
  }

  // the current btree
  BtreeIndex *btree;

  // The caller's Context
  Context *context;

  // the current cursor
  BtreeCursor *cursor;

  // the key that is retrieved
  ups_key_t *key;

  // the record that is retrieved
  ups_record_t *record;

  // flags of ups_db_find()
  uint32_t flags;

  // allocator for the key data
  ByteArray *key_arena;

  // allocator for the record data
  ByteArray *record_arena;
};

ups_status_t
BtreeIndex::find(Context *context, LocalCursor *cursor, ups_key_t *key,
              ByteArray *key_arena, ups_record_t *record,
              ByteArray *record_arena, uint32_t flags)
{
  BtreeFindAction bfa(this, context, cursor ? &cursor->btree_cursor : 0,
                  key, key_arena, record,
                record_arena, flags);
  return bfa.run();
}

} // namespace upscaledb

