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
 * btree inserting
 */

#include "0root/root.h"

#include <string.h>
#include <algorithm>

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "1base/dynamic_array.h"
#include "2page/page.h"
#include "3blob_manager/blob_manager.h"
#include "3page_manager/page_manager.h"
#include "3btree/btree_index.h"
#include "3btree/btree_stats.h"
#include "3btree/btree_node_proxy.h"
#include "3btree/btree_cursor.h"
#include "3btree/btree_update.h"
#include "4cursor/cursor_local.h"
#include "4db/db.h"
#include "4env/env.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

using namespace std;

namespace upscaledb {

struct BtreeInsertAction : public BtreeUpdateAction {
  BtreeInsertAction(BtreeIndex *btree_, Context *context_, BtreeCursor *cursor_,
                  ups_key_t *key_, ups_record_t *record_, uint32_t flags_)
    : BtreeUpdateAction(btree_, context_, cursor_,
                    cursor_ ? cursor_->duplicate_index() : 0),
      key(key_), record(record_), flags(flags_) {
  }

  // This is the entry point for the actual insert operation
  ups_status_t run() {
    BtreeStatistics *stats = btree->statistics();
    hints = stats->insert_hints(flags);
    
    assert(ISSETANY(hints.flags, UPS_DUPLICATE_INSERT_BEFORE
                                    | UPS_DUPLICATE_INSERT_AFTER
                                    | UPS_DUPLICATE_INSERT_FIRST
                                    | UPS_DUPLICATE_INSERT_LAST)
              ? ISSET(hints.flags, UPS_DUPLICATE)
              : true);

    /*
     * append the key? append_or_prepend_key() will try to append or
     * prepend the key; if this fails because the key is NOT the largest
     * (or smallest) key in the database or because the current page is
     * already full, it will remove the HINT_APPEND (or HINT_PREPEND)
     * flag and call insert()
     */
    ups_status_t st;
    if (hints.leaf_page_addr
            && ISSETANY(hints.flags, UPS_HINT_APPEND | UPS_HINT_PREPEND)) {
      st = append_or_prepend_key();
      if (unlikely(st == UPS_LIMITS_REACHED))
        st = insert();
    }
    else {
      st = insert();
    }

    if (st)
      stats->insert_failed();
    else {
      if (hints.processed_leaf_page)
        stats->insert_succeeded(hints.processed_leaf_page,
                hints.processed_slot);
    }

    return st;
  }

  // Appends a key at the "end" of the btree, or prepends it at the
  // "beginning"
  ups_status_t append_or_prepend_key() {
    LocalEnv *env = (LocalEnv *)btree->db()->env;
    bool force_append = false;
    bool force_prepend = false;

    /*
     * see if we get this btree leaf; if not, revert to regular scan
     *
     * As this is a speed-improvement hint re-using recent material, the page
     * should still sit in the cache, or we're using old info, which should
     * be discarded.
     */
    Page *page = env->page_manager->fetch(context, hints.leaf_page_addr,
                    PageManager::kOnlyFromCache);
    /* if the page is not in cache: do a regular insert */
    if (!page)
      return insert();

    BtreeNodeProxy *node = btree->get_node_from_page(page);
    assert(node->is_leaf());

    /*
     * if the page is already full OR this page is not the right-most page
     * when we APPEND or the left-most node when we PREPEND
     * OR the new key is not the highest key: perform a normal insert
     */
    if ((ISSET(hints.flags, UPS_HINT_APPEND) && node->right_sibling())
            || (ISSET(hints.flags, UPS_HINT_PREPEND) && node->left_sibling())
            || node->requires_split(context, key))
      return insert();

    /*
     * if the page is not empty: check if we append the key at the end/start
     * (depending on the flags), or if it's actually inserted in the middle.
     */
    if (node->length() != 0) {
      if (ISSET(hints.flags, UPS_HINT_APPEND)) {
        int cmp_hi = node->compare(context, key, node->length() - 1);
        /* key is at the end */
        if (cmp_hi > 0) {
          assert(node->right_sibling() == 0);
          force_append = true;
        }
      }

      if (ISSET(hints.flags, UPS_HINT_PREPEND)) {
        int cmp_lo = node->compare(context, key, 0);
        /* key is at the start of page */
        if (cmp_lo < 0) {
          assert(node->left_sibling() == 0);
          force_prepend = true;
        }
      }
    }

    /* OK - we're really appending/prepending the new key.  */
    if (force_append || force_prepend)
      return insert_in_page(page, key, record, hints, force_prepend,
                      force_append);

    /* otherwise reset the hints because they are no longer valid */
    hints.flags &= ~UPS_HINT_APPEND;
    hints.flags &= ~UPS_HINT_PREPEND;
    return insert();
  }

  ups_status_t insert() {
    // traverse the tree till a leaf is reached
    Page *parent;
    Page *page = traverse_tree(context, key, hints, &parent);

    // We've reached the leaf; it's still possible that we have to
    // split the page, therefore this case has to be handled
    ups_status_t st = insert_in_page(page, key, record, hints);
    if (unlikely(st == UPS_LIMITS_REACHED)) {
      page = split_page(page, parent, key, hints);
      return insert_in_page(page, key, record, hints);
    }

    return st;
  }

  // the key that is inserted
  ups_key_t *key;

  // the record that is inserted
  ups_record_t *record;

  // flags of ups_db_insert()
  uint32_t flags;

  // statistical hints for this operation
  BtreeStatistics::InsertHints hints;
};

ups_status_t
BtreeIndex::insert(Context *context, LocalCursor *cursor, ups_key_t *key,
                ups_record_t *record, uint32_t flags)
{
  context->db = db();

  BtreeInsertAction bia(this, context, cursor ? &cursor->btree_cursor : 0,
                  key, record, flags);
  ups_status_t st = bia.run();
  if (likely(st == 0)) {
    if (cursor)
      cursor->activate_btree();
  }
  return st;
}

} // namespace upscaledb

