/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
#include "4cursor/cursor.h"
#include "4db/db.h"
#include "4env/env.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

using namespace std;

namespace hamsterdb {

class BtreeInsertAction : public BtreeUpdateAction
{
  public:
    BtreeInsertAction(BtreeIndex *btree, Context *context, Cursor *cursor,
                    ham_key_t *key, ham_record_t *record, uint32_t flags)
      : BtreeUpdateAction(btree, context, cursor
                                            ? cursor->get_btree_cursor()
                                            : 0, 0),
        m_key(key), m_record(record), m_flags(flags) {
      if (m_cursor)
        m_duplicate_index = m_cursor->get_duplicate_index();
    }

    // This is the entry point for the actual insert operation
    ham_status_t run() {
      BtreeStatistics *stats = m_btree->get_statistics();

      m_hints = stats->get_insert_hints(m_flags);
      
      ham_assert((m_hints.flags & (HAM_DUPLICATE_INSERT_BEFORE
                            | HAM_DUPLICATE_INSERT_AFTER
                            | HAM_DUPLICATE_INSERT_FIRST
                            | HAM_DUPLICATE_INSERT_LAST))
                ? (m_hints.flags & HAM_DUPLICATE)
                : 1);

      /*
       * append the key? append_or_prepend_key() will try to append or
       * prepend the key; if this fails because the key is NOT the largest
       * (or smallest) key in the database or because the current page is
       * already full, it will remove the HINT_APPEND (or HINT_PREPEND)
       * flag and call insert()
       */
      ham_status_t st;
      if (m_hints.leaf_page_addr
          && (m_hints.flags & HAM_HINT_APPEND
              || m_hints.flags & HAM_HINT_PREPEND))
        st = append_or_prepend_key();
      else
        st = insert();

      if (st == HAM_LIMITS_REACHED)
        st = insert();

      if (st)
        stats->insert_failed();
      else {
        if (m_hints.processed_leaf_page)
          stats->insert_succeeded(m_hints.processed_leaf_page,
                  m_hints.processed_slot);
      }

      return (st);
    }

  private:
    // Appends a key at the "end" of the btree, or prepends it at the
    // "beginning"
    ham_status_t append_or_prepend_key() {
      Page *page;
      LocalDatabase *db = m_btree->get_db();
      LocalEnvironment *env = db->lenv();
      bool force_append = false;
      bool force_prepend = false;

      /*
       * see if we get this btree leaf; if not, revert to regular scan
       *
       * As this is a speed-improvement hint re-using recent material, the page
       * should still sit in the cache, or we're using old info, which should
       * be discarded.
       */
      page = env->page_manager()->fetch(m_context, m_hints.leaf_page_addr,
                      PageManager::kOnlyFromCache);
      /* if the page is not in cache: do a regular insert */
      if (!page)
        return (insert());

      BtreeNodeProxy *node = m_btree->get_node_from_page(page);
      ham_assert(node->is_leaf());

      /*
       * if the page is already full OR this page is not the right-most page
       * when we APPEND or the left-most node when we PREPEND
       * OR the new key is not the highest key: perform a normal insert
       */
      if ((m_hints.flags & HAM_HINT_APPEND && node->get_right() != 0)
              || (m_hints.flags & HAM_HINT_PREPEND && node->get_left() != 0)
              || node->requires_split(m_context, m_key))
        return (insert());

      /*
       * if the page is not empty: check if we append the key at the end/start
       * (depending on the flags), or if it's actually inserted in the middle.
       */
      if (node->get_count() != 0) {
        if (m_hints.flags & HAM_HINT_APPEND) {
          int cmp_hi = node->compare(m_context, m_key, node->get_count() - 1);
          /* key is at the end */
          if (cmp_hi > 0) {
            ham_assert(node->get_right() == 0);
            force_append = true;
          }
        }

        if (m_hints.flags & HAM_HINT_PREPEND) {
          int cmp_lo = node->compare(m_context, m_key, 0);
          /* key is at the start of page */
          if (cmp_lo < 0) {
            ham_assert(node->get_left() == 0);
            force_prepend = true;
          }
        }
      }

      /* OK - we're really appending/prepending the new key.  */
      if (force_append || force_prepend)
        return (insert_in_page(page, m_key, m_record, m_hints,
                                force_prepend, force_append));

      /* otherwise reset the hints because they are no longer valid */
      m_hints.flags &= ~HAM_HINT_APPEND;
      m_hints.flags &= ~HAM_HINT_PREPEND;
      return (insert());
    }

    ham_status_t insert() {
      // traverse the tree till a leaf is reached
      Page *parent;
      Page *page = traverse_tree(m_key, m_hints, &parent);

      // We've reached the leaf; it's still possible that we have to
      // split the page, therefore this case has to be handled
      ham_status_t st = insert_in_page(page, m_key, m_record, m_hints);
      if (st == HAM_LIMITS_REACHED) {
        page = split_page(page, parent, m_key, m_hints);
        return (insert_in_page(page, m_key, m_record, m_hints));
      }
      return (st);
    }

    // the key that is inserted
    ham_key_t *m_key;

    // the record that is inserted
    ham_record_t *m_record;

    // flags of ham_db_insert()
    uint32_t m_flags;

    // statistical hints for this operation
    BtreeStatistics::InsertHints m_hints;
};

ham_status_t
BtreeIndex::insert(Context *context, Cursor *cursor, ham_key_t *key,
                ham_record_t *record, uint32_t flags)
{
  context->db = get_db();

  BtreeInsertAction bia(this, context, cursor, key, record, flags);
  return (bia.run());
}

} // namespace hamsterdb

