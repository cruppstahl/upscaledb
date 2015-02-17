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

#include "0root/root.h"

#include <string.h>

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "1base/dynamic_array.h"
#include "2page/page.h"
#include "3page_manager/page_manager.h"
#include "3blob_manager/blob_manager.h"
#include "3btree/btree_stats.h"
#include "3btree/btree_index.h"
#include "3btree/btree_update.h"
#include "3btree/btree_node_proxy.h"
#include "4db/db.h"
#include "4cursor/cursor.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

/*
 * Erases key/value pairs from a btree
 */
class BtreeEraseAction : public BtreeUpdateAction
{
  public:
    BtreeEraseAction(BtreeIndex *btree, Context *context, Cursor *cursor,
                    ham_key_t *key, int duplicate_index = 0, uint32_t flags = 0)
      : BtreeUpdateAction(btree, context, cursor
                                            ? cursor->get_btree_cursor()
                                            : 0, duplicate_index),
        m_key(key), m_flags(flags) {
      if (m_cursor)
        m_duplicate_index = m_cursor->get_duplicate_index() + 1;
    }

    // This is the entry point for the erase operation
    ham_status_t run() {
      // Coupled cursor: try to remove the key directly from the page
      if (m_cursor) {
        if (m_cursor->get_state() == BtreeCursor::kStateCoupled) {
          Page *coupled_page;
          int coupled_index;
          m_cursor->get_coupled_key(&coupled_page, &coupled_index);

          BtreeNodeProxy *node = m_btree->get_node_from_page(coupled_page);
          ham_assert(node->is_leaf());

          // Now try to delete the key. This can require a page split if the
          // KeyList is not "delete-stable" (some compressed lists can
          // grow when keys are deleted).
          try {
            remove_entry(coupled_page, 0, coupled_index);
          }
          catch (Exception &ex) {
            if (ex.code != HAM_LIMITS_REACHED)
              throw ex;
            goto fall_through;
          }
          // TODO if the page is empty then ask the janitor to clean it up
          return (0);

fall_through:
          m_cursor->uncouple_from_page(m_context);
        }

        if (m_cursor->get_state() == BtreeCursor::kStateUncoupled)
          m_key = m_cursor->get_uncoupled_key();
      }

      return (erase());
    }

  private:
    ham_status_t erase() {
      // traverse the tree to the leaf, splitting/merging nodes as required
      Page *parent;
      BtreeStatistics::InsertHints hints;
      Page *page = traverse_tree(m_key, hints, &parent);
      BtreeNodeProxy *node = m_btree->get_node_from_page(page);

      // we have reached the leaf; search the leaf for the key
      int slot = node->find_exact(m_context, m_key);
      if (slot < 0) {
        m_btree->get_statistics()->erase_failed();
        return (HAM_KEY_NOT_FOUND);
      }

      // remove the key from the leaf
      return (remove_entry(page, parent, slot));
    }

    ham_status_t remove_entry(Page *page, Page *parent, int slot) {
      LocalDatabase *db = m_btree->get_db();
      BtreeNodeProxy *node = m_btree->get_node_from_page(page);

      ham_assert(slot >= 0);
      ham_assert(slot < (int)node->get_count());

      // delete the record, but only on leaf nodes! internal nodes don't have
      // records; they point to pages instead, and we do not want to delete
      // those.
      bool has_duplicates_left = false;
      if (node->is_leaf()) {
        // only delete a duplicate?
        if (m_duplicate_index > 0)
          node->erase_record(m_context, slot, m_duplicate_index - 1, false,
                        &has_duplicates_left);
        else
          node->erase_record(m_context, slot, 0, true, 0);
      }

      page->set_dirty(true);

      // still got duplicates left? then adjust all cursors
      if (node->is_leaf() && has_duplicates_left && db->cursor_list()) {
        Cursor *cursors = db->cursor_list();
        BtreeCursor *btcur = cursors->get_btree_cursor();

        int duplicate_index =
                m_cursor
                    ? m_cursor->get_duplicate_index()
                    : m_duplicate_index;

        while (btcur) {
          BtreeCursor *next = 0;
          if (cursors->get_next()) {
            cursors = cursors->get_next();
            next = cursors->get_btree_cursor();
          }

          if (btcur != m_cursor && btcur->points_to(m_context, page, slot)) {
            if (btcur->get_duplicate_index() == duplicate_index)
                btcur->set_to_nil();
            else if (btcur->get_duplicate_index() > duplicate_index)
              btcur->set_duplicate_index(btcur->get_duplicate_index() - 1);
          }
          btcur = next;
        }
        // all cursors were adjusted, the duplicate was deleted. return
        // to caller!
        return (0);
      }

      // no duplicates left, the key was deleted; all cursors pointing to
      // this key are set to nil, all cursors pointing to a key in the same
      // page are adjusted, if necessary
      if (node->is_leaf() && !has_duplicates_left && db->cursor_list()) {
        Cursor *cursors = db->cursor_list();
        BtreeCursor *btcur = cursors->get_btree_cursor();

        /* 'nil' every cursor which points to the deleted key, and adjust
         * other cursors attached to the same page */
        while (btcur) {
          BtreeCursor *cur = btcur;
          BtreeCursor *next = 0;
          if (cursors->get_next()) {
            cursors = cursors->get_next();
            next = cursors->get_btree_cursor();
          }
          if (btcur != m_cursor && cur->points_to(m_context, page, slot))
            cur->set_to_nil();
          else if (btcur != m_cursor
                  && (cur->get_state() & BtreeCursor::kStateCoupled)) {
            Page *coupled_page;
            int coupled_slot;
            cur->get_coupled_key(&coupled_page, &coupled_slot);
            if (coupled_page == page && coupled_slot > slot)
              cur->uncouple_from_page(m_context);
          }
          btcur = next;
        }
      }

      if (has_duplicates_left)
        return (0);

      // We've reached the leaf; it's still possible that we have to
      // split the page, therefore this case has to be handled
      try {
        node->erase(m_context, slot);
      }
      catch (Exception &ex) {
        if (ex.code != HAM_LIMITS_REACHED)
          throw ex;

        // Split the page in the middle. This will invalidate the |node| pointer
        // and the |slot| of the key, therefore restart the whole operation
        BtreeStatistics::InsertHints hints = {0};
        split_page(page, parent, m_key, hints);
        return (erase());
      }

      return (0);
    }

    // the key that is retrieved
    ham_key_t *m_key;

    // flags of ham_db_erase()
    uint32_t m_flags;
};

ham_status_t
BtreeIndex::erase(Context *context, Cursor *cursor, ham_key_t *key,
                int duplicate, uint32_t flags)
{
  context->db = get_db();

  BtreeEraseAction bea(this, context, cursor, key, duplicate, flags);
  return (bea.run());
}

} // namespace hamsterdb
