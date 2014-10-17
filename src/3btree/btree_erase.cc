/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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
#include "1base/byte_array.h"
#include "2page/page.h"
#include "3page_manager/page_manager.h"
#include "3blob_manager/blob_manager.h"
#include "3btree/btree_stats.h"
#include "3btree/btree_index.h"
#include "3btree/btree_node_proxy.h"
#include "4db/db.h"
#include "4txn/txn.h"
#include "4cursor/cursor.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

/*
 * Erases key/value pairs from a btree
 */
class BtreeEraseAction
{
  public:
    BtreeEraseAction(BtreeIndex *btree, Transaction *txn, Cursor *cursor,
        ham_key_t *key, int duplicate_index = 0, uint32_t flags = 0)
      : m_btree(btree), m_txn(txn), m_cursor(0), m_key(key),
        m_duplicate_index(duplicate_index), m_flags(flags) {
      if (cursor) {
        m_cursor = cursor->get_btree_cursor();
        m_duplicate_index = m_cursor->get_duplicate_index() + 1;
      }
    }

    // This is the entry point for the erase operation
    ham_status_t run() {
      LocalDatabase *db = m_btree->get_db();
      LocalEnvironment *env = db->get_local_env();

      /* coupled cursor: remove the key directly from the page. */
      if (m_cursor) {
        if (m_cursor->get_state() == BtreeCursor::kStateCoupled) {
          Page *coupled_page;
          int coupled_index;
          m_cursor->get_coupled_key(&coupled_page, &coupled_index);

          BtreeNodeProxy *node = m_btree->get_node_from_page(coupled_page);
          ham_assert(node->is_leaf());
          remove_entry(coupled_page, coupled_index);
          // TODO if the page is empty then ask the janitor to clean it up
          return (0);
        }

        if (m_cursor->get_state() == BtreeCursor::kStateUncoupled)
          m_key = m_cursor->get_uncoupled_key();
      }

      Page *page = env->get_page_manager()->fetch_page(db,
                        m_btree->get_root_address());
      BtreeNodeProxy *node = m_btree->get_node_from_page(page);

      // if the root page is empty with children then collapse it
      if (node->get_count() == 0 && !node->is_leaf()) {
        page = collapse_root(page);
        node = m_btree->get_node_from_page(page);
      }

      int slot;

      // now walk down the tree
      while (!node->is_leaf()) {
        // get the child page
        Page *sib_page = 0;
        Page *child_page = m_btree->find_child(page, m_key, &slot);
        BtreeNodeProxy *child_node = m_btree->get_node_from_page(child_page);

        // We can merge this child with the RIGHT sibling iff...
        // 1. it's not the right-most slot (and therefore the right sibling has
        //      the same parent as the child)
        // 2. the child is a leaf!
        // 3. it's empty or has too few elements
        // 4. its right sibling is also empty
        if (slot < (int)node->get_count() - 1
            && child_node->is_leaf()
            && child_node->requires_merge()
            && child_node->get_right() != 0) {
          sib_page = env->get_page_manager()->fetch_page(db,
                                child_node->get_right(),
                                PageManager::kOnlyFromCache);
          if (sib_page != 0) {
            BtreeNodeProxy *sib_node = m_btree->get_node_from_page(sib_page);
            if (sib_node->requires_merge()) {
              merge_page(child_page, sib_page);
              // also remove the link to the sibling from the parent
              remove_entry(page, slot + 1);
            }
          }
        }

        // We can also merge this child with the LEFT sibling iff...
        // 1. it's not the left-most slot
        // 2. the child is a leaf!
        // 3. it's empty or has too few elements
        // 4. its left sibling is also empty
        else if (slot > 0
            && child_node->is_leaf()
            && child_node->requires_merge()
            && child_node->get_left() != 0) {
          sib_page = env->get_page_manager()->fetch_page(db,
                                child_node->get_left(),
                                PageManager::kOnlyFromCache);
          if (sib_page != 0) {
            BtreeNodeProxy *sib_node = m_btree->get_node_from_page(sib_page);
            if (sib_node->requires_merge()) {
              merge_page(sib_page, child_page);
              // also remove the link to the sibling from the parent
              remove_entry(page, slot);
              // continue with the sibling
              child_page = sib_page;
              child_node = sib_node;
            }
          }
        }

        // go down one level in the tree
        page = child_page;
        node = child_node;
      }

      // we have reached the leaf; search the leaf for the key
      slot = node->find_exact(m_key);
      if (slot < 0) {
        m_btree->get_statistics()->erase_failed();
        return (HAM_KEY_NOT_FOUND);
      }

      // remove the key from the leaf
      remove_entry(page, slot);
      return (0);
    }

  private:
    /* remove an item from a page */
    void remove_entry(Page *page, int slot) {
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
          node->erase_record(slot, m_duplicate_index - 1, false,
                        &has_duplicates_left);
        else
          node->erase_record(slot, 0, true, 0);
      }

      page->set_dirty(true);

      // still got duplicates left? then adjust all cursors
      if (node->is_leaf() && has_duplicates_left && db->get_cursor_list()) {
        Cursor *cursors = db->get_cursor_list();
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

          if (btcur != m_cursor && btcur->points_to(page, slot)) {
            if (btcur->get_duplicate_index() == duplicate_index)
                btcur->set_to_nil();
            else if (btcur->get_duplicate_index() > duplicate_index)
              btcur->set_duplicate_index(btcur->get_duplicate_index() - 1);
          }
          btcur = next;
        }
        // all cursors were adjusted, the duplicate was deleted. return
        // to caller!
        return;
      }

      // no duplicates left, the key was deleted; all cursors pointing to
      // this key are set to nil, all cursors pointing to a key in the same
      // page are adjusted, if necessary
      if (node->is_leaf() && !has_duplicates_left && db->get_cursor_list()) {
        Cursor *cursors = db->get_cursor_list();
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
          if (btcur != m_cursor && cur->points_to(page, slot))
            cur->set_to_nil();
          else if (btcur != m_cursor
                  && (cur->get_state() & BtreeCursor::kStateCoupled)) {
            Page *coupled_page;
            int coupled_slot;
            cur->get_coupled_key(&coupled_page, &coupled_slot);
            if (coupled_page == page && coupled_slot > slot)
              cur->uncouple_from_page();
          }
          btcur = next;
        }
      }

      // now remove the key
      if (!has_duplicates_left)
        node->erase(slot);
    }

    /* Merges the |sibling| into |page|, returns the merged page and moves
     * the sibling to the freelist */ 
    Page *merge_page(Page *page, Page *sibling) {
      LocalDatabase *db = m_btree->get_db();
      LocalEnvironment *env = db->get_local_env();

      BtreeNodeProxy *node = m_btree->get_node_from_page(page);
      BtreeNodeProxy *sib_node = m_btree->get_node_from_page(sibling);

      node->merge_from(sib_node);
      page->set_dirty(true);

      // fix the linked list
      node->set_right(sib_node->get_right());
      if (node->get_right()) {
        Page *new_right = env->get_page_manager()->fetch_page(db,
                                  node->get_right());
        BtreeNodeProxy *new_right_node = m_btree->get_node_from_page(new_right);
        new_right_node->set_left(page->get_address());
        new_right->set_dirty(true);
      }

      m_btree->get_statistics()->reset_page(sibling);
      m_btree->get_statistics()->reset_page(page);
      env->get_page_manager()->add_to_freelist(sibling);

      BtreeIndex::ms_btree_smo_merge++;
      return (page);
    }

    /* collapse the root node; returns the new root */
    Page *collapse_root(Page *root_page) {
      LocalEnvironment *env = root_page->get_db()->get_local_env();
      BtreeNodeProxy *node = m_btree->get_node_from_page(root_page);
      ham_assert(node->get_count() == 0);

      m_btree->get_statistics()->reset_page(root_page);
      m_btree->set_root_address(node->get_ptr_down());

      Page *new_root = env->get_page_manager()->fetch_page(root_page->get_db(),
                            m_btree->get_root_address());
      new_root->set_type(Page::kTypeBroot);
      env->get_page_manager()->add_to_freelist(root_page);
      return (new_root);
    }

    // the current btree
    BtreeIndex *m_btree;

    // the current transaction
    Transaction *m_txn;

    // the current cursor
    BtreeCursor *m_cursor;

    // the key that is retrieved
    ham_key_t *m_key;

    // id of the duplicate to erase
    uint32_t m_duplicate_index;

    // flags of ham_db_erase()
    uint32_t m_flags;
};

ham_status_t
BtreeIndex::erase(Transaction *txn, Cursor *cursor, ham_key_t *key,
                int duplicate, uint32_t flags)
{
  BtreeEraseAction bea(this, txn, cursor, key, duplicate, flags);
  return (bea.run());
}

} // namespace hamsterdb
