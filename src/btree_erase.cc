/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include "config.h"

#include <string.h>

#include "db.h"
#include "error.h"
#include "page.h"
#include "txn.h"
#include "util.h"
#include "cursor.h"
#include "btree_stats.h"
#include "btree_index.h"
#include "btree_node_proxy.h"
#include "page_manager.h"
#include "blob_manager.h"

namespace hamsterdb {

/*
 * Erases key/value pairs from a btree
 */
class BtreeEraseAction
{
    enum {
      kShiftThreshold = 50 // only shift if at least x keys will be shifted
    };

  public:
    BtreeEraseAction(BtreeIndex *btree, Transaction *txn, Cursor *cursor,
        ham_key_t *key, ham_u32_t dupe_id = 0, ham_u32_t flags = 0)
      : m_btree(btree), m_txn(txn), m_cursor(0), m_key(key),
        m_dupe_id(dupe_id), m_flags(flags), m_mergepage(0) {
      if (cursor) {
        m_cursor = cursor->get_btree_cursor();
        m_dupe_id = m_cursor->get_duplicate_index() + 1;
      }
    }

    ham_status_t run() {
      Page *root, *p;
      LocalDatabase *db = m_btree->get_db();
      LocalEnvironment *env = db->get_local_env();

      /* coupled cursor: try to remove the key directly from the page.
       * if that's not possible (i.e. because of underflow): uncouple
       * the cursor and process the normal erase algorithm */
      if (m_cursor) {
        if (m_cursor->get_state() == BtreeCursor::kStateCoupled) {
          Page *coupled_page;
          ham_u32_t coupled_index;
          m_cursor->get_coupled_key(&coupled_page, &coupled_index);

          BtreeNodeProxy *node = m_btree->get_node_from_page(coupled_page);
          ham_assert(node->is_leaf());
          if (coupled_index > 0 && node->requires_merge()) {
            /* yes, we can remove the key */
            remove_entry(coupled_page, coupled_index);
            return (0);
          }
          else {
            /* otherwise uncouple and fall through */
            m_cursor->uncouple_from_page();
          }
        }

        if (m_cursor->get_state() == BtreeCursor::kStateUncoupled)
          m_key = m_cursor->get_uncoupled_key();
      }

      /* get the root-page...  */
      ham_u64_t rootaddr = m_btree->get_root_address();
      if (!rootaddr) {
        m_btree->get_statistics()->erase_failed();
        return (HAM_KEY_NOT_FOUND);
      }

      root = env->get_page_manager()->fetch_page(db, rootaddr);

      /* ... and start the recursion */
      ham_status_t st = erase_recursive(&p, root, 0, 0, 0, 0, 0);
      if (st) {
        m_btree->get_statistics()->erase_failed();
        return (st);
      }

      if (p) {
        /* delete the old root page */
        BtreeCursor::uncouple_all_cursors(root);

        collapse_root(root, p);

        m_btree->get_statistics()->reset_page(root);
      }

      return (0);
    }

  private:
    /* remove an item from a page */
    void remove_entry(Page *page, int slot) {
      LocalDatabase *db = m_btree->get_db();
      BtreeNodeProxy *node = m_btree->get_node_from_page(page);

      ham_assert(slot >= 0);
      ham_assert(slot < (int)node->get_count());

      /* uncouple all cursors */
      BtreeCursor::uncouple_all_cursors(page, slot + 1);

      // delete the record, but only on leaf nodes! internal nodes don't have
      // records; they point to pages instead, and we do not want to delete
      // those.
      bool has_duplicates_left = false;
      if (node->is_leaf()) {
        // only delete a duplicate?
        if (m_dupe_id > 0)
          node->erase_record(slot, m_dupe_id - 1, false, &has_duplicates_left);
        else
          node->erase_record(slot, 0, true, 0);
      }

      page->set_dirty(true);

      // still got duplicates left? then adjust all cursors
      if (node->is_leaf()) {
        BtreeCursor *btc = 0;
        Cursor *cursors = db->get_cursor_list();
        if (cursors)
          btc = cursors->get_btree_cursor();

        if (has_duplicates_left) {
          while (btc && m_cursor) {
            BtreeCursor *next = 0;
            if (cursors->get_next()) {
              cursors = cursors->get_next();
              next = cursors->get_btree_cursor();
            }

            if (btc != m_cursor && btc->points_to(page, slot)) {
              if (btc->get_duplicate_index()
                              == m_cursor->get_duplicate_index())
                  btc->set_to_nil();
              else if (btc->get_duplicate_index()
                              > m_cursor->get_duplicate_index())
                btc->set_duplicate_index(btc->get_duplicate_index() - 1);
            }
            btc = next;
          }
          // all cursors were adjusted, the duplicate was deleted. return
          // to caller!
          return;
        }
        // the full key was deleted; all cursors pointing to this key
        // are set to nil
        else {
          if (cursors) {
            btc = cursors->get_btree_cursor();

            /* make sure that no cursor is pointing to this key */
            while (btc) {
              BtreeCursor *cur = btc;
              BtreeCursor *next = 0;
              if (cursors->get_next()) {
                cursors = cursors->get_next();
                next = cursors->get_btree_cursor();
              }
              if (btc != m_cursor) {
                if (cur->points_to(page, slot))
                  cur->set_to_nil();
              }
              btc = next;
            }
          }
        }
      }

      // now remove the key
      node->erase(slot);
    }

    /*
     * recursively descend down the tree, delete the item and re-balance
     * the tree on the way back up
     *
     * returns the page which is deleted, if available
     */
    ham_status_t erase_recursive(Page **page_ref, Page *page,
                    ham_u64_t left, ham_u64_t right,
                    ham_u64_t lanchor, ham_u64_t ranchor, Page *parent) {
      int slot;
      ham_status_t st = 0;
      Page *newme;
      Page *child;
      Page *tempp = 0;
      LocalDatabase *db = m_btree->get_db();
      LocalEnvironment *env = db->get_local_env();
      BtreeNodeProxy *node = m_btree->get_node_from_page(page);

      *page_ref = 0;

      /* empty node? then most likely we're in the empty root page. */
      if (node->get_count() == 0)
        return (HAM_KEY_NOT_FOUND);

      /* mark the nodes which may need rebalancing */
      bool isfew;
      if (m_btree->get_root_address() == page->get_address())
        isfew = (node->get_count() <= 1);
      else
        isfew = node->requires_merge();

      if (!isfew)
        m_mergepage = 0;
      else if (!m_mergepage)
        m_mergepage = page;

      if (!node->is_leaf())
        child = m_btree->find_internal(page, m_key, &slot);
      else {
        slot = node->find(m_key);
        child = 0;
      }

      /* if this page is not a leaf: recursively descend down the tree */
      if (!node->is_leaf()) {
        ham_u64_t next_lanchor;
        ham_u64_t next_ranchor;
        ham_u64_t next_left;
        ham_u64_t next_right;

        /* calculate neighbor and anchor nodes */
        if (slot == -1) {
          if (!left)
            next_left = 0;
          else {
            tempp = env->get_page_manager()->fetch_page(db, left);
            BtreeNodeProxy *n = m_btree->get_node_from_page(tempp);
            next_left = n->get_record_id(n->get_count() - 1);
          }
          next_lanchor = lanchor;
        }
        else {
          if (slot == 0)
            next_left = node->get_ptr_down();
          else
            next_left = node->get_record_id(slot - 1);
          next_lanchor = page->get_address();
        }

        if (slot == (int)node->get_count() - 1) {
          if (!right)
            next_right = 0;
          else {
            tempp = env->get_page_manager()->fetch_page(db, right);
            BtreeNodeProxy *n = m_btree->get_node_from_page(tempp);
            next_right = n->get_record_id(0);
          }
          next_ranchor = ranchor;
        }
        else {
          next_right = node->get_record_id(slot + 1);
          next_ranchor = page->get_address();
        }

        st = erase_recursive(&newme, child, next_left, next_right,
                        next_lanchor, next_ranchor, page);
        if (st)
          return (st);
      }
      else {
        /*
         * otherwise (page is a leaf) delete the key...
         *
         * first, check if this entry really exists
         */
        newme = 0;
        if (slot != -1) {
          int cmp = node->compare(m_key, slot);
          if (cmp == 0)
            newme = page;
          else
            return (HAM_KEY_NOT_FOUND);
        }
        if (!newme) {
          m_mergepage = 0;
          return (HAM_KEY_NOT_FOUND);
        }
      }

      /* ... and rebalance the tree, if necessary */
      if (newme) {
        if (slot == -1)
          slot = 0;
        remove_entry(page, slot);
      }

      /* no need to rebalance in case of an error */
      rebalance(page_ref, page, left, right, lanchor, ranchor, parent);

      return (0);
    }

    /*
     * rebalance a page - either shifts elements to a sibling, or merges
     * the page with a sibling
     */
    void rebalance(Page **pnewpage, Page *page, ham_u64_t left,
                    ham_u64_t right, ham_u64_t lanchor,
                    ham_u64_t ranchor, Page *parent) {
      BtreeNodeProxy *node = m_btree->get_node_from_page(page);
      Page *leftpage = 0;
      Page *rightpage = 0;
      BtreeNodeProxy *leftnode = 0;
      BtreeNodeProxy *rightnode = 0;
      LocalDatabase *db = page->get_db();
      LocalEnvironment *env = db->get_local_env();
      bool fewleft = false;
      bool fewright = false;

      ham_assert(page->get_db());

      *pnewpage = 0;
      if (!m_mergepage)
        return;

      /* get the left and the right sibling of this page */
      if (left) {
        leftpage = env->get_page_manager()->fetch_page(db, node->get_left());
        if (leftpage) {
          leftnode = m_btree->get_node_from_page(leftpage);
          fewleft  = leftnode->requires_merge();
        }
      }
      if (right) {
        rightpage = env->get_page_manager()->fetch_page(db, node->get_right());
        if (rightpage) {
          rightnode = m_btree->get_node_from_page(rightpage);
          fewright  = rightnode->requires_merge();
        }
      }

      /* if we have no siblings, then we're rebalancing the root page */
      if (!leftpage && !rightpage) {
        if (!node->is_leaf())
          *pnewpage = env->get_page_manager()->fetch_page(db,
                  node->get_ptr_down());
        return;
      }

      /*
       * if one of the siblings is missing, or both of them are
       * too empty, we have to merge them
       */
      if ((!leftpage || fewleft) && (!rightpage || fewright)) {
        if (parent && lanchor != parent->get_address())
          merge_pages(pnewpage, page, rightpage, ranchor);
        else
          merge_pages(pnewpage, leftpage, page, lanchor);
        return;
      }

      /* otherwise choose the better of a merge or a shift */
      if (leftpage && fewleft && rightpage && !fewright) {
        if (parent && (!(ranchor == parent->get_address()) &&
            (page->get_address() == m_mergepage->get_address())))
          merge_pages(pnewpage, leftpage, page, lanchor);
        else
          shift_pages(page, rightpage, ranchor);
        return;
      }

      /* ... still choose the better of a merge or a shift... */
      if (leftpage && !fewleft && rightpage && fewright) {
        if (parent && (!(lanchor == parent->get_address()) &&
                (page->get_address() == m_mergepage->get_address())))
          merge_pages(pnewpage, page, rightpage, ranchor);
        else
          shift_pages(leftpage, page, lanchor);
        return;
      }

      /* choose the more effective of two shifts */
      if (lanchor == ranchor) {
        if (leftnode != 0 && rightnode != 0
                && leftnode->get_count() <= rightnode->get_count())
          shift_pages(page, rightpage, ranchor);
        else
          shift_pages(leftpage, page, lanchor);
        return;
      }

      /* choose the shift with more local effect */
      if (parent && lanchor == parent->get_address())
        shift_pages(leftpage, page, lanchor);
      else
        shift_pages(page, rightpage, ranchor);
      return;
    }

    /*
     * shift items from a sibling to this page, till both pages have an equal
     * number of items
     */
    void shift_pages(Page *page, Page *sibpage, ham_u64_t anchor) {
      LocalDatabase *db = m_btree->get_db();
      LocalEnvironment *env = db->get_local_env();
      int slot = 0;

      Page *ancpage = env->get_page_manager()->fetch_page(db, anchor);

      BtreeNodeProxy *node    = m_btree->get_node_from_page(page);
      BtreeNodeProxy *sibnode = m_btree->get_node_from_page(sibpage);
      BtreeNodeProxy *ancnode = m_btree->get_node_from_page(ancpage);

      ham_assert(sibnode->is_leaf() == node->is_leaf());

      /* do not shift if both pages have (nearly) equal size; too much
       * effort for too little gain! */
      if (node->get_count() > 20 && sibnode->get_count() > 20) {
        if (std::max(node->get_count(), sibnode->get_count())
                  - std::min(node->get_count(), sibnode->get_count())
              < kShiftThreshold) {
          m_mergepage = 0;
          return;
        }
      }

      /* uncouple all cursors */
      BtreeCursor::uncouple_all_cursors(page);
      BtreeCursor::uncouple_all_cursors(sibpage);
      BtreeCursor::uncouple_all_cursors(ancpage);

      bool internal = !node->is_leaf();

      ham_assert(node->get_count() != sibnode->get_count());

      /* shift from sibling to this node */
      if (sibnode->get_count() >= node->get_count()) {
        /* internal node: append the anchornode separator value to this node */
        if (internal) {
          slot = ancnode->find(sibnode, 0);
          ham_u32_t position = node->get_count();

          /* this appends the key at the end of the node */
          node->insert(position, ancnode, slot);

          /* the pointer of this new node is ptr_down of the sibling */
          node->set_record_id(position, sibnode->get_ptr_down());

          /* new pointer left of the sibling is sibling[0].ptr */
          sibnode->set_ptr_down(sibnode->get_record_id(0));

          /* update the anchor node with sibling[0] */
          sibnode->replace_key(0, ancnode, slot);

          /* and remove this key from the sibling */
          sibnode->erase(0);
        }

        ham_u32_t c = (sibnode->get_count() - node->get_count()) / 2;
        if (c == 0)
          goto cleanup;
        if (internal)
          c--;
        if (c == 0)
          goto cleanup;

        /* internal node: append the anchor key to the page */
        if (internal) {
          ham_u32_t position = node->get_count();

          /* this appends the key at the end of the node */
          node->insert(position, ancnode, slot);

          /* the pointer of this new node is ptr_down of the sibling */
          node->set_record_id(position, sibnode->get_ptr_down());
        }

        /* get the slot in the anchor node BEFORE the keys are shifted
         * (it will be required later) */
        slot = ancnode->find(sibnode, 0);

        /* shift |c| items from the right sibling to this page, then
         * delete the shifted items */
        node->shift_from_right(sibnode, c);

        /*
         * internal nodes: don't forget to set ptr_down of the sibling, and
         * replace the anchor key
         */
        if (internal) {
          sibnode->set_ptr_down(sibnode->get_record_id(0));

          if (anchor) {
            //slot = ancnode->find(sibnode, 0);
            /* replace the key */
            sibnode->replace_key(0, ancnode, slot);
          }

          /* shift once more */
          sibnode->erase(0);
        }
        else {
          /* in a leaf - update the anchor */
          //slot = ancnode->find(sibnode, 0);
          /* replace the key */
          sibnode->replace_key(0, ancnode, slot);
        }
      }
      /* this code path shifts keys from this node to the sibling */
      else {
        /* internal node: insert the anchornode separator value to this node */
        if (internal) {
          slot = ancnode->find(sibnode, 0);

          /* copy the old anchor element to sibling[0] */
          sibnode->insert(0, ancnode, slot);

          /* sibling[0].ptr = sibling.ptr_down */
          sibnode->set_record_id(0, sibnode->get_ptr_down());

          /* sibling.ptr_down = node[node.count-1].ptr */
          sibnode->set_ptr_down(node->get_record_id(node->get_count() - 1));

          /* new anchor element is node[node.count-1].key */
          node->replace_key(node->get_count() - 1, ancnode, slot);

          /* current page has now one item less */
          node->erase(node->get_count() - 1);
        }

        ham_u32_t c = (node->get_count() - sibnode->get_count()) / 2;
        if (c == 0)
          goto cleanup;
        if (internal)
          c--;
        if (c == 0)
          goto cleanup;

        /* internal pages: insert the anchor element */
        if (internal) {
          /* shift entire sibling by 1 to the right */
          sibnode->insert(0, ancnode, slot);
          sibnode->set_record_id(0, sibnode->get_ptr_down());
        }

        ham_u32_t s = node->get_count() - c - 1;

        /* shift items from this page to the right sibling, then delete the
         * items from this page */
        node->shift_to_right(sibnode, s + 1, c);

        /*
         * internal nodes: the pointer of the highest item
         * in the node will become the ptr_down of the sibling
         */
        if (internal) {
          sibnode->set_ptr_down(node->get_record_id(node->get_count() - 1));

          /* free the greatest key */
          node->erase(node->get_count() - 1);
        }

        /* replace the old anchor key with the new anchor key */
        if (anchor) {
          if (internal) {
            slot = ancnode->find(node, s);
            node->replace_key(s, ancnode, slot + 1);
          }
          else {
            slot = ancnode->find(sibnode, 0);
            sibnode->replace_key(0, ancnode, slot + 1);
          }
        }
      }

cleanup:
      /* mark pages as dirty */
      page->set_dirty(true);
      ancpage->set_dirty(true);
      sibpage->set_dirty(true);

      m_mergepage = 0;

      BtreeIndex::ms_btree_smo_shift++;
    }

    /* merge two pages */
    void merge_pages(Page **pnewpage, Page *page, Page *sibpage,
                        ham_u64_t anchor) {
      LocalDatabase *db = m_btree->get_db();
      LocalEnvironment *env = db->get_local_env();
      BtreeNodeProxy *sibnode = m_btree->get_node_from_page(sibpage);
      BtreeNodeProxy *node = m_btree->get_node_from_page(page);

      *pnewpage = 0;

      /* uncouple all cursors */
      BtreeCursor::uncouple_all_cursors(page);
      BtreeCursor::uncouple_all_cursors(sibpage);

      Page *ancpage = 0;
      BtreeNodeProxy *ancnode = 0;
      if (anchor) {
        ancpage = env->get_page_manager()->fetch_page(page->get_db(), anchor);
        ancnode = m_btree->get_node_from_page(ancpage);
        BtreeCursor::uncouple_all_cursors(ancpage);
      }

      /*
       * internal node: append the anchornode separator value to
       * this node
       */
      if (!node->is_leaf()) {
        int slot = ancnode->find(sibnode, 0);
        ham_u32_t position = node->get_count();

        node->insert(position, ancnode, slot);
        node->set_record_id(position, sibnode->get_ptr_down());
      }

      /* merge all items from the sibling into this page */
      node->merge_from(sibnode);
 
      page->set_dirty(true);
      sibpage->set_dirty(true);
      if (ancpage)
        ancpage->set_dirty(true);

      /* update the linked list of pages */
      if (node->get_left() == sibpage->get_address()) {
        if (sibnode->get_left()) {
          Page *p = env->get_page_manager()->fetch_page(page->get_db(),
                  sibnode->get_left());
          BtreeNodeProxy *n = m_btree->get_node_from_page(p);
          n->set_right(sibnode->get_right());
          node->set_left(sibnode->get_left());
          p->set_dirty(true);
        }
        else
          node->set_left(0);
      }
      else if (node->get_right() == sibpage->get_address()) {
        if (sibnode->get_right()) {
          Page *p = env->get_page_manager()->fetch_page(page->get_db(),
                  sibnode->get_right());
          BtreeNodeProxy *n = m_btree->get_node_from_page(p);
          node->set_right(sibnode->get_right());
          n->set_left(sibnode->get_left());
          p->set_dirty(true);
        }
        else
          node->set_right(0);
      }

      /* return this page for deletion */
      if (m_mergepage &&
          (m_mergepage->get_address() == page->get_address()
            || m_mergepage->get_address() == sibpage->get_address()))
        m_mergepage = 0;

      m_btree->get_statistics()->reset_page(sibpage);
      env->get_page_manager()->add_to_freelist(sibpage);

      *pnewpage = sibpage;

      BtreeIndex::ms_btree_smo_merge++;
    }

    /* collapse the root node */
    void collapse_root(Page *oldroot, Page *newroot) {
      LocalEnvironment *env = oldroot->get_db()->get_local_env();
      env->get_page_manager()->add_to_freelist(oldroot);

      m_btree->set_root_address(newroot->get_address());
      ham_assert(newroot->get_db());

      newroot->set_type(Page::kTypeBroot);
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
    ham_u32_t m_dupe_id;

    // flags of ham_db_erase()
    ham_u32_t m_flags;

    // a page which needs rebalancing
    Page *m_mergepage;
};

ham_status_t
BtreeIndex::erase(Transaction *txn, Cursor *cursor, ham_key_t *key,
                ham_u32_t duplicate, ham_u32_t flags)
{
  BtreeEraseAction bea(this, txn, cursor, key, duplicate, flags);
  return (bea.run());
}

} // namespace hamsterdb
