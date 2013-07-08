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

#include "blob_manager.h"
#include "btree.h"
#include "cache.h"
#include "db.h"
#include "device.h"
#include "env.h"
#include "error.h"
#include "extkeys.h"
#include "btree_key.h"
#include "log.h"
#include "mem.h"
#include "page.h"
#include "btree_stats.h"
#include "txn.h"
#include "util.h"
#include "cursor.h"
#include "btree_node.h"
#include "page_manager.h"

namespace hamsterdb {

/*
 * Erases key/value pairs from a btree
 */
class BtreeEraseAction
{
  enum {
    /* flags for replace_key */
    kInternalKey = 2
  };

  public:
    BtreeEraseAction(BtreeIndex *btree, Transaction *txn, Cursor *cursor,
        ham_key_t *key, ham_u32_t dupe_id = 0, ham_u32_t flags = 0)
      : m_btree(btree), m_txn(txn), m_cursor(0), m_key(key),
        m_dupe_id(dupe_id), m_flags(flags), m_mergepage(0) {
      if (cursor)
        m_cursor = cursor->get_btree_cursor();
    }

    ham_status_t run() {
      Page *root, *p;
      LocalDatabase *db = m_btree->get_db();

      /* coupled cursor: try to remove the key directly from the page.
       * if that's not possible (i.e. because of underflow): uncouple
       * the cursor and process the normal erase algorithm */
      if (m_cursor) {
        if (m_cursor->get_state() == BtreeCursor::kStateCoupled) {
          Page *coupled_page;
          ham_u32_t coupled_index;
          m_cursor->get_coupled_key(&coupled_page, &coupled_index);

          PBtreeNode *node = PBtreeNode::from_page(coupled_page);
          ham_assert(node->is_leaf());
          if (coupled_index > 0
              && node->get_count() > m_btree->get_minkeys()) {
            /* yes, we can remove the key */
            return (remove_entry(coupled_page, coupled_index));
          }
          else {
            /* otherwise uncouple and fall through */
            ham_status_t st = m_cursor->uncouple_from_page();
            if (st)
              return (st);
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
      ham_status_t st = db->get_env()->get_page_manager()->fetch_page(&root,
                      db, rootaddr);
      if (st)
        return (st);

      /* ... and start the recursion */
      st = erase_recursive(&p, root, 0, 0, 0, 0, 0);
      if (st) {
        m_btree->get_statistics()->erase_failed();
        return (st);
      }

      if (p) {
        /* delete the old root page */
        st = BtreeCursor::uncouple_all_cursors(root);
        if (st) {
          m_btree->get_statistics()->erase_failed();
          return (st);
        }

        st = collapse_root(root, p);
        if (st) {
          m_btree->get_statistics()->erase_failed();
          return (st);
        }

        m_btree->get_statistics()->reset_page(root);
      }

      return (0);
    }

  private:
    /* remove an item from a page */
    ham_status_t remove_entry(Page *page, ham_s32_t slot) {
      ham_status_t st;
      BtreeCursor *btc = 0;

      LocalDatabase *db = m_btree->get_db();
      PBtreeNode *node = PBtreeNode::from_page(page);
      ham_size_t keysize = m_btree->get_keysize();
      PBtreeKey *bte = node->get_key(db, slot);

      /* uncouple all cursors */
      if ((st = BtreeCursor::uncouple_all_cursors(page)))
        return (st);

      ham_assert(slot >= 0);
      ham_assert(slot < node->get_count());

      /*
       * leaf page: get rid of the record
       *
       * if duplicates are enabled and a cursor exists: remove the duplicate.
       * otherwise remove the full key with all duplicates
       */
      if (node->is_leaf()) {
        Cursor *cursors = db->get_cursor_list();
        ham_u32_t dupe_id = 0;

        if (cursors)
          btc = cursors->get_btree_cursor();

        if (m_cursor)
          dupe_id = m_cursor->get_duplicate_index() + 1;
        else if (m_dupe_id) /* +1-based index */
          dupe_id = m_dupe_id;

        if (bte->get_flags() & PBtreeKey::kDuplicates && dupe_id) {
          st = bte->erase_record(db, m_txn, dupe_id - 1, false);
          if (st)
            return (st);

          /*
           * if the last duplicate was erased (ptr and flags==0):
           * remove the entry completely
           */
          if (bte->get_ptr() == 0 && bte->get_flags() == 0)
            goto free_all;

          /*
           * make sure that no cursor is pointing to this dupe, and shift
           * all other cursors if they point to a different duplicate of
           * the same key
           */
          while (btc && m_cursor) {
            BtreeCursor *next = 0;
            if (cursors->get_next()) {
              cursors = cursors->get_next();
              next = cursors->get_btree_cursor();
            }

            if (btc != m_cursor) {
              if (btc->get_duplicate_index()
                              == m_cursor->get_duplicate_index()) {
                if (btc->points_to(bte))
                  btc->set_to_nil();
              }
              else if (btc->get_duplicate_index()
                              > m_cursor->get_duplicate_index()) {
                btc->set_duplicate_index(btc->get_duplicate_index() - 1);
              }
            }
            btc = next;
          }

          /* we've removed the duplicate; return to caller */
          return (0);
        }
        else {
          st = bte->erase_record(db, m_txn, 0, true);
          if (st)
            return (st);

free_all:
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
                if (cur->points_to(bte))
                  cur->set_to_nil();
              }
              btc = next;
            }
          }
        }
      }

      /*
       * get rid of the extended key (if there is one); also remove the key
       * from the cache
       */
      if (bte->get_flags() & PBtreeKey::kExtended) {
        ham_u64_t blobid = bte->get_extended_rid(db);
        ham_assert(blobid);

        st = db->remove_extkey(blobid);
        if (st)
          return (st);
      }

      /*
       * if we delete the last item, it's enough to decrement the item
       * counter and return...
       */
      if (slot != node->get_count() - 1) {
        PBtreeKey *lhs = node->get_key(db, slot);
        PBtreeKey *rhs = node->get_key(db, slot + 1);
        memmove(lhs, rhs, ((PBtreeKey::kSizeofOverhead + keysize))
                * (node->get_count() - slot - 1));
      }

      node->set_count(node->get_count() - 1);
      page->set_dirty(true);
      return (0);
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
      ham_s32_t slot;
      ham_status_t st;
      Page *newme;
      Page *child;
      Page *tempp = 0;
      LocalDatabase *db = m_btree->get_db();
      PBtreeNode *node = PBtreeNode::from_page(page);

      *page_ref = 0;

      /* empty node? then most likely we're in the empty root page. */
      if (node->get_count() == 0)
        return (HAM_KEY_NOT_FOUND);

      /* mark the nodes which may need rebalancing */
      bool isfew;
      if (m_btree->get_root_address() == page->get_address())
        isfew = (node->get_count() <= 1);
      else
        isfew = (node->get_count() < m_btree->get_minkeys());

      if (!isfew)
        m_mergepage = 0;
      else if (!m_mergepage)
        m_mergepage = page;

      if (!node->is_leaf()) {
        st = m_btree->find_internal(page, m_key, &child, &slot);
        if (st)
          return (st);
      }
      else {
        st = m_btree->get_slot(page, m_key, &slot);
        if (st)
          return (st);
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
            st = db->get_env()->get_page_manager()->fetch_page(&tempp,
                      db, left);
            if (st)
              return (st);
            PBtreeNode *n = PBtreeNode::from_page(tempp);
            PBtreeKey *bte = n->get_key(db, n->get_count() - 1);
            next_left = bte->get_ptr();
          }
          next_lanchor = lanchor;
        }
        else {
          if (slot == 0)
            next_left = node->get_ptr_left();
          else {
            PBtreeKey *bte = node->get_key(db, slot - 1);
            next_left = bte->get_ptr();
          }
          next_lanchor = page->get_address();
        }

        if (slot == node->get_count() - 1) {
          if (!right)
            next_right = 0;
          else {
            st = db->get_env()->get_page_manager()->fetch_page(&tempp,
                      db, right);
            if (st)
              return (st);
            PBtreeNode *n = PBtreeNode::from_page(tempp);
            PBtreeKey *bte = n->get_key(db, 0);
            next_right = bte->get_ptr();
          }
          next_ranchor = ranchor;
        }
        else {
          PBtreeKey *bte = node->get_key(db, slot + 1);
          next_right = bte->get_ptr();
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
          int cmp = m_btree->compare_keys(page, m_key, slot);
          if (cmp < -1)
            return ((ham_status_t)cmp);

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
        st = remove_entry(page, slot);
        if (st)
          return (st);
      }

      /* no need to rebalance in case of an error */
      ham_assert(!st);
      return (rebalance(page_ref, page, left, right, lanchor, ranchor, parent));
    }

    /*
     * rebalance a page - either shifts elements to a sibling, or merges
     * the page with a sibling
     */
    ham_status_t rebalance(Page **newpage_ref, Page *page, ham_u64_t left,
                    ham_u64_t right, ham_u64_t lanchor,
                    ham_u64_t ranchor, Page *parent) {
      ham_status_t st;
      PBtreeNode *node = PBtreeNode::from_page(page);
      Page *leftpage = 0;
      Page *rightpage = 0;
      PBtreeNode *leftnode = 0;
      PBtreeNode *rightnode = 0;
      LocalDatabase *db = page->get_db();
      bool fewleft = false;
      bool fewright = false;
      ham_size_t minkeys = m_btree->get_minkeys();

      ham_assert(page->get_db());

      *newpage_ref = 0;
      if (!m_mergepage)
        return (0);

      /* get the left and the right sibling of this page */
      if (left) {
        st = db->get_env()->get_page_manager()->fetch_page(&leftpage,
                      db, node->get_left());
        if (st)
          return (st);
        if (leftpage) {
          leftnode = PBtreeNode::from_page(leftpage);
          fewleft  = (leftnode->get_count() <= minkeys);
        }
      }
      if (right) {
        st = db->get_env()->get_page_manager()->fetch_page(&rightpage,
                      db, node->get_right());
        if (st)
          return (st);
        if (rightpage) {
          rightnode = PBtreeNode::from_page(rightpage);
          fewright  = (rightnode->get_count() <= minkeys);
        }
      }

      /* if we have no siblings, then we're rebalancing the root page */
      if (!leftpage && !rightpage) {
        if (node->is_leaf())
          return (0);
        else
          return (page->get_db()->get_env()->get_page_manager()->fetch_page(newpage_ref,
                      db, node->get_ptr_left()));
      }

      /*
       * if one of the siblings is missing, or both of them are
       * too empty, we have to merge them
       */
      if ((!leftpage || fewleft) && (!rightpage || fewright)) {
        if (parent && lanchor != parent->get_address()) {
          return (merge_pages(newpage_ref, page, rightpage, ranchor));
        }
        else {
          return (merge_pages(newpage_ref, leftpage, page, lanchor));
        }
      }

      /* otherwise choose the better of a merge or a shift */
      if (leftpage && fewleft && rightpage && !fewright) {
        if (parent && (!(ranchor == parent->get_address()) &&
            (page->get_address() == m_mergepage->get_address()))) {
          return (merge_pages(newpage_ref, leftpage, page, lanchor));
        }
        else {
          return (shift_pages(page, rightpage, ranchor));
        }
      }

      /* ... still choose the better of a merge or a shift... */
      if (leftpage && !fewleft && rightpage && fewright) {
        if (parent && (!(lanchor == parent->get_address()) &&
                (page->get_address() == m_mergepage->get_address())))
          return (merge_pages(newpage_ref, page, rightpage, ranchor));
        else
          return (shift_pages(leftpage, page, lanchor));
      }

      /* choose the more effective of two shifts */
      if (lanchor == ranchor) {
        if (leftnode != 0 && rightnode != 0
                && leftnode->get_count() <= rightnode->get_count())
          return (shift_pages(page, rightpage, ranchor));
        else
          return (shift_pages(leftpage, page, lanchor));
      }

      /* choose the shift with more local effect */
      if (parent && lanchor == parent->get_address())
        return (shift_pages(leftpage, page, lanchor));
      else
        return (shift_pages(page, rightpage, ranchor));
    }

    /*
     * shift items from a sibling to this page, till both pages have an equal
     * number of items
     */
    ham_status_t shift_pages(Page *page, Page *sibpage, ham_u64_t anchor) {
      ham_s32_t slot = 0;
      ham_size_t s;
      LocalDatabase *db = m_btree->get_db();
      Page *ancpage;
      PBtreeKey *bte_lhs, *bte_rhs;

      PBtreeNode *node    = PBtreeNode::from_page(page);
      PBtreeNode *sibnode = PBtreeNode::from_page(sibpage);
      ham_size_t keysize = m_btree->get_keysize();
      bool intern  = !node->is_leaf();
      ham_status_t st = db->get_env()->get_page_manager()->fetch_page(&ancpage,
                      db, anchor);
      if (st)
        return (st);
      PBtreeNode *ancnode = PBtreeNode::from_page(ancpage);

      ham_assert(node->get_count() != sibnode->get_count());

      /* uncouple all cursors */
      if ((st = BtreeCursor::uncouple_all_cursors(page)))
        return (st);
      if ((st = BtreeCursor::uncouple_all_cursors(sibpage)))
        return (st);
      if (ancpage)
        if ((st = BtreeCursor::uncouple_all_cursors(ancpage)))
          return (st);

      /* shift from sibling to this node */
      if (sibnode->get_count() >= node->get_count()) {
        /* internal node: insert the anchornode separator value to this node */
        if (intern) {
          PBtreeKey *bte = sibnode->get_key(db, 0);
          ham_key_t key = {0};
          key._flags = bte->get_flags();
          key.data   = bte->get_key();
          key.size   = bte->get_size();
          st = m_btree->get_slot(ancpage, &key, &slot);
          if (st)
            return (st);

          /* append the anchor node to the page */
          bte_rhs = ancnode->get_key(db, slot);
          bte_lhs = node->get_key(db, node->get_count());

          st = copy_key(db, bte_lhs, bte_rhs);
          if (st)
            return (st);

          /* the pointer of this new node is ptr_left of the sibling */
          bte_lhs->set_ptr(sibnode->get_ptr_left());

          /* new pointer left of the sibling is sibling[0].ptr */
          sibnode->set_ptr_left(bte->get_ptr());

          /* update the anchor node with sibling[0] */
          (void)replace_key(ancpage, slot, bte, kInternalKey);

          /* shift the remainder of sibling to the left */
          bte_lhs = sibnode->get_key(db, 0);
          bte_rhs = sibnode->get_key(db, 1);
          memmove(bte_lhs, bte_rhs, (PBtreeKey::kSizeofOverhead + keysize)
                    * (sibnode->get_count() - 1));

          /* adjust counters */
          node->set_count(node->get_count() + 1);
          sibnode->set_count(sibnode->get_count() - 1);
        }

        ham_size_t c = (sibnode->get_count() - node->get_count()) / 2;
        if (c == 0)
          goto cleanup;
        if (intern)
          c--;
        if (c == 0)
          goto cleanup;

        /* internal node: append the anchor key to the page */
        if (intern) {
          bte_lhs = node->get_key(db, node->get_count());
          bte_rhs = ancnode->get_key(db, slot);

          st = copy_key(db, bte_lhs, bte_rhs);
          if (st)
            return (st);

          bte_lhs->set_ptr(sibnode->get_ptr_left());
          node->set_count(node->get_count() + 1);
        }

        /*
         * shift items from the sibling to this page, then
         * delete the shifted items
         */
        bte_lhs = node->get_key(db, node->get_count());
        bte_rhs = sibnode->get_key(db, 0);

        memmove(bte_lhs, bte_rhs, (PBtreeKey::kSizeofOverhead + keysize) * c);

        bte_lhs = sibnode->get_key(db, 0);
        bte_rhs = sibnode->get_key(db, c);
        memmove(bte_lhs, bte_rhs, (PBtreeKey::kSizeofOverhead + keysize)
                * (sibnode->get_count() - c));

        /*
         * internal nodes: don't forget to set ptr_left of the sibling, and
         * replace the anchor key
         */
        if (intern) {
          PBtreeKey *bte = sibnode->get_key(db, 0);
          sibnode->set_ptr_left(bte->get_ptr());
          if (anchor) {
            ham_key_t key = {0};
            key._flags = bte->get_flags();
            key.data   = bte->get_key();
            key.size   = bte->get_size();
            st = m_btree->get_slot(ancpage, &key, &slot);
            if (st)
              return (st);
            /* replace the key */
            st = replace_key(ancpage, slot, bte, kInternalKey);
            if (st)
              return (st);
          }
          /* shift once more */
          bte_lhs = sibnode->get_key(db, 0);
          bte_rhs = sibnode->get_key(db, 1);
          memmove(bte_lhs, bte_rhs, (PBtreeKey::kSizeofOverhead + keysize)
                    * (sibnode->get_count() - 1));
        }
        else {
          /* in a leaf - update the anchor */
          ham_key_t key = {0};
          PBtreeKey *bte = sibnode->get_key(db, 0);
          key._flags = bte->get_flags();
          key.data   = bte->get_key();
          key.size   = bte->get_size();
          st = m_btree->get_slot(ancpage, &key, &slot);
          if (st)
            return (st);

          /* replace the key */
          st = replace_key(ancpage, slot, bte, kInternalKey);
          if (st)
            return (st);
        }

        /* update the page counter */
        ham_assert(node->get_count() + c <= 0xFFFF);
        ham_assert(sibnode->get_count() - c - (intern ? 1 : 0) <= 0xFFFF);
        node->set_count(node->get_count() + c);
        sibnode->set_count(sibnode->get_count() - c - (intern ? 1 : 0));
      }
      else {
        /* shift from this node to the sibling */

        /*
        * internal node: insert the anchornode separator value to
        * this node
        */
        if (intern) {
          ham_key_t key = {0};

          PBtreeKey *bte = sibnode->get_key(db, 0);
          key._flags = bte->get_flags();
          key.data   = bte->get_key();
          key.size   = bte->get_size();
          st = m_btree->get_slot(ancpage, &key, &slot);
          if (st)
            return (st);

          /* shift entire sibling by 1 to the right */
          bte_lhs = sibnode->get_key(db, 1);
          bte_rhs = sibnode->get_key(db, 0);
          memmove(bte_lhs, bte_rhs, (PBtreeKey::kSizeofOverhead + keysize)
                  * sibnode->get_count());

          /* copy the old anchor element to sibling[0] */
          bte_lhs = sibnode->get_key(db, 0);
          bte_rhs = ancnode->get_key(db, slot);

          st = copy_key(db, bte_lhs, bte_rhs);
          if (st)
            return (st);

          /* sibling[0].ptr = sibling.ptr_left */
          bte_lhs->set_ptr(sibnode->get_ptr_left());

          /* sibling.ptr_left = node[node.count-1].ptr */
          bte_lhs = node->get_key(db, node->get_count() - 1);
          sibnode->set_ptr_left(bte_lhs->get_ptr());

          /* new anchor element is node[node.count-1].key */
          st = replace_key(ancpage, slot, bte_lhs, kInternalKey);
          if (st)
            return (st);

          /* page: one item less; sibling: one item more */
          node->set_count(node->get_count() - 1);
          sibnode->set_count(sibnode->get_count() + 1);
        }

        ham_size_t c = (node->get_count() - sibnode->get_count()) / 2;
        if (c == 0)
          goto cleanup;
        if (intern)
          c--;
        if (c == 0)
          goto cleanup;

        /* internal pages: insert the anchor element */
        if (intern) {
          /* shift entire sibling by 1 to the right */
          bte_lhs = sibnode->get_key(db, 1);
          bte_rhs = sibnode->get_key(db, 0);
          memmove(bte_lhs, bte_rhs, (PBtreeKey::kSizeofOverhead + keysize)
                    * (sibnode->get_count()));

          bte_lhs = sibnode->get_key(db, 0);
          bte_rhs = ancnode->get_key(db, slot);

          /* clear the key - we don't want replace_key to free
           * an extended block which is still used by sibnode[1] */
          memset(bte_lhs, 0, sizeof(*bte_lhs));

          st = replace_key(sibpage, 0, bte_rhs,
                    (node->is_leaf() ? 0 : kInternalKey));
          if (st)
            return (st);

          bte_lhs->set_ptr(sibnode->get_ptr_left());
          sibnode->set_count(sibnode->get_count() + 1);
        }

        s = node->get_count() - c - 1;

        /*
         * shift items from this page to the sibling, then delete the
         * items from this page
         */
        bte_lhs = sibnode->get_key(db, c);
        bte_rhs = sibnode->get_key(db, 0);
        memmove(bte_lhs, bte_rhs, (PBtreeKey::kSizeofOverhead+keysize)
                * sibnode->get_count());

        bte_lhs = sibnode->get_key(db, 0);
        bte_rhs = node->get_key(db, s + 1);
        memmove(bte_lhs, bte_rhs, (PBtreeKey::kSizeofOverhead + keysize) * c);

        ham_assert(node->get_count() - c <= 0xFFFF);
        ham_assert(sibnode->get_count() + c <= 0xFFFF);
        node->set_count(node->get_count() - c);
        sibnode->set_count(sibnode->get_count() + c);

        /*
         * internal nodes: the pointer of the highest item
         * in the node will become the ptr_left of the sibling
         */
        if (intern) {
          bte_lhs = node->get_key(db, node->get_count() - 1);
          sibnode->set_ptr_left(bte_lhs->get_ptr());

          /* free the extended blob of this key */
          if (bte_lhs->get_flags() & PBtreeKey::kExtended) {
            ham_u64_t blobid = bte_lhs->get_extended_rid(db);
            ham_assert(blobid);

            st = db->remove_extkey(blobid);
            if (st)
              return (st);
          }
          node->set_count(node->get_count() - 1);
        }

        /* replace the old anchor key with the new anchor key */
        if (anchor) {
          PBtreeKey *bte;
          ham_key_t key = {0};

          if (intern)
            bte = node->get_key(db, s);
          else
            bte = sibnode->get_key(db, 0);

          key._flags = bte->get_flags();
          key.data   = bte->get_key();
          key.size   = bte->get_size();

          st = m_btree->get_slot(ancpage, &key, &slot);
          if (st)
            return (st);

          st = replace_key(ancpage, slot + 1, bte, kInternalKey);
          if (st)
            return (st);
        }
      }

cleanup:
      /* mark pages as dirty */
      page->set_dirty(true);
      ancpage->set_dirty(true);
      sibpage->set_dirty(true);

      m_mergepage = 0;

      return (st);
    }

    /* merge two pages */
    ham_status_t merge_pages(Page **newpage_ref, Page *page, Page *sibpage,
                        ham_u64_t anchor) {
      ham_status_t st;
      LocalDatabase *db = m_btree->get_db();
      Page *ancpage = 0;
      PBtreeNode *ancnode = 0;
      PBtreeKey *bte_lhs, *bte_rhs;
      ham_size_t keysize = m_btree->get_keysize();
      PBtreeNode *node    = PBtreeNode::from_page(page);
      PBtreeNode *sibnode = PBtreeNode::from_page(sibpage);

      *newpage_ref = 0;

      if (anchor) {
        st = page->get_db()->get_env()->get_page_manager()->fetch_page(&ancpage,
                      page->get_db(), anchor);
        if (st)
          return (st);
        ancnode = PBtreeNode::from_page(ancpage);
      }

      /* uncouple all cursors */
      if ((st = BtreeCursor::uncouple_all_cursors(page)))
        return (st);
      if ((st = BtreeCursor::uncouple_all_cursors(sibpage)))
        return (st);
      if (ancpage)
        if ((st = BtreeCursor::uncouple_all_cursors(ancpage)))
          return (st);

      /*
       * internal node: append the anchornode separator value to
       * this node
       */
      if (!node->is_leaf()) {
        PBtreeKey *bte = sibnode->get_key(db, 0);
        ham_key_t key = {0};
        key._flags = bte->get_flags();
        key.data   = bte->get_key();
        key.size   = bte->get_size();

        ham_s32_t slot;
        st = m_btree->get_slot(ancpage, &key, &slot);
        if (st)
          return (st);

        bte_lhs = node->get_key(db, node->get_count());
        bte_rhs = ancnode->get_key(db, slot);

        st = copy_key(db, bte_lhs, bte_rhs);
        if (st)
          return (st);
        bte_lhs->set_ptr(sibnode->get_ptr_left());
        node->set_count(node->get_count() + 1);
      }

      ham_size_t c = sibnode->get_count();
      bte_lhs = node->get_key(db, node->get_count());
      bte_rhs = sibnode->get_key(db, 0);

      /* shift items from the sibling to this page */
      memcpy(bte_lhs, bte_rhs, (PBtreeKey::kSizeofOverhead+keysize) * c);

      page->set_dirty(true);
      sibpage->set_dirty(true);
      ham_assert(node->get_count() + c <= 0xFFFF);
      node->set_count(node->get_count() + c);
      sibnode->set_count(0);

      /* update the linked list of pages */
      if (node->get_left() == sibpage->get_address()) {
        if (sibnode->get_left()) {
          Page *p;
          st = page->get_db()->get_env()->get_page_manager()->fetch_page(&p,
                      page->get_db(), sibnode->get_left());
          if (st)
            return (st);
          PBtreeNode *n = PBtreeNode::from_page(p);
          n->set_right(sibnode->get_right());
          node->set_left(sibnode->get_left());
          p->set_dirty(true);
        }
        else
          node->set_left(0);
      }
      else if (node->get_right() == sibpage->get_address()) {
        if (sibnode->get_right()) {
          Page *p;
          st = page->get_db()->get_env()->get_page_manager()->fetch_page(&p,
                      page->get_db(), sibnode->get_right());
          if (st)
            return (st);
          PBtreeNode *n = PBtreeNode::from_page(p);
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

      /* delete the page TODO */

      *newpage_ref = sibpage;
      return (HAM_SUCCESS);
    }

    /* collapse the root node */
    ham_status_t collapse_root(Page *oldroot, Page *newroot) {
      Environment *env = newroot->get_db()->get_env();
      env->get_page_manager()->add_to_freelist(oldroot);

      m_btree->set_root_address(newroot->get_address());
      ham_assert(newroot->get_db());

      newroot->set_type(Page::kTypeBroot);
      return (0);
    }

    /*
     * copy a key; extended keys will be cloned, otherwise two keys would
     * have the same blob id
     */
    ham_status_t copy_key(LocalDatabase *db, PBtreeKey *lhs, PBtreeKey *rhs) {
      memcpy(lhs, rhs, PBtreeKey::kSizeofOverhead + m_btree->get_keysize());

      if (rhs->get_flags() & PBtreeKey::kExtended) {
        ham_record_t record = {0};
        ByteArray *arena = (m_txn == 0
                                || (m_txn->get_flags() & HAM_TXN_TEMPORARY))
                            ? &db->get_record_arena()
                            : &m_txn->get_record_arena();

        ham_u64_t rhsblobid = rhs->get_extended_rid(db);
        ham_status_t st = db->get_env()->get_blob_manager()->read(db,
                                rhsblobid, &record, 0, arena);
        if (st)
          return (st);

        ham_u64_t lhsblobid;
        st = db->get_env()->get_blob_manager()->allocate(db, &record,
                                0, &lhsblobid);
        if (st)
          return (st);
        lhs->set_extended_rid(db, lhsblobid);
      }

      return (0);
    }

    /*
     * replace a key in a page
     */
    ham_status_t replace_key(Page *page, ham_s32_t slot, PBtreeKey *rhs,
                    ham_u32_t flags) {
      ham_status_t st;
      LocalDatabase *db = m_btree->get_db();
      PBtreeNode *node = PBtreeNode::from_page(page);

      /* uncouple all cursors */
      if ((st = BtreeCursor::uncouple_all_cursors(page)))
        return (st);

      PBtreeKey *lhs = node->get_key(db, slot);

      /* if we overwrite an extended key: delete the existing extended blob */
      if (lhs->get_flags() & PBtreeKey::kExtended) {
        ham_u64_t blobid = lhs->get_extended_rid(db);
        ham_assert(blobid);

        st = db->remove_extkey(blobid);
        if (st)
          return (st);
      }

      lhs->set_flags(rhs->get_flags());
      memcpy(lhs->get_key(), rhs->get_key(), m_btree->get_keysize());

      /*
       * internal keys are not allowed to have blob-flags, because only the
       * leaf-node can manage the blob. Therefore we have to disable those
       * flags if we modify an internal key.
       */
      if (flags & kInternalKey)
        lhs->set_flags(lhs->get_flags() &
                ~(PBtreeKey::kBlobSizeTiny
                    | PBtreeKey::kBlobSizeSmall
                    | PBtreeKey::kBlobSizeEmpty
                    | PBtreeKey::kDuplicates));

      /*
       * if this key is extended, we copy the extended blob; otherwise, we'd
       * have to add reference counting to the blob, because two keys are now
       * using the same blobid. this would be too complicated.
       */
      if (rhs->get_flags() & PBtreeKey::kExtended) {
        ham_record_t record = {0};
        ByteArray *arena = (m_txn == 0
                                || (m_txn->get_flags() & HAM_TXN_TEMPORARY))
                            ? &db->get_record_arena()
                            : &m_txn->get_record_arena();

        ham_u64_t rhsblobid = rhs->get_extended_rid(db);
        ham_status_t st = db->get_env()->get_blob_manager()->read(db,
                                rhsblobid, &record, 0, arena);
        if (st)
          return (st);

        ham_u64_t lhsblobid;
        st = db->get_env()->get_blob_manager()->allocate(db, &record, 0,
                                &lhsblobid);
        if (st)
          return (st);
        lhs->set_extended_rid(db, lhsblobid);
      }

      lhs->set_size(rhs->get_size());

      page->set_dirty(true);

      return (HAM_SUCCESS);
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
