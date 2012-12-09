/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief btree erasing
 *
 */

#include "config.h"

#include <string.h>

#include "blob.h"
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

namespace ham {

class BtreeEraseAction
{
  enum {
    /* flags for replace_key */
    INTERNAL_KEY = 2
  };

  public:
    BtreeEraseAction(BtreeBackend *backend, Transaction *txn, Cursor *cursor,
        ham_key_t *key, ham_u32_t dupe_id = 0, ham_u32_t flags = 0)
      : m_backend(backend), m_txn(txn), m_cursor(0), m_key(key),
        m_dupe_id(dupe_id), m_flags(flags), m_mergepage(0) {
      if (cursor && cursor->get_btree_cursor()->get_parent())
        m_cursor = cursor->get_btree_cursor();
      if (m_cursor && !key && m_cursor->is_uncoupled())
        m_key = m_cursor->get_uncoupled_key();
    }

    ham_status_t run() {
      Page *root, *p;
      Database *db = m_backend->get_db();

      /* coupled cursor: try to remove the key directly from the page.
       * if that's not possible (i.e. because of underflow): uncouple
       * the cursor and process the normal erase algorithm */
      if (m_cursor && m_cursor->is_coupled()) {
        Page *page = m_cursor->get_coupled_page();
        BtreeNode *node = BtreeNode::from_page(page);
        ham_assert(node->is_leaf());
        if (m_cursor->get_coupled_index() > 0
            && node->get_count() > m_backend->get_minkeys()) {
          /* yes, we can remove the key */
          return (remove_entry(m_cursor->get_coupled_page(),
                m_cursor->get_coupled_index()));
        }
        else {
          /* otherwise uncouple and call erase recursively */
          ham_status_t st = m_cursor->uncouple();
          if (st)
            return (st);
          BtreeEraseAction bea(m_backend, m_txn, m_cursor->get_parent(),
                    m_cursor->get_uncoupled_key(), m_flags);
          return (bea.run());
        }
      }

      /* get the root-page...  */
      ham_offset_t rootaddr = m_backend->get_rootpage();
      if (!rootaddr) {
        m_backend->get_statistics()->erase_failed();
        return (HAM_KEY_NOT_FOUND);
      }
      ham_status_t st = db->fetch_page(&root, rootaddr);
      if (st)
        return (st);

      /* ... and start the recursion */
      st = erase_recursive(&p, root, 0, 0, 0, 0, 0);
      if (st) {
        m_backend->get_statistics()->erase_failed();
        return (st);
      }

      if (p) {
        /* delete the old root page */
        st = btree_uncouple_all_cursors(root, 0);
        if (st) {
          m_backend->get_statistics()->erase_failed();
          return (st);
        }

        st = collapse_root(p);
        if (st) {
          m_backend->get_statistics()->erase_failed();
          return (st);
        }

        m_backend->get_statistics()->reset_page(root);
      }

      return (0);
    }

  private:
    /* remove an item from a page */
    ham_status_t remove_entry(Page *page, ham_s32_t slot) {
      ham_status_t st;
      BtreeCursor *btc = 0;

      Database *db = page->get_db();
      BtreeNode *node = BtreeNode::from_page(page);
      ham_size_t keysize = m_backend->get_keysize();
      BtreeKey *bte = node->get_key(db, slot);

      /* uncouple all cursors */
      if ((st = btree_uncouple_all_cursors(page, 0)))
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
        Cursor *cursors = db->get_cursors();
        ham_u32_t dupe_id = 0;

        if (cursors)
          btc = cursors->get_btree_cursor();

        if (m_cursor)
          dupe_id = m_cursor->get_dupe_id() + 1;
        else if (m_dupe_id) /* +1-based index */
          dupe_id = m_dupe_id;

        if (bte->get_flags() & BtreeKey::KEY_HAS_DUPLICATES && dupe_id) {
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
           * all other cursors
           */
          while (btc && m_cursor) {
            BtreeCursor *next = 0;
            if (cursors->get_next()) {
              cursors = cursors->get_next();
              next = cursors->get_btree_cursor();
            }
            if (btc != m_cursor) {
              if (btc->get_dupe_id() == m_cursor->get_dupe_id()) {
                if (btc->points_to(bte))
                  btc->set_to_nil();
              }
              else if (btc->get_dupe_id() > m_cursor->get_dupe_id()) {
                btc->set_dupe_id(btc->get_dupe_id()-1);
                memset(btc->get_dupe_cache(), 0, sizeof(dupe_entry_t));
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
      if (bte->get_flags() & BtreeKey::KEY_IS_EXTENDED) {
        ham_offset_t blobid = bte->get_extended_rid(db);
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
        BtreeKey *lhs = node->get_key(db, slot);
        BtreeKey *rhs = node->get_key(db, slot + 1);
        memmove(lhs, rhs, ((BtreeKey::ms_sizeof_overhead + keysize))
                * (node->get_count() - slot - 1));
      }

      node->set_count(node->get_count() - 1);
      page->set_dirty(true);
      return (0);
    }

    /**
     * recursively descend down the tree, delete the item and re-balance
     * the tree on the way back up
     *
     * returns the page which is deleted, if available
     */
    ham_status_t erase_recursive(Page **page_ref, Page *page,
                    ham_offset_t left, ham_offset_t right,
                    ham_offset_t lanchor, ham_offset_t ranchor, Page *parent) {
      ham_s32_t slot;
      ham_status_t st;
      Page *newme;
      Page *child;
      Page *tempp = 0;
      Database *db = page->get_db();
      BtreeNode *node = BtreeNode::from_page(page);

      *page_ref = 0;

      /* empty node? then most likely we're in the empty root page. */
      if (node->get_count() == 0)
        return (HAM_KEY_NOT_FOUND);

      /* mark the nodes which may need rebalancing */
      bool isfew;
      if (m_backend->get_rootpage() == page->get_self())
        isfew = (node->get_count() <= 1);
      else
        isfew = (node->get_count() < m_backend->get_minkeys());

      if (!isfew)
        m_mergepage = 0;
      else if (!m_mergepage)
        m_mergepage = page;

      if (!node->is_leaf()) {
        st = m_backend->find_internal(page, m_key, &child, &slot);
        if (st)
          return (st);
      }
      else {
        st = m_backend->get_slot(page, m_key, &slot);
        if (st)
          return (st);
        child = 0;
      }

      /* if this page is not a leaf: recursively descend down the tree */
      if (!node->is_leaf()) {
        ham_offset_t next_lanchor;
        ham_offset_t next_ranchor;
        ham_offset_t next_left;
        ham_offset_t next_right;

        /* calculate neighbor and anchor nodes */
        if (slot == -1) {
          if (!left)
            next_left = 0;
          else {
            st = db->fetch_page(&tempp, left);
            if (st)
              return (st);
            BtreeNode *n = BtreeNode::from_page(tempp);
            BtreeKey *bte = n->get_key(db, n->get_count() - 1);
            next_left = bte->get_ptr();
          }
          next_lanchor = lanchor;
        }
        else {
          if (slot == 0)
            next_left = node->get_ptr_left();
          else {
            BtreeKey *bte = node->get_key(db, slot - 1);
            next_left = bte->get_ptr();
          }
          next_lanchor = page->get_self();
        }

        if (slot == node->get_count() - 1) {
          if (!right)
            next_right = 0;
          else {
            st = db->fetch_page(&tempp, right);
            if (st)
              return (st);
            BtreeNode *n = BtreeNode::from_page(tempp);
            BtreeKey *bte = n->get_key(db, 0);
            next_right = bte->get_ptr();
          }
          next_ranchor = ranchor;
        }
        else {
          BtreeKey *bte = node->get_key(db, slot + 1);
          next_right = bte->get_ptr();
          next_ranchor = page->get_self();
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
          int cmp = m_backend->compare_keys(page, m_key, slot);
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

    /**
     * rebalance a page - either shifts elements to a sibling, or merges
     * the page with a sibling
     */
    ham_status_t rebalance(Page **newpage_ref, Page *page, ham_offset_t left,
                    ham_offset_t right, ham_offset_t lanchor,
                    ham_offset_t ranchor, Page *parent) {
      ham_status_t st;
      BtreeNode *node = BtreeNode::from_page(page);
      Page *leftpage = 0;
      Page *rightpage = 0;
      BtreeNode *leftnode = 0;
      BtreeNode *rightnode = 0;
      bool fewleft = false;
      bool fewright = false;
      ham_size_t minkeys = m_backend->get_minkeys();

      ham_assert(page->get_db());

      *newpage_ref = 0;
      if (!m_mergepage)
        return (0);

      /* get the left and the right sibling of this page */
      if (left) {
        st = page->get_db()->fetch_page(&leftpage, node->get_left());
        if (st)
          return (st);
        if (leftpage) {
          leftnode = BtreeNode::from_page(leftpage);
          fewleft  = (leftnode->get_count() <= minkeys);
        }
      }
      if (right) {
        st = page->get_db()->fetch_page(&rightpage, node->get_right());
        if (st)
          return (st);
        if (rightpage) {
          rightnode = BtreeNode::from_page(rightpage);
          fewright  = (rightnode->get_count() <= minkeys);
        }
      }

      /* if we have no siblings, then we're rebalancing the root page */
      if (!leftpage && !rightpage) {
        if (node->is_leaf())
          return (0);
        else
          return (page->get_db()->fetch_page(newpage_ref, node->get_ptr_left()));
      }

      /*
       * if one of the siblings is missing, or both of them are
       * too empty, we have to merge them
       */
      if ((!leftpage || fewleft) && (!rightpage || fewright)) {
        if (parent && lanchor != parent->get_self()) {
          return (merge_pages(newpage_ref, page, rightpage, ranchor));
        }
        else {
          return (merge_pages(newpage_ref, leftpage, page, lanchor));
        }
      }

      /* otherwise choose the better of a merge or a shift */
      if (leftpage && fewleft && rightpage && !fewright) {
        if (parent && (!(ranchor == parent->get_self()) &&
            (page->get_self() == m_mergepage->get_self()))) {
          return (merge_pages(newpage_ref, leftpage, page, lanchor));
        }
        else {
          return (shift_pages(page, rightpage, ranchor));
        }
      }

      /* ... still choose the better of a merge or a shift... */
      if (leftpage && !fewleft && rightpage && fewright) {
        if (parent && (!(lanchor == parent->get_self()) &&
                (page->get_self() == m_mergepage->get_self())))
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
      if (parent && lanchor == parent->get_self())
        return (shift_pages(leftpage, page, lanchor));
      else
        return (shift_pages(page, rightpage, ranchor));
    }

    /*
     * shift items from a sibling to this page, till both pages have an equal
     * number of items
     */
    ham_status_t shift_pages(Page *page, Page *sibpage, ham_offset_t anchor) {
      ham_s32_t slot = 0;
      ham_size_t s;
      Database *db = page->get_db();
      Page *ancpage;
      BtreeKey *bte_lhs, *bte_rhs;

      BtreeNode *node    = BtreeNode::from_page(page);
      BtreeNode *sibnode = BtreeNode::from_page(sibpage);
      ham_size_t keysize = m_backend->get_keysize();
      bool intern  = !node->is_leaf();
      ham_status_t st = db->fetch_page(&ancpage, anchor);
      if (st)
        return (st);
      BtreeNode *ancnode = BtreeNode::from_page(ancpage);

      ham_assert(node->get_count() != sibnode->get_count());

      /* uncouple all cursors */
      if ((st = btree_uncouple_all_cursors(page, 0)))
        return (st);
      if ((st = btree_uncouple_all_cursors(sibpage, 0)))
        return (st);
      if (ancpage)
        if ((st = btree_uncouple_all_cursors(ancpage, 0)))
          return (st);

      /* shift from sibling to this node */
      if (sibnode->get_count() >= node->get_count()) {
        /* internal node: insert the anchornode separator value to this node */
        if (intern) {
          BtreeKey *bte = sibnode->get_key(db, 0);
          ham_key_t key = {0};
          key._flags = bte->get_flags();
          key.data   = bte->get_key();
          key.size   = bte->get_size();
          st = m_backend->get_slot(ancpage, &key, &slot);
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
          (void)replace_key(ancpage, slot, bte, INTERNAL_KEY);

          /* shift the remainder of sibling to the left */
          bte_lhs = sibnode->get_key(db, 0);
          bte_rhs = sibnode->get_key(db, 1);
          memmove(bte_lhs, bte_rhs, (BtreeKey::ms_sizeof_overhead + keysize)
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

        memmove(bte_lhs, bte_rhs, (BtreeKey::ms_sizeof_overhead + keysize) * c);

        bte_lhs = sibnode->get_key(db, 0);
        bte_rhs = sibnode->get_key(db, c);
        memmove(bte_lhs, bte_rhs, (BtreeKey::ms_sizeof_overhead + keysize)
                * (sibnode->get_count() - c));

        /*
         * internal nodes: don't forget to set ptr_left of the sibling, and
         * replace the anchor key
         */
        if (intern) {
          BtreeKey *bte = sibnode->get_key(db, 0);
          sibnode->set_ptr_left(bte->get_ptr());
          if (anchor) {
            ham_key_t key = {0};
            key._flags = bte->get_flags();
            key.data   = bte->get_key();
            key.size   = bte->get_size();
            st = m_backend->get_slot(ancpage, &key, &slot);
            if (st)
              return (st);
            /* replace the key */
            st = replace_key(ancpage, slot, bte, INTERNAL_KEY);
            if (st)
              return (st);
          }
          /* shift once more */
          bte_lhs = sibnode->get_key(db, 0);
          bte_rhs = sibnode->get_key(db, 1);
          memmove(bte_lhs, bte_rhs, (BtreeKey::ms_sizeof_overhead + keysize)
                    * (sibnode->get_count() - 1));
        }
        else {
          /* in a leaf - update the anchor */
          ham_key_t key = {0};
          BtreeKey *bte = sibnode->get_key(db, 0);
          key._flags = bte->get_flags();
          key.data   = bte->get_key();
          key.size   = bte->get_size();
          st = m_backend->get_slot(ancpage, &key, &slot);
          if (st)
            return (st);

          /* replace the key */
          st = replace_key(ancpage, slot, bte, INTERNAL_KEY);
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

          BtreeKey *bte = sibnode->get_key(db, 0);
          key._flags = bte->get_flags();
          key.data   = bte->get_key();
          key.size   = bte->get_size();
          st = m_backend->get_slot(ancpage, &key, &slot);
          if (st)
            return (st);

          /* shift entire sibling by 1 to the right */
          bte_lhs = sibnode->get_key(db, 1);
          bte_rhs = sibnode->get_key(db, 0);
          memmove(bte_lhs, bte_rhs, (BtreeKey::ms_sizeof_overhead + keysize)
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
          st = replace_key(ancpage, slot, bte_lhs, INTERNAL_KEY);
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
          memmove(bte_lhs, bte_rhs, (BtreeKey::ms_sizeof_overhead + keysize)
                    * (sibnode->get_count()));

          bte_lhs = sibnode->get_key(db, 0);
          bte_rhs = ancnode->get_key(db, slot);

          /* clear the key - we don't want replace_key to free
           * an extended block which is still used by sibnode[1] */
          memset(bte_lhs, 0, sizeof(*bte_lhs));

          st = replace_key(sibpage, 0, bte_rhs,
                    (node->is_leaf() ? 0 : INTERNAL_KEY));
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
        memmove(bte_lhs, bte_rhs, (BtreeKey::ms_sizeof_overhead+keysize)
                * sibnode->get_count());

        bte_lhs = sibnode->get_key(db, 0);
        bte_rhs = node->get_key(db, s + 1);
        memmove(bte_lhs, bte_rhs, (BtreeKey::ms_sizeof_overhead + keysize) * c);

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
          if (bte_lhs->get_flags() & BtreeKey::KEY_IS_EXTENDED) {
            ham_offset_t blobid = bte_lhs->get_extended_rid(db);
            ham_assert(blobid);

            st = db->remove_extkey(blobid);
            if (st)
              return (st);
          }
          node->set_count(node->get_count() - 1);
        }

        /* replace the old anchor key with the new anchor key */
        if (anchor) {
          BtreeKey *bte;
          ham_key_t key = {0};

          if (intern)
            bte = node->get_key(db, s);
          else
            bte = sibnode->get_key(db, 0);

          key._flags = bte->get_flags();
          key.data   = bte->get_key();
          key.size   = bte->get_size();

          st = m_backend->get_slot(ancpage, &key, &slot);
          if (st)
            return (st);

          st = replace_key(ancpage, slot + 1, bte, INTERNAL_KEY);
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
                        ham_offset_t anchor) {
      ham_status_t st;
      Database *db = page->get_db();
      Page *ancpage = 0;
      BtreeNode *ancnode = 0;
      BtreeKey *bte_lhs, *bte_rhs;
      ham_size_t keysize = m_backend->get_keysize();
      BtreeNode *node    = BtreeNode::from_page(page);
      BtreeNode *sibnode = BtreeNode::from_page(sibpage);

      *newpage_ref = 0;

      if (anchor) {
        st = page->get_db()->fetch_page(&ancpage, anchor);
        if (st)
          return (st);
        ancnode = BtreeNode::from_page(ancpage);
      }

      /* uncouple all cursors */
      if ((st = btree_uncouple_all_cursors(page, 0)))
        return (st);
      if ((st = btree_uncouple_all_cursors(sibpage, 0)))
        return (st);
      if (ancpage)
        if ((st = btree_uncouple_all_cursors(ancpage, 0)))
          return (st);

      /*
       * internal node: append the anchornode separator value to
       * this node
       */
      if (!node->is_leaf()) {
        BtreeKey *bte = sibnode->get_key(db, 0);
        ham_key_t key = {0};
        key._flags = bte->get_flags();
        key.data   = bte->get_key();
        key.size   = bte->get_size();

        ham_s32_t slot;
        st = m_backend->get_slot(ancpage, &key, &slot);
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
      memcpy(bte_lhs, bte_rhs, (BtreeKey::ms_sizeof_overhead+keysize) * c);

      page->set_dirty(true);
      sibpage->set_dirty(true);
      ham_assert(node->get_count() + c <= 0xFFFF);
      node->set_count(node->get_count() + c);
      sibnode->set_count(0);

      /* update the linked list of pages */
      if (node->get_left() == sibpage->get_self()) {
        if (sibnode->get_left()) {
          Page *p;

          st = page->get_db()->fetch_page(&p, sibnode->get_left());
          if (st)
            return (st);
          BtreeNode *n = BtreeNode::from_page(p);
          n->set_right(sibnode->get_right());
          node->set_left(sibnode->get_left());
          p->set_dirty(true);
        }
        else
          node->set_left(0);
      }
      else if (node->get_right() == sibpage->get_self()) {
        if (sibnode->get_right()) {
          Page *p;

          st = page->get_db()->fetch_page(&p, sibnode->get_right());
          if (st)
            return (st);
          BtreeNode *n = BtreeNode::from_page(p);
          node->set_right(sibnode->get_right());
          n->set_left(sibnode->get_left());
          p->set_dirty(true);
        }
        else
          node->set_right(0);
      }

      /* return this page for deletion */
      if (m_mergepage &&
          (m_mergepage->get_self() == page->get_self()
            || m_mergepage->get_self() == sibpage->get_self()))
        m_mergepage = 0;

      m_backend->get_statistics()->reset_page(sibpage);

      /* delete the page TODO */

      *newpage_ref = sibpage;
      return (HAM_SUCCESS);
    }

    /* collapse the root node */
    ham_status_t collapse_root(Page *newroot) {
      m_backend->set_rootpage(newroot->get_self());
      m_backend->do_flush_indexdata();
      ham_assert(newroot->get_db());

      Environment *env = newroot->get_db()->get_env();
      env->set_dirty(true);

      /* add the page to the changeset to make sure that the changes are
       * logged */
      if (env->get_flags() & HAM_ENABLE_RECOVERY)
        env->get_changeset().add_page(env->get_header_page());

      newroot->set_type(Page::TYPE_B_ROOT);
      return (0);
    }

    /*
     * copy a key; extended keys will be cloned, otherwise two keys would
     * have the same blob id
     */
    ham_status_t copy_key(Database *db, BtreeKey *lhs, BtreeKey *rhs) {
      memcpy(lhs, rhs, BtreeKey::ms_sizeof_overhead + m_backend->get_keysize());

      if (rhs->get_flags() & BtreeKey::KEY_IS_EXTENDED) {
        ham_record_t record = {0};

        ham_offset_t rhsblobid = rhs->get_extended_rid(db);
        ham_status_t st = db->get_env()->get_blob_manager()->read(db, m_txn,
                                rhsblobid, &record, 0);
        if (st)
          return (st);

        ham_offset_t lhsblobid;
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
    ham_status_t replace_key(Page *page, ham_s32_t slot, BtreeKey *rhs,
                    ham_u32_t flags) {
      ham_status_t st;
      Database *db = page->get_db();
      BtreeNode *node = BtreeNode::from_page(page);

      /* uncouple all cursors */
      if ((st = btree_uncouple_all_cursors(page, 0)))
        return (st);

      BtreeKey *lhs = node->get_key(db, slot);

      /* if we overwrite an extended key: delete the existing extended blob */
      if (lhs->get_flags() & BtreeKey::KEY_IS_EXTENDED) {
        ham_offset_t blobid = lhs->get_extended_rid(db);
        ham_assert(blobid);

        st = db->remove_extkey(blobid);
        if (st)
          return (st);
      }

      lhs->set_flags(rhs->get_flags());
      memcpy(lhs->get_key(), rhs->get_key(), m_backend->get_keysize());

      /*
       * internal keys are not allowed to have blob-flags, because only the
       * leaf-node can manage the blob. Therefore we have to disable those
       * flags if we modify an internal key.
       */
      if (flags & INTERNAL_KEY)
        lhs->set_flags(lhs->get_flags() &
                ~(BtreeKey::KEY_BLOB_SIZE_TINY
                    | BtreeKey::KEY_BLOB_SIZE_SMALL
                    | BtreeKey::KEY_BLOB_SIZE_EMPTY
                    | BtreeKey::KEY_HAS_DUPLICATES));

      /*
       * if this key is extended, we copy the extended blob; otherwise, we'd
       * have to add reference counting to the blob, because two keys are now
       * using the same blobid. this would be too complicated.
       */
      if (rhs->get_flags() & BtreeKey::KEY_IS_EXTENDED) {
        ham_record_t record = {0};

        ham_offset_t rhsblobid = rhs->get_extended_rid(db);
        ham_status_t st = db->get_env()->get_blob_manager()->read(db,
                                m_txn, rhsblobid, &record, 0);
        if (st)
          return (st);

        ham_offset_t lhsblobid;
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

    /** the current backend */
    BtreeBackend *m_backend;

    /** the current transaction */
    Transaction *m_txn;

    /** the current cursor */
    BtreeCursor *m_cursor;

    /** the key that is retrieved */
    ham_key_t *m_key;

    /* id of the duplicate to erase */
    ham_u32_t m_dupe_id;

    /* flags of ham_db_find() */
    ham_u32_t m_flags;

    /* a page which needs rebalancing */
    Page *m_mergepage;
};

ham_status_t
BtreeBackend::do_erase(Transaction *txn, ham_key_t *key, ham_u32_t flags)
{
  BtreeEraseAction bea(this, txn, 0, key, 0, flags);
  return (bea.run());
}

ham_status_t
BtreeBackend::erase_duplicate(Transaction *txn, ham_key_t *key,
        ham_u32_t dupe_id, ham_u32_t flags)
{
  BtreeEraseAction bea(this, txn, 0, key, dupe_id, flags);
  return (bea.run());
}

ham_status_t
BtreeBackend::do_erase_cursor(Transaction *txn, ham_key_t *key,
        Cursor *cursor, ham_u32_t flags)
{
  BtreeEraseAction bea(this, txn, cursor, key, 0, flags);
  return (bea.run());
}

} // namespace ham
