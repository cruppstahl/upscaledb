/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

#include "config.h"

#include <string.h>
#include <algorithm>

#include "blob_manager.h"
#include "btree_index.h"
#include "page_manager.h"
#include "btree_cursor.h"
#include "extkeys.h"
#include "cursor.h"
#include "cache.h"
#include "db.h"
#include "env.h"
#include "error.h"
#include "mem.h"
#include "util.h"
#include "page.h"
#include "btree_key.h"
#include "btree_stats.h"
#include "btree_node_factory.h"

using namespace std;

namespace hamsterdb {

/* a unittest hook triggered when a page is split */
void (*g_BTREE_INSERT_SPLIT_HOOK)(void);

/*
 * btree inserting
 */
class BtreeInsertAction
{
  enum {
    // page split required
    kSplitRequired = 1
  };

  public:
    BtreeInsertAction(BtreeIndex *btree, Transaction *txn, Cursor *cursor,
        ham_key_t *key, ham_record_t *record, ham_u32_t flags)
      : m_btree(btree), m_txn(txn), m_cursor(0), m_key(key),
        m_record(record), m_split_rid(0), m_flags(flags) {
      if (cursor) {
        m_cursor = cursor->get_btree_cursor();
        ham_assert(m_btree->get_db() == m_cursor->get_parent()->get_db());
      }
    }

    ham_status_t run() {
      ham_status_t st;
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
       * flag and recursively call do_insert_cursor()
       */
      if (m_hints.leaf_page_addr
          && (m_hints.flags & HAM_HINT_APPEND
              || m_hints.flags & HAM_HINT_PREPEND))
        st = append_or_prepend_key();
      else
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
      ham_status_t st = 0;
      Page *page;
      LocalDatabase *db = m_btree->get_db();
      LocalEnvironment *env = db->get_local_env();
      bool force_append = false;
      bool force_prepend = false;

      /*
       * see if we get this btree leaf; if not, revert to regular scan
       *
       * As this is a speed-improvement hint re-using recent material, the page
       * should still sit in the cache, or we're using old info, which should
       * be discarded.
       */
      st = env->get_page_manager()->fetch_page(&page, db,
                      m_hints.leaf_page_addr, true);
      if (st)
        return st;
      /* if the page is not in cache: do a regular insert */
      if (!page)
        return (insert());

      BtreeNodeProxy *node = BtreeNodeFactory::get(page);
      ham_assert(node->is_leaf());

      /*
       * if the page is already full OR this page is not the right-most page
       * when we APPEND or the left-most node when we PREPEND
       * OR the new key is not the highest key: perform a normal insert
       */
      if ((m_hints.flags & HAM_HINT_APPEND && node->get_right() != 0)
              || (m_hints.flags & HAM_HINT_PREPEND && node->get_left() != 0)
              || node->requires_split(m_key))
        return (insert());

      /*
       * if the page is not empty: check if we append the key at the end / start
       * (depending on force_append/force_prepend),
       * or if it's actually inserted in the middle (when neither force_append
       * or force_prepend is specified: that'd be SEQUENTIAL insertion
       * hinting somewhere in the middle of the total key range.
       */
      if (node->get_count() != 0) {
        if (m_hints.flags & HAM_HINT_APPEND) {
          int cmp_hi = node->compare(m_key, node->get_count() - 1);
          /* key is at the end */
          if (cmp_hi > 0) {
            ham_assert(node->get_right() == 0);
            force_append = true;
          }
        }

        if (m_hints.flags & HAM_HINT_PREPEND) {
          int cmp_lo = node->compare(m_key, 0);
          /* key is at the start of page */
          if (cmp_lo < 0) {
            ham_assert(node->get_left() == 0);
            force_prepend = true;
          }
        }
      }

      /* OK - we're really appending/prepending the new key.  */
      if (force_append || force_prepend)
        return (insert_in_leaf(page, m_key, 0, force_prepend, force_append));
      else
        return (insert());
    }

    ham_status_t insert() {
      LocalDatabase *db = m_btree->get_db();
      LocalEnvironment *env = db->get_local_env();

      /* get the root-page...  */
      Page *root;
      ham_status_t st = env->get_page_manager()->fetch_page(&root, db,
                      m_btree->get_root_address());
      if (st)
        return (st);

      /* ... and start the recursion */
      st = insert_recursive(root, m_key, 0);

      /* create a new root page if it needs to be split */
      if (st == kSplitRequired) {
        st = split_root(root);
        if (st)
          return (st);
      }

      return (st);
    }

    ham_status_t split_root(Page *root) {
      LocalDatabase *db = m_btree->get_db();
      LocalEnvironment *env = db->get_local_env();

      /* allocate a new root page */
      Page *newroot;
      ham_status_t st = env->get_page_manager()->alloc_page(&newroot, db,
                      Page::kTypeBroot, 0);
      if (st)
        return (st);
      ham_assert(newroot->get_db());

      m_btree->get_statistics()->reset_page(root);

      // initialize the new page
      BtreeNodeProxy *node = BtreeNodeFactory::get(newroot);
      node->initialize();

      /* insert the pivot element and the ptr_down */
      node->set_ptr_down(m_btree->get_root_address());

      ham_key_t split_key = {0};
      split_key.data = m_split_key.get_ptr();
      split_key.size = m_split_key.get_size();
      st = insert_in_leaf(newroot, &split_key, m_split_rid);
      /* don't overwrite cursor if insert_in_leaf is called again */
      m_cursor = 0;
      if (st)
        return (st);

      /*
       * set the new root page
       *
       * !!
       * do NOT delete the old root page - it's still in use!
       */
      m_btree->set_root_address(newroot->get_address());
      root->set_type(Page::kTypeBindex);
      root->set_dirty(true);
      newroot->set_dirty(true);
      return (0);
    }

    // This is the function which does most of the work - traversing to a
    // leaf, inserting the key using insert_in_page()
    // and performing necessary SMOs. It works recursive.
    ham_status_t insert_recursive(Page *page, ham_key_t *key, ham_u64_t rid) {
      BtreeNodeProxy *node = BtreeNodeFactory::get(page);

      /* if we've reached a leaf: insert the key */
      if (node->is_leaf())
        return (insert_in_page(page, key, rid));

      /* otherwise traverse the root down to the leaf */
      Page *child;
      ham_status_t st = m_btree->find_internal(page, key, &child);
      if (st)
        return (st);

      /* and call this function recursively */
      st = insert_recursive(child, key, rid);
      switch (st) {
        /* if we're done, we're done */
        case HAM_SUCCESS:
          break;
        /* if we tried to insert a duplicate key, we're done, too */
        case HAM_DUPLICATE_KEY:
          break;
        /* the child was split, and we have to insert a new key/rid-pair.  */
        case kSplitRequired: {
          m_hints.flags |= HAM_OVERWRITE;
          m_cursor = 0;
          ham_key_t split_key = {0};
          split_key.data = m_split_key.get_ptr();
          split_key.size = m_split_key.get_size();
          st = insert_in_page(page, &split_key, m_split_rid);

          m_hints.flags = m_hints.original_flags;
          m_hints.processed_leaf_page = 0;
          m_hints.processed_slot = 0;
          break;
        }
        /* every other return value is unexpected and shouldn't happen */
        default:
          break;
      }

      return (st);
    }

    // Inserts a key in a page; if necessary, the page is split
    ham_status_t insert_in_page(Page *page, ham_key_t *key, ham_u64_t rid) {
      ham_status_t st;
      BtreeNodeProxy *node = BtreeNodeFactory::get(page);

      ham_assert(m_btree->get_maxkeys() > 5);

      /*
       * if we can insert the new key without splitting the page then
       * insert_in_leaf() will do the work for us
       */
      if (!node->requires_split(key)) {
        st = insert_in_leaf(page, key, rid);
        /* don't overwrite cursor if insert_in_leaf is called again */
        m_cursor = 0;
        return (st);
      }

      /* otherwise split the page. The split might be unnecessary because the
       * following insert can still fail, i.e. because the key already
       * exists. But these rare cases do not justify extra lookups in all other
       * cases, and the split would anyway happen sooner or later. */
      return (insert_split(page, key, rid));
    }

    // Splits a page and inserts the new element
    ham_status_t insert_split(Page *page, ham_key_t *key, ham_u64_t rid) {
      LocalDatabase *db = m_btree->get_db();
      LocalEnvironment *env = db->get_local_env();

      /* allocate a new page */
      Page *new_page;
      ham_status_t st = env->get_page_manager()->alloc_page(&new_page, db,
                      Page::kTypeBindex, 0);
      if (st)
        return st;

      m_btree->get_statistics()->reset_page(page);

      BtreeNodeProxy *new_node = BtreeNodeFactory::get(new_page);
      BtreeNodeProxy *old_node = BtreeNodeFactory::get(page);
      ham_size_t count = old_node->get_count();

      // initialize the new page
      new_node->initialize();

      /*
       * for databases with sequential access (this includes recno databases):
       * do not split in the middle, but at the very end of the page.
       *
       * if this page is the right-most page in the index, and this key is
       * inserted at the very end, then we select the same pivot as for
       * sequential access.
       */
      bool pivot_at_end = false;
      if (m_hints.flags & HAM_HINT_APPEND && m_hints.append_count > 5)
        pivot_at_end = true;
      else if (old_node->get_right() == 0) {
        int cmp = old_node->compare(key, old_node->get_count() - 1);
        if (cmp > 0)
          pivot_at_end = true;
      }

      /* The position of the pivot key depends on the previous inserts; if most
       * of them were appends then pick a pivot key at the "end" of the node */
      int pivot;
      if (pivot_at_end || m_hints.append_count > 30)
        pivot = count - 2;
      else if (m_hints.append_count > 10)
        pivot = (count / 100.f * 66);
      else if (m_hints.prepend_count > 10)
        pivot = (count / 100.f * 33);
      else if (m_hints.prepend_count > 30)
        pivot = 2;
      else
        pivot = count / 2;
      ham_assert(pivot > 0 && pivot <= (int)count - 2);

      /* uncouple all cursors */
      st = BtreeCursor::uncouple_all_cursors(page, pivot);
      if (st)
        return (st);

      /* move some of the key/rid-tuples to the new page */
      old_node->split(new_node, pivot);

      /* Store the pivot element to propagate it to the parent page.
       * This requires a separate ByteArray because key->data might
       * point to m_split_key, and overwriting m_split_key will effectively
       * change key->data. */
      ByteArray pivot_key;
      ham_key_t tmpkey = {0};
      st = old_node->copy_full_key(pivot, &pivot_key, &tmpkey);
      if (st)
        return (st);
      m_split_rid = new_page->get_address();

      /* if we're in an internal page: fix the ptr_down of the new page
       * (it points to the ptr of the pivot key) */
      if (!old_node->is_leaf())
        new_node->set_ptr_down(old_node->get_record_id(pivot));

      /* insert the new element */
      int cmp = old_node->compare(key, pivot);
      if (cmp >= 0)
        st = insert_in_leaf(new_page, key, rid);
      else
        st = insert_in_leaf(page, key, rid);

      // continue if the key is a duplicate; we nevertheless have to
      // finish the SMO 
      if (st && st != HAM_DUPLICATE_KEY)
        return (st);

      /* don't overwrite cursor if insert_in_leaf is called again */
      m_cursor = 0;

      /* fix the double-linked list of pages, and mark the pages as dirty */
      Page *sib_page = 0;
      if (old_node->get_right()) {
        st = env->get_page_manager()->fetch_page(&sib_page, db,
                        old_node->get_right());
        if (st)
          return (st);
      }

      new_node->set_left(page->get_address());
      new_node->set_right(old_node->get_right());
      old_node->set_right(new_page->get_address());
      if (sib_page) {
        BtreeNodeProxy *sib_node = BtreeNodeFactory::get(sib_page);
        sib_node->set_left(new_page->get_address());
        sib_page->set_dirty(true);
      }
      new_page->set_dirty(true);
      page->set_dirty(true);

      // assign the previously stored pivot key to m_split_key
      m_split_key.clear();
      m_split_key = pivot_key;
      pivot_key.disown();

      BtreeIndex::ms_btree_smo_split++;

      if (g_BTREE_INSERT_SPLIT_HOOK)
        g_BTREE_INSERT_SPLIT_HOOK();
      return (kSplitRequired);
    }

    ham_status_t insert_in_leaf(Page *page, ham_key_t *key, ham_u64_t rid,
                bool force_prepend = false, bool force_append = false) {
      ham_status_t st;
      ham_size_t new_dupe_id = 0;
      LocalDatabase *db = m_btree->get_db();
      LocalEnvironment *env = db->get_local_env();
      bool exists = false;
      ham_s32_t slot;

      BtreeNodeProxy *node = BtreeNodeFactory::get(page);
      int count = node->get_count();

      if (node->get_count() == 0)
        slot = 0;
      else if (force_prepend)
        slot = 0;
      else if (force_append)
        slot = node->get_count();
      else {
        int cmp;
        slot = node->get_slot(key, &cmp);

        /* insert the new key at the beginning? */
        if (slot == -1)
          slot = 0;
        else {
          /* key exists already */
          if (cmp == 0) {
            if (m_hints.flags & HAM_OVERWRITE) {
              /* key already exists; only overwrite the data */
              if (!node->is_leaf())
                return (HAM_SUCCESS);
            }
            else if (!(m_hints.flags & HAM_DUPLICATE))
              return (HAM_DUPLICATE_KEY);

            /* do NOT shift keys up to make room; just overwrite the
             * current [slot] */
            exists = true;
          }
          else {
            /*
             * otherwise, if the new key is > then the slot key, move to
             * the next slot
             */
            if (cmp > 0)
              slot++;
          }
        }
      }

      // uncouple the cursors
      if (!exists && count > slot) {
        st = BtreeCursor::uncouple_all_cursors(page, slot);
        if (st)
          return (st);
      }

      if (exists) {
        if (node->is_leaf()) {
          // overwrite record blob
          st = node->set_record(slot, m_txn, m_record,
                        m_cursor
                            ? m_cursor->get_duplicate_index()
                            : 0,
                        m_hints.flags, &new_dupe_id);
          if (st)
            return (st);

          m_hints.processed_leaf_page = page;
          m_hints.processed_slot = slot;
        }
        else {
          // overwrite record id
          node->set_record_id(slot, rid);
        }
      }
      // key does not exist and has to be inserted or appended
      else {
        // actually insert the key
        st = node->insert(slot, key, env->get_blob_manager());
        if (st)
          return (st);

        if (node->is_leaf()) {
          // allocate record id
          st = node->set_record(slot, m_txn, m_record,
                        m_cursor
                            ? m_cursor->get_duplicate_index()
                            : 0,
                        m_hints.flags, &new_dupe_id);
          if (st) // TODO undo the previous insert!
            return (st);

          m_hints.processed_leaf_page = page;
          m_hints.processed_slot = slot;
        }
        else {
          // set the internal record id
          node->set_record_id(slot, rid);
        }
        node->set_count(count + 1);
      }
      page->set_dirty(true);

      /* if we have a cursor: couple it to the new key */
      if (m_cursor) {
        m_cursor->get_parent()->set_to_nil(Cursor::kBtree);

        ham_assert(m_cursor->get_state() == BtreeCursor::kStateNil);
        m_cursor->couple_to_page(page, slot, new_dupe_id);
      }

      return (0);
    }

    // the current btree
    BtreeIndex *m_btree;

    // the current transaction
    Transaction *m_txn;

    // the current cursor
    BtreeCursor *m_cursor;

    // the key that is inserted
    ham_key_t *m_key;

    // the record that is inserted
    ham_record_t *m_record;

    // the pivot key for SMOs and splits
    ByteArray m_split_key;

    // the pivot record ID for SMOs and splits
    ham_u64_t m_split_rid;

    // flags of ham_db_insert()
    ham_u32_t m_flags;

    // statistical hints for this operation
    BtreeStatistics::InsertHints m_hints;
};

ham_status_t
BtreeIndex::insert(Transaction *txn, Cursor *cursor, ham_key_t *key,
                ham_record_t *record, ham_u32_t flags)
{
  BtreeInsertAction bia(this, txn, cursor, key, record, flags);
  return (bia.run());
}

} // namespace hamsterdb

