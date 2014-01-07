/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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

#include "cursor.h"
#include "cache.h"
#include "db.h"
#include "env.h"
#include "error.h"
#include "mem.h"
#include "util.h"
#include "page.h"
#include "blob_manager.h"
#include "page_manager.h"
#include "btree_index.h"
#include "btree_stats.h"
#include "btree_node_proxy.h"
#include "btree_cursor.h"

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
        m_record(record), m_split_rid(0), m_flags(flags), m_final_status(0) {
      if (cursor) {
        m_cursor = cursor->get_btree_cursor();
        ham_assert(m_btree->get_db() == m_cursor->get_parent()->get_db());
      }
    }

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
       * flag and recursively call do_insert_cursor()
       */
      ham_status_t st;
      if (m_hints.leaf_page_addr
          && (m_hints.flags & HAM_HINT_APPEND
              || m_hints.flags & HAM_HINT_PREPEND))
        st = append_or_prepend_key();
      else
        st = insert();

      if (st == 0 && m_final_status != 0)
        st = m_final_status;

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
      page = env->get_page_manager()->fetch_page(db,
                    m_hints.leaf_page_addr, true);
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
      Page *root = env->get_page_manager()->fetch_page(db,
                    m_btree->get_root_address());

      /* ... and start the recursion */
      ham_status_t st = insert_recursive(root, m_key, 0);

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
      Page *newroot = env->get_page_manager()->alloc_page(db,
                            Page::kTypeBroot, 0);
      ham_assert(newroot->get_db());

      m_btree->get_statistics()->reset_page(root);

      /* insert the pivot element and set ptr_down */
      BtreeNodeProxy *node = m_btree->get_node_from_page(newroot);
      node->set_ptr_down(m_btree->get_root_address());

      ham_key_t split_key = {0};
      split_key.data = m_split_key_arena.get_ptr();
      split_key.size = m_split_key_arena.get_size();
      ham_status_t st = insert_in_leaf(newroot, &split_key, m_split_rid);
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
      BtreeNodeProxy *node = m_btree->get_node_from_page(page);

      /* if we've reached a leaf: insert the key */
      if (node->is_leaf())
        return (insert_in_page(page, key, rid));

      /* otherwise traverse the root down to the leaf */
      Page *child = m_btree->find_internal(page, key);

      /* and call this function recursively */
      ham_status_t st = insert_recursive(child, key, rid);
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
          split_key.data = m_split_key_arena.get_ptr();
          split_key.size = m_split_key_arena.get_size();
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
      BtreeNodeProxy *node = m_btree->get_node_from_page(page);

      /*
       * if we can insert the new key without splitting the page then
       * insert_in_leaf() will do the work for us
       */
      if (!node->requires_split(key)) {
        ham_status_t st = insert_in_leaf(page, key, rid);
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

      BtreeNodeProxy *old_node = m_btree->get_node_from_page(page);

      /* allocate a new page */
      Page *new_page = env->get_page_manager()->alloc_page(db,
                            Page::kTypeBindex, 0);
      {
        PBtreeNode *node = PBtreeNode::from_page(new_page);
        node->set_flags(old_node->is_leaf() ? PBtreeNode::kLeafNode : 0);
      }

      m_btree->get_statistics()->reset_page(page);

      BtreeNodeProxy *new_node = m_btree->get_node_from_page(new_page);
      ham_u32_t count = old_node->get_count();

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
      BtreeCursor::uncouple_all_cursors(page, pivot);

      // Store the pivot key so it can be propagated to the parent page.
      // This requires a separate ByteArray because key->data might
      // point to m_split_key_arena, and overwriting m_split_key_arena
      // will effectively change key->data.
      ByteArray split_key_arena;
      ham_key_t split_key = {0};
      old_node->get_key(pivot, &split_key_arena, &split_key);
      m_split_rid = new_page->get_address();

      /* if we're in an internal page: fix the ptr_down of the new page
       * (it points to the ptr of the pivot key) */
      if (!old_node->is_leaf())
        new_node->set_ptr_down(old_node->get_record_id(pivot));

      /* now move some of the key/rid-tuples to the new page */
      old_node->split(new_node, pivot);

      /* insert the new element in the old or the new page? */
      int cmp = pivot_at_end
                    ? 1
                    : old_node->compare(key, &split_key);

      ham_status_t st;
      if (cmp >= 0)
        st = insert_in_leaf(new_page, key, rid);
      else
        st = insert_in_leaf(page, key, rid);

      // continue if the key is a duplicate; we nevertheless have to
      // finish the SMO (but make sure we do not lose the return value)
      if (st) {
        if (st == HAM_DUPLICATE_KEY)
          m_final_status = st;
        else
          return (st);
      }

      /* don't overwrite cursor if insert_in_leaf is called again */
      m_cursor = 0;

      /* fix the double-linked list of pages, and mark the pages as dirty */
      Page *sib_page = 0;
      if (old_node->get_right()) {
        sib_page = env->get_page_manager()->fetch_page(db,
                        old_node->get_right());
      }

      new_node->set_left(page->get_address());
      new_node->set_right(old_node->get_right());
      old_node->set_right(new_page->get_address());
      if (sib_page) {
        BtreeNodeProxy *sib_node = m_btree->get_node_from_page(sib_page);
        sib_node->set_left(new_page->get_address());
        sib_page->set_dirty(true);
      }
      new_page->set_dirty(true);
      page->set_dirty(true);

      // assign the previously stored pivot key to m_split_key_arena
      m_split_key_arena.clear();
      m_split_key_arena = split_key_arena;
      split_key_arena.disown();

      BtreeIndex::ms_btree_smo_split++;

      if (g_BTREE_INSERT_SPLIT_HOOK)
        g_BTREE_INSERT_SPLIT_HOOK();
      return (kSplitRequired);
    }

    ham_status_t insert_in_leaf(Page *page, ham_key_t *key, ham_u64_t rid,
                bool force_prepend = false, bool force_append = false) {
      ham_u32_t new_dupe_id = 0;
      bool exists = false;
      ham_s32_t slot;

      BtreeNodeProxy *node = m_btree->get_node_from_page(page);
      int count = node->get_count();

      if (count == 0)
        slot = 0;
      else if (force_prepend)
        slot = 0;
      else if (force_append)
        slot = count;
      else {
        int cmp;
        slot = node->find(key, &cmp);

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
      if (!exists && count > slot)
        BtreeCursor::uncouple_all_cursors(page, slot);

      if (exists) {
        if (node->is_leaf()) {
          // overwrite record blob
          node->set_record(slot, m_record,
                        m_cursor
                            ? m_cursor->get_duplicate_index()
                            : 0,
                        m_hints.flags, &new_dupe_id);

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
        node->insert(slot, key);

        if (node->is_leaf()) {
          // allocate record id
          node->set_record(slot, m_record,
                        m_cursor
                            ? m_cursor->get_duplicate_index()
                            : 0,
                        m_hints.flags, &new_dupe_id);

          m_hints.processed_leaf_page = page;
          m_hints.processed_slot = slot;
        }
        else {
          // set the internal record id
          node->set_record_id(slot, rid);
        }
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
    ByteArray m_split_key_arena;

    // the pivot record ID for SMOs and splits
    ham_u64_t m_split_rid;

    // flags of ham_db_insert()
    ham_u32_t m_flags;

    // helper to avoid losing the result
    ham_status_t m_final_status;

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

