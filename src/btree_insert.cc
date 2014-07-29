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

#include "config.h"

#include <string.h>
#include <algorithm>

#include "cursor.h"
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
        m_record(record), m_flags(flags) {
      if (cursor) {
        m_cursor = cursor->get_btree_cursor();
        ham_assert(m_btree->get_db() == m_cursor->get_parent()->get_db());
      }
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
                    m_hints.leaf_page_addr, PageManager::kOnlyFromCache);
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
              || split_required(node))
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

      /* otherwise reset the hints because they are no longer valid */
      m_hints.flags &= ~HAM_HINT_APPEND;
      m_hints.flags &= ~HAM_HINT_PREPEND;
      return (insert());
    }

    bool split_required(BtreeNodeProxy *node) {
      return (node->requires_split(m_key));
    }

    ham_status_t insert() {
      LocalDatabase *db = m_btree->get_db();
      LocalEnvironment *env = db->get_local_env();

      Page *parent = 0;
      Page *page = env->get_page_manager()->fetch_page(db,
                    m_btree->get_root_address());
      BtreeNodeProxy *node = m_btree->get_node_from_page(page);

      // now walk down the tree
      while (1) {
        if (split_required(node)) {
          if (node->is_leaf()
                 && !(m_flags & HAM_OVERWRITE)
                 && !(m_flags & HAM_DUPLICATE)) {
            int cmp;
            int slot = node->find_child(m_key, 0, &cmp);
            if (slot >= 0 && cmp == 0)
              return (HAM_DUPLICATE_KEY);
          }
          page = split_page(page, parent, m_key);
          node = m_btree->get_node_from_page(page);
        }

        if (node->is_leaf())
          break;

        parent = page;
        page = m_btree->find_child(page, m_key);
        node = m_btree->get_node_from_page(page);
      }

      // We've reached the leaf; it's still possible that we have to
      // split the page, therefore this case has to be handled
      ham_status_t st;
      try {
        st = insert_in_leaf(page, m_key, 0);
      }
      catch (Exception &ex) {
        if (ex.code == HAM_LIMITS_REACHED) {
          page = split_page(page, parent, m_key);
          return (insert_in_leaf(page, m_key, 0));
        }
        throw ex;
      }
      return (st);
    }

    // Splits |page| and updates the |parent|. If |parent| is null then
    // it's assumed that |page| is the root node.
    // Returns the new page in the path for |key|; caller can immediately
    // continue the traversal.
    Page *split_page(Page *old_page, Page *parent, ham_key_t *key) {
      LocalDatabase *db = m_btree->get_db();
      LocalEnvironment *env = db->get_local_env();

      m_btree->get_statistics()->reset_page(old_page);
      BtreeNodeProxy *old_node = m_btree->get_node_from_page(old_page);

      /* allocate a new page and initialize it */
      Page *new_page = env->get_page_manager()->alloc_page(db,
                                    Page::kTypeBindex);
      {
        PBtreeNode *node = PBtreeNode::from_page(new_page);
        node->set_flags(old_node->is_leaf() ? PBtreeNode::kLeafNode : 0);
      }
      BtreeNodeProxy *new_node = m_btree->get_node_from_page(new_page);

      /* no parent page? then we're splitting the root page. allocate
       * a new root page */
      if (!parent)
        parent = allocate_new_root(old_page);



      Page *to_return = 0;
      ByteArray pivot_key_arena;
      ham_key_t pivot_key = {0};

      /* if the key is appended then don't split the page; simply allocate
       * a new page and insert the new key. */
      int pivot = 0;
      if (m_hints.flags & HAM_HINT_APPEND && old_node->is_leaf()) {
        int cmp = old_node->compare(key, old_node->get_count() - 1);
        if (cmp == +1) {
          to_return = new_page;
          pivot_key = *key;
          pivot = old_node->get_count();
        }
      }

      /* no append? then calculate the pivot key and perform the split */
      if (pivot != (int)old_node->get_count()) {
        pivot = get_pivot(old_node);

        /* and store the pivot key for later */
        old_node->get_key(pivot, &pivot_key_arena, &pivot_key);

        /* leaf page: uncouple all cursors */
        if (old_node->is_leaf())
          BtreeCursor::uncouple_all_cursors(old_page, pivot);
        /* internal page: fix the ptr_down of the new page
         * (it must point to the ptr of the pivot key) */
        else
          new_node->set_ptr_down(old_node->get_record_id(pivot));

        /* now move some of the key/rid-tuples to the new page */
        old_node->split(new_node, pivot);

        // if the new key is >= the pivot key then continue with the right page,
        // otherwise continue with the left page
        to_return = m_btree->compare_keys(key, &pivot_key) >= 0
                          ? new_page
                          : old_page;
      }

      /* update the parent page */
      BtreeNodeProxy *parent_node = m_btree->get_node_from_page(parent);
      ham_status_t st = insert_in_leaf(parent, &pivot_key,
                            new_page->get_address());
      if (st)
        throw Exception(st);
      /* new root page? then also set ptr_down! */
      if (parent_node->get_count() == 0)
        parent_node->set_ptr_down(old_page->get_address());

      /* fix the double-linked list of pages, and mark the pages as dirty */
      if (old_node->get_right()) {
        Page *sib_page = env->get_page_manager()->fetch_page(db,
                        old_node->get_right());
        BtreeNodeProxy *sib_node = m_btree->get_node_from_page(sib_page);
        sib_node->set_left(new_page->get_address());
        sib_page->set_dirty(true);
      }
      new_node->set_left(old_page->get_address());
      new_node->set_right(old_node->get_right());
      old_node->set_right(new_page->get_address());
      new_page->set_dirty(true);
      old_page->set_dirty(true);

      BtreeIndex::ms_btree_smo_split++;

      if (g_BTREE_INSERT_SPLIT_HOOK)
        g_BTREE_INSERT_SPLIT_HOOK();

      return (to_return);
    }

    /*
     * Calculates the pivot index of a split.
     *
     * For databases with sequential access (this includes recno databases):
     * do not split in the middle, but at the very end of the page.
     *
     * If this page is the right-most page in the index, and the new key is
     * inserted at the very end, then we select the same pivot as for
     * sequential access.
     */
    int get_pivot(BtreeNodeProxy *old_node) {
      ham_u32_t old_count = old_node->get_count();
      ham_assert(old_count > 2);

      bool pivot_at_end = false;
      if (m_hints.flags & HAM_HINT_APPEND && m_hints.append_count > 5)
        pivot_at_end = true;
      else if (old_node->get_right() == 0) {
        int cmp = old_node->compare(m_key, old_node->get_count() - 1);
        if (cmp > 0)
          pivot_at_end = true;
      }

      /* The position of the pivot key depends on the previous inserts; if most
       * of them were appends then pick a pivot key at the "end" of the node */
      int pivot;
      if (pivot_at_end || m_hints.append_count > 30)
        pivot = old_count - 2;
      else if (m_hints.append_count > 10)
        pivot = (old_count / 100.f * 66);
      else if (m_hints.prepend_count > 10)
        pivot = (old_count / 100.f * 33);
      else if (m_hints.prepend_count > 30)
        pivot = 2;
      else
        pivot = old_count / 2;

      ham_assert(pivot > 0 && pivot <= (int)old_count - 2);

      return (pivot);
    }

    // Allocates a new root page and sets it up in the btree
    Page *allocate_new_root(Page *old_root) {
      LocalDatabase *db = m_btree->get_db();
      LocalEnvironment *env = db->get_local_env();

      Page *new_root = env->get_page_manager()->alloc_page(db,
                      Page::kTypeBroot);

      /* insert the pivot element and set ptr_down */
      BtreeNodeProxy *new_node = m_btree->get_node_from_page(new_root);
      new_node->set_ptr_down(old_root->get_address());

      m_btree->set_root_address(new_root->get_address());
      old_root->set_type(Page::kTypeBindex);

      return (new_root);
    }

    ham_status_t insert_in_leaf(Page *page, ham_key_t *key, ham_u64_t rid,
                bool force_prepend = false, bool force_append = false) {
      ham_u32_t new_dupe_id = 0;
      bool exists = false;

      BtreeNodeProxy *node = m_btree->get_node_from_page(page);
      ham_u32_t count = node->get_count();

      int slot;
      if (count == 0)
        slot = 0;
      else if (force_prepend)
        slot = 0;
      else if (force_append)
        slot = count;
      else {
        int cmp;
        slot = node->find_child(key, 0, &cmp);

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
             * otherwise, if the new key is > than the slot key: move to
             * the next slot
             */
            if (cmp > 0)
              slot++;
          }
        }
      }

      // uncouple the cursors
      if (!exists && (int)count > slot)
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

        try {
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
        // In case of an error: undo the insert. This happens very rarely but
        // it's possible, i.e. if the BlobManager fails to allocate storage.
        catch (Exception &ex) {
          if (slot < (int)node->get_count())
            node->erase(slot);
          throw ex;
        }
      }

      page->set_dirty(true);

      /* if we have a cursor (and this is a leaf node): couple it to the
       * inserted key */
      if (m_cursor && node->is_leaf()) {
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

