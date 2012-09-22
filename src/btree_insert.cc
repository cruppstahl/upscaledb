/**
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 *
 * btree inserting
 *
 */

#include "config.h"

#include <string.h>

#include "internal_fwd_decl.h"
#include "blob.h"
#include "btree.h"
#include "btree_cursor.h"
#include "extkeys.h"
#include "cursor.h"
#include "cache.h"
#include "db.h"
#include "device.h"
#include "env.h"
#include "error.h"
#include "btree_key.h"
#include "log.h"
#include "mem.h"
#include "page.h"
#include "btree_stats.h"
#include "txn.h"
#include "util.h"
#include "btree_node.h"

namespace ham {

/* a unittest hook triggered when a page is split */
void (*g_BTREE_INSERT_SPLIT_HOOK)(void);

class BtreeInsertAction
{
  enum {
    /** page split required */
    SPLIT = 1
  };

  public:
    BtreeInsertAction(BtreeBackend *backend, Transaction *txn, Cursor *cursor,
        ham_key_t *key, ham_record_t *record, ham_u32_t flags)
      : m_backend(backend), m_txn(txn), m_cursor(0), m_key(key),
        m_record(record), m_split_rid(0), m_flags(flags) {
      memset(&m_split_key, 0, sizeof(m_split_key));
      if (cursor) {
        m_cursor = cursor->get_btree_cursor();
        ham_assert(m_backend->get_db() == btree_cursor_get_db(m_cursor));
      }
    }

    ~BtreeInsertAction() {
      Environment *env = m_backend->get_db()->get_env();
      if (m_split_key.data)
        env->get_allocator()->free(m_split_key.data);
    }

    ham_status_t run() {
      ham_status_t st;
      BtreeStatistics *stats = m_backend->get_statistics();

      m_hints = stats->get_insert_hints(m_flags);

      /*
       * append the key? append_key() will try to append the key; if it
       * fails because the key is NOT the largest key in the database or
       * because the current page is already full, it will remove the
       * HINT_APPEND flag and recursively call do_insert_cursor()
       */
      if (m_hints.flags & HAM_HINT_APPEND)
        st = append_key();
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
    /** append a key to the "end" of the btree */
    ham_status_t append_key() {
      ham_status_t st = 0;
      Page *page;
      Database *db = m_backend->get_db();
      bool force_append = false;
      bool force_prepend = false;

      /*
       * see if we get this btree leaf; if not, revert to regular scan
       *
       * As this is a speed-improvement hint re-using recent material, the page
       * should still sit in the cache, or we're using old info, which should
       * be discarded.
       */
      st = db_fetch_page(&page, db, m_hints.leaf_page_addr, DB_ONLY_FROM_CACHE);
      if (st)
        return st;
      if (!page)
        return (insert());

      BtreeNode *node = BtreeNode::from_page(page);
      ham_assert(node->is_leaf());

      /*
       * if the page is already full OR this page is not the right-most page
       * when we APPEND or the left-most node when we PREPEND
       * OR the new key is not the highest key: perform a normal insert
       */
      if ((m_hints.flags & HAM_HINT_APPEND && node->get_right())
              || (m_hints.flags & HAM_HINT_PREPEND && node->get_left())
              || node->get_count() >= m_backend->get_maxkeys())
        return (insert());

      /*
       * if the page is not empty: check if we append the key at the end / start
       * (depending on force_append/force_prepend),
       * or if it's actually inserted in the middle (when neither force_append
       * or force_prepend is specified: that'd be SEQUENTIAL insertion
       * hinting somewhere in the middle of the total key range.
       */
      if (node->get_count() != 0) {
        int cmp_hi;
        int cmp_lo;

        if (m_hints.flags & HAM_HINT_APPEND) {
          cmp_hi = m_backend->compare_keys(page, m_key, node->get_count() - 1);
          /* key is in the middle */
          if (cmp_hi < -1)
            return ((ham_status_t)cmp_hi);
          /* key is at the end */
          if (cmp_hi > 0) {
            if (node->get_right()) {
              /* not at top end of the btree, so we can't do the
               * fast track */
              return (insert());
            }

            force_append = true;
          }
        }

        if (m_hints.flags & HAM_HINT_PREPEND) {
          cmp_lo = m_backend->compare_keys(page, m_key, 0);
          /* in the middle range */
          if (cmp_lo < -1)
            return ((ham_status_t)cmp_lo);
          /* key is at the start of page */
          if (cmp_lo < 0) {
            if (node->get_left()) {
              /* not at bottom end of the btree, so we can't
               * do the fast track */
              return (insert());
            }

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
      ham_status_t st;
      Page *root;
      Database *db = m_backend->get_db();

      /* get the root-page...  */
      st = db_fetch_page(&root, db, m_backend->get_rootpage(), 0);
      if (st)
        return (st);

      /* ... and start the recursion */
      st = insert_recursive(root, m_key, 0);

      /* create a new root page if it needs to be split */
      if (st == SPLIT) {
        st = split_root(root);
        if (st)
          return (st);
      }

      return (st);
    }

    ham_status_t split_root(Page *root) {
      /* allocate a new root page */
      Page *newroot;
      Database *db = m_backend->get_db();
      ham_status_t st = db_alloc_page(&newroot, db, Page::TYPE_B_ROOT, 0);
      if (st)
        return (st);
      ham_assert(newroot->get_db());

      /* clear the node header */
      memset(newroot->get_payload(), 0, sizeof(BtreeNode));

      m_backend->get_statistics()->reset_page(root);

      /* insert the pivot element and the ptr_left */
      BtreeNode *node = BtreeNode::from_page(newroot);
      node->set_ptr_left(m_backend->get_rootpage());
      st = insert_in_leaf(newroot, &m_split_key, m_split_rid);
      ham_assert(!(m_split_key.flags & HAM_KEY_USER_ALLOC));
      /* don't overwrite cursor if insert_in_leaf is called again */
      m_cursor = 0;
      if (st)
        return (st);

      /*
       * set the new root page
       *
       * !!
       * do NOT delete the old root page - it's still in use! also add the
       * root page to the changeset to make sure that the changes are logged
       */
      m_backend->set_rootpage(newroot->get_self());
      m_backend->do_flush_indexdata();
      if (db->get_env()->get_flags() & HAM_ENABLE_RECOVERY)
        db->get_env()->get_changeset().add_page(db->get_env()->get_header_page());
      root->set_type(Page::TYPE_B_INDEX);
      root->set_dirty(true);
      newroot->set_dirty(true);
      return (0);
    }

    /**
     * this is the function which does most of the work - traversing to a
     * leaf, inserting the key using insert_in_page()
     * and performing necessary SMOs. it works recursive.
     */
    ham_status_t insert_recursive(Page *page, ham_key_t *key,
                    ham_offset_t rid) {
      Page *child;
      BtreeNode *node = BtreeNode::from_page(page);

      /* if we've reached a leaf: insert the key */
      if (node->is_leaf())
        return (insert_in_page(page, key, rid));

      /* otherwise traverse the root down to the leaf */
      ham_status_t st = m_backend->find_internal(page, key, &child);
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
        case SPLIT:
          m_hints.flags |= HAM_OVERWRITE;
          m_cursor = 0;
          st = insert_in_page(page, &m_split_key, m_split_rid);
          m_hints.flags = m_hints.original_flags;
          m_hints.processed_leaf_page = 0;
          m_hints.processed_slot = 0;
          break;
        /* every other return value is unexpected and shouldn't happen */
        default:
          break;
      }

      return (st);
    }

    /**
     * this function inserts a key in a page; if necessary, the page is split
     */
    ham_status_t insert_in_page(Page *page, ham_key_t *key, ham_offset_t rid) {
      ham_status_t st;
      ham_size_t maxkeys = m_backend->get_maxkeys();
      BtreeNode *node = BtreeNode::from_page(page);

      ham_assert(maxkeys > 1);

      /*
       * if we can insert the new key without splitting the page then
       * insert_in_leaf() will do the work for us
       */
      if (node->get_count() < maxkeys) {
        st = insert_in_leaf(page, key, rid);
        /* don't overwrite cursor if insert_in_leaf is called again */
        m_cursor = 0;
        return (st);
      }

      /*
       * otherwise, we have to split the page.
       * but BEFORE we split, we check if the key already exists!
       */
      if (node->is_leaf()) {
        ham_s32_t idx = m_backend->find_leaf(page, key, HAM_FIND_EXACT_MATCH);
        /* key exists! */
        if (idx >= 0) {
          ham_assert((m_hints.flags & (HAM_DUPLICATE_INSERT_BEFORE
                                | HAM_DUPLICATE_INSERT_AFTER
                                | HAM_DUPLICATE_INSERT_FIRST
                                | HAM_DUPLICATE_INSERT_LAST))
                    ? (m_hints.flags & HAM_DUPLICATE)
                    : 1);
          if (!(m_hints.flags & (HAM_OVERWRITE | HAM_DUPLICATE)))
            return (HAM_DUPLICATE_KEY);
          st = insert_in_leaf(page, key, rid);
          /* don't overwrite cursor if insert_in_leaf is called again */
          m_cursor = 0;
          return (st);
        }
      }

      return (insert_split(page, key, rid));
    }

    /**
     * split a page and insert the new element
     */
    ham_status_t insert_split(Page *page, ham_key_t *key, ham_offset_t rid) {
      int cmp;
      Page *newpage, *oldsib;
      ham_size_t keysize = m_backend->get_keysize();
      Database *db = page->get_db();
      Environment *env = db->get_env();
      ham_u16_t pivot;
      ham_offset_t pivotrid;
      bool pivot_at_end = false;

      /* allocate a new page */
      ham_status_t st = db_alloc_page(&newpage, db, Page::TYPE_B_INDEX, 0);
      if (st)
        return st;

      /* clear the header of the new node */
      memset(newpage->get_payload(), 0, sizeof(BtreeNode));
      m_backend->get_statistics()->reset_page(page);

      /* move some of the key/rid-tuples to the new page */
      BtreeNode *nbtp = BtreeNode::from_page(newpage);
      BtreeKey *nbte = nbtp->get_key(db, 0);
      BtreeNode *obtp = BtreeNode::from_page(page);
      BtreeKey *obte = obtp->get_key(db, 0);
      ham_size_t count = obtp->get_count();

      /*
       * for databases with sequential access (this includes recno databases):
       * do not split in the middle, but at the very end of the page
       *
       * if this page is the right-most page in the index, and this key is
       * inserted at the very end, then we select the same pivot as for
       * sequential access
       */
      if (db->get_data_access_mode() & HAM_DAM_SEQUENTIAL_INSERT)
        pivot_at_end = true;
      else if (obtp->get_right() == 0) {
        cmp = m_backend->compare_keys(page, key, obtp->get_count() - 1);
        if (cmp > 0)
          pivot_at_end = true;
      }

      /*
       * the position of the pivot key depends on the previous inserts;
       * if most of them were appends then pick a pivot key at the "end" of
       * the node
       */
      if (pivot_at_end || m_hints.append_count > 30)
        pivot = count - 2;
      else if (m_hints.append_count > 10)
        pivot = (ham_u16_t)(count / 100.f * 66);
      else if (m_hints.prepend_count > 10)
        pivot = (ham_u16_t)(count / 100.f * 33);
      else if (m_hints.prepend_count > 30)
        pivot = 2;
      else
        pivot = count / 2;
      ham_assert(pivot > 0 && pivot <= count - 2);

      /* uncouple all cursors */
      st = btree_uncouple_all_cursors(page, pivot);
      if (st)
        return (st);

      /*
       * if we split a leaf, we'll insert the pivot element in the leaf
       * page, too. in internal nodes do not insert the pivot element, but
       * propagate it to the parent node only.
       */
      if (obtp->is_leaf()) {
        memcpy((char *)nbte,
               ((char *)obte) + (BtreeKey::ms_sizeof_overhead+keysize) * pivot,
               (BtreeKey::ms_sizeof_overhead+keysize) * (count - pivot));
      }
      else {
        memcpy((char *)nbte,
               ((char *)obte) + (BtreeKey::ms_sizeof_overhead+keysize)
                    * (pivot + 1),
               (BtreeKey::ms_sizeof_overhead + keysize) * (count - pivot - 1));
      }

      /*
       * store the pivot element, we'll need it later to propagate it
       * to the parent page
       */
      nbte = obtp->get_key(db, pivot);

      ham_key_t pivotkey = {0};
      ham_key_t oldkey = {0};
      oldkey.data = nbte->get_key();
      oldkey.size = nbte->get_size();
      oldkey._flags = nbte->get_flags();
      st = db->copy_key(&oldkey, &pivotkey);
      if (st)
        goto fail_dramatically;
      pivotrid = newpage->get_self();

      /* adjust the page count */
      if (obtp->is_leaf()) {
        obtp->set_count(pivot);
        nbtp->set_count(count - pivot);
      }
      else {
        obtp->set_count(pivot);
        nbtp->set_count(count - pivot - 1);
      }

      /*
       * if we're in an internal page: fix the ptr_left of the new page
       * (it points to the ptr of the pivot key)
       */
      if (!obtp->is_leaf()) {
        /* nbte still contains the pivot key */
        nbtp->set_ptr_left(nbte->get_ptr());
      }

      /* insert the new element */
      cmp = m_backend->compare_keys(page, key, pivot);
      if (cmp < -1) {
        st = (ham_status_t)cmp;
        goto fail_dramatically;
      }

      if (cmp >= 0)
        st = insert_in_leaf(newpage, key, rid);
      else
        st = insert_in_leaf(page, key, rid);
      if (st)
        goto fail_dramatically;
      /* don't overwrite cursor if insert_in_leaf is called again */
      m_cursor = 0;

      /* fix the double-linked list of pages, and mark the pages as dirty */
      if (obtp->get_right()) {
        st = db_fetch_page(&oldsib, db, obtp->get_right(), 0);
        if (st)
          goto fail_dramatically;
      }
      else
        oldsib = 0;

      nbtp->set_left(page->get_self());
      nbtp->set_right(obtp->get_right());
      obtp->set_right(newpage->get_self());
      if (oldsib) {
        BtreeNode *sbtp = BtreeNode::from_page(oldsib);
        sbtp->set_left(newpage->get_self());
        oldsib->set_dirty(true);
      }
      newpage->set_dirty(true);
      page->set_dirty(true);

      /* propagate the pivot key to the parent page */
      ham_assert(!(m_split_key.flags & HAM_KEY_USER_ALLOC));
      m_split_key = pivotkey;
      m_split_rid = pivotrid;

      if (g_BTREE_INSERT_SPLIT_HOOK)
        g_BTREE_INSERT_SPLIT_HOOK();
      return (SPLIT);

fail_dramatically:
      if (pivotkey.data)
        env->get_allocator()->free(pivotkey.data);
      ham_assert(!(pivotkey.flags & HAM_KEY_USER_ALLOC));
      return (st);
    }

    ham_status_t insert_in_leaf(Page *page, ham_key_t *key, ham_offset_t rid,
                bool force_prepend = false, bool force_append = false) {
      ham_status_t st;
      ham_size_t new_dupe_id = 0;
      Database *db = page->get_db();
      bool exists = false;
      ham_s32_t slot;

      BtreeNode *node = BtreeNode::from_page(page);
      ham_u16_t count = node->get_count();
      ham_size_t keysize = m_backend->get_keysize();

      if (node->get_count() == 0)
        slot = 0;
      else if (force_prepend)
        slot = 0;
      else if (force_append)
        slot = node->get_count();
      else {
        int cmp;

        st = m_backend->get_slot(page, key, &slot, &cmp);
        if (st)
          return (st);

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

      /*
       * in any case, uncouple the cursors and see if we must shift any
       * elements to the right
       */
      BtreeKey *bte = node->get_key(db, slot);

      if (!exists) {
        if (count > slot) {
          /* uncouple all cursors & shift any elements following [slot] */
          st = btree_uncouple_all_cursors(page, slot);
          if (st)
            return (st);

          memmove(((char *)bte) + BtreeKey::ms_sizeof_overhead + keysize, bte,
                    (BtreeKey::ms_sizeof_overhead + keysize) * (count - slot));
        }

        /* if a new key is created or inserted: initialize it with zeroes */
        memset(bte, 0, BtreeKey::ms_sizeof_overhead + keysize);
      }

      /*
       * if we're in the leaf: insert, overwrite or append the blob
       * (depends on the flags)
       */
      if (node->is_leaf()) {
        st = bte->set_record(db, m_txn, m_record,
                        m_cursor
                            ? btree_cursor_get_dupe_id(m_cursor)
                            : 0,
                        m_hints.flags, &new_dupe_id);
        if (st)
          return (st);

        m_hints.processed_leaf_page = page;
        m_hints.processed_slot = slot;
      }
      else
        bte->set_ptr(rid);

      page->set_dirty(true);
      bte->set_size(key->size);

      /* set a flag if the key is extended, and does not fit into the btree */
      if (key->size > keysize)
        bte->set_flags(bte->get_flags() | BtreeKey::KEY_IS_EXTENDED);

      /* if we have a cursor: couple it to the new key */
      if (m_cursor) {
        btree_cursor_get_parent(m_cursor)->set_to_nil(Cursor::CURSOR_BTREE);

        ham_assert(!btree_cursor_is_uncoupled(m_cursor));
        ham_assert(!btree_cursor_is_coupled(m_cursor));
        btree_cursor_set_flags(m_cursor,
                btree_cursor_get_flags(m_cursor) | BTREE_CURSOR_FLAG_COUPLED);
        btree_cursor_set_coupled_page(m_cursor, page);
        btree_cursor_set_coupled_index(m_cursor, slot);
        btree_cursor_set_dupe_id(m_cursor, new_dupe_id);
        memset(btree_cursor_get_dupe_cache(m_cursor), 0, sizeof(dupe_entry_t));
        page->add_cursor(btree_cursor_get_parent(m_cursor));
      }

      /* if we've overwritten a key: no need to continue, we're done */
      if (exists)
        return (0);

      /* we insert the extended key, if necessary */
      bte->set_key(key->data, std::min(keysize, (ham_size_t)key->size));

      /*
       * if we need an extended key, allocate a blob and store
       * the blob-id in the key
       */
      if (key->size > keysize) {
        ham_offset_t blobid;

        bte->set_key(key->data, keysize);

        ham_u8_t *data_ptr = (ham_u8_t *)key->data;
        ham_record_t rec = ham_record_t();
        rec.data = data_ptr  + (keysize - sizeof(ham_offset_t));
        rec.size = key->size - (keysize - sizeof(ham_offset_t));

        if ((st = db->get_env()->get_blob_manager()->allocate(db, &rec, 0,
                                        &blobid)))
          return (st);

        if (db->get_extkey_cache())
          db->get_extkey_cache()->insert(blobid, key->size,
                            (ham_u8_t *)key->data);

        ham_assert(blobid != 0);
        bte->set_extended_rid(db, blobid);
      }

      /* update the btree node-header */
      node->set_count(count + 1);

      return (0);
    }


    /** the current backend */
    BtreeBackend *m_backend;

    /** the current transaction */
    Transaction *m_txn;

    /** the current cursor */
    btree_cursor_t *m_cursor;

    /** the key that is inserted */
    ham_key_t *m_key;

    /** the key that is inserted */
    ham_record_t *m_record;

    /** the pivot key for SMOs and splits */
    ham_key_t m_split_key;

    /** the pivot record ID for SMOs and splits */
    ham_offset_t m_split_rid;

    /* flags of ham_find() */
    ham_u32_t m_flags;

    /** statistical hints for this operation */
    BtreeStatistics::InsertHints m_hints;
};

ham_status_t
BtreeBackend::do_insert_cursor(Transaction *txn, ham_key_t *key,
                ham_record_t *record, Cursor *cursor, ham_u32_t flags)
{
  BtreeInsertAction bia(this, txn, cursor, key, record, flags);
  return (bia.run());
}

} // namespace ham

#if 0
static void
dump_page(Database *db, ham_offset_t address) {
  Page *page;
  ham_status_t st=db_fetch_page(&page, db, address, 0);
  ham_assert(st==0);
  BtreeNode *node=BtreeNode::from_page(page);
  for (ham_size_t i = 0; i < node->get_count(); i++) {
    BtreeKey *btk = node->get_key(db, i);
    printf("%04d: %d\n", (int)i, *(int *)btk->get_key());
  }
}
#endif

