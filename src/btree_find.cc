/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 *
 * btree searching
 *
 */

#include "config.h"

#include <string.h>

#include "btree.h"
#include "cursor.h"
#include "btree_cursor.h"
#include "db.h"
#include "env.h"
#include "error.h"
#include "btree_key.h"
#include "mem.h"
#include "page.h"
#include "btree_stats.h"
#include "util.h"
#include "btree_node.h"

namespace hamsterdb {

class BtreeFindAction
{
  public:
    BtreeFindAction(BtreeIndex *btree, Transaction *txn, Cursor *cursor,
        ham_key_t *key, ham_record_t *record, ham_u32_t flags)
      : m_btree(btree), m_txn(txn), m_cursor(0), m_key(key),
        m_record(record), m_flags(flags) {
      if (cursor && cursor->get_btree_cursor()->get_parent())
        m_cursor = cursor->get_btree_cursor();
    }

    ham_status_t run() {
      ham_status_t st;
      Page *page = 0;
      ham_s32_t idx = -1;
      LocalDatabase *db = m_btree->get_db();
      BtreeNode *node;
      BtreeStatistics *stats = m_btree->get_statistics();
      BtreeStatistics::FindHints hints = stats->get_find_hints(m_flags);

      if (hints.try_fast_track) {
        /*
         * see if we get a sure hit within this btree leaf; if not, revert to
         * regular scan
         *
         * As this is a speed-improvement hint re-using recent material, the
         * page should still sit in the cache, or we're using old info, which
         * should be discarded.
         */
        st = db->fetch_page(&page, hints.leaf_page_addr, true);
        if (st == 0 && page) {
          node = BtreeNode::from_page(page);
          ham_assert(node->is_leaf());

          idx = m_btree->find_leaf(page, m_key, hints.flags);

          /*
           * if we didn't hit a match OR a match at either edge, FAIL.
           * A match at one of the edges is very risky, as this can also
           * signal a match far away from the current node, so we need
           * the full tree traversal then.
           */
          if (idx <= 0 || idx >= node->get_count() - 1)
            idx = -1;

          /*
           * else: we landed in the middle of the node, so we don't need to
           * traverse the entire tree now.
           */
        }
      }

      if (idx == -1) {
        /* get the address of the root page */
        if (!m_btree->get_rootpage())
          return (HAM_KEY_NOT_FOUND);

        /* load the root page */
        st = db->fetch_page(&page, m_btree->get_rootpage());
        if (st)
          return (st);

        /* now traverse the root to the leaf nodes, till we find a leaf */
        node = BtreeNode::from_page(page);
        if (!node->is_leaf()) {
          /* signal 'don't care' when we have multiple pages; we resolve
           * this once we've got a hit further down */
          if (hints.flags & (HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH))
            hints.flags |= (HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH);

          for (;;) {
            st = m_btree->find_internal(page, m_key, &page);
            if (!page) {
              stats->find_failed();
              return (st ? st : HAM_KEY_NOT_FOUND);
            }

            node = BtreeNode::from_page(page);
            if (node->is_leaf())
              break;
          }
        }

        /* check the leaf page for the key */
        idx = m_btree->find_leaf(page, m_key, hints.flags);
        if (idx < -1) {
          stats->find_failed();
          return ((ham_status_t)idx);
        }
      } /* end of regular search */

      /*
       * When we are performing an approximate match, the worst case
       * scenario is where we've picked the wrong side of the fence
       * while sitting at a page/node boundary: that's what this
       * next piece of code resolves:
       *
       * essentially it moves one record forwards or backward when
       * the flags tell us this is mandatory and we're not yet in the proper
       * position yet.
       *
       * The whole trick works, because the code above detects when
       * we need to traverse a multi-page btree -- where this worst-case
       * scenario can happen -- and adjusted the flags to accept
       * both LT and GT approximate matches so that find_leaf()
       * will be hard pressed to return a 'key not found' signal (idx==-1),
       * instead delivering the nearest LT or GT match; all we need to
       * do now is ensure we've got the right one and if not,
       * shift by one.
       */
      if (idx >= 0) {
        if ((ham_key_get_intflags(m_key) & BtreeKey::KEY_IS_APPROXIMATE)
            && (hints.original_flags & (HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH))
                != (HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH)) {
          if ((ham_key_get_intflags(m_key) & BtreeKey::KEY_IS_GT)
              && (hints.original_flags & HAM_FIND_LT_MATCH)) {
            /* if the index-1 is still in the page, just decrement the index */
            if (idx > 0)
              idx--;
            else {
              /* otherwise load the left sibling page */
              if (!node->get_left()) {
                stats->find_failed();
                return (HAM_KEY_NOT_FOUND);
              }

              st = db->fetch_page(&page, node->get_left());
              if (st)
                return (st);
              node = BtreeNode::from_page(page);
              idx = node->get_count() - 1;
            }
            ham_key_set_intflags(m_key, (ham_key_get_intflags(m_key)
                        & ~BtreeKey::KEY_IS_APPROXIMATE) | BtreeKey::KEY_IS_LT);
          }
          else if ((ham_key_get_intflags(m_key) & BtreeKey::KEY_IS_LT)
              && (hints.original_flags & HAM_FIND_GT_MATCH)) {
            /* if the index+1 is still in the page, just increment the index */
            if (idx + 1 < node->get_count())
              idx++;
            else {
              /* otherwise load the right sibling page */
              if (!node->get_right()) {
                stats->find_failed();
                return (HAM_KEY_NOT_FOUND);
              }

              st = db->fetch_page(&page, node->get_right());
              if (st)
                return (st);
              node = BtreeNode::from_page(page);
              idx = 0;
            }
            ham_key_set_intflags(m_key, (ham_key_get_intflags(m_key)
                        & ~BtreeKey::KEY_IS_APPROXIMATE) | BtreeKey::KEY_IS_GT);
          }
        }
        else if (!(ham_key_get_intflags(m_key) & BtreeKey::KEY_IS_APPROXIMATE)
            && !(hints.original_flags & HAM_FIND_EXACT_MATCH)
            && (hints.original_flags != 0)) {
          /*
           * 'true GT/LT' has been added @ 2009/07/18 to complete
           * the EQ/LEQ/GEQ/LT/GT functionality;
           *
           * 'true LT/GT' is simply an extension upon the already existing
           * LEQ/GEQ logic just above; all we do here is move one record
           * up/down as it just happens that we get an exact ('equal')
           * match here.
           *
           * The fact that the LT/GT constants share their bits with the
           * LEQ/GEQ flags so that LEQ==(LT|EXACT) and GEQ==(GT|EXACT)
           * ensures that we can restrict our work to a simple adjustment
           * right here; everything else has already been taken of by the
           * LEQ/GEQ logic in the section above when the key has been
           * flagged with the KEY_IS_APPROXIMATE flag.
           */
          if (hints.original_flags & HAM_FIND_LT_MATCH) {
            /* if the index-1 is still in the page, just decrement the index */
            if (idx > 0) {
              idx--;
              ham_key_set_intflags(m_key, (ham_key_get_intflags(m_key)
                          & ~BtreeKey::KEY_IS_APPROXIMATE) | BtreeKey::KEY_IS_LT);
            }
            else {
              /* otherwise load the left sibling page */
              if (!node->get_left()) {
                /* when an error is otherwise unavoidable, see if
                 * we have an escape route through GT? */
                if (hints.original_flags & HAM_FIND_GT_MATCH) {
                  /* if the index+1 is still in the page, just increment it */
                  if (idx + 1 < node->get_count())
                    idx++;
                  else {
                    /* otherwise load the right sibling page */
                    if (!node->get_right()) {
                      stats->find_failed();
                      return (HAM_KEY_NOT_FOUND);
                    }

                    st = db->fetch_page(&page, node->get_right());
                    if (st)
                      return (st);
                    node = BtreeNode::from_page(page);
                    idx = 0;
                  }
                  ham_key_set_intflags(m_key, (ham_key_get_intflags(m_key) &
                                      ~BtreeKey::KEY_IS_APPROXIMATE) | BtreeKey::KEY_IS_GT);
                }
                else {
                  stats->find_failed();
                  return (HAM_KEY_NOT_FOUND);
                }
              }
              else {
                st = db->fetch_page(&page, node->get_left());
                if (st)
                  return (st);
                node = BtreeNode::from_page(page);
                idx = node->get_count() - 1;

                ham_key_set_intflags(m_key, (ham_key_get_intflags(m_key)
                                  & ~BtreeKey::KEY_IS_APPROXIMATE) | BtreeKey::KEY_IS_LT);
              }
            }
          }
          else if (hints.original_flags&HAM_FIND_GT_MATCH) {
            /* if the index+1 is still in the page, just increment the it */
            if (idx + 1 < node->get_count())
              idx++;
            else {
              /* otherwise load the right sibling page */
              if (!node->get_right()) {
                stats->find_failed();
                return (HAM_KEY_NOT_FOUND);
              }

              st = db->fetch_page(&page, node->get_right());
              if (st)
                return (st);
              node = BtreeNode::from_page(page);
              idx = 0;
            }
            ham_key_set_intflags(m_key, (ham_key_get_intflags(m_key)
                                & ~BtreeKey::KEY_IS_APPROXIMATE)
                                | BtreeKey::KEY_IS_GT);
          }
        }
      }

      if (idx < 0) {
        stats->find_failed();
        return (HAM_KEY_NOT_FOUND);
      }

      /* load the entry, and store record ID and key flags */
      BtreeKey *entry = node->get_key(db, idx);

      /* set the cursor-position to this key */
      if (m_cursor) {
        ham_assert(!m_cursor->is_uncoupled());
        ham_assert(!m_cursor->is_coupled());
        page->add_cursor(m_cursor->get_parent());
        m_cursor->couple_to(page, idx);
      }

      /*
       * during read_key() and read_record() new pages might be needed,
       * and the page at which we're pointing could be moved out of memory;
       * that would mean that the cursor would be uncoupled, and we're losing
       * the 'entry'-pointer. therefore we 'lock' the page by incrementing
       * the reference counter
       */
      ham_assert(node->is_leaf());

      /* no need to load the key if we have an exact match, or if KEY_DONT_LOAD
       * is set: */
      if (m_key && (ham_key_get_intflags(m_key) & BtreeKey::KEY_IS_APPROXIMATE)
          && !(m_flags & Cursor::CURSOR_SYNC_DONT_LOAD_KEY)) {
        st = m_btree->read_key(m_txn, entry, m_key);
        if (st)
          return (st);
      }

      if (m_record) {
        m_record->_intflags = entry->get_flags();
        m_record->_rid = entry->get_ptr();
        st = m_btree->read_record(m_txn, m_record,
                        entry->get_rawptr(), m_flags);
        if (st)
          return (st);
      }

      // TODO merge these two calls
      stats->find_failed();
      return (0);
    }

  private:
    /** the current btree */
    BtreeIndex *m_btree;

    /** the current transaction */
    Transaction *m_txn;

    /** the current cursor */
    BtreeCursor *m_cursor;

    /** the key that is retrieved */
    ham_key_t *m_key;

    /** the key that is retrieved */
    ham_record_t *m_record;

    /* flags of ham_db_find() */
    ham_u32_t m_flags;
};

ham_status_t
BtreeIndex::find(Transaction *txn, Cursor *cursor, ham_key_t *key,
                ham_record_t *record, ham_u32_t flags)
{
  BtreeFindAction bfa(this, txn, cursor, key, record, flags);
  return (bfa.run());
}

} // namespace hamsterdb

