/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
 */

#include "config.h"

#include <string.h>

#include "db.h"
#include "error.h"
#include "mem.h"
#include "page.h"
#include "util.h"
#include "cursor.h"
#include "btree_index.h"
#include "btree_cursor.h"
#include "btree_stats.h"
#include "btree_node_proxy.h"
#include "page_manager.h"

namespace hamsterdb {

/*
 * btree searching
 */
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
      LocalDatabase *db = m_btree->get_db();
      LocalEnvironment *env = db->get_local_env();
      Page *page = 0;
      int slot = -1;
      BtreeNodeProxy *node = 0;

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
        page = env->get_page_manager()->fetch_page(db, hints.leaf_page_addr,
                                            PageManager::kOnlyFromCache
                                            | PageManager::kReadOnly);
        if (page) {
          node = m_btree->get_node_from_page(page);
          ham_assert(node->is_leaf());

          slot = m_btree->find_leaf(page, m_key, hints.flags);

          /*
           * if we didn't hit a match OR a match at either edge, FAIL.
           * A match at one of the edges is very risky, as this can also
           * signal a match far away from the current node, so we need
           * the full tree traversal then.
           */
          if (slot <= 0 || slot >= (int)node->get_count() - 1)
            slot = -1;

          /*
           * else: we landed in the middle of the node, so we don't need to
           * traverse the entire tree now.
           */
        }
      }

      if (slot == -1) {
        /* get the address of the root page */
        if (!m_btree->get_root_address())
          return (HAM_KEY_NOT_FOUND);

        /* load the root page */
        page = env->get_page_manager()->fetch_page(db,
                        m_btree->get_root_address(),
                        PageManager::kReadOnly);

        /* now traverse the root to the leaf nodes, till we find a leaf */
        node = m_btree->get_node_from_page(page);
        if (!node->is_leaf()) {
          /* signal 'don't care' when we have multiple pages; we resolve
           * this once we've got a hit further down */
          if (hints.flags & (HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH))
            hints.flags |= (HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH);

          for (;;) {
            page = m_btree->find_child(page, m_key);
            if (!page) {
              stats->find_failed();
              return (HAM_KEY_NOT_FOUND);
            }

            node = m_btree->get_node_from_page(page);
            if (node->is_leaf())
              break;
          }
        }

        /* check the leaf page for the key */
        const ham_u32_t mask = ~(HAM_OVERWRITE
                                  | HAM_HINT_APPEND
                                  | HAM_HINT_PREPEND);
        if (m_flags == 0 || (m_flags & mask) == 0)
          slot = node->find_exact(m_key);
        else
          slot = m_btree->find_leaf(page, m_key, hints.flags);
        if (slot < -1) {
          stats->find_failed();
          return (HAM_KEY_NOT_FOUND);
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
       * will be hard pressed to return a 'key not found' signal (slot==-1),
       * instead delivering the nearest LT or GT match; all we need to
       * do now is ensure we've got the right one and if not,
       * shift by one.
       */
      if (slot >= 0) {
        if ((ham_key_get_intflags(m_key) & BtreeKey::kApproximate)
            && (hints.original_flags & (HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH))
                != (HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH)) {
          if ((ham_key_get_intflags(m_key) & BtreeKey::kGreater)
              && (hints.original_flags & HAM_FIND_LT_MATCH)) {
            /* if the index-1 is still in the page, just decrement the index */
            if (slot > 0)
              slot--;
            else {
              /* otherwise load the left sibling page */
              if (!node->get_left()) {
                stats->find_failed();
                return (HAM_KEY_NOT_FOUND);
              }

              page = env->get_page_manager()->fetch_page(db, node->get_left(),
                                                    PageManager::kReadOnly);
              node = m_btree->get_node_from_page(page);
              slot = node->get_count() - 1;
            }
            ham_key_set_intflags(m_key, (ham_key_get_intflags(m_key)
                        & ~BtreeKey::kApproximate) | BtreeKey::kLower);
          }
          else if ((ham_key_get_intflags(m_key) & BtreeKey::kLower)
              && (hints.original_flags & HAM_FIND_GT_MATCH)) {
            /* if the index+1 is still in the page, just increment the index */
            if (slot + 1 < (int)node->get_count())
              slot++;
            else {
              /* otherwise load the right sibling page */
              if (!node->get_right()) {
                stats->find_failed();
                return (HAM_KEY_NOT_FOUND);
              }

              page = env->get_page_manager()->fetch_page(db, node->get_right(),
                                                PageManager::kReadOnly);
              node = m_btree->get_node_from_page(page);
              slot = 0;
            }
            ham_key_set_intflags(m_key, (ham_key_get_intflags(m_key)
                        & ~BtreeKey::kApproximate) | BtreeKey::kGreater);
          }
        }
        else if (!(ham_key_get_intflags(m_key) & BtreeKey::kApproximate)
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
           * flagged with the kApproximate flag.
           */
          if (hints.original_flags & HAM_FIND_LT_MATCH) {
            /* if the index-1 is still in the page, just decrement the index */
            if (slot > 0) {
              slot--;
              ham_key_set_intflags(m_key, (ham_key_get_intflags(m_key)
                          & ~BtreeKey::kApproximate) | BtreeKey::kLower);
            }
            else {
              /* otherwise load the left sibling page */
              if (!node->get_left()) {
                /* when an error is otherwise unavoidable, see if
                 * we have an escape route through GT? */
                if (hints.original_flags & HAM_FIND_GT_MATCH) {
                  /* if the index+1 is still in the page, just increment it */
                  if (slot + 1 < (int)node->get_count())
                    slot++;
                  else {
                    /* otherwise load the right sibling page */
                    if (!node->get_right()) {
                      stats->find_failed();
                      return (HAM_KEY_NOT_FOUND);
                    }

                    page = env->get_page_manager()->fetch_page(db,
                                    node->get_right(),
                                    PageManager::kReadOnly);
                    node = m_btree->get_node_from_page(page);
                    slot = 0;
                  }
                  ham_key_set_intflags(m_key, (ham_key_get_intflags(m_key) &
                              ~BtreeKey::kApproximate) | BtreeKey::kGreater);
                }
                else {
                  stats->find_failed();
                  return (HAM_KEY_NOT_FOUND);
                }
              }
              else {
                page = env->get_page_manager()->fetch_page(db,
                                    node->get_left(),
                                    PageManager::kReadOnly);
                node = m_btree->get_node_from_page(page);
                slot = node->get_count() - 1;

                ham_key_set_intflags(m_key, (ham_key_get_intflags(m_key)
                              & ~BtreeKey::kApproximate) | BtreeKey::kLower);
              }
            }
          }
          else if (hints.original_flags & HAM_FIND_GT_MATCH) {
            /* if index+1 is still in the page, just increment it */
            if (slot + 1 < (int)node->get_count())
              slot++;
            else {
              /* otherwise load the right sibling page */
              if (!node->get_right()) {
                stats->find_failed();
                return (HAM_KEY_NOT_FOUND);
              }

              page = env->get_page_manager()->fetch_page(db, node->get_right(),
                                                PageManager::kReadOnly);
              node = m_btree->get_node_from_page(page);
              slot = 0;
            }
            ham_key_set_intflags(m_key, (ham_key_get_intflags(m_key)
                                & ~BtreeKey::kApproximate)
                                | BtreeKey::kGreater);
          }
        }
      }

      if (slot < 0) {
        stats->find_failed();
        return (HAM_KEY_NOT_FOUND);
      }

      ham_assert(node->is_leaf());

      /* set the cursor-position to this key */
      if (m_cursor) {
        ham_assert(m_cursor->get_state() == BtreeCursor::kStateNil);
        m_cursor->couple_to_page(page, slot, 0);
      }

      /* no need to load the key if we have an exact match, or if KEY_DONT_LOAD
       * is set: */
      if (m_key && (ham_key_get_intflags(m_key) & BtreeKey::kApproximate)
          && !(m_flags & Cursor::kSyncDontLoadKey)) {
        ByteArray *arena = (m_txn == 0
                        || (m_txn->get_flags() & HAM_TXN_TEMPORARY))
              ? &db->get_key_arena()
              : &m_txn->get_key_arena();

        node->get_key(slot, arena, m_key);
      }

      if (m_record) {
        ByteArray *arena = (m_txn == 0
                        || (m_txn->get_flags() & HAM_TXN_TEMPORARY))
               ? &db->get_record_arena()
               : &m_txn->get_record_arena();

        node->get_record(slot, arena, m_record, m_flags);
      }

      return (0);
    }

  private:
    // the current btree
    BtreeIndex *m_btree;

    // the current transaction
    Transaction *m_txn;

    // the current cursor
    BtreeCursor *m_cursor;

    // the key that is retrieved
    ham_key_t *m_key;

    // the record that is retrieved
    ham_record_t *m_record;

    // flags of ham_db_find()
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

