/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 * Original author: Ger Hobbelt
 */

#include "config.h"

#include <string.h>
#include <stdio.h>

#include "statistics.h"
#include "btree.h"
#include "btree_cursor.h"
#include "cursor.h"
#include "cache.h"
#include "db.h"
#include "endianswap.h"
#include "env.h"
#include "error.h"
#include "freelist_statistics.h"
#include "mem.h"
#include "page.h"
#include "btree_stats.h"
#include "util.h"
#include "btree_node.h"

namespace ham {

/*
 *  TODO statistics gatherer/hinter:
 *
 * keep track of two areas' 'utilization':
 *
 * 1) for fast/uberfast mode, keep track of the LAST free zone, i.e.
 * the free zone at the end;
 * ONLY move the start marker for that BACKWARDS when we get a freeing
 * op just before it OR when we specifically scan backwards to find the
 * adjusted start after lots of fragmented delete ops and we're nog in
 * turbo-fast mode: this would save space.
 *
 * 2) keep track of the marker where the FIRST free chunk just was,
 * i.e. before which' point there definitely is NO free space. Use this
 * marker as the start for a free-space-search when in
 * space-saving/classic mode; use the other 'start of free space at end
 * of the page' marker as the starting point for (uber-)fast searches.
 *
 * 'utilization': keep track of the number of free chunks and allocated
 * chunks in the middle zone ~ the zone between FIRST and LST marker:
 * the ratio is a measure of the chance we expect to have when searching
 * this zone for a free spot - by not coding/designing to cover a
 * specific pathological case (add+delete @ start & end of store and
 * repeating this cycle over and over causing the DB to 'jump all over
 * the place' in classic mode; half of the free slot searches would be
 * 'full scans' of the freelist then. Anyway, we do not wish to code for
 * this specific pathological case, as such code will certainly
 * introduce another pathological case instead, which should be fixed,
 * resulting in expanded code and yet another pathological case fit for
 * the new code situation, etc.etc. ad nauseam. Instead, we use
 * statistical measures to express an estimate, i.e. the chance that we
 * might need to scan a large portion of the freelist when we
 * run in classic spacesaving insert mode, and apply that statistical
 * data to the hinter, using the current mode.
 *
 * -- YES, that also means we are able to switch freelist scanning
 * mode, and thus speed- versus storage consumption hints, on a per-insert basis: a single
 * database can mix slow but spacesaving record inserts for those times / tables when we do not need the extra oemph, while other inserts can
 * be told (using the flags in the API calls) to act optimized for
 *  - none (classic) --> ~ storage space saving
 *  - storage space saving
 *  - insertion speed By using 2 bits: 1 for speed and one for
 * uber/turbo or regular, we can have 3 or 4 modes, where a 'speedy
 * space saving' mode might imply we're using those freelist stats to
 * decide whether to start the scan at the end or near the start of the
 * freelist, in order to arrive at a 'reasonable space utilization'
 * while keeping up the speed, at least when determined over multiple
 * inserts.
 *
 * And mode 4 can be used to enforce full scan or something like that:
 * this can be used to improve the statistics as those are not persisted
 * on disc.
 *
 * The stats gatherer is delivering the most oomph, especially for tiny
 * keys and records, where Boyer-Moore is not really effective (or even
 * counter productive); gathering stats about the free slots and
 * occupied slots helps us speeding up multiple inserts, even while the
 * data is only alive for 1 run-time open-close period of time.
 *
 * Make the cache counter code indirect, so we can switch and test
 * various cache aging systems quickly.
 *
 * When loading a freelist page, we can use sampling to get an idea of
 * where the LAST zone starts and ends (2 bsearches: one assuming the
 * freelist is sorted in descending order --> last 1 bit, one assuming the freelist is sorted in ascending
 * order (now that we 'know' the last free bit, this will scan the range 0..last-1-bit to
 * find the first 1 bit in there.
 *
 * Making sure we limit our # of samples irrespective of freelist page
 * size, so we can use the same stats gather for classic and modern
 * modes.
 *
 * perform such sampling using semi-random intervals: prevent being
 * sensitive to a particular pathologic case this way.
 */


#define rescale_2(val)                            \
    val = ((val + 2 - 1) >> 1) /* make sure non-zero numbers remain non-zero: roundup(x) */

BtreeStatistics::BtreeStatistics(Database *db)
  : m_db(db), m_lower_arena(db->get_env()->get_allocator()),
    m_upper_arena(db->get_env()->get_allocator())
{
}

void
BtreeStatistics::update_failed_oob(int op, bool try_fast_track)
{
  OperationStatistics *opstats = get_op_data(op);

  ham_assert(op == HAM_OPERATION_STATS_FIND || op == HAM_OPERATION_STATS_ERASE);

  opstats->btree_last_page_sq_hits = 0; /* reset */
}

void
BtreeStatistics::update_failed(int op, bool try_fast_track)
{
  OperationStatistics *opstats = get_op_data(op);

  ham_assert(op == HAM_OPERATION_STATS_FIND
              || op == HAM_OPERATION_STATS_INSERT
              || op == HAM_OPERATION_STATS_ERASE);

  //opstats->btree_last_page_addr = 0; -- keep page from previous match around!
  opstats->btree_last_page_sq_hits = 0; /* reset */

  if (try_fast_track) {
    opstats->btree_hinting_fail_count++;
    opstats->btree_hinting_count++;
  }
}

void
BtreeStatistics::update_succeeded(int op, Page *page, bool try_fast_track)
{
  OperationStatistics *opstats = get_op_data(op);

  ham_assert(op == HAM_OPERATION_STATS_FIND
              || op == HAM_OPERATION_STATS_INSERT
              || op == HAM_OPERATION_STATS_ERASE);
  ham_assert(page);

  /* when we got a hint, account for its success/failure */
  if (try_fast_track) {
    if (opstats->btree_last_page_addr != page->get_self())
      opstats->btree_hinting_fail_count++;
    opstats->btree_hinting_count++;
  }

  if (opstats->btree_last_page_addr
        && opstats->btree_last_page_addr == page->get_self())
    opstats->btree_last_page_sq_hits++;
  else
    opstats->btree_last_page_addr = page->get_self();
}

/*
 * when the last hit leaf node is split or shrunk, blow it away for all
 * operations!
 *
 * Also blow away a page when a transaction aborts which has modified this
 * page. We'd rather reconstruct our critical statistics then carry the
 * wrong bounds, etc. around.
 *
 * This is done to prevent the hinter from hinting/pointing at an (by now)
 * INVALID btree node later on!
 */
void
BtreeStatistics::reset_page(Page *page, bool split)
{
  for (int i = 0; i <= 2; i++) {
    OperationStatistics *opstats = get_op_data(i);

    ham_assert(i == HAM_OPERATION_STATS_FIND
                || i == HAM_OPERATION_STATS_INSERT
                || i == HAM_OPERATION_STATS_ERASE);

    if (opstats->btree_last_page_addr == page->get_self()) {
      opstats->btree_last_page_addr = 0;
      opstats->btree_last_page_sq_hits = 0;
    }
  }

  if (m_perf_data.lower_bound_page_address == page->get_self()) {
    m_perf_data.lower_bound = ham_key_t();
    m_perf_data.lower_bound_index = 0;
    m_perf_data.lower_bound_page_address = 0;
    m_perf_data.lower_bound_set = false;
  }

  if (m_perf_data.upper_bound_page_address == page->get_self()) {
    m_perf_data.upper_bound = ham_key_t();
    m_perf_data.upper_bound_index = 0;
    m_perf_data.upper_bound_page_address = 0;
    m_perf_data.upper_bound_set = false;
  }
}

void
BtreeStatistics::update_any_bound(int op, Page *page, ham_key_t *key,
                ham_u32_t find_flags, ham_s32_t slot)
{
  ham_status_t st;
  BtreeNode *node = BtreeNode::from_page(page);

  /* reset both flags - they will be set if lower_bound or
   * upper_bound are modified */
  m_perf_data.last_insert_was_prepend = 0;
  m_perf_data.last_insert_was_append = 0;

  ham_assert(node->is_leaf());
  if (!node->get_left()) {
    /* this is the leaf page which carries the lower bound key */
    ham_assert(node->get_count() == 0
                ? !node->get_right()
                : 1);
    if (node->get_count() == 0) {
      /* range is empty
       *
       * do not set the lower/upper boundary; otherwise we may trigger
       * a key comparison with an empty key, and the comparison function
       * could not be fit to handle this.
       */
      if (m_perf_data.lower_bound_index != 1
          || m_perf_data.upper_bound_index != 0) {
        /* only set when not done already */
        m_perf_data.lower_bound = ham_key_t();
        m_perf_data.upper_bound = ham_key_t();
        m_perf_data.lower_bound_index = 1; /* impossible value for lower bound*/
        m_perf_data.upper_bound_index = 0;
        m_perf_data.lower_bound_page_address = page->get_self();
        m_perf_data.upper_bound_page_address = 0; /* page->get_self(); */
        m_perf_data.lower_bound_set = true;
        /* cannot be TRUE or subsequent updates for single record carrying
         * tables may fail */
        m_perf_data.upper_bound_set = false;
        ham_assert(m_perf_data.lower_bound_page_address != 0);
      }
    }
    else {
      /*
       * lower bound key is always located at index [0]
       * update our key info when either our current data is undefined
       * (startup condition) or the first key was edited in some way
       * (slot == 0). This 'copy anyway' approach saves us one costly key
       * comparison.
       */
      if (m_perf_data.lower_bound_index != 0
           || m_perf_data.lower_bound_page_address != page->get_self()
           || slot == 0) {
        /* only set when not done already */
        m_perf_data.lower_bound_set = true;
        m_perf_data.lower_bound_index = 0;
        m_perf_data.lower_bound_page_address = page->get_self();
        m_perf_data.lower_bound = ham_key_t();

        btree_key_t *btk = node->get_key(m_db,
                    m_perf_data.lower_bound_index);
        m_lower_arena.resize(key_get_size(btk));
        m_perf_data.lower_bound.data = m_lower_arena.get_ptr();
        m_perf_data.lower_bound.flags |= HAM_KEY_USER_ALLOC;

        st = ((BtreeBackend *)m_db->get_backend())->copy_key(btk,
                    &m_perf_data.lower_bound);
        if (st) {
          /* panic! is case of failure, just drop the lower bound
           * entirely. */
          m_perf_data.lower_bound = ham_key_t();
          m_perf_data.lower_bound_index = 0;
          m_perf_data.lower_bound_page_address = 0;
          m_perf_data.lower_bound_set = false;
        }
        else {
          ham_assert(m_perf_data.lower_bound.data == NULL ?
                    m_perf_data.lower_bound.size == 0 :
                    m_perf_data.lower_bound.size > 0);
          ham_assert(m_perf_data.lower_bound_page_address != 0);
        }
        if (op == HAM_OPERATION_STATS_INSERT)
           m_perf_data.last_insert_was_prepend = 1;
       }
    }
  }

  if (!node->get_right()) {
    /* this is the leaf page which carries the upper bound key */
    ham_assert(node->get_count() == 0
               ? !node->get_left()
               : 1);
    if (node->get_count() != 0) {
      /*
       * range is non-empty; the other case has already been handled
       * above upper bound key is always located at index [size-1]
       * update our key info when either our current data is
       * undefined (startup condition) or the last key was edited in
       * some way (slot == size-1). This 'copy anyway' approach
       * saves us one costly key comparison.
       */
      if (m_perf_data.upper_bound_index
            != (ham_u32_t)node->get_count() - 1
          || m_perf_data.upper_bound_page_address != page->get_self()
          || (ham_u16_t)slot == node->get_count() - 1) {
        /* only set when not done already */
        m_perf_data.upper_bound_set = true;
        m_perf_data.upper_bound_index = node->get_count() - 1;
        m_perf_data.upper_bound_page_address = page->get_self();
        m_perf_data.upper_bound = ham_key_t();

        btree_key_t *btk = node->get_key(m_db,
                    m_perf_data.upper_bound_index);
        m_upper_arena.resize(key_get_size(btk));
        m_perf_data.upper_bound.data = m_upper_arena.get_ptr();
        m_perf_data.upper_bound.flags |= HAM_KEY_USER_ALLOC;

        st = ((BtreeBackend *)m_db->get_backend())->copy_key(btk,
                    &m_perf_data.upper_bound);
        if (st) {
          /* panic! is case of failure, just drop the upper bound entirely. */
          m_perf_data.upper_bound = ham_key_t();
          m_perf_data.upper_bound_index = 0;
          m_perf_data.upper_bound_page_address = 0;
          m_perf_data.upper_bound_set = false;
        }
        if (op == HAM_OPERATION_STATS_INSERT)
           m_perf_data.last_insert_was_append = 1;
      }
    }
  }
}

 /*
  * NOTE:
  *
  * The current statistics collectors recognize scenarios where insert &
  * delete mix with find, as both insert and delete (pardon, erase)
  * can split/merge/rebalance the B-tree and thus completely INVALIDATE
  * btree leaf nodes, the address of which is kept in DB-wide statistics
  * storage. The current way of doing things is to keep the statistics simple,
  * i.e. btree leaf node pointers are nuked when an insert operation splits
  * them or an erase operation merges or erases such pages. I don't think
  * it's really useful to have complex leaf-node tracking in here to improve
  * hinting in such mixed use cases.
  */
BtreeStatistics::FindHints
BtreeStatistics::get_find_hints(ham_key_t *key, ham_u32_t flags)
{
  BtreeStatistics::FindHints hints = {flags, flags, 0, false, false};
  OperationStatistics *opstats = get_op_data(HAM_OPERATION_STATS_FIND);

  /*
   * we can only give some possibly helpful hints, when we
   * know the tree leaf node (page) we can direct find() to...
   */
  if (opstats->btree_last_page_addr != 0) {
    /*
     * When we're in SEQUENTIAL mode, we'll advise to check the previously
     * used leaf. When the FAIL ratio increases above a certain number, we
     * STOP hinting as we clearly hinted WRONG before. We'll try again later,
     * though.
     *
     * Note also that we 'age' the HINT FAIL info collected during FIND
     * statistics gathering, so that things will be attempted again after while.
     */
    if (flags & (HAM_HINT_APPEND | HAM_HINT_PREPEND)) {
      /* find specific: APPEND / PREPEND --> SEQUENTIAL */
      flags &= ~(HAM_HINT_APPEND | HAM_HINT_PREPEND);
      flags |= HAM_HINT_SEQUENTIAL;
    }

    if ((flags & HAM_HINTS_MASK) == 0) {
      /* no local preference specified; go with the DB-wide DAM config */
      switch (m_db->get_data_access_mode()) {
        default:
          break;

        case HAM_DAM_SEQUENTIAL_INSERT:
          flags = HAM_HINT_SEQUENTIAL;
          break;
      }
    }

    unsigned hits_required = 3;

    switch (flags & HAM_HINTS_MASK) {
      default:
      case HAM_HINT_RANDOM_ACCESS:
        /* do not provide any hints for the fast track */
        break;

      case HAM_HINT_SEQUENTIAL | HAM_HINT_UBER_FAST_ACCESS:
        hits_required = 1;
        /* fall through */

      case HAM_HINT_SEQUENTIAL: {
        /*
         * when we have more than 4 hits on the same page already, we'll
         * assume this one will end up there as well. As this counter will
         * reset itself on the first FAIL, there's no harm in acting this
         * quick. In pathological cases, the worst what can happen is that
         * in 20% of cases there will be performed an extra check on a cached
         * btree leaf node, which is still minimal overhead then.
         */
        if (opstats->btree_last_page_sq_hits >= hits_required) {
          hints.leaf_page_addr = opstats->btree_last_page_addr;
          hints.try_fast_track = true;
          break;
        }

        /*
         * we assume this request is located near the previous request, so
         * we check if there's anything in the statistics that can help out.
         *
         * Note #1: since the hinting counts are 'aged' down to a value of
         * 0..1K (with 2K peak), we don't need to use a 64-bit integer for
         * the ratio calculation here.
         * 
         * Note #2: the ratio is only 'trustworthy' when the base count is
         * about 4 or higher.  This is because the aging rounds up while
         * scaling down, which means one single FAIL can get you a ratio as
         * large as 50% when total count is 1 as well, due to either startup
         * or aging rescale; without this minimum size check, the ratio +
         * aging would effectively stop the hinter from working after either
         * an aging step or when a few FAILs happen during the initial few
         * FIND operations (startup condition).
         *
         * EDIT: the above bit about the hinter stopping due to too much FAIL
         * at start or after rescale does NOT apply any more as the hinter
         * now also includes checks which trigger when a (small) series of
         * hits on the same page are found, which acts as a restarter for
         * this as well.
         */
         ham_u32_t ratio = opstats->btree_hinting_fail_count;

         ratio = ratio * 1000 / (1 + opstats->btree_hinting_count);
         if (ratio < 200) {
           hints.leaf_page_addr = opstats->btree_last_page_addr;
           hints.try_fast_track = true;
         }
         break;
       }
     }
  }

  /* age the hinting statistics to avoid integer overflows and to reduce
   * the influence of older hinting results on newer output */
  opstats->aging_tracker++;
  if (opstats->aging_tracker >= 1000) {
    rescale_2(opstats->btree_hinting_fail_count);
    rescale_2(opstats->btree_hinting_count);

    opstats->aging_tracker = 0;
  }

  /*
   * and lastly check whether the key is out of range: when the adequate
   * LE/GE search flags are not set in such a case, we can quickly decide
   * right here that a match won't be forthcoming: KEY_NOT_FOUND will be your
   * thanks.
   */
  ham_assert(!(key->_flags & KEY_IS_EXTENDED));
  key->_flags &= ~KEY_IS_EXTENDED;

  if (!dam_is_set(flags, HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH)
      && m_perf_data.lower_bound_page_address
        != m_perf_data.upper_bound_page_address
      && (hints.try_fast_track
        ? (m_perf_data.lower_bound_page_address == hints.leaf_page_addr
            || m_perf_data.upper_bound_page_address == hints.leaf_page_addr)
        : true)) {
    if (m_perf_data.lower_bound_set && !dam_is_set(flags, HAM_FIND_GT_MATCH)) {
      if (m_perf_data.lower_bound_index == 1) {
        /*
         * impossible index: this is a marker to signal the table
         * is completely empty
         */
        hints.key_is_out_of_bounds = true;
        hints.try_fast_track = true;
      }
      else {
        ham_assert(m_perf_data.lower_bound_index == 0);
        ham_assert(m_perf_data.lower_bound.data == NULL ?
                    m_perf_data.lower_bound.size == 0 :
                    m_perf_data.lower_bound.size > 0);
        ham_assert(m_perf_data.lower_bound_page_address != 0);
        int cmp = m_db->compare_keys(key, &m_perf_data.lower_bound);
        if (cmp < 0) {
          hints.key_is_out_of_bounds = true;
          hints.try_fast_track = true;
        }
      }
    }

    if (m_perf_data.upper_bound_set && !dam_is_set(flags, HAM_FIND_LT_MATCH)) {
      ham_assert(m_perf_data.upper_bound_index >= 0);
      ham_assert(m_perf_data.upper_bound.data == NULL ?
                m_perf_data.upper_bound.size == 0 :
                m_perf_data.upper_bound.size > 0);
      ham_assert(m_perf_data.upper_bound_page_address != 0);
      int cmp = m_db->compare_keys(key, &m_perf_data.upper_bound);
      if (cmp > 0) {
        hints.key_is_out_of_bounds = true;
        hints.try_fast_track = true;
      }
    }
  }

  return (hints);
}

BtreeStatistics::InsertHints
BtreeStatistics::get_insert_hints(ham_u32_t flags, Cursor *cursor,
                ham_key_t *key)
{
  InsertHints hints = {flags, flags, 0, false, false,
                false, NULL, -1};
  btree_cursor_t *btc = cursor ? cursor->get_btree_cursor() : 0;

  /* if the previous insert-operation replaced the upper_bound (or
   * lower_bound) key then it was actually an append (or prepend) operation.
   * in this case there's some probability that the next operation is also
   * appending/prepending.
   */
  if (m_perf_data.last_insert_was_append)
    hints.flags |= HAM_HINT_APPEND;
  else if (m_perf_data.last_insert_was_prepend)
    hints.flags |= HAM_HINT_PREPEND;

  if ((hints.flags & HAM_HINT_APPEND) && cursor) {
    if (!cursor->is_nil(0)) {
      ham_assert(m_db == cursor->get_db());

      /*
       * fetch the page of the cursor. We deem the cost of an uncoupled cursor
       * too high as that implies calling a full-fledged key search on the
       * given key - which can be rather costly - so we rather wait for the
       * statistical cavalry a little later on in this program then.
       */
      if (btree_cursor_is_coupled(btc)) {
        Page *page = btree_cursor_get_coupled_page(btc);
        BtreeNode *node = BtreeNode::from_page(page);
        ham_assert(node->is_leaf());
        /*
         * if cursor is not coupled to the LAST (right-most) leaf
         * in the Database, it does not make sense to append
         */
        if (node->get_right()) {
          hints.force_append = false;
          hints.try_fast_track = false;
        }
        else {
          hints.leaf_page_addr = page->get_self();
          hints.force_append = true;
          hints.try_fast_track = true;
        }
      }
    }
  }
  else if ((hints.flags & HAM_HINT_PREPEND) && cursor) {
    if (!cursor->is_nil(0)) {
      ham_assert(m_db == cursor->get_db());

      /*
       * fetch the page of the cursor. We deem the cost of an uncoupled cursor
       * too high as that implies calling a full-fledged key search on the
       * given key - which can be rather costly - so we rather wait for the
       * statistical cavalry a little later on in this program then.
       */
      if (btree_cursor_is_coupled(btc)) {
        Page *page = btree_cursor_get_coupled_page(btc);
        BtreeNode *node = BtreeNode::from_page(page);
        ham_assert(node->is_leaf());
        /*
         * if cursor is not coupled to the FIRST (left-most) leaf
         * in the Database, it does not make sense to prepend
         */
        if (node->get_left()) {
          hints.force_prepend = false;
          hints.try_fast_track = false;
        }
        else {
          hints.leaf_page_addr = page->get_self();
          hints.force_prepend = true;
          hints.try_fast_track = true;
        }
      }
    }
  }

  /*
   * The statistical cavalry:
   * - when the given key is positioned beyond the end, hint 'append' anyway.
   * - When the given key is positioned before the start, hint 'prepend' anyway.
   * NOTE: This 'auto-detect' mechanism (thanks to the key bounds being
   * collected through
   * the statistics gathering calls) renders the manual option
   * HAM_HINT_APPEND/_PREPEND somewhat obsolete, really.
   *
   * The only advantage of manually specifying HAM_HINT_APPEND/_PREPEND is
   * that it can save you two key comparisons in here.
   */
  ham_assert(!(key->_flags & KEY_IS_EXTENDED));
  key->_flags &= ~KEY_IS_EXTENDED;

  if (!hints.try_fast_track) {
    OperationStatistics *opstats = get_op_data(HAM_OPERATION_STATS_INSERT);

    ham_assert(opstats != NULL);

    if (hints.flags & (HAM_HINT_APPEND | HAM_HINT_PREPEND)) {
      /* find specific: APPEND / PREPEND --> SEQUENTIAL */
      hints.flags &= ~(HAM_HINT_APPEND | HAM_HINT_PREPEND);
      hints.flags |= HAM_HINT_SEQUENTIAL;
    }

    if ((hints.flags & HAM_HINTS_MASK) == 0) {
      /* no local preference specified; go with the DB-wide DAM config */
      switch (m_db->get_data_access_mode()) {
        default:
          break;

        case HAM_DAM_SEQUENTIAL_INSERT:
          hints.flags |= HAM_HINT_SEQUENTIAL;
          break;
      }
    }

    unsigned hits_required = 3;

    switch (hints.flags & HAM_HINTS_MASK) {
      default:
      case HAM_HINT_RANDOM_ACCESS:
        /* do not provide any hints for the fast track */
        break;

      case HAM_HINT_SEQUENTIAL | HAM_HINT_UBER_FAST_ACCESS:
        hits_required = 1;
        /* fall through */

      case HAM_HINT_SEQUENTIAL:
        /*
         * when we have more than 4 hits on the same page already, we'll
         * assume this one will end up there as well. As this counter will
         * reset itself on the first FAIL,
         * there's no harm in acting this quick. In pathological cases, the
         * worst what can happen is that in 20% of cases there will be
         * performed an extra check on a cached btree leaf node, which is
         * still minimal overhead then.
         */
        if (opstats->btree_last_page_sq_hits >= hits_required) {
          hints.leaf_page_addr = opstats->btree_last_page_addr;
          hints.try_fast_track = true;
          break;
        }
        /*
         * we assume this request is located near the previous request, so
         * we check if there's anything in the statistics that can help out.
         *
         * Note #1: since the hinting counts are 'aged' down to a value of
         * 0..1K (with 2K peak), we don't need to use a 64-bit integer for
         * the ratio calculation here.
         * 
         * Note #2: the ratio is only 'trustworthy' when the base count is
         * about 4 or higher.  This is because the aging rounds up while
         * scaling down, which means one single FAIL can get you a ratio as
         * large as 50% when total count is 1 as well, due to either startup
         * or aging rescale; without this minimum size check, the ratio +
         * aging would effectively stop the hinter from working after either
         * an aging step or when a few FAILs happen during the initial few
         * FIND operations (startup condition).
         *
         * EDIT: the above bit about the hinter stopping due to too much FAIL
         * at start or after rescale does NOT apply any more as the hinter
         * now also includes checks which trigger when a (small) series of
         * hits on the same page are found, which acts as a restarter for
         * this as well.
         */
        ham_u32_t ratio = opstats->btree_hinting_fail_count;

        ratio = ratio * 1000 / (1 + opstats->btree_hinting_count);
        if (ratio < 200) {
          hints.leaf_page_addr = opstats->btree_last_page_addr;
          hints.try_fast_track = true;
          hints.force_append = true;
        }

        if (m_perf_data.lower_bound_set) {
          if (m_perf_data.lower_bound_index == 1) {
            /*
             * impossible index: this is a marker to signal the table
             * is completely empty
             */
            hints.force_prepend = true;
            hints.leaf_page_addr = m_perf_data.lower_bound_page_address;
            hints.try_fast_track = true;
          }
          else {
            ham_assert(m_perf_data.lower_bound_index == 0);
            ham_assert(m_perf_data.lower_bound.data == NULL ?
                      m_perf_data.lower_bound.size == 0 :
                      m_perf_data.lower_bound.size > 0);
            ham_assert(m_perf_data.lower_bound_page_address != 0);
            int cmp = m_db->compare_keys(key, &m_perf_data.lower_bound);
            if (cmp < 0) {
              hints.force_prepend = true;
              hints.leaf_page_addr = m_perf_data.lower_bound_page_address;
              hints.try_fast_track = true;
            }
          }
        }

        if (m_perf_data.upper_bound_set) {
          ham_assert(m_perf_data.upper_bound_index >= 0);
          ham_assert(m_perf_data.upper_bound.data == NULL ?
                      m_perf_data.upper_bound.size == 0 :
                      m_perf_data.upper_bound.size > 0);
          ham_assert(m_perf_data.upper_bound_page_address != 0);
          int cmp = m_db->compare_keys(key, &m_perf_data.upper_bound);
          if (cmp > 0) {
            hints.force_append = true;
            hints.leaf_page_addr = m_perf_data.upper_bound_page_address;
            hints.try_fast_track = true;
          }
        }
      break;
    }
  }

  return (hints);
}

BtreeStatistics::EraseHints
BtreeStatistics::get_erase_hints(ham_u32_t flags, ham_key_t *key)
{
  BtreeStatistics::EraseHints hints = {flags, flags,
                    0, false, false, NULL, -1};

  ham_assert(!(key->_flags & KEY_IS_EXTENDED));
  key->_flags &= ~KEY_IS_EXTENDED;

  /* forget about deleting a key when it's out of bounds */
  if (m_perf_data.lower_bound_set) {
    if (m_perf_data.lower_bound_index == 1) {
      /*
       * impossible index: this is a marker to signal the table
       * is completely empty
       */
      hints.key_is_out_of_bounds = true;
      hints.try_fast_track = true;
    }
    else {
      ham_assert(m_perf_data.lower_bound_index == 0);
      ham_assert(m_perf_data.lower_bound.data == NULL ?
                m_perf_data.lower_bound.size == 0 :
                m_perf_data.lower_bound.size > 0);
      ham_assert(m_perf_data.lower_bound_page_address != 0);
      int cmp = m_db->compare_keys(key, &m_perf_data.lower_bound);
      if (cmp < 0) {
        hints.key_is_out_of_bounds = true;
        hints.try_fast_track = true;
      }
    }
  }

  if (m_perf_data.upper_bound_set) {
    ham_assert(m_perf_data.upper_bound_index >= 0);
    ham_assert(m_perf_data.upper_bound.data == NULL ?
                m_perf_data.upper_bound.size == 0 :
                m_perf_data.upper_bound.size > 0);
    ham_assert(m_perf_data.upper_bound_page_address != 0);
    int cmp = m_db->compare_keys(key, &m_perf_data.upper_bound);
    if (cmp > 0) {
      hints.key_is_out_of_bounds = true;
      hints.try_fast_track = true;
    }
  }

  return (hints);
}

} // namespace ham
