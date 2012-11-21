/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifndef HAM_STATISTICS_H__
#define HAM_STATISTICS_H__

#include <ham/hamsterdb.h>

#include "util.h"

namespace ham {

/**
 * The upper bound value which will trigger a statistics data rescale operation
 * to be initiated in order to prevent integer overflow in the statistics data
 * elements.
 */
#define HAM_STATISTICS_HIGH_WATER_MARK  0x7FFFFFFF /* could be 0xFFFFFFFF */

/* 
 * As we [can] support record sizes up to 4Gb, at least theoretically,
 * we can express this size range as a spanning DB_CHUNKSIZE size range:
 * 1..N, where N = log2(4Gb) - log2(DB_CHUNKSIZE). As we happen to know
 * DB_CHUNKSIZE == 32, at least for all regular hamsterdb builds, our
 * biggest power-of-2 for the freelist slot count ~ 32-5 = 27, where 0
 * represents slot size = 1 DB_CHUNKSIZE, 1 represents size of 2
 * DB_CHUNKSIZEs, 2 ~ 4 DB_CHUNKSIZEs, and so on.
 *
 * EDIT:
 * In order to cut down on statistics management cost due to overhead
 * caused by having to keep up with the latest for VERY large sizes, we
 * cut this number down to support sizes up to a maximum size of 64Kb ~
 * 2^16, meaning any requests for more than 64Kb/CHUNKSIZE bytes is
 * sharing their statistics.
 *
 */
#define HAM_FREELIST_SLOT_SPREAD   (16-5+1) /* 1 chunk .. 2^(SPREAD-1) chunks */

/**
 * We keep track of VERY first free slot index + free slot index
 * pointing at last (~ supposed largest) free range + 'utilization' of the
 * range between FIRST and LAST as a ratio of number of free slots in
 * there vs. total number of slots in that range (giving us a 'fill'
 * ratio) + a fragmentation indication, determined by counting the number
 * of freelist slot searches that FAILed vs. SUCCEEDed within the
 * first..last range, when the search begun at the 'first' position
 * (a FAIL here meaning the freelist scan did not deliver a free slot
 * WITHIN the first..last range, i.e. it has scanned this entire range
 * without finding anything suitably large).
 *
 * Note that the free_fill in here is AN ESTIMATE.
 */
struct ham_freelist_slotsize_stats_t
{
  ham_u32_t first_start;

  /* reserved: */
  ham_u32_t free_fill;
  ham_u32_t epic_fail_midrange;
  ham_u32_t epic_win_midrange;

  /** number of scans per size range */
  ham_u32_t scan_count;

  ham_u32_t ok_scan_count;

  /** summed cost ('duration') of all scans per size range */
  ham_u32_t scan_cost;
  ham_u32_t ok_scan_cost;
};

/**
 * freelist statistics as they are persisted on disc.
 *
 * Stats are kept with each freelist entry record, but we also keep
 * some derived data in the nonpermanent space with each freelist:
 * it's not required to keep a freelist page in cache just so the
 * statistics + our operational mode combined can tell us it's a waste
 * of time to go there.
 */
struct ham_freelist_page_statistics_t
{
  ham_freelist_slotsize_stats_t per_size[HAM_FREELIST_SLOT_SPREAD];

  /**
   * (bit) offset which tells us which free slot is the EVER LAST
   * created one; after all, freelistpage:maxbits is a scandalously
   * optimistic lie: all it tells us is how large the freelist page
   * _itself_ can grow, NOT how many free slots we actually have
   * _alive_ in there.
   *
   * 0: special case, meaning: not yet initialized...
   */
  ham_u32_t last_start;

  /**
   * total number of available bits in the page ~ all the chunks which
   * actually represent a chunk in the DB storage space.
   *
   * (Note that a freelist can be larger (_max_bits) than the actual
   * number of storage pages currently sitting in the database file.)
   *
   * The number of chunks already in use in the database therefore ~
   * persisted_bits - _allocated_bits.
   */
  ham_u32_t persisted_bits;

  /**
   * count the number of insert operations where this freelist page
   * played a role
   */
  ham_u32_t insert_count;
  ham_u32_t delete_count;
  ham_u32_t extend_count;
  ham_u32_t fail_count;
  ham_u32_t search_count;

  ham_u32_t rescale_monitor;
};

/**
 * global freelist algorithm specific run-time info: per cache
 */
struct EnvironmentStatistics
{
  EnvironmentStatistics() {
    memset(this, 0, sizeof(*this));
  }

  /** number of scans per size range */
  ham_u32_t scan_count[HAM_FREELIST_SLOT_SPREAD];
  ham_u32_t ok_scan_count[HAM_FREELIST_SLOT_SPREAD];

  /** summed cost ('duration') of all scans per size range */
  ham_u32_t scan_cost[HAM_FREELIST_SLOT_SPREAD];
  ham_u32_t ok_scan_cost[HAM_FREELIST_SLOT_SPREAD];

  /** count the number of insert operations for this DB */
  ham_u32_t insert_count;
  ham_u32_t delete_count;
  ham_u32_t extend_count;
  ham_u32_t fail_count;
  ham_u32_t search_count;

  ham_u32_t insert_query_count;
  ham_u32_t erase_query_count;
  ham_u32_t query_count;

  ham_u32_t first_page_with_free_space[HAM_FREELIST_SLOT_SPREAD];

  /**
   * Note: counter/statistics value overflow management:
   *
   * As the 'cost' numbers will be the fastest growing numbers of
   * them all, it is sufficient to check cost against a suitable
   * high water mark, and once it reaches that mark, to rescale
   * all statistics.
   *
   * Of course, we could have done without the rescaling by using
   * 64-bit integers for all statistics elements, but 64-bit
   * integers are not native to all platforms and incur a (minor)
   * run-time penalty when used. It is felt that slower machines,
   * which are often 32-bit only, benefit from a compare plus
   * once-in-a-while rescale, as this overhead can be amortized
   * over a large multitude of statistics updates.
   *
   * How does the rescaling work?
   *
   * The statistics all are meant to represent relative numbers,
   * so uniformly scaling these numbers will not produce worse
   * results from the hinters -- as long as the scaling does not produce
   * edge values (0 or 1) which destroy the significance of the numbers
   * gathered thus far.
   *
   * I believe a rescale by a factor of 256 (2^8) is quite safe
   * when the high water mark is near the MAXINT (2^32) edge, even
   * when the cost number can be 100 times as large as the other
   * numbers in some regular use cases. Meanwhile, a division by
   * 256 will reduce the collected numeric values so much that
   * there is ample headroom again for the next 100K+ operations;
   * at an average monitored cost increase of 10-20 per
   * insert/delete trial and, for very large databases using an
   * overly conservative freelist management setting, ~50-200 trials
   * per insert/delete API invocation.
   *
   * Assuming a high water mark for signed int, i.e. 2^31 ~ 2.14
   * billion, dividing ('rescaling') that number down to 2^(31-8) ~ 8M
   * produces a headroom of ~ 2.13 billion points, which,
   * assuming the nominal worst case of a cost addition of 4000
   * points per insert/delete, implies new headroom for ~ 500K
   * insert/delete API operations.
   *
   * Which, in my book, is ample space. This also means that the
   * costs incurred by the rescaling can be amortized over 500K+
   * operations, resulting in an - on average - negligible overhead.
   *
   * So we can use 32-bits for all statistics counters quite
   * safely. Assuming our 'cost is the fastest riser' position holds for
   * all use cases, that is.
   *
   * A quick analysis shows this to be probably true, even for
   * fringe cases (a mathematical proof would be nicer here, but
   * alas):
   * let's assume worst case, where we have a lot of trials
   * (testing each freelist page entry in a very long freelist,
   * i.e. a huge database table) which all fail. 'Cost' is
   * calculated EVERY TIME the innermost freelist search method is
   * invoked, i.e. when the freelist bitarray is inspected, and
   * both fail and success costs are immediately fed into the
   * statistics, so our worst case for the 'cost-is-fastest' lemma
   * would be a long trace of fail trials, which do NOT test the
   * freelist bitarrays, i.e. fails which are discarded in the outer layers,
   * thanks to the hinters (global and per-entry) kicking in and preventing
   * those freelist bitarray scans. Assume then that all counters
   * have the same value, which would mean that the number of
   * fails has to be higher that the final cost, repetitively.
   *
   * This _can_ happen when the number of fail trials at the
   * per-entry outer level is higher than the cost of the final
   * (and only) freelist bitarray scan, which clocks in at a
   * nominal 4-10 points for success cases. However, those _outer_
   * fail trials are NOT counted and fed to the statistics, so
   * this case will only register a single, successful or failing,
   * trial - with cost.
   *
   * As long as the code is not changed to count those
   * hinter-induced fast rounds in the outer layers when searching
   * for a slot in the freelist, the lemma 'cost grows fastest'
   * holds, as any other possible 'worst case' will either
   * succeed quite quickly or fail through a bitarray scan, which
   * results in such fail rounds having associated non-zero, 1+
   * costs associated with them.
   *
   * To be on the safe side of it all, we accumulate all costs in
   * a special statistics counter, which is specifically designed
   * to be used for the high water mark monitoring and subsequent
   * decision to rescale: rescale_monitor.
   */
  ham_u32_t rescale_monitor;
};

/**
 * @defgroup ham_operation_types hamsterdb Database Operation Types
 * @{
 *
 * Indices into find/insert/erase specific statistics
 */
#define HAM_OPERATION_STATS_FIND        0
#define HAM_OPERATION_STATS_INSERT      1
#define HAM_OPERATION_STATS_ERASE       2

/** The number of operations defined for the statistics gathering process */
#define HAM_OPERATION_STATS_MAX         3

/**
 * @}
 */

/**
 * Statistics gathered for a single database
 */
struct DatabaseStatistics
{
  DatabaseStatistics() {
    memset(this, 0, sizeof(*this));
  }

  /* last leaf page for find/insert/erase */
  ham_offset_t last_leaf_pages[HAM_OPERATION_STATS_MAX];

  /* count of how often this leaf page was used */
  ham_size_t last_leaf_count[HAM_OPERATION_STATS_MAX];

  /* count the number of appends */
  ham_size_t append_count;

  /* count the number of prepends */
  ham_size_t prepend_count;
};

} // namespace ham

#endif /* HAM_STATISTICS_H__ */

