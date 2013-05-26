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

#ifndef HAM_STATISTICS_H__
#define HAM_STATISTICS_H__

#include <ham/hamsterdb.h>

#include "util.h"

namespace hamsterdb {

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
 * global freelist algorithm specific run-time info
 */
struct EnvironmentStatistics
{
  EnvironmentStatistics() {
    memset(this, 0, sizeof(*this));
  }

  ham_u32_t first_page_with_free_space[HAM_FREELIST_SLOT_SPREAD];
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
  ham_u64_t last_leaf_pages[HAM_OPERATION_STATS_MAX];

  /* count of how often this leaf page was used */
  ham_size_t last_leaf_count[HAM_OPERATION_STATS_MAX];

  /* count the number of appends */
  ham_size_t append_count;

  /* count the number of prepends */
  ham_size_t prepend_count;
};

} // namespace hamsterdb

#endif /* HAM_STATISTICS_H__ */

