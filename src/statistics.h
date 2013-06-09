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

