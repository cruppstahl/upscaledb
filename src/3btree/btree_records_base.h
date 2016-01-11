/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

/*
 * Base class for RecordLists
 */

#ifndef UPS_BTREE_RECORDS_BASE_H
#define UPS_BTREE_RECORDS_BASE_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct BaseRecordList
{
  enum {
    // A flag whether this RecordList supports the scan() call
    kSupportsBlockScans = 0,
  };

  BaseRecordList()
    : m_range_size(0) {
  }

  // Checks the integrity of this node. Throws an exception if there is a
  // violation.
  void check_integrity(Context *context, size_t node_count) const {
  }

  // Rearranges the list
  void vacuumize(size_t node_count, bool force) const {
  }

  // Fills the btree_metrics structure
  void fill_metrics(btree_metrics_t *metrics, size_t node_count) {
    BtreeStatistics::update_min_max_avg(&metrics->recordlist_ranges,
                        m_range_size);
  }

  // Returns the record id. Only required for internal nodes
  uint64_t get_record_id(int slot, int duplicate_index = 0) const {
    ups_assert(!"shouldn't be here");
    return (0);
  }

  // Sets the record id. Not required for fixed length leaf nodes
  void set_record_id(int slot, uint64_t ptr) {
    ups_assert(!"shouldn't be here");
  }

  // Iterates all records, calls the |visitor| on each
  void scan(Context *context, ScanVisitor *visitor, uint32_t start,
                  size_t length) {
    ups_assert(!"shouldn't be here");
    throw Exception(UPS_INTERNAL_ERROR);
  }

  // The size of the range (in bytes)
  size_t m_range_size;
};

} // namespace upscaledb

#endif /* UPS_BTREE_RECORDS_BASE_H */
