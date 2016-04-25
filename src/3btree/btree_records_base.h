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
#include "3btree/btree_list_base.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct BaseRecordList : BaseList {
  enum {
    // A flag whether this RecordList supports the scan() call
    kSupportsBlockScans = 0,

    // A flag whether this RecordList has sequential data
    kHasSequentialData = 0
  };

  BaseRecordList(LocalDb *db, PBtreeNode *node)
    : BaseList(db, node) {
  }

  // Fills the btree_metrics structure
  void fill_metrics(btree_metrics_t *metrics, size_t node_count) {
    BtreeStatistics::update_min_max_avg(&metrics->recordlist_ranges,
                        range_size);
  }

  // Returns the record id. Only required for internal nodes
  uint64_t record_id(int slot, int duplicate_index = 0) const {
    assert(!"shouldn't be here");
    return 0;
  }

  // Sets the record id. Not required for fixed length leaf nodes
  void set_record_id(int slot, uint64_t ptr) {
    assert(!"shouldn't be here");
  }
};

} // namespace upscaledb

#endif // UPS_BTREE_RECORDS_BASE_H
