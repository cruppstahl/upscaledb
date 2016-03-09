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
 * Base class for KeyLists
 */

#ifndef UPS_BTREE_KEYS_BASE_H
#define UPS_BTREE_KEYS_BASE_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct BaseKeyList
{
  enum {
    // This KeyList cannot reduce its capacity in order to release storage
    kCanReduceCapacity = 0,

    // This KeyList does NOT have a custom insert() implementation
    kCustomInsert = 0,

    // This KeyList does NOT have a custom find() implementation
    kCustomFind = 0,

    // This KeyList does NOT have a custom find_lower_bound() implementation
    kCustomFindLowerBound = 0,

    // A flag whether this KeyList supports the scan() call
    kSupportsBlockScans = 0,

    // A flag whether this KeyList has sequential data
    kHasSequentialData = 0,
  };

  BaseKeyList()
    : range_size_(0) {
  }

  // Erases the extended part of a key; nothing to do here
  void erase_extended_key(Context *context, int slot) const {
  }

  // Checks the integrity of this node. Throws an exception if there is a
  // violation.
  void check_integrity(Context *context, size_t node_count) const {
  }

  // Rearranges the list
  void vacuumize(size_t node_count, bool force) const {
  }

  // Performs a lower-bound search for a key
  template<typename Cmp>
  int find_lower_bound(Context *context, size_t node_count,
                  const ups_key_t *hkey, Cmp &comparator, int *pcmp) {
    throw Exception(UPS_NOT_IMPLEMENTED);
  }

  // Finds a key
  template<typename Cmp>
  int find(Context *context, size_t node_count, const ups_key_t *hkey,
                  Cmp &comparator) {
    throw Exception(UPS_NOT_IMPLEMENTED);
  }

  // Fills the btree_metrics structure
  void fill_metrics(btree_metrics_t *metrics, size_t node_count) {
    BtreeStatistics::update_min_max_avg(&metrics->keylist_ranges, range_size_);
  }

  // The size of the range (in bytes)
  uint32_t range_size_;
};

} // namespace upscaledb

#endif /* UPS_BTREE_KEYS_BASE_H */
