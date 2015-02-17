/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Base class for KeyLists
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_BTREE_KEYS_BASE_H
#define HAM_BTREE_KEYS_BASE_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct BaseKeyList
{
  enum {
      // This KeyList cannot reduce its capacity in order to release storage
      kCanReduceCapacity = 0,

      // This KeyList uses binary search combined with linear search
      kBinaryLinear,

      // This KeyList has a custom search implementation
      kCustomSearch,

      // This KeyList has a custom search implementation for exact matches
      // *only*
      kCustomExactImplementation,

      // This KeyList uses binary search (this is the default)
      kBinarySearch,

      // Specifies the search implementation: 
      kSearchImplementation = kBinarySearch,

      // This KeyList does NOT have a custom insert implementation
      kCustomInsert = 0,
  };

  BaseKeyList()
    : m_range_size(0) {
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

  // Finds a key
  template<typename Cmp>
  int find(Context *, size_t node_count, const ham_key_t *key, Cmp &comparator,
                  int *pcmp) {
    ham_assert(!"shouldn't be here");
    return (0);
  }

  // Returns the threshold when switching from binary search to
  // linear search. Disabled by default
  size_t get_linear_search_threshold() const {
    return ((size_t)-1);
  }

  // Performs a linear search in a given range between |start| and
  // |start + length|. Disabled by default.
  template<typename Cmp>
  int linear_search(size_t start, size_t length, const ham_key_t *hkey,
                  Cmp &comparator, int *pcmp) {
    ham_assert(!"shouldn't be here");
    throw Exception(HAM_INTERNAL_ERROR);
  }

  // Fills the btree_metrics structure
  void fill_metrics(btree_metrics_t *metrics, size_t node_count) {
    BtreeStatistics::update_min_max_avg(&metrics->keylist_ranges, m_range_size);
  }

  // The size of the range (in bytes)
  size_t m_range_size;
};

} // namespace hamsterdb

#endif /* HAM_BTREE_KEYS_BASE_H */
