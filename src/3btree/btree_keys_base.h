/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

      // This KeyList does NOT have a custom insert() implementation
      kCustomInsert = 0,

      // This KeyList does NOT have a custom find() implementation
      kCustomFind = 0,

      // This KeyList does NOT have a custom find_lower_bound() implementation
      kCustomFindLowerBound = 0,
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

  // Performs a lower-bound search for a key
  template<typename Cmp>
  int find_lower_bound(Context *context, size_t node_count,
                  const ham_key_t *hkey, Cmp &comparator, int *pcmp) {
    throw Exception(HAM_NOT_IMPLEMENTED);
  }

  // Finds a key
  template<typename Cmp>
  int find(Context *context, size_t node_count, const ham_key_t *hkey,
                  Cmp &comparator) {
    throw Exception(HAM_NOT_IMPLEMENTED);
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
