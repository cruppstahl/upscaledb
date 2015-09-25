/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
 * btree find/insert/erase statistical structures, functions and macros
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_BTREE_STATS_H
#define HAM_BTREE_STATS_H

#include "0root/root.h"

#include <limits>

#include "ham/hamsterdb_int.h"

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class Page;

class BtreeStatistics {
  public:
    // Indices into find/insert/erase specific statistics
    enum {
      kOperationFind      = 0,
      kOperationInsert    = 1,
      kOperationErase     = 2,
      kOperationMax       = 3
    };

    struct FindHints {
      // the original flags of ham_find
      uint32_t original_flags;

      // the modified flags
      uint32_t flags;

      // page/btree leaf to check first
      uint64_t leaf_page_addr;

      // check specified btree leaf node page first
      bool try_fast_track;
    };

    struct InsertHints {
      // the original flags of ham_insert
      uint32_t original_flags;

      // the modified flags
      uint32_t flags;

      // page/btree leaf to check first
      uint64_t leaf_page_addr;

      // the processed leaf page
      Page *processed_leaf_page;

      // the slot in that page
      uint16_t processed_slot;

      // count the number of appends
      size_t append_count;

      // count the number of prepends
      size_t prepend_count;
    };

    // Constructor
    BtreeStatistics();

    // Returns the btree hints for ham_find
    FindHints get_find_hints(uint32_t flags);

    // Returns the btree hints for insert
    InsertHints get_insert_hints(uint32_t flags);

    // Reports that a ham_find/ham_cusor_find succeeded
    void find_succeeded(Page *page);

    // Reports that a ham_find/ham_cursor_find failed
    void find_failed();

    // Reports that a ham_insert/ham_cursor_insert succeeded
    void insert_succeeded(Page *page, uint16_t slot);

    // Reports that a ham_insert/ham_cursor_insert failed
    void insert_failed();

    // Reports that a ham_erase/ham_cusor_erase succeeded
    void erase_succeeded(Page *page);

    // Reports that a ham_erase/ham_cursor_erase failed
    void erase_failed();

    // Resets the statistics for a single page
    void reset_page(Page *page);

    // Keep track of the KeyList range size
    void set_keylist_range_size(bool leaf, size_t size) {
      m_keylist_range_size[(int)leaf] = size;
    }

    // Retrieves the KeyList range size
    size_t get_keylist_range_size(bool leaf) const {
      return (m_keylist_range_size[(int)leaf]);
    }

    // Keep track of the KeyList capacities
    void set_keylist_capacities(bool leaf, size_t capacity) {
      m_keylist_capacities[(int)leaf] = capacity;
    }

    // Retrieves the KeyList capacities size
    size_t get_keylist_capacities(bool leaf) const {
      return (m_keylist_capacities[(int)leaf]);
    }

    // Calculate the "average" values
    static void finalize_metrics(btree_metrics_t *metrics);

    // Update a min_max_avg structure
    static void update_min_max_avg(min_max_avg_u32_t *data, uint32_t value) {
      // first update? then perform initialization
      if (data->_instances == 0)
        data->min = std::numeric_limits<uint32_t>::max();

      if (data->min > value)
        data->min = value;
      if (data->max < value)
        data->max = value;
      data->_total += value;
      data->_instances++;
    }

  private:
    // last leaf page for find/insert/erase
    uint64_t m_last_leaf_pages[kOperationMax];

    // count of how often this leaf page was used
    size_t m_last_leaf_count[kOperationMax];

    // count the number of appends
    size_t m_append_count;

    // count the number of prepends
    size_t m_prepend_count;

    // the range size of the KeyList
    size_t m_keylist_range_size[2];

    // the capacities of the KeyList
    size_t m_keylist_capacities[2];
};

} // namespace hamsterdb

#endif /* HAM_BTREE_STATS_H */
