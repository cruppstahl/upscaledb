/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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
 * btree find/insert/erase statistical structures, functions and macros
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_BTREE_STATS_H
#define HAM_BTREE_STATS_H

#include "0root/root.h"

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
      ham_u32_t original_flags;

      // the modified flags
      ham_u32_t flags;

      // page/btree leaf to check first
      ham_u64_t leaf_page_addr;

      // check specified btree leaf node page first
      bool try_fast_track;
    };

    struct InsertHints {
      // the original flags of ham_insert
      ham_u32_t original_flags;

      // the modified flags
      ham_u32_t flags;

      // page/btree leaf to check first
      ham_u64_t leaf_page_addr;

      // the processed leaf page
      Page *processed_leaf_page;

      // the slot in that page
      ham_u16_t processed_slot;

      // count the number of appends
      size_t append_count;

      // count the number of prepends
      size_t prepend_count;
    };

    // Constructor
    BtreeStatistics();

    // Returns the btree hints for ham_find
    FindHints get_find_hints(ham_u32_t flags);

    // Returns the btree hints for insert
    InsertHints get_insert_hints(ham_u32_t flags);

    // Reports that a ham_find/ham_cusor_find succeeded
    void find_succeeded(Page *page);

    // Reports that a ham_find/ham_cursor_find failed
    void find_failed();

    // Reports that a ham_insert/ham_cursor_insert succeeded
    void insert_succeeded(Page *page, ham_u16_t slot);

    // Reports that a ham_insert/ham_cursor_insert failed
    void insert_failed();

    // Reports that a ham_erase/ham_cusor_erase succeeded
    void erase_succeeded(Page *page);

    // Reports that a ham_erase/ham_cursor_erase failed
    void erase_failed();

    // Resets the statistics for a single page
    void reset_page(Page *page);

    // Sets the capacity of a page. The capacities of leaf nodes and
    // internal pages is tracked separately. If |leaf| is true
    // then |capacity| is for leaf pages, otherwise for internal pages.
    void set_page_capacity(bool leaf, size_t capacity) {
      m_page_capacity[(int)leaf] = capacity;
    }

    // Returns the default capacity for a page (default layout), or 0
    // if there was not enough data
    size_t get_page_capacity(bool leaf) const {
      return (m_page_capacity[(int)leaf]);
    }

    // Also keep track of the KeyList range size
    void set_keylist_range_size(bool leaf, size_t size) {
      m_keylist_range_size[(int)leaf] = size;
    }

    // Retrieves the KeyList range size
    size_t get_keylist_range_size(bool leaf) const {
      return (m_keylist_range_size[(int)leaf]);
    }

  private:
    // last leaf page for find/insert/erase
    ham_u64_t m_last_leaf_pages[kOperationMax];

    // count of how often this leaf page was used
    size_t m_last_leaf_count[kOperationMax];

    // count the number of appends
    size_t m_append_count;

    // count the number of prepends
    size_t m_prepend_count;

    // the capacity of the btree nodes
    size_t m_page_capacity[2];

    // the range size of the KeyList
    size_t m_keylist_range_size[2];
};

} // namespace hamsterdb

#endif /* HAM_BTREE_STATS_H */
