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

#ifndef HAM_BTREE_STATS_H__
#define HAM_BTREE_STATS_H__

#include <ham/hamsterdb_int.h>

namespace hamsterdb {

class Page;

/**
 * btree find/insert/erase statistical structures, functions and macros
 */
class BtreeStatistics {
  public:
    // Indices into find/insert/erase specific statistics
    enum {
      kOperationFind      = 0,
      kOperationInsert    = 1,
      kOperationErase     = 2,
      kOperationMax       = 3,

      kMaxCapacities      = 5
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

    // Sets the capacity of a page
    void set_page_capacity(size_t capacity);

    // Returns the default capacity for a page (default layout), or 0
    // if there was not enough data
    size_t get_default_page_capacity() const;

  private:
    // last leaf page for find/insert/erase
    ham_u64_t m_last_leaf_pages[kOperationMax];

    // count of how often this leaf page was used
    size_t m_last_leaf_count[kOperationMax];

    // count the number of appends
    size_t m_append_count;

    // count the number of prepends
    size_t m_prepend_count;

    // the page capacities of the last couple of pages
    size_t m_page_capacities[kMaxCapacities];
};

} // namespace hamsterdb

#endif /* HAM_BTREE_STATS_H__ */
