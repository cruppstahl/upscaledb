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
 * RecordList for Inline Records
 *
 * Inline Records are records that are stored directly in the leaf node, and
 * not in an external blob. Only for fixed length records.
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_BTREE_RECORDS_INLINE_H
#define HAM_BTREE_RECORDS_INLINE_H

#include "0root/root.h"

#include <sstream>
#include <iostream>

// Always verify that a file of level N does not include headers > N!
#include "1globals/globals.h"
#include "1base/dynamic_array.h"
#include "2page/page.h"
#include "3blob_manager/blob_manager.h"
#include "3btree/btree_node.h"
#include "3btree/btree_records_base.h"
#include "4env/env_local.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

//
// The template classes in this file are wrapped in a separate namespace
// to avoid naming clashes with btree_impl_default.h
//
namespace PaxLayout {

class InlineRecordList : public BaseRecordList
{
  public:
    enum {
      // A flag whether this RecordList has sequential data
      kHasSequentialData = 1
    };

    // Constructor
    InlineRecordList(LocalDatabase *db, PBtreeNode *node)
      : m_db(db), m_record_size(db->config().record_size), m_data(0) {
      ham_assert(m_record_size != HAM_RECORD_SIZE_UNLIMITED);
    }

    // Sets the data pointer
    void create(uint8_t *data, size_t range_size) {
      m_data = (uint8_t *)data;
      m_range_size = range_size;
    }

    // Opens an existing RecordList
    void open(uint8_t *ptr, size_t range_size, size_t node_count) {
      m_data = ptr;
      m_range_size = range_size;
    }

    // Returns the actual record size including overhead
    size_t get_full_record_size() const {
      return (m_record_size);
    }

    // Calculates the required size for a range with the specified |capacity|
    size_t get_required_range_size(size_t node_count) const {
      return (node_count * m_record_size);
    }

    // Returns the record counter of a key
    int get_record_count(Context *context, int slot) const {
      return (1);
    }

    // Returns the record size
    uint64_t get_record_size(Context *context, int slot,
                    int duplicate_index = 0) const {
      return (m_record_size);
    }

    // Returns the full record and stores it in |dest|; memory must be
    // allocated by the caller
    void get_record(Context *context, int slot, ByteArray *arena,
                    ham_record_t *record, uint32_t flags,
                    int duplicate_index) const {
      bool direct_access = (flags & HAM_DIRECT_ACCESS) != 0;

      if (flags & HAM_PARTIAL) {
        ham_trace(("flag HAM_PARTIAL is not allowed if record is "
                   "stored inline"));
        throw Exception(HAM_INV_PARAMETER);
      }

      // the record is stored inline
      record->size = m_record_size;

      if (m_record_size == 0)
        record->data = 0;
      else if (direct_access)
        record->data = &m_data[slot * m_record_size];
      else {
        if ((record->flags & HAM_RECORD_USER_ALLOC) == 0) {
          arena->resize(record->size);
          record->data = arena->get_ptr();
        }
        memcpy(record->data, &m_data[slot * m_record_size], record->size);
      }
    }

    // Updates the record of a key
    void set_record(Context *context, int slot, int duplicate_index,
                ham_record_t *record, uint32_t flags,
                uint32_t *new_duplicate_index = 0) {
      ham_assert(record->size == m_record_size);
      // it's possible that the records have size 0 - then don't copy anything
      if (m_record_size)
        memcpy(&m_data[m_record_size * slot], record->data, m_record_size);
    }

    // Erases the record
    void erase_record(Context *context, int slot, int duplicate_index = 0,
                    bool all_duplicates = true) {
      if (m_record_size)
        memset(&m_data[m_record_size * slot], 0, m_record_size);
    }

    // Erases a whole slot by shifting all larger records to the "left"
    void erase(Context *context, size_t node_count, int slot) {
      if (slot < (int)node_count - 1)
        memmove(&m_data[m_record_size * slot],
                        &m_data[m_record_size * (slot + 1)],
                        m_record_size * (node_count - slot - 1));
    }

    // Creates space for one additional record
    void insert(Context *context, size_t node_count, int slot) {
      if (slot < (int)node_count) {
        memmove(&m_data[m_record_size * (slot + 1)],
                        &m_data[m_record_size * slot],
                        m_record_size * (node_count - slot));
      }
      memset(&m_data[m_record_size * slot], 0, m_record_size);
    }

    // Copies |count| records from this[sstart] to dest[dstart]
    void copy_to(int sstart, size_t node_count, InlineRecordList &dest,
                    size_t other_count, int dstart) {
      memcpy(&dest.m_data[m_record_size * dstart],
                      &m_data[m_record_size * sstart],
                      m_record_size * (node_count - sstart));
    }

    // Returns the record id. Not required for fixed length leaf nodes
    uint64_t get_record_id(int slot, int duplicate_index = 0)
                    const {
      ham_assert(!"shouldn't be here");
      return (0);
    }

    // Sets the record id. Not required for fixed length leaf nodes
    void set_record_id(int slot, uint64_t ptr) {
      ham_assert(!"shouldn't be here");
    }

    // Returns true if there's not enough space for another record
    bool requires_split(size_t node_count) const {
      if (m_range_size == 0)
        return (false);
      return ((node_count + 1) * m_record_size >= m_range_size);
    }

    // Change the capacity; for PAX layouts this just means copying the
    // data from one place to the other
    void change_range_size(size_t node_count, uint8_t *new_data_ptr,
                    size_t new_range_size, size_t capacity_hint) {
      memmove(new_data_ptr, m_data, node_count * m_record_size);
      m_data = new_data_ptr;
      m_range_size = new_range_size;
    }

    // Fills the btree_metrics structure
    void fill_metrics(btree_metrics_t *metrics, size_t node_count) {
      BaseRecordList::fill_metrics(metrics, node_count);
      BtreeStatistics::update_min_max_avg(&metrics->recordlist_unused,
                          m_range_size - get_required_range_size(node_count));
    }

    // Prints a slot to |out| (for debugging)
    void print(Context *context, int slot, std::stringstream &out) const {
      out << "(" << get_record_size(context, slot) << " bytes)";
    }

  private:
    // The parent database of this btree
    LocalDatabase *m_db;

    // The record size, as specified when the database was created
    size_t m_record_size;

    // The actual record data
    uint8_t *m_data;
};

} // namespace PaxLayout

} // namespace hamsterdb

#endif /* HAM_BTREE_RECORDS_INLINE_H */
