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
#include "1base/byte_array.h"
#include "2page/page.h"
#include "3blob_manager/blob_manager.h"
#include "3btree/btree_node.h"
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

class InlineRecordList
{
  public:
    enum {
      // A flag whether this RecordList has sequential data
      kHasSequentialData = 1
    };

    // Constructor
    InlineRecordList(LocalDatabase *db, PBtreeNode *node)
      : m_db(db), m_record_size(db->get_record_size()), m_data(0),
        m_capacity(0) {
      ham_assert(m_record_size != HAM_RECORD_SIZE_UNLIMITED);
    }

    // Sets the data pointer
    void create(ham_u8_t *data, size_t full_range_size_bytes, size_t capacity) {
      m_data = (ham_u8_t *)data;
      m_capacity = capacity;
    }

    // Opens an existing RecordList
    void open(ham_u8_t *ptr, size_t capacity) {
      m_data = ptr;
      m_capacity = capacity;
    }

    // Returns the actual record size including overhead
    double get_full_record_size() const {
      return (m_record_size);
    }

    // Returns the full size of the range
    size_t get_range_size() const {
      return (m_capacity * m_record_size);
    }

    // Calculates the required size for a range with the specified |capacity|
    size_t calculate_required_range_size(size_t node_count,
            size_t new_capacity) const {
      return (new_capacity * m_record_size);
    }

    // Returns the record counter of a key
    ham_u32_t get_record_count(ham_u32_t slot) const {
      return (1);
    }

    // Returns the record size
    ham_u64_t get_record_size(ham_u32_t slot,
                    ham_u32_t duplicate_index = 0) const {
      return (m_record_size);
    }

    // Returns the full record and stores it in |dest|; memory must be
    // allocated by the caller
    void get_record(ham_u32_t slot, ByteArray *arena, ham_record_t *record,
                    ham_u32_t flags, ham_u32_t duplicate_index) const {
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
    void set_record(ham_u32_t slot, ham_u32_t duplicate_index,
                ham_record_t *record, ham_u32_t flags,
                ham_u32_t *new_duplicate_index = 0) {
      ham_assert(record->size == m_record_size);
      // it's possible that the records have size 0 - then don't copy anything
      if (m_record_size)
        memcpy(&m_data[m_record_size * slot], record->data, m_record_size);
    }

    // Erases the record
    void erase_record(ham_u32_t slot, ham_u32_t duplicate_index = 0,
                    bool all_duplicates = true) {
      if (m_record_size)
        memset(&m_data[m_record_size * slot], 0, m_record_size);
    }

    // Erases a whole slot by shifting all larger records to the "left"
    void erase_slot(size_t node_count, ham_u32_t slot) {
      if (slot < node_count - 1)
        memmove(&m_data[m_record_size * slot],
                        &m_data[m_record_size * (slot + 1)],
                        m_record_size * (node_count - slot - 1));
    }

    // Creates space for one additional record
    void insert_slot(size_t node_count, ham_u32_t slot) {
      if (slot < node_count) {
        memmove(&m_data[m_record_size * (slot + 1)],
                        &m_data[m_record_size * slot],
                        m_record_size * (node_count - slot));
      }
      memset(&m_data[m_record_size * slot], 0, m_record_size);
    }

    // Copies |count| records from this[sstart] to dest[dstart]
    void copy_to(ham_u32_t sstart, size_t node_count, InlineRecordList &dest,
                    size_t other_count, ham_u32_t dstart) {
      memcpy(&dest.m_data[m_record_size * dstart],
                      &m_data[m_record_size * sstart],
                      m_record_size * (node_count - sstart));
    }

    // Returns the record id. Not required for fixed length leaf nodes
    ham_u64_t get_record_id(ham_u32_t slot, ham_u32_t duplicate_index = 0)
                    const {
      ham_assert(!"shouldn't be here");
      return (0);
    }

    // Sets the record id. Not required for fixed length leaf nodes
    void set_record_id(ham_u32_t slot, ham_u64_t ptr) {
      ham_assert(!"shouldn't be here");
    }

    // Returns true if there's not enough space for another record
    bool requires_split(size_t node_count, bool vacuumize = false) const {
      return (node_count >= m_capacity);
    }

    // Checks the integrity of this node. Throws an exception if there is a
    // violation.
    void check_integrity(size_t node_count, bool quick = false) const {
    }

    // Rearranges the list; not supported
    void vacuumize(size_t node_count, bool force) const {
    }

    // Change the capacity; for PAX layouts this just means copying the
    // data from one place to the other
    void change_capacity(size_t node_count, size_t old_capacity,
            size_t new_capacity, ham_u8_t *new_data_ptr,
            size_t new_range_size) {
      memmove(new_data_ptr, m_data, node_count * m_record_size);
      m_data = new_data_ptr;
      m_capacity = new_capacity;
    }

    // Prints a slot to |out| (for debugging)
    void print(ham_u32_t slot, std::stringstream &out) const {
      out << "(" << get_record_size(slot) << " bytes)";
    }

  private:
    // The parent database of this btree
    LocalDatabase *m_db;

    // The record size, as specified when the database was created
    size_t m_record_size;

    // The actual record data
    ham_u8_t *m_data;

    // The capacity of m_data
    size_t m_capacity;
};

} // namespace PaxLayout

} // namespace hamsterdb

#endif /* HAM_BTREE_RECORDS_INLINE_H */
