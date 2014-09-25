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
 * Internal RecordList
 *
 * Only for records of internal nodes. Internal nodes only store page IDs,
 * therefore this |InternalRecordList| is optimized for 64bit IDs
 * (and is implemented as a ham_u64_t[] array).
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_BTREE_RECORDS_INTERNAL_H
#define HAM_BTREE_RECORDS_INTERNAL_H

#include "0root/root.h"

#include <sstream>
#include <iostream>

// Always verify that a file of level N does not include headers > N!
#include "1globals/globals.h"
#include "1base/byte_array.h"
#include "2page/page.h"
#include "3blob_manager/blob_manager.h"
#include "3btree/btree_records_base.h"
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

class InternalRecordList : public BaseRecordList
{
  public:
    enum {
      // A flag whether this RecordList has sequential data
      kHasSequentialData = 1
    };

    // Constructor
    InternalRecordList(LocalDatabase *db, PBtreeNode *node)
      : m_db(db), m_data(0) {
    }

    // Sets the data pointer
    void create(ham_u8_t *data, size_t range_size) {
      m_data = (ham_u64_t *)data;
      m_range_size = range_size;
    }

    // Opens an existing RecordList
    void open(ham_u8_t *ptr, size_t range_size, size_t node_count) {
      m_data = (ham_u64_t *)ptr;
      m_range_size = range_size;
    }

    // Returns the actual size including overhead
    size_t get_full_record_size() const {
      return (sizeof(ham_u64_t));
    }

    // Calculates the required size for a range with the specified |capacity|
    size_t get_required_range_size(size_t node_count) const {
      return (node_count * sizeof(ham_u64_t));
    }

    // Returns the record counter of a key; this implementation does not
    // support duplicates, therefore the record count is always 1
    ham_u32_t get_record_count(ham_u32_t slot) const {
      return (1);
    }

    // Returns the record size
    ham_u64_t get_record_size(ham_u32_t slot,
                    ham_u32_t duplicate_index = 0) const {
      return (sizeof(ham_u64_t));
    }

    // Returns the full record and stores it in |dest|; memory must be
    // allocated by the caller
    void get_record(ham_u32_t slot, ByteArray *arena, ham_record_t *record,
                    ham_u32_t flags, ham_u32_t duplicate_index) const {
      bool direct_access = (flags & HAM_DIRECT_ACCESS) != 0;

      // the record is stored inline
      record->size = sizeof(ham_u64_t);

      if (direct_access)
        record->data = (void *)&m_data[slot];
      else {
        if ((record->flags & HAM_RECORD_USER_ALLOC) == 0) {
          arena->resize(record->size);
          record->data = arena->get_ptr();
        }
        memcpy(record->data, &m_data[slot], record->size);
      }
    }

    // Updates the record of a key
    void set_record(ham_u32_t slot, ham_u32_t duplicate_index,
                ham_record_t *record, ham_u32_t flags,
                ham_u32_t *new_duplicate_index = 0) {
      ham_assert(record->size == sizeof(ham_u64_t));
      m_data[slot] = *(ham_u64_t *)record->data;
    }

    // Erases the record
    void erase_record(ham_u32_t slot, ham_u32_t duplicate_index = 0,
                    bool all_duplicates = true) {
      m_data[slot] = 0;
    }

    // Erases a whole slot by shifting all larger records to the "left"
    void erase_slot(size_t node_count, ham_u32_t slot) {
      if (slot < node_count - 1)
        memmove(&m_data[slot], &m_data[slot + 1],
                      sizeof(ham_u64_t) * (node_count - slot - 1));
    }

    // Creates space for one additional record
    void insert_slot(size_t node_count, ham_u32_t slot) {
      if (slot < node_count) {
        memmove(&m_data[slot + 1], &m_data[slot],
                       sizeof(ham_u64_t) * (node_count - slot));
      }
      memset(&m_data[slot], 0, sizeof(ham_u64_t));
    }

    // Copies |count| records from this[sstart] to dest[dstart]
    void copy_to(ham_u32_t sstart, size_t node_count, InternalRecordList &dest,
                    size_t other_count, ham_u32_t dstart) {
      memcpy(&dest.m_data[dstart], &m_data[sstart],
                      sizeof(ham_u64_t) * (node_count - sstart));
    }

    // Sets the record id
    void set_record_id(ham_u32_t slot, ham_u64_t ptr) {
      m_data[slot] = ptr;
    }

    // Returns the record id
    ham_u64_t get_record_id(ham_u32_t slot,
                    ham_u32_t duplicate_index = 0) const {
      return (m_data[slot]);
    }

    // Returns true if there's not enough space for another record
    bool requires_split(size_t node_count, bool vacuumize = false) const {
      return (node_count * sizeof(ham_u64_t) >= m_range_size);
    }

    // Change the capacity; for PAX layouts this just means copying the
    // data from one place to the other
    void change_range_size(size_t node_count, ham_u8_t *new_data_ptr,
                size_t new_range_size, size_t capacity_hint) {
      memmove(new_data_ptr, m_data, node_count * sizeof(ham_u64_t));
      m_data = (ham_u64_t *)new_data_ptr;
      m_range_size = new_range_size;
    }

    // Prints a slot to |out| (for debugging)
    void print(ham_u32_t slot, std::stringstream &out) const {
      out << "(" << get_record_id(slot) << " bytes)";
    }

  private:
    // The parent database of this btree
    LocalDatabase *m_db;

    // The record data is an array of page IDs
    ham_u64_t *m_data;
};

} // namespace PaxLayout

} // namespace hamsterdb

#endif /* HAM_BTREE_RECORDS_INTERNAL_H */
