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
 * The DefaultRecordList provides simplified access to a list of records,
 * where each record is either a 8-byte record identifier (specifying the
 * address of a blob) or is stored inline, if the record's size is <= 8 bytes.
 *
 * Stores 1 byte of flags per record (see btree_flags.h).
 */

#ifndef HAM_BTREE_RECORDS_DEFAULT_H
#define HAM_BTREE_RECORDS_DEFAULT_H

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

class DefaultRecordList
{
  public:
    enum {
      // A flag whether this RecordList has sequential data
      kHasSequentialData = 1
    };

    // Constructor
    DefaultRecordList(LocalDatabase *db, PBtreeNode *node)
      : m_db(db), m_flags(0), m_data(0), m_capacity(0) {
    }

    // Sets the data pointer; required for initialization
    void create(ham_u8_t *data, size_t full_range_size_bytes, size_t capacity) {
      m_flags = data;
      m_data = (ham_u64_t *)&data[capacity];
      m_capacity = capacity;
    }

    // Opens an existing RecordList
    void open(ham_u8_t *data, size_t capacity) {
      m_flags = data;
      m_data = (ham_u64_t *)&data[capacity];
      m_capacity = capacity;
    }

    // Returns the full size of the range
    size_t get_range_size() const {
      return (m_capacity * (sizeof(ham_u64_t) + 1));
    }

    // Calculates the required size for a range with the specified |capacity|
    size_t calculate_required_range_size(size_t node_count,
            size_t new_capacity) const {
      return (new_capacity * (sizeof(ham_u64_t) + 1));
    }

    // Returns the actual record size including overhead
    double get_full_record_size() const {
      return (sizeof(ham_u64_t) + 1);
    }

    // Returns the record counter of a key
    ham_u32_t get_record_count(ham_u32_t slot) const {
      if (unlikely(!is_record_inline(slot) && get_record_id(slot) == 0))
        return (0);
      return (1);
    }

    // Returns the record size
    ham_u64_t get_record_size(ham_u32_t slot,
                    ham_u32_t duplicate_index = 0) const {
      if (is_record_inline(slot))
        return (get_inline_record_size(slot));

      LocalEnvironment *env = m_db->get_local_env();
      return (env->get_blob_manager()->get_blob_size(m_db,
                              get_record_id(slot)));
    }

    // Returns the full record and stores it in |dest|; memory must be
    // allocated by the caller
    void get_record(ham_u32_t slot, ByteArray *arena, ham_record_t *record,
                    ham_u32_t flags, ham_u32_t duplicate_index) const {
      bool direct_access = (flags & HAM_DIRECT_ACCESS) != 0;

      // the record is stored inline
      if (is_record_inline(slot)) {
        record->size = get_inline_record_size(slot);
        if (record->size == 0) {
          record->data = 0;
          return;
        }
        if (flags & HAM_PARTIAL) {
          ham_trace(("flag HAM_PARTIAL is not allowed if record is "
                     "stored inline"));
          throw Exception(HAM_INV_PARAMETER);
        }
        if (direct_access)
          record->data = (void *)&m_data[slot];
        else {
          if ((record->flags & HAM_RECORD_USER_ALLOC) == 0) {
            arena->resize(record->size);
            record->data = arena->get_ptr();
          }
          memcpy(record->data, &m_data[slot], record->size);
        }
        return;
      }

      // the record is stored as a blob
      LocalEnvironment *env = m_db->get_local_env();
      env->get_blob_manager()->read(m_db, get_record_id(slot), record,
                      flags, arena);
    }

    // Updates the record of a key
    void set_record(ham_u32_t slot, ham_u32_t duplicate_index,
                ham_record_t *record, ham_u32_t flags,
                ham_u32_t *new_duplicate_index = 0) {
      ham_u64_t ptr = get_record_id(slot);
      LocalEnvironment *env = m_db->get_local_env();

      // key does not yet exist
      if (!ptr && !is_record_inline(slot)) {
        // a new inline key is inserted
        if (record->size <= sizeof(ham_u64_t)) {
          set_record_data(slot, record->data, record->size);
        }
        // a new (non-inline) key is inserted
        else {
          ptr = env->get_blob_manager()->allocate(m_db, record, flags);
          set_record_id(slot, ptr);
        }
        return;
      }

      // an inline key exists
      if (is_record_inline(slot)) {
        // disable small/tiny/empty flags
        set_record_flags(slot, get_record_flags(slot)
                        & ~(BtreeRecord::kBlobSizeSmall
                            | BtreeRecord::kBlobSizeTiny
                            | BtreeRecord::kBlobSizeEmpty));
        // ... and is overwritten with another inline key
        if (record->size <= sizeof(ham_u64_t)) {
          set_record_data(slot, record->data, record->size);
        }
        // ... or with a (non-inline) key
        else {
          ptr = env->get_blob_manager()->allocate(m_db, record, flags);
          set_record_id(slot, ptr);
        }
        return;
      }

      // a (non-inline) key exists
      if (ptr) {
        // ... and is overwritten by a inline key
        if (record->size <= sizeof(ham_u64_t)) {
          env->get_blob_manager()->erase(m_db, ptr);
          set_record_data(slot, record->data, record->size);
        }
        // ... and is overwritten by a (non-inline) key
        else {
          ptr = env->get_blob_manager()->overwrite(m_db, ptr, record, flags);
          set_record_id(slot, ptr);
        }
        return;
      }

      ham_assert(!"shouldn't be here");
      throw Exception(HAM_INTERNAL_ERROR);
    }

    // Erases the record
    void erase_record(ham_u32_t slot, ham_u32_t duplicate_index = 0,
                    bool all_duplicates = true) {
      if (is_record_inline(slot)) {
        remove_inline_record(slot);
        return;
      }

      // now erase the blob
      m_db->get_local_env()->get_blob_manager()->erase(m_db,
                      get_record_id(slot), 0);
      set_record_id(slot, 0);
    }

    // Erases a whole slot by shifting all larger records to the "left"
    void erase_slot(size_t node_count, ham_u32_t slot) {
      if (slot < node_count - 1) {
        memmove(&m_flags[slot], &m_flags[slot + 1], node_count - slot - 1);
        memmove(&m_data[slot], &m_data[slot + 1],
                        sizeof(ham_u64_t) * (node_count - slot - 1));
      }
    }

    // Creates space for one additional record
    void insert_slot(size_t node_count, ham_u32_t slot) {
      if (slot < node_count) {
        memmove(&m_flags[slot + 1], &m_flags[slot], node_count - slot);
        memmove(&m_data[slot + 1], &m_data[slot],
                       sizeof(ham_u64_t) * (node_count - slot));
      }
      m_flags[slot] = 0;
      memset(&m_data[slot], 0, sizeof(ham_u64_t));
    }

    // Copies |count| records from this[sstart] to dest[dstart]
    void copy_to(ham_u32_t sstart, size_t node_count, DefaultRecordList &dest,
                    size_t other_count, ham_u32_t dstart) {
      memcpy(&dest.m_flags[dstart], &m_flags[sstart], (node_count - sstart));
      memcpy(&dest.m_data[dstart], &m_data[sstart],
                      sizeof(ham_u64_t) * (node_count - sstart));
    }

    // Sets the record id
    void set_record_id(ham_u32_t slot, ham_u64_t ptr) {
      m_data[slot] = ptr;
    }

    // Returns the record id
    ham_u64_t get_record_id(ham_u32_t slot, ham_u32_t duplicate_index = 0)
                    const {
      return (m_data[slot]);
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
      // shift "to the right"? then first shift key data, otherwise
      // the flags might overwrite the data
      if (new_data_ptr > m_flags) {
        memmove(&new_data_ptr[new_capacity], m_data,
                node_count * sizeof(ham_u64_t));
        memmove(new_data_ptr, m_flags, node_count);
      }
      else {
        memmove(new_data_ptr, m_flags, node_count);
        memmove(&new_data_ptr[new_capacity], m_data,
                node_count * sizeof(ham_u64_t));
      }
      m_flags = new_data_ptr;
      m_data = (ham_u64_t *)&new_data_ptr[new_capacity];
      m_capacity = new_capacity;
    }

    // Prints a slot to |out| (for debugging)
    void print(ham_u32_t slot, std::stringstream &out) const {
      out << "(" << get_record_size(slot) << " bytes)";
    }

  private:
    // Sets record data
    void set_record_data(ham_u32_t slot, const void *ptr, size_t size) {
      ham_u8_t flags = get_record_flags(slot);
      flags &= ~(BtreeRecord::kBlobSizeSmall
                      | BtreeRecord::kBlobSizeTiny
                      | BtreeRecord::kBlobSizeEmpty);

      if (size == 0) {
        m_data[slot] = 0;
        set_record_flags(slot, flags | BtreeRecord::kBlobSizeEmpty);
      }
      else if (size < 8) {
        /* the highest byte of the record id is the size of the blob */
        char *p = (char *)&m_data[slot];
        p[sizeof(ham_u64_t) - 1] = size;
        memcpy(&m_data[slot], ptr, size);
        set_record_flags(slot, flags | BtreeRecord::kBlobSizeTiny);
      }
      else if (size == 8) {
        memcpy(&m_data[slot], ptr, size);
        set_record_flags(slot, flags | BtreeRecord::kBlobSizeSmall);
      }
      else {
        ham_assert(!"shouldn't be here");
        set_record_flags(slot, flags);
      }
    }

    // Returns the record flags of a given |slot|
    ham_u8_t get_record_flags(ham_u32_t slot, ham_u32_t duplicate_index = 0)
                    const {
      return (m_flags[slot]);
    }

    // Sets the record flags of a given |slot|
    void set_record_flags(ham_u32_t slot, ham_u8_t flags) {
      m_flags[slot] = flags;
    }

    // Returns the size of an inline record
    ham_u32_t get_inline_record_size(ham_u32_t slot) const {
      ham_u8_t flags = get_record_flags(slot);
      ham_assert(is_record_inline(slot));
      if (flags & BtreeRecord::kBlobSizeTiny) {
        /* the highest byte of the record id is the size of the blob */
        char *p = (char *)&m_data[slot];
        return (p[sizeof(ham_u64_t) - 1]);
      }
      if (flags & BtreeRecord::kBlobSizeSmall)
        return (sizeof(ham_u64_t));
      if (flags & BtreeRecord::kBlobSizeEmpty)
        return (0);
      ham_assert(!"shouldn't be here");
      return (0);
    }

    // Returns true if the record is inline, false if the record is a blob
    bool is_record_inline(ham_u32_t slot) const {
      ham_u8_t flags = get_record_flags(slot);
      return ((flags & BtreeRecord::kBlobSizeTiny)
              || (flags & BtreeRecord::kBlobSizeSmall)
              || (flags & BtreeRecord::kBlobSizeEmpty) != 0);
    }

    // Removes an inline record; returns the updated record flags
    void remove_inline_record(ham_u32_t slot) {
      ham_u8_t flags = get_record_flags(slot);
      m_data[slot] = 0;
      set_record_flags(slot,
                      flags & ~(BtreeRecord::kBlobSizeSmall
                        | BtreeRecord::kBlobSizeTiny
                        | BtreeRecord::kBlobSizeEmpty));
    }

    // The parent database of this btree
    LocalDatabase *m_db;

    // The record flags
    ham_u8_t *m_flags;

    // The actual record data - an array of 64bit record IDs
    ham_u64_t *m_data;

    // The capacity of m_data
    size_t m_capacity;
};

} // namespace PaxLayout

} // namespace hamsterdb

#endif /* HAM_BTREE_RECORDS_DEFAULT_H */
