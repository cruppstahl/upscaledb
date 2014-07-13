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

/**
 * Btree node layout for variable length keys/records and/or duplicates
 * ====================================================================
 *
 * This is the default hamsterdb layout. It is chosen for
 * 1. variable length keys (with or without duplicates)
 * 2. fixed length keys with duplicates
 *
 * Like the PAX layout implemented in btree_impl_pax.h, the layout implemented
 * here stores key data and records separated from each other. This layout is
 * more complex, because it is capable of resizing the KeyList and RecordList
 * if the node becomes full.
 *
 * Duplicate records are stored inline till a certain threshold limit
 * (m_duptable_threshold) is reached. In this case the duplicates are stored
 * in a separate blob (the DuplicateTable), and the previously occupied storage
 * in the node is reused for other records.
 *
 * Each key and record group (= all duplicate records of a key) is stored in
 * a "chunk", and the chunks are managed by an upfront index which contains
 * offset and size of each chunk. The index also keeps track of deleted chunks.
 *
 * The actual chunk data contains the key's data (which can be a 64bit blob
 * ID if the key is too big), and the record's data.
 *
 * To avoid expensive memcpy-operations, erasing a key only affects this
 * upfront index: the relevant slot is moved to a "freelist". This freelist
 * contains the same meta information as the index table.
 *
 * The flat memory layout looks like this:
 *
 * |Idx1|Idx2|...|Idxn|F1|F2|...|Fn|...(space)...|Key1Rec1|Key2Rec2|...|
 *
 * ... where Idx<n> are the indices (of slot <n>)
 *     where F<n> are freelist entries
 *     where Key<n> is the key data of slot <n>
 *        ... directly followed by one or more Records.
 *
 * In addition, the first few bytes in the node store the following
 * information:
 *   0  (4 bytes): total capacity of index keys (used keys + freelist)
 *   4  (4 bytes): number of used freelist entries
 *   8  (4 bytes): offset for the next key at the end of the page
 *
 * In total, |capacity| contains the number of maximum keys (and index
 * entries) that can be stored in the node. The number of used index keys
 * is in |m_node->get_count()|. The number of used freelist entries is
 * returned by |get_freelist_count()|. The freelist indices start directly
 * after the key indices. The key space (with key data and records) starts at
 * N * capacity, where |N| is the size of an index entry (the size depends
 * on the actual btree configuration, i.e. whether key size is fixed,
 * duplicates are used etc).
 *
 * If keys exceed a certain threshold (get_extended_threshold()), they're moved
 * to a blob and the flag |kExtendedKey| is set for this key. These extended
 * keys are cached in a std::map to improve performance.
 *
 * If records have fixed length then all records of a key (with duplicates)
 * are stored next to each other. If they have variable length then each of
 * these records is stored with 1 byte for flags:
 *   Rec1|F1|Rec2|F2|...
 * where Recn is an 8 bytes record-ID (offset in the file) OR inline record,
 * and F1 is 1 byte for flags (kBlobSizeSmall etc).
 */

#ifndef HAM_BTREE_IMPL_DEFAULT_H__
#define HAM_BTREE_IMPL_DEFAULT_H__

#include <algorithm>
#include <iostream>
#include <vector>
#include <map>

#include "globals.h"
#include "util.h"
#include "page.h"
#include "btree_node.h"
#include "blob_manager.h"
#include "env_local.h"
#include "btree_index.h"

#ifdef WIN32
// MSVC: disable warning about use of 'this' in base member initializer list
#  pragma warning(disable:4355)
#  undef min  // avoid MSVC conflicts with std::min
#endif

namespace hamsterdb {

namespace DefLayout {

// helper function which returns true if a record is inline
static bool is_record_inline(ham_u8_t flags) {
  return (flags != 0);
}

//
// A helper class for dealing with extended duplicate tables
//
//  Byte [0..3] - count
//       [4..7] - capacity
//       [8.. [ - the record list
//                  if m_inline_records:
//                      each record has n bytes record-data
//                  else
//                      each record has 1 byte flags, n bytes record-data
//
class DuplicateTable
{
  public:
    // Constructor; the flag |inline_records| indicates whether record
    // flags should be stored for each record. |record_size| is the
    // fixed length size of each record, or HAM_RECORD_SIZE_UNLIMITED
    DuplicateTable(LocalDatabase *db, bool inline_records, size_t record_size)
      : m_db(db), m_store_flags(!inline_records), m_record_size(record_size),
        m_inline_records(inline_records), m_table_id(0) {
    }

    // Destructor; deletes all blobs if running in-memory
    ~DuplicateTable() {
    }

    // Allocates and fills the table; returns the new table id.
    // Can allocate empty tables (required for testing purposes).
    // The initial capacity of the table is twice the current
    // |record_count|.
    ham_u64_t create(const ham_u8_t *data, size_t record_count) {
      ham_assert(m_table_id == 0);

      // This sets the initial capacity as described above
      size_t capacity = record_count * 2;
      m_table.resize(8 + capacity * get_record_width());
      if (likely(record_count > 0))
        m_table.overwrite(8, data, (m_inline_records
                                    ? m_record_size * record_count
                                    : 9 * record_count));

      set_record_count(record_count);
      set_record_capacity(record_count * 2);

      // Flush the table to disk, returns the blob-id of the table
      return (flush_duplicate_table());
    }

    // Reads the table from disk
    void open(ham_u64_t table_id) {
      ham_record_t record = {0};
      m_db->get_local_env()->get_blob_manager()->read(m_db, table_id, &record,
                      0, &m_table);
      m_table_id = table_id;
    }

    // Returns the number of duplicates in that table
    ham_u32_t get_record_count() const {
      ham_assert(m_table.get_size() > 4);
      return (*(ham_u32_t *)m_table.get_ptr());
    }

    // Returns the record size of a duplicate
    ham_u32_t get_record_size(ham_u32_t duplicate_index) {
      ham_assert(duplicate_index < get_record_count());
      if (m_inline_records)
        return (m_record_size);
      ham_assert(m_store_flags == true);

      ham_u8_t *precord_flags;
      ham_u8_t *p = get_record_data(duplicate_index, &precord_flags);
      ham_u8_t flags = *(precord_flags);

      if (flags & BtreeRecord::kBlobSizeTiny)
        return (p[sizeof(ham_u64_t) - 1]);
      if (flags & BtreeRecord::kBlobSizeSmall)
        return (sizeof(ham_u64_t));
      if (flags & BtreeRecord::kBlobSizeEmpty)
        return (0);

      ham_u64_t blob_id = *(ham_u64_t *)p;
      return (m_db->get_local_env()->get_blob_manager()->get_blob_size(m_db,
                              blob_id));
    }

    // Returns the full record and stores it in |record|. |flags| can
    // be 0 or |HAM_DIRECT_ACCESS|, |HAM_PARTIAL|. These are the default
    // flags of ham_db_find et al.
    void get_record(ham_u32_t duplicate_index, ByteArray *arena,
                    ham_record_t *record, ham_u32_t flags) {
      ham_assert(duplicate_index < get_record_count());
      bool direct_access = (flags & HAM_DIRECT_ACCESS) != 0;

      ham_u8_t *precord_flags;
      ham_u8_t *p = get_record_data(duplicate_index, &precord_flags);
      ham_u8_t record_flags = precord_flags ? *precord_flags : 0;

      if (m_inline_records) {
        if (flags & HAM_PARTIAL) {
          ham_trace(("flag HAM_PARTIAL is not allowed if record is "
                     "stored inline"));
          throw Exception(HAM_INV_PARAMETER);
        }

        record->size = m_record_size;
        if (direct_access)
          record->data = p;
        else {
          if ((record->flags & HAM_RECORD_USER_ALLOC) == 0) {
            arena->resize(record->size);
            record->data = arena->get_ptr();
          }
          memcpy(record->data, p, m_record_size);
        }
        return;
      }

      ham_assert(m_store_flags == true);

      if (record_flags & BtreeRecord::kBlobSizeEmpty) {
        record->data = 0;
        record->size = 0;
        return;
      }

      if (record_flags & BtreeRecord::kBlobSizeTiny) {
        record->size = p[sizeof(ham_u64_t) - 1];
        if (direct_access)
          record->data = &p[0];
        else {
          if ((record->flags & HAM_RECORD_USER_ALLOC) == 0) {
            arena->resize(record->size);
            record->data = arena->get_ptr();
          }
          memcpy(record->data, &p[0], record->size);
        }
        return;
      }

      if (record_flags & BtreeRecord::kBlobSizeSmall) {
        record->size = sizeof(ham_u64_t);
        if (direct_access)
          record->data = &p[0];
        else {
          if ((record->flags & HAM_RECORD_USER_ALLOC) == 0) {
            arena->resize(record->size);
            record->data = arena->get_ptr();
          }
          memcpy(record->data, &p[0], record->size);
        }
        return;
      }

      ham_u64_t blob_id = *(ham_u64_t *)p;

      // the record is stored as a blob
      LocalEnvironment *env = m_db->get_local_env();
      env->get_blob_manager()->read(m_db, blob_id, record, flags, arena);
    }

    // Updates the record of a key. Analog to the set_record() method
    // of the NodeLayout class. Returns the new table id and the
    // new duplicate index, if |new_duplicate_index| is not null.
    ham_u64_t set_record(ham_u32_t duplicate_index, ham_record_t *record,
                    ham_u32_t flags, ham_u32_t *new_duplicate_index) {
      BlobManager *blob_manager = m_db->get_local_env()->get_blob_manager();

      // the duplicate is overwritten
      if (flags & HAM_OVERWRITE) {
        ham_u8_t *record_flags = 0;
        ham_u8_t *p = get_record_data(duplicate_index, &record_flags);

        // the record is stored inline w/ fixed length?
        if (m_inline_records) {
          ham_assert(record->size == m_record_size);
          memcpy(p, record->data, record->size);
          return (flush_duplicate_table());
        }
        // the existing record is a blob
        if (!is_record_inline(*record_flags)) {
          ham_u64_t ptr = *(ham_u64_t *)p;
          // overwrite the blob record
          if (record->size > sizeof(ham_u64_t)) {
            *(ham_u64_t *)p = blob_manager->overwrite(m_db, ptr, record, flags);
            return (flush_duplicate_table());
          }
          // otherwise delete it and continue
          blob_manager->erase(m_db, ptr, 0);
        }
      }

      // If the key is not overwritten but inserted or appended: create a
      // "gap" in the table
      else {
        ham_u32_t count = get_record_count();

        // check for overflow
        if (unlikely(count == 0xffffffff)) {
          ham_log(("Duplicate table overflow"));
          throw Exception(HAM_LIMITS_REACHED);
        }

        // adjust flags
        if (flags & HAM_DUPLICATE_INSERT_BEFORE && duplicate_index == 0)
          flags |= HAM_DUPLICATE_INSERT_FIRST;
        else if (flags & HAM_DUPLICATE_INSERT_AFTER) {
          if (duplicate_index == count)
            flags |= HAM_DUPLICATE_INSERT_LAST;
          else {
            flags |= HAM_DUPLICATE_INSERT_BEFORE;
            duplicate_index++;
          }
        }

        // resize the table, if necessary
        if (unlikely(count == get_record_capacity()))
          grow_duplicate_table();

        // handle overwrites or inserts/appends
        if (flags & HAM_DUPLICATE_INSERT_FIRST) {
          if (count) {
            ham_u8_t *ptr = get_raw_record_data(0);
            memmove(ptr + get_record_width(), ptr, count * get_record_width());
          }
          duplicate_index = 0;
        }
        else if (flags & HAM_DUPLICATE_INSERT_BEFORE) {
          ham_u8_t *ptr = get_raw_record_data(duplicate_index);
          memmove(ptr + get_record_width(), ptr,
                      (count - duplicate_index) * get_record_width());
        }
        else // HAM_DUPLICATE_INSERT_LAST
          duplicate_index = count;

        set_record_count(count + 1);
      }

      ham_u8_t *record_flags = 0;
      ham_u8_t *p = get_record_data(duplicate_index, &record_flags);

      // store record inline?
      if (m_inline_records) {
          ham_assert(m_record_size == record->size);
          if (m_record_size > 0)
            memcpy(p, record->data, record->size);
      }
      else if (record->size == 0) {
        memcpy(p, "\0\0\0\0\0\0\0\0", 8);
        *record_flags = BtreeRecord::kBlobSizeEmpty;
      }
      else if (record->size < sizeof(ham_u64_t)) {
        p[sizeof(ham_u64_t) - 1] = (ham_u8_t)record->size;
        memcpy(&p[0], record->data, record->size);
        *record_flags = BtreeRecord::kBlobSizeTiny;
      }
      else if (record->size == sizeof(ham_u64_t)) {
        memcpy(&p[0], record->data, record->size);
        *record_flags = BtreeRecord::kBlobSizeSmall;
      }
      else {
        *record_flags = 0;
        ham_u64_t blob_id = blob_manager->allocate(m_db, record, flags);
        memcpy(p, &blob_id, sizeof(blob_id));
      }

      if (new_duplicate_index)
        *new_duplicate_index = duplicate_index;

      // write the duplicate table to disk and return the table-id
      return (flush_duplicate_table());
    }

    // Deletes a record from the table; also adjusts the count. If
    // |all_duplicates| is true or if the last element of the table is
    // deleted then the table itself will also be deleted. Returns 0
    // if this is the case, otherwise returns the table id.
    ham_u64_t erase_record(ham_u32_t duplicate_index, bool all_duplicates) {
      ham_u32_t count = get_record_count();

      if (count == 1 && duplicate_index == 0)
        all_duplicates = true;

      if (all_duplicates) {
        if (m_store_flags && !m_inline_records) {
          for (ham_u32_t i = 0; i < count; i++) {
            ham_u8_t *record_flags;
            ham_u8_t *p = get_record_data(i, &record_flags);
            if (is_record_inline(*record_flags))
              continue;
            if (*(ham_u64_t *)p != 0) {
              m_db->get_local_env()->get_blob_manager()->erase(m_db,
                              *(ham_u64_t *)p);
              *(ham_u64_t *)p = 0;
            }
          }
        }
        if (m_table_id != 0)
          m_db->get_local_env()->get_blob_manager()->erase(m_db, m_table_id);
        set_record_count(0);
        m_table_id = 0;
        return (0);
      }

      ham_assert(count > 0 && duplicate_index < count);

      ham_u8_t *record_flags;
      ham_u8_t *lhs = get_record_data(duplicate_index, &record_flags);
      if (record_flags != 0 && *record_flags == 0 && !m_inline_records) {
        m_db->get_local_env()->get_blob_manager()->erase(m_db,
                          *(ham_u64_t *)lhs);
        *(ham_u64_t *)lhs = 0;
      }

      if (duplicate_index < count - 1) {
        lhs = get_raw_record_data(duplicate_index);
        ham_u8_t *rhs = lhs + get_record_width();
        memmove(lhs, rhs, get_record_width() * (count - duplicate_index - 1));
      }

      // adjust the counter
      set_record_count(count - 1);

      // write the duplicate table to disk and return the table-id
      return (flush_duplicate_table());
    }

    // Returns the maximum capacity of elements in a duplicate table
    // This method could be private, but it's required by the unittests
    ham_u32_t get_record_capacity() const {
      ham_assert(m_table.get_size() >= 8);
      return (*(ham_u32_t *)((ham_u8_t *)m_table.get_ptr() + 4));
    }

  private:
    // Doubles the capacity of the ByteArray which backs the table
    void grow_duplicate_table() {
      ham_u32_t capacity = get_record_capacity();
      if (capacity == 0)
        capacity = 8;
      m_table.resize(8 + (capacity * 2) * get_record_width());
      set_record_capacity(capacity * 2);
    }

    // Writes the modified duplicate table to disk; returns the new
    // table-id
    ham_u64_t flush_duplicate_table() {
      ham_record_t record = {0};
      record.data = m_table.get_ptr();
      record.size = m_table.get_size();
      if (!m_table_id)
        m_table_id = m_db->get_local_env()->get_blob_manager()->allocate(m_db,
                        &record, 0);
      else
        m_table_id = m_db->get_local_env()->get_blob_manager()->overwrite(m_db,
                        m_table_id, &record, 0);
      return (m_table_id);
    }

    // Returns the size of a record structure in the ByteArray
    size_t get_record_width() const {
      if (m_inline_records)
        return (m_record_size);
      ham_assert(m_store_flags == true);
      return (sizeof(ham_u64_t) + 1);
    }

    // Returns a pointer to the record data (including flags)
    ham_u8_t *get_raw_record_data(ham_u32_t duplicate_index) {
      if (m_inline_records)
        return ((ham_u8_t *)m_table.get_ptr()
                              + 8
                              + m_record_size * duplicate_index);
      else
        return ((ham_u8_t *)m_table.get_ptr()
                              + 8
                              + 9 * duplicate_index);
    }

    // Returns a pointer to the record data, and the flags
    ham_u8_t *get_record_data(ham_u32_t duplicate_index,
                    ham_u8_t **pflags = 0) {
      ham_u8_t *p = get_raw_record_data(duplicate_index);
      if (m_store_flags) {
        if (pflags)
          *pflags = p++;
        else
          p++;
      }
      else if (pflags)
        *pflags = 0;
      return (p);
    }

    // Sets the number of used elements in a duplicate table
    void set_record_count(ham_u32_t count) {
      *(ham_u32_t *)m_table.get_ptr() = count;
    }

    // Sets the maximum capacity of elements in a duplicate table
    void set_record_capacity(ham_u32_t capacity) {
      ham_assert(m_table.get_size() >= 8);
      *(ham_u32_t *)((ham_u8_t *)m_table.get_ptr() + 4) = capacity;
    }

    // The database
    LocalDatabase *m_db;

    // Whether to store flags per record or not (true unless records
    // have constant length)
    bool m_store_flags;

    // The constant length record size, or HAM_RECORD_SIZE_UNLIMITED
    size_t m_record_size;

    // Stores the actual data of the table
    ByteArray m_table;

    // True if records are inline
    bool m_inline_records;

    // The blob id for persisting the table
    ham_u64_t m_table_id;
};

//
// A helper class to sort ranges; used during validation of the up-front
// index in check_index_integrity()
//
struct SortHelper {
  ham_u32_t offset;
  ham_u32_t slot;

  bool operator<(const SortHelper &rhs) const {
    return (offset < rhs.offset);
  }
};

static bool
sort_by_offset(const SortHelper &lhs, const SortHelper &rhs) {
  return (lhs.offset < rhs.offset);
}

//
// A small index which manages variable length buffers. Used to manage
// variable length keys or records.
//
// The UpfrontIndex manages a range of bytes, organized in variable length
// |chunks|, assigned at initialization time when calling |allocate()|
// or |open()|. 
// 
// These chunks are organized in |slots|, each slot stores the offset and
// the size of the chunk data. The offset is stored as 16- or 32-bit, depending
// on the page size. The size is always a 16bit integer.
//
// The number of used slots is not stored in the UpfrontIndex, since it is
// already managed in the caller (this is equal to |PBtreeNode::get_count()|).
// Therefore you will see a lot of methods receiving a |node_count| parameter.
//
// Deleted chunks are moved to a |freelist|, which is simply a list of slots
// directly following those slots that are in use.
//
// In addition, the UpfrontIndex keeps track of the unused space at the end
// of the range (via |get_next_offset()|), in order to allow a fast
// allocation of space.
//
// The UpfrontIndex stores metadata at the beginning:
//     [0..3]  freelist count
//     [4..7]  next offset
//     [8..11] range size
//
// Data is stored in the following layout:
// |metadata|slot1|slot2|...|slotN|free1|free2|...|freeM|data1|data2|...|dataN|
//
class UpfrontIndex
{
    enum {
      // width of the 'size' field
      kSizeofSize = 1 // sizeof(ham_u16_t)
    };

  public:
    enum {
      // for freelist_count, next_offset, range_size
      kPayloadOffset = 12,
    };

    // Constructor; creates an empty index which needs to be initialized
    // with |create()| or |open()|.
    UpfrontIndex(LocalDatabase *db)
      : m_data(0), m_capacity(0), m_vacuumize_counter(0) {
      size_t page_size = db->get_local_env()->get_page_size();
      if (page_size <= 64 * 1024)
        m_sizeof_offset = 2;
      else
        m_sizeof_offset = 4;
    }

    // Initialization routine; sets data pointer, range size and the
    // initial capacity.
    void create(ham_u8_t *data, size_t full_range_size_bytes, size_t capacity) {
      m_data = data;
      m_capacity = capacity;
      set_full_range_size(full_range_size_bytes);
      clear();
    }

    // "Opens" an existing index from memory. This method sets the data
    // pointer and initializes itself.
    void open(ham_u8_t *data, size_t capacity) {
      m_data = data;
      m_capacity = capacity;
      // the vacuumize-counter is not persisted, therefore
      // pretend that the counter is very high; in worst case this will cause
      // an invalid call to vacuumize(), which is not a problem
      if (get_freelist_count())
        m_vacuumize_counter = get_range_size();
    }

    // Returns the capacity; required for tests
    size_t get_capacity() const {
      return (m_capacity);
    }

    // Changes the capacity of the index; used to resize the KeyList or
    // RecordList
    void change_capacity(size_t node_count, ham_u8_t *new_data_ptr,
            size_t full_range_size_bytes, size_t new_capacity) {
      size_t used_data_size = get_next_offset(node_count); 
      ham_u8_t *src = &m_data[kPayloadOffset
                            + m_capacity * get_full_index_size()];
      ham_u8_t *dst = &new_data_ptr[kPayloadOffset
                            + new_capacity * get_full_index_size()];
      ham_assert(dst - new_data_ptr + used_data_size <= full_range_size_bytes);
      // shift "to the right"? Then first move the data and afterwards
      // the index
      if (dst > src) {
        memmove(dst, src, used_data_size);
        memmove(new_data_ptr, m_data,
                kPayloadOffset + new_capacity * get_full_index_size());
      }
      // vice versa otherwise
      else {
        if (new_data_ptr != m_data)
          memmove(new_data_ptr, m_data,
                  kPayloadOffset + new_capacity * get_full_index_size());
        memmove(dst, src, used_data_size);
      }
      m_data = new_data_ptr;
      m_capacity = new_capacity;
      set_next_offset(used_data_size);
      set_full_range_size(full_range_size_bytes);
    }

    // Returns the size of a single index entry
    size_t get_full_index_size() const {
      return (m_sizeof_offset + kSizeofSize);
    }

    // Transforms a relative offset of the payload data to an absolute offset
    // in |m_data|
    ham_u32_t get_absolute_offset(ham_u32_t offset) const {
      return (offset
                      + kPayloadOffset
                      + m_capacity * get_full_index_size());
    }

    // Returns the absolute start offset of a chunk
    ham_u32_t get_absolute_chunk_offset(ham_u32_t slot) const {
      return (get_absolute_offset(get_chunk_offset(slot)));
    }

    // Returns the relative start offset of a chunk
    ham_u32_t get_chunk_offset(ham_u32_t slot) const {
      ham_u8_t *p = &m_data[kPayloadOffset + get_full_index_size() * slot];
      if (m_sizeof_offset == 2)
        return (*(ham_u16_t *)p);
      else {
        ham_assert(m_sizeof_offset == 4);
        return (*(ham_u32_t *)p);
      }
    }

    // Returns the size of a chunk
    ham_u16_t get_chunk_size(ham_u32_t slot) const {
      return (m_data[kPayloadOffset + get_full_index_size() * slot
                                + m_sizeof_offset]);
    }

    // Sets the size of a chunk (does NOT actually resize the chunk!)
    void set_chunk_size(ham_u32_t slot, ham_u16_t size) {
      ham_assert(size <= 255);
      m_data[kPayloadOffset + get_full_index_size() * slot
                                + m_sizeof_offset] = (ham_u8_t)size;
    }

    // Increases the "vacuumize-counter", which is an indicator whether
    // rearranging the node makes sense
    void increase_vacuumize_counter(size_t gap_size) {
      m_vacuumize_counter += gap_size;
    }

    // Returns the 'vacuumize counter' - an indicator how much space can
    // be gained by calling vacuumize()
    size_t get_vacuumize_counter() const {
      return (m_vacuumize_counter);
    }

    // Returns true if this index has at least one free slot available.
    // |node_count| is the number of used slots (this is managed by the caller)
    bool can_insert_slot(size_t node_count) {
      return (likely(node_count + get_freelist_count() < m_capacity));
    }

    // Inserts a slot at the position |slot|. |node_count| is the number of
    // used slots (this is managed by the caller)
    void insert_slot(size_t node_count, ham_u32_t slot) {
      ham_assert(can_insert_slot(node_count) == true);

      size_t slot_size = get_full_index_size();
      size_t total_count = node_count + get_freelist_count();
      ham_u8_t *p = &m_data[kPayloadOffset + slot_size * slot];
      if (total_count > 0 && slot < total_count) {
        // create a gap in the index
        memmove(p + slot_size, p, slot_size * (total_count - slot));
     }

      // now fill the gap
      memset(p, 0, slot_size);
    }

    // Erases a slot at the position |slot|
    // |node_count| is the number of used slots (this is managed by the caller)
    void erase_slot(size_t node_count, ham_u32_t slot) {
      size_t slot_size = get_full_index_size();
      size_t total_count = node_count + get_freelist_count();

      ham_assert(slot < total_count);

      set_freelist_count(get_freelist_count() + 1);

      size_t chunk_size = get_chunk_size(slot);

      increase_vacuumize_counter(chunk_size);

      // nothing to do if we delete the very last (used) slot; the freelist
      // counter was already incremented, the used counter is decremented
      // by the caller
      if (slot == node_count - 1)
        return;

      size_t chunk_offset = get_chunk_offset(slot);

      // shift all items to the left
      ham_u8_t *p = &m_data[kPayloadOffset + slot_size * slot];
      memmove(p, p + slot_size, slot_size * (total_count - slot));

      // then copy the deleted chunk to the freelist
      set_chunk_offset(total_count - 1, chunk_offset);
      set_chunk_size(total_count - 1, chunk_size);
    }

    // Adds a chunk to the freelist. Will not do anything if the node
    // is already full.
    void add_to_freelist(size_t node_count, ham_u32_t chunk_offset,
                    ham_u32_t chunk_size) {
      size_t total_count = node_count + get_freelist_count();
      if (likely(total_count < m_capacity)) {
        set_freelist_count(get_freelist_count() + 1);
        set_chunk_size(total_count, chunk_size);
        set_chunk_offset(total_count, chunk_offset);
      }
    }

    // Returns true if this page has enough space to store at least |num_bytes|
    // bytes.
    bool can_allocate_space(size_t node_count, size_t num_bytes) {
      // first check if we can append the data; this is the cheapest check,
      // therefore it comes first
      if (get_next_offset(node_count) + num_bytes <= get_usable_data_size())
        return (true);

      // otherwise check the freelist
      ham_u32_t total_count = node_count + get_freelist_count();
      for (ham_u32_t i = node_count; i < total_count; i++)
        if (get_chunk_size(i) >= num_bytes)
          return (true);
      return (false);
    }

    // Allocates space for a |slot| and returns the offset of that chunk
    ham_u32_t allocate_space(ham_u32_t node_count, ham_u32_t slot,
                    size_t num_bytes) {
      ham_assert(can_allocate_space(node_count, num_bytes));

      size_t next_offset = get_next_offset(node_count);

      // try to allocate space at the end of the node
      if (next_offset + num_bytes <= get_usable_data_size()) {
        ham_u32_t offset = get_chunk_offset(slot);
        // if this slot's data is at the very end then maybe it can be
        // resized without actually moving the data
        if (unlikely(next_offset == offset + get_chunk_size(slot))) {
          set_next_offset(offset + num_bytes);
          set_chunk_size(slot, num_bytes);
          return (offset);
        }
        set_next_offset(next_offset + num_bytes);
        set_chunk_offset(slot, next_offset);
        set_chunk_size(slot, num_bytes);
        return (next_offset);
      }

      size_t slot_size = get_full_index_size();

      // otherwise check the freelist
      ham_u32_t total_count = node_count + get_freelist_count();
      for (ham_u32_t i = node_count; i < total_count; i++) {
        ham_u32_t chunk_size = get_chunk_size(i);
        ham_u32_t chunk_offset = get_chunk_offset(i);
        if (chunk_size >= num_bytes) {
          // update next_offset?
          if (unlikely(next_offset == chunk_offset + chunk_size))
            invalidate_next_offset();
          else if (unlikely(next_offset == get_chunk_offset(slot)
                                  + get_chunk_size(slot)))
            invalidate_next_offset();
          // copy the chunk to the new slot
          set_chunk_size(slot, num_bytes);
          set_chunk_offset(slot, chunk_offset);
          // remove from the freelist
          if (i < total_count - 1) {
            ham_u8_t *p = &m_data[kPayloadOffset + slot_size * i];
            memmove(p, p + slot_size, slot_size * (total_count - i - 1));
          }
          set_freelist_count(get_freelist_count() - 1);
          return (get_chunk_offset(slot));
        }
      }

      ham_assert(!"shouldn't be here");
      throw Exception(HAM_INTERNAL_ERROR);
    }

    // Returns true if |key| cannot be inserted because a split is required.
    // Unlike implied by the name, this function will try to re-arrange the
    // node in order for the key to fit in.
    bool requires_split(ham_u32_t node_count, size_t required_size) {
      return (!can_insert_slot(node_count)
                || !can_allocate_space(node_count, required_size));
    }

    // Verifies that there are no overlapping chunks
    void check_integrity(ham_u32_t node_count) const {
      typedef std::pair<ham_u32_t, ham_u32_t> Range;
      //typedef std::vector<Range> RangeVec;
      ham_u32_t total_count = node_count + get_freelist_count();

      ham_assert(node_count > 1
                    ? get_next_offset(node_count) > 0
                    : true);

      if (total_count > m_capacity) {
        ham_trace(("integrity violated: total count %u (%u+%u) > capacity %u",
                    total_count, node_count, get_freelist_count(),
                    m_capacity));
        throw Exception(HAM_INTEGRITY_VIOLATED);
      }

      //RangeVec ranges;
      //ranges.reserve(total_count);
      ham_u32_t next_offset = 0;
      for (ham_u32_t i = 0; i < total_count; i++) {
        Range range = std::make_pair(get_chunk_offset(i), get_chunk_size(i));
        ham_u32_t next = range.first + range.second;
        if (next >= next_offset)
          next_offset = next;
        //ranges.push_back(range);
      }

#if 0
      std::sort(ranges.begin(), ranges.end());

      if (!ranges.empty()) {
        for (ham_u32_t i = 0; i < ranges.size() - 1; i++) {
          if (ranges[i].first + ranges[i].second > ranges[i + 1].first) {
            ham_trace(("integrity violated: slot %u/%u overlaps with %lu",
                        ranges[i].first, ranges[i].second,
                        ranges[i + 1].first));
            throw Exception(HAM_INTEGRITY_VIOLATED);
          }
        }
      }
#endif

      if (next_offset != get_next_offset(node_count)) {
        ham_trace(("integrity violated: next offset %d, cached offset %d",
                    next_offset, get_next_offset(node_count)));
        throw Exception(HAM_INTEGRITY_VIOLATED);
      }
      if (next_offset != calc_next_offset(node_count)) {
        ham_trace(("integrity violated: next offset %d, calculated offset %d",
                    next_offset, calc_next_offset(node_count)));
        throw Exception(HAM_INTEGRITY_VIOLATED);
      }
    }

    // Splits an index and moves all chunks starting from position |pivot|
    // to the other index.
    // The other index *must* be empty!
    void split(UpfrontIndex *other, size_t node_count, size_t pivot) {
      other->clear();

      // now copy key by key
      for (size_t i = pivot; i < node_count; i++) {
        other->insert_slot(i - pivot, i - pivot);
        ham_u32_t size = get_chunk_size(i);
        ham_u32_t offset = other->allocate_space(i - pivot, i - pivot, size);
        memcpy(other->get_chunk_data_by_offset(offset),
                    get_chunk_data_by_offset(get_chunk_offset(i)),
                    size);
      }

      // this node has lost lots of its data - make sure that it will be
      // vacuumized as soon as more data is allocated
      m_vacuumize_counter += node_count;
      set_freelist_count(0);
      set_next_offset((ham_u32_t)-1);
    }

    // Merges all chunks from the |other| index to this index
    void merge_from(UpfrontIndex *other, size_t node_count,
                    size_t other_node_count) {
      vacuumize(node_count);
      
      for (size_t i = 0; i < other_node_count; i++) {
        insert_slot(i + node_count, i + node_count);
        ham_u32_t size = other->get_chunk_size(i);
        ham_u32_t offset = allocate_space(i + node_count, i + node_count, size);
        memcpy(get_chunk_data_by_offset(offset),
                    other->get_chunk_data_by_offset(other->get_chunk_offset(i)),
                    size);
      }

      other->clear();
    }

    // Returns a pointer to the actual data of a chunk
    ham_u8_t *get_chunk_data_by_offset(ham_u32_t offset) {
      return (&m_data[kPayloadOffset
                      + m_capacity * get_full_index_size()
                      + offset]);
    }

    // Returns a pointer to the actual data of a chunk
    ham_u8_t *get_chunk_data_by_offset(ham_u32_t offset) const {
      return (&m_data[kPayloadOffset
                      + m_capacity * get_full_index_size()
                      + offset]);
    }

    // Re-arranges the node: moves all keys sequentially to the beginning
    // of the key space, removes the whole freelist.
    //
    // This call is extremely expensive! Try to avoid it as good as possible.
    void vacuumize(size_t node_count) {
      if (m_vacuumize_counter == 0) {
        if (get_freelist_count() > 0) {
          set_freelist_count(0);
          invalidate_next_offset();
        }
        return;
      }

      // get rid of the freelist - this node is now completely rewritten,
      // and the freelist would just complicate things
      set_freelist_count(0);

      // make a copy of all indices (excluding the freelist)
      bool requires_sort = false;
	  SortHelper *s = (SortHelper *)::alloca(node_count * sizeof(SortHelper));
      for (ham_u32_t i = 0; i < node_count; i++) {
        s[i].slot = i;
        s[i].offset = get_chunk_offset(i);
        if (i > 0 && s[i].offset < s[i - 1].offset)
          requires_sort = true;
      }

      // sort them by offset; this is a very expensive call. only sort if
      // it's absolutely necessary!
      if (requires_sort)
        std::sort(&s[0], &s[node_count], sort_by_offset);

      // shift all keys to the left, get rid of all gaps at the front of the
      // key data or between the keys
      ham_u32_t next_offset = 0;
      ham_u32_t start = kPayloadOffset + m_capacity * get_full_index_size();
      for (ham_u32_t i = 0; i < node_count; i++) {
        ham_u32_t offset = s[i].offset;
        ham_u32_t slot = s[i].slot;
        ham_u32_t size = get_chunk_size(slot);
        if (offset != next_offset) {
          // shift key to the left
          memmove(&m_data[start + next_offset],
                          get_chunk_data_by_offset(offset), size);
          // store the new offset
          set_chunk_offset(slot, next_offset);
        }
        next_offset += size;
      }

      set_next_offset(next_offset);
      m_vacuumize_counter = 0;
    }

    // Invalidates the cached "next offset". In some cases it's necessary
    // that the caller forces a re-evaluation of the next offset. Although
    // i *think* that this method could become private, but the effort
    // is not worth the gain.
    void invalidate_next_offset() {
      set_next_offset((ham_u32_t)-1);
    }

    // Returns the full size of the range
    ham_u32_t get_range_size() const {
      return (*(ham_u32_t *)(m_data + 8));
    }

    // Returns the offset of the unused space at the end of the page
    ham_u32_t get_next_offset(size_t node_count) {
      ham_u32_t ret = *(ham_u32_t *)(m_data + 4);
      if (unlikely(ret == (ham_u32_t)-1 && node_count > 0)) {
        ret = calc_next_offset(node_count);
        set_next_offset(ret);
      }
      return (ret);
    }

    // Returns the offset of the unused space at the end of the page
    // (const version)
    ham_u32_t get_next_offset(size_t node_count) const {
      ham_u32_t ret = *(ham_u32_t *)(m_data + 4);
      if (unlikely(ret == (ham_u32_t)-1))
        return (calc_next_offset(node_count));
      return (ret);
    }

    // Returns the number of freelist entries
    size_t get_freelist_count() const {
      return (*(ham_u32_t *)m_data);
    }

  private:
    friend class UpfrontIndexFixture;

    // Resets the page
    void clear() {
      set_freelist_count(0);
      set_next_offset(0);
      m_vacuumize_counter = 0;
    }

    // Returns the size (in bytes) where payload data can be stored
    size_t get_usable_data_size() const {
      return (get_range_size()
                      - kPayloadOffset
                      - m_capacity * get_full_index_size());
    }

    // Sets the chunk offset of a slot
    void set_chunk_offset(ham_u32_t slot, ham_u32_t offset) {
      ham_u8_t *p = &m_data[kPayloadOffset + get_full_index_size() * slot];
      if (m_sizeof_offset == 2)
        *(ham_u16_t *)p = (ham_u16_t)offset;
      else
        *(ham_u32_t *)p = offset;
    }

    // Sets the number of freelist entries
    void set_freelist_count(size_t freelist_count) {
      ham_assert(freelist_count <= m_capacity);
      *(ham_u32_t *)m_data = freelist_count;
    }

    // Calculates and returns the next offset; does not store it
    ham_u32_t calc_next_offset(size_t node_count) const {
      ham_u32_t total_count = node_count + get_freelist_count();
      ham_u32_t next_offset = 0;
      for (ham_u32_t i = 0; i < total_count; i++) {
        ham_u32_t next = get_chunk_offset(i) + get_chunk_size(i);
        if (next >= next_offset)
          next_offset = next;
      }
      return (next_offset);
    }

    // Sets the offset of the unused space at the end of the page
    void set_next_offset(ham_u32_t next_offset) {
      *(ham_u32_t *)(m_data + 4) = next_offset;
    }

    // The full size of the whole range (includes metadata overhead at the
    // beginning)
    void set_full_range_size(ham_u32_t full_size) {
      *(ham_u32_t *)(m_data + 8) = full_size;
    }

    // The physical data in the node
    ham_u8_t *m_data;

    // The size of the offset; either 16 or 32 bits, depending on page size
    size_t m_sizeof_offset;

    // The capacity (number of available slots)
    size_t m_capacity;

    // A counter to indicate when rearranging the data makes sense
    int m_vacuumize_counter;
};

//
// Variable length keys
//
// This KeyList uses an UpfrontIndex to manage the variable length data
// chunks. The UpfrontIndex knows the sizes of the chunks, and therefore
// the VariableLengthKeyList does *not* store additional size information.
//
// The format of a single key is:
//   |Flags|Data...|
// where Flags are 8 bit.
//
// The key size (as specified by the user when inserting the key) therefore
// is UpfrontIndex::get_chunk_size() - 1.
//
class VariableLengthKeyList
{
    // for caching external keys
    typedef std::map<ham_u64_t, ByteArray> ExtKeyCache;

  public:
    enum {
      // A flag whether this KeyList has sequential data
      kHasSequentialData = 0,

      // A flag whether SIMD style linear access is supported
      kHasSimdSupport = 0
    };

    // Constructor
    VariableLengthKeyList(LocalDatabase *db)
      : m_db(db), m_index(db), m_data(0), m_extkey_cache(0) {
      size_t page_size = db->get_local_env()->get_page_size();
      if (Globals::ms_extended_threshold)
        m_extkey_threshold = Globals::ms_extended_threshold;
      else {
        if (page_size == 1024)
          m_extkey_threshold = 64;
        else if (page_size <= 1024 * 8)
          m_extkey_threshold = 128;
        else {
          // UpfrontIndex's chunk size has 8 bit (max 255), and reserve
          // a few bytes for metadata (flags)
          m_extkey_threshold = 250;
        }
      }
    }

    // Destructor; clears the cache to avoid memory leaks
    ~VariableLengthKeyList() {
      if (m_extkey_cache) {
        delete m_extkey_cache;
        m_extkey_cache = 0;
      }
    }

    // Creates a new KeyList starting at |ptr|, total size is
    // |full_range_size_bytes| (in bytes)
    void create(ham_u8_t *data, size_t full_range_size_bytes, size_t capacity) {
      m_data = data;
      m_index.create(m_data, full_range_size_bytes, capacity);
    }

    // Opens an existing KeyList
    void open(ham_u8_t *ptr, size_t capacity) {
      m_data = ptr;
      m_index.open(m_data, capacity);
    }

    // Returns the full size of the range
    size_t get_range_size() const {
      return (m_index.get_range_size());
    }

    // Calculates the required size for a range with the specified |capacity|.
    size_t calculate_required_range_size(size_t node_count,
            size_t new_capacity) const {
      return (UpfrontIndex::kPayloadOffset
                    + new_capacity * m_index.get_full_index_size()
                    + m_index.get_next_offset(node_count));
    }

    // Returns the actual key size including overhead. This is an estimate
    // since we don't know how large the keys will be
    size_t get_full_key_size(const ham_key_t *key = 0) const {
      if (!key)
        return (24 + m_index.get_full_index_size() + 1);
      // always make sure to have enough space for an extkey id
      if (key->size < 8 || key->size > m_extkey_threshold)
        return (sizeof(ham_u64_t) + m_index.get_full_index_size() + 1);
      return (key->size + m_index.get_full_index_size() + 1);
    }

    // Copies a key into |dest|
    void get_key(ham_u32_t slot, ByteArray *arena, ham_key_t *dest,
                    bool deep_copy = true) {
      ham_key_t tmp;
      ham_u32_t offset = m_index.get_chunk_offset(slot);
      ham_u8_t *p = m_index.get_chunk_data_by_offset(offset);

      if (unlikely(*p & BtreeKey::kExtendedKey)) {
        memset(&tmp, 0, sizeof(tmp));
        get_extended_key(get_extended_blob_id(slot), &tmp);
      }
      else {
        tmp.size = get_key_size(slot);
        tmp.data = p + 1;
      }

      dest->size = tmp.size;

      if (likely(deep_copy == false)) {
        dest->data = tmp.data;
        return;
      }

      // allocate memory (if required)
      if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
        arena->resize(tmp.size);
        dest->data = arena->get_ptr();
      }
      memcpy(dest->data, tmp.data, tmp.size);
    }

    // Returns the threshold when switching from binary search to
    // linear search. For this layout we do not want to use any linear
    // search, therefore return -1.
    size_t get_linear_search_threshold() const {
      return (0xffffffff);
    }

    // Performs a linear search in a given range between |start| and
    // |start + length|. Not implemented - callers must not use linear
    // search with this KeyList.
    template<typename Cmp>
    int linear_search(ham_u32_t start, ham_u32_t count, ham_key_t *hkey,
                    Cmp &comparator, int *pcmp) {
      ham_assert(!"shouldn't be here");
      throw Exception(HAM_INTERNAL_ERROR);
    }

    // Iterates all keys, calls the |visitor| on each. Not supported by
    // this KeyList implementation. For variable length keys, the caller
    // must iterate over all keys. The |scan()| interface is only implemented
    // for PAX style layouts.
    void scan(ScanVisitor *visitor, size_t node_count, ham_u32_t start) {
      ham_assert(!"shouldn't be here");
      throw Exception(HAM_INTERNAL_ERROR);
    }

    // Erases a key's payload. Does NOT remove the chunk from the UpfrontIndex
    // (see |erase_slot()|).
    void erase_data(ham_u32_t slot) {
      ham_u8_t flags = get_key_flags(slot);
      if (flags & BtreeKey::kExtendedKey) {
        // delete the extended key from the cache
        erase_extended_key(get_extended_blob_id(slot));
        // and transform into a key which is non-extended and occupies
        // the same space as before, when it was extended
        set_key_flags(slot, flags & (~BtreeKey::kExtendedKey));
        set_key_size(slot, sizeof(ham_u64_t));
      }
    }

    // Erases a key, including extended blobs
    void erase_slot(size_t node_count, ham_u32_t slot) {
      erase_data(slot);
      m_index.erase_slot(node_count, slot);
    }

    // Inserts the |key| at the position identified by |slot|.
    // This method cannot fail; there MUST be sufficient free space in the
    // node (otherwise the caller would have split the node).
    void insert(size_t node_count, ham_u32_t slot, const ham_key_t *key) {
      m_index.insert_slot(node_count, slot);

      // now there's one additional slot
      node_count++;

      // When inserting the data: always add 1 byte for key flags
      if (key->size <= m_extkey_threshold
            && m_index.can_allocate_space(node_count, key->size + 1)) {
        ham_u32_t offset = m_index.allocate_space(node_count, slot,
                        key->size + 1);
        ham_u8_t *p = m_index.get_chunk_data_by_offset(offset);
        *p = 0; // sets flags
        memcpy(p + 1, key->data, key->size); // and data
      }
      else {
        ham_u64_t blob_id = add_extended_key(key);
        m_index.allocate_space(node_count, slot, 8 + 1);
        set_extended_blob_id(slot, blob_id);
        set_key_flags(slot, BtreeKey::kExtendedKey);
      }
    }

    // Returns true if the |key| no longer fits into the node and a split
    // is required. Makes sure that there is ALWAYS enough headroom
    // for an extended key!
    bool requires_split(size_t node_count, const ham_key_t *key,
                    bool vacuumize = false) {
      size_t required = key->size + 1;
      // add 1 byte for flags
      if (key->size > m_extkey_threshold || key->size < 8 + 1)
        required = 8 + 1;
      bool ret = m_index.requires_split(node_count, required);
      if (ret == false || vacuumize == false)
        return (ret);
      if (m_index.get_vacuumize_counter() < required
              || m_index.get_freelist_count() > 0) {
        m_index.vacuumize(node_count);
        ret = requires_split(node_count, key, false);
      }
      return (ret);
    }

    // Copies |count| key from this[sstart] to dest[dstart]
    void copy_to(ham_u32_t sstart, size_t node_count,
                    VariableLengthKeyList &dest, size_t other_node_count,
                    ham_u32_t dstart) {
      size_t i = 0;
      for (; i < node_count - sstart; i++) {
        size_t size = get_key_size(sstart + i);

        ham_u8_t *p = m_index.get_chunk_data_by_offset(
                        m_index.get_chunk_offset(sstart + i));
        ham_u8_t flags = *p;
        ham_u8_t *data = p + 1;

        dest.m_index.insert_slot(other_node_count + i, dstart + i);
        // Add 1 byte for key flags
        ham_u32_t offset = dest.m_index.allocate_space(other_node_count + i + 1,
                        dstart + i, size + 1);
        p = dest.m_index.get_chunk_data_by_offset(offset);
        *p = flags; // sets flags
        memcpy(p + 1, data, size); // and data
      }

      // A lot of keys will be invalidated after copying, therefore make
      // sure that the next_offset is recalculated when it's required
      m_index.invalidate_next_offset();
    }

    // Checks the integrity of this node. Throws an exception if there is a
    // violation.
    void check_integrity(size_t node_count, bool quick = false) const {
      ByteArray arena;

      // verify that the offsets and sizes are not overlapping
      m_index.check_integrity(node_count);
      if (quick)
        return;

      // make sure that extkeys are handled correctly
      for (ham_u32_t i = 0; i < node_count; i++) {
        if (get_key_size(i) > m_extkey_threshold
            && !(get_key_flags(i) & BtreeKey::kExtendedKey)) {
          ham_log(("key size %d, but key is not extended", get_key_size(i)));
          throw Exception(HAM_INTEGRITY_VIOLATED);
        }

        if (get_key_flags(i) & BtreeKey::kExtendedKey) {
          ham_u64_t blobid = get_extended_blob_id(i);
          if (!blobid) {
            ham_log(("integrity check failed: item %u "
                    "is extended, but has no blob", i));
            throw Exception(HAM_INTEGRITY_VIOLATED);
          }

          // make sure that the extended blob can be loaded
          ham_record_t record = {0};
          m_db->get_local_env()->get_blob_manager()->read(m_db, blobid,
                          &record, 0, &arena);

          // compare it to the cached key (if there is one)
          if (m_extkey_cache) {
            ExtKeyCache::iterator it = m_extkey_cache->find(blobid);
            if (it != m_extkey_cache->end()) {
              if (record.size != it->second.get_size()) {
                ham_log(("Cached extended key differs from real key"));
                throw Exception(HAM_INTEGRITY_VIOLATED);
              }
              if (memcmp(record.data, it->second.get_ptr(), record.size)) {
                ham_log(("Cached extended key differs from real key"));
                throw Exception(HAM_INTEGRITY_VIOLATED);
              }
            }
          }
        }
      }
    }

    // Rearranges the list
    void vacuumize(size_t node_count, bool force) {
      if (force)
        m_index.increase_vacuumize_counter(1);
      m_index.vacuumize(node_count);
    }

    // Change the capacity; the capacity will be reduced, growing is not
    // implemented. Which means that the data area must be copied; the offsets
    // do not have to be changed.
    void change_capacity(size_t node_count, size_t old_capacity,
            size_t new_capacity, ham_u8_t *new_data_ptr,
            size_t new_range_size) {
      m_index.change_capacity(node_count, new_data_ptr, new_range_size,
              new_capacity);
      m_data = new_data_ptr;
    }

    // Prints a slot to |out| (for debugging)
    void print(ham_u32_t slot, std::stringstream &out) {
      ham_key_t tmp = {0};
      if (get_key_flags(slot) & BtreeKey::kExtendedKey) {
        get_extended_key(get_extended_blob_id(slot), &tmp);
      }
      else {
        tmp.size = get_key_size(slot);
        tmp.data = get_key_data(slot);
      }
      out << (const char *)tmp.data;
    }

  private:
    // Returns the flags of a key. Flags are defined in btree_flags.h
    ham_u8_t get_key_flags(ham_u32_t slot) const {
      ham_u32_t offset = m_index.get_chunk_offset(slot);
      return (*m_index.get_chunk_data_by_offset(offset));
    }

    // Sets the flags of a key. Flags are defined in btree_flags.h
    void set_key_flags(ham_u32_t slot, ham_u8_t flags) {
      ham_u32_t offset = m_index.get_chunk_offset(slot);
      *m_index.get_chunk_data_by_offset(offset) = flags;
    }

    // Returns the pointer to a key's inline data
    ham_u8_t *get_key_data(ham_u32_t slot) {
      ham_u32_t offset = m_index.get_chunk_offset(slot);
      return (m_index.get_chunk_data_by_offset(offset) + 1);
    }

    // Returns the pointer to a key's inline data (const flavour)
    ham_u8_t *get_key_data(ham_u32_t slot) const {
      ham_u32_t offset = m_index.get_chunk_offset(slot);
      return (m_index.get_chunk_data_by_offset(offset) + 1);
    }

    // Overwrites the (inline) data of the key
    void set_key_data(ham_u32_t slot, const void *ptr, size_t size) {
      ham_assert(m_index.get_chunk_size(slot) >= size);
      set_key_size(slot, (ham_u16_t)size);
      memcpy(get_key_data(slot), ptr, size);
    }

    // Returns the size of a key
    size_t get_key_size(ham_u32_t slot) const {
      return (m_index.get_chunk_size(slot) - 1);
    }

    // Sets the size of a key
    void set_key_size(ham_u32_t slot, size_t size) {
      ham_assert(size + 1 <= m_index.get_chunk_size(slot));
      m_index.set_chunk_size(slot, size + 1);
    }

    // Returns the record address of an extended key overflow area
    ham_u64_t get_extended_blob_id(ham_u32_t slot) const {
      return (*(ham_u64_t *)get_key_data(slot));
    }

    // Sets the record address of an extended key overflow area
    void set_extended_blob_id(ham_u32_t slot, ham_u64_t blobid) {
      *(ham_u64_t *)get_key_data(slot) = blobid;
    }

    // Erases an extended key from disk and from the cache
    void erase_extended_key(ham_u64_t blobid) {
      m_db->get_local_env()->get_blob_manager()->erase(m_db, blobid);
      if (m_extkey_cache) {
        ExtKeyCache::iterator it = m_extkey_cache->find(blobid);
        if (it != m_extkey_cache->end())
          m_extkey_cache->erase(it);
      }
    }

    // Retrieves the extended key at |blobid| and stores it in |key|; will
    // use the cache.
    void get_extended_key(ham_u64_t blob_id, ham_key_t *key) {
      if (!m_extkey_cache)
        m_extkey_cache = new ExtKeyCache();
      else {
        ExtKeyCache::iterator it = m_extkey_cache->find(blob_id);
        if (it != m_extkey_cache->end()) {
          key->size = it->second.get_size();
          key->data = it->second.get_ptr();
          return;
        }
      }

      ByteArray arena;
      ham_record_t record = {0};
      m_db->get_local_env()->get_blob_manager()->read(m_db, blob_id, &record,
                      0, &arena);
      (*m_extkey_cache)[blob_id] = arena;
      arena.disown();
      key->data = record.data;
      key->size = record.size;
    }

    // Allocates an extended key and stores it in the cache
    ham_u64_t add_extended_key(const ham_key_t *key) {
      if (!m_extkey_cache)
        m_extkey_cache = new ExtKeyCache();

      ham_record_t rec = {0};
      rec.data = key->data;
      rec.size = key->size;

      ham_u64_t blob_id = m_db->get_local_env()->get_blob_manager()->allocate(
                                            m_db, &rec, 0);
      ham_assert(blob_id != 0);
      ham_assert(m_extkey_cache->find(blob_id) == m_extkey_cache->end());

      ByteArray arena;
      arena.resize(key->size);
      memcpy(arena.get_ptr(), key->data, key->size);
      (*m_extkey_cache)[blob_id] = arena;
      arena.disown();

      // increment counter (for statistics)
      Globals::ms_extended_keys++;

      return (blob_id);
    }

    // The database
    LocalDatabase *m_db;

    // The index for managing the variable-length chunks
    UpfrontIndex m_index;

    // Pointer to the data of the node 
    ham_u8_t *m_data;

    // Cache for extended keys
    ExtKeyCache *m_extkey_cache;

    // Threshold for extended keys; if key size is > threshold then the
    // key is moved to a blob
    size_t m_extkey_threshold;
};

//
// Common functions for duplicate record lists
//
class DuplicateRecordList
{
  protected:
    // for caching external duplicate tables
    typedef std::map<ham_u64_t, DuplicateTable *> DuplicateTableCache;

  public:
    enum {
      // A flag whether this RecordList has sequential data
      kHasSequentialData = 0
    };

    // Constructor
    DuplicateRecordList(LocalDatabase *db, PBtreeNode *node,
                    bool store_flags, size_t record_size)
      : m_db(db), m_node(node), m_index(db), m_data(0),
        m_store_flags(store_flags), m_record_size(record_size),
        m_duptable_cache(0) {
      size_t page_size = db->get_local_env()->get_page_size();
      if (Globals::ms_duplicate_threshold)
        m_duptable_threshold = Globals::ms_duplicate_threshold;
      else {
        if (page_size == 1024)
          m_duptable_threshold = 8;
        else if (page_size <= 1024 * 8)
          m_duptable_threshold = 12;
        else if (page_size <= 1024 * 16)
          m_duptable_threshold = 20;
        else if (page_size <= 1024 * 32)
          m_duptable_threshold = 32;
        else {
          // 0x7f/127 is the maximum that we can store in the record
          // counter (7 bits), but we won't exploit this fully
          m_duptable_threshold = 64;
        }
      }

      // UpfrontIndex's chunk_size is just 1 byte (max 255); make sure that
      // the duplicate list fits into a single chunk!
      size_t rec_size = m_record_size;
      if (rec_size == HAM_RECORD_SIZE_UNLIMITED)
        rec_size = 9;
      if (m_duptable_threshold * rec_size > 250)
        m_duptable_threshold = 250 / rec_size;
    }

    // Destructor - clears the cache
    ~DuplicateRecordList() {
      if (m_duptable_cache) {
        for (DuplicateTableCache::iterator it = m_duptable_cache->begin();
                        it != m_duptable_cache->end(); it++)
          delete it->second;
        delete m_duptable_cache;
        m_duptable_cache = 0;
      }
    }

    // Creates a new RecordList starting at |data|
    void create(ham_u8_t *data, size_t full_range_size_bytes, size_t capacity) {
      m_data = data;
      m_index.create(m_data, full_range_size_bytes, capacity);
    }

    // Opens an existing RecordList
    void open(ham_u8_t *ptr, size_t capacity) {
      m_data = ptr;
      m_index.open(m_data, capacity);
    }

    // Returns the full size of the range
    size_t get_range_size() const {
      return (m_index.get_range_size());
    }

    // Returns a duplicate table; uses a cache to speed up access
    DuplicateTable *get_duplicate_table(ham_u64_t table_id) {
      if (!m_duptable_cache)
        m_duptable_cache = new DuplicateTableCache();
      else {
        DuplicateTableCache::iterator it = m_duptable_cache->find(table_id);
        if (it != m_duptable_cache->end())
          return (it->second);
      }

      DuplicateTable *dt = new DuplicateTable(m_db, !m_store_flags,
                                m_record_size);
      dt->open(table_id);
      (*m_duptable_cache)[table_id] = dt;
      return (dt);
    }

    // Updates the DupTableCache and changes the table id of a DuplicateTable.
    // Called whenever a DuplicateTable's size increases, and the new blob-id
    // differs from the old one.
    void update_duplicate_table_id(DuplicateTable *dt,
                    ham_u64_t old_table_id, ham_u64_t new_table_id) {
      m_duptable_cache->erase(old_table_id);
      (*m_duptable_cache)[new_table_id] = dt;
    }

    // Erases a slot. Only updates the UpfrontIndex; does NOT delete the
    // record blobs!
    void erase_slot(size_t node_count, ham_u32_t slot) {
      m_index.erase_slot(node_count, slot);
    }

    // Inserts a slot for one additional record
    void insert_slot(size_t node_count, ham_u32_t slot) {
      m_index.insert_slot(node_count, slot);
    }

    // Copies |count| items from this[sstart] to dest[dstart]
    void copy_to(ham_u32_t sstart, size_t node_count,
                    DuplicateRecordList &dest, size_t other_node_count,
                    ham_u32_t dstart) {
      size_t i = 0;
      ham_u32_t doffset;

      for (; i < node_count - sstart; i++) {
        size_t size = m_index.get_chunk_size(sstart + i);

        dest.m_index.insert_slot(other_node_count + i, dstart + i);
        // destination offset
        doffset = dest.m_index.allocate_space(other_node_count + i + 1,
                dstart + i, size);
        doffset = dest.m_index.get_absolute_offset(doffset);
        // source offset
        ham_u32_t soffset = m_index.get_chunk_offset(sstart + i);
        soffset = m_index.get_absolute_offset(soffset);
        // copy the data
        memcpy(&dest.m_data[doffset], &m_data[soffset], size);
      }

      // After copying, the caller will reduce the node count drastically.
      // Therefore invalidate the cached next_offset.
      m_index.invalidate_next_offset();
    }

    // Rearranges the list
    void vacuumize(size_t node_count, bool force) {
      if (force)
        m_index.increase_vacuumize_counter(1);
      m_index.vacuumize(node_count);
    }

    // Change the capacity; the capacity will be reduced, growing is not
    // implemented. Which means that the data area must be copied; the offsets
    // do not have to be changed.
    void change_capacity(size_t node_count, size_t old_capacity,
            size_t new_capacity, ham_u8_t *new_data_ptr,
            size_t new_range_size) {
      m_index.change_capacity(node_count, new_data_ptr, new_range_size,
              new_capacity);
      m_data = new_data_ptr;
    }

  protected:
    // The database
    LocalDatabase *m_db;

    // The current node
    PBtreeNode *m_node;

    // The index which manages variable length chunks
    UpfrontIndex m_index;

    // The actual data of the node
    ham_u8_t *m_data;

    // Whether record flags are required
    bool m_store_flags;

    // The constant record size, or HAM_RECORD_SIZE_UNLIMITED
    size_t m_record_size;

    // The duplicate threshold
    size_t m_duptable_threshold;

    // A cache for duplicate tables
    DuplicateTableCache *m_duptable_cache;
};

//
// RecordList for records with fixed length, with duplicates. It uses
// an UpfrontIndex to manage the variable length chunks.
//
// If a key has duplicates, then all duplicates are stored sequentially.
// If that duplicate list exceeds a certain threshold then they are moved
// to a DuplicateTable, which is stored as a blob.
//
//   Format for each slot:
//
//       1 byte meta data
//              bit 1 - 7: duplicate counter, if kExtendedDuplicates == 0
//              bit 8: kExtendedDuplicates
//       if kExtendedDuplicates == 0:
//              <counter> * <length> bytes 
//                  <length> byte data (always inline)
//       if kExtendedDuplicates == 1:
//              8 byte: record id of the extended duplicate table
//
class DuplicateInlineRecordList : public DuplicateRecordList
{
  public:
    // Constructor
    DuplicateInlineRecordList(LocalDatabase *db, PBtreeNode *node)
      : DuplicateRecordList(db, node, false, db->get_record_size()),
        m_record_size(db->get_record_size()) {
    }

    // Calculates the required size for a range with the specified |capacity|
    size_t calculate_required_range_size(size_t node_count,
            size_t new_capacity) const {
      return (UpfrontIndex::kPayloadOffset
                    + new_capacity * m_index.get_full_index_size()
                    + m_index.get_next_offset(node_count));
    }

    // Returns the actual record size including overhead
    size_t get_full_record_size() const {
      return (1 + m_record_size + m_index.get_full_index_size());
    }

    // Returns the number of duplicates for a slot
    ham_u32_t get_record_count(ham_u32_t slot) {
      ham_u32_t offset = m_index.get_absolute_chunk_offset(slot);
      if (m_data[offset] & BtreeRecord::kExtendedDuplicates) {
        DuplicateTable *dt = get_duplicate_table(get_record_id(slot));
        return (dt->get_record_count());
      }
      
      return (m_data[offset] & 0x7f);
    }

    // Returns the size of a record; the size is always constant
    ham_u64_t get_record_size(ham_u32_t slot, ham_u32_t duplicate_index = 0)
                    const {
      return (m_record_size);
    }

    // Returns the full record and stores it in |dest|
    void get_record(ham_u32_t slot, ham_u32_t duplicate_index,
                    ByteArray *arena, ham_record_t *record,
                    ham_u32_t flags) {
      // forward to duplicate table?
      ham_u32_t offset = m_index.get_absolute_chunk_offset(slot);
      if (unlikely(m_data[offset] & BtreeRecord::kExtendedDuplicates)) {
        DuplicateTable *dt = get_duplicate_table(get_record_id(slot));
        dt->get_record(duplicate_index, arena, record, flags);
        return;
      }

      if (flags & HAM_PARTIAL) {
        ham_trace(("flag HAM_PARTIAL is not allowed if record is "
                   "stored inline"));
        throw Exception(HAM_INV_PARAMETER);
      }

      ham_assert(duplicate_index < get_inline_record_count(slot));
      bool direct_access = (flags & HAM_DIRECT_ACCESS) != 0;

      // the record is always stored inline
      const ham_u8_t *ptr = get_record_data(slot, duplicate_index);
      record->size = m_record_size;
      if (direct_access)
        record->data = (void *)ptr;
      else {
        if ((record->flags & HAM_RECORD_USER_ALLOC) == 0) {
          arena->resize(record->size);
          record->data = arena->get_ptr();
        }
        memcpy(record->data, ptr, m_record_size);
      }
    }

    // Adds or overwrites a record
    void set_record(ham_u32_t slot, ham_u32_t duplicate_index,
                ham_record_t *record, ham_u32_t flags,
                ham_u32_t *new_duplicate_index) {
      ham_u32_t chunk_offset = m_index.get_absolute_chunk_offset(slot);
      ham_u32_t current_size = m_index.get_chunk_size(slot);

      ham_assert(m_record_size == record->size);

      // if the slot was not yet allocated: allocate new space, initialize
      // it and then overwrite the record
      if (current_size == 0) {
        duplicate_index = 0;
        flags |= HAM_OVERWRITE;
        chunk_offset = m_index.allocate_space(m_node->get_count(), slot,
                                    1 + m_record_size);
        chunk_offset = m_index.get_absolute_offset(chunk_offset);
        // clear the flags
        m_data[chunk_offset] = 0;

        set_inline_record_count(slot, 1);
      }

      // if there's no duplicate table, but we're not able to add another
      // duplicate because of size constraints, then offload all
      // existing duplicates to an external DuplicateTable
      ham_u32_t record_count = get_inline_record_count(slot);
      size_t required_size = 1 + (record_count + 1) * m_record_size;

      if (!(m_data[chunk_offset] & BtreeRecord::kExtendedDuplicates)
             && !(flags & HAM_OVERWRITE)) {
        bool force_duptable = record_count >= m_duptable_threshold;
        if (!force_duptable
              && !m_index.can_allocate_space(m_node->get_count(),
                            required_size))
          force_duptable = true;

        // update chunk_offset - it might have been modified if
        // m_index.can_allocate_space triggered a vacuumize() operation
        chunk_offset = m_index.get_absolute_chunk_offset(slot);

        // already too many duplicates, or the record does not fit? then
        // allocate an overflow duplicate list and move all duplicates to
        // this list
        if (force_duptable) {
          DuplicateTable *dt = new DuplicateTable(m_db, !m_store_flags,
                                        m_record_size);
          ham_u64_t table_id = dt->create(get_record_data(slot, 0),
                          record_count);
          if (!m_duptable_cache)
            m_duptable_cache = new DuplicateTableCache();
          (*m_duptable_cache)[table_id] = dt;

          // write the id of the duplicate table
          if (m_index.get_chunk_size(slot) < 8 + 1) {
            // do not erase the slot because it occupies so little space
            size_t node_count = m_node->get_count();
            // force a split in the caller if the duplicate table cannot
            // be inserted
            if (!m_index.can_allocate_space(node_count, 8 + 1))
              throw Exception(HAM_LIMITS_REACHED);
            m_index.allocate_space(node_count, slot, 8 + 1);
            chunk_offset = m_index.get_absolute_chunk_offset(slot);
          }

          m_data[chunk_offset] |= BtreeRecord::kExtendedDuplicates;
          set_record_id(slot, table_id);
          set_inline_record_count(slot, 0);

          m_index.set_chunk_size(slot, 8 + 1);
          m_index.increase_vacuumize_counter(m_index.get_chunk_size(slot) - 9);
          m_index.invalidate_next_offset();

          // fall through
        }
      }

      // forward to duplicate table?
      if (unlikely(m_data[chunk_offset] & BtreeRecord::kExtendedDuplicates)) {
        ham_u64_t table_id = get_record_id(slot);
        DuplicateTable *dt = get_duplicate_table(table_id);
        ham_u64_t new_table_id = dt->set_record(duplicate_index, record,
                        flags, new_duplicate_index);
        if (new_table_id != table_id) {
          update_duplicate_table_id(dt, table_id, new_table_id);
          set_record_id(slot, new_table_id);
        }
        return;
      }

      // the duplicate is overwritten
      if (flags & HAM_OVERWRITE) {
        // the record is always stored inline w/ fixed length
        ham_u8_t *p = (ham_u8_t *)get_record_data(slot, duplicate_index);
        memcpy(p, record->data, record->size);
        return;
      }

      // Allocate new space for the duplicate table, if required
      if (current_size < required_size) {
        ham_u8_t *oldp = &m_data[chunk_offset];
        ham_u32_t old_chunk_size = m_index.get_chunk_size(slot);
        ham_u32_t old_chunk_offset = m_index.get_chunk_offset(slot);
        ham_u32_t new_chunk_offset = m_index.allocate_space(m_node->get_count(),
                        slot, required_size);
        chunk_offset = m_index.get_absolute_offset(new_chunk_offset);
        if (current_size > 0)
          memmove(&m_data[chunk_offset], oldp, current_size);
        if (old_chunk_offset != new_chunk_offset)
          m_index.add_to_freelist(m_node->get_count(), old_chunk_offset,
                          old_chunk_size);
      }

      // adjust flags
      if (flags & HAM_DUPLICATE_INSERT_BEFORE && duplicate_index == 0)
        flags |= HAM_DUPLICATE_INSERT_FIRST;
      else if (flags & HAM_DUPLICATE_INSERT_AFTER) {
        if (duplicate_index == record_count)
          flags |= HAM_DUPLICATE_INSERT_LAST;
        else {
          flags |= HAM_DUPLICATE_INSERT_BEFORE;
          duplicate_index++;
        }
      }

      // handle overwrites or inserts/appends
      if (flags & HAM_DUPLICATE_INSERT_FIRST) {
        if (record_count > 0) {
          ham_u8_t *ptr = get_record_data(slot, 0);
          memmove(get_record_data(1), ptr, record_count * m_record_size);
        }
        duplicate_index = 0;
      }
      else if (flags & HAM_DUPLICATE_INSERT_BEFORE) {
        memmove(get_record_data(slot, duplicate_index),
                    get_record_data(slot, duplicate_index + 1),
                    (record_count - duplicate_index) * m_record_size);
      }
      else // HAM_DUPLICATE_INSERT_LAST
        duplicate_index = record_count;

      set_inline_record_count(slot, record_count + 1);

      // store the new record inline
      if (m_record_size > 0)
        memcpy(get_record_data(slot, duplicate_index),
                        record->data, record->size);

      if (new_duplicate_index)
        *new_duplicate_index = duplicate_index;
    }

    // Erases a record's blob (does not remove the slot!)
    void erase_record(ham_u32_t slot, ham_u32_t duplicate_index,
                    bool all_duplicates) {
      ham_u32_t offset = m_index.get_absolute_chunk_offset(slot);

      // forward to external duplicate table?
      if (unlikely(m_data[offset] & BtreeRecord::kExtendedDuplicates)) {
        ham_u64_t table_id = get_record_id(slot);
        DuplicateTable *dt = get_duplicate_table(table_id);
        ham_u64_t new_table_id = dt->erase_record(duplicate_index,
                        all_duplicates);
        if (new_table_id == 0) {
          m_duptable_cache->erase(table_id);
          set_record_id(slot, 0);
          m_data[offset] &= ~BtreeRecord::kExtendedDuplicates;
          delete dt;
        }
        else if (new_table_id != table_id) {
          update_duplicate_table_id(dt, table_id, new_table_id);
          set_record_id(slot, new_table_id);
        }
        return;
      }

      // there's only one record left which is erased?
      ham_u32_t count = get_inline_record_count(slot);
      if (count == 1 && duplicate_index == 0)
        all_duplicates = true;

      // erase all duplicates?
      if (all_duplicates) {
        set_inline_record_count(slot, 0);
      }
      else {
        if (duplicate_index < count - 1)
          memmove(get_record_data(duplicate_index),
                      get_record_data(duplicate_index + 1), 
                      m_record_size * (count - duplicate_index - 1));
        set_inline_record_count(slot, count - 1);
      }
    }

    // Returns a 64bit record id from a record
    ham_u64_t get_record_id(ham_u32_t slot,
                    ham_u32_t duplicate_index = 0) const {
      return (*(ham_u64_t *)get_record_data(slot, duplicate_index));
    }

    // Sets a 64bit record id; used for internal nodes to store Page IDs
    // or for leaf nodes to store DuplicateTable IDs
    void set_record_id(ham_u32_t slot, ham_u64_t id) {
      ham_assert(m_index.get_chunk_size(slot) >= sizeof(id));
      *(ham_u64_t *)get_record_data(slot, 0) = id;
    }

    // Checks the integrity of this node. Throws an exception if there is a
    // violation.
    void check_integrity(size_t node_count, bool quick = false) const {
      for (size_t i = 0; i < node_count; i++) {
        ham_u32_t offset = m_index.get_absolute_chunk_offset(i);
        if (m_data[offset] & BtreeRecord::kExtendedDuplicates) {
          ham_assert((m_data[offset] & 0x7f) == 0);
        }
      }

      m_index.check_integrity(node_count);
    }

    // Returns true if there's not enough space for another record
    bool requires_split(size_t node_count, bool vacuumize = false) {
      // if the record is extremely small then make sure there's some headroom;
      // this is required for DuplicateTable ids which are 64bit numbers
      size_t required = get_full_record_size();
      if (required < 10)
        required = 10;
      bool ret = m_index.requires_split(node_count, required);
      if (ret == false || vacuumize == false)
        return (ret);
      if (m_index.get_vacuumize_counter() < required
              || m_index.get_freelist_count() > 0) {
        m_index.vacuumize(node_count);
        ret = requires_split(node_count, false);
      }
      return (ret);
    }

    // Prints a slot to |out| (for debugging)
    void print(ham_u32_t slot, std::stringstream &out) {
      out << "(" << get_record_count(slot) << " records)";
    }

  private:
    // Returns the number of records that are stored inline
    ham_u32_t get_inline_record_count(ham_u32_t slot) {
      ham_u32_t offset = m_index.get_absolute_chunk_offset(slot);
      return (m_data[offset] & 0x7f);
    }

    // Sets the number of records that are stored inline
    void set_inline_record_count(ham_u32_t slot, size_t count) {
      ham_assert(count <= 0x7f);
      ham_u32_t offset = m_index.get_absolute_chunk_offset(slot);
      m_data[offset] &= BtreeRecord::kExtendedDuplicates;
      m_data[offset] |= count;
    }

    // Returns a pointer to the record data
    ham_u8_t *get_record_data(ham_u32_t slot, ham_u32_t duplicate_index = 0) {
      ham_u32_t offset = m_index.get_absolute_chunk_offset(slot);
      return (&m_data[offset + 1 + m_record_size * duplicate_index]);
    }

    // Returns a pointer to the record data (const flavour)
    const ham_u8_t *get_record_data(ham_u32_t slot,
                        ham_u32_t duplicate_index = 0) const {
      ham_u32_t offset = m_index.get_absolute_chunk_offset(slot);
      return (&m_data[offset + 1 + m_record_size * duplicate_index]);
    }

    // The constant length record size
    size_t m_record_size;
};

//
// RecordList for default records (8 bytes; either inline or a record id),
// with duplicates
//
//   Format for each slot:
//
//       1 byte meta data
//              bit 1 - 7: duplicate counter, if kExtendedDuplicates == 0
//              bit 8: kExtendedDuplicates
//       if kExtendedDuplicates == 0:
//              <counter> * 9 bytes 
//                  1 byte flags (RecordFlag::*)
//                  8 byte data (either inline or record-id)
//       if kExtendedDuplicates == 1:
//              8 byte: record id of the extended duplicate table
//
class DuplicateDefaultRecordList : public DuplicateRecordList
{
  public:
    // Constructor
    DuplicateDefaultRecordList(LocalDatabase *db, PBtreeNode *node)
      : DuplicateRecordList(db, node, true, HAM_RECORD_SIZE_UNLIMITED) {
    }

    // Calculates the required size for a range with the specified |capacity|
    size_t calculate_required_range_size(size_t node_count,
            size_t new_capacity) const {
      return (UpfrontIndex::kPayloadOffset
                    + new_capacity * m_index.get_full_index_size()
                    + m_index.get_next_offset(node_count));
    }

    // Returns the actual key record including overhead
    size_t get_full_record_size() const {
      return (1 + 1 + 8 + m_index.get_full_index_size());
    }

    // Returns the number of duplicates
    ham_u32_t get_record_count(ham_u32_t slot) {
      ham_u32_t offset = m_index.get_absolute_chunk_offset(slot);
      if (unlikely(m_data[offset] & BtreeRecord::kExtendedDuplicates)) {
        DuplicateTable *dt = get_duplicate_table(get_record_id(slot));
        return (dt->get_record_count());
      }
      
      return (m_data[offset] & 0x7f);
    }

    // Returns the size of a record
    ham_u64_t get_record_size(ham_u32_t slot, ham_u32_t duplicate_index = 0) {
      ham_u32_t offset = m_index.get_absolute_chunk_offset(slot);
      if (unlikely(m_data[offset] & BtreeRecord::kExtendedDuplicates)) {
        DuplicateTable *dt = get_duplicate_table(get_record_id(slot));
        return (dt->get_record_size(duplicate_index));
      }
      
      ham_u8_t *p = &m_data[offset + 1 + 9 * duplicate_index];
      ham_u8_t flags = *(p++);
      if (flags & BtreeRecord::kBlobSizeTiny)
        return (p[sizeof(ham_u64_t) - 1]);
      if (flags & BtreeRecord::kBlobSizeSmall)
        return (sizeof(ham_u64_t));
      if (flags & BtreeRecord::kBlobSizeEmpty)
        return (0);

      return (m_db->get_local_env()->get_blob_manager()->get_blob_size(m_db,
                              *(ham_u64_t *)p));
    }

    // Returns the full record and stores it in |dest|; memory must be
    // allocated by the caller
    void get_record(ham_u32_t slot, ham_u32_t duplicate_index,
                    ByteArray *arena, ham_record_t *record,
                    ham_u32_t flags) {
      // forward to duplicate table?
      ham_u32_t offset = m_index.get_absolute_chunk_offset(slot);
      if (unlikely(m_data[offset] & BtreeRecord::kExtendedDuplicates)) {
        DuplicateTable *dt = get_duplicate_table(get_record_id(slot));
        dt->get_record(duplicate_index, arena, record, flags);
        return;
      }

      ham_assert(duplicate_index < get_inline_record_count(slot));
      bool direct_access = (flags & HAM_DIRECT_ACCESS) != 0;

      ham_u8_t *p = &m_data[offset + 1 + 9 * duplicate_index];
      ham_u8_t record_flags = *(p++);

      if (record_flags && (flags & HAM_PARTIAL)) {
        ham_trace(("flag HAM_PARTIAL is not allowed if record is "
                   "stored inline"));
        throw Exception(HAM_INV_PARAMETER);
      }

      if (record_flags & BtreeRecord::kBlobSizeEmpty) {
        record->data = 0;
        record->size = 0;
        return;
      }

      if (record_flags & BtreeRecord::kBlobSizeTiny) {
        record->size = p[sizeof(ham_u64_t) - 1];
        if (direct_access)
          record->data = &p[0];
        else {
          if ((record->flags & HAM_RECORD_USER_ALLOC) == 0) {
            arena->resize(record->size);
            record->data = arena->get_ptr();
          }
          memcpy(record->data, &p[0], record->size);
        }
        return;
      }

      if (record_flags & BtreeRecord::kBlobSizeSmall) {
        record->size = sizeof(ham_u64_t);
        if (direct_access)
          record->data = &p[0];
        else {
          if ((record->flags & HAM_RECORD_USER_ALLOC) == 0) {
            arena->resize(record->size);
            record->data = arena->get_ptr();
          }
          memcpy(record->data, &p[0], record->size);
        }
        return;
      }

      ham_u64_t blob_id = *(ham_u64_t *)p;

      // the record is stored as a blob
      LocalEnvironment *env = m_db->get_local_env();
      env->get_blob_manager()->read(m_db, blob_id, record, flags, arena);
    }

    // Updates the record of a key
    void set_record(ham_u32_t slot, ham_u32_t duplicate_index,
                ham_record_t *record, ham_u32_t flags,
                ham_u32_t *new_duplicate_index) {
      ham_u32_t chunk_offset = m_index.get_absolute_chunk_offset(slot);
      ham_u32_t current_size = m_index.get_chunk_size(slot);

      // if the slot was not yet allocated: allocate new space, initialize
      // it and then overwrite the record
      if (current_size == 0) {
        duplicate_index = 0;
        flags |= HAM_OVERWRITE;
        chunk_offset = m_index.allocate_space(m_node->get_count(), slot, 1 + 9);
        chunk_offset = m_index.get_absolute_offset(chunk_offset);
        // clear the record flags
        m_data[chunk_offset] = 0;
        m_data[chunk_offset + 1] = BtreeRecord::kBlobSizeEmpty;

        set_inline_record_count(slot, 1);
      }

      // if there's no duplicate table, but we're not able to add another
      // duplicate then offload all existing duplicates to a table
      ham_u32_t record_count = get_inline_record_count(slot);
      size_t required_size = 1 + (record_count + 1) * 9;

      if (!(m_data[chunk_offset] & BtreeRecord::kExtendedDuplicates)
             && !(flags & HAM_OVERWRITE)) {
        bool force_duptable = record_count >= m_duptable_threshold;
        if (!force_duptable
              && !m_index.can_allocate_space(m_node->get_count(),
                            required_size))
          force_duptable = true;
      
        // update chunk_offset - it might have been modified if
        // m_index.can_allocate_space triggered a vacuumize() operation
        chunk_offset = m_index.get_absolute_chunk_offset(slot);

        // already too many duplicates, or the record does not fit? then
        // allocate an overflow duplicate list and move all duplicates to
        // this list
        if (force_duptable) {
          DuplicateTable *dt = new DuplicateTable(m_db, !m_store_flags,
                                        HAM_RECORD_SIZE_UNLIMITED);
          ham_u64_t table_id = dt->create(get_record_data(slot, 0),
                          record_count);
          if (!m_duptable_cache)
            m_duptable_cache = new DuplicateTableCache();
          (*m_duptable_cache)[table_id] = dt;

          // write the id of the duplicate table
          if (m_index.get_chunk_size(slot) < 8 + 1) {
            // do not erase the slot because it obviously occupies so
            // little space
            m_index.allocate_space(m_node->get_count(), slot, 8 + 1);
            chunk_offset = m_index.get_absolute_chunk_offset(slot);
          }

          m_data[chunk_offset] |= BtreeRecord::kExtendedDuplicates;
          set_record_id(slot, table_id);
          set_inline_record_count(slot, 0);

          m_index.set_chunk_size(slot, 10);
          m_index.increase_vacuumize_counter(m_index.get_chunk_size(slot) - 10);
          m_index.invalidate_next_offset();

          // fall through
        }
      }

      // forward to duplicate table?
      if (unlikely(m_data[chunk_offset] & BtreeRecord::kExtendedDuplicates)) {
        ham_u64_t table_id = get_record_id(slot);
        DuplicateTable *dt = get_duplicate_table(table_id);
        ham_u64_t new_table_id = dt->set_record(duplicate_index, record,
                        flags, new_duplicate_index);
        if (new_table_id != table_id) {
          update_duplicate_table_id(dt, table_id, new_table_id);
          set_record_id(slot, new_table_id);
        }
        return;
      }

      ham_u64_t overwrite_blob_id = 0;
      ham_u8_t *record_flags = 0;
      ham_u8_t *p = 0;

      // the (inline) duplicate is overwritten
      if (flags & HAM_OVERWRITE) {
        record_flags = &m_data[chunk_offset + 1 + 9 * duplicate_index];
        p = record_flags + 1;

        // If a blob is overwritten with an inline record then the old blob
        // has to be deleted
        if (*record_flags == 0) {
          if (record->size <= 8) {
            ham_u64_t blob_id = *(ham_u64_t *)p;
            if (blob_id)
              m_db->get_local_env()->get_blob_manager()->erase(m_db, blob_id);
          }
          else
            overwrite_blob_id = *(ham_u64_t *)p;
          // fall through
        }
        // then jump to the code which performs the actual insertion
        goto write_record;
      }

      // Allocate new space for the duplicate table, if required
      if (current_size < required_size) {
        ham_u8_t *oldp = &m_data[chunk_offset];
        ham_u32_t old_chunk_size = m_index.get_chunk_size(slot);
        ham_u32_t old_chunk_offset = m_index.get_chunk_offset(slot);
        ham_u32_t new_chunk_offset = m_index.allocate_space(m_node->get_count(),
                        slot, required_size);
        chunk_offset = m_index.get_absolute_offset(new_chunk_offset);
        if (current_size > 0)
          memmove(&m_data[chunk_offset], oldp, current_size);
        if (old_chunk_offset != new_chunk_offset)
          m_index.add_to_freelist(m_node->get_count(), old_chunk_offset,
                          old_chunk_size);
      }

      // adjust flags
      if (flags & HAM_DUPLICATE_INSERT_BEFORE && duplicate_index == 0)
        flags |= HAM_DUPLICATE_INSERT_FIRST;
      else if (flags & HAM_DUPLICATE_INSERT_AFTER) {
        if (duplicate_index == record_count)
          flags |= HAM_DUPLICATE_INSERT_LAST;
        else {
          flags |= HAM_DUPLICATE_INSERT_BEFORE;
          duplicate_index++;
        }
      }

      // handle overwrites or inserts/appends
      if (flags & HAM_DUPLICATE_INSERT_FIRST) {
        if (record_count > 0) {
          ham_u8_t *ptr = &m_data[chunk_offset + 1];
          memmove(&m_data[chunk_offset + 1 + 9], ptr, record_count * 9);
        }
        duplicate_index = 0;
      }
      else if (flags & HAM_DUPLICATE_INSERT_BEFORE) {
        memmove(&m_data[chunk_offset + 1 + 9 * (duplicate_index + 1)],
                    &m_data[chunk_offset + 1 + 9 * duplicate_index],
                    (record_count - duplicate_index) * 9);
      }
      else // HAM_DUPLICATE_INSERT_LAST
        duplicate_index = record_count;

      set_inline_record_count(slot, record_count + 1);

      record_flags = &m_data[chunk_offset + 1 + 9 * duplicate_index];
      p = record_flags + 1;

write_record:
      if (record->size == 0) {
        memcpy(p, "\0\0\0\0\0\0\0\0", 8);
        *record_flags = BtreeRecord::kBlobSizeEmpty;
      }
      else if (record->size < sizeof(ham_u64_t)) {
        p[sizeof(ham_u64_t) - 1] = (ham_u8_t)record->size;
        memcpy(&p[0], record->data, record->size);
        *record_flags = BtreeRecord::kBlobSizeTiny;
      }
      else if (record->size == sizeof(ham_u64_t)) {
        memcpy(&p[0], record->data, record->size);
        *record_flags = BtreeRecord::kBlobSizeSmall;
      }
      else {
        LocalEnvironment *env = m_db->get_local_env();
        *record_flags = 0;
        ham_u64_t blob_id;
        if (overwrite_blob_id)
          blob_id = env->get_blob_manager()->overwrite(m_db, overwrite_blob_id,
                          record, flags);
        else
          blob_id = env->get_blob_manager()->allocate(m_db, record, flags);
        memcpy(p, &blob_id, sizeof(blob_id));
      }

      if (new_duplicate_index)
        *new_duplicate_index = duplicate_index;
    }

    // Erases a record
    void erase_record(ham_u32_t slot, ham_u32_t duplicate_index,
                    bool all_duplicates) {
      ham_u32_t offset = m_index.get_absolute_chunk_offset(slot);

      // forward to external duplicate table?
      if (unlikely(m_data[offset] & BtreeRecord::kExtendedDuplicates)) {
        ham_u64_t table_id = get_record_id(slot);
        DuplicateTable *dt = get_duplicate_table(table_id);
        ham_u64_t new_table_id = dt->erase_record(duplicate_index,
                        all_duplicates);
        if (new_table_id == 0) {
          m_duptable_cache->erase(table_id);
          set_record_id(slot, 0);
          m_data[offset] &= ~BtreeRecord::kExtendedDuplicates;
          delete dt;
        }
        else if (new_table_id != table_id) {
          update_duplicate_table_id(dt, table_id, new_table_id);
          set_record_id(slot, new_table_id);
        }
        return;
      }

      // erase the last duplicate?
      ham_u32_t count = get_inline_record_count(slot);
      if (count == 1 && duplicate_index == 0)
        all_duplicates = true;

      // adjust next_offset, if necessary. Note that get_next_offset() is
      // called with a node_count of zero, which is valid (it avoids a
      // recalculation in case there is no next_offset)
      if (m_index.get_next_offset(0) == m_index.get_chunk_offset(slot)
                                            + m_index.get_chunk_size(slot))
        m_index.invalidate_next_offset();

      // erase all duplicates?
      if (all_duplicates) {
        for (ham_u32_t i = 0; i < count; i++) {
          ham_u8_t *p = &m_data[offset + 1 + 9 * i];
          if (!is_record_inline(*p)) {
            m_db->get_local_env()->get_blob_manager()->erase(m_db,
                            *(ham_u64_t *)(p + 1));
            *(ham_u64_t *)(p + 1) = 0;
          }
        }
        set_inline_record_count(slot, 0);
        m_index.set_chunk_size(slot, 0);
      }
      else {
        ham_u8_t *p = &m_data[offset + 1 + 9 * duplicate_index];
        if (!is_record_inline(*p)) {
          m_db->get_local_env()->get_blob_manager()->erase(m_db,
                          *(ham_u64_t *)(p + 1));
          *(ham_u64_t *)(p + 1) = 0;
        }
        if (duplicate_index < count - 1)
          memmove(&m_data[offset + 1 + 9 * duplicate_index],
                  &m_data[offset + 1 + 9 * (duplicate_index + 1)],
                  9 * (count - duplicate_index - 1));
        set_inline_record_count(slot, count - 1);
      }
    }

    // Returns a record id
    ham_u64_t get_record_id(ham_u32_t slot,
                    ham_u32_t duplicate_index = 0) const {
      return (*(ham_u64_t *)get_record_data(slot, duplicate_index));
    }

    // Sets a record id
    void set_record_id(ham_u32_t slot, ham_u64_t id) {
      *(ham_u64_t *)get_record_data(slot, 0) = id;
    }

    // Checks the integrity of this node. Throws an exception if there is a
    // violation.
    void check_integrity(ham_u32_t node_count, bool quick = false) const {
      for (size_t i = 0; i < node_count; i++) {
        ham_u32_t offset = m_index.get_absolute_chunk_offset(i);
        if (m_data[offset] & BtreeRecord::kExtendedDuplicates) {
          ham_assert((m_data[offset] & 0x7f) == 0);
        }
      }

      m_index.check_integrity(node_count);
    }

    // Returns true if there's not enough space for another record
    bool requires_split(size_t node_count, bool vacuumize = false) {
      // if the record is extremely small then make sure there's some headroom;
      // this is required for DuplicateTable ids which are 64bit numbers
      size_t required = get_full_record_size();
      if (required < 10)
        required = 10;
      bool ret = m_index.requires_split(node_count, required);
      if (ret == false || vacuumize == false)
        return (ret);
      if (m_index.get_vacuumize_counter() < required
              || m_index.get_freelist_count() > 0) {
        m_index.vacuumize(node_count);
        ret = requires_split(node_count, false);
      }
      return (ret);
    }

    // Prints a slot to |out| (for debugging)
    void print(ham_u32_t slot, std::stringstream &out) {
      out << "(" << get_record_count(slot) << " records)";
    }

  private:
    // Returns the number of records that are stored inline
    ham_u32_t get_inline_record_count(ham_u32_t slot) {
      ham_u32_t offset = m_index.get_absolute_chunk_offset(slot);
      return (m_data[offset] & 0x7f);
    }

    // Sets the number of records that are stored inline
    void set_inline_record_count(ham_u32_t slot, size_t count) {
      ham_assert(count <= 0x7f);
      ham_u32_t offset = m_index.get_absolute_chunk_offset(slot);
      m_data[offset] &= BtreeRecord::kExtendedDuplicates;
      m_data[offset] |= count;
    }

    // Returns a pointer to the record data (const flavour)
    ham_u8_t *get_record_data(ham_u32_t slot, ham_u32_t duplicate_index = 0) {
      ham_u32_t offset = m_index.get_absolute_chunk_offset(slot);
      return (&m_data[offset + 1 + 9 * duplicate_index]);
    }

    // Returns a pointer to the record data (const flavour)
    const ham_u8_t *get_record_data(ham_u32_t slot,
                        ham_u32_t duplicate_index = 0) const {
      ham_u32_t offset = m_index.get_absolute_chunk_offset(slot);
      return (&m_data[offset + 1 + 9 * duplicate_index]);
    }
};

} // namespace DefLayout

//
// A BtreeNodeProxy layout which can handle...
//
//   1. fixed length keys w/ duplicates
//   2. variable length keys w/ duplicates
//   3. variable length keys w/o duplicates
//
// Fixed length keys are stored sequentially and reuse the layout from pax.
// Same for the distinct RecordList (if duplicates are disabled).
//
template<typename KeyList, typename RecordList>
class DefaultNodeImpl
{
    // the type of |this| object
    typedef DefaultNodeImpl<KeyList, RecordList> NodeType;

    enum {
      // for capacity
      kPayloadOffset = 4
    };

  public:
    // Constructor
    DefaultNodeImpl(Page *page)
      : m_page(page), m_node(PBtreeNode::from_page(m_page)),
        m_keys(page->get_db()), m_records(page->get_db(), m_node),
        m_capacity(0) {
      initialize();
#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count());
#endif
    }

    // Destructor
    ~DefaultNodeImpl() {
    }

    // Returns the capacity of the node
    size_t get_capacity() const {
      return (m_capacity);
    }

    // Checks the integrity of this node. Throws an exception if there is a
    // violation.
    void check_integrity() const {
      size_t node_count = m_node->get_count();
      if (node_count == 0)
        return;

      check_index_integrity(node_count);
    }

    // Compares two keys
    template<typename Cmp>
    int compare(const ham_key_t *lhs, ham_u32_t rhs, Cmp &cmp) {
      ham_key_t tmp = {0};
      m_keys.get_key(rhs, &m_arena, &tmp, false);
      return (cmp(lhs->data, lhs->size, tmp.data, tmp.size));
    }

    // Searches the node for the key and returns the slot of this key
    template<typename Cmp>
    int find_child(ham_key_t *key, Cmp &comparator, ham_u64_t *precord_id,
                    int *pcmp) {
      int slot = find_impl(key, comparator, pcmp);
      if (precord_id) {
        if (slot == -1)
          *precord_id = m_node->get_ptr_down();
        else
          *precord_id = m_records.get_record_id(slot);
      }
      return (slot);
    }

    // Searches the node for the key and returns the slot of this key
    // - only for exact matches!
    template<typename Cmp>
    int find_exact(ham_key_t *key, Cmp &comparator) {
      int cmp;
      int r = find_impl(key, comparator, &cmp);
      return (cmp ? -1 : r);
    }

    // Iterates all keys, calls the |visitor| on each
    void scan(ScanVisitor *visitor, ham_u32_t start, bool distinct) {
#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count());
#endif

      // a distinct scan over fixed-length keys can be moved to the KeyList
      if (KeyList::kHasSequentialData && distinct) {
        m_keys.scan(visitor, start, m_node->get_count() - start);
        return;
      }

      // otherwise iterate over the keys, call visitor for each key
      size_t node_count = m_node->get_count() - start;
      ham_key_t key = {0};

      for (size_t i = start; i < node_count; i++) {
        m_keys.get_key(i, &m_arena, &key, false);
        (*visitor)(key.data, key.size, distinct ? 1 : get_record_count(i));
      }
    }

    // Returns a deep copy of the key
    void get_key(ham_u32_t slot, ByteArray *arena, ham_key_t *dest) {
      m_keys.get_key(slot, arena, dest);
    }

    // Returns the record size of a key or one of its duplicates
    ham_u64_t get_record_size(ham_u32_t slot, int duplicate_index) {
      return (m_records.get_record_size(slot, duplicate_index));
    }

    // Returns the number of records of a key
    ham_u32_t get_record_count(ham_u32_t slot) {
      return (m_records.get_record_count(slot));
    }

    // Returns the full record and stores it in |dest|
    void get_record(ham_u32_t slot, ByteArray *arena, ham_record_t *record,
                    ham_u32_t flags, ham_u32_t duplicate_index) {
#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count());
#endif
      m_records.get_record(slot, duplicate_index, arena, record, flags);
    }

    // Sets the record of a key, or adds a duplicate
    void set_record(ham_u32_t slot, ham_record_t *record,
                    ham_u32_t duplicate_index, ham_u32_t flags,
                    ham_u32_t *new_duplicate_index) {
      // automatically overwrite an existing key unless this is a
      // duplicate operation
      if ((flags & (HAM_DUPLICATE
                    | HAM_DUPLICATE
                    | HAM_DUPLICATE_INSERT_BEFORE
                    | HAM_DUPLICATE_INSERT_AFTER
                    | HAM_DUPLICATE_INSERT_FIRST
                    | HAM_DUPLICATE_INSERT_LAST)) == 0)
        flags |= HAM_OVERWRITE;

      m_records.set_record(slot, duplicate_index, record, flags,
              new_duplicate_index);
#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count());
#endif
    }

    // Erases an extended key
    void erase_key(ham_u32_t slot) {
      m_keys.erase_data(slot);
    }

    // Erases one (or all) records of a key
    void erase_record(ham_u32_t slot, ham_u32_t duplicate_index,
                    bool all_duplicates) {
      m_records.erase_record(slot, duplicate_index, all_duplicates);
#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count());
#endif
    }

    // Erases a key from the index. Does NOT erase the record(s)!
    void erase(ham_u32_t slot) {
      ham_u32_t node_count = m_node->get_count();
      m_keys.erase_slot(node_count, slot);
      m_records.erase_slot(node_count, slot);
#ifdef HAM_DEBUG
      check_index_integrity(node_count - 1);
#endif
    }

    // Inserts a new key at |slot|. 
    // Also inserts an empty record which has to be overwritten in
    // the next call of set_record().
    void insert(ham_u32_t slot, const ham_key_t *key) {
      size_t node_count = m_node->get_count();

      // make space for 1 additional element.
      // only store the key data; flags and record IDs are set by the caller
      m_keys.insert(node_count, slot, key);
      m_records.insert_slot(node_count, slot);

#ifdef HAM_DEBUG
      check_index_integrity(node_count + 1);
#endif
    }

    // Returns true if |key| cannot be inserted because a split is required.
    // This function will try to re-arrange the node in order for the new
    // key to fit in.
    bool requires_split(const ham_key_t *key) {
      size_t node_count = m_node->get_count();

      if (node_count == 0)
        return (false);

      // try to resize the lists before admitting defeat and splitting
      // the page
      bool keys_require_split = m_keys.requires_split(node_count, key);
      bool records_require_split = m_records.requires_split(node_count);
      if (!keys_require_split && !records_require_split)
        return (false);
      if (keys_require_split)
        keys_require_split = m_keys.requires_split(node_count, key, true);
      if (records_require_split)
        records_require_split = m_records.requires_split(node_count, true);
      if (keys_require_split || records_require_split) {
        if (adjust_capacity(key, keys_require_split, records_require_split)) {
#ifdef HAM_DEBUG
          check_index_integrity(node_count);
#endif
          return (false);
        }

#ifdef HAM_DEBUG
        check_index_integrity(node_count);
#endif

        // still here? then there's no way to avoid the split
        BtreeIndex *bi = m_page->get_db()->get_btree_index();
        bi->get_statistics()->set_page_capacity(m_node->is_leaf(),
                        m_capacity);
        bi->get_statistics()->set_keylist_range_size(m_node->is_leaf(),
                        m_keys.get_range_size());
        return (true);
      }

      return (false);
    }

    // Splits this node and moves some/half of the keys to |other|
    void split(DefaultNodeImpl *other, int pivot) {
      size_t node_count = m_node->get_count();

#ifdef HAM_DEBUG
      check_index_integrity(node_count);
      ham_assert(other->m_node->get_count() == 0);
#endif

      // make sure that the other node has enough free space
      other->initialize(this);

      //
      // if a leaf page is split then the pivot element must be inserted in
      // the leaf page AND in the internal node. this internal node update
      // is handled by the caller (in btree_insert.cc).
      //
      // in internal nodes the pivot element is propagated to the
      // parent node, and not inserted in the new sibling. the pivot element
      // is skipped.
      //
      // afterwards immediately vacuumize the indices, otherwise the next
      // insert() will not be able to reuse the new space
      if (m_node->is_leaf()) {
        m_keys.copy_to(pivot, node_count, other->m_keys, 0, 0);
        m_records.copy_to(pivot, node_count, other->m_records, 0,  0);
      }
      else {
        m_keys.copy_to(pivot + 1, node_count, other->m_keys, 0,  0);
        m_records.copy_to(pivot + 1, node_count, other->m_records, 0,  0);
      }

      m_keys.vacuumize(pivot, true);
      m_records.vacuumize(pivot, true);

#ifdef HAM_DEBUG
      check_index_integrity(pivot);
      if (m_node->is_leaf())
        other->check_index_integrity(node_count - pivot);
      else
        other->check_index_integrity(node_count - pivot - 1);
#endif
    }

    // Returns true if the node requires a merge or a shift
    bool requires_merge() const {
      return (m_node->get_count() <= 3);
    }

    // Merges keys from |other| to this node
    void merge_from(DefaultNodeImpl *other) {
      size_t node_count = m_node->get_count();
      size_t other_node_count = other->m_node->get_count();

      m_keys.vacuumize(node_count, true);
      m_records.vacuumize(node_count, true);

      // shift items from the sibling to this page
      other->m_keys.copy_to(0, other_node_count, m_keys,
                      node_count, node_count);
      other->m_records.copy_to(0, other_node_count, m_records,
                      node_count, node_count);

#ifdef HAM_DEBUG
      check_index_integrity(node_count + other_node_count);
#endif
    }

    // Returns a record id
    ham_u64_t get_record_id(ham_u32_t slot,
                    ham_u32_t duplicate_index = 0) const {
      return (m_records.get_record_id(slot, duplicate_index));
    }

    // Sets a record id; only for internal nodes!
    void set_record_id(ham_u32_t slot, ham_u64_t ptr) {
      m_records.set_record_id(slot, ptr);
    }

    // Prints a slot to stdout (for debugging)
    void print(ham_u32_t slot) {
      std::stringstream ss;
      ss << "   ";
      m_keys.print(slot, ss);
      ss << " -> ";
      m_records.print(slot, ss);
      std::cout << ss.str() << std::endl;
    }

  private:
    // Initializes the node
    void initialize(NodeType *other = 0) {
      LocalDatabase *db = m_page->get_db();

      // initialize this page in the same way as |other| was initialized
      if (other) {
        m_capacity = other->m_capacity;

        // persist the capacity
        ham_u8_t *p = m_node->get_data();
        *(ham_u32_t *)p = m_capacity;
        p += sizeof(ham_u32_t);

        // create the KeyList and RecordList
        size_t usable_page_size = get_usable_page_size();
        size_t key_range_size = other->m_keys.get_range_size();
        m_keys.create(p, key_range_size, m_capacity);
        m_records.create(p + key_range_size,
                        usable_page_size - key_range_size,
                        m_capacity);
      }
      // initialize a new page from scratch
      else if ((m_node->get_count() == 0
                && !(db->get_rt_flags() & HAM_READ_ONLY))) {
        size_t key_range_size;
        size_t record_range_size;
        size_t usable_page_size = get_usable_page_size();

        // if yes then ask the btree for the default capacity (it keeps
        // track of the average capacity of older pages).
        BtreeStatistics *bstats = db->get_btree_index()->get_statistics();
        m_capacity = bstats->get_page_capacity(m_node->is_leaf());
        key_range_size = bstats->get_keylist_range_size(m_node->is_leaf());

        // no data so far? then come up with a good default
        if (m_capacity == 0) {
          m_capacity = usable_page_size
                            / (m_keys.get_full_key_size()
                                + m_records.get_full_record_size());

          // calculate the sizes of the KeyList and RecordList
          if (KeyList::kHasSequentialData) {
            key_range_size = m_keys.get_full_key_size() * m_capacity;
            record_range_size = usable_page_size - key_range_size;
          }
          else if (RecordList::kHasSequentialData) {
            record_range_size = m_records.get_full_record_size() * m_capacity;
            key_range_size = usable_page_size - record_range_size;
          }
          else {
            key_range_size = m_keys.get_full_key_size() * m_capacity;
            record_range_size = m_records.get_full_record_size() * m_capacity;
          }
        }
        else {
          record_range_size = usable_page_size - key_range_size;
        }

        // persist the capacity
        ham_u8_t *p = m_node->get_data();
        *(ham_u32_t *)p = m_capacity;
        p += sizeof(ham_u32_t);

        // and create the lists
        m_keys.create(p, key_range_size, m_capacity);
        m_records.create(p + key_range_size, record_range_size, m_capacity);
      }
      // open a page; read initialization parameters from persisted storage
      else {
        // get the capacity
        ham_u8_t *p = m_node->get_data();
        m_capacity = *(ham_u32_t *)p;
        p += sizeof(ham_u32_t);

        m_keys.open(p, m_capacity);
        size_t key_range_size = m_keys.get_range_size();
        m_records.open(p + key_range_size, m_capacity);
      }
    }

    // Adjusts the capacity of both lists; either increases it or decreases
    // it (in order to free up space for variable length data).
    // Returns true if |key| and an additional record can be inserted, or
    // false if not; in this case the caller can perform a split.
    bool adjust_capacity(const ham_key_t *key, bool keys_require_split,
                    bool records_require_split) {
      size_t node_count = m_node->get_count();

      // One of the lists must be resizable (otherwise they would be managed
      // by the PaxLayout)
      ham_assert(!KeyList::kHasSequentialData
              || !RecordList::kHasSequentialData);

      size_t key_range_size = 0;
      size_t record_range_size = 0;
      size_t old_capacity = m_capacity;
      size_t new_capacity;
      size_t usable_page_size = get_usable_page_size();

      // We now have three options to make room for the new key/record pair:
      //
      // Option 1: if both lists are VariableLength and the capacity is
      // sufficient then we can just change the sizes of both lists
      if (!KeyList::kHasSequentialData && !RecordList::kHasSequentialData
              && node_count < old_capacity) {
        // KeyList range is too small: calculate the minimum required range
        // for the KeyList and check if the remaining space is large enough
        // for the RecordList
        size_t required = m_records.calculate_required_range_size(node_count,
                                      old_capacity);
        if (m_records.get_full_record_size() < 10)
          required += 10;
        else
          required += m_records.get_full_record_size();

        if (keys_require_split) {
          key_range_size = m_keys.calculate_required_range_size(node_count,
                                      old_capacity)
                            + m_keys.get_full_key_size(key);
          record_range_size = usable_page_size - key_range_size;
          if (record_range_size >= required) {
            new_capacity = old_capacity;
            goto apply_changes;
          }
        }
        // RecordList range is too small: calculate the minimum required range
        // for the RecordList and check if the remaining space is large enough
        // for the Keylist
        else {
          record_range_size = required;
          key_range_size = usable_page_size - record_range_size;
          if (key_range_size > m_keys.calculate_required_range_size(node_count,
                                old_capacity) + m_keys.get_full_key_size(key)) {
            new_capacity = old_capacity;
            goto apply_changes;
          }
        }
      }

      // Option 2: if the capacity is expleted then increase it.  
      if (node_count == old_capacity) {
        new_capacity = old_capacity + 1;
      }
      // Option 3: we reduce the capacity. This also reduces the metadata in
      // the Lists (the UpfrontIndex shrinks) and therefore generates room
      // for more data.
      else {
        size_t shrink_slots = (old_capacity - node_count) / 2;
        if (shrink_slots == 0)
          shrink_slots = 1;
        new_capacity = old_capacity - shrink_slots;
        if (new_capacity < node_count + 1)
          return (false);
      }

      // Calculate the range sizes for the new capacity
      if (KeyList::kHasSequentialData) {
        key_range_size = m_keys.calculate_required_range_size(node_count,
                                    new_capacity);
        record_range_size = m_records.calculate_required_range_size(
                                  node_count, new_capacity);
      }
      else if (RecordList::kHasSequentialData) {
        record_range_size = m_records.calculate_required_range_size(
                                  node_count, new_capacity);
        key_range_size = usable_page_size - record_range_size;
        if (key_range_size < m_keys.calculate_required_range_size(
                    node_count, new_capacity))
          return (false);
      }
      else {
        key_range_size = m_keys.calculate_required_range_size(node_count,
                                  new_capacity - 1)
                          + m_keys.get_full_key_size(key);
        record_range_size = m_records.calculate_required_range_size(
                                  node_count, new_capacity);
        int diff = usable_page_size - (key_range_size + record_range_size);
        if (diff > 10) // additional 10 bytes are reserved for the record list
          key_range_size += diff / 2;
      }

      // Check if the required record space is large enough, and make sure
      // there is enough room for a DuplicateTable id (if duplicates
      // are enabled)
apply_changes:
      if (key_range_size + record_range_size > usable_page_size)
        return (false);

      // Get a pointer to the data area and persist the new capacity
      ham_u8_t *p = m_node->get_data();
      *(ham_u32_t *)p = new_capacity;
      p += sizeof(ham_u32_t);

      // Now change the capacity in both lists. If the KeyList grows then
      // start with resizing the RecordList, otherwise the moved KeyList
      // will overwrite the beginning of the RecordList.
      if (key_range_size > m_keys.get_range_size()) {
        m_records.change_capacity(node_count, old_capacity, new_capacity,
                        p + key_range_size,
                        usable_page_size - key_range_size);
        m_keys.change_capacity(node_count, old_capacity, new_capacity,
                        p, key_range_size);
      }
      // And vice versa if the RecordList grows
      else {
        m_keys.change_capacity(node_count, old_capacity, new_capacity,
                        p, key_range_size);
        m_records.change_capacity(node_count, old_capacity, new_capacity,
                        p + key_range_size,
                        usable_page_size - key_range_size);
      }
      
      m_capacity = new_capacity;

      // make sure that the page is flushed to disk
      m_page->set_dirty(true);

      // finally check if the new space is sufficient for the new key
      return (!m_records.requires_split(node_count)
                && !m_keys.requires_split(node_count, key));
    }

    // Implementation of the find method; uses a linear search if possible
    template<typename Cmp>
    int find_impl(ham_key_t *key, Cmp &comparator, int *pcmp) {
      size_t node_count = m_node->get_count();
      ham_assert(node_count > 0);

#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count());
#endif

      int i, l = 0, r = (int)node_count;
      int last = node_count + 1;
      int cmp = -1;

      // Run a binary search, but fall back to linear search as soon as
      // the remaining range is too small. Sets threshold to 0 if linear
      // search is disabled for this KeyList.
      int threshold = m_keys.get_linear_search_threshold();

      /* repeat till we found the key or the remaining range is so small that
       * we rather perform a linear search (which is faster for small ranges) */
      while (r - l > threshold) {
        /* get the median item; if it's identical with the "last" item,
         * we've found the slot */
        i = (l + r) / 2;

        if (i == last) {
          ham_assert(i >= 0);
          ham_assert(i < (int)node_count);
          *pcmp = 1;
          return (i);
        }

        /* compare it against the key */
        cmp = compare(key, i, comparator);

        /* found it? */
        if (cmp == 0) {
          *pcmp = cmp;
          return (i);
        }
        /* if the key is bigger than the item: search "to the left" */
        else if (cmp < 0) {
          if (r == 0) {
            ham_assert(i == 0);
            *pcmp = cmp;
            return (-1);
          }
          r = i;
        }
        /* otherwise search "to the right" */
        else {
          last = i;
          l = i;
        }
      }

      // still here? then perform a linear search for the remaining range
      ham_assert(r - l <= threshold);
      return (m_keys.linear_search(l, r - l, key, comparator, pcmp));
    }

    // Checks the integrity of the key- and record-ranges. Throws an exception
    // if there's a problem.
    void check_index_integrity(size_t node_count) const {
      m_keys.check_integrity(node_count, true);
      m_records.check_integrity(node_count, true);
    }

    // Returns the usable page size that can be used for actually
    // storing the data
    size_t get_usable_page_size() const {
      return (m_page->get_db()->get_local_env()->get_usable_page_size()
                    - kPayloadOffset
                    - PBtreeNode::get_entry_offset()
                    - sizeof(ham_u32_t));
    }


    // The page that we're operating on
    Page *m_page;

    // The node that we're operating on
    PBtreeNode *m_node;

    // The KeyList provides access to the stored keys
    KeyList m_keys;

    // The RecordList provides access to the stored records
    RecordList m_records;

    // A memory arena for various tasks
    ByteArray m_arena;

    // The current capacity of the node
    size_t m_capacity;
};

} // namespace hamsterdb

#endif /* HAM_BTREE_IMPL_DEFAULT_H__ */
