/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 * Btree node layout for variable length keys/records and duplicates
 * =================================================================
 *
 * This is the default hamsterdb layout. It is chosen for
 * 1. variable length keys (with or without duplicates)
 * 2. fixed length keys with duplicates
 *
 * Unlike the PAX layout implemented in btree_impl_pax.h, the layout implemented
 * here stores key data and records next to each other. However, since keys
 * (and duplicate records) have variable length, each node has a small
 * index area upfront. This index area stores metadata about the key like
 * the key's size, the number of records (=duplicates), flags and the
 * offset of the actual data.
 *
 * The actual data starting at this offset contains the key's data (which
 * can be a 64bit blob ID if the key is too big), and the record's data.
 * If duplicate keys exist, then all records are stored next to each other.
 * If there are too many duplicates, then all of them are offloaded to
 * a blob - a "duplicate table".
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
 * If keys exceed a certain Threshold (get_extended_threshold()), they're moved
 * to a blob and the flag |kExtendedKey| is set for this key. These extended
 * keys are cached in a std::map to improve performance.
 *
 * This layout supports duplicate keys. If the number of duplicate keys
 * exceeds a certain threshold (get_duplicate_threshold()), they are all moved
 * to a table which is stored as a blob, and the |kExtendedDuplicates| flag
 * is set.
 * The record counter is 1 byte. It counts the total number of inline records
 * assigned to the current key (a.k.a the number of duplicate keys). It is
 * not used if the records were moved to a duplicate table.
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
#include <vector>
#include <map>

#include "util.h"
#include "page.h"
#include "btree_node.h"
#include "blob_manager.h"
#include "env_local.h"
#include "btree_index.h"

#ifdef WIN32
// MSVC: disable warning about use of 'this' in base member initializer list
#  pragma warning(disable:4355)
#endif

namespace hamsterdb {

#undef min  // avoid MSVC conflicts with std::min

template<typename LayoutImpl, typename RecordList>
class DefaultNodeImpl;

//
// An iterator for the DefaultNodeImpl
//
template<typename LayoutImpl, typename RecordList>
class DefaultIterator
{
  public:
    // Constructor
    DefaultIterator(DefaultNodeImpl<LayoutImpl, RecordList> *node,
                    ham_u32_t slot)
      : m_node(node), m_slot(slot) {
    } 

    // Constructor
    DefaultIterator(const DefaultNodeImpl<LayoutImpl, RecordList> *node,
                    ham_u32_t slot)
      : m_node((DefaultNodeImpl<LayoutImpl, RecordList> *)node),
        m_slot(slot) {
    } 

    // Moves this iterator forward
    void next() {
      m_slot++;
    }

    // Returns the slot
    int get_slot() const {
      return (m_slot);
    }

    // Returns the (persisted) flags of a key; defined in btree_flags.h
    ham_u8_t get_key_flags() const {
      return (m_node->get_key_flags(m_slot));
    }

    // Sets the flags of a key; defined in btree_flags.h
    void set_key_flags(ham_u32_t flags) {
      m_node->set_key_flags(m_slot, flags);
    }

    // Returns the flags of a record; defined in btree_flags.h
    ham_u8_t get_record_flags(ham_u32_t duplicate_index = 0) const {
      return (m_node->m_records.get_record_flags(m_slot, duplicate_index));
    }

    // Sets the flags of a record; defined in btree_flags.h
    void set_record_flags(ham_u8_t flags, ham_u32_t duplicate_index = 0) {
      m_node->m_records.set_record_flags(m_slot, flags, duplicate_index);
    }

    // Returns the key size
    ham_u16_t get_key_size() const {
      return (m_node->get_key_size(m_slot));
    }

    // Sets the key size
    void set_key_size(ham_u16_t size) {
      return (m_node->set_key_size(m_slot, size));
    }

    // Returns the actually used size of memory occupied; if this key is an
    // extended key then it returns 8 (the size of the blob id), otherwise
    // the key size is returned
    ham_u16_t get_key_data_size() const {
      return (m_node->get_key_data_size(m_slot));
    }

    // Returns a pointer to the inline key data (which can be a blob id if
    // the key is extended)
    ham_u8_t *get_key_data() {
      return (m_node->get_key_data(m_slot));
    }

    // Returns a pointer to the key data (which can be a blob id if
    // the key is extended)
    const ham_u8_t *get_key_data() const {
      return (m_node->get_key_data(m_slot));
    }

    // Overwrites the key data
    void set_key_data(const void *ptr, ham_u32_t len) {
      return (m_node->set_key_data(m_slot, ptr, len));
    }
  
    // Returns the record address of an extended key overflow area
    ham_u64_t get_extended_blob_id() const {
      ham_u64_t rid = *(ham_u64_t *)get_key_data();
      return (ham_db2h_offset(rid));
    }

    // Sets the record address of an extended key overflow area
    void set_extended_blob_id(ham_u64_t rid) {
      *(ham_u64_t *)get_key_data() = ham_h2db_offset(rid);
    }

    // Returns the record id
    ham_u64_t get_record_id(ham_u32_t duplicate_index = 0) const {
      return (m_node->get_record_id(m_slot, duplicate_index));
    }

    // Sets the record id
    void set_record_id(ham_u64_t ptr, ham_u32_t duplicate_index = 0) {
      set_key_flags(get_key_flags() & (~BtreeKey::kInitialized));
      return (m_node->set_record_id(m_slot, ptr, duplicate_index));
    }

    // Returns true if the record is inline
    bool is_record_inline(ham_u32_t duplicate_index = 0) const {
      return (m_node->is_record_inline(m_slot, duplicate_index));
    }

    // Returns a pointer to the record's inline data
    void *get_inline_record_data(ham_u32_t duplicate_index = 0) {
      return (m_node->get_inline_record_data(m_slot, duplicate_index));
    }

    // Returns a pointer to the record's inline data
    void *get_inline_record_data(ham_u32_t duplicate_index = 0) const {
      ham_assert(is_record_inline() == true);
      return (m_node->get_inline_record_data(m_slot, duplicate_index));
    }

    // Sets the inline record data
    void set_inline_record_data(const void *data, ham_u32_t size,
                    ham_u32_t duplicate_index = 0) {
      m_node->set_inline_record_data(m_slot, data, size, duplicate_index);
    }

    // Returns the size of the record, if inline
    ham_u32_t get_inline_record_size(ham_u32_t duplicate_index = 0) const {
      return (m_node->get_inline_record_size(m_slot, duplicate_index));
    }

    // Returns the maximum size of inline records (Payload only!)
    ham_u32_t get_max_inline_record_size() const {
      return (m_node->get_max_inline_record_size());
    }

    // Returns the maximum size of inline records (INCL overhead!)
    ham_u32_t get_total_inline_record_size() const {
      return (m_node->get_total_inline_record_size());
    }

    // Removes an inline record; basically this overwrites the record's
    // data with nulls and resets the flags. Does NOT "shift" the remaining
    // records "to the left"!
    void remove_inline_record(ham_u32_t duplicate_index = 0) {
      set_record_flags(0, duplicate_index);
      memset(get_inline_record_data(duplicate_index),
                      0, get_total_inline_record_size());
    }

    // Returns the total record count of the current key (only inline!)
    ham_u32_t get_inline_record_count() {
      return (m_node->m_layout.get_inline_record_count(m_slot));
    }

    // Sets the inline record count
    void set_inline_record_count(ham_u8_t count) {
      m_node->set_inline_record_count(m_slot, count);
    }

    // Allows use of operator-> in the caller
    DefaultIterator *operator->() {
      return (this);
    }

    // Allows use of operator-> in the caller
    const DefaultIterator *operator->() const {
      return (this);
    }

  private:
    // Pointer to the node
    DefaultNodeImpl<LayoutImpl, RecordList> *m_node;

    // The current slot
    int m_slot;
};

//
// A (static) helper class for dealing with extended duplicate tables
//
struct DuplicateTable
{
  // Returns the number of used elements in a duplicate table
  static ham_u32_t get_count(ByteArray *table) {
    ham_assert(table->get_size() > 4);
    ham_u32_t count = *(ham_u32_t *)table->get_ptr();
    return (ham_db2h32(count));
  }

  // Sets the number of used elements in a duplicate table
  static void set_count(ByteArray *table, ham_u32_t count) {
    *(ham_u32_t *)table->get_ptr() = ham_h2db32(count);
  }

  // Returns the maximum capacity of elements in a duplicate table
  static ham_u32_t get_capacity(ByteArray *table) {
    ham_assert(table->get_size() >= 8);
    ham_u32_t count = *(ham_u32_t *)((ham_u8_t *)table->get_ptr() + 4);
    return (ham_db2h32(count));
  }

  // Sets the maximum capacity of elements in a duplicate table
  static void set_capacity(ByteArray *table, ham_u32_t capacity) {
    ham_assert(table->get_size() >= 8);
    *(ham_u32_t *)((ham_u8_t *)table->get_ptr() + 4) = ham_h2db32(capacity);
  }
};

//
// A LayoutImplementation for fixed size keys WITH duplicates.
// This class has two template parameters:
//   |Offset| is the type that is supposed to be used for offset pointers
//     into the data area of the node. If the page size is small enough, two
//     bytes (ham_u16_t) are used. Otherwise four bytes (ham_u32_t) are
//     required.
//   |HasDuplicates| is a boolean whether this layout should support duplicate
//     keys. If yes, each index contains a duplicate counter.
//
template<typename Offset, bool HasDuplicates>
class FixedLayoutImpl
{
    enum {
      // 1 byte flags + 2 (or 4) byte offset
      //   + 1 byte record counter (optional)
      kSpan = 1 + sizeof(Offset) + (HasDuplicates ? 1 : 0)
    };

  public:
    // Performs initialization
    void initialize(ham_u8_t *data, ham_u32_t key_size) {
      m_data = data;
      m_key_size = key_size;
      // this layout only works with fixed sizes!
      ham_assert(m_key_size != HAM_KEY_SIZE_UNLIMITED);
    }

    // Returns a pointer to this key's index
    ham_u8_t *get_key_index_ptr(ham_u32_t slot) {
      return (&m_data[kSpan * slot]);
    }

    // Returns the memory span from one key to the next
    ham_u32_t get_key_index_span() const {
      return (kSpan);
    }

    // Returns the (persisted) flags of a key; defined in btree_flags.h
    ham_u8_t get_key_flags(ham_u32_t slot) const {
      return (m_data[kSpan * slot]);
    }

    // Sets the flags of a key; defined in btree_flags.h
    void set_key_flags(ham_u32_t slot, ham_u8_t flags) {
      m_data[kSpan * slot] = flags;
    }

    // Returns the size of a key
    ham_u16_t get_key_size(ham_u32_t slot) const {
      return (m_key_size);
    }

    // Sets the size of a key
    void set_key_size(ham_u32_t slot, ham_u16_t size) {
      ham_assert(size == m_key_size);
    }

    // Sets the start offset of the key data
    void set_key_data_offset(ham_u32_t slot, ham_u32_t offset) {
      ham_u8_t *p;
      if (HasDuplicates)
        p = &m_data[kSpan * slot + 2];
      else
        p = &m_data[kSpan * slot + 1];
      if (sizeof(Offset) == 4)
        *(ham_u32_t *)p = ham_h2db32(offset);
      else
        *(ham_u16_t *)p = ham_h2db16((ham_u16_t)offset);
    }

    // Returns the start offset of a key's data
    ham_u32_t get_key_data_offset(ham_u32_t slot) const {
      ham_u8_t *p;
      if (HasDuplicates)
        p = &m_data[kSpan * slot + 2];
      else
        p = &m_data[kSpan * slot + 1];
      if (sizeof(Offset) == 4)
        return (ham_db2h32(*(ham_u32_t *)p));
      else
        return (ham_db2h16(*(ham_u16_t *)p));
    }

    // Sets the record count of a key.
    // If this layout does not support duplicates, then an internal flag
    // is set.
    void set_inline_record_count(ham_u32_t slot, ham_u8_t count) {
      if (HasDuplicates == true)
        m_data[kSpan * slot + 1] = count;
      else {
        if (count == 0)
          set_key_flags(slot, get_key_flags(slot) | BtreeKey::kHasNoRecords);
        else
          set_key_flags(slot, get_key_flags(slot) & (~BtreeKey::kHasNoRecords));
      }
    }

    // Returns the record counter
    ham_u8_t get_inline_record_count(ham_u32_t slot) const {
      if (HasDuplicates)
        return (m_data[kSpan * slot + 1]);
      return (get_key_flags(slot) & BtreeKey::kHasNoRecords
                      ? 0
                      : 1);
    }

  private:
    // Pointer to the data
    ham_u8_t *m_data;

    // The constant key size
    ham_u16_t m_key_size;
};

//
// A LayoutImplementation for variable size keys. Uses the same template
// parameters as |FixedLayoutImpl|.
//
template<typename Offset, bool HasDuplicates>
class DefaultLayoutImpl
{
    enum {
      // 1 byte flags + 2 byte key size + 2 (or 4) byte offset
      //   + 1 byte record counter (optional)
      kSpan = 3 + sizeof(Offset) + (HasDuplicates ? 1 : 0)
    };

  public:
    // Initialization
    void initialize(ham_u8_t *data, ham_u32_t key_size) {
      m_data = data;
      // this layout only works with unlimited/variable sizes!
      ham_assert(key_size == HAM_KEY_SIZE_UNLIMITED);
    }

    // Returns a pointer to the index of a key
    ham_u8_t *get_key_index_ptr(ham_u32_t slot) {
      return (&m_data[kSpan * slot]);
    }

    // Returns the memory span from one key to the next
    ham_u32_t get_key_index_span() const {
      return (kSpan);
    }

    // Returns the (persisted) flags of a key; see btree_flags.h
    ham_u8_t get_key_flags(ham_u32_t slot) const {
      return (m_data[kSpan * slot]);
    }

    // Sets the flags of a key; defined in btree_flags.h
    void set_key_flags(ham_u32_t slot, ham_u8_t flags) {
      m_data[kSpan * slot] = flags;
    }

    // Returns the size of a key
    ham_u16_t get_key_size(ham_u32_t slot) const {
      ham_u8_t *p = &m_data[kSpan * slot + 1];
      return (ham_db2h16(*(ham_u16_t *)p));
    }

    // Sets the size of a key
    void set_key_size(ham_u32_t slot, ham_u16_t size) {
      ham_u8_t *p = &m_data[kSpan * slot + 1];
      *(ham_u16_t *)p = ham_h2db16(size);
    }

    // Sets the start offset of the key data
    void set_key_data_offset(ham_u32_t slot, ham_u32_t offset) {
      ham_u8_t *p;
      if (HasDuplicates)
        p = &m_data[kSpan * slot + 4];
      else
        p = &m_data[kSpan * slot + 3];
      if (sizeof(Offset) == 4)
        *(ham_u32_t *)p = ham_h2db32(offset);
      else
        *(ham_u16_t *)p = ham_h2db16((ham_u16_t)offset);
    }

    // Returns the start offset of the key data
    ham_u32_t get_key_data_offset(ham_u32_t slot) const {
      ham_u8_t *p;
      if (HasDuplicates)
        p = &m_data[kSpan * slot + 4];
      else
        p = &m_data[kSpan * slot + 3];
      if (sizeof(Offset) == 4)
        return (ham_db2h32(*(ham_u32_t *)p));
      else
        return (ham_db2h16(*(ham_u16_t *)p));
    }

    // Sets the record counter
    void set_inline_record_count(ham_u32_t slot, ham_u8_t count) {
      if (HasDuplicates == true)
        m_data[kSpan * slot + 3] = count;
      else {
        if (count == 0)
          set_key_flags(slot, get_key_flags(slot) | BtreeKey::kHasNoRecords);
        else
          set_key_flags(slot, get_key_flags(slot) & (~BtreeKey::kHasNoRecords));
      }
    }

    // Returns the record counter
    ham_u8_t get_inline_record_count(ham_u32_t slot) const {
      if (HasDuplicates)
        return (m_data[kSpan * slot + 3]);
      return (get_key_flags(slot) & BtreeKey::kHasNoRecords
                      ? 0
                      : 1);
    }

  private:
    // the serialized data
    ham_u8_t *m_data;
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

// move every key > threshold to a blob. For testing purposes.
extern ham_u32_t g_extended_threshold;

// create duplicate table if amount of duplicates > threshold. For testing
// purposes.
extern ham_u32_t g_duplicate_threshold;

// for counting extended keys
extern ham_u64_t g_extended_keys;

// for counting extended duplicate tables
extern ham_u64_t g_extended_duptables;

//
// A RecordList for the default inline records, storing 8 byte record IDs
// or inline records with size <= 8 bytes. If duplicates are supported then
// the record flags are stored in an additional byte, otherwise they're
// stored in the key flags.
//
template<typename LayoutImpl, bool HasDuplicates>
class DefaultInlineRecordImpl
{
    typedef DefaultNodeImpl<LayoutImpl, DefaultInlineRecordImpl> NodeType;

  public:
    // Constructor
    DefaultInlineRecordImpl(NodeType *layout, ham_u32_t record_size)
      : m_layout(layout) {
    }

    // Constructor
    DefaultInlineRecordImpl(const NodeType *layout, ham_u32_t record_size)
      : m_layout((NodeType *)layout) {
    }

    // Returns true if the record is inline
    bool is_record_inline(ham_u32_t slot,
                    ham_u32_t duplicate_index = 0) const {
      ham_u32_t flags = get_record_flags(slot, duplicate_index);
      return ((flags & BtreeRecord::kBlobSizeTiny)
              || (flags & BtreeRecord::kBlobSizeSmall)
              || (flags & BtreeRecord::kBlobSizeEmpty) != 0);
    }

    // Sets the inline record data
    void set_inline_record_data(ham_u32_t slot, const void *data,
                    ham_u32_t size, ham_u32_t duplicate_index) {
      if (size == 0) {
        set_record_flags(slot, BtreeRecord::kBlobSizeEmpty, duplicate_index);
        return;
      }
      if (size < 8) {
        /* the highest byte of the record id is the size of the blob */
        char *p = (char *)m_layout->get_inline_record_data(slot,
                        duplicate_index);
        p[sizeof(ham_u64_t) - 1] = size;
        memcpy(p, data, size);
        set_record_flags(slot, BtreeRecord::kBlobSizeTiny, duplicate_index);
        return;
      }
      else if (size == 8) {
        char *p = (char *)m_layout->get_inline_record_data(slot,
                        duplicate_index);
        memcpy(p, data, size);
        set_record_flags(slot, BtreeRecord::kBlobSizeSmall, duplicate_index);
        return;
      }

      ham_verify(!"shouldn't be here");
    }

    // Returns the size of the record, if inline
    ham_u32_t get_inline_record_size(ham_u32_t slot,
                    ham_u32_t duplicate_index)const {
      ham_u32_t flags = get_record_flags(slot, duplicate_index);

      ham_assert(m_layout->is_record_inline(slot, duplicate_index) == true);

      if (flags & BtreeRecord::kBlobSizeTiny) {
        /* the highest byte of the record id is the size of the blob */
        char *p = (char *)m_layout->get_inline_record_data(slot,
                        duplicate_index);
        return (p[sizeof(ham_u64_t) - 1]);
      }
      if (flags & BtreeRecord::kBlobSizeSmall)
        return (sizeof(ham_u64_t));
      if (flags & BtreeRecord::kBlobSizeEmpty)
        return (0);
      ham_verify(!"shouldn't be here");
      return (0);
    }

    // Returns the maximum size of inline records PAYLOAD ONLY!
    ham_u32_t get_max_inline_record_size() const {
      return (sizeof(ham_u64_t));
    }

    // Returns the maximum size of inline records INCL overhead!
    ham_u32_t get_total_inline_record_size() const {
      return (sizeof(ham_u64_t) + 1);
    }

    // Returns the flags of a record
    ham_u8_t get_record_flags(ham_u32_t slot, ham_u32_t duplicate_index) const {
      if (!HasDuplicates) {
        ham_u8_t flags = m_layout->get_key_flags(slot);
        return (flags & (BtreeRecord::kBlobSizeTiny
                        | BtreeRecord::kBlobSizeSmall
                        | BtreeRecord::kBlobSizeEmpty));
      }
      ham_u8_t *p = (ham_u8_t *)m_layout->get_inline_record_data(slot,
                      duplicate_index);
      return (*(p + get_max_inline_record_size()));
    }

    // Sets the flags of a record
    void set_record_flags(ham_u32_t slot, ham_u8_t flags,
                    ham_u32_t duplicate_index) {
      if (!HasDuplicates) {
        ham_u8_t oldflags = m_layout->get_key_flags(slot);
        oldflags &= ~(BtreeRecord::kBlobSizeTiny
                        | BtreeRecord::kBlobSizeSmall
                        | BtreeRecord::kBlobSizeEmpty);
        m_layout->set_key_flags(slot, oldflags | flags);
        return;
      }
      ham_u8_t *p = (ham_u8_t *)m_layout->get_inline_record_data(slot,
                      duplicate_index);
      *(p + get_max_inline_record_size()) = flags;
    }

    // Returns true if a record in a duplicate table is inline
    bool table_is_record_inline(ByteArray *table,
                    ham_u32_t duplicate_index) const {
      ham_assert(HasDuplicates == true);
      ham_assert(duplicate_index < DuplicateTable::get_count(table));
      ham_u8_t *ptr = table_get_record_data(table, duplicate_index);
      ham_u32_t flags = *(ptr + get_max_inline_record_size());
      return ((flags & BtreeRecord::kBlobSizeTiny)
              || (flags & BtreeRecord::kBlobSizeSmall)
              || (flags & BtreeRecord::kBlobSizeEmpty) != 0);
    }

    // Returns the size of a record from the duplicate table
    ham_u32_t table_get_record_size(ByteArray *table,
                    ham_u32_t duplicate_index) const {
      ham_assert(HasDuplicates == true);
      ham_assert(duplicate_index < DuplicateTable::get_count(table));
      ham_u8_t *ptr = table_get_record_data(table, duplicate_index);
      ham_u32_t flags = *(ptr + get_max_inline_record_size());
      if (flags & BtreeRecord::kBlobSizeTiny) {
        /* the highest byte of the record id is the size of the blob */
        return (ptr[sizeof(ham_u64_t) - 1]);
      }
      if (flags & BtreeRecord::kBlobSizeSmall)
        return (sizeof(ham_u64_t));
      if (flags & BtreeRecord::kBlobSizeEmpty)
        return (0);
      ham_assert(!"shouldn't be here");
      return (0);
    }

    // Returns a record id from the duplicate table
    ham_u64_t table_get_record_id(ByteArray *table,
                    ham_u32_t duplicate_index) const {
      ham_assert(HasDuplicates == true);
      ham_assert(duplicate_index < DuplicateTable::get_count(table));
      ham_u8_t *ptr = table_get_record_data(table, duplicate_index);
      return (ham_db2h_offset(*(ham_u64_t *)ptr));
    }

    // Sets a record id in the duplicate table
    void table_set_record_id(ByteArray *table,
                    ham_u32_t duplicate_index, ham_u64_t id) {
      ham_assert(HasDuplicates == true);
      ham_assert(duplicate_index < DuplicateTable::get_capacity(table));
      ham_u8_t *ptr = table_get_record_data(table, duplicate_index);
      *(ham_u64_t *)ptr = ham_h2db_offset(id);
      // initialize flags
      *(ptr + get_max_inline_record_size()) = 0;
    }

    // Returns a pointer to the inline record data from the duplicate table
    ham_u8_t *table_get_record_data(ByteArray *table,
                    ham_u32_t duplicate_index) const {
      ham_assert(HasDuplicates == true);
      ham_assert(duplicate_index < DuplicateTable::get_capacity(table));
      ham_u8_t *ptr = (ham_u8_t *)table->get_ptr();
      ptr += 8; // skip count, capacity and other records
      ptr += get_total_inline_record_size() * duplicate_index;
      return (ptr);
    }

    // Sets the inline record data in the duplicate table
    void table_set_record_data(ByteArray *table, ham_u32_t duplicate_index,
                    void *data, ham_u32_t size) {
      ham_assert(HasDuplicates == true);
      ham_assert(duplicate_index < DuplicateTable::get_capacity(table));
      ham_u8_t *ptr = table_get_record_data(table, duplicate_index);
      ham_assert(size <= 8);

      if (size == 0) {
        *(ptr + get_max_inline_record_size()) = BtreeRecord::kBlobSizeEmpty;
        return;
      }
      if (size < 8) {
        /* the highest byte of the record id is the size of the blob */
        ptr[sizeof(ham_u64_t) - 1] = size;
        memcpy(ptr, data, size);
        *(ptr + get_max_inline_record_size()) = BtreeRecord::kBlobSizeTiny;
        return;
      }
      if (size == 8) {
        *(ptr + get_max_inline_record_size()) = BtreeRecord::kBlobSizeSmall;
        memcpy(ptr, data, size);
        return;
      }
      ham_assert(!"shouldn't be here");
    }

    // Deletes a record from the table; also adjusts the count
    void table_erase_record(ByteArray *table, ham_u32_t duplicate_index) {
      ham_u32_t count = DuplicateTable::get_count(table);
      ham_assert(duplicate_index < count);
      ham_assert(count > 0);
      if (duplicate_index < count - 1) {
        ham_u8_t *lhs = table_get_record_data(table, duplicate_index);
        ham_u8_t *rhs = lhs + get_total_inline_record_size();
        memmove(lhs, rhs, get_total_inline_record_size()
                                * (count - duplicate_index - 1));
      }
      // adjust the counter
      DuplicateTable::set_count(table, count - 1);
    }

  private:
    NodeType *m_layout;
};

//
// A RecordList for fixed length inline records
//
template<typename LayoutImpl>
class FixedInlineRecordImpl
{
    typedef DefaultNodeImpl<LayoutImpl, FixedInlineRecordImpl> NodeType;

  public:
    FixedInlineRecordImpl(NodeType *layout, ham_u32_t record_size)
      : m_record_size(record_size), m_layout(layout) {
    }

    FixedInlineRecordImpl(const NodeType *layout, ham_u32_t record_size)
      : m_record_size(record_size), m_layout((NodeType *)layout) {
    }

    // Returns true if the record is inline
    bool is_record_inline(ham_u32_t slot, ham_u32_t duplicate_index = 0) const {
      return (true);
    }

    // Sets the inline record data
    void set_inline_record_data(ham_u32_t slot, const void *data,
                    ham_u32_t size, ham_u32_t duplicate_index) {
      ham_assert(size == m_record_size);
      char *p = (char *)m_layout->get_inline_record_data(slot, duplicate_index);
      memcpy(p, data, size);
    }

    // Returns the size of the record, if inline
    ham_u32_t get_inline_record_size(ham_u32_t slot,
                    ham_u32_t duplicate_index) const {
      return (m_record_size);
    }

    // Returns the maximum size of inline records PAYLOAD only!
    ham_u32_t get_max_inline_record_size() const {
      return (m_record_size);
    }

    // Returns the maximum size of inline records INCL overhead!
    ham_u32_t get_total_inline_record_size() const {
      return (get_max_inline_record_size());
    }

    // Returns the flags of a record
    ham_u8_t get_record_flags(ham_u32_t slot, ham_u32_t duplicate_index) const {
      ham_u8_t flags = m_layout->get_key_flags(slot);
      return (flags & (BtreeRecord::kBlobSizeTiny
                        | BtreeRecord::kBlobSizeSmall
                        | BtreeRecord::kBlobSizeEmpty));
    }

    // Sets the flags of a record
    void set_record_flags(ham_u32_t slot, ham_u8_t flags,
                    ham_u32_t duplicate_index) {
      ham_u8_t oldflags = m_layout->get_key_flags(slot);
      oldflags &= ~(BtreeRecord::kBlobSizeTiny
                      | BtreeRecord::kBlobSizeSmall
                      | BtreeRecord::kBlobSizeEmpty);
      m_layout->set_key_flags(slot, oldflags | flags);
    }

    // Returns true if a record in a duplicate table is inline
    bool table_is_record_inline(ByteArray *table,
                    ham_u32_t duplicate_index) const {
      return (true);
    }

    // Returns the size of a record from the duplicate table
    ham_u32_t table_get_record_size(ByteArray *table,
                    ham_u32_t duplicate_index) const {
      return (m_record_size);
    }

    // Returns a record id from the duplicate table
    ham_u64_t table_get_record_id(ByteArray *table,
                    ham_u32_t duplicate_index) const {
      ham_assert(duplicate_index < DuplicateTable::get_count(table));
      ham_u8_t *ptr = table_get_record_data(table, duplicate_index);
      return (ham_db2h_offset(*(ham_u64_t *)ptr));
    }

    // Sets a record id in the duplicate table
    void table_set_record_id(ByteArray *table,
                    ham_u32_t duplicate_index, ham_u64_t id) {
      ham_assert(duplicate_index < DuplicateTable::get_count(table));
      ham_u8_t *ptr = table_get_record_data(table, duplicate_index);
      *(ham_u64_t *)ptr = ham_h2db_offset(*(ham_u64_t *)ptr);
    }

    // Returns a pointer to the inline record data from the duplicate table
    ham_u8_t *table_get_record_data(ByteArray *table,
                    ham_u32_t duplicate_index) const {
      ham_assert(duplicate_index < DuplicateTable::get_capacity(table));
      ham_u8_t *ptr = (ham_u8_t *)table->get_ptr();
      ptr += 8; // skip count, capacity and other records
      ptr += get_max_inline_record_size() * duplicate_index;
      return (ptr);
    }

    // Sets the inline record data in the duplicate table
    void table_set_record_data(ByteArray *table, ham_u32_t duplicate_index,
                    void *data, ham_u32_t size) {
      ham_assert(duplicate_index < DuplicateTable::get_capacity(table));
      ham_assert(size == m_record_size);
      ham_u8_t *ptr = table_get_record_data(table, duplicate_index);
      memcpy(ptr, data, size);
    }

    // Deletes a record from the table; also adjusts the count
    void table_erase_record(ByteArray *table, ham_u32_t duplicate_index) {
      ham_u32_t count = DuplicateTable::get_count(table);
      ham_assert(duplicate_index < count);
      ham_assert(count > 0);
      if (duplicate_index < count - 1) {
        ham_u8_t *lhs = table_get_record_data(table, duplicate_index);
        ham_u8_t *rhs = table_get_record_data(table, duplicate_index + 1);
        memmove(lhs, rhs, get_max_inline_record_size()
                                * (count - duplicate_index - 1));
      }
      // adjust the counter
      DuplicateTable::set_count(table, count - 1);
    }

  private:
    // the record size, as specified when the database was created
    ham_u32_t m_record_size;

    // pointer to the parent layout
    NodeType *m_layout;
};

//
// A RecordList for variable length inline records of size 8 (for internal
// nodes, no duplicates). Internal nodes only store Page IDs as records. This
// class is optimized for record IDs.
//
template<typename LayoutImpl>
class InternalInlineRecordImpl
{
    typedef DefaultNodeImpl<LayoutImpl, InternalInlineRecordImpl> NodeType;

  public:
    // Constructor
    InternalInlineRecordImpl(NodeType *layout, ham_u32_t record_size)
      : m_layout(layout) {
    }

    // Constructor
    InternalInlineRecordImpl(const NodeType *layout, ham_u32_t record_size)
      : m_layout((NodeType *)layout) {
    }

    // Returns true if the record is inline
    bool is_record_inline(ham_u32_t slot, ham_u32_t duplicate_index = 0) const {
      ham_assert(duplicate_index == 0);
      return (true);
    }

    // Sets the inline record data
    void set_inline_record_data(ham_u32_t slot, const void *data,
                    ham_u32_t size, ham_u32_t duplicate_index) {
      ham_assert(size == sizeof(ham_u64_t));
      ham_assert(duplicate_index == 0);
      char *p = (char *)m_layout->get_inline_record_data(slot);
      memcpy(p, data, size);
    }

    // Returns the size of the record, if inline
    ham_u32_t get_inline_record_size(ham_u32_t slot,
                    ham_u32_t duplicate_index) const {
      ham_assert(duplicate_index == 0);
      return (sizeof(ham_u64_t));
    }

    // Returns the maximum size of inline records PAYLOAD only!
    ham_u32_t get_max_inline_record_size() const {
      return (sizeof(ham_u64_t));
    }

    // Returns the maximum size of inline records INCL overhead!
    ham_u32_t get_total_inline_record_size() const {
      return (get_max_inline_record_size());
    }

    // Returns the flags of a record
    ham_u8_t get_record_flags(ham_u32_t slot, ham_u32_t duplicate_index) const {
      ham_u8_t flags = m_layout->get_key_flags(slot);
      return (flags & (BtreeRecord::kBlobSizeTiny
                        | BtreeRecord::kBlobSizeSmall
                        | BtreeRecord::kBlobSizeEmpty));
    }

    // Sets the flags of a record
    void set_record_flags(ham_u32_t slot, ham_u8_t flags,
                    ham_u32_t duplicate_index) {
      ham_u8_t oldflags = m_layout->get_key_flags(slot);
      oldflags &= ~(BtreeRecord::kBlobSizeTiny
                      | BtreeRecord::kBlobSizeSmall
                      | BtreeRecord::kBlobSizeEmpty);
      m_layout->set_key_flags(slot, oldflags | flags);
    }

    // Returns true if a record in a duplicate table is inline
    bool table_is_record_inline(ByteArray *table,
                    ham_u32_t duplicate_index) const {
      ham_assert(!"shouldn't be here");
      return (false);
    }

    // Returns the size of a record from the duplicate table
    ham_u32_t table_get_record_size(ByteArray *table,
                    ham_u32_t duplicate_index) const {
      ham_assert(!"shouldn't be here");
      return (0);
    }

    // Returns a record id from the duplicate table
    ham_u64_t table_get_record_id(ByteArray *table,
                    ham_u32_t duplicate_index) const {
      ham_assert(!"shouldn't be here");
      return (0);
    }

    // Sets a record id in the duplicate table
    void table_set_record_id(ByteArray *table,
                    ham_u32_t duplicate_index, ham_u64_t id) {
      ham_assert(!"shouldn't be here");
    }

    // Returns a pointer to the inline record data from the duplicate table
    ham_u8_t *table_get_record_data(ByteArray *table,
                    ham_u32_t duplicate_index) const {
      ham_assert(!"shouldn't be here");
      return (0);
    }

    // Sets the inline record data in the duplicate table
    void table_set_record_data(ByteArray *table, ham_u32_t duplicate_index,
                    void *data, ham_u32_t size) {
      ham_assert(!"shouldn't be here");
    }

    // Deletes a record from the table; also adjusts the count
    void table_erase_record(ByteArray *table, ham_u32_t duplicate_index) {
      ham_assert(!"shouldn't be here");
    }

  private:
    NodeType *m_layout;
};

//
// A BtreeNodeProxy layout which stores key flags, key size, key data
// and the record pointer next to each other.
// This is the format used since the initial hamsterdb version.
//
template<typename LayoutImpl, typename RecordList>
class DefaultNodeImpl
{
    // for caching external keys
    typedef std::map<ham_u64_t, ByteArray> ExtKeyCache;

    // for caching external duplicate tables
    typedef std::map<ham_u64_t, ByteArray> DupTableCache;

    enum {
      // for capacity, freelist_count, next_offset
      kPayloadOffset = 12,

      // only rearrange if freelist_count is high enough
      kRearrangeThreshold = 5,

      // sizeof(ham_u64_t) + 1 (for flags)
      kExtendedDuplicatesSize = 9
    };

  public:
    typedef DefaultIterator<LayoutImpl, RecordList> Iterator;
    typedef const DefaultIterator<LayoutImpl, RecordList> ConstIterator;

    // Constructor
    DefaultNodeImpl(Page *page)
      : m_page(page), m_node(PBtreeNode::from_page(m_page)),
        m_records(this, m_page->get_db()->get_record_size()),
        m_extkey_cache(0), m_duptable_cache(0) {
      initialize();
    }

    // Destructor
    ~DefaultNodeImpl() {
      clear_caches();
    }

    // Returns the actual key size (including overhead, without record)
    static ham_u16_t get_actual_key_size(ham_u32_t key_size,
                        bool enable_duplicates = false) {
      // unlimited/variable keys require 5 bytes for flags + key size + offset;
      // assume an average key size of 32 bytes (this is a random guess, but
      // will be good enough)
      if (key_size == HAM_KEY_SIZE_UNLIMITED)
        return ((ham_u16_t)32 - 8);// + 5 - 8

      // otherwise 1 byte for flags and 1 byte for record counter
      return ((ham_u16_t)(key_size + (enable_duplicates ? 2 : 0)));
    }

    // Returns an iterator which points at the first key
    Iterator begin() {
      return (at(0));
    }

    // Returns an iterator which points at the first key (const flavour)
    Iterator begin() const {
      return (at(0));
    }

    // Returns an interator pointing at an arbitrary position
    //
    // Note that this function does not check the boundaries (i.e. whether
    // i <= get_count(), because some functions deliberately write to
    // elements "after" get_count()
    Iterator at(ham_u32_t slot) {
      return (Iterator(this, slot));
    }

    // Returns an interator pointing at an arbitrary position (const flavour)
    ConstIterator at(ham_u32_t slot) const {
      return (ConstIterator(this, slot));
    }

    // Checks the integrity of this node. Throws an exception if there is a
    // violation.
    void check_integrity() const {
      if (m_node->get_count() == 0)
        return;
      ham_assert(m_node->get_count() + get_freelist_count() <= get_capacity());

      ByteArray arena;
      ham_u32_t count = m_node->get_count();
      Iterator it = begin();
      for (ham_u32_t i = 0; i < count; i++, it->next()) {
        // internal nodes: only allowed flag is kExtendedKey
        if ((it->get_key_flags() != 0
            && it->get_key_flags() != BtreeKey::kExtendedKey)
            && !m_node->is_leaf()) {
          ham_log(("integrity check failed in page 0x%llx: item #%u "
                  "has flags but it's not a leaf page",
                  m_page->get_address(), i));
          throw Exception(HAM_INTEGRITY_VIOLATED);
        }

        if (it->get_key_size() > get_extended_threshold()
            && !(it->get_key_flags() & BtreeKey::kExtendedKey)) {
          ham_log(("key size %d, but is not extended", it->get_key_size()));
          throw Exception(HAM_INTEGRITY_VIOLATED);
        }

        if (it->get_key_flags() & BtreeKey::kInitialized) {
          ham_log(("integrity check failed in page 0x%llx: item #%u"
                  "is initialized (w/o record)", m_page->get_address(), i));
          throw Exception(HAM_INTEGRITY_VIOLATED);
        }

        if (it->get_key_flags() & BtreeKey::kExtendedKey) {
          ham_u64_t blobid = it->get_extended_blob_id();
          if (!blobid) {
            ham_log(("integrity check failed in page 0x%llx: item "
                    "is extended, but has no blob", m_page->get_address()));
            throw Exception(HAM_INTEGRITY_VIOLATED);
          }

          // make sure that the extended blob can be loaded
          ham_record_t record = {0};
          m_page->get_db()->get_local_env()->get_blob_manager()->read(
                          m_page->get_db(), blobid, &record, 0, &arena);

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

      check_index_integrity(m_node->get_count());
    }

    // Compares two keys
    template<typename Cmp>
    int compare(const ham_key_t *lhs, Iterator it, Cmp &cmp) {
      if (it->get_key_flags() & BtreeKey::kExtendedKey) {
        ham_key_t tmp = {0};
        get_extended_key(it->get_extended_blob_id(), &tmp);
        return (cmp(lhs->data, lhs->size, tmp.data, tmp.size));
      }
      return (cmp(lhs->data, lhs->size, it->get_key_data(),
                              it->get_key_size()));
    }

    // Searches the node for the key and returns the slot of this key
    template<typename Cmp>
    int find(ham_key_t *key, Cmp &comparator, int *pcmp = 0) {
      ham_u32_t count = m_node->get_count();
      int i, l = 1, r = count - 1;
      int ret = 0, last = count + 1;
      int cmp = -1;

#ifdef HAM_DEBUG
      check_index_integrity(count);
#endif

      ham_assert(count > 0);

      /* only one element in this node? */
      if (r == 0) {
        cmp = compare(key, at(0), comparator);
        if (pcmp)
          *pcmp = cmp;
        return (cmp < 0 ? -1 : 0);
      }

      for (;;) {
        /* get the median item; if it's identical with the "last" item,
         * we've found the slot */
        i = (l + r) / 2;

        if (i == last) {
          ham_assert(i >= 0);
          ham_assert(i < (int)count);
          cmp = 1;
          ret = i;
          break;
        }

        /* compare it against the key */
        cmp = compare(key, at(i), comparator);

        /* found it? */
        if (cmp == 0) {
          ret = i;
          break;
        }

        /* if the key is bigger than the item: search "to the left" */
        if (cmp < 0) {
          if (r == 0) {
            ham_assert(i == 0);
            ret = -1;
            break;
          }
          r = i - 1;
        }
        else {
          last = i;
          l = i + 1;
        }
      }

      if (pcmp)
        *pcmp = cmp;
      return (ret);
    }

    // Returns a deep copy of the key
    void get_key(ham_u32_t slot, ByteArray *arena, ham_key_t *dest) {
      LocalDatabase *db = m_page->get_db();
      ConstIterator it = at(slot);

#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count());
#endif

      if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
        arena->resize(it->get_key_size());
        dest->data = arena->get_ptr();
        dest->size = it->get_key_size();
      }

      if (it->get_key_flags() & BtreeKey::kExtendedKey) {
        ham_key_t tmp = {0};
        get_extended_key(it->get_extended_blob_id(), &tmp);
        memcpy(dest->data, tmp.data, tmp.size);
      }
      else
        memcpy(dest->data, it->get_key_data(), it->get_key_size());

      /* recno databases: recno is stored in db-endian! */
      if (db->get_rt_flags() & HAM_RECORD_NUMBER) {
        ham_assert(dest->data != 0);
        ham_assert(dest->size == sizeof(ham_u64_t));
        ham_u64_t recno = *(ham_u64_t *)dest->data;
        recno = ham_db2h64(recno);
        memcpy(dest->data, &recno, sizeof(ham_u64_t));
      }
    }

    // Returns the number of records of a key
    ham_u32_t get_total_record_count(ham_u32_t slot) {
      Iterator it = at(slot);
      if (it->get_key_flags() & BtreeKey::kExtendedDuplicates) {
        ByteArray table = get_duplicate_table(it->get_record_id());
        return (DuplicateTable::get_count(&table));
      }
      return (m_layout.get_inline_record_count(slot));
    }

    // Returns the full record and stores it in |dest|
    void get_record(ham_u32_t slot, ByteArray *arena, ham_record_t *record,
                    ham_u32_t flags, ham_u32_t duplicate_index) {
      LocalDatabase *db = m_page->get_db();
      LocalEnvironment *env = db->get_local_env();
      Iterator it = at(slot);

#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count());
#endif

      // extended duplicate table
      if (it->get_key_flags() & BtreeKey::kExtendedDuplicates) {
        ByteArray table = get_duplicate_table(it->get_record_id());
        if (m_records.table_is_record_inline(&table, duplicate_index)) {
          ham_u32_t size = m_records.table_get_record_size(&table,
                                    duplicate_index);
          if (size == 0) {
            record->data = 0;
            record->size = 0;
            return;
          }
          if (flags & HAM_PARTIAL) {
            ham_trace(("flag HAM_PARTIAL is not allowed if record->size <= 8"));
            throw Exception(HAM_INV_PARAMETER);
          }
          if (!(record->flags & HAM_RECORD_USER_ALLOC)
              && (flags & HAM_DIRECT_ACCESS)) {
            record->data = m_records.table_get_record_data(&table,
                                duplicate_index);
          }
          else {
            if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
              arena->resize(size);
              record->data = arena->get_ptr();
            }
            void *p = m_records.table_get_record_data(&table,
                                duplicate_index);
            memcpy(record->data, p, size);
          }
          record->size = size;
          return;
        }

        // non-inline duplicate (record data is a record ID)
        ham_u64_t rid = m_records.table_get_record_id(&table,
                        duplicate_index);
        env->get_blob_manager()->read(db, rid, record, flags, arena);
        return;
      }

      // inline records (with or without duplicates)
      if (it->is_record_inline(duplicate_index)) {
        ham_u32_t size = it->get_inline_record_size(duplicate_index);
        if (size == 0) {
          record->data = 0;
          record->size = 0;
          return;
        }
        if (flags & HAM_PARTIAL) {
          ham_trace(("flag HAM_PARTIAL is not allowed if record->size <= 8"));
          throw Exception(HAM_INV_PARAMETER);
        }
        if (!(record->flags & HAM_RECORD_USER_ALLOC)
            && (flags & HAM_DIRECT_ACCESS)) {
          record->data = it->get_inline_record_data(duplicate_index);
        }
        else {
          if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
            arena->resize(size);
            record->data = arena->get_ptr();
          }
          void *p = it->get_inline_record_data(duplicate_index);
          memcpy(record->data, p, size);
        }
        record->size = size;
        return;
      }

      // non-inline duplicate (record data is a record ID)
      env->get_blob_manager()->read(db, it->get_record_id(duplicate_index),
                                  record, flags, arena);
    }

    // Sets the record of a key, or adds a duplicate
    void set_record(ham_u32_t slot, ham_record_t *record,
                    ham_u32_t duplicate_index, ham_u32_t flags,
                    ham_u32_t *new_duplicate_index) {
      LocalDatabase *db = m_page->get_db();
      LocalEnvironment *env = db->get_local_env();
      Iterator it = at(slot);

#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count());
#endif

      // automatically overwrite an existing key unless this is a
      // duplicate operation
      if ((flags & (HAM_DUPLICATE
                    | HAM_DUPLICATE
                    | HAM_DUPLICATE_INSERT_BEFORE
                    | HAM_DUPLICATE_INSERT_AFTER
                    | HAM_DUPLICATE_INSERT_FIRST
                    | HAM_DUPLICATE_INSERT_LAST)) == 0)
        flags |= HAM_OVERWRITE;

      // record does not yet exist - simply overwrite the first record
      // of this key
      if (it->get_key_flags() & BtreeKey::kInitialized) {
        flags |= HAM_OVERWRITE;
        duplicate_index = 0;
        // also remove the kInitialized flag
        it->set_key_flags(it->get_key_flags() & (~BtreeKey::kInitialized));
        // fall through into the next branch
      }

      // a key is overwritten (NOT in a duplicate table)
      if ((flags & HAM_OVERWRITE)
          && !(it->get_key_flags() & BtreeKey::kExtendedDuplicates)) {
        // existing record is stored non-inline (in a blob)
        if (!it->is_record_inline(duplicate_index)) {
          ham_u64_t ptr = it->get_record_id(duplicate_index);
          // non-inline record is overwritten with another non-inline record?
          if (record->size > it->get_max_inline_record_size()) {
            ptr = env->get_blob_manager()->overwrite(db, ptr, record, flags);
            it->set_record_id(ptr, duplicate_index);
            return;
          }
          // free the existing record (if there is one)
          env->get_blob_manager()->erase(db, ptr);
          // fall through
        }
        // write the new inline record
        if (record->size <= it->get_max_inline_record_size())
          it->set_inline_record_data(record->data, record->size,
                          duplicate_index);
        // ... or the non-inline record
        else {
          ham_u64_t ptr = env->get_blob_manager()->allocate(db, record, flags);
          it->set_record_flags(0, duplicate_index);
          it->set_record_id(ptr, duplicate_index);
        }

#ifdef HAM_DEBUG
        check_index_integrity(m_node->get_count());
#endif
        return;
      }

      // a key is overwritten in a duplicate table
      if ((flags & HAM_OVERWRITE)
          && (it->get_key_flags() & BtreeKey::kExtendedDuplicates)) {
        ByteArray table = get_duplicate_table(it->get_record_id());
        // old record is non-inline? free or overwrite it
        if (!m_records.table_is_record_inline(&table, duplicate_index)) {
          ham_u64_t ptr = m_records.table_get_record_id(&table,
                                duplicate_index);
          if (record->size > it->get_max_inline_record_size()) {
            ptr = env->get_blob_manager()->overwrite(db, ptr, record, flags);
            m_records.table_set_record_id(&table, duplicate_index, ptr);
            return;
          }
          db->get_local_env()->get_blob_manager()->erase(db, ptr, 0);
        }

        // now overwrite with an inline key
        if (record->size <= it->get_max_inline_record_size())
          m_records.table_set_record_data(&table, duplicate_index,
                          record->data, record->size);
        // ... or with a (non-inline) key
        else {
          m_records.table_set_record_id(&table, duplicate_index,
                    env->get_blob_manager()->allocate(db, record, flags));
        }
#ifdef HAM_DEBUG
        check_index_integrity(m_node->get_count());
#endif
        return;
      }

      // from here on we insert duplicate keys!

      // it's possible that the page is full and new space cannot be allocated.
      // even if there's no duplicate overflow table (yet): check if a new
      // record would fit into the page. If not then force the use of a
      // duplicate table.
      bool force_duptable = false;

      if ((it->get_key_flags() & BtreeKey::kExtendedDuplicates) == 0) {
        ham_u32_t threshold = get_duplicate_threshold();

        if (it->get_inline_record_count() >= threshold)
          force_duptable = true;
        else {
          ham_key_t tmpkey = {0};
          tmpkey.size = get_total_key_data_size(slot)
                        + get_total_inline_record_size();

          if (!has_enough_space(&tmpkey, false))
            force_duptable = true;
        }

        // already too many duplicates, or the record does not fit? then
        // allocate an overflow duplicate list and move all duplicates to
        // this list
        if (force_duptable) {
          ham_u32_t size = 8 + get_total_inline_record_size() * (threshold * 2);
          ByteArray table(size);
          DuplicateTable::set_count(&table, it->get_inline_record_count());
          DuplicateTable::set_capacity(&table, threshold * 2);
          ham_u8_t *ptr = (ham_u8_t *)table.get_ptr() + 8;
          memcpy(ptr, it->get_inline_record_data(0),
                          it->get_inline_record_count()
                                * get_total_inline_record_size());

          // add to cache
          ham_u64_t tableid = add_duplicate_table(&table);

          // allocate new space, copy the key data and write the new record id
          m_layout.set_key_data_offset(slot,
                          append_key(m_node->get_count(), it->get_key_data(),
                                  it->get_key_data_size()
                                        + kExtendedDuplicatesSize,
                                  true));

          // finally write the new record id
          it->set_key_flags(it->get_key_flags()
                          | BtreeKey::kExtendedDuplicates);
          it->set_record_id(tableid);
          it->set_inline_record_count(0);

          // adjust next offset
          set_next_offset(calc_next_offset(m_node->get_count()));

          // ran out of space? rearrange, otherwise the space which just was
          // freed would be lost
          if (force_duptable)
            rearrange(m_node->get_count(), true);

          // fall through
        }
      }

      // a duplicate table exists
      if (it->get_key_flags() & BtreeKey::kExtendedDuplicates) {
        ByteArray table = get_duplicate_table(it->get_record_id());
        ham_u32_t count = DuplicateTable::get_count(&table);

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

        // need to resize the table? (also update the cache)
        if (count == DuplicateTable::get_capacity(&table))
          table = grow_duplicate_table(it->get_record_id());

        ham_u32_t position;

        // handle overwrites or inserts/appends
        if (flags & HAM_DUPLICATE_INSERT_FIRST) {
          if (count) {
            ham_u8_t *ptr = m_records.table_get_record_data(&table, 0);
            memmove(m_records.table_get_record_data(&table, 1),
                        ptr, count * get_total_inline_record_size());
          }
          position = 0;

        }
        else if (flags & HAM_DUPLICATE_INSERT_BEFORE) {
          memmove(m_records.table_get_record_data(&table, duplicate_index),
                      m_records.table_get_record_data(&table,
                              duplicate_index + 1),
                      (count - duplicate_index)
                            * get_total_inline_record_size());
          position = duplicate_index;
        }
        else // HAM_DUPLICATE_INSERT_LAST
          position = count;

        // now write the record
        if (record->size <= get_max_inline_record_size())
          m_records.table_set_record_data(&table, position, record->data,
                    record->size);
        else
          m_records.table_set_record_id(&table, position,
                    env->get_blob_manager()->allocate(db, record, flags));

        DuplicateTable::set_count(&table, count + 1);

        // store the modified duplicate table
        it->set_record_id(flush_duplicate_table(it->get_record_id(), &table));

        if (new_duplicate_index)
          *new_duplicate_index = position;

#ifdef HAM_DEBUG
        check_index_integrity(m_node->get_count());
#endif
        return;
      }

      // still here? then we have to insert/append a duplicate to an existing
      // inline duplicate list. This means we have to allocte new space
      // for the list, and add the previously used space to the freelist (if
      // there is enough capacity).
      ham_u32_t required_size = get_total_key_data_size(slot)
                                    + get_total_inline_record_size();
      ham_u32_t offset = allocate(m_node->get_count(), required_size, true);

      // first copy the key data
      ham_u8_t *orig = it->get_key_data();
      ham_u8_t *dest = m_node->get_data() + kPayloadOffset + offset
                      + m_layout.get_key_index_span() * get_capacity();
      ham_u32_t size = it->get_key_data_size();
      memcpy(dest, orig, size);
      orig += size;
      dest += size;

      // adjust flags
      if (flags & HAM_DUPLICATE_INSERT_BEFORE && duplicate_index == 0)
        flags |= HAM_DUPLICATE_INSERT_FIRST;
      else if (flags & HAM_DUPLICATE_INSERT_AFTER) {
        if (duplicate_index == it->get_inline_record_count())
          flags |= HAM_DUPLICATE_INSERT_LAST;
        else {
          flags |= HAM_DUPLICATE_INSERT_BEFORE;
          duplicate_index++;
        }
      }

      // store the new offset, otherwise set_inline_record_data() et al
      // will not work
      bool set_offset = (get_next_offset()
                        == m_layout.get_key_data_offset(slot)
                            + get_total_key_data_size(slot));
      m_layout.set_key_data_offset(slot, offset);

      ham_u32_t position;

      // we have enough space - copy all duplicates, and insert the new
      // duplicate wherever required
      if (flags & HAM_DUPLICATE_INSERT_FIRST) {
        memcpy(dest + get_total_inline_record_size(), orig,
                it->get_inline_record_count()
                      * get_total_inline_record_size());
        position = 0;

      }
      else if (flags & HAM_DUPLICATE_INSERT_BEFORE) {
        memcpy(dest, orig, duplicate_index * get_total_inline_record_size());
        orig += duplicate_index * get_total_inline_record_size();
        dest += (duplicate_index + 1) * get_total_inline_record_size();
        memcpy(dest, orig,
                  (it->get_inline_record_count() - duplicate_index)
                        * get_total_inline_record_size());
        position = duplicate_index;
      }
      else { // HAM_DUPLICATE_INSERT_LAST
        memcpy(dest, orig, it->get_inline_record_count()
                                * get_total_inline_record_size());
        position = it->get_inline_record_count();
      }

      // now insert the record
      if (record->size <= get_max_inline_record_size())
        it->set_inline_record_data(record->data, record->size, position);
      else
        it->set_record_id(env->get_blob_manager()->allocate(db,
                                record, flags), position);

      if (new_duplicate_index)
        *new_duplicate_index = position;

      // increase the record counter
      it->set_inline_record_count(it->get_inline_record_count() + 1);

      // adjust next offset, if necessary
      if (set_offset)
        set_next_offset(calc_next_offset(m_node->get_count()));

#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count());
#endif
    }

    // Returns the record size of a key or one of its duplicates
    ham_u64_t get_record_size(ham_u32_t slot, int duplicate_index) {
      ham_u64_t ptr = 0;

      // extended duplicate table
      if (get_key_flags(slot) & BtreeKey::kExtendedDuplicates) {
        ByteArray table = get_duplicate_table(get_record_id(slot));
        if (m_records.table_is_record_inline(&table, duplicate_index))
          return (m_records.table_get_record_size(&table,
                                  duplicate_index));

        ptr = m_records.table_get_record_id(&table, duplicate_index);
      }
      // inline record
      else {
        if (is_record_inline(slot, duplicate_index))
          return (get_inline_record_size(slot, duplicate_index));
        ptr = get_record_id(slot, duplicate_index);
      }

      LocalDatabase *db = m_page->get_db();
      LocalEnvironment *env = db->get_local_env();
      return (env->get_blob_manager()->get_blob_size(db, ptr));
    }

    // Erases an extended key
    void erase_key(ham_u32_t slot) {
      if (get_key_flags(slot) & BtreeKey::kExtendedKey) {
        Iterator it = at(slot);
        // delete the extended key from the cache
        erase_extended_key(it->get_extended_blob_id());
        // and transform into a key which is non-extended and occupies
        // the same space as before, when it was extended
        it->set_key_flags(it->get_key_flags() & (~BtreeKey::kExtendedKey));
        it->set_key_size(sizeof(ham_u64_t));
      }
    }

    // Erases one (or all) records of a key
    void erase_record(ham_u32_t slot, ham_u32_t duplicate_index,
                    bool all_duplicates) {
      LocalDatabase *db = m_page->get_db();
      Iterator it = at(slot);

      // extended duplicate list?
      if (it->get_key_flags() & BtreeKey::kExtendedDuplicates) {
        ham_u64_t ptr;
        ByteArray table = get_duplicate_table(it->get_record_id());
        // delete ALL duplicates?
        if (all_duplicates) {
          ham_u32_t count = DuplicateTable::get_count(&table);
          for (ham_u32_t i = 0; i < count; i++) {
            // non-inline record? free the blob
            if (!m_records.table_is_record_inline(&table, i)) {
              ptr = m_records.table_get_record_id(&table, i);
              db->get_local_env()->get_blob_manager()->erase(db, ptr, 0);
            }
          }

          DuplicateTable::set_count(&table, 0);
          // fall through
        }
        else {
          // non-inline record? free the blob
          if (!m_records.table_is_record_inline(&table, duplicate_index)) {
            ptr = m_records.table_get_record_id(&table, duplicate_index);
            db->get_local_env()->get_blob_manager()->erase(db, ptr, 0);
          }

          // remove the record from the duplicate table
          m_records.table_erase_record(&table, duplicate_index);
        }

        // if the table is now empty: delete it
        if (DuplicateTable::get_count(&table) == 0) {
          ham_u32_t flags = it->get_key_flags();
          it->set_key_flags(flags & (~BtreeKey::kExtendedDuplicates));

          // delete duplicate table blob and cached table
          erase_duplicate_table(it->get_record_id());

          // adjust next offset
          if (get_next_offset() == m_layout.get_key_data_offset(it->get_slot())
                                        + it->get_key_data_size()
                                        + kExtendedDuplicatesSize)
            set_next_offset(get_next_offset() - kExtendedDuplicatesSize);
        }
        // otherwise store the modified table
        else {
          it->set_record_id(flush_duplicate_table(it->get_record_id(), &table));
        }
#ifdef HAM_DEBUG
        check_index_integrity(m_node->get_count());
#endif
        return;
      }

      // handle duplicate keys
      ham_u32_t record_count = it->get_inline_record_count();
      ham_assert(record_count > 0);

      // ALL duplicates?
      if (all_duplicates) {
        // if records are not inline: delete the blobs
        for (ham_u32_t i = 0; i < record_count; i++) {
          if (!it->is_record_inline(i))
            db->get_local_env()->get_blob_manager()->erase(db,
                            it->get_record_id(i), 0);
            it->remove_inline_record(i);
        }
        it->set_inline_record_count(0);

        // adjust next offset?
        if (get_next_offset() == m_layout.get_key_data_offset(it->get_slot())
                        + it->get_key_data_size()
                        + it->get_total_inline_record_size()
                              * record_count)
          set_next_offset(m_layout.get_key_data_offset(it->get_slot())
                        + it->get_key_data_size());
      }
      else {
        // if record is not inline: delete the blob
        if (!it->is_record_inline(duplicate_index))
          db->get_local_env()->get_blob_manager()->erase(db,
                          it->get_record_id(duplicate_index), 0);

        // shift duplicate records "to the left"
        if (duplicate_index + 1 < it->get_inline_record_count())
          memmove(it->get_inline_record_data(duplicate_index),
                        it->get_inline_record_data(duplicate_index + 1),
                        it->get_total_inline_record_size()
                              * (record_count - 1));
        else
          it->remove_inline_record(duplicate_index);

        // decrease record counter and adjust next offset
        it->set_inline_record_count(record_count - 1);
        if (get_next_offset() == m_layout.get_key_data_offset(it->get_slot())
                        + it->get_key_data_size()
                        + it->get_total_inline_record_size()
                              * record_count)
          set_next_offset(get_next_offset()
                          - it->get_total_inline_record_size());
      }
#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count());
#endif
    }

    // Erases a key from the index. Does NOT erase the records!
    void erase(ham_u32_t slot) {
#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count());
#endif

      // if this is the last key in this page: just re-initialize
      if (m_node->get_count() == 1) {
        set_freelist_count(0);
        set_next_offset(0);
        return;
      }

      // adjust next offset?
      bool recalc_offset = false;
      if (get_next_offset() == m_layout.get_key_data_offset(slot)
                                    + get_key_data_size(slot)
                                    + get_total_inline_record_size())
        recalc_offset = true;

      // get rid of the extended key (if there is one)
      if (get_key_flags(slot) & BtreeKey::kExtendedKey)
        erase_key(slot);

      // now add this key to the freelist
      if (get_freelist_count() + 1 + m_node->get_count() <= get_capacity())
        freelist_add(slot);

      // then remove index key by shifting all remaining indices/freelist
      // items "to the left"
      memmove(m_layout.get_key_index_ptr(slot),
                      m_layout.get_key_index_ptr(slot + 1),
                      m_layout.get_key_index_span()
                            * (get_freelist_count() + m_node->get_count()
                                    - slot - 1));

      if (recalc_offset)
        set_next_offset(calc_next_offset(m_node->get_count() - 1));

#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count() - 1);
#endif
    }

    // Inserts a new key at |slot|. 
    // Also inserts an empty record which has to be overwritten in
    // the next call of set_record().
    void insert(ham_u32_t slot, const ham_key_t *key,
                    bool is_recursive = false) {
      ham_u32_t count = m_node->get_count();

#ifdef HAM_DEBUG
      check_index_integrity(count);
#endif

      bool extended_key = key->size > get_extended_threshold();

      ham_u32_t offset = (ham_u32_t)-1;

      // search the freelist for free key space
      int idx = freelist_find(count,
                      (extended_key ? sizeof(ham_u64_t) : key->size)
                            + get_total_inline_record_size());
      // found: remove this freelist entry
      if (idx != -1) {
        offset = m_layout.get_key_data_offset(idx);
        ham_u32_t size = get_total_key_data_size(idx);
        freelist_remove(idx);
        // adjust the next key offset, if required
        if (get_next_offset() == offset + size)
          set_next_offset(offset
                      + (extended_key ? sizeof(ham_u64_t) : key->size)
                      + get_total_inline_record_size());
      }
      // not found: append at the end
      else {
        if (count == 0)
          offset = 0;
        else
          offset = get_next_offset();

        // make sure that the key really fits! if not then use an extended key.
        // this can happen if a page is split, but the new key still doesn't
        // fit into the splitted page.
        if (!extended_key) {
          if (offset + m_layout.get_key_index_span() * get_capacity()
              + key->size + get_total_inline_record_size()
                  >= get_usable_page_size()) {
            extended_key = true;
            // check once more if the key fits
            if (offset
                  + m_layout.get_key_index_span() * get_capacity()
                  + sizeof(ham_u64_t) + get_total_inline_record_size()
                      >= get_usable_page_size()) {
              // Arghl, still does not fit - rearrange the page, try again.
              //
              // This scenario can occur when we're inserting an anchor element
              // while two pages are merged (during erase). These pages are
              // both nearly empty, and calling rearrange() will give us ample
              // free space.
              //
              // This can NOT occur durning regular inserts, because the page
              // would have been splitted already.
              ham_verify(is_recursive == false);
              if (!is_recursive) {
                rearrange(m_node->get_count(), true);
                insert(slot, key, true);
                return;
              }
            }
          }
        }

        set_next_offset(offset
                        + (extended_key ? sizeof(ham_u64_t) : key->size)
                        + get_total_inline_record_size());
      }

      // once more assert that the new key fits
      ham_assert(offset
              + m_layout.get_key_index_span() * get_capacity()
              + (extended_key ? sizeof(ham_u64_t) : key->size)
              + get_total_inline_record_size()
                  <= get_usable_page_size());

      // make space for the new index
      if (slot < count || get_freelist_count() > 0) {
        memmove(m_layout.get_key_index_ptr(slot + 1),
                      m_layout.get_key_index_ptr(slot),
                      m_layout.get_key_index_span()
                            * (count + get_freelist_count() - slot));
      }

      // store the key index
      m_layout.set_key_data_offset(slot, offset);

      Iterator it = at(slot);

      // now finally copy the key data
      if (extended_key) {
        ham_u64_t blobid = add_extended_key(key);

        it->set_extended_blob_id(blobid);
        // remove all flags, set Extended flag
        it->set_key_flags(BtreeKey::kExtendedKey | BtreeKey::kInitialized);
        it->set_key_size(key->size);
      }
      else {
        it->set_key_flags(BtreeKey::kInitialized);
        it->set_key_size(key->size);
        it->set_key_data(key->data, key->size);
      }

      it->set_inline_record_count(1);
      it->set_record_flags(BtreeRecord::kBlobSizeEmpty);

#ifdef HAM_DEBUG
      check_index_integrity(count + 1);
#endif
    }

    // Same as above, but copies the key from |src_node[src_slot]|
    void insert(ham_u32_t slot, DefaultNodeImpl *src_node,
                    ham_u32_t src_slot) {
      ham_key_t key = {0};
      ConstIterator it = src_node->at(src_slot);
      if (it->get_key_flags() & BtreeKey::kExtendedKey) {
        src_node->get_extended_key(it->get_extended_blob_id(), &key);
      }
      else {
        key.data = (void *)it->get_key_data();
        key.size = it->get_key_size();
      }
      insert(slot, &key);
    }

    // Returns true if |key| cannot be inserted because a split is required
    // Rearranges the node if required
    // Leaves some additional headroom in internal pages, in case we
    // overwrite small keys with longer keys and need more space
    bool requires_split() {
      ham_key_t key = {0};
      key.size = 32;
      return (!has_enough_space(&key, true, false, 0));
      // TODO re-enable this when erase SMOs are cleaned up
#if 0
      if (has_enough_space(&key, true, false, (!m_node->is_leaf() ? 128 : 0)))
        return (false);

      rearrange(m_node->get_count());
      return (resize(m_node->get_count() + 1, &key));
#endif
    }

    // Returns true if the node requires a merge or a shift
    bool requires_merge() const {
      return (m_node->get_count() <= 3);
    }

    // Splits this node and moves some/half of the keys to |other|
    void split(DefaultNodeImpl *other, int pivot) {
      int start = pivot;
      int count = m_node->get_count() - pivot;

#ifdef HAM_DEBUG
      check_index_integrity(m_node->get_count());
#endif
      ham_assert(0 == other->m_node->get_count());
      ham_assert(0 == other->get_freelist_count());

      // if we split a leaf then the pivot element is inserted in the leaf
      // page. in internal nodes it is propagated to the parent instead.
      // (this propagation is handled by the caller.)
      if (!m_node->is_leaf()) {
        start++;
        count--;
      }

      clear_caches();

      // make sure that the other node (which is currently empty) can fit
      // all the keys
      if (other->get_capacity() <= (ham_u32_t)count)
        other->set_capacity(count + 1); // + 1 for the pivot key

      // move |count| keys to the other node
      memcpy(other->m_layout.get_key_index_ptr(0),
                      m_layout.get_key_index_ptr(start),
                      m_layout.get_key_index_span() * count);
      for (int i = 0; i < count; i++) {
        ham_u32_t key_size = get_key_data_size(start + i);
        ham_u32_t rec_size = get_record_data_size(start + i);
        ham_u8_t *data = get_key_data(start + i);
        ham_u32_t offset = other->append_key(i, data, key_size + rec_size,
                                false);
        other->m_layout.set_key_data_offset(i, offset);

        if (m_layout.get_key_flags(start + i) & BtreeKey::kExtendedKey)
          other->m_layout.set_key_flags(i,
                    other->m_layout.get_key_flags(i) | BtreeKey::kExtendedKey);
        else
          other->m_layout.set_key_flags(i,
                    other->m_layout.get_key_flags(i) & ~BtreeKey::kExtendedKey);
      }

      // now move all shifted keys to the freelist. those shifted keys are
      // always at the "right end" of the node, therefore we just decrease
      // m_node->get_count() and increase freelist_count simultaneously
      // (m_node->get_count() is decreased by the caller).
      set_freelist_count(get_freelist_count() + count);
      if (get_freelist_count() > kRearrangeThreshold)
        rearrange(pivot);
      else
        set_next_offset(calc_next_offset(pivot));

#ifdef HAM_DEBUG
      check_index_integrity(pivot);
      other->check_index_integrity(count);
#endif
    }

    // Merges keys from |other| to this node
    void merge_from(DefaultNodeImpl *other) {
      ham_u32_t count = m_node->get_count();
      ham_u32_t other_count = other->m_node->get_count();

#ifdef HAM_DEBUG
      check_index_integrity(count);
      other->check_index_integrity(other_count);
#endif

      other->clear_caches();

      // re-arrange the node: moves all keys sequentially to the beginning
      // of the key space, removes the whole freelist
      rearrange(m_node->get_count(), true);

      ham_assert(m_node->get_count() + other_count <= get_capacity());

      // now append all indices from the sibling
      memcpy(m_layout.get_key_index_ptr(count),
                      other->m_layout.get_key_index_ptr(0),
                      m_layout.get_key_index_span() * other_count);

      // for each new key: copy the key data
      for (ham_u32_t i = 0; i < other_count; i++) {
        ham_u32_t key_size = other->get_key_data_size(i);
        ham_u32_t rec_size = other->get_record_data_size(i);
        ham_u8_t *data = other->get_key_data(i);
        ham_u32_t offset = append_key(count + i, data,
                                key_size + rec_size, false);
        m_layout.set_key_data_offset(count + i, offset);
        m_layout.set_key_size(count + i, other->get_key_size(i));

        if (other->m_layout.get_key_flags(i) & BtreeKey::kExtendedKey)
          m_layout.set_key_flags(count + i,
                    m_layout.get_key_flags(count + i) | BtreeKey::kExtendedKey);
        else
          m_layout.set_key_flags(count + i,
                    m_layout.get_key_flags(count + i) & ~BtreeKey::kExtendedKey);
      }

      other->set_next_offset(0);
      other->set_freelist_count(0);
#ifdef HAM_DEBUG
      check_index_integrity(count + other_count);
#endif
    }

    // Clears the page with zeroes and reinitializes it; only
    // for testing
    void test_clear_page() {
      memset(m_page->get_payload(), 0,
                    m_page->get_db()->get_local_env()->get_usable_page_size());
      initialize();
    }

  private:
    friend class DefaultIterator<LayoutImpl, RecordList>;
    friend class FixedInlineRecordImpl<LayoutImpl>;
    friend class InternalInlineRecordImpl<LayoutImpl>;
    friend class DefaultInlineRecordImpl<LayoutImpl, true>;
    friend class DefaultInlineRecordImpl<LayoutImpl, false>;

    // Initializes the node
    void initialize() {
      LocalDatabase *db = m_page->get_db();
      ham_u32_t key_size = db->get_btree_index()->get_key_size();

      m_layout.initialize(m_node->get_data() + kPayloadOffset, key_size);

      if (m_node->get_count() == 0 && !(db->get_rt_flags() & HAM_READ_ONLY)) {
        ham_u32_t page_size = get_usable_page_size();

        ham_u32_t rec_size = db->get_btree_index()->get_record_size();
        if (rec_size == HAM_RECORD_SIZE_UNLIMITED)
          rec_size = 9;

        bool has_duplicates = db->get_local_env()->get_flags()
                                & HAM_ENABLE_DUPLICATES;
        ham_u32_t capacity = page_size
                            / (m_layout.get_key_index_span()
                              + get_actual_key_size(key_size, has_duplicates)
                              + rec_size);
        capacity = (capacity & 1 ? capacity - 1 : capacity);

        set_capacity(capacity);
        set_freelist_count(0);
        set_next_offset(0);
      }
    }

    // Clears the caches for extended keys and duplicate keys
    void clear_caches() {
      if (m_extkey_cache) {
        delete m_extkey_cache;
        m_extkey_cache = 0;
      }
      if (m_duptable_cache) {
        delete m_duptable_cache;
        m_duptable_cache = 0;
      }
    }

    // Retrieves the extended key at |blobid| and stores it in |key|; will
    // use the cache.
    void get_extended_key(ham_u64_t blobid, ham_key_t *key) {
      if (!m_extkey_cache)
        m_extkey_cache = new ExtKeyCache();
      else {
        ExtKeyCache::iterator it = m_extkey_cache->find(blobid);
        if (it != m_extkey_cache->end()) {
          key->size = it->second.get_size();
          key->data = it->second.get_ptr();
          return;
        }
      }

      ByteArray arena;
      ham_record_t record = {0};
      LocalDatabase *db = m_page->get_db();
      db->get_local_env()->get_blob_manager()->read(db, blobid, &record,
                      0, &arena);
      (*m_extkey_cache)[blobid] = arena;
      arena.disown();
      key->data = record.data;
      key->size = record.size;
    }

    // Retrieves the extended duplicate table at |tableid| and stores it the
    // cache; returns the ByteArray with the cached data
    ByteArray get_duplicate_table(ham_u64_t tableid) {
      if (!m_duptable_cache)
        m_duptable_cache = new DupTableCache();
      else {
        DupTableCache::iterator it = m_duptable_cache->find(tableid);
        if (it != m_duptable_cache->end()) {
          ByteArray arena = it->second;
          arena.disown();
          return (arena);
        }
      }

      ByteArray arena;
      ham_record_t record = {0};
      LocalDatabase *db = m_page->get_db();
      db->get_local_env()->get_blob_manager()->read(db, tableid, &record,
                      0, &arena);
      (*m_duptable_cache)[tableid] = arena;
      arena.disown();
      return (arena);
    }

    // Adds a new duplicate table to the cache
    ham_u64_t add_duplicate_table(ByteArray *table) {
      LocalDatabase *db = m_page->get_db();

      if (!m_duptable_cache)
        m_duptable_cache = new DupTableCache();

      ham_record_t rec = {0};
      rec.data = table->get_ptr();
      rec.size = table->get_size();
      ham_u64_t tableid = db->get_local_env()->get_blob_manager()->allocate(db,
                                &rec, 0);

      (*m_duptable_cache)[tableid] = *table;
      table->disown();

      // increment counter (for statistics)
      g_extended_duptables++;

      return (tableid);
    }

    // Doubles the size of a duplicate table; only works on the cached table;
    // does not update the underlying record id!
    ByteArray grow_duplicate_table(ham_u64_t tableid) {
      DupTableCache::iterator it = m_duptable_cache->find(tableid);
      ham_assert(it != m_duptable_cache->end());

      ByteArray &table = it->second;
      ham_u32_t count = DuplicateTable::get_count(&table);
      table.resize(8 + (count * 2) * get_total_inline_record_size());
      DuplicateTable::set_capacity(&table, count * 2);

      // return a "disowned" copy
      ByteArray copy = table;
      copy.disown();
      return (copy);
    }

    // Writes the modified duplicate table to disk; returns the new
    // table-id
    ham_u64_t flush_duplicate_table(ham_u64_t tableid, ByteArray *table) {
      LocalDatabase *db = m_page->get_db();
      ham_record_t record = {0};
      record.data = table->get_ptr();
      record.size = table->get_size();
      ham_u64_t newid = db->get_local_env()->get_blob_manager()->overwrite(db,
                      tableid, &record, 0);
      if (tableid != newid) {
        DupTableCache::iterator it = m_duptable_cache->find(tableid);
        ham_assert(it != m_duptable_cache->end());
        ByteArray copy = it->second;
        // do not free memory in |*table|
        it->second.disown();
        (*m_duptable_cache).erase(it);
        // now re-insert the table in the cache
        (*m_duptable_cache)[newid] = copy;
        // again make sure that it won't be freed when |copy| goes out of scope
        copy.disown();
      }
      return (newid);
    }

    // Deletes the duplicate table from disk (and from the cache)
    void erase_duplicate_table(ham_u64_t tableid) {
      DupTableCache::iterator it = m_duptable_cache->find(tableid);
      if (it != m_duptable_cache->end())
        m_duptable_cache->erase(it);

      LocalDatabase *db = m_page->get_db();
      db->get_local_env()->get_blob_manager()->erase(db, tableid, 0);
    }

    // Erases an extended key from disk and from the cache
    void erase_extended_key(ham_u64_t blobid) {
      LocalDatabase *db = m_page->get_db();
      db->get_local_env()->get_blob_manager()->erase(db, blobid);
      if (m_extkey_cache) {
        ExtKeyCache::iterator it = m_extkey_cache->find(blobid);
        if (it != m_extkey_cache->end())
          m_extkey_cache->erase(it);
      }
    }

    // Copies an extended key; adds the copy to the extkey-cache
    ham_u64_t copy_extended_key(ham_u64_t oldblobid) {
      ham_key_t oldkey = {0};

      // do NOT use the cache when retrieving the existing blob - this
      // blob belongs to a different page and we do not have access to
      // its layout
      ham_record_t record = {0};
      LocalDatabase *db = m_page->get_db();
      db->get_local_env()->get_blob_manager()->read(db, oldblobid, &record,
                      0, &m_arena);
      oldkey.data = record.data;
      oldkey.size = record.size;

      return (add_extended_key(&oldkey));
    }

    // Allocates an extended key and stores it in the extkey-Cache
    ham_u64_t add_extended_key(const ham_key_t *key) {
      if (!m_extkey_cache)
        m_extkey_cache = new ExtKeyCache();

      ham_record_t rec = {0};
      rec.data = key->data;
      rec.size = key->size;

      LocalDatabase *db = m_page->get_db();
      ham_u64_t blobid = db->get_local_env()->get_blob_manager()->allocate(db,
                            &rec, 0);
      ham_assert(blobid != 0);
      ham_assert(m_extkey_cache->find(blobid) == m_extkey_cache->end());

      ByteArray arena;
      arena.resize(key->size);
      memcpy(arena.get_ptr(), key->data, key->size);
      (*m_extkey_cache)[blobid] = arena;
      arena.disown();

      // increment counter (for statistics)
      g_extended_keys++;

      return (blobid);
    }

    // Returns the key's flags
    ham_u32_t get_key_flags(ham_u32_t slot) const {
      return (m_layout.get_key_flags(slot));
    }

    // Sets the flags of a key
    void set_key_flags(ham_u32_t slot, ham_u32_t flags) {
      m_layout.set_key_flags(slot, flags);
    }

    // Returns the key size as specified by the user
    ham_u32_t get_key_size(ham_u32_t slot) const {
      return (m_layout.get_key_size(slot));
    }

    // Sets the size of a key
    void set_key_size(ham_u32_t slot, ham_u32_t size) {
      m_layout.set_key_size(slot, size);
    }

    // Returns the size of the memory occupied by the key
    ham_u32_t get_key_data_size(ham_u32_t slot) const {
      if (m_layout.get_key_flags(slot) & BtreeKey::kExtendedKey)
        return (sizeof(ham_u64_t));
      return (m_layout.get_key_size(slot));
    }

    // Returns the total size of the key  - key data + record(s)
    ham_u32_t get_total_key_data_size(ham_u32_t slot) const {
      ham_u32_t size;
      if (m_layout.get_key_flags(slot) & BtreeKey::kExtendedKey)
        size = sizeof(ham_u64_t);
      else
        size = m_layout.get_key_size(slot);
      if (m_layout.get_key_flags(slot) & BtreeKey::kExtendedDuplicates)
        return (size + kExtendedDuplicatesSize);
      else
        return (size + get_total_inline_record_size()
                        * m_layout.get_inline_record_count(slot));
    }

    // Returns the size of the memory occupied by the record
    ham_u32_t get_record_data_size(ham_u32_t slot) const {
      if (get_key_flags(slot) & BtreeKey::kExtendedDuplicates)
        return (kExtendedDuplicatesSize);
      else
        return (m_layout.get_inline_record_count(slot)
                        * get_total_inline_record_size());
    }

    // Returns a pointer to the (inline) key data
    ham_u8_t *get_key_data(ham_u32_t slot) {
      ham_u32_t offset = m_layout.get_key_data_offset(slot)
              + m_layout.get_key_index_span() * get_capacity();
      return (m_node->get_data() + kPayloadOffset + offset);
    }

    // Returns a pointer to the (inline) key data (const flavour)
    ham_u8_t *get_key_data(ham_u32_t slot) const {
      ham_u32_t offset = m_layout.get_key_data_offset(slot)
              + m_layout.get_key_index_span() * get_capacity();
      return (m_node->get_data() + kPayloadOffset + offset);
    }

    // Sets the inline key data
    void set_key_data(ham_u32_t slot, const void *ptr, ham_u32_t len) {
      ham_u32_t offset = m_layout.get_key_data_offset(slot)
              + m_layout.get_key_index_span() * get_capacity();
      memcpy(m_node->get_data() + kPayloadOffset + offset, ptr, len);
    }

    // Returns a record id
    ham_u64_t get_record_id(ham_u32_t slot,
                    ham_u32_t duplicate_index = 0) const {
      ham_u64_t ptr = *(ham_u64_t *)get_inline_record_data(slot,
                      duplicate_index);
      return (ham_db2h_offset(ptr));
    }

    // Sets a record id
    void set_record_id(ham_u32_t slot, ham_u64_t ptr,
                    ham_u32_t duplicate_index = 0) {
      ham_u8_t *p = (ham_u8_t *)get_inline_record_data(slot, duplicate_index);
      *(ham_u64_t *)p = ham_h2db_offset(ptr);
      // initialize flags
      m_records.set_record_flags(slot, 0, duplicate_index);
    }

    // Returns the inline record data
    void *get_inline_record_data(ham_u32_t slot,
                    ham_u32_t duplicate_index = 0) {
      return (get_key_data(slot)
                      + get_key_data_size(slot)
                      + get_total_inline_record_size() * (duplicate_index));
    }

    // Returns the inline record data (const flavour)
    void *get_inline_record_data(ham_u32_t slot,
                    ham_u32_t duplicate_index = 0) const {
      return (get_key_data(slot)
                      + get_key_data_size(slot)
                      + get_total_inline_record_size() * duplicate_index);
    }

    // Searches for a freelist index with at least |required_size| bytes;
    // returns its index
    int freelist_find(ham_u32_t count, ham_u32_t required_size) const {
      int best = -1;
      ham_u32_t freelist_count = get_freelist_count();

      for (ham_u32_t i = 0; i < freelist_count; i++) {
        ham_u32_t size = get_total_key_data_size(count + i);
        if (size == required_size)
          return (count + i);
        if (size > required_size) {
          if (best == -1 || size < get_total_key_data_size(best))
            best = count + i;
        }
      }
      return (best);
    }

    // Removes a freelist entry at |slot|
    void freelist_remove(ham_u32_t slot) {
      ham_assert(get_freelist_count() > 0);

      if ((ham_u32_t)slot < m_node->get_count() + get_freelist_count() - 1) {
        memmove(m_layout.get_key_index_ptr(slot),
                        m_layout.get_key_index_ptr(slot + 1),
                        m_layout.get_key_index_span()
                              * (m_node->get_count() + get_freelist_count()
                                      - slot - 1));
      }

      set_freelist_count(get_freelist_count() - 1);
    }

    // Adds a bunch of indices to the freelist
    void freelist_add_many(int count) {
      // Copy the indices to the freelist area. This might overwrite the
      // end of the index area and corrupt the payload data, therefore
      // first copy those indices to a temporary array
      void *tmp = m_arena.resize(count * m_layout.get_key_index_span());
      memcpy(tmp, m_layout.get_key_index_ptr(0),
                      m_layout.get_key_index_span() * count);

      // then remove the deleted index keys by shifting all remaining
      // indices/freelist items "to the left"
      memmove(m_layout.get_key_index_ptr(0),
                      m_layout.get_key_index_ptr(count),
                      m_layout.get_key_index_span()
                            * (get_freelist_count() + m_node->get_count()
                                    - count));

      // Add the previously stored indices to the freelist
      memcpy(m_layout.get_key_index_ptr(m_node->get_count()
                              + get_freelist_count() - count), tmp,
                      m_layout.get_key_index_span() * count);

      set_freelist_count(get_freelist_count() + count);

      ham_assert(get_freelist_count() + m_node->get_count() - count
                      <= get_capacity());
    }

    // Adds the index at |slot| to the freelist
    //
    // TODO this has a different behavior than freelist_add_many(), because
    // it leaves the slot in place. change this!
    void freelist_add(ham_u32_t slot) {
      memcpy(m_layout.get_key_index_ptr(m_node->get_count()
                              + get_freelist_count()),
                      m_layout.get_key_index_ptr(slot),
                      m_layout.get_key_index_span());

      set_freelist_count(get_freelist_count() + 1);

      ham_assert(get_freelist_count() + m_node->get_count() <= get_capacity());
    }

    // Appends a key to the key space; if |use_freelist| is true, it will
    // first search for a sufficiently large freelist entry. Returns the
    // offset of the new key.
    ham_u32_t append_key(ham_u32_t count, const void *key_data,
                    ham_u32_t total_size, bool use_freelist) {
      ham_u32_t offset = allocate(count, total_size, use_freelist);
     
      // copy the key data AND the record data
      ham_u8_t *p = m_node->get_data() + kPayloadOffset + offset
                      + m_layout.get_key_index_span() * get_capacity();
      memmove(p, key_data, total_size);

      return (offset);
    }

    // Allocates storage for a key. If |use_freelist| is true, it will
    // first search for a sufficiently large freelist entry. Returns the
    // offset.
    ham_u32_t allocate(ham_u32_t count, ham_u32_t total_size,
                    bool use_freelist) {
      // search the freelist for free key space
      int idx = -1;
      ham_u32_t offset;

      if (use_freelist) {
        idx = freelist_find(count, total_size);
        // found: remove this freelist entry
        if (idx != -1) {
          offset = m_layout.get_key_data_offset(idx);
          ham_u32_t size = get_total_key_data_size(idx);
          freelist_remove(idx);
          // adjust the next key offset, if required
          if (get_next_offset() == offset + size)
            set_next_offset(offset + total_size);
        }
      }

      if (idx == -1) {
        if (count == 0) {
          offset = 0;
          set_next_offset(total_size);
        }
        else {
          offset = get_next_offset();
          set_next_offset(offset + total_size);
        }
      }

      return (offset);
    }

    // Calculates the next offset (but does not store it)
    ham_u32_t calc_next_offset(ham_u32_t count) const {
      ham_u32_t next_offset = 0;
      ham_u32_t total = count + get_freelist_count();
      for (ham_u32_t i = 0; i < total; i++) {
        ham_u32_t next = m_layout.get_key_data_offset(i)
                    + get_total_key_data_size(i);
        if (next >= next_offset)
          next_offset = next;
      }
      return (next_offset);
    }

    // Create a map with all occupied ranges in freelist and indices;
    // then make sure that there are no overlaps
    void check_index_integrity(ham_u32_t count) const {
      typedef std::pair<ham_u32_t, ham_u32_t> Range;
      typedef std::vector<Range> RangeVec;
      ham_u32_t total = count + get_freelist_count();
      RangeVec ranges;
      ranges.reserve(total);
      ham_u32_t next_offset = 0;
      for (ham_u32_t i = 0; i < total; i++) {
        ham_u32_t next = m_layout.get_key_data_offset(i)
                    + get_total_key_data_size(i);
        if (next >= next_offset)
          next_offset = next;
        ranges.push_back(std::make_pair(m_layout.get_key_data_offset(i),
                             get_total_key_data_size(i)));
      }
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
      if (next_offset != get_next_offset()) {
        ham_trace(("integrity violated: next offset %d, cached offset %d",
                    next_offset, get_next_offset()));
        throw Exception(HAM_INTEGRITY_VIOLATED);
      }
      if (next_offset != calc_next_offset(count)) {
        ham_trace(("integrity violated: next offset %d, cached offset %d",
                    next_offset, get_next_offset()));
        throw Exception(HAM_INTEGRITY_VIOLATED);
      }
    }

    // Re-arranges the node: moves all keys sequentially to the beginning
    // of the key space, removes the whole freelist
    void rearrange(ham_u32_t count, bool force = false) {
      // only continue if it's very likely that we can make free space;
      // otherwise this call would be too expensive
      if (!force && get_freelist_count() == 0 && count > 10)
        return;

      // get rid of the freelist - this node is now completely rewritten,
      // and the freelist would just complicate things
      set_freelist_count(0);

      // make a copy of all indices (excluding the freelist)
      SortHelper *s = (SortHelper *)m_arena.resize(count * sizeof(SortHelper));
      for (ham_u32_t i = 0; i < count; i++) {
        s[i].slot = i;
        s[i].offset = m_layout.get_key_data_offset(i);
      }

      // sort them by offset
      std::sort(&s[0], &s[count], sort_by_offset);

      // shift all keys to the left, get rid of all gaps at the front of the
      // key data or between the keys
      ham_u32_t next_offset = 0;
      ham_u32_t start = kPayloadOffset
                       + m_layout.get_key_index_span() * get_capacity();
      for (ham_u32_t i = 0; i < count; i++) {
        ham_u32_t offset = s[i].offset;
        ham_u32_t slot = s[i].slot;
        ham_u32_t size = get_total_key_data_size(slot);
        if (offset != next_offset) {
          // shift key to the left
          memmove(m_node->get_data() + start + next_offset,
                          get_key_data(slot), size);
          // store the new offset
          m_layout.set_key_data_offset(slot, next_offset);
        }
        next_offset += size;
      }

      set_next_offset(next_offset);

#ifdef HAM_DEBUG
      check_index_integrity(count);
#endif
    }

    // Tries to resize the node's capacity to fit |new_count| keys and at
    // least |key->size| additional bytes
    //
    // Returns true if the resize operation was not successful and a split
    // is required
    bool resize(ham_u32_t new_count, const ham_key_t *key) {
      ham_u32_t page_size = get_usable_page_size();

      // increase capacity of the indices by shifting keys "to the right"
      if (new_count + get_freelist_count() >= get_capacity() - 1) {
        // the absolute offset of the new key (including length and record)
        ham_u32_t capacity = get_capacity();
        ham_u32_t offset = get_next_offset();
        offset += (key->size > get_extended_threshold()
                        ? sizeof(ham_u64_t)
                        : key->size)
                + get_total_inline_record_size();
        offset += m_layout.get_key_index_span() * (capacity + 1);

        if (offset >= page_size)
          return (true);

        ham_u8_t *src = m_node->get_data() + kPayloadOffset
                        + capacity * m_layout.get_key_index_span();
        capacity++;
        ham_u8_t *dst = m_node->get_data() + kPayloadOffset
                        + capacity * m_layout.get_key_index_span();
        memmove(dst, src, get_next_offset());

        // store the new capacity
        set_capacity(capacity);

        // check if the new space is sufficient
        return (!has_enough_space(key, true));
      }

      // increase key data capacity by reducing capacity and shifting
      // keys "to the left"
      else {
        // number of slots that we would have to shift left to get enough
        // room for the new key
        ham_u32_t gap = (key->size + get_total_inline_record_size())
                            / m_layout.get_key_index_span();
        gap++;

        // if the space is not available then return, and the caller can
        // perform a split
        if (gap + new_count + get_freelist_count() >= get_capacity() - 1)
          return (true);

        ham_u32_t capacity = get_capacity();

        // if possible then shift a bit more, hopefully this can avoid another
        // shift when the next key is inserted
        gap = std::min(gap, (capacity - new_count - get_freelist_count()) / 2);

        // now shift the keys and adjust the capacity
        ham_u8_t *src = m_node->get_data() + kPayloadOffset
                + capacity * m_layout.get_key_index_span();
        capacity -= gap;
        ham_u8_t *dst = m_node->get_data() + kPayloadOffset
                + capacity * m_layout.get_key_index_span();
        memmove(dst, src, get_next_offset());

        // store the new capacity
        set_capacity(capacity);

        return (!has_enough_space(key, true));
      }
    }

    // Returns true if |key| can be inserted without splitting the page
    bool has_enough_space(const ham_key_t *key, bool use_extended,
                    bool force = false, int headroom = 0) {
      ham_u32_t count = m_node->get_count();

      if (count == 0) {
        set_freelist_count(0);
        set_next_offset(0);
        return (true);
      }

      // leave some headroom - a few operations create new indices; make sure
      // that they have index capacity left
      if (count + get_freelist_count() >= get_capacity() - 
                      (force ? 1 : 2))
        return (false);

      ham_u32_t offset = headroom + get_next_offset();
      if (use_extended)
        offset += key->size > get_extended_threshold()
                      ? sizeof(ham_u64_t)
                      : key->size;
      else
        offset += key->size;

      // need at least 8 byte for the record, in case we need to store a
      // reference to a duplicate table
      if (get_total_inline_record_size() < sizeof(ham_u64_t))
        offset += sizeof(ham_u64_t);
      else
        offset += get_total_inline_record_size();
      offset += m_layout.get_key_index_span() * get_capacity();
      if (offset < get_usable_page_size())
        return (true);

      // if there's a freelist entry which can store the new key then
      // a split won't be required
      return (-1 != freelist_find(count,
                        key->size + get_total_inline_record_size()));
    }

    // Returns the index capacity
    ham_u32_t get_capacity() const {
      return (ham_db2h32(*(ham_u32_t *)m_node->get_data()));
    }

    // Sets the index capacity
    void set_capacity(ham_u32_t capacity) {
      *(ham_u32_t *)m_node->get_data() = ham_h2db32(capacity);
    }

    // Returns the number of freelist entries
    ham_u32_t get_freelist_count() const {
      return (ham_db2h32(*(ham_u32_t *)(m_node->get_data() + 4)));
    }

    // Sets the number of freelist entries
    void set_freelist_count(ham_u32_t freelist_count) {
      *(ham_u32_t *)(m_node->get_data() + 4) = ham_h2db32(freelist_count);
    }

    // Returns the offset of the unused space at the end of the page
    ham_u32_t get_next_offset() const {
      return (ham_db2h32(*(ham_u32_t *)(m_node->get_data() + 8)));
    }

    // Sets the offset of the unused space at the end of the page
    void set_next_offset(ham_u32_t next_offset) {
      *(ham_u32_t *)(m_node->get_data() + 8) = ham_h2db32(next_offset);
    }

    // Returns true if the record is inline
    bool is_record_inline(ham_u32_t slot, ham_u32_t duplicate_index = 0) const {
      return (m_records.is_record_inline(slot, duplicate_index));
    }

    // Sets the inline record data
    void set_inline_record_data(ham_u32_t slot, const void *data,
                    ham_u32_t size, ham_u32_t duplicate_index) {
      m_records.set_inline_record_data(slot, data, size, duplicate_index);
    }

    // Returns the size of the record, if inline
    ham_u32_t get_inline_record_size(ham_u32_t slot,
                    ham_u32_t duplicate_index = 0) const {
      return (m_records.get_inline_record_size(slot, duplicate_index));
    }

    // Returns the maximum size of inline records (payload only!)
    ham_u32_t get_max_inline_record_size() const {
      return (m_records.get_max_inline_record_size());
    }

    // Returns the maximum size of inline records (incl. overhead!)
    ham_u32_t get_total_inline_record_size() const {
      return (m_records.get_total_inline_record_size());
    }

    // Sets the record counter
    void set_inline_record_count(ham_u32_t slot, ham_u8_t count) {
      m_layout.set_inline_record_count(slot, count);
    }

    // Returns the threshold for extended keys
    ham_u32_t get_extended_threshold() const {
      if (g_extended_threshold)
        return (g_extended_threshold);
      ham_u32_t page_size = m_page->get_db()->get_local_env()->get_page_size();
      if (page_size == 1024)
        return (64);
      if (page_size <= 1024 * 8)
        return (128);
      return (256);
    }

    // Returns the threshold for duplicate tables
    ham_u32_t get_duplicate_threshold() const {
      if (g_duplicate_threshold)
        return (g_duplicate_threshold);
      ham_u32_t page_size = m_page->get_db()->get_local_env()->get_page_size();
      if (page_size == 1024)
        return (32);
      if (page_size <= 1024 * 8)
        return (64);
      if (page_size <= 1024 * 16)
        return (128);
      // 0xff/255 is the maximum that we can store in the record
      // counter (1 byte!)
      return (255);
    }

    // Returns the usable page size that can be used for actually
    // storing the data
    ham_u32_t get_usable_page_size() const {
      return (m_page->get_db()->get_local_env()->get_usable_page_size()
                    - kPayloadOffset
                    - PBtreeNode::get_entry_offset());
    }

    // Generate more space for at least |required| bytes; returns the offset
    // to this space
    void force_more_space() {
      // pick the largest key
      ham_u32_t count = m_node->get_count();
      for (ham_u32_t i = 0; i < count; i++) {
        Iterator it = at(i);
        // can we move this key to an extended key?
        // we need 8 byte for the extkey + 16 byte for the new key and record
        // id
        if (it->get_key_flags() & BtreeKey::kExtendedKey)
          continue;

        if (m_layout.get_key_size(i) >= 24) {
          ham_u64_t rid = it->get_record_id();
          ham_key_t key = {0};
          key.data = it->get_key_data();
          key.size = it->get_key_size();
          ham_u64_t blobid = add_extended_key(&key);
          it->set_extended_blob_id(blobid);
          it->set_key_flags(BtreeKey::kExtendedKey);
          it->set_record_id(rid);
          break;
        }
      }

      rearrange(m_node->get_count(), true);
    }

    // The page that we're operating on
    Page *m_page;

    // The node that we're operating on
    PBtreeNode *m_node;

    // The LayoutImpl provides access to the upfront index
    LayoutImpl m_layout;

    // The RecordList provides access to the stored records
    RecordList m_records;

    // A memory arena for various tasks
    ByteArray m_arena;

    // Cache for extended keys
    ExtKeyCache *m_extkey_cache;

    // Cache for external duplicate tables
    DupTableCache *m_duptable_cache;
};

} // namespace hamsterdb

#endif /* HAM_BTREE_IMPL_DEFAULT_H__ */
