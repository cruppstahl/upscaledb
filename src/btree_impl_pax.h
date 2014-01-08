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
 * Btree node layout for fixed length keys (w/o duplicates)
 * ========================================================
 *
 * This layout supports fixed length keys and fixed length records. It does
 * not support duplicates and extended keys. Keys and records are always
 * inlined, but records can refer to blobs (in this case the "fixed length"
 * record is the 8 byte record ID).
 *
 * Unlike the original PAX, which stored multiple columns in one page,
 * hamsterdb stores only one column (= database) in a page, but keys and
 * records are separated from each other. The keys (flags + key data) are
 * stored in the beginning of the page, the records start somewhere in the
 * middle (the exact start position depends on key size, page size and other
 * parameters).
 *
 * This layout's implementation is relatively simple because the offset
 * of the key data and record data is easy to calculate, since all keys
 * and records have the same size.
 *
 * This separation of keys and records allows a more compact layout and a
 * high density of the key data, which better exploits CPU caches and allows
 * very tight loops when searching through the keys.
 *
 * This layout has two incarnations:
 * 1. Fixed length keys, fixed length records
 *  -> does not require additional flags
 * 2. Fixed length keys, variable length records
 *  -> requires a 1 byte flag per key
 *
 * The flat memory layout looks like this:
 *
 * |Flag1|Flag2|...|Flagn|...|Key1|Key2|...|Keyn|...|Rec1|Rec2|...|Recn|
 *
 * Flags are optional, as described above.
 *
 * If records have a fixed length and are small enough then they're
 * stored inline. Otherwise a 64bit record ID is stored, which is the
 * absolute file offset of the blob with the record's data.
 */

#ifndef HAM_BTREE_IMPL_PAX_H__
#define HAM_BTREE_IMPL_PAX_H__

#include "util.h"
#include "page.h"
#include "btree_node.h"
#include "blob_manager.h"
#include "env_local.h"

namespace hamsterdb {

template<typename KeyList, typename RecordList>
class PaxNodeImpl;

//
// An iterator for the PaxNodeImpl class. It offers simple access to a single
// key ("slot") in the node, and can move forward to the next key.
//
template<typename KeyList, typename RecordList>
struct PaxIterator
{
  public:
    // Constructor
    PaxIterator(PaxNodeImpl<KeyList, RecordList> *node, ham_u32_t slot)
      : m_node(node), m_slot(slot) {
    }

    // Constructor
    PaxIterator(const PaxNodeImpl<KeyList, RecordList> *node, ham_u32_t slot)
      : m_node((PaxNodeImpl<KeyList, RecordList> *)node), m_slot(slot) {
    }

    // Returns the (persisted) flags of a key; see the |BtreeKey| namespace
    // in btree_flags.h
    ham_u8_t get_key_flags() const {
      return (m_node->get_key_flags(m_slot));
    }

    // Sets the flags of a key (BtreeRecord::kBlobSizeTiny etc); see
    // the |BtreeKey| namespace in btree_flags.h
    void set_key_flags(ham_u8_t flags) {
      m_node->set_key_flags(m_slot, flags);
    }

    // Returns the (persisted) flags of a record; not used, but supplied
    // because the BtreeNodeProxy relies on the existence of this function
    ham_u8_t get_record_flags() const {
      return (0);
    }

    // Returns the size of a btree key
    ham_u16_t get_key_size() const {
      return (m_node->get_key_size());
    }

    // Sets the size of a btree key
    void set_key_size(ham_u16_t size) {
      ham_assert(size == get_key_size());
    }

    // Returns a pointer to the key data
    ham_u8_t *get_key_data() {
      return (m_node->get_key_data(m_slot));
    }

    // Returns a pointer to the key data; const flavour
    ham_u8_t *get_key_data() const {
      return (m_node->get_key_data(m_slot));
    }

    // Overwrites the key data with a new key. The |size| of the new data
    // MUST be identical to the fixed key size that was specified when the
    // database was created!
    void set_key_data(const void *ptr, ham_u32_t size) {
      ham_assert(size == get_key_size());
      m_node->set_key_data(m_slot, ptr, size);
    }

    // Returns the number of records stored with this key; usually 1,
    // because duplicate keys are not supported by the PAX layout. If this
    // key's record was erased then 0 is returned
    ham_u32_t get_record_count() const {
      if (get_record_id() == 0 && !is_record_inline())
        return (0);
      return (1);
    }

    // Same as above, required for btree_node_proxy.h
    ham_u32_t get_total_record_count() const {
      return (get_record_count());
    }

    // Returns the record address of an extended key overflow area.
    // The PaxNodeImpl does not support extended keys.
    ham_u64_t get_extended_rid(LocalDatabase *db) const {
      // nop, but required to compile
      ham_verify(!"shouldn't be here");
      return (0);
    }

    // Sets the record address of an extended key overflow area
    // The PaxNodeImpl does not support extended keys.
    void set_extended_rid(LocalDatabase *db, ham_u64_t rid) {
      // nop, but required to compile
      ham_verify(!"shouldn't be here");
    }

    // Returns true if the record is inline
    bool is_record_inline() const {
      return (m_node->is_record_inline(m_slot));
    }

    // Returns the record id
    ham_u64_t get_record_id() const {
      return (m_node->get_record_id(m_slot));
    }

    // Sets the record id
    void set_record_id(ham_u64_t ptr) {
      m_node->set_record_id(m_slot, ham_h2db_offset(ptr));
    }

    // Returns a pointer to the record's inline data
    void *get_inline_record_data() {
      ham_assert(is_record_inline() == true);
      return (m_node->get_record_data(m_slot));
    }

    // Returns a pointer to the record's inline data
    const void *get_inline_record_data() const {
      ham_assert(is_record_inline() == true);
      return (m_node->get_record_data(m_slot));
    }

    // Sets the record data
    void set_inline_record_data(const void *ptr, ham_u32_t size) {
      m_node->set_record_data(m_slot, ptr, size);
    }

    // Returns the size of the record, if inline
    ham_u32_t get_inline_record_size() const {
      return (m_node->get_inline_record_size(m_slot));
    }

    // Returns the maximum size of inline records
    ham_u32_t get_max_inline_record_size() const {
      return (m_node->get_max_inline_record_size());
    }

    // Removes an inline record; this simply overwrites the inline
    // record data with zeroes and resets the record flags.
    void remove_inline_record() {
      ham_assert(is_record_inline() == true);
      m_node->remove_inline_record(m_slot);
    }

    // Moves this Iterator to the next key
    void next() {
      m_slot++;
    }

    // Allows use of operator-> in the caller
    PaxIterator<KeyList, RecordList> *operator->() {
      return (this);
    }

    // Allows use of operator-> in the caller
    const PaxIterator<KeyList, RecordList> *operator->() const {
      return (this);
    }

  private:
    // The node of this iterator
    PaxNodeImpl<KeyList, RecordList> *m_node;

    // The current slot in the node
    ham_u32_t m_slot;
};

//
// The PodKeyList provides simplified access to a list of keys, where each
// key is of type T (i.e. ham_u32_t).
//
template<typename T>
class PodKeyList
{
  public:
    // Constructor
    PodKeyList(LocalDatabase *db, ham_u8_t *data)
      : m_data((T *)data) {
    }

    // Returns the size of a single key
    ham_u32_t get_key_size() const {
      return (sizeof(T));
    }

    // Returns a pointer to the key's data
    ham_u8_t *get_key_data(ham_u32_t slot) {
      return ((ham_u8_t *)&m_data[slot]);
    }

    // Returns a pointer to the key's data (const flavour)
    ham_u8_t *get_key_data(ham_u32_t slot) const {
      return ((ham_u8_t *)&m_data[slot]);
    }

    // Overwrites an existing key; the |size| of the new data HAS to be
    // identical with the key size specified when the database was created!
    void set_key_data(ham_u32_t slot, const void *ptr, ham_u32_t size) {
      ham_assert(size == get_key_size());
      m_data[slot] = *(T *)ptr;
    }

  private:
    // The actual array of T's
    T *m_data;
};

//
// Same as the PodKeyList, but for binary arrays of fixed length
//
class BinaryKeyList
{
  public:
    // Constructor
    BinaryKeyList(LocalDatabase *db, ham_u8_t *data)
        : m_data(data) {
      m_key_size = db->get_key_size();
      ham_assert(m_key_size != 0);
    }

    // Returns the key size
    ham_u32_t get_key_size() const {
      return (m_key_size);
    }

    // Returns the pointer to a key's data
    ham_u8_t *get_key_data(ham_u32_t slot) {
      return (&m_data[slot * m_key_size]);
    }

    // Returns the pointer to a key's data (const flavour)
    ham_u8_t *get_key_data(ham_u32_t slot) const {
      return (&m_data[slot * m_key_size]);
    }

    // Overwrites a key's data (again, the |size| of the new data HAS
    // to be identical to the "official" key size
    void set_key_data(ham_u32_t slot, const void *ptr, ham_u32_t size) {
      ham_assert(size == get_key_size());
      memcpy(&m_data[slot * m_key_size], ptr, size);
    }

  private:
    // The size of a single key
    ham_u32_t m_key_size;

    // Pointer to the actual key data
    ham_u8_t *m_data;
};

//
// The DefaultRecordList provides simplified access to a list of records,
// where each record is either a 8-byte record identifier (specifying the
// address of a blob) or is stored inline, if the record's size is <= 8 bytes
//
class DefaultRecordList
{
  public:
    // Constructor
    DefaultRecordList(LocalDatabase *db)
      : m_data(0) {
    }

    // Sets the data pointer; required for initialization
    void set_data_pointer(void *ptr) {
      m_data = (ham_u64_t *)ptr;
    }

    // Returns false because this DefaultRecordList supports records of
    // different sizes
    static bool is_always_fixed_size() {
      return (false);
    }

    // Returns true if the record is inline, false if the record is a blob
    bool is_record_inline(ham_u32_t slot, ham_u8_t flags) const {
      return ((flags & BtreeRecord::kBlobSizeTiny)
              || (flags & BtreeRecord::kBlobSizeSmall)
              || (flags & BtreeRecord::kBlobSizeEmpty) != 0);
    }

    // Returns the size of an inline record
    ham_u32_t get_inline_record_size(ham_u32_t slot, ham_u32_t flags) const {
      ham_assert(is_record_inline(slot, flags));
      if (flags & BtreeRecord::kBlobSizeTiny) {
        /* the highest byte of the record id is the size of the blob */
        char *p = (char *)get_record_data(slot);
        return (p[sizeof(ham_u64_t) - 1]);
      }
      if (flags & BtreeRecord::kBlobSizeSmall)
        return (sizeof(ham_u64_t));
      if (flags & BtreeRecord::kBlobSizeEmpty)
        return (0);
      ham_assert(!"shouldn't be here");
      return (0);
    }

    // Returns the maximum size of an inline record
    ham_u32_t get_max_inline_record_size() const {
      return (sizeof(ham_u64_t));
    }

    // Returns a pointer to the data of a specific record
    void *get_record_data(ham_u32_t slot) {
      return (&m_data[slot]);
    }

    // Returns a pointer to the data to a specific record (const flavour)
    const void *get_record_data(ham_u32_t slot) const {
      return (&m_data[slot]);
    }

    // Sets the record id
    void set_record_id(ham_u32_t slot, ham_u64_t ptr) {
      m_data[slot] = ptr;
    }

    // Sets record data; returns the updated record flags
    ham_u32_t set_record_data(ham_u32_t slot, ham_u32_t flags, const void *ptr,
                    ham_u32_t size) {
      flags &= ~(BtreeRecord::kBlobSizeSmall
                      | BtreeRecord::kBlobSizeTiny
                      | BtreeRecord::kBlobSizeEmpty);

      if (size == 0) {
        m_data[slot] = 0;
        return (flags | BtreeRecord::kBlobSizeEmpty);
      }
      if (size < 8) {
        /* the highest byte of the record id is the size of the blob */
        char *p = (char *)&m_data[slot];
        p[sizeof(ham_u64_t) - 1] = size;
        memcpy(&m_data[slot], ptr, size);
        return (flags | BtreeRecord::kBlobSizeTiny);
      }
      if (size == 8) {
        memcpy(&m_data[slot], ptr, size);
        return (flags | BtreeRecord::kBlobSizeSmall);
      }
      ham_assert(!"shouldn't be here");
      return (flags);
    }

    // Removes an inline record; returns the updated record flags
    ham_u32_t remove_inline_record(ham_u32_t slot, ham_u32_t flags) {
      m_data[slot] = 0;
      return (flags & ~(BtreeRecord::kBlobSizeSmall
                        | BtreeRecord::kBlobSizeTiny
                        | BtreeRecord::kBlobSizeEmpty
                        | BtreeKey::kExtendedDuplicates));
    }

    // Clears a record
    void reset(ham_u32_t slot) {
      m_data[slot] = 0;
    }

  private:
    // The actual record data - an array of 64bit record IDs
    ham_u64_t *m_data;
};

//
// Same as above, but only for records of internal nodes. Internal nodes
// only store page IDs, therefore this |InternalRecordList| is optimized
// for 64bit IDs.
//
class InternalRecordList
{
  public:
    // Constructor
    InternalRecordList(LocalDatabase *db)
      : m_data(0) {
    }

    // Returns true if all records have a fixed size
    static bool is_always_fixed_size() {
      return (true);
    }

    // Returns the maximum size of an inline record
    ham_u32_t get_max_inline_record_size() const {
      return (sizeof(ham_u64_t));
    }

    // Returns true if the record is inline
    bool is_record_inline(ham_u32_t slot, ham_u8_t flags) const {
      return (true);
    }

    // Returns the size of an inline record
    ham_u32_t get_inline_record_size(ham_u32_t slot, ham_u32_t flags) const {
      return (get_max_inline_record_size());
    }

    // Sets the data pointer
    void set_data_pointer(void *ptr) {
      m_data = (ham_u64_t *)ptr;
    }

    // Returns data to a specific record
    void *get_record_data(ham_u32_t slot) {
      return (&m_data[slot]);
    }

    // Returns data to a specific record
    const void *get_record_data(ham_u32_t slot) const {
      return (&m_data[slot]);
    }

    // Removes an inline record; returns the updated flags
    ham_u32_t remove_inline_record(ham_u32_t slot, ham_u32_t flags) {
      flags &= ~(BtreeRecord::kBlobSizeSmall
                      | BtreeRecord::kBlobSizeTiny
                      | BtreeRecord::kBlobSizeEmpty
                      | BtreeKey::kExtendedDuplicates);
      m_data[slot] = 0;
      return (flags);
    }

    // Sets the record id
    void set_record_id(ham_u32_t slot, ham_u64_t ptr) {
      m_data[slot] = ptr;
    }

    // Sets record data; returns the updated flags
    ham_u32_t set_record_data(ham_u32_t slot, ham_u32_t flags, const void *ptr,
                    ham_u32_t size) {
      flags &= ~(BtreeRecord::kBlobSizeSmall
                      | BtreeRecord::kBlobSizeTiny
                      | BtreeRecord::kBlobSizeEmpty);
      ham_assert(size == get_max_inline_record_size());
      m_data[slot] = *(ham_u64_t *)ptr;
      return (flags);
    }

    // Clears a record
    void reset(ham_u32_t slot) {
      m_data[slot] = 0;
    }

  private:
    // The record data is an array of page IDs
    ham_u64_t *m_data;
};

//
// Same as above, but for binary (inline) records of fixed length; this
// RecordList does NOT support page IDs! All records are stored directly
// in the leaf.
//
class InlineRecordList
{
  public:
    // Constructor
    InlineRecordList(LocalDatabase *db)
      : m_record_size(db->get_record_size()), m_data(0), m_dummy(0) {
      ham_assert(m_record_size != HAM_RECORD_SIZE_UNLIMITED);
    }

    // This RecordList only manages fixed size records
    static bool is_always_fixed_size() {
      return (true);
    }

    // Returns the maximum size of an inline record
    ham_u32_t get_max_inline_record_size() const {
      return (m_record_size);
    }

    // Returns true if the record is inline
    bool is_record_inline(ham_u32_t slot, ham_u8_t flags) const {
      return (true);
    }

    // Returns the size of an inline record
    ham_u32_t get_inline_record_size(ham_u32_t slot, ham_u32_t flags) const {
      return (get_max_inline_record_size());
    }

    // Sets the data pointer
    void set_data_pointer(void *ptr) {
      m_data = (ham_u8_t *)ptr;
    }

    // Returns data to a specific record
    void *get_record_data(ham_u32_t slot) {
      if (m_record_size == 0)
        return (&m_dummy);
      return (&m_data[slot * m_record_size]);
    }

    // Returns data to a specific record
    const void *get_record_data(ham_u32_t slot) const {
      if (m_record_size == 0)
        return (&m_dummy);
      return (&m_data[slot * m_record_size]);
    }

    // Sets the record id
    void set_record_id(ham_u32_t slot, ham_u64_t ptr) {
      ham_assert(!"shouldn't be here");
    }

    // Sets record data; returns the updated record flags
    ham_u32_t set_record_data(ham_u32_t slot, ham_u32_t flags, const void *ptr,
                    ham_u32_t size) {
      ham_assert(size == get_max_inline_record_size());
      if (size)
        memcpy(&m_data[m_record_size * slot], ptr, size);
      return (flags);
    }

    // Clears a record
    void reset(ham_u32_t slot) {
      if (m_record_size)
        memset(&m_data[m_record_size * slot], 0, m_record_size);
    }

    // Removes an inline record; returns the updated record flags
    ham_u32_t remove_inline_record(ham_u32_t slot, ham_u32_t flags) {
      if (m_record_size)
        memset(&m_data[m_record_size * slot], 0, m_record_size);
      return (flags);
    }

  private:
    // The record size, as specified when the database was created
    ham_u32_t m_record_size;

    // The actual record data
    ham_u8_t *m_data;

    // dummy data for record pointers (if record size == 0)
    ham_u64_t m_dummy;
};

//
// A BtreeNodeProxy layout which stores key data, key flags and
// and the record pointers in a PAX style layout.
//
template<typename KeyList, typename RecordList>
class PaxNodeImpl
{
  public:
    typedef PaxIterator<KeyList, RecordList> Iterator;
    typedef const PaxIterator<KeyList, RecordList> ConstIterator;

    // Constructor
    PaxNodeImpl(Page *page)
      : m_page(page), m_node(PBtreeNode::from_page(page)),
        m_keys(page->get_db(), m_node->get_data()),
        m_records(page->get_db()) {
      ham_u32_t usable_nodesize
              = page->get_db()->get_local_env()->get_usable_page_size()
                    - PBtreeNode::get_entry_offset();
      ham_u32_t key_size = get_actual_key_size(m_keys.get_key_size());
      m_capacity = usable_nodesize / (key_size
                      + m_records.get_max_inline_record_size());

      ham_u8_t *p = m_node->get_data();
      // if records are fixed then flags are not required
      if (RecordList::is_always_fixed_size()) {
        m_flags = 0;
        m_records.set_data_pointer(&p[m_capacity * get_key_size()]);
      }
      // Otherwise initialize the pointer to the flags
      else {
        m_flags = &p[m_capacity * get_key_size()];
        m_records.set_data_pointer(&p[m_capacity * (1 + get_key_size())]);
      }
    }

    // Returns the actual key size (including overhead, without record)
    static ham_u16_t get_actual_key_size(ham_u32_t key_size) {
      ham_assert(key_size != HAM_KEY_SIZE_UNLIMITED);
      return ((ham_u16_t)(key_size
                      + (RecordList::is_always_fixed_size() ? 0 : 1)));
    }

    // Returns an iterator pointing to the first slot
    Iterator begin() {
      return (at(0));
    }

    // Returns an iterator pointing to the specified |slot|
    Iterator at(ham_u32_t slot) {
      return (Iterator(this, slot));
    }

    // Returns an iterator pointing to the specified |slot| (const flavour)
    ConstIterator at(ham_u32_t slot) const {
      return (ConstIterator(this, slot));
    }

    // Checks this node's integrity; due to the limited complexity, there's
    // not many possibilities how things can go wrong, therefore this function
    // never fails.
    void check_integrity() const {
    }

    // Compares two keys using the supplied comparator
    template<typename Cmp>
    int compare(const ham_key_t *lhs, Iterator it, Cmp &cmp) {
      return (cmp(lhs->data, lhs->size, it->get_key_data(), get_key_size()));
    }

    // Searches the node for the key and returns the slot of this key
    template<typename Cmp>
    int find(ham_key_t *key, Cmp &comparator, int *pcmp = 0) {
      ham_u32_t count = m_node->get_count();
      int i, l = 1, r = count - 1;
      int ret = 0, last = count + 1;
      int cmp = -1;

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

    // Returns a copy of a key and stores it in |dest|
    void get_key(ham_u32_t slot, ByteArray *arena, ham_key_t *dest) const {
      LocalDatabase *db = m_page->get_db();

      if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
        arena->resize(get_key_size());
        dest->data = arena->get_ptr();
        dest->size = get_key_size();
      }

      ham_assert(get_key_size() == db->get_key_size());
      memcpy(dest->data, get_key_data(slot), get_key_size());
    }

    // Returns the full record and stores it in |dest|
    void get_record(ham_u32_t slot, ByteArray *arena, ham_record_t *record,
                    ham_u32_t flags, ham_u32_t duplicate_index) const {
      Iterator it = at(slot);

      // regular inline record, no duplicates
      if (it->is_record_inline()) {
        ham_u32_t size = it->get_inline_record_size();
        if (size == 0) {
          record->data = 0;
          record->size = 0;
          return;
        }
        if (flags & HAM_PARTIAL) {
          ham_trace(("flag HAM_PARTIAL is not allowed if record is "
                     "stored inline"));
          throw Exception(HAM_INV_PARAMETER);
        }
        if (!(record->flags & HAM_RECORD_USER_ALLOC)
            && (flags & HAM_DIRECT_ACCESS)) {
          record->data = it->get_inline_record_data();
        }
        else {
          if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
            arena->resize(size);
            record->data = arena->get_ptr();
          }
          memcpy(record->data, it->get_inline_record_data(), size);
        }
        record->size = size;
        return;
      }

      // non-inline record, no duplicates
      LocalDatabase *db = m_page->get_db();
      LocalEnvironment *env = db->get_local_env();
      env->get_blob_manager()->read(db, it->get_record_id(), record,
                                flags, arena);
    }

    // Returns the record size of a key or one of its duplicates
    ham_u64_t get_record_size(ham_u32_t slot, int duplicate_index) const {
      Iterator it = at(slot);
      if (it->is_record_inline())
        return (it->get_inline_record_size());

      LocalDatabase *db = m_page->get_db();
      LocalEnvironment *env = db->get_local_env();
      return (env->get_blob_manager()->get_blob_size(db, it->get_record_id()));
    }

    // Updates the record of a key
    void set_record(ham_u32_t slot, ham_record_t *record,
                    ham_u32_t duplicate_index, ham_u32_t flags,
                    ham_u32_t *new_duplicate_index) {
      LocalDatabase *db = m_page->get_db();
      LocalEnvironment *env = db->get_local_env();
      Iterator it = at(slot);
      ham_u64_t ptr = it->get_record_id();

      ham_assert(duplicate_index == 0);

      // key does not yet exist
      if (!ptr && !it->is_record_inline()) {
        // a new inline key is inserted
        if (record->size <= it->get_max_inline_record_size()) {
          it->set_inline_record_data(record->data, record->size);
        }
        // a new (non-inline) key is inserted
        else {
          ptr = env->get_blob_manager()->allocate(db, record, flags);
          it->set_record_id(ptr);
        }
        return;
      }

      // an inline key exists
      if (it->is_record_inline()) {
        // disable small/tiny/empty flags
        it->set_key_flags(it->get_key_flags() & ~(BtreeRecord::kBlobSizeSmall
                            | BtreeRecord::kBlobSizeTiny
                            | BtreeRecord::kBlobSizeEmpty));
        // ... and is overwritten with another inline key
        if (record->size <= it->get_max_inline_record_size()) {
          it->set_inline_record_data(record->data, record->size);
        }
        // ... or with a (non-inline) key
        else {
          ptr = env->get_blob_manager()->allocate(db, record, flags);
          it->set_record_id(ptr);
        }
        return;
      }

      // a (non-inline) key exists
      if (ptr) {
        // ... and is overwritten by a inline key
        if (record->size <= it->get_max_inline_record_size()) {
          env->get_blob_manager()->erase(db, ptr);
          it->set_inline_record_data(record->data, record->size);
        }
        // ... and is overwritten by a (non-inline) key
        else {
          ptr = env->get_blob_manager()->overwrite(db, ptr, record, flags);
          it->set_record_id(ptr);
        }
        return;
      }

      ham_assert(!"shouldn't be here");
    }

    // Erases the extended part of a key; not supported by the PAX layout
    void erase_key(ham_u32_t slot) {
      // nop
    }

    // Erases the record; not supported by the PAX layout
    void erase_record(ham_u32_t slot, int duplicate_id, bool all_duplicates) {
      Iterator it = at(slot);

      if (it->is_record_inline()) {
        it->remove_inline_record();
        return;
      }

      // now erase the blob
      LocalDatabase *db = m_page->get_db();
      db->get_local_env()->get_blob_manager()->erase(db, it->get_record_id(), 0);
      it->set_record_id(0);
    }

    // Erases a key
    void erase(ham_u32_t slot) {
      ham_u32_t count = m_node->get_count();

      if (slot != count - 1) {
        memmove(m_keys.get_key_data(slot), m_keys.get_key_data(slot + 1),
                get_key_size() * (count - slot - 1));
        if (!RecordList::is_always_fixed_size()) {
          memmove(&m_flags[slot], &m_flags[slot + 1],
                  count - slot - 1);
        }
        memmove(m_records.get_record_data(slot),
                m_records.get_record_data(slot + 1),
                m_records.get_max_inline_record_size() * (count - slot - 1));
      }
    }

    // Replace |dest| with |src|
    void replace_key(ConstIterator src, Iterator dest) const {
      dest->set_key_flags(src->get_key_flags());
      dest->set_key_data(src->get_key_data(), src->get_key_size());
      dest->set_key_size(src->get_key_size());
    }

    // Replace |dest| with |src|
    void replace_key(ham_key_t *src, Iterator dest) {
      dest->set_key_flags(src->_flags);
      dest->set_key_data(src->data, src->size);
      dest->set_key_size(src->size);
    }

    // Same as above, but copies the key from |src_node[src_slot]|
    void insert(ham_u32_t slot, PaxNodeImpl *src_node, ham_u32_t src_slot) {
      ham_key_t key = {0};
      ConstIterator it = src_node->at(src_slot);
      key.data = it->get_key_data();
      key.size = it->get_key_size();
      insert(slot, &key);
    }

    // Inserts a new key
    void insert(ham_u32_t slot, const ham_key_t *key) {
      ham_assert(key->size == get_key_size());

      ham_u32_t count = m_node->get_count();

      // make space for 1 additional element.
      // only store the key data; flags and record IDs are set by the caller
      if (count > slot) {
        memmove(m_keys.get_key_data(slot + 1), m_keys.get_key_data(slot),
                        get_key_size() * (count - slot));
        m_keys.set_key_data(slot, key->data, key->size);
        if (!RecordList::is_always_fixed_size()) {
          memmove(&m_flags[slot + 1], &m_flags[slot],
                          count - slot);
          m_flags[slot] = 0;
        }
        memmove(m_records.get_record_data(slot + 1),
                        m_records.get_record_data(slot),
                        m_records.get_max_inline_record_size() * (count - slot));
        m_records.reset(slot);
      }
      else {
        m_keys.set_key_data(slot, key->data, key->size);
        if (!RecordList::is_always_fixed_size())
          m_flags[slot] = 0;
        m_records.reset(slot);
      }
    }

    // Returns true if |key| cannot be inserted because a split is required
    bool requires_split(const ham_key_t *key) const {
      return (m_node->get_count() >= m_capacity - 1);
    }

    // Returns true if the node requires a merge or a shift
    bool requires_merge() const {
      return (m_node->get_count() <= std::max(3u, m_capacity / 5));
    }

    // Splits a node and moves parts of the current node into |other|, starting
    // at the |pivot| slot
    void split(PaxNodeImpl *other, int pivot) {
      ham_u32_t count = m_node->get_count();

      //
      // if a leaf page is split then the pivot element must be inserted in
      // the leaf page AND in the internal node. the internal node update
      // is handled by the caller.
      //
      // in internal nodes the pivot element is only propagated to the
      // parent node. the pivot element is skipped.
      //
      if (m_node->is_leaf()) {
        memcpy(other->m_keys.get_key_data(0), m_keys.get_key_data(pivot),
                    get_key_size() * (count - pivot));
        if (!RecordList::is_always_fixed_size())
          memcpy(&other->m_flags[0], &m_flags[pivot],
                      count - pivot);
        memcpy(other->m_records.get_record_data(0),
                    m_records.get_record_data(pivot),
                    m_records.get_max_inline_record_size() * (count - pivot));
      }
      else {
        memcpy(other->m_keys.get_key_data(0), m_keys.get_key_data(pivot + 1),
                    get_key_size() * (count - pivot - 1));
        if (!RecordList::is_always_fixed_size())
          memcpy(&other->m_flags[0], &m_flags[pivot + 1],
                    count - pivot - 1);
        memcpy(other->m_records.get_record_data(0),
                    m_records.get_record_data(pivot + 1),
                    m_records.get_max_inline_record_size() * (count - pivot - 1));
      }
    }

    // Merges this node with the |other| node
    void merge_from(PaxNodeImpl *other) {
      ham_u32_t count = m_node->get_count();

      // shift items from the sibling to this page
      memcpy(m_keys.get_key_data(count), other->m_keys.get_key_data(0),
                      get_key_size() * other->m_node->get_count());
      if (!RecordList::is_always_fixed_size())
        memcpy(&m_flags[count], &other->m_flags[0], other->m_node->get_count());
      memcpy(m_records.get_record_data(count),
                      other->m_records.get_record_data(0),
                      m_records.get_max_inline_record_size()
                            * other->m_node->get_count());
    }

    // Shifts |count| elements from the right sibling (|other|) to this node
    void shift_from_right(PaxNodeImpl *other, int count) {
      ham_u32_t pos = m_node->get_count();

      // first perform the shift
      memcpy(m_keys.get_key_data(pos), other->m_keys.get_key_data(0),
                      get_key_size() * count);
      if (!RecordList::is_always_fixed_size())
        memcpy(&m_flags[pos], &other->m_flags[0], count);
      memcpy(m_records.get_record_data(pos),
                      other->m_records.get_record_data(0),
                      m_records.get_max_inline_record_size() * count);

      // then reduce the other page
      memmove(other->m_keys.get_key_data(0), other->m_keys.get_key_data(count),
                      get_key_size() * (other->m_node->get_count() - count));
      if (!RecordList::is_always_fixed_size())
        memmove(&other->m_flags[0], &other->m_flags[count],
                        (other->m_node->get_count() - count));
      memmove(other->m_records.get_record_data(0),
                      other->m_records.get_record_data(count),
                      m_records.get_max_inline_record_size()
                            * (other->m_node->get_count() - count));
    }

    // Shifts |count| elements from this node to |other|, starting at
    // |slot|
    void shift_to_right(PaxNodeImpl *other, ham_u32_t slot, int count) {
      // make room in the right sibling
      memmove(other->m_keys.get_key_data(count), other->m_keys.get_key_data(0),
                      get_key_size() * other->m_node->get_count());
      if (!RecordList::is_always_fixed_size())
        memmove(&other->m_flags[count], &other->m_flags[0],
                        other->m_node->get_count());
      memmove(other->m_records.get_record_data(count),
                      other->m_records.get_record_data(0),
                      m_records.get_max_inline_record_size()
                            * other->m_node->get_count());

      // shift |count| elements from this page to |other|
      memcpy(other->m_keys.get_key_data(0), m_keys.get_key_data(slot),
                      get_key_size() * count);
      if (!RecordList::is_always_fixed_size())
        memcpy(&other->m_flags[0], &m_flags[slot], count);
      memcpy(other->m_records.get_record_data(0),
                      m_records.get_record_data(slot),
                      m_records.get_max_inline_record_size() * count);
    }

    // Returns the record counter of a key
    ham_u32_t get_total_record_count(ham_u32_t slot) const {
      Iterator it = at(slot);
      return (it->get_record_count());
    }

    // Clears the page with zeroes and reinitializes it
    void test_clear_page() {
      // this is not yet in use
      ham_assert(!"shouldn't be here");
    }

  private:
    friend struct PaxIterator<KeyList, RecordList>;

    // Returns the key size
    ham_u32_t get_key_size() const {
      return (m_keys.get_key_size());
    }

    // Returns the flags of a key
    ham_u8_t get_key_flags(ham_u32_t slot) const {
      if (RecordList::is_always_fixed_size())
        return (0);
      else
        return (m_flags[slot]);
    }

    // Sets the flags of a key
    void set_key_flags(ham_u32_t slot, ham_u8_t flags) {
      if (!RecordList::is_always_fixed_size())
        m_flags[slot] = flags;
    }

    // Returns a pointer to the key data
    ham_u8_t *get_key_data(ham_u32_t slot) const {
      return (m_keys.get_key_data(slot));
    }

    // Sets the key data
    void set_key_data(ham_u32_t slot, const void *ptr, ham_u32_t size) {
      m_keys.set_key_data(slot, ptr, size);
    }

    // Returns true if the record is inline
    bool is_record_inline(ham_u32_t slot) const {
      return (m_records.is_record_inline(slot, get_key_flags(slot)));
    }

    // Returns the maximum size of an inline record
    ham_u32_t get_max_inline_record_size() const {
      return (m_records.get_max_inline_record_size());
    }

    // Returns the size of an inline record
    ham_u32_t get_inline_record_size(ham_u32_t slot) const {
      ham_assert(is_record_inline(slot) == true);
      return (m_records.get_inline_record_size(slot, get_key_flags(slot)));
    }

    // Removes an inline record
    void remove_inline_record(ham_u32_t slot) {
      if (RecordList::is_always_fixed_size())
        m_records.remove_inline_record(slot, 0);
      else
        m_flags[slot] = m_records.remove_inline_record(slot, m_flags[slot]);
    }

    // Returns a pointer to the record id
    ham_u64_t *get_record_data(ham_u32_t slot) {
      return ((ham_u64_t *)m_records.get_record_data(slot));
    }

    // Returns a pointer to the record id
    const ham_u64_t *get_record_data(ham_u32_t slot) const {
      return ((ham_u64_t *)m_records.get_record_data(slot));
    }

    // Returns the record id
    ham_u64_t get_record_id(ham_u32_t slot) const {
      return (ham_db2h_offset(*get_record_data(slot)));
    }

    // Sets the record id
    void set_record_id(ham_u32_t slot, ham_u64_t ptr) {
      m_records.set_record_id(slot, ptr);
    }

    // Sets the record data
    void set_record_data(ham_u32_t slot, const void *ptr, ham_u32_t size) {
      if (RecordList::is_always_fixed_size())
        m_records.set_record_data(slot, 0, ptr, size);
      else
        m_flags[slot] = m_records.set_record_data(slot, m_flags[slot],
                        ptr, size);
    }

    // The page we're operating on
    Page *m_page;

    // The node we're operating on
    PBtreeNode *m_node;

    // Capacity of this node (maximum number of key/record pairs that
    // can be stored)
    ham_u32_t m_capacity;

    // Pointer to the flags - can be null if flags are not required
    ham_u8_t *m_flags;

    // for accessing the keys
    KeyList m_keys;

    // for accessing the records
    RecordList m_records;
};

} // namespace hamsterdb

#endif /* HAM_BTREE_IMPL_PAX_H__ */
