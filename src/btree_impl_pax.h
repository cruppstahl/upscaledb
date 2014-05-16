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

#include <sstream>

#include "globals.h"
#include "util.h"
#include "page.h"
#include "btree_node.h"
#include "blob_manager.h"
#include "env_local.h"

namespace hamsterdb {

//
// The template classes in this file are wrapped in a separate namespace
// to avoid naming clashes with btree_impl_default.h
//
namespace PaxLayout {

//
// The PodKeyList provides simplified access to a list of keys where each
// key is of type T (i.e. ham_u32_t).
//
template<typename T>
class PodKeyList
{
  public:
    enum {
      // A flag whether this KeyList has sequential data
      kHasSequentialData = 1,

      // A flag whether SIMD style linear access is supported
      kHasSimdSupport = 1
    };

    // Constructor
    PodKeyList(LocalDatabase *db)
      : m_data(0), m_capacity(0) {
    }

    // Creates a new PodKeyList starting at |ptr|, total size is
    // |full_range_size_bytes| (in bytes)
    void create(ham_u8_t *data, size_t full_range_size_bytes, size_t capacity) {
      m_data = (T *)data;
      m_capacity = capacity;
    }

    // Opens an existing PodKeyList starting at |ptr|
    void open(ham_u8_t *data, size_t capacity) {
      m_capacity = capacity;
      m_data = (T *)data;
    }

    // Returns the full size of the range
    size_t get_range_size() const {
      return (m_capacity * get_full_key_size());
    }

    // Calculates the required size for a range with the specified |capacity|
    size_t calculate_required_range_size(size_t node_count,
            size_t new_capacity) const {
      return (new_capacity * get_full_key_size());
    }

    // Returns the actual key size including overhead
    ham_u16_t get_full_key_size(const ham_key_t *key = 0) const {
      return (sizeof(T));
    }

    // Copies a key into |dest|
    void get_key(ham_u32_t slot, ByteArray *arena, ham_key_t *dest) const {
      dest->size = sizeof(T);

      // allocate memory (if required)
      if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
        arena->resize(dest->size);
        dest->data = arena->get_ptr();
      }

      memcpy(dest->data, &m_data[slot], sizeof(T));
    }

    // Returns the threshold when switching from binary search to
    // linear search
    size_t get_linear_search_threshold() const {
      // disabled the check for linear_threshold because it avoids
      // inlining of this function
#if 0
      if (Globals::ms_linear_threshold)
        return (Globals::ms_linear_threshold);
#endif
      return (128 / sizeof(T));
    }

    // Performs a linear search in a given range between |start| and
    // |start + length|
    template<typename Cmp>
    int linear_search(ham_u32_t start, ham_u32_t count, ham_key_t *hkey,
                    Cmp &comparator, int *pcmp) {
      T key = *(T *)hkey->data;

      ham_u32_t c = start;
      ham_u32_t end = start + count;

#undef COMPARE
#define COMPARE(c)      if (key <= m_data[c]) {                         \
                          if (key < m_data[c]) {                        \
                            if (c == 0)                                 \
                              *pcmp = -1; /* key < m_data[0] */         \
                            else                                        \
                              *pcmp = +1; /* key > m_data[c - 1] */     \
                            return ((c) - 1);                           \
                          }                                             \
                          *pcmp = 0;                                    \
                          return (c);                                   \
                        }

      while (c + 8 < end) {
        COMPARE(c)
        COMPARE(c + 1)
        COMPARE(c + 2)
        COMPARE(c + 3)
        COMPARE(c + 4)
        COMPARE(c + 5)
        COMPARE(c + 6)
        COMPARE(c + 7)
        c += 8;
      }

      while (c < end) {
        COMPARE(c)
        c++;
      }

      /* the new key is > the last key in the page */
      *pcmp = 1;
      return (start + count - 1);
    }

    // Iterates all keys, calls the |visitor| on each
    void scan(ScanVisitor *visitor, ham_u32_t start, size_t count) {
      (*visitor)(&m_data[start], count);
    }

    // Erases the extended part of a key; nothing to do here
    void erase_data(ham_u32_t slot) {
    }

    // Erases a whole slot by shifting all larger keys to the "left"
    void erase_slot(size_t node_count, ham_u32_t slot) {
      if (slot < node_count - 1)
        memmove(&m_data[slot], &m_data[slot + 1],
                        sizeof(T) * (node_count - slot - 1));
    }

    // Inserts a key
    void insert(size_t node_count, ham_u32_t slot, const ham_key_t *key) {
      if (node_count > slot)
        memmove(&m_data[slot + 1], &m_data[slot],
                        sizeof(T) * (node_count - slot));
      set_key_data(slot, key->data, key->size);
    }

    // Copies |count| key from this[sstart] to dest[dstart]
    void copy_to(ham_u32_t sstart, size_t node_count, PodKeyList<T> &dest,
                    size_t other_count, ham_u32_t dstart) {
      memcpy(&dest.m_data[dstart], &m_data[sstart],
                      sizeof(T) * (node_count - sstart));
    }

    // Returns true if the |key| no longer fits into the node
    bool requires_split(size_t node_count, const ham_key_t *key) {
      return (node_count >= m_capacity);
    }

    // Checks the integrity of this node. Throws an exception if there is a
    // violation.
    void check_integrity(ham_u32_t count, bool quick = false) const {
    }

    // Rearranges the list; not supported
    void vacuumize(ham_u32_t node_count, bool force) const {
    }

    // Change the capacity; for PAX layouts this just means copying the
    // data from one place to the other
    void change_capacity(size_t node_count, size_t old_capacity,
            size_t new_capacity, ham_u8_t *new_data_ptr,
            size_t new_range_size) {
      memmove(new_data_ptr, m_data, node_count * sizeof(T));
      m_data = (T *)new_data_ptr;
      m_capacity = new_capacity;
    }

    // Prints a slot to |out| (for debugging)
    void print(ham_u32_t slot, std::stringstream &out) const {
      out << m_data[slot];
    }

    // Returns the size of a key
    ham_u32_t get_key_size(ham_u32_t slot) const {
      return (sizeof(T));
    }

    // Returns a pointer to the key's data
    ham_u8_t *get_key_data(ham_u32_t slot) {
      return ((ham_u8_t *)&m_data[slot]);
    }

  private:
    // Returns a pointer to the key's data (const flavour)
    ham_u8_t *get_key_data(ham_u32_t slot) const {
      return ((ham_u8_t *)&m_data[slot]);
    }

    // Overwrites an existing key; the |size| of the new data HAS to be
    // identical with the key size specified when the database was created!
    void set_key_data(ham_u32_t slot, const void *ptr, size_t size) {
      ham_assert(size == sizeof(T));
      m_data[slot] = *(T *)ptr;
    }

    // The actual array of T's
    T *m_data;

    // The capacity of m_data
    size_t m_capacity;
};

//
// Same as the PodKeyList, but for binary arrays of fixed length
//
class BinaryKeyList
{
  public:
    enum {
      // A flag whether this KeyList has sequential data
      kHasSequentialData = 1,

      // A flag whether SIMD style linear access is supported
      kHasSimdSupport = 0
    };

    // Constructor
    BinaryKeyList(LocalDatabase *db)
        : m_data(0), m_capacity(0) {
      m_key_size = db->get_key_size();
      ham_assert(m_key_size != 0);
    }

    // Creates a new KeyList starting at |data|, total size is
    // |full_range_size_bytes| (in bytes)
    void create(ham_u8_t *data, size_t full_range_size_bytes, size_t capacity) {
      m_data = data;
      m_capacity = capacity;
    }

    // Opens an existing KeyList starting at |data|
    void open(ham_u8_t *data, size_t capacity) {
      m_capacity = capacity;
      m_data = data;
    }

    // Returns the full size of the range
    size_t get_range_size() const {
      return (m_capacity * m_key_size);
    }

    // Calculates the required size for a range with the specified |capacity|
    size_t calculate_required_range_size(size_t node_count,
            size_t new_capacity) const {
      return (new_capacity * get_full_key_size());
    }

    // Returns the actual key size including overhead
    ham_u16_t get_full_key_size(const ham_key_t *key = 0) const {
      return (m_key_size);
    }

    // Copies a key into |dest|
    void get_key(ham_u32_t slot, ByteArray *arena, ham_key_t *dest) const {
      dest->size = m_key_size;

      // allocate memory (if required)
      if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
        arena->resize(dest->size);
        dest->data = arena->get_ptr();
      }

      memcpy(dest->data, &m_data[slot * m_key_size], m_key_size);
    }

    // Returns the threshold when switching from binary search to
    // linear search
    size_t get_linear_search_threshold() const {
      // disabled the check for linear_threshold because it avoids
      // inlining of this function
#if 0
      if (Globals::ms_linear_threshold)
        return (Globals::ms_linear_threshold);
#endif
      if (m_key_size > 32)
        return (0xffffffff); // disable linear search for large keys
      return (128 / m_key_size);

    }

    // Performs a linear search in a given range between |start| and
    // |start + length|
    template<typename Cmp>
    int linear_search(ham_u32_t start, ham_u32_t count, ham_key_t *key,
                    Cmp &comparator, int *pcmp) {
      ham_u8_t *begin = &m_data[start * m_key_size];
      ham_u8_t *end = &m_data[(start + count) * m_key_size];
      ham_u8_t *current = begin;

      int c = start;

      while (current < end) {
        /* compare it against the key */
        int cmp = comparator(key->data, key->size, current, m_key_size);

        /* found it, or moved past the key? */
        if (cmp <= 0) {
          if (cmp < 0) {
            if (c == 0)
              *pcmp = -1; // key is < #m_data[0]
            else
              *pcmp = +1; // key is > #m_data[c - 1]!
            return (c - 1);
          }
          *pcmp = 0;
          return (c);
        }

        current += m_key_size;
        c++;
      }

      /* the new key is > the last key in the page */
      *pcmp = 1;
      return (start + count - 1);
    }

    // Iterates all keys, calls the |visitor| on each
    void scan(ScanVisitor *visitor, ham_u32_t start, size_t count) {
      (*visitor)(&m_data[start * m_key_size], count);
    }

    // Erases the extended part of a key; nothing to do here
    void erase_data(ham_u32_t slot) {
    }

    // Erases a whole slot by shifting all larger keys to the "left"
    void erase_slot(size_t node_count, ham_u32_t slot) {
      if (slot < node_count - 1)
        memmove(&m_data[slot * m_key_size], &m_data[(slot + 1) * m_key_size],
                      m_key_size * (node_count - slot - 1));
    }

    // Inserts a key
    void insert(size_t node_count, ham_u32_t slot, const ham_key_t *key) {
      if (node_count > slot)
        memmove(&m_data[(slot + 1) * m_key_size], &m_data[slot * m_key_size],
                      m_key_size * (node_count - slot));
      set_key_data(slot, key->data, key->size);
    }

    // Returns true if the |key| no longer fits into the node
    bool requires_split(size_t node_count, const ham_key_t *key) {
      return (node_count >= m_capacity);
    }

    // Copies |count| key from this[sstart] to dest[dstart]
    void copy_to(ham_u32_t sstart, size_t node_count, BinaryKeyList &dest,
                    size_t other_count, ham_u32_t dstart) {
      memcpy(&dest.m_data[dstart * m_key_size], &m_data[sstart * m_key_size],
                      m_key_size * (node_count - sstart));
    }

    // Checks the integrity of this node. Throws an exception if there is a
    // violation.
    void check_integrity(ham_u32_t count, bool quick = false) const {
    }

    // Rearranges the list; not supported
    void vacuumize(ham_u32_t node_count, bool force) const {
    }

    // Change the capacity; for PAX layouts this just means copying the
    // data from one place to the other
    void change_capacity(size_t node_count, size_t old_capacity,
            size_t new_capacity, ham_u8_t *new_data_ptr,
            size_t new_range_size) {
      memmove(new_data_ptr, m_data, node_count * m_key_size);
      m_data = new_data_ptr;
      m_capacity = new_capacity;
    }

    // Prints a slot to |out| (for debugging)
    void print(ham_u32_t slot, std::stringstream &out) const {
      for (size_t i = 0; i < m_key_size; i++)
        out << m_data[slot * m_key_size + i];
    }

    // Returns the key size
    ham_u32_t get_key_size(ham_u32_t slot) const {
      return ((ham_u32_t)m_key_size);
    }

    // Returns the pointer to a key's data
    ham_u8_t *get_key_data(ham_u32_t slot) {
      return (&m_data[slot * m_key_size]);
    }

  private:
    // Returns the pointer to a key's data (const flavour)
    ham_u8_t *get_key_data(ham_u32_t slot) const {
      return (&m_data[slot * m_key_size]);
    }

    // Overwrites a key's data. The |size| of the new data HAS
    // to be identical to the "official" key size
    void set_key_data(ham_u32_t slot, const void *ptr, size_t size) {
      ham_assert(size == get_key_size(slot));
      memcpy(&m_data[slot * m_key_size], ptr, size);
    }

    // The size of a single key
    size_t m_key_size;

    // Pointer to the actual key data
    ham_u8_t *m_data;

    // The capacity of m_data
    size_t m_capacity;
};

//
// The DefaultRecordList provides simplified access to a list of records,
// where each record is either a 8-byte record identifier (specifying the
// address of a blob) or is stored inline, if the record's size is <= 8 bytes
//
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
      return (m_capacity * get_full_record_size());
    }

    // Calculates the required size for a range with the specified |capacity|
    size_t calculate_required_range_size(size_t node_count,
            size_t new_capacity) const {
      return (new_capacity * get_full_record_size());
    }

    // Returns the actual record size including overhead
    ham_u32_t get_full_record_size() const {
      return (sizeof(ham_u64_t) + 1);
    }

    // Returns the record counter of a key
    // TODO can this method always return 1?
    ham_u32_t get_record_count(ham_u32_t slot) const {
      if (!is_record_inline(slot) && get_record_id(slot) == 0)
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
    void get_record(ham_u32_t slot, ham_u32_t duplicate_index,
                    ByteArray *arena, ham_record_t *record,
                    ham_u32_t flags) const {
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
    bool requires_split(size_t node_count) {
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

//
// Same as above, but only for records of internal nodes. Internal nodes
// only store page IDs, therefore this |InternalRecordList| is optimized
// for 64bit IDs.
//
class InternalRecordList
{
  public:
    enum {
      // A flag whether this RecordList has sequential data
      kHasSequentialData = 1
    };

    // Constructor
    InternalRecordList(LocalDatabase *db, PBtreeNode *node)
      : m_db(db), m_data(0), m_capacity(0) {
    }

    // Sets the data pointer
    void create(ham_u8_t *data, size_t full_range_size_bytes, size_t capacity) {
      m_data = (ham_u64_t *)data;
      m_capacity = capacity;
    }

    // Opens an existing RecordList
    void open(ham_u8_t *ptr, size_t capacity) {
      m_data = (ham_u64_t *)ptr;
      m_capacity = capacity;
    }

    // Returns the actual size including overhead
    size_t get_full_record_size() const {
      return (sizeof(ham_u64_t));
    }

    // Returns the full size of the range
    size_t get_range_size() const {
      return (m_capacity * get_full_record_size());
    }

    // Calculates the required size for a range with the specified |capacity|
    size_t calculate_required_range_size(size_t node_count,
            size_t new_capacity) const {
      return (new_capacity * get_full_record_size());
    }

    // Returns the record counter of a key
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
    void get_record(ham_u32_t slot, ham_u32_t duplicate_index,
                    ByteArray *arena, ham_record_t *record,
                    ham_u32_t flags) const {
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
    bool requires_split(size_t node_count) {
      return (node_count >= m_capacity);
    }

    // Checks the integrity of this node. Throws an exception if there is a
    // violation.
    void check_integrity(ham_u32_t count, bool quick = false) const {
    }

    // Rearranges the list; not supported
    void vacuumize(ham_u32_t node_count, bool force) const {
    }

    // Change the capacity; for PAX layouts this just means copying the
    // data from one place to the other
    void change_capacity(size_t node_count, size_t old_capacity,
            size_t new_capacity, ham_u8_t *new_data_ptr,
            size_t new_range_size) {
      memmove(new_data_ptr, m_data, node_count * get_full_record_size());
      m_data = (ham_u64_t *)new_data_ptr;
      m_capacity = new_capacity;
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

    // The capacity of m_data
    size_t m_capacity;
};

//
// Same as above, but for binary (inline) records of fixed length; this
// RecordList does NOT support page IDs! All records are stored directly
// in the leaf.
//
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
    size_t get_full_record_size() const {
      return ((ham_u32_t)m_record_size);
    }

    // Returns the full size of the range
    size_t get_range_size() const {
      return (m_capacity * get_full_record_size());
    }

    // Calculates the required size for a range with the specified |capacity|
    size_t calculate_required_range_size(size_t node_count,
            size_t new_capacity) const {
      return (new_capacity * get_full_record_size());
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
    void get_record(ham_u32_t slot, ham_u32_t duplicate_index,
                    ByteArray *arena, ham_record_t *record,
                    ham_u32_t flags) const {
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
    bool requires_split(size_t node_count) {
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
      memmove(new_data_ptr, m_data, node_count * get_full_record_size());
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

//
// A BtreeNodeProxy layout which stores key data, key flags and
// and the record pointers in a PAX style layout.
//
template<typename KeyList, typename RecordList>
class PaxNodeImpl
{
  public:
    // Constructor
    PaxNodeImpl(Page *page)
      : m_page(page), m_node(PBtreeNode::from_page(page)),
        m_keys(page->get_db()), m_records(page->get_db(), m_node) {
      ham_u32_t usable_nodesize
              = page->get_db()->get_local_env()->get_usable_page_size()
                    - PBtreeNode::get_entry_offset();
      m_capacity = usable_nodesize / (m_keys.get_full_key_size()
                      + m_records.get_full_record_size());

      ham_u8_t *p = m_node->get_data();
      if (m_node->get_count() == 0) {
        m_keys.create(&p[0], m_capacity * m_keys.get_full_key_size(),
                        m_capacity);
        m_records.create(&p[m_capacity * m_keys.get_full_key_size()],
                        m_capacity * m_records.get_full_record_size(),
                        m_capacity);
      }
      else {
        m_keys.open(&p[0], m_capacity);
        m_records.open(&p[m_capacity * m_keys.get_full_key_size()], m_capacity);
      }
    }

    // Returns the page's capacity
    size_t get_capacity() const {
      return (m_capacity);
    }

    // Checks this node's integrity; due to the limited complexity, there's
    // not many possibilities how things can go wrong, therefore this function
    // never fails.
    void check_integrity() const {
    }

    // Compares two keys using the supplied comparator
    template<typename Cmp>
    int compare(const ham_key_t *lhs, ham_u32_t rhs, Cmp &cmp) {
      return (cmp(lhs->data, lhs->size, m_keys.get_key_data(rhs),
                              m_keys.get_key_size(rhs)));
    }

    // Searches the node for the key and returns the slot of this key
    template<typename Cmp>
    int find_child(ham_key_t *key, Cmp &comparator, ham_u64_t *precord_id,
                    int *pcmp) {
      ham_u32_t count = m_node->get_count();
      ham_assert(count > 0);

      // Run a binary search, but fall back to linear search as soon as
      // the remaining range is too small
      int threshold = m_keys.get_linear_search_threshold();
      int i, l = 0, r = count;
      int last = count + 1;
      int cmp = -1;

      /* repeat till we found the key or the remaining range is so small that
       * we rather perform a linear search (which is faster for small ranges) */
      while (r - l > threshold) {
        /* get the median item; if it's identical with the "last" item,
         * we've found the slot */
        i = (l + r) / 2;

        if (i == last) {
          ham_assert(i >= 0);
          ham_assert(i < (int)count);
          *pcmp = 1;
          if (precord_id)
            *precord_id = get_record_id(i);
          return (i);
        }

        /* compare it against the key */
        cmp = compare(key, i, comparator);

        /* found it? */
        if (cmp == 0) {
          *pcmp = cmp;
          if (precord_id)
            *precord_id = get_record_id(i);
          return (i);
        }
        /* if the key is bigger than the item: search "to the left" */
        else if (cmp < 0) {
          if (r == 0) {
            ham_assert(i == 0);
            *pcmp = cmp;
            if (precord_id)
              *precord_id = m_node->get_ptr_down();
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
      int slot = m_keys.linear_search(l, r - l, key, comparator, pcmp);
      if (precord_id) {
        if (slot == -1)
          *precord_id = m_node->get_ptr_down();
        else
          *precord_id = get_record_id(slot);
      }
      return (slot);
    }

    // Searches the node for the key and returns the slot of this key
    // - only for exact matches!
    template<typename Cmp>
    int find_exact(ham_key_t *key, Cmp &comparator) {
      int cmp;
      int r = find_child(key, comparator, 0, &cmp);
      if (cmp)
        return (-1);
      return (r);
    }

    // Iterates all keys, calls the |visitor| on each
    void scan(ScanVisitor *visitor, ham_u32_t start, bool distinct) {
      m_keys.scan(visitor, start, m_node->get_count() - start);
    }

    // Returns a copy of a key and stores it in |dest|
    void get_key(ham_u32_t slot, ByteArray *arena, ham_key_t *dest) const {
      // copy (or assign) the key data
      m_keys.get_key(slot, arena, dest);
    }

    // Returns the record size of a key or one of its duplicates
    ham_u64_t get_record_size(ham_u32_t slot, int duplicate_index = 0) const {
      return (m_records.get_record_size(slot));
    }

    // Returns the record counter of a key
    ham_u32_t get_record_count(ham_u32_t slot) const {
      return (m_records.get_record_count(slot));
    }

    // Returns the full record and stores it in |dest|
    void get_record(ham_u32_t slot, ByteArray *arena, ham_record_t *record,
                    ham_u32_t flags, ham_u32_t duplicate_index) const {
      // copy the record data
      m_records.get_record(slot, duplicate_index, arena, record, flags);
    }

    // Updates the record of a key
    void set_record(ham_u32_t slot, ham_record_t *record,
                    ham_u32_t duplicate_index, ham_u32_t flags,
                    ham_u32_t *new_duplicate_index) {
      m_records.set_record(slot, duplicate_index, record, flags);
    }

    // Erases the extended part of a key
    void erase_key(ham_u32_t slot) {
      m_keys.erase_data(slot);
    }

    // Erases the record
    void erase_record(ham_u32_t slot, int duplicate_index,
                    bool all_duplicates) {
      m_records.erase_record(slot);
    }

    // Erases a key
    void erase(ham_u32_t slot) {
      size_t node_count = m_node->get_count();

      m_keys.erase_slot(node_count, slot);
      m_records.erase_slot(node_count, slot);
    }

    // Inserts a new key
    void insert(ham_u32_t slot, const ham_key_t *key) {
      ham_assert(key->size == m_keys.get_key_size(slot));

      size_t node_count = m_node->get_count();

      // make space for 1 additional element.
      // only store the key data; flags and record IDs are set by the caller
      m_keys.insert(node_count, slot, key);
      m_records.insert_slot(node_count, slot);
    }

    // Returns true if |key| cannot be inserted because a split is required
    bool requires_split(const ham_key_t *key) const {
      return (m_node->get_count() >= m_capacity);
    }

    // Splits a node and moves parts of the current node into |other|, starting
    // at the |pivot| slot
    void split(PaxNodeImpl *other, int pivot) {
      size_t node_count = m_node->get_count();
      size_t other_node_count = other->m_node->get_count();

      //
      // if a leaf page is split then the pivot element must be inserted in
      // the leaf page AND in the internal node. the internal node update
      // is handled by the caller.
      //
      // in internal nodes the pivot element is only propagated to the
      // parent node. the pivot element is skipped.
      //
      if (m_node->is_leaf()) {
        m_keys.copy_to(pivot, node_count, other->m_keys,
                        other_node_count, 0);
        m_records.copy_to(pivot, node_count, other->m_records,
                        other_node_count, 0);
      }
      else {
        m_keys.copy_to(pivot + 1, node_count, other->m_keys,
                        other_node_count, 0);
        m_records.copy_to(pivot + 1, node_count, other->m_records,
                        other_node_count, 0);
      }
    }

    // Returns true if the node requires a merge or a shift
    bool requires_merge() const {
      return (m_node->get_count() <= std::max((size_t)3, m_capacity / 5));
    }

    // Merges this node with the |other| node
    void merge_from(PaxNodeImpl *other) {
      size_t node_count = m_node->get_count();
      size_t other_node_count = other->m_node->get_count();

      // shift items from the sibling to this page
      other->m_keys.copy_to(0, other_node_count, m_keys,
                      node_count, node_count);
      other->m_records.copy_to(0, other_node_count, m_records,
                      node_count, node_count);
    }

    // Prints a slot to stdout (for debugging)
    void print(ham_u32_t slot) const {
      std::stringstream ss;
      ss << "   ";
      m_keys.print(slot, ss);
      ss << " -> ";
      m_records.print(slot, ss);
      printf("%s\n", ss.str().c_str());
    }

    // Returns the record id
    ham_u64_t get_record_id(ham_u32_t slot) const {
      return (m_records.get_record_id(slot));
    }

    // Sets the record id
    void set_record_id(ham_u32_t slot, ham_u64_t ptr) {
      m_records.set_record_id(slot, ptr);
    }

  private:
    // The page we're operating on
    Page *m_page;

    // The node we're operating on
    PBtreeNode *m_node;

    // Capacity of this node (maximum number of key/record pairs that
    // can be stored)
    size_t m_capacity;

    // for accessing the keys
    KeyList m_keys;

    // for accessing the records
    RecordList m_records;
};

} // namespace hamsterdb

#endif /* HAM_BTREE_IMPL_PAX_H__ */
