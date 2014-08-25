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
 * Fixed length KeyList for built-in data types ("POD types")
 *
 * This is the fastest KeyList available. It stores POD data sequentially
 * in an array, i.e. PodKeyList<ham_u32_t> is simply a plain
 * C array of type ham_u32_t[]. Each key has zero overhead.
 *
 * This KeyList cannot be resized.
 */

#ifndef HAM_BTREE_KEYS_POD_H
#define HAM_BTREE_KEYS_POD_H

#include "0root/root.h"

#include <sstream>
#include <iostream>

// Always verify that a file of level N does not include headers > N!
#include "1globals/globals.h"
#include "1base/byte_array.h"
#include "2page/page.h"
#include "3btree/btree_node.h"
#include "3btree/btree_keys_base.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

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
class PodKeyList : public BaseKeyList
{
  public:
    typedef T type;

    enum {
      // A flag whether this KeyList has sequential data
      kHasSequentialData = 1,

      // A flag whether this KeyList supports the scan() call
      kSupportsBlockScans = 1
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
      return (m_capacity * sizeof(T));
    }

    // Calculates the required size for a range with the specified |capacity|
    size_t calculate_required_range_size(size_t node_count,
            size_t new_capacity) const {
      return (new_capacity * sizeof(T));
    }

    // Returns the actual key size including overhead
    double get_full_key_size(const ham_key_t *key = 0) const {
      return (sizeof(T));
    }

    // Copies a key into |dest|
    void get_key(ham_u32_t slot, ByteArray *arena, ham_key_t *dest,
                    bool deep_copy = true) const {
      dest->size = sizeof(T);
      if (deep_copy == false) {
        dest->data = &m_data[slot];
        return;
      }

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
    bool requires_split(size_t node_count, const ham_key_t *key,
                    bool vacuumize = false) const {
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

} // namespace PaxLayout

} // namespace hamsterdb

#endif /* HAM_BTREE_KEYS_POD_H */
