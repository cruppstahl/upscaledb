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
 * Fixed length KeyList for binary data
 *
 * This KeyList stores binary keys of fixed length size. It is implemented
 * as a plain C array of type uint8_t[]. It has fast random access, i.e.
 * key #N starts at data[N * keysize].
 *
 * This KeyList cannot be resized.
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_BTREE_KEYS_BINARY_H
#define HAM_BTREE_KEYS_BINARY_H

#include "0root/root.h"

#include <sstream>
#include <iostream>

// Always verify that a file of level N does not include headers > N!
#include "1globals/globals.h"
#include "1base/dynamic_array.h"
#include "2page/page.h"
#include "3btree/btree_node.h"
#include "3blob_manager/blob_manager.h"
#include "3btree/btree_keys_base.h"
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

//
// Same as the PodKeyList, but for binary arrays of fixed length
//
class BinaryKeyList : public BaseKeyList
{
  public:
    enum {
      // A flag whether this KeyList has sequential data
      kHasSequentialData = 1,

      // A flag whether this KeyList supports the scan() call
      kSupportsBlockScans = 1,

      // This KeyList uses binary search in combination with linear search
      kSearchImplementation = kBinaryLinear,
    };

    // Constructor
    BinaryKeyList(LocalDatabase *db)
        : m_data(0) {
      m_key_size = db->config().key_size;
      ham_assert(m_key_size != 0);
    }

    // Creates a new KeyList starting at |data|, total size is
    // |range_size| (in bytes)
    void create(uint8_t *data, size_t range_size) {
      m_data = data;
      m_range_size = range_size;
    }

    // Opens an existing KeyList starting at |data|
    void open(uint8_t *data, size_t range_size, size_t node_count) {
      m_data = data;
      m_range_size = range_size;
    }

    // Calculates the required size for this range
    size_t get_required_range_size(size_t node_count) const {
      return (node_count * m_key_size);
    }

    // Returns the actual key size including overhead
    size_t get_full_key_size(const ham_key_t *key = 0) const {
      return (m_key_size);
    }

    // Copies a key into |dest|
    void get_key(Context *context, int slot, ByteArray *arena, ham_key_t *dest,
                    bool deep_copy = true) const {
      dest->size = (uint16_t)m_key_size;
      if (likely(deep_copy == false)) {
        dest->data = &m_data[slot * m_key_size];
        return;
      }

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
      if (m_key_size > 32)
        return (-1); // disable linear search for large keys
      return (128 / m_key_size);
    }

    // Performs a linear search in a given range between |start| and
    // |start + length|
    template<typename Cmp>
    int linear_search(size_t start, size_t length, const ham_key_t *key,
                    Cmp &comparator, int *pcmp) {
      uint8_t *begin = &m_data[start * m_key_size];
      uint8_t *end = &m_data[(start + length) * m_key_size];
      uint8_t *current = begin;

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
      return (start + length - 1);
    }

    // Iterates all keys, calls the |visitor| on each
    void scan(Context *context, ScanVisitor *visitor, uint32_t start,
                    size_t length) {
      (*visitor)(&m_data[start * m_key_size], length);
    }

    // Erases a whole slot by shifting all larger keys to the "left"
    void erase(Context *context, size_t node_count, int slot) {
      if (slot < (int)node_count - 1)
        memmove(&m_data[slot * m_key_size], &m_data[(slot + 1) * m_key_size],
                      m_key_size * (node_count - slot - 1));
    }

    // Inserts a key
    template<typename Cmp>
    PBtreeNode::InsertResult insert(Context *context, size_t node_count,
                    const ham_key_t *key, uint32_t flags, Cmp &comparator,
                    int slot) {
      if (node_count > (size_t)slot)
        memmove(&m_data[(slot + 1) * m_key_size], &m_data[slot * m_key_size],
                      m_key_size * (node_count - slot));
      set_key_data(slot, key->data, key->size);
      return (PBtreeNode::InsertResult(0, slot));
    }

    // Returns true if the |key| no longer fits into the node
    bool requires_split(size_t node_count, const ham_key_t *key) const {
      return ((node_count + 1) * m_key_size >= m_range_size);
    }

    // Copies |count| key from this[sstart] to dest[dstart]
    void copy_to(int sstart, size_t node_count, BinaryKeyList &dest,
                    size_t other_count, int dstart) {
      memcpy(&dest.m_data[dstart * m_key_size], &m_data[sstart * m_key_size],
                      m_key_size * (node_count - sstart));
    }

    // Change the capacity; for PAX layouts this just means copying the
    // data from one place to the other
    void change_range_size(size_t node_count, uint8_t *new_data_ptr,
            size_t new_range_size, size_t capacity_hint) {
      memmove(new_data_ptr, m_data, node_count * m_key_size);
      m_data = new_data_ptr;
      m_range_size = new_range_size;
    }

    // Fills the btree_metrics structure
    void fill_metrics(btree_metrics_t *metrics, size_t node_count) {
      BaseKeyList::fill_metrics(metrics, node_count);
      BtreeStatistics::update_min_max_avg(&metrics->keylist_unused,
              m_range_size - (node_count * m_key_size));
    }

    // Prints a slot to |out| (for debugging)
    void print(Context *context, int slot, std::stringstream &out) const {
      for (size_t i = 0; i < m_key_size; i++)
        out << (char)m_data[slot * m_key_size + i];
    }

    // Returns the key size
    size_t get_key_size(int slot) const {
      return (m_key_size);
    }

    // Returns the pointer to a key's data
    uint8_t *get_key_data(int slot) {
      return (&m_data[slot * m_key_size]);
    }

    // Has support for SIMD style search?
    bool has_simd_support() const {
      return (false);
    }

    // Returns the pointer to the key's inline data - for SIMD calculations
    // Not implemented by this KeyList
    uint8_t *get_simd_data() {
      return (0);
    }

  private:
    // Returns the pointer to a key's data (const flavour)
    uint8_t *get_key_data(int slot) const {
      return (&m_data[slot * m_key_size]);
    }

    // Overwrites a key's data. The |size| of the new data HAS
    // to be identical to the "official" key size
    void set_key_data(int slot, const void *ptr, size_t size) {
      ham_assert(size == get_key_size(slot));
      memcpy(&m_data[slot * m_key_size], ptr, size);
    }

    // The size of a single key
    size_t m_key_size;

    // Pointer to the actual key data
    uint8_t *m_data;
};

} // namespace PaxLayout

} // namespace hamsterdb

#endif /* HAM_BTREE_KEYS_BINARY_H */
