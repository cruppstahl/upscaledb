/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

/*
 * Fixed length KeyList for built-in data types ("POD types")
 *
 * This is the fastest KeyList available. It stores POD data sequentially
 * in an array, i.e. PodKeyList<uint32_t> is simply a plain
 * C array of type uint32_t[]. Each key has zero overhead.
 *
 * This KeyList cannot be resized.
 */

#ifndef UPS_BTREE_KEYS_POD_H
#define UPS_BTREE_KEYS_POD_H

#include "0root/root.h"

#include <sstream>
#include <iostream>

// Always verify that a file of level N does not include headers > N!
#include "1globals/globals.h"
#include "1base/dynamic_array.h"
#include "2page/page.h"
#include "3btree/btree_node.h"
#include "3btree/btree_keys_base.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

//
// The PodKeyList provides simplified access to a list of keys where each
// key is of type T (i.e. uint32_t).
//
template<typename T>
struct PodKeyList : BaseKeyList {
  enum {
    // A flag whether this KeyList has sequential data
    kHasSequentialData = 1,

    // A flag whether this KeyList supports the scan() call
    kSupportsBlockScans = 1,

    // This KeyList has a custom find() implementation
    kCustomFind = 1,

    // This KeyList has a custom find_lower_bound() implementation
    kCustomFindLowerBound = 1,
  };

  // Constructor
  PodKeyList(LocalDb *db, PBtreeNode *node)
    : BaseKeyList(db, node), _data(0) {
  }

  // Creates a new PodKeyList starting at |ptr|, total size is
  // |range_size| (in bytes)
  void create(uint8_t *ptr, size_t range_size_) {
    _data = (T *)ptr;
    range_size = range_size_;
  }

  // Opens an existing PodKeyList starting at |ptr|
  void open(uint8_t *ptr, size_t range_size_, size_t) {
    _data = (T *)ptr;
    range_size = range_size_;
  }

  // Returns the required size for the current set of keys
  size_t required_range_size(size_t node_count) const {
    return node_count * sizeof(T);
  }

  // Returns the actual key size including overhead
  size_t full_key_size(const ups_key_t * = 0) const {
    return sizeof(T);
  }

  // Finds a key
#ifdef __SSE__
  // Searches the node for the key and returns the slot of this key
  // - only for exact matches!
  //
  // This is the SIMD implementation. If SIMD is disabled then the
  // BaseKeyList::find method is used.
  template<typename Cmp>
  int find(Context *, size_t node_count, const ups_key_t *key, Cmp &) {
    return find_simd_sse<T>(node_count, &_data[0], key);
  }
#else
  template<typename Cmp>
  int find(Context *, size_t node_count, const ups_key_t *hkey, Cmp &) {
    T key = *(T *)hkey->data;
    T *result = std::lower_bound(&_data[0], &_data[node_count], key);
    if (unlikely(result == &_data[node_count] || *result != key))
      return -1;
    return result - &_data[0];
  }
#endif

  // Performs a lower-bound search for a key
  template<typename Cmp>
  int find_lower_bound(Context *, size_t node_count, const ups_key_t *hkey,
                  Cmp &, int *pcmp) {
    T key = *(T *)hkey->data;
    T *result = std::lower_bound(&_data[0], &_data[node_count], key);
    if (unlikely(result == &_data[node_count])) {
      if (key > _data[node_count - 1]) {
        *pcmp = +1;
        return node_count - 1;
      }
      if (key < _data[0]) {
        *pcmp = -1;
        return 0;
      }
      assert(!"shouldn't be here");
      throw Exception(UPS_INTERNAL_ERROR);
    }

    if (key > *result) {
      *pcmp = +1;
      return result - &_data[0];
    }

    if (key < *result) {
      *pcmp = +1;
      return (result - 1) - &_data[0];
    }

    *pcmp = 0;
    return result - &_data[0];
  }

  // Copies a key into |dest|
  void key(Context *, int slot, ByteArray *arena, ups_key_t *dest,
                  bool deep_copy = true) const {
    dest->size = sizeof(T);
    if (deep_copy == false) {
      dest->data = &_data[slot];
      return;
    }

    // allocate memory (if required)
    if (NOTSET(dest->flags, UPS_KEY_USER_ALLOC)) {
      arena->resize(dest->size);
      dest->data = arena->data();
    }

    *(T *)dest->data = _data[slot];
  }

  // Iterates all keys, calls the |visitor| on each
  ScanResult scan(ByteArray *, size_t node_count, uint32_t start) {
    return std::make_pair(&_data[start], node_count - start);
  }

  // Erases a whole slot by shifting all larger keys to the "left"
  void erase(Context *, size_t node_count, int slot) {
    if (slot < (int)node_count - 1)
      ::memmove(&_data[slot], &_data[slot + 1],
                      sizeof(T) * (node_count - slot - 1));
  }

  // Inserts a key
  template<typename Cmp>
  PBtreeNode::InsertResult insert(Context *, size_t node_count,
                  const ups_key_t *key, uint32_t flags, Cmp &, int slot) {
    if (node_count > (size_t)slot)
      ::memmove(&_data[slot + 1], &_data[slot],
                      sizeof(T) * (node_count - slot));
    assert(key->size == sizeof(T));
    _data[slot] = *(T *)key->data;
    return PBtreeNode::InsertResult(0, slot);
  }

  // Copies |count| key from this[sstart] to dest[dstart]
  void copy_to(int sstart, size_t node_count, PodKeyList<T> &dest,
                  size_t other_count, int dstart) {
    ::memcpy(&dest._data[dstart], &_data[sstart],
                    sizeof(T) * (node_count - sstart));
  }

  // Returns true if the |key| no longer fits into the node
  bool requires_split(size_t node_count, const ups_key_t *key) const {
    return (node_count + 1) * sizeof(T) >= range_size;
  }

  // Change the range size; just copy the data from one place to the other
  void change_range_size(size_t node_count, uint8_t *new_data_ptr,
          size_t new_range_size, size_t capacity_hint) {
    ::memmove(new_data_ptr, _data, node_count * sizeof(T));
    _data = (T *)new_data_ptr;
    range_size = new_range_size;
  }

  // Fills the btree_metrics structure
  void fill_metrics(btree_metrics_t *metrics, size_t node_count) {
    BaseKeyList::fill_metrics(metrics, node_count);
    BtreeStatistics::update_min_max_avg(&metrics->keylist_unused,
            range_size - (node_count * sizeof(T)));
  }

  // Prints a slot to |out| (for debugging)
  void print(Context *context, int slot, std::stringstream &out) const {
    out << _data[slot];
  }

  // Returns the key size
  size_t key_size(int) const {
    return sizeof(T);
  }

  // Returns a pointer to the key's data
  uint8_t *key_data(int slot) const {
    return (uint8_t *)&_data[slot];
  }

  // The actual array of T's
  T *_data;
};

} // namespace upscaledb

#endif // UPS_BTREE_KEYS_POD_H
