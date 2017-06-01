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
 * Fixed length KeyList for binary data
 *
 * This KeyList stores binary keys of fixed length size. It is implemented
 * as a plain C array of type uint8_t[]. It has fast random access, i.e.
 * key #N starts at data[N * keysize].
 *
 * This KeyList cannot be resized.
 */

#ifndef UPS_BTREE_KEYS_BINARY_H
#define UPS_BTREE_KEYS_BINARY_H

#include "0root/root.h"

#include <cstdio>
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

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

//
// Same as the PodKeyList, but for binary arrays of fixed length
//
struct BinaryKeyList : BaseKeyList {
  enum {
    // A flag whether this KeyList has sequential data
    kHasSequentialData = 1,

    // A flag whether this KeyList supports the scan() call
    kSupportsBlockScans = 1,
  };

  // Constructor
  BinaryKeyList(LocalDb *db, PBtreeNode *node)
    : BaseKeyList(db, node), _data(0), _fixed_key_size(db->config.key_size) {
    assert(_fixed_key_size != 0);
  }

  // Creates a new KeyList starting at |ptr|, total size is
  // |range_size| (in bytes)
  void create(uint8_t *ptr, size_t range_size_) {
    _data = ptr;
    range_size = (uint32_t)range_size_;
  }

  // Opens an existing KeyList starting at |ptr|
  void open(uint8_t *ptr, size_t range_size_, size_t) {
    _data = ptr;
    range_size = (uint32_t)range_size_;
  }

  // Calculates the required size for this range
  size_t required_range_size(size_t node_count) const {
    return node_count * _fixed_key_size;
  }

  // Returns the actual key size including overhead
  size_t full_key_size(const ups_key_t *key = 0) const {
    return _fixed_key_size;
  }

  // Copies a key into |dest|
  void key(Context *, int slot, ByteArray *arena, ups_key_t *dest,
                  bool deep_copy = true) const {
    dest->size = (uint16_t)_fixed_key_size;
    if (likely(deep_copy == false)) {
      dest->data = &_data[slot * _fixed_key_size];
      return;
    }

    // allocate memory (if required)
    if (NOTSET(dest->flags, UPS_KEY_USER_ALLOC)) {
      arena->resize(dest->size);
      dest->data = arena->data();
    }

    ::memcpy(dest->data, &_data[slot * _fixed_key_size], _fixed_key_size);
  }

  // Iterates all keys, calls the |visitor| on each
  ScanResult scan(ByteArray *arena, size_t node_count, uint32_t start) {
    return std::make_pair(&_data[_fixed_key_size * start], node_count - start);
  }

  // Erases a whole slot by shifting all larger keys to the "left"
  void erase(Context *context, size_t node_count, int slot) {
    if (slot < (int)node_count - 1)
      ::memmove(&_data[slot * _fixed_key_size],
                      &_data[(slot + 1) * _fixed_key_size],
                      _fixed_key_size * (node_count - slot - 1));
  }

  // Inserts a key
  template<typename Cmp>
  PBtreeNode::InsertResult insert(Context *, size_t node_count,
                  const ups_key_t *key, uint32_t flags, Cmp &, int slot) {
    if (node_count > (size_t)slot)
      ::memmove(&_data[(slot + 1) * _fixed_key_size],
                      &_data[slot * _fixed_key_size],
                      _fixed_key_size * (node_count - slot));
    assert(key->size == _fixed_key_size);
    ::memcpy(&_data[slot * _fixed_key_size], key->data, _fixed_key_size);
    return PBtreeNode::InsertResult(0, slot);
  }

  // Returns true if the |key| no longer fits into the node
  bool requires_split(size_t node_count, const ups_key_t *key) const {
    return (node_count + 1) * _fixed_key_size >= range_size;
  }

  // Copies |count| key from this[sstart] to dest[dstart]
  void copy_to(int sstart, size_t node_count, BinaryKeyList &dest,
                  size_t other_count, int dstart) {
    ::memcpy(&dest._data[dstart * _fixed_key_size],
                    &_data[sstart * _fixed_key_size],
                    _fixed_key_size * (node_count - sstart));
  }

  // Change the capacity; for PAX layouts this just means copying the
  // data from one place to the other
  void change_range_size(size_t node_count, uint8_t *new_data_ptr,
          size_t new_range_size, size_t capacity_hint) {
    ::memmove(new_data_ptr, _data, node_count * _fixed_key_size);
    _data = new_data_ptr;
    range_size = new_range_size;
  }

  // Fills the btree_metrics structure
  void fill_metrics(btree_metrics_t *metrics, size_t node_count) {
    BaseKeyList::fill_metrics(metrics, node_count);
    BtreeStatistics::update_min_max_avg(&metrics->keylist_unused,
            range_size - (node_count * _fixed_key_size));
  }

  // Prints a slot to |out| (for debugging)
  void print(Context *context, int slot, std::stringstream &out) const {
    for (size_t i = 0; i < _fixed_key_size; i++)
      out << (char)_data[slot * _fixed_key_size + i];
  }

  // Returns the key size
  size_t key_size(int) const {
    return _fixed_key_size;
  }

  // Returns the pointer to a key's data
  uint8_t *key_data(int slot) const {
    return &_data[slot * _fixed_key_size];
  }

  // Pointer to the actual key data
  uint8_t *_data;

  // The size of a single key
  size_t _fixed_key_size;
};

} // namespace upscaledb

#endif // UPS_BTREE_KEYS_BINARY_H
