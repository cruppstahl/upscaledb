/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
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
// The template classes in this file are wrapped in a separate namespace
// to avoid naming clashes with btree_impl_default.h
//
namespace PaxLayout {

//
// Same as the PodKeyList, but for binary arrays of fixed length
//
struct BinaryKeyList : public BaseKeyList
{
  enum {
    // A flag whether this KeyList has sequential data
    kHasSequentialData = 1,

    // A flag whether this KeyList supports the scan() call
    kSupportsBlockScans = 1,
  };

  // Constructor
  BinaryKeyList(LocalDatabase *db)
      : data_(0), key_size_(db->config().key_size) {
    assert(key_size_ != 0);
  }

  // Creates a new KeyList starting at |ptr|, total size is
  // |range_size| (in bytes)
  void create(uint8_t *ptr, size_t range_size) {
    data_ = ptr;
    range_size_ = (uint32_t)range_size;
  }

  // Opens an existing KeyList starting at |ptr|
  void open(uint8_t *ptr, size_t range_size, size_t) {
    data_ = ptr;
    range_size_ = (uint32_t)range_size;
  }

  // Calculates the required size for this range
  size_t required_range_size(size_t node_count) const {
    return node_count * key_size_;
  }

  // Returns the actual key size including overhead
  size_t full_key_size(const ups_key_t *key = 0) const {
    return key_size_;
  }

  // Copies a key into |dest|
  void key(Context *, int slot, ByteArray *arena, ups_key_t *dest,
                  bool deep_copy = true) const {
    dest->size = (uint16_t)key_size_;
    if (likely(deep_copy == false)) {
      dest->data = &data_[slot * key_size_];
      return;
    }

    // allocate memory (if required)
    if (notset(dest->flags, UPS_KEY_USER_ALLOC)) {
      arena->resize(dest->size);
      dest->data = arena->data();
    }

    ::memcpy(dest->data, &data_[slot * key_size_], key_size_);
  }

  // Iterates all keys, calls the |visitor| on each
  ScanResult scan(ByteArray *arena, size_t node_count, uint32_t start) {
    return std::make_pair(&data_[key_size_ * start], node_count - start);
  }

  // Erases a whole slot by shifting all larger keys to the "left"
  void erase(Context *context, size_t node_count, int slot) {
    if (slot < (int)node_count - 1)
      ::memmove(&data_[slot * key_size_], &data_[(slot + 1) * key_size_],
                    key_size_ * (node_count - slot - 1));
  }

  // Inserts a key
  template<typename Cmp>
  PBtreeNode::InsertResult insert(Context *, size_t node_count,
                  const ups_key_t *key, uint32_t flags, Cmp &, int slot) {
    if (node_count > (size_t)slot)
      ::memmove(&data_[(slot + 1) * key_size_], &data_[slot * key_size_],
                    key_size_ * (node_count - slot));
    assert(key->size == key_size_);
    ::memcpy(&data_[slot * key_size_], key->data, key_size_);
    return PBtreeNode::InsertResult(0, slot);
  }

  // Returns true if the |key| no longer fits into the node
  bool requires_split(size_t node_count, const ups_key_t *key) const {
    return (node_count + 1) * key_size_ >= range_size_;
  }

  // Copies |count| key from this[sstart] to dest[dstart]
  void copy_to(int sstart, size_t node_count, BinaryKeyList &dest,
                  size_t other_count, int dstart) {
    ::memcpy(&dest.data_[dstart * key_size_], &data_[sstart * key_size_],
                    key_size_ * (node_count - sstart));
  }

  // Change the capacity; for PAX layouts this just means copying the
  // data from one place to the other
  void change_range_size(size_t node_count, uint8_t *new_data_ptr,
          size_t new_range_size, size_t capacity_hint) {
    ::memmove(new_data_ptr, data_, node_count * key_size_);
    data_ = new_data_ptr;
    range_size_ = new_range_size;
  }

  // Fills the btree_metrics structure
  void fill_metrics(btree_metrics_t *metrics, size_t node_count) {
    BaseKeyList::fill_metrics(metrics, node_count);
    BtreeStatistics::update_min_max_avg(&metrics->keylist_unused,
            range_size_ - (node_count * key_size_));
  }

  // Prints a slot to |out| (for debugging)
  void print(Context *context, int slot, std::stringstream &out) const {
    for (size_t i = 0; i < key_size_; i++)
      out << (char)data_[slot * key_size_ + i];
  }

  // Returns the key size
  size_t key_size(int) const {
    return key_size_;
  }

  // Returns the pointer to a key's data
  uint8_t *key_data(int slot) const {
    return &data_[slot * key_size_];
  }

  // Pointer to the actual key data
  uint8_t *data_;

  // The size of a single key
  size_t key_size_;
};

} // namespace PaxLayout

} // namespace upscaledb

#endif /* UPS_BTREE_KEYS_BINARY_H */
