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
 * Compressed 32bit integer keys
 */

#ifndef UPS_BTREE_KEYS_SIMDFOR_H
#define UPS_BTREE_KEYS_SIMDFOR_H

#ifdef HAVE_SSE2

#include <sstream>
#include <iostream>
#include <algorithm>

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "3btree/btree_zint32_block.h"
#include "3btree/btree_zint32_for.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace SimdFor {

extern uint32_t *
simd_compress_length_sorted(const uint32_t *in, uint32_t length,
                uint32_t *out);

extern const uint32_t *
simd_uncompress_length(const uint32_t *in, uint32_t *out, uint32_t nvalue);

extern size_t
simd_findLowerBound(const uint32_t *in, const size_t length, uint32_t key,
                uint32_t *presult);

extern uint32_t
simd_select_length(const uint32_t *in, size_t index);

}

namespace upscaledb {

//
// The template classes in this file are wrapped in a separate namespace
// to avoid naming clashes with other KeyLists
//
namespace Zint32 {

static inline uint32_t
align16(uint32_t v)
{
  return ((v + 15) / 16) * 16;
}

// This structure is an "index" entry which describes the location
// of a variable-length block
#include "1base/packstart.h"
UPS_PACK_0 struct UPS_PACK_1 SimdForIndex : IndexBase {
  enum {
    // Initial size of a new block
    kInitialBlockSize = 8 + 16,

    // Maximum keys per block
    kMaxKeysPerBlock = 256 + 1,
  };

  // initialize this block index
  void initialize(uint32_t offset, uint8_t *block_data, size_t block_size) {
    IndexBase::initialize(offset, block_data, block_size);
    _block_size = block_size;
    _used_size = 0;
    _key_count = 0;

    uint32_t *block = (uint32_t *)block_data;
    block[0] = block[1] = 0;
  }

  // returns the used size of the block
  uint32_t used_size() const {
    return _used_size;
  }

  // sets the used size of the block
  void set_used_size(uint32_t size) {
    _used_size = size;
    assert(_used_size == size);
  }

  // returns the total block size
  uint32_t block_size() const {
    return _block_size;
  }

  // sets the total block size
  void set_block_size(uint32_t size) {
    _block_size = size;
  }

  // returns the key count
  uint32_t key_count() const {
    return _key_count;
  }

  // sets the key count
  void set_key_count(uint32_t key_count) {
    _key_count = key_count;
  }

  // copies this block to the |dest| block
  void copy_to(const uint8_t *block_data, SimdForIndex *dest,
                  uint8_t *dest_data) {
    dest->set_value(value());
    dest->set_key_count(key_count());
    dest->set_used_size(used_size());
    dest->set_highest(highest());
    ::memcpy(dest_data, block_data, block_size());
  }

  // the total size of this block; max 1023 bytes
  unsigned int _block_size : 11;

  // used size of this block
  unsigned int _used_size : 11;

  // the number of keys in this block; max 511 (kMaxKeysPerBlock)
  unsigned int _key_count : 9;
} UPS_PACK_2;
#include "1base/packstop.h"

struct SimdForCodecImpl : BlockCodecBase<SimdForIndex> {
  enum {
    kHasCompressApi = 1,
    kHasFindLowerBoundApi = 1,
    kHasSelectApi = 1,
    kHasAppendApi = 1,
  };

  static uint32_t *uncompress_block(SimdForIndex *index,
                  const uint32_t *in, uint32_t *out) {
    SimdFor::simd_uncompress_length(in, out, index->key_count() - 1);
    return out;
  }

  static uint32_t compress_block(SimdForIndex *index, const uint32_t *in,
                  uint32_t *out) {
    assert(index->key_count() > 0);
    uint32_t length = index->key_count() - 1;
    uint32_t *p = SimdFor::simd_compress_length_sorted(in, length, out);
    index->set_used_size((p - out) * 4);
    return index->used_size();
  }

  static int find_lower_bound(SimdForIndex *index, const uint32_t *in,
                  uint32_t key, uint32_t *result) {
    if (likely(index->key_count() > 1))
      return (int)SimdFor::simd_findLowerBound(in, index->key_count() - 1,
                        key, result);
    *result = key + 1;
    return 1;
  }

  // Returns a decompressed value
  static uint32_t select(SimdForIndex *index, uint32_t *block_data,
                        int position_in_block) {
    return SimdFor::simd_select_length(block_data, (size_t)position_in_block);
  }

  static bool append(SimdForIndex *index, uint32_t *in32,
                        uint32_t key, int *pslot) {
    // block is empty?
    if (unlikely(index->key_count() == 1)) {
      uint32_t b = bits(static_cast<uint32_t>(key - index->value()));
      simdfastset((__m128i *)(in32 + 2), b,
                b == 32 ? key : key - index->value(),
                0);
      in32[0] = key;
      in32[1] = key;
    }
    else {
      uint32_t m = in32[0];
      uint32_t M = in32[1];
      uint32_t b = bits(static_cast<uint32_t>(M - m));
      assert(key > M);
      assert(bits(key - m) <= b);
      simdfastset((__m128i *)(in32 + 2), b, b == 32 ? key : key - m,
                index->key_count() - 1);
      in32[1] = key;
    }

    index->set_key_count(index->key_count() + 1);
    *pslot += index->key_count() - 1;
    return true;
  }

  // Estimates the required size after inserting |key| into the block
  static uint32_t estimate_required_size(SimdForIndex *index,
                    uint8_t *block_data, uint32_t key) {
    uint32_t b;
    uint32_t length = index->key_count() - 1;

    if (likely(length > 0)) {
      uint32_t min = *(uint32_t *)(block_data + 0);
      uint32_t max = *(uint32_t *)(block_data + 4);
      assert(min <= max);
      if (key < min)
        min = key;
      else if (key > max)
        max = key;
      b = bits(max - min);
    }
    else {
      b = bits(key);
    }

    length++; // +1 for the new key

    uint32_t s = 8;
    while (length > 128) {
      s += b * 16;
      length -= 128;
    }
    if (length > 4) {
      s += align16(((((length / 4) * 4) * b) + 7) / 8);
      length %= 4;
    }
    if (length)
      s += align16(((length * b) + 7) / 8);
    if (length * b > 32)
      s += 16;
    return s;
  }
};

typedef Zint32Codec<SimdForIndex, SimdForCodecImpl> SimdForCodec;

struct SimdForKeyList : BlockKeyList<SimdForCodec> {
  // Constructor
  SimdForKeyList(LocalDb *db, PBtreeNode *node)
    : BlockKeyList<SimdForCodec>(db, node) {
  }

  // Implementation for insert()
  virtual PBtreeNode::InsertResult insert_impl(size_t node_count,
                  uint32_t key, uint32_t flags) {
    block_cache.is_active = false;

    int slot = 0;

    // perform a linear search through the index and get the block
    // which will receive the new key
    Index *index = find_index(key, &slot);

    // first key in an empty block? then don't store a delta
    if (unlikely(index->key_count() == 0)) {
      index->set_key_count(1);
      index->set_value(key);
      index->set_highest(key);
      return PBtreeNode::InsertResult(0, slot);
    }

    // fail if the key already exists
    if (unlikely(key == index->value() || key == index->highest()))
      return PBtreeNode::InsertResult(UPS_DUPLICATE_KEY, slot);

    uint32_t new_data[Index::kMaxKeysPerBlock];
    uint32_t datap[Index::kMaxKeysPerBlock];
    uint32_t estimated_size = 0;

    // A split is required if the block maxxed out the keys or if
    // (used_size >= block_size and block_size >= max_size)
    bool requires_split = index->key_count() + 1
                              >= SimdForIndex::kMaxKeysPerBlock;

    // split the block if it is full
    if (unlikely(requires_split)) {
      int block = index - block_index(0);

      // if the new key is prepended then also prepend the new block
      if (key < index->value()) {
        Index *new_index = add_block(block + 1,
                              SimdForIndex::kInitialBlockSize);
        new_index->set_key_count(1);
        new_index->set_value(key);
        new_index->set_highest(key);

        // swap the indices, done
        std::swap(*index, *new_index);

        assert(check_integrity(0, node_count + 1));
        return PBtreeNode::InsertResult(0, slot < 0 ? 0 : slot);
      }

      // if the new key is appended then also append the new block
      if (key > index->highest()) {
        Index *new_index = add_block(block + 1,
                              SimdForIndex::kInitialBlockSize);
        new_index->set_key_count(1);
        new_index->set_value(key);
        new_index->set_highest(key);

        assert(check_integrity(0, node_count + 1));
        return PBtreeNode::InsertResult(0, slot + index->key_count());
      }

      // otherwise split the block in the middle and move half of the keys
      // to the new block.
      //
      // The pivot position is aligned to 4.
      uint32_t *data = uncompress_block(index, datap);
      uint32_t to_copy = (index->key_count() / 2) & ~0x03;
      assert(to_copy > 0);
      uint32_t new_key_count = index->key_count() - to_copy - 1;
      uint32_t new_value = data[to_copy];

      // once more check if the key already exists
      if (unlikely(new_value == key))
        return (PBtreeNode::InsertResult(UPS_DUPLICATE_KEY, slot + to_copy));

      to_copy++;
      ::memmove(&new_data[0], &data[to_copy],
                  sizeof(int32_t) * (index->key_count() - to_copy));

      // Now create a new block. This can throw, but so far we have not
      // modified existing data.
      Index *new_index = add_block(block + 1, index->block_size());
      new_index->set_value(new_value);
      new_index->set_highest(index->highest());
      new_index->set_key_count(new_key_count);

      // Adjust the size of the old block
      index->set_key_count(index->key_count() - new_key_count);
      index->set_highest(data[to_copy - 2]);

      // Now check if the new key will be inserted in the old or the new block
      if (key >= new_index->value()) {
        compress_block(index, data);
        slot += index->key_count();

        // continue with the new block
        index = new_index;
        data = new_data;
      }
      else {
        new_index->set_used_size(compress_block(new_index, new_data));
        assert(new_index->used_size() <= new_index->block_size());
      }

      // the block was modified and needs to be compressed again, even if
      // the actual insert operation fails (i.e. b/c the key already exists)
      index->set_used_size(compress_block(index, data));
      assert(index->used_size() <= index->block_size());

      // fall through...
    }
    // or grow the block if more space is required
    else {
      estimated_size = SimdForCodecImpl::estimate_required_size(index,
                              block_data(index), key);
      if (index->block_size() < estimated_size)
        grow_block_size(index, estimated_size);
    }

    uint32_t *in32 = (uint32_t *)block_data(index);
    uint32_t m = in32[0];
    uint32_t M = in32[1];
    uint32_t b = bits(static_cast<uint32_t>(M - m));

    // now append or insert the key, but only append if the sequence
    // does not have to be re-encoded
    if (key > index->highest() && bits(key - m) <= b) {
      SimdForCodecImpl::append(index, in32, key, &slot);

      if (!estimated_size)
        estimated_size = SimdForCodecImpl::estimate_required_size(index,
                                              (uint8_t *)in32, key);
      index->set_used_size(estimated_size);
    }
    else {
      uint32_t *data = uncompress_block(index, datap);

      // swap |key| and |index->value|
      if (key < index->value()) {
        uint32_t tmp = index->value();
        index->set_value(key);
        key = tmp;
      }

      // locate the position of the new key
      uint32_t *it = data;
      if (likely(index->key_count() > 1)) {
        uint32_t *end = &data[index->key_count() - 1];
        it = std::lower_bound(&data[0], end, key);

        // if the new key already exists then throw an exception
        if (it < end && *it == key)
          return (PBtreeNode::InsertResult(UPS_DUPLICATE_KEY,
                              slot + (it - &data[0]) + 1));

        // insert the new key
        if (it < end)
          ::memmove(it + 1, it, (end - it) * sizeof(uint32_t));
      }

      *it = key;
      slot += it - &data[0] + 1;

      index->set_key_count(index->key_count() + 1);

      // then compress and store the block
      compress_block(index, data);
    }

    if (key > index->highest())
      index->set_highest(key);

    assert(check_integrity(0, node_count + 1));
    return PBtreeNode::InsertResult(0, slot);
  }
};

} // namespace Zint32

} // namespace upscaledb

#endif // HAVE_SSE2

#endif // UPS_BTREE_KEYS_SIMDFOR_H
