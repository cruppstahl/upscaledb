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
 * Compressed 32bit integer keys
 *
 * @exception_safe: strong
 * @thread_safe: no
 */

#ifndef UPS_BTREE_KEYS_SIMDFOR_H
#define UPS_BTREE_KEYS_SIMDFOR_H

#include <sstream>
#include <iostream>
#include <algorithm>

#include "0root/root.h"

//#include "3rdparty/for/include/compression.h"

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
align16(uint32_t v) {
  return (((v + 15) / 16) * 16);
}

// This structure is an "index" entry which describes the location
// of a variable-length block
#include "1base/packstart.h"
UPS_PACK_0 class UPS_PACK_1 SimdForIndex : public IndexBase {
  public:
    enum {
      // Initial size of a new block
      kInitialBlockSize = 8 + 16,

      // Maximum keys per block
      kMaxKeysPerBlock = 256 + 1,
    };

    // initialize this block index
    void initialize(uint32_t offset, uint8_t *block_data, size_t block_size) {
      IndexBase::initialize(offset, block_data, block_size);
      m_block_size = block_size;
      m_used_size = 0;
      m_key_count = 0;

      uint32_t *block = (uint32_t *)block_data;
      block[0] = block[1] = 0;
    }

    // returns the used size of the block
    uint32_t used_size() const {
      return (m_used_size);
    }

    // sets the used size of the block
    void set_used_size(uint32_t size) {
      m_used_size = size;
      ups_assert(m_used_size == size);
    }

    // returns the total block size
    uint32_t block_size() const {
      return (m_block_size);
    }

    // sets the total block size
    void set_block_size(uint32_t size) {
      m_block_size = size;
    }

    // returns the key count
    uint32_t key_count() const {
      return (m_key_count);
    }

    // sets the key count
    void set_key_count(uint32_t key_count) {
      m_key_count = key_count;
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

  private:
    // the total size of this block; max 1023 bytes
    unsigned int m_block_size : 11;

    // used size of this block
    unsigned int m_used_size : 11;

    // the number of keys in this block; max 511 (kMaxKeysPerBlock)
    unsigned int m_key_count : 9;
} UPS_PACK_2;
#include "1base/packstop.h"

struct SimdForCodecImpl : public BlockCodecBase<SimdForIndex>
{
  enum {
    kHasCompressApi = 1,
    kHasFindLowerBoundApi = 1,
    kHasSelectApi = 1,
    kHasAppendApi = 1,
  };

  static uint32_t *uncompress_block(SimdForIndex *index,
                  const uint32_t *in, uint32_t *out) {
    SimdFor::simd_uncompress_length(in, out, index->key_count() - 1);
    return (out);
  }

  static uint32_t compress_block(SimdForIndex *index, const uint32_t *in,
                  uint32_t *out) {
    ups_assert(index->key_count() > 0);
    uint32_t length = index->key_count() - 1;
    uint32_t *p = SimdFor::simd_compress_length_sorted(in, length, out);
    index->set_used_size((p - out) * 4);
    return (index->used_size());
  }

  static int find_lower_bound(SimdForIndex *index, const uint32_t *in,
                  uint32_t key, uint32_t *result) {
    if (index->key_count() > 1) {
      return ((int)SimdFor::simd_findLowerBound(in, index->key_count() - 1,
                        key, result));
    }
    else {
      *result = key + 1;
      return (1);
    }
  }

  // Returns a decompressed value
  static uint32_t select(SimdForIndex *index, uint32_t *block_data,
                        int position_in_block) {
    return (SimdFor::simd_select_length(block_data, (size_t)position_in_block));
  }

  static bool append(SimdForIndex *index, uint32_t *in32,
                        uint32_t key, int *pslot) {
    // block is empty?
    if (index->key_count() == 1) {
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
      ups_assert(key > M);
      ups_assert(bits(key - m) <= b);
      simdfastset((__m128i *)(in32 + 2), b, b == 32 ? key : key - m,
                index->key_count() - 1);
      in32[1] = key;
    }

    index->set_key_count(index->key_count() + 1);
    *pslot += index->key_count() - 1;
    return (true);
  }

  // Estimates the required size after inserting |key| into the block
  static uint32_t estimate_required_size(SimdForIndex *index,
                    uint8_t *block_data, uint32_t key) {
    uint32_t b;
    uint32_t length = index->key_count() - 1;

    if (length > 0) {
      uint32_t min = *(uint32_t *)(block_data + 0);
      uint32_t max = *(uint32_t *)(block_data + 4);
      ups_assert(min <= max);
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
    return (s);
  }
};

typedef Zint32Codec<SimdForIndex, SimdForCodecImpl> SimdForCodec;

class SimdForKeyList : public BlockKeyList<SimdForCodec>
{
  public:
    // Constructor
    SimdForKeyList(LocalDatabase *db)
      : BlockKeyList<SimdForCodec>(db) {
    }

  protected:
    // Implementation for insert()
    virtual PBtreeNode::InsertResult insert_impl(size_t node_count,
                    uint32_t key, uint32_t flags) {
      m_block_cache.is_active = false;

      int slot = 0;

      // perform a linear search through the index and get the block
      // which will receive the new key
      Index *index = find_index(key, &slot);

      // first key in an empty block? then don't store a delta
      if (index->key_count() == 0) {
        index->set_key_count(1);
        index->set_value(key);
        index->set_highest(key);
        return (PBtreeNode::InsertResult(0, slot));
      }

      // fail if the key already exists
      if (key == index->value() || key == index->highest())
        return (PBtreeNode::InsertResult(UPS_DUPLICATE_KEY, slot));

      uint32_t new_data[Index::kMaxKeysPerBlock];
      uint32_t datap[Index::kMaxKeysPerBlock];
      uint32_t estimated_size = 0;

      // A split is required if the block maxxed out the keys or if
      // (used_size >= block_size and block_size >= max_size)
      bool requires_split = index->key_count() + 1
                                >= SimdForIndex::kMaxKeysPerBlock;

      // split the block if it is full
      if (requires_split) {
        int block = index - get_block_index(0);

        // if the new key is prepended then also prepend the new block
        if (key < index->value()) {
          Index *new_index = add_block(block + 1,
                                SimdForIndex::kInitialBlockSize);
          new_index->set_key_count(1);
          new_index->set_value(key);
          new_index->set_highest(key);

          // swap the indices, done
          std::swap(*index, *new_index);

          ups_assert(check_integrity(0, node_count + 1));
          return (PBtreeNode::InsertResult(0, slot < 0 ? 0 : slot));
        }

        // if the new key is appended then also append the new block
        if (key > index->highest()) {
          Index *new_index = add_block(block + 1,
                                SimdForIndex::kInitialBlockSize);
          new_index->set_key_count(1);
          new_index->set_value(key);
          new_index->set_highest(key);

          ups_assert(check_integrity(0, node_count + 1));
          return (PBtreeNode::InsertResult(0, slot + index->key_count()));
        }

        // otherwise split the block in the middle and move half of the keys
        // to the new block.
        //
        // The pivot position is aligned to 4.
        uint32_t *data = uncompress_block(index, datap);
        uint32_t to_copy = (index->key_count() / 2) & ~0x03;
        ups_assert(to_copy > 0);
        uint32_t new_key_count = index->key_count() - to_copy - 1;
        uint32_t new_value = data[to_copy];

        // once more check if the key already exists
        if (new_value == key)
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
          ups_assert(new_index->used_size() <= new_index->block_size());
        }

        // the block was modified and needs to be compressed again, even if
        // the actual insert operation fails (i.e. b/c the key already exists)
        index->set_used_size(compress_block(index, data));
        ups_assert(index->used_size() <= index->block_size());

        // fall through...
      }
      // or grow the block if more space is required
      else {
        estimated_size = SimdForCodecImpl::estimate_required_size(index,
                                get_block_data(index), key);
        if (index->block_size() < estimated_size)
          grow_block_size(index, estimated_size);
      }

      uint32_t *in32 = (uint32_t *)get_block_data(index);
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
        if (index->key_count() > 1) {
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

      ups_assert(check_integrity(0, node_count + 1));
      return (PBtreeNode::InsertResult(0, slot));
    }
};

} // namespace Zint32

} // namespace upscaledb

#endif /* UPS_BTREE_KEYS_SIMDFOR_H */
