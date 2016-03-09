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
 */

#ifndef UPS_BTREE_KEYS_FOR_H
#define UPS_BTREE_KEYS_FOR_H

#include <sstream>
#include <iostream>

#include "0root/root.h"

#include "3rdparty/for/for.h"

// Always verify that a file of level N does not include headers > N!
#include "3btree/btree_zint32_block.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

//
// The template classes in this file are wrapped in a separate namespace
// to avoid naming clashes with other KeyLists
//
namespace Zint32 {

static inline uint32_t
for_select(const uint8_t *in, uint32_t index)
{
  /* load min and the bits */
  uint32_t base = *(uint32_t *)(in + 0);
  uint32_t bits = *(in + 4);
  in += 5;

  uint32_t b, start;
  const uint32_t *in32;

  assert(bits <= 32);

  if (bits == 32) {
    in32 = (uint32_t *)in;
    return base + in32[index];
  }

  if (index > 32) {
    b = index / 32;
    in += (b * 32 * bits) / 8;
    index %= 32;
  }

  if (index > 16) {
    b = index / 16;
    in += (b * 16 * bits) / 8;
    index %= 16;
  }

  if (index > 8) {
    b = index / 8;
    in += (b * 8 * bits) / 8;
    index %= 8;
  }

  start = index * bits;

  in += start / 8;
  start %= 8;

  /* |in| now points to the byte where the requested index is stored */
  /* |start| is the bit position where the compressed value starts */

  in32 = (uint32_t *)in;

  /* easy common case: the compressed value is not split between words */
  if (start + bits < 32) {
    uint32_t mask = (1 << bits) - 1;
    return base + ((*in32 >> start) & mask);
  }
  /* not so easy: restore value from two words */
  else {
    uint32_t mask1 = (1 << bits) - 1;
    uint32_t mask2 = (1 << (bits - (32 - start))) - 1;
    uint32_t v1 = (*(in32 + 0) >> start) & mask1;
    uint32_t v2 =  *(in32 + 1) & mask2;
    return base + ((v2 << (32 - start)) | v1);
  }
}

// This structure is an "index" entry which describes the location
// of a variable-length block
#include "1base/packstart.h"
UPS_PACK_0 struct UPS_PACK_1 ForIndex : public IndexBase
{
    enum {
      // Initial size of a new block
      kInitialBlockSize = 9 + 16,

      // Maximum keys per block
      kMaxKeysPerBlock = 256 + 1,
    };

    // initialize this block index
    void initialize(uint32_t offset, uint8_t *block_data, size_t block_size) {
      IndexBase::initialize(offset, block_data, block_size);
      block_size_ = block_size;
      used_size_ = 0;
      key_count_ = 0;

      // clear the metadata
      ::memset(block_data, 0, 2 * sizeof(uint32_t));
    }

    // returns the used size of the block
    uint32_t used_size() const {
      return used_size_;
    }

    // sets the used size of the block
    void set_used_size(uint32_t size) {
      used_size_ = size;
    }

    // returns the total block size
    uint32_t block_size() const {
      return block_size_;
    }

    // sets the total block size
    void set_block_size(uint32_t size) {
      block_size_ = size;
    }

    // returns the key count
    uint32_t key_count() const {
      return key_count_;
    }

    // sets the key count
    void set_key_count(uint32_t key_count) {
      key_count_ = key_count;
    }

    // copies this block to the |dest| block
    void copy_to(const uint8_t *block_data, ForIndex *dest,
                    uint8_t *dest_data) {
      dest->set_value(value());
      dest->set_key_count(key_count());
      dest->set_used_size(used_size());
      dest->set_highest(highest());
      ::memcpy(dest_data, block_data, block_size());
    }

  private:
    // the total size of this block
    unsigned int block_size_ : 11;

    // used size of this block
    unsigned int used_size_ : 11;

    // the number of keys in this block; max 511 (kMaxKeysPerBlock)
    unsigned int key_count_ : 9;
} UPS_PACK_2;
#include "1base/packstop.h"

struct ForCodecImpl : public BlockCodecBase<ForIndex>
{
  enum {
    kHasCompressApi = 1,
    kHasFindLowerBoundApi = 1,
    kHasSelectApi = 1,
    kHasAppendApi = 1,
  };

  static uint32_t *uncompress_block(ForIndex *index,
                  const uint32_t *block_data, uint32_t *out) {
    for_uncompress((const uint8_t *)block_data, out, index->key_count() - 1);
    return out;
  }

  static uint32_t compress_block(ForIndex *index, const uint32_t *in,
                  uint32_t *out) {
    assert(index->key_count() > 0);
    uint32_t count = index->key_count() - 1;
    uint32_t s = for_compress_sorted(in, (uint8_t *)out, count);
    index->set_used_size(s);
    return s;
  }

  static bool append(ForIndex *index, uint32_t *in32,
                  uint32_t key, int *pslot) {
    uint32_t s = for_append_sorted((uint8_t *)in32,
                        index->key_count() - 1, key);

    index->set_key_count(index->key_count() + 1);
    index->set_used_size(s);
    *pslot += index->key_count() - 1;
    return true;
  }

  static int find_lower_bound(ForIndex *index, const uint32_t *block_data,
                  uint32_t key, uint32_t *result) {
    if (likely(index->key_count() > 1)) {
      return (int)for_lower_bound_search((const uint8_t *)block_data,
                           index->key_count() - 1, key, result);
    }
    else {
      *result = key + 1;
      return 1;
    }
  }

  // Returns a decompressed value
  static uint32_t select(ForIndex *index, uint32_t *block_data,
                        int position_in_block) {
    return Zint32::for_select((const uint8_t *)block_data, position_in_block);
  }

  static uint32_t estimate_required_size(ForIndex *index, uint8_t *block_data,
                        uint32_t key) {
      uint32_t min = *(uint32_t *)block_data;
      uint32_t oldbits = *(block_data + 4);
      uint32_t newbits;
      if (key > min)
        newbits = bits(key - min);
      else
        newbits = oldbits + bits(min - key);
      if (newbits < oldbits)
        newbits = oldbits;
      if (newbits > 32)
        newbits = 32;
      uint32_t s = 5 + ((index->key_count() * newbits) + 7) / 8;
      return s + 4; // reserve a few bytes for the next key
    }

  /* returns the integer logarithm of v (bit width) */
  static uint32_t bits(const uint32_t v) {
#ifdef _MSC_VER
    unsigned long answer;
    if (v == 0)
      return 0;
    _BitScanReverse(&answer, v);
    return answer + 1;
#else
    return v == 0 ? 0 : 32 - __builtin_clz(v); /* assume GCC-like compiler if not microsoft */
#endif
  }

};

typedef Zint32Codec<ForIndex, ForCodecImpl> ForCodec;

struct ForKeyList : public BlockKeyList<ForCodec>
{
  // Constructor
  ForKeyList(LocalDatabase *db)
    : BlockKeyList<ForCodec>(db) {
  }
};

} // namespace Zint32

} // namespace upscaledb

#endif /* UPS_BTREE_KEYS_FOR_H */
