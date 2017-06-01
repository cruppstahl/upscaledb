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

#ifndef UPS_BTREE_KEYS_VARBYTE_H
#define UPS_BTREE_KEYS_VARBYTE_H

#include <sstream>
#include <iostream>

#include "3rdparty/libvbyte/vbyte.h"

#include "0root/root.h"

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

// This structure is an "index" entry which describes the location
// of a variable-length block
#include "1base/packstart.h"
UPS_PACK_0 struct UPS_PACK_1 VarbyteIndex : IndexBase {
  enum {
    // Initial size of a new block
    kInitialBlockSize = 16,

    // Maximum keys per block (9 bits)
    kMaxKeysPerBlock = 256 + 1,
  };

  // initialize this block index
  void initialize(uint32_t offset, uint8_t *block_data, size_t block_size) {
    IndexBase::initialize(offset, block_data, block_size);
    _block_size = block_size;
    _used_size = 0;
    _key_count = 0;
  }

  // returns the used size of the block
  uint32_t used_size() const {
    return _used_size;
  }

  // sets the used size of the block
  void set_used_size(uint32_t size) {
    _used_size = size;
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
  void copy_to(const uint8_t *block_data, VarbyteIndex *dest,
                  uint8_t *dest_data) {
    dest->set_value(value());
    dest->set_key_count(key_count());
    dest->set_used_size(used_size());
    dest->set_highest(highest());
    ::memcpy(dest_data, block_data, block_size());
  }

  // the total size of this block
  unsigned int _block_size : 11;

  // used size of this block
  unsigned int _used_size : 11;

  // the number of keys in this block; max 511 (kMaxKeysPerBlock)
  unsigned int _key_count : 9;
} UPS_PACK_2;
#include "1base/packstop.h"

struct VarbyteCodecImpl : BlockCodecBase<VarbyteIndex> {
  enum {
    kHasCompressApi = 1,
    kHasFindLowerBoundApi = 1,
    kHasDelApi = 1,
    kHasInsertApi = 1,
    kHasAppendApi = 1,
    kHasSelectApi = 1,
  };

  static uint32_t *uncompress_block(VarbyteIndex *index,
                  const uint32_t *block_data, uint32_t *out) {
    const uint8_t *in = (const uint8_t *)block_data;
    uint32_t previous = index->value();
    vbyte_uncompress_sorted32(in, out, previous, index->key_count() - 1);
    return out;
  }

  static uint32_t compress_block(VarbyteIndex *index, const uint32_t *in,
                  uint32_t *out32) {
    uint8_t *out = (uint8_t *)out32;
    uint32_t previous = index->value();
    return vbyte_compress_sorted32(in, out, previous, index->key_count() - 1);
  }

  static int find_lower_bound(VarbyteIndex *index, const uint32_t *block_data,
                  uint32_t key, uint32_t *result) {
    const uint8_t *in = (const uint8_t *)block_data;
    return vbyte_search_lower_bound_sorted32(in, index->key_count() - 1,
                    key, index->value(), result);
  }

  static bool append(VarbyteIndex *index, uint32_t *block_data32,
                  uint32_t key, int *pslot) {
    uint8_t *end = (uint8_t *)block_data32 + index->used_size();
    size_t space = vbyte_append_sorted32(end, index->highest(), key);

    index->set_key_count(index->key_count() + 1);
    index->set_used_size(index->used_size() + space);
    *pslot += index->key_count() - 1;
    return true;
  }

  static bool insert(VarbyteIndex *index, uint32_t *block_data32,
                  uint32_t key, int *pslot) {
    uint32_t prev = index->value();

    // swap |key| and |index->value|, then replace the first key with its delta
    if (unlikely(key < prev)) {
      uint32_t delta = index->value() - key;
      index->set_value(key);

      int required_space = calculate_delta_size(delta);
      uint8_t *p = (uint8_t *)block_data32;

      if (likely(index->used_size() > 0))
        ::memmove(p + required_space, p, index->used_size());
      write_int(p, delta);

      index->set_key_count(index->key_count() + 1);
      index->set_used_size(index->used_size() + required_space);
      *pslot += 1;
      return true;
    }

    uint8_t *block_data = (uint8_t *)block_data32;

    // fast-forward to the position of the new key
    uint8_t *p = fast_forward_to_key(index, block_data, key, &prev, pslot);
    // make sure that we don't have a duplicate key
    if (unlikely(key == prev))
      return false;

    // reached the end of the block? then append the new key
    if (unlikely(*pslot == (int)index->key_count())) {
      key -= prev;
      int size = write_int(p, key);
      index->set_used_size(index->used_size() + size);
      index->set_key_count(index->key_count() + 1);
      return true;
    }

    // otherwise read the next key at |position + 1|, because
    // its delta will change when the new key is inserted
    uint32_t next_key;
    uint8_t *next_p = p + read_int(p, &next_key);
    next_key += prev;

    if (unlikely(next_key == key)) {
      *pslot += 1;
      return false;
    }

    // how much additional space is required to store the delta of the
    // new key *and* the updated delta of the next key?
    int required_space = calculate_delta_size(key - prev)
                          + calculate_delta_size(next_key - key)
                          // minus the space that next_key currently occupies
                          - (int)(next_p - p);

    // create a gap large enough for the two deltas
    ::memmove(p + required_space, p, index->used_size() - (p - block_data));

    // now insert the new key
    p += write_int(p, key - prev);
    // and the delta of the next key
    p += write_int(p, next_key - key);

    index->set_key_count(index->key_count() + 1);
    index->set_used_size(index->used_size() + required_space);

    *pslot += 1;
    return true;
  }

  template<typename GrowHandler>
  static void del(VarbyteIndex *index, uint32_t *block_data, int slot,
                  GrowHandler *unused) {
    assert(index->key_count() > 1);

    uint8_t *data = (uint8_t *)block_data;
    uint8_t *p = (uint8_t *)block_data;

    // delete the first key?
    if (slot == 0) {
      uint32_t second, first = index->value();
      uint8_t *start = p;
      p += read_int(p, &second);
      // replace the first key with the second key (uncompressed)
      index->set_value(first + second);

      // shift all remaining deltas to the left
      index->set_key_count(index->key_count() - 1);
      if (unlikely(index->key_count() == 1)) {
        index->set_used_size(0);
      }
      else {
        ::memmove(start, p, index->used_size());
        index->set_used_size(index->used_size() - (p - start));
      }

      // update the cached highest block value?
      if (unlikely(index->key_count() <= 1))
        index->set_highest(index->value());

      return;
    }

    // otherwise fast-forward to the slot of the key and remove it;
    // then update the delta of the next key
    uint32_t key = index->value();
    uint32_t delta;
    uint8_t *prev_p = p;
    for (int i = 1; i < slot; i++) {
      prev_p = p;
      p += read_int(p, &delta);
      key += delta;
    }

    if (unlikely(index->key_count() == 2)) {
      index->set_used_size(0);
      index->set_key_count(index->key_count() - 1);
      index->set_highest(index->value());
      return;
    }

    // cut off the last key in the block?
    if (slot == (int)index->key_count() - 1) {
      index->set_used_size(index->used_size()
              - ((data + index->used_size()) - p));
      index->set_key_count(index->key_count() - 1);
      index->set_highest(key);
      return;
    }

    // save the current key, it will be required later
    uint32_t prev_key = key;
    prev_p = p;

    // now skip the key which is deleted
    p += read_int(p, &delta);
    key += delta;

    // read the next delta, it has to be updated
    p += read_int(p, &delta);
    uint32_t next_key = key + delta;

    // |prev_p| points to the start of the deleted key. |p| points *behind*
    // |next_key|.
    prev_p += write_int(prev_p, next_key - prev_key);

    // now shift all remaining keys "to the left", append them to |prev_p|
    ::memmove(prev_p, p, (data + index->used_size()) - prev_p);

    index->set_used_size(index->used_size() - (p - prev_p));
    index->set_key_count(index->key_count() - 1);
  }

  // Returns a decompressed value
  static uint32_t select(VarbyteIndex *index, uint32_t *block_data,
                        int position_in_block) {
    const uint8_t *in = (const uint8_t *)block_data;
    return vbyte_select_sorted32(in, index->key_count() - 1,
                    index->value(), position_in_block);
  }

  static uint32_t estimate_required_size(VarbyteIndex *index,
                        uint8_t *block_data, uint32_t key) {
    return index->used_size() + calculate_delta_size(key - index->value());
  }

  // fast-forwards to the specified key in a block
  static uint8_t *fast_forward_to_key(VarbyteIndex *index, uint8_t *block_data,
                        uint32_t key, uint32_t *pprev, int *pslot) {
    *pprev = index->value();
    if (key < *pprev) {
      *pslot = 0;
      return block_data;
    }

    uint32_t delta;
    for (int i = 0; i < (int)index->key_count() - 1; i++) {
      uint8_t *next = block_data + read_int(block_data, &delta);
      if (*pprev + delta >= key) {
        *pslot = i;
        return block_data;
      }
      block_data = next;
      *pprev += delta;
    }

    *pslot = index->key_count();
    return block_data;
  }

  // this assumes that there is a value to be read
  static int read_int(const uint8_t *in, uint32_t *out) {
    *out = in[0] & 0x7F;
    if (likely(in[0] < 128))
      return 1;
    *out = ((in[1] & 0x7FU) << 7) | *out;
    if (likely(in[1] < 128))
      return 2;
    *out = ((in[2] & 0x7FU) << 14) | *out;
    if (likely(in[2] < 128))
      return 3;
    *out = ((in[3] & 0x7FU) << 21) | *out;
    if (likely(in[3] < 128))
      return 4;
    *out = ((in[4] & 0x7FU) << 28) | *out;
    return 5;
  }

  // returns the compressed size of |value|
  static int calculate_delta_size(uint32_t value) {
    if (likely(value < (1U << 7)))
      return 1;
    if (likely(value < (1U << 14)))
      return 2;
    if (likely(value < (1U << 21)))
      return 3;
    if (likely(value < (1U << 28)))
      return 4;
    return 5;
  }

  // writes |value| to |p|
  static int write_int(uint8_t *p, uint32_t value) {
    assert(value > 0);
    if (value < (1U << 7)) {
      *p = value & 0x7F;
      return 1;
    }
    if (value < (1U << 14)) {
      *p = static_cast<uint8_t>((value & 0x7F) | (1U << 7));
      ++p;
      *p = static_cast<uint8_t>(value >> 7);
      return 2;
    }
    if (value < (1U << 21)) {
      *p = static_cast<uint8_t>((value & 0x7F) | (1U << 7));
      ++p;
      *p = static_cast<uint8_t>(((value >> 7) & 0x7F) | (1U << 7));
      ++p;
      *p = static_cast<uint8_t>(value >> 14);
      return 3;
    }
    if (value < (1U << 28)) {
      *p = static_cast<uint8_t>((value & 0x7F) | (1U << 7));
      ++p;
      *p = static_cast<uint8_t>(((value >> 7) & 0x7F) | (1U << 7));
      ++p;
      *p = static_cast<uint8_t>(((value >> 14) & 0x7F) | (1U << 7));
      ++p;
      *p = static_cast<uint8_t>(value >> 21);
      return 4;
    }
    else {
      *p = static_cast<uint8_t>((value & 0x7F) | (1U << 7));
      ++p;
      *p = static_cast<uint8_t>(((value >> 7) & 0x7F) | (1U << 7));
      ++p;
      *p = static_cast<uint8_t>(((value >> 14) & 0x7F) | (1U << 7));
      ++p;
      *p = static_cast<uint8_t>(((value >> 21) & 0x7F) | (1U << 7));
      ++p;
      *p = static_cast<uint8_t>(value >> 28);
      return 5;
    }
  }
};

typedef Zint32Codec<VarbyteIndex, VarbyteCodecImpl> VarbyteCodec;

struct VarbyteKeyList : BlockKeyList<VarbyteCodec> {
  // Constructor
  VarbyteKeyList(LocalDb *db, PBtreeNode *node)
    : BlockKeyList<VarbyteCodec>(db, node) {
  }
};

} // namespace Zint32

} // namespace upscaledb

#endif // UPS_BTREE_KEYS_VARBYTE_H
