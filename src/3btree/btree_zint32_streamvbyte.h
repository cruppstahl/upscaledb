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

#ifndef UPS_BTREE_KEYS_STREAMVBYTE_H
#define UPS_BTREE_KEYS_STREAMVBYTE_H

#ifdef HAVE_SSE2

#include <sstream>
#include <iostream>
#include <algorithm>

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "3btree/btree_zint32_block.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

extern uint8_t *svb_encode_scalar_d1_init(const uint32_t *in,
                uint8_t * /*restrict*/ keyPtr, uint8_t * /*restrict */dataPtr,
                uint32_t count, uint32_t prev);
extern uint8_t *svb_decode_scalar_d1_init(uint32_t *out,
                const uint8_t * keyPtr, uint8_t * dataPtr, uint32_t count,
                uint32_t prev);
extern uint8_t *svb_decode_avx_d1_init(uint32_t *out,
                uint8_t * keyPtr, uint8_t * dataPtr, uint64_t count,
                uint32_t prev);
extern int      svb_find_avx_d1_init(uint8_t * /*restrict*/ keyPtr,
                uint8_t * /*restrict*/ dataPtr, uint64_t count,
                uint32_t prev, uint32_t key, uint32_t *presult);
extern int      svb_find_scalar_d1_init(uint8_t * /*restrict*/ keyPtr,
                uint8_t * /*restrict*/ dataPtr, uint64_t count,
                uint32_t prev, uint32_t key, uint32_t *presult);
extern uint32_t svb_select_avx_d1_init(uint8_t *keyPtr,
                uint8_t *dataPtr, uint64_t count,
                uint32_t prev, int slot);
extern uint32_t svb_select_scalar_d1_init(uint8_t *keyPtr, uint8_t *dataPtr,
                uint64_t count, uint32_t prev, int slot);
extern bool svb_insert_scalar_d1_init_unique(uint8_t *keyPtr, uint8_t *dataPtr,
                size_t dataSize, uint32_t count,
                uint32_t prev, uint32_t new_key,
                uint32_t *position, uint8_t **dataEnd);
extern bool svb_insert_scalar_d1_init_front(uint8_t *keyPtr, uint8_t *dataPtr,
                size_t dataSize, uint32_t count, uint32_t old_prev,
                uint32_t new_prev, uint32_t new_key, uint8_t **dataEnd);


namespace upscaledb {

//
// The template classes in this file are wrapped in a separate namespace
// to avoid naming clashes with other KeyLists
//
namespace Zint32 {

// This structure is an "index" entry which describes the location
// of a variable-length block
#include "1base/packstart.h"
UPS_PACK_0 struct UPS_PACK_1 StreamVbyteIndex : IndexBase {
  enum {
    // Initial size of a new block
    kInitialBlockSize = 32,

    // Maximum keys per block
    kMaxKeysPerBlock = 256 + 1,
  };

  // initialize this block index
  void initialize(uint32_t offset, uint8_t *block_data, size_t block_size) {
    IndexBase::initialize(offset, block_data, block_size);
    _block_size = block_size;
    _used_size = 0;
    _key_size = 0;
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
    return _key_size;
  }

  // sets the key count
  void set_key_count(uint32_t key_count) {
    _key_size = key_count;
  }

  // copies this block to the |dest| block
  void copy_to(const uint8_t *block_data, StreamVbyteIndex *dest,
                  uint8_t *dest_data) {
    dest->set_value(value());
    dest->set_key_count(key_count());
    dest->set_used_size(used_size());
    dest->set_highest(highest());
    ::memcpy(dest_data, block_data, block_size());
  }

  // the total size of this block; max 2048-1 bytes
  unsigned int _block_size : 11;

  // used size of this block; max 2048-1 bytes
  unsigned int _used_size : 11;

  // the number of keys in this block; max 1024-1 (kMaxKeysPerBlock)
  unsigned int _key_size : 10;
} UPS_PACK_2;
#include "1base/packstop.h"

struct StreamVbyteCodecImpl : BlockCodecBase<StreamVbyteIndex> {
  enum {
    kHasCompressApi = 1,
    kHasFindLowerBoundApi = 1,
    kHasInsertApi = 1,
    kHasSelectApi = 1,
    kHasAppendApi = 1,
    kGapWidth = 4,
  };

  static uint32_t round_up(uint32_t count) {
    uint32_t r = (count == 0 ? 1 : (count + 3) / 4);
    if (r % kGapWidth != 0)
      r = ((r / kGapWidth) + 1) * kGapWidth;
    return r;
  }

  static uint32_t compress_block(StreamVbyteIndex *index, const uint32_t *in,
                  uint32_t *out32) {
    assert(index->key_count() > 0);
    uint8_t *out = (uint8_t *)out32;
    uint32_t count = index->key_count() - 1;
    uint32_t key_len = round_up(count);
    uint8_t *data = out + key_len;
  
    return svb_encode_scalar_d1_init(in, out, data, count,
                    index->value()) - out;
  }

  static uint32_t *uncompress_block(StreamVbyteIndex *index,
                  const uint32_t *block_data, uint32_t *out) {
    if (likely(index->key_count() > 1)) {
      uint32_t count = index->key_count() - 1;
      uint8_t *in = (uint8_t *)block_data;
      uint32_t key_len = round_up(count);
      uint8_t *data = in + key_len;

      if (os_has_avx())
        svb_decode_avx_d1_init(out, in, data, count, index->value());
      else
        svb_decode_scalar_d1_init(out, in, data, count, index->value());
    }
    return out;
  }

  static int find_lower_bound(StreamVbyteIndex *index,
                  const uint32_t *block_data, uint32_t key, uint32_t *result) {
    uint32_t count = index->key_count() - 1;
    uint8_t *in = (uint8_t *)block_data;
    uint32_t key_len = round_up(count);
    uint8_t *data = in + key_len;

    if (os_has_avx())
      return svb_find_avx_d1_init(in, data, count, index->value(),
                              key, result);
    else
      return svb_find_scalar_d1_init(in, data, count, index->value(),
                              key, result);
  }

  // Returns a decompressed value
  static uint32_t select(StreamVbyteIndex *index, uint32_t *block_data,
                        int slot) {
    uint32_t count = index->key_count() - 1;
    uint8_t *in = (uint8_t *)block_data;
    uint32_t key_len = round_up(count);
    uint8_t *data = in + key_len;

    if (os_has_avx())
      return svb_select_avx_d1_init(in, data, count, index->value(), slot);
    else
      return svb_select_scalar_d1_init(in, data, count, index->value(), slot);
  }

  static bool insert(StreamVbyteIndex *index, uint32_t *block_data32,
                  uint32_t key, int *pslot) {
    uint32_t prev = index->value();
    uint32_t new_index_value = prev;

    uint32_t count = index->key_count() - 1;
    uint8_t *keys = (uint8_t *)block_data32;
    uint32_t key_len = round_up(count);
    uint8_t *data = keys + key_len;

    uint8_t *data_end = 0;
    uint32_t position = 0;

    // make space for the new key?
    bool moved_data = false;
    if (count >= key_len * 4) {
      ::memmove(data + kGapWidth, data, index->used_size() - key_len);
      data += kGapWidth;
      key_len += kGapWidth;
      index->set_used_size(index->used_size() + kGapWidth);
      moved_data = true;
    }

    bool insert_at_front = false;

    // swap |key| and |index->value|, then replace the first key with its delta
    if (key < new_index_value) {
      new_index_value = key;
      key = index->value();
      insert_at_front = true;
    }

    if (count > 0 && insert_at_front) {
      (void)svb_insert_scalar_d1_init_front(keys, data,
                index->used_size() - key_len, count,
                index->value(), new_index_value, key, &data_end);
    }
    else {
      if (!svb_insert_scalar_d1_init_unique(keys, data,
                      index->used_size() - key_len,
                      count, new_index_value, key, &position, &data_end)) {
        // undo previous changes?
        if (moved_data) {
          ::memmove(data - kGapWidth, data, index->used_size() - key_len);
          index->set_used_size(index->used_size() - kGapWidth);
        }
        *pslot += position + 1;
        return false;
      }
    }

    index->set_value(new_index_value);
    index->set_key_count(index->key_count() + 1);
    index->set_used_size(data_end - keys);

    *pslot += position + 1;
    return true;
  }

  static bool append(StreamVbyteIndex *index, uint32_t *in32,
                  uint32_t key, int *pslot) {
    uint32_t count = index->key_count() - 1;

    if (unlikely(count == 0))
      return insert(index, in32, key, pslot);

    key -= index->highest();

    uint8_t *keys = (uint8_t *)in32;
    uint32_t key_len = round_up(count);

    // make space for the new key?
    if (count >= key_len * 4) {
      uint8_t *data = keys + key_len;
      ::memmove(data + kGapWidth, data, index->used_size() - key_len);
      index->set_used_size(index->used_size() + kGapWidth);
    }

    uint8_t *keyp = keys + count / 4;
    uint8_t *bout = (uint8_t *)in32 + index->used_size();
    uint8_t *bend = bout;
    int shift = (count % 4) * 2;

    if (key < (1U << 8)) {
      *bout++ = static_cast<uint8_t> (key);
      *keyp &= ~(3 << shift);
    }
    else if (key < (1U << 16)) {
      *bout++ = static_cast<uint8_t> (key);
      *bout++ = static_cast<uint8_t> (key >> 8);
      *keyp &= ~(3 << shift);
      *keyp |= (1 << shift);
    }
    else if (key < (1U << 24)) {
      *bout++ = static_cast<uint8_t> (key);
      *bout++ = static_cast<uint8_t> (key >> 8);
      *bout++ = static_cast<uint8_t> (key >> 16);
      *keyp &= ~(3 << shift);
      *keyp |= (2 << shift);
    }
    else {
      // the compiler will do the right thing
      *reinterpret_cast<uint32_t *> (bout) = key;
      bout += 4;
      *keyp |= (3 << shift);
    }

    index->set_key_count(index->key_count() + 1);
    index->set_used_size(index->used_size() + (bout - bend));
    *pslot += index->key_count() - 1;
    return true;
  }

  static uint32_t estimate_required_size(StreamVbyteIndex *index,
                        uint8_t *block_data, uint32_t key) {
    uint32_t size = index->used_size();

    // kGapWidth byte gap between keys and data
    // up to 5 byte for the new key
    if (index->key_count() >= round_up(index->key_count()))
      size += kGapWidth;

    key -= index->value();
    if (key < (1U << 8))
      size += 1;
    else if (key < (1U << 16)) 
      size += 2;
    else if (key < (1U << 24))
      size += 3;
    else
      size += 4;
    return size > 16 ? size : 16;
  }
};

typedef Zint32Codec<StreamVbyteIndex, StreamVbyteCodecImpl> StreamVbyteCodec;

struct StreamVbyteKeyList : BlockKeyList<StreamVbyteCodec> {
  // Constructor
  StreamVbyteKeyList(LocalDb *db, PBtreeNode *node)
    : BlockKeyList<StreamVbyteCodec>(db, node) {
  }
};

} // namespace Zint32

} // namespace upscaledb

#endif // HAVE_SSE2

#endif // UPS_BTREE_KEYS_STREAMVBYTE_H
