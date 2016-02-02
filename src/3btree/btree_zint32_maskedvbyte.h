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

#ifndef UPS_BTREE_KEYS_MASKEDVBYTE_H
#define UPS_BTREE_KEYS_MASKEDVBYTE_H

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

extern void simdvbyteinit(void);
extern uint32_t masked_vbyte_read_loop_delta(const uint8_t* in, uint32_t* out,
		        uint64_t length, uint32_t prev);
extern int masked_vbyte_search_delta(const uint8_t *block_data, int length,
                uint32_t prev, uint32_t key, uint32_t *presult);
extern uint32_t masked_vbyte_select_delta(const uint8_t *in, int length,
                uint32_t prev, int slot);

namespace upscaledb {

//
// The template classes in this file are wrapped in a separate namespace
// to avoid naming clashes with other KeyLists
//
namespace Zint32 {

// This structure is an "index" entry which describes the location
// of a variable-length block
#include "1base/packstart.h"
UPS_PACK_0 class UPS_PACK_1 MaskedVbyteIndex : public IndexBase {
  public:
    enum {
      // Initial size of a new block
      kInitialBlockSize = 16,

      // Maximum keys per block
      kMaxKeysPerBlock = 256 + 1,
    };

    // initialize this block index
    void initialize(uint32_t offset, uint8_t *data, uint32_t block_size) {
      IndexBase::initialize(offset, data, block_size);
      m_block_size = block_size;
      m_used_size = 0;
      m_key_count = 0;
    }

    // returns the used size of the block
    uint32_t used_size() const {
      return (m_used_size);
    }

    // sets the used size of the block
    void set_used_size(uint32_t size) {
      m_used_size = size;
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
    void copy_to(const uint8_t *block_data, MaskedVbyteIndex *dest,
                    uint8_t *dest_data) {
      dest->set_value(value());
      dest->set_key_count(key_count());
      dest->set_used_size(used_size());
      dest->set_highest(highest());
      ::memcpy(dest_data, block_data, block_size());
    }

  private:
    // the total size of this block
    unsigned int m_block_size : 11;

    // used size of this block
    unsigned int m_used_size : 11;

    // the number of keys in this block; max 512 - 1 (kMaxKeysPerBlock)
    unsigned int m_key_count : 9;
} UPS_PACK_2;
#include "1base/packstop.h"

struct MaskedVbyteCodecImpl : public BlockCodecBase<MaskedVbyteIndex>
{
  enum {
    kHasCompressApi = 1,
    kCompressInPlace = 0,
    kHasFindLowerBoundApi = 1,
    kHasSelectApi = 1,
    kHasDelApi = 1,
    kHasInsertApi = 1,
    kHasAppendApi = 1,
  };

  static uint32_t compress_block(MaskedVbyteIndex *index, const uint32_t *in,
                  uint32_t *out32) {
    assert(index->key_count() > 0);
    uint8_t *out = (uint8_t *)out32;
    size_t length = index->key_count() - 1;
  
    const uint8_t *const initbout = reinterpret_cast<uint8_t *> (out);
    uint8_t *bout = reinterpret_cast<uint8_t *> (out);
    uint32_t prev = index->value();

    for (size_t k = 0; k < length; ++k) {
      const uint32_t val = in[k] - prev;
      prev = in[k];
      bout += write_int(bout, val);
    }
    return (bout - initbout);
  }

  static uint32_t *uncompress_block(MaskedVbyteIndex *index,
                  const uint32_t *block_data, uint32_t *out) {
    masked_vbyte_read_loop_delta((const uint8_t *)block_data,
                        out, index->key_count() - 1, index->value());
    return (out);
  }

  static bool append(MaskedVbyteIndex *index, uint32_t *block_data32,
                  uint32_t key, int *pslot) {
    uint8_t *p = (uint8_t *)block_data32 + index->used_size();
    int space = write_int(p, key - index->highest());

    index->set_key_count(index->key_count() + 1);
    index->set_used_size(index->used_size() + space);
    *pslot += index->key_count() - 1;
    return (true);
  }


  static bool insert(MaskedVbyteIndex *index, uint32_t *block_data32,
                  uint32_t key, int *pslot) {
    uint32_t prev = index->value();

    // swap |key| and |index->value|, then replace the first key with its delta
    if (key < prev) {
      uint32_t delta = index->value() - key;
      index->set_value(key);

      int required_space = calculate_delta_size(delta);
      uint8_t *p = (uint8_t *)block_data32;

      if (index->used_size() > 0)
        ::memmove(p + required_space, p, index->used_size());
      write_int(p, delta);

      index->set_key_count(index->key_count() + 1);
      index->set_used_size(index->used_size() + required_space);
      *pslot += 1;
      return (true);
    }

    uint8_t *block_data = (uint8_t *)block_data32;

    // fast-forward to the position of the new key
    uint8_t *p = fast_forward_to_key(index, block_data, key, &prev, pslot);
    // make sure that we don't have a duplicate key
    if (key == prev)
      return (false);

    // reached the end of the block? then append the new key
    if (*pslot == (int)index->key_count()) {
      key -= prev;
      int size = write_int(p, key);
      index->set_used_size(index->used_size() + size);
      index->set_key_count(index->key_count() + 1);
      return (true);
    }

    // otherwise read the next key at |position + 1|, because
    // its delta will change when the new key is inserted
    uint32_t next_key;
    uint8_t *next_p = p + read_int(p, &next_key);
    next_key += prev;

    if (next_key == key) {
      *pslot += 1;
      return (false);
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
    return (true);
  }

  template<typename GrowHandler>
  static void del(MaskedVbyteIndex *index, uint32_t *block_data, int slot,
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
      if (index->key_count() == 1)
        index->set_used_size(0);
      else {
        ::memmove(start, p, index->used_size());
        index->set_used_size(index->used_size() - (p - start));
      }

      // update the cached highest block value?
      if (index->key_count() <= 1)
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

    if (index->key_count() == 2) {
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

  static int find_lower_bound(MaskedVbyteIndex *index,
                  const uint32_t *block_data, uint32_t key, uint32_t *presult) {
    return (masked_vbyte_search_delta((const uint8_t *)block_data,
                        index->key_count() - 1, index->value(), key, presult));
  }

  static uint32_t select(MaskedVbyteIndex *index, uint32_t *block_data,
                        int position_in_block) {
    return (masked_vbyte_select_delta((const uint8_t *)block_data,
                        index->key_count() - 1, index->value(),
                        position_in_block));
  }

  static uint32_t estimate_required_size(MaskedVbyteIndex *index,
                        uint8_t *block_data, uint32_t key) {
    return (index->used_size() + calculate_delta_size(key - index->value()));
  }

  // returns the compressed size of |value|
  static int calculate_delta_size(uint32_t value) {
    if (value < (1U << 7)) {
      return (1);
    }
    if (value < (1U << 14)) {
      return (2);
    }
    if (value < (1U << 21)) {
      return (3);
    }
    if (value < (1U << 28)) {
      return (4);
    }
    return (5);
  }

  // fast-forwards to the specified key in a block
  static uint8_t *fast_forward_to_key(MaskedVbyteIndex *index,
                  uint8_t *block_data, uint32_t key,
                  uint32_t *pprev, int *pslot) {
    uint32_t delta;

    *pprev = index->value();
    if (key < *pprev) {
      *pslot = 0;
      return (block_data);
    }

    for (int i = 0; i < (int)index->key_count() - 1; i++) {
      uint8_t *next = block_data + read_int(block_data, &delta);
      if (*pprev + delta >= key) {
        *pslot = i;
        return (block_data);
      }
      block_data = next;
      *pprev += delta;
    }

    *pslot = index->key_count();
    return (block_data);
  }

  // this assumes that there is a value to be read
  static int read_int(const uint8_t *in, uint32_t *out) {
    *out = in[0] & 0x7F;
    if (in[0] < 128) {
      return (1);
    }
    *out = ((in[1] & 0x7FU) << 7) | *out;
    if (in[1] < 128) {
      return (2);
    }
    *out = ((in[2] & 0x7FU) << 14) | *out;
    if (in[2] < 128) {
      return (3);
    }
    *out = ((in[3] & 0x7FU) << 21) | *out;
    if (in[3] < 128) {
      return (4);
    }
    *out = ((in[4] & 0x7FU) << 28) | *out;
    return (5);
  }

  // writes |value| to |p|
  static int write_int(uint8_t *p, uint32_t value) {
    assert(value > 0);
    if (value < (1U << 7)) {
      *p = value & 0x7F;
      return (1);
    }
    if (value < (1U << 14)) {
      *p = static_cast<uint8_t>((value & 0x7F) | (1U << 7));
      ++p;
      *p = static_cast<uint8_t>(value >> 7);
      return (2);
    }
    if (value < (1U << 21)) {
      *p = static_cast<uint8_t>((value & 0x7F) | (1U << 7));
      ++p;
      *p = static_cast<uint8_t>(((value >> 7) & 0x7F) | (1U << 7));
      ++p;
      *p = static_cast<uint8_t>(value >> 14);
      return (3);
    }
    if (value < (1U << 28)) {
      *p = static_cast<uint8_t>((value & 0x7F) | (1U << 7));
      ++p;
      *p = static_cast<uint8_t>(((value >> 7) & 0x7F) | (1U << 7));
      ++p;
      *p = static_cast<uint8_t>(((value >> 14) & 0x7F) | (1U << 7));
      ++p;
      *p = static_cast<uint8_t>(value >> 21);
      return (4);
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
      return (5);
    }
  }

  template<uint32_t i>
  static uint8_t extract7bits(const uint32_t val) {
    return static_cast<uint8_t>((val >> (7 * i)) & ((1U << 7) - 1));
  }

  template<uint32_t i>
  static uint8_t extract7bitsmaskless(const uint32_t val) {
    return static_cast<uint8_t>((val >> (7 * i)));
  }
};

typedef Zint32Codec<MaskedVbyteIndex, MaskedVbyteCodecImpl> MaskedVbyteCodec;

class MaskedVbyteKeyList : public BlockKeyList<MaskedVbyteCodec>
{
  public:
    // Constructor
    MaskedVbyteKeyList(LocalDatabase *db)
      : BlockKeyList<MaskedVbyteCodec>(db) {
      static bool initialized = false;
      if (!initialized) {
        simdvbyteinit();
        initialized = true;
      }
    }
};

} // namespace Zint32

} // namespace upscaledb

#endif // HAVE_SSE2

#endif /* UPS_BTREE_KEYS_MASKEDVBYTE_H */
