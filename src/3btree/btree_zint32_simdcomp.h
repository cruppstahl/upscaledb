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

#ifndef UPS_BTREE_KEYS_SIMDCOMP_H
#define UPS_BTREE_KEYS_SIMDCOMP_H

#ifdef HAVE_SSE2

#include <sstream>
#include <iostream>
#include <algorithm>

#include "0root/root.h"

#include "3rdparty/simdcomp/include/simdcomp.h"

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
UPS_PACK_0 struct UPS_PACK_1 SimdCompIndex : IndexBase {
  enum {
    // Initial size of a new block (1 bit per key = 16 bytes)
    kInitialBlockSize = 16,

    // Maximum keys per block (a compressed block holds up to 128 keys,
    // and one key is stored in the index)
    kMaxKeysPerBlock = 128 + 1,
  };

  // initialize this block index
  void initialize(uint32_t offset, uint8_t *block_data, uint32_t block_size) {
    IndexBase::initialize(offset, block_data, block_size);
    _bits = block_size / 16;
    _key_count = 0;
  }

  // returns the used block of the block
  uint32_t used_size() const {
    return block_size();
  }

  // sets the used size; not required
  void set_used_size(uint32_t size) {
    // nop
  }

  // returns the total block size
  uint32_t block_size() const {
    return _bits * 128 / 8;
  }

  // sets the block size; not required
  void set_block_size(uint32_t new_size) {
    // nop
  }

  // returns the key count
  uint32_t key_count() const {
    return _key_count;
  }

  // sets the key count
  void set_key_count(uint32_t key_count) {
    _key_count = key_count;
  }

  // returns the bits used to encode the block
  uint32_t bits() const {
    return _bits;
  }

  // sets the bits used to encode the block
  void set_bits(uint32_t bits) {
    _bits = bits;
  }

  // copies this block to the |dest| block
  void copy_to(const uint8_t *block_data, SimdCompIndex *dest,
                  uint8_t *dest_data) {
    assert(dest->bits() == bits());
    dest->set_value(value());
    dest->set_key_count(key_count());
    dest->set_highest(highest());
    ::memcpy(dest_data, block_data, block_size());
  }

  // the number of keys in this block; max 129 (kMaxKeysPerBlock)
  unsigned short _key_count : 8;

  // stored bits per integer; max 32
  unsigned short _bits : 6;
} UPS_PACK_2;
#include "1base/packstop.h"

struct SimdCompCodecImpl : BlockCodecBase<SimdCompIndex> {
  enum {
    kHasCompressApi = 1,
    kHasFindLowerBoundApi = 1,
    kHasSelectApi = 1,
    kHasAppendApi = 1,
    kHasDelApi = 1,
  };

  static uint32_t compress_block(SimdCompIndex *index, const uint32_t *in,
                  uint32_t *out) {
    assert(index->key_count() > 0);
    simdpackwithoutmaskd1(index->value(), in, (__m128i *)out, index->bits());
    return index->used_size();
  }

  static uint32_t *uncompress_block(SimdCompIndex *index,
                  const uint32_t *block_data, uint32_t *out) {
    simdunpackd1(index->value(), (__m128i *)block_data, out, index->bits());
    return out;
  }

  static int find_lower_bound(SimdCompIndex *index, const uint32_t *block_data,
                  uint32_t key, uint32_t *presult) {
    return simdsearchwithlengthd1(index->value(), (const __m128i *)block_data,
                                    index->bits(), (int)index->key_count() - 1,
                                    key, presult);
  }

  // Returns a decompressed value
  static uint32_t select(SimdCompIndex *index, uint32_t *block_data,
                        int position_in_block) {
    return simdselectd1(index->value(), (const __m128i *)block_data,
                                    index->bits(), position_in_block);
  }

  static bool append(SimdCompIndex *index, uint32_t *in32,
                        uint32_t key, int *pslot) {
    // 32 bits: don't store delta
    if (unlikely(index->bits() == 32))
      simdfastset((__m128i *)in32, index->bits(), key,
                        index->key_count() - 1);
    else
      simdfastset((__m128i *)in32, index->bits(), key - index->highest(),
                        index->key_count() - 1);

    index->set_key_count(index->key_count() + 1);
    *pslot += index->key_count() - 1;
    return true;
  }

  template<typename GrowHandler>
  static void del(SimdCompIndex *index, uint32_t *block_data, int slot,
                  GrowHandler *key_list) {
    // The key is now deleted from the block, and afterwards the block
    // is compressed again. The simdcomp algorithm is not delete-stable,
    // which means that after compression it might require more storage
    // than before. If this is the case an Exception is thrown and the
    // caller will provide more space.
    //
    // !!
    // Make sure that this code path is Exception Safe! Do not modify any
    // persistent data until we are 100% sure that no exception will be
    // thrown!

    // uncompress the block and remove the key
    uint32_t data[128];
    uncompress_block(index, block_data, data);

    // delete the first value?
    if (slot == 0) {
      index->set_value(data[0]);
      slot++;
    }

    if (slot < (int)index->key_count() - 1)
      ::memmove(&data[slot - 1], &data[slot],
              sizeof(uint32_t) * (index->key_count() - slot - 1));

    // grow the block?
    if (unlikely(index->bits() < 32 && slot < (int)index->key_count() - 1)) {
      uint32_t new_bits;
      assert(slot > 0);
      if (unlikely(slot == 1))
        new_bits = bits(data[0] - index->value());
      else
        new_bits = bits(data[slot - 1] - data[slot - 2]);
      if (new_bits > index->bits()) {
        // yes, try to grow; this will cause a split if it fails
        uint32_t new_size = new_bits * 128 / 8;
        key_list->grow_block_size(index, new_size);
        index->set_bits(new_bits);
      }
    }

    index->set_key_count(index->key_count() - 1);

    // update the cached highest block value?
    if (unlikely(index->key_count() <= 1))
      index->set_highest(index->value());
    else
      index->set_highest(data[index->key_count() - 2]);

    if (likely(index->key_count() > 1))
      compress_block(index, data, block_data);
  }

  static uint32_t estimate_required_size(SimdCompIndex *index,
                        uint8_t *block_data, uint32_t key) {
    /* not used */
    assert(!"shouldn't be here");
    return 0;
  }
};

typedef Zint32Codec<SimdCompIndex, SimdCompCodecImpl> SimdCompCodec;

class SimdCompKeyList : public BlockKeyList<SimdCompCodec>
{
  public:
    // Constructor
    SimdCompKeyList(LocalDb *db, PBtreeNode *node)
      : BlockKeyList<SimdCompCodec>(db, node) {
    }

    // Copies all keys from this[sstart] to dest[dstart]; this method
    // is used to split and merge btree nodes.
    void copy_to(int sstart, size_t node_count, SimdCompKeyList &dest,
                    size_t other_count, int dstart) {
      assert(check_integrity(0, node_count));

      // if the destination node is empty (often the case when merging nodes)
      // then re-initialize it.
      if (other_count == 0)
        dest.initialize();

      // find the start block
      int src_position_in_block;
      Index *srci = find_block_by_slot(sstart, &src_position_in_block);
      // find the destination block
      int dst_position_in_block;
      Index *dsti = dest.find_block_by_slot(dstart, &dst_position_in_block);

      bool initial_block_used = false;

      // If start offset or destination offset > 0: uncompress both blocks,
      // merge them
      if (src_position_in_block > 0 || dst_position_in_block > 0) {
        uint32_t sdata_buf[Index::kMaxKeysPerBlock];
        uint32_t ddata_buf[Index::kMaxKeysPerBlock];
        uint32_t *sdata = uncompress_block(srci, &sdata_buf[0]);
        uint32_t *ddata = dest.uncompress_block(dsti, &ddata_buf[0]);

        uint32_t *d = &ddata[srci->key_count()];

        dsti->set_highest(srci->highest());

        if (src_position_in_block == 0) {
          assert(dst_position_in_block != 0);
          srci->set_highest(srci->value());
          *d = srci->value();
          d++;
        }
        else {
          assert(dst_position_in_block == 0);
          dsti->set_value(sdata[src_position_in_block - 1]);
          if (src_position_in_block == 1)
            srci->set_highest(sdata[src_position_in_block - 1]);
          else
            srci->set_highest(sdata[src_position_in_block - 2]);
          src_position_in_block++;
        }
        dsti->set_key_count(dsti->key_count() + 1);

        for (int i = src_position_in_block; i < (int)srci->key_count(); i++) {
          ddata[dsti->key_count() - 1] = sdata[i - 1];
          dsti->set_key_count(dsti->key_count() + 1);
        }

        srci->set_key_count(srci->key_count() - dsti->key_count());
        if (srci->key_count() == 1)
          srci->set_highest(srci->value());

        // grow destination block?
        if (dsti->bits() < 32) {
          uint32_t new_bits = calc_max_bits(dsti->value(), &ddata[0],
                                    dsti->key_count() - 1);
          if (new_bits > dsti->bits()) {
            uint32_t new_size = new_bits * 128 / 8;
            dest.grow_block_size(dsti, new_size);
            dsti->set_bits(new_bits);
          }
        }

        dest.compress_block(dsti, ddata);

        srci++;
        dsti++;
        initial_block_used = true;
      }

      // When merging nodes, check if we actually append to the other node
      if (dst_position_in_block == 0 && dstart > 0)
        initial_block_used = true; // forces loop below to create a new block

      // Now copy the remaining blocks (w/o uncompressing them)
      // TODO this could be sped up by adding multiple blocks in one step
      int copied_blocks = 0;
      for (; srci < block_index(block_count());
                      srci++, copied_blocks++) {
        if (initial_block_used == true)
          dsti = dest.add_block(dest.block_count(), srci->block_size());
        else if (dsti->bits() < srci->bits()) {
          dest.grow_block_size(dsti, srci->block_size());
          dsti->set_bits(srci->bits());
          initial_block_used = true;
        }

        srci->copy_to(block_data(srci), dsti, dest.block_data(dsti));
      }

      // remove the copied blocks
      uint8_t *pend = &data_[used_size()];
      uint8_t *pold = (uint8_t *)block_index(block_count());
      uint8_t *pnew = (uint8_t *)block_index(block_count() - copied_blocks);
      ::memmove(pnew, pold, pend - pold);

      set_block_count(block_count() - copied_blocks);

      reset_used_size();

      // we need at least ONE empty block, otherwise a few functions will bail
      if (block_count() == 0) {
        initialize();
      }

      assert(dest.check_integrity(0, other_count + (node_count - sstart)));
      assert(check_integrity(0, sstart));
    }

  private:
    // Returns the number of bits required to store a block
    uint32_t calc_max_bits(uint32_t initial_value, uint32_t *data,
                    uint32_t length) const {
      if (unlikely(length == 0))
        return 1;
      return simdmaxbitsd1_length(initial_value, data, length);
    }

    // Implementation for insert()
    virtual PBtreeNode::InsertResult insert_impl(size_t node_count,
                    uint32_t key, uint32_t flags) {
      int slot = 0;

      block_cache.is_active = false;

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

      // A split is required if the block maxxed out the keys or if
      // (used_size >= block_size and block_size >= max_size)
      bool requires_split = index->key_count() + 1
                                >= (SimdCompIndex::kMaxKeysPerBlock + 1);

      // grow the block if it is full
      if (unlikely(requires_split)) {
        int block = index - block_index(0);

        // if the new key is prepended then also prepend the new block
        if (key < index->value()) {
          Index *new_index = add_block(block + 1,
                                SimdCompIndex::kInitialBlockSize);
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
                                SimdCompIndex::kInitialBlockSize);
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
          return PBtreeNode::InsertResult(UPS_DUPLICATE_KEY, slot + to_copy);

        to_copy++;
        ::memmove(&new_data[0], &data[to_copy],
                    sizeof(int32_t) * (index->key_count() - to_copy));

        // calculate the required bits for the new block
        uint32_t required_bits = calc_max_bits(new_value, &new_data[0],
                                    new_key_count - 1);

        // Now create a new block. This can throw, but so far we have not
        // modified existing data.
        Index *new_index = add_block(block + 1, required_bits * 128 / 8);
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

      uint32_t *data = 0;
      uint32_t required_bits = 0;

      // check if the block needs to grow; this CAN be the case if the stored
      // bits are not large enough for the new delta
      if (key > index->highest()) {
        required_bits = bits(key - index->highest());
      }
      else if (key < index->value()) {
        required_bits = bits(index->value() - key);
      }
      else if (index->key_count() == 1) {
        required_bits = bits(key - index->value());
      }
      else {
        data = uncompress_block(index, datap);
        if (key < data[0])
          required_bits = bits(key - index->value());
      }

      bool resized = false;
      if (required_bits > index->bits()) {
        data = uncompress_block(index, datap);
        grow_block_size(index, required_bits * 128 / 8);
        index->set_bits(required_bits);
        resized = true;
      }

      // now append or insert the key, but only if the block was not resized;
      // otherwise the block has to be fully re-encoded
      if (key > index->highest() && !resized) {
        SimdCompCodecImpl::append(index, (uint32_t *)block_data(index),
                            key, &slot);
      }
      else {
        if (!data)
          data = uncompress_block(index, datap);

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
          if (unlikely(it < end && *it == key))
            return PBtreeNode::InsertResult(UPS_DUPLICATE_KEY,
                                slot + (it - &data[0]) + 1);

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

    // Implementation of vacuumize()
    void vacuumize_weak() {
      // This is not implemented. Caller will abort the current operation and
      // perform a page split.
      throw Exception(UPS_LIMITS_REACHED);
    }

    // Implementation of vacuumize()
    void vacuumize_full() {
      block_cache.is_active = false;

      int capacity = block_count() * SimdCompIndex::kMaxKeysPerBlock;

      // iterate over all blocks, uncompress them into a big array
      uint32_t *p = (uint32_t *)::alloca(capacity * sizeof(uint32_t));
      uint32_t *p_end = p;

      Index *index = block_index(0);
      Index *end = index + block_count();
      for (; index < end; index++) {
        *p_end = index->value();
        p_end++;
        uncompress_block(index, p_end);
        p_end += index->key_count() - 1;
      }

      // now re-build the page
      initialize();

      index = block_index(0);

      // how many blocks are required?
      int required_blocks = (p_end - p) / SimdCompIndex::kMaxKeysPerBlock;
      if (required_blocks * SimdCompIndex::kMaxKeysPerBlock < p_end - p)
        required_blocks++;
      set_block_count(required_blocks);

      // Now create and fill all the blocks
      uint32_t offset = 0;
      while (p_end - p >= SimdCompIndex::kMaxKeysPerBlock) {
        uint32_t required_bits = calc_max_bits(*p, p + 1, 128);
        uint32_t required_size = required_bits * 128 / 8;

        index->set_bits(required_bits);
        index->set_offset(offset);
        index->set_value(*p);
        index->set_highest(*(p + 128));
        index->set_key_count(SimdCompIndex::kMaxKeysPerBlock);
        compress_block(index, p + 1);

        offset += required_size;
        p += SimdCompIndex::kMaxKeysPerBlock;
        index++;
      }

      // only one key left? then create an empty block with an initial value
      if (p_end - p == 1) {
        index->set_value(*p);
        index->set_highest(*p);
        index->set_key_count(1);
        index->set_bits(1);
        index->set_offset(offset);
        offset += 16; // minimum block size for 1 bit
      }
      // more keys left? then create a new block and fill it
      else if (p_end - p > 1) {
        uint32_t value = *p;
        p++;

        uint32_t required_bits = calc_max_bits(value, p, p_end - p);
        uint32_t required_size = required_bits * 128 / 8;
        index->set_offset(offset);
        index->set_bits(required_bits);
        index->set_key_count(p_end - p + 1);
        index->set_value(value);
        index->set_highest(*(p_end - 1));

        compress_block(index, p);
        offset += required_size;
      }

      set_used_size(2 * sizeof(uint32_t)
                        + required_blocks * sizeof(Index)
                        + offset);
    }
};

} // namespace Zint32

} // namespace upscaledb

#endif // HAVE_SSE2

#endif // UPS_BTREE_KEYS_SIMDCOMP_H
