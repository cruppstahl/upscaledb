/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
 */

/*
 * Compressed 32bit integer keys
 *
 * @exception_safe: strong
 * @thread_safe: no
 */

#ifndef HAM_BTREE_KEYS_VARBYTE_H
#define HAM_BTREE_KEYS_VARBYTE_H

#include <sstream>
#include <iostream>

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "3btree/btree_keys_block.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

//
// The template classes in this file are wrapped in a separate namespace
// to avoid naming clashes with other KeyLists
//
namespace Zint32 {

// This structure is an "index" entry which describes the location
// of a variable-length block
#include "1base/packstart.h"
HAM_PACK_0 class HAM_PACK_1 VarbyteIndex : public IndexBase {
  public:
    enum {
      // Initial size of a new block
      kInitialBlockSize = 16,

      // Grow blocks by this factor
      kGrowFactor = 16,

      // Maximum keys per block (9 bits)
      kMaxKeysPerBlock = 384,

      // Maximum size of an encoded integer
      kMaxSizePerInt = 5,

      // Maximum block size
      kMaxBlockSize = 511,
    };

    // initialize this block index
    void initialize(uint32_t offset, uint32_t block_size) {
      IndexBase::initialize(offset);
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
    void copy_to(const uint8_t *block_data, VarbyteIndex *dest,
                    uint8_t *dest_data) {
      dest->set_value(value());
      dest->set_key_count(key_count());
      dest->set_used_size(used_size());
      ::memcpy(dest_data, block_data, block_size());
    }

  private:
    // the total size of this block; max 511 bytes (kMaxBlockSize)
    unsigned int m_block_size : 9;

    // used size of this block; max 511 bytes
    unsigned int m_used_size : 9;

    // the number of keys in this block; max 511 (kMaxKeysPerBlock)
    unsigned int m_key_count : 9;
} HAM_PACK_2;
#include "1base/packstop.h"

struct VarbyteCodecImpl : public BlockCodecBase<VarbyteIndex>
{
  enum {
    kHasCompressApi = 1,
    kHasFindLowerBoundApi = 1,
    kHasDelApi = 1,
    kHasInsertApi = 1,
    kHasSelectApi = 1,
  };

  static uint32_t *uncompress_block(VarbyteIndex *index,
                  const uint32_t *block_data, uint32_t *out) {
    uint32_t *initout = out;
    const uint8_t *p = (const uint8_t *)block_data;
    uint32_t delta, prev = index->value();
    for (uint32_t i = 1; i < index->key_count(); i++, out++) {
      p += read_int(p, &delta);
      prev += delta;
      *out = prev;
    }
    return (initout);
  }

  static uint32_t compress_block(VarbyteIndex *index, const uint32_t *in,
                  uint32_t *out32) {
    uint8_t *out = (uint8_t *)out32;
    uint8_t *p = out;
    uint32_t prev = index->value();
    for (uint32_t i = 1; i < index->key_count(); i++, in++) {
      p += write_int(p, *in - prev);
      prev = *in;
    }
    return (p - out);
  }

  static int find_lower_bound(VarbyteIndex *index, const uint32_t *block_data,
                  uint32_t key, uint32_t *result) {
    uint32_t delta, prev = index->value();
    uint8_t *p = (uint8_t *)block_data;
    uint32_t s;
    for (s = 1; s < index->key_count(); s++) {
      p += read_int(p, &delta);
      prev += delta;

      if (prev >= key) {
        *result = prev;
        break;
      }
    }
    return (s - 1);
  }

  static bool insert(VarbyteIndex *index, uint32_t *block_data32,
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

    if (next_key == key)
      return (false);

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
  static void del(VarbyteIndex *index, uint32_t *block_data, int slot,
                  GrowHandler *unused) {
    ham_assert(index->key_count() > 1);

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
      return;
    }

    // cut off the last key in the block?
    if (slot == (int)index->key_count() - 1) {
      index->set_used_size(index->used_size()
              - ((data + index->used_size()) - p));
      index->set_key_count(index->key_count() - 1);
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
    uint8_t *p = (uint8_t *)block_data;
    uint32_t delta, key = index->value();
    for (int i = 0; i <= position_in_block; i++) {
      p += read_int(p, &delta);
      key += delta;
    }
    return (key);
  }

  // fast-forwards to the specified key in a block
  static uint8_t *fast_forward_to_key(VarbyteIndex *index, uint8_t *block_data,
                        uint32_t key, uint32_t *pprev, int *pslot) {
    *pprev = index->value();
    if (key < *pprev) {
      *pslot = 0;
      return (block_data);
    }

    uint32_t delta;
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

  // writes |value| to |p|
  static int write_int(uint8_t *p, uint32_t value) {
    ham_assert(value > 0);
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
};

typedef Zint32Codec<VarbyteIndex, VarbyteCodecImpl> VarbyteCodec;

class VarbyteKeyList : public BlockKeyList<VarbyteCodec>
{
  public:
    enum {
      // Maximum block size, in bytes
      kMaxBlockSize = 256,
    };

    // Constructor
    VarbyteKeyList(LocalDatabase *db)
      : BlockKeyList<VarbyteCodec>(db) {
    }
#if 0
    // Inserts a key
    template<typename Cmp>
    PBtreeNode::InsertResult insert(Context *context, size_t node_count,
                    const ham_key_t *hkey, uint32_t flags, Cmp &comparator,
                    int /* unused */ slot) {
      ham_assert(check_integrity(0, node_count));
      ham_assert(hkey->size == sizeof(uint32_t));

      // first perform a linear search through the index and get the block
      // which will receive the new key
      uint32_t key = *(uint32_t *)hkey->data;
      Index *index = find_index(key, &slot);
      if (slot == -1) {
        flags |= PBtreeNode::kInsertPrepend;
        slot = 0;
      }

      // if the block is full then grow or split it
      if (index->used_size() + 5 > index->block_size()) {
        // already reached max. size? then perform a split
        if (index->block_size() >= kMaxBlockSize
             || index->key_count() == VarbyteIndex::kMaxKeysPerBlock) {
          if (flags & PBtreeNode::kInsertPrepend) {
            index = add_block(0, VarbyteIndex::kInitialBlockSize);
          }
          else if (flags & PBtreeNode::kInsertAppend) {
            slot += index->key_count();
            index = add_block(get_block_count(),
                            VarbyteIndex::kInitialBlockSize);
          }
          else {
            Index *new_index = split_block(index, key);
            if (index != new_index) {
              slot += index->key_count();
              index = new_index;
            }
          }
        }
        else {
          grow_block(index, VarbyteIndex::kGrowFactor);
        }
      }

      // now perform the actual insert into this block
      PBtreeNode::InsertResult result = insert_impl(index, key, slot, flags);
      ham_assert(check_integrity(0, node_count + 1));
      return (result);
    }
#endif

    // Copies all keys from this[sstart] to dest[dstart]; this method
    // is used to split btree nodes.
    //
    // TODO this function is too complex. rewrite it!
    void copy_to(int sstart, size_t node_count, VarbyteKeyList &dest,
                    size_t other_count, int dstart) {
      ham_assert(check_integrity(0, node_count));

      Index *index = 0;
      
      // find the start block 
      int src_position_in_block;
      Index *srci = find_block_by_slot(sstart, &src_position_in_block);

      // get the destination block
      int dst_position_in_block;
      Index *dsti = dest.find_block_by_slot(dstart, &dst_position_in_block);

      // make sure it has free space
      if (dsti->block_size() < dsti->used_size() + srci->used_size() + 10) {
        int bytes = dsti->used_size() + srci->used_size() + 10
                        - dsti->block_size();
        if (dsti->block_size() + bytes > VarbyteIndex::kMaxBlockSize)
          bytes = VarbyteIndex::kMaxBlockSize - dsti->block_size();
        dest.grow_block(dsti, bytes);
      }

      int copied_blocks = 0;

      if (src_position_in_block > 0) {
        src_position_in_block++; // TODO inc, and dec is below??

        uint8_t *s;
        uint32_t srckey = 0;
        uint32_t delta;

        // need to keep a copy of the pointer where we started, so we can later
        // figure out how many bytes were copied
        uint8_t *start_s;

        // if the start position is in the middle of a block then fast-forward
        // to that position
        if (src_position_in_block > 2) {
          s = fast_forward_to_position(srci, src_position_in_block - 2, &srckey);
          start_s = s;
          s += read_int(s, &delta);
          srckey += delta;
        }
        else {
          s = get_block_data(srci);
          start_s = s;
          s += read_int(s, &delta);
          srckey = srci->value() + delta;
        }

        // fast-forward to the start position in the destination block
        uint8_t *d;
        uint32_t dstkey = 0;
        if (dst_position_in_block > 0) {
          d = fast_forward_to_position(dsti, dst_position_in_block, &dstkey);
        }
        // otherwise get a pointer to the beginning of the data
        else {
          d = dest.get_block_data(dsti);
        }

        // need to keep a copy of the pointer where we started, so we can later
        // figure out how many bytes were copied
        uint8_t *start_d = d;

        // now copy the first key. take into account that the first key of
        // a block is uncompressed.
        if (src_position_in_block == 0) {
          srckey = srci->value();
          src_position_in_block = 1;
        }

        if (dst_position_in_block == 0) {
          dsti->set_value(srckey);
          dstkey = srckey;
        }
        else {
          d += write_int(d, srckey - dstkey);
        }
        dsti->set_key_count(dsti->key_count() + 1);

        // now copy the remaining keys of the first block
        for (int i = src_position_in_block; i < (int)srci->key_count(); i++) {
          s += read_int(s, &delta); // TODO use memcpy!
          d += write_int(d, delta);
          dsti->set_key_count(dsti->key_count() + 1);
        }
        srci->set_key_count(srci->key_count() - dsti->key_count());
        if (srci->key_count() == 1)
          srci->set_used_size(0);
        else
          srci->set_used_size(srci->used_size() - (s - start_s));
        dsti->set_used_size(dsti->used_size() + (d - start_d));

        index = srci + 1;
      }
      // |src_position_in_block| is > 0 - copy the current block, then
      // fall through
      else {
        if (dsti->key_count() > 0) {
          uint32_t data[VarbyteIndex::kMaxKeysPerBlock];
          data[0] = srci->value();
          uncompress_block(srci, data + 1);

          uint32_t dstkey;
          uint8_t *d;
          if (dsti->key_count() == 1) {
            d = dest.get_block_data(dsti);
            dstkey = dsti->value();
          }
          else {
            d = dest.fast_forward_to_position(dsti, dsti->key_count() - 1,
                            &dstkey);
          }

          uint8_t *d_start = d;
          int src_count = srci->key_count();
          for (int i = src_position_in_block; i < src_count; i++) {
            d += write_int(d, data[i] - dstkey);
            dstkey = data[i];
            dsti->set_key_count(dsti->key_count() + 1);
            srci->set_key_count(srci->key_count() - 1);
          }
          dsti->set_used_size(dsti->used_size() + (d - d_start));
        }
        else {
          srci->copy_to(get_block_data(srci), dsti, dest.get_block_data(dsti));
        }
        index = srci + 1;
        copied_blocks++;
      }

      // now copy the remaining blocks
      // TODO it would be faster to introduce add_many_blocks()
      // instead of invoking add_block() several times
      // TODO each block has a minimum size of 16 - even if the current block
      // is much smaller
      // TODO it would be better if small blocks would be merged!
      Index *endi = get_block_index(get_block_count()); 
      for (; index < endi; index++, copied_blocks++) {
        dsti = dest.add_block(dest.get_block_count(), index->block_size());
        index->copy_to(get_block_data(index), dsti, dest.get_block_data(dsti));
      }

      // remove the copied blocks
      uint8_t *pend = &m_data[get_used_size()];
      uint8_t *pold = (uint8_t *)get_block_index(get_block_count());
      uint8_t *pnew = (uint8_t *)get_block_index(get_block_count()
                                    - copied_blocks);
      ::memmove(pnew, pold, pend - pold);

      set_block_count(get_block_count() - copied_blocks);

      reset_used_size();

      // we need at least ONE empty block, otherwise a few functions will bail
      if (get_block_count() == 0) {
        initialize();
      }

      ham_assert(dest.check_integrity(0, other_count + (node_count - sstart)));
      ham_assert(check_integrity(0, sstart));
    }

  private:
#if 0
    // Inserts a key in the specified block
    virtual PBtreeNode::InsertResult insert_impl(Index *index, uint32_t key,
                    int skipped_slots, uint32_t flags) {
      // first key in an empty block? then don't store a delta
      if (index->key_count() == 0) {
        index->set_key_count(1);
        index->set_value(key);
        ham_assert(index->used_size() == 0);
        return (PBtreeNode::InsertResult(0, skipped_slots));
      }

      // now prepend, append or insert
      skipped_slots += insert_key_in_block(index, key, flags);

      return (PBtreeNode::InsertResult(0, skipped_slots));
    }

    // Splits a block; returns the index where the new |key| will be inserted
    Index *split_block(Index *index, uint32_t key) {
      int block = index - get_block_index(0);
      // the new block will have the same size as the old one
      Index *new_index = add_block(block + 1, index->block_size());

      uint8_t *src = get_block_data(index);
      uint32_t prev = index->value();
      uint32_t delta;
      int old_key_count = index->key_count();
      int old_used_size = index->used_size();

      // roughly skip half of the data
      for (int i = 1; i < old_key_count / 2; i++) {
        int delta_size = read_int(src, &delta);
        prev += delta;
        src += delta_size;
      }
      index->set_key_count(index->key_count() / 2);
      index->set_used_size(src - get_block_data(index));

      // the next delta will be the initial key of the new block
      src += read_int(src, &delta);
      prev += delta;
      new_index->set_value(prev);

      // now copy the remaining data
      uint8_t *dst = get_block_data(new_index);
      new_index->set_used_size((get_block_data(index) + old_used_size) - src);
      ::memcpy(dst, src, new_index->used_size());

      // and update all counters
      new_index->set_key_count(old_key_count - index->key_count());

      // now figure out whether the key will be inserted in the old or
      // the new block
      return (key >= new_index->value() ? new_index : index);
    }

    // Inserts a new |key| in a block. This method has 3 code paths: it can
    // prepend the key to the block, append it to the block or insert it
    // in the middle
    int insert_key_in_block(Index *index, uint32_t key, uint32_t flags) {
      int slot = 0;
      ham_assert(index->key_count() > 0);

      // fail if the key already exists
      if (index->value() == key)
        throw Exception(HAM_DUPLICATE_KEY);

      // Replace the first key with its delta?
      if (flags & PBtreeNode::kInsertPrepend) {
        uint32_t delta = index->value() - key;
        index->set_value(key);

        int required_space = calculate_delta_size(delta);
        uint8_t *p = get_block_data(index);

        if (index->used_size() > 0)
          ::memmove(p + required_space, p, index->used_size());
        write_int(p, delta);

        Globals::ms_bytes_before_compression += sizeof(uint32_t);
        Globals::ms_bytes_after_compression += required_space;
        index->set_key_count(index->key_count() + 1);
        index->set_used_size(index->used_size() + required_space);
        return (0);
      }

      // fast-forward to the position of the new key
      uint32_t prev;
      uint8_t *p = fast_forward_to_key(index, key, &prev, &slot);

      // again make sure that we don't have a duplicate key
      if (key == prev)
        throw Exception(HAM_DUPLICATE_KEY);

      // reached the end of the block? then append the new key
      if (slot == (int)index->key_count()) {
        key -= prev;
        int size = write_int(p, key);
        Globals::ms_bytes_before_compression += sizeof(uint32_t);
        Globals::ms_bytes_after_compression += size;
        index->set_used_size(index->used_size() + size);
        index->set_key_count(index->key_count() + 1);
        return (slot);
      }

      // otherwise read the next key at |position + 1|, because
      // its delta will change when the new key is inserted
      uint32_t next_key;
      uint8_t *next_p = p + read_int(p, &next_key);
      next_key += prev;

      if (next_key == key)
        throw Exception(HAM_DUPLICATE_KEY);

      // how much additional space is required to store the delta of the
      // new key *and* the updated delta of the next key?
      int required_space = calculate_delta_size(key - prev)
                            + calculate_delta_size(next_key - key)
                            // minus the space that next_key currently occupies
                            - (int)(next_p - p);

      // create a gap large enough for the two deltas
      ::memmove(p + required_space, p, index->used_size()
                      - (p - get_block_data(index)));

      // now insert the new key
      p += write_int(p, key - prev);
      // and the delta of the next key
      p += write_int(p, next_key - key);

      index->set_key_count(index->key_count() + 1);
      index->set_used_size(index->used_size() + required_space);

      return (slot + 1);
    }
#endif

    // fast-forwards to the specified position in a block
    uint8_t *fast_forward_to_position(Index *index, int position,
                    uint32_t *pkey) const {
      ham_assert(position > 0 && position <= (int)index->key_count());
      uint8_t *p = get_block_data(index);
      uint32_t key = index->value();

      uint32_t delta;
      for (int i = 1; i <= position; i++) {
        p += read_int(p, &delta);
        key += delta;
      }

      *pkey = key;
      return (p);
    }

    // fast-forwards to the specified key in a block
    uint8_t *fast_forward_to_key(Index *index, uint32_t key, uint32_t *pprev,
                    int *pslot) {
      uint32_t delta;
      uint8_t *p = get_block_data(index);

      *pprev = index->value();
      if (key < *pprev) {
        *pslot = 0;
        return (p);
      }

      for (int i = 0; i < (int)index->key_count() - 1; i++) {
        uint8_t *next = p + read_int(p, &delta);
        if (*pprev + delta >= key) {
          *pslot = i;
          return (p);
        }
        p = next;
        *pprev += delta;
      }

      *pslot = index->key_count();
      return (p);
    }

    // this assumes that there is a value to be read
    int read_int(const uint8_t *in, uint32_t *out) const {
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

    // returns the compressed size of |value|
    int calculate_delta_size(uint32_t value) {
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

    // writes |value| to |p|
    int write_int(uint8_t *p, uint32_t value) const {
      ham_assert(value > 0);
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
};

} // namespace Zint32

} // namespace hamsterdb

#endif /* HAM_BTREE_KEYS_VARBYTE_H */
