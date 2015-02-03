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

#ifndef HAM_BTREE_KEYS_GROUPVARINT_H
#define HAM_BTREE_KEYS_GROUPVARINT_H

#include <sstream>
#include <iostream>
#include <algorithm>

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

const uint32_t varintgb_mask[4] = { 0xFF, 0xFFFF, 0xFFFFFF, 0xFFFFFFFF };

// This structure is an "index" entry which describes the location
// of a variable-length block
#include "1base/packstart.h"
HAM_PACK_0 struct HAM_PACK_1 GroupVarintIndex {
  enum {
    // Initial size of a new block (1 bit per key = 16 bytes)
    kInitialBlockSize = 17, // 1 + 4 * 4

    // Grow blocks by this factor
    kGrowFactor = 17,
  };

  // initialize this block index
  void initialize(uint32_t offset, uint32_t block_size) {
    ::memset(this, 0, sizeof(*this));
    this->offset = offset;
    this->block_size = block_size;
  }

  // returns the used block of the block
  uint32_t get_used_size() const {
    return (used_size);
  }

  // returns the total block size
  uint32_t get_block_size() const {
    return (block_size);
  }

  // sets the total block size
  void set_block_size(uint32_t size) {
    block_size = size;
  }

  // offset of the payload, relative to the beginning of the payloads
  // (starts after the Index structures)
  uint16_t offset;

  // the start value of this block
  uint32_t value;

  // the total size of this block; max 255 bytes
  unsigned int block_size : 8;

  // used size of this block; max 255 bytes
  unsigned int used_size : 8;

  // the number of keys in this block; max 255 (kMaxKeysPerBlock)
  unsigned int key_count : 8;
} HAM_PACK_2;
#include "1base/packstop.h"

class GroupVarintKeyList : public BlockKeyList<GroupVarintIndex>
{
  public:
    enum {
      // Maximum GroupVarints per block
      kMaxGroupVarintsPerBlock = 8,

      // Maximum keys per block
      kMaxKeysPerBlock = kMaxGroupVarintsPerBlock * 4
    };

    // Constructor
    GroupVarintKeyList(LocalDatabase *db)
      : BlockKeyList<GroupVarintIndex>(db) {
    }

    // Returns the key at the given |slot|.
    void get_key(Context *, int slot, ByteArray *arena, ham_key_t *dest,
                    bool deep_copy = true) {
      // uncompress the key value, store it in a member (not in a local
      // variable!), otherwise couldn't return a pointer to it
      m_dummy = value(slot);

      dest->size = sizeof(uint32_t);
      if (deep_copy == false) {
        dest->data = (uint8_t *)&m_dummy;
        return;
      }

      // allocate memory (if required)
      if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
        arena->resize(dest->size);
        dest->data = arena->get_ptr();
      }

      *(uint32_t *)dest->data = m_dummy;
    }

    // Searches the node for the key and returns the slot of this key
    template<typename Cmp>
    int find(Context *, size_t node_count, const ham_key_t *hkey,
                    Cmp &comparator, int *pcmp) {
      ham_assert(get_block_count() > 0);

      uint32_t key = *(uint32_t *)hkey->data;
      int slot = 0;

      // first perform a linear search through the index
      Index *index = find_index(key, &slot);

      // key is the new minimum in this node?
      if (key < index->value) {
        ham_assert(slot == -1);
        *pcmp = -1;
        return (slot);
      }

      if (index->value == key) {
        *pcmp = 0;
        return (slot);
      }

      // uncompress the block, then perform a linear search
      uint32_t data[kMaxKeysPerBlock];
      uncompress_block(index, data);

      uint32_t *begin = &data[0];
      uint32_t *end = &data[index->key_count - 1];
      return (slot + lower_bound_search(begin, end, key, pcmp));
    }

    // Inserts a key
    template<typename Cmp>
    PBtreeNode::InsertResult insert(Context *, size_t node_count,
                    const ham_key_t *hkey, uint32_t flags, Cmp &comparator,
                    int /* unused */ slot) {
      ham_assert(check_integrity(0, node_count));
      ham_assert(hkey->size == sizeof(uint32_t));

      uint32_t key = *(uint32_t *)hkey->data;

      // if a split is required: vacuumize the node, then retry
      try {
        return (insert_impl(node_count, key, flags));
      }
      catch (Exception &ex) {
        if (ex.code != HAM_LIMITS_REACHED)
          throw (ex);

        vacuumize_impl(false);

        // try again; if it still fails then let the caller handle the
        // exception
        return (insert_impl(node_count, key, flags));
      }
    }

    // Erases the key at the specified |slot|
    void erase(Context *, size_t node_count, int slot) {
      ham_assert(check_integrity(0, node_count));

      // get the block and the position of the key inside the block
      int position_in_block;
      Index *index;
      if (slot == 0) {
        index = get_block_index(0);
        position_in_block = 0;
      }
      else if (slot == (int)node_count) {
        index = get_block_index(get_block_count() - 1);
        position_in_block = index->key_count;
      }
      else
        index = find_block_by_slot(slot, &position_in_block);

      // uncompress the block and remove the key
      uint32_t data[kMaxKeysPerBlock];
      uncompress_block(index, data);

      // delete the first value?
      if (position_in_block == 0) {
        index->value = data[0];
        position_in_block++;
      }

      if (position_in_block < index->key_count - 1) {
        ::memmove(&data[position_in_block - 1], &data[position_in_block],
                sizeof(uint32_t) * (index->key_count - position_in_block - 1));
      }

      // if the block is now empty then remove it, unless it's the last block
      if (index->key_count == 1 && get_block_count() > 1) {
        index->key_count = 0;
        remove_block(index);
      }
      // otherwise decrease key count and compress the block
      else {
        if (--index->key_count > 0) {
          index->used_size = compress_block(index, data);
          ham_assert(index->used_size <= index->block_size);
        }
      }

      ham_assert(check_integrity(0, node_count - 1));
    }

    // Copies all keys from this[sstart] to dest[dstart]; this method
    // is used to split and merge btree nodes.
    void copy_to(int sstart, size_t node_count, GroupVarintKeyList &dest,
                    size_t other_count, int dstart) {
      ham_assert(check_integrity(0, node_count));

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

      // grow destination block
      if (srci->used_size > dsti->block_size)
        dest.grow_block(dsti, srci->used_size - dsti->block_size);

      bool initial_block_used = false;

      // If start offset or destination offset > 0: uncompress both blocks,
      // merge them
      if (src_position_in_block > 0 || dst_position_in_block > 0) {
        uint32_t sdata[kMaxKeysPerBlock];
        uncompress_block(srci, sdata);
        uint32_t ddata[kMaxKeysPerBlock];
        dest.uncompress_block(dsti, ddata);

        uint32_t *d = &ddata[srci->key_count];

        if (src_position_in_block == 0) {
          ham_assert(dst_position_in_block != 0);
          *d = srci->value;
          d++;
        }
        else {
          ham_assert(dst_position_in_block == 0);
          dsti->value = sdata[src_position_in_block - 1];
          src_position_in_block++;
        }
        dsti->key_count++;

        for (int i = src_position_in_block; i < srci->key_count; i++) {
          ddata[dsti->key_count - 1] = sdata[i - 1];
          dsti->key_count++;
        }

        srci->key_count -= dsti->key_count;

        dsti->used_size = dest.compress_block(dsti, ddata);
        ham_assert(dsti->used_size <= dsti->block_size);

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
      for (; srci < get_block_index(get_block_count());
                      srci++, copied_blocks++) {
        if (initial_block_used == true)
          dsti = dest.add_block(dest.get_block_count(), srci->get_block_size());
        else
          initial_block_used = true;

        copy_blocks(srci, dest, dsti);
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

    // Scans all keys; used for the hola* APIs.
    //
    // This method decompresses each block, and then calls the |visitor|
    // to process the decompressed keys.
    void scan(Context *, ScanVisitor *visitor, uint32_t start, size_t count) {
      Index *it = get_block_index(0);
      Index *end = get_block_index(get_block_count());
      for (; it < end; it++) {
        uint32_t data[kMaxKeysPerBlock];
        uncompress_block(it, data);
        (*visitor)(data, it->key_count);
      }
    }

    // Checks the integrity of this node. Throws an exception if there is a
    // violation.
    bool check_integrity(Context *, size_t node_count) const {
      if (!BlockKeyList<GroupVarintIndex>::check_integrity(node_count))
        return (false);

      Index *index = get_block_index(0);
      Index *end = index + get_block_count();
      for (; index < end; index++) {
        if (index->used_size == 0 && index->key_count > 1)
          return (false);
      }
      return (true);
    }

    // Prints a key to |out| (for debugging)
    void print(Context *, int slot, std::stringstream &out) const {
      out << value(slot);
    }

  private:
    // Uncompresses a whole block
    void uncompress_block(Index *index, uint32_t *data) const {
      size_t nvalue;
      if (index->key_count > 1) {
        ham_assert(index->used_size % 4 == 0);
        decodeArray(index->value, (uint32_t *)get_block_data(index),
                        (size_t)index->used_size / 4, data, nvalue);
      }
    }

    // Compresses a whole block
    uint32_t compress_block(Index *index, uint32_t *data) const {
      ham_assert(index->key_count > 0);
      size_t nvalue;
      encodeArray(index->value, data, (size_t)index->key_count - 1,
                      (uint32_t *)get_block_data(index), nvalue);
      return ((uint32_t)(nvalue * sizeof(uint32_t)));
    }

    // Implementation for insert()
    PBtreeNode::InsertResult insert_impl(size_t node_count, uint32_t key,
                    uint32_t flags) {
      int slot = 0;

      // perform a linear search through the index and get the block
      // which will receive the new key
      Index *index = find_index(key, &slot);

      // first key in an empty block? then don't store a delta
      if (index->key_count == 0) {
        index->key_count = 1;
        index->value = key;
        return (PBtreeNode::InsertResult(0, slot));
      }

      // fail if the key already exists
      if (index->value == key)
        throw Exception(HAM_DUPLICATE_KEY);

      uint32_t old_data[kMaxKeysPerBlock];
      uint32_t new_data[kMaxKeysPerBlock];
      uint32_t *data = &old_data[0];
      uncompress_block(index, data);

      // if the block is empty then just write the new key
      if (index->key_count == 1) {
        // grow the block if required
        uint32_t required_size = 2 * sizeof(uint32_t);
        if (required_size > index->block_size) {
          grow_block(index, required_size - index->get_block_size());
        }

        // swap |key| and |index->value| (cannot use std::swap because
        // g++ refuses swapping a packed value)
        if (key < index->value) {
          uint32_t tmp = index->value;
          index->value = key;
          key = tmp;
        }

        // overwrite the first entry
        data[0] = key;
        slot += 1;

        index->key_count++;

        // then compress and store the block
        index->used_size = compress_block(index, data);
        ham_assert(index->used_size <= index->block_size);

        ham_assert(check_integrity(0, node_count + 1));
        return (PBtreeNode::InsertResult(0, slot));
      }

      bool needs_compress = false;

      // if the block is full then split it
      if (index->key_count + 1 == kMaxKeysPerBlock) {
        int block = index - get_block_index(0);

        // prepend the key?
        if (key < index->value) {
          Index *new_index = add_block(block + 1,
                          GroupVarintIndex::kInitialBlockSize);
          new_index->key_count = 1;
          new_index->value = key;

          // swap the indices
          Index tmp = *index;
          *index = *new_index;
          *new_index = tmp;

          // then compress and store the block
          index->used_size = compress_block(index, new_data);
          ham_assert(index->used_size <= index->block_size);
          ham_assert(check_integrity(0, node_count + 1));
          return (PBtreeNode::InsertResult(0, slot < 0 ? 0 : slot));
        }

        // append the key?
        if (key > old_data[index->key_count - 2]) {
          Index *new_index = add_block(block + 1,
                          GroupVarintIndex::kInitialBlockSize);
          new_index->key_count = 1;
          new_index->value = key;

          // then compress and store the block
          new_index->used_size = compress_block(new_index, new_data);
          ham_assert(new_index->used_size <= new_index->block_size);
          ham_assert(check_integrity(0, node_count + 1));
          return (PBtreeNode::InsertResult(0, slot + index->key_count));
        }

        // otherwise split the block in the middle and move half of the keys
        // to the new block.
        //
        // The pivot position is aligned to 4.
        uint32_t to_copy = (index->key_count / 2) & ~0x03;
        if (to_copy == 0)
          to_copy = (index->key_count / 2) & ~0x03;
        ham_assert(to_copy > 0);

        uint32_t new_key_count = index->key_count - to_copy - 1;
        uint32_t new_value = data[to_copy];

        // once more check if the key already exists
        if (new_value == key)
          throw Exception(HAM_DUPLICATE_KEY);

        to_copy++;
        ::memmove(&new_data[0], &data[to_copy],
                        sizeof(int32_t) * (index->key_count - to_copy));

        // Now create a new block. This can throw, but so far we have not
        // modified existing data.
        Index *new_index = add_block(block + 1, index->block_size);
        new_index->value = new_value;
        new_index->key_count = new_key_count;

        // Adjust the size of the old block
        index->key_count -= new_key_count;

        // Now check if the new key will be inserted in the old or the new block
        if (key >= new_index->value) {
          index->used_size = compress_block(index, data);
          ham_assert(index->used_size <= index->block_size);
          slot += index->key_count;

          // continue with the new block
          index = new_index;
          data = new_data;
        }
        else {
          new_index->used_size = compress_block(new_index, new_data);
          ham_assert(new_index->used_size <= new_index->block_size);
        }

        needs_compress = true;

        // fall through...
      }
      // if required, increase block capacity by 5 bytes; this is
      // the minimum size for a new GroupVarint (1 byte for the index
      // + max. 4 bytes for 1 compressed integer)
      else if (index->used_size + 5 > index->block_size) {
        uint32_t additional_size = 1 + sizeof(uint32_t);
        // since grow_block() can throw: flush the modified block, otherwise
        // it's in an inconsistent state 
        if (needs_compress && get_used_size() + additional_size >= m_range_size) {
          index->used_size = compress_block(index, data);
          ham_assert(index->used_size <= index->block_size);
        }

        grow_block(index, additional_size);
      }


      // swap |key| and |index->value| (cannot use std::swap because
      // gcc refuses swapping a packed value)
      if (key < index->value) {
        uint32_t tmp = index->value;
        index->value = key;
        key = tmp;
      }

      // locate the position of the new key
      uint32_t *begin = &data[0];
      uint32_t *end = &data[index->key_count - 1];
      uint32_t *it = std::lower_bound(begin, end, key);

      // if the new key already exists then throw an exception
      if (it < end && *it == key) {
        if (needs_compress) {
          index->used_size = compress_block(index, data);
          ham_assert(index->used_size <= index->block_size);
        }
        throw Exception(HAM_DUPLICATE_KEY);
      }

      // insert the new key
      if (it < end)
        ::memmove(it + 1, it, (end - it) * sizeof(uint32_t));
      *it = key;
      slot += it - &data[0] + 1;

      index->key_count++;

      // then compress and store the block
      index->used_size = compress_block(index, data);
      ham_assert(index->used_size <= index->block_size);

      ham_assert(check_integrity(0, node_count + 1));
      return (PBtreeNode::InsertResult(0, slot));
    }

    // Copies two blocks; assumes that the new block |dst| has been properly
    // allocated
    void copy_blocks(Index *src, GroupVarintKeyList &dest, Index *dst) {
      dst->value     = src->value;
      dst->key_count = src->key_count;
      dst->used_size = src->used_size;

      ::memcpy(dest.get_block_data(dst), get_block_data(src),
                      src->get_block_size());
    }

    // Prints all keys of a block to stdout (for debugging)
    void print_block(Index *index) const {
      uint32_t key = index->value;
      std::cout << "0: " << key << std::endl;

      uint32_t data[kMaxKeysPerBlock];
      uncompress_block(index, data);

      for (uint32_t i = 1; i < index->key_count; i++)
        std::cout << i << ": " << data[i - 1] << std::endl;
    }

    // Implementation of vacuumize()
    void vacuumize_impl(bool internal) {
      // make a copy of all indices
      bool requires_sort = false;
      int block_count = get_block_count();
	  SortHelper *s = (SortHelper *)::alloca(block_count * sizeof(SortHelper));
      for (int i = 0; i < block_count; i++) {
        s[i].index = i;
        s[i].offset = get_block_index(i)->offset;
        if (i > 0 && requires_sort == false && s[i].offset < s[i - 1].offset)
          requires_sort = true;
      }

      // sort them by offset; this is a very expensive call. only sort if
      // it's absolutely necessary!
      if (requires_sort)
        std::sort(&s[0], &s[block_count], sort_by_offset);

      // shift all blocks "to the left" and reduce their size as much as
      // possible
      uint32_t next_offset = 0;
      uint8_t *block_data = &m_data[8 + sizeof(Index) * block_count];

      for (int i = 0; i < block_count; i++) {
        Index *index = get_block_index(s[i].index);

        if (index->offset != next_offset) {
          // shift block data to the left
          memmove(&block_data[next_offset], &block_data[index->offset],
                          index->used_size);
          // overwrite the offset
          index->offset = next_offset;
        }

        if (index->used_size == 0)
          index->block_size = GroupVarintIndex::kInitialBlockSize;
        else
          index->block_size = index->used_size;

        next_offset += index->block_size;
      }

      set_used_size((block_data - m_data) + next_offset);
    }

    // Returns a decompressed value
    uint32_t value(int slot) const {
      int position_in_block;
      Index *index = find_block_by_slot(slot, &position_in_block);

      if (position_in_block == 0)
        return (index->value);

      ham_assert(position_in_block < index->key_count);

      uint32_t data[kMaxKeysPerBlock];
      uncompress_block(index, data);
      return (data[position_in_block - 1]);
    }

    void encodeArray(uint32_t initial, uint32_t *in, const size_t length,
                    uint32_t *out, size_t &nvalue) const {
      uint8_t *bout = reinterpret_cast<uint8_t *> (out);
      const uint8_t *const initbout = reinterpret_cast<uint8_t *> (out);
      *out = static_cast<uint32_t>(length);
      bout += 4;

      size_t k = 0;
      for (; k + 3 < length;) {
        uint8_t * keyp = bout++;
        *keyp = 0;
        for (int j = 0; j < 8; j += 2, ++k) {
          const uint32_t val = in[k] - initial;
          initial = in[k];
          if (val < (1U << 8)) {
            *bout++ = static_cast<uint8_t> (val);
          } else if (val < (1U << 16)) {
            *bout++ = static_cast<uint8_t> (val);
            *bout++ = static_cast<uint8_t> (val >> 8);
            *keyp |= static_cast<uint8_t>(1 << j);
          } else if (val < (1U << 24)) {
            *bout++ = static_cast<uint8_t> (val);
            *bout++ = static_cast<uint8_t> (val >> 8);
            *bout++ = static_cast<uint8_t> (val >> 16);
            *keyp |= static_cast<uint8_t>(2 << j);
          } else {
            // the compiler will do the right thing
            *reinterpret_cast<uint32_t *> (bout) = val;
            bout += 4;
            *keyp |= static_cast<uint8_t>(3 << j);
          }
        }
      }
      if (k < length) {
        uint8_t * keyp = bout++;
        *keyp = 0;
        for (int j = 0; k < length && j < 8; j += 2, ++k) {
          const uint32_t val = in[k] - initial;
          initial = in[k];
          if (val < (1U << 8)) {
            *bout++ = static_cast<uint8_t> (val);
          } else if (val < (1U << 16)) {
            *bout++ = static_cast<uint8_t> (val);
            *bout++ = static_cast<uint8_t> (val >> 8);
            *keyp |= static_cast<uint8_t>(1 << j);
          } else if (val < (1U << 24)) {
            *bout++ = static_cast<uint8_t> (val);
            *bout++ = static_cast<uint8_t> (val >> 8);
            *bout++ = static_cast<uint8_t> (val >> 16);
            *keyp |= static_cast<uint8_t>(2 << j);
          } else {
            // the compiler will do the right thing
            *reinterpret_cast<uint32_t *> (bout) = val;
            bout += 4;
            *keyp |= static_cast<uint8_t>(3 << j);
          }
        }
      }
      size_t storageinbytes = bout - initbout;
      while (needPaddingTo32Bits(storageinbytes)) {
        storageinbytes++;
      }
      assert((storageinbytes % 4) == 0);
      nvalue = storageinbytes / 4;
    }

    template <class T>
    inline bool needPaddingTo32Bits(T value) const {
      return value & 3;
    }

    const uint32_t *decodeArray(uint32_t initial, const uint32_t *in,
                    const size_t length, uint32_t *out, size_t &nvalue) const {
      const uint8_t * inbyte = reinterpret_cast<const uint8_t *> (in);
      nvalue = *in;
      inbyte += 4;

      const uint8_t * const endbyte = reinterpret_cast<const uint8_t *> (in
                + length);
      const uint32_t * const endout(out + nvalue);
      uint32_t val;

      while (endbyte > inbyte + 1 + 4 * 4) {
    	inbyte = decodeGroupVarIntDelta(inbyte, &initial, out);
        out += 4;
      }
      while (endbyte > inbyte) {
        uint8_t key = *inbyte++;
        for (int k = 0; out < endout and k < 4; k++) {
          const uint32_t howmanybyte = key & 3;
          key = static_cast<uint8_t>(key>>2);
          val = static_cast<uint32_t> (*inbyte++);
          if (howmanybyte >= 1) {
            val |= (static_cast<uint32_t> (*inbyte++) << 8) ;
            if (howmanybyte >= 2) {
              val |= (static_cast<uint32_t> (*inbyte++) << 16) ;
              if (howmanybyte >= 3) {
                val |= (static_cast<uint32_t> (*inbyte++) << 24);
              }
            }
          }
          initial += val;
          *out++ = initial;
        }
        assert(inbyte <= endbyte);
      }
      inbyte = padTo32bits(inbyte);
      return reinterpret_cast<const uint32_t *> (inbyte);
    }

    template <class T>
    T *padTo32bits(T *inbyte) const {
      return reinterpret_cast<T *>((reinterpret_cast<uintptr_t>(inbyte)
                                        + 3) & ~3);
    }

    const uint8_t *decodeGroupVarIntDelta(const uint8_t* in, uint32_t *val,
                    uint32_t* out) const {
      const uint32_t sel = *in++;
      if(sel == 0) {
        out[0] = (* val += static_cast<uint32_t> (in[0]));
        out[1] = (* val += static_cast<uint32_t> (in[1]));
        out[2] = (* val += static_cast<uint32_t> (in[2]));
        out[3] = (* val += static_cast<uint32_t> (in[3]));
        return in + 4;
      }
      const uint32_t sel1 = (sel & 3);
      *val += *((uint32_t*)(in)) & varintgb_mask[sel1];
      *out++ = *val;
      in += sel1 + 1;
      const uint32_t sel2 = ((sel >> 2) & 3);
      *val += *((uint32_t*)(in)) & varintgb_mask[sel2];
      *out++ = *val;
      in += sel2 + 1;
      const uint32_t sel3 = ((sel >> 4) & 3);
      *val += *((uint32_t*)(in)) & varintgb_mask[sel3];
      *out++ = *val;
      in += sel3 + 1;
      const uint32_t sel4 = (sel >> 6);
      *val += *((uint32_t*)(in)) & varintgb_mask[sel4];
      *out++ = *val;
      in += sel4 + 1;
      return in;
    }

    // helper variable to avoid returning pointers to local memory
    uint32_t m_dummy;
};

} // namespace Zint32

} // namespace hamsterdb

#endif /* HAM_BTREE_KEYS_GROUPVARINT_H */
