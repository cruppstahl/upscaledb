/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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

#ifndef HAM_BTREE_KEYS_ZINT32_H
#define HAM_BTREE_KEYS_ZINT32_H

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
HAM_PACK_0 struct HAM_PACK_1 VarbyteIndex {
  enum {
    // Initial size of a new block
    kInitialBlockSize = 16,

    // Grow blocks by this factor
    kGrowFactor = 16,
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

  // the total size of this block; max 511 bytes (kMaxBlockSize)
  unsigned int block_size : 9;

  // used size of this block; max 511 bytes
  unsigned int used_size : 9;

  // the number of keys in this block; max 511 (kMaxKeysPerBlock)
  unsigned int key_count : 9;
} HAM_PACK_2;
#include "1base/packstop.h"

class Zint32KeyList : public BlockKeyList<VarbyteIndex>
{
  public:
    enum {
      // Maximum block size, in bytes
      kMaxBlockSize = 256,

      // Maximum keys per block (9 bits)
      kMaxKeysPerBlock = 511,
    };

    // Constructor
    Zint32KeyList(LocalDatabase *db)
      : BlockKeyList<VarbyteIndex>(db) {
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

      int slot = 0;
      find_impl(*(uint32_t *)hkey->data, pcmp, &slot);
      return (slot);
    }

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
      if (index->used_size + 5 > index->block_size) {
        // already reached max. size? then perform a split
        if (index->block_size >= kMaxBlockSize
             || index->key_count == kMaxKeysPerBlock) {
          if (flags & PBtreeNode::kInsertPrepend) {
            index = add_block(0, VarbyteIndex::kInitialBlockSize);
          }
          else if (flags & PBtreeNode::kInsertAppend) {
            slot += index->key_count;
            index = add_block(get_block_count(),
                            VarbyteIndex::kInitialBlockSize);
          }
          else {
            Index *new_index = split_block(index, key);
            if (index != new_index) {
              slot += index->key_count;
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

      // is there just one key left in that block? then reduce the counters
      if (index->key_count == 1) {
        index->key_count = 0;
        index->used_size = 0;
      }
      // otherwise remove the key from the block. This does not change
      // the size of the block!
      else {
        erase_key_from_block(index, position_in_block);
      }

      // if the block is now empty then remove it, unless it's the last block
      if (index->key_count == 0 && get_block_count() > 1) {
        remove_block(index);
      }

      ham_assert(check_integrity(0, node_count - 1));
    }

    // Copies all keys from this[sstart] to dest[dstart]; this method
    // is used to split btree nodes.
    //
    // TODO this function is too complex. rewrite it!
    void copy_to(int sstart, size_t node_count, Zint32KeyList &dest,
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
      if (dsti->block_size < srci->block_size)
        dest.grow_block(dsti, srci->block_size);

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
          srckey = srci->value + delta;
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
          srckey = srci->value;
          src_position_in_block = 1;
        }

        if (dst_position_in_block == 0) {
          dsti->value = srckey;
          dstkey = srckey;
        }
        else {
          d += write_int(d, srckey - dstkey);
        }
        dsti->key_count++;

        // now copy the remaining keys of the first block
        for (int i = src_position_in_block; i < srci->key_count; i++) {
          s += read_int(s, &delta); // TODO use memcpy!
          d += write_int(d, delta);
          dsti->key_count++;
        }
        srci->key_count -= dsti->key_count;
        if (srci->key_count == 1)
          srci->used_size = 0;
        else
          srci->used_size -= s - start_s;
        dsti->used_size += d - start_d;

        index = srci + 1;
      }
      // |src_position_in_block| is > 0 - copy the current block, then
      // fall through
      else {
        if (dsti->key_count > 0) {
          uint32_t data[kMaxKeysPerBlock];
          uncompress_block(srci, data);

          uint32_t dstkey;
          uint8_t *d;
          if (dsti->key_count == 1) {
            d = dest.get_block_data(dsti);
            dstkey = dsti->value;
          }
          else {
            d = dest.fast_forward_to_position(dsti, dsti->key_count - 1,
                            &dstkey);
          }

          uint8_t *d_start = d;
          int src_count = srci->key_count;
          for (int i = src_position_in_block; i < src_count; i++) {
            d += write_int(d, data[i] - dstkey);
            dstkey = data[i];
            dsti->key_count++;
            srci->key_count--;
          }
          dsti->used_size += d - d_start;
        }
        else {
          copy_blocks(srci, dest, dsti);
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
        dsti = dest.add_block(dest.get_block_count(), index->block_size);
        copy_blocks(index, dest, dsti);
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
      if (!BlockKeyList<VarbyteIndex>::check_integrity(node_count))
        return (false);

      Index *index = get_block_index(0);
      Index *end = get_block_index(get_block_count());

      for (; index < end; index++) {
        if (index->key_count == 1)
          ham_assert(index->used_size == 0);

        if (index->used_size > index->block_size) {
          ham_trace(("Used block size %d exceeds allocated size %d",
                        (int)index->used_size,
                        (int)index->block_size));
          throw Exception(HAM_INTEGRITY_VIOLATED);
        }
      }

      /*
      for (; index < end; index++) {
        uint8_t *p = get_block_data(index);
        for (int i = 1; i < index->key_count; i++) {
          uint32_t delta;
          p += read_int(p, &delta);
        }
        ham_assert(index->used_size == p - get_block_data(index));
      }
      */
      return (true);
    }

    // Prints a key to |out| (for debugging)
    void print(Context *, int slot, std::stringstream &out) const {
      out << value(slot);
    }

  private:
    // Uncompresses a whole block
    void uncompress_block(Index *index, uint32_t *data) {
      data[0] = index->value;
      uint8_t *p = get_block_data(index);
      for (int i = 1; i < index->key_count; i++) {
        uint32_t delta;
        p += read_int(p, &delta);
        data[i] = data[i - 1] + delta;
      }
    }

    // Finds a key; returns a pointer to its compressed location.
    // Returns the compare result in |*pcmp| and the slot of the |key| in
    // |*pslot|.
    uint8_t *find_impl(uint32_t key, int *pcmp, int *pslot) {
      // first perform a linear search through the index
      Index *index = find_index(key, pslot);

      // key is the new minimum in this node?
      if (key < index->value) {
        ham_assert(*pslot == -1);
        *pcmp = -1;
        return (get_block_data(index));
      }

      if (index->value == key) {
        *pcmp = 0;
        return (get_block_data(index));
      }

      // then search in the compressed blog
      uint32_t delta, prev = index->value;
      uint8_t *p = get_block_data(index);
      int slot;
      for (slot = 1; slot < index->key_count; slot++) {
        p += read_int(p, &delta);
        prev += delta;

        if (prev >= key) {
          *pslot += slot;
          *pcmp = (prev == key) ? 0 : +1;
          return (p);
        }
      }

      *pcmp = +1;
      *pslot += slot;
      return (p);
    }

    // Inserts a key in the specified block
    PBtreeNode::InsertResult insert_impl(Index *index, uint32_t key,
                    int skipped_slots, uint32_t flags) {
      // first key in an empty block? then don't store a delta
      if (index->key_count == 0) {
        index->key_count = 1;
        index->value = key;
        ham_assert(index->used_size == 0);
        return (PBtreeNode::InsertResult(0, skipped_slots));
      }

      // now prepend, append or insert
      skipped_slots += insert_key_in_block(index, key, flags);

      return (PBtreeNode::InsertResult(0, skipped_slots));
    }

    // Prints all keys of a block to stdout (for debugging)
    void print_block(Index *index) const {
      uint32_t key = index->value;
      std::cout << "0: " << key << std::endl;

      uint8_t *p = get_block_data(index);
      for (int i = 1; i < index->key_count; i++) {
        uint32_t delta;
        p += read_int(p, &delta);
        key += delta;
        std::cout << i << ": " << key << std::endl;
      }
    }

    // Splits a block; returns the index where the new |key| will be inserted
    Index *split_block(Index *index, uint32_t key) {
      int block = index - get_block_index(0);
      // the new block will have the same size as the old one
      Index *new_index = add_block(block + 1, index->block_size);

      uint8_t *src = get_block_data(index);
      uint32_t prev = index->value;
      uint32_t delta;
      int old_key_count = index->key_count;
      int old_used_size = index->used_size;

      // roughly skip half of the data
      for (int i = 1; i < old_key_count / 2; i++) {
        int delta_size = read_int(src, &delta);
        prev += delta;
        src += delta_size;
      }
      index->key_count /= 2;
      index->used_size = src - get_block_data(index);

      // the next delta will be the initial key of the new block
      src += read_int(src, &delta);
      prev += delta;
      new_index->value = prev;

      // now copy the remaining data
      uint8_t *dst = get_block_data(new_index);
      new_index->used_size = (get_block_data(index) + old_used_size) - src;
      ::memcpy(dst, src, new_index->used_size);

      // and update all counters
      new_index->key_count = old_key_count - index->key_count;

      // now figure out whether the key will be inserted in the old or
      // the new block
      if (key >= new_index->value) {
        return (new_index);
      }
      else {
        return (index);
      }
    }

    // Implementation of vacuumize()
    virtual void vacuumize_impl(bool internal) {
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
          index->block_size = VarbyteIndex::kInitialBlockSize;
        else
          index->block_size = index->used_size;

        next_offset += index->block_size;
      }

      set_used_size((block_data - m_data) + next_offset);
    }

    // Inserts a new |key| in a block. This method has 3 code paths: it can
    // prepend the key to the block, append it to the block or insert it
    // in the middle
    int insert_key_in_block(Index *index, uint32_t key, uint32_t flags) {
      int slot = 0;
      ham_assert(index->key_count > 0);

      // fail if the key already exists
      if (index->value == key)
        throw Exception(HAM_DUPLICATE_KEY);

      // Replace the first key with its delta?
      if (flags & PBtreeNode::kInsertPrepend) {
        uint32_t delta = index->value - key;
        index->value = key;

        int required_space = calculate_delta_size(delta);
        uint8_t *p = get_block_data(index);

        if (index->used_size > 0)
          ::memmove(p + required_space, p, index->used_size);
        write_int(p, delta);

        Globals::ms_bytes_before_compression += sizeof(uint32_t);
        Globals::ms_bytes_after_compression += required_space;
        index->key_count++;
        index->used_size += required_space;
        return (0);
      }

      // fast-forward to the position of the new key
      uint32_t prev;
      uint8_t *p = fast_forward_to_key(index, key, &prev, &slot);

      // again make sure that we don't have a duplicate key
      if (key == prev)
        throw Exception(HAM_DUPLICATE_KEY);

      // reached the end of the block? then append the new key
      if (slot == index->key_count) {
        key -= prev;
        int size = write_int(p, key);
        Globals::ms_bytes_before_compression += sizeof(uint32_t);
        Globals::ms_bytes_after_compression += size;
        index->used_size += size;
        index->key_count++;
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
      ::memmove(p + required_space, p, index->used_size
                      - (p - get_block_data(index)));

      // now insert the new key
      p += write_int(p, key - prev);
      // and the delta of the next key
      p += write_int(p, next_key - key);

      Globals::ms_bytes_before_compression += sizeof(uint32_t);
      Globals::ms_bytes_after_compression += required_space;

      index->key_count++;
      index->used_size += required_space;

      return (slot + 1);
    }

    // Erases a key from a block
    void erase_key_from_block(Index *index, int position) {
      ham_assert(index->key_count > 1);

      uint8_t *p = get_block_data(index);

      // erase the first key?
      if (position == 0) {
        uint32_t second, first = index->value;
        uint8_t *start = p;
        p += read_int(p, &second);
        // replace the first key with the second key (uncompressed)
        index->value = first + second;
        // shift all remaining deltas to the left
        index->key_count--;
        if (index->key_count == 1)
          index->used_size = 0;
        else {
          ::memmove(start, p, index->used_size);
          index->used_size -= p - start;
        }
        return;
      }

      // otherwise fast-forward to the position of the key and remove it;
      // then update the delta of the next key
      uint32_t key = index->value;
      uint32_t delta;
      uint8_t *prev_p = p;
      for (int i = 1; i < position; i++) {
        prev_p = p;
        p += read_int(p, &delta);
        key += delta;
      }

      if (index->key_count == 2) {
        index->used_size = 0;
        index->key_count--;
        return;
      }

      // cut off the last key in the block?
      if (position == index->key_count - 1) {
        index->used_size -= (get_block_data(index) + index->used_size) - p;
        index->key_count--;
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
      ::memmove(prev_p, p,
              (get_block_data(index) + index->used_size) - prev_p);

      index->used_size -= p - prev_p;
      index->key_count--;
    }

    // Copies two blocks; assumes that the new block |dst| has been properly
    // allocated
    void copy_blocks(Index *src, Zint32KeyList &dest, Index *dst) {
      dst->value     = src->value;
      dst->used_size = src->used_size;
      dst->key_count = src->key_count;

      ::memcpy(dest.get_block_data(dst), get_block_data(src), src->used_size);
    }

    // Returns a decompressed value
    uint32_t value(int slot) const {
      int position_in_block;
      Index *index = find_block_by_slot(slot, &position_in_block);

      if (position_in_block == 0)
        return (index->value);

      uint32_t key;
      fast_forward_to_position(index, position_in_block, &key);
      return (key);
    }
    
    // fast-forwards to the specified position in a block
    uint8_t *fast_forward_to_position(Index *index, int position,
                    uint32_t *pkey) const {
      ham_assert(position > 0 && position <= index->key_count);
      uint8_t *p = get_block_data(index);
      uint32_t key = index->value;

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

      *pprev = index->value;
      if (key < *pprev) {
        *pslot = 0;
        return (p);
      }

      for (int i = 0; i < index->key_count - 1; i++) {
        uint8_t *next = p + read_int(p, &delta);
        if (*pprev + delta >= key) {
          *pslot = i;
          return (p);
        }
        p = next;
        *pprev += delta;
      }

      *pslot = index->key_count;
      return (p);
    }

    // this assumes that there is a value to be read
    int read_int(const uint8_t *in, uint32_t *out) const {
      *out = in[0] & 0x7F;
      if (in[0] >= 128) {
        return (1);
      }
      *out = ((in[1] & 0x7FU) << 7) | *out;
      if (in[1] >= 128) {
        return (2);
      }
      *out = ((in[2] & 0x7FU) << 14) | *out;
      if (in[2] >= 128) {
        return (3);
      }
      *out = ((in[3] & 0x7FU) << 21) | *out;
      if (in[3] >= 128) {
        return (4);
      }
      *out = ((in[4] & 0x7FU) << 28) | *out;
      return (5);
    }

    // writes |value| to |p|
    int write_int(uint8_t *p, uint32_t value) const {
      ham_assert(value > 0);
      if (value < (1U << 7)) {
        *p = static_cast<uint8_t>(value | (1U << 7));
        return (1);
      }
      else if (value < (1U << 14)) {
        *p = extract7bits<0>(value);
        ++p;
        *p = extract7bitsmaskless<1>(value) | (1U << 7);
        return (2);
      }
      else if (value < (1U << 21)) {
        *p = extract7bits<0>(value);
        ++p;
        *p = extract7bits<1>(value);
        ++p;
        *p = extract7bitsmaskless<2>(value) | (1U << 7);
        return (3);
      }
      else if (value < (1U << 28)) {
        *p = extract7bits<0>(value);
        ++p;
        *p = extract7bits<1>(value);
        ++p;
        *p = extract7bits<2>(value);
        ++p;
        *p = extract7bitsmaskless<3>(value) | (1U << 7);
        return (4);
      }
      else {
        *p = extract7bits<0>(value);
        ++p;
        *p = extract7bits<1>(value);
        ++p;
        *p = extract7bits<2>(value);
        ++p;
        *p = extract7bits<3>(value);
        ++p;
        *p = extract7bitsmaskless<4>(value) | (1U << 7);
        return (5);
      }
    }

    // returns the compressed size of |value|
    int calculate_delta_size(uint32_t value) {
      if (value < (1U << 7)) {
        return (1);
      }
      else if (value < (1U << 14)) {
        return (2);
      }
      else if (value < (1U << 21)) {
        return (3);
      }
      else if (value < (1U << 28)) {
        return (4);
      }
      else {
        return (5);
      }
    }

    template<uint32_t i>
    uint8_t extract7bits(const uint32_t val) const {
      return static_cast<uint8_t>((val >> (7 * i)) & ((1U << 7) - 1));
    }

    template<uint32_t i>
    uint8_t extract7bitsmaskless(const uint32_t val) const {
      return static_cast<uint8_t>((val >> (7 * i)));
    }

    // helper variable to avoid returning pointers to local memory
    uint32_t m_dummy;
};

} // namespace Zint32

} // namespace hamsterdb

#endif /* HAM_BTREE_KEYS_ZINT32_H */
