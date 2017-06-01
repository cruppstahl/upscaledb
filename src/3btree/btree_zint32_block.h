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
 * Base class for key lists where keys are separated in blocks
 */

#ifndef UPS_BTREE_KEYS_BLOCK_H
#define UPS_BTREE_KEYS_BLOCK_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "3btree/btree_node.h"
#include "3btree/btree_keys_base.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

//
// The template classes in this file are wrapped in a separate namespace
// to avoid naming clashes with other KeyLists
//
namespace Zint32 {

// A helper class to sort ranges; used in vacuumize()
struct SortHelper {
  uint32_t offset;
  int index;

  bool operator<(const SortHelper &rhs) const {
    return offset < rhs.offset;
  }
};

static inline bool
sort_by_offset(const SortHelper &lhs, const SortHelper &rhs)
{
  return lhs.offset < rhs.offset;
}

// The BlockCache is used to speed up multiple select() operations for
// a single block. This is frequently used when iterating over a block
// with a cursor.
struct BlockCache {
  BlockCache()
    : is_active(false) {
  }

  bool is_active;
  uint32_t index_value;
  uint32_t data[256]; // TODO replace with kMaxKeysPerBlock
};

// This structure is an "index" entry which describes the location
// of a variable-length block
#include "1base/packstart.h"
UPS_PACK_0 struct UPS_PACK_1 IndexBase {
  // initialize this block index
  void initialize(uint32_t offset, uint8_t *, size_t) {
    ::memset(this, 0, sizeof(*this));
    _offset = offset;
  }

  // returns the offset of the payload
  uint16_t offset() const {
    return _offset;
  }

  // sets the offset of the payload
  void set_offset(uint16_t offset) {
    _offset = offset;
  }

  // returns the initial value
  uint32_t value() const {
    return _value;
  }

  // sets the initial value
  void set_value(uint32_t value) {
    _value = value;
  }

  // returns the highest value
  uint32_t highest() const {
    return _highest;
  }

  // sets the highest value
  void set_highest(uint32_t highest) {
    _highest = highest;
  }

  // offset of the payload, relative to the beginning of the payloads
  // (starts after the Index structures)
  uint16_t _offset;

  // the start value of this block
  uint32_t _value;

  // the highest value of this block
  uint32_t _highest;
} UPS_PACK_2;
#include "1base/packstop.h"

// Base class for a BlockCodec
template <typename Index>
struct BlockCodecBase {
  enum {
    kHasCompressApi = 0,
    kHasFindLowerBoundApi = 0,
    kHasDelApi = 0,
    kHasInsertApi = 0,
    kHasAppendApi = 0,
    kHasSelectApi = 0,
    kCompressInPlace = 0,
  };

  static uint32_t compress_block(Index *index, const uint32_t *in,
                  uint32_t *out) {
    assert(!"shouldn't be here");
    throw Exception(UPS_INTERNAL_ERROR);
  }

  static uint32_t *uncompress_block(Index *index, const uint32_t *block_data,
                  uint32_t *out) {
    assert(!"shouldn't be here");
    throw Exception(UPS_INTERNAL_ERROR);
  }

  static int find_lower_bound(Index *index, const uint32_t *block_data,
                  uint32_t key, uint32_t *result) {
    assert(!"shouldn't be here");
    throw Exception(UPS_INTERNAL_ERROR);
  }

  static bool insert(Index *index, uint32_t *block_data,
                  uint32_t key, int *pslot) {
    assert(!"shouldn't be here");
    throw Exception(UPS_INTERNAL_ERROR);
  }

  static bool append(Index *index, uint32_t *block_data,
                  uint32_t key, int *pslot) {
    assert(!"shouldn't be here");
    throw Exception(UPS_INTERNAL_ERROR);
  }

  template<typename GrowHandler>
  static void del(Index *index, uint32_t *block_data, int slot,
                  GrowHandler *grow_handler) {
    assert(!"shouldn't be here");
    throw Exception(UPS_INTERNAL_ERROR);
  }

  static uint32_t select(Index *index, uint32_t *block_data, int slot) {
    assert(!"shouldn't be here");
    throw Exception(UPS_INTERNAL_ERROR);
  }
};

template<typename BlockIndex, typename BlockCodec>
struct Zint32Codec {
  typedef BlockIndex Index;
  typedef BlockCodec Codec;

  static uint32_t compress_block(Index *index, BlockCache *block_cache,
                    const uint32_t *in, uint32_t *out) {
    block_cache->is_active = false;

    if (Codec::kHasCompressApi)
      return Codec::compress_block(index, in, out);
    assert(!"shouldn't be here");
    throw Exception(UPS_INTERNAL_ERROR);
  }

  static uint32_t *uncompress_block(Index *index, const uint32_t *block_data,
                  uint32_t *out) {
    if (likely(index->key_count() > 1))
      return Codec::uncompress_block(index, block_data, out);
    else
      return out;
  }

  static int find_lower_bound(Index *index, const uint32_t *block_data,
                  uint32_t key, uint32_t *result) {
    if (Codec::kHasFindLowerBoundApi)
      return Codec::find_lower_bound(index, block_data, key, result);

    uint32_t tmp[Index::kMaxKeysPerBlock];
    uint32_t *begin = uncompress_block(index, block_data, &tmp[0]);
    uint32_t *end = begin + index->key_count() - 1;
    uint32_t *it = std::lower_bound(begin, end, key);
    *result = *it;
    return it - begin;
  }

  static bool insert(Index *index, BlockCache *block_cache,
                    uint32_t *block_data, uint32_t key, int *pslot) {
    block_cache->is_active = false;

    if (Codec::kHasInsertApi)
      return Codec::insert(index, block_data, key, pslot);

    // now decode the block
    uint32_t datap[Index::kMaxKeysPerBlock];
    uint32_t *data = uncompress_block(index, block_data, datap);

    // swap |key| and |index->value|
    if (key < index->value()) {
      uint32_t tmp = index->value();
      index->set_value(key);
      key = tmp;
    }

    // locate the position of the new key
    uint32_t *it = data;
    uint32_t *begin = &data[0];
    uint32_t *end = &data[index->key_count() - 1];

    if (likely(index->key_count() > 1)) {
      it = std::lower_bound(begin, end, key);

      // if the new key already exists then throw an exception
      if (it < end && *it == key) {
        *pslot = it - begin + 1;
        return false;
      }

      // insert the new key
      if (it < end)
        ::memmove(it + 1, it, (end - it) * sizeof(uint32_t));
    }

    *it = key;
    *pslot = it - begin + 1;

    index->set_key_count(index->key_count() + 1);

    // then compress and store the block
    index->set_used_size(compress_block(index, block_cache, data, block_data));
    return true;
  }

  static bool append(Index *index, BlockCache *block_cache,
                    uint32_t *block_data, uint32_t key, int *pslot) {
    block_cache->is_active = false;

    if (Codec::kHasAppendApi)
      return Codec::append(index, block_data, key, pslot);

    // decode the block
    uint32_t datap[Index::kMaxKeysPerBlock];
    uint32_t *data = uncompress_block(index, block_data, datap);

    // append the new key
    uint32_t *it = &data[index->key_count() - 1];
    *it = key;
    *pslot = it - &data[0] + 1;

    index->set_key_count(index->key_count() + 1);

    // then compress and store the block
    index->set_used_size(compress_block(index, block_cache, data, block_data));
    return true;
  }

  template<typename GrowHandler>
  static void del(Index *index, BlockCache *block_cache, uint32_t *block_data,
                    int slot, GrowHandler *grow_handler) {
    block_cache->is_active = false;

    if (Codec::kHasDelApi)
      return Codec::del(index, block_data, slot, grow_handler);

    // uncompress the block and remove the key
    uint32_t datap[Index::kMaxKeysPerBlock];
    uint32_t *data = uncompress_block(index, block_data, datap);

    // delete the first value?
    if (slot == 0) {
      index->set_value(data[0]);
      slot++;
    }

    if (slot < (int)index->key_count() - 1) {
      ::memmove(&data[slot - 1], &data[slot],
              sizeof(uint32_t) * (index->key_count() - slot - 1));
    }

    // adjust key count
    index->set_key_count(index->key_count() - 1);

    // compress block and write it back
    if (likely(index->key_count() > 1)) {
      index->set_highest(data[index->key_count() - 2]);
      index->set_used_size(compress_block(index, block_cache,
                                data, block_data));
      assert(index->used_size() <= index->block_size());
    }
    else {
      index->set_highest(index->value());
      index->set_used_size(0);
    }
  }

  static uint32_t select(Index *index, BlockCache *block_cache,
                    uint32_t *block_data, int position_in_block) {
    if (unlikely(position_in_block == 0))
      return index->value();

    // can we satisfy the request through the block cache?
    if (likely(block_cache->is_active
                    && block_cache->index_value == index->value()))
      return block_cache->data[position_in_block - 1];

    block_cache->is_active = true;
    block_cache->index_value = index->value();
    uint32_t *data = uncompress_block(index, block_data, block_cache->data);
    return data[position_in_block - 1];
  }
};

template<typename Zint32Codec>
struct BlockKeyList : BaseKeyList {
  typedef typename Zint32Codec::Index Index;

  enum {
    // A flag whether this KeyList supports the scan() call
    kSupportsBlockScans = 1,

    // This KeyList has a custom find() implementation
    kCustomFind = 1,

    // This KeyList has a custom find_lower_bound() implementation
    kCustomFindLowerBound = 1,

    // This KeyList has a custom insert() implementation
    kCustomInsert = 1,

    // Each KeyList has a static overhead of 8 bytes
    kSizeofOverhead = 8
  };

  // Constructor
  BlockKeyList(LocalDb *db, PBtreeNode *node)
    : BaseKeyList(db, node), data_(0) {
  }

  // Creates a new KeyList starting at |data|, total size is
  // |range_size| (in bytes)
  void create(uint8_t *data, size_t range_size_) {
    data_ = data;
    range_size = range_size_;
    initialize();
  }

  // Opens an existing KeyList. Called after a btree node was fetched from
  // disk.
  void open(uint8_t *data, size_t range_size_, size_t) {
    data_ = data;
    range_size = range_size_;
  }

  // Returns the required size for this KeyList. Required to re-arrange
  // the space between KeyList and RecordList.
  size_t required_range_size(size_t) const {
    return (size_t)used_size();
  }

  // Returns the size or a single key including overhead. This is an estimate,
  // required to calculate the capacity of a node.
  size_t full_key_size(const ups_key_t * = 0) const {
    return 3;
  }

  // Returns true if the |key| no longer fits into the node.
  //
  // This KeyList always returns false because it assumes that the
  // compressed block has enough capacity for |key|. If that turns out to
  // be wrong then insert() will throw an Exception and the caller can
  // split.
  //
  // This code path only works for leaf nodes, but the zint32 compression
  // is anyway disabled for internal nodes.
  bool requires_split(size_t, const ups_key_t *) const {
    return false;
  }

  // Change the range size. Called when the range of the btree node is
  // re-distributed between KeyList and RecordList (to avoid splits).
  void change_range_size(size_t node_count, uint8_t *new_data_ptr,
                  size_t new_range_size, size_t capacity_hint) {
    if (data_ != new_data_ptr) {
      ::memmove(new_data_ptr, data_, used_size());
      data_ = new_data_ptr;
    }
    range_size = new_range_size;
  }

  // "Vacuumizes" the KeyList; packs all blocks tightly to reduce the size
  // that is consumed by this KeyList.
  void vacuumize(size_t node_count, bool force) {
    assert(check_integrity(0, node_count));
    assert(block_count() > 0);

    if (unlikely(node_count == 0))
      initialize();
    else
      vacuumize_full();

    assert(check_integrity(0, node_count));
  }

  // Checks the integrity of this node. Throws an exception if there is a
  // violation.
  bool check_integrity(Context *, size_t node_count) const {
    assert(block_count() > 0);
    Index *index = block_index(0);
    Index *end = block_index(block_count());

    size_t total_keys = 0;
    uint32_t currently_used_size = 0;
    // uint32_t highest = 0;

    for (; index < end; index++) {
      assert(index->used_size() <= index->block_size());
      assert(index->key_count() <= Index::kMaxKeysPerBlock + 1);
      assert(index->highest() >= index->value());

      if (index > block_index(0))
        assert(index->value() > (index - 1)->value());
      if (node_count > 0)
        assert(index->key_count() > 0);
      total_keys += index->key_count();
      if (currently_used_size < index->offset() + index->block_size())
        currently_used_size = index->offset() + index->block_size();

      if (index->key_count() == 1)
        assert(index->highest() == index->value());

      if (index->key_count() > 1) {
        assert(index->used_size() > 0);
#if 0
        uint32_t data[Index::kMaxKeysPerBlock];
        uint32_t *pdata = uncompress_block(index, &data[0]);
        assert(pdata[0] > index->value());
        assert(highest <= index->value());

        if (index->key_count() > 2) {
          for (uint32_t i = 1; i < index->key_count() - 1; i++)
            assert(pdata[i - 1] < pdata[i]);
        }
        highest = pdata[index->key_count() - 2];
        assert(index->highest() == highest);
#endif
      }
    }

    // add static overhead
    currently_used_size += kSizeofOverhead + sizeof(Index) * block_count();

    if (currently_used_size != used_size()) {
      ups_log(("used size %u differs from expected %u",
              currently_used_size, used_size()));
      throw Exception(UPS_INTEGRITY_VIOLATED);
    }

    if (currently_used_size > (uint32_t)range_size) {
      ups_log(("used size %u exceeds range size %u",
              currently_used_size, (uint32_t)range_size));
      throw Exception(UPS_INTEGRITY_VIOLATED);
    }

    if (total_keys != node_count) {
      ups_log(("key count %d differs from expected %d",
              (int)total_keys, (int)node_count));
      throw Exception(UPS_INTEGRITY_VIOLATED);
    }

    return true;
  }

  // Returns the size of a key; only required to appease the compiler,
  // but never called
  size_t key_size(int slot) const {
    assert(!"shouldn't be here");
    return sizeof(uint32_t);
  }

  // Returns a pointer to the key's data; only required to appease the
  // compiler, but never called
  uint8_t *key_data(int slot) {
    assert(!"shouldn't be here");
    return 0;
  }

  // Fills the btree_metrics structure
  void fill_metrics(btree_metrics_t *metrics, size_t node_count) {
    BaseKeyList::fill_metrics(metrics, node_count);
    BtreeStatistics::update_min_max_avg(&metrics->keylist_index,
            (uint32_t)(block_count() * sizeof(Index)));
    BtreeStatistics::update_min_max_avg(&metrics->keylist_blocks_per_page,
            block_count());

    Index *index = block_index(0);
    Index *end = block_index(block_count());

    uint32_t currently_used_size = 0;
    for (; index < end; index++) {
      currently_used_size += sizeof(Index) + index->used_size();
      BtreeStatistics::update_min_max_avg(&metrics->keylist_block_sizes,
              index->block_size());
    }
    BtreeStatistics::update_min_max_avg(&metrics->keylist_unused,
            range_size - currently_used_size);
  }

  // Erases the key at the specified |slot|
  void erase(Context *, size_t node_count, int slot) {
    assert(check_integrity(0, node_count));

    // get the block and the position of the key inside the block
    int position_in_block;
    Index *index;
    if (slot == 0) {
      index = block_index(0);
      position_in_block = 0;
    }
    else if (slot == (int)node_count) {
      index = block_index(block_count() - 1);
      position_in_block = index->key_count();
    }
    else
      index = find_block_by_slot(slot, &position_in_block);

    // remove the key from the block
    if (unlikely(index->key_count() == 1))
      index->set_key_count(0);
    else
      Zint32Codec::del(index, &block_cache,
                              (uint32_t *)block_data(index),
                              position_in_block, this);

    // if the block is now empty then remove it, unless it's the last block
    if (unlikely(index->key_count() == 0 && block_count() > 1))
      remove_block(index);

    assert(check_integrity(0, node_count - 1));
  }

  // Searches the node for the key and returns the slot of this key
  template<typename Cmp>
  int find(Context *context, size_t node_count, const ups_key_t *hkey,
                  Cmp &comparator) {
    int cmp;
    int slot = find_lower_bound(context, node_count, hkey, comparator, &cmp);
    return cmp == 0 ? slot : -1;
  }

  // Searches the node for the key and returns the slot of this key
  template<typename Cmp>
  int find_lower_bound(Context *, size_t node_count, const ups_key_t *hkey,
                  Cmp &comparator, int *pcmp) {
    assert(block_count() > 0);

    *pcmp = 0;

    uint32_t key = *(uint32_t *)hkey->data;
    int slot = 0;

    // first perform a linear search through the index
    Index *index = find_index(key, &slot);

    // key is the new minimum in this node?
    if (key < index->value()) {
      assert(slot == -1);
      *pcmp = -1;
      return slot;
    }

    if (index->value() == key)
      return slot;

    // increment result by 1 because index 0 is index->value()
    uint32_t result;
    int s = Zint32Codec::find_lower_bound(index,
                    (uint32_t *)block_data(index), key, &result);
    if (result != key || s == (int)index->key_count())
      *pcmp = +1;
    return slot + s + 1;
  }

  // Inserts a key
  template<typename Cmp>
  PBtreeNode::InsertResult insert(Context *, size_t node_count,
                  const ups_key_t *hkey, uint32_t flags, Cmp &comparator,
                  int /* unused */ slot) {
    assert(check_integrity(0, node_count));
    assert(hkey->size == sizeof(uint32_t));

    uint32_t key = *(uint32_t *)hkey->data;

    // if a split is required: vacuumize the node, then retry
    try {
      return insert_impl(node_count, key, flags);
    }
    catch (Exception &ex) {
      if (ex.code != UPS_LIMITS_REACHED)
        throw ex;

      vacuumize_full();

      // try again; if it still fails then let the caller handle the
      // exception
      return insert_impl(node_count, key, flags);
    }
  }

  // Grows a block's size to |new_size| bytes
  void grow_block_size(Index *index, uint32_t new_size) {
    assert(new_size > index->block_size());

    check_available_size(new_size - index->block_size());

    uint32_t additional_size = new_size - index->block_size();

    if (used_size() + additional_size > range_size)
      throw Exception(UPS_LIMITS_REACHED);

    // move all other blocks unless the current block is the last one
    if ((size_t)index->offset() + index->block_size()
            < used_size() - kSizeofOverhead - sizeof(Index) * block_count()) {
      uint8_t *p = block_data(index) + index->block_size();
      uint8_t *q = &data_[used_size()];
      ::memmove(p + additional_size, p, q - p);

      // now update the offsets of the other blocks
      Index *next = block_index(0);
      Index *end = block_index(block_count());
      for (; next < end; next++) {
        if (next->offset() > index->offset())
          next->set_offset(next->offset() + additional_size);
      }
    }

    index->set_block_size(new_size);
    set_used_size(used_size() + additional_size);
  }

  // Returns the key at the given |slot|.
  void key(Context *, int slot, ByteArray *arena, ups_key_t *dest,
                  bool deep_copy = true) {
    // uncompress the key value, store it in a member (not in a local
    // variable!), otherwise couldn't return a pointer to it
    int position_in_block;
    Index *index;

    // performing a linear search through the index is very time consuming.
    // therefore cache the last block location and hope that it's the same
    // one as requested here and now.
    //
    // the index caching is implicitely coupled with the block_cache.
    if (likely(block_cache.is_active
          && slot >= cached_index_position
          && slot < cached_index_position + (int)cached_index->key_count())) {
      index = cached_index;
      position_in_block = slot - cached_index_position;
      int pos;
      assert(cached_index == find_block_by_slot(slot, &pos));
      assert(position_in_block == pos);
    }
    else {
      index = find_block_by_slot(slot, &position_in_block);
      cached_index = index;
      cached_index_position = slot - position_in_block;
    }

    assert(position_in_block < (int)index->key_count());

    dummy = Zint32Codec::select(index, &block_cache,
                              (uint32_t *)block_data(index),
                              position_in_block);

    dest->size = sizeof(uint32_t);
    if (deep_copy == false) {
      dest->data = (uint8_t *)&dummy;
      return;
    }

    // allocate memory (if required)
    if (NOTSET(dest->flags, UPS_KEY_USER_ALLOC)) {
      arena->resize(dest->size);
      dest->data = arena->data();
    }

    *(uint32_t *)dest->data = dummy;
  }

  // Prints a key to |out| (for debugging)
  void print(Context *, int slot, std::stringstream &out) {
    int position_in_block;
    Index *index = find_block_by_slot(slot, &position_in_block);
    out << Zint32Codec::select(index, &block_cache,
                              (uint32_t *)block_data(index),
                              position_in_block);
  }

  typedef int ScanIterator;

  // Scans all keys; used for the UQI APIs.
  ScanResult scan(ByteArray *arena, size_t node_count, uint32_t start) {
    arena->resize((block_count() * (Index::kMaxKeysPerBlock + 1)) * 4);

    Index *it = block_index(0);
    Index *end = block_index(block_count());

    uint32_t *out = (uint32_t *)arena->data();

    for (; it < end; it++) {
      if (start > it->key_count()) {
        start -= it->key_count();
        continue;
      }

      *out = it->value();
      uncompress_block(it, out + 1);
      out += it->key_count();
    }

    out = (uint32_t *)arena->data();
    return std::make_pair(out + start, node_count - start);
  }

  // Copies all keys from this[sstart] to dest[dstart]; this method
  // is used to split and merge btree nodes.
  void copy_to(int sstart, size_t node_count, BlockKeyList &dest,
                  size_t other_count, int dstart) {
    block_cache.is_active = false;

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

    // grow destination block
    if (srci->used_size() > dsti->block_size())
      dest.grow_block_size(dsti, srci->used_size());

    bool initial_block_used = false;

    // If start offset or destination offset > 0: uncompress both blocks,
    // merge them
    if (src_position_in_block > 0 || dst_position_in_block > 0) {
      uint32_t sdata_buf[Index::kMaxKeysPerBlock];
      uint32_t ddata_buf[Index::kMaxKeysPerBlock];
      uint32_t *sdata = uncompress_block(srci, &sdata_buf[0]);
      uint32_t *ddata = dest.uncompress_block(dsti, &ddata_buf[0]);

      uint32_t *d = &ddata[srci->key_count()];

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
      dsti->set_highest(dsti->value());

      for (int i = src_position_in_block; i < (int)srci->key_count(); i++) {
        ddata[dsti->key_count() - 1] = sdata[i - 1];
        dsti->set_key_count(dsti->key_count() + 1);
      }

      if (dsti->key_count() > 1)
        dsti->set_highest(ddata[dsti->key_count() - 2]);
      srci->set_key_count(srci->key_count() - dsti->key_count());
      srci->set_used_size(compress_block(srci, sdata));
      assert(srci->used_size() <= srci->block_size());
      if (srci->key_count() == 1)
        srci->set_highest(srci->value());

      dsti->set_used_size(dest.compress_block(dsti, ddata));
      assert(dsti->used_size() <= dsti->block_size());

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
    for (; srci < block_index(block_count()); srci++, copied_blocks++) {
      if (initial_block_used == true)
        dsti = dest.add_block(dest.block_count(), srci->block_size());
      else
        initial_block_used = true;

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
    if (block_count() == 0)
      initialize();

    assert(dest.check_integrity(0, other_count + (node_count - sstart)));
    assert(check_integrity(0, sstart));
  }

  // Create an initial empty block
  void initialize() {
    set_block_count(0);
    set_used_size(kSizeofOverhead);
    add_block(0, Index::kInitialBlockSize);
    block_cache.is_active = false;
    assert(sizeof(block_cache.data) >= 4 * (Index::kMaxKeysPerBlock - 1));
  }

  // Calculates the used size and updates the stored value
  void reset_used_size() {
    Index *index = block_index(0);
    Index *end = block_index(block_count());
    uint32_t used_size = 0;
    for (; index < end; index++) {
      if (index->offset() + index->block_size() > used_size)
        used_size = index->offset() + index->block_size();
    }
    set_used_size(used_size + kSizeofOverhead
                    + sizeof(Index) * block_count());
  }

  // Implementation for insert()
  virtual PBtreeNode::InsertResult insert_impl(size_t node_count,
                  uint32_t key, uint32_t flags) {
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
    if (unlikely(key == index->value()))
      return (PBtreeNode::InsertResult(UPS_DUPLICATE_KEY, slot));
    if (unlikely(key == index->highest()))
      return (PBtreeNode::InsertResult(UPS_DUPLICATE_KEY,
                  slot + index->key_count() - 1));

    uint32_t new_data[Index::kMaxKeysPerBlock];
    uint32_t datap[Index::kMaxKeysPerBlock];

    // A split is required if the block overflows
    bool requires_split = index->key_count() + 1 >= Index::kMaxKeysPerBlock;

    // If the block is full then split it, otherwise check if it has
    // to grow
    if (unlikely(requires_split == false)) {
      uint32_t size = Zint32Codec::Codec::estimate_required_size(index,
                                  block_data(index), key);
      if (size > index->block_size())
        grow_block_size(index, size);
    }
    // if the block is full then split it
    else {
      int block = index - block_index(0);

      // if the new key is prepended then also prepend the new block
      if (key < index->value()) {
        Index *new_index = add_block(block + 1, Index::kInitialBlockSize);
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
        Index *new_index = add_block(block + 1, Index::kInitialBlockSize);
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

      // Now create a new block. This can throw, but so far we have not
      // modified existing data.
      Index *new_index = add_block(block + 1, index->block_size());
      new_index->set_value(new_value);
      new_index->set_highest(index->highest());
      new_index->set_key_count(new_key_count);

      // add_block() can invalid the data pointer, therefore fetch it again
      if (Zint32Codec::Codec::kCompressInPlace)
        data = (uint32_t *)block_data(index);

      // Adjust the size of the old block
      index->set_key_count(index->key_count() - new_key_count);
      index->set_highest(data[to_copy - 2]);

      // Now check if the new key will be inserted in the old or the new block
      if (key >= new_index->value()) {
        index->set_used_size(compress_block(index, data));
        assert(index->used_size() <= index->block_size());
        slot += index->key_count();

        // continue with the new block
        index = new_index;
        data = new_data;
      }
      else {
        new_index->set_used_size(compress_block(new_index, new_data));
        assert(new_index->used_size() <= new_index->block_size());

        // hack for BlockIndex: fetch data pointer once more because
        // it was invalidated when the new block was added
        if (Zint32Codec::Codec::kCompressInPlace)
          data = (uint32_t *)block_data(index);
      }

      // the block was modified and needs to be compressed again, even if
      // the actual insert operation fails (i.e. b/c the key already exists)
      index->set_used_size(compress_block(index, data));
      assert(index->used_size() <= index->block_size());

      // fall through...
    }

    int s = 0;
    if (key > index->highest()) {
      Zint32Codec::append(index, &block_cache, (uint32_t *)block_data(index),
                      key, &s);
      index->set_highest(key);
    }
    else {
      bool inserted = Zint32Codec::insert(index, &block_cache,
                    (uint32_t *)block_data(index), key, &s);
      if (unlikely(!inserted))
        return PBtreeNode::InsertResult(UPS_DUPLICATE_KEY, slot + s);
    }

    assert(index->used_size() <= index->block_size());
    assert(check_integrity(0, node_count + 1));
    return PBtreeNode::InsertResult(0, slot + s);
  }

  // Prints all keys of a block to stdout (for debugging)
  void print_block(Index *index) const {
    std::cout << "0: " << index->value() << std::endl;

    uint32_t datap[Index::kMaxKeysPerBlock];
    uint32_t *data = uncompress_block(index, datap);

    for (uint32_t i = 1; i < index->key_count(); i++)
      std::cout << i << ": " << data[i - 1] << std::endl;
  }

  // Returns the index for a block with that slot
  Index *find_block_by_slot(int slot, int *position_in_block) const {
    assert(block_count() > 0);
    Index *index = block_index(0);
    Index *end = block_index(block_count());

    for (; index < end; index++) {
      if ((int)index->key_count() > slot) {
        *position_in_block = slot;
        return index;
      } 

      slot -= index->key_count();
    }

    *position_in_block = slot;
    return index - 1;
  }

  // Performs a linear search through the index; returns the index
  // and the slot of the first key in this block in |*pslot|.
  Index *find_index(uint32_t key, int *pslot) {
    Index *index = block_index(0);
    Index *iend = block_index(block_count());

    if (key < index->value()) {
      *pslot = -1;
      return index;
    }

    *pslot = 0;

    for (; index < iend - 1; index++) {
      if (key < (index + 1)->value())
        break;
      *pslot += index->key_count();
    }

    return index;
  }

  // Inserts a new block at the specified |position|
  Index *add_block(int position, int initial_size) {
    check_available_size(initial_size + sizeof(Index));

    assert(initial_size > 0);

    // shift the whole data to the right to make space for the new block
    // index
    Index *index = block_index(position);

    if (likely(block_count() != 0))
      ::memmove(index + 1, index,
                  used_size() - (position * sizeof(Index)) - kSizeofOverhead);

    set_block_count(block_count() + 1);
    set_used_size(used_size() + sizeof(Index) + initial_size);

    // initialize the new block index; the offset is relative to the start
    // of the payload data, and does not include the indices
    uint32_t new_offset = used_size() - 2 * sizeof(uint32_t)
                          - sizeof(Index) * block_count() - initial_size;
    uint8_t *block_data = &data_[kSizeofOverhead + new_offset
                          + sizeof(Index) * block_count()];
    index->initialize(new_offset, block_data, initial_size);
    return index;
  }

  // Removes the specified block
  void remove_block(Index *index) {
    assert(block_count() > 1);
    assert(index->key_count() == 0);

    bool do_reset_used_size = false;
    // is this the last block? then re-calculate the |used_size|, because
    // there may be other unused blocks at the end
    if (used_size() == index->offset()
                              + index->block_size()
                              + block_count() * sizeof(Index)
                              + kSizeofOverhead)
      do_reset_used_size = true;

    // shift all indices (and the payload data) to the left
    ::memmove(index, index + 1, used_size()
                  - sizeof(Index) * (index - block_index(0) + 1));
    set_block_count(block_count() - 1);
    if (do_reset_used_size)
      reset_used_size();
    else
      set_used_size(used_size() - sizeof(Index));
  }

  // Checks if this range has enough space for additional |additional_size|
  // bytes. If not then it tries to vacuumize and then checks again.
  // If that also was not successful then an exception is thrown and the
  // Btree layer can re-arrange or split the page.
  void check_available_size(size_t additional_size) {
    if (likely(used_size() + additional_size <= range_size))
      return;
    vacuumize_weak();
    if (used_size() + additional_size > range_size)
      throw Exception(UPS_LIMITS_REACHED);
  }

  // Vacuumizes the node, without modifying the block pointers
  virtual void vacuumize_weak() {
    // make a copy of all indices
    bool requires_sort = false;
    int bc = block_count();
    SortHelper *s = (SortHelper *)::alloca(bc * sizeof(SortHelper));
    for (int i = 0; i < bc; i++) {
      s[i].index = i;
      s[i].offset = block_index(i)->offset();
      if (i > 0 && requires_sort == false && s[i].offset < s[i - 1].offset)
        requires_sort = true;
    }

    // sort them by offset; this is a very expensive call. only sort if
    // it's absolutely necessary!
    if (requires_sort)
      std::sort(&s[0], &s[bc], sort_by_offset);

    // shift all blocks "to the left" and reduce their size as much as
    // possible
    uint32_t next_offset = 0;
    uint8_t *block_data = &data_[8 + sizeof(Index) * bc];

    for (int i = 0; i < bc; i++) {
      Index *index = block_index(s[i].index);

      if (index->offset() != next_offset) {
        // shift block data to the left
        ::memmove(&block_data[next_offset], &block_data[index->offset()],
                        index->used_size());
        // overwrite the offset
        index->set_offset(next_offset);
      }

      // make sure that the index occupies at least 1 byte; otherwise two
      // blocks will start at the same offset, which can lead to problems
      if (unlikely(index->used_size() == 0))
        index->set_block_size(1);
      else
        index->set_block_size(index->used_size());
      next_offset += index->block_size();
    }

    set_used_size((block_data - data_) + next_offset);
  }

  // Same as above, but is allowed to modify the block pointers and i.e.
  // merge/shuffle the block indices
  virtual void vacuumize_full() {
    vacuumize_weak();
  }

  // Performs a lower bound search
  int lower_bound_search(uint32_t *begin, uint32_t *end, uint32_t key,
                  int *pcmp) const {
    uint32_t *it = std::lower_bound(begin, end, key);
    if (likely(it != end))
      *pcmp = (*it == key) ? 0 : +1;
    else // not found
      *pcmp = +1;
    return (it - begin) + 1;
  }

  // Returns the payload data of a block
  uint8_t *block_data(Index *index) const {
    return &data_[kSizeofOverhead + index->offset()
                      + sizeof(Index) * block_count()];
  }

  // Sets the block count
  void set_block_count(int count) {
    *(uint32_t *)data_ = (uint32_t)count;
  }

  // Returns the block count
  int block_count() const {
    return (int) *(uint32_t *)data_;
  }

  // Sets the used size of the range
  void set_used_size(uint32_t used_size) {
    assert(used_size <= (uint32_t)range_size);
    *(uint32_t *)(data_ + 4) = used_size;
  }

  // Returns the block count
  uint32_t used_size() const {
    return *(uint32_t *)(data_ + 4);
  }

  // Returns a pointer to a block index
  Index *block_index(int i) const {
    return (Index *)(data_ + kSizeofOverhead + i * sizeof(Index));
  }

  // Compresses a block of data
  uint32_t compress_block(Index *index, uint32_t *in) {
    return Zint32Codec::compress_block(index, &block_cache,
                            in, (uint32_t *)block_data(index));
  }

  // Uncompresses a block of data
  uint32_t *uncompress_block(Index *index, uint32_t *out) const {
    return Zint32Codec::uncompress_block(index,
                            (uint32_t *)block_data(index), out);
  }

  // The persisted (compressed) data
  uint8_t *data_;

  // helper variable to avoid returning pointers to local memory
  uint32_t dummy;

  // Cache for speeding up the select() operation
  BlockCache block_cache;

  // Cached pointer to the last index used in get_key()
  Index *cached_index;

  // The position of cached_index
  int cached_index_position;
};

} // namespace Zint32

} // namespace upscaledb

#endif // UPS_BTREE_KEYS_BLOCK_H
