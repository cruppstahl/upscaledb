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
 * Variable length KeyList
 *
 * Each key is stored in a "chunk", and the chunks are managed by an upfront
 * index which contains offset and size of each chunk. The index also keeps
 * track of deleted chunks.
 *
 * The actual chunk data contains the key's data (which can be a 64bit blob
 * ID if the key is too big).
 *
 * If the key is too big (exceeds |_extkey_threshold|) then it's offloaded
 * to an external blob, and only the 64bit record id of this blob is stored
 * in the node. These "extended keys" are cached; the cache's lifetime is
 * coupled to the lifetime of the node.
 *
 * To avoid expensive memcpy-operations, erasing a key only affects this
 * upfront index: the relevant slot is moved to a "freelist". This freelist
 * contains the same meta information as the index table.
 */

#ifndef UPS_BTREE_KEYS_VARLEN_H
#define UPS_BTREE_KEYS_VARLEN_H

#include "0root/root.h"

#include <algorithm>
#include <iostream>
#include <vector>
#include <map>

// Always verify that a file of level N does not include headers > N!
#include "1base/dynamic_array.h"
#include "2compressor/compressor_factory.h"
#include "3blob_manager/blob_manager.h"
#include "3btree/btree_node.h"
#include "3btree/btree_index.h"
#include "3btree/upfront_index.h"
#include "3btree/btree_keys_base.h"
#include "4env/env_local.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

//
// Variable length keys
//
// This KeyList uses an UpfrontIndex to manage the variable length data
// chunks. The UpfrontIndex knows the sizes of the chunks, and therefore
// the VariableLengthKeyList does *not* store additional size information.
//
// The format of a single key is:
//   |Flags|Data...|
// where Flags are 8 bit.
//
// The key size (as specified by the user when inserting the key) therefore
// is UpfrontIndex::get_chunk_size() - 1.
//
struct VariableLengthKeyList : BaseKeyList {
  // for caching external keys
  typedef std::map<uint64_t, ByteArray> ExtKeyCache;

  enum {
    // This KeyList can reduce its capacity in order to release storage
    kCanReduceCapacity = 1,
  };

  // Constructor
  VariableLengthKeyList(LocalDb *db, PBtreeNode *node)
    : BaseKeyList(db, node), _index(db), _data(0) {
    LocalEnv *env = (LocalEnv *)db->env;
    _blob_manager = env->blob_manager.get();

    size_t page_size = env->config.page_size_bytes;
    int algo = db->config.key_compressor;
    if (algo)
      _compressor.reset(CompressorFactory::create(algo));
    if (unlikely(Globals::ms_extended_threshold))
      _extkey_threshold = Globals::ms_extended_threshold;
    else {
      if (unlikely(page_size == 1024))
        _extkey_threshold = 64;
      else if (unlikely(page_size <= 1024 * 8))
        _extkey_threshold = 128;
      else {
        // UpfrontIndex's chunk size has 8 bit (max 255), and reserve
        // a few bytes for metadata (flags)
        _extkey_threshold = 250;
      }
    }
  }

  // Creates a new KeyList starting at |ptr|, total size is
  // |range_size| (in bytes)
  void create(uint8_t *ptr, size_t range_size_) {
    _data = ptr;
    range_size = range_size_;
    _index.create(_data, range_size, range_size / full_key_size());
  }

  // Opens an existing KeyList
  void open(uint8_t *ptr, size_t range_size_, size_t node_count) {
    _data = ptr;
    range_size = range_size_;
    _index.open(_data, range_size);
  }

  // Calculates the required size for a range
  size_t required_range_size(size_t node_count) const {
    return _index.required_range_size(node_count);
  }

  // Returns the actual key size including overhead. This is an estimate
  // since we don't know how large the keys will be
  size_t full_key_size(const ups_key_t *key = 0) const {
    if (!key)
      return 24 + _index.full_index_size() + 1;
    // always make sure to have enough space for an extkey id
    if (key->size < 8 || key->size > _extkey_threshold)
      return sizeof(uint64_t) + _index.full_index_size() + 1;
    return key->size + _index.full_index_size() + 1;
  }

  // Copies a key into |dest|
  void key(Context *context, int slot, ByteArray *arena, ups_key_t *dest,
                  bool deep_copy = true) {
    ups_key_t tmp = {0};
    uint32_t offset = _index.get_chunk_offset(slot);
    uint8_t *p = _index.get_chunk_data_by_offset(offset);

    if (unlikely(ISSET(*p, BtreeKey::kExtendedKey))) {
      get_extended_key(context, get_extended_blob_id(slot), &tmp);
      if (unlikely(ISSET(*p, BtreeKey::kCompressed)))
        uncompress(&tmp, &tmp);
    }
    else {
      tmp.size = key_size(slot);
      tmp.data = p + 1;
      if (unlikely(ISSET(*p, BtreeKey::kCompressed)))
        uncompress(&tmp, &tmp);
    }

    dest->size = tmp.size;

    if (likely(deep_copy == false)) {
      dest->data = tmp.data;
      return;
    }

    // allocate memory (if required)
    if (NOTSET(dest->flags, UPS_KEY_USER_ALLOC)) {
      arena->resize(tmp.size);
      dest->data = arena->data();
    }
    ::memcpy(dest->data, tmp.data, tmp.size);
  }

  // Iterates all keys, calls the |visitor| on each. Not supported by
  // this KeyList implementation. For variable length keys, the caller
  // must iterate over all keys. The |scan()| interface is only implemented
  // for PAX style layouts.
  ScanResult scan(ByteArray *arena, size_t node_count, uint32_t start) {
    assert(!"shouldn't be here");
    throw Exception(UPS_INTERNAL_ERROR);
  }

  // Erases a key's payload. Does NOT remove the chunk from the UpfrontIndex
  // (see |erase()|).
  void erase_extended_key(Context *context, int slot) {
    uint8_t flags = get_key_flags(slot);
    if (ISSET(flags, BtreeKey::kExtendedKey)) {
      // delete the extended key from the cache
      erase_extended_key(context, get_extended_blob_id(slot));
      // and transform into a key which is non-extended and occupies
      // the same space as before, when it was extended
      set_key_flags(slot, flags & (~BtreeKey::kExtendedKey));
      set_key_size(slot, sizeof(uint64_t));
    }
  }

  // Erases a key, including extended blobs
  void erase(Context *context, size_t node_count, int slot) {
    erase_extended_key(context, slot);
    _index.erase(node_count, slot);
  }

  // Inserts the |key| at the position identified by |slot|.
  // This method cannot fail; there MUST be sufficient free space in the
  // node (otherwise the caller would have split the node).
  template<typename Cmp>
  PBtreeNode::InsertResult insert(Context *context, size_t node_count,
                              const ups_key_t *key, uint32_t flags,
                              Cmp &comparator, int slot) {
    _index.insert(node_count, slot);

    // now there's one additional slot
    node_count++;

    uint32_t key_flags = 0;
    // try to compress the key
    ups_key_t helper = {0};
    if (_compressor && compress(key, &helper)) {
      key_flags = BtreeKey::kCompressed;
      key = &helper;
    }

    // When inserting the data: always add 1 byte for key flags
    if (likely(key->size <= _extkey_threshold
                && _index.can_allocate_space(node_count, key->size + 1))) {
      uint32_t offset = _index.allocate_space(node_count, slot,
                      key->size + 1);
      uint8_t *p = _index.get_chunk_data_by_offset(offset);
      *p = key_flags;
      ::memcpy(p + 1, key->data, key->size);
    }
    else {
      uint64_t blob_id = add_extended_key(context, key);
      _index.allocate_space(node_count, slot, 8 + 1);
      set_extended_blob_id(slot, blob_id);
      set_key_flags(slot, key_flags | BtreeKey::kExtendedKey);
    }

    return PBtreeNode::InsertResult(0, slot);
  }

  // Returns true if the |key| no longer fits into the node and a split
  // is required. Makes sure that there is ALWAYS enough headroom
  // for an extended key!
  //
  // If there's no key specified then always assume the worst case and
  // pretend that the key has the maximum length
  bool requires_split(size_t node_count, const ups_key_t *key) {
    size_t required;
    if (key) {
      required = key->size + 1;
      // add 1 byte for flags
      if (key->size > _extkey_threshold || key->size < 8 + 1)
        required = 8 + 1;
    }
    else
      required = _extkey_threshold + 1;
    return _index.requires_split(node_count, required);
  }

  // Copies |count| key from this[sstart] to dest[dstart]
  void copy_to(int sstart, size_t node_count,
                  VariableLengthKeyList &dest, size_t other_node_count,
                  int dstart) {
    size_t to_copy = node_count - sstart;
    assert(to_copy > 0);

    // make sure that the other node has sufficient capacity in its
    // UpfrontIndex
    dest._index.change_range_size(other_node_count, 0, 0, _index.capacity());

    for (size_t i = 0; i < to_copy; i++) {
      size_t size = key_size(sstart + i);

      uint8_t *p = _index.get_chunk_data_by_offset(
                      _index.get_chunk_offset(sstart + i));
      uint8_t flags = *p;
      uint8_t *data = p + 1;

      dest._index.insert(other_node_count + i, dstart + i);
      // Add 1 byte for key flags
      uint32_t offset = dest._index.allocate_space(other_node_count + i + 1,
                      dstart + i, size + 1);
      p = dest._index.get_chunk_data_by_offset(offset);
      *p = flags; // sets flags
      ::memcpy(p + 1, data, size); // and data
    }

    // A lot of keys will be invalidated after copying, therefore make
    // sure that the next_offset is recalculated when it's required
    _index.invalidate_next_offset();
  }

  // Checks the integrity of this node. Throws an exception if there is a
  // violation.
  void check_integrity(Context *context, size_t node_count) const {
    ByteArray arena;

    // verify that the offsets and sizes are not overlapping
    _index.check_integrity(node_count);

    // make sure that extkeys are handled correctly
    for (size_t i = 0; i < node_count; i++) {
      if (key_size(i) > _extkey_threshold
            && NOTSET(get_key_flags(i), BtreeKey::kExtendedKey)) {
        ups_log(("key size %d, but key is not extended", key_size(i)));
        throw Exception(UPS_INTEGRITY_VIOLATED);
      }

      if (ISSET(get_key_flags(i), BtreeKey::kExtendedKey)) {
        uint64_t blobid = get_extended_blob_id(i);
        if (!blobid) {
          ups_log(("integrity check failed: item %u "
                  "is extended, but has no blob", i));
          throw Exception(UPS_INTEGRITY_VIOLATED);
        }

        // make sure that the extended blob can be loaded
        ups_record_t record = {0};
        _blob_manager->read(context, blobid, &record, 0, &arena);

        // compare it to the cached key (if there is one)
        if (_extkey_cache) {
          ExtKeyCache::iterator it = _extkey_cache->find(blobid);
          if (it != _extkey_cache->end()) {
            if (record.size != it->second.size()) {
              ups_log(("Cached extended key differs from real key"));
              throw Exception(UPS_INTEGRITY_VIOLATED);
            }
            if (::memcmp(record.data, it->second.data(), record.size)) {
              ups_log(("Cached extended key differs from real key"));
              throw Exception(UPS_INTEGRITY_VIOLATED);
            }
          }
        }
      }
    }
  }

  // Rearranges the list
  void vacuumize(size_t node_count, bool force) {
    if (force)
      _index.increase_vacuumize_counter(100);
    _index.maybe_vacuumize(node_count);
  }

  // Change the range size; the capacity will be adjusted, the data is
  // copied as necessary
  void change_range_size(size_t node_count, uint8_t *new_data_ptr,
                  size_t new_range_size, size_t capacity_hint) {
    // no capacity given? then try to find a good default one
    if (capacity_hint == 0) {
      capacity_hint = (new_range_size - _index.next_offset(node_count)
              - full_key_size()) / _index.full_index_size();
      if (capacity_hint <= node_count)
        capacity_hint = node_count + 1;
    }

    // if there's not enough space for the new capacity then try to reduce
    // the capacity
    if (_index.next_offset(node_count) + full_key_size(0)
                    + capacity_hint * _index.full_index_size()
                    + UpfrontIndex::kPayloadOffset
              > new_range_size)
      capacity_hint = node_count + 1;

    _index.change_range_size(node_count, new_data_ptr, new_range_size,
                      capacity_hint);
    _data = new_data_ptr;
    range_size = new_range_size;
  }

  // Fills the btree_metrics structure
  void fill_metrics(btree_metrics_t *metrics, size_t node_count) {
    BaseKeyList::fill_metrics(metrics, node_count);
    BtreeStatistics::update_min_max_avg(&metrics->keylist_index,
            (uint32_t)(_index.capacity()
                  * _index.full_index_size()));
    BtreeStatistics::update_min_max_avg(&metrics->keylist_unused,
            range_size - (uint32_t)_index.required_range_size(node_count));
  }

  // Prints a slot to |out| (for debugging)
  void print(Context *context, int slot, std::stringstream &out) {
    ups_key_t tmp = {0};
    if (ISSET(get_key_flags(slot), BtreeKey::kExtendedKey)) {
      get_extended_key(context, get_extended_blob_id(slot), &tmp);
    }
    else {
      tmp.size = key_size(slot);
      tmp.data = key_data(slot);
    }
    out << (const char *)tmp.data;
  }

  // Returns the pointer to a key's inline data (const flavour)
  uint8_t *key_data(int slot) const {
    uint32_t offset = _index.get_chunk_offset(slot);
    return _index.get_chunk_data_by_offset(offset) + 1;
  }

  // Returns the size of a key
  size_t key_size(int slot) const {
    return _index.get_chunk_size(slot) - 1;
  }

  // Returns the flags of a key. Flags are defined in btree_flags.h
  uint8_t get_key_flags(int slot) const {
    uint32_t offset = _index.get_chunk_offset(slot);
    return *_index.get_chunk_data_by_offset(offset);
  }

  // Sets the flags of a key. Flags are defined in btree_flags.h
  void set_key_flags(int slot, uint8_t flags) {
    uint32_t offset = _index.get_chunk_offset(slot);
    *_index.get_chunk_data_by_offset(offset) = flags;
  }

  // Overwrites the (inline) data of the key
  void set_key_data(int slot, const void *ptr, size_t size) {
    assert(_index.get_chunk_size(slot) >= size);
    set_key_size(slot, (uint16_t)size);
    ::memcpy(key_data(slot), ptr, size);
  }

  // Sets the size of a key
  void set_key_size(int slot, size_t size) {
    assert(size + 1 <= _index.get_chunk_size(slot));
    _index.set_chunk_size(slot, size + 1);
  }

  // Returns the record address of an extended key overflow area
  uint64_t get_extended_blob_id(int slot) const {
    return *(uint64_t *)key_data(slot);
  }

  // Sets the record address of an extended key overflow area
  void set_extended_blob_id(int slot, uint64_t blobid) {
    *(uint64_t *)key_data(slot) = blobid;
  }

  // Erases an extended key from disk and from the cache
  void erase_extended_key(Context *context, uint64_t blobid) {
    _blob_manager->erase(context, blobid);
    if (_extkey_cache) {
      ExtKeyCache::iterator it = _extkey_cache->find(blobid);
      if (it != _extkey_cache->end())
        _extkey_cache->erase(it);
    }
  }

  // Retrieves the extended key at |blobid| and stores it in |key|; will
  // use the cache.
  void get_extended_key(Context *context, uint64_t blob_id, ups_key_t *key) {
    if (unlikely(!_extkey_cache))
      _extkey_cache.reset(new ExtKeyCache());
    else {
      ExtKeyCache::iterator it = _extkey_cache->find(blob_id);
      if (it != _extkey_cache->end()) {
        key->size = it->second.size();
        key->data = it->second.data();
        return;
      }
    }

    ByteArray arena;
    ups_record_t record = {0};
    _blob_manager->read(context, blob_id, &record, UPS_FORCE_DEEP_COPY,
                    &arena);
    (*_extkey_cache)[blob_id] = arena;
    arena.disown();
    key->data = record.data;
    key->size = record.size;
  }

  // Allocates an extended key and stores it in the cache
  uint64_t add_extended_key(Context *context, const ups_key_t *key) {
    if (unlikely(!_extkey_cache))
      _extkey_cache.reset(new ExtKeyCache());

    ups_record_t rec = {0};
    rec.data = key->data;
    rec.size = key->size;

    // if keys are compressed then disable the compression for the
    // extended blob, because compressing already compressed data usually
    // has not much of an effect
    uint64_t blob_id = _blob_manager->allocate(context, &rec,
                                      _compressor
                                          ? BlobManager::kDisableCompression
                                          : 0);
    assert(blob_id != 0);
    assert(_extkey_cache->find(blob_id) == _extkey_cache->end());

    ByteArray arena;
    arena.resize(key->size);
    ::memcpy(arena.data(), key->data, key->size);
    (*_extkey_cache)[blob_id] = arena;
    arena.disown();

    // increment counter (for statistics)
    Globals::ms_extended_keys++;

    return blob_id;
  }

  bool compress(const ups_key_t *src, ups_key_t *dest) {
    assert(_compressor != 0);

    // reserve 2 bytes for the uncompressed key length
    _compressor->reserve(sizeof(uint16_t));

    // perform compression, but abort if the compressed data exceeds
    // the uncompressed data
    uint32_t clen = _compressor->compress((uint8_t *)src->data, src->size);
    if (clen >= src->size)
      return false;

    // fill in the length
    uint8_t *ptr = _compressor->arena.data();
    *(uint16_t *)ptr = src->size;

    dest->data = ptr;
    dest->size = clen + sizeof(uint16_t);
    Globals::ms_bytes_before_compression += src->size;
    Globals::ms_bytes_after_compression += dest->size;

    return true;
  }

  void uncompress(const ups_key_t *src, ups_key_t *dest) {
    assert(_compressor != 0);

    uint8_t *ptr = (uint8_t *)src->data;

    // first 2 bytes are the uncompressed length
    uint16_t uclen = *(uint16_t *)ptr;
    _compressor->decompress(ptr + sizeof(uint16_t),
                    src->size - sizeof(uint16_t), uclen);

    dest->size = uclen;
    dest->data = _compressor->arena.data();
  }

  // The BlobManager
  BlobManager *_blob_manager;

  // The index for managing the variable-length chunks
  UpfrontIndex _index;

  // Pointer to the data of the node 
  uint8_t *_data;

  // Cache for extended keys
  ScopedPtr<ExtKeyCache> _extkey_cache;

  // Threshold for extended keys; if key size is > threshold then the
  // key is moved to a blob
  size_t _extkey_threshold;

  // Compressor for the keys
  ScopedPtr<Compressor> _compressor;
};

} // namespace upscaledb

#endif // UPS_BTREE_KEYS_VARLEN_H
