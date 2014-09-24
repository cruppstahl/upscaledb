/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
 * If the key is too big (exceeds |m_extkey_threshold|) then it's offloaded
 * to an external blob, and only the 64bit record id of this blob is stored
 * in the node. These "extended keys" are cached; the cache's lifetime is
 * coupled to the lifetime of the node.
 *
 * To avoid expensive memcpy-operations, erasing a key only affects this
 * upfront index: the relevant slot is moved to a "freelist". This freelist
 * contains the same meta information as the index table.
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_BTREE_KEYS_VARLEN_H
#define HAM_BTREE_KEYS_VARLEN_H

#include "0root/root.h"

#include <algorithm>
#include <iostream>
#include <vector>
#include <map>

// Always verify that a file of level N does not include headers > N!
#include "1globals/globals.h"
#include "1base/byte_array.h"
#include "1base/scoped_ptr.h"
#include "2page/page.h"
#include "3blob_manager/blob_manager.h"
#include "3btree/btree_node.h"
#include "3btree/btree_index.h"
#include "3btree/btree_keys_base.h"
#include "4env/env_local.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

namespace DefLayout {

//
// A helper class to sort ranges; used during validation of the up-front
// index in check_index_integrity()
//
struct SortHelper {
  ham_u32_t offset;
  ham_u32_t slot;

  bool operator<(const SortHelper &rhs) const {
    return (offset < rhs.offset);
  }
};

static bool
sort_by_offset(const SortHelper &lhs, const SortHelper &rhs) {
  return (lhs.offset < rhs.offset);
}

//
// A small index which manages variable length buffers. Used to manage
// variable length keys or records.
//
// The UpfrontIndex manages a range of bytes, organized in variable length
// |chunks|, assigned at initialization time when calling |allocate()|
// or |open()|. 
// 
// These chunks are organized in |slots|, each slot stores the offset and
// the size of the chunk data. The offset is stored as 16- or 32-bit, depending
// on the page size. The size is always a 16bit integer.
//
// The number of used slots is not stored in the UpfrontIndex, since it is
// already managed in the caller (this is equal to |PBtreeNode::get_count()|).
// Therefore you will see a lot of methods receiving a |node_count| parameter.
//
// Deleted chunks are moved to a |freelist|, which is simply a list of slots
// directly following those slots that are in use.
//
// In addition, the UpfrontIndex keeps track of the unused space at the end
// of the range (via |get_next_offset()|), in order to allow a fast
// allocation of space.
//
// The UpfrontIndex stores metadata at the beginning:
//     [0..3]  freelist count
//     [4..7]  next offset
//     [8..11] capacity
//
// Data is stored in the following layout:
// |metadata|slot1|slot2|...|slotN|free1|free2|...|freeM|data1|data2|...|dataN|
//
class UpfrontIndex
{
    enum {
      // width of the 'size' field
      kSizeofSize = 1 // sizeof(ham_u16_t)
    };

  public:
    enum {
      // for freelist_count, next_offset, capacity
      kPayloadOffset = 12,
    };

    // Constructor; creates an empty index which needs to be initialized
    // with |create()| or |open()|.
    UpfrontIndex(LocalDatabase *db)
      : m_data(0), m_range_size(0), m_vacuumize_counter(0) {
      size_t page_size = db->get_local_env()->get_page_size();
      if (page_size <= 64 * 1024)
        m_sizeof_offset = 2;
      else
        m_sizeof_offset = 4;
    }

    // Initialization routine; sets data pointer, range size and the
    // initial capacity.
    void create(ham_u8_t *data, size_t range_size, size_t capacity) {
      m_data = data;
      m_range_size = range_size;
      set_capacity(capacity);
      clear();
    }

    // "Opens" an existing index from memory. This method sets the data
    // pointer and initializes itself.
    void open(ham_u8_t *data, size_t range_size) {
      m_data = data;
      m_range_size = range_size;
      // the vacuumize-counter is not persisted, therefore
      // pretend that the counter is very high; in worst case this will cause
      // an invalid call to vacuumize(), which is not a problem
      if (get_freelist_count())
        m_vacuumize_counter = m_range_size;
    }

    // Changes the range size and capacity of the index; used to resize the
    // KeyList or RecordList
    void change_range_size(size_t node_count, ham_u8_t *new_data_ptr,
                    size_t new_range_size, size_t new_capacity) {
      if (!new_data_ptr)
        new_data_ptr = m_data;
      if (!new_range_size)
        new_range_size = m_range_size;

      size_t used_data_size = get_next_offset(node_count); 
      size_t old_capacity = get_capacity();
      ham_u8_t *src = &m_data[kPayloadOffset
                            + old_capacity * get_full_index_size()];
      ham_u8_t *dst = &new_data_ptr[kPayloadOffset
                            + new_capacity * get_full_index_size()];
      ham_assert(dst - new_data_ptr + used_data_size <= new_range_size);
      // shift "to the right"? Then first move the data and afterwards
      // the index
      if (dst > src) {
        memmove(dst, src, used_data_size);
        memmove(new_data_ptr, m_data,
                kPayloadOffset + new_capacity * get_full_index_size());
      }
      // vice versa otherwise
      else if (dst < src) {
        if (new_data_ptr != m_data)
          memmove(new_data_ptr, m_data,
                  kPayloadOffset + new_capacity * get_full_index_size());
        memmove(dst, src, used_data_size);
      }
      m_data = new_data_ptr;
      m_range_size = new_range_size;
      set_capacity(new_capacity);
      set_next_offset(used_data_size);
    }

    // Calculates the required size for a range
    size_t get_required_range_size(size_t node_count) const {
      return (UpfrontIndex::kPayloadOffset
                    + node_count * get_full_index_size()
                    + get_next_offset(node_count));
    }

    // Returns the size of a single index entry
    size_t get_full_index_size() const {
      return (m_sizeof_offset + kSizeofSize);
    }

    // Transforms a relative offset of the payload data to an absolute offset
    // in |m_data|
    ham_u32_t get_absolute_offset(ham_u32_t offset) const {
      return (offset
                      + kPayloadOffset
                      + get_capacity() * get_full_index_size());
    }

    // Returns the absolute start offset of a chunk
    ham_u32_t get_absolute_chunk_offset(ham_u32_t slot) const {
      return (get_absolute_offset(get_chunk_offset(slot)));
    }

    // Returns the relative start offset of a chunk
    ham_u32_t get_chunk_offset(ham_u32_t slot) const {
      ham_u8_t *p = &m_data[kPayloadOffset + get_full_index_size() * slot];
      if (m_sizeof_offset == 2)
        return (*(ham_u16_t *)p);
      else {
        ham_assert(m_sizeof_offset == 4);
        return (*(ham_u32_t *)p);
      }
    }

    // Returns the size of a chunk
    ham_u16_t get_chunk_size(ham_u32_t slot) const {
      return (m_data[kPayloadOffset + get_full_index_size() * slot
                                + m_sizeof_offset]);
    }

    // Sets the size of a chunk (does NOT actually resize the chunk!)
    void set_chunk_size(ham_u32_t slot, ham_u16_t size) {
      ham_assert(size <= 255);
      m_data[kPayloadOffset + get_full_index_size() * slot
                                + m_sizeof_offset] = (ham_u8_t)size;
    }

    // Increases the "vacuumize-counter", which is an indicator whether
    // rearranging the node makes sense
    void increase_vacuumize_counter(size_t gap_size) {
      m_vacuumize_counter += gap_size;
    }

    // Vacuumizes the index, *if it makes sense*. Returns true if the
    // operation was successful, otherwise false 
    bool maybe_vacuumize(size_t node_count) {
      if (m_vacuumize_counter > 0 || get_freelist_count() > 0) {
        vacuumize(node_count);
        return (true);
      }
      return (false);
    }

    // Returns true if this index has at least one free slot available.
    // |node_count| is the number of used slots (this is managed by the caller)
    bool can_insert_slot(size_t node_count) {
      return (likely(node_count + get_freelist_count() < get_capacity()));
    }

    // Inserts a slot at the position |slot|. |node_count| is the number of
    // used slots (this is managed by the caller)
    void insert_slot(size_t node_count, ham_u32_t slot) {
      ham_assert(can_insert_slot(node_count) == true);

      size_t slot_size = get_full_index_size();
      size_t total_count = node_count + get_freelist_count();
      ham_u8_t *p = &m_data[kPayloadOffset + slot_size * slot];
      if (total_count > 0 && slot < total_count) {
        // create a gap in the index
        memmove(p + slot_size, p, slot_size * (total_count - slot));
     }

      // now fill the gap
      memset(p, 0, slot_size);
    }

    // Erases a slot at the position |slot|
    // |node_count| is the number of used slots (this is managed by the caller)
    void erase_slot(size_t node_count, ham_u32_t slot) {
      size_t slot_size = get_full_index_size();
      size_t total_count = node_count + get_freelist_count();

      ham_assert(slot < total_count);

      set_freelist_count(get_freelist_count() + 1);

      size_t chunk_size = get_chunk_size(slot);

      increase_vacuumize_counter(chunk_size);

      // nothing to do if we delete the very last (used) slot; the freelist
      // counter was already incremented, the used counter is decremented
      // by the caller
      if (slot == node_count - 1)
        return;

      size_t chunk_offset = get_chunk_offset(slot);

      // shift all items to the left
      ham_u8_t *p = &m_data[kPayloadOffset + slot_size * slot];
      memmove(p, p + slot_size, slot_size * (total_count - slot));

      // then copy the deleted chunk to the freelist
      set_chunk_offset(total_count - 1, chunk_offset);
      set_chunk_size(total_count - 1, chunk_size);
    }

    // Adds a chunk to the freelist. Will not do anything if the node
    // is already full.
    void add_to_freelist(size_t node_count, ham_u32_t chunk_offset,
                    ham_u32_t chunk_size) {
      size_t total_count = node_count + get_freelist_count();
      if (likely(total_count < get_capacity())) {
        set_freelist_count(get_freelist_count() + 1);
        set_chunk_size(total_count, chunk_size);
        set_chunk_offset(total_count, chunk_offset);
      }
    }

    // Returns true if this page has enough space to store at least |num_bytes|
    // bytes.
    bool can_allocate_space(size_t node_count, size_t num_bytes) {
      // first check if we can append the data; this is the cheapest check,
      // therefore it comes first
      if (get_next_offset(node_count) + num_bytes <= get_usable_data_size())
        return (true);

      // otherwise check the freelist
      ham_u32_t total_count = node_count + get_freelist_count();
      for (ham_u32_t i = node_count; i < total_count; i++)
        if (get_chunk_size(i) >= num_bytes)
          return (true);
      return (false);
    }

    // Allocates space for a |slot| and returns the offset of that chunk
    ham_u32_t allocate_space(ham_u32_t node_count, ham_u32_t slot,
                    size_t num_bytes) {
      ham_assert(can_allocate_space(node_count, num_bytes));

      size_t next_offset = get_next_offset(node_count);

      // try to allocate space at the end of the node
      if (next_offset + num_bytes <= get_usable_data_size()) {
        ham_u32_t offset = get_chunk_offset(slot);
        // if this slot's data is at the very end then maybe it can be
        // resized without actually moving the data
        if (unlikely(next_offset == offset + get_chunk_size(slot))) {
          set_next_offset(offset + num_bytes);
          set_chunk_size(slot, num_bytes);
          return (offset);
        }
        set_next_offset(next_offset + num_bytes);
        set_chunk_offset(slot, next_offset);
        set_chunk_size(slot, num_bytes);
        return (next_offset);
      }

      size_t slot_size = get_full_index_size();

      // otherwise check the freelist
      ham_u32_t total_count = node_count + get_freelist_count();
      for (ham_u32_t i = node_count; i < total_count; i++) {
        ham_u32_t chunk_size = get_chunk_size(i);
        ham_u32_t chunk_offset = get_chunk_offset(i);
        if (chunk_size >= num_bytes) {
          // update next_offset?
          if (unlikely(next_offset == chunk_offset + chunk_size))
            invalidate_next_offset();
          else if (unlikely(next_offset == get_chunk_offset(slot)
                                  + get_chunk_size(slot)))
            invalidate_next_offset();
          // copy the chunk to the new slot
          set_chunk_size(slot, num_bytes);
          set_chunk_offset(slot, chunk_offset);
          // remove from the freelist
          if (i < total_count - 1) {
            ham_u8_t *p = &m_data[kPayloadOffset + slot_size * i];
            memmove(p, p + slot_size, slot_size * (total_count - i - 1));
          }
          set_freelist_count(get_freelist_count() - 1);
          return (get_chunk_offset(slot));
        }
      }

      ham_assert(!"shouldn't be here");
      throw Exception(HAM_INTERNAL_ERROR);
    }

    // Returns true if |key| cannot be inserted because a split is required.
    // Unlike implied by the name, this function will try to re-arrange the
    // node in order for the key to fit in.
    bool requires_split(ham_u32_t node_count, size_t required_size) {
      return (!can_insert_slot(node_count)
                || !can_allocate_space(node_count, required_size));
    }

    // Verifies that there are no overlapping chunks
    void check_integrity(ham_u32_t node_count) const {
      typedef std::pair<ham_u32_t, ham_u32_t> Range;
      //typedef std::vector<Range> RangeVec;
      ham_u32_t total_count = node_count + get_freelist_count();

      ham_assert(node_count > 1
                    ? get_next_offset(node_count) > 0
                    : true);

      if (total_count > get_capacity()) {
        ham_trace(("integrity violated: total count %u (%u+%u) > capacity %u",
                    total_count, node_count, get_freelist_count(),
                    get_capacity()));
        throw Exception(HAM_INTEGRITY_VIOLATED);
      }

      //RangeVec ranges;
      //ranges.reserve(total_count);
      ham_u32_t next_offset = 0;
      for (ham_u32_t i = 0; i < total_count; i++) {
        Range range = std::make_pair(get_chunk_offset(i), get_chunk_size(i));
        ham_u32_t next = range.first + range.second;
        if (next >= next_offset)
          next_offset = next;
        //ranges.push_back(range);
      }

#if 0
      std::sort(ranges.begin(), ranges.end());

      if (!ranges.empty()) {
        for (ham_u32_t i = 0; i < ranges.size() - 1; i++) {
          if (ranges[i].first + ranges[i].second > ranges[i + 1].first) {
            ham_trace(("integrity violated: slot %u/%u overlaps with %lu",
                        ranges[i].first, ranges[i].second,
                        ranges[i + 1].first));
            throw Exception(HAM_INTEGRITY_VIOLATED);
          }
        }
      }
#endif

      if (next_offset != get_next_offset(node_count)) {
        ham_trace(("integrity violated: next offset %d, cached offset %d",
                    next_offset, get_next_offset(node_count)));
        throw Exception(HAM_INTEGRITY_VIOLATED);
      }
      if (next_offset != calc_next_offset(node_count)) {
        ham_trace(("integrity violated: next offset %d, calculated offset %d",
                    next_offset, calc_next_offset(node_count)));
        throw Exception(HAM_INTEGRITY_VIOLATED);
      }
    }

    // Splits an index and moves all chunks starting from position |pivot|
    // to the other index.
    // The other index *must* be empty!
    void split(UpfrontIndex *other, size_t node_count, size_t pivot) {
      other->clear();

      // now copy key by key
      for (size_t i = pivot; i < node_count; i++) {
        other->insert_slot(i - pivot, i - pivot);
        ham_u32_t size = get_chunk_size(i);
        ham_u32_t offset = other->allocate_space(i - pivot, i - pivot, size);
        memcpy(other->get_chunk_data_by_offset(offset),
                    get_chunk_data_by_offset(get_chunk_offset(i)),
                    size);
      }

      // this node has lost lots of its data - make sure that it will be
      // vacuumized as soon as more data is allocated
      m_vacuumize_counter += node_count;
      set_freelist_count(0);
      set_next_offset((ham_u32_t)-1);
    }

    // Merges all chunks from the |other| index to this index
    void merge_from(UpfrontIndex *other, size_t node_count,
                    size_t other_node_count) {
      vacuumize(node_count);
      
      for (size_t i = 0; i < other_node_count; i++) {
        insert_slot(i + node_count, i + node_count);
        ham_u32_t size = other->get_chunk_size(i);
        ham_u32_t offset = allocate_space(i + node_count, i + node_count, size);
        memcpy(get_chunk_data_by_offset(offset),
                    other->get_chunk_data_by_offset(other->get_chunk_offset(i)),
                    size);
      }

      other->clear();
    }

    // Returns a pointer to the actual data of a chunk
    ham_u8_t *get_chunk_data_by_offset(ham_u32_t offset) {
      return (&m_data[kPayloadOffset
                      + get_capacity() * get_full_index_size()
                      + offset]);
    }

    // Returns a pointer to the actual data of a chunk
    ham_u8_t *get_chunk_data_by_offset(ham_u32_t offset) const {
      return (&m_data[kPayloadOffset
                      + get_capacity() * get_full_index_size()
                      + offset]);
    }

    // Re-arranges the node: moves all keys sequentially to the beginning
    // of the key space, removes the whole freelist.
    //
    // This call is extremely expensive! Try to avoid it as good as possible.
    void vacuumize(size_t node_count) {
      if (m_vacuumize_counter == 0) {
        if (get_freelist_count() > 0) {
          set_freelist_count(0);
          invalidate_next_offset();
        }
        return;
      }

      // get rid of the freelist - this node is now completely rewritten,
      // and the freelist would just complicate things
      set_freelist_count(0);

      // make a copy of all indices (excluding the freelist)
      bool requires_sort = false;
	  SortHelper *s = (SortHelper *)::alloca(node_count * sizeof(SortHelper));
      for (ham_u32_t i = 0; i < node_count; i++) {
        s[i].slot = i;
        s[i].offset = get_chunk_offset(i);
        if (i > 0 && s[i].offset < s[i - 1].offset)
          requires_sort = true;
      }

      // sort them by offset; this is a very expensive call. only sort if
      // it's absolutely necessary!
      if (requires_sort)
        std::sort(&s[0], &s[node_count], sort_by_offset);

      // shift all keys to the left, get rid of all gaps at the front of the
      // key data or between the keys
      ham_u32_t next_offset = 0;
      ham_u32_t start = kPayloadOffset + get_capacity() * get_full_index_size();
      for (ham_u32_t i = 0; i < node_count; i++) {
        ham_u32_t offset = s[i].offset;
        ham_u32_t slot = s[i].slot;
        ham_u32_t size = get_chunk_size(slot);
        if (offset != next_offset) {
          // shift key to the left
          memmove(&m_data[start + next_offset],
                          get_chunk_data_by_offset(offset), size);
          // store the new offset
          set_chunk_offset(slot, next_offset);
        }
        next_offset += size;
      }

      set_next_offset(next_offset);
      m_vacuumize_counter = 0;
    }

    // Invalidates the cached "next offset". In some cases it's necessary
    // that the caller forces a re-evaluation of the next offset. Although
    // i *think* that this method could become private, but the effort
    // is not worth the gain.
    void invalidate_next_offset() {
      set_next_offset((ham_u32_t)-1);
    }

    // Same as above, but only if the next_offset equals |new_offset|
    void maybe_invalidate_next_offset(size_t new_offset) {
      if (get_next_offset(0) == new_offset)
        invalidate_next_offset();
    }

    // Returns the capacity; required for tests
    size_t test_get_capacity() const {
      return (get_capacity());
    }

  private:
    friend class UpfrontIndexFixture;

    // Resets the page
    void clear() {
      set_freelist_count(0);
      set_next_offset(0);
      m_vacuumize_counter = 0;
    }

    // Returns the offset of the unused space at the end of the page
    ham_u32_t get_next_offset(size_t node_count) {
      ham_u32_t ret = *(ham_u32_t *)(m_data + 4);
      if (unlikely(ret == (ham_u32_t)-1 && node_count > 0)) {
        ret = calc_next_offset(node_count);
        set_next_offset(ret);
      }
      return (ret);
    }

    // Returns the offset of the unused space at the end of the page
    // (const version)
    ham_u32_t get_next_offset(size_t node_count) const {
      ham_u32_t ret = *(ham_u32_t *)(m_data + 4);
      if (unlikely(ret == (ham_u32_t)-1))
        return (calc_next_offset(node_count));
      return (ret);
    }

    // Returns the size (in bytes) where payload data can be stored
    size_t get_usable_data_size() const {
      return (m_range_size - kPayloadOffset
                      - get_capacity() * get_full_index_size());
    }

    // Sets the chunk offset of a slot
    void set_chunk_offset(ham_u32_t slot, ham_u32_t offset) {
      ham_u8_t *p = &m_data[kPayloadOffset + get_full_index_size() * slot];
      if (m_sizeof_offset == 2)
        *(ham_u16_t *)p = (ham_u16_t)offset;
      else
        *(ham_u32_t *)p = offset;
    }

    // Returns the number of freelist entries
    size_t get_freelist_count() const {
      return (*(ham_u32_t *)m_data);
    }

    // Sets the number of freelist entries
    void set_freelist_count(size_t freelist_count) {
      ham_assert(freelist_count <= get_capacity());
      *(ham_u32_t *)m_data = freelist_count;
    }

    // Calculates and returns the next offset; does not store it
    ham_u32_t calc_next_offset(size_t node_count) const {
      ham_u32_t total_count = node_count + get_freelist_count();
      ham_u32_t next_offset = 0;
      for (ham_u32_t i = 0; i < total_count; i++) {
        ham_u32_t next = get_chunk_offset(i) + get_chunk_size(i);
        if (next >= next_offset)
          next_offset = next;
      }
      return (next_offset);
    }

    // Sets the offset of the unused space at the end of the page
    void set_next_offset(ham_u32_t next_offset) {
      *(ham_u32_t *)(m_data + 4) = next_offset;
    }

    // Returns the capacity
    size_t get_capacity() const {
      return (*(ham_u32_t *)(m_data + 8));
    }

    // Sets the capacity (number of slots)
    void set_capacity(size_t capacity) {
      ham_assert(capacity > 0);
      *(ham_u32_t *)(m_data + 8) = (ham_u32_t)capacity;
    }

    // The physical data in the node
    ham_u8_t *m_data;

    // The size of the offset; either 16 or 32 bits, depending on page size
    size_t m_sizeof_offset;

    // The size of the range, in bytes
    size_t m_range_size;

    // A counter to indicate when rearranging the data makes sense
    int m_vacuumize_counter;
};

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
class VariableLengthKeyList : public BaseKeyList
{
    // for caching external keys
    typedef std::map<ham_u64_t, ByteArray> ExtKeyCache;

  public:
    typedef ham_u8_t type;

    enum {
      // A flag whether this KeyList has sequential data
      kHasSequentialData = 0,

      // A flag whether this KeyList supports the scan() call
      kSupportsBlockScans = 0
    };

    // Constructor
    VariableLengthKeyList(LocalDatabase *db)
      : m_db(db), m_index(db), m_data(0) {
      size_t page_size = db->get_local_env()->get_page_size();
      if (Globals::ms_extended_threshold)
        m_extkey_threshold = Globals::ms_extended_threshold;
      else {
        if (page_size == 1024)
          m_extkey_threshold = 64;
        else if (page_size <= 1024 * 8)
          m_extkey_threshold = 128;
        else {
          // UpfrontIndex's chunk size has 8 bit (max 255), and reserve
          // a few bytes for metadata (flags)
          m_extkey_threshold = 250;
        }
      }
    }

    // Creates a new KeyList starting at |ptr|, total size is
    // |range_size| (in bytes)
    void create(ham_u8_t *data, size_t range_size) {
      m_data = data;
      m_range_size = range_size;
      m_index.create(m_data, range_size, range_size / get_full_key_size());
    }

    // Opens an existing KeyList
    void open(ham_u8_t *data, size_t range_size, size_t node_count) {
      m_data = data;
      m_range_size = range_size;
      m_index.open(m_data, range_size);
    }

    // Has support for SIMD style search?
    bool has_simd_support() const {
      return (false);
    }

    // Calculates the required size for a range
    size_t get_required_range_size(size_t node_count) const {
      return (m_index.get_required_range_size(node_count));
    }

    // Returns the actual key size including overhead. This is an estimate
    // since we don't know how large the keys will be
    size_t get_full_key_size(const ham_key_t *key = 0) const {
      if (!key)
        return (24 + m_index.get_full_index_size() + 1);
      // always make sure to have enough space for an extkey id
      if (key->size < 8 || key->size > m_extkey_threshold)
        return (sizeof(ham_u64_t) + m_index.get_full_index_size() + 1);
      return (key->size + m_index.get_full_index_size() + 1);
    }

    // Copies a key into |dest|
    void get_key(ham_u32_t slot, ByteArray *arena, ham_key_t *dest,
                    bool deep_copy = true) {
      ham_key_t tmp;
      ham_u32_t offset = m_index.get_chunk_offset(slot);
      ham_u8_t *p = m_index.get_chunk_data_by_offset(offset);

      if (unlikely(*p & BtreeKey::kExtendedKey)) {
        memset(&tmp, 0, sizeof(tmp));
        get_extended_key(get_extended_blob_id(slot), &tmp);
      }
      else {
        tmp.size = get_key_size(slot);
        tmp.data = p + 1;
      }

      dest->size = tmp.size;

      if (likely(deep_copy == false)) {
        dest->data = tmp.data;
        return;
      }

      // allocate memory (if required)
      if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
        arena->resize(tmp.size);
        dest->data = arena->get_ptr();
      }
      memcpy(dest->data, tmp.data, tmp.size);
    }

    // Returns the threshold when switching from binary search to
    // linear search. For this layout we do not want to use any linear
    // search, therefore return -1.
    size_t get_linear_search_threshold() const {
      return ((size_t)-1);
    }

    // Performs a linear search in a given range between |start| and
    // |start + length|. Not implemented - callers must not use linear
    // search with this KeyList.
    template<typename Cmp>
    int linear_search(ham_u32_t start, ham_u32_t count, ham_key_t *hkey,
                    Cmp &comparator, int *pcmp) {
      ham_assert(!"shouldn't be here");
      throw Exception(HAM_INTERNAL_ERROR);
    }

    // Iterates all keys, calls the |visitor| on each. Not supported by
    // this KeyList implementation. For variable length keys, the caller
    // must iterate over all keys. The |scan()| interface is only implemented
    // for PAX style layouts.
    void scan(ScanVisitor *visitor, size_t node_count, ham_u32_t start) {
      ham_assert(!"shouldn't be here");
      throw Exception(HAM_INTERNAL_ERROR);
    }

    // Erases a key's payload. Does NOT remove the chunk from the UpfrontIndex
    // (see |erase_slot()|).
    void erase_data(ham_u32_t slot) {
      ham_u8_t flags = get_key_flags(slot);
      if (flags & BtreeKey::kExtendedKey) {
        // delete the extended key from the cache
        erase_extended_key(get_extended_blob_id(slot));
        // and transform into a key which is non-extended and occupies
        // the same space as before, when it was extended
        set_key_flags(slot, flags & (~BtreeKey::kExtendedKey));
        set_key_size(slot, sizeof(ham_u64_t));
      }
    }

    // Erases a key, including extended blobs
    void erase_slot(size_t node_count, ham_u32_t slot) {
      erase_data(slot);
      m_index.erase_slot(node_count, slot);
    }

    // Inserts the |key| at the position identified by |slot|.
    // This method cannot fail; there MUST be sufficient free space in the
    // node (otherwise the caller would have split the node).
    void insert(size_t node_count, ham_u32_t slot, const ham_key_t *key) {
      m_index.insert_slot(node_count, slot);

      // now there's one additional slot
      node_count++;

      ham_u32_t key_flags = 0;

      // When inserting the data: always add 1 byte for key flags
      if (key->size <= m_extkey_threshold
            && m_index.can_allocate_space(node_count, key->size + 1)) {
        ham_u32_t offset = m_index.allocate_space(node_count, slot,
                        key->size + 1);
        ham_u8_t *p = m_index.get_chunk_data_by_offset(offset);
        *p = key_flags;
        memcpy(p + 1, key->data, key->size); // and data
      }
      else {
        ham_u64_t blob_id = add_extended_key(key);
        m_index.allocate_space(node_count, slot, 8 + 1);
        set_extended_blob_id(slot, blob_id);
        set_key_flags(slot, key_flags | BtreeKey::kExtendedKey);
      }
    }

    // Returns true if the |key| no longer fits into the node and a split
    // is required. Makes sure that there is ALWAYS enough headroom
    // for an extended key!
    bool requires_split(size_t node_count, const ham_key_t *key,
                    bool vacuumize = false) {
      size_t required = key->size + 1;
      // add 1 byte for flags
      if (key->size > m_extkey_threshold || key->size < 8 + 1)
        required = 8 + 1;
      bool ret = m_index.requires_split(node_count, required);
      if (ret == false || vacuumize == false)
        return (ret);
      if (m_index.maybe_vacuumize(node_count))
        ret = requires_split(node_count, key, false);
      return (ret);
    }

    // Copies |count| key from this[sstart] to dest[dstart]
    void copy_to(ham_u32_t sstart, size_t node_count,
                    VariableLengthKeyList &dest, size_t other_node_count,
                    ham_u32_t dstart) {
      ham_assert(node_count - sstart > 0);

      // make sure that the other node has sufficient capacity in its
      // UpfrontIndex
      dest.m_index.change_range_size(other_node_count, 0, 0, node_count);

      size_t i = 0;
      for (; i < node_count - sstart; i++) {
        size_t size = get_key_size(sstart + i);

        ham_u8_t *p = m_index.get_chunk_data_by_offset(
                        m_index.get_chunk_offset(sstart + i));
        ham_u8_t flags = *p;
        ham_u8_t *data = p + 1;

        dest.m_index.insert_slot(other_node_count + i, dstart + i);
        // Add 1 byte for key flags
        ham_u32_t offset = dest.m_index.allocate_space(other_node_count + i + 1,
                        dstart + i, size + 1);
        p = dest.m_index.get_chunk_data_by_offset(offset);
        *p = flags; // sets flags
        memcpy(p + 1, data, size); // and data
      }

      // A lot of keys will be invalidated after copying, therefore make
      // sure that the next_offset is recalculated when it's required
      m_index.invalidate_next_offset();
    }

    // Checks the integrity of this node. Throws an exception if there is a
    // violation.
    void check_integrity(size_t node_count, bool quick = false) const {
      ByteArray arena;

      // verify that the offsets and sizes are not overlapping
      m_index.check_integrity(node_count);
      if (quick)
        return;

      // make sure that extkeys are handled correctly
      for (ham_u32_t i = 0; i < node_count; i++) {
        if (get_key_size(i) > m_extkey_threshold
            && !(get_key_flags(i) & BtreeKey::kExtendedKey)) {
          ham_log(("key size %d, but key is not extended", get_key_size(i)));
          throw Exception(HAM_INTEGRITY_VIOLATED);
        }

        if (get_key_flags(i) & BtreeKey::kExtendedKey) {
          ham_u64_t blobid = get_extended_blob_id(i);
          if (!blobid) {
            ham_log(("integrity check failed: item %u "
                    "is extended, but has no blob", i));
            throw Exception(HAM_INTEGRITY_VIOLATED);
          }

          // make sure that the extended blob can be loaded
          ham_record_t record = {0};
          m_db->get_local_env()->get_blob_manager()->read(m_db, blobid,
                          &record, 0, &arena);

          // compare it to the cached key (if there is one)
          if (m_extkey_cache) {
            ExtKeyCache::iterator it = m_extkey_cache->find(blobid);
            if (it != m_extkey_cache->end()) {
              if (record.size != it->second.get_size()) {
                ham_log(("Cached extended key differs from real key"));
                throw Exception(HAM_INTEGRITY_VIOLATED);
              }
              if (memcmp(record.data, it->second.get_ptr(), record.size)) {
                ham_log(("Cached extended key differs from real key"));
                throw Exception(HAM_INTEGRITY_VIOLATED);
              }
            }
          }
        }
      }
    }

    // Rearranges the list
    void vacuumize(size_t node_count, bool force) {
      if (force)
        m_index.increase_vacuumize_counter(1);
      m_index.vacuumize(node_count);
    }

    // Change the range size; the capacity will be adjusted, the data is
    // copied as necessary
    void change_range_size(size_t node_count, ham_u8_t *new_data_ptr,
            size_t new_range_size) {
      m_index.change_range_size(node_count, new_data_ptr, new_range_size,
                        node_count + 1); // TODO +1 is bogus - only increase
                                         // if new capacity is required! (but
                                         // we don't know that here)
      m_data = new_data_ptr;
      m_range_size = new_range_size;
    }

    // Prints a slot to |out| (for debugging)
    void print(ham_u32_t slot, std::stringstream &out) {
      ham_key_t tmp = {0};
      if (get_key_flags(slot) & BtreeKey::kExtendedKey) {
        get_extended_key(get_extended_blob_id(slot), &tmp);
      }
      else {
        tmp.size = get_key_size(slot);
        tmp.data = get_key_data(slot);
      }
      out << (const char *)tmp.data;
    }

  private:
    // Returns the flags of a key. Flags are defined in btree_flags.h
    ham_u8_t get_key_flags(ham_u32_t slot) const {
      ham_u32_t offset = m_index.get_chunk_offset(slot);
      return (*m_index.get_chunk_data_by_offset(offset));
    }

    // Sets the flags of a key. Flags are defined in btree_flags.h
    void set_key_flags(ham_u32_t slot, ham_u8_t flags) {
      ham_u32_t offset = m_index.get_chunk_offset(slot);
      *m_index.get_chunk_data_by_offset(offset) = flags;
    }

    // Returns the pointer to a key's inline data
    ham_u8_t *get_key_data(ham_u32_t slot) {
      ham_u32_t offset = m_index.get_chunk_offset(slot);
      return (m_index.get_chunk_data_by_offset(offset) + 1);
    }

    // Returns the pointer to a key's inline data (const flavour)
    ham_u8_t *get_key_data(ham_u32_t slot) const {
      ham_u32_t offset = m_index.get_chunk_offset(slot);
      return (m_index.get_chunk_data_by_offset(offset) + 1);
    }

    // Overwrites the (inline) data of the key
    void set_key_data(ham_u32_t slot, const void *ptr, size_t size) {
      ham_assert(m_index.get_chunk_size(slot) >= size);
      set_key_size(slot, (ham_u16_t)size);
      memcpy(get_key_data(slot), ptr, size);
    }

    // Returns the size of a key
    size_t get_key_size(ham_u32_t slot) const {
      return (m_index.get_chunk_size(slot) - 1);
    }

    // Sets the size of a key
    void set_key_size(ham_u32_t slot, size_t size) {
      ham_assert(size + 1 <= m_index.get_chunk_size(slot));
      m_index.set_chunk_size(slot, size + 1);
    }

    // Returns the record address of an extended key overflow area
    ham_u64_t get_extended_blob_id(ham_u32_t slot) const {
      return (*(ham_u64_t *)get_key_data(slot));
    }

    // Sets the record address of an extended key overflow area
    void set_extended_blob_id(ham_u32_t slot, ham_u64_t blobid) {
      *(ham_u64_t *)get_key_data(slot) = blobid;
    }

    // Erases an extended key from disk and from the cache
    void erase_extended_key(ham_u64_t blobid) {
      m_db->get_local_env()->get_blob_manager()->erase(m_db, blobid);
      if (m_extkey_cache) {
        ExtKeyCache::iterator it = m_extkey_cache->find(blobid);
        if (it != m_extkey_cache->end())
          m_extkey_cache->erase(it);
      }
    }

    // Retrieves the extended key at |blobid| and stores it in |key|; will
    // use the cache.
    void get_extended_key(ham_u64_t blob_id, ham_key_t *key) {
      if (!m_extkey_cache)
        m_extkey_cache.reset(new ExtKeyCache());
      else {
        ExtKeyCache::iterator it = m_extkey_cache->find(blob_id);
        if (it != m_extkey_cache->end()) {
          key->size = it->second.get_size();
          key->data = it->second.get_ptr();
          return;
        }
      }

      ByteArray arena;
      ham_record_t record = {0};
      m_db->get_local_env()->get_blob_manager()->read(m_db, blob_id, &record,
                      0, &arena);
      (*m_extkey_cache)[blob_id] = arena;
      arena.disown();
      key->data = record.data;
      key->size = record.size;
    }

    // Allocates an extended key and stores it in the cache
    ham_u64_t add_extended_key(const ham_key_t *key) {
      if (!m_extkey_cache)
        m_extkey_cache.reset(new ExtKeyCache());

      ham_record_t rec = {0};
      rec.data = key->data;
      rec.size = key->size;

      ham_u64_t blob_id = m_db->get_local_env()->get_blob_manager()->allocate(
                                            m_db, &rec, 0);
      ham_assert(blob_id != 0);
      ham_assert(m_extkey_cache->find(blob_id) == m_extkey_cache->end());

      ByteArray arena;
      arena.resize(key->size);
      memcpy(arena.get_ptr(), key->data, key->size);
      (*m_extkey_cache)[blob_id] = arena;
      arena.disown();

      // increment counter (for statistics)
      Globals::ms_extended_keys++;

      return (blob_id);
    }

    // The database
    LocalDatabase *m_db;

    // The index for managing the variable-length chunks
    UpfrontIndex m_index;

    // Pointer to the data of the node 
    ham_u8_t *m_data;

    // Cache for extended keys
    ScopedPtr<ExtKeyCache> m_extkey_cache;

    // Threshold for extended keys; if key size is > threshold then the
    // key is moved to a blob
    size_t m_extkey_threshold;
};

} // namespace DefLayout

} // namespace hamsterdb

#endif /* HAM_BTREE_KEYS_VARLEN_H */
