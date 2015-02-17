/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
 * A small index which manages variable length buffers. Used to manage
 * variable length keys or records.
 *
 * The UpfrontIndex manages a range of bytes, organized in variable length
 * |chunks|, assigned at initialization time when calling |allocate()|
 * or |open()|. 
 * 
 * These chunks are organized in |slots|, each slot stores the offset and
 * the size of the chunk data. The offset is stored as 16- or 32-bit, depending
 * on the page size. The size is always a 16bit integer.
 *
 * The number of used slots is not stored in the UpfrontIndex, since it is
 * already managed in the caller (this is equal to |PBtreeNode::get_count()|).
 * Therefore you will see a lot of methods receiving a |node_count| parameter.
 *
 * Deleted chunks are moved to a |freelist|, which is simply a list of slots
 * directly following those slots that are in use.
 *
 * In addition, the UpfrontIndex keeps track of the unused space at the end
 * of the range (via |get_next_offset()|), in order to allow a fast
 * allocation of space.
 *
 * The UpfrontIndex stores metadata at the beginning:
 *     [0..3]  freelist count
 *     [4..7]  next offset
 *     [8..11] capacity
 *
 * Data is stored in the following layout:
 * |metadata|slot1|slot2|...|slotN|free1|free2|...|freeM|data1|data2|...|dataN|
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_BTREE_UPFRONT_INDEX_H
#define HAM_BTREE_UPFRONT_INDEX_H

#include "0root/root.h"

#include <algorithm>
#include <vector>

// Always verify that a file of level N does not include headers > N!
#include "1globals/globals.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

namespace DefLayout {

/*
 * A helper class to sort ranges; used during validation of the up-front
 * index in check_index_integrity()
 */
struct SortHelper {
  uint32_t offset;
  int slot;

  bool operator<(const SortHelper &rhs) const {
    return (offset < rhs.offset);
  }
};

static bool
sort_by_offset(const SortHelper &lhs, const SortHelper &rhs) {
  return (lhs.offset < rhs.offset);
}

class UpfrontIndex
{
    enum {
      // width of the 'size' field
      kSizeofSize = 1 // 1 byte - max chunk size is 255
    };

  public:
    enum {
      // for freelist_count, next_offset, capacity
      kPayloadOffset = 12,
    
      // minimum capacity of the index
      kMinimumCapacity = 16
    };

    // Constructor; creates an empty index which needs to be initialized
    // with |create()| or |open()|.
    UpfrontIndex(LocalDatabase *db)
      : m_data(0), m_range_size(0), m_vacuumize_counter(0) {
      size_t page_size = db->lenv()->config().page_size_bytes;
      if (page_size <= 64 * 1024)
        m_sizeof_offset = 2;
      else
        m_sizeof_offset = 4;
    }

    // Initialization routine; sets data pointer, range size and the
    // initial capacity.
    void create(uint8_t *data, size_t range_size, size_t capacity) {
      m_data = data;
      m_range_size = range_size;
      set_capacity(capacity);
      clear();
    }

    // "Opens" an existing index from memory. This method sets the data
    // pointer and initializes itself.
    void open(uint8_t *data, size_t range_size) {
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
    void change_range_size(size_t node_count, uint8_t *new_data_ptr,
                    size_t new_range_size, size_t new_capacity) {
      if (!new_data_ptr)
        new_data_ptr = m_data;
      if (!new_range_size)
        new_range_size = m_range_size;

      // get rid of the freelist and collect the garbage
      if (get_freelist_count() > 0)
        vacuumize(node_count);
      ham_assert(get_freelist_count() == 0);

      size_t used_data_size = get_next_offset(node_count); 
      size_t old_capacity = get_capacity();
      uint8_t *src = &m_data[kPayloadOffset
                            + old_capacity * get_full_index_size()];
      uint8_t *dst = &new_data_ptr[kPayloadOffset
                            + new_capacity * get_full_index_size()];

      // if old range == new range then leave
      if (m_range_size == new_range_size
            && old_capacity == new_capacity 
            && m_data == new_data_ptr )
        return;

      ham_assert(dst - new_data_ptr + used_data_size <= new_range_size);

      // shift "to the right"? Then first move the data and afterwards
      // the index
      if (dst > src) {
        memmove(dst, src, used_data_size);
        memmove(new_data_ptr, m_data,
                kPayloadOffset + new_capacity * get_full_index_size());
      }
      // vice versa otherwise
      else if (dst <= src) {
        if (new_data_ptr != m_data)
          memmove(new_data_ptr, m_data,
                  kPayloadOffset + new_capacity * get_full_index_size());
        memmove(dst, src, used_data_size);
      }

      m_data = new_data_ptr;
      m_range_size = new_range_size;
      set_capacity(new_capacity);
      set_freelist_count(0);
      set_next_offset(used_data_size); // has dependency to get_freelist_count()
    }

    // Calculates the required size for a range
    size_t get_required_range_size(size_t node_count) const {
      return (UpfrontIndex::kPayloadOffset
                    + get_capacity() * get_full_index_size()
                    + get_next_offset(node_count));
    }

    // Returns the size of a single index entry
    size_t get_full_index_size() const {
      return (m_sizeof_offset + kSizeofSize);
    }

    // Transforms a relative offset of the payload data to an absolute offset
    // in |m_data|
    uint32_t get_absolute_offset(uint32_t offset) const {
      return (offset
                      + kPayloadOffset
                      + get_capacity() * get_full_index_size());
    }

    // Returns the absolute start offset of a chunk
    uint32_t get_absolute_chunk_offset(int slot) const {
      return (get_absolute_offset(get_chunk_offset(slot)));
    }

    // Returns the relative start offset of a chunk
    uint32_t get_chunk_offset(int slot) const {
      uint8_t *p = &m_data[kPayloadOffset + get_full_index_size() * slot];
      if (m_sizeof_offset == 2)
        return (*(uint16_t *)p);
      else {
        ham_assert(m_sizeof_offset == 4);
        return (*(uint32_t *)p);
      }
    }

    // Returns the size of a chunk
    uint16_t get_chunk_size(int slot) const {
      return (m_data[kPayloadOffset + get_full_index_size() * slot
                                + m_sizeof_offset]);
    }

    // Sets the size of a chunk (does NOT actually resize the chunk!)
    void set_chunk_size(int slot, uint16_t size) {
      ham_assert(size <= 255);
      m_data[kPayloadOffset + get_full_index_size() * slot + m_sizeof_offset]
              = (uint8_t)size;
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
    bool can_insert(size_t node_count) {
      return (likely(node_count + get_freelist_count() < get_capacity()));
    }

    // Inserts a slot at the position |slot|. |node_count| is the number of
    // used slots (this is managed by the caller)
    void insert(size_t node_count, int slot) {
      ham_assert(can_insert(node_count) == true);

      size_t slot_size = get_full_index_size();
      size_t total_count = node_count + get_freelist_count();
      uint8_t *p = &m_data[kPayloadOffset + slot_size * slot];
      if (total_count > 0 && slot < (int)total_count) {
        // create a gap in the index
        memmove(p + slot_size, p, slot_size * (total_count - slot));
     }

      // now fill the gap
      memset(p, 0, slot_size);
    }

    // Erases a slot at the position |slot|
    // |node_count| is the number of used slots (this is managed by the caller)
    void erase(size_t node_count, int slot) {
      size_t slot_size = get_full_index_size();
      size_t total_count = node_count + get_freelist_count();

      ham_assert(slot < (int)total_count);

      set_freelist_count(get_freelist_count() + 1);

      size_t chunk_size = get_chunk_size(slot);

      increase_vacuumize_counter(chunk_size);

      // nothing to do if we delete the very last (used) slot; the freelist
      // counter was already incremented, the used counter is decremented
      // by the caller
      if (slot == (int)node_count - 1)
        return;

      size_t chunk_offset = get_chunk_offset(slot);

      // shift all items to the left
      uint8_t *p = &m_data[kPayloadOffset + slot_size * slot];
      memmove(p, p + slot_size, slot_size * (total_count - slot));

      // then copy the deleted chunk to the freelist
      set_chunk_offset(total_count - 1, chunk_offset);
      set_chunk_size(total_count - 1, chunk_size);
    }

    // Adds a chunk to the freelist. Will not do anything if the node
    // is already full.
    void add_to_freelist(size_t node_count, uint32_t chunk_offset,
                    uint32_t chunk_size) {
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
      uint32_t total_count = node_count + get_freelist_count();
      for (uint32_t i = node_count; i < total_count; i++)
        if (get_chunk_size(i) >= num_bytes)
          return (true);
      return (false);
    }

    // Allocates space for a |slot| and returns the offset of that chunk
    uint32_t allocate_space(size_t node_count, int slot,
                    size_t num_bytes) {
      ham_assert(can_allocate_space(node_count, num_bytes));

      size_t next_offset = get_next_offset(node_count);

      // try to allocate space at the end of the node
      if (next_offset + num_bytes <= get_usable_data_size()) {
        uint32_t offset = get_chunk_offset(slot);
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
      uint32_t total_count = node_count + get_freelist_count();
      for (uint32_t i = node_count; i < total_count; i++) {
        uint32_t chunk_size = get_chunk_size(i);
        uint32_t chunk_offset = get_chunk_offset(i);
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
            uint8_t *p = &m_data[kPayloadOffset + slot_size * i];
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
    bool requires_split(size_t node_count, size_t required_size) {
      return (!can_insert(node_count)
                || !can_allocate_space(node_count, required_size));
    }

    // Verifies that there are no overlapping chunks
    void check_integrity(size_t node_count) const {
      typedef std::pair<uint32_t, uint32_t> Range;
      //typedef std::vector<Range> RangeVec;
      uint32_t total_count = node_count + get_freelist_count();

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
      uint32_t next_offset = 0;
      for (uint32_t i = 0; i < total_count; i++) {
        Range range = std::make_pair(get_chunk_offset(i), get_chunk_size(i));
        uint32_t next = range.first + range.second;
        if (next >= next_offset)
          next_offset = next;
        //ranges.push_back(range);
      }

#if 0
      std::sort(ranges.begin(), ranges.end());

      if (!ranges.empty()) {
        for (uint32_t i = 0; i < ranges.size() - 1; i++) {
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
    void split(UpfrontIndex *other, size_t node_count, int pivot) {
      other->clear();

      // now copy key by key
      for (size_t i = pivot; i < node_count; i++) {
        other->insert(i - pivot, i - pivot);
        uint32_t size = get_chunk_size(i);
        uint32_t offset = other->allocate_space(i - pivot, i - pivot, size);
        memcpy(other->get_chunk_data_by_offset(offset),
                    get_chunk_data_by_offset(get_chunk_offset(i)),
                    size);
      }

      // this node has lost lots of its data - make sure that it will be
      // vacuumized as soon as more data is allocated
      m_vacuumize_counter += node_count;
      set_freelist_count(0);
      set_next_offset((uint32_t)-1);
    }

    // Merges all chunks from the |other| index to this index
    void merge_from(UpfrontIndex *other, size_t node_count,
                    size_t other_node_count) {
      vacuumize(node_count);
      
      for (size_t i = 0; i < other_node_count; i++) {
        insert(i + node_count, i + node_count);
        uint32_t size = other->get_chunk_size(i);
        uint32_t offset = allocate_space(i + node_count, i + node_count, size);
        memcpy(get_chunk_data_by_offset(offset),
                    other->get_chunk_data_by_offset(other->get_chunk_offset(i)),
                    size);
      }

      other->clear();
    }

    // Returns a pointer to the actual data of a chunk
    uint8_t *get_chunk_data_by_offset(uint32_t offset) {
      return (&m_data[kPayloadOffset
                      + get_capacity() * get_full_index_size()
                      + offset]);
    }

    // Returns a pointer to the actual data of a chunk
    uint8_t *get_chunk_data_by_offset(uint32_t offset) const {
      return (&m_data[kPayloadOffset
                      + get_capacity() * get_full_index_size()
                      + offset]);
    }

    // Reduces the capacity of the UpfrontIndex, if required
    void reduce_capacity(size_t node_count) {
      size_t old_capacity = get_capacity();
      if (node_count > 0 && old_capacity > node_count + 4) {
        size_t new_capacity = old_capacity - (old_capacity - node_count) / 2;
        if (new_capacity != old_capacity)
          change_range_size(node_count, m_data, m_range_size, new_capacity);
      }
    }

    // Re-arranges the node: moves all keys sequentially to the beginning
    // of the key space, removes the whole freelist.
    //
    // This call is extremely expensive! Try to avoid it as much as possible.
    void vacuumize(size_t node_count) {
      if (m_vacuumize_counter < 10) {
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
      for (size_t i = 0; i < node_count; i++) {
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
      uint32_t next_offset = 0;
      uint32_t start = kPayloadOffset + get_capacity() * get_full_index_size();
      for (size_t i = 0; i < node_count; i++) {
        uint32_t offset = s[i].offset;
        int slot = s[i].slot;
        uint32_t size = get_chunk_size(slot);
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
      set_next_offset((uint32_t)-1);
    }

    // Same as above, but only if the next_offset equals |new_offset|
    void maybe_invalidate_next_offset(size_t new_offset) {
      if (get_next_offset(0) == new_offset)
        invalidate_next_offset();
    }

    // Returns the capacity
    size_t get_capacity() const {
      return (*(uint32_t *)(m_data + 8));
    }

    // Returns the offset of the unused space at the end of the page
    uint32_t get_next_offset(size_t node_count) {
      uint32_t ret = *(uint32_t *)(m_data + 4);
      if (unlikely(ret == (uint32_t)-1 && node_count > 0)) {
        ret = calc_next_offset(node_count);
        set_next_offset(ret);
      }
      return (ret);
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
    // (const version)
    uint32_t get_next_offset(size_t node_count) const {
      uint32_t ret = *(uint32_t *)(m_data + 4);
      if (unlikely(ret == (uint32_t)-1))
        return (calc_next_offset(node_count));
      return (ret);
    }

    // Returns the size (in bytes) where payload data can be stored
    size_t get_usable_data_size() const {
      return (m_range_size - kPayloadOffset
                      - get_capacity() * get_full_index_size());
    }

    // Sets the chunk offset of a slot
    void set_chunk_offset(int slot, uint32_t offset) {
      uint8_t *p = &m_data[kPayloadOffset + get_full_index_size() * slot];
      if (m_sizeof_offset == 2)
        *(uint16_t *)p = (uint16_t)offset;
      else
        *(uint32_t *)p = offset;
    }

    // Returns the number of freelist entries
    size_t get_freelist_count() const {
      return (*(uint32_t *)m_data);
    }

    // Sets the number of freelist entries
    void set_freelist_count(size_t freelist_count) {
      ham_assert(freelist_count <= get_capacity());
      *(uint32_t *)m_data = freelist_count;
    }

    // Calculates and returns the next offset; does not store it
    uint32_t calc_next_offset(size_t node_count) const {
      uint32_t total_count = node_count + get_freelist_count();
      uint32_t next_offset = 0;
      for (uint32_t i = 0; i < total_count; i++) {
        uint32_t next = get_chunk_offset(i) + get_chunk_size(i);
        if (next >= next_offset)
          next_offset = next;
      }
      return (next_offset);
    }

    // Sets the offset of the unused space at the end of the page
    void set_next_offset(uint32_t next_offset) {
      *(uint32_t *)(m_data + 4) = next_offset;
    }

    // Sets the capacity (number of slots)
    void set_capacity(size_t capacity) {
      ham_assert(capacity > 0);
      *(uint32_t *)(m_data + 8) = (uint32_t)capacity;
    }

    // The physical data in the node
    uint8_t *m_data;

    // The size of the offset; either 16 or 32 bits, depending on page size
    size_t m_sizeof_offset;

    // The size of the range, in bytes
    size_t m_range_size;

    // A counter to indicate when rearranging the data makes sense
    int m_vacuumize_counter;
};

} // namespace DefLayout

} // namespace hamsterdb

#endif /* HAM_BTREE_UPFRONT_INDEX_H */
