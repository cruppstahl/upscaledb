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
 * Base class for key lists where keys are separated in blocks
 *
 * @exception_safe: strong
 * @thread_safe: no
 */

#ifndef HAM_BTREE_KEYS_BLOCK_H
#define HAM_BTREE_KEYS_BLOCK_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "3btree/btree_node.h"
#include "3btree/btree_keys_base.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

//
// The template classes in this file are wrapped in a separate namespace
// to avoid naming clashes with other KeyLists
//
namespace Zint32 {

/*
 * A helper class to sort ranges; used in vacuumize()
 */
struct SortHelper {
  uint32_t offset;
  int index;

  bool operator<(const SortHelper &rhs) const {
    return (offset < rhs.offset);
  }
};

static bool
sort_by_offset(const SortHelper &lhs, const SortHelper &rhs) {
  return (lhs.offset < rhs.offset);
}

template<typename IndexType>
class BlockKeyList : public BaseKeyList
{
  public:
    typedef IndexType Index;

    enum {
      // A flag whether this KeyList has sequential data
      kHasSequentialData = 0,

      // A flag whether this KeyList supports the scan() call
      kSupportsBlockScans = 1,

      // Use a custom search implementation
      kSearchImplementation = kCustomSearch,

      // Use a custom insert implementation
      kCustomInsert = 1
    };

    // Constructor
    BlockKeyList(LocalDatabase *db)
      : m_data(0), m_range_size(0) {
    }

    // Creates a new KeyList starting at |data|, total size is
    // |range_size_bytes| (in bytes)
    void create(uint8_t *data, size_t range_size) {
      m_data = data;
      m_range_size = range_size;

      initialize();
    }

    // Opens an existing KeyList. Called after a btree node was fetched from
    // disk.
    void open(uint8_t *data, size_t range_size, size_t node_count) {
      m_data = data;
      m_range_size = range_size;
    }

    // Returns the required size for this KeyList. Required to re-arrange
    // the space between KeyList and RecordList.
    size_t get_required_range_size(size_t node_count) const {
      return (get_used_size());
    }

    // Returns the size or a single key including overhead. This is an estimate,
    // required to calculate the capacity of a node.
    size_t get_full_key_size(const ham_key_t *key = 0) const {
      return (3);
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
    bool requires_split(size_t node_count, const ham_key_t *key) const {
      return (false);
    }

    // Change the range size. Called when the range of the btree node is
    // re-distributed between KeyList and RecordList (to avoid splits).
    void change_range_size(size_t node_count, uint8_t *new_data_ptr,
                    size_t new_range_size, size_t capacity_hint) {
      if (m_data != new_data_ptr) {
        ::memmove(new_data_ptr, m_data, get_used_size());

        m_data = new_data_ptr;
      }
      m_range_size = new_range_size;
    }

    // "Vacuumizes" the KeyList; packs all blocks tightly to reduce the size
    // that is consumed by this KeyList.
    void vacuumize(size_t node_count, bool force) {
      ham_assert(check_integrity(node_count));
      ham_assert(get_block_count() > 0);

      if (node_count == 0)
        initialize();
      else
        vacuumize_impl(false);

      ham_assert(check_integrity(node_count));
    }

    // Checks the integrity of this node. Throws an exception if there is a
    // violation.
    bool check_integrity(size_t node_count) const {
      ham_assert(get_block_count() > 0);
      Index *index = get_block_index(0);
      Index *end = get_block_index(get_block_count());

      size_t total_keys = 0;
      int used_size = 0;
      for (; index < end; index++) {
        if (node_count > 0)
          ham_assert(index->key_count > 0);
        total_keys += index->key_count;
        if ((uint32_t)used_size < index->offset + index->get_block_size())
          used_size = index->offset + index->get_block_size();
      }

      // add static overhead
      used_size += 8 + sizeof(Index) * get_block_count();

      if (used_size != (int)get_used_size()) {
        ham_log(("used size %d differs from expected %d",
                (int)used_size, (int)get_used_size()));
        throw Exception(HAM_INTEGRITY_VIOLATED);
      }

      if (used_size > (int)m_range_size) {
        ham_log(("used size %d exceeds range size %d",
                (int)used_size, (int)m_range_size));
        throw Exception(HAM_INTEGRITY_VIOLATED);
      }

      if (total_keys != node_count) {
        ham_log(("key count %d differs from expected %d",
                (int)total_keys, (int)node_count));
        throw Exception(HAM_INTEGRITY_VIOLATED);
      }

      return (true);
    }

    // Returns the size of a key; only required to appease the compiler,
    // but never called
    size_t get_key_size(int slot) const {
      ham_assert(!"shouldn't be here");
      return (sizeof(uint32_t));
    }

    // Returns a pointer to the key's data; only required to appease the
    // compiler, but never called
    uint8_t *get_key_data(int slot) {
      ham_assert(!"shouldn't be here");
      return (0);
    }

    // Fills the btree_metrics structure
    void fill_metrics(btree_metrics_t *metrics, size_t node_count) {
      BaseKeyList::fill_metrics(metrics, node_count);
      BtreeStatistics::update_min_max_avg(&metrics->keylist_index,
              (uint32_t)(get_block_count() * sizeof(Index)));
      BtreeStatistics::update_min_max_avg(&metrics->keylist_blocks_per_page,
              get_block_count());

      Index *index = get_block_index(0);
      Index *end = get_block_index(get_block_count());

      int used_size = 0;
      for (; index < end; index++) {
        used_size += sizeof(Index) + index->get_used_size();
        BtreeStatistics::update_min_max_avg(&metrics->keylist_block_sizes,
                index->get_block_size());
      }
      BtreeStatistics::update_min_max_avg(&metrics->keylist_unused,
              m_range_size - used_size);
    }

  protected:
    // Create an initial empty block
    void initialize() {
      set_block_count(0);
      set_used_size(sizeof(uint32_t) * 2);
      add_block(0, Index::kInitialBlockSize);
    }

    // Calculates the used size and updates the stored value
    void reset_used_size() {
      Index *index = get_block_index(0);
      Index *end = get_block_index(get_block_count());
      uint32_t used_size = 0;
      for (; index < end; index++) {
        if (index->offset + index->get_block_size() > used_size)
          used_size = index->offset + index->get_block_size();
      }
      set_used_size(used_size + 8 + sizeof(Index) * get_block_count());
    }

    // Returns the index for a block with that slot
    Index *find_block_by_slot(int slot, int *position_in_block) const {
      ham_assert(get_block_count() > 0);
      Index *index = get_block_index(0);
      Index *end = get_block_index(get_block_count());

      for (; index < end; index++) {
        if (index->key_count > slot) {
          *position_in_block = slot;
          return (index);
        } 

        slot -= index->key_count;
      }

      *position_in_block = slot;
      return (index - 1);
    }

    // Performs a linear search through the index; returns the index
    // and the slot of the first key in this block in |*pslot|.
    Index *find_index(uint32_t key, int *pslot) {
      Index *index = get_block_index(0);
      Index *iend = get_block_index(get_block_count());

      if (key < index->value) {
        *pslot = -1;
        return (index);
      }

      *pslot = 0;

      for (; index < iend - 1; index++) {
        if (key < (index + 1)->value)
          break;
        *pslot += index->key_count;
      }

      return (index);
    }

    // Inserts a new block at the specified |position|
    Index *add_block(int position, int initial_size) {
      check_available_size(initial_size + sizeof(Index));

      ham_assert(initial_size > 0);

      // shift the whole data to the right to make space for the new block
      // index
      Index *index = get_block_index(position);

      if (get_block_count() != 0) {
        ::memmove(index + 1, index,
                        get_used_size() - (position * sizeof(Index)));
      }

      set_block_count(get_block_count() + 1);
      set_used_size(get_used_size() + sizeof(Index) + initial_size);

      // initialize the new block index; the offset is relative to the start
      // of the payload data, and does not include the indices
      index->initialize(get_used_size() - 2 * sizeof(uint32_t)
                            - sizeof(Index) * get_block_count() - initial_size,
                          initial_size);
      return (index);
    }

    // Removes the specified block
    void remove_block(Index *index) {
      ham_assert(get_block_count() > 1);
      ham_assert(index->key_count == 0);

      bool do_reset_used_size = false;
      // is this the last block? then re-calculate the |used_size|, because
      // there may be other unused blocks at the end
      if (get_used_size() == index->offset
                                + index->get_block_size()
                                + get_block_count() * sizeof(Index)
                                + 8)
        do_reset_used_size = true;

      // shift all indices (and the payload data) to the left
      ::memmove(index, index + 1, get_used_size()
                    - sizeof(Index) * (index - get_block_index(0) + 1));
      set_block_count(get_block_count() - 1);
      if (do_reset_used_size) {
        reset_used_size();
      }
      else {
        set_used_size(get_used_size() - sizeof(Index));
      }
    }

    // Grows a block by |additinal_size| bytes
    void grow_block(Index *index, int additional_size) {
      check_available_size(additional_size);

      // move all other blocks unless the current block is the last one
      if ((size_t)index->offset + index->get_block_size()
              < get_used_size() - 8 - sizeof(Index) * get_block_count()) {
        uint8_t *p = get_block_data(index) + index->get_block_size();
        uint8_t *q = &m_data[get_used_size()];
        ::memmove(p + additional_size, p, q - p);

        // now update the offsets of the other blocks
        Index *next = get_block_index(0);
        Index *end = get_block_index(get_block_count());
        for (; next < end; next++)
          if (next->offset > index->offset)
            next->offset += additional_size;
      }

      index->set_block_size(index->get_block_size() + additional_size);
      set_used_size(get_used_size() + additional_size);
    }

    // Checks if this range has enough space for additional |additional_size|
    // bytes. If not then it tries to vacuumize and then checks again.
    // If that also was not successful then an exception is thrown and the
    // Btree layer can re-arrange or split the page.
    void check_available_size(size_t additional_size) {
      if (get_used_size() + additional_size <= m_range_size)
        return;
      vacuumize_impl(true);
      if (get_used_size() + additional_size > m_range_size)
        throw Exception(HAM_LIMITS_REACHED);
    }

    // Vacuumizes the node
    //
    // |internal| is true if the function is invoked by another method of
    // this class (or a derived class), i.e. if growing blocks failed.
    //
    // It is set to false if it's invoked by the public vacuumize() API. 
    virtual void vacuumize_impl(bool internal) = 0;

    // Performs a lower bound search
    int lower_bound_search(uint32_t *begin, uint32_t *end, uint32_t key,
                    int *pcmp) const {
      uint32_t *it = std::lower_bound(begin, end, key);
      if (it != end) {
        *pcmp = (*it == key) ? 0 : +1;
      }
      else { // not found
        *pcmp = +1;
      }
      return ((it - begin) + 1);
    }

    // Returns the payload data of a block
    uint8_t *get_block_data(Index *index) const {
      return (&m_data[8 + index->offset + sizeof(Index) * get_block_count()]);
    }

    // Sets the block count
    void set_block_count(int count) {
      *(uint32_t *)m_data = (uint32_t)count;
    }

    // Returns the block count
    int get_block_count() const {
      return ((int) *(uint32_t *)m_data);
    }

    // Sets the used size of the range
    void set_used_size(size_t used_size) {
      ham_assert(used_size <= m_range_size);
      *(uint32_t *)(m_data + 4) = (uint32_t)used_size;
    }

    // Returns the block count
    size_t get_used_size() const {
      return (*(uint32_t *)(m_data + 4));
    }

    // Returns a pointer to a block index
    Index *get_block_index(int i) const {
      return ((Index *)(m_data + 8 + i * sizeof(Index)));
    }

    // The persisted (compressed) data
    uint8_t *m_data;

    // The size of the persisted data
    size_t m_range_size;
};

} // namespace Zint32

} // namespace hamsterdb

#endif /* HAM_BTREE_KEYS_BLOCK_H */
