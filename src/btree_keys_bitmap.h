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

#ifndef HAM_BTREE_KEYS_BITMAP_H__
#define HAM_BTREE_KEYS_BITMAP_H__

#include "globals.h"
#include "page.h"
#include "btree_node.h"
#include "env_local.h"
#include "btree_index.h"
#include "btree_keys_base.h"
#include "3rdparty/sparsemap/sparsemap.h"

#ifdef WIN32
// MSVC: disable warning about use of 'this' in base member initializer list
#  pragma warning(disable:4355)
#  undef min  // avoid MSVC conflicts with std::min
#endif

using namespace sparsemap;

namespace hamsterdb {

namespace ProLayout {

//
// PRO: KeyList with compressed bitmaps
//
template <typename T>
class BitmapKeyList : public BaseKeyList
{
  public:
    typedef ham_u8_t type;
    typedef ham_u32_t BitVector;

    enum {
      // A flag whether this KeyList has sequential data
      kHasSequentialData = 0,

      // A flag whether this KeyList supports the scan() call
      kSupportsBlockScans = 1
    };

    // Constructor
    BitmapKeyList(LocalDatabase *db)
      : m_db(db), m_data(0) {
    }

    // Creates a new KeyList starting at |ptr|, total size is
    // |full_range_size_bytes| (in bytes)
    void create(ham_u8_t *data, size_t full_range_size_bytes, size_t capacity) {
      m_data = data;
      *(ham_u32_t *)data = (ham_u32_t)full_range_size_bytes;
      m_sparsemap.create(data + sizeof(ham_u32_t), full_range_size_bytes);
    }

    // Opens an existing KeyList
    void open(ham_u8_t *data, size_t capacity) {
      m_data = data;
      size_t full_range_size_bytes = *(ham_u32_t *)data;
      m_sparsemap.open(data + sizeof(ham_u32_t), full_range_size_bytes);
    }

    // Returns the full size of the range
    size_t get_range_size() {
      return (*(ham_u32_t *)m_data);
    }

    // Calculates the required size for a range with the specified |capacity|.
    size_t calculate_required_range_size(size_t node_count,
            size_t new_capacity) {
      int num_minimaps = 1 + new_capacity / MiniMap<BitVector>::kMaxCapacity;
      if (num_minimaps == (int)m_sparsemap.get_minimap_count())
        return (sizeof(ham_u32_t) + m_sparsemap.get_range_size());
      if (num_minimaps > (int)m_sparsemap.get_minimap_count()) {
        num_minimaps -= m_sparsemap.get_minimap_count();
        return (num_minimaps * (sizeof(T)
                      + (sizeof(BitVector) * 4) * sizeof(BitVector)
                      + sizeof(BitVector)));
      }
      if (node_count + MiniMap<BitVector>::kMaxCapacity == new_capacity) {
        return (sizeof(ham_u32_t) + m_sparsemap.get_range_size()
                      + (sizeof(T) + (sizeof(BitVector) * 4) * sizeof(BitVector)
                      + sizeof(BitVector)));
      }
      return (num_minimaps * (sizeof(T)
                    + (sizeof(BitVector) * 4) * sizeof(BitVector)
                    + sizeof(BitVector)));
    }

    // Returns the actual key size including overhead. This is an estimate
    // since we don't know how large the keys will be
    double get_full_key_size(const ham_key_t *key = 0) const {
      // we need at least one bit per key (this is an estimate - sometimes
      // it's more, sometimes it's less)
      return (1.0 / 8.0);
    }

    // Copies a key into |dest|
    void get_key(ham_u32_t slot, ByteArray *arena, ham_key_t *dest,
                    bool deep_copy = true) {
      m_dummy = m_sparsemap.select(slot);
      ham_assert(m_sparsemap.is_set(m_dummy));

      dest->size = sizeof(T);
      if (likely(deep_copy == false)) {
        dest->data = &m_dummy;
        return;
      }

      // allocate memory (if required)
      if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
        arena->resize(dest->size);
        dest->data = arena->get_ptr();
      }

      memcpy(dest->data, &m_dummy, sizeof(T));
    }

    // Returns the threshold when switching from binary search to
    // linear search. We *always* want to use linear search, because the
    // SparseMap is not a good fit for random access.
    size_t get_linear_search_threshold() const {
      return (std::numeric_limits<int>::max());
    }

    // Performs a linear search in a given range between |start| and
    // |start + length|. Returns the "slot" of the key.
    template<typename Cmp>
    int linear_search(ham_u32_t start, ham_u32_t count, ham_key_t *hkey,
                    Cmp &comparator, int *pcmp) {
      ham_assert(sizeof(T) == hkey->size);
      // make sure that we always start searching from the beginning
      ham_assert(start == 0);

      T t = *(T *)hkey->data;

      // now check the bit that corresponds to |t|
      *pcmp = m_sparsemap.is_set(t) ? 0 : -1;
      return (m_sparsemap.calc_popcount(t));
      // TODO combine those two calls!
      //return (m_sparsemap.is_set(t)
                   //? m_sparsemap.calc_popcount(t)
                   //: -1);
    }

    // Iterates all keys, calls the |visitor| on each
    void scan(ScanVisitor *visitor, size_t node_count, ham_u32_t start) {
      m_sparsemap.scan(*visitor, start);
    }

    // Erases a key's payload. Does NOT remove the chunk from the UpfrontIndex
    // (see |erase_slot()|).
    void erase_data(ham_u32_t slot) {
      // nothing to do here
    }

    // Erases a key, including extended blobs
    void erase_slot(size_t node_count, ham_u32_t slot) {
      m_sparsemap.set(m_sparsemap.select(slot), false);
    }

    // Inserts the |key| at the position identified by |slot|.
    // This method cannot fail; there MUST be sufficient free space in the
    // node (otherwise the caller would have split the node).
    void insert(size_t node_count, ham_u32_t slot, const ham_key_t *hkey) {
      ham_assert(sizeof(T) == hkey->size);

      T t = *(T *)hkey->data;
      // now set the bit that corresponds to |t|
      m_sparsemap.set(t, true);
    }

    // Can return a modified pivot key. Since the SparseMap aligns its
    // bitvectors, the pivot position has to be aligned as well
    //
    // Note that the pivot is a (1-based) slot index, whereas the bitmap
    // position is 0-based; therefore the substraction of 1
    int adjust_split_pivot(int pivot) {
      const size_t capacity = MiniMap<BitVector>::kBitsPerVector;
      return ((pivot / capacity) * capacity);
    }

    // Returns true if the |key| no longer fits into the node and a split
    // is required. Makes sure that there is ALWAYS enough headroom
    // for an extended key!
    //
    // TODO this can be improved; currently, we always end up
    // losing 8+sizeof(T) bytes
    bool requires_split(size_t node_count, const ham_key_t *key,
                    bool vacuumize = false) {
      size_t required = sizeof(T) + sizeof(BitVector) * 2;
      size_t full_range_size_bytes = m_sparsemap.get_range_size();
      return (sizeof(ham_u32_t) + m_sparsemap.get_size() + required
                      > full_range_size_bytes);
    }

    // Copies |count| key from this[sstart] to dest[dstart]
    void copy_to(ham_u32_t sstart, size_t node_count,
                    BitmapKeyList &dest, size_t other_node_count,
                    ham_u32_t dstart) {
      ham_assert(sstart % MiniMap<BitVector>::kBitsPerVector == 0);
      ham_assert(dstart == other_node_count);

      m_sparsemap.split(sstart, &dest.m_sparsemap);
    }

    // Checks the integrity of this node. Throws an exception if there is a
    // violation.
    void check_integrity(size_t node_count, bool quick = false) const {
      // nothing to do
    }

    // Rearranges the list
    void vacuumize(size_t node_count, bool force) {
      // nothing to do
    }

    // Change the capacity; the capacity will be reduced, growing is not
    // implemented. Which means that the data area must be copied; the offsets
    // do not have to be changed.
    void change_capacity(size_t node_count, size_t old_capacity,
            size_t new_capacity, ham_u8_t *new_data_ptr,
            size_t new_range_size) {
      ham_assert(get_range_size() <= new_range_size);
      memmove(new_data_ptr, m_data, get_range_size());
      m_data = new_data_ptr;
      *(ham_u32_t *)m_data = new_range_size;

      m_sparsemap.set_data_size(new_range_size);
    }

    // Prints a slot to |out| (for debugging)
    void print(ham_u32_t slot, std::stringstream &out) {
      T t = m_sparsemap.select(slot);
      out << t;
    }

    // Has support for SIMD style search?
    bool has_simd_support() const {
      return (false);
    }

    // Returns the pointer to the key's inline data - for SIMD calculations
    // Not implemented by this KeyList
    ham_u8_t *get_simd_data() {
      return (0);
    }

  private:
    // The database
    LocalDatabase *m_db;

    // The compressed bitmap
    SparseMap<T, BitVector> m_sparsemap;

    // The serialized data
    ham_u8_t *m_data;

    // A helper to return pointers to T; otherwise stale pointers will
    // go out of scope
    T m_dummy;
};

} // namespace ProLayout

} // namespace hamsterdb

#endif /* HAM_BTREE_KEYS_BITMAP_H__ */
