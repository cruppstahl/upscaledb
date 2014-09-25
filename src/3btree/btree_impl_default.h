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
 * Btree node layout for variable length keys/records and/or duplicates
 * ====================================================================
 *
 * This is the default hamsterdb layout. It is chosen for
 * 1. variable length keys (with or without duplicates)
 * 2. fixed length keys with duplicates
 *
 * Like the PAX layout implemented in btree_impl_pax.h, the layout implemented
 * here stores key data and records separated from each other. This layout is
 * more complex, because it is capable of resizing the KeyList and RecordList
 * if the node becomes full.
 *
 * The flat memory layout looks like this:
 *
 * |Idx1|Idx2|...|Idxn|F1|F2|...|Fn|...(space)...|Key1|Key2|...|Keyn|
 *
 * ... where Idx<n> are the indices (of slot <n>)
 *     where F<n> are freelist entries
 *     where Key<n> is the key data of slot <n>.
 *
 * In addition, the first few bytes in the node store the following
 * information:
 *   0  (4 bytes): total capacity of index keys (used keys + freelist)
 *   4  (4 bytes): number of used freelist entries
 *   8  (4 bytes): offset for the next key at the end of the page
 *
 * In total, |capacity| contains the number of maximum keys (and index
 * entries) that can be stored in the node. The number of used index keys
 * is in |m_node->get_count()|. The number of used freelist entries is
 * returned by |get_freelist_count()|. The freelist indices start directly
 * after the key indices. The key space (with key data and records) starts at
 * N * capacity, where |N| is the size of an index entry (the size depends
 * on the actual btree configuration, i.e. whether key size is fixed,
 * duplicates are used etc).
 *
 * If records have fixed length then all records of a key (with duplicates)
 * are stored next to each other. If they have variable length then each of
 * these records is stored with 1 byte for flags:
 *   Rec1|F1|Rec2|F2|...
 * where Recn is an 8 bytes record-ID (offset in the file) OR inline record,
 * and F1 is 1 byte for flags (kBlobSizeSmall etc).
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_BTREE_IMPL_DEFAULT_H
#define HAM_BTREE_IMPL_DEFAULT_H

#include "0root/root.h"

#include <algorithm>
#include <iostream>
#include <vector>
#include <map>

// Always verify that a file of level N does not include headers > N!
#include "1globals/globals.h"
#include "1base/byte_array.h"
#include "2page/page.h"
#include "3blob_manager/blob_manager.h"
#include "3btree/btree_index.h"
#include "3btree/btree_impl_base.h"
#include "3btree/btree_node.h"
#include "3btree/btree_visitor.h"
#include "4env/env_local.h"
#include "4db/db_local.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

//
// A BtreeNodeProxy layout which can handle...
//
//   1. fixed length keys w/ duplicates
//   2. variable length keys w/ duplicates
//   3. variable length keys w/o duplicates
//
// Fixed length keys are stored sequentially and reuse the layout from pax.
// Same for the distinct RecordList (if duplicates are disabled).
//
template<typename KeyList, typename RecordList>
class DefaultNodeImpl : public BaseNodeImpl<KeyList, RecordList>
{
    // C++ does not allow access to members of base classes unless they're
    // explicitly named; this typedef helps to make the code "less" ugly,
    // but it still sucks that i have to use it
    //
    // http://stackoverflow.com/questions/1120833/derived-template-class-access-to-base-class-member-data
    typedef BaseNodeImpl<KeyList, RecordList> P;

    // the type of |this| object
    typedef DefaultNodeImpl<KeyList, RecordList> NodeType;

    enum {
      // for capacity
      kPayloadOffset = 4
    };

  public:
    // Constructor
    DefaultNodeImpl(Page *page)
      : BaseNodeImpl<KeyList, RecordList>(page) {
      initialize();
#ifdef HAM_DEBUG
      check_index_integrity(P::m_node->get_count());
#endif
    }

    // Checks the integrity of this node. Throws an exception if there is a
    // violation.
    virtual void check_integrity() const {
      size_t node_count = P::m_node->get_count();
      if (node_count == 0)
        return;

      check_index_integrity(node_count);
    }

    // Compares two keys
    template<typename Cmp>
    int compare(const ham_key_t *lhs, ham_u32_t rhs, Cmp &cmp) {
      ham_key_t tmp = {0};
      P::m_keys.get_key(rhs, &m_arena, &tmp, false);
      return (cmp(lhs->data, lhs->size, tmp.data, tmp.size));
    }

    // Searches the node for the key and returns the slot of this key
    template<typename Cmp>
    int find_child(ham_key_t *key, Cmp &comparator, ham_u64_t *precord_id,
                    int *pcmp) {
      int slot = find_impl(key, comparator, pcmp);
      if (precord_id) {
        if (slot == -1)
          *precord_id = P::m_node->get_ptr_down();
        else
          *precord_id = P::m_records.get_record_id(slot);
      }
      return (slot);
    }

    // Searches the node for the key and returns the slot of this key
    // - only for exact matches!
    template<typename Cmp>
    int find_exact(ham_key_t *key, Cmp &comparator) {
      int cmp;
      int r = find_impl(key, comparator, &cmp);
      return (cmp ? -1 : r);
    }

    // Iterates all keys, calls the |visitor| on each
    void scan(ScanVisitor *visitor, ham_u32_t start, bool distinct) {
#ifdef HAM_DEBUG
      check_index_integrity(P::m_node->get_count());
#endif

      // a distinct scan over fixed-length keys can be moved to the KeyList
      if (KeyList::kSupportsBlockScans && distinct) {
        P::m_keys.scan(visitor, start, P::m_node->get_count() - start);
        return;
      }

      // otherwise iterate over the keys, call visitor for each key
      size_t node_count = P::m_node->get_count() - start;
      ham_key_t key = {0};

      for (size_t i = start; i < node_count; i++) {
        P::m_keys.get_key(i, &m_arena, &key, false);
        (*visitor)(key.data, key.size, distinct ? 1 : P::get_record_count(i));
      }
    }

    // Returns the full record and stores it in |dest|
    void get_record(int slot, ByteArray *arena, ham_record_t *record,
                    ham_u32_t flags, ham_u32_t duplicate_index) {
#ifdef HAM_DEBUG
      check_index_integrity(P::m_node->get_count());
#endif
      P::get_record(slot, arena, record, flags, duplicate_index);
    }

    // Updates the record of a key
    void set_record(int slot, ham_record_t *record,
                    ham_u32_t duplicate_index, ham_u32_t flags,
                    ham_u32_t *new_duplicate_index) {
      P::set_record(slot, record, duplicate_index, flags, new_duplicate_index);
#ifdef HAM_DEBUG
      check_index_integrity(P::m_node->get_count());
#endif
    }

    // Erases the record
    void erase_record(int slot, int duplicate_index,
                    bool all_duplicates) {
      P::erase_record(slot, duplicate_index, all_duplicates);
#ifdef HAM_DEBUG
      check_index_integrity(P::m_node->get_count());
#endif
    }

    // Erases a key
    void erase(int slot) {
      P::erase(slot);
#ifdef HAM_DEBUG
      check_index_integrity(P::m_node->get_count() - 1);
#endif
    }

    // Inserts a new key
    void insert(int slot, const ham_key_t *key) {
      P::insert(slot, key);
#ifdef HAM_DEBUG
      check_index_integrity(P::m_node->get_count() + 1);
#endif
    }

    // Returns true if |key| cannot be inserted because a split is required.
    // This function will try to re-arrange the node in order for the new
    // key to fit in.
    bool requires_split(const ham_key_t *key) {
      size_t node_count = P::m_node->get_count();

      if (node_count == 0) {
        P::m_records.vacuumize(node_count, true);
        P::m_keys.vacuumize(node_count, true);
        return (false);
      }

      bool keys_require_split = P::m_keys.requires_split(node_count, key);
      bool records_require_split = P::m_records.requires_split(node_count);
      if (!keys_require_split && !records_require_split)
        return (false);

      // try to resize the lists before admitting defeat and splitting
      // the page
      if (keys_require_split || records_require_split) {
        // "vacuumize" both lists
        // TODO this requires cleanups. The last parameter of requires_split()
        // should no longer be necessary
        P::m_records.vacuumize(node_count, false);
        P::m_keys.vacuumize(node_count, false);
        if (records_require_split)
          records_require_split = P::m_records.requires_split(node_count, true);
        if (keys_require_split)
          keys_require_split = P::m_keys.requires_split(node_count, key, true);

        if (adjust_capacity(key, keys_require_split, records_require_split)) {
#ifdef HAM_DEBUG
          check_index_integrity(node_count);
#endif
          return (false);
        }

#ifdef HAM_DEBUG
        check_index_integrity(node_count);
#endif

        // still here? then there's no way to avoid the split
        BtreeIndex *bi = P::m_page->get_db()->get_btree_index();
        bi->get_statistics()->set_keylist_range_size(P::m_node->is_leaf(),
                        load_range_size());
        return (true);
      }

      return (false);
    }

    // Splits this node and moves some/half of the keys to |other|
    void split(DefaultNodeImpl *other, int pivot) {
      size_t node_count = P::m_node->get_count();

#ifdef HAM_DEBUG
      check_index_integrity(node_count);
      ham_assert(other->m_node->get_count() == 0);
#endif

      // make sure that the other node has enough free space
      other->initialize(this);

      P::split(other, pivot);

      P::m_keys.vacuumize(pivot, true);
      P::m_records.vacuumize(pivot, true);

#ifdef HAM_DEBUG
      check_index_integrity(pivot);
      if (P::m_node->is_leaf())
        other->check_index_integrity(node_count - pivot);
      else
        other->check_index_integrity(node_count - pivot - 1);
#endif
    }

    // Merges keys from |other| to this node
    void merge_from(DefaultNodeImpl *other) {
      size_t node_count = P::m_node->get_count();

      P::m_keys.vacuumize(node_count, true);
      P::m_records.vacuumize(node_count, true);

      P::merge_from(other);

#ifdef HAM_DEBUG
      check_index_integrity(node_count + other->m_node->get_count());
#endif
    }

    // Prepares the page for a flush to disk; compressed KeyLists can use
    // this to serialize their data
    void prepare_flush() {
      P::m_keys.prepare_flush(P::m_node->get_count());
    }

  private:
    // Initializes the node
    void initialize(NodeType *other = 0) {
      LocalDatabase *db = P::m_page->get_db();
      size_t usable_page_size = get_usable_page_size();

      // initialize this page in the same way as |other| was initialized
      if (other) {
        size_t key_range_size = other->load_range_size();

        // persist the range size
        store_range_size(key_range_size);
        ham_u8_t *p = P::m_node->get_data();
        p += sizeof(ham_u32_t);

        // create the KeyList and RecordList
        P::m_keys.create(p, key_range_size);
        P::m_records.create(p + key_range_size,
                        usable_page_size - key_range_size);
      }
      // initialize a new page from scratch
      else if ((P::m_node->get_count() == 0
                && !(db->get_rt_flags() & HAM_READ_ONLY))) {
        size_t key_range_size;
        size_t record_range_size;

        // if yes then ask the btree for the default range size (it keeps
        // track of the average range size of older pages).
        BtreeStatistics *bstats = db->get_btree_index()->get_statistics();
        key_range_size = bstats->get_keylist_range_size(P::m_node->is_leaf());

        // no data so far? then come up with a good default
        if (key_range_size == 0) {
          // calculate the sizes of the KeyList and RecordList
          double ratio = (double)P::m_keys.get_full_key_size() /
                              (double)P::m_records.get_full_record_size();
          if (ratio == 1.)
            key_range_size = usable_page_size / 2;
          else if (ratio > 1) {
            ratio = (double)P::m_records.get_full_record_size() /
                        (double)P::m_keys.get_full_key_size();
            record_range_size = (double)usable_page_size * ratio;
            key_range_size = usable_page_size - record_range_size;
          }
          else // ratio < 1
            key_range_size = (double)usable_page_size * ratio;
        }

        record_range_size = usable_page_size - key_range_size;

        // persist the key range size
        store_range_size(key_range_size);
        ham_u8_t *p = P::m_node->get_data();
        p += sizeof(ham_u32_t);

        // and create the lists
        P::m_keys.create(p, key_range_size);
        P::m_records.create(p + key_range_size, record_range_size);

        P::m_estimated_capacity = key_range_size
                / (size_t)P::m_keys.get_full_key_size();
      }
      // open a page; read initialization parameters from persisted storage
      else {
        size_t key_range_size = load_range_size();
        size_t record_range_size = usable_page_size - key_range_size;
        ham_u8_t *p = P::m_node->get_data();
        p += sizeof(ham_u32_t);

        P::m_keys.open(p, key_range_size, P::m_node->get_count());
        P::m_records.open(p + key_range_size, record_range_size,
                        P::m_node->get_count());

        P::m_estimated_capacity = key_range_size
                / (size_t)P::m_keys.get_full_key_size();
      }
    }

    // Adjusts the size of both lists; either increases it or decreases
    // it (in order to free up space for variable length data).
    // Returns true if |key| and an additional record can be inserted, or
    // false if not; in this case the caller must perform a split.
    bool adjust_capacity(const ham_key_t *key, bool keys_require_split,
                    bool records_require_split) {
      size_t node_count = P::m_node->get_count();

      // One of the lists must be resizable (otherwise they would be managed
      // by the PaxLayout)
      ham_assert(!KeyList::kHasSequentialData
              || !RecordList::kHasSequentialData);

      // Retrieve the minimum sizes that both lists require to store their
      // data
      size_t old_key_range_size = load_range_size();
      size_t key_range_size, record_range_size;
      size_t required_key_range, required_record_range;
      required_key_range = P::m_keys.get_required_range_size(node_count)
                                + P::m_keys.get_full_key_size(key);
      required_record_range = P::m_records.get_required_range_size(node_count)
                                + P::m_records.get_full_record_size();

      size_t usable_page_size = get_usable_page_size();
      size_t remainder = usable_page_size
                            - (required_key_range + required_record_range); 

      // Now split the remainder between both lists, according to the
      // ratio of a single item
      double ratio = (double)P::m_keys.get_full_key_size() /
                          (double)P::m_records.get_full_record_size();
      if (ratio == 1.)
        key_range_size = required_key_range + remainder / 2;
      else if (ratio > 1) {
        ratio = (double)P::m_records.get_full_record_size() /
                    (double)P::m_keys.get_full_key_size();
        key_range_size = required_key_range + (double)remainder * ratio;
      }
      else // ratio < 1
        key_range_size = required_key_range + (double)remainder * ratio;
      record_range_size = usable_page_size - key_range_size;

      // Check if the required record space is large enough, and make sure
      // there is enough room for a new item
      if (key_range_size > usable_page_size
          || record_range_size > usable_page_size
          || key_range_size == old_key_range_size
          || key_range_size < required_key_range
          || record_range_size < required_record_range
          || key_range_size + record_range_size > usable_page_size)
        return (false);

      // Try to get a clue about the capacity of the lists; this will help
      // those lists with an UpfrontIndex to better arrange their layout
      size_t capacity_hint = 0;
      if (KeyList::kHasSequentialData)
        capacity_hint = key_range_size / P::m_keys.get_full_key_size();
      else if (RecordList::kHasSequentialData)
        capacity_hint = record_range_size / P::m_records.get_full_record_size();

      // sanity check: make sure that the new capacity would be big
      // enough for all the keys
      if (capacity_hint > 0 && capacity_hint < node_count)
        return (false);

      // Get a pointer to the data area and persist the new range size
      // of the KeyList
      store_range_size(key_range_size);
      ham_u8_t *p = P::m_node->get_data();
      p += sizeof(ham_u32_t);

      // Now update the lists. If the KeyList grows then start with resizing
      // the RecordList, otherwise the moved KeyList will overwrite the
      // beginning of the RecordList.
      if (key_range_size > old_key_range_size) {
        P::m_records.change_range_size(node_count, p + key_range_size,
                        usable_page_size - key_range_size,
                        capacity_hint);
        P::m_keys.change_range_size(node_count, p, key_range_size,
                        capacity_hint);
      }
      // And vice versa if the RecordList grows
      else {
        P::m_keys.change_range_size(node_count, p, key_range_size,
                        capacity_hint);
        P::m_records.change_range_size(node_count, p + key_range_size,
                        usable_page_size - key_range_size,
                        capacity_hint);
      }
      
      // make sure that the page is flushed to disk
      P::m_page->set_dirty(true);

      // finally check if the new space is sufficient for the new key
      // TODO this shouldn't be required if the check above is implemented
      // -> change to an assert, then return true
      return (!P::m_records.requires_split(node_count)
                && !P::m_keys.requires_split(node_count, key));
    }

    // Implementation of the find method; uses a linear search if possible
    template<typename Cmp>
    int find_impl(ham_key_t *key, Cmp &comparator, int *pcmp) {
      size_t node_count = P::m_node->get_count();
      ham_assert(node_count > 0);

#ifdef HAM_DEBUG
      check_index_integrity(node_count);
#endif

      int i, l = 0, r = (int)node_count;
      int last = node_count + 1;
      int cmp = -1;

      // Run a binary search, but fall back to linear search as soon as
      // the remaining range is too small. Sets threshold to 0 if linear
      // search is disabled for this KeyList.
      int threshold = P::m_keys.get_linear_search_threshold();

      /* repeat till we found the key or the remaining range is so small that
       * we rather perform a linear search (which is faster for small ranges) */
      while (r - l > threshold) {
        /* get the median item; if it's identical with the "last" item,
         * we've found the slot */
        i = (l + r) / 2;

        if (i == last) {
          ham_assert(i >= 0);
          ham_assert(i < (int)node_count);
          *pcmp = 1;
          return (i);
        }

        /* compare it against the key */
        cmp = compare(key, i, comparator);

        /* found it? */
        if (cmp == 0) {
          *pcmp = cmp;
          return (i);
        }
        /* if the key is bigger than the item: search "to the left" */
        else if (cmp < 0) {
          if (r == 0) {
            ham_assert(i == 0);
            *pcmp = cmp;
            return (-1);
          }
          r = i;
        }
        /* otherwise search "to the right" */
        else {
          last = i;
          l = i;
        }
      }

      // still here? then perform a linear search for the remaining range
      ham_assert(r - l <= threshold);
      return (P::m_keys.linear_search(l, r - l, key, comparator, pcmp));
    }

    // Checks the integrity of the key- and record-ranges. Throws an exception
    // if there's a problem.
    void check_index_integrity(size_t node_count) const {
      P::m_keys.check_integrity(node_count, true);
      P::m_records.check_integrity(node_count, true);
    }

    // Returns the usable page size that can be used for actually
    // storing the data
    size_t get_usable_page_size() const {
      return (P::m_page->get_db()->get_local_env()->get_usable_page_size()
                    - kPayloadOffset
                    - PBtreeNode::get_entry_offset()
                    - sizeof(ham_u32_t));
    }

    // Persists the KeyList's range size
    void store_range_size(size_t key_range_size) {
      ham_u8_t *p = P::m_node->get_data();
      *(ham_u32_t *)p = (ham_u32_t)key_range_size;
    }

    // Load the stored KeyList's range size
    size_t load_range_size() const {
      ham_u8_t *p = P::m_node->get_data();
      return (*(ham_u32_t *)p);
    }

    // A memory arena for various tasks
    ByteArray m_arena;
};

} // namespace hamsterdb

#endif /* HAM_BTREE_IMPL_DEFAULT_H */
