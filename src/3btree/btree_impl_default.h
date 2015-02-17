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
#include "1base/dynamic_array.h"
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
    }

    // Checks the integrity of this node. Throws an exception if there is a
    // violation.
    virtual void check_integrity(Context *context) const {
      size_t node_count = P::m_node->get_count();
      if (node_count == 0)
        return;

      check_index_integrity(context, node_count);
    }

    // Iterates all keys, calls the |visitor| on each
    void scan(Context *context, ScanVisitor *visitor, uint32_t start,
                    bool distinct) {
#ifdef HAM_DEBUG
      check_index_integrity(context, P::m_node->get_count());
#endif

      // a distinct scan over fixed-length keys can be moved to the KeyList
      if (KeyList::kSupportsBlockScans && distinct) {
        P::m_keys.scan(context, visitor, start, P::m_node->get_count() - start);
        return;
      }

      // otherwise iterate over the keys, call visitor for each key
      ham_key_t key = {0};
      ByteArray arena;
      size_t node_count = P::m_node->get_count() - start;

      for (size_t i = start; i < node_count; i++) {
        P::m_keys.get_key(context, i, &arena, &key, false);
        (*visitor)(key.data, key.size, distinct
                                          ? 1
                                          : P::get_record_count(context, i));
      }
    }

    // Returns the full record and stores it in |dest|
    void get_record(Context *context, int slot, ByteArray *arena,
                    ham_record_t *record, uint32_t flags, int duplicate_index) {
#ifdef HAM_DEBUG
      check_index_integrity(context, P::m_node->get_count());
#endif
      P::get_record(context, slot, arena, record, flags, duplicate_index);
    }

    // Updates the record of a key
    void set_record(Context *context, int slot, ham_record_t *record,
                    int duplicate_index, uint32_t flags,
                    uint32_t *new_duplicate_index) {
      P::set_record(context, slot, record, duplicate_index,
                      flags, new_duplicate_index);
#ifdef HAM_DEBUG
      check_index_integrity(context, P::m_node->get_count());
#endif
    }

    // Erases the record
    void erase_record(Context *context, int slot, int duplicate_index,
                    bool all_duplicates) {
      P::erase_record(context, slot, duplicate_index, all_duplicates);
#ifdef HAM_DEBUG
      check_index_integrity(context, P::m_node->get_count());
#endif
    }

    // Erases a key
    void erase(Context *context, int slot) {
      P::erase(context, slot);
#ifdef HAM_DEBUG
      check_index_integrity(context, P::m_node->get_count() - 1);
#endif
    }

    // Returns true if |key| cannot be inserted because a split is required.
    // This function will try to re-arrange the node in order for the new
    // key to fit in.
    bool requires_split(Context *context, const ham_key_t *key) {
      size_t node_count = P::m_node->get_count();

      // the node is empty? that's either because nothing was inserted yet,
      // or because all keys were erased. For the latter case make sure
      // that no garbage remains behind, otherwise it's possible that
      // following inserts can fail
      if (node_count == 0) {
        P::m_records.vacuumize(node_count, true);
        P::m_keys.vacuumize(node_count, true);
        return (false);
      }

      bool keys_require_split = P::m_keys.requires_split(node_count, key);
      bool records_require_split = P::m_records.requires_split(node_count);
      if (!keys_require_split && !records_require_split)
        return (false);

      // first try to vaccumize the lists without rearranging them
      if (keys_require_split) {
        P::m_keys.vacuumize(node_count, false);
        keys_require_split = P::m_keys.requires_split(node_count, key);
      }
     
      if (records_require_split) {
        P::m_records.vacuumize(node_count, false);
        records_require_split = P::m_records.requires_split(node_count);
      }

      if (!keys_require_split && !records_require_split)
        return (false);

      // now adjust the ranges and the capacity
      if (reorganize(context, key)) {
#ifdef HAM_DEBUG
        check_index_integrity(context, node_count);
#endif
        return (false);
      }

#ifdef HAM_DEBUG
      check_index_integrity(context, node_count);
#endif

      // still here? then there's no way to avoid the split
      BtreeIndex *bi = P::m_page->get_db()->btree_index();
      bi->get_statistics()->set_keylist_range_size(P::m_node->is_leaf(),
                      load_range_size());
      bi->get_statistics()->set_keylist_capacities(P::m_node->is_leaf(),
                      node_count);
      return (true);
    }

    // Splits this node and moves some/half of the keys to |other|
    void split(Context *context, DefaultNodeImpl *other, int pivot) {
      size_t node_count = P::m_node->get_count();

#ifdef HAM_DEBUG
      check_index_integrity(context, node_count);
      ham_assert(other->m_node->get_count() == 0);
#endif

      // make sure that the other node has enough free space
      other->initialize(this);

      P::split(context, other, pivot);

      P::m_keys.vacuumize(pivot, true);
      P::m_records.vacuumize(pivot, true);

#ifdef HAM_DEBUG
      check_index_integrity(context, pivot);
      if (P::m_node->is_leaf())
        other->check_index_integrity(context, node_count - pivot);
      else
        other->check_index_integrity(context, node_count - pivot - 1);
#endif
    }

    // Merges keys from |other| to this node
    void merge_from(Context *context, DefaultNodeImpl *other) {
      size_t node_count = P::m_node->get_count();

      P::m_keys.vacuumize(node_count, true);
      P::m_records.vacuumize(node_count, true);

      P::merge_from(context, other);

#ifdef HAM_DEBUG
      check_index_integrity(context, node_count + other->m_node->get_count());
#endif
    }

    // Adjusts the size of both lists; either increases it or decreases
    // it (in order to free up space for variable length data).
    // Returns true if |key| and an additional record can be inserted, or
    // false if not; in this case the caller must perform a split.
    bool reorganize(Context *context, const ham_key_t *key) {
      size_t node_count = P::m_node->get_count();

      // One of the lists must be resizable (otherwise they would be managed
      // by the PaxLayout)
      ham_assert(!KeyList::kHasSequentialData
              || !RecordList::kHasSequentialData);

      // Retrieve the minimum sizes that both lists require to store their
      // data
      size_t capacity_hint;
      size_t old_key_range_size = load_range_size();
      size_t key_range_size, record_range_size;
      size_t required_key_range, required_record_range;
      size_t usable_size = usable_range_size();
      required_key_range = P::m_keys.get_required_range_size(node_count)
                                + P::m_keys.get_full_key_size(key);
      required_record_range = P::m_records.get_required_range_size(node_count)
                                + P::m_records.get_full_record_size();

      uint8_t *p = P::m_node->get_data();
      p += sizeof(uint32_t);

      // no records? then there's no way to change the ranges. but maybe we
      // can increase the capacity
      if (required_record_range == 0) {
        if (required_key_range > usable_size)
          return (false);
        P::m_keys.change_range_size(node_count, p, usable_size,
                        node_count + 5);
        return (!P::m_keys.requires_split(node_count, key));
      }

      int remainder = usable_size
                            - (required_key_range + required_record_range); 
      if (remainder < 0)
        return (false);

      // Now split the remainder between both lists
      size_t additional_capacity = remainder
              / (P::m_keys.get_full_key_size(0) +
                              P::m_records.get_full_record_size());
      if (additional_capacity == 0)
        return (false);

      key_range_size = required_key_range + additional_capacity
              * P::m_keys.get_full_key_size(0);
      record_range_size = usable_size - key_range_size;

      ham_assert(key_range_size + record_range_size <= usable_size);

      // Check if the required record space is large enough, and make sure
      // there is enough room for a new item
      if (key_range_size > usable_size
          || record_range_size > usable_size
          || key_range_size == old_key_range_size
          || key_range_size < required_key_range
          || record_range_size < required_record_range
          || key_range_size + record_range_size > usable_size)
        return (false);

      capacity_hint = get_capacity_hint(key_range_size, record_range_size);

      // sanity check: make sure that the new capacity would be big
      // enough for all the keys
      if (capacity_hint > 0 && capacity_hint < node_count)
        return (false);

      if (capacity_hint == 0) {
        BtreeStatistics *bstats = P::m_page->get_db()->btree_index()->get_statistics();
        capacity_hint = bstats->get_keylist_capacities(P::m_node->is_leaf());
      }

      if (capacity_hint < node_count)
        capacity_hint = node_count + 1;

      // Get a pointer to the data area and persist the new range size
      // of the KeyList
      store_range_size(key_range_size);

      // Now update the lists. If the KeyList grows then start with resizing
      // the RecordList, otherwise the moved KeyList will overwrite the
      // beginning of the RecordList.
      if (key_range_size > old_key_range_size) {
        P::m_records.change_range_size(node_count, p + key_range_size,
                        usable_size - key_range_size,
                        capacity_hint);
        P::m_keys.change_range_size(node_count, p, key_range_size,
                        capacity_hint);
      }
      // And vice versa if the RecordList grows
      else {
        P::m_keys.change_range_size(node_count, p, key_range_size,
                        capacity_hint);
        P::m_records.change_range_size(node_count, p + key_range_size,
                        usable_size - key_range_size,
                        capacity_hint);
      }
      
      // make sure that the page is flushed to disk
      P::m_page->set_dirty(true);

#ifdef HAM_DEBUG
      check_index_integrity(context, node_count);
#endif

      // finally check if the new space is sufficient for the new key
      // TODO this shouldn't be required if the check above is implemented
      // -> change to an assert, then return true
      return (!P::m_records.requires_split(node_count)
                && !P::m_keys.requires_split(node_count, key));
    }

  private:
    // Initializes the node
    void initialize(NodeType *other = 0) {
      LocalDatabase *db = P::m_page->get_db();
      size_t usable_size = usable_range_size();

      // initialize this page in the same way as |other| was initialized
      if (other) {
        size_t key_range_size = other->load_range_size();

        // persist the range size
        store_range_size(key_range_size);
        uint8_t *p = P::m_node->get_data();
        p += sizeof(uint32_t);

        // create the KeyList and RecordList
        P::m_keys.create(p, key_range_size);
        P::m_records.create(p + key_range_size,
                        usable_size - key_range_size);
      }
      // initialize a new page from scratch
      else if ((P::m_node->get_count() == 0
                && !(db->get_flags() & HAM_READ_ONLY))) {
        size_t key_range_size;
        size_t record_range_size;

        // if yes then ask the btree for the default range size (it keeps
        // track of the average range size of older pages).
        BtreeStatistics *bstats = db->btree_index()->get_statistics();
        key_range_size = bstats->get_keylist_range_size(P::m_node->is_leaf());

        // no data so far? then come up with a good default
        if (key_range_size == 0) {
          // no records? then assign the full range to the KeyList
          if (P::m_records.get_full_record_size() == 0) {
            key_range_size = usable_size;
          }
          // Otherwise split the range between both lists
          else {
            size_t capacity = usable_size
                    / (P::m_keys.get_full_key_size(0) +
                                  P::m_records.get_full_record_size());
            key_range_size = capacity * P::m_keys.get_full_key_size(0);
          }
        }

        record_range_size = usable_size - key_range_size;

        ham_assert(key_range_size + record_range_size <= usable_size);

        // persist the key range size
        store_range_size(key_range_size);
        uint8_t *p = P::m_node->get_data();
        p += sizeof(uint32_t);

        // and create the lists
        P::m_keys.create(p, key_range_size);
        P::m_records.create(p + key_range_size, record_range_size);

        P::m_estimated_capacity = key_range_size
                / (size_t)P::m_keys.get_full_key_size();
      }
      // open a page; read initialization parameters from persisted storage
      else {
        size_t key_range_size = load_range_size();
        size_t record_range_size = usable_size - key_range_size;
        uint8_t *p = P::m_node->get_data();
        p += sizeof(uint32_t);

        P::m_keys.open(p, key_range_size, P::m_node->get_count());
        P::m_records.open(p + key_range_size, record_range_size,
                        P::m_node->get_count());

        P::m_estimated_capacity = key_range_size
                / (size_t)P::m_keys.get_full_key_size();
      }
    }

    // Try to get a clue about the capacity of the lists; this will help
    // those lists with an UpfrontIndex to better arrange their layout
    size_t get_capacity_hint(size_t key_range_size, size_t record_range_size) {
      if (KeyList::kHasSequentialData)
        return (key_range_size / P::m_keys.get_full_key_size());
      if (RecordList::kHasSequentialData && P::m_records.get_full_record_size())
        return (record_range_size / P::m_records.get_full_record_size());
      return (0);
    }

    // Checks the integrity of the key- and record-ranges. Throws an exception
    // if there's a problem.
    void check_index_integrity(Context *context, size_t node_count) const {
      P::m_keys.check_integrity(context, node_count);
      P::m_records.check_integrity(context, node_count);
    }

    // Returns the usable page size that can be used for actually
    // storing the data
    size_t usable_range_size() const {
      return (Page::usable_page_size(P::m_page->get_db()->lenv()->config().page_size_bytes)
                    - kPayloadOffset
                    - PBtreeNode::get_entry_offset()
                    - sizeof(uint32_t));
    }

    // Persists the KeyList's range size
    void store_range_size(size_t key_range_size) {
      uint8_t *p = P::m_node->get_data();
      *(uint32_t *)p = (uint32_t)key_range_size;
    }

    // Load the stored KeyList's range size
    size_t load_range_size() const {
      uint8_t *p = P::m_node->get_data();
      return (*(uint32_t *)p);
    }
};

} // namespace hamsterdb

#endif /* HAM_BTREE_IMPL_DEFAULT_H */
