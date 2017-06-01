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
 * Btree node layout for variable length keys/records and/or duplicates
 * ====================================================================
 *
 * This is the default upscaledb layout. It is chosen for
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
 * is in |node->length()|. The number of used freelist entries is
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
 */

#ifndef UPS_BTREE_IMPL_DEFAULT_H
#define UPS_BTREE_IMPL_DEFAULT_H

#include "0root/root.h"

#include <algorithm>
#include <iostream>
#include <vector>
#include <map>

// Always verify that a file of level N does not include headers > N!
#include "1globals/globals.h"
#include "1base/dynamic_array.h"
#include "2page/page.h"
#ifdef __SSE__
#  include "2simd/simd.h"
#endif
#include "3blob_manager/blob_manager.h"
#include "3btree/btree_index.h"
#include "3btree/btree_impl_base.h"
#include "3btree/btree_node.h"
#include "3btree/btree_visitor.h"
#include "4env/env_local.h"
#include "4db/db_local.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

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
struct DefaultNodeImpl : BaseNodeImpl<KeyList, RecordList> {
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

  // Constructor
  DefaultNodeImpl(Page *page)
    : BaseNodeImpl<KeyList, RecordList>(page) {
    initialize();
  }

  // Checks the integrity of this node. Throws an exception if there is a
  // violation.
  virtual void check_integrity(Context *context) const {
    size_t node_count = P::node->length();
    if (node_count == 0)
      return;

    check_index_integrity(context, node_count);
  }

  // Returns the full record and stores it in |dest|
  void record(Context *context, int slot, ByteArray *arena,
                  ups_record_t *record, uint32_t flags, int duplicate_index) {
    assert(check_index_integrity(context, P::node->length()));
    P::record(context, slot, arena, record, flags, duplicate_index);
  }

  // Updates the record of a key
  void set_record(Context *context, int slot, ups_record_t *record,
                  int duplicate_index, uint32_t flags,
                  uint32_t *new_duplicate_index) {
    P::set_record(context, slot, record, duplicate_index,
                    flags, new_duplicate_index);
    assert(check_index_integrity(context, P::node->length()));
  }

  // Erases the record
  void erase_record(Context *context, int slot, int duplicate_index,
                  bool all_duplicates) {
    P::erase_record(context, slot, duplicate_index, all_duplicates);
    assert(check_index_integrity(context, P::node->length()));
  }

  // Erases a key
  void erase(Context *context, int slot) {
    P::erase(context, slot);
    assert(check_index_integrity(context, P::node->length() - 1));
  }

  // Returns true if |key| cannot be inserted because a split is required.
  // This function will try to re-arrange the node in order for the new
  // key to fit in.
  bool requires_split(Context *context, const ups_key_t *key) {
    size_t node_count = P::node->length();

    // the node is empty? that's either because nothing was inserted yet,
    // or because all keys were erased. For the latter case make sure
    // that no garbage remains behind, otherwise it's possible that
    // following inserts can fail
    if (node_count == 0) {
      P::records.vacuumize(node_count, true);
      P::keys.vacuumize(node_count, true);
      return false;
    }

    bool keys_require_split = P::keys.requires_split(node_count, key);
    bool records_require_split = P::records.requires_split(node_count);
    if (!keys_require_split && !records_require_split)
      return false;

    // first try to vaccumize the lists without rearranging them
    if (keys_require_split) {
      P::keys.vacuumize(node_count, false);
      keys_require_split = P::keys.requires_split(node_count, key);
    }
   
    if (records_require_split) {
      P::records.vacuumize(node_count, false);
      records_require_split = P::records.requires_split(node_count);
    }

    if (!keys_require_split && !records_require_split)
      return false;

    // now adjust the ranges and the capacity
    if (reorganize(context, key)) {
      assert(check_index_integrity(context, node_count));
      return false;
    }

    assert(check_index_integrity(context, node_count));

    // still here? then there's no way to avoid the split
    BtreeIndex *bi = P::page->db()->btree_index.get();
    bi->statistics()->set_keylist_range_size(P::node->is_leaf(),
                    load_range_size());
    bi->statistics()->set_keylist_capacities(P::node->is_leaf(),
                    node_count);
    return true;
  }

  // Splits this node and moves some/half of the keys to |other|
  void split(Context *context, DefaultNodeImpl *other, int pivot) {
    size_t node_count = P::node->length();

    assert(check_index_integrity(context, node_count));
    assert(other->node->length() == 0);

    // make sure that the other node has enough free space
    other->initialize(this);

    P::split(context, other, pivot);

    P::keys.vacuumize(pivot, true);
    P::records.vacuumize(pivot, true);

    assert(check_index_integrity(context, pivot));
    if (P::node->is_leaf())
      assert(other->check_index_integrity(context, node_count - pivot));
    else
      assert(other->check_index_integrity(context, node_count - pivot - 1));
  }

  // Merges keys from |other| to this node
  void merge_from(Context *context, DefaultNodeImpl *other) {
    size_t node_count = P::node->length();

    P::keys.vacuumize(node_count, true);
    P::records.vacuumize(node_count, true);

    P::merge_from(context, other);

    assert(check_index_integrity(context, node_count + other->node->length()));
  }

  // Adjusts the size of both lists; either increases it or decreases
  // it (in order to free up space for variable length data).
  // Returns true if |key| and an additional record can be inserted, or
  // false if not; in this case the caller must perform a split.
  bool reorganize(Context *context, const ups_key_t *key) {
    size_t node_count = P::node->length();

    // One of the lists must be resizable (otherwise they would be managed
    // by the PaxLayout)
    assert(!KeyList::kHasSequentialData || !RecordList::kHasSequentialData);

    // Retrieve the minimum sizes that both lists require to store their
    // data
    size_t capacity_hint;
    size_t old_key_range_size = load_range_size();
    size_t key_range_size, record_range_size;
    size_t required_key_range, required_record_range;
    size_t usable_size = usable_range_size();
    required_key_range = P::keys.required_range_size(node_count)
                              + P::keys.full_key_size(key);
    required_record_range = P::records.required_range_size(node_count)
                              + P::records.full_record_size();

    uint8_t *p = P::node->data();
    p += sizeof(uint32_t);

    // no records? then there's no way to change the ranges. but maybe we
    // can increase the capacity
    if (required_record_range == 0) {
      if (required_key_range > usable_size)
        return false;
      P::keys.change_range_size(node_count, p, usable_size,
                      node_count + 5);
      return !P::keys.requires_split(node_count, key);
    }

    int remainder = usable_size - (required_key_range + required_record_range); 
    if (remainder < 0)
      return false;

    // Now split the remainder between both lists
    size_t additional_capacity = remainder
            / (P::keys.full_key_size(0) +
                            P::records.full_record_size());
    if (additional_capacity == 0)
      return false;

    key_range_size = required_key_range + additional_capacity
            * P::keys.full_key_size(0);
    record_range_size = usable_size - key_range_size;

    assert(key_range_size + record_range_size <= usable_size);

    // Check if the required record space is large enough, and make sure
    // there is enough room for a new item
    if (key_range_size > usable_size
        || record_range_size > usable_size
        || key_range_size == old_key_range_size
        || key_range_size < required_key_range
        || record_range_size < required_record_range
        || key_range_size + record_range_size > usable_size)
      return false;

    capacity_hint = get_capacity_hint(key_range_size, record_range_size);

    // sanity check: make sure that the new capacity would be big
    // enough for all the keys
    if (capacity_hint > 0 && capacity_hint < node_count)
      return false;

    if (capacity_hint == 0) {
      BtreeStatistics *bstats = P::page->db()->btree_index->statistics();
      capacity_hint = bstats->keylist_capacities(P::node->is_leaf());
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
      P::records.change_range_size(node_count, p + key_range_size,
                      usable_size - key_range_size,
                      capacity_hint);
      P::keys.change_range_size(node_count, p, key_range_size,
                      capacity_hint);
    }
    // And vice versa if the RecordList grows
    else {
      P::keys.change_range_size(node_count, p, key_range_size,
                      capacity_hint);
      P::records.change_range_size(node_count, p + key_range_size,
                      usable_size - key_range_size,
                      capacity_hint);
    }
    
    // make sure that the page is flushed to disk
    P::page->set_dirty(true);

    assert(check_index_integrity(context, node_count));

    // finally check if the new space is sufficient for the new key
    // TODO this shouldn't be required if the check above is implemented
    // -> change to an assert, then return true
    return !P::records.requires_split(node_count)
              && !P::keys.requires_split(node_count, key);
  }

  // Initializes the node
  void initialize(NodeType *other = 0) {
    LocalDb *db = P::page->db();
    size_t usable_size = usable_range_size();

    // initialize this page in the same way as |other| was initialized
    if (other) {
      size_t key_range_size = other->load_range_size();

      // persist the range size
      store_range_size(key_range_size);
      uint8_t *p = P::node->data();
      p += sizeof(uint32_t);

      // create the KeyList and RecordList
      P::keys.create(p, key_range_size);
      P::records.create(p + key_range_size, usable_size - key_range_size);
    }
    // initialize a new page from scratch
    else if (P::node->length() == 0 && NOTSET(db->flags(), UPS_READ_ONLY)) {
      size_t key_range_size;
      size_t record_range_size;

      // if yes then ask the btree for the default range size (it keeps
      // track of the average range size of older pages).
      BtreeStatistics *bstats = db->btree_index->statistics();
      key_range_size = bstats->keylist_range_size(P::node->is_leaf());

      // no data so far? then come up with a good default
      if (key_range_size == 0) {
        // no records? then assign the full range to the KeyList
        if (P::records.full_record_size() == 0) {
          key_range_size = usable_size;
        }
        // Otherwise split the range between both lists
        else {
          size_t capacity = usable_size
                  / (P::keys.full_key_size(0) +
                                P::records.full_record_size());
          key_range_size = capacity * P::keys.full_key_size(0);
        }
      }

      record_range_size = usable_size - key_range_size;

      assert(key_range_size + record_range_size <= usable_size);

      // persist the key range size
      store_range_size(key_range_size);
      uint8_t *p = P::node->data();
      p += sizeof(uint32_t);

      // and create the lists
      P::keys.create(p, key_range_size);
      P::records.create(p + key_range_size, record_range_size);

      P::estimated_capacity = key_range_size
              / (size_t)P::keys.full_key_size();
    }
    // open a page; read initialization parameters from persisted storage
    else {
      size_t key_range_size = load_range_size();
      size_t record_range_size = usable_size - key_range_size;
      uint8_t *p = P::node->data();
      p += sizeof(uint32_t);

      P::keys.open(p, key_range_size, P::node->length());
      P::records.open(p + key_range_size, record_range_size,
                      P::node->length());

      P::estimated_capacity = key_range_size
              / (size_t)P::keys.full_key_size();
    }
  }

  // Try to get a clue about the capacity of the lists; this will help
  // those lists with an UpfrontIndex to better arrange their layout
  size_t get_capacity_hint(size_t key_range_size, size_t record_range_size) {
    if (KeyList::kHasSequentialData)
      return key_range_size / P::keys.full_key_size();
    if (RecordList::kHasSequentialData && P::records.full_record_size())
      return record_range_size / P::records.full_record_size();
    return 0;
  }

  // Checks the integrity of the key- and record-ranges. Throws an exception
  // if there's a problem.
  bool check_index_integrity(Context *context, size_t node_count) const {
    P::keys.check_integrity(context, node_count);
    P::records.check_integrity(context, node_count);
    return true;
  }

  // Returns the usable page size that can be used for actually
  // storing the data
  size_t usable_range_size() const {
    return P::page->usable_page_size()
                  - kPayloadOffset
                  - PBtreeNode::entry_offset()
                  - sizeof(uint32_t);
  }

  // Persists the KeyList's range size
  void store_range_size(size_t key_range_size) {
    uint8_t *p = P::node->data();
    *(uint32_t *)p = (uint32_t)key_range_size;
  }

  // Load the stored KeyList's range size
  size_t load_range_size() const {
    uint8_t *p = P::node->data();
    return *(uint32_t *)p;
  }
};

} // namespace upscaledb

#endif // UPS_BTREE_IMPL_DEFAULT_H
