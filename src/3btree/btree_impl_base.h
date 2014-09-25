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
 * Base class for btree node implementations
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_BTREE_IMPL_BASE_H
#define HAM_BTREE_IMPL_BASE_H

#include "0root/root.h"

#include <sstream>
#include <iostream>

// Always verify that a file of level N does not include headers > N!
#include "1globals/globals.h"
#include "1base/byte_array.h"
#include "2page/page.h"
#include "3btree/btree_node.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

template<typename KeyList, typename RecordList>
struct BaseNodeImpl
{
  // Constructor
  BaseNodeImpl(Page *page)
    : m_page(page), m_node(PBtreeNode::from_page(page)),
      m_estimated_capacity(0), m_keys(page->get_db()),
      m_records(page->get_db(), m_node) {
  }

  // Returns the estimated page's capacity
  size_t estimate_capacity() const {
    return (m_estimated_capacity);
  }

  // Checks this node's integrity
  virtual void check_integrity() const {
  }

  // Returns a copy of a key and stores it in |dest|
  void get_key(ham_u32_t slot, ByteArray *arena, ham_key_t *dest) {
    // copy (or assign) the key data
    m_keys.get_key(slot, arena, dest, true);
  }

  // Returns the record size of a key or one of its duplicates
  ham_u64_t get_record_size(ham_u32_t slot, int duplicate_index) {
    return (m_records.get_record_size(slot, duplicate_index));
  }

  // Returns the record counter of a key
  ham_u32_t get_record_count(ham_u32_t slot) {
    return (m_records.get_record_count(slot));
  }

  // Returns the full record and stores it in |dest|
  void get_record(ham_u32_t slot, ByteArray *arena, ham_record_t *record,
                  ham_u32_t flags, ham_u32_t duplicate_index) {
    // copy the record data
    m_records.get_record(slot, arena, record, flags, duplicate_index);
  }

  // Updates the record of a key
  void set_record(ham_u32_t slot, ham_record_t *record,
                  ham_u32_t duplicate_index, ham_u32_t flags,
                  ham_u32_t *new_duplicate_index) {
    // automatically overwrite an existing key unless this is a
    // duplicate operation
    if ((flags & (HAM_DUPLICATE
                  | HAM_DUPLICATE
                  | HAM_DUPLICATE_INSERT_BEFORE
                  | HAM_DUPLICATE_INSERT_AFTER
                  | HAM_DUPLICATE_INSERT_FIRST
                  | HAM_DUPLICATE_INSERT_LAST)) == 0)
      flags |= HAM_OVERWRITE;

    m_records.set_record(slot, duplicate_index, record, flags,
            new_duplicate_index);
  }

  // Erases the extended part of a key
  void erase_key(ham_u32_t slot) {
    m_keys.erase_data(slot);
  }

  // Erases the record
  void erase_record(ham_u32_t slot, int duplicate_index,
                  bool all_duplicates) {
    m_records.erase_record(slot, duplicate_index, all_duplicates);
  }

  // Erases a key
  void erase(ham_u32_t slot) {
    size_t node_count = m_node->get_count();

    m_keys.erase_slot(node_count, slot);
    m_records.erase_slot(node_count, slot);
  }

  // Inserts a new key
  void insert(ham_u32_t slot, const ham_key_t *key) {
    size_t node_count = m_node->get_count();

    // make space for 1 additional element.
    // only store the key data; flags and record IDs are set by the caller
    m_keys.insert(node_count, slot, key);
    m_records.insert_slot(node_count, slot);
  }

  // Splits a node and moves parts of the current node into |other|, starting
  // at the |pivot| slot
  void split(BaseNodeImpl<KeyList, RecordList> *other, int pivot) {
    size_t node_count = m_node->get_count();
    size_t other_node_count = other->m_node->get_count();

    //
    // if a leaf page is split then the pivot element must be inserted in
    // the leaf page AND in the internal node. the internal node update
    // is handled by the caller.
    //
    // in internal nodes the pivot element is only propagated to the
    // parent node. the pivot element is skipped.
    //
    if (m_node->is_leaf()) {
      m_keys.copy_to(pivot, node_count, other->m_keys,
                      other_node_count, 0);
      m_records.copy_to(pivot, node_count, other->m_records,
                      other_node_count, 0);
    }
    else {
      m_keys.copy_to(pivot + 1, node_count, other->m_keys,
                      other_node_count, 0);
      m_records.copy_to(pivot + 1, node_count, other->m_records,
                      other_node_count, 0);
    }
  }

  // Returns true if the node requires a merge or a shift
  bool requires_merge() const {
    return (m_node->get_count() <= 3);
  }

  // Merges this node with the |other| node
  void merge_from(BaseNodeImpl<KeyList, RecordList> *other) {
    size_t node_count = m_node->get_count();
    size_t other_node_count = other->m_node->get_count();

    // shift items from the sibling to this page
    if (other_node_count > 0) {
      other->m_keys.copy_to(0, other_node_count, m_keys,
                      node_count, node_count);
      other->m_records.copy_to(0, other_node_count, m_records,
                      node_count, node_count);
    }
  }

  // Prints a slot to stdout (for debugging)
  void print(ham_u32_t slot) {
    std::stringstream ss;
    ss << "   ";
    m_keys.print(slot, ss);
    ss << " -> ";
    m_records.print(slot, ss);
    std::cout << ss.str() << std::endl;
  }

  // Returns the record id
  ham_u64_t get_record_id(ham_u32_t slot) const {
    return (m_records.get_record_id(slot));
  }

  // Sets the record id
  void set_record_id(ham_u32_t slot, ham_u64_t ptr) {
    m_records.set_record_id(slot, ptr);
  }

  // The page we're operating on
  Page *m_page;

  // The node we're operating on
  PBtreeNode *m_node;

  // Capacity of this node (maximum number of key/record pairs that
  // can be stored)
  size_t m_estimated_capacity;

  // for accessing the keys
  KeyList m_keys;

  // for accessing the records
  RecordList m_records;
};

} // namespace hamsterdb

#endif /* HAM_BTREE_IMPL_BASE_H */
