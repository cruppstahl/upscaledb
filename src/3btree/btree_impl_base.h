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
#include "1base/dynamic_array.h"
#include "2page/page.h"
#include "3btree/btree_node.h"
#include "3btree/btree_keys_base.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct Context;

template<typename KeyList, typename RecordList>
class BaseNodeImpl
{
  public:
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
    virtual void check_integrity(Context *context) const {
    }

    // Returns a copy of a key and stores it in |dest|
    void get_key(Context *context, int slot, ByteArray *arena,
                    ham_key_t *dest) {
      // copy (or assign) the key data
      m_keys.get_key(context, slot, arena, dest, true);
    }

    // Returns the record size of a key or one of its duplicates
    uint64_t get_record_size(Context *context, int slot, int duplicate_index) {
      return (m_records.get_record_size(context, slot, duplicate_index));
    }

    // Returns the record counter of a key
    int get_record_count(Context *context, int slot) {
      return (m_records.get_record_count(context, slot));
    }

    // Returns the full record and stores it in |dest|
    void get_record(Context *context, int slot, ByteArray *arena,
                    ham_record_t *record, uint32_t flags, int duplicate_index) {
      // copy the record data
      m_records.get_record(context, slot, arena, record,
                      flags, duplicate_index);
    }

    // Updates the record of a key
    void set_record(Context *context, int slot, ham_record_t *record,
                    int duplicate_index, uint32_t flags,
                    uint32_t *new_duplicate_index) {
      // automatically overwrite an existing key unless this is a
      // duplicate operation
      if ((flags & (HAM_DUPLICATE
                    | HAM_DUPLICATE
                    | HAM_DUPLICATE_INSERT_BEFORE
                    | HAM_DUPLICATE_INSERT_AFTER
                    | HAM_DUPLICATE_INSERT_FIRST
                    | HAM_DUPLICATE_INSERT_LAST)) == 0)
        flags |= HAM_OVERWRITE;

      m_records.set_record(context, slot, duplicate_index, record, flags,
              new_duplicate_index);
    }

    // Erases the extended part of a key
    void erase_extended_key(Context *context, int slot) {
      m_keys.erase_extended_key(context, slot);
    }

    // Erases the record
    void erase_record(Context *context, int slot, int duplicate_index,
                    bool all_duplicates) {
      m_records.erase_record(context, slot, duplicate_index, all_duplicates);
    }

    // Erases a key
    void erase(Context *context, int slot) {
      size_t node_count = m_node->get_count();

      m_keys.erase(context, node_count, slot);
      m_records.erase(context, node_count, slot);
    }

    // Inserts a new key
    //
    // Most KeyLists first calculate the slot of the new key, then insert
    // the key at this slot. Both operations are separate from each other.
    // However, compressed KeyLists can overwrite this behaviour and
    // combine both calls into one to save performance. 
    template<typename Cmp>
    PBtreeNode::InsertResult insert(Context *context, ham_key_t *key,
                    uint32_t flags, Cmp &comparator) {
      PBtreeNode::InsertResult result(0, 0);
      size_t node_count = m_node->get_count();

      if (node_count == 0)
        result.slot = 0;
      else if (flags & PBtreeNode::kInsertPrepend)
        result.slot = 0;
      else if (flags & PBtreeNode::kInsertAppend)
        result.slot = node_count;
      else {
        int cmp;
        result.slot = find_lowerbound_impl(context, key, comparator, &cmp);

        /* insert the new key at the beginning? */
        if (result.slot == -1) {
          result.slot = 0;
          ham_assert(cmp != 0);
        }
        /* key exists already */
        else if (cmp == 0) {
          result.status = HAM_DUPLICATE_KEY;
          return (result);
        }
        /* if the new key is > than the slot key: move to the next slot */
        else if (cmp > 0)
          result.slot++;
      }

      // Uncouple the cursors.
      //
      // for custom inserts we have to uncouple all cursors, because the
      // KeyList doesn't have access to the cursors in the page. In this
      // case result.slot is 0.
      if ((int)node_count > result.slot)
        BtreeCursor::uncouple_all_cursors(context, m_page, result.slot);

      // make space for 1 additional element.
      // only store the key data; flags and record IDs are set by the caller
      result = m_keys.insert(context, node_count, key, flags, comparator,
                        result.slot);
      m_records.insert(context, node_count, result.slot);
      return (result);
    }

    // Compares two keys using the supplied comparator
    template<typename Cmp>
    int compare(Context *context, const ham_key_t *lhs,
                    uint32_t rhs, Cmp &cmp) {
      if (KeyList::kHasSequentialData) {
        return (cmp(lhs->data, lhs->size, m_keys.get_key_data(rhs),
                                m_keys.get_key_size(rhs)));
      }
      else {
        ham_key_t tmp = {0};
        m_keys.get_key(context, rhs, &m_arena, &tmp, false);
        return (cmp(lhs->data, lhs->size, tmp.data, tmp.size));
      }
    }

    // Searches the node for the key and returns the slot of this key
    template<typename Cmp>
    int find_child(Context *context, ham_key_t *key, Cmp &comparator,
                    uint64_t *precord_id, int *pcmp) {
      int slot = find_lowerbound_impl(context, key, comparator, pcmp);
      if (precord_id) {
        if (slot == -1)
          *precord_id = m_node->get_ptr_down();
        else
          *precord_id = m_records.get_record_id(slot);
      }
      return (slot);
    }

    // Searches the node for the key and returns the slot of this key
    // - only for exact matches!
    template<typename Cmp>
    int find_exact(Context *context, ham_key_t *key, Cmp &comparator) {
      int cmp = 0;
      int r = find_exact_impl(context, key, comparator, &cmp);
      return (cmp ? -1 : r);
    }

    // Splits a node and moves parts of the current node into |other|, starting
    // at the |pivot| slot
    void split(Context *context, BaseNodeImpl<KeyList, RecordList> *other,
                    int pivot) {
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
    void merge_from(Context *context,
                    BaseNodeImpl<KeyList, RecordList> *other) {
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

    // Reorganize this node; re-arranges capacities of KeyList and RecordList
    // in order to free space and avoid splits
    bool reorganize(Context *context, const ham_key_t *key) const {
      return (false);
    }

    // Fills the btree_metrics structure
    void fill_metrics(btree_metrics_t *metrics, size_t node_count) {
      metrics->number_of_pages++;
      metrics->number_of_keys += node_count;

      BtreeStatistics::update_min_max_avg(&metrics->keys_per_page, node_count);

      m_keys.fill_metrics(metrics, node_count);
      m_records.fill_metrics(metrics, node_count);
    }

    // Prints a slot to stdout (for debugging)
    void print(Context *context, int slot) {
      std::stringstream ss;
      ss << "   ";
      m_keys.print(context, slot, ss);
      ss << " -> ";
      m_records.print(context, slot, ss);
      std::cout << ss.str() << std::endl;
    }

    // Returns the record id
    uint64_t get_record_id(Context *context, int slot) const {
      return (m_records.get_record_id(slot));
    }

    // Sets the record id
    void set_record_id(Context *context, int slot, uint64_t ptr) {
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

  private:
    // Implementation of the find method for lower-bound matches. If there
    // is no exact match then the lower bound is returned, and the compare value
    // is returned in |*pcmp|.
    template<typename Cmp>
    int find_lowerbound_impl(Context *context, const ham_key_t *key,
                    Cmp &comparator, int *pcmp) {
      switch ((int)KeyList::kSearchImplementation) {
        case BaseKeyList::kBinaryLinear:
          return (find_impl_binlin(context, key, comparator, pcmp));
        case BaseKeyList::kCustomSearch:
          return (m_keys.find(context, m_node->get_count(), key,
                      comparator, pcmp));
        default: // BaseKeyList::kBinarySearch
          return (find_impl_binary(context, key, comparator, pcmp));
      }
    }

    // Implementation of the find method for exact matches. Supports a custom
    // search implementation in the KeyList (i.e. for SIMD).
    template<typename Cmp>
    int find_exact_impl(Context *context, const ham_key_t *key,
                    Cmp &comparator, int *pcmp) {
      switch ((int)KeyList::kSearchImplementation) {
        case BaseKeyList::kBinaryLinear:
          return (find_impl_binlin(context, key, comparator, pcmp));
        case BaseKeyList::kCustomSearch:
        case BaseKeyList::kCustomExactImplementation:
          return (m_keys.find(context, m_node->get_count(), key,
                      comparator, pcmp));
        default: // BaseKeyList::kBinarySearch
          return (find_impl_binary(context, key, comparator, pcmp));
      }
    }

    // Binary search
    template<typename Cmp>
    int find_impl_binary(Context *context, const ham_key_t *key,
            Cmp &comparator, int *pcmp) {
      size_t node_count = m_node->get_count();
      ham_assert(node_count > 0);

      int i, l = 0, r = (int)node_count;
      int last = node_count + 1;
      int cmp = -1;

      /* repeat till we found the key or the remaining range is so small that
       * we rather perform a linear search (which is faster for small ranges) */
      while (r - l > 0) {
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
        cmp = compare(context, key, i, comparator);

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

      *pcmp = cmp;
      return (-1);
    }

    // Binary search combined with linear search
    template<typename Cmp>
    int find_impl_binlin(Context *context, const ham_key_t *key,
                    Cmp &comparator, int *pcmp) {
      size_t node_count = m_node->get_count();
      ham_assert(node_count > 0);

      int i, l = 0, r = (int)node_count;
      int last = node_count + 1;
      int cmp = -1;

      // Run a binary search, but fall back to linear search as soon as
      // the remaining range is too small. Sets threshold to 0 if linear
      // search is disabled for this KeyList.
      int threshold = m_keys.get_linear_search_threshold();

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
        cmp = compare(context, key, i, comparator);

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
      return (m_keys.linear_search(l, r - l, key, comparator, pcmp));
    }

    // A memory arena for various tasks
    ByteArray m_arena;
};

} // namespace hamsterdb

#endif /* HAM_BTREE_IMPL_BASE_H */
