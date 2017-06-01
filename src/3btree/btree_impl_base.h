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
 * Base class for btree node implementations
 */

#ifndef UPS_BTREE_IMPL_BASE_H
#define UPS_BTREE_IMPL_BASE_H

#include "0root/root.h"

#include <sstream>
#include <iostream>

// Always verify that a file of level N does not include headers > N!
#include "1globals/globals.h"
#include "1base/dynamic_array.h"
#include "2page/page.h"
#include "3btree/btree_node.h"
#include "3btree/btree_keys_base.h"
#include "3btree/btree_visitor.h"
#include "4uqi/statements.h"
#include "4uqi/scanvisitor.h"
#include "4context/context.h"
#include "4db/db_local.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct Context;

template<typename KeyList, typename RecordList>
struct BaseNodeImpl {
  public:
    // Constructor
    BaseNodeImpl(Page *page_)
      : page(page_), node(PBtreeNode::from_page(page_)),
        estimated_capacity(0), keys(page->db(), node),
        records(page_->db(), node) {
    }

    // Returns the estimated page's capacity
    size_t estimate_capacity() const {
      return this->estimated_capacity;
    }

    // Checks this node's integrity
    virtual void check_integrity(Context *context) const {
    }

    // Returns a copy of a key and stores it in |dest|
    void key(Context *context, int slot, ByteArray *arena,
                    ups_key_t *dest) {
      // copy (or assign) the key data
      keys.key(context, slot, arena, dest, true);
    }

    // Returns the record size of a key or one of its duplicates
    uint32_t record_size(Context *context, int slot, int duplicate_index) {
      return records.record_size(context, slot, duplicate_index);
    }

    // Returns the number of duplicate records
    int record_count(Context *context, int slot) {
      return records.record_count(context, slot);
    }

    // Returns the full record and stores it in |dest|
    void record(Context *context, int slot, ByteArray *arena,
                    ups_record_t *record, uint32_t flags, int duplicate_index) {
      // copy the record data
      records.record(context, slot, arena, record, flags,
                      duplicate_index);
    }

    // Updates the record of a key
    void set_record(Context *context, int slot, ups_record_t *record,
                    int duplicate_index, uint32_t flags,
                    uint32_t *new_duplicate_index) {
      // automatically overwrite an existing key unless this is a
      // duplicate operation
      if (!ISSETANY(flags, UPS_DUPLICATE
                            | UPS_DUPLICATE
                            | UPS_DUPLICATE_INSERT_BEFORE
                            | UPS_DUPLICATE_INSERT_AFTER
                            | UPS_DUPLICATE_INSERT_FIRST
                            | UPS_DUPLICATE_INSERT_LAST))
        flags |= UPS_OVERWRITE;

      records.set_record(context, slot, duplicate_index, record, flags,
                      new_duplicate_index);
    }

    // Iterates all keys, calls the |visitor| on each
    void scan(Context *context, ScanVisitor *visitor,
                    SelectStatement *statement, uint32_t start, bool distinct) {
      // pass keys AND records to the visitor, or only one?
      bool requires_keys = statement->requires_keys;
      bool requires_records = statement->requires_records;

      // no records required? then use the "distinct" code path, which is
      // faster
      if (!requires_records)
        distinct = true;

      ByteArray *key_arena = &context->db->key_arena(context->txn);
      ByteArray *rec_arena = &context->db->record_arena(context->txn);

      // this branch handles non-duplicate block scans without an iterator
      if (distinct) {
        // only scan keys?
        if (KeyList::kSupportsBlockScans && !requires_records) {
          ScanResult sr = keys.scan(key_arena, node->length(), start);
          (*visitor)(sr.first, 0, sr.second);
          return;
        }

        // only scan records?
        if (RecordList::kSupportsBlockScans && !requires_keys) {
          ScanResult sr = records.scan(rec_arena, node->length(), start);
          (*visitor)(0, sr.first, sr.second);
          return;
        }

        // scan both?
        if (KeyList::kSupportsBlockScans
                && requires_keys
                && RecordList::kSupportsBlockScans
                && requires_records) {
          ScanResult srk = keys.scan(key_arena, node->length(), start);
          ScanResult srr = records.scan(rec_arena, node->length(), start);
          assert(srr.second == srk.second);
          (*visitor)(srk.first, srr.first, srk.second);
          return;
        }
      }

      // still here? then we have to use iterators
      ups_key_t key = {0};
      ups_record_t record = {0};
      ByteArray record_arena;
      size_t node_length = node->length();

      // otherwise iterate over the keys, call visitor for each key AND record
      if (distinct) {
        if (requires_keys && requires_records) {
          for (size_t i = start; i < node_length; i++) {
            keys.key(context, i, key_arena, &key, false);
            records.record(context, i, &record_arena, &record,
                          UPS_DIRECT_ACCESS, 0);
            (*visitor)(key.data, key.size, record.data, record.size);
          }
        }
        else if (requires_keys) {
          for (size_t i = start; i < node_length; i++) {
            keys.key(context, i, key_arena, &key, false);
            (*visitor)(key.data, key.size, 0, 0);
          }
        }
        else { // if (requires_records)
          for (size_t i = start; i < node_length; i++) {
            records.record(context, i, &record_arena, &record,
                          UPS_DIRECT_ACCESS, 0);
            (*visitor)(0, 0, record.data, record.size);
          }
        }
      }
      else {
        if (requires_keys && requires_records) {
          for (size_t i = start; i < node_length; i++) {
            keys.key(context, i, key_arena, &key, false);
            size_t duplicates = record_count(context, i);
            for (size_t d = 0; d < duplicates; d++) {
              records.record(context, i, &record_arena, &record,
                            UPS_DIRECT_ACCESS, d);
              (*visitor)(key.data, key.size, record.data, record.size);
            }
          }
        }
        else if (requires_keys) {
          for (size_t i = start; i < node_length; i++) {
            keys.key(context, i, key_arena, &key, false);
            size_t duplicates = record_count(context, i);
            for (size_t d = 0; d < duplicates; d++)
              (*visitor)(key.data, key.size, 0, 0);
          }
        }
        else { // if (requires_records)
          for (size_t i = start; i < node_length; i++) {
            size_t duplicates = record_count(context, i);
            for (size_t d = 0; d < duplicates; d++) {
              records.record(context, i, &record_arena, &record,
                            UPS_DIRECT_ACCESS, d);
              (*visitor)(0, 0, record.data, record.size);
            }
          }
        }
      }
    }

    // Erases the extended part of a key
    void erase_extended_key(Context *context, int slot) {
      keys.erase_extended_key(context, slot);
    }

    // Erases the record
    void erase_record(Context *context, int slot, int duplicate_index,
                    bool all_duplicates) {
      records.erase_record(context, slot, duplicate_index, all_duplicates);
    }

    // Erases a key
    void erase(Context *context, int slot) {
      size_t node_length = node->length();

      keys.erase(context, node_length, slot);
      records.erase(context, node_length, slot);
    }

    // Inserts a new key
    //
    // Most KeyLists first calculate the slot of the new key, then insert
    // the key at this slot. Both operations are separate from each other.
    // However, compressed KeyLists can overwrite this behaviour and
    // combine both calls into one to save performance. 
    template<typename Cmp>
    PBtreeNode::InsertResult insert(Context *context, ups_key_t *key,
                    uint32_t flags, Cmp &comparator) {
      PBtreeNode::InsertResult result(0, 0);
      size_t node_length = node->length();

      /* KeyLists with a custom insert function don't need a slot; only
       * calculate the slot for the default insert functions */
      if (!KeyList::kCustomInsert) {
        if (node_length == 0)
          result.slot = 0;
        else if (ISSET(flags, PBtreeNode::kInsertPrepend))
          result.slot = 0;
        else if (ISSET(flags, PBtreeNode::kInsertAppend))
          result.slot = node_length;
        else {
          int cmp;
          result.slot = find_lower_bound_impl(context, key, comparator, &cmp);

          /* insert the new key at the beginning? */
          if (result.slot == -1) {
            result.slot = 0;
            assert(cmp != 0);
          }
          /* key exists already */
          else if (cmp == 0) {
            result.status = UPS_DUPLICATE_KEY;
            return result;
          }
          /* if the new key is > than the slot key: move to the next slot */
          else if (cmp > 0)
            result.slot++;
        }
      }

      // Uncouple the cursors.
      //
      // for custom inserts we have to uncouple all cursors, because the
      // KeyList doesn't have access to the cursors in the page. In this
      // case result.slot is 0.
      if ((int)node_length > result.slot)
        BtreeCursor::uncouple_all_cursors(context, page, result.slot);

      // make space for 1 additional element.
      // only store the key data; flags and record IDs are set by the caller
      result = keys.insert(context, node_length, key, flags, comparator,
                        result.slot);
      if (result.status == 0)
        records.insert(context, node_length, result.slot);
      return result;
    }

    // Compares two keys using the supplied comparator
    template<typename Cmp>
    int compare(Context *context, const ups_key_t *lhs,
                    uint32_t rhs, Cmp &cmp) {
      if (KeyList::kHasSequentialData) {
        return cmp(lhs->data, lhs->size, keys.key_data(rhs),
                                keys.key_size(rhs));
      }
      else {
        ups_key_t tmp = {0};
        keys.key(context, rhs, &private_arena, &tmp, false);
        return cmp(lhs->data, lhs->size, tmp.data, tmp.size);
      }
    }

    // Searches the node for the key and returns the slot of this key
    template<typename Cmp>
    int find_lower_bound(Context *context, ups_key_t *key, Cmp &comparator,
                    uint64_t *precord_id, int *pcmp) {
      int slot = find_lower_bound_impl(context, key, comparator, pcmp);
      if (precord_id) {
        if (slot == -1 || (slot == 0 && *pcmp == -1))
          *precord_id = node->left_child();
        else
          *precord_id = records.record_id(slot);
      }
      return slot;
    }

    // Searches the node for the key and returns the slot of this key
    // - only for exact matches!
    template<typename Cmp>
    int find(Context *context, ups_key_t *key, Cmp &comparator) {
      return find_impl(context, key, comparator);
    }

    // Splits a node and moves parts of the current node into |other|, starting
    // at the |pivot| slot
    void split(Context *context, BaseNodeImpl<KeyList, RecordList> *other,
                    int pivot) {
      size_t node_length = node->length();
      size_t other_node_count = other->node->length();

      //
      // if a leaf page is split then the pivot element must be inserted in
      // the leaf page AND in the internal node. the internal node update
      // is handled by the caller.
      //
      // in internal nodes the pivot element is only propagated to the
      // parent node. the pivot element is skipped.
      //
      if (node->is_leaf()) {
        keys.copy_to(pivot, node_length, other->keys,
                        other_node_count, 0);
        records.copy_to(pivot, node_length, other->records,
                        other_node_count, 0);
      }
      else {
        keys.copy_to(pivot + 1, node_length, other->keys,
                        other_node_count, 0);
        records.copy_to(pivot + 1, node_length, other->records,
                        other_node_count, 0);
      }
    }

    // Returns true if the node requires a merge or a shift
    bool requires_merge() const {
      return node->length() <= 3;
    }

    // Merges this node with the |other| node
    void merge_from(Context *context,
                    BaseNodeImpl<KeyList, RecordList> *other) {
      size_t node_length = node->length();
      size_t other_node_count = other->node->length();

      // shift items from the sibling to this page
      if (other_node_count > 0) {
        other->keys.copy_to(0, other_node_count, keys,
                        node_length, node_length);
        other->records.copy_to(0, other_node_count, records,
                        node_length, node_length);
      }
    }

    // Reorganize this node; re-arranges capacities of KeyList and RecordList
    // in order to free space and avoid splits
    bool reorganize(Context *context, const ups_key_t *key) const {
      return false;
    }

    // Fills the btree_metrics structure
    void fill_metrics(btree_metrics_t *metrics, size_t node_length) {
      metrics->number_of_pages++;
      metrics->number_of_keys += node_length;

      BtreeStatistics::update_min_max_avg(&metrics->keys_per_page, node_length);

      keys.fill_metrics(metrics, node_length);
      records.fill_metrics(metrics, node_length);
    }

    // Prints a slot to stdout (for debugging)
    void print(Context *context, int slot) {
      std::stringstream ss;
      ss << "   ";
      keys.print(context, slot, ss);
      ss << " -> ";
      records.print(context, slot, ss);
      std::cout << ss.str() << std::endl;
    }

    // Returns the record id
    uint64_t record_id(Context *context, int slot) const {
      return records.record_id(slot);
    }

    // Sets the record id
    void set_record_id(Context *context, int slot, uint64_t ptr) {
      records.set_record_id(slot, ptr);
    }

    // The page we're operating on
    Page *page;

    // The node we're operating on
    PBtreeNode *node;

    // Capacity of this node (maximum number of key/record pairs that
    // can be stored)
    size_t estimated_capacity;

    // for accessing the keys
    KeyList keys;

    // for accessing the records
    RecordList records;

  private:
    // Implementation of the find method for lower-bound matches. If there
    // is no exact match then the lower bound is returned, and the compare value
    // is returned in |*pcmp|.
    template<typename Cmp>
    int find_lower_bound_impl(Context *context, const ups_key_t *key,
                    Cmp &comparator, int *pcmp) {
      if (KeyList::kCustomFindLowerBound)
        return keys.find_lower_bound(context, node->length(), key,
                      comparator, pcmp);

      return find_impl_binary(context, key, comparator, pcmp);
    }

    // Implementation of the find method for exact matches. Supports a custom
    // search implementation in the KeyList (i.e. for SIMD).
    template<typename Cmp>
    int find_impl(Context *context, const ups_key_t *key, Cmp &comparator) {
      if (KeyList::kCustomFind)
        return keys.find(context, node->length(), key, comparator);

      int cmp = 0;
      int slot = find_impl_binary(context, key, comparator, &cmp);
      if (slot == -1 || cmp != 0)
        return -1;
      return slot;
    }

    // Binary search
    template<typename Cmp>
    int find_impl_binary(Context *context, const ups_key_t *key,
            Cmp &comparator, int *pcmp) {
      int right = (int)node->length();
      int left = 0;
      int last = right + 1;

      *pcmp = -1;

      while (right - left > 0) {
        /* get the median item; if it's identical with the "last" item,
         * we've found the slot */
        int middle = (left + right) / 2;

        if (middle == last) {
          *pcmp = 1;
          return middle;
        }

        /* compare it against the key */
        *pcmp = compare(context, key, middle, comparator);

        /* found it? */
        if (*pcmp == 0) {
          return middle;
        }
        /* if the key is bigger than the item: search "to the left" */
        if (*pcmp < 0) {
          if (right == 0) {
            assert(middle == 0);
            return -1;
          }
          right = middle;
        }
        /* otherwise search "to the right" */
        else {
          last = middle;
          left = middle;
        }
      }

      return -1;
    }

    // A memory arena for various tasks
    ByteArray private_arena;
};

} // namespace upscaledb

#endif // UPS_BTREE_IMPL_BASE_H
