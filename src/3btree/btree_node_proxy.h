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
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_BTREE_NODE_PROXY_H
#define HAM_BTREE_NODE_PROXY_H

#include "0root/root.h"

#include <set>
#include <string.h>
#include <iostream>
#include <sstream>
#include <fstream>

// Always verify that a file of level N does not include headers > N!
#include "1base/abi.h"
#include "1base/dynamic_array.h"
#include "1base/error.h"
#include "2page/page.h"
#include "3btree/btree_node.h"
#include "3blob_manager/blob_manager.h"
#include "4env/env_local.h"
#include "4db/db_local.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct Context;
struct ScanVisitor;

//
// A BtreeNodeProxy wraps a PBtreeNode structure and defines the actual
// format of the btree payload.
//
// The BtreeNodeProxy class provides access to the actual Btree nodes. The
// layout of those nodes depends heavily on the database configuration,
// and is implemented by template classes (btree_impl_default.h,
// btree_impl_pax.h.).
//
class BtreeNodeProxy
{
  public:
    // Constructor
    BtreeNodeProxy(Page *page)
      : m_page(page) {
    }

    // Destructor
    virtual ~BtreeNodeProxy() {
    }

    // Returns the flags of the btree node (|kLeafNode|)
    uint32_t get_flags() const {
      return (PBtreeNode::from_page(m_page)->get_flags());
    }

    // Sets the flags of the btree node (|kLeafNode|)
    void set_flags(uint32_t flags) {
      PBtreeNode::from_page(m_page)->set_flags(flags);
    }

    // Returns the number of entries in the BtreeNode
    size_t get_count() const {
      return (PBtreeNode::from_page(m_page)->get_count());
    }

    // Sets the number of entries in the BtreeNode
    void set_count(size_t count) {
      PBtreeNode::from_page(m_page)->set_count((uint32_t)count);
    }

    // Returns true if this btree node is a leaf node
    bool is_leaf() const {
      return (PBtreeNode::from_page(m_page)->is_leaf());
    }

    // Returns the address of the left sibling of this node
    uint64_t get_left() const {
      return (PBtreeNode::from_page(m_page)->get_left());
    }

    // Sets the address of the left sibling of this node
    void set_left(uint64_t address) {
      PBtreeNode::from_page(m_page)->set_left(address);
    }

    // Returns the address of the right sibling of this node
    uint64_t get_right() const {
      return (PBtreeNode::from_page(m_page)->get_right());
    }

    // Sets the address of the right sibling of this node
    void set_right(uint64_t address) {
      PBtreeNode::from_page(m_page)->set_right(address);
    }

    // Returns the ptr_down of this node
    uint64_t get_ptr_down() const {
      return (PBtreeNode::from_page(m_page)->get_ptr_down());
    }

    // Sets the ptr_down of this node
    void set_ptr_down(uint64_t address) {
      PBtreeNode::from_page(m_page)->set_ptr_down(address);
    }

    // Returns the page pointer - const version
    const Page *get_page() const {
      return (m_page);
    }

    // Returns the page pointer
    Page *get_page() {
      return (m_page);
    }

    // Returns the estimated capacity of this node
    virtual size_t estimate_capacity() const = 0;

    // Checks the integrity of the node. Throws an exception if it is
    // not. Called by ham_db_check_integrity().
    virtual void check_integrity(Context *context) const = 0;

    // Iterates all keys, calls the |visitor| on each
    virtual void scan(Context *context, ScanVisitor *visitor,
                    size_t start, bool distinct) = 0;

    // Compares the two keys. Returns 0 if both are equal, otherwise -1 (if
    // |lhs| is greater) or +1 (if |rhs| is greater).
    virtual int compare(const ham_key_t *lhs, const ham_key_t *rhs) const = 0;

    // Compares a public key and an internal key
    virtual int compare(Context *context, const ham_key_t *lhs, int rhs) = 0;

    // Returns true if the public key (|lhs|) and an internal key (slot
    // |rhs|) are equal
    virtual bool equals(Context *context, const ham_key_t *lhs, int rhs) = 0;

    // Searches the node for the |key|, and returns the slot of this key.
    // If |record_id| is not null then it will store the result of the last
    // compare operation.
    // If |pcmp| is not null then it will store the result of the last
    // compare operation.
    virtual int find_child(Context *context, ham_key_t *key,
                    uint64_t *record_id = 0, int *pcmp = 0) = 0;

    // Searches the node for the |key|, but will always return -1 if
    // an exact match was not found
    virtual int find_exact(Context *context, ham_key_t *key) = 0;

    // Returns the full key at the |slot|. Also resolves extended keys
    // and respects HAM_KEY_USER_ALLOC in dest->flags.
    virtual void get_key(Context *context, int slot, ByteArray *arena,
                    ham_key_t *dest) = 0;

    // Returns the number of records of a key at the given |slot|. This is
    // either 1 or higher, but only if duplicate keys exist.
    virtual int get_record_count(Context *context, int slot) = 0;

    // Returns the record size of a key or one of its duplicates.
    virtual uint64_t get_record_size(Context *context, int slot,
                    int duplicate_index) = 0;

    // Returns the record id of the key at the given |slot|
    // Only for internal nodes!
    virtual uint64_t get_record_id(Context *context, int slot) const = 0;

    // Sets the record id of the key at the given |slot|
    // Only for internal nodes!
    virtual void set_record_id(Context *context, int slot, uint64_t id) = 0;

    // Returns the full record and stores it in |dest|. The record is identified
    // by |slot| and |duplicate_index|. TINY and SMALL records are handled
    // correctly, as well as HAM_DIRECT_ACCESS.
    virtual void get_record(Context *context, int slot, ByteArray *arena,
                    ham_record_t *record, uint32_t flags,
                    int duplicate_index = 0) = 0;

    // High-level function to set a new record
    //
    // flags can be
    // - HAM_OVERWRITE
    // - HAM_DUPLICATE*
    //
    // a previously existing blob will be deleted if necessary
    virtual void set_record(Context *context, int slot, ham_record_t *record,
                    int duplicate_index, uint32_t flags,
                    uint32_t *new_duplicate_index) = 0;

    // Removes the record (or the duplicate of it, if |duplicate_index| is > 0).
    // If |all_duplicates| is set then all duplicates of this key are deleted.
    // |has_duplicates_left| will return true if there are more duplicates left
    // after the current one was deleted.
    virtual void erase_record(Context *context, int slot, int duplicate_index,
                    bool all_duplicates, bool *has_duplicates_left) = 0;

    // High level function to remove an existing entry
    virtual void erase(Context *context, int slot) = 0;

    // Erases all extended keys, overflow areas and records that are
    // linked from this page; usually called when the Database is deleted
    // or an In-Memory Database is freed
    virtual void remove_all_entries(Context *context) = 0;

    // High level function to insert a new key. Only inserts the key. The
    // actual record is then updated with |set_record|.
    virtual PBtreeNode::InsertResult insert(Context *context, ham_key_t *key,
                    uint32_t flags) = 0;

    // Returns true if a node requires a split to insert a new |key|
    virtual bool requires_split(Context *context, const ham_key_t *key = 0) = 0;

    // Returns true if a node requires a merge or a shift
    virtual bool requires_merge() const = 0;

    // Splits a page and moves all elements at a position >= |pivot|
    // to the |other| page. If the node is a leaf node then the pivot element
    // is also copied, otherwise it is not because it will be propagated
    // to the parent node instead (by the caller).
    virtual void split(Context *context, BtreeNodeProxy *other, int pivot) = 0;

    // Merges all keys from the |other| node to this node
    virtual void merge_from(Context *context, BtreeNodeProxy *other) = 0;

    // Fills the btree_metrics structure
    virtual void fill_metrics(btree_metrics_t *metrics) = 0;

    // Prints the node to stdout. Only for testing and debugging!
    virtual void print(Context *context, size_t node_count = 0) = 0;

    // Returns the class name. Only for testing! Uses the functions exported
    // by abi.h, which are only available on assorted platforms. Other
    // platforms will return empty strings.
    virtual std::string test_get_classname() const = 0;

  protected:
    Page *m_page;
};

//
// A comparator which uses a user-supplied callback function (installed
// with |ham_db_set_compare_func|) to compare two keys
//
struct CallbackCompare
{
  CallbackCompare(LocalDatabase *db)
    : m_db(db) {
  }

  int operator()(const void *lhs_data, uint32_t lhs_size,
          const void *rhs_data, uint32_t rhs_size) const {
    return (m_db->compare_func()((::ham_db_t *)m_db, (uint8_t *)lhs_data,
                            lhs_size, (uint8_t *)rhs_data, rhs_size));
  }

  LocalDatabase *m_db;
};

//
// A comparator for numeric keys.
// The actual type for the key is supplied with a template parameter.
// This has to be a POD type with support for operators < and >.
//
template<typename T>
struct NumericCompare
{
  NumericCompare(LocalDatabase *) {
  }

  int operator()(const void *lhs_data, uint32_t lhs_size,
          const void *rhs_data, uint32_t rhs_size) const {
    ham_assert(lhs_size == rhs_size);
    ham_assert(lhs_size == sizeof(T));
    T l = *(T *)lhs_data;
    T r = *(T *)rhs_data;
    return (l < r ? -1 : (l > r ? +1 : 0));
  }
};

//
// The default comparator for two keys, implemented with memcmp(3).
// Both keys have the same size!
//
struct FixedSizeCompare
{
  FixedSizeCompare(LocalDatabase *) {
  }

  int operator()(const void *lhs_data, uint32_t lhs_size,
          const void *rhs_data, uint32_t rhs_size) const {
    ham_assert(lhs_size == rhs_size);
    return (::memcmp(lhs_data, rhs_data, lhs_size));
  }
};

//
// The default comparator for two keys, implemented with memcmp(3).
// Both keys can have different sizes! shorter strings are treated as
// "greater"
//
struct VariableSizeCompare
{
  VariableSizeCompare(LocalDatabase *) {
  }

  int operator()(const void *lhs_data, uint32_t lhs_size,
          const void *rhs_data, uint32_t rhs_size) const {
    if (lhs_size < rhs_size) {
      int m = ::memcmp(lhs_data, rhs_data, lhs_size);
      return (m == 0 ? -1 : m);
    }
    if (rhs_size < lhs_size) {
      int m = ::memcmp(lhs_data, rhs_data, rhs_size);
      return (m == 0 ? +1 : m);
    }
    return (::memcmp(lhs_data, rhs_data, lhs_size));
  }
};

//
// An implementation of the BtreeNodeProxy interface declared above.
// Its actual memory implementation of the btree keys/records is delegated
// to a template parameter |NodeImpl|, and the key comparisons are
// delegated to |Comparator|.
//
template<class NodeImpl, class Comparator>
class BtreeNodeProxyImpl : public BtreeNodeProxy
{
  typedef BtreeNodeProxyImpl<NodeImpl, Comparator> ClassType;

  public:
    // Constructor
    BtreeNodeProxyImpl(Page *page)
      : BtreeNodeProxy(page), m_impl(page) {
    }

    // Returns the estimated capacity of this node
    virtual size_t estimate_capacity() const {
      return (m_impl.estimate_capacity());
    }

    // Checks the integrity of the node
    virtual void check_integrity(Context *context) const {
      m_impl.check_integrity(context);
    }

    // Iterates all keys, calls the |visitor| on each
    virtual void scan(Context *context, ScanVisitor *visitor,
                    size_t start, bool distinct) {
      m_impl.scan(context, visitor, start, distinct);
    }

    // Compares two internal keys using the supplied comparator
    virtual int compare(const ham_key_t *lhs, const ham_key_t *rhs) const {
      Comparator cmp(m_page->get_db());
      return (cmp(lhs->data, lhs->size, rhs->data, rhs->size));
    }

    // Compares a public key and an internal key
    virtual int compare(Context *context, const ham_key_t *lhs, int rhs) {
      Comparator cmp(m_page->get_db());
      return (m_impl.compare(context, lhs, rhs, cmp));
    }

    // Returns true if the public key and an internal key are equal
    virtual bool equals(Context *context, const ham_key_t *lhs, int rhs) {
      return (0 == compare(context, lhs, rhs));
    }

    // Searches the node for the key and returns the slot of this key.
    // If |pcmp| is not null then it will store the result of the last
    // compare operation.
    virtual int find_child(Context *context, ham_key_t *key,
                    uint64_t *precord_id = 0, int *pcmp = 0) {
      int dummy;
      if (get_count() == 0) {
        if (pcmp)
          *pcmp = 1;
        if (precord_id)
          *precord_id = get_ptr_down();
        return (-1);
      }
      Comparator cmp(m_page->get_db());
      return (m_impl.find_child(context, key, cmp,
                              precord_id ? precord_id : 0,
                              pcmp ? pcmp : &dummy));
    }

    // Searches the node for the |key|, but will always return -1 if
    // an exact match was not found
    virtual int find_exact(Context *context, ham_key_t *key) {
      if (get_count() == 0)
        return (-1);
      Comparator cmp(m_page->get_db());
      return (m_impl.find_exact(context, key, cmp));
    }

    // Returns the full key at the |slot|. Also resolves extended keys
    // and respects HAM_KEY_USER_ALLOC in dest->flags.
    virtual void get_key(Context *context, int slot, ByteArray *arena,
                    ham_key_t *dest) {
      m_impl.get_key(context, slot, arena, dest);
    }

    // Returns the number of records of a key at the given |slot|
    virtual int get_record_count(Context *context, int slot) {
      ham_assert(slot < (int)get_count());
      return (m_impl.get_record_count(context, slot));
    }

    // Returns the full record and stores it in |dest|. The record is identified
    // by |slot| and |duplicate_index|. TINY and SMALL records are handled
    // correctly, as well as HAM_DIRECT_ACCESS.
    virtual void get_record(Context *context, int slot, ByteArray *arena,
                    ham_record_t *record, uint32_t flags,
                    int duplicate_index = 0) {
      ham_assert(slot < (int)get_count());
      m_impl.get_record(context, slot, arena, record, flags, duplicate_index);
    }

    virtual void set_record(Context *context, int slot, ham_record_t *record,
                    int duplicate_index, uint32_t flags,
                    uint32_t *new_duplicate_index) {
      m_impl.set_record(context, slot, record, duplicate_index, flags,
                      new_duplicate_index);
    }

    // Returns the record size of a key or one of its duplicates
    virtual uint64_t get_record_size(Context *context, int slot,
                    int duplicate_index) {
      ham_assert(slot < (int)get_count());
      return (m_impl.get_record_size(context, slot, duplicate_index));
    }

    // Returns the record id of the key at the given |slot|
    // Only for internal nodes!
    virtual uint64_t get_record_id(Context *context, int slot) const {
      ham_assert(slot < (int)get_count());
      return (m_impl.get_record_id(context, slot));
    }

    // Sets the record id of the key at the given |slot|
    // Only for internal nodes!
    virtual void set_record_id(Context *context, int slot, uint64_t id) {
      return (m_impl.set_record_id(context, slot, id));
    }

    // High level function to remove an existing entry. Will call
    // |erase_extended_key| to clean up (a potential) extended key,
    // and |erase_record| on each record that is associated with the key.
    virtual void erase(Context *context, int slot) {
      ham_assert(slot < (int)get_count());
      m_impl.erase(context, slot);
      set_count(get_count() - 1);
    }

    // Removes the record (or the duplicate of it, if |duplicate_index| is > 0).
    // If |all_duplicates| is set then all duplicates of this key are deleted.
    // |has_duplicates_left| will return true if there are more duplicates left
    // after the current one was deleted.
    virtual void erase_record(Context *context, int slot, int duplicate_index,
                    bool all_duplicates, bool *has_duplicates_left) {
      ham_assert(slot < (int)get_count());
      m_impl.erase_record(context, slot, duplicate_index, all_duplicates);
      if (has_duplicates_left)
        *has_duplicates_left = get_record_count(context, slot) > 0;
    }

    // Erases all extended keys, overflow areas and records that are
    // linked from this page; usually called when the Database is deleted
    // or an In-Memory Database is closed
    virtual void remove_all_entries(Context *context) {
      size_t node_count = get_count();
      for (size_t i = 0; i < node_count; i++) {
        m_impl.erase_extended_key(context, i);

        // If we're in the leaf page, delete the associated record. (Only
        // leaf nodes have records; internal nodes have record IDs that
        // reference other pages, and these pages must not be deleted.)
        if (is_leaf())
          erase_record(context, i, 0, true, 0);
      }
    }

    // High level function to insert a new key. Only inserts the key. The
    // actual record is then updated with |set_record|.
    virtual PBtreeNode::InsertResult insert(Context *context,
                    ham_key_t *key, uint32_t flags) {
      PBtreeNode::InsertResult result(0, 0);
      if (m_impl.requires_split(context, key)) {
        result.status = HAM_LIMITS_REACHED;
        return (result);
      }

      Comparator cmp(m_page->get_db());
      try {
        result = m_impl.insert(context, key, flags, cmp);
      }
      catch (Exception &ex) {
        result.status = ex.code;
      }

      // split required? then reorganize the node, try again
      if (result.status == HAM_LIMITS_REACHED) {
        try {
          if (m_impl.reorganize(context, key))
            result = m_impl.insert(context, key, flags, cmp);
        }
        catch (Exception &ex) {
          result.status = ex.code;
        }
      }

      if (result.status == HAM_SUCCESS)
        set_count(get_count() + 1);

      return (result);
    }

    // Returns true if a node requires a split to insert |key|
    virtual bool requires_split(Context *context, const ham_key_t *key = 0) {
      return (m_impl.requires_split(context, key));
    }

    // Returns true if a node requires a merge or a shift
    virtual bool requires_merge() const {
      return (m_impl.requires_merge());
    }

    // Splits the node
    virtual void split(Context *context, BtreeNodeProxy *other_node,
                    int pivot) {
      ClassType *other = dynamic_cast<ClassType *>(other_node);
      ham_assert(other != 0);

      m_impl.split(context, &other->m_impl, pivot);

      size_t node_count = get_count();
      set_count(pivot);

      if (is_leaf())
        other->set_count(node_count - pivot);
      else
        other->set_count(node_count - pivot - 1);
    }

    // Merges all keys from the |other| node into this node
    virtual void merge_from(Context *context, BtreeNodeProxy *other_node) {
      ClassType *other = dynamic_cast<ClassType *>(other_node);
      ham_assert(other != 0);

      m_impl.merge_from(context, &other->m_impl);

      set_count(get_count() + other->get_count());
      other->set_count(0);
    }

    // Fills the btree_metrics structure
    virtual void fill_metrics(btree_metrics_t *metrics) {
      m_impl.fill_metrics(metrics, get_count());
    }

    // Prints the node to stdout (for debugging)
    virtual void print(Context *context, size_t node_count = 0) {
      std::cout << "page " << m_page->get_address() << ": " << get_count()
          << " elements (leaf: " << (is_leaf() ? 1 : 0) << ", left: "
          << get_left() << ", right: " << get_right() << ", ptr_down: "
          << get_ptr_down() << ")" << std::endl;
      if (!node_count)
        node_count = get_count();
      for (size_t i = 0; i < node_count; i++)
        m_impl.print(context, i);
    }

    // Returns the class name. Only for testing! Uses the functions exported
    // by abi.h, which are only available on assorted platforms. Other
    // platforms will return empty strings.
    virtual std::string test_get_classname() const {
      return (get_classname(*this));
    }

  private:
    NodeImpl m_impl;
};

} // namespace hamsterdb

#endif /* HAM_BTREE_NODE_PROXY_H */
