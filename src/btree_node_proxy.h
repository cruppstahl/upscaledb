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

#ifndef HAM_BTREE_NODE_PROXY_H__
#define HAM_BTREE_NODE_PROXY_H__

#include "abi.h"
#include "util.h"
#include "page.h"
#include "btree_node.h"
#include "blob_manager.h"
#include "env_local.h"

#undef min  // avoid MSVC conflicts with std::min

namespace hamsterdb {

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
    ham_u32_t get_flags() const {
      return (PBtreeNode::from_page(m_page)->get_flags());
    }

    // Sets the flags of the btree node (|kLeafNode|)
    void set_flags(ham_u32_t flags) {
      PBtreeNode::from_page(m_page)->set_flags(flags);
    }

    // Returns the number of entries in the BtreeNode
    ham_u32_t get_count() const {
      return (PBtreeNode::from_page(m_page)->get_count());
    }

    // Sets the number of entries in the BtreeNode
    void set_count(ham_u32_t count) {
      PBtreeNode::from_page(m_page)->set_count(count);
    }

    // Returns true if this btree node is a leaf node
    bool is_leaf() const {
      return (PBtreeNode::from_page(m_page)->is_leaf());
    }

    // Returns the address of the left sibling of this node
    ham_u64_t get_left() const {
      return (PBtreeNode::from_page(m_page)->get_left());
    }

    // Sets the address of the left sibling of this node
    void set_left(ham_u64_t address) {
      PBtreeNode::from_page(m_page)->set_left(address);
    }

    // Returns the address of the right sibling of this node
    ham_u64_t get_right() const {
      return (PBtreeNode::from_page(m_page)->get_right());
    }

    // Sets the address of the right sibling of this node
    void set_right(ham_u64_t address) {
      PBtreeNode::from_page(m_page)->set_right(address);
    }

    // Returns the ptr_down of this node
    ham_u64_t get_ptr_down() const {
      return (PBtreeNode::from_page(m_page)->get_ptr_down());
    }

    // Sets the ptr_down of this node
    void set_ptr_down(ham_u64_t address) {
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
    virtual size_t get_capacity() const = 0;

    // Checks the integrity of the node. Throws an exception if it is
    // not. Called by ham_db_check_integrity().
    virtual void check_integrity() const = 0;

    // Iterates all keys, calls the |visitor| on each. Aborts if the
    // |visitor| returns false.
    virtual void enumerate(BtreeVisitor &visitor) = 0;

    // Iterates all keys, calls the |visitor| on each
    virtual void scan(ScanVisitor *visitor, ham_u32_t start, bool distinct) = 0;

    // Compares the two keys. Returns 0 if both are equal, otherwise -1 (if
    // |lhs| is greater) or +1 (if |rhs| is greater).
    virtual int compare(const ham_key_t *lhs, const ham_key_t *rhs) const = 0;

    // Compares a public key and an internal key
    virtual int compare(const ham_key_t *lhs, int rhs) = 0;

    // Returns true if the public key (|lhs|) and an internal key (slot
    // |rhs|) are equal
    virtual bool equals(const ham_key_t *lhs, int rhs) = 0;

    // Searches the node for the |key|, and returns the slot of this key.
    // If |record_id| is not null then it will store the result of the last
    // compare operation.
    // If |pcmp| is not null then it will store the result of the last
    // compare operation.
    virtual int find_child(ham_key_t *key, ham_u64_t *record_id = 0,
                    int *pcmp = 0) = 0;

    // Searches the node for the |key|, but will always return -1 if
    // an exact match was not found
    virtual int find_exact(ham_key_t *key) = 0;

    // Returns the full key at the |slot|. Also resolves extended keys
    // and respects HAM_KEY_USER_ALLOC in dest->flags. Record number keys
    // are endian-translated.
    virtual void get_key(ham_u32_t slot, ByteArray *arena, ham_key_t *dest) = 0;

    // Returns the number of records of a key at the given |slot|. This is
    // either 1 or higher, but only if duplicate keys exist.
    virtual ham_u32_t get_record_count(ham_u32_t slot) = 0;

    // Returns the record size of a key or one of its duplicates.
    virtual ham_u64_t get_record_size(ham_u32_t slot, int duplicate_index) = 0;

    // Returns the record id of the key at the given |slot|
    // Only for internal nodes!
    virtual ham_u64_t get_record_id(ham_u32_t slot) const = 0;

    // Sets the record id of the key at the given |slot|
    // Only for internal nodes!
    virtual void set_record_id(ham_u32_t slot, ham_u64_t id) = 0;

    // Returns the full record and stores it in |dest|. The record is identified
    // by |slot| and |duplicate_index|. TINY and SMALL records are handled
    // correctly, as well as HAM_DIRECT_ACCESS.
    virtual void get_record(ham_u32_t slot, ByteArray *arena,
                    ham_record_t *record, ham_u32_t flags,
                    ham_u32_t duplicate_index = 0) = 0;

    // High-level function to set a new record
    //
    // flags can be
    // - HAM_OVERWRITE
    // - HAM_DUPLICATE*
    //
    // a previously existing blob will be deleted if necessary
    virtual void set_record(ham_u32_t slot, ham_record_t *record,
                    ham_u32_t duplicate_index, ham_u32_t flags,
                    ham_u32_t *new_duplicate_index) = 0;

    // Removes the record (or the duplicate of it, if |duplicate_index| is > 0).
    // If |all_duplicates| is set then all duplicates of this key are deleted.
    // |has_duplicates_left| will return true if there are more duplicates left
    // after the current one was deleted.
    virtual void erase_record(ham_u32_t slot, int duplicate_index,
                    bool all_duplicates, bool *has_duplicates_left) = 0;

    // High level function to remove an existing entry
    virtual void erase(ham_u32_t slot) = 0;

    // Erases all extended keys, overflow areas and records that are
    // linked from this page; usually called when the Database is deleted
    // or an In-Memory Database is freed
    virtual void remove_all_entries() = 0;

    // High level function to insert a new key. Only inserts the key. The
    // actual record is then updated with |set_record|.
    virtual void insert(ham_u32_t slot, const ham_key_t *key) = 0;

    // Returns true if a node requires a split to insert a new |key|
    virtual bool requires_split() = 0;

    // Returns true if a node requires a merge or a shift
    virtual bool requires_merge() const = 0;

    // Splits a page and moves all elements at a position >= |pivot|
    // to the |other| page. If the node is a leaf node then the pivot element
    // is also copied, otherwise it is not because it will be propagated
    // to the parent node instead (by the caller).
    virtual void split(BtreeNodeProxy *other, int pivot) = 0;

    // Merges all keys from the |other| node to this node
    virtual void merge_from(BtreeNodeProxy *other) = 0;

    // Prints the node to stdout. Only for testing and debugging!
    virtual void print(ham_u32_t count = 0) = 0;

    // Returns the flags of the key at the given |slot|. Only for testing!
    virtual ham_u32_t test_get_flags(ham_u32_t slot) const = 0;

    // Sets a key. Only for testing
    virtual void test_set_key(ham_u32_t slot, const char *data,
                    size_t data_size, ham_u32_t flags, ham_u64_t record_id) = 0;

    // Clears the page with zeroes and reinitializes it. Only for testing!
    virtual void test_clear_page() = 0;

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

  int operator()(const void *lhs_data, ham_u32_t lhs_size,
          const void *rhs_data, ham_u32_t rhs_size) const {
    return (m_db->get_compare_func()((::ham_db_t *)m_db, (ham_u8_t *)lhs_data,
                            lhs_size, (ham_u8_t *)rhs_data, rhs_size));
  }

  LocalDatabase *m_db;
};

//
// A comparator for record number keys (includes endian conversion)
//
struct RecordNumberCompare
{
  RecordNumberCompare(LocalDatabase *) {
  }

  int operator()(const void *lhs_data, ham_u32_t lhs_size,
          const void *rhs_data, ham_u32_t rhs_size) const {
    ham_assert(lhs_size == rhs_size);
    ham_assert(lhs_size == sizeof(ham_u64_t));
    ham_u64_t l = ham_db2h64(*(ham_u64_t *)lhs_data);
    ham_u64_t r = ham_db2h64(*(ham_u64_t *)rhs_data);
    return (l < r ? -1 : (l > r ? +1 : 0));
  }
};

//
// A comparator for numeric keys (without endian conversion).
// The actual type for the key is supplied with a template parameter.
// This has to be a POD type with support for operators < and >.
//
template<typename T>
struct NumericCompare
{
  NumericCompare(LocalDatabase *) {
  }

  int operator()(const void *lhs_data, ham_u32_t lhs_size,
          const void *rhs_data, ham_u32_t rhs_size) const {
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

  int operator()(const void *lhs_data, ham_u32_t lhs_size,
          const void *rhs_data, ham_u32_t rhs_size) const {
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

  int operator()(const void *lhs_data, ham_u32_t lhs_size,
          const void *rhs_data, ham_u32_t rhs_size) const {
    if (lhs_size < rhs_size) {
      int m = ::memcmp(lhs_data, rhs_data, lhs_size);
      if (m < 0)
        return (-1);
      if (m > 0)
        return (+1);
      return (-1);
    }
    else if (rhs_size < lhs_size) {
      int m = ::memcmp(lhs_data, rhs_data, rhs_size);
      if (m < 0)
        return (-1);
      if (m > 0)
        return (+1);
      return (+1);
    }
    else {
      int m = memcmp(lhs_data, rhs_data, lhs_size);
      if (m < 0)
        return (-1);
      if (m > 0)
        return (+1);
      return (0);
    }
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
    virtual size_t get_capacity() const {
      return (m_impl.get_capacity());
    }

    // Checks the integrity of the node
    virtual void check_integrity() const {
      m_impl.check_integrity();
    }

    // Iterates all keys, calls the |visitor| on each. Aborts if the
    // |visitor| returns false
    virtual void enumerate(BtreeVisitor &visitor) {
      ham_u32_t count = get_count();
      for (ham_u32_t i = 0; i < count; i++) {
        if (!visitor(this, m_impl.get_key_data(i), 0,
                        m_impl.get_key_size(i), 0))
          break;
      }
    }

    // Iterates all keys, calls the |visitor| on each
    virtual void scan(ScanVisitor *visitor, ham_u32_t start, bool distinct) {
      m_impl.scan(visitor, start, distinct);
    }

    // Compares two internal keys using the supplied comparator
    virtual int compare(const ham_key_t *lhs, const ham_key_t *rhs) const {
      Comparator cmp(m_page->get_db());
      return (cmp(lhs->data, lhs->size, rhs->data, rhs->size));
    }

    // Compares a public key and an internal key
    virtual int compare(const ham_key_t *lhs, int rhs) {
      Comparator cmp(m_page->get_db());
      return (m_impl.compare(lhs, rhs, cmp));
    }

    // Returns true if the public key and an internal key are equal
    virtual bool equals(const ham_key_t *lhs, int rhs) {
      if (m_impl.get_key_size(rhs) != lhs->size)
        return (false);
      return (0 == compare(lhs, rhs));
    }

    // Searches the node for the key and returns the slot of this key.
    // If |pcmp| is not null then it will store the result of the last
    // compare operation.
    virtual int find_child(ham_key_t *key, ham_u64_t *precord_id = 0,
                    int *pcmp = 0) {
      int dummy;
      if (get_count() == 0) {
        if (pcmp)
          *pcmp = 1;
        if (precord_id)
          *precord_id = get_ptr_down();
        return (-1);
      }
      Comparator cmp(m_page->get_db());
      return (m_impl.find_child(key, cmp,
                              precord_id ? precord_id : 0,
                              pcmp ? pcmp : &dummy));
    }

    // Searches the node for the |key|, but will always return -1 if
    // an exact match was not found
    virtual int find_exact(ham_key_t *key) {
      if (get_count() == 0)
        return (-1);
      Comparator cmp(m_page->get_db());
      return (m_impl.find_exact(key, cmp));
    }

    // Returns the full key at the |slot|. Also resolves extended keys
    // and respects HAM_KEY_USER_ALLOC in dest->flags. Record number keys
    // are endian-translated.
    virtual void get_key(ham_u32_t slot, ByteArray *arena, ham_key_t *dest) {
      if (dest->flags & HAM_KEY_USER_ALLOC) {
        arena->assign(dest->data, dest->size);
        arena->disown();
      }
      m_impl.get_key(slot, arena, dest);
    }

    // Returns the number of records of a key at the given |slot|
    virtual ham_u32_t get_record_count(ham_u32_t slot) {
      ham_assert(slot < get_count());
      return (m_impl.get_record_count(slot));
    }

    // Returns the full record and stores it in |dest|. The record is identified
    // by |slot| and |duplicate_index|. TINY and SMALL records are handled
    // correctly, as well as HAM_DIRECT_ACCESS.
    virtual void get_record(ham_u32_t slot, ByteArray *arena,
                    ham_record_t *record, ham_u32_t flags,
                    ham_u32_t duplicate_index = 0) {
      ham_assert(slot < get_count());
      m_impl.get_record(slot, arena, record, flags, duplicate_index);
    }

    virtual void set_record(ham_u32_t slot, ham_record_t *record,
                    ham_u32_t duplicate_index, ham_u32_t flags,
                    ham_u32_t *new_duplicate_index) {
      m_impl.set_record(slot, record, duplicate_index, flags,
                      new_duplicate_index);
    }

    // Returns the record size of a key or one of its duplicates
    virtual ham_u64_t get_record_size(ham_u32_t slot, int duplicate_index) {
      ham_assert(slot < get_count());
      return (m_impl.get_record_size(slot, duplicate_index));
    }

    // Returns the record id of the key at the given |slot|
    // Only for internal nodes!
    virtual ham_u64_t get_record_id(ham_u32_t slot) const {
      ham_assert(slot < get_count());
      return (m_impl.get_record_id(slot));
    }

    // Sets the record id of the key at the given |slot|
    // Only for internal nodes!
    virtual void set_record_id(ham_u32_t slot, ham_u64_t id) {
      return (m_impl.set_record_id(slot, id));
    }

    // High level function to remove an existing entry. Will call |erase_key|
    // to clean up (a potential) extended key, and |erase_record| on each
    // record that is associated with the key.
    virtual void erase(ham_u32_t slot) {
      ham_assert(slot < get_count());
      m_impl.erase(slot);
      set_count(get_count() - 1);
    }

    // Removes the record (or the duplicate of it, if |duplicate_index| is > 0).
    // If |all_duplicates| is set then all duplicates of this key are deleted.
    // |has_duplicates_left| will return true if there are more duplicates left
    // after the current one was deleted.
    virtual void erase_record(ham_u32_t slot, int duplicate_index,
                    bool all_duplicates, bool *has_duplicates_left) {
      ham_assert(slot < get_count());
      m_impl.erase_record(slot, duplicate_index, all_duplicates);
      if (has_duplicates_left)
        *has_duplicates_left = get_record_count(slot) > 0;
    }

    // Erases all extended keys, overflow areas and records that are
    // linked from this page; usually called when the Database is deleted
    // or an In-Memory Database is closed
    virtual void remove_all_entries() {
      ham_u32_t count = get_count();
      for (ham_u32_t i = 0; i < count; i++) {
        m_impl.erase_key(i);

        // If we're in the leaf page, delete the associated record. (Only
        // leaf nodes have records; internal nodes have record IDs that
        // reference other pages, and these pages must not be deleted.)
        if (is_leaf())
          erase_record(i, 0, true, 0);
      }
    }

    // High level function to insert a new key. Only inserts the key. The
    // actual record is then updated with |set_record|.
    virtual void insert(ham_u32_t slot, const ham_key_t *key) {
      m_impl.insert(slot, key);
      set_count(get_count() + 1);
    }

    // Returns true if a node requires a split to insert |key|
    virtual bool requires_split() {
      return (m_impl.requires_split());
    }

    // Returns true if a node requires a merge or a shift
    virtual bool requires_merge() const {
      return (m_impl.requires_merge());
    }

    // Splits the node
    virtual void split(BtreeNodeProxy *other_node, int pivot) {
      ClassType *other = dynamic_cast<ClassType *>(other_node);
      ham_assert(other != 0);

      m_impl.split(&other->m_impl, pivot);

      ham_u32_t count = get_count();
      set_count(pivot);

      if (is_leaf())
        other->set_count(count - pivot);
      else
        other->set_count(count - pivot - 1);
    }

    // Merges all keys from the |other| node into this node
    virtual void merge_from(BtreeNodeProxy *other_node) {
      ClassType *other = dynamic_cast<ClassType *>(other_node);
      ham_assert(other != 0);

      m_impl.merge_from(&other->m_impl);

      set_count(get_count() + other->get_count());
      other->set_count(0);
    }

    // Prints the node to stdout (for debugging)
    virtual void print(ham_u32_t count = 0) {
      printf("page %llu: %u elements (leaf: %d, left: %llu, right: %llu, "
              "ptr_down: %llu)\n",
              (unsigned long long)m_page->get_address(), get_count(),
              is_leaf() ? 1 : 0,
              (unsigned long long)get_left(), (unsigned long long)get_right(),
              (unsigned long long)get_ptr_down());
      ByteArray arena;
      if (!count)
        count = get_count();
      for (ham_u32_t i = 0; i < count; i++) {
        if (m_impl.get_key_flags(i) & BtreeKey::kExtendedKey
            || m_impl.get_key_flags(i) & BtreeKey::kCompressed) {
          ham_key_t key = {0};
          get_key(i, &arena, &key);
          printf("%03u: EX ", i);
          for (ham_u32_t j = 0; j < 5; j++) {
            char ch = ((const char *)key.data)[j];
            if (ch >= 10)
              ch += 'a' - 10;
            else
              ch += '0';
            printf("%c", ch);
          }
          printf(" (%d) -> %08llx\n",
                      key.size, (unsigned long long)m_impl.get_record_id(i));
          //printf("%03u: EX %s (%d) -> %08llx\n", i, (const char *)key.data,
                      //key.size, (unsigned long long)m_impl.get_record_id(i));
        }
        else {
         printf("%03u:    ", i);
         printf("    %08u -> %08llx\n", *(ham_u32_t *)m_impl.get_key_data(i),
                  (unsigned long long)m_impl.get_record_id(i));
         //for (ham_u32_t j = 0; j < m_impl.get_key_size(i); j++)
           //printf("%c", ((const char *)m_impl.get_key_data(i))[j]);
         //printf(" (%d) -> %08llx\n", m_impl.get_key_size(i),
                          //(unsigned long long)m_impl.get_record_id(i));
        }
      }
    }

    // Returns the flags of the key at the given |slot|; only for testing!
    virtual ham_u32_t test_get_flags(ham_u32_t slot) const {
      return (m_impl.get_key_flags(slot) | m_impl.get_record_flags(slot));
    }

    // Sets a key; only for testing
    virtual void test_set_key(ham_u32_t slot, const char *data,
                    size_t data_size, ham_u32_t flags, ham_u64_t record_id) {
      m_impl.test_set_key(slot, data, data_size, flags, record_id);
    }

    // Clears the page with zeroes and reinitializes it; only for testing
    virtual void test_clear_page() {
      m_impl.test_clear_page();
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

#endif /* HAM_BTREE_NODE_PROXY_H__ */
