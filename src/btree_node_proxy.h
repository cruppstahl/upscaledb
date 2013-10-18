/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifndef HAM_BTREE_NODE_PROXY_H__
#define HAM_BTREE_NODE_PROXY_H__

#include "util.h"
#include "page.h"
#include "extkeys.h"
#include "btree_node.h"
#include "blob_manager.h"
#include "duplicates.h"
#include "env_local.h"

#undef min  // avoid MSVC conflicts with std::min

namespace hamsterdb {

//
// A BtreeNodeProxy wraps a PBtreeNode structure and defines the actual
// format of the btree payload.
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

    // Returns the number of entries in the BtreeNode
    ham_u32_t get_count() const {
      return (PBtreeNode::from_page(m_page)->get_count());
    }

    // Sets the number of entries in the BtreeNode
    void set_count(ham_u32_t count) {
      ham_assert(count <= m_page->get_db()->get_btree_index()->get_maxkeys());
      PBtreeNode::from_page(m_page)->set_count(count);
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

    // Returns true if this btree node is a leaf node
    bool is_leaf() const {
      return (get_ptr_down() == 0);
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

    // Sets a key; only for testing
    virtual void test_set_key(int slot, const char *data, size_t data_size,
                    ham_u32_t flags, ham_u64_t record_id) = 0;

    // Iterates all keys, calls the |visitor| on each. Aborts if the
    // |visitor| returns false
    virtual void enumerate(BtreeVisitor &visitor) = 0;

    // Erases all extended keys, overflow areas and records that are
    // linked from this page; usually called when the Database is deleted
    // or an In-Memory Database is freed
    virtual void release() = 0;

    // Compares the keys in the two keys
    virtual int compare(const ham_key_t *lhs, const ham_key_t *rhs) const = 0;

    // Compares a public key and an internal key
    virtual int compare(const ham_key_t *lhs, int rhs) = 0;

    // Returns true if the public key and an internal key are equal
    virtual bool equals(const ham_key_t *lhs, int rhs) = 0;

    // Returns the full key at the |slot|. Also resolves extended keys
    // and respects HAM_KEY_USER_ALLOC in dest->flags. Record number keys
    // are endian-translated.
    virtual ham_status_t copy_full_key(int slot, ByteArray *arena,
                    ham_key_t *dest) const = 0;

    // Returns the full record and stores it in |dest|. The record is identified
    // by |slot| and |duplicate_index|. TINY and SMALL records are handled
    // correctly, as well as HAM_DIRECT_ACCESS.
    virtual ham_status_t copy_full_record(int slot, ByteArray *arena,
                    ham_record_t *record, ham_u32_t flags,
                    ham_u32_t duplicate_index = 0,
                    PDupeEntry *duplicate_entry = 0) const = 0;


    // Checks the integrity of the key at |slot|
    virtual ham_status_t check_integrity(int slot) const = 0;

    // Clears the header of a new node
    virtual void initialize() = 0;

    // Splits a page and moves all elements at a position >= |pivot|
    // to the |other| page. If the node is a leaf node then the pivot element
    // is also copied, otherwise it is not because it will be propagated
    // to the parent node instead (by the caller).
    virtual void split(BtreeNodeProxy *other, int pivot) = 0;

    // Returns the number of duplicates of a key at the given |slot|
    virtual ham_status_t get_duplicate_count(int slot,
                    ham_size_t *pcount) const = 0;

    // Returns the record size of a key or one of its duplicates
    virtual ham_status_t get_record_size(int slot, int duplicate_index,
                    ham_u64_t *psize) const = 0;

    // Returns true if the key at the given |slot| has duplicates
    virtual bool has_duplicates(ham_u32_t slot) const = 0;

    // Returns the flags of the key at the given |slot|; only for testing!
    virtual ham_u32_t test_get_flags(int slot) const = 0;

    // Returns the record id of the key at the given |slot|
    virtual ham_u64_t get_record_id(int slot) const = 0;

    // Sets the record id of the key at the given |slot|
    virtual void set_record_id(int slot, ham_u64_t id) = 0;

    // Searches the node for the key, and returns the slot of this key
    virtual int get_slot(ham_key_t *key, int *pcmp = 0) = 0;

    // Searches the node for the key, and returns the slot of this key
    virtual int get_slot(BtreeNodeProxy *key_node, int key_slot,
                   int *pcmp = 0) = 0;

    // Merges all keys from the |other| node to this node
    virtual void merge_from(BtreeNodeProxy *other) = 0;

    // Append |count| keys from the |other| node to this node
    virtual void shift_from_right(BtreeNodeProxy *other, int count) = 0;

    // Prepends |count| keys from this node to the |other| node
    virtual void shift_to_right(BtreeNodeProxy *other, int slot, int count) = 0;

    // High level function to insert a new key
    virtual ham_status_t insert(ham_u32_t slot, const ham_key_t *key,
                    BlobManager *blob_manager) = 0;

    // High level function to create space for a new key
    virtual void make_space(ham_u32_t slot) = 0;

    // High level function to remove an existing key
    virtual void remove(ham_u32_t slot) = 0;

    // High-level function to set a new record
    //
    // flags can be
    // - HAM_OVERWRITE
    // - HAM_DUPLICATE_INSERT_BEFORE
    // - HAM_DUPLICATE_INSERT_AFTER
    // - HAM_DUPLICATE_INSERT_FIRST
    // - HAM_DUPLICATE_INSERT_LAST
    // - HAM_DUPLICATE
    //
    // a previously existing blob will be deleted if necessary
    virtual ham_status_t set_record(int slot, Transaction *txn,
                    ham_record_t *record, ham_size_t position, ham_u32_t flags,
                    ham_size_t *new_position) = 0;

    // Returns true if a node requires a split to insert |key|
    virtual bool requires_split(const ham_key_t *key) const = 0;

    // Replaces the |dest|-key with the key in |slot|
    virtual ham_status_t replace_key(int slot, BtreeNodeProxy *dest,
                    int dest_slot, bool dest_is_internal,
                    BlobManager *blob_manager) const = 0;

    // Removes the record (or the duplicate of it, if |duplicate_id| is > 0).
    // If |all_duplicates| is set then all duplicates of this key are deleted.
    // |has_duplicates_left| will return true if there are more duplicates left
    // after the current one was deleted.
    virtual ham_status_t remove_record(int slot, int duplicate_id,
                    bool all_duplicates, bool *has_duplicates_left) = 0;

    // Prints the node to stdout (for debugging)
    virtual void print() const = 0;

  protected:
    Page *m_page;
};

struct CallbackCompare
{
  CallbackCompare(LocalDatabase *db)
    : m_db(db) {
  }

  int operator()(const void *lhs_data, ham_size_t lhs_size,
          const void *rhs_data, ham_size_t rhs_size) const {
    return (m_db->get_compare_func()((::ham_db_t *)m_db, (ham_u8_t *)lhs_data,
                            lhs_size, (ham_u8_t *)rhs_data, rhs_size));
  }

  LocalDatabase *m_db;
};

// Compare object for record number keys (includes endian conversion)
struct RecordNumberCompare
{
  RecordNumberCompare(LocalDatabase *) {
  }

  int operator()(const void *lhs_data, ham_size_t lhs_size,
          const void *rhs_data, ham_size_t rhs_size) const {
    ham_assert(lhs_size == rhs_size);
    ham_assert(lhs_size == sizeof(ham_u64_t));
    ham_u64_t l = ham_db2h64(*(ham_u64_t *)lhs_data);
    ham_u64_t r = ham_db2h64(*(ham_u64_t *)rhs_data);
    return (l < r ? -1 : (l > r ? +1 : 0));
  }
};

// Compare object for numeric keys (no endian conversion)
template<typename T>
struct NumericCompare
{
  NumericCompare(LocalDatabase *) {
  }

  int operator()(const void *lhs_data, ham_size_t lhs_size,
          const void *rhs_data, ham_size_t rhs_size) const {
    ham_assert(lhs_size == rhs_size);
    ham_assert(lhs_size == sizeof(T));
    T l = *(T *)lhs_data;
    T r = *(T *)rhs_data;
    return (l < r ? -1 : (l > r ? +1 : 0));
  }
};

// default comparison function for two keys, implemented with memcmp
// both keys have the same size!
struct FixedSizeCompare
{
  FixedSizeCompare(LocalDatabase *) {
  }

  int operator()(const void *lhs_data, ham_size_t lhs_size,
          const void *rhs_data, ham_size_t rhs_size) const {
    ham_assert(lhs_size == rhs_size);
    return (::memcmp(lhs_data, rhs_data, lhs_size));
  }
};

// default comparison function for two keys, implemented with memcmp
// both keys can have different sizes! shorter strings are treated as
// "greater"
struct VariableSizeCompare
{
  VariableSizeCompare(LocalDatabase *) {
  }

  int operator()(const void *lhs_data, ham_size_t lhs_size,
          const void *rhs_data, ham_size_t rhs_size) const {
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

template<class NodeLayout, class Comparator>
class BtreeNodeProxyImpl : public BtreeNodeProxy
{
  typedef BtreeNodeProxyImpl<NodeLayout, Comparator> ClassType;

  public:
    typedef typename NodeLayout::Iterator Iterator;

    // Constructor
    BtreeNodeProxyImpl(Page *page)
      : BtreeNodeProxy(page), m_layout(page) {
    }

    // Sets a key; only for testing
    virtual void test_set_key(int slot, const char *data, size_t data_size,
                    ham_u32_t flags, ham_u64_t record_id) {
      typename NodeLayout::Iterator it = m_layout.at(slot);
      it->set_record_id(record_id);
      it->set_flags(flags);
      it->set_size(data_size);
      it->set_key(data, data_size);
    }

    // Retrieves the key at the specific |slot|
    virtual void enumerate(BtreeVisitor &visitor) {
      typename NodeLayout::Iterator it = m_layout.begin();
      for (ham_size_t i = 0; i < get_count(); i++, it = m_layout.next(it)) {
        if (!visitor(this, it->get_key(), it->get_flags(), it->get_size(),
                                it->get_record_id()))
          break;
      }
    }

    // Erases all extended keys, overflow areas and records that are
    // linked from this page; usually called when the Database is deleted
    // or an In-Memory Database is closed
    virtual void release() {
      typename NodeLayout::Iterator it = m_layout.begin();
      for (ham_size_t i = 0; i < get_count(); i++, it = m_layout.next(it)) {
        m_layout.release_key(it);

        if (!is_leaf()
           || (it->get_flags() & (BtreeKey::kBlobSizeTiny
                  | BtreeKey::kBlobSizeSmall
                  | BtreeKey::kBlobSizeEmpty)))
          continue;

        /* if we're in the leaf page, delete the associated record */
        remove_record(i, 0, true, 0);
      }
    }

    // Compares two internal keys using the supplied comparator
    virtual int compare(const ham_key_t *lhs, const ham_key_t *rhs) const {
      Comparator cmp(m_page->get_db());
      return (cmp(lhs->data, lhs->size, rhs->data, rhs->size));
    }

    // Compares a public key and an internal key
    virtual int compare(const ham_key_t *lhs, int rhs) {
      Comparator cmp(m_page->get_db());
      typename NodeLayout::Iterator it = m_layout.at(rhs);
      return (m_layout.compare(lhs, it, cmp));
    }

    // Returns true if the public key and an internal key are equal
    virtual bool equals(const ham_key_t *lhs, int rhs) {
      typename NodeLayout::Iterator it = m_layout.at(rhs);
      if (it->get_size() != lhs->size)
        return (false);
      return (0 == compare(lhs, rhs));
    }

    // Returns the full key at the |slot|. Also resolves extended keys
    // and respects HAM_KEY_USER_ALLOC in dest->flags. Record number keys
    // are endian-translated.
    virtual ham_status_t copy_full_key(int slot, ByteArray *arena,
                    ham_key_t *dest) const {
      typename NodeLayout::ConstIterator it = m_layout.at(slot);
      if (dest->flags & HAM_KEY_USER_ALLOC) {
        arena->assign(dest->data, dest->size);
        arena->disown();
      }
      return (m_layout.copy_full_key(it, arena, dest));
    }

    // Returns the full record and stores it in |dest|. The record is identified
    // by |slot| and |duplicate_index|. TINY and SMALL records are handled
    // correctly, as well as HAM_DIRECT_ACCESS.
    virtual ham_status_t copy_full_record(int slot, ByteArray *arena,
                    ham_record_t *record, ham_u32_t flags,
                    ham_u32_t duplicate_index = 0,
                    PDupeEntry *duplicate_entry = 0) const {
      LocalDatabase *db = m_page->get_db();
      LocalEnvironment *env = db->get_local_env();
      ham_u64_t *ridptr = 0;

      typename NodeLayout::Iterator it = m_layout.at(slot);

      record->_intflags = it->get_flags();
      record->_rid = it->get_record_id();

      bool noblob = false;
      ham_size_t blobsize;

      /* if this key has duplicates: fetch the duplicate entry */
      if (record->_intflags & BtreeKey::kDuplicates) {
        PDupeEntry tmp;
        if (!duplicate_entry)
          duplicate_entry = &tmp;
        ham_status_t st = env->get_duplicate_manager()->get(record->_rid,
                        duplicate_index, duplicate_entry);
        if (st)
          return st;
        record->_intflags = dupe_entry_get_flags(duplicate_entry);
        record->_rid = dupe_entry_get_rid(duplicate_entry);
        /* ridptr must not point to entry._rid because it's on the stack! */
        ridptr = &record->_rid;
      }
      else {
        ridptr = it->get_rawptr();
      }

      /*
       * if the record size is small enough there's
       * no blob available, but the data is stored compressed in the record's
       * offset.
       */
      if (record->_intflags & BtreeKey::kBlobSizeTiny) {
        /* the highest byte of the record id is the size of the blob */
        char *p = (char *)ridptr;
        blobsize = p[sizeof(ham_u64_t) - 1];
        noblob = true;
      }
      else if (record->_intflags & BtreeKey::kBlobSizeSmall) {
        /* record size is sizeof(ham_u64_t) */
        blobsize = sizeof(ham_u64_t);
        noblob = true;
      }
      else if (record->_intflags & BtreeKey::kBlobSizeEmpty) {
        /* record size is 0 */
        blobsize = 0;
        noblob = true;
      }
      else {
        /* set to a dummy value, so the third if-branch is executed */
        blobsize = 0xffffffff;
      }

      if (noblob && blobsize == 0) {
        record->size = 0;
        record->data = 0;
      }
      else if (noblob && blobsize > 0) {
        if (flags & HAM_PARTIAL) {
          ham_trace(("flag HAM_PARTIAL is not allowed if record->size <= 8"));
          return (HAM_INV_PARAMETER);
        }

        if (!(record->flags & HAM_RECORD_USER_ALLOC)
            && (flags & HAM_DIRECT_ACCESS)) {
          record->data = ridptr;
          record->size = blobsize;
        }
        else {
          if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
            arena->resize(blobsize);
            record->data = arena->get_ptr();
          }
          memcpy(record->data, ridptr, blobsize);
          record->size = blobsize;
        }
      }
      else if (!noblob && blobsize != 0)
        return (env->get_blob_manager()->read(db, record->_rid, record,
                                flags, arena));

      return (HAM_SUCCESS);
    }

    // Checks the integrity of the key at |slot|
    virtual ham_status_t check_integrity(int slot) const {
      typename NodeLayout::Iterator it = m_layout.at(slot);
      // internal nodes: only allowed flag is kExtendedKey
      if ((it->get_flags() != 0
          && it->get_flags() != BtreeKey::kExtended)
          && !is_leaf()) {
        ham_log(("integrity check failed in page 0x%llx: item #0 "
                "has flags, but it's not a leaf page", m_page->get_address()));
        return (HAM_INTEGRITY_VIOLATED);
      }
      return (m_layout.check_integrity(it,
                  m_page->get_db()->get_local_env()->get_blob_manager()));
    }

    // Clears the header of a new node
    virtual void initialize() {
      m_layout.initialize();
    }

    // Splits the node
    virtual void split(BtreeNodeProxy *other_node, int pivot) {
      ClassType *other = dynamic_cast<ClassType *>(other_node);

      m_layout.split(&other->m_layout, pivot);

      ham_u32_t count = get_count();
      set_count(pivot);

      if (is_leaf())
        other->set_count(count - pivot);
      else
        other->set_count(count - pivot - 1);
    }

    // Returns the number of duplicates of a key at the given |slot|
    virtual ham_status_t get_duplicate_count(int slot,
                    ham_size_t *pcount) const {
      typename NodeLayout::Iterator it = m_layout.at(slot);
      if (!(it->get_flags() & BtreeKey::kDuplicates)) {
        *pcount = 1;
        return (0);
      }
      LocalDatabase *db = m_page->get_db();
      LocalEnvironment *env = db->get_local_env();
      return (env->get_duplicate_manager()->get_count(it->get_record_id(),
                              pcount, 0));
    }

    // Returns the record size of a key or one of its duplicates
    virtual ham_status_t get_record_size(int slot, int duplicate_index,
                    ham_u64_t *psize) const {
      LocalDatabase *db = m_page->get_db();
      LocalEnvironment *env = db->get_local_env();
      ham_u32_t keyflags = 0;
      ham_u64_t *ridptr = 0;
      ham_u64_t rid = 0;
      PDupeEntry dupeentry;

      typename NodeLayout::Iterator it = m_layout.at(slot);

      if (it->get_flags() & BtreeKey::kDuplicates) {
        ham_status_t st = env->get_duplicate_manager()->get(it->get_record_id(),
                        duplicate_index, &dupeentry);
        if (st)
          return (st);
        keyflags = dupe_entry_get_flags(&dupeentry);
        ridptr = &dupeentry._rid;
        rid = dupeentry._rid;
      }
      else {
        keyflags = it->get_flags();
        ridptr = it->get_rawptr();
        rid = it->get_record_id();
      }

      if (keyflags & BtreeKey::kBlobSizeTiny) {
        // the highest byte of the record id is the size of the blob
        char *p = (char *)ridptr;
        *psize = p[sizeof(ham_u64_t) - 1];
        return (0);
      }
      else if (keyflags & BtreeKey::kBlobSizeSmall) {
        // record size is sizeof(ham_u64_t)
        *psize = sizeof(ham_u64_t);
        return (0);
      }
      else if (keyflags & BtreeKey::kBlobSizeEmpty) {
        // record size is 0
        *psize = 0;
        return (0);
      }
      return (env->get_blob_manager()->get_datasize(db, rid, psize));
    }

    // Returns true if the key at the given |slot| has duplicates
    virtual bool has_duplicates(ham_u32_t slot) const {
      typename NodeLayout::ConstIterator it = m_layout.at(slot);
      return ((bool)(it->get_flags() & BtreeKey::kDuplicates));
    }

    // Returns the flags of the key at the given |slot|; only for testing!
    virtual ham_u32_t test_get_flags(int slot) const {
      typename NodeLayout::Iterator it = m_layout.at(slot);
      return (it->get_flags());
    }

    virtual ham_u64_t get_record_id(int slot) const {
      typename NodeLayout::Iterator it = m_layout.at(slot);
      return (it->get_record_id());
    }

    virtual void set_record_id(int slot, ham_u64_t id) {
      typename NodeLayout::Iterator it = m_layout.at(slot);
      it->set_record_id(id);
    }

    // Searches the node for the key, and returns the slot of this key
    virtual int get_slot(ham_key_t *key, int *pcmp = 0) {
      int i, l = 1, r = get_count() - 1;
      int ret = 0, last = get_count() + 1;
      int cmp = -1;
      Comparator comparator(m_page->get_db());

      ham_assert(get_count() > 0);

      /* only one element in this node? */
      if (r == 0) {
        cmp = m_layout.compare(key, m_layout.at(0), comparator);
        if (pcmp)
          *pcmp = cmp;
        return (cmp < 0 ? -1 : 0);
      }

      for (;;) {
        /* get the median item; if it's identical with the "last" item,
         * we've found the slot */
        i = (l + r) / 2;

        if (i == last) {
          ham_assert(i >= 0);
          ham_assert(i < (int)get_count());
          cmp = 1;
          ret = i;
          break;
        }

        /* compare it against the key */
        cmp = m_layout.compare(key, m_layout.at(i), comparator);

        /* found it? */
        if (cmp == 0) {
          ret = i;
          break;
        }

        /* if the key is bigger than the item: search "to the left" */
        if (cmp < 0) {
          if (r == 0) {
            ham_assert(i == 0);
            ret = -1;
            break;
          }
          r = i - 1;
        }
        else {
          last = i;
          l = i + 1;
        }
      }

      if (pcmp)
        *pcmp = cmp;
      return (ret);
    }

    // Searches the node for the key, and returns the slot of this key
    virtual int get_slot(BtreeNodeProxy *key_node, int key_slot,
                   int *pcmp = 0) {
      ClassType *other = dynamic_cast<ClassType *>(key_node);
      typename NodeLayout::Iterator it = other->m_layout.at(key_slot);

      ham_key_t key = {0};
      key._flags = it->get_flags();
      key.data   = it->get_key();
      key.size   = it->get_size();

      return (get_slot(&key, pcmp));
    }

    // Merges all keys from the |other| node into this node
    virtual void merge_from(BtreeNodeProxy *other_node) {
      ClassType *other = dynamic_cast<ClassType *>(other_node);
      m_layout.merge_from(&other->m_layout);

      set_count(get_count() + other->get_count());
      other->set_count(0);
    }

    // Shifts |count| keys from the |other| node to this node
    virtual void shift_from_right(BtreeNodeProxy *other_node, int count) {
      ClassType *other = dynamic_cast<ClassType *>(other_node);
      m_layout.shift_from_right(&other->m_layout, count);

      set_count(get_count() + count);
      other->set_count(other->get_count() - count);
    }

    // Prepends |count| keys from this node to the |other| node
    virtual void shift_to_right(BtreeNodeProxy *other_node,
                    int slot, int count) {
      ClassType *other = dynamic_cast<ClassType *>(other_node);
      m_layout.shift_to_right(&other->m_layout, slot, count);

      set_count(get_count() - count);
      other->set_count(other->get_count() + count);
    }

    // High level function to insert a new key
    virtual ham_status_t insert(ham_u32_t slot, const ham_key_t *key,
                    BlobManager *blob_manager) {
      LocalDatabase *db = m_page->get_db();
      ham_size_t keysize = db->get_keysize();

      typename NodeLayout::Iterator it = m_layout.insert(slot, key);

      /* extended key: allocate a blob and store it in the key */
      if (key->size > keysize) {
        ham_u64_t blobid;
        ham_record_t rec = {0};
        ham_u8_t *data_ptr = (ham_u8_t *)key->data;
        rec.data = data_ptr + (keysize - sizeof(ham_u64_t));
        rec.size = key->size - (keysize - sizeof(ham_u64_t));

        ham_status_t st;
        if ((st = blob_manager->allocate(db, &rec, 0, &blobid)))
          return (st);

        if (db->get_extkey_cache())
          db->get_extkey_cache()->insert(blobid, key->size,
                            (ham_u8_t *)key->data);

        ham_assert(blobid != 0);
        it->set_extended_rid(db, blobid);
      }
      return (0);
    }

    // High level function to create space for a new key
    virtual void make_space(ham_u32_t slot) {
      m_layout.make_space(slot);
    }

    virtual void remove(ham_u32_t slot) {
      m_layout.remove(slot);
      set_count(get_count() - 1);
    }

    virtual ham_status_t set_record(int slot, Transaction *txn,
                    ham_record_t *record, ham_size_t position, ham_u32_t flags,
                    ham_size_t *new_position) {
      typename NodeLayout::Iterator it = m_layout.at(slot);

      ham_status_t st;
      LocalDatabase *db = m_page->get_db();
      LocalEnvironment *env = db->get_local_env();
      ham_u64_t rid = 0;
      ham_u64_t ptr = it->get_record_id();
      ham_u8_t oldflags = it->get_flags();

      it->set_flags(oldflags & ~(BtreeKey::kBlobSizeSmall
                              | BtreeKey::kBlobSizeTiny
                              | BtreeKey::kBlobSizeEmpty));

      /* no existing key, just create a new key (but not a duplicate)? */
      if (!ptr && !(oldflags & (BtreeKey::kBlobSizeSmall
                    | BtreeKey::kBlobSizeTiny
                    | BtreeKey::kBlobSizeEmpty))) {
        if (record->size <= sizeof(ham_u64_t)) {
          if (record->data)
            memcpy(&rid, record->data, record->size);
          if (record->size == 0)
            it->set_flags(it->get_flags() | BtreeKey::kBlobSizeEmpty);
          else if (record->size < sizeof(ham_u64_t)) {
            char *p = (char *)&rid;
            p[sizeof(ham_u64_t) - 1] = (char)record->size;
            it->set_flags(it->get_flags() | BtreeKey::kBlobSizeTiny);
          }
          else
            it->set_flags(it->get_flags() | BtreeKey::kBlobSizeSmall);
          it->set_record_id(rid);
        }
        else {
          st = env->get_blob_manager()->allocate(db, record, flags, &rid);
          if (st)
            return (st);
          it->set_record_id(rid);
        }
      }
      else if (!(oldflags & BtreeKey::kDuplicates)
          && record->size > sizeof(ham_u64_t)
          && !(flags & (HAM_DUPLICATE | HAM_DUPLICATE_INSERT_BEFORE
                    | HAM_DUPLICATE_INSERT_AFTER | HAM_DUPLICATE_INSERT_FIRST
                    | HAM_DUPLICATE_INSERT_LAST))) {
        /*
         * an existing key, which is overwritten with a big record
         * Note that the case where old record is EMPTY (!ptr) or
         * SMALL (size = 8, but content = 00000000 --> !ptr) are caught here
         * and in the next branch, as they should.
         */
        if (oldflags & (BtreeKey::kBlobSizeSmall
                                | BtreeKey::kBlobSizeTiny
                                | BtreeKey::kBlobSizeEmpty)) {
          rid = 0;
          st = env->get_blob_manager()->allocate(db, record, flags, &rid);
          if (st)
            return (st);
          if (rid)
            it->set_record_id(rid);
        }
        else {
          st = env->get_blob_manager()->overwrite(db, ptr, record, flags, &rid);
          if (st)
            return (st);
          it->set_record_id(rid);
        }
      }
      else if (!(oldflags & BtreeKey::kDuplicates)
              && record->size <= sizeof(ham_u64_t)
              && !(flags & (HAM_DUPLICATE
                      | HAM_DUPLICATE_INSERT_BEFORE
                      | HAM_DUPLICATE_INSERT_AFTER
                      | HAM_DUPLICATE_INSERT_FIRST
                      | HAM_DUPLICATE_INSERT_LAST))) {
        /* an existing key which is overwritten with a small record */
        if (!(oldflags & (BtreeKey::kBlobSizeSmall
                        | BtreeKey::kBlobSizeTiny
                        | BtreeKey::kBlobSizeEmpty))) {
          st = env->get_blob_manager()->free(db, ptr, 0);
          if (st)
            return (st);
        }
        if (record->data)
          memcpy(&rid, record->data, record->size);
        if (record->size == 0)
          it->set_flags(it->get_flags() | BtreeKey::kBlobSizeEmpty);
        else if (record->size < sizeof(ham_u64_t)) {
          char *p = (char *)&rid;
          p[sizeof(ham_u64_t) - 1] = (char)record->size;
          it->set_flags(it->get_flags() | BtreeKey::kBlobSizeTiny);
        }
        else
          it->set_flags(it->get_flags() | BtreeKey::kBlobSizeSmall);
        it->set_record_id(rid);
      }
      else {
        /*
         * a duplicate of an existing key - always insert it at the end of
         * the duplicate list (unless the DUPLICATE flags say otherwise OR
         * when we have a duplicate-record comparison function for
         * ordered insertion of duplicate records)
         *
         * create a duplicate list, if it does not yet exist
         */
        PDupeEntry entries[2];
        int i = 0;
        ham_assert((flags & (HAM_DUPLICATE | HAM_DUPLICATE_INSERT_BEFORE
                    | HAM_DUPLICATE_INSERT_AFTER | HAM_DUPLICATE_INSERT_FIRST
                    | HAM_DUPLICATE_INSERT_LAST | HAM_OVERWRITE)));
        memset(entries, 0, sizeof(entries));
        if (!(oldflags & BtreeKey::kDuplicates)) {
          ham_assert((flags & (HAM_DUPLICATE | HAM_DUPLICATE_INSERT_BEFORE
                      | HAM_DUPLICATE_INSERT_AFTER | HAM_DUPLICATE_INSERT_FIRST
                      | HAM_DUPLICATE_INSERT_LAST)));
          dupe_entry_set_flags(&entries[i],
                    oldflags & (BtreeKey::kBlobSizeSmall
                        | BtreeKey::kBlobSizeTiny
                        | BtreeKey::kBlobSizeEmpty));
          dupe_entry_set_rid(&entries[i], ptr);
          i++;
        }
        if (record->size <= sizeof(ham_u64_t)) {
          if (record->data)
            memcpy(&rid, record->data, record->size);
          if (record->size == 0)
            dupe_entry_set_flags(&entries[i], BtreeKey::kBlobSizeEmpty);
          else if (record->size < sizeof(ham_u64_t)) {
            char *p = (char *)&rid;
            p[sizeof(ham_u64_t) - 1] = (char)record->size;
            dupe_entry_set_flags(&entries[i], BtreeKey::kBlobSizeTiny);
          }
          else
            dupe_entry_set_flags(&entries[i], BtreeKey::kBlobSizeSmall);
          dupe_entry_set_rid(&entries[i], rid);
        }
        else {
          st = env->get_blob_manager()->allocate(db, record, flags, &rid);
          if (st)
            return (st);
          dupe_entry_set_flags(&entries[i], 0);
          dupe_entry_set_rid(&entries[i], rid);
        }
        i++;

        rid = 0;
        st = env->get_duplicate_manager()->insert(db, txn,
                (i == 2 ? 0 : ptr), record, position,
                flags, &entries[0], i, &rid, new_position);
        if (st) {
          /* don't leak memory through the blob allocation above */
          if (record->size > sizeof(ham_u64_t)) {
            (void)env->get_blob_manager()->free(db,
                    dupe_entry_get_rid(&entries[i - 1]), 0);
          }
          return (st);
        }

        it->set_flags(it->get_flags() | BtreeKey::kDuplicates);
        if (rid)
          it->set_record_id(rid);
      }

      return (0);
    }

    virtual bool requires_split(const ham_key_t *key) const {
      ham_size_t maxkeys = m_page->get_db()->get_btree_index()->get_maxkeys();
      return (get_count() >= maxkeys);
    }

    virtual ham_status_t replace_key(int slot, BtreeNodeProxy *dest_node,
                    int dest_slot, bool dest_is_internal,
                    BlobManager *blob_manager) const {
      ham_status_t st = 0;
      LocalDatabase *db = m_page->get_db();

      ClassType *other = dynamic_cast<ClassType *>(dest_node);
      typename NodeLayout::ConstIterator src = m_layout.at(slot);
      typename NodeLayout::Iterator dest = other->m_layout.at(dest_slot);

      // release the extended blob of the destination key (if there is one)
      if (dest_slot < (int)other->get_count())
        other->m_layout.release_key(dest);

      dest->set_flags(src->get_flags());

      /*
       * internal keys are not allowed to have blob-flags, because only the
       * leaf-node can manage the blob. Therefore we have to disable those
       * flags if we modify an internal key.
       */
      if (dest_is_internal)
        dest->set_flags(dest->get_flags() &
                ~(BtreeKey::kBlobSizeTiny
                    | BtreeKey::kBlobSizeSmall
                    | BtreeKey::kBlobSizeEmpty
                    | BtreeKey::kDuplicates));

      /*
       * if this key is extended, we copy the extended blob; otherwise, we'd
       * have to add reference counting to the blob, because two keys are now
       * using the same blobid. this would be too complicated.
       */
      if (src->get_flags() & BtreeKey::kExtended) {
        dest->set_key(src->get_key(),
                        std::min(db->get_keysize(), src->get_size()));

        ham_record_t record = {0};
        ByteArray arena;
        ham_u64_t rhsblobid = src->get_extended_rid(db);
        st = blob_manager->read(db, rhsblobid, &record, 0, &arena);
        if (st)
          return (st);

        ham_u64_t lhsblobid;
        st = blob_manager->allocate(db, &record, 0, &lhsblobid);
        if (st)
          return (st);
        dest->set_extended_rid(db, lhsblobid);
      }
      else
        dest->set_key(src->get_key(), src->get_size());

      dest->set_size(src->get_size());
      return (0);
    }

    virtual ham_status_t remove_record(int slot, int duplicate_id,
                    bool all_duplicates, bool *has_duplicates_left) {
      typename NodeLayout::Iterator it = m_layout.at(slot);

      ham_u64_t rid;
      ham_status_t st;
      LocalDatabase *db = m_page->get_db();

      /* if the record is > 8 bytes then it needs to be freed explicitly */
      if (!(it->get_flags() & (BtreeKey::kBlobSizeSmall
                                      | BtreeKey::kBlobSizeTiny
                                      | BtreeKey::kBlobSizeEmpty))) {
        if (it->get_flags() & BtreeKey::kDuplicates) {
          /* delete one (or all) duplicates */
          st = db->get_local_env()->get_duplicate_manager()->erase(db,
                            it->get_record_id(), duplicate_id,
                            all_duplicates, &rid);
          if (st)
            return (st);
          if (all_duplicates) {
            it->set_flags(it->get_flags() & ~BtreeKey::kDuplicates);
            it->set_record_id(0);
          }
          else {
            it->set_record_id(rid);
            if (!rid) /* rid == 0: the last duplicate was deleted */
              it->set_flags(0);
          }
        }
        else {
          /* delete the blob */
          st = db->get_local_env()->get_blob_manager()->free(db,
                          it->get_record_id(), 0);
          if (st)
            return (st);
          it->set_record_id(0);
        }
      }
      /* otherwise just reset the blob flags of the key and set the record
       * pointer to 0 */
      else {
        it->set_flags(it->get_flags() & ~(BtreeKey::kBlobSizeSmall
                                | BtreeKey::kBlobSizeTiny
                                | BtreeKey::kBlobSizeEmpty
                                | BtreeKey::kDuplicates));
        it->set_record_id(0);
      }

      if (has_duplicates_left)
        *has_duplicates_left = (it->get_flags() & BtreeKey::kDuplicates
                                && it->get_record_id() != 0);
      return (0);
    }

    virtual void print() const {
      printf("page %llu: %u elements (left: %llu, right: %llu, "
              "ptr_down: %llu)\n",
              (unsigned long long)m_page->get_address(), get_count(),
              (unsigned long long)get_left(), (unsigned long long)get_right(),
              (unsigned long long)get_ptr_down());
      typename NodeLayout::Iterator it = m_layout.begin();
      for (ham_size_t i = 0; i < get_count(); i++, it = m_layout.next(it)) {
        printf("    %08d -> %08lld\n", *(int *)it->get_key(),
                (unsigned long long)it->get_record_id());
      }
    }

  private:
    NodeLayout m_layout;
};

} // namespace hamsterdb

#endif /* HAM_BTREE_NODE_PROXY_H__ */
