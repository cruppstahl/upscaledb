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

#ifndef HAM_BTREE_NODE_PAX_H__
#define HAM_BTREE_NODE_PAX_H__

#include "util.h"
#include "page.h"
#include "btree_node.h"
#include "blob_manager.h"
#include "duplicates.h"
#include "env_local.h"

namespace hamsterdb {

template<typename KeyList>
class PaxNodeLayout;

/*
 * A helper class to access (flags/key data/record data) values in a
 * BtreeNode with PAX-style layout
 */
template<typename KeyList>
struct PaxIterator
{
  public:
    // Constructor
    PaxIterator(PaxNodeLayout<KeyList> *node, ham_size_t slot)
      : m_node(node), m_slot(slot) {
    }

    // Constructor
    PaxIterator(const PaxNodeLayout<KeyList> *node, ham_size_t slot)
      : m_node((PaxNodeLayout<KeyList> *)node), m_slot(slot) {
    }

    // Returns the record id
    //
    // !!!
    // if TINY or SMALL is set, the key is actually a char*-pointer;
    // in this case, we must not use endian-conversion!
    ham_u64_t get_record_id() {
      ham_u8_t flags = get_flags();
      return (((flags & BtreeKey::kBlobSizeTiny)
                              || (flags & BtreeKey::kBlobSizeSmall))
              ? *get_record_ptr()
              : ham_db2h_offset(*get_record_ptr()));
    }

    // Returns the record id
    ham_u64_t get_record_id() const {
      ham_u8_t flags = get_flags();
      return (((flags & BtreeKey::kBlobSizeTiny)
                              || (flags & BtreeKey::kBlobSizeSmall))
              ? *get_record_ptr()
              : ham_db2h_offset(*get_record_ptr()));
    }

    // Same as above, but without endian conversion
    ham_u64_t *get_rawptr() {
      return (get_record_ptr());
    }

    // Same as above, but without endian conversion
    const ham_u64_t *get_rawptr() const {
      return (get_record_ptr());
    }

    // Sets the record id
    //
    // !!! same problems as with get_record_id():
    // if TINY or SMALL is set, the key is actually a char*-pointer;
    // in this case, we must not use endian-conversion
    void set_record_id(ham_u64_t ptr) {
      ham_u8_t flags = get_flags();
      set_record_ptr((((flags & BtreeKey::kBlobSizeTiny)
                              || (flags & BtreeKey::kBlobSizeSmall))
              ? ptr
              : ham_h2db_offset(ptr)));
    }

    // Returns the size of a btree key
    ham_u16_t get_size() const {
      return (m_node->get_size());
    }

    // Sets the size of a btree key
    void set_size(ham_u16_t size) {
      ham_assert(size == get_size());
    }

    // Returns the (persisted) flags of a key
    ham_u8_t get_flags() const {
      return (m_node->get_flags(m_slot));
    }

    // Sets the flags of a key
    //
    // Note that the ham_find/ham_cursor_find/ham_cursor_find_ex flags must be
    // defined such that those can peacefully co-exist with these; that's why
    // those public flags start at the value 0x1000 (4096).
    void set_flags(ham_u8_t flags) {
      m_node->set_flags(m_slot, flags);
    }

    // Returns a pointer to the key data
    ham_u8_t *get_key() {
      return (m_node->get_key_ptr(m_slot));
    }

    // Returns a pointer to the key data
    ham_u8_t *get_key() const {
      return (m_node->get_key_ptr(m_slot));
    }

    // Overwrites the key data
    void set_key(const void *ptr, ham_size_t size) {
      ham_assert(size == get_size());
      m_node->set_key(m_slot, ptr, size);
    }

    // Returns a pointer to the record ID
    ham_u64_t *get_record_ptr() {
      return (m_node->get_record_ptr(m_slot));
    }

    // Returns a pointer to the record ID
    ham_u64_t *get_record_ptr() const {
      return (m_node->get_record_ptr(m_slot));
    }

    // Sets a record ID
    void set_record_ptr(ham_u64_t rid) {
      m_node->set_record_ptr(m_slot, rid);
    }
  
    // Returns the record address of an extended key overflow area
    ham_u64_t get_extended_rid(LocalDatabase *db) const {
      // nop, but required to compile
      ham_assert(!"shouldn't be here");
      return (0);
    }

    // Sets the record address of an extended key overflow area
    void set_extended_rid(LocalDatabase *db, ham_u64_t rid) {
      // nop, but required to compile
      ham_assert(!"shouldn't be here");
    }

    // Moves this Iterator to the next key
    PaxIterator<KeyList> next() {
      return (PaxIterator<KeyList>(m_node, m_slot + 1));
    }

    // Moves this Iterator to the next key
    PaxIterator<KeyList> next() const {
      return (PaxIterator<KeyList>(m_node, m_slot + 1));
    }

    // Allows use of operator-> in the caller
    PaxIterator<KeyList> *operator->() {
      return (this);
    }

    // Allows use of operator-> in the caller
    const PaxIterator<KeyList> *operator->() const {
      return (this);
    }

  private:
    // The node of this iterator
    PaxNodeLayout<KeyList> *m_node;

    // The current slot in the node
    ham_size_t m_slot;
};

//
// A template class managing an array of POD types
//
template<typename T>
class PodKeyList
{
  public:
    PodKeyList(LocalDatabase *db, ham_u8_t *data)
      : m_data((T *)data) {
    }

    ham_size_t get_key_size() const {
      return (sizeof(T));
    }

    ham_u8_t *get_key_ptr(int slot) {
      return ((ham_u8_t *)&m_data[slot]);
    }

    ham_u8_t *get_key_ptr(int slot) const {
      return ((ham_u8_t *)&m_data[slot]);
    }

    void set_key(int slot, const void *ptr, ham_size_t size) {
      ham_assert(size == get_key_size());
      m_data[slot] = *(T *)ptr;
    }

  private:
    T *m_data;
};

//
// A template class managing an array of fixed length bytes
//
class BinaryKeyList
{
  public:
    BinaryKeyList(LocalDatabase *db, ham_u8_t *data)
      : m_data(data) {
      m_key_size = db->get_keysize();
      ham_assert(m_key_size != 0);
    }

    ham_size_t get_key_size() const {
      return (m_key_size);
    }

    ham_u8_t *get_key_ptr(int slot) {
      return (&m_data[slot * m_key_size]);
    }

    ham_u8_t *get_key_ptr(int slot) const {
      return (&m_data[slot * m_key_size]);
    }

    void set_key(int slot, const void *ptr, ham_size_t size) {
      ham_assert(size == get_key_size());
      memcpy(&m_data[slot * m_key_size], ptr, size);
    }

  private:
    ham_u8_t *m_data;
    ham_size_t m_key_size;
};

//
// A BtreeNodeProxy layout which stores key data, key flags and
// and the record pointers in a PAX style layout.
//
template<typename KeyList>
class PaxNodeLayout
{
  public:
    typedef PaxIterator<KeyList> Iterator;
    typedef const PaxIterator<KeyList> ConstIterator;

    PaxNodeLayout(Page *page)
      : m_page(page), m_node(PBtreeNode::from_page(page)),
        m_keys(page->get_db(), m_node->get_data()) {
      ham_size_t usable_nodesize = page->get_env()->get_pagesize()
                    - PBtreeNode::get_entry_offset()
                    - Page::sizeof_persistent_header;
      ham_size_t keysize = get_system_keysize(m_keys.get_key_size());
      m_max_count = usable_nodesize / keysize;

      ham_u8_t *p = m_node->get_data();
      m_flags = &p[m_max_count * get_size()];
      m_record_ids = (ham_u64_t *)&p[m_max_count * (1 + get_size())];
    }

    // Returns the actual key size (including overhead)
    static ham_u16_t get_system_keysize(ham_size_t keysize) {
      return ((ham_u16_t)(keysize + 1 + sizeof(ham_u64_t)));
    }

    Iterator begin() {
      return (at(0));
    }

    Iterator begin() const {
      return (at(0));
    }

    Iterator at(int slot) {
      return (Iterator(this, slot));
    }

    ConstIterator at(int slot) const {
      return (ConstIterator(this, slot));
    }

    Iterator next(Iterator it) {
      return (it->next());
    }

    ConstIterator next(ConstIterator it) const {
      return (it->next());
    }

    void release_key(Iterator it) {
    }

    ham_status_t copy_full_key(ConstIterator it, ByteArray *arena,
                    ham_key_t *dest) const {
      LocalDatabase *db = m_page->get_db();

      if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
        if (!arena->resize(get_size()))
          return (HAM_OUT_OF_MEMORY);
        dest->data = arena->get_ptr();
        dest->size = get_size();
      }

      ham_assert(get_size() == db->get_keysize());
      memcpy(dest->data, it->get_key(), get_size());
      return (0);
    }

    ham_status_t check_integrity(Iterator it, BlobManager *bm) const {
      return (0);
    }

    void initialize() {
      memset(m_page->get_payload(), 0, sizeof(PBtreeNode));
    }

    template<typename Cmp>
    int compare(const ham_key_t *lhs, Iterator it, Cmp &cmp) {
      return (cmp(lhs->data, lhs->size, it->get_key(), get_size()));
    }

    void split(PaxNodeLayout *other, int pivot) {
      ham_size_t count = m_node->get_count();

      /*
       * if a leaf page is split then the pivot element must be inserted in
       * the leaf page AND in the internal node. the internal node update
       * is handled by the caller.
       *
       * in internal nodes the pivot element is only propagated to the
       * parent node. the pivot element is skipped.
       */
      if (m_node->is_leaf()) {
        memcpy(other->m_keys.get_key_ptr(0), m_keys.get_key_ptr(pivot),
                    get_size() * (count - pivot));
        memcpy(&other->m_flags[0], &m_flags[pivot],
                    count - pivot);
        memcpy(&other->m_record_ids[0], &m_record_ids[pivot],
                    sizeof(ham_u64_t) * (count - pivot));
      }
      else {
        memcpy(other->m_keys.get_key_ptr(0), m_keys.get_key_ptr(pivot + 1),
                    get_size() * (count - pivot - 1));
        memcpy(&other->m_flags[0], &m_flags[pivot + 1],
                    count - pivot - 1);
        memcpy(&other->m_record_ids[0], &m_record_ids[pivot + 1],
                    sizeof(ham_u64_t) * (count - pivot - 1));
      }
    }

    Iterator insert(ham_u32_t slot, const ham_key_t *key) {
      ham_assert(key->size == get_size());

      ham_size_t count = m_node->get_count();

      // make space for 1 additional element.
      // only store the key data; flags and record IDs are set by the caller
      if (count > slot) {
        memmove(m_keys.get_key_ptr(slot + 1), m_keys.get_key_ptr(slot),
                        get_size() * (count - slot));
        m_keys.set_key(slot, key->data, key->size);
        memmove(&m_flags[slot + 1], &m_flags[slot],
                        count - slot);
        m_flags[slot] = 0;
        memmove(&m_record_ids[slot + 1], &m_record_ids[slot],
                        sizeof(ham_u64_t) * (count - slot));
        m_record_ids[slot] = 0;
      }
      else {
        m_keys.set_key(slot, key->data, key->size);
        m_flags[slot] = 0;
        m_record_ids[slot] = 0;
      }

      return (at(slot));
    }

    void remove(ham_u32_t slot) {
      ham_size_t count = m_node->get_count();

      if (slot != count - 1) {
        memmove(m_keys.get_key_ptr(slot), m_keys.get_key_ptr(slot + 1),
                get_size() * (count - slot - 1));
        memmove(&m_flags[slot], &m_flags[slot + 1],
                count - slot - 1);
        memmove(&m_record_ids[slot], &m_record_ids[slot + 1],
                sizeof(ham_u64_t) * (count - slot - 1));
      }
    }

    void merge_from(PaxNodeLayout *other) {
      ham_size_t count = m_node->get_count();

      /* shift items from the sibling to this page */
      memcpy(m_keys.get_key_ptr(count), other->m_keys.get_key_ptr(0),
                      get_size() * other->m_node->get_count());
      memcpy(&m_flags[count], &other->m_flags[0],
                      other->m_node->get_count());
      memcpy(&m_record_ids[count], &other->m_record_ids[0],
                      sizeof(ham_u64_t) * other->m_node->get_count());
    }

    void shift_from_right(PaxNodeLayout *other, int count) {
      ham_size_t pos = m_node->get_count();

      // shift |count| elements from |other| to this page
      memcpy(m_keys.get_key_ptr(pos), other->m_keys.get_key_ptr(0),
                      get_size() * count);
      memcpy(&m_flags[pos], &other->m_flags[0],
                      count);
      memcpy(&m_record_ids[pos], &other->m_record_ids[0],
                      sizeof(ham_u64_t) * count);

      // and reduce the other page
      memmove(other->m_keys.get_key_ptr(0), other->m_keys.get_key_ptr(count),
                      get_size() * (other->m_node->get_count() - count));
      memmove(&other->m_flags[0], &other->m_flags[count],
                      (other->m_node->get_count() - count));
      memmove(&other->m_record_ids[0], &other->m_record_ids[count],
                      sizeof(ham_u64_t) * (other->m_node->get_count() - count));
    }

    void shift_to_right(PaxNodeLayout *other, int slot, int count) {
      // make room in the right sibling
      memmove(other->m_keys.get_key_ptr(count), other->m_keys.get_key_ptr(0),
                      get_size() * other->m_node->get_count());
      memmove(&other->m_flags[count], &other->m_flags[0],
                      other->m_node->get_count());
      memmove(&other->m_record_ids[count], &other->m_record_ids[0],
                      sizeof(ham_u64_t) * other->m_node->get_count());

      // shift |count| elements from this page to |other|
      memcpy(other->m_keys.get_key_ptr(0), m_keys.get_key_ptr(slot),
                      get_size() * count);
      memcpy(&other->m_flags[0], &m_flags[slot],
                      count);
      memcpy(&other->m_record_ids[0], &m_record_ids[slot],
                      sizeof(ham_u64_t) * count);
    }

  private:
    friend class PaxIterator<KeyList>;

    // Returns the BtreeKey at index |i| in this node
    //
    // note that this function does not check the boundaries (i.e. whether
    // i <= get_count(), because some functions deliberately write to
    // elements "after" get_count()
    Iterator *get_iterator(LocalDatabase *db, int i) {
      return (Iterator(this, i));
    }

    // Same as above, const flavor
    ConstIterator *get_iterator(LocalDatabase *db, int i) const {
      return (ConstIterator(this, i));
    }

    // Returns the key size
    ham_size_t get_size() const {
      return (m_keys.get_key_size());
    }

    // Returns the flags of a key
    ham_u8_t get_flags(int slot) const {
      return (m_flags[slot]);
    }

    // Sets the flags of a key
    void set_flags(int slot, ham_u8_t flags) {
      m_flags[slot] = flags;
    }

    // Returns a pointer to the key data
    ham_u8_t *get_key_ptr(int slot) const {
      return (m_keys.get_key_ptr(slot));
    }

    // Sets the key data
    void set_key(int slot, const void *ptr, ham_size_t size) {
      m_keys.set_key(slot, ptr, size);
    }

    // Returns a pointer to the record id
    ham_u64_t *get_record_ptr(int slot) {
      return (&m_record_ids[slot]);
    }

    // Returns a pointer to the record id
    const ham_u64_t *get_record_ptr(int slot) const {
      return (&m_record_ids[slot]);
    }

    // Sets the record id
    void set_record_ptr(int slot, ham_u64_t rid) {
      m_record_ids[slot] = rid;
    }

    Page *m_page;
    PBtreeNode *m_node;
    ham_size_t m_max_count;
    ham_u8_t *m_flags;
    KeyList m_keys;
    ham_u64_t *m_record_ids;
};

} // namespace hamsterdb

#endif /* HAM_BTREE_NODE_PAX_H__ */
