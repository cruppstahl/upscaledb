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

#ifndef HAM_BTREE_NODE_LEGACY_H__
#define HAM_BTREE_NODE_LEGACY_H__

#include "util.h"
#include "page.h"
#include "extkeys.h"
#include "btree_node.h"
#include "blob_manager.h"
#include "duplicates.h"
#include "env_local.h"

namespace hamsterdb {

#include "packstart.h"

/*
 * the internal representation of a serialized key
 */
HAM_PACK_0 class HAM_PACK_1 PBtreeKeyLegacy
{
  public:
    // Returns the record id
    //
    // !!!
    // if TINY or SMALL is set, the key is actually a char*-pointer;
    // in this case, we must not use endian-conversion!
    ham_u64_t get_record_id() {
      return (((m_flags8 & BtreeKey::kBlobSizeTiny)
                              || (m_flags8 & BtreeKey::kBlobSizeSmall))
              ? m_ptr
              : ham_db2h_offset(m_ptr));
    }

    // Returns the record id
    ham_u64_t get_record_id() const {
      return (((m_flags8 & BtreeKey::kBlobSizeTiny)
                              || (m_flags8 & BtreeKey::kBlobSizeSmall))
              ? m_ptr
              : ham_db2h_offset(m_ptr));
    }

    // Same as above, but without endian conversion
    ham_u64_t *get_rawptr() {
      return (&m_ptr);
    }

    // Same as above, but without endian conversion
    const ham_u64_t *get_rawptr() const {
      return (&m_ptr);
    }

    // Sets the record id
    //
    // !!! same problems as with get_record_id():
    // if TINY or SMALL is set, the key is actually a char*-pointer;
    // in this case, we must not use endian-conversion
    void set_record_id(ham_u64_t ptr) {
      m_ptr = (((m_flags8 & BtreeKey::kBlobSizeTiny)
                              || (m_flags8 & BtreeKey::kBlobSizeSmall))
              ? ptr
              : ham_h2db_offset(ptr));
    }

    // Returns the size of an btree-entry
    ham_u16_t get_size() const {
      return (ham_db2h16(m_keysize));
    }

    // Sets the size of an btree-entry
    void set_size(ham_u16_t size) {
      m_keysize = ham_h2db16(size);
    }

    // Returns the (persisted) flags of a key
    ham_u8_t get_flags() const {
      return (m_flags8);
    }

    // Sets the flags of a key
    //
    // Note that the ham_find/ham_cursor_find/ham_cursor_find_ex flags must be
    // defined such that those can peacefully co-exist with these; that's why
    // those public flags start at the value 0x1000 (4096).
    void set_flags(ham_u8_t flags) {
      m_flags8 = flags;
    }

    // Returns a pointer to the key data
    ham_u8_t *get_key() {
      return (m_key);
    }

    // Returns a pointer to the key data
    const ham_u8_t *get_key() const {
      return (m_key);
    }

    // Overwrites the key data
    void set_key(const void *ptr, ham_size_t len) {
      memcpy(m_key, ptr, len);
    }
  
    // Returns the record address of an extended key overflow area
    ham_u64_t get_extended_rid(LocalDatabase *db) const;

    // Sets the record address of an extended key overflow area
    void set_extended_rid(LocalDatabase *db, ham_u64_t rid);

    // The size of this structure without the single byte for the m_key
    static ham_size_t kSizeofOverhead;

  private:
    friend struct MiscFixture;

    // the pointer/record ID of this entry
    ham_u64_t m_ptr;

    // the size of this entry
    ham_u16_t m_keysize;

    // key flags (see above)
    ham_u8_t m_flags8;

    // the key data
    ham_u8_t m_key[1];

} HAM_PACK_2;

#include "packstop.h"

//
// A BtreeNodeProxy layout which stores key flags, key size, key data
// and the record pointer next to each other.
// This is the format used since the initial hamsterdb version.
//
class LegacyNodeLayout
{
  public:
    typedef PBtreeKeyLegacy *Iterator;
    typedef const PBtreeKeyLegacy *ConstIterator;

    LegacyNodeLayout(Page *page)
      : m_page(page), m_node(PBtreeNode::from_page(page)) {
    }

    // Returns the actual key size (including overhead)
    static ham_u16_t get_system_keysize(ham_size_t keysize) {
      return ((ham_u16_t)(keysize + PBtreeKeyLegacy::kSizeofOverhead));
    }

    Iterator begin() {
      return (at(0));
    }

    Iterator begin() const {
      return (at(0));
    }

    Iterator at(int slot) {
      return (get_iterator(m_page->get_db(), slot));
    }

    Iterator at(int slot) const {
      return (get_iterator(m_page->get_db(), slot));
    }

    Iterator next(Iterator it) {
      return (Iterator)(((const char *)it)
                      + m_page->get_db()->get_keysize()
                      + PBtreeKeyLegacy::kSizeofOverhead);
    }

    Iterator next(ConstIterator it) const {
      return (Iterator)(((const char *)it)
                      + m_page->get_db()->get_keysize()
                      + PBtreeKeyLegacy::kSizeofOverhead);
    }

    void release_key(Iterator it) {
      LocalDatabase *db = m_page->get_db();
      /* delete the extended key */
      if (it->get_flags() & BtreeKey::kExtended) {
        ham_u64_t blobid = it->get_extended_rid(db);
        db->remove_extkey(blobid);
      }
    }

    ham_status_t copy_full_key(ConstIterator it, ByteArray *arena,
                    ham_key_t *dest) const {
      LocalDatabase *db = m_page->get_db();
      ham_status_t st = 0;

      if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
        if (!arena->resize(it->get_size()) && it->get_size() > 0)
          return (HAM_OUT_OF_MEMORY);
        dest->data = arena->get_ptr();
        dest->size = it->get_size();
      }

      size_t size = std::min(it->get_size(), db->get_keysize());
      memcpy(dest->data, it->get_key(), size);

      // TODO not really efficient; get rid of Db::get_extended_key
      // and rewrite this function.
      if (it->get_flags() & BtreeKey::kExtended) {
        ham_key_t key = {0};
        key.data = arena->get_ptr();
        key.size = it->get_size();
        key.flags = HAM_KEY_USER_ALLOC;
        key._flags = BtreeKey::kExtended;

        st = db->get_extended_key((ham_u8_t *)key.data, key.size,
                       key._flags, &key);
      }

      /* recno databases: recno is stored in db-endian! */
      if (st == 0 && db->get_rt_flags() & HAM_RECORD_NUMBER) {
        ham_assert(dest->data != 0);
        ham_assert(dest->size == sizeof(ham_u64_t));
        ham_u64_t recno = *(ham_u64_t *)dest->data;
        recno = ham_db2h64(recno);
        memcpy(dest->data, &recno, sizeof(ham_u64_t));
      }

      return (st);
    }

    ham_status_t check_integrity(Iterator it, BlobManager *bm) const {
      if (it->get_flags() & BtreeKey::kExtended) {
        ham_u64_t blobid = it->get_extended_rid(m_page->get_db());
        if (!blobid) {
          ham_log(("integrity check failed in page 0x%llx: item "
                  "is extended, but has no blob", m_page->get_address()));
          return (HAM_INTEGRITY_VIOLATED);
        }

        // make sure that the extended blob can be loaded
        ham_record_t record = {0};
        ByteArray arena;
        ham_status_t st = bm->read(m_page->get_db(), blobid, &record, 0, &arena);
        if (st) {
          ham_log(("integrity check failed in page 0x%llx: item "
                  "is extended, but failed to read blob: %d",
                  m_page->get_address(), st));
          return (HAM_INTEGRITY_VIOLATED);
        }
      }
      return (0);
    }

    void initialize() {
      memset(m_page->get_payload(), 0, sizeof(PBtreeNode));
    }

    template<typename Cmp>
    int compare(const ham_key_t *lhs, Iterator it, Cmp &cmp) {
      if (it->get_flags() & BtreeKey::kExtended) {
        ham_key_t tmp = {0};
        copy_full_key(it, &m_arena, &tmp);
        return (cmp(lhs->data, lhs->size, tmp.data, tmp.size));
      }
      return (cmp(lhs->data, lhs->size, it->get_key(), it->get_size()));
    }

    void split(LegacyNodeLayout *other, int pivot) {
      Iterator nit = other->begin();
      Iterator oit = at(pivot);
      ham_size_t keysize = m_page->get_db()->get_keysize();

      /*
       * if we split a leaf, we'll insert the pivot element in the leaf
       * page, too. in internal nodes do not insert the pivot element, but
       * propagate it to the parent node only.
       */
      if (m_node->is_leaf()) {
        memcpy(nit, oit, (PBtreeKeyLegacy::kSizeofOverhead + keysize)
                    * (m_node->get_count() - pivot));
      }
      else {
        oit = next(oit), // skip pivot element
        memcpy(nit, oit, (PBtreeKeyLegacy::kSizeofOverhead + keysize)
                    * (m_node->get_count() - pivot - 1));
      }
    }

    Iterator insert(ham_u32_t slot, const ham_key_t *key) {
      Iterator it = at(slot);
      ham_size_t keysize = m_page->get_db()->get_keysize();
      ham_size_t count = m_node->get_count();

      if (count > slot) {
        memmove(((char *)it) + PBtreeKeyLegacy::kSizeofOverhead + keysize, it,
                  (PBtreeKeyLegacy::kSizeofOverhead + keysize) * (count - slot));
      }
      /* if a new key is created or inserted: initialize it with zeroes */
      memset(it, 0, PBtreeKeyLegacy::kSizeofOverhead + keysize);

      it->set_size(key->size);

      /* set a flag if the key is extended, and does not fit into the btree */
      if (key->size > keysize)
        it->set_flags(it->get_flags() | BtreeKey::kExtended);

      /* store the key */
      it->set_key(key->data, std::min(keysize, (ham_size_t)key->size));

      return (it);
    }

    void remove(ham_u32_t slot) {
      LocalDatabase *db = m_page->get_db();
      Iterator lhs = at(slot);

      /* get rid of the extended key (if there is one); also remove the key
       * from the cache */
      if (lhs->get_flags() & BtreeKey::kExtended) {
        ham_u64_t blobid = lhs->get_extended_rid(db);
        ham_assert(blobid);

        (void)db->remove_extkey(blobid);
      }

      if (slot != m_node->get_count() - 1) {
        Iterator rhs = at(slot + 1);
        memmove(lhs, rhs, ((PBtreeKeyLegacy::kSizeofOverhead + db->get_keysize()))
                * (m_node->get_count() - slot - 1));
      }
    }

    void merge_from(LegacyNodeLayout *other) {
      Iterator lhs = at(m_node->get_count());
      Iterator rhs = other->begin();

      /* shift items from the sibling to this page */
      ham_size_t keysize = m_page->get_db()->get_keysize();
      memcpy(lhs, rhs, (PBtreeKeyLegacy::kSizeofOverhead + keysize)
                      * other->m_node->get_count());
    }

    void shift_from_right(LegacyNodeLayout *other, int count) {
      Iterator lhs = at(m_node->get_count());
      Iterator rhs = other->begin();
      ham_size_t keysize = m_page->get_db()->get_keysize();
      memmove(lhs, rhs, (PBtreeKeyLegacy::kSizeofOverhead + keysize) * count);

      lhs = other->begin();
      rhs = other->at(count);
      memmove(lhs, rhs, (PBtreeKeyLegacy::kSizeofOverhead + keysize)
              * (other->m_node->get_count() - count));
    }

    void shift_to_right(LegacyNodeLayout *other, int slot, int count) {
      Iterator lhs = other->at(count);
      Iterator rhs = other->begin();
      ham_size_t keysize = m_page->get_db()->get_keysize();
      memmove(lhs, rhs, (PBtreeKeyLegacy::kSizeofOverhead + keysize)
              * other->m_node->get_count());

      lhs = other->begin();
      rhs = at(slot);
      memmove(lhs, rhs, (PBtreeKeyLegacy::kSizeofOverhead + keysize) * count);
    }

  private:
    // Returns the BtreeKey at index |i| in this node
    //
    // note that this function does not check the boundaries (i.e. whether
    // i <= get_count(), because some functions deliberately write to
    // elements "after" get_count()
    Iterator get_iterator(LocalDatabase *db, int i) {
      return ((PBtreeKeyLegacy *)&(m_node->get_data()
                [(db->get_keysize() + PBtreeKeyLegacy::kSizeofOverhead) * i]));
    }

    // Same as above, const flavor
    Iterator get_iterator(LocalDatabase *db, int i) const {
      return ((PBtreeKeyLegacy *)&(m_node->get_data()
                [(db->get_keysize() + PBtreeKeyLegacy::kSizeofOverhead) * i]));
    }

    Page *m_page;
    PBtreeNode *m_node;
    ByteArray m_arena;
};

} // namespace hamsterdb

#endif /* HAM_BTREE_NODE_LEGACY_H__ */
