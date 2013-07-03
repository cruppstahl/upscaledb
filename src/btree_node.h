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

#ifndef HAM_BTREE_NODE_H__
#define HAM_BTREE_NODE_H__

#include "endianswap.h"
#include "btree_key.h"
#include "db_local.h"

#include "packstart.h"

namespace hamsterdb {

/*
 * A BtreeNode structure spans the persistent part of a Page
 *
 * This structure is directly written to/read from the file. The
 * getters/setters provide endian-agnostic access.
 */
HAM_PACK_0 struct HAM_PACK_1 PBtreeNode
{
  public:
    // Returns a PBtreeNode from a Page
    static PBtreeNode *from_page(Page *page) {
      return ((PBtreeNode *)page->get_payload());
    }

    // Returns the offset (in bytes) of the member |m_entries|
    static ham_size_t get_entry_offset() {
      return (OFFSETOF(PBtreeNode, m_entries));
    }

    // Returns the number of entries in a BtreeNode
    ham_u16_t get_count() const {
      return (ham_db2h16(m_count));
    }

    // Sets the number of entries in a BtreeNode
    void set_count(ham_u16_t c) {
      m_count = ham_h2db16(c);
    }

    // Returns the address of the left sibling of this node
    ham_u64_t get_left() const {
      return (ham_db2h_offset(m_left));
    }

    // Sets the address of the left sibling of this node
    void set_left(ham_u64_t o) {
      m_left = ham_h2db_offset(o);
    }

    // Returns the address of the right sibling of this node
    ham_u64_t get_right() const {
      return (ham_db2h_offset(m_right));
    }

    // Sets the address of the right sibling of this node
    void set_right(ham_u64_t o) {
      m_right = ham_h2db_offset(o);
    }

    // Returns the ptr_left of this node
    ham_u64_t get_ptr_left() const {
      return (ham_db2h_offset(m_ptr_left));
    }

    // Returns true if this btree node is a leaf node
    bool is_leaf() const {
      return (m_ptr_left == 0);
    }

    // Sets the ptr_left of this node
    void set_ptr_left(ham_u64_t o) {
      m_ptr_left = ham_h2db_offset(o);
    }

    // Returns the BtreeKey at index |i| in this node
    //
    // note that this function does not check the boundaries (i.e. whether
    // i <= get_count(), because some functions deliberately write to
    // elements "after" get_count()
    PBtreeKey *get_key(LocalDatabase *db, int i) {
      return ((PBtreeKey *)&((const char *)m_entries)
                [(db->get_keysize() + PBtreeKey::kSizeofOverhead) * i]);
    }

  private:
    // flags of this node - flags are always the first member
    // of every page - regardless of the btree.
    // Currently only used for the page type.
    ham_u16_t m_flags;

    // number of used entries in the node
    ham_u16_t m_count;
  
    // address of left sibling
    ham_u64_t m_left;

    // address of right sibling
    ham_u64_t m_right;

    // address of child node whose items are smaller than all items
    // in this node
    ham_u64_t m_ptr_left;

    // the entries of this node
    PBtreeKey m_entries[1];

} HAM_PACK_2;

#include "packstop.h"

} // namespace hamsterdb

#endif /* HAM_BTREE_NODE_H__ */
