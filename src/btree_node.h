/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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

#include "internal_fwd_decl.h"

#include "endianswap.h"
#include "btree_key.h"
#include "db.h"

#include "packstart.h"

namespace ham {

/**
 * A btree-node; it spans the persistent part of a Page
 *
 * This structure is directly written to/read from the file. The
 * getters/setters provide endian-agnostic access.
 */
HAM_PACK_0 struct HAM_PACK_1 BtreeNode
{
  /** get a BtreeNode from a Page */
  static BtreeNode *from_page(Page *page) {
    return ((BtreeNode *)page->get_payload());
  }

  /** get the number of entries of a btree-node */
  ham_u16_t get_count() {
    return (ham_db2h16(_count));
  }

  /** set the number of entries of a btree-node */
  void set_count(ham_u16_t c) {
    _count = ham_h2db16(c);
  }

  /** get the address of the left sibling of a btree-node */
  ham_offset_t get_left() {
    return (ham_db2h_offset(_left));
  }

  /** set the address of the left sibling of a btree-node */
  void set_left(ham_offset_t o) {
    _left = ham_h2db_offset(o);
  }

  /** get the address of the right sibling of a btree-node */
  ham_offset_t get_right() {
    return (ham_db2h_offset(_right));
  }

  /** set the address of the right sibling of a btree-node */
  void set_right(ham_offset_t o) {
    _right = ham_h2db_offset(o);
  }

  /** get the ptr_left of a btree-node */
  ham_offset_t get_ptr_left() {
    return (ham_db2h_offset(_ptr_left));
  }

  /** check if a btree node is a leaf node */
  bool is_leaf() {
    return (_ptr_left == 0);
  }

  /** set the ptr_left of a btree-node */
  void set_ptr_left(ham_offset_t o) {
    _ptr_left = ham_h2db_offset(o);
  }

  /** get entry @a i of a btree node */
  BtreeKey *get_key(Database *db, int i) {
    ham_assert(i <= get_count());
    return ((BtreeKey *)&((const char *)_entries)
              [(db_get_keysize(db) + BtreeKey::ms_sizeof_overhead) * i]);
  }

  /**
   * flags of this node - flags are always the first member
   * of every page - regardless of the backend.
   * Currently only used for the page type.
   */
  ham_u16_t _flags;

  /** number of used entries in the node */
  ham_u16_t _count;

  /** address of left sibling */
  ham_offset_t _left;

  /** address of right sibling */
  ham_offset_t _right;

  /**
   * address of child node whose items are smaller than all items
   * in this node
   */
  ham_offset_t _ptr_left;

  /** the entries of this node */
  BtreeKey _entries[1];

} HAM_PACK_2;

#include "packstop.h"


} // namespace ham

#endif /* HAM_BTREE_NODE_H__ */
