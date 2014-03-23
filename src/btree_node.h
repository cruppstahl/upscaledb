/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

#ifndef HAM_BTREE_NODE_H__
#define HAM_BTREE_NODE_H__

#include "endianswap.h"
#include "btree_flags.h"
#include "db_local.h"
#include "page.h"

#include "packstart.h"

namespace hamsterdb {

class PBtreeKeyDefault;

/*
 * A BtreeNode structure spans the persistent part of a Page
 *
 * This structure is directly written to/read from the file. The
 * getters/setters provide endian-agnostic access.
 */
HAM_PACK_0 struct HAM_PACK_1 PBtreeNode
{
  public:
    enum {
      // node is a leaf
      kLeafNode = 1
    };

    // Returns a PBtreeNode from a Page
    static PBtreeNode *from_page(Page *page) {
      return ((PBtreeNode *)page->get_payload());
    }

    // Returns the offset (in bytes) of the member |m_data|
    static ham_u32_t get_entry_offset() {
      return (OFFSETOF(PBtreeNode, m_data));
    }

    // Returns the flags of the btree node (|kLeafNode|)
    ham_u32_t get_flags() const {
      return (ham_db2h32(m_flags));
    }

    // Sets the flags of the btree node (|kLeafNode|)
    void set_flags(ham_u32_t flags) {
      m_flags = ham_h2db32(flags);
    }

    // Returns the number of entries in a BtreeNode
    ham_u32_t get_count() const {
      return (ham_db2h32(m_count));
    }

    // Sets the number of entries in a BtreeNode
    void set_count(ham_u32_t c) {
      ham_assert((int)c >= 0);
      m_count = ham_h2db32(c);
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

    // Returns the ptr_down of this node
    ham_u64_t get_ptr_down() const {
      return (ham_db2h_offset(m_ptr_down));
    }

    // Returns true if this btree node is a leaf node
    bool is_leaf() const {
      return (m_flags & kLeafNode);
    }

    // Sets the ptr_down of this node
    void set_ptr_down(ham_u64_t o) {
      m_ptr_down = ham_h2db_offset(o);
    }

    // Returns a pointer to the key data
    ham_u8_t *get_data() {
      return (&m_data[0]);
    }

    const ham_u8_t *get_data() const {
      return (&m_data[0]);
    }

  private:
    // flags of this node
    ham_u32_t m_flags;

    // number of used entries in the node
    ham_u32_t m_count;
  
    // address of left sibling
    ham_u64_t m_left;

    // address of right sibling
    ham_u64_t m_right;

    // address of child node whose items are smaller than all items
    // in this node
    ham_u64_t m_ptr_down;

    // the entries of this node
    ham_u8_t m_data[1];

} HAM_PACK_2;

#include "packstop.h"

} // namespace hamsterdb

#endif /* HAM_BTREE_NODE_H__ */
