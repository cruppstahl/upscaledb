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

#ifndef HAM_BTREE_NODE_H__
#define HAM_BTREE_NODE_H__

#include "btree_flags.h"
#include "db_local.h"
#include "page.h"

#include "packstart.h"

namespace hamsterdb {

class PBtreeKeyDefault;

/*
 * A BtreeNode structure spans the persistent part of a Page
 *
 * This structure is directly written to/read from the file.
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
      return (m_flags);
    }

    // Sets the flags of the btree node (|kLeafNode|)
    void set_flags(ham_u32_t flags) {
      m_flags = flags;
    }

    // Returns the number of entries in a BtreeNode
    ham_u32_t get_count() const {
      return (m_count);
    }

    // Sets the number of entries in a BtreeNode
    void set_count(ham_u32_t count) {
      ham_assert((int)count >= 0);
      m_count = count;
    }

    // Returns the address of the left sibling of this node
    ham_u64_t get_left() const {
      return (m_left);
    }

    // Sets the address of the left sibling of this node
    void set_left(ham_u64_t left) {
      m_left = left;
    }

    // Returns the address of the right sibling of this node
    ham_u64_t get_right() const {
      return (m_right);
    }

    // Sets the address of the right sibling of this node
    void set_right(ham_u64_t right) {
      m_right = right;
    }

    // Returns the ptr_down of this node
    ham_u64_t get_ptr_down() const {
      return (m_ptr_down);
    }

    // Returns true if this btree node is a leaf node
    bool is_leaf() const {
      return (m_flags & kLeafNode);
    }

    // Sets the ptr_down of this node
    void set_ptr_down(ham_u64_t ptr_down) {
      m_ptr_down = ptr_down;
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
