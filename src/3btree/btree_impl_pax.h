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
 * Btree node layout for fixed length keys WITHOUT duplicates
 * ==========================================================
 *
 * This layout supports fixed length keys and fixed length records. It does
 * not support duplicates and extended keys. Keys and records are always
 * inlined, but records can refer to blobs (in this case the "fixed length"
 * record is the 8 byte record ID).
 *
 * Unlike the academic PAX paper, which stored multiple columns in one page,
 * upscaledb stores only one column (= database) in a page, but keys and
 * records are separated from each other. The keys (flags + key data) are
 * stored in the beginning of the page, the records start somewhere in the
 * middle (the exact start position depends on key size, page size and other
 * parameters).
 *
 * This layout's implementation is relatively simple because the offset
 * of the key data and record data is easy to calculate since all keys
 * and records have the same size.
 *
 * This separation of keys and records allows a more compact layout and a
 * high density of the key data, which better exploits CPU caches and allows
 * very tight loops when searching through the keys.
 *
 * This layout has two incarnations:
 * 1. Fixed length keys, fixed length inline records
 *  -> does not require additional flags
 * 2. Fixed length keys, variable length records (8 byte record id)
 *  -> requires a 1 byte flag per key
 *
 * The flat memory layout looks like this:
 *
 * |Flag1|Flag2|...|Flagn|...|Key1|Key2|...|Keyn|...|Rec1|Rec2|...|Recn|
 *
 * Flags are optional, as described above.
 */

#ifndef UPS_BTREE_IMPL_PAX_H
#define UPS_BTREE_IMPL_PAX_H

#include "0root/root.h"

#include <sstream>
#include <iostream>

// Always verify that a file of level N does not include headers > N!
#include "1globals/globals.h"
#include "1base/dynamic_array.h"
#include "2page/page.h"
#include "3blob_manager/blob_manager.h"
#include "3btree/btree_node.h"
#include "3btree/btree_impl_base.h"
#include "4env/env_local.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

//
// A BtreeNodeProxy layout which stores key data, key flags and
// and the record pointers in a PAX style layout.
//
template<typename KeyList, typename RecordList>
struct PaxNodeImpl : public BaseNodeImpl<KeyList, RecordList> {
  // C++ does not allow access to members of base classes unless they're
  // explicitly named; this typedef helps to make the code "less" ugly,
  // but it still sucks that i have to use it
  //
  // http://stackoverflow.com/questions/1120833/derived-template-class-access-to-base-class-member-data
  typedef BaseNodeImpl<KeyList, RecordList> P;

  // Constructor
  PaxNodeImpl(Page *page)
    : BaseNodeImpl<KeyList, RecordList>(page) {
    initialize();
  }

  // Returns true if |key| cannot be inserted because a split is required
  bool requires_split(Context *context, const ups_key_t *key) const {
    return P::node->length() >= P::estimated_capacity;
  }

  void initialize() {
    uint32_t usable_nodesize = P::page->usable_page_size()
                  - PBtreeNode::entry_offset();
    size_t ks = P::keys.full_key_size();
    size_t rs = P::records.full_record_size();
    size_t capacity = usable_nodesize / (ks + rs);

    uint8_t *p = P::node->data();
    if (P::node->length() == 0) {
      P::keys.create(&p[0], capacity * ks);
      P::records.create(&p[capacity * ks], capacity * rs);
    }
    else {
      size_t key_range_size = capacity * ks;
      size_t record_range_size = capacity * rs;

      P::keys.open(p, key_range_size, P::node->length());
      P::records.open(p + key_range_size, record_range_size,
                      P::node->length());
    }

    P::estimated_capacity = capacity;
  }
};

} // namespace upscaledb

#endif // UPS_BTREE_IMPL_PAX_H
