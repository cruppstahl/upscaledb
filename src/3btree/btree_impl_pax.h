/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
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
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
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
class PaxNodeImpl : public BaseNodeImpl<KeyList, RecordList>
{
    // C++ does not allow access to members of base classes unless they're
    // explicitly named; this typedef helps to make the code "less" ugly,
    // but it still sucks that i have to use it
    //
    // http://stackoverflow.com/questions/1120833/derived-template-class-access-to-base-class-member-data
    typedef BaseNodeImpl<KeyList, RecordList> P;

  public:
    // Constructor
    PaxNodeImpl(Page *page)
      : BaseNodeImpl<KeyList, RecordList>(page) {
      initialize();
    }

    // Iterates all keys, calls the |visitor| on each
    void scan(Context *context, ScanVisitor *visitor, uint32_t start,
                    bool distinct) {
      P::m_keys.scan(context, visitor, start, P::m_node->get_count() - start);
    }

    // Returns true if |key| cannot be inserted because a split is required
    bool requires_split(Context *context, const ups_key_t *key) const {
      return (P::m_node->get_count() >= P::m_estimated_capacity);
    }

  private:
    void initialize() {
      uint32_t usable_nodesize
              = Page::usable_page_size(P::m_page->get_db()->lenv()->config().page_size_bytes)
                    - PBtreeNode::get_entry_offset();
      size_t ks = P::m_keys.get_full_key_size();
      size_t rs = P::m_records.get_full_record_size();
      size_t capacity = usable_nodesize / (ks + rs);

      uint8_t *p = P::m_node->get_data();
      if (P::m_node->get_count() == 0) {
        P::m_keys.create(&p[0], capacity * ks);
        P::m_records.create(&p[capacity * ks], capacity * rs);
      }
      else {
        size_t key_range_size = capacity * ks;
        size_t record_range_size = capacity * rs;

        P::m_keys.open(p, key_range_size, P::m_node->get_count());
        P::m_records.open(p + key_range_size, record_range_size,
                        P::m_node->get_count());
      }

      P::m_estimated_capacity = capacity;
    }
};

} // namespace upscaledb

#endif /* UPS_BTREE_IMPL_PAX_H */
