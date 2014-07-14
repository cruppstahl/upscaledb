/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
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
 * hamsterdb stores only one column (= database) in a page, but keys and
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

#ifndef HAM_BTREE_IMPL_PAX_H
#define HAM_BTREE_IMPL_PAX_H

#include "0root/root.h"

#include <sstream>
#include <iostream>

// Always verify that a file of level N does not include headers > N!
#include "1globals/globals.h"
#include "1base/byte_array.h"
#include "2page/page.h"
#include "3blob_manager/blob_manager.h"
#include "3btree/btree_node.h"
#include "3btree/btree_impl_base.h"
#include "4env/env_local.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

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

    // Searches the node for the key and returns the slot of this key
    // NO simd support!
    template<typename Cmp>
    int find_child(ham_key_t *key, Cmp &comparator, uint64_t *precord_id,
                    int *pcmp) {
      size_t node_count = P::m_node->get_count();
      ham_assert(node_count > 0);

      // Run a binary search, but fall back to linear search as soon as
      // the remaining range is too small
      int threshold = P::m_keys.get_linear_search_threshold();
      int i, l = 0, r = node_count;
      int last = node_count + 1;
      int cmp = -1;

      /* repeat till we found the key or the remaining range is so small that
       * we rather perform a linear search (which is faster for small ranges) */
      while (r - l > threshold) {
        /* get the median item; if it's identical with the "last" item,
         * we've found the slot */
        i = (l + r) / 2;

        if (i == last) {
          ham_assert(i >= 0);
          ham_assert(i < (int)node_count);
          *pcmp = 1;
          if (precord_id)
            *precord_id = P::get_record_id(i);
          return (i);
        }

        /* compare it against the key */
        cmp = compare(key, i, comparator);

        /* found it? */
        if (cmp == 0) {
          *pcmp = cmp;
          if (precord_id)
            *precord_id = P::get_record_id(i);
          return (i);
        }
        /* if the key is < the current item: search "to the left" */
        else if (cmp < 0) {
          if (r == 0) {
            ham_assert(i == 0);
            *pcmp = cmp;
            if (precord_id)
              *precord_id = P::m_node->get_ptr_down();
            return (-1);
          }
          r = i;
        }
        /* otherwise search "to the right" */
        else {
          last = i;
          l = i;
        }
      }

      // still here? then perform a linear search for the remaining range
      ham_assert(r - l <= threshold);
      int slot = P::m_keys.linear_search(l, r - l, key, comparator, pcmp);
      if (precord_id) {
        if (slot == -1)
          *precord_id = P::m_node->get_ptr_down();
        else
          *precord_id = P::get_record_id(slot);
      }
      return (slot);
    }

>>>>>>> Pro 2.1.7: commercial fork, heavy-weight compression for records, journal and     keys, AES encryption, SIMD
    // Iterates all keys, calls the |visitor| on each
    void scan(ScanVisitor *visitor, uint32_t start, bool distinct) {
      P::m_keys.scan(visitor, start, P::m_node->get_count() - start);
    }

    // Returns true if |key| cannot be inserted because a split is required
    bool requires_split(const ham_key_t *key) const {
      return (P::m_node->get_count() >= P::m_estimated_capacity);
    }

  private:
    void initialize() {
      uint32_t usable_nodesize
              = P::m_page->get_db()->get_local_env()->get_usable_page_size()
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

} // namespace hamsterdb

#endif /* HAM_BTREE_IMPL_PAX_H */
