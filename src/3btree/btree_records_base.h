/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
 * Base class for RecordLists
 *
 * @exception_safe: nothrow
 * @thread_safe: unknown
 */

#ifndef HAM_BTREE_RECORDS_BASE_H
#define HAM_BTREE_RECORDS_BASE_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct BaseRecordList
{
  BaseRecordList()
    : m_range_size(0) {
  }

  // Checks the integrity of this node. Throws an exception if there is a
  // violation.
  void check_integrity(Context *context, size_t node_count) const {
  }

  // Rearranges the list
  void vacuumize(size_t node_count, bool force) const {
  }

  // Fills the btree_metrics structure
  void fill_metrics(btree_metrics_t *metrics, size_t node_count) {
    BtreeStatistics::update_min_max_avg(&metrics->recordlist_ranges,
                        m_range_size);
  }

  // The size of the range (in bytes)
  size_t m_range_size;
};

} // namespace hamsterdb

#endif /* HAM_BTREE_RECORDS_BASE_H */
