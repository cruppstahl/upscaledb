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
 * @exception_safe: nothrow
 * @thread_safe: yes
 */

#ifndef HAM_BTREE_VISITOR_H
#define HAM_BTREE_VISITOR_H

#include "0root/root.h"

#include "ham/hamsterdb_ola.h"

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

//
// The ScanVisitor is the callback implementation for the scan call.
// It will either receive single keys or multiple keys in an array.
//
struct ScanVisitor {
  // Operates on a single key
  virtual void operator()(const void *key_data, uint16_t key_size, 
                  size_t duplicate_count) = 0;

  // Operates on an array of keys
  virtual void operator()(const void *key_array, size_t key_count) = 0;

  // Assigns the internal result to |result|
  virtual void assign_result(hola_result_t *result) = 0;
};

class Context;
class BtreeNodeProxy;

//
// The BtreeVisitor is the callback implementation for the visit call.
// It will visit each node instead of each key.
//
struct BtreeVisitor {
  // Specifies if the visitor modifies the node
  virtual bool is_read_only() const = 0;

  // called for each node
  virtual void operator()(Context *context, BtreeNodeProxy *node) = 0;
};

} // namespace hamsterdb

#endif /* HAM_BTREE_VISITOR_H */
