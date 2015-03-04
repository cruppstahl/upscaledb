/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

struct Context;
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
