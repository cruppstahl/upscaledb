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

/*
 * A factory to create TransactionOperation and TransactionNode instances.
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_TXN_FACTORY_H
#define HAM_TXN_FACTORY_H

#include "0root/root.h"

#include "ham/types.h"

// Always verify that a file of level N does not include headers > N!
#include "1mem/mem.h"
#include "4txn/txn.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct TransactionFactory
{
  // Creates a new TransactionOperation
  static TransactionOperation *create_operation(LocalTransaction *txn,
            TransactionNode *node, ham_u32_t flags, ham_u32_t orig_flags,
            ham_u64_t lsn, ham_key_t *key, ham_record_t *record) {
    TransactionOperation *op;
    op = (TransactionOperation *)Memory::allocate<char>(sizeof(*op)
                                            + (record ? record->size : 0)
                                            + (key ? key->size : 0));
    op->initialize(txn, node, flags, orig_flags, lsn, key, record);
    return (op);
  }

  // Destroys a TransactionOperation
  static void destroy_operation(TransactionOperation *op) {
    op->destroy();
  }
};

} // namespace hamsterdb

#endif /* HAM_TXN_FACTORY_H */
