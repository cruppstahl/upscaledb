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
            TransactionNode *node, uint32_t flags, uint32_t orig_flags,
            uint64_t lsn, ham_key_t *key, ham_record_t *record) {
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
