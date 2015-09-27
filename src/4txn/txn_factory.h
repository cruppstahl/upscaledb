/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
 * A factory to create TransactionOperation and TransactionNode instances.
 *
 * @exception_safe: strong
 * @thread_safe: yes
 */

#ifndef UPS_TXN_FACTORY_H
#define UPS_TXN_FACTORY_H

#include "0root/root.h"

#include "ups/types.h"

// Always verify that a file of level N does not include headers > N!
#include "1mem/mem.h"
#include "4txn/txn.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct TransactionFactory
{
  // Creates a new TransactionOperation
  static TransactionOperation *create_operation(LocalTransaction *txn,
            TransactionNode *node, uint32_t flags, uint32_t orig_flags,
            uint64_t lsn, ups_key_t *key, ups_record_t *record) {
    TransactionOperation *op;
    op = Memory::allocate<TransactionOperation>(sizeof(*op)
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

} // namespace upscaledb

#endif /* UPS_TXN_FACTORY_H */
