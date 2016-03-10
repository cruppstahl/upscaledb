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
 * A factory to create TxnOperation and TxnNode instances.
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

struct TxnFactory {
  // Creates a new TxnOperation
  static TxnOperation *create_operation(LocalTxn *txn,
            TxnNode *node, uint32_t flags, uint32_t orig_flags,
            uint64_t lsn, ups_key_t *key, ups_record_t *record) {
    TxnOperation *op;
    op = Memory::allocate<TxnOperation>(sizeof(*op)
                                            + (record ? record->size : 0)
                                            + (key ? key->size : 0));
    op->initialize(txn, node, flags, orig_flags, lsn, key, record);
    return op;
  }

  // Destroys a TxnOperation
  static void destroy_operation(TxnOperation *op) {
    op->destroy();
  }
};

} // namespace upscaledb

#endif /* UPS_TXN_FACTORY_H */
