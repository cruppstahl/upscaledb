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
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef UPS_TXN_REMOTE_H
#define UPS_TXN_REMOTE_H

#ifdef UPS_ENABLE_REMOTE

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "4txn/txn.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct Context;

//
// A remote Transaction
//
class RemoteTransaction : public Transaction
{
  public:
    // Constructor; "begins" the Transaction
    // supported flags: UPS_TXN_READ_ONLY, UPS_TXN_TEMPORARY
    RemoteTransaction(Environment *env, const char *name, uint32_t flags,
                    uint64_t remote_handle);

    // Commits the Transaction
    virtual void commit(uint32_t flags = 0);

    // Aborts the Transaction
    virtual void abort(uint32_t flags = 0);

    // Returns the remote Transaction handle
    uint64_t get_remote_handle() const {
      return (m_remote_handle);
    }

  private:
    // The remote Transaction handle
    uint64_t m_remote_handle;
};


//
// A TransactionManager for remote Transactions
//
class RemoteTransactionManager : public TransactionManager
{
  public:
    // Constructor
    RemoteTransactionManager(Environment *env)
      : TransactionManager(env) {
    }

    // Begins a new Transaction
    virtual void begin(Transaction *txn);

    // Commits a Transaction; the derived subclass has to take care of
    // flushing and/or releasing memory
    virtual ups_status_t commit(Transaction *txn, uint32_t flags = 0);

    // Aborts a Transaction; the derived subclass has to take care of
    // flushing and/or releasing memory
    virtual ups_status_t abort(Transaction *txn, uint32_t flags = 0);

    // Flushes committed (queued) transactions
    virtual void flush_committed_txns(Context *context = 0);
};

} // namespace upscaledb

#endif // UPS_ENABLE_REMOTE

#endif /* UPS_TXN_REMOTE_H */
