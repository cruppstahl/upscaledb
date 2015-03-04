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
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_TXN_REMOTE_H
#define HAM_TXN_REMOTE_H

#ifdef HAM_ENABLE_REMOTE

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "4txn/txn.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

struct Context;

//
// A remote Transaction
//
class RemoteTransaction : public Transaction
{
  public:
    // Constructor; "begins" the Transaction
    // supported flags: HAM_TXN_READ_ONLY, HAM_TXN_TEMPORARY
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
    virtual ham_status_t commit(Transaction *txn, uint32_t flags = 0);

    // Aborts a Transaction; the derived subclass has to take care of
    // flushing and/or releasing memory
    virtual ham_status_t abort(Transaction *txn, uint32_t flags = 0);

    // Flushes committed (queued) transactions
    virtual void flush_committed_txns(Context *context = 0);
};

} // namespace hamsterdb

#endif // HAM_ENABLE_REMOTE

#endif /* HAM_TXN_REMOTE_H */
