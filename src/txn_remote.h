/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

#ifndef HAM_TXN_REMOTE_H__
#define HAM_TXN_REMOTE_H__

#ifdef HAM_ENABLE_REMOTE

#include "txn.h"

namespace hamsterdb {

//
// A remote Transaction
//
class RemoteTransaction : public Transaction
{
  public:
    // Constructor; "begins" the Transaction
    // supported flags: HAM_TXN_READ_ONLY, HAM_TXN_TEMPORARY
    RemoteTransaction(Environment *env, const char *name, ham_u32_t flags);

    // Commits the Transaction
    virtual void commit(ham_u32_t flags = 0);

    // Aborts the Transaction
    virtual void abort(ham_u32_t flags = 0);

    // Returns the remote Transaction handle
    ham_u64_t get_remote_handle() const {
      return (m_remote_handle);
    }

    // Sets the remote Transaction handle
    void set_remote_handle(ham_u64_t handle) {
      m_remote_handle = handle;
    }

  private:
    // The remote Transaction handle
    ham_u64_t m_remote_handle;
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
    virtual Transaction *begin(const char *name, ham_u32_t flags);

    // Commits a Transaction; the derived subclass has to take care of
    // flushing and/or releasing memory
    virtual void commit(Transaction *txn, ham_u32_t flags = 0);

    // Aborts a Transaction; the derived subclass has to take care of
    // flushing and/or releasing memory
    virtual void abort(Transaction *txn, ham_u32_t flags = 0);

    // Flushes committed (queued) transactions
    virtual void flush_committed_txns();
};


} // namespace hamsterdb

#endif // HAM_ENABLE_REMOTE

#endif /* HAM_TXN_REMOTE_H__ */
