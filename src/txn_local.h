/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifndef HAM_TXN_LOCAL_H__
#define HAM_TXN_LOCAL_H__

#include "txn.h"

namespace hamsterdb {

class LocalEnvironment;

//
// A local Transaction
//
class LocalTransaction : public Transaction
{
  public:
    // Constructor; "begins" the Transaction
    // supported flags: HAM_TXN_READ_ONLY, HAM_TXN_TEMPORARY
    LocalTransaction(LocalEnvironment *env, const char *name, ham_u32_t flags);

    // Destructor; frees all TransactionOperation structures associated
    // with this Transaction
    virtual ~LocalTransaction();

    // Commits the Transaction
    void commit(ham_u32_t flags = 0);

    // Aborts the Transaction
    void abort(ham_u32_t flags = 0);

    // Returns the first (or 'oldest') TransactionOperation of this Transaction
    // TODO required?
    TransactionOperation *get_oldest_op() const {
      return (m_oldest_op);
    }

    // Sets the first (or 'oldest') TransactionOperation of this Transaction
    // TODO required?
    void set_oldest_op(TransactionOperation *op) {
      m_oldest_op = op;
    }

    // Returns the last (or 'newest') TransactionOperation of this Transaction
    // TODO required?
    TransactionOperation *get_newest_op() const {
      return (m_newest_op);
    }

    // Sets the last (or 'newest') TransactionOperation of this Transaction
    // TODO required?
    void set_newest_op(TransactionOperation *op) {
      m_newest_op = op;
    }

  private:
    friend class Journal;
    friend struct TxnFixture;
    friend struct TxnCursorFixture;

    // Frees the internal structures; releases all the memory. This is
    // called in the destructor, but also when aborting a Transaction
    // (before it's deleted by the Environment).
    void free_operations();

    // Returns the index of the journal's log file descriptor
    int get_log_desc() const {
      return (m_log_desc);
    }

    // Sets the index of the journal's log file descriptor
    void set_log_desc(int desc) {
      m_log_desc = desc;
    }

    // index of the log file descriptor for this transaction [0..1]
    int m_log_desc;

    // the linked list of operations - head is oldest operation
    TransactionOperation *m_oldest_op;

    // the linked list of operations - tail is newest operation
    TransactionOperation *m_newest_op;
};


} // namespace hamsterdb

#endif /* HAM_TXN_LOCAL_H__ */
