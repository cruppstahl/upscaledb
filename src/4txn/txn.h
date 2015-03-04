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
 * The hamsterdb Transaction implementation
 *
 * hamsterdb stores Transactions in volatile RAM (with an append-only journal
 * in case the RAM is lost). Each Transaction and each modification *in* a 
 * Transaction is stored in a complex data structure.
 *
 * When a Database is created, it contains a BtreeIndex for persistent
 * (committed and flushed) data, and a TransactionIndex for active Transactions
 * and those Transactions which were committed but not yet flushed to disk.
 * This TransactionTree is implemented as a binary search tree (see rb.h).
 *
 * Each node in the TransactionTree is implemented by TransactionNode. Each
 * node is identified by its database key, and groups all modifications of this
 * key (of all Transactions!).
 *
 * Each modification in the node is implemented by TransactionOperation. There
 * is one such TransactionOperation for 'insert', 'erase' etc. The
 * TransactionOperations form two linked lists - one stored in the Transaction
 * ("all operations from this Transaction") and another one stored in the
 * TransactionNode ("all operations on the same key").
 *
 * All Transactions in an Environment for a linked list, where the tail is
 * the chronologically newest Transaction and the head is the oldest
 * (see Transaction::get_newer and Transaction::get_older).
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef HAM_TXN_H
#define HAM_TXN_H

#include "0root/root.h"

#include <string>

// Always verify that a file of level N does not include headers > N!
#include "1base/dynamic_array.h"
#include "1base/error.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

//
// A helper structure; ham_txn_t is declared in ham/hamsterdb.h as an
// opaque C structure, but internally we use a C++ class. The ham_txn_t
// struct satisfies the C compiler, and internally we just cast the pointers.
//
struct ham_txn_t
{
  int dummy;
};

namespace hamsterdb {

struct Context;
class Environment;

//
// An abstract base class for a Transaction. Overwritten for local and
// remote implementations
//
class Transaction
{
  protected:
    enum {
      // Transaction was aborted
      kStateAborted   = 0x10000,

      // Transaction was committed
      kStateCommitted = 0x20000
    };

  public:
    // Constructor; "begins" the Transaction
    // supported flags: HAM_TXN_READ_ONLY, HAM_TXN_TEMPORARY
    Transaction(Environment *env, const char *name, uint32_t flags)
      : m_id(0), m_env(env), m_flags(flags), m_next(0), m_cursor_refcount(0) {
        if (name)
          m_name = name;
    }

    // Destructor
    virtual ~Transaction() { }

    // Commits the Transaction
    virtual void commit(uint32_t flags = 0) = 0;

    // Aborts the Transaction
    virtual void abort(uint32_t flags = 0) = 0;

    // Returns true if the Transaction was aborted
    bool is_aborted() const {
      return (m_flags & kStateAborted) != 0;
    }

    // Returns true if the Transaction was committed
    bool is_committed() const {
      return (m_flags & kStateCommitted) != 0;
    }

    // Returns the unique id of this Transaction
    uint64_t get_id() const {
      return (m_id);
    }

    // Returns the environment pointer
    Environment *get_env() const {
      return (m_env);
    }

    // Returns the txn name
    const std::string &get_name() const {
      return (m_name);
    }

    // Returns the flags
    uint32_t get_flags() const {
      return (m_flags);
    }

    // Returns the cursor refcount (numbers of Cursors using this Transaction)
    uint32_t get_cursor_refcount() const {
      return (m_cursor_refcount);
    }

    // Increases the cursor refcount (numbers of Cursors using this Transaction)
    void increase_cursor_refcount() {
      m_cursor_refcount++;
    }

    // Decreases the cursor refcount (numbers of Cursors using this Transaction)
    void decrease_cursor_refcount() {
      ham_assert(m_cursor_refcount > 0);
      m_cursor_refcount--;
    }

    // Returns the memory buffer for the key data.
    // Used to allocate array in ham_find, ham_cursor_move etc. which is
    // then returned to the user.
    ByteArray &key_arena() {
      return (m_key_arena);
    }

    // Returns the memory buffer for the record data.
    // Used to allocate array in ham_find, ham_cursor_move etc. which is
    // then returned to the user.
    ByteArray &record_arena() {
      return (m_record_arena);
    }

    // Returns the next Transaction in the linked list */
    Transaction *get_next() const {
      return (m_next);
    }

    // Sets the next Transaction in the linked list */
    void set_next(Transaction *n) {
      m_next = n;
    }

  protected:
    // the id of this Transaction
    uint64_t m_id;

    // the Environment pointer
    Environment *m_env;

    // flags for this Transaction
    uint32_t m_flags;

    // the Transaction name
    std::string m_name;

    // the linked list of all transactions
    Transaction *m_next;

    // reference counter for cursors (number of cursors attached to this txn)
    uint32_t m_cursor_refcount;

    // this is where key->data points to when returning a key to the user
    ByteArray m_key_arena;

    // this is where record->data points to when returning a record to the user
    ByteArray m_record_arena;

  private:
    friend class Journal;

    // Sets the unique id of this Transaction; the journal needs this to patch
    // in the id when recovering a Transaction 
    void set_id(uint64_t id) {
      m_id = id;
    }
};


//
// An abstract base class for the TransactionManager. Overwritten for local and
// remote implementations.
//
// The TransactionManager is part of the Environment and manages all
// Transactions.
//
class TransactionManager
{
  public:
    // Constructor
    TransactionManager(Environment *env)
      : m_env(env), m_oldest_txn(0), m_newest_txn(0) {
    }

    // Destructor
    virtual ~TransactionManager() { }

    // Begins a new Transaction
    virtual void begin(Transaction *txn) = 0;

    // Commits a Transaction; the derived subclass has to take care of
    // flushing and/or releasing memory
    virtual ham_status_t commit(Transaction *txn, uint32_t flags = 0) = 0;

    // Aborts a Transaction; the derived subclass has to take care of
    // flushing and/or releasing memory
    virtual ham_status_t abort(Transaction *txn, uint32_t flags = 0) = 0;

    // Flushes committed (queued) transactions
    virtual void flush_committed_txns(Context *context = 0) = 0;

    // Returns the oldest transaction which not yet flushed to disk
    Transaction *get_oldest_txn() {
      return (m_oldest_txn);
    }

    // Returns the newest transaction which not yet flushed to disk
    Transaction *get_newest_txn() {
      return (m_newest_txn);
    }

  protected:
    // Adds a new transaction to this Environment
    void append_txn_at_tail(Transaction *txn) {
      if (!m_newest_txn) {
        ham_assert(m_oldest_txn == 0);
        m_oldest_txn = txn;
        m_newest_txn = txn;
      }
      else {
        m_newest_txn->set_next(txn);
        m_newest_txn = txn;
        /* if there's no oldest txn (this means: all txn's but the
         * current one were already flushed) then set this txn as
         * the oldest txn */
        if (!m_oldest_txn)
          m_oldest_txn = txn;
      }
    }

    // Removes a transaction from this Environment
    void remove_txn_from_head(Transaction *txn) {
      if (m_newest_txn == txn)
        m_newest_txn = 0;

      ham_assert(m_oldest_txn == txn);
      m_oldest_txn = txn->get_next();
    }

    // The Environment which created this TransactionManager
    Environment *m_env;

    // The head of the transaction list (the oldest transaction)
    Transaction *m_oldest_txn;

    // The tail of the transaction list (the youngest/newest transaction)
    Transaction *m_newest_txn;
};

} // namespace hamsterdb

#endif /* HAM_TXN_H */
