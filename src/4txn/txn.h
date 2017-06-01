/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

/*
 * The upscaledb Transaction ("Txn") implementation
 *
 * upscaledb stores transactions in volatile RAM (with an append-only journal
 * in case the RAM is lost). Each transaction and each modification *in* a 
 * transaction is stored in a complex data structure.
 *
 * When a Database is created, it contains a BtreeIndex for persistent
 * (committed and flushed) data, and a TxnIndex for active transactions
 * and those transactions which were committed but not yet flushed to disk.
 * This TxnTree is implemented as a binary search tree (see rb.h).
 *
 * Each node in the TxnTree is implemented by TxnNode. Each
 * node is identified by its database key, and groups all modifications of this
 * key (of all Transactions!).
 *
 * Each modification in the node is implemented by TxnOperation. There
 * is one such TxnOperation for 'insert', 'erase' etc. The
 * TxnOperations form two linked lists - one stored in the Transaction
 * ("all operations from this Txn") and another one stored in the
 * TxnNode ("all operations on the same key").
 *
 * All transaction in an Environment form a linked list, where the tail is
 * the chronologically newest transaction and the head is the oldest
 * (see Txn::get_newer and Txn::get_older).
 */

#ifndef UPS_TXN_H
#define UPS_TXN_H

#include "0root/root.h"

#include <string>

// Always verify that a file of level N does not include headers > N!
#include "1base/dynamic_array.h"
#include "1base/error.h"
#include "1base/ref_counted.h"
#include "1base/intrusive_list.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

//
// A helper structure; ups_txn_t is declared in ups/upscaledb.h as an
// opaque C structure, but internally we use a C++ class. The ups_txn_t
// struct satisfies the C compiler, and internally we just cast the pointers.
//
struct ups_txn_t {
  int dummy;
};

namespace upscaledb {

struct Context;
struct Env;

//
// An abstract base class for a Txn. Overwritten for local and
// remote implementations
//
struct Txn : ReferenceCounted {
  enum {
    // Txn was aborted
    kStateAborted   = 0x10000,

    // Txn was committed
    kStateCommitted = 0x20000
  };

  // Constructor; "begins" the Txn
  // supported flags: UPS_TXN_READ_ONLY, UPS_TXN_TEMPORARY
  Txn(Env *env_, const char *name_, uint32_t flags_)
    : id(0), env(env_), flags(flags_) {
      if (unlikely(name_ != 0))
        name = name_;
  }

  // Destructor
  virtual ~Txn() { }

  // Commits the Txn
  virtual void commit() = 0;

  // Aborts the Txn
  virtual void abort() = 0;

  // Returns true if the Txn was aborted
  bool is_aborted() const {
    return ISSET(flags, kStateAborted);
  }

  // Returns true if the Txn was committed
  bool is_committed() const {
    return ISSET(flags, kStateCommitted);
  }

  // Returns the next transaction in the linked list of transactions
  Txn *next() const {
    return list_node.next[0];
  }

  // Increases the cursor refcount (numbers of Cursors using this Txn)
  // the id of this Txn
  uint64_t id;

  // the Environment pointer
  Env *env;

  // flags for this Txn
  uint32_t flags;

  // This is a node in a linked list
  IntrusiveListNode<Txn> list_node;

  // the Txn name
  std::string name;

  // this is where key->data points to when returning a key to the user
  ByteArray key_arena;

  // this is where record->data points to when returning a record to the user
  ByteArray record_arena;
};


//
// An abstract base class for the TxnManager. Overwritten for local and
// remote implementations.
//
// The TxnManager is part of the Environment and manages all transactions.
//
struct TxnManager {
  // Constructor
  TxnManager(Env *env_)
    : env(env_) {
  }

  // Destructor
  virtual ~TxnManager() { }

  // Begins a new Txn
  virtual void begin(Txn *txn) = 0;

  // Commits a Txn; the derived subclass has to take care of
  // flushing and/or releasing memory
  virtual ups_status_t commit(Txn *txn) = 0;

  // Aborts a Txn; the derived subclass has to take care of
  // flushing and/or releasing memory
  virtual ups_status_t abort(Txn *txn) = 0;

  // Flushes committed (queued) transactions
  virtual void flush_committed_txns(Context *context = 0) = 0;

  // Adds a new transaction to this Environment
  void append_txn_at_tail(Txn *txn) {
    list.append(txn);
  }

  // Removes a transaction from this Environment
  void remove_txn_from_head(Txn *txn) {
    assert(list.head() == txn);
    list.del(txn);
  }

  Txn *newest_txn() {
    return list.tail();
  }

  Txn *oldest_txn() {
    return list.head();
  }

  // The Environment which created this TxnManager
  Env *env;

  // Double linked list of transaction objects
  IntrusiveList<Txn> list;
};

} // namespace upscaledb

#endif /* UPS_TXN_H */
