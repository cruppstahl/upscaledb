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
 * The upscaledb Txn implementation
 *
 * upscaledb stores Txns in volatile RAM (with an append-only journal
 * in case the RAM is lost). Each Txn and each modification *in* a 
 * Txn is stored in a complex data structure.
 *
 * When a Database is created, it contains a BtreeIndex for persistent
 * (committed and flushed) data, and a TxnIndex for active Txns
 * and those Txns which were committed but not yet flushed to disk.
 * This TxnTree is implemented as a binary search tree (see rb.h).
 *
 * Each node in the TxnTree is implemented by TxnNode. Each
 * node is identified by its database key, and groups all modifications of this
 * key (of all Txns!).
 *
 * Each modification in the node is implemented by TxnOperation. There
 * is one such TxnOperation for 'insert', 'erase' etc. The
 * TxnOperations form two linked lists - one stored in the Txn
 * ("all operations from this Txn") and another one stored in the
 * TxnNode ("all operations on the same key").
 *
 * All Txns in an Environment for a linked list, where the tail is
 * the chronologically newest Txn and the head is the oldest
 * (see Txn::get_newer and Txn::get_older).
 *
 * @exception_safe: unknown
 * @thread_safe: unknown
 */

#ifndef UPS_TXN_H
#define UPS_TXN_H

#include "0root/root.h"

#include <string>

// Always verify that a file of level N does not include headers > N!
#include "1base/dynamic_array.h"
#include "1base/error.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

//
// A helper structure; ups_txn_t is declared in ups/upscaledb.h as an
// opaque C structure, but internally we use a C++ class. The ups_txn_t
// struct satisfies the C compiler, and internally we just cast the pointers.
//
struct ups_txn_t
{
  int dummy;
};

namespace upscaledb {

struct Context;
struct Env;

//
// An abstract base class for a Txn. Overwritten for local and
// remote implementations
//
struct Txn
{
  enum {
    // Txn was aborted
    kStateAborted   = 0x10000,

    // Txn was committed
    kStateCommitted = 0x20000
  };

  // Constructor; "begins" the Txn
  // supported flags: UPS_TXN_READ_ONLY, UPS_TXN_TEMPORARY
  Txn(Env *env_, const char *name_, uint32_t flags_)
    : id(0), env(env_), flags(flags_), next(0), _cursor_refcount(0) {
      if (unlikely(name_ != 0))
        name = name_;
  }

  // Destructor
  virtual ~Txn() { }

  // Commits the Txn
  virtual void commit(uint32_t flags = 0) = 0;

  // Aborts the Txn
  virtual void abort(uint32_t flags = 0) = 0;

  // Returns true if the Txn was aborted
  bool is_aborted() const {
    return isset(flags, kStateAborted);
  }

  // Returns true if the Txn was committed
  bool is_committed() const {
    return isset(flags, kStateCommitted);
  }

  // Increases the cursor refcount (numbers of Cursors using this Txn)
  void increase_cursor_refcount() {
    _cursor_refcount++;
  }

  // Decreases the cursor refcount (numbers of Cursors using this Txn)
  void decrease_cursor_refcount() {
    assert(_cursor_refcount > 0);
    _cursor_refcount--;
  }

  // the id of this Txn
  uint64_t id;

  // the Environment pointer
  Env *env;

  // flags for this Txn
  uint32_t flags;

  // the Txn name
  std::string name;

  // the linked list of all transactions
  Txn *next;

  // reference counter for cursors (number of cursors attached to this txn)
  uint32_t _cursor_refcount;

  // this is where key->data points to when returning a key to the user
  ByteArray key_arena;

  // this is where record->data points to when returning a record to the user
  ByteArray record_arena;
};


//
// An abstract base class for the TxnManager. Overwritten for local and
// remote implementations.
//
// The TxnManager is part of the Environment and manages all
// Txns.
//
struct TxnManager
{
  // Constructor
  TxnManager(Env *env_)
    : env(env_), oldest_txn(0), newest_txn(0) {
  }

  // Destructor
  virtual ~TxnManager() { }

  // Begins a new Txn
  virtual void begin(Txn *txn) = 0;

  // Commits a Txn; the derived subclass has to take care of
  // flushing and/or releasing memory
  virtual ups_status_t commit(Txn *txn, uint32_t flags = 0) = 0;

  // Aborts a Txn; the derived subclass has to take care of
  // flushing and/or releasing memory
  virtual ups_status_t abort(Txn *txn, uint32_t flags = 0) = 0;

  // Flushes committed (queued) transactions
  virtual void flush_committed_txns(Context *context = 0) = 0;

  // Adds a new transaction to this Environment
  void append_txn_at_tail(Txn *txn) {
    if (!newest_txn) {
      assert(oldest_txn == 0);
      oldest_txn = txn;
      newest_txn = txn;
    }
    else {
      newest_txn->next = txn;
      newest_txn = txn;
      /* if there's no oldest txn (this means: all txn's but the
       * current one were already flushed) then set this txn as
       * the oldest txn */
      if (!oldest_txn)
        oldest_txn = txn;
    }
  }

  // Removes a transaction from this Environment
  void remove_txn_from_head(Txn *txn) {
    if (newest_txn == txn)
      newest_txn = 0;

    assert(oldest_txn == txn);
    oldest_txn = txn->next;
  }

  // The Environment which created this TxnManager
  Env *env;

  // The head of the transaction list (the oldest transaction)
  Txn *oldest_txn;

  // The tail of the transaction list (the youngest/newest transaction)
  Txn *newest_txn;
};

} // namespace upscaledb

#endif /* UPS_TXN_H */
