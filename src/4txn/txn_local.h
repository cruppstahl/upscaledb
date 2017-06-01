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

#ifndef UPS_TXN_LOCAL_H
#define UPS_TXN_LOCAL_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "1rb/rb.h"
#include "4txn/txn.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

struct Context;
struct TxnNode;
struct TxnIndex;
struct TxnCursor;
struct LocalTxn;
struct LocalDb;
struct LocalEnv;


//
// The TxnOperation class describes a single operation (i.e.
// insert or erase) in a Txn.
//
struct TxnOperation {
  enum {
    // a NOP operation (empty)
    kNop              = 0x000000u,

    // txn operation is an insert
    kInsert           = 0x010000u,

    // txn operation is an insert w/ overwrite
    kInsertOverwrite  = 0x020000u,

    // txn operation is an insert w/ duplicate
    kInsertDuplicate  = 0x040000u,

    // txn operation erases the key
    kErase            = 0x080000u,

    // txn operation was already flushed
    kIsFlushed        = 0x100000u
  };

  // This Operation was flushed to disk
  void set_flushed() {
    flags |= kIsFlushed;
  }

  // Initialization
  // TODO use placement new??
  void initialize(LocalTxn *txn, TxnNode *node,
                  uint32_t flags, uint32_t orig_flags, uint64_t lsn,
                  ups_key_t *key, ups_record_t *record);

  // Destructor
  void destroy();

  // the Txn of this operation
  // TODO reqired?
  LocalTxn *txn;

  // the parent node
  TxnNode *node;

  // flags and type of this operation; defined in this file
  uint32_t flags;

  // the original flags of this operation, used when calling
  // ups_cursor_insert, ups_insert, ups_erase etc
  uint32_t original_flags;

  // the referenced duplicate id (if neccessary) - used if this is
  // i.e. a ups_cursor_erase, ups_cursor_overwrite or ups_cursor_insert
  // with a DUPLICATE_AFTER/BEFORE flag
  // this is 1-based (like dupecache-index, which is also 1-based)
  uint32_t referenced_duplicate;

  // the log serial number (lsn) of this operation
  uint64_t lsn;

  // a linked list of cursors which are attached to this operation
  TxnCursor *cursor_list;

  // next in linked list (managed in TxnNode)
  TxnOperation *next_in_node;

  // previous in linked list (managed in TxnNode)
  TxnOperation *previous_in_node;

  // next in linked list (managed in Txn)
  TxnOperation *next_in_txn;

  // previous in linked list (managed in Txn)
  TxnOperation *previous_in_txn;

  // the key which is inserted or overwritten
  ups_key_t key;

  // the record which is inserted or overwritten
  ups_record_t record;

  // Storage for key->data and record->data. This saves us two
  // memory allocations.
  uint8_t _data[1];
};


//
// A node in the Txn Index, used as the node structure in rb.h.
// Manages a group of TxnOperation objects which all modify the
// same key.
//
// To avoid chicken-egg problems when inserting a new TxnNode
// into the TxnTree, it is possible to assign a temporary key
// to this node. However, as soon as an operation is attached to this node,
// the TxnNode class will use the key structure in this operation.
//
// This basically avoids one memory allocation.
//
struct TxnNode {
  // Constructor;
  // The default parameters are required for the compilation of rb.h.
  // |key| is just a temporary pointer which allows to create a
  // TxnNode without further memory allocations/copying. The actual
  // key is then fetched from |oldest_op| as soon as this node is fully
  // initialized.
  TxnNode(LocalDb *db = 0, ups_key_t *key = 0);

  // Returns the modified key
  ups_key_t *key() {
    return oldest_op ? &oldest_op->key : _key;
  }

  // Retrieves the next larger sibling of a given node, or NULL if there
  // is no sibling
  TxnNode *next_sibling();

  // Retrieves the previous larger sibling of a given node, or NULL if there
  // is no sibling
  TxnNode *previous_sibling();

  // Appends an actual operation to this node
  TxnOperation *append(LocalTxn *txn, uint32_t orig_flags,
              uint32_t flags, uint64_t lsn, ups_key_t *key,
              ups_record_t *record);

  // red-black tree stub, required for rb.h
  rb_node(TxnNode) node;

  // the database - need this to get the compare function
  LocalDb *db;

  // the linked list of operations - head is oldest operation
  TxnOperation *oldest_op;

  // the linked list of operations - tail is newest operation
  TxnOperation *newest_op;

  // Pointer to the key data; only used as long as there are no operations
  // attached. Otherwise we have a chicken-egg problem in rb.h.
  ups_key_t *_key;
};


//
// Each Database has a binary tree which stores the current Txn
// operations; this tree is implemented in TxnIndex
//
struct TxnIndex {
  // Traverses a TxnIndex; for each node, a callback is executed
  struct Visitor {
    virtual void visit(Context *context, TxnNode *node) = 0;
  };

  // Constructor
  TxnIndex(LocalDb *db);

  // Destructor; frees all nodes and their operations
  ~TxnIndex();

  // Stores a new TxnNode in the index, but only if the node does not yet
  // exist. Returns the new (or existing) node.
  TxnNode *store(ups_key_t *key, bool *node_created);

  // Removes a TxnNode from the index
  void remove(TxnNode *node);

  // Visits every node in the TxnTree
  void enumerate(Context *context, Visitor *visitor);

  // Returns an opnode for an optree; if a node with this
  // key already exists then the existing node is returned, otherwise NULL.
  // |flags| can be UPS_FIND_GEQ_MATCH, UPS_FIND_LEQ_MATCH etc
  TxnNode *get(ups_key_t *key, uint32_t flags);

  // Returns the first (= "smallest") node of the tree, or NULL if the
  // tree is empty
  TxnNode *first();

  // Returns the last (= "greatest") node of the tree, or NULL if the
  // tree is empty
  TxnNode *last();

  // Returns the key count of this index
  uint64_t count(Context *context, LocalTxn *txn, bool distinct);

  // the Database for all operations in this tree
  // TODO is this required?
  LocalDb *db;

  // stuff for rb.h
  TxnNode *rbt_root;
  TxnNode rbt_nil;
};


//
// A local Txn
//
struct LocalTxn : Txn {
  // Constructor; "begins" the Txn
  // supported flags: UPS_TXN_READ_ONLY, UPS_TXN_TEMPORARY
  LocalTxn(LocalEnv *env, const char *name, uint32_t flags);

  // Destructor; frees all TxnOperation structures associated
  // with this Txn
  virtual ~LocalTxn();

  // Commits the Txn
  void commit();

  // Aborts the Txn
  void abort();

  // Frees the internal structures; releases all the memory. This is
  // called in the destructor, but also when aborting a Txn
  // (before it's deleted by the Environment).
  void free_operations();

  // index of the log file descriptor for this transaction [0..1]
  int log_descriptor;

  // the lsn of the "txn begin" operation
  uint64_t lsn;

  // the linked list of operations - head is oldest operation
  TxnOperation *oldest_op;

  // the linked list of operations - tail is newest operation
  TxnOperation *newest_op;
};


//
// A TxnManager for local Txns
//
struct LocalTxnManager : TxnManager {
  // Constructor
  LocalTxnManager(Env *env)
    : TxnManager(env), _txn_id(0) {
  }

  // Begins a new Txn
  virtual void begin(Txn *txn);

  // Commits a Txn; the derived subclass has to take care of
  // flushing and/or releasing memory
  virtual ups_status_t commit(Txn *txn);

  // Aborts a Txn; the derived subclass has to take care of
  // flushing and/or releasing memory
  virtual ups_status_t abort(Txn *txn);

  // Flushes committed (queued) transactions
  virtual void flush_committed_txns(Context *context = 0);

  // Increments the global transaction ID and returns the new value. 
  uint64_t incremented_txn_id() {
    return ++_txn_id;
  }

  // Sets the global transaction ID. Used by the journal during recovery.
  void set_txn_id(uint64_t id) {
    _txn_id = id;
  }

  // Flushes a single committed Txn; returns the lsn of the
  // last operation in this transaction
  uint64_t flush_txn_to_changeset(Context *context, LocalTxn *txn);

  // Casts env to a LocalEnv
  LocalEnv *lenv() const {
    return (LocalEnv *)env;
  }

  // The current transaction ID
  uint64_t _txn_id;
};

} // namespace upscaledb

#endif /* UPS_TXN_LOCAL_H */
