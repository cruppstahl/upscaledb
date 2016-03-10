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
class TxnNode;
class TxnIndex;
struct TxnCursor;
class LocalTxn;
class LocalDatabase;
class LocalEnvironment;


//
// The TxnOperation class describes a single operation (i.e.
// insert or erase) in a Txn.
//
struct TxnOperation
{
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

    // Storage for record->data. This saves us one memory allocation.
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
struct TxnNode
{
    // Constructor;
    // The default parameters are required for the compilation of rb.h.
    // |key| is just a temporary pointer which allows to create a
    // TxnNode without further memory allocations/copying. The actual
    // key is then fetched from |m_oldest_op| as soon as this node is fully
    // initialized.
    TxnNode(LocalDatabase *db = 0, ups_key_t *key = 0);

    // Destructor; removes this node from the tree, unless |dont_insert|
    // was set to true
    ~TxnNode();

    // Returns the modified key
    ups_key_t *key() {
      return (m_oldest_op ? &m_oldest_op->key : _key);
    }

    // Retrieves the next larger sibling of a given node, or NULL if there
    // is no sibling
    TxnNode *get_next_sibling();

    // Retrieves the previous larger sibling of a given node, or NULL if there
    // is no sibling
    TxnNode *get_previous_sibling();

    // Returns the first (oldest) TxnOperation in this node
    TxnOperation *get_oldest_op() {
      return (m_oldest_op);
    };

    // Sets the first (oldest) TxnOperation in this node
    void set_oldest_op(TxnOperation *oldest) {
      m_oldest_op = oldest;
    }

    // Returns the last (newest) TxnOperation in this node
    TxnOperation *get_newest_op() {
      return (m_newest_op);
    };

    // Sets the last (newest) TxnOperation in this node
    void set_newest_op(TxnOperation *newest) {
      m_newest_op = newest;
    }

    // Appends an actual operation to this node
    TxnOperation *append(LocalTxn *txn, uint32_t orig_flags,
                uint32_t flags, uint64_t lsn, ups_key_t *key,
                ups_record_t *record);

    // red-black tree stub, required for rb.h
    rb_node(TxnNode) node;

    // the database - need this to get the compare function
    LocalDatabase *db;

    // the linked list of operations - head is oldest operation
    TxnOperation *m_oldest_op;

    // the linked list of operations - tail is newest operation
    TxnOperation *m_newest_op;

    // Pointer to the key data; only used as long as there are no operations
    // attached. Otherwise we have a chicken-egg problem in rb.h.
    ups_key_t *_key;
};


//
// Each Database has a binary tree which stores the current Txn
// operations; this tree is implemented in TxnIndex
//
class TxnIndex
{
  public:
    // Traverses a TxnIndex; for each node, a callback is executed
    struct Visitor {
      virtual void visit(Context *context, TxnNode *node) = 0;
    };

    // Constructor
    TxnIndex(LocalDatabase *db);

    // Destructor; frees all nodes and their operations
    ~TxnIndex();

    // Stores a new TxnNode in the index
    void store(TxnNode *node);

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
    TxnNode *get_first();

    // Returns the last (= "greatest") node of the tree, or NULL if the
    // tree is empty
    TxnNode *get_last();

    // Returns the key count of this index
    uint64_t count(Context *context, LocalTxn *txn, bool distinct);

 // private: //TODO re-enable this; currently disabled because rb.h needs it
    // the Database for all operations in this tree
    LocalDatabase *m_db;

    // stuff for rb.h
    TxnNode *rbt_root;
    TxnNode rbt_nil;
};


//
// A local Txn
//
class LocalTxn : public Txn
{
  public:
    // Constructor; "begins" the Txn
    // supported flags: UPS_TXN_READ_ONLY, UPS_TXN_TEMPORARY
    LocalTxn(LocalEnvironment *env, const char *name, uint32_t flags);

    // Destructor; frees all TxnOperation structures associated
    // with this Txn
    virtual ~LocalTxn();

    // Commits the Txn
    void commit(uint32_t flags = 0);

    // Aborts the Txn
    void abort(uint32_t flags = 0);

    // Returns the first (or 'oldest') TxnOperation of this Txn
    TxnOperation *get_oldest_op() const {
      return (m_oldest_op);
    }

    // Sets the first (or 'oldest') TxnOperation of this Txn
    void set_oldest_op(TxnOperation *op) {
      m_oldest_op = op;
    }

    // Returns the last (or 'newest') TxnOperation of this Txn
    TxnOperation *get_newest_op() const {
      return (m_newest_op);
    }

    // Sets the last (or 'newest') TxnOperation of this Txn
    void set_newest_op(TxnOperation *op) {
      if (op) {
        m_op_counter++;
        m_accum_data_size += op->record.size;
        m_accum_data_size += op->node->key()->size;
      }
      m_newest_op = op;
    }

    // Returns the number of operations attached to this Txn
    int get_op_counter() const {
      return (m_op_counter);
    }

    // Returns the accumulated data size of all operations
    int get_accum_data_size() const {
      return (m_accum_data_size);
    }

  private:
    friend struct Journal;
    friend struct TxnFixture;
    friend struct TxnCursorFixture;

    // Frees the internal structures; releases all the memory. This is
    // called in the destructor, but also when aborting a Txn
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
    TxnOperation *m_oldest_op;

    // the linked list of operations - tail is newest operation
    TxnOperation *m_newest_op;

    // For counting the operations
    int m_op_counter;

    // The approximate accumulated memory consumed by this Txn
    // (sums up key->size and record->size over all operations)
    int m_accum_data_size;
};


//
// A TxnManager for local Txns
//
struct LocalTxnManager : public TxnManager
{
  public:
    // Constructor
    LocalTxnManager(Environment *env)
      : TxnManager(env), _txn_id(0) {
    }

    // Begins a new Txn
    virtual void begin(Txn *txn);

    // Commits a Txn; the derived subclass has to take care of
    // flushing and/or releasing memory
    virtual ups_status_t commit(Txn *txn, uint32_t flags = 0);

    // Aborts a Txn; the derived subclass has to take care of
    // flushing and/or releasing memory
    virtual ups_status_t abort(Txn *txn, uint32_t flags = 0);

    // Flushes committed (queued) transactions
    virtual void flush_committed_txns(Context *context = 0);

    // Increments the global transaction ID and returns the new value. 
    uint64_t incremented_txn_id() {
      return ++_txn_id;
    }

    // Sets the current transaction ID; used by the Journal to
    // reset the original txn id during recovery.
    void set_txn_id(uint64_t id) {
      _txn_id = id;
    }

    void flush_committed_txns_impl(Context *context);

    // Flushes a single committed Txn; returns the lsn of the
    // last operation in this transaction
    uint64_t flush_txn(Context *context, LocalTxn *txn);

    // Casts env to a LocalEnvironment
    LocalEnvironment *lenv() {
      return ((LocalEnvironment *)env);
    }

    // Flushes committed transactions if there are enough committed
    // transactions waiting to be flushed, or if other conditions apply
    void maybe_flush_committed_txns(Context *context);

    // The current transaction ID
    uint64_t _txn_id;
};

} // namespace upscaledb

#endif /* UPS_TXN_LOCAL_H */
