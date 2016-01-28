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
class TransactionNode;
class TransactionIndex;
class TransactionCursor;
class LocalTransaction;
class LocalDatabase;
class LocalEnvironment;


//
// The TransactionOperation class describes a single operation (i.e.
// insert or erase) in a Transaction.
//
class TransactionOperation
{
  public:
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

    // Returns the flags
    uint32_t get_flags() const {
      return (m_flags);
    }

    // This Operation was flushed to disk
    void set_flushed() {
      m_flags |= kIsFlushed;
    }

    // Returns the original flags of ups_insert/ups_cursor_insert/ups_erase...
    uint32_t get_orig_flags() const {
      return (m_orig_flags);
    }

    // Returns the referenced duplicate id
    uint32_t get_referenced_dupe() const {
      return (m_referenced_dupe);
    }

    // Sets the referenced duplicate id
    void set_referenced_dupe(uint32_t id) {
      m_referenced_dupe = id;
    }

    // Returns a pointer to the Transaction of this update
    LocalTransaction *get_txn() {
      return (m_txn);
    }

    // Returns a pointer to the parent node of this update */
    TransactionNode *get_node() {
      return (m_node);
    }

    // Returns the lsn of this operation
    uint64_t get_lsn() const {
      return (m_lsn);
    }

    // Returns the key of this operation
    ups_key_t *get_key() {
      return (&m_key);
    }

    // Returns the record of this operation
    ups_record_t *get_record() {
      return (&m_record);
    }

    // Returns the list of Cursors coupled to this operation
    TransactionCursor *cursor_list() {
      return (m_cursor_list);
    }

    // Sets the list of Cursors coupled to this operation
    void set_cursor_list(TransactionCursor *cursors) {
      m_cursor_list = cursors;
    }

    // Returns the next TransactionOperation which modifies the
    // same TransactionNode
    TransactionOperation *get_next_in_node() {
      return (m_node_next);
    }

    // Returns the previous TransactionOperation which modifies the
    // same TransactionNode
    TransactionOperation *get_previous_in_node() {
      return (m_node_prev);
    }

    // Returns the next TransactionOperation in the same Transaction
    TransactionOperation *get_next_in_txn() {
      return (m_txn_next);
    }

    // Returns the previous TransactionOperation in the same Transaction
    TransactionOperation *get_previous_in_txn() {
      return (m_txn_prev);
    }

  private:
    friend class TransactionNode;
    friend struct TransactionFactory;

    // Initialization
    void initialize(LocalTransaction *txn, TransactionNode *node,
                    uint32_t flags, uint32_t orig_flags, uint64_t lsn,
                    ups_key_t *key, ups_record_t *record);

    // Destructor
    void destroy();

    // Sets the next TransactionOperation which modifies the
    // same TransactionNode
    void set_next_in_node(TransactionOperation *next) {
      m_node_next = next;
    }

    // Sets the previous TransactionOperation which modifies the
    // same TransactionNode
    void set_previous_in_node(TransactionOperation *prev) {
      m_node_prev = prev;
    }

    // Sets the next TransactionOperation in the same Transaction
    void set_next_in_txn(TransactionOperation *next) {
      m_txn_next = next;
    }

    // Sets the previous TransactionOperation in the same Transaction
    void set_previous_in_txn(TransactionOperation *prev) {
      m_txn_prev = prev;
    }

    // the Transaction of this operation
    LocalTransaction *m_txn;

    // the parent node
    TransactionNode *m_node;

    // flags and type of this operation; defined in this file
    uint32_t m_flags;

    // the original flags of this operation, used when calling
    // ups_cursor_insert, ups_insert, ups_erase etc
    uint32_t m_orig_flags;

    // the referenced duplicate id (if neccessary) - used if this is
    // i.e. a ups_cursor_erase, ups_cursor_overwrite or ups_cursor_insert
    // with a DUPLICATE_AFTER/BEFORE flag
    // this is 1-based (like dupecache-index, which is also 1-based)
    uint32_t m_referenced_dupe;

    // the log serial number (lsn) of this operation
    uint64_t m_lsn;

    // a linked list of cursors which are attached to this operation
    TransactionCursor *m_cursor_list;

    // next in linked list (managed in TransactionNode)
    TransactionOperation *m_node_next;

    // previous in linked list (managed in TransactionNode)
    TransactionOperation *m_node_prev;

    // next in linked list (managed in Transaction)
    TransactionOperation *m_txn_next;

    // previous in linked list (managed in Transaction)
    TransactionOperation *m_txn_prev;

    // the key which is inserted or overwritten
    ups_key_t m_key;

    // the record which is inserted or overwritten
    ups_record_t m_record;

    // Storage for record->data. This saves us one memory allocation.
    uint8_t m_data[1];
};


//
// A node in the Transaction Index, used as the node structure in rb.h.
// Manages a group of TransactionOperation objects which all modify the
// same key.
//
// To avoid chicken-egg problems when inserting a new TransactionNode
// into the TransactionTree, it is possible to assign a temporary key
// to this node. However, as soon as an operation is attached to this node,
// the TransactionNode class will use the key structure in this operation.
//
// This basically avoids one memory allocation.
//
class TransactionNode
{
  public:
    // Constructor;
    // The default parameters are required for the compilation of rb.h.
    // |key| is just a temporary pointer which allows to create a
    // TransactionNode without further memory allocations/copying. The actual
    // key is then fetched from |m_oldest_op| as soon as this node is fully
    // initialized.
    TransactionNode(LocalDatabase *db = 0, ups_key_t *key = 0);

    // Destructor; removes this node from the tree, unless |dont_insert|
    // was set to true
    ~TransactionNode();

    // Returns the database
    LocalDatabase *get_db() {
      return (m_db);
    }

    // Returns the modified key
    ups_key_t *get_key() {
      return (m_oldest_op ? m_oldest_op->get_key() : m_key);
    }

    // Retrieves the next larger sibling of a given node, or NULL if there
    // is no sibling
    TransactionNode *get_next_sibling();

    // Retrieves the previous larger sibling of a given node, or NULL if there
    // is no sibling
    TransactionNode *get_previous_sibling();

    // Returns the first (oldest) TransactionOperation in this node
    TransactionOperation *get_oldest_op() {
      return (m_oldest_op);
    };

    // Sets the first (oldest) TransactionOperation in this node
    void set_oldest_op(TransactionOperation *oldest) {
      m_oldest_op = oldest;
    }

    // Returns the last (newest) TransactionOperation in this node
    TransactionOperation *get_newest_op() {
      return (m_newest_op);
    };

    // Sets the last (newest) TransactionOperation in this node
    void set_newest_op(TransactionOperation *newest) {
      m_newest_op = newest;
    }

    // Appends an actual operation to this node
    TransactionOperation *append(LocalTransaction *txn, uint32_t orig_flags,
                uint32_t flags, uint64_t lsn, ups_key_t *key,
                ups_record_t *record);

    // red-black tree stub, required for rb.h
    rb_node(TransactionNode) node;

  private:
    friend struct TxnFixture;

    // the database - need this to get the compare function
    LocalDatabase *m_db;

    // the linked list of operations - head is oldest operation
    TransactionOperation *m_oldest_op;

    // the linked list of operations - tail is newest operation
    TransactionOperation *m_newest_op;

    // Pointer to the key data; only used as long as there are no operations
    // attached. Otherwise we have a chicken-egg problem in rb.h.
    ups_key_t *m_key;
};


//
// Each Database has a binary tree which stores the current Transaction
// operations; this tree is implemented in TransactionIndex
//
class TransactionIndex
{
  public:
    // Traverses a TransactionIndex; for each node, a callback is executed
    struct Visitor {
      virtual void visit(Context *context, TransactionNode *node) = 0;
    };

    // Constructor
    TransactionIndex(LocalDatabase *db);

    // Destructor; frees all nodes and their operations
    ~TransactionIndex();

    // Stores a new TransactionNode in the index
    void store(TransactionNode *node);

    // Removes a TransactionNode from the index
    void remove(TransactionNode *node);

    // Visits every node in the TransactionTree
    void enumerate(Context *context, Visitor *visitor);

    // Returns an opnode for an optree; if a node with this
    // key already exists then the existing node is returned, otherwise NULL.
    // |flags| can be UPS_FIND_GEQ_MATCH, UPS_FIND_LEQ_MATCH etc
    TransactionNode *get(ups_key_t *key, uint32_t flags);

    // Returns the first (= "smallest") node of the tree, or NULL if the
    // tree is empty
    TransactionNode *get_first();

    // Returns the last (= "greatest") node of the tree, or NULL if the
    // tree is empty
    TransactionNode *get_last();

    // Returns the key count of this index
    uint64_t count(Context *context, LocalTransaction *txn, bool distinct);

 // private: //TODO re-enable this; currently disabled because rb.h needs it
    // the Database for all operations in this tree
    LocalDatabase *m_db;

    // stuff for rb.h
    TransactionNode *rbt_root;
    TransactionNode rbt_nil;
};


//
// A local Transaction
//
class LocalTransaction : public Transaction
{
  public:
    // Constructor; "begins" the Transaction
    // supported flags: UPS_TXN_READ_ONLY, UPS_TXN_TEMPORARY
    LocalTransaction(LocalEnvironment *env, const char *name, uint32_t flags);

    // Destructor; frees all TransactionOperation structures associated
    // with this Transaction
    virtual ~LocalTransaction();

    // Commits the Transaction
    void commit(uint32_t flags = 0);

    // Aborts the Transaction
    void abort(uint32_t flags = 0);

    // Returns the first (or 'oldest') TransactionOperation of this Transaction
    TransactionOperation *get_oldest_op() const {
      return (m_oldest_op);
    }

    // Sets the first (or 'oldest') TransactionOperation of this Transaction
    void set_oldest_op(TransactionOperation *op) {
      m_oldest_op = op;
    }

    // Returns the last (or 'newest') TransactionOperation of this Transaction
    TransactionOperation *get_newest_op() const {
      return (m_newest_op);
    }

    // Sets the last (or 'newest') TransactionOperation of this Transaction
    void set_newest_op(TransactionOperation *op) {
      if (op) {
        m_op_counter++;
        m_accum_data_size += op->get_record()
                            ? op->get_record()->size
                            : 0;
        m_accum_data_size += op->get_node()->get_key()->size;
      }
      m_newest_op = op;
    }

    // Returns the number of operations attached to this Transaction
    int get_op_counter() const {
      return (m_op_counter);
    }

    // Returns the accumulated data size of all operations
    int get_accum_data_size() const {
      return (m_accum_data_size);
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

    // For counting the operations
    int m_op_counter;

    // The approximate accumulated memory consumed by this Transaction
    // (sums up key->size and record->size over all operations)
    int m_accum_data_size;
};


//
// A TransactionManager for local Transactions
//
class LocalTransactionManager : public TransactionManager
{
  public:
    // Constructor
    LocalTransactionManager(Environment *env);

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

    // Increments the global transaction ID and returns the new value. 
    uint64_t get_incremented_txn_id() {
      return (++m_txn_id);
    }

    // Returns the current transaction ID; only for testing!
    uint64_t test_get_txn_id() const {
      return (m_txn_id);
    }

    // Sets the current transaction ID; used by the Journal to
    // reset the original txn id during recovery.
    void set_txn_id(uint64_t id) {
      m_txn_id = id;
    }

  private:
    void flush_committed_txns_impl(Context *context);

    // Flushes a single committed Transaction; returns the lsn of the
    // last operation in this transaction
    uint64_t flush_txn(Context *context, LocalTransaction *txn);

    // Casts m_env to a LocalEnvironment
    LocalEnvironment *lenv() {
      return ((LocalEnvironment *)m_env);
    }

    // Flushes committed transactions if there are enough committed
    // transactions waiting to be flushed, or if other conditions apply
    void maybe_flush_committed_txns(Context *context);

    // The current transaction ID
    uint64_t m_txn_id;
};

} // namespace upscaledb

#endif /* UPS_TXN_LOCAL_H */
