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
#include "rb.h"

namespace hamsterdb {

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
    ham_u32_t get_flags() const {
      return (m_flags);
    }

    // This Operation was flushed to disk
    void set_flushed() {
      m_flags |= kIsFlushed;
    }

    // Returns the original flags of ham_insert/ham_cursor_insert/ham_erase...
    ham_u32_t get_orig_flags() const {
      return (m_orig_flags);
    }

    // Returns the referenced duplicate id
    ham_u32_t get_referenced_dupe() const {
      return (m_referenced_dupe);
    }

    // Sets the referenced duplicate id
    void set_referenced_dupe(ham_u32_t id) {
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
    ham_u64_t get_lsn() const {
      return (m_lsn);
    }

    // Returns the record of this operation
    ham_record_t *get_record() {
      return (&m_record);
    }

    // Returns the list of Cursors coupled to this operation
    TransactionCursor *get_cursor_list() {
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
                    ham_u32_t flags, ham_u32_t orig_flags, ham_u64_t lsn,
                    ham_record_t *record);

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
    ham_u32_t m_flags;

    // the original flags of this operation, used when calling
    // ham_cursor_insert, ham_insert, ham_erase etc
    ham_u32_t m_orig_flags;

    // the referenced duplicate id (if neccessary) - used if this is
    // i.e. a ham_cursor_erase, ham_cursor_overwrite or ham_cursor_insert
    // with a DUPLICATE_AFTER/BEFORE flag
    // this is 1-based (like dupecache-index, which is also 1-based)
    ham_u32_t m_referenced_dupe;

    // the log serial number (lsn) of this operation
    ham_u64_t m_lsn;

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

    // the record which is inserted or overwritten
    ham_record_t m_record;

    // Storage for record->data. This saves us one memory allocation.
    ham_u8_t m_data[1];
};


//
// A node in the Transaction Index, used as the node structure in rb.h.
// Manages a group of TransactionOperation objects which all modify the
// same key.
//
class TransactionNode
{
  public:
    // Constructor; creates a deep copy of |key|. Inserts itself into the
    // TransactionIndex of |db| (unless |dont_insert| is true).
    // The default parameters are required for the compilation of rb.h.
    TransactionNode(LocalDatabase *db = 0, ham_key_t *key = 0);

    // Destructor; removes this node from the tree, unless |dont_insert|
    // was set to true
    ~TransactionNode();

    // Returns the database
    LocalDatabase *get_db() {
      return (m_db);
    }

    // Returns the modified key
    ham_key_t *get_key() {
      return (&m_key);
    }

    // Appends an actual operation to this node
    TransactionOperation *append(LocalTransaction *txn, ham_u32_t orig_flags,
              ham_u32_t flags, ham_u64_t lsn, ham_record_t *record);

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

    // red-black tree stub, required for rb.h
    rb_node(TransactionNode) node;

  private:
    // the database - need this to get the compare function
    LocalDatabase *m_db;

    // the linked list of operations - head is oldest operation
    TransactionOperation *m_oldest_op;

    // the linked list of operations - tail is newest operation
    TransactionOperation *m_newest_op;

    // this is the key which is modified in this node
    ham_key_t m_key;

    // Storage for key->data. This saves us one memory allocation.
    ham_u8_t m_data[1];
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
      virtual void visit(TransactionNode *node) = 0;
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
    void enumerate(Visitor *visitor);

    // Returns an opnode for an optree; if a node with this
    // key already exists then the existing node is returned, otherwise NULL.
    // |flags| can be HAM_FIND_GEQ_MATCH, HAM_FIND_LEQ_MATCH etc
    TransactionNode *get(ham_key_t *key, ham_u32_t flags);

    // Returns the first (= "smallest") node of the tree, or NULL if the
    // tree is empty
    TransactionNode *get_first();

    // Returns the last (= "greatest") node of the tree, or NULL if the
    // tree is empty
    TransactionNode *get_last();

    // Returns the key count of this index
    ham_u64_t get_key_count(LocalTransaction *txn, ham_u32_t flags);

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
    enum {
      // flush if this limit is exceeded
      kFlushTxnThreshold = 64,

      // flush if this limit is exceeded
      kFlushOperationsThreshold = kFlushTxnThreshold * 20,

      // flush if this limit is exceeded
      kFlushBytesThreshold = 1024 * 1024 // 1 mb - same as journal buffer
    };

  public:
    // Constructor
    LocalTransactionManager(Environment *env);

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

    // Increments the global transaction ID and returns the new value. 
    ham_u64_t get_incremented_txn_id() {
      return (++m_txn_id);
    }

    // Returns the current transaction ID; only for testing!
    ham_u64_t test_get_txn_id() const {
      return (m_txn_id);
    }

    // Sets the current transaction ID; used by the Journal to
    // reset the original txn id during recovery.
    void set_txn_id(ham_u64_t id) {
      m_txn_id = id;
    }

  private:
    // Flushes a single committed Transaction
    void flush_txn(LocalTransaction *txn);

    // Casts m_env to a LocalEnvironment
    LocalEnvironment *get_local_env() {
      return ((LocalEnvironment *)m_env);
    }

    // Flushes committed transactions if there are enough committed
    // transactions waiting to be flushed, or if other conditions apply
    void maybe_flush_committed_txns();

    // The current transaction ID
    ham_u64_t m_txn_id;

    // Number of Transactions waiting to be flushed
    int m_queued_txn_for_flush;

    // Combined number of Operations in these transactions waiting to be flushed
    int m_queued_ops_for_flush;

    // Approx. memory consumption of all these operations in the flush queue
    int m_queued_bytes_for_flush;

    // Threshold for transactio queue
    int m_txn_threshold;

    // Threshold for transactio queue
    int m_ops_threshold;

    // Threshold for transactio queue
    int m_bytes_threshold;
};


} // namespace hamsterdb

#endif /* HAM_TXN_LOCAL_H__ */
