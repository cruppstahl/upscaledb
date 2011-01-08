/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief transactions
 *
 */

#ifndef HAM_TXN_H__
#define HAM_TXN_H__

#include "internal_fwd_decl.h"
#include "rb.h"



#ifdef __cplusplus
extern "C" {
#endif 


/**
 * a single operation in a transaction
 */
typedef struct txn_op_t
{
    /**
     * flags and type of this operation
     *
     * accepted flags are also defined in keys.h - 
     * KEY_BLOB_SIZE_TINY, KEY_BLOB_SIZE_SMALL, KEY_BLOB_SIZE_EMPTY 
     */
    ham_u32_t _flags;

    /** the Transaction of this operation */
    ham_txn_t *_txn;

    /** the parent node */
    struct txn_opnode_t *_node;

    /** next in linked list (managed in txn_opnode_t) */
    struct txn_op_t *_node_next;

    /** previous in linked list (managed in txn_opnode_t) */
    struct txn_op_t *_node_prev;

    /** next in linked list (managed in ham_txn_t) */
    struct txn_op_t *_txn_next;

    /** previous in linked list (managed in ham_txn_t) */
    struct txn_op_t *_txn_prev;

    /** the log serial number (lsn) of this operation */
    ham_u64_t _lsn;

    /** if this op overwrites or erases another operation in the same node,
     * then _other_lsn stores the lsn of the other txn_op (only used when
     * the other txn_op is a duplicate) */
    ham_u64_t _other_lsn;

    /** the record */
    ham_record_t *_record;

    /** a linked list of cursors which are attached to this txn_op */
    struct txn_cursor_t *_cursors;

} txn_op_t;

/** a NOP operation (empty) */
#define TXN_OP_NOP          0x00000u

/** txn operation is an insert */
#define TXN_OP_INSERT       0x10000u

/** txn operation is an insert w/ overwrite */
#define TXN_OP_INSERT_OW    0x20000u

/** txn operation is an insert w/ duplicate */
#define TXN_OP_INSERT_DUP   0x40000u

/** txn operation erases the key (with all duplicates) */
#define TXN_OP_ERASE        0x80000u

/** txn operation erases a duplicate key */
#define TXN_OP_ERASE_DUP    0x100000u

/** txn operation was already flushed */
#define TXN_OP_FLUSHED      0x200000u

/** get flags */
#define txn_op_get_flags(t)                (t)->_flags

/** set flags */
#define txn_op_set_flags(t, f)             (t)->_flags=f

/** get the Transaction pointer */
#define txn_op_get_txn(t)                  (t)->_txn

/** set the Transaction pointer */
#define txn_op_set_txn(t, txn)             (t)->_txn=txn

/** get the parent node pointer */
#define txn_op_get_node(t)                 (t)->_node

/** set the parent node pointer */
#define txn_op_set_node(t, n)              (t)->_node=n

/** get next txn_op_t structure */
#define txn_op_get_next_in_node(t)         (t)->_node_next

/** set next txn_op_t structure */
#define txn_op_set_next_in_node(t, n)      (t)->_node_next=n

/** get previous txn_op_t structure */
#define txn_op_get_previous_in_node(t)     (t)->_node_prev

/** set previous txn_op_t structure */
#define txn_op_set_previous_in_node(t, p)  (t)->_node_prev=p

/** get next txn_op_t structure */
#define txn_op_get_next_in_txn(t)          (t)->_txn_next

/** set next txn_op_t structure */
#define txn_op_set_next_in_txn(t, n)       (t)->_txn_next=n

/** get previous txn_op_t structure */
#define txn_op_get_previous_in_txn(t)      (t)->_txn_prev

/** set next txn_op_t structure */
#define txn_op_set_previous_in_txn(t, p)   (t)->_txn_prev=p

/** get lsn */
#define txn_op_get_lsn(t)                  (t)->_lsn

/** set lsn */
#define txn_op_set_lsn(t, l)               (t)->_lsn=l

/** get lsn of other op */
#define txn_op_get_other_lsn(t)            (t)->_other_lsn

/** set lsn of other op */
#define txn_op_set_other_lsn(t, l)         (t)->_other_lsn=l

/** get record */
#define txn_op_get_record(t)               (t)->_record

/** set record */
#define txn_op_set_record(t, r)            (t)->_record=r

/** get cursor list */
#define txn_op_get_cursors(t)              (t)->_cursors

/** set cursor list */
#define txn_op_set_cursors(t, c)           (t)->_cursors=c

/**
 * add a cursor to this txn_op structure
 */
extern void
txn_op_add_cursor(txn_op_t *op, struct txn_cursor_t *cursor);

/**
 * remove a cursor from this txn_op structure
 */
extern void
txn_op_remove_cursor(txn_op_t *op, struct txn_cursor_t *cursor);


/*
 * a node in the red-black Transaction tree (implemented in rb.h); 
 * a group of Transaction operations which modify the same key
 */
typedef struct txn_opnode_t
{
    /** red-black tree stub */
    rb_node(struct txn_opnode_t) node;

    /** the database - need this pointer for the compare function */
    ham_db_t *_db;

    /** this is the key which is modified */
    ham_key_t *_key;

    /** the parent key */
    struct txn_optree_t *_tree;

    /** the linked list of operations - head is oldest operation */
    txn_op_t *_oldest_op;

    /** the linked list of operations - tail is newest operation */
    txn_op_t *_newest_op;

} txn_opnode_t;

/** get the database */
#define txn_opnode_get_db(t)                    (t)->_db

/** set the database */
#define txn_opnode_set_db(t, db)                (t)->_db=db

/** get pointer to the modified key */
#define txn_opnode_get_key(t)                   (t)->_key

/** set pointer to the modified key */
#define txn_opnode_set_key(t, k)                (t)->_key=k

/** get pointer to the parent tree */
#define txn_opnode_get_tree(t)                  (t)->_tree

/** set pointer to the parent tree */
#define txn_opnode_set_tree(t, tree)            (t)->_tree=tree

/** get pointer to the first (oldest) node in list */
#define txn_opnode_get_oldest_op(t)             (t)->_oldest_op

/** set pointer to the first (oldest) node in list */
#define txn_opnode_set_oldest_op(t, o)          (t)->_oldest_op=o

/** get pointer to the last (newest) node in list */
#define txn_opnode_get_newest_op(t)             (t)->_newest_op

/** set pointer to the last (newest) node in list */
#define txn_opnode_set_newest_op(t, n)          (t)->_newest_op=n


/*
 * each Transaction has one tree per Database for a fast lookup; this
 * is just a normal binary tree
 */
typedef struct txn_optree_t
{
    /* the Database for all operations in this tree */
    ham_db_t *_db;

    /* stuff for rb.h */
    txn_opnode_t *rbt_root;
    txn_opnode_t rbt_nil;

} txn_optree_t;

/** get the database handle of this txn tree */
#define txn_optree_get_db(t)        (t)->_db

/** set the database handle of this txn tree */
#define txn_optree_set_db(t, d)     (t)->_db=d


/**
 * a Transaction structure
 */
struct ham_txn_t
{
    /** the id of this txn */
    ham_u64_t _id;

    /** owner of this transaction */
    ham_env_t *_env;

    /** flags for this transaction */
    ham_u32_t _flags;

    /**
     * reference counter for cursors (how many cursors are
     * attached to this txn?)
     */
    ham_u32_t _cursor_refcount;

    /** index of the log file descriptor for this transaction [0..1] */
    int _log_desc;

#if HAM_ENABLE_REMOTE
    /** the remote database handle */
    ham_u64_t _remote_handle;
#endif

    /** linked list of all transactions */
    ham_txn_t *_newer, *_older;

    /** the linked list of operations - head is oldest operation */
    txn_op_t *_oldest_op;

    /** the linked list of operations - tail is newest operation */
    txn_op_t *_newest_op;
};

/** transaction is still alive but was aborted */
#define TXN_STATE_ABORTED               0x10000

/** transaction is still alive but was committed */
#define TXN_STATE_COMMITTED             0x20000

/** get the id */
#define txn_get_id(txn)                         (txn)->_id

/** set the id */
#define txn_set_id(txn, id)                     (txn)->_id=(id)

/** get the environment pointer */
#define txn_get_env(txn)                        (txn)->_env

/** set the environment pointer */
#define txn_set_env(txn, env)                   (txn)->_env=(env)

/** get the flags */
#define txn_get_flags(txn)                      (txn)->_flags

/** set the flags */
#define txn_set_flags(txn, f)                   (txn)->_flags=(f)

/** get the cursor refcount */
#define txn_get_cursor_refcount(txn)            (txn)->_cursor_refcount

/** set the cursor refcount */
#define txn_set_cursor_refcount(txn, cfc)       (txn)->_cursor_refcount=(cfc)

/** get the index of the log file descriptor */
#define txn_get_log_desc(txn)                   (txn)->_log_desc

/** set the index of the log file descriptor */
#define txn_set_log_desc(txn, desc)             (txn)->_log_desc=(desc)

/** get the remote database handle */
#define txn_get_remote_handle(txn)              (txn)->_remote_handle

/** set the remote database handle */
#define txn_set_remote_handle(txn, h)           (txn)->_remote_handle=(h)

/** get the 'newer' pointer of the linked list */
#define txn_get_newer(txn)                      (txn)->_newer

/** set the 'newer' pointer of the linked list */
#define txn_set_newer(txn, n)                   (txn)->_newer=(n)

/** get the 'older' pointer of the linked list */
#define txn_get_older(txn)                      (txn)->_older

/** set the 'older' pointer of the linked list */
#define txn_set_older(txn, o)                   (txn)->_older=(o)

/** get the oldest transaction operation */
#define txn_get_oldest_op(txn)                  (txn)->_oldest_op

/** set the oldest transaction operation */
#define txn_set_oldest_op(txn, o)               (txn)->_oldest_op=o

/** get the newest transaction operation */
#define txn_get_newest_op(txn)                  (txn)->_newest_op

/** set the newest transaction operation */
#define txn_set_newest_op(txn, o)               (txn)->_newest_op=o

/**
 * creates an optree for a Database, or retrieves it if it was
 * already created
 *
 * returns NULL if out of memory
 */
extern txn_optree_t *
txn_tree_get_or_create(ham_db_t *db);

/**
 * traverses a tree; for each node, a callback function is executed
 */
typedef void(*txn_tree_enumerate_cb)(txn_opnode_t *node, void *data);

/**
 * retrieves the first (=smallest) node of the tree, or NULL if the
 * tree is empty
 */
extern txn_opnode_t *
txn_tree_get_first(txn_optree_t *tree);

/**
 * retrieves the next larger sibling of a given node, or NULL if there
 * is no sibling
 */
extern txn_opnode_t *
txn_tree_get_next_node(txn_optree_t *tree, txn_opnode_t *node);

/**
 * retrieves the last (=largest) node of the tree, or NULL if the
 * tree is empty
 */
extern txn_opnode_t *
txn_tree_get_last(txn_optree_t *tree);

extern void
txn_tree_enumerate(txn_optree_t *tree, txn_tree_enumerate_cb cb, void *data);

/**
 * get an opnode for an optree; if a node with this
 * key already exists then the existing node is returned, otherwise NULL
 */
extern txn_opnode_t *
txn_opnode_get(ham_db_t *db, ham_key_t *key);

/**
 * creates an opnode for an optree; asserts that a node with this
 * key does not yet exist
 *
 * returns NULL if out of memory 
 */
extern txn_opnode_t *
txn_opnode_create(ham_db_t *db, ham_key_t *key);

/**
 * insert an actual operation into the txn_tree
 */
extern txn_op_t *
txn_opnode_append(ham_txn_t *txn, txn_opnode_t *node, 
                    ham_u32_t flags, ham_u64_t lsn, ham_record_t *record);

/**
 * frees a txn_opnode_t structure, and removes it from its tree
 */
extern void
txn_opnode_free(ham_env_t *env, txn_opnode_t *node);

/**
 * start a Transaction
 *
 * @remark flags are defined below
 */
extern ham_status_t
txn_begin(ham_txn_t **ptxn, ham_env_t *env, ham_u32_t flags);

/* #define HAM_TXN_READ_ONLY       1   -- already defined in hamsterdb.h */

/**
 * commit a Transaction
 */
extern ham_status_t
txn_commit(ham_txn_t *txn, ham_u32_t flags);

/* #define TXN_FORCE_WRITE         1   -- moved to hamsterdb.h */

/**
 * abort a Transaction
 */
extern ham_status_t
txn_abort(ham_txn_t *txn, ham_u32_t flags);

/**
 * frees the txn_optree_t structure; asserts that the tree is empty
 */
extern void
txn_free_optree(txn_optree_t *tree);

/**
 * frees the internal txn trees, nodes and ops
 * This function is a test gate for the unittests. do not use it.
 */
extern void
txn_free_ops(ham_txn_t *txn);

/**
 * free the txn structure
 *
 * will call txn_free_ops() and then free the txn pointer itself
 */
extern void
txn_free(ham_txn_t *txn);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_TXN_H__ */
