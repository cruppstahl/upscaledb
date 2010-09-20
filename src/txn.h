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

    /** next in linked list (managed in txn_node_t) */
    struct txn_op_t *_next;

    /** next in chronological linked list (managed in ham_txn_t) */
    struct txn_op_t *_txn_next;

    /** the log serial number (lsn) of this operation */
    ham_u64_t _lsn;

    /** the record id - can be compressed */
    ham_offset_t _rid;

} txn_op_t;

/** a NOP operation (empty) */
#define TXN_OP_NOP          0x00000

/** txn operation is an insert (w/ overwrite) */
#define TXN_OP_INSERT_OW    0x10000

/** txn operation is an insert (w/ duplicates) */
#define TXN_OP_INSERT_DUP   0x20000

/** txn operation erases the key */
#define TXN_OP_ERASE        0x40000

/** get flags */
#define txn_op_get_flags(t)         (t)->_flags

/** set flags */
#define txn_op_set_flags(t, f)      (t)->_flags=f

/** get next txn_op_t structure */
#define txn_op_get_next(t)          (t)->_next

/** set next txn_op_t structure */
#define txn_op_set_next(t, n)       (t)->_next=n

/** get next txn_op_t structure of this txn */
#define txn_op_get_txn_next(t)      (t)->_txn_next

/** set next txn_op_t structure of this txn */
#define txn_op_set_txn_next(t, n)   (t)->_txn_next=n

/** get lsn */
#define txn_op_get_lsn(t)           (t)->_lsn

/** set lsn */
#define txn_op_set_lsn(t, l)        (t)->_lsn=l

/** get record id */
#define txn_op_get_rid(t)           (t)->_rid

/** set record id */
#define txn_op_set_rid(t, r)        (t)->_rid=r


/**
 * a Transaction node - modifies a key; manages a list of txn_op_t
 * operations which describe the exact operations for this key
 */
typedef struct txn_node_t
{
    /** this is the key which is modified */
    ham_key_t *_key;

    /** the linked list of operations - head is oldest operation */
    txn_op_t *_oldest;

    /** the linked list of operations - tail is newest operation */
    txn_op_t *_newest;

} txn_node_t;

/** get pointer to the modified key */
#define txn_node_get_key(t)         (t)->_key

/** set pointer to the modified key */
#define txn_node_set_key(t, k)      (t)->_key=k

/** get pointer to the first (oldest) node in list */
#define txn_node_get_oldest(t)      (t)->_oldest

/** set pointer to the first (oldest) node in list */
#define txn_node_set_oldest(t, o)   (t)->_oldest=o

/** get pointer to the last (newest) node in list */
#define txn_node_get_newest(t)      (t)->_newest

/** set pointer to the last (newest) node in list */
#define txn_node_set_newest(t, n)   (t)->_newest=n


/*
 * a node in the red-black Transaction tree (implemented in rb.h)
 */
typedef struct txn_optree_node_t
{
    /** red-black tree stub */
    rb_node(struct txn_optree_node_t) node;

    /** the operation */
    txn_node_t *_op;

    /** the left child */
    txn_node_t *_left;

    /** the right child */
    txn_node_t *_right;

} txn_optree_node_t;

/** get the operation details of this node */
#define txn_optree_node_get_node(t)     (t)->_op

/** set the operation details of this node */
#define txn_optree_node_set_node(t, o)  (t)->_op=o

/** get the left child of this node */
#define txn_optree_node_get_left(t)     (t)->_left

/** set the left child of this node */
#define txn_optree_node_set_left(t, l)  (t)->_left=l

/** get the right child of this node */
#define txn_optree_node_get_right(t)    (t)->_right

/** set the right child of this node */
#define txn_optree_node_set_right(t, r) (t)->_right=r


/*
 * each Transaction has one tree per Database for a fast lookup; this
 * is just a normal binary tree
 */
typedef struct txn_optree_t
{
    /* the Database for all operations in this tree */
    ham_db_t *_db;

    /* stuff for rb.h */
    txn_optree_node_t *rbt_root;
    txn_optree_node_t rbt_nil;

    /* the next optree - optrees form a linked list of all optrees which
     * belong to the same transaction */
    struct txn_optree_t *_next;

} txn_optree_t;

/** get the database handle of this txn tree */
#define txn_optree_get_db(t)        (t)->_db

/** set the database handle of this txn tree */
#define txn_optree_set_db(t, d)     (t)->_db=d

/** get the next optree in the linked list of optrees */
#define txn_optree_get_next(t)      (t)->_next

/** set the next optree in the linked list of optrees */
#define txn_optree_set_next(t, n)   (t)->_next=n


/**
 * a transaction structure
 */
struct ham_txn_t
{
    /** the id of this txn */
    ham_u64_t _id;

    /** owner of this transaction */
    ham_env_t *_env;

    /** flags for this transaction */
    ham_u32_t _flags;

    /** chronological linked list of all operations of this txn - oldest 
     * operation */
    struct txn_op_t *_oplist_oldest;

    /** chronological linked list of all operations of this txn - newest
     * operation */
    struct txn_op_t *_oplist_newest;

    /** a list of transaction trees */
    struct txn_optree_t *_trees;

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
#define txn_get_env(txn)                         (txn)->_env

/** set the environment pointer */
#define txn_set_env(txn, env)                     (txn)->_env=(env)

/** get the flags */
#define txn_get_flags(txn)                      (txn)->_flags

/** set the flags */
#define txn_set_flags(txn, f)                   (txn)->_flags=(f)

/** get the cursor refcount */
#define txn_get_cursor_refcount(txn)            (txn)->_cursor_refcount

/** set the cursor refcount */
#define txn_set_cursor_refcount(txn, cfc)       (txn)->_cursor_refcount=(cfc)

/** get the oldest transaction operation */
#define txn_get_oplist_oldest(txn)              (txn)->_oplist_oldest

/** set the oldest transaction operation */
#define txn_set_oplist_oldest(txn, o)           (txn)->_oplist_oldest=o

/** get the newest transaction operation */
#define txn_get_oplist_newest(txn)              (txn)->_oplist_newest

/** set the newest transaction operation */
#define txn_set_oplist_newest(txn, n)           (txn)->_oplist_newest=n

/** get the linked list of transaction trees */
#define txn_get_trees(txn)                      (txn)->_trees

/** set the linked list of transaction trees */
#define txn_set_trees(txn, t)                   (txn)->_trees=t

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

/**
 * start a transaction
 *
 * @remark flags are defined below
 */
extern ham_status_t
txn_begin(ham_txn_t **ptxn, ham_env_t *env, ham_u32_t flags);

/* #define HAM_TXN_READ_ONLY       1   -- already defined in hamsterdb.h */

/**
 * commit a transaction
 */
extern ham_status_t
txn_commit(ham_txn_t *txn, ham_u32_t flags);

/* #define TXN_FORCE_WRITE         1   -- moved to hamsterdb.h */

/**
 * abort a transaction
 */
extern ham_status_t
txn_abort(ham_txn_t *txn, ham_u32_t flags);

/**
 * free the txn structure
 */
extern void
txn_free(ham_txn_t *txn);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_TXN_H__ */
