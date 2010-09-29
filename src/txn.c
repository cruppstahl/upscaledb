/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

#include "config.h"

#include <string.h>

#include "db.h"
#include "env.h"
#include "error.h"
#include "freelist.h"
#include "log.h"
#include "mem.h"
#include "page.h"
#include "btree_stats.h"
#include "txn.h"

/* stuff for rb.h */
typedef signed ssize_t;
typedef int bool;
#define true 1
#define false (!true)

static int
__cmpfoo(void *vlhs, void *vrhs)
{
    ham_compare_func_t foo;
    txn_optree_node_t *lhs=(txn_optree_node_t *)vlhs;
    txn_optree_node_t *rhs=(txn_optree_node_t *)vrhs;
    ham_db_t *db=txn_optree_node_get_db(lhs);

    ham_assert(txn_optree_node_get_db(lhs)==txn_optree_node_get_db(rhs), (""));

    foo=db_get_compare_func(db);

    return (foo(db, 
                txn_optree_node_get_key(lhs)->data, 
                txn_optree_node_get_key(lhs)->size,
                txn_optree_node_get_key(rhs)->data, 
                txn_optree_node_get_key(rhs)->size));
}

rb_wrap(static, rbt_, txn_optree_t, txn_optree_node_t, node, __cmpfoo)

txn_optree_t *
txn_tree_get_or_create(ham_txn_t *txn, ham_db_t *db)
{
    txn_optree_t *t=txn_get_trees(txn);
    while (t) {
        if (txn_optree_get_db(t)==db)
            return (t);
        t=txn_optree_get_next(t);
    }

    t=(txn_optree_t *)allocator_alloc(env_get_allocator(txn_get_env(txn)), 
                sizeof(*t));
    if (!t)
        return (0);
    txn_optree_set_db(t, db);
    txn_optree_set_next(t, txn_get_trees(txn));
    txn_set_trees(txn, t);
    rbt_new(t);

    return (t);
}

txn_optree_node_t *
txn_optree_node_get_or_create(ham_db_t *db, txn_optree_t *tree, 
                    ham_key_t *key)
{
    txn_optree_node_t *node=0, tmp;
    mem_allocator_t *alloc=env_get_allocator(db_get_env(db));

    /* create a temporary node that we can search for */
    memset(&tmp, 0, sizeof(tmp));
    txn_optree_node_set_key(&tmp, key);
    txn_optree_node_set_db(&tmp, db);

    /* search if node already exists - if yes, return it */
    if ((node=rbt_search(tree, &tmp)))
        return (node);

    /* node does not exist - create a new one */
    node=(txn_optree_node_t *)allocator_alloc(alloc,  sizeof(*node));
    if (!node)
        return (0);
    memset(node, 0, sizeof(*node));
    txn_optree_node_set_key(node, key);
    txn_optree_node_set_db(node, db);

    /* store the node in the tree */
    rbt_insert(tree, node);

    return (node);
}

txn_op_t *
txn_optree_node_append(ham_txn_t *txn, txn_optree_node_t *node, 
                    ham_u32_t flags, ham_u64_t lsn, ham_record_t *record)
{
    mem_allocator_t *alloc=env_get_allocator(txn_get_env(txn));
    txn_op_t *op;
    ham_record_t *newrec=0;

    /* create a copy of the record structure */
    if (record) {
        newrec=(ham_record_t *)allocator_alloc(alloc, sizeof(ham_record_t));
        if (!newrec)
            return (0);
        *newrec=*record;
        newrec->data=allocator_alloc(alloc, record->size);
        if (!newrec->data) {
            allocator_free(alloc, newrec);
            return (0);
        }
        memcpy(newrec->data, record->data, record->size);
    }

    /* create and initialize a new structure */
    op=(txn_op_t *)allocator_alloc(alloc, sizeof(*op));
    if (!op)
        return (0);
    memset(op, 0, sizeof(*op));
    txn_op_set_flags(op, flags);
    txn_op_set_lsn(op, lsn);
    txn_op_set_record(op, newrec);

    /* store it in the chronological linked list which is managed by the txn */
    if (!txn_get_newest_op(txn)) {
        ham_assert(txn_get_oldest_op(txn)==0, (""));
        txn_set_newest_op(txn, op);
        txn_set_oldest_op(txn, op);
    }
    else {
        txn_op_set_next_in_txn(op, txn_get_newest_op(txn));
        txn_set_newest_op(txn, op);
    }

    /* store it in the chronological list which is managed by the node */
    if (!txn_optree_node_get_newest_op(node)) {
        ham_assert(txn_optree_node_get_oldest_op(node)==0, (""));
        txn_optree_node_set_newest_op(node, op);
        txn_optree_node_set_oldest_op(node, op);
    }
    else {
        txn_op_set_next_in_node(op, txn_optree_node_get_newest_op(node));
        txn_optree_node_set_newest_op(node, op);
    }

    return (op);
}

ham_status_t
txn_begin(ham_txn_t **ptxn, ham_env_t *env, ham_u32_t flags)
{
    ham_status_t st=0;
    ham_txn_t *txn;

    txn=(ham_txn_t *)allocator_alloc(env_get_allocator(env), sizeof(ham_txn_t));
    if (!txn)
        return (HAM_OUT_OF_MEMORY);

    memset(txn, 0, sizeof(*txn));
    txn_set_id(txn, env_get_txn_id(env)+1);
    txn_set_flags(txn, flags);
    env_set_txn_id(env, txn_get_id(txn));

    if (env_get_log(env) && !(flags&HAM_TXN_READ_ONLY))
        st=ham_log_append_txn_begin(env_get_log(env), txn);

    /* link this txn with the Environment */
    env_append_txn(env, txn);

    *ptxn=txn;

    return (st);
}

ham_status_t
txn_commit(ham_txn_t *txn, ham_u32_t flags)
{
    ham_env_t *env=txn_get_env(txn);

    /*
     * are cursors attached to this txn? if yes, fail
     */
    if (txn_get_cursor_refcount(txn)) {
        ham_trace(("Transaction cannot be committed till all attached "
                    "Cursors are closed"));
        return (HAM_CURSOR_STILL_OPEN);
    }

    /*
     * this transaction is now committed!
     */
    txn_set_flags(txn, txn_get_flags(txn)|TXN_STATE_COMMITTED);

#if 0
    /* TODO write log! */

    /* decrease the reference counter of the modified databases */
    __decrease_db_refcount(txn);
#endif

    /* now flush all committed Transactions to disk */
    return (env_flush_committed_txns(env));
}

ham_status_t
txn_abort(ham_txn_t *txn, ham_u32_t flags)
{
    /*
     * are cursors attached to this txn? if yes, fail
     */
    if (txn_get_cursor_refcount(txn)) {
        ham_trace(("Transaction cannot be aborted till all attached "
                    "Cursors are closed"));
        return (HAM_CURSOR_STILL_OPEN);
    }

    /*
     * this transaction is now aborted!
     */
    txn_set_flags(txn, txn_get_flags(txn)|TXN_STATE_ABORTED);

#if 0
    TODO
    if (env_get_log(env) && !(txn_get_flags(txn)&HAM_TXN_READ_ONLY)) {
        st=ham_log_append_txn_abort(env_get_log(env), txn);
        if (st) 
            return st;
    }
#endif

#if 0
    /* decrease the reference counter of the modified databases */
    __decrease_db_refcount(txn);

    /* immediately release memory of the cached operations */
    txn_free_ops(txn);
#endif

    return (0);
}

static void
txn_optree_free(ham_env_t *env, txn_optree_t *tree)
{
    txn_op_t *op, *nop;
    txn_optree_node_t *node;

    while ((node=rbt_last(tree))) {
        op=txn_optree_node_get_oldest_op(node);
        while (op) {
            nop=txn_op_get_next_in_node(op);
            allocator_free(env_get_allocator(env), op);
            op=nop;
        }
        rbt_remove(tree, node);
        allocator_free(env_get_allocator(env), node);
    }
    allocator_free(env_get_allocator(env), tree);
}

void
txn_free_ops(ham_txn_t *txn)
{
    ham_env_t *env=txn_get_env(txn);

    txn_optree_t *n, *t=txn_get_trees(txn);
    while (t) {
        n=txn_optree_get_next(t);
        txn_optree_free(env, t);
        t=n;
    };
    txn_set_trees(txn, 0);
}

void
txn_free(ham_txn_t *txn)
{
    ham_env_t *env=txn_get_env(txn);

    txn_free_ops(txn);

#if DEBUG
    memset(txn, 0, sizeof(*txn));
#endif

    allocator_free(env_get_allocator(env), txn);
}

