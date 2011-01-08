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
#include "txn_cursor.h"

/* stuff for rb.h */
typedef signed ssize_t;
typedef int bool;
#define true 1
#define false (!true)

static int
__cmpfoo(void *vlhs, void *vrhs)
{
    ham_compare_func_t foo;
    txn_opnode_t *lhs=(txn_opnode_t *)vlhs;
    txn_opnode_t *rhs=(txn_opnode_t *)vrhs;
    ham_db_t *db=txn_opnode_get_db(lhs);

    ham_assert(txn_opnode_get_db(lhs)==txn_opnode_get_db(rhs), (""));
    if (lhs==rhs)
        return (0);

    foo=db_get_compare_func(db);

    return (foo(db, 
                txn_opnode_get_key(lhs)->data, 
                txn_opnode_get_key(lhs)->size,
                txn_opnode_get_key(rhs)->data, 
                txn_opnode_get_key(rhs)->size));
}

rb_wrap(static, rbt_, txn_optree_t, txn_opnode_t, node, __cmpfoo)

void
txn_op_add_cursor(txn_op_t *op, struct txn_cursor_t *cursor)
{
    ham_assert(txn_cursor_get_flags(cursor)&TXN_CURSOR_FLAG_COUPLED, (""));

    txn_cursor_set_coupled_next(cursor, txn_op_get_cursors(op));
    txn_cursor_set_coupled_previous(cursor, 0);

    if (txn_op_get_cursors(op)) {
        txn_cursor_t *old=txn_op_get_cursors(op);
        txn_cursor_set_coupled_previous(old, cursor);
    }

    txn_op_set_cursors(op, cursor);
}

void
txn_op_remove_cursor(txn_op_t *op, struct txn_cursor_t *cursor)
{
    ham_assert(txn_cursor_get_flags(cursor)&TXN_CURSOR_FLAG_COUPLED, (""));

    if (txn_op_get_cursors(op)==cursor) {
        txn_op_set_cursors(op, txn_cursor_get_coupled_next(cursor));
        if (txn_cursor_get_coupled_next(cursor))
            txn_cursor_set_coupled_previous(txn_cursor_get_coupled_next(cursor),
                            0);
    }
    else {
        if (txn_cursor_get_coupled_next(cursor))
            txn_cursor_set_coupled_previous(txn_cursor_get_coupled_next(cursor),
                            txn_cursor_get_coupled_previous(cursor));
        if (txn_cursor_get_coupled_previous(cursor))
            txn_cursor_set_coupled_next(txn_cursor_get_coupled_previous(cursor),
                            txn_cursor_get_coupled_next(cursor));
    }
    txn_cursor_set_coupled_next(cursor, 0);
    txn_cursor_set_coupled_previous(cursor, 0);
}

txn_optree_t *
txn_tree_get_or_create(ham_db_t *db)
{
    ham_env_t *env=db_get_env(db);
    txn_optree_t *t=db_get_optree(db);
    if (t)
        return (t);

    t=(txn_optree_t *)allocator_alloc(env_get_allocator(env), sizeof(*t));
    if (!t)
        return (0);
    txn_optree_set_db(t, db);
    db_set_optree(db, t);
    rbt_new(t);

    return (t);
}

txn_opnode_t *
txn_tree_get_first(txn_optree_t *tree)
{
    if (tree)
        return (rbt_first(tree));
    else
        return (0);
}

txn_opnode_t *
txn_tree_get_next_node(txn_optree_t *tree, txn_opnode_t *node)
{
    return (rbt_next(tree, node));
}

txn_opnode_t *
txn_tree_get_last(txn_optree_t *tree)
{
    if (tree)
        return (rbt_last(tree));
    else
        return (0);
}

void
txn_tree_enumerate(txn_optree_t *tree, txn_tree_enumerate_cb cb, void *data)
{
    txn_opnode_t *node=rbt_first(tree);

    while (node) {
        cb(node, data);
        node=rbt_next(tree, node);
    }
}

static ham_key_t *
__copy_key(mem_allocator_t *alloc, ham_key_t *key)
{
    ham_key_t *keycopy;
    keycopy=(ham_key_t *)allocator_alloc(alloc, sizeof(*key));
    if (!keycopy)
        return (0);
    *keycopy=*key;
    if (key->data && key->size) {
        keycopy->data=(void *)allocator_alloc(alloc, key->size);
        if (!keycopy->data)
            return (0);
        memcpy(keycopy->data, key->data, key->size);
    }
    else
        keycopy->data=0;

    return (keycopy);
}

txn_opnode_t *
txn_opnode_get(ham_db_t *db, ham_key_t *key)
{
    txn_opnode_t *node=0, tmp;
    txn_optree_t *tree=db_get_optree(db);

    /* create a temporary node that we can search for */
    memset(&tmp, 0, sizeof(tmp));
    txn_opnode_set_key(&tmp, key);
    txn_opnode_set_db(&tmp, db);

    /* search if node already exists - if yes, return it */
    if ((node=rbt_search(tree, &tmp)))
        return (node);
    return (0);
}

txn_opnode_t *
txn_opnode_create(ham_db_t *db, ham_key_t *key)
{
    txn_opnode_t *node=0;
    txn_optree_t *tree=db_get_optree(db);
    mem_allocator_t *alloc=env_get_allocator(db_get_env(db));

    /* make sure that a node with this key does not yet exist */
    ham_assert(txn_opnode_get(db, key)==0, (""));

    /* create the new node (with a copy for the key) */
    node=(txn_opnode_t *)allocator_alloc(alloc, sizeof(*node));
    if (!node)
        return (0);
    memset(node, 0, sizeof(*node));
    txn_opnode_set_key(node, __copy_key(alloc, key));
    if (!txn_opnode_get_key(node)) {
        allocator_free(alloc, node);
        return (0);
    }
    txn_opnode_set_db(node, db);
    txn_opnode_set_tree(node, tree);

    /* store the node in the tree */
    rbt_insert(tree, node);

    return (node);
}

txn_op_t *
txn_opnode_append(ham_txn_t *txn, txn_opnode_t *node, 
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
        if (record->size && record->data) {
            newrec->data=allocator_alloc(alloc, record->size);
            if (!newrec->data) {
                allocator_free(alloc, newrec);
                return (0);
            }
            memcpy(newrec->data, record->data, record->size);
        }
        else {
            newrec->data=0;
            newrec->size=0;
        }
    }

    /* create and initialize a new txn_op_t structure */
    op=(txn_op_t *)allocator_alloc(alloc, sizeof(*op));
    if (!op) /* TODO free newrec->data, newrec */
        return (0);
    memset(op, 0, sizeof(*op));
    txn_op_set_flags(op, flags);
    txn_op_set_lsn(op, lsn);
    txn_op_set_record(op, newrec);
    txn_op_set_txn(op, txn);
    txn_op_set_node(op, node);

    /* store it in the chronological list which is managed by the node */
    if (!txn_opnode_get_newest_op(node)) {
        ham_assert(txn_opnode_get_oldest_op(node)==0, (""));
        txn_opnode_set_newest_op(node, op);
        txn_opnode_set_oldest_op(node, op);
    }
    else {
        txn_op_t *newest=txn_opnode_get_newest_op(node);
        txn_op_set_next_in_node(newest, op);
        txn_op_set_previous_in_node(op, newest);
        txn_opnode_set_newest_op(node, op);
    }

    /* store it in the chronological list which is managed by the transaction */
    if (!txn_get_newest_op(txn)) {
        ham_assert(txn_get_oldest_op(txn)==0, (""));
        txn_set_newest_op(txn, op);
        txn_set_oldest_op(txn, op);
    }
    else {
        txn_op_t *newest=txn_get_newest_op(txn);
        txn_op_set_next_in_txn(newest, op);
        txn_op_set_previous_in_txn(op, newest);
        txn_set_newest_op(txn, op);
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
    if (!(env_get_rt_flags(env)&DB_DISABLE_AUTO_FLUSH))
        return (env_flush_committed_txns(env));
    else
        return (0);
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
    /* decrease the reference counter of the modified databases */
    __decrease_db_refcount(txn);
#endif

    /* immediately release memory of the cached operations */
    txn_free_ops(txn);

    /* clean up the changeset */
    changeset_clear(env_get_changeset(txn_get_env(txn)));

    return (0);
}

void
txn_free_optree(txn_optree_t *tree)
{
    ham_env_t *env=db_get_env(txn_optree_get_db(tree));
    txn_opnode_t *node;

    while ((node=rbt_last(tree))) {
        txn_opnode_free(env, node);
    }
    allocator_free(env_get_allocator(env), tree);
}

void
txn_opnode_free(ham_env_t *env, txn_opnode_t *node)
{
    txn_optree_t *tree=txn_opnode_get_tree(node);
    rbt_remove(tree, node);
    /* also remove the ham_key_t structure */
    if (txn_opnode_get_key(node)) {
        ham_key_t *key=txn_opnode_get_key(node);
        if (key->data)
            allocator_free(env_get_allocator(env), key->data);
        allocator_free(env_get_allocator(env), key);
    }
    allocator_free(env_get_allocator(env), node);
}

static void
txn_op_free(ham_env_t *env, ham_txn_t *txn, txn_op_t *op)
{
    ham_record_t *rec;
    txn_op_t *next, *prev;
    txn_opnode_t *node;

    rec=txn_op_get_record(op);
    if (rec) {
        if (rec->data)
            allocator_free(env_get_allocator(env), rec->data);
        allocator_free(env_get_allocator(env), rec);
    }

    /* remove 'op' from the two linked lists */
    next=txn_op_get_next_in_node(op);
    prev=txn_op_get_previous_in_node(op);
    if (next)
        txn_op_set_previous_in_node(next, prev);
    if (prev)
        txn_op_set_next_in_node(prev, next);

    next=txn_op_get_next_in_txn(op);
    prev=txn_op_get_previous_in_txn(op);
    if (next)
        txn_op_set_previous_in_txn(next, prev);
    if (prev)
        txn_op_set_next_in_txn(prev, next);

    /* remove this op from the node */
    node=txn_op_get_node(op);
    if (txn_opnode_get_oldest_op(node)==op)
        txn_opnode_set_oldest_op(node, txn_op_get_next_in_node(op));

    /* if the node is empty: remove the node from the tree */
    if (txn_opnode_get_oldest_op(node)==0)
        txn_opnode_free(env, node);

    allocator_free(env_get_allocator(env), op);
}

void
txn_free_ops(ham_txn_t *txn)
{
    ham_env_t *env=txn_get_env(txn);
    txn_op_t *n, *op=txn_get_oldest_op(txn);

    while (op) {
        n=txn_op_get_next_in_txn(op);
        txn_op_free(env, txn, op);
        op=n;
    }

    txn_set_oldest_op(txn, 0);
    txn_set_newest_op(txn, 0);
}

void
txn_free(ham_txn_t *txn)
{
    ham_env_t *env=txn_get_env(txn);

    txn_free_ops(txn);

    /* fix double linked transaction list */
    if (txn_get_older(txn))
        txn_set_newer(txn_get_older(txn), txn_get_newer(txn));
    if (txn_get_newer(txn))
        txn_set_older(txn_get_newer(txn), txn_get_older(txn));

#if DEBUG
    memset(txn, 0, sizeof(*txn));
#endif

    allocator_free(env_get_allocator(env), txn);
}

