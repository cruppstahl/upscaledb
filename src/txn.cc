/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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
#include "btree_key.h"
#include "txn.h"
#include "txn_cursor.h"
#include "cursor.h"

namespace hamsterdb {

/* stuff for rb.h */
#ifndef __ssize_t_defined
typedef signed ssize_t;
#endif
#ifndef __cplusplus
typedef int bool;
#define true 1
#define false (!true)
#endif /* __cpluscplus */

static int
__cmpfoo(void *vlhs, void *vrhs)
{
    ham_compare_func_t foo;
    txn_opnode_t *lhs=(txn_opnode_t *)vlhs;
    txn_opnode_t *rhs=(txn_opnode_t *)vrhs;
    Database *db=txn_opnode_get_db(lhs);

    ham_assert(txn_opnode_get_db(lhs)==txn_opnode_get_db(rhs));
    if (lhs==rhs)
        return (0);

    foo=db->get_compare_func();

    return (foo((ham_db_t *)db,
                (ham_u8_t *)txn_opnode_get_key(lhs)->data,
                txn_opnode_get_key(lhs)->size,
                (ham_u8_t *)txn_opnode_get_key(rhs)->data,
                txn_opnode_get_key(rhs)->size));
}

rb_wrap(static, rbt_, TransactionTree, txn_opnode_t, node, __cmpfoo)

TransactionOperation::TransactionOperation(Transaction *txn,
        txn_opnode_t *node, ham_u32_t flags, ham_u32_t orig_flags,
        ham_u64_t lsn, ham_record_t *record)
  : m_txn(txn), m_node(node), m_flags(flags), m_orig_flags(orig_flags),
    m_referenced_dupe(0), m_lsn(lsn), m_cursors(0), m_node_next(0),
    m_node_prev(0), m_txn_next(0), m_txn_prev(0)
{
    Allocator *alloc = txn->get_env()->get_allocator();
    /* create a copy of the record structure */
    if (record) {
        m_record = *record;
        if (record->size && record->data) {
            m_record.data = alloc->alloc(record->size);
            memcpy(m_record.data, record->data, record->size);
        }
        else {
            m_record.size = 0;
            m_record.data = 0;
        }
    }
    else
        memset(&m_record, 0, sizeof(m_record));
}

void
txn_op_add_cursor(TransactionOperation *op, struct TransactionCursor *cursor)
{
    ham_assert(!cursor->is_nil());

    cursor->set_coupled_next(op->get_cursors());
    cursor->set_coupled_previous(0);

    if (op->get_cursors()) {
        TransactionCursor *old=op->get_cursors();
        old->set_coupled_previous(cursor);
    }

    op->set_cursors(cursor);
}

void
txn_op_remove_cursor(TransactionOperation *op, struct TransactionCursor *cursor)
{
    ham_assert(!cursor->is_nil());

    if (op->get_cursors()==cursor) {
        op->set_cursors(cursor->get_coupled_next());
        if (cursor->get_coupled_next())
            cursor->get_coupled_next()->set_coupled_previous(0);
    }
    else {
        if (cursor->get_coupled_next())
            cursor->get_coupled_next()->set_coupled_previous(
                            cursor->get_coupled_previous());
        if (cursor->get_coupled_previous())
            cursor->get_coupled_previous()->set_coupled_next(
                            cursor->get_coupled_next());
    }
    cursor->set_coupled_next(0);
    cursor->set_coupled_previous(0);
}

ham_bool_t
txn_op_conflicts(TransactionOperation *op, Transaction *current_txn)
{
    Transaction *optxn = op->get_txn();
    if (optxn->is_aborted())
        return (HAM_FALSE);
    else if (optxn->is_committed() || current_txn==optxn)
        return (HAM_FALSE);
    else /* txn is still active */
        return (HAM_TRUE);
}

txn_opnode_t *
txn_tree_get_first(TransactionTree *tree)
{
    if (tree)
        return (rbt_first(tree));
    else
        return (0);
}

txn_opnode_t *
txn_tree_get_last(TransactionTree *tree)
{
    if (tree)
        return (rbt_last(tree));
    else
        return (0);
}

txn_opnode_t *
txn_opnode_get_next_sibling(txn_opnode_t *node)
{
    return (rbt_next(txn_opnode_get_tree(node), node));
}

txn_opnode_t *
txn_opnode_get_previous_sibling(txn_opnode_t *node)
{
    return (rbt_prev(txn_opnode_get_tree(node), node));
}

void
txn_tree_enumerate(TransactionTree *tree, txn_tree_enumerate_cb cb, void *data)
{
    txn_opnode_t *node=rbt_first(tree);

    while (node) {
        cb(node, data);
        node=rbt_next(tree, node);
    }
}

static void *
__copy_key_data(Allocator *alloc, ham_key_t *key)
{
    void *data=0;

    if (key->data && key->size) {
        data=(void *)alloc->alloc(key->size);
        if (!data)
            return (0);
        memcpy(data, key->data, key->size);
    }

    return (data);
}

txn_opnode_t *
txn_opnode_get(Database *db, ham_key_t *key, ham_u32_t flags)
{
    txn_opnode_t *node=0, tmp;
    TransactionTree *tree=db->get_optree();
    int match=0;

    if (!tree)
        return (0);

    /* create a temporary node that we can search for */
    memset(&tmp, 0, sizeof(tmp));
    txn_opnode_set_key(&tmp, key);
    txn_opnode_set_db(&tmp, db);

    /* search if node already exists - if yes, return it */
    if ((flags&HAM_FIND_GEQ_MATCH)==HAM_FIND_GEQ_MATCH) {
        node=rbt_nsearch(tree, &tmp);
        if (node)
            match=__cmpfoo(&tmp, node);
    }
    else if ((flags&HAM_FIND_LEQ_MATCH)==HAM_FIND_LEQ_MATCH) {
        node=rbt_psearch(tree, &tmp);
        if (node)
            match=__cmpfoo(&tmp, node);
    }
    else if (flags&HAM_FIND_GT_MATCH) {
        node=rbt_search(tree, &tmp);
        if (node)
            node=txn_opnode_get_next_sibling(node);
        else
            node=rbt_nsearch(tree, &tmp);
        match=1;
    }
    else if (flags&HAM_FIND_LT_MATCH) {
        node=rbt_search(tree, &tmp);
        if (node)
            node=txn_opnode_get_previous_sibling(node);
        else
            node=rbt_psearch(tree, &tmp);
        match=-1;
    }
    else
        return (rbt_search(tree, &tmp));

    /* tree is empty? */
    if (!node)
        return (0);

    /* approx. matching: set the key flag */
    if (match<0)
        ham_key_set_intflags(key, (ham_key_get_intflags(key)
                        & ~BtreeKey::KEY_IS_APPROXIMATE) | BtreeKey::KEY_IS_LT);
    else if (match>0)
        ham_key_set_intflags(key, (ham_key_get_intflags(key)
                        & ~BtreeKey::KEY_IS_APPROXIMATE) | BtreeKey::KEY_IS_GT);

    return (node);
}

txn_opnode_t *
txn_opnode_create(Database *db, ham_key_t *key)
{
    txn_opnode_t *node = 0;
    TransactionTree *tree = db->get_optree();
    Allocator *alloc = db->get_env()->get_allocator();

    /* make sure that a node with this key does not yet exist */
    ham_assert(txn_opnode_get(db, key, 0) == 0);

    /* create the new node (with a copy for the key) */
    node=(txn_opnode_t *)alloc->alloc(sizeof(*node));
    if (!node)
        return (0);
    memset(node, 0, sizeof(*node));
    txn_opnode_set_key(node, key);
    txn_opnode_get_key(node)->data=__copy_key_data(alloc, key);
    txn_opnode_set_db(node, db);
    txn_opnode_set_tree(node, tree);

    /* store the node in the tree */
    rbt_insert(tree, node);

    return (node);
}

TransactionOperation *
txn_opnode_append(Transaction *txn, txn_opnode_t *node, ham_u32_t orig_flags,
                    ham_u32_t flags, ham_u64_t lsn, ham_record_t *record)
{
    TransactionOperation *op = new TransactionOperation(txn, node, flags,
            orig_flags, lsn, record);

    /* store it in the chronological list which is managed by the node */
    if (!txn_opnode_get_newest_op(node)) {
        ham_assert(txn_opnode_get_oldest_op(node)==0);
        txn_opnode_set_newest_op(node, op);
        txn_opnode_set_oldest_op(node, op);
    }
    else {
        TransactionOperation *newest=txn_opnode_get_newest_op(node);
        newest->set_next_in_node(op);
        op->set_previous_in_node(newest);
        txn_opnode_set_newest_op(node, op);
    }

    /* store it in the chronological list which is managed by the transaction */
    if (!txn->get_newest_op()) {
        ham_assert(txn->get_oldest_op()==0);
        txn->set_newest_op(op);
        txn->set_oldest_op(op);
    }
    else {
        TransactionOperation *newest=txn->get_newest_op();
        newest->set_next_in_txn(op);
        op->set_previous_in_txn(newest);
        txn->set_newest_op(op);
    }

    return (op);
}

void
txn_free_optree(TransactionTree *tree)
{
    Environment *env=txn_optree_get_db(tree)->get_env();
    txn_opnode_t *node;

    while ((node=rbt_last(tree))) {
        txn_opnode_free(env, node);
    }

    rbt_new(tree);
}

void
txn_opnode_free(Environment *env, txn_opnode_t *node)
{
    ham_key_t *key;

    TransactionTree *tree=txn_opnode_get_tree(node);
    rbt_remove(tree, node);

    key=txn_opnode_get_key(node);
    if (key->data)
        env->get_allocator()->free(key->data);

    env->get_allocator()->free(node);
}

static void
txn_op_free(Environment *env, Transaction *txn, TransactionOperation *op)
{
    ham_record_t *rec;
    TransactionOperation *next, *prev;
    txn_opnode_t *node;

    // TODO move to destructor of TransactionOperation
    rec=op->get_record();
    if (rec->data) {
        env->get_allocator()->free(rec->data);
        rec->data=0;
    }

    /* remove 'op' from the two linked lists */
    next=op->get_next_in_node();
    prev=op->get_previous_in_node();
    if (next)
        next->set_previous_in_node(prev);
    if (prev)
        prev->set_next_in_node(next);

    next=op->get_next_in_txn();
    prev=op->get_previous_in_txn();
    if (next)
        next->set_previous_in_txn(prev);
    if (prev)
        prev->set_next_in_txn(next);

    /* remove this op from the node */
    node=op->get_node();
    if (txn_opnode_get_oldest_op(node)==op)
        txn_opnode_set_oldest_op(node, op->get_next_in_node());

    /* if the node is empty: remove the node from the tree */
    if (txn_opnode_get_oldest_op(node)==0)
        txn_opnode_free(env, node);

    delete op;
}

Transaction::Transaction(Environment *env, const char *name, ham_u32_t flags)
  : m_id(0), m_env(env), m_flags(flags), m_cursor_refcount(0), m_log_desc(0),
    m_remote_handle(0), m_newer(0), m_older(0), m_oldest_op(0), m_newest_op(0) {
  m_id = env->get_txn_id() + 1;
  env->set_txn_id(m_id);
  if (name)
    m_name = name;
  if (!(flags & HAM_TXN_TEMPORARY)) {
    get_key_arena().set_allocator(env->get_allocator());
    get_record_arena().set_allocator(env->get_allocator());
  }

  /* link this txn with the Environment */
  env->append_txn(this);
}

Transaction::~Transaction()
{
    free_ops();

    /* fix double linked transaction list */
    if (get_older())
        get_older()->set_newer(get_newer());
    if (get_newer())
        get_newer()->set_older(get_older());
}

ham_status_t
Transaction::commit(ham_u32_t flags)
{
    /* are cursors attached to this txn? if yes, fail */
    ham_assert(get_cursor_refcount()==0);

    /* this transaction is now committed!  */
    set_flags(get_flags()|TXN_STATE_COMMITTED);

    /* now flush all committed Transactions to disk */
    return (get_env()->flush_committed_txns());
}

ham_status_t
Transaction::abort(ham_u32_t flags)
{
    /* are cursors attached to this txn? if yes, fail */
    if (get_cursor_refcount()) {
        ham_trace(("Transaction cannot be aborted till all attached "
                    "Cursors are closed"));
        return (HAM_CURSOR_STILL_OPEN);
    }

    /* this transaction is now aborted!  */
    set_flags(get_flags()|TXN_STATE_ABORTED);

    /* immediately release memory of the cached operations */
    free_ops();

    /* clean up the changeset */
    get_env()->get_changeset().clear();

    return (0);
}

void
Transaction::free_ops()
{
    Environment *env=get_env();
    TransactionOperation *n, *op=get_oldest_op();

    while (op) {
        n=op->get_next_in_txn();
        txn_op_free(env, this, op);
        op=n;
    }

    set_oldest_op(0);
    set_newest_op(0);
}

TransactionTree::TransactionTree(Database *db)
  : m_db(db)
{
  rbt_new(this);
}

} // namespace hamsterdb
