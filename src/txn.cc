/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
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
compare(void *vlhs, void *vrhs)
{
  TransactionNode *lhs = (TransactionNode *)vlhs;
  TransactionNode *rhs = (TransactionNode *)vrhs;
  LocalDatabase *db = lhs->get_db();

  if (lhs == rhs)
    return (0);

  ham_compare_func_t foo = db->get_compare_func();

  return (foo((ham_db_t *)db,
        (ham_u8_t *)lhs->get_key()->data, lhs->get_key()->size,
        (ham_u8_t *)rhs->get_key()->data, rhs->get_key()->size));
}

static void *
copy_key_data(ham_key_t *key)
{
  void *data = 0;

  if (key->data && key->size) {
    data = Memory::allocate<void>(key->size);
    if (!data)
      return (0);
    memcpy(data, key->data, key->size);
  }

  return (data);
}

static void
txn_op_free(Environment *env, Transaction *txn, TransactionOperation *op)
{
  ham_record_t *rec;
  TransactionNode *node;

  // TODO move to destructor of TransactionOperation
  rec = op->get_record();
  Memory::release(rec->data);
  rec->data = 0;

  /* remove 'op' from the two linked lists */
  TransactionOperation *next = op->get_next_in_node();
  TransactionOperation *prev = op->get_previous_in_node();
  if (next)
    next->set_previous_in_node(prev);
  if (prev)
    prev->set_next_in_node(next);

  next = op->get_next_in_txn();
  prev = op->get_previous_in_txn();
  if (next)
    next->set_previous_in_txn(prev);
  if (prev)
    prev->set_next_in_txn(next);

  /* remove this op from the node */
  node = op->get_node();
  if (node->get_oldest_op()==op)
    node->set_oldest_op(op->get_next_in_node());

  /* if the node is empty: remove the node from the tree */
  if (node->get_oldest_op() == 0)
    delete node;

  delete op;
}


rb_wrap(static, rbt_, TransactionIndex, TransactionNode, node, compare)

TransactionOperation::TransactionOperation(Transaction *txn,
    TransactionNode *node, ham_u32_t flags, ham_u32_t orig_flags,
    ham_u64_t lsn, ham_record_t *record)
  : m_txn(txn), m_node(node), m_flags(flags), m_orig_flags(orig_flags),
  m_referenced_dupe(0), m_lsn(lsn), m_cursors(0), m_node_next(0),
  m_node_prev(0), m_txn_next(0), m_txn_prev(0)
{
  /* create a copy of the record structure */
  if (record) {
    m_record = *record;
    if (record->size && record->data) {
      m_record.data = Memory::allocate<void>(record->size);
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

TransactionNode *
TransactionNode::get_next_sibling()
{
  return (rbt_next(m_tree, this));
}

TransactionNode *
TransactionNode::get_previous_sibling()
{
  return (rbt_prev(m_tree, this));
}

TransactionNode::TransactionNode(LocalDatabase *db, ham_key_t *key,
                bool dont_insert)
  : m_db(db), m_tree(db->get_txn_index()), m_oldest_op(0), m_newest_op(0),
  m_dont_insert(dont_insert)
{
  /* make sure that a node with this key does not yet exist */
  // TODO re-enable this; currently leads to a stack overflow because
  // TransactionIndex::get() creates a new TransactionNode
  // ham_assert(TransactionIndex::get(key, 0) == 0);

  m_key = *key;
  m_key.data = copy_key_data(key);

  /* store the node in the tree */
  if (dont_insert == false)
    rbt_insert(m_tree, this);
}

TransactionNode::TransactionNode()
  : m_db(0), m_tree(0), m_oldest_op(0), m_newest_op(0), m_dont_insert(true)
{
  memset(&m_key, 0, sizeof(m_key));
}

TransactionNode::~TransactionNode()
{
  if (m_dont_insert == false && m_tree)
    rbt_remove(m_tree, this);

  Memory::release(m_key.data);
}

TransactionOperation *
TransactionNode::append(Transaction *txn, ham_u32_t orig_flags,
      ham_u32_t flags, ham_u64_t lsn, ham_record_t *record)
{
  TransactionOperation *op = new TransactionOperation(txn, this, flags,
      orig_flags, lsn, record);

  /* store it in the chronological list which is managed by the node */
  if (!get_newest_op()) {
    ham_assert(get_oldest_op() == 0);
    set_newest_op(op);
    set_oldest_op(op);
  }
  else {
    TransactionOperation *newest = get_newest_op();
    newest->set_next_in_node(op);
    op->set_previous_in_node(newest);
    set_newest_op(op);
  }

  /* store it in the chronological list which is managed by the transaction */
  if (!txn->get_newest_op()) {
    ham_assert(txn->get_oldest_op() == 0);
    txn->set_newest_op(op);
    txn->set_oldest_op(op);
  }
  else {
    TransactionOperation *newest = txn->get_newest_op();
    newest->set_next_in_txn(op);
    op->set_previous_in_txn(newest);
    txn->set_newest_op(op);
  }

  return (op);
}

void
TransactionIndex::close()
{
  TransactionNode *node;

  while ((node = rbt_last(this)))
    delete node;

  rbt_new(this);
}

Transaction::Transaction(Environment *env, const char *name, ham_u32_t flags)
  : m_id(0), m_env(env), m_flags(flags), m_cursor_refcount(0), m_log_desc(0),
    m_remote_handle(0), m_newer(0), m_older(0), m_oldest_op(0), m_newest_op(0) {
  m_id = env->get_txn_id() + 1;
  env->set_txn_id(m_id);
  if (name)
    m_name = name;

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
  ham_assert(get_cursor_refcount() == 0);

  /* this transaction is now committed!  */
  set_flags(get_flags() | TXN_STATE_COMMITTED);

  /* now flush all committed Transactions to disk */
  return (get_env()->flush_committed_txns());
}

ham_status_t
Transaction::abort(ham_u32_t flags)
{
  /* are cursors attached to this txn? if yes, fail */
  // TODO not required - already in LocalEnvironment::txn_abort
  if (get_cursor_refcount()) {
    ham_trace(("Transaction cannot be aborted till all attached "
          "Cursors are closed"));
    return (HAM_CURSOR_STILL_OPEN);
  }

  /* this transaction is now aborted!  */
  set_flags(get_flags() | TXN_STATE_ABORTED);

  /* immediately release memory of the cached operations */
  free_ops();

  /* clean up the changeset */
  get_env()->get_changeset().clear();

  return (0);
}

void
Transaction::free_ops()
{
  Environment *env = get_env();
  TransactionOperation *n, *op = get_oldest_op();

  while (op) {
    n = op->get_next_in_txn();
    txn_op_free(env, this, op);
    op = n;
  }

  set_oldest_op(0);
  set_newest_op(0);
}

TransactionIndex::TransactionIndex(LocalDatabase *db)
  : m_db(db)
{
  rbt_new(this);

  // avoid warning about unused function
  if (0) {
    (void) (rbt_ppsearch(this, 0));
  }
}

TransactionNode *
TransactionIndex::get(ham_key_t *key, ham_u32_t flags)
{
  TransactionNode *node = 0;
  int match = 0;

  /* create a temporary node that we can search for */
  TransactionNode tmp(m_db, key, true);

  /* search if node already exists - if yes, return it */
  if ((flags & HAM_FIND_GEQ_MATCH) == HAM_FIND_GEQ_MATCH) {
    node = rbt_nsearch(this, &tmp);
    if (node)
      match = compare(&tmp, node);
  }
  else if ((flags & HAM_FIND_LEQ_MATCH) == HAM_FIND_LEQ_MATCH) {
    node = rbt_psearch(this, &tmp);
    if (node)
      match = compare(&tmp, node);
  }
  else if (flags & HAM_FIND_GT_MATCH) {
    node = rbt_search(this, &tmp);
    if (node)
      node = node->get_next_sibling();
    else
      node = rbt_nsearch(this, &tmp);
    match = 1;
  }
  else if (flags & HAM_FIND_LT_MATCH) {
    node = rbt_search(this, &tmp);
    if (node)
      node = node->get_previous_sibling();
    else
      node = rbt_psearch(this, &tmp);
    match = -1;
  }
  else
    return (rbt_search(this, &tmp));

  /* tree is empty? */
  if (!node)
    return (0);

  /* approx. matching: set the key flag */
  if (match < 0)
    ham_key_set_intflags(key, (ham_key_get_intflags(key)
            & ~PBtreeKey::KEY_IS_APPROXIMATE) | PBtreeKey::KEY_IS_LT);
  else if (match > 0)
    ham_key_set_intflags(key, (ham_key_get_intflags(key)
            & ~PBtreeKey::KEY_IS_APPROXIMATE) | PBtreeKey::KEY_IS_GT);

  return (node);
}

TransactionNode *
TransactionIndex::get_first()
{
  return (rbt_first(this));
}

TransactionNode *
TransactionIndex::get_last()
{
  return (rbt_last(this));
}

void
txn_tree_enumerate(TransactionIndex *tree, TxnTreeVisitor *visitor)
{
  TransactionNode *node = rbt_first(tree);

  while (node) {
    visitor->visit(node);
    node = rbt_next(tree, node);
  }
}

} // namespace hamsterdb
