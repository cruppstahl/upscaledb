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
#include "env_local.h"
#include "error.h"
#include "log.h"
#include "mem.h"
#include "page.h"
#include "btree_stats.h"
#include "btree_key.h"
#include "txn.h"
#include "txn_cursor.h"
#include "cursor.h"
#include "btree_index.h"

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

  return (db->get_btree_index()->compare_keys(lhs->get_key(), rhs->get_key()));
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

TransactionOperation::~TransactionOperation()
{
  Memory::release(m_record.data);
  m_record.data = 0;

  /* remove this operation from the two linked lists */
  TransactionOperation *next = get_next_in_node();
  TransactionOperation *prev = get_previous_in_node();
  if (next)
    next->set_previous_in_node(prev);
  if (prev)
    prev->set_next_in_node(next);

  next = get_next_in_txn();
  prev = get_previous_in_txn();
  if (next)
    next->set_previous_in_txn(prev);
  if (prev)
    prev->set_next_in_txn(next);

  /* remove this op from the node */
  // TODO should this be done in here??
  TransactionNode *node = get_node();
  if (node->get_oldest_op() == this)
    node->set_oldest_op(get_next_in_node());

  /* if the node is empty: remove the node from the tree */
  // TODO should this be done in here??
  if (node->get_oldest_op() == 0) {
    node->get_txn_index()->remove(node);
    delete node;
  }
}

rb_wrap(static, rbt_, TransactionIndex, TransactionNode, node, compare)

TransactionOperation::TransactionOperation(Transaction *txn,
    TransactionNode *node, ham_u32_t flags, ham_u32_t orig_flags,
    ham_u64_t lsn, ham_record_t *record)
  : m_txn(txn), m_node(node), m_flags(flags), m_orig_flags(orig_flags),
  m_referenced_dupe(0), m_lsn(lsn), m_cursor_list(0), m_node_next(0),
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
  return (rbt_next(m_txn_index, this));
}

TransactionNode *
TransactionNode::get_previous_sibling()
{
  return (rbt_prev(m_txn_index, this));
}

TransactionNode::TransactionNode(LocalDatabase *db, ham_key_t *key)
  : m_db(db), m_txn_index(db ? db->get_txn_index() : 0),
    m_oldest_op(0), m_newest_op(0)
{
  /* make sure that a node with this key does not yet exist */
  // TODO re-enable this; currently leads to a stack overflow because
  // TransactionIndex::get() creates a new TransactionNode
  // ham_assert(TransactionIndex::get(key, 0) == 0);

  if (key) {
    m_key = *key;
    m_key.data = copy_key_data(key);
  }
  else
    memset(&m_key, 0, sizeof(m_key));
}

TransactionNode::~TransactionNode()
{
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
TransactionIndex::store(TransactionNode *node)
{
  rbt_insert(this, node);
}

void
TransactionIndex::remove(TransactionNode *node)
{
  rbt_remove(this, node);
}

Transaction::Transaction(Environment *env, const char *name,
                ham_u32_t flags)
  : m_id(0), m_env(env), m_flags(flags), m_cursor_refcount(0), m_log_desc(0),
    m_remote_handle(0), m_newer(0), m_older(0), m_oldest_op(0), m_newest_op(0) {
  LocalEnvironment *lenv = dynamic_cast<LocalEnvironment *>(env);
  if (lenv)
    m_id = lenv->get_incremented_txn_id();
  if (name)
    m_name = name;
}

Transaction::~Transaction()
{
  free_operations();

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
  m_flags |= kStateCommitted;

  // TODO ugly - better move flush_committed_txns() in the caller
  LocalEnvironment *lenv = dynamic_cast<LocalEnvironment *>(m_env);
  if (lenv)
    return (lenv->flush_committed_txns());
  else
    return (0);
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
  m_flags |= kStateAborted;

  /* immediately release memory of the cached operations */
  free_operations();

  /* clean up the changeset */
  LocalEnvironment *lenv = dynamic_cast<LocalEnvironment *>(m_env);
  if (lenv)
    lenv->get_changeset().clear();

  return (0);
}

void
Transaction::free_operations()
{
  TransactionOperation *n, *op = get_oldest_op();

  while (op) {
    n = op->get_next_in_txn();
    delete op;
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

TransactionIndex::~TransactionIndex()
{
  TransactionNode *node;

  while ((node = rbt_last(this))) {
    remove(node);
    delete node;
  }

  // re-initialize the tree
  rbt_new(this);
}

TransactionNode *
TransactionIndex::get(ham_key_t *key, ham_u32_t flags)
{
  TransactionNode *node = 0;
  int match = 0;

  /* create a temporary node that we can search for */
  TransactionNode tmp(m_db, key);

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
            & ~BtreeKey::kApproximate) | BtreeKey::kLower);
  else if (match > 0)
    ham_key_set_intflags(key, (ham_key_get_intflags(key)
            & ~BtreeKey::kApproximate) | BtreeKey::kGreater);

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
TransactionIndex::enumerate(TransactionIndex::Visitor *visitor)
{
  TransactionNode *node = rbt_first(this);

  while (node) {
    visitor->visit(node);
    node = rbt_next(this, node);
  }
}

struct KeyCounter : public TransactionIndex::Visitor
{
  KeyCounter(LocalDatabase *_db, Transaction *_txn, ham_u32_t _flags)
    : counter(0), flags(_flags), txn(_txn), db(_db) {
  }

  void visit(TransactionNode *node) {
    BtreeIndex *be = db->get_btree_index();
    TransactionOperation *op;

    /*
     * look at each tree_node and walk through each operation
     * in reverse chronological order (from newest to oldest):
     * - is this op part of an aborted txn? then skip it
     * - is this op part of a committed txn? then include it
     * - is this op part of an txn which is still active? then include it
     * - if a committed txn has erased the item then there's no need
     *    to continue checking older, committed txns of the same key
     *
     * !!
     * if keys are overwritten or a duplicate key is inserted, then
     * we have to consolidate the btree keys with the txn-tree keys.
     */
    op = node->get_newest_op();
    while (op) {
      Transaction *optxn = op->get_txn();
      if (optxn->is_aborted())
        ; // nop
      else if (optxn->is_committed() || txn == optxn) {
        if (op->get_flags() & TransactionOperation::kIsFlushed)
          ; // nop
        // if key was erased then it doesn't exist
        else if (op->get_flags() & TransactionOperation::kErase)
          return;
        else if (op->get_flags() & TransactionOperation::kInsert) {
          counter++;
          return;
        }
        // key exists - include it
        else if ((op->get_flags() & TransactionOperation::kInsert)
            || (op->get_flags() & TransactionOperation::kInsertOverwrite)) {
          // check if the key already exists in the btree - if yes,
          // we do not count it (it will be counted later)
          if (HAM_KEY_NOT_FOUND == be->find(0, 0, node->get_key(), 0, 0))
            counter++;
          return;
        }
        else if (op->get_flags() & TransactionOperation::kInsertDuplicate) {
          // check if btree has other duplicates
          if (0 == be->find(0, 0, node->get_key(), 0, 0)) {
            // yes, there's another one
            if (flags & HAM_SKIP_DUPLICATES)
              return;
            else
              counter++;
          }
          else {
            // check if other key is in this node
            counter++;
            if (flags & HAM_SKIP_DUPLICATES)
              return;
          }
        }
        else if (!(op->get_flags() & TransactionOperation::kNop)) {
          ham_assert(!"shouldn't be here");
          return;
        }
      }
      else { // txn is still active
        counter++;
      }

      op = op->get_previous_in_node();
    }
  }

  ham_u64_t counter;
  ham_u32_t flags;
  Transaction *txn;
  LocalDatabase *db;
};

ham_status_t
TransactionIndex::get_key_count(Transaction *txn, ham_u32_t flags,
                ham_u64_t *pkeycount)
{
  *pkeycount = 0;
  KeyCounter k(m_db, txn, flags);
  enumerate(&k);
  *pkeycount = k.counter;
  return (0);
}

} // namespace hamsterdb
