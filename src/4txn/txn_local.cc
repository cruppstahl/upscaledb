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

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "3btree/btree_index.h"
#include "3journal/journal.h"
#include "4db/db_local.h"
#include "4txn/txn_local.h"
#include "4txn/txn_factory.h"
#include "4txn/txn_cursor.h"
#include "4env/env_local.h"
#include "4cursor/cursor_local.h"
#include "4context/context.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

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
  TxnNode *lhs = (TxnNode *)vlhs;
  TxnNode *rhs = (TxnNode *)vrhs;
  LocalDb *db = lhs->db;

  if (unlikely(lhs == rhs))
    return 0;

  ups_key_t *lhskey = lhs->key();
  ups_key_t *rhskey = rhs->key();
  assert(lhskey && rhskey);
  return db->btree_index->compare_keys(lhskey, rhskey);
}

rb_proto(static, rbt_, TxnIndex, TxnNode)
rb_gen(static, rbt_, TxnIndex, TxnNode, node, compare)

static inline void
flush_committed_txns_impl(LocalTxnManager *tm, Context *context)
{
  LocalTxn *oldest;
  Journal *journal = tm->lenv()->journal.get();
  uint64_t highest_lsn = 0;

  assert(context->changeset.is_empty());

  /* always get the oldest transaction; if it was committed: flush
   * it; if it was aborted: discard it; otherwise return */
  while ((oldest = (LocalTxn *)tm->oldest_txn())) {
    if (oldest->is_committed()) {
      uint64_t lsn = tm->flush_txn(context, (LocalTxn *)oldest);
      if (lsn > highest_lsn)
        highest_lsn = lsn;

      /* this transaction was flushed! */
      if (journal && notset(oldest->flags, UPS_TXN_TEMPORARY))
        journal->transaction_flushed(oldest);
    }
    else if (oldest->is_aborted()) {
      ; /* nop */
    }
    else
      break;

    /* now remove the txn from the linked list */
    tm->remove_txn_from_head(oldest);

    /* and release the memory */
    delete oldest;
  }

  /* now flush the changeset and write the modified pages to disk */
  if (highest_lsn && context->env->journal.get())
    context->changeset.flush(tm->lenv()->lsn_manager.next());
  else
    context->changeset.clear();
  assert(context->changeset.is_empty());
}

void
TxnOperation::initialize(LocalTxn *txn_, TxnNode *node_,
            uint32_t flags_, uint32_t original_flags_, uint64_t lsn_,
            ups_key_t *key_, ups_record_t *record_)
{
  ::memset(this, 0, sizeof(*this));

  txn = txn_;
  node = node_;
  lsn = lsn_;
  flags = flags_;
  original_flags = original_flags_;

  /* copy the key data */
  if (key_) {
    key = *key_;
    if (likely(key.size)) {
      key.data = &_data[0];
      ::memcpy(key.data, key_->data, key.size);
    }
  }

  /* copy the record data */
  if (record_) {
    record = *record_;
    if (likely(record.size)) {
      record.data = &_data[key_ ? key.size : 0];
      ::memcpy(record.data, record_->data, record.size);
    }
  }
}

void
TxnOperation::destroy()
{
  bool delete_node = false;

  /* remove this op from the node */
  if (node->oldest_op == this) {
    /* if the node is empty: remove the node from the tree */
    // TODO should this be done in here??
    if (next_in_node == 0) {
      node->db->txn_index->remove(node);
      delete_node = true;
    }
    node->oldest_op = next_in_node;
  }

  /* remove this operation from the two linked lists */
  TxnOperation *next = next_in_node;
  TxnOperation *prev = previous_in_node;
  if (next)
    next->previous_in_node = prev;
  if (prev)
    prev->next_in_node = next;

  next = next_in_txn;
  prev = previous_in_txn;
  if (next)
    next->previous_in_txn = prev;
  if (prev)
    prev->next_in_txn = next;

  if (delete_node)
    delete node;

  Memory::release(this);
}

TxnNode *
TxnNode::next_sibling()
{
  return rbt_next(db->txn_index.get(), this);
}

TxnNode *
TxnNode::previous_sibling()
{
  return rbt_prev(db->txn_index.get(), this);
}

TxnNode::TxnNode(LocalDb *db_, ups_key_t *key)
  : db(db_), oldest_op(0), newest_op(0), _key(key)
{
  /* make sure that a node with this key does not yet exist */
  // TODO re-enable this; currently leads to a stack overflow because
  // TxnIndex::get() creates a new TxnNode
  // assert(TxnIndex::get(key, 0) == 0);
}

TxnOperation *
TxnNode::append(LocalTxn *txn, uint32_t orig_flags, uint32_t flags,
                uint64_t lsn, ups_key_t *key, ups_record_t *record)
{
  TxnOperation *op = TxnFactory::create_operation(txn, this, flags,
                        orig_flags, lsn, key, record);

  /* store it in the chronological list which is managed by the node */
  if (!newest_op) {
    assert(oldest_op == 0);
    newest_op = op;
    oldest_op = op;
  }
  else {
    TxnOperation *newest = newest_op;
    newest->next_in_node = op;
    op->previous_in_node = newest;
    newest_op = op;
  }

  /* store it in the chronological list which is managed by the transaction */
  if (!txn->newest_op) {
    assert(txn->oldest_op == 0);
    txn->newest_op = op;
    txn->oldest_op = op;
  }
  else {
    TxnOperation *newest = txn->newest_op;
    newest->next_in_txn = op;
    op->previous_in_txn = newest;
    txn->newest_op = op;
  }

  // now that an operation is attached make sure that the node no
  // longer uses the temporary key pointer
  _key = 0;

  return op;
}

void
TxnIndex::store(TxnNode *node)
{
  rbt_insert(this, node);
}

void
TxnIndex::remove(TxnNode *node)
{
  rbt_remove(this, node);
}

LocalTxn::LocalTxn(LocalEnv *env, const char *name, uint32_t flags)
  : Txn(env, name, flags), log_descriptor(0), oldest_op(0), newest_op(0)
{
  LocalTxnManager *ltm = (LocalTxnManager *)env->txn_manager.get();
  id = ltm->incremented_txn_id();

  /* append journal entry */
  if (env->journal.get() && notset(flags, UPS_TXN_TEMPORARY))
    env->journal->append_txn_begin(this, name, env->lsn_manager.next());
}

LocalTxn::~LocalTxn()
{
  free_operations();
}

void
LocalTxn::commit()
{
  /* are cursors attached to this txn? if yes, fail */
  if (unlikely(refcounter > 0)) {
    ups_trace(("Txn cannot be committed till all attached Cursors are closed"));
    throw Exception(UPS_CURSOR_STILL_OPEN);
  }

  /* this transaction is now committed! */
  flags |= kStateCommitted;
}

void
LocalTxn::abort()
{
  /* are cursors attached to this txn? if yes, fail */
  if (unlikely(refcounter > 0)) {
    ups_trace(("Txn cannot be aborted till all attached Cursors are closed"));
    throw Exception(UPS_CURSOR_STILL_OPEN);
  }

  /* this transaction is now aborted!  */
  flags |= kStateAborted;

  /* immediately release memory of the cached operations */
  free_operations();
}

void
LocalTxn::free_operations()
{
  TxnOperation *n, *op = oldest_op;

  while (op) {
    n = op->next_in_txn;
    TxnFactory::destroy_operation(op);
    op = n;
  }

  oldest_op = 0;
  newest_op = 0;
}

TxnIndex::TxnIndex(LocalDb *db)
  : db(db)
{
  rbt_new(this);
}

TxnIndex::~TxnIndex()
{
  TxnNode *node;

  while ((node = rbt_last(this))) {
    remove(node);
    delete node;
  }

  // re-initialize the tree
  rbt_new(this);
}

TxnNode *
TxnIndex::get(ups_key_t *key, uint32_t flags)
{
  TxnNode *node = 0;
  int match = 0;

  /* create a temporary node that we can search for */
  TxnNode tmp(db, key);

  /* search if node already exists - if yes, return it */
  if (isset(flags, UPS_FIND_GEQ_MATCH)) {
    node = rbt_nsearch(this, &tmp);
    if (node)
      match = compare(&tmp, node);
  }
  else if (isset(flags, UPS_FIND_LEQ_MATCH)) {
    node = rbt_psearch(this, &tmp);
    if (node)
      match = compare(&tmp, node);
  }
  else if (isset(flags, UPS_FIND_GT_MATCH)) {
    node = rbt_search(this, &tmp);
    if (node)
      node = node->next_sibling();
    else
      node = rbt_nsearch(this, &tmp);
    match = 1;
  }
  else if (isset(flags, UPS_FIND_LT_MATCH)) {
    node = rbt_search(this, &tmp);
    if (node)
      node = node->previous_sibling();
    else
      node = rbt_psearch(this, &tmp);
    match = -1;
  }
  else
    return rbt_search(this, &tmp);

  /* tree is empty? */
  if (!node)
    return 0;

  /* approx. matching: set the key flag */
  if (match < 0)
    ups_key_set_intflags(key, (ups_key_get_intflags(key)
            & ~BtreeKey::kApproximate) | BtreeKey::kLower);
  else if (match > 0)
    ups_key_set_intflags(key, (ups_key_get_intflags(key)
            & ~BtreeKey::kApproximate) | BtreeKey::kGreater);

  return node;
}

TxnNode *
TxnIndex::first()
{
  return rbt_first(this);
}

TxnNode *
TxnIndex::last()
{
  return rbt_last(this);
}

void
TxnIndex::enumerate(Context *context, TxnIndex::Visitor *visitor)
{
  TxnNode *node = rbt_first(this);

  while (node) {
    visitor->visit(context, node);
    node = rbt_next(this, node);
  }
}

struct KeyCounter : public TxnIndex::Visitor
{
  KeyCounter(LocalDb *_db, LocalTxn *_txn, bool _distinct)
    : counter(0), distinct(_distinct), txn(_txn), db(_db) {
  }

  void visit(Context *context, TxnNode *node) {
    BtreeIndex *be = db->btree_index.get();

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
    TxnOperation *op = node->newest_op;
    while (op) {
      LocalTxn *optxn = op->txn;
      if (optxn->is_aborted())
        ; // nop
      else if (optxn->is_committed() || txn == optxn) {
        if (isset(op->flags, TxnOperation::kIsFlushed))
          ; // nop
        // if key was erased then it doesn't exist
        else if (isset(op->flags, TxnOperation::kErase))
          return;
        else if (isset(op->flags, TxnOperation::kInsert)) {
          counter++;
          return;
        }
        // key exists - include it
        else if (isset(op->flags, TxnOperation::kInsert)
            || (isset(op->flags, TxnOperation::kInsertOverwrite))) {
          // check if the key already exists in the btree - if yes,
          // we do not count it (it will be counted later)
          if (UPS_KEY_NOT_FOUND == be->find(context, 0, node->key(), 0, 0, 0, 0))
            counter++;
          return;
        }
        else if (isset(op->flags, TxnOperation::kInsertDuplicate)) {
          // check if btree has other duplicates
          if (0 == be->find(context, 0, node->key(), 0, 0, 0, 0)) {
            // yes, there's another one
            if (distinct)
              return;
            counter++;
          }
          else {
            // check if other key is in this node
            counter++;
            if (distinct)
              return;
          }
        }
        else if (notset(op->flags, TxnOperation::kNop)) {
          assert(!"shouldn't be here");
          return;
        }
      }
      else { // txn is still active
        counter++;
      }

      op = op->previous_in_node;
    }
  }

  uint64_t counter;
  bool distinct;
  LocalTxn *txn;
  LocalDb *db;
};

uint64_t
TxnIndex::count(Context *context, LocalTxn *txn, bool distinct)
{
  KeyCounter k(db, txn, distinct);
  enumerate(context, &k);
  return k.counter;
}

void
LocalTxnManager::begin(Txn *txn)
{
  append_txn_at_tail(txn);
}

ups_status_t
LocalTxnManager::commit(Txn *htxn)
{
  LocalTxn *txn = dynamic_cast<LocalTxn *>(htxn);
  Context context(lenv(), txn, 0);

  try {
    txn->commit();

    /* append journal entry */
    if (lenv()->journal.get() && notset(txn->flags, UPS_TXN_TEMPORARY))
      lenv()->journal->append_txn_commit(txn, lenv()->lsn_manager.next());

    /* flush committed transactions */
    if (likely(notset(lenv()->flags(), UPS_DONT_FLUSH_TRANSACTIONS)))
      flush_committed_txns_impl(this, &context);
  }
  catch (Exception &ex) {
    return ex.code;
  }
  return 0;
}

ups_status_t
LocalTxnManager::abort(Txn *htxn)
{
  LocalTxn *txn = dynamic_cast<LocalTxn *>(htxn);
  Context context(lenv(), txn, 0);

  try {
    txn->abort();

    /* append journal entry */
    if (lenv()->journal.get() && notset(txn->flags, UPS_TXN_TEMPORARY))
      lenv()->journal->append_txn_abort(txn, lenv()->lsn_manager.next());

    /* no need to increment m_queued_{ops,bytes}_for_flush because this
     * operation does no longer contain any operations */
    if (likely(notset(lenv()->flags(), UPS_DONT_FLUSH_TRANSACTIONS)))
      flush_committed_txns_impl(this, &context);
  }
  catch (Exception &ex) {
    return ex.code;
  }
  return 0;
}

void
LocalTxnManager::flush_committed_txns(Context *context /* = 0 */)
{
  if (!context) {
    Context new_context(lenv(), 0, 0);
    flush_committed_txns_impl(this, &new_context);
  }
  else
    flush_committed_txns_impl(this, context);
}

uint64_t
LocalTxnManager::flush_txn(Context *context, LocalTxn *txn)
{
  TxnOperation *op = txn->oldest_op;
  TxnCursor *cursor = 0;
  uint64_t highest_lsn = 0;

  while (op) {
    TxnNode *node = op->node;

    if (isset(op->flags, TxnOperation::kIsFlushed))
      goto next_op;

    // perform the actual operation in the btree
    node->db->flush_txn_operation(context, txn, op);

    /*
     * this op is about to be flushed!
     *
     * as a consequence, all (txn)cursors which are coupled to this op
     * have to be uncoupled, as their parent (btree) cursor was
     * already coupled to the btree item instead
     */
    op->set_flushed();
next_op:
    while ((cursor = op->cursor_list)) {
      LocalCursor *pc = cursor->parent();
      assert(pc->get_txn_cursor() == cursor);
      pc->couple_to_btree(); // TODO merge both calls?
      if (!pc->is_nil(LocalCursor::kTxn))
        pc->set_to_nil(LocalCursor::kTxn);
    }

    assert(op->lsn > highest_lsn);
    highest_lsn = op->lsn;

    /* continue with the next operation of this txn */
    op = op->next_in_txn;
  }

  return highest_lsn;
}

} // namespace upscaledb
