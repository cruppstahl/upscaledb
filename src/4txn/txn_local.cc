/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "3btree/btree_index.h"
#include "3journal/journal.h"
#include "4txn/txn_local.h"
#include "4txn/txn_factory.h"
#include "4txn/txn_cursor.h"
#include "4env/env_local.h"
#include "4cursor/cursor.h"
#include "4context/context.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

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

  ham_key_t *lhskey = lhs->get_key();
  ham_key_t *rhskey = rhs->get_key();
  ham_assert(lhskey && rhskey);
  return (db->btree_index()->compare_keys(lhskey, rhskey));
}

rb_proto(static, rbt_, TransactionIndex, TransactionNode)
rb_gen(static, rbt_, TransactionIndex, TransactionNode, node, compare)

void
TransactionOperation::initialize(LocalTransaction *txn, TransactionNode *node,
            uint32_t flags, uint32_t orig_flags, uint64_t lsn,
            ham_key_t *key, ham_record_t *record)
{
  memset(this, 0, sizeof(*this));

  m_txn = txn;
  m_node = node;
  m_flags = flags;
  m_lsn = lsn;
  m_orig_flags = orig_flags;

  /* copy the key data */
  if (key) {
    m_key = *key;
    if (key->size) {
      m_key.data = &m_data[0];
      memcpy(m_key.data, key->data, key->size);
    }
  }

  /* copy the record data */
  if (record) {
    m_record = *record;
    if (record->size) {
      m_record.data = &m_data[key ? key->size : 0];
      memcpy(m_record.data, record->data, record->size);
    }
  }
}

void
TransactionOperation::destroy()
{
  bool delete_node = false;

  /* remove this op from the node */
  TransactionNode *node = get_node();
  if (node->get_oldest_op() == this) {
    /* if the node is empty: remove the node from the tree */
    // TODO should this be done in here??
    if (get_next_in_node() == 0) {
      node->get_db()->txn_index()->remove(node);
      delete_node = true;
    }
    node->set_oldest_op(get_next_in_node());
  }

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

  if (delete_node)
    delete node;

  Memory::release(this);
}

TransactionNode *
TransactionNode::get_next_sibling()
{
  return (rbt_next(get_db()->txn_index(), this));
}

TransactionNode *
TransactionNode::get_previous_sibling()
{
  return (rbt_prev(get_db()->txn_index(), this));
}

TransactionNode::TransactionNode(LocalDatabase *db, ham_key_t *key)
  : m_db(db), m_oldest_op(0), m_newest_op(0), m_key(key)
{
  /* make sure that a node with this key does not yet exist */
  // TODO re-enable this; currently leads to a stack overflow because
  // TransactionIndex::get() creates a new TransactionNode
  // ham_assert(TransactionIndex::get(key, 0) == 0);
}

TransactionNode::~TransactionNode()
{
}

TransactionOperation *
TransactionNode::append(LocalTransaction *txn, uint32_t orig_flags,
            uint32_t flags, uint64_t lsn, ham_key_t *key,
            ham_record_t *record)
{
  TransactionOperation *op = TransactionFactory::create_operation(txn,
                                    this, flags, orig_flags, lsn,
                                    key, record);

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

  // now that an operation is attached make sure that the node no
  // longer uses the temporary key pointer
  m_key = 0;

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
#ifdef HAM_DEBUG
  bool found = false;
  TransactionNode *n = rbt_first(this);
  while (n) {
    if (n == node) {
      found = true;
      break;
    }
    n = rbt_next(this, n);
  }
  ham_assert(found == true);
#endif

  rbt_remove(this, node);
}

LocalTransactionManager::LocalTransactionManager(Environment *env)
  : TransactionManager(env), m_txn_id(0), m_queued_txn_for_flush(0),
    m_queued_ops_for_flush(0), m_queued_bytes_for_flush(0),
    m_txn_threshold(kFlushTxnThreshold),
    m_ops_threshold(kFlushOperationsThreshold),
    m_bytes_threshold(kFlushBytesThreshold)
{
  if (m_env->get_flags() & HAM_FLUSH_WHEN_COMMITTED) {
    m_txn_threshold = 0;
    m_ops_threshold = 0;
    m_bytes_threshold = 0;
  }
}

LocalTransaction::LocalTransaction(LocalEnvironment *env, const char *name,
        uint32_t flags)
  : Transaction(env, name, flags), m_log_desc(0), m_oldest_op(0),
    m_newest_op(0), m_op_counter(0), m_accum_data_size(0)
{
  LocalTransactionManager *ltm = 
        (LocalTransactionManager *)env->txn_manager();
  m_id = ltm->get_incremented_txn_id();

  /* append journal entry */
  if (env->get_flags() & HAM_ENABLE_RECOVERY
      && env->get_flags() & HAM_ENABLE_TRANSACTIONS
      && !(flags & HAM_TXN_TEMPORARY)) {
    env->journal()->append_txn_begin(this, name,
            env->next_lsn());
  }
}

LocalTransaction::~LocalTransaction()
{
  free_operations();
}

void
LocalTransaction::commit(uint32_t flags)
{
  /* are cursors attached to this txn? if yes, fail */
  if (get_cursor_refcount()) {
    ham_trace(("Transaction cannot be committed till all attached "
          "Cursors are closed"));
    throw Exception(HAM_CURSOR_STILL_OPEN);
  }

  /* this transaction is now committed! */
  m_flags |= kStateCommitted;
}

void
LocalTransaction::abort(uint32_t flags)
{
  /* are cursors attached to this txn? if yes, fail */
  if (get_cursor_refcount()) {
    ham_trace(("Transaction cannot be aborted till all attached "
          "Cursors are closed"));
    throw Exception(HAM_CURSOR_STILL_OPEN);
  }

  /* this transaction is now aborted!  */
  m_flags |= kStateAborted;

  /* immediately release memory of the cached operations */
  free_operations();
}

void
LocalTransaction::free_operations()
{
  TransactionOperation *n, *op = get_oldest_op();

  while (op) {
    n = op->get_next_in_txn();
    TransactionFactory::destroy_operation(op);
    op = n;
  }

  set_oldest_op(0);
  set_newest_op(0);
}

TransactionIndex::TransactionIndex(LocalDatabase *db)
  : m_db(db)
{
  rbt_new(this);
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
TransactionIndex::get(ham_key_t *key, uint32_t flags)
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
TransactionIndex::enumerate(Context *context,
                TransactionIndex::Visitor *visitor)
{
  TransactionNode *node = rbt_first(this);

  while (node) {
    visitor->visit(context, node);
    node = rbt_next(this, node);
  }
}

struct KeyCounter : public TransactionIndex::Visitor
{
  KeyCounter(LocalDatabase *_db, LocalTransaction *_txn, bool _distinct)
    : counter(0), distinct(_distinct), txn(_txn), db(_db) {
  }

  void visit(Context *context, TransactionNode *node) {
    BtreeIndex *be = db->btree_index();
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
      LocalTransaction *optxn = op->get_txn();
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
          if (HAM_KEY_NOT_FOUND == be->find(context, 0, node->get_key(), 0, 0, 0, 0))
            counter++;
          return;
        }
        else if (op->get_flags() & TransactionOperation::kInsertDuplicate) {
          // check if btree has other duplicates
          if (0 == be->find(context, 0, node->get_key(), 0, 0, 0, 0)) {
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

  uint64_t counter;
  bool distinct;
  LocalTransaction *txn;
  LocalDatabase *db;
};

uint64_t
TransactionIndex::count(Context *context, LocalTransaction *txn, bool distinct)
{
  KeyCounter k(m_db, txn, distinct);
  enumerate(context, &k);
  return (k.counter);
}

void
LocalTransactionManager::begin(Transaction *txn)
{
  append_txn_at_tail(txn);
}

ham_status_t 
LocalTransactionManager::commit(Transaction *htxn, uint32_t flags)
{
  LocalTransaction *txn = dynamic_cast<LocalTransaction *>(htxn);
  Context context(lenv(), txn, 0);

  try {
    txn->commit(flags);

    /* append journal entry */
    if (m_env->get_flags() & HAM_ENABLE_RECOVERY
        && m_env->get_flags() & HAM_ENABLE_TRANSACTIONS
        && !(txn->get_flags() & HAM_TXN_TEMPORARY))
      lenv()->journal()->append_txn_commit(txn,
                      lenv()->next_lsn());

    /* flush committed transactions */
    m_queued_txn_for_flush++;
    m_queued_ops_for_flush += txn->get_op_counter();
    m_queued_bytes_for_flush += txn->get_accum_data_size();
    maybe_flush_committed_txns(&context);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
  return (0);
}

ham_status_t 
LocalTransactionManager::abort(Transaction *htxn, uint32_t flags)
{
  LocalTransaction *txn = dynamic_cast<LocalTransaction *>(htxn);
  Context context(lenv(), txn, 0);

  try {
    txn->abort(flags);

    /* append journal entry */
    if (m_env->get_flags() & HAM_ENABLE_RECOVERY
        && m_env->get_flags() & HAM_ENABLE_TRANSACTIONS
        && !(txn->get_flags() & HAM_TXN_TEMPORARY))
      lenv()->journal()->append_txn_abort(txn,
                      lenv()->next_lsn());

    /* flush committed transactions; while this one was not committed,
     * we might have cleared the way now to flush other committed
     * transactions */
    m_queued_txn_for_flush++;

    /* no need to increment m_queued_{ops,bytes}_for_flush because this
     * operation does no longer contain any operations */
    maybe_flush_committed_txns(&context);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
  return (0);
}

void
LocalTransactionManager::maybe_flush_committed_txns(Context *context)
{
  if (m_queued_txn_for_flush > m_txn_threshold
      || m_queued_ops_for_flush > m_ops_threshold
      || m_queued_bytes_for_flush > m_bytes_threshold)
    flush_committed_txns_impl(context);
}

void 
LocalTransactionManager::flush_committed_txns(Context *context /* = 0 */)
{
  if (!context) {
    Context new_context(lenv(), 0, 0);
    flush_committed_txns_impl(&new_context);
  }
  else
    flush_committed_txns_impl(context);
}

void 
LocalTransactionManager::flush_committed_txns_impl(Context *context)
{
  LocalTransaction *oldest;
  Journal *journal = lenv()->journal();
  uint64_t highest_lsn = 0;

  ham_assert(context->changeset.is_empty());

  /* always get the oldest transaction; if it was committed: flush
   * it; if it was aborted: discard it; otherwise return */
  while ((oldest = (LocalTransaction *)get_oldest_txn())) {
    if (oldest->is_committed()) {
      m_queued_ops_for_flush -= oldest->get_op_counter();
      ham_assert(m_queued_ops_for_flush >= 0);
      m_queued_bytes_for_flush -= oldest->get_accum_data_size();
      ham_assert(m_queued_bytes_for_flush >= 0);
      uint64_t lsn = flush_txn(context, (LocalTransaction *)oldest);
      if (lsn > highest_lsn)
        highest_lsn = lsn;

      /* this transaction was flushed! */
      if (journal && (oldest->get_flags() & HAM_TXN_TEMPORARY) == 0)
        journal->transaction_flushed(oldest);
    }
    else if (oldest->is_aborted()) {
      ; /* nop */
    }
    else
      break;

    /* it's possible that Transactions were aborted directly, and not through
     * the TransactionManager (i.e. in Journal::abort_uncommitted_txns).
     * so don't rely on m_queued_txn_for_flush, it might be zero */ 
    if (m_queued_txn_for_flush > 0)
      m_queued_txn_for_flush--;

    /* now remove the txn from the linked list */
    remove_txn_from_head(oldest);

    /* and release the memory */
    delete oldest;
  }

  /* now flush the changeset and write the modified pages to disk */
  if (highest_lsn && m_env->get_flags() & HAM_ENABLE_RECOVERY)
    context->changeset.flush(highest_lsn);
  else
    context->changeset.clear();

  ham_assert(context->changeset.is_empty());
}

uint64_t
LocalTransactionManager::flush_txn(Context *context, LocalTransaction *txn)
{
  TransactionOperation *op = txn->get_oldest_op();
  TransactionCursor *cursor = 0;
  uint64_t highest_lsn = 0;

  while (op) {
    TransactionNode *node = op->get_node();

    if (op->get_flags() & TransactionOperation::kIsFlushed)
      goto next_op;

    // perform the actual operation in the btree
    node->get_db()->flush_txn_operation(context, txn, op);

    /*
     * this op is about to be flushed!
     *
     * as a consequence, all (txn)cursors which are coupled to this op
     * have to be uncoupled, as their parent (btree) cursor was
     * already coupled to the btree item instead
     */
    op->set_flushed();
next_op:
    while ((cursor = op->cursor_list())) {
      Cursor *pc = cursor->get_parent();
      ham_assert(pc->get_txn_cursor() == cursor);
      pc->couple_to_btree(); // TODO merge both calls?
      if (!pc->is_nil(Cursor::kTxn))
        pc->set_to_nil(Cursor::kTxn);
    }

    ham_assert(op->get_lsn() > highest_lsn);
    highest_lsn = op->get_lsn();

    /* continue with the next operation of this txn */
    op = op->get_next_in_txn();
  }

  return (highest_lsn);
}

} // namespace hamsterdb
