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
#include "3btree/btree_cursor.h"
#include "4db/db.h"
#include "4txn/txn.h"
#include "4txn/txn_cursor.h"
#include "4txn/txn_local.h"
#include "4env/env.h"
#include "4cursor/cursor.h"
#include "4context/context.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

void
TransactionCursor::clone(const TransactionCursor *other)
{
  m_coupled_op = 0;
  m_coupled_next = 0;
  m_coupled_previous = 0;

  if (!other->is_nil())
    couple_to_op(other->get_coupled_op());
}

void
TransactionCursor::set_to_nil()
{
  /* uncoupled cursor? remove from the txn_op structure */
  if (!is_nil()) {
    TransactionOperation *op = get_coupled_op();
    if (op)
      remove_cursor_from_op(op);
    m_coupled_op = 0;
  }

  /* otherwise cursor is already nil */
}

void
TransactionCursor::couple_to_op(TransactionOperation *op)
{
  set_to_nil();
  m_coupled_op = op;

  m_coupled_next = op->cursor_list();
  m_coupled_previous = 0;

  if (op->cursor_list()) {
    TransactionCursor *old = op->cursor_list();
    old->m_coupled_previous = this;
  }

  op->set_cursor_list(this);
}

ham_status_t
TransactionCursor::overwrite(Context *context, LocalTransaction *txn,
                ham_record_t *record)
{
  ham_assert(context->txn == txn);

  if (is_nil())
    return (HAM_CURSOR_IS_NIL);

  TransactionNode *node = m_coupled_op->get_node();

  /* an overwrite is actually an insert w/ HAM_OVERWRITE of the
   * current key */
  return (((LocalDatabase *)get_db())->insert_txn(context, node->get_key(),
                          record, HAM_OVERWRITE, this));
}

ham_status_t
TransactionCursor::move_top_in_node(TransactionNode *node,
        TransactionOperation *op, bool ignore_conflicts, uint32_t flags)
{
  Transaction *optxn = 0;

  if (!op)
    op = node->get_newest_op();
  else
    goto next;

  while (op) {
    optxn = op->get_txn();
    /* only look at ops from the current transaction and from
     * committed transactions */
    if (optxn == m_parent->get_txn() || optxn->is_committed()) {
      /* a normal (overwriting) insert will return this key */
      if ((op->get_flags() & TransactionOperation::kInsert)
          || (op->get_flags() & TransactionOperation::kInsertOverwrite)) {
        couple_to_op(op);
        return (0);
      }
      /* retrieve a duplicate key */
      if (op->get_flags() & TransactionOperation::kInsertDuplicate) {
        /* the duplicates are handled by the caller. here we only
         * couple to the first op */
        couple_to_op(op);
        return (0);
      }
      /* a normal erase will return an error (but we still couple the
       * cursor because the caller might need to know WHICH key was
       * deleted!) */
      if (op->get_flags() & TransactionOperation::kErase) {
        couple_to_op(op);
        return (HAM_KEY_ERASED_IN_TXN);
      }
      /* everything else is a bug! */
      ham_assert(op->get_flags() == TransactionOperation::kNop);
    }
    else if (optxn->is_aborted())
      ; /* nop */
    else if (!ignore_conflicts) {
      /* we still have to couple, because higher-level functions
       * will need to know about the op when consolidating the trees */
      couple_to_op(op);
      return (HAM_TXN_CONFLICT);
    }

next:
    m_parent->set_dupecache_index(0);
    op = op->get_previous_in_node();
  }

  return (HAM_KEY_NOT_FOUND);
}

ham_status_t
TransactionCursor::move(uint32_t flags)
{
  ham_status_t st;
  TransactionNode *node;

  if (flags & HAM_CURSOR_FIRST) {
    /* first set cursor to nil */
    set_to_nil();

    node = get_db()->txn_index()->get_first();
    if (!node)
      return (HAM_KEY_NOT_FOUND);
    return (move_top_in_node(node, 0, false, flags));
  }
  else if (flags & HAM_CURSOR_LAST) {
    /* first set cursor to nil */
    set_to_nil();

    node = get_db()->txn_index()->get_last();
    if (!node)
      return (HAM_KEY_NOT_FOUND);
    return (move_top_in_node(node, 0, false, flags));
  }
  else if (flags & HAM_CURSOR_NEXT) {
    if (is_nil())
      return (HAM_CURSOR_IS_NIL);

    node = m_coupled_op->get_node();

    ham_assert(!is_nil());

    /* first move to the next key in the current node; if we fail,
     * then move to the next node. repeat till we've found a key or
     * till we've reached the end of the tree */
    while (1) {
      node = node->get_next_sibling();
      if (!node)
        return (HAM_KEY_NOT_FOUND);
      st = move_top_in_node(node, 0, true, flags);
      if (st == HAM_KEY_NOT_FOUND)
        continue;
      return (st);
    }
  }
  else if (flags & HAM_CURSOR_PREVIOUS) {
    if (is_nil())
      return (HAM_CURSOR_IS_NIL);

    node = m_coupled_op->get_node();

    ham_assert(!is_nil());

    /* first move to the previous key in the current node; if we fail,
     * then move to the previous node. repeat till we've found a key or
     * till we've reached the end of the tree */
    while (1) {
      node = node->get_previous_sibling();
      if (!node)
        return (HAM_KEY_NOT_FOUND);
      st = move_top_in_node(node, 0, true, flags);
      if (st == HAM_KEY_NOT_FOUND)
        continue;
      return (st);
    }
  }
  else {
    ham_assert(!"this flag is not yet implemented");
  }

  return (0);
}

ham_status_t
TransactionCursor::find(ham_key_t *key, uint32_t flags)
{
  TransactionNode *node = 0;

  /* first set cursor to nil */
  set_to_nil();

  /* then lookup the node */
  if (get_db()->txn_index())
    node = get_db()->txn_index()->get(key, flags);
  if (!node)
    return (HAM_KEY_NOT_FOUND);

  while (1) {
    /* and then move to the newest insert*-op */
    ham_status_t st = move_top_in_node(node, 0, false, 0);
    if (st != HAM_KEY_ERASED_IN_TXN)
      return (st);

    /* if the key was erased and approx. matching is enabled, then move
    * next/prev till we found a valid key. */
    if (flags & HAM_FIND_GT_MATCH)
      node = node->get_next_sibling();
    else if (flags & HAM_FIND_LT_MATCH)
      node = node->get_previous_sibling();
    else
      return (st);

    if (!node)
      return (HAM_KEY_NOT_FOUND);
  }

  ham_assert(!"should never reach this");
  return (0);
}

void
TransactionCursor::copy_coupled_key(ham_key_t *key)
{
  Transaction *txn = m_parent->get_txn();
  ham_key_t *source = 0;

  ByteArray *arena = &get_db()->key_arena(txn);

  /* coupled cursor? get key from the txn_op structure */
  if (!is_nil()) {
    TransactionNode *node = m_coupled_op->get_node();

    ham_assert(get_db() == node->get_db());
    source = node->get_key();

    key->size = source->size;
    if (source->data && source->size) {
      if (!(key->flags & HAM_KEY_USER_ALLOC)) {
        arena->resize(source->size);
        key->data = arena->get_ptr();
      }
      memcpy(key->data, source->data, source->size);
    }
    else
      key->data = 0;
    return;
  }

  /* otherwise cursor is nil and we cannot return a key */
  throw Exception(HAM_CURSOR_IS_NIL);
}

void
TransactionCursor::copy_coupled_record(ham_record_t *record)
{
  ham_record_t *source = 0;
  Transaction *txn = m_parent->get_txn();

  ByteArray *arena = &get_db()->record_arena(txn);

  /* coupled cursor? get record from the txn_op structure */
  if (!is_nil()) {
    source = m_coupled_op->get_record();

    record->size = source->size;
    if (source->data && source->size) {
      if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
        arena->resize(source->size);
        record->data = arena->get_ptr();
      }
      memcpy(record->data, source->data, source->size);
    }
    else
      record->data = 0;
    return;
  }

  /* otherwise cursor is nil and we cannot return a key */
  throw Exception(HAM_CURSOR_IS_NIL);
}

uint64_t
TransactionCursor::get_record_size()
{
  /* coupled cursor? get record from the txn_op structure */
  if (!is_nil())
    return (m_coupled_op->get_record()->size);

  /* otherwise cursor is nil and we cannot return a key */
  throw Exception(HAM_CURSOR_IS_NIL);
}

LocalDatabase *
TransactionCursor::get_db()
{
  return (m_parent->get_db());
}

ham_status_t
TransactionCursor::test_insert(ham_key_t *key, ham_record_t *record,
                uint32_t flags)
{
  LocalTransaction *txn = dynamic_cast<LocalTransaction *>(m_parent->get_txn());
  Context context(get_db()->lenv(), txn, get_db());

  return (get_db()->insert_txn(&context, key, record, flags, this));
}

void
TransactionCursor::remove_cursor_from_op(TransactionOperation *op)
{
  ham_assert(!is_nil());

  if (op->cursor_list() == this) {
    op->set_cursor_list(m_coupled_next);
    if (m_coupled_next)
      m_coupled_next->m_coupled_previous = 0;
  }
  else {
    if (m_coupled_next)
      m_coupled_next->m_coupled_previous = m_coupled_previous;
    if (m_coupled_previous)
      m_coupled_previous->m_coupled_next = m_coupled_next;
  }
  m_coupled_next = 0;
  m_coupled_previous = 0;
}

} // namespace hamsterdb
