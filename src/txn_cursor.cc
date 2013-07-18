/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include "txn_cursor.h"
#include "txn.h"
#include "db.h"
#include "env.h"
#include "mem.h"
#include "cursor.h"
#include "btree_cursor.h"

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

  m_coupled_next = op->get_cursors();
  m_coupled_previous = 0;

  if (op->get_cursors()) {
    TransactionCursor *old = op->get_cursors();
    old->m_coupled_previous = this;
  }

  op->set_cursors(this);
}

ham_status_t
TransactionCursor::overwrite(ham_record_t *record)
{
  Transaction *txn = m_parent->get_txn();

  if (is_nil())
    return (HAM_CURSOR_IS_NIL);

  TransactionNode *node = m_coupled_op->get_node();

  /* check if the op is part of a conflicting txn */
  if (has_conflict())
    return (HAM_TXN_CONFLICT);

  /* an overwrite is actually an insert w/ HAM_OVERWRITE of the
   * current key */
  return (((LocalDatabase *)get_db())->insert_txn(txn,
          node->get_key(), record, HAM_OVERWRITE, this));
}

ham_status_t
TransactionCursor::move_top_in_node(TransactionNode *node,
        TransactionOperation *op, bool ignore_conflicts, ham_u32_t flags)
{
  TransactionOperation *lastdup = 0;
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
      if ((op->get_flags() & TransactionOperation::TXN_OP_INSERT)
          || (op->get_flags() & TransactionOperation::TXN_OP_INSERT_OW)) {
        couple_to_op(op);
        return (0);
      }
      /* retrieve a duplicate key */
      if (op->get_flags() & TransactionOperation::TXN_OP_INSERT_DUP) {
        /* the duplicates are handled by the caller. here we only
         * couple to the first op */
        couple_to_op(op);
        return (0);
      }
      /* a normal erase will return an error (but we still couple the
       * cursor because the caller might need to know WHICH key was
       * deleted!) */
      if (op->get_flags() & TransactionOperation::TXN_OP_ERASE) {
        couple_to_op(op);
        return (HAM_KEY_ERASED_IN_TXN);
      }
      /* everything else is a bug! */
      ham_assert(op->get_flags() == TransactionOperation::TXN_OP_NOP);
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

  /* did we find a duplicate key? then return it */
  if (lastdup) {
    couple_to_op(op);
    return (0);
  }

  return (HAM_KEY_NOT_FOUND);
}

ham_status_t
TransactionCursor::move(ham_u32_t flags)
{
  ham_status_t st;
  TransactionNode *node;

  if (flags & HAM_CURSOR_FIRST) {
    /* first set cursor to nil */
    set_to_nil();

    node = get_db()->get_txn_index()->get_first();
    if (!node)
      return (HAM_KEY_NOT_FOUND);
    return (move_top_in_node(node, 0, false, flags));
  }
  else if (flags & HAM_CURSOR_LAST) {
    /* first set cursor to nil */
    set_to_nil();

    node = get_db()->get_txn_index()->get_last();
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
TransactionCursor::find(ham_key_t *key, ham_u32_t flags)
{
  TransactionNode *node = 0;

  /* first set cursor to nil */
  set_to_nil();

  /* then lookup the node */
  LocalDatabase *db = get_db();
  if (db->get_txn_index())
    node = db->get_txn_index()->get(key, flags);
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

ham_status_t
TransactionCursor::copy_coupled_key(ham_key_t *key)
{
  Transaction *txn = m_parent->get_txn();
  ham_key_t *source = 0;

  ByteArray *arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
              ? &get_db()->get_key_arena()
              : &txn->get_key_arena();

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
  }
  /* otherwise cursor is nil and we cannot return a key */
  else
    return (HAM_CURSOR_IS_NIL);

  return (0);
}

ham_status_t
TransactionCursor::copy_coupled_record(ham_record_t *record)
{
  ham_record_t *source = 0;
  Transaction *txn = m_parent->get_txn();

  ByteArray *arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
              ? &get_db()->get_record_arena()
              : &txn->get_record_arena();

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
    return (0);
  }

  /* otherwise cursor is nil and we cannot return a key */
  return (HAM_CURSOR_IS_NIL);
}

ham_status_t
TransactionCursor::get_record_size(ham_u64_t *psize)
{
  /* coupled cursor? get record from the txn_op structure */
  if (!is_nil()) {
    *psize = m_coupled_op->get_record()->size;
    return (0);
  }

  /* otherwise cursor is nil and we cannot return a key */
  return (HAM_CURSOR_IS_NIL);
}

ham_status_t
TransactionCursor::erase()
{
  ham_status_t st;
  TransactionNode *node;
  Transaction *txn = m_parent->get_txn();

  /* don't continue if cursor is nil */
  // TODO not nice to access the btree cursor here
  if (m_parent->get_btree_cursor()->get_state() == BtreeCursor::kStateNil
        && is_nil())
    return (HAM_CURSOR_IS_NIL);

  /*
   * !!
   * we have two cases:
   *
   * 1. the cursor is coupled to a btree item (or uncoupled, but not nil)
   *  and the txn_cursor is nil; in that case, we have to
   *    - uncouple the btree cursor
   *    - insert the erase-op for the key which is used by the btree cursor
   *
   * 2. the cursor is coupled to a txn-op; in this case, we have to
   *    - insert the erase-op for the key which is used by the txn-op
   */

  /* case 1 described above */
  // TODO uncoupling the btree cursor should be a private operation; i am not
  // convinced that it is necessary here
  if (is_nil()) {
    BtreeCursor *btc = m_parent->get_btree_cursor();
    if (btc->get_state() == BtreeCursor::kStateCoupled) {
      st = btc->uncouple_from_page();
      if (st)
        return (st);
    }
    st = get_db()->erase_txn(txn, btc->get_uncoupled_key(), 0, this);
    if (st)
      return (st);
  }
  /* case 2 described above */
  else {
    node = m_coupled_op->get_node();
    st = get_db()->erase_txn(txn, node->get_key(), 0, this);
    if (st)
      return (st);
  }

  return (0);
}

LocalDatabase *
TransactionCursor::get_db()
{
  return (m_parent->get_db());
}

ham_status_t
TransactionCursor::test_insert(ham_key_t *key, ham_record_t *record,
                ham_u32_t flags)
{
  Transaction *txn = m_parent->get_txn();

  return (get_db()->insert_txn(txn, key, record, flags, this));
}

bool
TransactionCursor::has_conflict() const
{
  const Transaction *txn = m_parent->get_txn();
  Transaction *optxn = m_coupled_op->get_txn();

  if (optxn != txn) {
    if (!optxn->is_committed() && !optxn->is_aborted())
      return (true);
  }

  return (false);
}

void
TransactionCursor::remove_cursor_from_op(TransactionOperation *op)
{
  ham_assert(!is_nil());

  if (op->get_cursors() == this) {
    op->set_cursors(m_coupled_next);
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
