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
#include "3btree/btree_cursor.h"
#include "4db/db_local.h"
#include "4txn/txn.h"
#include "4txn/txn_cursor.h"
#include "4txn/txn_local.h"
#include "4env/env.h"
#include "4cursor/cursor_local.h"
#include "4context/context.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

static inline LocalDb *
db(TxnCursorState &state_)
{
  return state_.parent->ldb();
}

static inline LocalEnvironment *
env(TxnCursorState &state_)
{
  return (LocalEnvironment *)state_.parent->ldb()->env;
}

static inline void
remove_cursor_from_op(TxnCursor *cursor, TxnOperation *op)
{
  TxnCursorState &state_ = cursor->state_;

  if (op->cursor_list == cursor) {
    op->cursor_list = state_.coupled_next;
    if (state_.coupled_next)
      state_.coupled_next->state_.coupled_previous = 0;
  }
  else {
    if (state_.coupled_next)
      state_.coupled_next->state_.coupled_previous = state_.coupled_previous;
    if (state_.coupled_previous)
      state_.coupled_previous->state_.coupled_next = state_.coupled_next;
  }
  state_.coupled_next = 0;
  state_.coupled_previous = 0;
}

static inline ups_status_t
move_top_in_node(TxnCursor *cursor, TxnNode *node,
                TxnOperation *op, bool ignore_conflicts, uint32_t flags)
{
  TxnCursorState &state_ = cursor->state_;
  Txn *optxn = 0;

  if (!op)
    op = node->newest_op;
  else
    goto next;

  while (op) {
    optxn = op->txn;
    /* only look at ops from the current transaction and from
     * committed transactions */
    if (optxn == state_.parent->txn || optxn->is_committed()) {
      /* a normal (overwriting) insert will return this key */
      if (isset(op->flags, TxnOperation::kInsert)
          || isset(op->flags, TxnOperation::kInsertOverwrite)) {
        cursor->couple_to_op(op);
        return 0;
      }
      /* retrieve a duplicate key */
      if (isset(op->flags, TxnOperation::kInsertDuplicate)) {
        /* the duplicates are handled by the caller. here we only
         * couple to the first op */
        cursor->couple_to_op(op);
        return 0;
      }
      /* a normal erase will return an error (but we still couple the
       * cursor because the caller might need to know WHICH key was
       * deleted!) */
      if (isset(op->flags, TxnOperation::kErase)) {
        cursor->couple_to_op(op);
        return UPS_KEY_ERASED_IN_TXN;
      }
      /* everything else is a bug! */
      assert(op->flags == TxnOperation::kNop);
    }
    else if (optxn->is_aborted())
      ; /* nop */
    else if (!ignore_conflicts) {
      /* we still have to couple, because higher-level functions
       * will need to know about the op when consolidating the trees */
      cursor->couple_to_op(op);
      return UPS_TXN_CONFLICT;
    }

next:
    state_.parent->set_duplicate_cache_index(0);
    op = op->previous_in_node;
  }

  return UPS_KEY_NOT_FOUND;
}

void
TxnCursor::clone(const TxnCursor *other)
{
  state_.coupled_op = 0;
  state_.coupled_next = 0;
  state_.coupled_previous = 0;

  if (!other->is_nil())
    couple_to_op(other->get_coupled_op());
}

void
TxnCursor::set_to_nil()
{
  /* uncoupled cursor? remove from the txn_op structure */
  if (!is_nil()) {
    TxnOperation *op = get_coupled_op();
    if (op)
      remove_cursor_from_op(this, op);
  }

  state_.coupled_op = 0;

  /* otherwise cursor is already nil */
}

void
TxnCursor::couple_to_op(TxnOperation *op)
{
  set_to_nil();
  state_.coupled_op = op;

  state_.coupled_next = op->cursor_list;
  state_.coupled_previous = 0;

  if (unlikely(op->cursor_list != 0)) {
    TxnCursor *old = op->cursor_list;
    old->state_.coupled_previous = this;
  }

  op->cursor_list = this;
}

ups_status_t
TxnCursor::move(uint32_t flags)
{
  ups_status_t st;
  TxnNode *node;

  if (isset(flags, UPS_CURSOR_FIRST)) {
    /* first set cursor to nil */
    set_to_nil();

    node = db(state_)->txn_index->first();
    if (unlikely(!node))
      return UPS_KEY_NOT_FOUND;
    return move_top_in_node(this, node, 0, false, flags);
  }

  if (isset(flags, UPS_CURSOR_LAST)) {
    /* first set cursor to nil */
    set_to_nil();

    node = db(state_)->txn_index->last();
    if (unlikely(!node))
      return UPS_KEY_NOT_FOUND;
    return move_top_in_node(this, node, 0, false, flags);
  }

  if (isset(flags, UPS_CURSOR_NEXT)) {
    if (unlikely(is_nil()))
      return UPS_CURSOR_IS_NIL;

    node = state_.coupled_op->node;

    /* first move to the next key in the current node; if we fail,
     * then move to the next node. repeat till we've found a key or
     * till we've reached the end of the tree */
    while (1) {
      node = node->next_sibling();
      if (!node)
        return UPS_KEY_NOT_FOUND;
      st = move_top_in_node(this, node, 0, true, flags);
      if (st == UPS_KEY_NOT_FOUND)
        continue;
      return st;
    }
  }

  if (isset(flags, UPS_CURSOR_PREVIOUS)) {
    if (unlikely(is_nil()))
      return UPS_CURSOR_IS_NIL;

    node = state_.coupled_op->node;

    /* first move to the previous key in the current node; if we fail,
     * then move to the previous node. repeat till we've found a key or
     * till we've reached the end of the tree */
    while (1) {
      node = node->previous_sibling();
      if (!node)
        return UPS_KEY_NOT_FOUND;
      st = move_top_in_node(this, node, 0, true, flags);
      if (st == UPS_KEY_NOT_FOUND)
        continue;
      return st;
    }
  }

  assert(!"shouldn't be here");
  return 0;
}

ups_status_t
TxnCursor::find(ups_key_t *key, uint32_t flags)
{
  TxnNode *node = 0;

  /* first set cursor to nil */
  set_to_nil();

  /* then lookup the node */
  node = db(state_)->txn_index->get(key, flags);
  if (!node)
    return UPS_KEY_NOT_FOUND;

  while (1) {
    /* and then move to the newest insert*-op */
    ups_status_t st = move_top_in_node(this, node, 0, false, 0);
    if (unlikely(st != UPS_KEY_ERASED_IN_TXN))
      return st;

    /* if the key was erased and approx. matching is enabled, then move
    * next/prev till we found a valid key. */
    if (isset(flags, UPS_FIND_GT_MATCH))
      node = node->next_sibling();
    else if (isset(flags, UPS_FIND_LT_MATCH))
      node = node->previous_sibling();
    else
      return st;

    if (!node)
      return UPS_KEY_NOT_FOUND;
  }

  assert(!"shouldn't be here");
  return 0;
}

void
TxnCursor::copy_coupled_key(ups_key_t *key)
{
  Txn *txn = state_.parent->txn;
  ups_key_t *source = 0;

  ByteArray *arena = &db(state_)->key_arena(txn);

  if (unlikely(is_nil()))
    throw Exception(UPS_CURSOR_IS_NIL);

  /* coupled cursor? get key from the txn_op structure */
  TxnNode *node = state_.coupled_op->node;
  assert(db(state_) == node->db);

  source = node->key();
  key->size = source->size;

  if (likely(source->data && source->size)) {
    if (notset(key->flags, UPS_KEY_USER_ALLOC)) {
      arena->resize(source->size);
      key->data = arena->data();
    }
    ::memcpy(key->data, source->data, source->size);
  }
  else
    key->data = 0;
}

void
TxnCursor::copy_coupled_record(ups_record_t *record)
{
  Txn *txn = state_.parent->txn;
  ups_record_t *source = 0;

  ByteArray *arena = &db(state_)->record_arena(txn);

  if (unlikely(is_nil()))
    throw Exception(UPS_CURSOR_IS_NIL);

  /* coupled cursor? get record from the txn_op structure */
  source = &state_.coupled_op->record;
  record->size = source->size;

  if (likely(source->data && source->size)) {
    if (notset(record->flags, UPS_RECORD_USER_ALLOC)) {
      arena->resize(source->size);
      record->data = arena->data();
    }
    ::memcpy(record->data, source->data, source->size);
  }
  else
    record->data = 0;
}

uint32_t
TxnCursor::record_size()
{
  if (unlikely(is_nil()))
    throw Exception(UPS_CURSOR_IS_NIL);

  return state_.coupled_op->record.size;
}

ups_status_t
TxnCursor::test_insert(ups_key_t *key, ups_record_t *record,
                uint32_t flags)
{
  LocalTxn *txn = dynamic_cast<LocalTxn *>(state_.parent->txn);
  Context context(env(state_), txn, db(state_));

  return db(state_)->insert_txn(&context, key, record, flags, this);
}

} // namespace upscaledb
