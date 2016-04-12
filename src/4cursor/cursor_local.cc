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

#include <string.h>

// Always verify that a file of level N does not include headers > N!
#include "3btree/btree_cursor.h"
#include "3btree/btree_index.h"
#include "3btree/btree_node_proxy.h"
#include "3page_manager/page_manager.h"
#include "4cursor/cursor_local.h"
#include "4env/env_local.h"
#include "4txn/txn_local.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

using namespace upscaledb;

// Returns the LocalEnv instance
static inline LocalEnv *
lenv(LocalCursor *cursor)
{
  return ((LocalEnv *)cursor->db->env);
}

// Returns the Database that this cursor is operating on
static inline LocalDb *
ldb(LocalCursor *cursor)
{
  return (LocalDb *)cursor->db;
}

// Clears the dupecache and disconnect the Cursor from any duplicate key
static inline void
clear_duplicate_cache(LocalCursor *cursor)
{
  cursor->duplicate_cache.clear();
  cursor->duplicate_cache_index = 0;
}

// Appends the duplicates of the BtreeCursor to the duplicate cache.
static inline void
append_btree_duplicates(LocalCursor *cursor, Context *context)
{
  uint32_t count = cursor->btree_cursor.record_count(context, 0);
  for (uint32_t i = 0; i < count; i++)
    cursor->duplicate_cache.push_back(DuplicateCacheLine(true, i));
}

// Updates (or builds) the duplicate cache for a cursor
//
// The |what| parameter specifies if the dupecache is initialized from
// btree (kBtree), from txn (kTxn) or both.
static inline void
update_duplicate_cache(LocalCursor *cursor, Context *context, uint32_t what)
{
  if (notset(cursor->db->flags(), UPS_ENABLE_DUPLICATE_KEYS))
    return;

  /* if the cache already exists: no need to continue, it should be
   * up to date */
  if (!cursor->duplicate_cache.empty())
    return;

  if ((what & LocalCursor::kBtree) && (what & LocalCursor::kTxn)) {
    if (cursor->is_nil(LocalCursor::kBtree) && !cursor->is_nil(LocalCursor::kTxn)) {
      bool equal_keys;
      cursor->synchronize(context, 0, &equal_keys);
      if (!equal_keys)
        cursor->set_to_nil(LocalCursor::kBtree);
    }
  }

  /* first collect all duplicates from the btree. They're already sorted,
   * therefore we can just append them to our duplicate-cache. */
  if ((what & LocalCursor::kBtree) && !cursor->is_nil(LocalCursor::kBtree))
    append_btree_duplicates(cursor, context);

  /* read duplicates from the txn-cursor? */
  if ((what & LocalCursor::kTxn) && !cursor->is_nil(LocalCursor::kTxn)) {
    TxnOperation *op = cursor->txn_cursor.get_coupled_op();
    TxnNode *node = op ? op->node : 0;

    if (!node)
      return;

    /* now start integrating the items from the transactions */
    for (op = node->oldest_op; op; op = op->next_in_node) {
      Txn *optxn = op->txn;
      /* collect all ops that are valid (even those that are
       * from conflicting transactions) */
      if (unlikely(optxn->is_aborted()))
        continue;

      /* a normal (overwriting) insert will overwrite ALL duplicates,
       * but an overwrite of a duplicate will only overwrite
       * an entry in the DuplicateCache */
      if (isset(op->flags, TxnOperation::kInsert)) {
        /* all existing duplicates are overwritten */
        cursor->duplicate_cache.clear();
        cursor->duplicate_cache.push_back(DuplicateCacheLine(false, op));
        continue;
      }

      if (isset(op->flags, TxnOperation::kInsertOverwrite)) {
        uint32_t ref = op->referenced_duplicate;
        if (ref) {
          assert(ref <= cursor->duplicate_cache.size());
          DuplicateCacheLine *e = &cursor->duplicate_cache[0];
          (&e[ref - 1])->set_txn_op(op);
        }
        else {
          /* all existing duplicates are overwritten */
          cursor->duplicate_cache.clear();
          cursor->duplicate_cache.push_back(DuplicateCacheLine(false, op));
        }
        continue;
      }

      /* insert a duplicate key */
      if (isset(op->flags, TxnOperation::kInsertDuplicate)) {
        uint32_t of = op->original_flags;
        uint32_t ref = op->referenced_duplicate - 1;
        DuplicateCacheLine dcl(false, op);
        if (isset(of, UPS_DUPLICATE_INSERT_FIRST))
          cursor->duplicate_cache.insert(cursor->duplicate_cache.begin(), dcl);
        else if (isset(of, UPS_DUPLICATE_INSERT_BEFORE)) {
          cursor->duplicate_cache.insert(cursor->duplicate_cache.begin() + ref, dcl);
        }
        else if (isset(of, UPS_DUPLICATE_INSERT_AFTER)) {
          if (ref + 1 >= cursor->duplicate_cache.size())
            cursor->duplicate_cache.push_back(dcl);
          else
          cursor->duplicate_cache.insert(cursor->duplicate_cache.begin() + ref + 1, dcl);
        }
        else /* default is UPS_DUPLICATE_INSERT_LAST */
          cursor->duplicate_cache.push_back(dcl);
        continue;
      }

      /* a normal erase will erase ALL duplicate keys */
      if (isset(op->flags, TxnOperation::kErase)) {
        uint32_t ref = op->referenced_duplicate;
        if (ref) {
          assert(ref <= cursor->duplicate_cache.size());
          cursor->duplicate_cache.erase(cursor->duplicate_cache.begin() + (ref - 1));
        }
        else {
          /* all existing duplicates are erased */
          cursor->duplicate_cache.clear();
        }
        continue;
      }

      /* everything else is a bug! */
      assert(op->flags == TxnOperation::kNop);
    }
  }
}

// Checks if a btree cursor points to a key that was overwritten or erased
// in the txn-cursor
//
// This is needed when moving the cursor backwards/forwards
// and consolidating the btree and the txn-tree
static inline ups_status_t
check_if_btree_key_is_erased_or_overwritten(LocalCursor *cursor,
                Context *context)
{
  ups_key_t key = {0};
  TxnOperation *op;
  // TODO will leak if an exception is thrown
  LocalCursor *clone = new LocalCursor(*cursor);

  ups_status_t st = cursor->btree_cursor.move(context, &key,
                  &cursor->db->key_arena(cursor->txn), 0, 0, 0);
  if (st) {
    clone->close();
    delete clone;
    return st;
  }

  st = clone->txn_cursor.find(&key, 0);
  if (st) {
    clone->close();
    delete clone;
    return st;
  }

  op = clone->txn_cursor.get_coupled_op();
  if (isset(op->flags, TxnOperation::kInsertDuplicate))
    st = UPS_KEY_NOT_FOUND;
  clone->close();
  delete clone;
  return st;
}

static inline bool
__txn_cursor_is_erase(TxnCursor *txnc)
{
  TxnOperation *op = txnc->get_coupled_op();
  return op ? isset(op->flags, TxnOperation::kErase) : false;
}

// Compares btree and txn-cursor; stores result in lastcmp
static inline int
compare(LocalCursor *cursor, Context *context)
{
  TxnNode *node = cursor->txn_cursor.get_coupled_op()->node;

  assert(!cursor->is_nil(0));
  assert(!cursor->txn_cursor.is_nil());

  // call btree_cursor.compare() and let the btree cursor deal
  // with its state (coupled vs uncoupled)
  return cursor->m_last_cmp
            = cursor->btree_cursor.compare(context, node->key());
}


LocalCursor::LocalCursor(LocalDb *db, Txn *txn)
  : Cursor(db, txn), txn_cursor(this), btree_cursor(this),
    duplicate_cache_index(0), m_last_operation(0), m_flags(0), m_last_cmp(0),
    m_is_first_use(true)
{
}

LocalCursor::LocalCursor(LocalCursor &other)
  : Cursor(other), txn_cursor(this), btree_cursor(this)
{
  txn = other.txn;
  next = other.next;
  previous = other.previous;
  duplicate_cache_index = other.duplicate_cache_index;
  m_last_operation = other.m_last_operation;
  m_last_cmp = other.m_last_cmp;
  m_flags = other.m_flags;
  m_is_first_use = other.m_is_first_use;

  btree_cursor.clone(&other.btree_cursor);
  txn_cursor.clone(&other.txn_cursor);

  if (isset(db->flags(), UPS_ENABLE_DUPLICATE_KEYS))
    other.duplicate_cache = duplicate_cache;
}

void
LocalCursor::couple_to_duplicate(uint32_t duplicate_index)
{
  assert(duplicate_cache.size() >= duplicate_index);
  assert(duplicate_index >= 1);

  /* duplicate_index is a 1-based index! */
  DuplicateCacheLine &e = duplicate_cache[duplicate_index - 1];
  if (e.use_btree()) {
    couple_to_btree();
    btree_cursor.set_duplicate_index(e.btree_duplicate_index());
  }
  else {
    assert(e.txn_op() != 0);
    couple_to_txnop(e.txn_op());
  }
  duplicate_cache_index = duplicate_index;
}

void
LocalCursor::synchronize(Context *context, uint32_t flags, bool *equal_keys)
{
  if (equal_keys)
    *equal_keys = false;

  if (is_nil(kBtree)) {
    if (!txn_cursor.get_coupled_op())
      return;
    ups_key_t *key = txn_cursor.get_coupled_op()->node->key();

    if (notset(flags, kSyncOnlyEqualKeys))
      flags = flags | (isset(flags, UPS_CURSOR_NEXT)
                            ? UPS_FIND_GEQ_MATCH
                            : UPS_FIND_LEQ_MATCH);
    /* the flag |kSyncDontLoadKey| does not load the key if there's an
     * approx match - it only positions the cursor */
    ups_status_t st = btree_cursor.find(context, key, 0, 0, 0,
                            kSyncDontLoadKey | flags);
    /* if we had a direct hit instead of an approx. match then
     * set |equal_keys| to false; otherwise Cursor::move()
     * will move the btree cursor again */
    if (st == 0 && equal_keys && !ups_key_get_approximate_match_type(key))
      *equal_keys = true;
  }
  else if (is_nil(kTxn)) {
    // TODO will leak if an exception is thrown
    LocalCursor *clone = new LocalCursor(*this);
    clone->btree_cursor.uncouple_from_page(context);
    ups_key_t *key = clone->btree_cursor.uncoupled_key();
    if (notset(flags, kSyncOnlyEqualKeys))
      flags = flags | (isset(flags, UPS_CURSOR_NEXT)
                            ? UPS_FIND_GEQ_MATCH
                            : UPS_FIND_LEQ_MATCH);

    ups_status_t st = txn_cursor.find(key, kSyncDontLoadKey | flags);
    /* if we had a direct hit instead of an approx. match then
    * set |equal_keys| to false; otherwise Cursor::move()
    * will move the btree cursor again */
    if (st == 0 && equal_keys && !ups_key_get_approximate_match_type(key))
      *equal_keys = true;
    clone->close();
    delete clone;
  }
}

uint32_t
LocalCursor::duplicate_cache_count(Context *context, bool clear_cache)
{
  if (notset(db->flags(), UPS_ENABLE_DUPLICATE_KEYS))
    return 0;

  if (clear_cache)
    clear_duplicate_cache(this);

  if (is_coupled_to_txnop())
    update_duplicate_cache(this, context, kBtree | kTxn);
  else
    update_duplicate_cache(this, context, kBtree);
  return duplicate_cache.size();
}

ups_status_t
LocalCursor::move_next_duplicate(Context *context)
{
  if (duplicate_cache_index > 0
          && duplicate_cache_index < duplicate_cache.size()) {
    duplicate_cache_index++;
    couple_to_duplicate(duplicate_cache_index);
    return 0;
  }
  return UPS_LIMITS_REACHED;
}

ups_status_t
LocalCursor::move_previous_duplicate(Context *context)
{
  if (duplicate_cache_index > 1) {
    duplicate_cache_index--;
    couple_to_duplicate(duplicate_cache_index);
    return 0;
  }
  return UPS_LIMITS_REACHED;
}

ups_status_t
LocalCursor::move_first_duplicate(Context *context)
{
  if (duplicate_cache.size()) {
    duplicate_cache_index = 1;
    couple_to_duplicate(duplicate_cache_index);
    return 0;
  }
  return UPS_LIMITS_REACHED;
}

ups_status_t
LocalCursor::move_last_duplicate(Context *context)
{
  if (duplicate_cache.size()) {
    duplicate_cache_index = duplicate_cache.size();
    couple_to_duplicate(duplicate_cache_index);
    return 0;
  }
  return UPS_LIMITS_REACHED;
}

ups_status_t
LocalCursor::move_next_key_singlestep(Context *context)
{
  ups_status_t st = 0;

  /* make sure that the cursor advances if the other cursor is nil */
  if ((is_nil(kTxn) && !is_nil(kBtree))
      || (is_nil(kBtree) && !is_nil(kTxn))) {
    m_last_cmp = 0;
  }

  /* if both cursors point to the same key: move next with both */
  if (m_last_cmp == 0) {
    if (!is_nil(kBtree)) {
      st = btree_cursor.move(context, 0, 0, 0, 0,
                    UPS_CURSOR_NEXT | UPS_SKIP_DUPLICATES);
      if (st == UPS_KEY_NOT_FOUND || st == UPS_CURSOR_IS_NIL) {
        set_to_nil(kBtree);
        if (unlikely(txn_cursor.is_nil()))
          return UPS_KEY_NOT_FOUND;
        else {
          couple_to_txnop();
          m_last_cmp = 1;
        }
      }
    }

    if (!txn_cursor.is_nil()) {
      st = txn_cursor.move(UPS_CURSOR_NEXT);
      if (st == UPS_KEY_NOT_FOUND || st == UPS_CURSOR_IS_NIL) {
        set_to_nil(kTxn);
        if (unlikely(is_nil(kBtree)))
          return UPS_KEY_NOT_FOUND;
        else {
          couple_to_btree();
          m_last_cmp = -1;

          ups_status_t st2 = check_if_btree_key_is_erased_or_overwritten(this, context);
          if (st2 == UPS_TXN_CONFLICT)
            st = st2;
        }
      }
    }
  }
  /* if the btree-key is smaller: move it next */
  else if (m_last_cmp < 0) {
    st = btree_cursor.move(context, 0, 0, 0, 0,
                    UPS_CURSOR_NEXT | UPS_SKIP_DUPLICATES);
    if (st == UPS_KEY_NOT_FOUND) {
      set_to_nil(kBtree);
      if (txn_cursor.is_nil())
        return st;
      couple_to_txnop();
      m_last_cmp = +1;
    }
    else {
      ups_status_t st2 = check_if_btree_key_is_erased_or_overwritten(this, context);
      if (st2 == UPS_TXN_CONFLICT)
        st = st2;
    }
    if (txn_cursor.is_nil())
      m_last_cmp = -1;
  }
  /* if the txn-key is smaller OR if both keys are equal: move next
   * with the txn-key (which is chronologically newer) */
  else {
    st = txn_cursor.move(UPS_CURSOR_NEXT);
    if (st == UPS_KEY_NOT_FOUND) {
      set_to_nil(kTxn);
      if (is_nil(kBtree))
        return st;
      couple_to_btree();
      m_last_cmp = -1;
    }
    if (is_nil(kBtree))
      m_last_cmp = 1;
  }

  /* compare keys again */
  if (!is_nil(kBtree) && !txn_cursor.is_nil())
    compare(this, context);

  /* if there's a txn conflict: move next */
  if (st == UPS_TXN_CONFLICT)
    return move_next_key_singlestep(context);

  /* btree-key is smaller */
  if (m_last_cmp < 0 || txn_cursor.is_nil()) {
    couple_to_btree();
    update_duplicate_cache(this, context, kBtree);
    return 0;
  }

  /* txn-key is smaller */
  if (m_last_cmp > 0 || btree_cursor.is_nil()) {
    couple_to_txnop();
    update_duplicate_cache(this, context, kTxn);
    return 0;
  }

  /* both keys are equal */
  couple_to_txnop();
  update_duplicate_cache(this, context, kTxn | kBtree);
  return 0;
}

ups_status_t
LocalCursor::move_next_key(Context *context, uint32_t flags)
{
  ups_status_t st;

  /* are we in the middle of a duplicate list? if yes then move to the
   * next duplicate */
  if (duplicate_cache_index > 0 && notset(flags, UPS_SKIP_DUPLICATES)) {
    st = move_next_duplicate(context);
    if (st != UPS_LIMITS_REACHED)
      return st;
    if (st == UPS_LIMITS_REACHED && isset(flags, UPS_ONLY_DUPLICATES))
      return UPS_KEY_NOT_FOUND;
  }

  clear_duplicate_cache(this);

  /* either there were no duplicates or we've reached the end of the
   * duplicate list. move next till we found a new candidate */
  while (1) {
    st = move_next_key_singlestep(context);
    if (unlikely(st))
      return st;

    /* check for duplicates. the duplicate cache was already updated in
     * move_next_key_singlestep() */
    if (isset(db->flags(), UPS_ENABLE_DUPLICATE_KEYS)) {
      /* are there any duplicates? if not then they were all erased and
       * we move to the previous key */
      if (unlikely(duplicate_cache.empty()))
        continue;

      /* otherwise move to the first duplicate */
      return move_first_duplicate(context);
    }

    /* no duplicates - make sure that we've not coupled to an erased item */
    if (is_coupled_to_txnop()) {
      if (__txn_cursor_is_erase(&txn_cursor))
        continue;
      return 0;
    }

    if (is_coupled_to_btree()) {
      st = check_if_btree_key_is_erased_or_overwritten(this, context);
      if (st == UPS_KEY_ERASED_IN_TXN)
        continue;
      if (st == 0) {
        couple_to_txnop();
        return 0;
      }
      if (st == UPS_KEY_NOT_FOUND)
        return 0;
      return st;
    }

    return UPS_KEY_NOT_FOUND;
  }

  assert(!"should never reach this");
  return UPS_INTERNAL_ERROR;
}

ups_status_t
LocalCursor::move_previous_key_singlestep(Context *context)
{
  ups_status_t st = 0;

  /* make sure that the cursor advances if the other cursor is nil */
  if ((is_nil(kTxn) && !is_nil(kBtree))
      || (is_nil(kBtree) && !is_nil(kTxn))) {
    m_last_cmp = 0;
  }

  /* if both cursors point to the same key: move previous with both */
  if (m_last_cmp == 0) {
    if (!is_nil(kBtree)) {
      st = btree_cursor.move(context, 0, 0, 0, 0,
                    UPS_CURSOR_PREVIOUS | UPS_SKIP_DUPLICATES);
      if (st == UPS_KEY_NOT_FOUND || st == UPS_CURSOR_IS_NIL) {
        set_to_nil(kBtree);
        if (txn_cursor.is_nil())
          return UPS_KEY_NOT_FOUND;
        else {
          couple_to_txnop();
          m_last_cmp = -1;
        }
      }
    }

    if (!txn_cursor.is_nil()) {
      st = txn_cursor.move(UPS_CURSOR_PREVIOUS);
      if (st == UPS_KEY_NOT_FOUND || st == UPS_CURSOR_IS_NIL) {
        set_to_nil(kTxn);
        if (is_nil(kBtree))
          return UPS_KEY_NOT_FOUND;
        else {
          couple_to_btree();
          m_last_cmp = 1;
        }
      }
    }
  }
  /* if the btree-key is greater: move previous */
  else if (m_last_cmp > 0) {
    st = btree_cursor.move(context, 0, 0, 0, 0,
                    UPS_CURSOR_PREVIOUS | UPS_SKIP_DUPLICATES);
    if (st == UPS_KEY_NOT_FOUND) {
      set_to_nil(kBtree);
      if (txn_cursor.is_nil())
        return st;
      couple_to_txnop();
      m_last_cmp = -1;
    }
    else {
      ups_status_t st2 = check_if_btree_key_is_erased_or_overwritten(this, context);
      if (st2 == UPS_TXN_CONFLICT)
        st = st2;
    }
    if (txn_cursor.is_nil())
      m_last_cmp = 1;
  }
  /* if the txn-key is greater OR if both keys are equal: move previous
   * with the txn-key (which is chronologically newer) */
  else {
    st = txn_cursor.move(UPS_CURSOR_PREVIOUS);
    if (st == UPS_KEY_NOT_FOUND) {
      set_to_nil(kTxn);
      if (is_nil(kBtree))
        return st;
      couple_to_btree();
      m_last_cmp = 1;

      ups_status_t st2 = check_if_btree_key_is_erased_or_overwritten(this, context);
      if (st2 == UPS_TXN_CONFLICT)
        st = st2;
    }
    if (is_nil(kBtree))
      m_last_cmp = -1;
  }

  /* compare keys again */
  if (!is_nil(kBtree) && !txn_cursor.is_nil())
    compare(this, context);

  /* if there's a txn conflict: move previous */
  if (st == UPS_TXN_CONFLICT)
    return move_previous_key_singlestep(context);

  /* btree-key is greater */
  if (m_last_cmp > 0 || txn_cursor.is_nil()) {
    couple_to_btree();
    update_duplicate_cache(this, context, kBtree);
    return 0;
  }

  /* txn-key is greater */
  if (m_last_cmp < 0 || btree_cursor.is_nil()) {
    couple_to_txnop();
    update_duplicate_cache(this, context, kTxn);
    return 0;
  }

  /* both keys are equal */
  couple_to_txnop();
  update_duplicate_cache(this, context, kTxn | kBtree);
  return 0;
}

ups_status_t
LocalCursor::move_previous_key(Context *context, uint32_t flags)
{
  ups_status_t st;

  /* are we in the middle of a duplicate list? if yes then move to the
   * previous duplicate */
  if (duplicate_cache_index > 0 && notset(flags, UPS_SKIP_DUPLICATES)) {
    st = move_previous_duplicate(context);
    if (st != UPS_LIMITS_REACHED)
      return st;
    if (st == UPS_LIMITS_REACHED && isset(flags, UPS_ONLY_DUPLICATES))
      return UPS_KEY_NOT_FOUND;
  }

  clear_duplicate_cache(this);

  /* either there were no duplicates or we've reached the end of the
   * duplicate list. move previous till we found a new candidate */
  while (!is_nil(kBtree) || !txn_cursor.is_nil()) {
    st = move_previous_key_singlestep(context);
    if (unlikely(st))
      return st;

    /* check for duplicates. the duplicate cache was already updated in
     * move_previous_key_singlestep() */
    if (isset(db->flags(), UPS_ENABLE_DUPLICATE_KEYS)) {
      /* are there any duplicates? if not then they were all erased and
       * we move to the previous key */
      if (duplicate_cache.empty())
        continue;

      /* otherwise move to the last duplicate */
      return move_last_duplicate(context);
    }

    /* no duplicates - make sure that we've not coupled to an erased
     * item */
    if (is_coupled_to_txnop()) {
      if (__txn_cursor_is_erase(&txn_cursor))
        continue;
      return 0;
    }

    if (is_coupled_to_btree()) {
      st = check_if_btree_key_is_erased_or_overwritten(this, context);
      if (st == UPS_KEY_ERASED_IN_TXN)
        continue;
      if (st == 0) {
        couple_to_txnop();
        return 0;
      }
      if (st == UPS_KEY_NOT_FOUND)
        return 0;
      return st;
    }

    return UPS_KEY_NOT_FOUND;
  }

  return UPS_KEY_NOT_FOUND;
}

ups_status_t
LocalCursor::move_first_key_singlestep(Context *context)
{
  ups_status_t btrs, txns;

  /* fetch the smallest key from the transaction tree. */
  txns = txn_cursor.move(UPS_CURSOR_FIRST);
  /* fetch the smallest key from the btree tree. */
  btrs = btree_cursor.move(context, 0, 0, 0, 0,
                UPS_CURSOR_FIRST | UPS_SKIP_DUPLICATES);
  /* now consolidate - if both trees are empty then return */
  if (btrs == UPS_KEY_NOT_FOUND && txns == UPS_KEY_NOT_FOUND)
    return UPS_KEY_NOT_FOUND;

  /* if btree is empty but txn-tree is not: couple to txn */
  if (btrs == UPS_KEY_NOT_FOUND && txns != UPS_KEY_NOT_FOUND) {
    if (txns == UPS_TXN_CONFLICT)
      return txns;
    couple_to_txnop();
    update_duplicate_cache(this, context, kTxn);
    return 0;
  }

  /* if txn-tree is empty but btree is not: couple to btree */
  if (txns == UPS_KEY_NOT_FOUND && btrs != UPS_KEY_NOT_FOUND) {
    couple_to_btree();
    update_duplicate_cache(this, context, kBtree);
    return 0;
  }

  /* if both trees are not empty then compare them and couple to the
   * smaller one */
  assert(btrs == 0 && (txns == 0
      || txns == UPS_KEY_ERASED_IN_TXN
      || txns == UPS_TXN_CONFLICT));
  compare(this, context);

  /* both keys are equal - couple to txn; it's chronologically
   * newer */
  if (m_last_cmp == 0) {
    if (txns && txns != UPS_KEY_ERASED_IN_TXN)
      return txns;
    couple_to_txnop();
    update_duplicate_cache(this, context, kBtree | kTxn);
  }
  /* couple to txn */
  else if (m_last_cmp > 0) {
    if (txns && txns != UPS_KEY_ERASED_IN_TXN)
      return txns;
    couple_to_txnop();
    update_duplicate_cache(this, context, kTxn);
  }
  /* couple to btree */
  else {
    couple_to_btree();
    update_duplicate_cache(this, context, kBtree);
  }

  return 0;
}

ups_status_t
LocalCursor::move_first_key(Context *context, uint32_t flags)
{
  ups_status_t st = 0;

  /* move to the very very first key */
  st = move_first_key_singlestep(context);
  if (unlikely(st))
    return st;

  /* check for duplicates. the duplicate cache was already updated in
   * move_first_key_singlestep() */
  if (isset(db->flags(), UPS_ENABLE_DUPLICATE_KEYS)) {
    /* are there any duplicates? if not then they were all erased and we
     * move to the previous key */
    if (duplicate_cache.empty())
      return move_next_key(context, flags);

    /* otherwise move to the first duplicate */
    return move_first_duplicate(context);
  }

  /* no duplicates - make sure that we've not coupled to an erased
   * item */
  if (is_coupled_to_txnop()) {
    if (__txn_cursor_is_erase(&txn_cursor))
      return move_next_key(context, flags);
    return 0;
  }

  if (is_coupled_to_btree()) {
    st = check_if_btree_key_is_erased_or_overwritten(this, context);
    if (st == UPS_KEY_ERASED_IN_TXN)
      return move_next_key(context, flags);
    if (st == 0) {
      couple_to_txnop();
      return 0;
    }
    if (st == UPS_KEY_NOT_FOUND)
      return 0;
    return st;
  }

  return UPS_KEY_NOT_FOUND;
}

ups_status_t
LocalCursor::move_last_key_singlestep(Context *context)
{
  ups_status_t btrs, txns;

  /* fetch the largest key from the transaction tree. */
  txns = txn_cursor.move(UPS_CURSOR_LAST);
  /* fetch the largest key from the btree tree. */
  btrs = btree_cursor.move(context, 0, 0, 0, 0,
                  UPS_CURSOR_LAST | UPS_SKIP_DUPLICATES);
  /* now consolidate - if both trees are empty then return */
  if (unlikely(btrs == UPS_KEY_NOT_FOUND && txns == UPS_KEY_NOT_FOUND))
    return UPS_KEY_NOT_FOUND;

  /* if btree is empty but txn-tree is not: couple to txn */
  if (btrs == UPS_KEY_NOT_FOUND && txns != UPS_KEY_NOT_FOUND) {
    if (txns == UPS_TXN_CONFLICT)
      return txns;
    couple_to_txnop();
    update_duplicate_cache(this, context, kTxn);
    return 0;
  }

  /* if txn-tree is empty but btree is not: couple to btree */
  if (txns == UPS_KEY_NOT_FOUND && btrs != UPS_KEY_NOT_FOUND) {
    couple_to_btree();
    update_duplicate_cache(this, context, kBtree);
    return 0;
  }

  /* if both trees are not empty then compare them and couple to the
   * greater one */
  assert(btrs == 0 && (txns == 0
      || txns == UPS_KEY_ERASED_IN_TXN
      || txns == UPS_TXN_CONFLICT));
  compare(this, context);

  /* both keys are equal - couple to txn; it's chronologically
   * newer */
  if (m_last_cmp == 0) {
    if (txns && txns != UPS_KEY_ERASED_IN_TXN)
      return txns;
    couple_to_txnop();
    update_duplicate_cache(this, context, kBtree | kTxn);
  }
  /* couple to txn */
  else if (m_last_cmp < 1) {
    if (txns && txns != UPS_KEY_ERASED_IN_TXN)
      return txns;
    couple_to_txnop();
    update_duplicate_cache(this, context, kTxn);
  }
  /* couple to btree */
  else {
    couple_to_btree();
    update_duplicate_cache(this, context, kBtree);
  }
  return 0;
}

ups_status_t
LocalCursor::move_last_key(Context *context, uint32_t flags)
{
  ups_status_t st = 0;

  /* move to the very very last key */
  st = move_last_key_singlestep(context);
  if (unlikely(st))
    return st;

  /* check for duplicates. the duplicate cache was already updated in
   * move_last_key_singlestep() */
  if (isset(db->flags(), UPS_ENABLE_DUPLICATE_KEYS)) {
    /* are there any duplicates? if not then they were all erased and we
     * move to the previous key */
    if (duplicate_cache.empty())
      return move_previous_key(context, flags);

    /* otherwise move to the last duplicate */
    return move_last_duplicate(context);
  }

  /* no duplicates - make sure that we've not coupled to an erased
   * item */
  if (is_coupled_to_txnop()) {
    if (__txn_cursor_is_erase(&txn_cursor))
      return move_previous_key(context, flags);
    return 0;
  }

  if (is_coupled_to_btree()) {
    st = check_if_btree_key_is_erased_or_overwritten(this, context);
    if (st == UPS_KEY_ERASED_IN_TXN)
      return move_previous_key(context, flags);
    if (st == 0) {
      couple_to_txnop();
      return 0;
    }
    if (st == UPS_KEY_NOT_FOUND)
      return 0;
    return st;
  }

  return UPS_KEY_NOT_FOUND;
}

ups_status_t
LocalCursor::move(Context *context, ups_key_t *key, ups_record_t *record,
                uint32_t flags)
{
  ups_status_t st = 0;
  bool changed_dir = false;

  /* in non-transactional mode - just call the btree function and return */
  if (notset(ldb(this)->flags(), UPS_ENABLE_TRANSACTIONS))
    return btree_cursor.move(context,
                           key, &ldb(this)->key_arena(context->txn),
                           record, &ldb(this)->record_arena(context->txn),
                           flags);

  /* no movement requested? directly retrieve key/record */
  if (!flags)
    goto retrieve_key_and_record;

  /* synchronize the btree and transaction cursor if the last operation was
   * not a move next/previous OR if the direction changed */
  if (m_last_operation == UPS_CURSOR_PREVIOUS
        && isset(flags, UPS_CURSOR_NEXT))
    changed_dir = true;
  else if (m_last_operation == UPS_CURSOR_NEXT
        && isset(flags, UPS_CURSOR_PREVIOUS))
    changed_dir = true;

  if (issetany(flags, UPS_CURSOR_NEXT | UPS_CURSOR_PREVIOUS)
        && (m_last_operation == LocalCursor::kLookupOrInsert || changed_dir)) {
    if (is_coupled_to_txnop())
      set_to_nil(kBtree);
    else
      set_to_nil(kTxn);

    (void)synchronize(context, flags, 0);

    if (!txn_cursor.is_nil() && !is_nil(kBtree))
      compare(this, context);
  }

  /* we have either skipped duplicates or reached the end of the duplicate
   * list. btree cursor and txn cursor are synced and as close to
   * each other as possible. Move the cursor in the requested direction. */
  if (isset(flags, UPS_CURSOR_NEXT))
    st = move_next_key(context, flags);
  else if (isset(flags, UPS_CURSOR_PREVIOUS))
    st = move_previous_key(context, flags);
  else if (isset(flags, UPS_CURSOR_FIRST)) {
    clear_duplicate_cache(this);
    st = move_first_key(context, flags);
  }
  else {
    assert(isset(flags, UPS_CURSOR_LAST));
    clear_duplicate_cache(this);
    st = move_last_key(context, flags);
  }

  if (unlikely(st))
    return st;

retrieve_key_and_record:
  /* retrieve key/record, if requested */
  if (is_coupled_to_txnop()) {
#ifdef UPS_DEBUG
    TxnOperation *op = txn_cursor.get_coupled_op();
    if (op)
      assert(notset(op->flags, TxnOperation::kErase));
#endif
    try {
      if (key)
        txn_cursor.copy_coupled_key(key);
      if (record)
        txn_cursor.copy_coupled_record(record);
    }
    catch (Exception &ex) {
      return ex.code;
    }
    return 0;
  }

  return btree_cursor.move(context, key, &db->key_arena(txn),
                  record, &db->record_arena(txn), 0);
}

ups_status_t
LocalCursor::overwrite(ups_record_t *record, uint32_t flags)
{
  Context context(lenv(this), (LocalTxn *)txn, ldb(this));
  ups_status_t st = 0;

  if (isset(ldb(this)->flags(), UPS_ENABLE_TRANSACTIONS)) {
    if (txn_cursor.is_nil() && !(is_nil(0))) {
      btree_cursor.uncouple_from_page(&context);
      st = ldb(this)->insert(this, txn, btree_cursor.uncoupled_key(),
                      record, flags | UPS_OVERWRITE);
    }
    else {
      if (txn_cursor.is_nil())
        st = UPS_CURSOR_IS_NIL;
      else
        st = ldb(this)->insert(this, txn, txn_cursor.coupled_key(), record,
                        flags | UPS_OVERWRITE);
    }

    if (st == 0)
      couple_to_txnop();
  }
  else {
    btree_cursor.overwrite(&context, record, flags);
    couple_to_btree();
  }

  return st;
}

bool
LocalCursor::is_nil(int what)
{
  switch (what) {
    case kBtree:
      return btree_cursor.is_nil();
    case kTxn:
      return txn_cursor.is_nil();
    default:
      assert(what == 0);
      return btree_cursor.is_nil() && txn_cursor.is_nil();
  }
}

void
LocalCursor::set_to_nil(int what)
{
  switch (what) {
    case kBtree:
      btree_cursor.set_to_nil();
      break;
    case kTxn:
      txn_cursor.set_to_nil();
      couple_to_btree(); /* reset flag */
      break;
    default:
      assert(what == 0);
      btree_cursor.set_to_nil();
      txn_cursor.set_to_nil();
      couple_to_btree(); /* reset flag */
      m_is_first_use = true;
      clear_duplicate_cache(this);
      break;
  }
}

void
LocalCursor::close()
{
  btree_cursor.close();
  duplicate_cache.clear();
}

uint32_t
LocalCursor::get_duplicate_position()
{
  if (is_nil())
    throw Exception(UPS_CURSOR_IS_NIL);

  // use btree cursor? otherwise return the index in the duplicate cache
  if (txn_cursor.is_nil())
    return btree_cursor.duplicate_index();
  return duplicate_cache_index - 1;
}

uint32_t
LocalCursor::get_duplicate_count(uint32_t flags)
{
  Context context(lenv(this), (LocalTxn *)txn, ldb(this));

  if (unlikely(is_nil()))
    throw Exception(UPS_CURSOR_IS_NIL);

  if (txn || is_coupled_to_txnop()) {
    if (isset(db->flags(), UPS_ENABLE_DUPLICATE_KEYS)) {
      synchronize(&context, 0, 0);
      update_duplicate_cache(this, &context, kTxn | kBtree);
      return duplicate_cache.size();
    }

    /* obviously the key exists, since the cursor is coupled */
    return 1;
  }

  return btree_cursor.record_count(&context, 0);
}

uint32_t
LocalCursor::get_record_size()
{
  Context context(lenv(this), (LocalTxn *)txn, ldb(this));

  if (unlikely(is_nil()))
    throw Exception(UPS_CURSOR_IS_NIL);

  if (is_coupled_to_txnop())
    return txn_cursor.record_size();
  return btree_cursor.record_size(&context);
}

