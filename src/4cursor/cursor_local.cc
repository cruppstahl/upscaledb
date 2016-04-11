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

LocalCursor::LocalCursor(LocalDb *db, Txn *txn)
  : Cursor(db, txn), m_txn_cursor(this), m_btree_cursor(this),
    m_duplicate_cache_index(0), m_last_operation(0), m_flags(0), m_last_cmp(0),
    m_is_first_use(true)
{
}

LocalCursor::LocalCursor(LocalCursor &other)
  : Cursor(other), m_txn_cursor(this), m_btree_cursor(this)
{
  txn = other.txn;
  next = other.next;
  previous = other.previous;
  m_duplicate_cache_index = other.m_duplicate_cache_index;
  m_last_operation = other.m_last_operation;
  m_last_cmp = other.m_last_cmp;
  m_flags = other.m_flags;
  m_is_first_use = other.m_is_first_use;

  m_btree_cursor.clone(&other.m_btree_cursor);
  m_txn_cursor.clone(&other.m_txn_cursor);

  if (isset(db->flags(), UPS_ENABLE_DUPLICATE_KEYS))
    other.m_duplicate_cache = m_duplicate_cache;
}

void
LocalCursor::append_btree_duplicates(Context *context)
{
  uint32_t count = m_btree_cursor.record_count(context, 0);
  for (uint32_t i = 0; i < count; i++)
    m_duplicate_cache.push_back(DuplicateCacheLine(true, i));
}

void
LocalCursor::update_duplicate_cache(Context *context, uint32_t what)
{
  if (notset(db->flags(), UPS_ENABLE_DUPLICATE_KEYS))
    return;

  /* if the cache already exists: no need to continue, it should be
   * up to date */
  if (!m_duplicate_cache.empty())
    return;

  if ((what & kBtree) && (what & kTxn)) {
    if (is_nil(kBtree) && !is_nil(kTxn)) {
      bool equal_keys;
      synchronize(context, 0, &equal_keys);
      if (!equal_keys)
        set_to_nil(kBtree);
    }
  }

  /* first collect all duplicates from the btree. They're already sorted,
   * therefore we can just append them to our duplicate-cache. */
  if ((what & kBtree) && !is_nil(kBtree))
    append_btree_duplicates(context);

  /* read duplicates from the txn-cursor? */
  if ((what & kTxn) && !is_nil(kTxn)) {
    TxnOperation *op = m_txn_cursor.get_coupled_op();
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
        m_duplicate_cache.clear();
        m_duplicate_cache.push_back(DuplicateCacheLine(false, op));
        continue;
      }

      if (isset(op->flags, TxnOperation::kInsertOverwrite)) {
        uint32_t ref = op->referenced_duplicate;
        if (ref) {
          assert(ref <= m_duplicate_cache.size());
          DuplicateCacheLine *e = &m_duplicate_cache[0];
          (&e[ref - 1])->set_txn_op(op);
        }
        else {
          /* all existing duplicates are overwritten */
          m_duplicate_cache.clear();
          m_duplicate_cache.push_back(DuplicateCacheLine(false, op));
        }
        continue;
      }

      /* insert a duplicate key */
      if (isset(op->flags, TxnOperation::kInsertDuplicate)) {
        uint32_t of = op->original_flags;
        uint32_t ref = op->referenced_duplicate - 1;
        DuplicateCacheLine dcl(false, op);
        if (isset(of, UPS_DUPLICATE_INSERT_FIRST))
          m_duplicate_cache.insert(m_duplicate_cache.begin(), dcl);
        else if (isset(of, UPS_DUPLICATE_INSERT_BEFORE)) {
          m_duplicate_cache.insert(m_duplicate_cache.begin() + ref, dcl);
        }
        else if (isset(of, UPS_DUPLICATE_INSERT_AFTER)) {
          if (ref + 1 >= m_duplicate_cache.size())
            m_duplicate_cache.push_back(dcl);
          else
          m_duplicate_cache.insert(m_duplicate_cache.begin() + ref + 1, dcl);
        }
        else /* default is UPS_DUPLICATE_INSERT_LAST */
          m_duplicate_cache.push_back(dcl);
        continue;
      }

      /* a normal erase will erase ALL duplicate keys */
      if (isset(op->flags, TxnOperation::kErase)) {
        uint32_t ref = op->referenced_duplicate;
        if (ref) {
          assert(ref <= m_duplicate_cache.size());
          m_duplicate_cache.erase(m_duplicate_cache.begin() + (ref - 1));
        }
        else {
          /* all existing duplicates are erased */
          m_duplicate_cache.clear();
        }
        continue;
      }

      /* everything else is a bug! */
      assert(op->flags == TxnOperation::kNop);
    }
  }
}

void
LocalCursor::couple_to_duplicate(uint32_t duplicate_index)
{
  assert(m_duplicate_cache.size() >= duplicate_index);
  assert(duplicate_index >= 1);

  /* duplicate_index is a 1-based index! */
  DuplicateCacheLine &e = m_duplicate_cache[duplicate_index - 1];
  if (e.use_btree()) {
    couple_to_btree();
    m_btree_cursor.set_duplicate_index(e.btree_duplicate_index());
  }
  else {
    assert(e.txn_op() != 0);
    m_txn_cursor.couple_to(e.txn_op());
    couple_to_txnop();
  }
  set_duplicate_cache_index(duplicate_index);
}

ups_status_t
LocalCursor::check_if_btree_key_is_erased_or_overwritten(Context *context)
{
  ups_key_t key = {0};
  TxnOperation *op;
  // TODO will leak if an exception is thrown
  LocalCursor *clone = new LocalCursor(*this);

  ups_status_t st = m_btree_cursor.move(context, &key,
                  &db->key_arena(txn), 0, 0, 0);
  if (st) {
    clone->close();
    delete clone;
    return st;
  }

  st = clone->m_txn_cursor.find(&key, 0);
  if (st) {
    clone->close();
    delete clone;
    return st;
  }

  op = clone->m_txn_cursor.get_coupled_op();
  if (isset(op->flags, TxnOperation::kInsertDuplicate))
    st = UPS_KEY_NOT_FOUND;
  clone->close();
  delete clone;
  return st;
}

void
LocalCursor::synchronize(Context *context, uint32_t flags, bool *equal_keys)
{
  if (equal_keys)
    *equal_keys = false;

  if (is_nil(kBtree)) {
    if (!m_txn_cursor.get_coupled_op())
      return;
    ups_key_t *key = m_txn_cursor.get_coupled_op()->node->key();

    if (notset(flags, kSyncOnlyEqualKeys))
      flags = flags | (isset(flags, UPS_CURSOR_NEXT)
                            ? UPS_FIND_GEQ_MATCH
                            : UPS_FIND_LEQ_MATCH);
    /* the flag |kSyncDontLoadKey| does not load the key if there's an
     * approx match - it only positions the cursor */
    ups_status_t st = m_btree_cursor.find(context, key, 0, 0, 0,
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
    clone->m_btree_cursor.uncouple_from_page(context);
    ups_key_t *key = clone->m_btree_cursor.uncoupled_key();
    if (notset(flags, kSyncOnlyEqualKeys))
      flags = flags | (isset(flags, UPS_CURSOR_NEXT)
                            ? UPS_FIND_GEQ_MATCH
                            : UPS_FIND_LEQ_MATCH);

    ups_status_t st = m_txn_cursor.find(key, kSyncDontLoadKey | flags);
    /* if we had a direct hit instead of an approx. match then
    * set |equal_keys| to false; otherwise Cursor::move()
    * will move the btree cursor again */
    if (st == 0 && equal_keys && !ups_key_get_approximate_match_type(key))
      *equal_keys = true;
    clone->close();
    delete clone;
  }
}

ups_status_t
LocalCursor::move_next_duplicate(Context *context)
{
  if (duplicate_cache_index()
          && duplicate_cache_index() < m_duplicate_cache.size()) {
    set_duplicate_cache_index(duplicate_cache_index() + 1);
    couple_to_duplicate(duplicate_cache_index());
    return 0;
  }
  return UPS_LIMITS_REACHED;
}

ups_status_t
LocalCursor::move_previous_duplicate(Context *context)
{
  if (duplicate_cache_index() > 1) {
    set_duplicate_cache_index(duplicate_cache_index() - 1);
    couple_to_duplicate(duplicate_cache_index());
    return 0;
  }
  return UPS_LIMITS_REACHED;
}

ups_status_t
LocalCursor::move_first_duplicate(Context *context)
{
  if (m_duplicate_cache.size()) {
    set_duplicate_cache_index(1);
    couple_to_duplicate(duplicate_cache_index());
    return 0;
  }
  return UPS_LIMITS_REACHED;
}

ups_status_t
LocalCursor::move_last_duplicate(Context *context)
{
  if (m_duplicate_cache.size()) {
    set_duplicate_cache_index(m_duplicate_cache.size());
    couple_to_duplicate(duplicate_cache_index());
    return 0;
  }
  return UPS_LIMITS_REACHED;
}

static bool
__txn_cursor_is_erase(TxnCursor *txnc)
{
  TxnOperation *op = txnc->get_coupled_op();
  return op ? isset(op->flags, TxnOperation::kErase) : false;
}

int
LocalCursor::compare(Context *context)
{
  TxnNode *node = m_txn_cursor.get_coupled_op()->node;

  assert(!is_nil(0));
  assert(!m_txn_cursor.is_nil());

  // call m_btree_cursor.compare() and let the btree cursor deal
  // with its state (coupled vs uncoupled)
  m_last_cmp = m_btree_cursor.compare(context, node->key());
  return m_last_cmp;
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
      st = m_btree_cursor.move(context, 0, 0, 0, 0,
                    UPS_CURSOR_NEXT | UPS_SKIP_DUPLICATES);
      if (st == UPS_KEY_NOT_FOUND || st == UPS_CURSOR_IS_NIL) {
        set_to_nil(kBtree); // TODO muss raus
        if (unlikely(m_txn_cursor.is_nil()))
          return UPS_KEY_NOT_FOUND;
        else {
          couple_to_txnop();
          m_last_cmp = 1;
        }
      }
    }

    if (!m_txn_cursor.is_nil()) {
      st = m_txn_cursor.move(UPS_CURSOR_NEXT);
      if (st == UPS_KEY_NOT_FOUND || st == UPS_CURSOR_IS_NIL) {
        set_to_nil(kTxn); // TODO muss raus
        if (unlikely(is_nil(kBtree)))
          return UPS_KEY_NOT_FOUND;
        else {
          couple_to_btree();
          m_last_cmp = -1;

          ups_status_t st2 = check_if_btree_key_is_erased_or_overwritten(context);
          if (st2 == UPS_TXN_CONFLICT)
            st = st2;
        }
      }
    }
  }
  /* if the btree-key is smaller: move it next */
  else if (m_last_cmp < 0) {
    st = m_btree_cursor.move(context, 0, 0, 0, 0,
                    UPS_CURSOR_NEXT | UPS_SKIP_DUPLICATES);
    if (st == UPS_KEY_NOT_FOUND) {
      set_to_nil(kBtree); // TODO Das muss raus!
      if (m_txn_cursor.is_nil())
        return st;
      couple_to_txnop();
      m_last_cmp = +1;
    }
    else {
      ups_status_t st2 = check_if_btree_key_is_erased_or_overwritten(context);
      if (st2 == UPS_TXN_CONFLICT)
        st = st2;
    }
    if (m_txn_cursor.is_nil())
      m_last_cmp = -1;
  }
  /* if the txn-key is smaller OR if both keys are equal: move next
   * with the txn-key (which is chronologically newer) */
  else {
    st = m_txn_cursor.move(UPS_CURSOR_NEXT);
    if (st == UPS_KEY_NOT_FOUND) {
      set_to_nil(kTxn); // TODO Das muss raus!
      if (is_nil(kBtree))
        return st;
      couple_to_btree();
      m_last_cmp = -1;
    }
    if (is_nil(kBtree))
      m_last_cmp = 1;
  }

  /* compare keys again */
  if (!is_nil(kBtree) && !m_txn_cursor.is_nil())
    compare(context);

  /* if there's a txn conflict: move next */
  if (st == UPS_TXN_CONFLICT)
    return move_next_key_singlestep(context);

  /* btree-key is smaller */
  if (m_last_cmp < 0 || m_txn_cursor.is_nil()) {
    couple_to_btree();
    update_duplicate_cache(context, kBtree);
    return 0;
  }

  /* txn-key is smaller */
  if (m_last_cmp > 0 || m_btree_cursor.is_nil()) {
    couple_to_txnop();
    update_duplicate_cache(context, kTxn);
    return 0;
  }

  /* both keys are equal */
  couple_to_txnop();
  update_duplicate_cache(context, kTxn | kBtree);
  return 0;
}

ups_status_t
LocalCursor::move_next_key(Context *context, uint32_t flags)
{
  ups_status_t st;

  /* are we in the middle of a duplicate list? if yes then move to the
   * next duplicate */
  if (duplicate_cache_index() > 0 && notset(flags, UPS_SKIP_DUPLICATES)) {
    st = move_next_duplicate(context);
    if (st != UPS_LIMITS_REACHED)
      return st;
    if (st == UPS_LIMITS_REACHED && isset(flags, UPS_ONLY_DUPLICATES))
      return UPS_KEY_NOT_FOUND;
  }

  clear_duplicate_cache();

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
      if (unlikely(!has_duplicates()))
        continue;

      /* otherwise move to the first duplicate */
      return move_first_duplicate(context);
    }

    /* no duplicates - make sure that we've not coupled to an erased item */
    if (is_coupled_to_txnop()) {
      if (__txn_cursor_is_erase(&m_txn_cursor))
        continue;
      return 0;
    }

    if (is_coupled_to_btree()) {
      st = check_if_btree_key_is_erased_or_overwritten(context);
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
      st = m_btree_cursor.move(context, 0, 0, 0, 0,
                    UPS_CURSOR_PREVIOUS | UPS_SKIP_DUPLICATES);
      if (st == UPS_KEY_NOT_FOUND || st == UPS_CURSOR_IS_NIL) {
        set_to_nil(kBtree); // TODO muss raus
        if (m_txn_cursor.is_nil())
          return UPS_KEY_NOT_FOUND;
        else {
          couple_to_txnop();
          m_last_cmp = -1;
        }
      }
    }

    if (!m_txn_cursor.is_nil()) {
      st = m_txn_cursor.move(UPS_CURSOR_PREVIOUS);
      if (st == UPS_KEY_NOT_FOUND || st == UPS_CURSOR_IS_NIL) {
        set_to_nil(kTxn); // TODO muss raus
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
    st = m_btree_cursor.move(context, 0, 0, 0, 0,
                    UPS_CURSOR_PREVIOUS | UPS_SKIP_DUPLICATES);
    if (st == UPS_KEY_NOT_FOUND) {
      set_to_nil(kBtree); // TODO Das muss raus!
      if (m_txn_cursor.is_nil())
        return st;
      couple_to_txnop();
      m_last_cmp = -1;
    }
    else {
      ups_status_t st2 = check_if_btree_key_is_erased_or_overwritten(context);
      if (st2 == UPS_TXN_CONFLICT)
        st = st2;
    }
    if (m_txn_cursor.is_nil())
      m_last_cmp = 1;
  }
  /* if the txn-key is greater OR if both keys are equal: move previous
   * with the txn-key (which is chronologically newer) */
  else {
    st = m_txn_cursor.move(UPS_CURSOR_PREVIOUS);
    if (st == UPS_KEY_NOT_FOUND) {
      set_to_nil(kTxn); // TODO Das muss raus!
      if (is_nil(kBtree))
        return st;
      couple_to_btree();
      m_last_cmp = 1;

      ups_status_t st2 = check_if_btree_key_is_erased_or_overwritten(context);
      if (st2 == UPS_TXN_CONFLICT)
        st = st2;
    }
    if (is_nil(kBtree))
      m_last_cmp = -1;
  }

  /* compare keys again */
  if (!is_nil(kBtree) && !m_txn_cursor.is_nil())
    compare(context);

  /* if there's a txn conflict: move previous */
  if (st == UPS_TXN_CONFLICT)
    return move_previous_key_singlestep(context);

  /* btree-key is greater */
  if (m_last_cmp > 0 || m_txn_cursor.is_nil()) {
    couple_to_btree();
    update_duplicate_cache(context, kBtree);
    return 0;
  }

  /* txn-key is greater */
  if (m_last_cmp < 0 || m_btree_cursor.is_nil()) {
    couple_to_txnop();
    update_duplicate_cache(context, kTxn);
    return 0;
  }

  /* both keys are equal */
  couple_to_txnop();
  update_duplicate_cache(context, kTxn | kBtree);
  return 0;
}

ups_status_t
LocalCursor::move_previous_key(Context *context, uint32_t flags)
{
  ups_status_t st;

  /* are we in the middle of a duplicate list? if yes then move to the
   * previous duplicate */
  if (duplicate_cache_index() > 0 && notset(flags, UPS_SKIP_DUPLICATES)) {
    st = move_previous_duplicate(context);
    if (st != UPS_LIMITS_REACHED)
      return st;
    if (st == UPS_LIMITS_REACHED && isset(flags, UPS_ONLY_DUPLICATES))
      return UPS_KEY_NOT_FOUND;
  }

  clear_duplicate_cache();

  /* either there were no duplicates or we've reached the end of the
   * duplicate list. move previous till we found a new candidate */
  while (!is_nil(kBtree) || !m_txn_cursor.is_nil()) {
    st = move_previous_key_singlestep(context);
    if (unlikely(st))
      return st;

    /* check for duplicates. the duplicate cache was already updated in
     * move_previous_key_singlestep() */
    if (isset(db->flags(), UPS_ENABLE_DUPLICATE_KEYS)) {
      /* are there any duplicates? if not then they were all erased and
       * we move to the previous key */
      if (!has_duplicates())
        continue;

      /* otherwise move to the last duplicate */
      return move_last_duplicate(context);
    }

    /* no duplicates - make sure that we've not coupled to an erased
     * item */
    if (is_coupled_to_txnop()) {
      if (__txn_cursor_is_erase(&m_txn_cursor))
        continue;
      return 0;
    }

    if (is_coupled_to_btree()) {
      st = check_if_btree_key_is_erased_or_overwritten(context);
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
  txns = m_txn_cursor.move(UPS_CURSOR_FIRST);
  /* fetch the smallest key from the btree tree. */
  btrs = m_btree_cursor.move(context, 0, 0, 0, 0,
                UPS_CURSOR_FIRST | UPS_SKIP_DUPLICATES);
  /* now consolidate - if both trees are empty then return */
  if (btrs == UPS_KEY_NOT_FOUND && txns == UPS_KEY_NOT_FOUND)
    return UPS_KEY_NOT_FOUND;

  /* if btree is empty but txn-tree is not: couple to txn */
  if (btrs == UPS_KEY_NOT_FOUND && txns != UPS_KEY_NOT_FOUND) {
    if (txns == UPS_TXN_CONFLICT)
      return txns;
    couple_to_txnop();
    update_duplicate_cache(context, kTxn);
    return 0;
  }

  /* if txn-tree is empty but btree is not: couple to btree */
  if (txns == UPS_KEY_NOT_FOUND && btrs != UPS_KEY_NOT_FOUND) {
    couple_to_btree();
    update_duplicate_cache(context, kBtree);
    return 0;
  }

  /* if both trees are not empty then compare them and couple to the
   * smaller one */
  assert(btrs == 0 && (txns == 0
      || txns == UPS_KEY_ERASED_IN_TXN
      || txns == UPS_TXN_CONFLICT));
  compare(context);

  /* both keys are equal - couple to txn; it's chronologically
   * newer */
  if (m_last_cmp == 0) {
    if (txns && txns != UPS_KEY_ERASED_IN_TXN)
      return txns;
    couple_to_txnop();
    update_duplicate_cache(context, kBtree | kTxn);
  }
  /* couple to txn */
  else if (m_last_cmp > 0) {
    if (txns && txns != UPS_KEY_ERASED_IN_TXN)
      return txns;
    couple_to_txnop();
    update_duplicate_cache(context, kTxn);
  }
  /* couple to btree */
  else {
    couple_to_btree();
    update_duplicate_cache(context, kBtree);
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
    if (!has_duplicates())
      return move_next_key(context, flags);

    /* otherwise move to the first duplicate */
    return move_first_duplicate(context);
  }

  /* no duplicates - make sure that we've not coupled to an erased
   * item */
  if (is_coupled_to_txnop()) {
    if (__txn_cursor_is_erase(&m_txn_cursor))
      return move_next_key(context, flags);
    return 0;
  }

  if (is_coupled_to_btree()) {
    st = check_if_btree_key_is_erased_or_overwritten(context);
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
  txns = m_txn_cursor.move(UPS_CURSOR_LAST);
  /* fetch the largest key from the btree tree. */
  btrs = m_btree_cursor.move(context, 0, 0, 0, 0,
                  UPS_CURSOR_LAST | UPS_SKIP_DUPLICATES);
  /* now consolidate - if both trees are empty then return */
  if (unlikely(btrs == UPS_KEY_NOT_FOUND && txns == UPS_KEY_NOT_FOUND))
    return UPS_KEY_NOT_FOUND;

  /* if btree is empty but txn-tree is not: couple to txn */
  if (btrs == UPS_KEY_NOT_FOUND && txns != UPS_KEY_NOT_FOUND) {
    if (txns == UPS_TXN_CONFLICT)
      return txns;
    couple_to_txnop();
    update_duplicate_cache(context, kTxn);
    return 0;
  }

  /* if txn-tree is empty but btree is not: couple to btree */
  if (txns == UPS_KEY_NOT_FOUND && btrs != UPS_KEY_NOT_FOUND) {
    couple_to_btree();
    update_duplicate_cache(context, kBtree);
    return 0;
  }

  /* if both trees are not empty then compare them and couple to the
   * greater one */
  assert(btrs == 0 && (txns == 0
      || txns == UPS_KEY_ERASED_IN_TXN
      || txns == UPS_TXN_CONFLICT));
  compare(context);

  /* both keys are equal - couple to txn; it's chronologically
   * newer */
  if (m_last_cmp == 0) {
    if (txns && txns != UPS_KEY_ERASED_IN_TXN)
      return txns;
    couple_to_txnop();
    update_duplicate_cache(context, kBtree | kTxn);
  }
  /* couple to txn */
  else if (m_last_cmp < 1) {
    if (txns && txns != UPS_KEY_ERASED_IN_TXN)
      return txns;
    couple_to_txnop();
    update_duplicate_cache(context, kTxn);
  }
  /* couple to btree */
  else {
    couple_to_btree();
    update_duplicate_cache(context, kBtree);
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
    if (!has_duplicates())
      return move_previous_key(context, flags);

    /* otherwise move to the last duplicate */
    return move_last_duplicate(context);
  }

  /* no duplicates - make sure that we've not coupled to an erased
   * item */
  if (is_coupled_to_txnop()) {
    if (__txn_cursor_is_erase(&m_txn_cursor))
      return move_previous_key(context, flags);
    return 0;
  }

  if (is_coupled_to_btree()) {
    st = check_if_btree_key_is_erased_or_overwritten(context);
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
  if (notset(ldb()->flags(), UPS_ENABLE_TRANSACTIONS))
    return m_btree_cursor.move(context,
                           key, &ldb()->key_arena(context->txn),
                           record, &ldb()->record_arena(context->txn), flags);

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

    if (!m_txn_cursor.is_nil() && !is_nil(kBtree))
      compare(context);
  }

  /* we have either skipped duplicates or reached the end of the duplicate
   * list. btree cursor and txn cursor are synced and as close to
   * each other as possible. Move the cursor in the requested direction. */
  if (isset(flags, UPS_CURSOR_NEXT))
    st = move_next_key(context, flags);
  else if (isset(flags, UPS_CURSOR_PREVIOUS))
    st = move_previous_key(context, flags);
  else if (isset(flags, UPS_CURSOR_FIRST)) {
    clear_duplicate_cache();
    st = move_first_key(context, flags);
  }
  else {
    assert(isset(flags, UPS_CURSOR_LAST));
    clear_duplicate_cache();
    st = move_last_key(context, flags);
  }

  if (unlikely(st))
    return st;

retrieve_key_and_record:
  /* retrieve key/record, if requested */
  if (is_coupled_to_txnop()) {
#ifdef UPS_DEBUG
    TxnOperation *op = m_txn_cursor.get_coupled_op();
    if (op)
      assert(notset(op->flags, TxnOperation::kErase));
#endif
    try {
      if (key)
        m_txn_cursor.copy_coupled_key(key);
      if (record)
        m_txn_cursor.copy_coupled_record(record);
    }
    catch (Exception &ex) {
      return ex.code;
    }
    return 0;
  }

  return m_btree_cursor.move(context, key, &db->key_arena(txn),
                  record, &db->record_arena(txn), 0);
}

ups_status_t
LocalCursor::overwrite(ups_record_t *record, uint32_t flags)
{
  Context context(lenv(), (LocalTxn *)txn, ldb());
  ups_status_t st = 0;

  if (isset(ldb()->flags(), UPS_ENABLE_TRANSACTIONS)) {
    if (m_txn_cursor.is_nil() && !(is_nil(0))) {
      m_btree_cursor.uncouple_from_page(&context);
      st = ldb()->insert(this, txn, m_btree_cursor.uncoupled_key(),
                      record, flags | UPS_OVERWRITE);
    }
    else {
      if (m_txn_cursor.is_nil())
        st = UPS_CURSOR_IS_NIL;
      else
        st = ldb()->insert(this, txn, m_txn_cursor.coupled_key(), record,
                        flags | UPS_OVERWRITE);
    }

    if (st == 0)
      couple_to_txnop();
  }
  else {
    m_btree_cursor.overwrite(&context, record, flags);
    couple_to_btree();
  }

  return st;
}

bool
LocalCursor::is_nil(int what)
{
  switch (what) {
    case kBtree:
      return m_btree_cursor.is_nil();
    case kTxn:
      return m_txn_cursor.is_nil();
    default:
      assert(what == 0);
      return m_btree_cursor.is_nil() && m_txn_cursor.is_nil();
  }
}

void
LocalCursor::set_to_nil(int what)
{
  switch (what) {
    case kBtree:
      m_btree_cursor.set_to_nil();
      break;
    case kTxn:
      m_txn_cursor.set_to_nil();
      couple_to_btree(); /* reset flag */
      break;
    default:
      assert(what == 0);
      m_btree_cursor.set_to_nil();
      m_txn_cursor.set_to_nil();
      couple_to_btree(); /* reset flag */
      m_is_first_use = true;
      clear_duplicate_cache();
      break;
  }
}

void
LocalCursor::close()
{
  m_btree_cursor.close();
  m_duplicate_cache.clear();
}

uint32_t
LocalCursor::get_duplicate_position()
{
  if (is_nil())
    throw Exception(UPS_CURSOR_IS_NIL);

  // use btree cursor? otherwise return the index in the duplicate cache
  if (m_txn_cursor.is_nil())
    return m_btree_cursor.duplicate_index();
  return duplicate_cache_index() - 1;
}

uint32_t
LocalCursor::get_duplicate_count(Context *context)
{
  assert(!is_nil());

  if (txn || is_coupled_to_txnop()) {
    if (isset(db->flags(), UPS_ENABLE_DUPLICATE_KEYS)) {
      synchronize(context, 0, 0);
      update_duplicate_cache(context, kTxn | kBtree);
      return m_duplicate_cache.size();
    }

    /* obviously the key exists, since the cursor is coupled */
    return 1;
  }

  return m_btree_cursor.record_count(context, 0);
}

uint32_t
LocalCursor::get_duplicate_count(uint32_t flags)
{
  Context context(lenv(), (LocalTxn *)txn, ldb());

  if (unlikely(is_nil()))
    throw Exception(UPS_CURSOR_IS_NIL);

  return get_duplicate_count(&context);
}

uint32_t
LocalCursor::get_record_size()
{
  Context context(lenv(), (LocalTxn *)txn, ldb());

  if (unlikely(is_nil()))
    throw Exception(UPS_CURSOR_IS_NIL);

  if (is_coupled_to_txnop())
    return m_txn_cursor.record_size();
  return m_btree_cursor.record_size(&context);
}

