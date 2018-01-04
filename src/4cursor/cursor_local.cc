/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

static inline void
backup_duplicate_cache(LocalCursor *cursor)
{
  cursor->old_duplicate_cache = cursor->duplicate_cache;
  cursor->old_duplicate_cache_index = cursor->duplicate_cache_index;
}

static inline void
restore_duplicate_cache(LocalCursor *cursor)
{
  cursor->duplicate_cache = cursor->old_duplicate_cache;
  cursor->duplicate_cache_index = cursor->old_duplicate_cache_index;
}

// Appends the duplicates of the BtreeCursor to the duplicate cache.
static inline void
append_btree_duplicates(LocalCursor *cursor, Context *context)
{
  uint32_t count = cursor->btree_cursor.record_count(context, 0);
  for (uint32_t i = 0; i < count; i++)
    cursor->duplicate_cache.push_back(DuplicateCacheLine(true, i));
}

// Appends the duplicates of the TxnCursor to the duplicate cache.
static inline void
append_txn_duplicates(LocalCursor *cursor, Context *context)
{
  TxnOperation *op = cursor->txn_cursor.get_coupled_op();
  TxnNode *node = op ? op->node : 0;
  if (!node)
    return;

  // now start integrating the items from the transactions
  for (op = node->oldest_op; op; op = op->next_in_node) {
    Txn *optxn = op->txn;
    // collect all ops that are valid (even those that are
    // from conflicting transactions)
    if (unlikely(optxn->is_aborted()))
      continue;

    // a normal (overwriting) insert will overwrite ALL duplicates,
    // but an overwrite of a duplicate will only overwrite
    // an entry in the DuplicateCache
    if (ISSET(op->flags, TxnOperation::kInsert)) {
      // all existing duplicates are overwritten
      cursor->duplicate_cache.clear();
      cursor->duplicate_cache.push_back(DuplicateCacheLine(false, op));
      continue;
    }

    if (ISSET(op->flags, TxnOperation::kInsertOverwrite)) {
      uint32_t ref = op->referenced_duplicate;
      if (ref) {
        assert(ref <= cursor->duplicate_cache.size());
        DuplicateCacheLine *e = &cursor->duplicate_cache[0];
        e[ref - 1].set_txn_op(op);
      }
      else {
        // all existing duplicates are overwritten
        cursor->duplicate_cache.clear();
        cursor->duplicate_cache.push_back(DuplicateCacheLine(false, op));
      }
      continue;
    }

    // insert a duplicate key
    if (ISSET(op->flags, TxnOperation::kInsertDuplicate)) {
      uint32_t of = op->original_flags;
      uint32_t ref = op->referenced_duplicate - 1;
      DuplicateCacheLine dcl(false, op);
      if (ISSET(of, UPS_DUPLICATE_INSERT_FIRST)) {
        cursor->duplicate_cache.insert(cursor->duplicate_cache.begin(), dcl);
      }
      else if (ISSET(of, UPS_DUPLICATE_INSERT_BEFORE)) {
        cursor->duplicate_cache.insert(cursor->duplicate_cache.begin()
                        + ref, dcl);
      }
      else if (ISSET(of, UPS_DUPLICATE_INSERT_AFTER)) {
        if (ref + 1 >= cursor->duplicate_cache.size())
          cursor->duplicate_cache.push_back(dcl);
        else
          cursor->duplicate_cache.insert(cursor->duplicate_cache.begin()
                          + ref + 1, dcl);
      }
      else { /* default is UPS_DUPLICATE_INSERT_LAST */
        cursor->duplicate_cache.push_back(dcl);
      }
      continue;
    }

    // a normal erase will erase ALL duplicate keys
    if (ISSET(op->flags, TxnOperation::kErase)) {
      uint32_t ref = op->referenced_duplicate;
      if (ref) {
        assert(ref <= cursor->duplicate_cache.size());
        cursor->duplicate_cache.erase(cursor->duplicate_cache.begin()
                        + (ref - 1));
      }
      else {
        // all existing duplicates are erased
        cursor->duplicate_cache.clear();
      }
      continue;
    }

    // everything else is a bug!
    assert(op->flags == TxnOperation::kNop);
  }
}

// Updates (or builds) the duplicate cache for a cursor
//
// The |what| parameter specifies if the dupecache is initialized from
// btree (kBtree), from txn (kTxn) or both.
static inline void
update_duplicate_cache(LocalCursor *cursor, Context *context, uint32_t what)
{
  if (NOTSET(cursor->db->flags(), UPS_ENABLE_DUPLICATE_KEYS))
    return;

  // if the cache already exists: no need to continue, it should be
  // up to date */
  if (!cursor->duplicate_cache.empty())
    return;

  if (ISSET(what, LocalCursor::kBtree | LocalCursor::kTxn)) {
    if (cursor->is_nil(LocalCursor::kBtree)
            && !cursor->is_nil(LocalCursor::kTxn)) {
      bool equal_keys;
      cursor->synchronize(context, LocalCursor::kSyncOnlyEqualKeys, &equal_keys);
      if (!equal_keys)
        cursor->set_to_nil(LocalCursor::kBtree);
    }
  }

  // first collect all duplicates from the btree. They're already sorted,
  // therefore we can just append them to our duplicate-cache.
  if (ISSET(what, LocalCursor::kBtree) && !cursor->is_nil(LocalCursor::kBtree))
    append_btree_duplicates(cursor, context);

  // read duplicates from the txn-cursor?
  if (ISSET(what, LocalCursor::kTxn)
          && !cursor->is_nil(LocalCursor::kTxn))
    append_txn_duplicates(cursor, context);
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

  ups_status_t st = cursor->btree_cursor.move(context, &key,
                        &cursor->db->key_arena(cursor->txn), 0, 0, 0);
  if (unlikely(st))
    return st;

  TxnCursor txn_cursor(cursor);
  st = txn_cursor.find(&key, 0);
  if (unlikely(st))
    return st;

  TxnOperation *op = txn_cursor.get_coupled_op();
  if (ISSET(op->flags, TxnOperation::kInsertDuplicate))
    st = UPS_KEY_NOT_FOUND;
  return st;
}

static inline bool
txn_cursor_is_erase(TxnCursor *txnc)
{
  TxnOperation *op = txnc->get_coupled_op();
  return op ? ISSET(op->flags, TxnOperation::kErase) : false;
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
  return cursor->last_cmp = cursor->btree_cursor.compare(context, node->key());
}

LocalCursor::LocalCursor(LocalDb *db, Txn *txn)
  : Cursor(db, txn), txn_cursor(this), btree_cursor(this),
    duplicate_cache_index(0), last_operation(0), state(0), last_cmp(0)
{
}

LocalCursor::LocalCursor(LocalCursor &other)
  : Cursor(other), txn_cursor(this), btree_cursor(this)
{
  txn = other.txn;
  next = other.next;
  previous = other.previous;
  duplicate_cache_index = other.duplicate_cache_index;
  last_operation = other.last_operation;
  last_cmp = other.last_cmp;
  state = other.state;

  btree_cursor.clone(&other.btree_cursor);
  txn_cursor.clone(&other.txn_cursor);

  if (ISSET(db->flags(), UPS_ENABLE_DUPLICATE_KEYS))
    other.duplicate_cache = duplicate_cache;
}

void
LocalCursor::couple_to_duplicate(uint32_t duplicate_index)
{
  assert(duplicate_cache.size() >= duplicate_index);
  assert(duplicate_index >= 1);

  // duplicate_index is a 1-based index!
  DuplicateCacheLine &e = duplicate_cache[duplicate_index - 1];
  if (e.use_btree()) {
    activate_btree();
    btree_cursor.set_duplicate_index(e.btree_duplicate_index());
  }
  else {
    assert(e.txn_op() != 0);
    activate_txn(e.txn_op());
  }
  duplicate_cache_index = duplicate_index;
}

// TODO rename to reset()?
void
LocalCursor::synchronize(Context *context, uint32_t flags, bool *equal_keys)
{
  if (equal_keys)
    *equal_keys = false;

  if (is_nil(kBtree)) {
    if (!txn_cursor.get_coupled_op())
      return;
    // TODO this line is ugly
    ups_key_t *key = txn_cursor.get_coupled_op()->node->key();

    if (NOTSET(flags, kSyncOnlyEqualKeys))
      flags = flags | (ISSET(flags, UPS_CURSOR_NEXT)
                            ? UPS_FIND_GEQ_MATCH
                            : UPS_FIND_LEQ_MATCH);
    flags &= ~kSyncOnlyEqualKeys;
    // the flag |kSyncDontLoadKey| does not load the key if there's an
    // approx match - it only positions the cursor
    ups_status_t st = btree_cursor.find(context, key, 0, 0, 0,
                            kSyncDontLoadKey | flags);
    // if we had a direct hit instead of an approx. match then
    // set |equal_keys| to false; otherwise Cursor::move()
    // will move the btree cursor again
    if (st == 0 && equal_keys && !ups_key_get_approximate_match_type(key))
      *equal_keys = true;
  }
  else if (is_nil(kTxn)) {
    LocalCursor clone(*this);
    clone.btree_cursor.uncouple_from_page(context);
    ups_key_t *key = clone.btree_cursor.uncoupled_key();
    if (NOTSET(flags, kSyncOnlyEqualKeys))
      flags = flags | (ISSET(flags, UPS_CURSOR_NEXT)
                            ? UPS_FIND_GEQ_MATCH
                            : UPS_FIND_LEQ_MATCH);

    ups_status_t st = txn_cursor.find(key, kSyncDontLoadKey | flags);
    if (likely(st == 0)) {
      // if we had a direct hit instead of an approx. match then
      // set |equal_keys| to false; otherwise Cursor::move()
      // will move the btree cursor again
      if (equal_keys && !ups_key_get_approximate_match_type(key))
        *equal_keys = true;
    }
  }
}

uint32_t
LocalCursor::duplicate_cache_count(Context *context, bool clear_cache)
{
  if (NOTSET(db->flags(), UPS_ENABLE_DUPLICATE_KEYS))
    return 0;

  if (clear_cache)
    clear_duplicate_cache(this);

  if (is_txn_active())
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
  assert(duplicate_cache.size() > 0);

  duplicate_cache_index = 1;
  couple_to_duplicate(duplicate_cache_index);
  return 0;
}

ups_status_t
LocalCursor::move_last_duplicate(Context *context)
{
  assert(duplicate_cache.size() > 0);

  duplicate_cache_index = duplicate_cache.size();
  couple_to_duplicate(duplicate_cache_index);
  return 0;
}

ups_status_t
LocalCursor::move_next_key_singlestep(Context *context)
{
  ups_status_t st = 0;

  // make sure that the cursor advances if the other cursor is nil
  if ((is_nil(kTxn) && !is_nil(kBtree))
      || (is_nil(kBtree) && !is_nil(kTxn))) {
    last_cmp = 0;
  }

  // if both cursors point to the same key: move next with both
  if (last_cmp == 0) {
    if (!is_nil(kBtree)) {
      st = btree_cursor.move(context, 0, 0, 0, 0,
                    UPS_CURSOR_NEXT | UPS_SKIP_DUPLICATES);
      if (st == UPS_KEY_NOT_FOUND || st == UPS_CURSOR_IS_NIL) {
        set_to_nil(kBtree);
        if (unlikely(txn_cursor.is_nil()))
          return UPS_KEY_NOT_FOUND;
        activate_txn();
        last_cmp = 1;
      }
    }

    if (!txn_cursor.is_nil()) {
      st = txn_cursor.move(UPS_CURSOR_NEXT);
      if (st == UPS_KEY_NOT_FOUND || st == UPS_CURSOR_IS_NIL) {
        if (unlikely(is_nil(kBtree)))
          return UPS_KEY_NOT_FOUND;
        set_to_nil(kTxn);
        activate_btree();
        last_cmp = -1;

        if (check_if_btree_key_is_erased_or_overwritten(this, context)
                        == UPS_TXN_CONFLICT)
          st = UPS_TXN_CONFLICT;
      }
    }
  }
  // if the btree-key is smaller: move next
  else if (last_cmp < 0) {
    st = btree_cursor.move(context, 0, 0, 0, 0,
                    UPS_CURSOR_NEXT | UPS_SKIP_DUPLICATES);
    if (unlikely(st == UPS_KEY_NOT_FOUND)) {
      if (txn_cursor.is_nil())
        return st;
      set_to_nil(kBtree);
      activate_txn();
      last_cmp = +1;
    }
    else {
      if (check_if_btree_key_is_erased_or_overwritten(this, context)
                      == UPS_TXN_CONFLICT)
        st = UPS_TXN_CONFLICT;
    }
    if (txn_cursor.is_nil())
      last_cmp = -1;
  }
  // if the txn-key is smaller OR if both keys are equal: move next
  // with the txn-key (which is chronologically newer)
  else {
    st = txn_cursor.move(UPS_CURSOR_NEXT);
    if (st == UPS_KEY_NOT_FOUND) {
      if (unlikely(is_nil(kBtree)))
        return st;
      set_to_nil(kTxn);
      activate_btree();
      last_cmp = -1;
    }
    if (unlikely(is_nil(kBtree)))
      last_cmp = 1;
  }

  // one of the cursors was moved. Compare both cursors to see which one
  // is greater (or lower) than the other
  if (!is_nil(kBtree) && !txn_cursor.is_nil())
    compare(this, context);

  // if there's a txn conflict: move next
  if (st == UPS_TXN_CONFLICT)
    return move_next_key_singlestep(context);

  // btree-key is smaller
  if (last_cmp < 0 || txn_cursor.is_nil()) {
    activate_btree();
    update_duplicate_cache(this, context, kBtree);
    return 0;
  }

  // txn-key is smaller
  if (last_cmp > 0 || btree_cursor.is_nil()) {
    activate_txn();
    update_duplicate_cache(this, context, kTxn);
    return 0;
  }

  // if both keys are equal: use the txn-key, it is newer
  activate_txn();
  update_duplicate_cache(this, context, kTxn | kBtree);
  return 0;
}

ups_status_t
LocalCursor::move_next_key(Context *context, uint32_t flags)
{
  ups_status_t st;

  // are we in the middle of a duplicate list? if yes then move to the
  // next duplicate
  if (duplicate_cache_index > 0 && NOTSET(flags, UPS_SKIP_DUPLICATES)) {
    st = move_next_duplicate(context);
    if (st != UPS_LIMITS_REACHED)
      return st;
    if (ISSET(flags, UPS_ONLY_DUPLICATES))
      return UPS_KEY_NOT_FOUND;
  }

  backup_duplicate_cache(this);
  clear_duplicate_cache(this);

  // either there were no duplicates or we've reached the end of the
  // duplicate list. move next till we found a new candidate
  while (1) {
    st = move_next_key_singlestep(context);
    if (unlikely(st)) {
      restore_duplicate_cache(this);
      return st;
    }

    // check for duplicates. the duplicate cache was already updated in
    // move_next_key_singlestep()
    if (ISSET(db->flags(), UPS_ENABLE_DUPLICATE_KEYS)) {
      // are there any duplicates? if not then they were all erased and
      // we move to the previous key
      if (duplicate_cache.empty())
        continue;

      // otherwise move to the first duplicate */
      return move_first_duplicate(context);
    }

    // no duplicates - make sure that we've not coupled to an erased item
    if (is_txn_active()) {
      if (unlikely(txn_cursor_is_erase(&txn_cursor)))
        continue;
      return 0;
    }

    if (is_btree_active()) {
      st = check_if_btree_key_is_erased_or_overwritten(this, context);
      if (unlikely(st == UPS_KEY_ERASED_IN_TXN))
        continue;
      if (likely(st == 0)) {
        activate_txn();
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

  // make sure that the cursor advances if the other cursor is nil
  if ((is_nil(kTxn) && !is_nil(kBtree))
      || (is_nil(kBtree) && !is_nil(kTxn))) {
    last_cmp = 0;
  }

  // if both cursors point to the same key: move previous with both
  if (last_cmp == 0) {
    if (!is_nil(kBtree)) {
      st = btree_cursor.move(context, 0, 0, 0, 0,
                    UPS_CURSOR_PREVIOUS | UPS_SKIP_DUPLICATES);
      if (st == UPS_KEY_NOT_FOUND || st == UPS_CURSOR_IS_NIL) {
        if (txn_cursor.is_nil())
          return UPS_KEY_NOT_FOUND;
        set_to_nil(kBtree);
        activate_txn();
        last_cmp = -1;
      }
    }

    if (!txn_cursor.is_nil()) {
      st = txn_cursor.move(UPS_CURSOR_PREVIOUS);
      if (st == UPS_KEY_NOT_FOUND || st == UPS_CURSOR_IS_NIL) {
        if (is_nil(kBtree))
          return UPS_KEY_NOT_FOUND;
        set_to_nil(kTxn);
        activate_btree();
        last_cmp = 1;
      }
    }
  }
  // if the btree-key is greater: move previous
  else if (last_cmp > 0) {
    st = btree_cursor.move(context, 0, 0, 0, 0,
                    UPS_CURSOR_PREVIOUS | UPS_SKIP_DUPLICATES);
    if (st == UPS_KEY_NOT_FOUND) {
      if (txn_cursor.is_nil())
        return st;
      set_to_nil(kBtree);
      activate_txn();
      last_cmp = -1;
    }
    else {
      if (check_if_btree_key_is_erased_or_overwritten(this, context)
                      == UPS_TXN_CONFLICT)
        st = UPS_TXN_CONFLICT;
    }
    if (txn_cursor.is_nil())
      last_cmp = 1;
  }
  // if the txn-key is greater OR if both keys are equal: move previous
  // with the txn-key (which is chronologically newer)
  else {
    st = txn_cursor.move(UPS_CURSOR_PREVIOUS);
    if (st == UPS_KEY_NOT_FOUND) {
      if (is_nil(kBtree))
        return st;
      set_to_nil(kTxn);
      activate_btree();
      last_cmp = 1;

      if (check_if_btree_key_is_erased_or_overwritten(this, context)
                      == UPS_TXN_CONFLICT)
        st = UPS_TXN_CONFLICT;
    }
    if (is_nil(kBtree))
      last_cmp = -1;
  }

  // one of the cursors was moved. Compare both cursors to see which one
  // is greater (or lower) than the other
  if (!is_nil(kBtree) && !txn_cursor.is_nil())
    compare(this, context);

  // if there's a txn conflict: move previous
  if (unlikely(st == UPS_TXN_CONFLICT))
    return move_previous_key_singlestep(context);

  // btree-key is greater
  if (last_cmp > 0 || txn_cursor.is_nil()) {
    activate_btree();
    update_duplicate_cache(this, context, kBtree);
    return 0;
  }

  // txn-key is greater
  if (last_cmp < 0 || btree_cursor.is_nil()) {
    activate_txn();
    update_duplicate_cache(this, context, kTxn);
    return 0;
  }

  // both keys are equal
  activate_txn();
  update_duplicate_cache(this, context, kTxn | kBtree);
  return 0;
}

ups_status_t
LocalCursor::move_previous_key(Context *context, uint32_t flags)
{
  ups_status_t st;

  // are we in the middle of a duplicate list? if yes then move to the
  // previous duplicate
  if (duplicate_cache_index > 0 && NOTSET(flags, UPS_SKIP_DUPLICATES)) {
    st = move_previous_duplicate(context);
    if (st != UPS_LIMITS_REACHED)
      return st;
    if (ISSET(flags, UPS_ONLY_DUPLICATES))
      return UPS_KEY_NOT_FOUND;
  }

  backup_duplicate_cache(this);
  clear_duplicate_cache(this);

  // either there were no duplicates or we've reached the end of the
  // duplicate list. move previous till we found a new candidate
  while (!is_nil(kBtree) || !txn_cursor.is_nil()) {
    st = move_previous_key_singlestep(context);
    if (unlikely(st)) {
      restore_duplicate_cache(this);
      return st;
    }

    // check for duplicates. the duplicate cache was already updated in
    // move_previous_key_singlestep()
    if (ISSET(db->flags(), UPS_ENABLE_DUPLICATE_KEYS)) {
      // are there any duplicates? if not then they were all erased and
      // we move to the previous key
      if (duplicate_cache.empty())
        continue;

      // otherwise move to the last duplicate
      return move_last_duplicate(context);
    }

    // no duplicates - make sure that we've not coupled to an erased
    // item
    if (is_txn_active()) {
      if (unlikely(txn_cursor_is_erase(&txn_cursor)))
        continue;
      return 0;
    }

    if (is_btree_active()) {
      st = check_if_btree_key_is_erased_or_overwritten(this, context);
      if (unlikely(st == UPS_KEY_ERASED_IN_TXN))
        continue;
      if (likely(st == 0)) {
        activate_txn();
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
  // fetch the smallest key from the transaction tree
  ups_status_t txns = txn_cursor.move(UPS_CURSOR_FIRST);
  // fetch the smallest key from the btree tree
  ups_status_t btrs = btree_cursor.move(context, 0, 0, 0, 0,
                            UPS_CURSOR_FIRST | UPS_SKIP_DUPLICATES);
  // now consolidate - if both trees are empty then return
  if (btrs == UPS_KEY_NOT_FOUND && txns == UPS_KEY_NOT_FOUND)
    return UPS_KEY_NOT_FOUND;

  // if btree is empty but txn-tree is not: couple to txn
  if (btrs == UPS_KEY_NOT_FOUND && txns != UPS_KEY_NOT_FOUND) {
    if (unlikely(txns == UPS_TXN_CONFLICT))
      return txns;
    activate_txn();
    update_duplicate_cache(this, context, kTxn);
    return 0;
  }

  // if txn-tree is empty but btree is not: couple to btree
  if (txns == UPS_KEY_NOT_FOUND && btrs != UPS_KEY_NOT_FOUND) {
    activate_btree();
    update_duplicate_cache(this, context, kBtree);
    return 0;
  }

  // if both trees are not empty then compare them and couple to the smaller one
  assert(btrs == 0 && (txns == 0
                        || txns == UPS_KEY_ERASED_IN_TXN
                        || txns == UPS_TXN_CONFLICT));
  compare(this, context);

  // both keys are equal - couple to txn; it's chronologically newer
  if (last_cmp == 0) {
    if (unlikely(txns && txns != UPS_KEY_ERASED_IN_TXN))
      return txns;
    activate_txn();
    update_duplicate_cache(this, context, kBtree | kTxn);
    return 0;
  }

  // couple to txn
  if (last_cmp > 0) {
    if (unlikely(txns && txns != UPS_KEY_ERASED_IN_TXN))
      return txns;
    activate_txn();
    update_duplicate_cache(this, context, kTxn);
    return 0;
  }

  // couple to btree
  activate_btree();
  update_duplicate_cache(this, context, kBtree);
  return 0;
}

ups_status_t
LocalCursor::move_first_key(Context *context, uint32_t flags)
{
  // move to the very very first key
  ups_status_t st = move_first_key_singlestep(context);
  if (unlikely(st))
    return st;

  // check for duplicates. the duplicate cache was already updated in
  // move_first_key_singlestep()
  if (ISSET(db->flags(), UPS_ENABLE_DUPLICATE_KEYS)) {
    // are there any duplicates? if not then they were all erased and we
    // move to the previous key
    if (duplicate_cache.empty())
      return move_next_key(context, flags);

    // otherwise move to the first duplicate
    return move_first_duplicate(context);
  }

  // no duplicates - make sure that we've not coupled to an erased item
  if (is_txn_active()) {
    if (unlikely(txn_cursor_is_erase(&txn_cursor)))
      return move_next_key(context, flags);
    return 0;
  }

  if (is_btree_active()) {
    st = check_if_btree_key_is_erased_or_overwritten(this, context);
    if (unlikely(st == UPS_KEY_ERASED_IN_TXN))
      return move_next_key(context, flags);
    if (likely(st == 0)) {
      activate_txn();
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
  // fetch the largest key from the transaction tree
  ups_status_t txns = txn_cursor.move(UPS_CURSOR_LAST);
  // fetch the largest key from the btree tree
  ups_status_t btrs = btree_cursor.move(context, 0, 0, 0, 0,
                  UPS_CURSOR_LAST | UPS_SKIP_DUPLICATES);
  // now consolidate - if both trees are empty then return
  if (unlikely(btrs == UPS_KEY_NOT_FOUND && txns == UPS_KEY_NOT_FOUND))
    return UPS_KEY_NOT_FOUND;

  // if btree is empty but txn-tree is not: couple to txn
  if (btrs == UPS_KEY_NOT_FOUND && txns != UPS_KEY_NOT_FOUND) {
    if (unlikely(txns == UPS_TXN_CONFLICT))
      return txns;
    activate_txn();
    update_duplicate_cache(this, context, kTxn);
    return 0;
  }

  // if txn-tree is empty but btree is not: couple to btree
  if (txns == UPS_KEY_NOT_FOUND && btrs != UPS_KEY_NOT_FOUND) {
    activate_btree();
    update_duplicate_cache(this, context, kBtree);
    return 0;
  }

  // if both trees are not empty then compare them and couple to the
  // greater one
  assert(btrs == 0 && (txns == 0
                        || txns == UPS_KEY_ERASED_IN_TXN
                        || txns == UPS_TXN_CONFLICT));
  compare(this, context);

  // both keys are equal - couple to txn; it's chronologically newer
  if (last_cmp == 0) {
    if (unlikely(txns && txns != UPS_KEY_ERASED_IN_TXN))
      return txns;
    activate_txn();
    update_duplicate_cache(this, context, kBtree | kTxn);
    return 0;
  }

  // couple to txn
  if (last_cmp < 1) {
    if (unlikely(txns && txns != UPS_KEY_ERASED_IN_TXN))
      return txns;
    activate_txn();
    update_duplicate_cache(this, context, kTxn);
    return 0;
  }

  // couple to btree
  activate_btree();
  update_duplicate_cache(this, context, kBtree);
  return 0;
}

ups_status_t
LocalCursor::move_last_key(Context *context, uint32_t flags)
{
  ups_status_t st = 0;

  // move to the very very last key
  st = move_last_key_singlestep(context);
  if (unlikely(st))
    return st;

  // check for duplicates. the duplicate cache was already updated in
  // move_last_key_singlestep()
  if (ISSET(db->flags(), UPS_ENABLE_DUPLICATE_KEYS)) {
    // are there any duplicates? if not then they were all erased and we
    // move to the previous key
    if (duplicate_cache.empty())
      return move_previous_key(context, flags);

    // otherwise move to the last duplicate
    return move_last_duplicate(context);
  }

  // no duplicates - make sure that we've not coupled to an erased item
  if (is_txn_active()) {
    if (unlikely(txn_cursor_is_erase(&txn_cursor)))
      return move_previous_key(context, flags);
    return 0;
  }

  if (is_btree_active()) {
    st = check_if_btree_key_is_erased_or_overwritten(this, context);
    if (unlikely(st == UPS_KEY_ERASED_IN_TXN))
      return move_previous_key(context, flags);
    if (likely(st == 0)) {
      activate_txn();
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

  // no movement requested? directly retrieve key/record
  if (unlikely(!flags))
    goto retrieve_key_and_record;

  // in non-transactional mode - just call the btree function and return
  if (NOTSET(ldb(this)->flags(), UPS_ENABLE_TRANSACTIONS)) {
    st = btree_cursor.move(context, key, &ldb(this)->key_arena(context->txn),
                           record, &ldb(this)->record_arena(context->txn),
                           flags);
    if (likely(st == 0))
      activate_btree();
    return st;
  }

  // synchronize the btree and transaction cursor if the last operation was
  // not a move next/previous OR if the direction changed
  if (likely(ISSETANY(flags, UPS_CURSOR_NEXT | UPS_CURSOR_PREVIOUS))) {
    bool changed_dir = false;

    if (unlikely(last_operation == UPS_CURSOR_PREVIOUS
                && ISSET(flags, UPS_CURSOR_NEXT)))
      changed_dir = true;
    else if (unlikely(last_operation == UPS_CURSOR_NEXT
                && ISSET(flags, UPS_CURSOR_PREVIOUS)))
      changed_dir = true;

    if (unlikely(last_operation == kLookupOrInsert || changed_dir)) {
      set_to_nil(is_txn_active() ? kBtree : kTxn);

      // TODO merge those four lines
      synchronize(context, flags, 0);
      if (!txn_cursor.is_nil() && !is_nil(kBtree))
        compare(this, context);
      // only include transactional updates if btree cursor and txn-cursor
      // point to the same key!
      update_duplicate_cache(this, context,
                      last_cmp == 0 ? kTxn | kBtree : kBtree);
    }
  }

  // we have either skipped duplicates or reached the end of the duplicate
  // list. btree cursor and txn cursor are synced and as close to
  // each other as possible. Move the cursor in the requested direction.
  if (ISSET(flags, UPS_CURSOR_NEXT))
    st = move_next_key(context, flags);
  else if (ISSET(flags, UPS_CURSOR_PREVIOUS))
    st = move_previous_key(context, flags);
  else if (ISSET(flags, UPS_CURSOR_FIRST)) {
    backup_duplicate_cache(this);
    clear_duplicate_cache(this);
    st = move_first_key(context, flags);
  }
  else {
    assert(ISSET(flags, UPS_CURSOR_LAST));
    backup_duplicate_cache(this);
    clear_duplicate_cache(this);
    st = move_last_key(context, flags);
  }

  if (unlikely(st)) {
    restore_duplicate_cache(this);
    return st;
  }

retrieve_key_and_record:
  // retrieve key/record, if requested
  if (unlikely(is_txn_active())) {
    if (likely(key != 0))
      txn_cursor.copy_coupled_key(key);
    if (likely(record != 0))
      txn_cursor.copy_coupled_record(record);
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
  int old_last_operation = last_operation;

  // we need to restore the duplicate_cache_index, since it's overwritten
  // in |LocalDb::insert|
  int old_index = duplicate_cache_index;

  if (ISSET(ldb(this)->flags(), UPS_ENABLE_TRANSACTIONS)) {
    if (is_btree_active()) {
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

    duplicate_cache_index = old_index;

    if (likely(st == 0))
      activate_txn();
  }
  else {
    btree_cursor.overwrite(&context, record, flags);
    activate_btree();
  }

  // restore last operation; the cursor was NOT moved!
  last_operation = old_last_operation;

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
      break;
    default:
      assert(what == 0);
      btree_cursor.set_to_nil();
      txn_cursor.set_to_nil();
      clear_duplicate_cache(this);
      state = 0;
      break;
  }
}

void
LocalCursor::close()
{
  set_to_nil();
  btree_cursor.close();
  duplicate_cache.clear();
}

uint32_t
LocalCursor::get_duplicate_position()
{
  if (unlikely(is_nil()))
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

  if (txn || is_txn_active()) {
    if (ISSET(db->flags(), UPS_ENABLE_DUPLICATE_KEYS)) {
      synchronize(&context, 0, 0);
      update_duplicate_cache(this, &context, kTxn | kBtree);
      return duplicate_cache.size();
    }

    // duplicate keys are disabled
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

  if (is_txn_active())
    return txn_cursor.record_size();
  return btree_cursor.record_size(&context);
}

