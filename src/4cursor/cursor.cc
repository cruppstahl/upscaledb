/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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

#include <string.h>

// Always verify that a file of level N does not include headers > N!
#include "3btree/btree_cursor.h"
#include "3btree/btree_index.h"
#include "3btree/btree_node_proxy.h"
#include "4cursor/cursor.h"
#include "4env/env_local.h"
#include "4txn/txn_local.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

using namespace hamsterdb;

Cursor::Cursor(LocalDatabase *db, Transaction *txn, ham_u32_t flags)
  : m_db(db), m_txn(txn), m_txn_cursor(this), m_btree_cursor(this),
    m_remote_handle(0), m_next(0), m_previous(0), m_dupecache_index(0),
    m_lastop(0), m_last_cmp(0), m_flags(flags), m_is_first_use(true)
{
}

Cursor::Cursor(Cursor &other)
  : m_db(other.m_db), m_txn_cursor(this), m_btree_cursor(this)
{
  m_txn = other.m_txn;
  m_remote_handle = other.m_remote_handle;
  m_next = other.m_next;
  m_previous = other.m_previous;
  m_dupecache_index = other.m_dupecache_index;
  m_lastop = other.m_lastop;
  m_last_cmp = other.m_last_cmp;
  m_flags = other.m_flags;
  m_is_first_use = other.m_is_first_use;

  m_btree_cursor.clone(other.get_btree_cursor());
  m_txn_cursor.clone(other.get_txn_cursor());

  if (m_db->get_rt_flags() & HAM_ENABLE_DUPLICATE_KEYS)
    other.get_dupecache()->clone(get_dupecache());
}

void
Cursor::append_btree_duplicates(BtreeCursor *btc, DupeCache *dc)
{
  ham_u32_t count = btc->get_record_count(0);
  for (ham_u32_t i = 0; i < count; i++)
    dc->append(DupeCacheLine(true, i));

  m_db->get_local_env()->get_changeset().clear();
}

void
Cursor::update_dupecache(ham_u32_t what)
{
  DupeCache *dc = get_dupecache();
  BtreeCursor *btc = get_btree_cursor();
  TransactionCursor *txnc = get_txn_cursor();

  if (!(m_db->get_rt_flags() & HAM_ENABLE_DUPLICATE_KEYS))
    return;

  /* if the cache already exists: no need to continue, it should be
   * up to date */
  if (dc->get_count() != 0)
    return;

  if ((what & kBtree) && (what & kTxn)) {
    if (is_nil(kBtree) && !is_nil(kTxn)) {
      bool equal_keys;
      sync(0, &equal_keys);
      if (!equal_keys)
        set_to_nil(kBtree);
    }
  }

  /* first collect all duplicates from the btree. They're already sorted,
   * therefore we can just append them to our duplicate-cache. */
  if ((what & kBtree) && !is_nil(kBtree))
    append_btree_duplicates(btc, dc);

  /* read duplicates from the txn-cursor? */
  if ((what & kTxn) && !is_nil(kTxn)) {
    TransactionOperation *op = txnc->get_coupled_op();
    TransactionNode *node = op->get_node();

    if (!node)
      return;

    /* now start integrating the items from the transactions */
    op = node->get_oldest_op();
    while (op) {
      Transaction *optxn = op->get_txn();
      /* collect all ops that are valid (even those that are
       * from conflicting transactions) */
      if (!optxn->is_aborted()) {
        /* a normal (overwriting) insert will overwrite ALL dupes,
         * but an overwrite of a duplicate will only overwrite
         * an entry in the dupecache */
        if (op->get_flags() & TransactionOperation::kInsert) {
          /* all existing dupes are overwritten */
          dc->clear();
          dc->append(DupeCacheLine(false, op));
        }
        else if (op->get_flags() & TransactionOperation::kInsertOverwrite) {
          ham_u32_t ref = op->get_referenced_dupe();
          if (ref) {
            ham_assert(ref <= dc->get_count());
            DupeCacheLine *e = dc->get_first_element();
            (&e[ref - 1])->set_txn_op(op);
          }
          else {
            /* all existing dupes are overwritten */
            dc->clear();
            dc->append(DupeCacheLine(false, op));
          }
        }
        /* insert a duplicate key */
        else if (op->get_flags() & TransactionOperation::kInsertDuplicate) {
          ham_u32_t of = op->get_orig_flags();
          ham_u32_t ref = op->get_referenced_dupe() - 1;
          DupeCacheLine dcl(false, op);
          if (of & HAM_DUPLICATE_INSERT_FIRST)
            dc->insert(0, dcl);
          else if (of & HAM_DUPLICATE_INSERT_BEFORE) {
            dc->insert(ref, dcl);
          }
          else if (of & HAM_DUPLICATE_INSERT_AFTER) {
            if (ref + 1 >= dc->get_count())
              dc->append(dcl);
            else
              dc->insert(ref + 1, dcl);
          }
          else /* default is HAM_DUPLICATE_INSERT_LAST */
            dc->append(dcl);
        }
        /* a normal erase will erase ALL duplicate keys */
        else if (op->get_flags() & TransactionOperation::kErase) {
          ham_u32_t ref = op->get_referenced_dupe();
          if (ref) {
            ham_assert(ref <= dc->get_count());
            dc->erase(ref - 1);
          }
          else {
            /* all existing dupes are erased */
            dc->clear();
          }
        }
        else {
          /* everything else is a bug! */
          ham_assert(op->get_flags() == TransactionOperation::kNop);
        }
      }

      /* continue with the previous/older operation */
      op = op->get_next_in_node();
    }
  }
}

void
Cursor::couple_to_dupe(ham_u32_t dupe_id)
{
  TransactionCursor *txnc = get_txn_cursor();
  DupeCache *dc = get_dupecache();
  DupeCacheLine *e = 0;

  ham_assert(dc->get_count() >= dupe_id);
  ham_assert(dupe_id >= 1);

  /* dupe-id is a 1-based index! */
  e = dc->get_element(dupe_id - 1);
  if (e->use_btree()) {
    BtreeCursor *btc = get_btree_cursor();
    couple_to_btree();
    btc->set_duplicate_index((ham_u32_t)e->get_btree_dupe_idx());
  }
  else {
    ham_assert(e->get_txn_op() != 0);
    txnc->couple_to_op(e->get_txn_op());
    couple_to_txnop();
  }
  set_dupecache_index(dupe_id);
}

ham_status_t
Cursor::check_if_btree_key_is_erased_or_overwritten()
{
  ham_key_t key = {0};
  TransactionOperation *op;
  ham_status_t st;
  Cursor *clone = get_db()->cursor_clone(this);
  TransactionCursor *txnc = clone->get_txn_cursor();
  st = get_btree_cursor()->move(&key, 0, 0);
  if (st) {
    get_db()->cursor_close(clone);
    return (st);
  }

  st = txnc->find(&key, 0);
  if (st) {
    get_db()->cursor_close(clone);
    return (st);
  }

  op = txnc->get_coupled_op();
  if (op->get_flags() & TransactionOperation::kInsertDuplicate)
    st = HAM_KEY_NOT_FOUND;
  get_db()->cursor_close(clone);
  return (st);
}

void
Cursor::sync(ham_u32_t flags, bool *equal_keys)
{
  TransactionCursor *txnc = get_txn_cursor();
  if (equal_keys)
    *equal_keys = false;

  if (is_nil(kBtree)) {
    TransactionNode *node;
    ham_key_t *k;
    if (!txnc->get_coupled_op())
      return;
    node = txnc->get_coupled_op()->get_node();
    k = node->get_key();

    if (!(flags & kSyncOnlyEqualKeys))
      flags = flags | ((flags & HAM_CURSOR_NEXT)
                ? HAM_FIND_GEQ_MATCH
                : HAM_FIND_LEQ_MATCH);
    /* the flag DONT_LOAD_KEY does not load the key if there's an
     * approx match - it only positions the cursor */
    ham_status_t st = get_btree_cursor()->find(k, 0, kSyncDontLoadKey | flags);
    /* if we had a direct hit instead of an approx. match then
     * set fresh_start to false; otherwise do_local_cursor_move
     * will move the btree cursor again */
    if (st == 0 && equal_keys && !ham_key_get_approximate_match_type(k))
      *equal_keys = true;
  }
  else if (is_nil(kTxn)) {
    ham_key_t *k;
    Cursor *clone = get_db()->cursor_clone(this);
    clone->get_btree_cursor()->uncouple_from_page();
    k = clone->get_btree_cursor()->get_uncoupled_key();
    if (!(flags & kSyncOnlyEqualKeys))
      flags = flags | ((flags & HAM_CURSOR_NEXT)
          ? HAM_FIND_GEQ_MATCH
          : HAM_FIND_LEQ_MATCH);
    ham_status_t st = txnc->find(k, kSyncDontLoadKey | flags);
    /* if we had a direct hit instead of an approx. match then
    * set fresh_start to false; otherwise do_local_cursor_move
    * will move the btree cursor again */
    if (st == 0 && equal_keys && !ham_key_get_approximate_match_type(k))
      *equal_keys = true;
    get_db()->cursor_close(clone);
  }

  m_db->get_local_env()->get_changeset().clear();
}

ham_status_t
Cursor::move_next_dupe()
{
  DupeCache *dc = get_dupecache();

  if (get_dupecache_index()) {
    if (get_dupecache_index() < dc->get_count()) {
      set_dupecache_index(get_dupecache_index() + 1);
      couple_to_dupe(get_dupecache_index());
      return (0);
    }
  }
  return (HAM_LIMITS_REACHED);
}

ham_status_t
Cursor::move_previous_dupe()
{
  if (get_dupecache_index()) {
    if (get_dupecache_index() > 1) {
      set_dupecache_index(get_dupecache_index() - 1);
      couple_to_dupe(get_dupecache_index());
      return (0);
    }
  }
  return (HAM_LIMITS_REACHED);
}

ham_status_t
Cursor::move_first_dupe()
{
  DupeCache *dc = get_dupecache();

  if (dc->get_count()) {
    set_dupecache_index(1);
    couple_to_dupe(get_dupecache_index());
    return (0);
  }
  return (HAM_LIMITS_REACHED);
}

ham_status_t
Cursor::move_last_dupe()
{
  DupeCache *dc = get_dupecache();

  if (dc->get_count()) {
    set_dupecache_index(dc->get_count());
    couple_to_dupe(get_dupecache_index());
    return (0);
  }
  return (HAM_LIMITS_REACHED);
}

static bool
__txn_cursor_is_erase(TransactionCursor *txnc)
{
  TransactionOperation *op = txnc->get_coupled_op();
  return (op ? (op->get_flags() & TransactionOperation::kErase) : false);
}

int
Cursor::compare()
{
  BtreeCursor *btrc = get_btree_cursor();
  TransactionCursor *txnc = get_txn_cursor();
  BtreeIndex *btree = get_db()->get_btree_index();

  TransactionNode *node = txnc->get_coupled_op()->get_node();
  ham_key_t *txnk = node->get_key();

  ham_assert(!is_nil(0));
  ham_assert(!txnc->is_nil());

  if (btrc->get_state() == BtreeCursor::kStateCoupled) {
    Page *page;
    int slot;
    btrc->get_coupled_key(&page, &slot, 0);
    m_last_cmp = btree->get_node_from_page(page)->compare(txnk, slot);

    // need to fix the sort order - we compare txnk vs page[slot], but the
    // caller expects m_last_cmp to be the comparison of page[slot] vs txnk
    if (m_last_cmp == -1)
      m_last_cmp = +1;
    else if (m_last_cmp == +1)
      m_last_cmp = -1;

    return (m_last_cmp);
  }
  else if (btrc->get_state() == BtreeCursor::kStateUncoupled) {
    m_last_cmp = btree->compare_keys(btrc->get_uncoupled_key(), txnk);
    return (m_last_cmp);
  }

  ham_assert(!"shouldn't be here");
  return (0);
}

ham_status_t
Cursor::move_next_key_singlestep()
{
  ham_status_t st = 0;
  TransactionCursor *txnc = get_txn_cursor();
  BtreeCursor *btrc = get_btree_cursor();

  /* if both cursors point to the same key: move next with both */
  if (m_last_cmp == 0) {
    if (!is_nil(kBtree)) {
      st = btrc->move(0, 0, HAM_CURSOR_NEXT | HAM_SKIP_DUPLICATES);
      if (st == HAM_KEY_NOT_FOUND || st == HAM_CURSOR_IS_NIL) {
        set_to_nil(kBtree); // TODO muss raus
        if (txnc->is_nil())
          return (HAM_KEY_NOT_FOUND);
        else {
          couple_to_txnop();
          m_last_cmp = 1;
        }
      }
    }
    if (!txnc->is_nil()) {
      st = txnc->move(HAM_CURSOR_NEXT);
      if (st == HAM_KEY_NOT_FOUND || st==HAM_CURSOR_IS_NIL) {
        set_to_nil(kTxn); // TODO muss raus
        if (is_nil(kBtree))
          return (HAM_KEY_NOT_FOUND);
        else {
          couple_to_btree();
          m_last_cmp = -1;

          ham_status_t st2 = check_if_btree_key_is_erased_or_overwritten();
          if (st2 == HAM_TXN_CONFLICT)
            st = st2;
        }
      }
    }
  }
  /* if the btree-key is smaller: move it next */
  else if (m_last_cmp < 0) {
    st = btrc->move(0, 0, HAM_CURSOR_NEXT | HAM_SKIP_DUPLICATES);
    if (st == HAM_KEY_NOT_FOUND) {
      set_to_nil(kBtree); // TODO Das muss raus!
      if (txnc->is_nil())
        return (st);
      couple_to_txnop();
      m_last_cmp = +1;
    }
    else {
      ham_status_t st2 = check_if_btree_key_is_erased_or_overwritten();
      if (st2 == HAM_TXN_CONFLICT)
        st = st2;
    }
    if (txnc->is_nil())
      m_last_cmp = -1;
  }
  /* if the txn-key is smaller OR if both keys are equal: move next
   * with the txn-key (which is chronologically newer) */
  else {
    st = txnc->move(HAM_CURSOR_NEXT);
    if (st == HAM_KEY_NOT_FOUND) {
      set_to_nil(kTxn); // TODO Das muss raus!
      if (is_nil(kBtree))
        return (st);
      couple_to_btree();
      m_last_cmp = -1;
    }
    if (is_nil(kBtree))
      m_last_cmp = 1;
  }

  /* compare keys again */
  if (!is_nil(kBtree) && !txnc->is_nil())
    compare();

  /* if there's a txn conflict: move next */
  if (st == HAM_TXN_CONFLICT)
    return (move_next_key_singlestep());

  /* btree-key is smaller */
  if (m_last_cmp < 0 || txnc->is_nil()) {
    couple_to_btree();
    update_dupecache(kBtree);
    return (0);
  }
  /* txn-key is smaller */
  else if (m_last_cmp > 0 || btrc->get_state() == BtreeCursor::kStateNil) {
    couple_to_txnop();
    update_dupecache(kTxn);
    return (0);
  }
  /* both keys are equal */
  else {
    couple_to_txnop();
    update_dupecache(kTxn | kBtree);
    return (0);
  }
}

ham_status_t
Cursor::move_next_key(ham_u32_t flags)
{
  ham_status_t st;
  TransactionCursor *txnc = get_txn_cursor();

  /* are we in the middle of a duplicate list? if yes then move to the
   * next duplicate */
  if (get_dupecache_index() > 0 && !(flags & HAM_SKIP_DUPLICATES)) {
    st = move_next_dupe();
    if (st != HAM_LIMITS_REACHED)
      return (st);
    else if (st == HAM_LIMITS_REACHED && (flags & HAM_ONLY_DUPLICATES))
      return (HAM_KEY_NOT_FOUND);
  }

  clear_dupecache();

  /* either there were no duplicates or we've reached the end of the
   * duplicate list. move next till we found a new candidate */
  while (1) {
    st = move_next_key_singlestep();
    if (st)
      return (st);

    /* check for duplicates. the dupecache was already updated in
     * move_next_key_singlestep() */
    if (m_db->get_rt_flags() & HAM_ENABLE_DUPLICATE_KEYS) {
      /* are there any duplicates? if not then they were all erased and
       * we move to the previous key */
      if (!has_duplicates())
        continue;

      /* otherwise move to the first duplicate */
      return (move_first_dupe());
    }

    /* no duplicates - make sure that we've not coupled to an erased
     * item */
    if (is_coupled_to_txnop()) {
      if (__txn_cursor_is_erase(txnc))
        continue;
      else
        return (0);
    }
    if (is_coupled_to_btree()) {
      st = check_if_btree_key_is_erased_or_overwritten();
      if (st == HAM_KEY_ERASED_IN_TXN)
        continue;
      else if (st == 0) {
        couple_to_txnop();
        return (0);
      }
      else if (st == HAM_KEY_NOT_FOUND)
        return (0);
      else
        return (st);
    }
    else
      return (HAM_KEY_NOT_FOUND);
  }

  ham_assert(!"should never reach this");
  return (HAM_INTERNAL_ERROR);
}

ham_status_t
Cursor::move_previous_key_singlestep()
{
  ham_status_t st = 0;
  TransactionCursor *txnc = get_txn_cursor();
  BtreeCursor *btrc = get_btree_cursor();

  /* if both cursors point to the same key: move previous with both */
  if (m_last_cmp == 0) {
    if (!is_nil(kBtree)) {
      st = btrc->move(0, 0, HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES);
      if (st == HAM_KEY_NOT_FOUND || st==HAM_CURSOR_IS_NIL) {
        set_to_nil(kBtree); // TODO muss raus
        if (txnc->is_nil())
          return (HAM_KEY_NOT_FOUND);
        else {
          couple_to_txnop();
          m_last_cmp = -1;
        }
      }
    }
    if (!txnc->is_nil()) {
      st = txnc->move(HAM_CURSOR_PREVIOUS);
      if (st == HAM_KEY_NOT_FOUND || st==HAM_CURSOR_IS_NIL) {
        set_to_nil(kTxn); // TODO muss raus
        if (is_nil(kBtree))
          return (HAM_KEY_NOT_FOUND);
        else {
          couple_to_btree();
          m_last_cmp = 1;
        }
      }
    }
  }
  /* if the btree-key is greater: move previous */
  else if (m_last_cmp > 0) {
    st = btrc->move(0, 0, HAM_CURSOR_PREVIOUS | HAM_SKIP_DUPLICATES);
    if (st == HAM_KEY_NOT_FOUND) {
      set_to_nil(kBtree); // TODO Das muss raus!
      if (txnc->is_nil())
        return (st);
      couple_to_txnop();
      m_last_cmp = -1;
    }
    else {
      ham_status_t st2 = check_if_btree_key_is_erased_or_overwritten();
      if (st2 == HAM_TXN_CONFLICT)
        st = st2;
    }
    if (txnc->is_nil())
      m_last_cmp = 1;
  }
  /* if the txn-key is greater OR if both keys are equal: move previous
   * with the txn-key (which is chronologically newer) */
  else {
    st = txnc->move(HAM_CURSOR_PREVIOUS);
    if (st == HAM_KEY_NOT_FOUND) {
      set_to_nil(kTxn); // TODO Das muss raus!
      if (is_nil(kBtree))
        return (st);
      couple_to_btree();
      m_last_cmp = 1;

      ham_status_t st2 = check_if_btree_key_is_erased_or_overwritten();
      if (st2 == HAM_TXN_CONFLICT)
        st = st2;
    }
    if (is_nil(kBtree))
      m_last_cmp = -1;
  }

  /* compare keys again */
  if (!is_nil(kBtree) && !txnc->is_nil())
    compare();

  /* if there's a txn conflict: move previous */
  if (st == HAM_TXN_CONFLICT)
    return (move_previous_key_singlestep());

  /* btree-key is greater */
  if (m_last_cmp > 0 || txnc->is_nil()) {
    couple_to_btree();
    update_dupecache(kBtree);
    return (0);
  }
  /* txn-key is greater */
  else if (m_last_cmp < 0 || btrc->get_state() == BtreeCursor::kStateNil) {
    couple_to_txnop();
    update_dupecache(kTxn);
    return (0);
  }
  /* both keys are equal */
  else {
    couple_to_txnop();
    update_dupecache(kTxn | kBtree);
    return (0);
  }
}

ham_status_t
Cursor::move_previous_key(ham_u32_t flags)
{
  ham_status_t st;
  TransactionCursor *txnc = get_txn_cursor();

  /* are we in the middle of a duplicate list? if yes then move to the
   * previous duplicate */
  if (get_dupecache_index() > 0 && !(flags & HAM_SKIP_DUPLICATES)) {
    st = move_previous_dupe();
    if (st != HAM_LIMITS_REACHED)
      return (st);
    else if (st == HAM_LIMITS_REACHED && (flags & HAM_ONLY_DUPLICATES))
      return (HAM_KEY_NOT_FOUND);
  }

  clear_dupecache();

  /* either there were no duplicates or we've reached the end of the
   * duplicate list. move previous till we found a new candidate */
  while (!is_nil(kBtree) || !txnc->is_nil()) {
    st = move_previous_key_singlestep();
    if (st)
      return (st);

    /* check for duplicates. the dupecache was already updated in
     * move_previous_key_singlestep() */
    if (m_db->get_rt_flags() & HAM_ENABLE_DUPLICATE_KEYS) {
      /* are there any duplicates? if not then they were all erased and
       * we move to the previous key */
      if (!has_duplicates())
        continue;

      /* otherwise move to the last duplicate */
      return (move_last_dupe());
    }

    /* no duplicates - make sure that we've not coupled to an erased
     * item */
    if (is_coupled_to_txnop()) {
      if (__txn_cursor_is_erase(txnc))
        continue;
      else
        return (0);
    }
    if (is_coupled_to_btree()) {
      st = check_if_btree_key_is_erased_or_overwritten();
      if (st == HAM_KEY_ERASED_IN_TXN)
        continue;
      else if (st == 0) {
        couple_to_txnop();
        return (0);
      }
      else if (st == HAM_KEY_NOT_FOUND)
        return (0);
      else
        return (st);
    }
    else
      return (HAM_KEY_NOT_FOUND);
  }

  return (HAM_KEY_NOT_FOUND);
}

ham_status_t
Cursor::move_first_key_singlestep()
{
  ham_status_t btrs, txns;
  TransactionCursor *txnc = get_txn_cursor();
  BtreeCursor *btrc = get_btree_cursor();

  /* fetch the smallest key from the transaction tree. */
  txns = txnc->move(HAM_CURSOR_FIRST);
  /* fetch the smallest key from the btree tree. */
  btrs = btrc->move(0, 0, HAM_CURSOR_FIRST|HAM_SKIP_DUPLICATES);
  /* now consolidate - if both trees are empty then return */
  if (btrs == HAM_KEY_NOT_FOUND && txns == HAM_KEY_NOT_FOUND) {
    return (HAM_KEY_NOT_FOUND);
  }
  /* if btree is empty but txn-tree is not: couple to txn */
  else if (btrs == HAM_KEY_NOT_FOUND && txns != HAM_KEY_NOT_FOUND) {
    if (txns == HAM_TXN_CONFLICT)
      return (txns);
    couple_to_txnop();
    update_dupecache(kTxn);
    return (0);
  }
  /* if txn-tree is empty but btree is not: couple to btree */
  else if (txns == HAM_KEY_NOT_FOUND && btrs != HAM_KEY_NOT_FOUND) {
    couple_to_btree();
    update_dupecache(kBtree);
    return (0);
  }
  /* if both trees are not empty then compare them and couple to the
   * smaller one */
  else {
    ham_assert(btrs == 0 && (txns == 0
        || txns == HAM_KEY_ERASED_IN_TXN
        || txns == HAM_TXN_CONFLICT));
    compare();

    /* both keys are equal - couple to txn; it's chronologically
     * newer */
    if (m_last_cmp == 0) {
      if (txns && txns != HAM_KEY_ERASED_IN_TXN)
        return (txns);
      couple_to_txnop();
      update_dupecache(kBtree | kTxn);
    }
    /* couple to txn */
    else if (m_last_cmp > 0) {
      if (txns && txns != HAM_KEY_ERASED_IN_TXN)
        return (txns);
      couple_to_txnop();
      update_dupecache(kTxn);
    }
    /* couple to btree */
    else {
      couple_to_btree();
      update_dupecache(kBtree);
    }
    return (0);
  }
}

ham_status_t
Cursor::move_first_key(ham_u32_t flags)
{
  ham_status_t st = 0;
  TransactionCursor *txnc = get_txn_cursor();

  /* move to the very very first key */
  st = move_first_key_singlestep();
  if (st)
    return (st);

  /* check for duplicates. the dupecache was already updated in
   * move_first_key_singlestep() */
  if (m_db->get_rt_flags() & HAM_ENABLE_DUPLICATE_KEYS) {
    /* are there any duplicates? if not then they were all erased and we
     * move to the previous key */
    if (!has_duplicates())
      return (move_next_key(flags));

    /* otherwise move to the first duplicate */
    return (move_first_dupe());
  }

  /* no duplicates - make sure that we've not coupled to an erased
   * item */
  if (is_coupled_to_txnop()) {
    if (__txn_cursor_is_erase(txnc))
      return (move_next_key(flags));
    else
      return (0);
  }
  if (is_coupled_to_btree()) {
    st = check_if_btree_key_is_erased_or_overwritten();
    if (st == HAM_KEY_ERASED_IN_TXN)
      return (move_next_key(flags));
    else if (st == 0) {
      couple_to_txnop();
      return (0);
    }
    else if (st == HAM_KEY_NOT_FOUND)
      return (0);
    else
      return (st);
  }
  else
    return (HAM_KEY_NOT_FOUND);
}

ham_status_t
Cursor::move_last_key_singlestep()
{
  ham_status_t btrs, txns;
  TransactionCursor *txnc = get_txn_cursor();
  BtreeCursor *btrc = get_btree_cursor();

  /* fetch the largest key from the transaction tree. */
  txns = txnc->move(HAM_CURSOR_LAST);
  /* fetch the largest key from the btree tree. */
  btrs = btrc->move(0, 0, HAM_CURSOR_LAST | HAM_SKIP_DUPLICATES);
  /* now consolidate - if both trees are empty then return */
  if (btrs == HAM_KEY_NOT_FOUND && txns == HAM_KEY_NOT_FOUND) {
    return (HAM_KEY_NOT_FOUND);
  }
  /* if btree is empty but txn-tree is not: couple to txn */
  else if (btrs == HAM_KEY_NOT_FOUND && txns != HAM_KEY_NOT_FOUND) {
    if (txns == HAM_TXN_CONFLICT)
      return (txns);
    couple_to_txnop();
    update_dupecache(kTxn);
    return (0);
  }
  /* if txn-tree is empty but btree is not: couple to btree */
  else if (txns == HAM_KEY_NOT_FOUND && btrs != HAM_KEY_NOT_FOUND) {
    couple_to_btree();
    update_dupecache(kBtree);
    return (0);
  }
  /* if both trees are not empty then compare them and couple to the
   * greater one */
  else {
    ham_assert(btrs == 0 && (txns == 0
        || txns == HAM_KEY_ERASED_IN_TXN
        || txns == HAM_TXN_CONFLICT));
    compare();

    /* both keys are equal - couple to txn; it's chronologically
     * newer */
    if (m_last_cmp == 0) {
      if (txns && txns != HAM_KEY_ERASED_IN_TXN)
        return (txns);
      couple_to_txnop();
      update_dupecache(kBtree | kTxn);
    }
    /* couple to txn */
    else if (m_last_cmp < 1) {
      if (txns && txns != HAM_KEY_ERASED_IN_TXN)
        return (txns);
      couple_to_txnop();
      update_dupecache(kTxn);
    }
    /* couple to btree */
    else {
      couple_to_btree();
      update_dupecache(kBtree);
    }
    return (0);
  }
}

ham_status_t
Cursor::move_last_key(ham_u32_t flags)
{
  ham_status_t st = 0;
  TransactionCursor *txnc = get_txn_cursor();

  /* move to the very very last key */
  st = move_last_key_singlestep();
  if (st)
    return (st);

  /* check for duplicates. the dupecache was already updated in
   * move_last_key_singlestep() */
  if (m_db->get_rt_flags() & HAM_ENABLE_DUPLICATE_KEYS) {
    /* are there any duplicates? if not then they were all erased and we
     * move to the previous key */
    if (!has_duplicates())
      return (move_previous_key(flags));

    /* otherwise move to the last duplicate */
    return (move_last_dupe());
  }

  /* no duplicates - make sure that we've not coupled to an erased
   * item */
  if (is_coupled_to_txnop()) {
    if (__txn_cursor_is_erase(txnc))
      return (move_previous_key(flags));
    else
      return (0);
  }
  if (is_coupled_to_btree()) {
    st = check_if_btree_key_is_erased_or_overwritten();
    if (st == HAM_KEY_ERASED_IN_TXN)
      return (move_previous_key(flags));
    else if (st == 0) {
      couple_to_txnop();
      return (0);
    }
    else if (st == HAM_KEY_NOT_FOUND)
      return (0);
    else
      return (st);
  }
  else
    return (HAM_KEY_NOT_FOUND);
}

ham_status_t
Cursor::move(ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st = 0;
  bool changed_dir = false;
  TransactionCursor *txnc = get_txn_cursor();
  BtreeCursor *btrc = get_btree_cursor();

  /* no movement requested? directly retrieve key/record */
  if (!flags)
    goto retrieve_key_and_record;

  /* synchronize the btree and transaction cursor if the last operation was
   * not a move next/previous OR if the direction changed */
  if ((m_lastop == HAM_CURSOR_PREVIOUS) && (flags & HAM_CURSOR_NEXT))
    changed_dir = true;
  else if ((m_lastop == HAM_CURSOR_NEXT) && (flags & HAM_CURSOR_PREVIOUS))
    changed_dir = true;
  if (((flags & HAM_CURSOR_NEXT) || (flags & HAM_CURSOR_PREVIOUS))
      && (m_lastop == Cursor::kLookupOrInsert
        || changed_dir)) {
    if (is_coupled_to_txnop())
      set_to_nil(kBtree);
    else
      set_to_nil(kTxn);
    (void)sync(flags, 0);

    if (!txnc->is_nil() && !is_nil(kBtree))
      compare();
  }

  /* we have either skipped duplicates or reached the end of the duplicate
   * list. btree cursor and txn cursor are synced and as close to
   * each other as possible. Move the cursor in the requested direction. */
  if (flags & HAM_CURSOR_NEXT) {
    st = move_next_key(flags);
  }
  else if (flags & HAM_CURSOR_PREVIOUS) {
    st = move_previous_key(flags);
  }
  else if (flags & HAM_CURSOR_FIRST) {
    clear_dupecache();
    st = move_first_key(flags);
  }
  else {
    ham_assert(flags & HAM_CURSOR_LAST);
    clear_dupecache();
    st = move_last_key(flags);
  }

  if (st)
    return (st);

retrieve_key_and_record:
  /* retrieve key/record, if requested */
  if (st == 0) {
    if (is_coupled_to_txnop()) {
#ifdef HAM_DEBUG
      TransactionOperation *op = txnc->get_coupled_op();
      ham_assert(!(op->get_flags() & TransactionOperation::kErase));
#endif
      try {
        if (key)
          txnc->copy_coupled_key(key);
        if (record)
          txnc->copy_coupled_record(record);
      }
      catch (Exception &ex) {
        return (ex.code);
      }
    }
    else {
      st = btrc->move(key, record, 0);
    }
  }

  return (st);
}

bool
Cursor::is_nil(int what)
{
  switch (what) {
    case kBtree:
      return (get_btree_cursor()->get_state() == BtreeCursor::kStateNil);
    case kTxn:
      return (get_txn_cursor()->is_nil());
    default:
      ham_assert(what == 0);
      return (get_btree_cursor()->get_state() == BtreeCursor::kStateNil
                      && get_txn_cursor()->is_nil());
  }
}

void
Cursor::set_to_nil(int what)
{
  switch (what) {
    case kBtree:
      get_btree_cursor()->set_to_nil();
      break;
    case kTxn:
      get_txn_cursor()->set_to_nil();
      couple_to_btree(); /* reset flag */
      break;
    default:
      ham_assert(what == 0);
      get_btree_cursor()->set_to_nil();
      get_txn_cursor()->set_to_nil();
      couple_to_btree(); /* reset flag */
      m_is_first_use = true;
      break;
  }
}

ham_status_t
Cursor::erase(Transaction *txn, ham_u32_t flags)
{
  ham_status_t st;

  /* if transactions are enabled: add a erase-op to the txn-tree */
  if (txn) {
    /* if cursor is coupled to a btree item: set the txn-cursor to
     * nil; otherwise txn_cursor_erase() doesn't know which cursor
     * part is the valid one */
    if (is_coupled_to_btree())
      set_to_nil(kTxn);
    st = get_txn_cursor()->erase();
  }
  else {
    st = get_btree_cursor()->erase(flags);
  }

  if (st == 0)
    set_to_nil(0);

  return (st);
}

int
Cursor::get_record_count(Transaction *txn, ham_u32_t flags)
{
  if (txn) {
    if (m_db->get_rt_flags() & HAM_ENABLE_DUPLICATE_KEYS) {
      bool dummy;
      DupeCache *dc = get_dupecache();

      sync(0, &dummy);
      update_dupecache(kTxn | kBtree);
      return (dc->get_count());
    }
    else {
      /* obviously the key exists, since the cursor is coupled */
      return (1);
    }
  }

  return (get_btree_cursor()->get_record_count(flags));
}

ham_u64_t
Cursor::get_record_size(Transaction *txn)
{
  if (is_coupled_to_txnop())
    return (get_txn_cursor()->get_record_size());
  else
    return (get_btree_cursor()->get_record_size());
}

ham_status_t
Cursor::overwrite(Transaction *htxn, ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st = 0;
  LocalTransaction *txn = dynamic_cast<LocalTransaction *>(htxn);

  /*
   * if we're in transactional mode then just append an "insert/OW" operation
   * to the txn-tree.
   *
   * if the txn_cursor is already coupled to a txn-op, then we can use
   * txn_cursor_overwrite(). Otherwise we have to call db_insert_txn().
   *
   * If transactions are disabled then overwrite the item in the btree.
   */
  if (txn) {
    if (get_txn_cursor()->is_nil() && !(is_nil(0))) {
      get_btree_cursor()->uncouple_from_page();
      st = m_db->insert_txn(txn,
                  get_btree_cursor()->get_uncoupled_key(),
                  record, flags | HAM_OVERWRITE, get_txn_cursor());
    }
    else {
      st = get_txn_cursor()->overwrite(record);
    }

    if (st == 0)
      couple_to_txnop();
  }
  else {
    get_btree_cursor()->overwrite(record, flags);
    couple_to_btree();
  }

  return (st);
}

void
Cursor::close()
{
  get_btree_cursor()->close();
  get_dupecache()->clear();
}

