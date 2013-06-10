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

#include "cursor.h"
#include "db.h"
#include "env.h"
#include "error.h"
#include "mem.h"
#include "btree_cursor.h"
#include "btree_key.h"

using namespace hamsterdb;


static bool
__btree_cursor_is_nil(BtreeCursor *btc)
{
  return (!btc->is_coupled() && !btc->is_uncoupled());
}

ham_status_t
Cursor::update_dupecache(ham_u32_t what)
{
  ham_status_t st = 0;
  Environment *env = m_db->get_env();
  DupeCache *dc = get_dupecache();
  BtreeCursor *btc = get_btree_cursor();
  TransactionCursor *txnc = get_txn_cursor();

  if (!(m_db->get_rt_flags() & HAM_ENABLE_DUPLICATES))
    return (0);

  /* if the cache already exists: no need to continue, it should be
   * up to date */
  if (dc->get_count() != 0)
    return (0);

  if ((what & CURSOR_BTREE) && (what & CURSOR_TXN)) {
    if (is_nil(CURSOR_BTREE) && !is_nil(CURSOR_TXN)) {
      bool equal_keys;
      (void)sync(0, &equal_keys);
      if (!equal_keys)
        set_to_nil(CURSOR_BTREE);
    }
  }

  /* first collect all duplicates from the btree. They're already sorted,
   * therefore we can just append them to our duplicate-cache. */
  if ((what & CURSOR_BTREE) && !is_nil(CURSOR_BTREE)) {
    ham_size_t i;
    bool needs_free = false;
    PDupeTable *table = 0;
    st = btc->get_duplicate_table(&table, &needs_free);
    if (st && st != HAM_CURSOR_IS_NIL)
      return (st);
    st = 0;
    if (table) {
      for (i = 0; i < dupe_table_get_count(table); i++) {
        dc->append(DupeCacheLine(true, i));
      }
      if (needs_free)
        Memory::release(table);
    }
    env->get_changeset().clear();
  }

  /* read duplicates from the txn-cursor? */
  if ((what & CURSOR_TXN) && !is_nil(CURSOR_TXN)) {
    TransactionOperation *op = txnc->get_coupled_op();
    TransactionNode *node = op->get_node();

    if (!node)
      goto bail;

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
        if (op->get_flags() & TransactionOperation::TXN_OP_INSERT) {
          /* all existing dupes are overwritten */
          dc->clear();
          dc->append(DupeCacheLine(false, op));
        }
        else if (op->get_flags() & TransactionOperation::TXN_OP_INSERT_OW) {
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
        else if (op->get_flags() & TransactionOperation::TXN_OP_INSERT_DUP) {
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
        else if (op->get_flags() & TransactionOperation::TXN_OP_ERASE) {
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
          ham_assert(op->get_flags() == TransactionOperation::TXN_OP_NOP);
        }
      }

      /* continue with the previous/older operation */
      op = op->get_next_in_node();
    }
  }

bail:
  return (0);
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
    btc->set_dupe_id((ham_size_t)e->get_btree_dupe_idx());
  }
  else {
    ham_assert(e->get_txn_op() != 0);
    txnc->couple(e->get_txn_op());
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
  if (op->get_flags() & TransactionOperation::TXN_OP_INSERT_DUP)
    st = HAM_KEY_NOT_FOUND;
  get_db()->cursor_close(clone);
  return (st);
}

ham_status_t
Cursor::sync(ham_u32_t flags, bool *equal_keys)
{
  ham_status_t st = 0;
  TransactionCursor *txnc = get_txn_cursor();
  if (equal_keys)
    *equal_keys = false;

  if (is_nil(CURSOR_BTREE)) {
    TransactionNode *node;
    ham_key_t *k;
    if (!txnc->get_coupled_op())
      return (0);
    node = txnc->get_coupled_op()->get_node();
    k = node->get_key();

    if (!(flags & CURSOR_SYNC_ONLY_EQUAL_KEY))
      flags = flags | ((flags & HAM_CURSOR_NEXT)
                ? HAM_FIND_GEQ_MATCH
                : HAM_FIND_LEQ_MATCH);
    /* the flag DONT_LOAD_KEY does not load the key if there's an
     * approx match - it only positions the cursor */
    st = get_btree_cursor()->find(k, 0, CURSOR_SYNC_DONT_LOAD_KEY | flags);
    /* if we had a direct hit instead of an approx. match then
     * set fresh_start to false; otherwise do_local_cursor_move
     * will move the btree cursor again */
    if (st == 0 && equal_keys && !ham_key_get_approximate_match_type(k))
      *equal_keys = true;
  }
  else if (is_nil(CURSOR_TXN)) {
    ham_key_t *k;
    Cursor *clone = get_db()->cursor_clone(this);
    st = clone->get_btree_cursor()->uncouple();
    if (st) {
      get_db()->cursor_close(clone);
      goto bail;
    }
    k = clone->get_btree_cursor()->get_uncoupled_key();
    if (!(flags & CURSOR_SYNC_ONLY_EQUAL_KEY))
      flags = flags | ((flags & HAM_CURSOR_NEXT)
          ? HAM_FIND_GEQ_MATCH
          : HAM_FIND_LEQ_MATCH);
    st = txnc->find(k, CURSOR_SYNC_DONT_LOAD_KEY|flags);
    /* if we had a direct hit instead of an approx. match then
    * set fresh_start to false; otherwise do_local_cursor_move
    * will move the btree cursor again */
    if (st == 0 && equal_keys && !ham_key_get_approximate_match_type(k))
      *equal_keys = true;
    get_db()->cursor_close(clone);
  }

  get_db()->get_env()->get_changeset().clear();

bail:
  return (st);
}

ham_status_t
Cursor::move_next_dupe(void)
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
  return (op ? (op->get_flags() & TransactionOperation::TXN_OP_ERASE) : false);
}

int
Cursor::compare()
{
  int cmp;
  BtreeCursor *btrc = get_btree_cursor();
  TransactionCursor *txnc = get_txn_cursor();

  TransactionNode *node = txnc->get_coupled_op()->get_node();
  ham_key_t *txnk = node->get_key();

  ham_assert(!is_nil(0));
  ham_assert(!txnc->is_nil());

  if (btrc->is_coupled()) {
    /* clone the cursor, then uncouple the clone; get the uncoupled key
     * and discard the clone again */

    /*
     * TODO TODO TODO
     * this is all correct, but of course quite inefficient, because
     *  a) new structures have to be allocated/released
     *  b) uncoupling fetches the whole extended key, which is often
     *    not necessary
     *  -> fix it!
     */
    Cursor *clone = get_db()->cursor_clone(this);
    ham_status_t st = clone->get_btree_cursor()->uncouple();
    if (st) {
      get_db()->cursor_close(clone);
      return (0); /* TODO throw */
    }
    /* TODO error codes are swallowed */
    cmp = get_db()->compare_keys(
                clone->get_btree_cursor()->get_uncoupled_key(),
                txnk);
    get_db()->cursor_close(clone);

    set_lastcmp(cmp);
    return (cmp);
  }
  else if (btrc->is_uncoupled()) {
    /* TODO error codes are swallowed */
    cmp = get_db()->compare_keys(btrc->get_uncoupled_key(), txnk);
    set_lastcmp(cmp);
    return (cmp);
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
  if (get_lastcmp() == 0) {
    if (!is_nil(CURSOR_BTREE)) {
      st = btrc->move(0, 0, HAM_CURSOR_NEXT | HAM_SKIP_DUPLICATES);
      if (st == HAM_KEY_NOT_FOUND || st == HAM_CURSOR_IS_NIL) {
        set_to_nil(CURSOR_BTREE); // TODO muss raus
        if (txnc->is_nil())
          return (HAM_KEY_NOT_FOUND);
        else {
          couple_to_txnop();
          set_lastcmp( + 1);
        }
      }
    }
    if (!txnc->is_nil()) {
      st = txnc->move(HAM_CURSOR_NEXT);
      if (st == HAM_KEY_NOT_FOUND || st==HAM_CURSOR_IS_NIL) {
        set_to_nil(CURSOR_TXN); // TODO muss raus
        if (is_nil(CURSOR_BTREE))
          return (HAM_KEY_NOT_FOUND);
        else {
          couple_to_btree();
          set_lastcmp(-1);

          ham_status_t st2 = check_if_btree_key_is_erased_or_overwritten();
          if (st2 == HAM_TXN_CONFLICT)
            st = st2;
        }
      }
    }
  }
  /* if the btree-key is smaller: move it next */
  else if (get_lastcmp() < 0) {
    st = btrc->move(0, 0, HAM_CURSOR_NEXT | HAM_SKIP_DUPLICATES);
    if (st == HAM_KEY_NOT_FOUND) {
      set_to_nil(CURSOR_BTREE); // TODO Das muss raus!
      if (txnc->is_nil())
        return (st);
      couple_to_txnop();
      set_lastcmp(+1);
    }
    else {
      ham_status_t st2 = check_if_btree_key_is_erased_or_overwritten();
      if (st2 == HAM_TXN_CONFLICT)
        st = st2;
    }
    if (txnc->is_nil())
      set_lastcmp(-1);
  }
  /* if the txn-key is smaller OR if both keys are equal: move next
   * with the txn-key (which is chronologically newer) */
  else {
    st = txnc->move(HAM_CURSOR_NEXT);
    if (st == HAM_KEY_NOT_FOUND) {
      set_to_nil(CURSOR_TXN); // TODO Das muss raus!
      if (is_nil(CURSOR_BTREE))
        return (st);
      couple_to_btree();
      set_lastcmp(-1);
    }
    if (is_nil(CURSOR_BTREE))
      set_lastcmp(+1);
  }

  /* compare keys again */
  if (!is_nil(CURSOR_BTREE) && !txnc->is_nil())
    compare();

  /* if there's a txn conflict: move next */
  if (st == HAM_TXN_CONFLICT)
    return (move_next_key_singlestep());

  /* btree-key is smaller */
  if (get_lastcmp() < 0 || txnc->is_nil()) {
    couple_to_btree();
    update_dupecache(CURSOR_BTREE);
    return (0);
  }
  /* txn-key is smaller */
  else if (get_lastcmp() > 0 || btrc->is_nil()) {
    couple_to_txnop();
    update_dupecache(CURSOR_TXN);
    return (0);
  }
  /* both keys are equal */
  else {
    couple_to_txnop();
    update_dupecache(CURSOR_TXN | CURSOR_BTREE);
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
    if (m_db->get_rt_flags() & HAM_ENABLE_DUPLICATES) {
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
  if (get_lastcmp() == 0) {
    if (!is_nil(CURSOR_BTREE)) {
      st = btrc->move(0, 0, HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES);
      if (st == HAM_KEY_NOT_FOUND || st==HAM_CURSOR_IS_NIL) {
        set_to_nil(CURSOR_BTREE); // TODO muss raus
        if (txnc->is_nil())
          return (HAM_KEY_NOT_FOUND);
        else {
          couple_to_txnop();
          set_lastcmp(-1);
        }
      }
    }
    if (!txnc->is_nil()) {
      st = txnc->move(HAM_CURSOR_PREVIOUS);
      if (st == HAM_KEY_NOT_FOUND || st==HAM_CURSOR_IS_NIL) {
        set_to_nil(CURSOR_TXN); // TODO muss raus
        if (is_nil(CURSOR_BTREE))
          return (HAM_KEY_NOT_FOUND);
        else {
          couple_to_btree();
          set_lastcmp(+1);
        }
      }
    }
  }
  /* if the btree-key is greater: move previous */
  else if (get_lastcmp() > 0) {
    st = btrc->move(0, 0, HAM_CURSOR_PREVIOUS | HAM_SKIP_DUPLICATES);
    if (st == HAM_KEY_NOT_FOUND) {
      set_to_nil(CURSOR_BTREE); // TODO Das muss raus!
      if (txnc->is_nil())
        return (st);
      couple_to_txnop();
      set_lastcmp(-1);
    }
    else {
      ham_status_t st2 = check_if_btree_key_is_erased_or_overwritten();
      if (st2 == HAM_TXN_CONFLICT)
        st = st2;
    }
    if (txnc->is_nil())
      set_lastcmp(+1);
  }
  /* if the txn-key is greater OR if both keys are equal: move previous
   * with the txn-key (which is chronologically newer) */
  else {
    st = txnc->move(HAM_CURSOR_PREVIOUS);
    if (st == HAM_KEY_NOT_FOUND) {
      set_to_nil(CURSOR_TXN); // TODO Das muss raus!
      if (is_nil(CURSOR_BTREE))
        return (st);
      couple_to_btree();
      set_lastcmp(+1);

      ham_status_t st2 = check_if_btree_key_is_erased_or_overwritten();
      if (st2 == HAM_TXN_CONFLICT)
        st = st2;
    }
    if (is_nil(CURSOR_BTREE))
      set_lastcmp(-1);
  }

  /* compare keys again */
  if (!is_nil(CURSOR_BTREE) && !txnc->is_nil())
    compare();

  /* if there's a txn conflict: move previous */
  if (st == HAM_TXN_CONFLICT)
    return (move_previous_key_singlestep());

  /* btree-key is greater */
  if (get_lastcmp() > 0 || txnc->is_nil()) {
    couple_to_btree();
    update_dupecache(CURSOR_BTREE);
    return (0);
  }
  /* txn-key is greater */
  else if (get_lastcmp() < 0 || btrc->is_nil()) {
    couple_to_txnop();
    update_dupecache(CURSOR_TXN);
    return (0);
  }
  /* both keys are equal */
  else {
    couple_to_txnop();
    update_dupecache(CURSOR_TXN | CURSOR_BTREE);
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
  while (!is_nil(CURSOR_BTREE) || !txnc->is_nil()) {
    st = move_previous_key_singlestep();
    if (st)
      return (st);

    /* check for duplicates. the dupecache was already updated in
     * move_previous_key_singlestep() */
    if (m_db->get_rt_flags() & HAM_ENABLE_DUPLICATES) {
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
    update_dupecache(CURSOR_TXN);
    return (0);
  }
  /* if txn-tree is empty but btree is not: couple to btree */
  else if (txns == HAM_KEY_NOT_FOUND && btrs != HAM_KEY_NOT_FOUND) {
    couple_to_btree();
    update_dupecache(CURSOR_BTREE);
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
    if (get_lastcmp() == 0) {
      if (txns && txns != HAM_KEY_ERASED_IN_TXN)
        return (txns);
      couple_to_txnop();
      update_dupecache(CURSOR_BTREE | CURSOR_TXN);
    }
    /* couple to txn */
    else if (get_lastcmp() > 0) {
      if (txns && txns != HAM_KEY_ERASED_IN_TXN)
        return (txns);
      couple_to_txnop();
      update_dupecache(CURSOR_TXN);
    }
    /* couple to btree */
    else {
      couple_to_btree();
      update_dupecache(CURSOR_BTREE);
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
  if (m_db->get_rt_flags() & HAM_ENABLE_DUPLICATES) {
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
    update_dupecache(CURSOR_TXN);
    return (0);
  }
  /* if txn-tree is empty but btree is not: couple to btree */
  else if (txns == HAM_KEY_NOT_FOUND && btrs != HAM_KEY_NOT_FOUND) {
    couple_to_btree();
    update_dupecache(CURSOR_BTREE);
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
    if (get_lastcmp() == 0) {
      if (txns && txns != HAM_KEY_ERASED_IN_TXN)
        return (txns);
      couple_to_txnop();
      update_dupecache(CURSOR_BTREE | CURSOR_TXN);
    }
    /* couple to txn */
    else if (get_lastcmp() < 1) {
      if (txns && txns != HAM_KEY_ERASED_IN_TXN)
        return (txns);
      couple_to_txnop();
      update_dupecache(CURSOR_TXN);
    }
    /* couple to btree */
    else {
      couple_to_btree();
      update_dupecache(CURSOR_BTREE);
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
  if (m_db->get_rt_flags() & HAM_ENABLE_DUPLICATES) {
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
  if ((get_lastop() == HAM_CURSOR_PREVIOUS) && (flags & HAM_CURSOR_NEXT))
    changed_dir = true;
  else if ((get_lastop() == HAM_CURSOR_NEXT) && (flags & HAM_CURSOR_PREVIOUS))
    changed_dir = true;
  if (((flags & HAM_CURSOR_NEXT) || (flags & HAM_CURSOR_PREVIOUS))
      && (get_lastop() == CURSOR_LOOKUP_INSERT
        || changed_dir)) {
    if (is_coupled_to_txnop())
      set_to_nil(CURSOR_BTREE);
    else
      set_to_nil(CURSOR_TXN);
    (void)sync(flags, 0);

    if (!txnc->is_nil() && !is_nil(CURSOR_BTREE))
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
      ham_assert(!(op->get_flags() & TransactionOperation::TXN_OP_ERASE));
#endif
      if (key) {
        st = txnc->get_key(key);
        if (st)
          goto bail;
      }
      if (record) {
        st = txnc->get_record(record);
        if (st)
          goto bail;
      }
    }
    else {
      st = btrc->move(key, record, 0);
    }
  }

bail:
  return (st);
}

Cursor::Cursor(Database *db, Transaction *txn, ham_u32_t flags)
  : m_db(db), m_txn(txn), m_txn_cursor(this), m_btree_cursor(this),
  m_remote_handle(0), m_next(0), m_previous(0), m_next_in_page(0),
  m_previous_in_page(0), m_dupecache_index(0), m_lastop(0), m_lastcmp(0),
  m_flags(flags), m_is_first_use(true)
{
}

Cursor::Cursor(Cursor &other)
  : m_txn_cursor(this, other.get_txn_cursor()), m_btree_cursor(this)
{
  m_db = other.m_db;
  m_txn = other.m_txn;
  m_remote_handle = other.m_remote_handle;
  m_next = other.m_next;
  m_previous = other.m_previous;
  m_next_in_page = other.m_next_in_page;
  m_previous_in_page = other.m_previous_in_page;
  m_dupecache_index = other.m_dupecache_index;
  m_lastop = other.m_lastop;
  m_lastcmp = other.m_lastcmp;
  m_flags = other.m_flags;
  m_is_first_use = other.m_is_first_use;

  set_next_in_page(0);
  set_previous_in_page(0);

  m_btree_cursor.clone(other.get_btree_cursor());

  if (m_db->get_rt_flags() & HAM_ENABLE_DUPLICATES)
    other.get_dupecache()->clone(get_dupecache());
}

bool
Cursor::is_nil(int what)
{
  switch (what) {
    case CURSOR_BTREE:
      return (__btree_cursor_is_nil(get_btree_cursor()));
    case CURSOR_TXN:
      return (get_txn_cursor()->is_nil());
    default:
      ham_assert(what == 0);
      /* TODO btree_cursor_is_nil is different from __btree_cursor_is_nil
       * - refactor and clean up! */
      return (get_btree_cursor()->is_nil());
  }
}

void
Cursor::set_to_nil(int what)
{
  switch (what) {
    case CURSOR_BTREE:
      get_btree_cursor()->set_to_nil();
      break;
    case CURSOR_TXN:
      get_txn_cursor()->set_to_nil();
      couple_to_btree(); /* reset flag */
      break;
    default:
      ham_assert(what == 0);
      get_btree_cursor()->set_to_nil();
      get_txn_cursor()->set_to_nil();
      set_first_use(true);
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
      set_to_nil(CURSOR_TXN);
    st = get_txn_cursor()->erase();
  }
  else {
    st = get_btree_cursor()->erase(flags);
  }

  if (st == 0)
    set_to_nil(0);

  return (st);
}

ham_status_t
Cursor::get_duplicate_count(Transaction *txn, ham_u32_t *pcount,
        ham_u32_t flags)
{
  ham_status_t st = 0;

  *pcount = 0;

  if (txn) {
    if (m_db->get_rt_flags() & HAM_ENABLE_DUPLICATES) {
      bool dummy;
      DupeCache *dc = get_dupecache();

      (void)sync(0, &dummy);
      st = update_dupecache(CURSOR_TXN | CURSOR_BTREE);
      if (st)
        return (st);
      *pcount = dc->get_count();
    }
    else {
      /* obviously the key exists, since the cursor is coupled to
       * a valid item */
      *pcount = 1;
    }
  }
  else {
    st = get_btree_cursor()->get_duplicate_count(pcount, flags);
  }

  return (st);
}

ham_status_t
Cursor::get_record_size(Transaction *txn, ham_u64_t *psize)
{
  ham_status_t st = 0;

  *psize = 0;

  if (txn) {
    if (is_coupled_to_txnop())
      st = get_txn_cursor()->get_record_size(psize);
    else
      st = get_btree_cursor()->get_record_size(psize);
  }
  else
    st = get_btree_cursor()->get_record_size(psize);

  return (st);
}

ham_status_t
Cursor::overwrite(Transaction *txn, ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st = 0;

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
      st = get_btree_cursor()->uncouple();
      if (st == 0)
        st = ((LocalDatabase *)m_db)->insert_txn(txn,
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
    st = get_btree_cursor()->overwrite(record, flags);
    if (st == 0)
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

