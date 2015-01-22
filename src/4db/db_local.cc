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

#include <boost/scope_exit.hpp>

// Always verify that a file of level N does not include headers > N!
#include "1mem/mem.h"
#include "1os/os.h"
#include "2page/page.h"
#include "2device/device.h"
#include "3page_manager/page_manager.h"
#include "3journal/journal.h"
#include "3blob_manager/blob_manager.h"
#include "3btree/btree_index.h"
#include "3btree/btree_index_factory.h"
#include "3btree/btree_cursor.h"
#include "3btree/btree_stats.h"
#include "4db/db_local.h"
#include "4cursor/cursor.h"
#include "4txn/txn_local.h"
#include "4txn/txn_cursor.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

#define FINALIZE_ON_SCOPE_EXIT(db, st, txn)                 \
  LocalDatabase *db__ = db;                                 \
  BOOST_SCOPE_EXIT(db__, st, txn) {                         \
    db__->finalize(st, txn);                                \
  } BOOST_SCOPE_EXIT_END

namespace hamsterdb {

ham_status_t
LocalDatabase::check_insert_conflicts(LocalTransaction *txn,
            TransactionNode *node, ham_key_t *key, uint32_t flags)
{
  TransactionOperation *op = 0;

  /*
   * pick the tree_node of this key, and walk through each operation
   * in reverse chronological order (from newest to oldest):
   * - is this op part of an aborted txn? then skip it
   * - is this op part of a committed txn? then look at the
   *    operation in detail
   * - is this op part of an txn which is still active? return an error
   *    because we've found a conflict
   * - if a committed txn has erased the item then there's no need
   *    to continue checking older, committed txns
   */
  op = node->get_newest_op();
  while (op) {
    LocalTransaction *optxn = op->get_txn();
    if (optxn->is_aborted())
      ; /* nop */
    else if (optxn->is_committed() || txn == optxn) {
      /* if key was erased then it doesn't exist and can be
       * inserted without problems */
      if (op->get_flags() & TransactionOperation::kIsFlushed)
        ; /* nop */
      else if (op->get_flags() & TransactionOperation::kErase)
        return (0);
      /* if the key already exists then we can only continue if
       * we're allowed to overwrite it or to insert a duplicate */
      else if ((op->get_flags() & TransactionOperation::kInsert)
          || (op->get_flags() & TransactionOperation::kInsertOverwrite)
          || (op->get_flags() & TransactionOperation::kInsertDuplicate)) {
        if ((flags & HAM_OVERWRITE) || (flags & HAM_DUPLICATE))
          return (0);
        else
          return (HAM_DUPLICATE_KEY);
      }
      else if (!(op->get_flags() & TransactionOperation::kNop)) {
        ham_assert(!"shouldn't be here");
        return (HAM_DUPLICATE_KEY);
      }
    }
    else { /* txn is still active */
      return (HAM_TXN_CONFLICT);
    }

    op = op->get_previous_in_node();
  }

  /*
   * we've successfully checked all un-flushed transactions and there
   * were no conflicts. Now check all transactions which are already
   * flushed - basically that's identical to a btree lookup.
   *
   * however we can skip this check if we do not care about duplicates.
   */
  if ((flags & HAM_OVERWRITE)
          || (flags & HAM_DUPLICATE)
          || (get_rt_flags() & (HAM_RECORD_NUMBER32 | HAM_RECORD_NUMBER64)))
    return (0);

  ham_status_t st = m_btree_index->find(0, key, 0, 0, 0, flags);
  switch (st) {
    case HAM_KEY_NOT_FOUND:
      return (0);
    case HAM_SUCCESS:
      return (HAM_DUPLICATE_KEY);
    default:
      return (st);
  }
}

ham_status_t
LocalDatabase::check_erase_conflicts(LocalTransaction *txn,
            TransactionNode *node, ham_key_t *key, uint32_t flags)
{
  TransactionOperation *op = 0;

  /*
   * pick the tree_node of this key, and walk through each operation
   * in reverse chronological order (from newest to oldest):
   * - is this op part of an aborted txn? then skip it
   * - is this op part of a committed txn? then look at the
   *    operation in detail
   * - is this op part of an txn which is still active? return an error
   *    because we've found a conflict
   * - if a committed txn has erased the item then there's no need
   *    to continue checking older, committed txns
   */
  op = node->get_newest_op();
  while (op) {
    Transaction *optxn = op->get_txn();
    if (optxn->is_aborted())
      ; /* nop */
    else if (optxn->is_committed() || txn == optxn) {
      if (op->get_flags() & TransactionOperation::kIsFlushed)
        ; /* nop */
      /* if key was erased then it doesn't exist and we fail with
       * an error */
      else if (op->get_flags() & TransactionOperation::kErase)
        return (HAM_KEY_NOT_FOUND);
      /* if the key exists then we're successful */
      else if ((op->get_flags() & TransactionOperation::kInsert)
          || (op->get_flags() & TransactionOperation::kInsertOverwrite)
          || (op->get_flags() & TransactionOperation::kInsertDuplicate)) {
        return (0);
      }
      else if (!(op->get_flags() & TransactionOperation::kNop)) {
        ham_assert(!"shouldn't be here");
        return (HAM_KEY_NOT_FOUND);
      }
    }
    else { /* txn is still active */
      return (HAM_TXN_CONFLICT);
    }

    op = op->get_previous_in_node();
  }

  /*
   * we've successfully checked all un-flushed transactions and there
   * were no conflicts. Now check all transactions which are already
   * flushed - basically that's identical to a btree lookup.
   */
  return (m_btree_index->find(0, key, 0, 0, 0, flags));
}

ham_status_t
LocalDatabase::insert_txn(LocalTransaction *txn, ham_key_t *key,
            ham_record_t *record, uint32_t flags,
            TransactionCursor *cursor)
{
  ham_status_t st = 0;
  TransactionOperation *op;
  bool node_created = false;

  /* get (or create) the node for this key */
  TransactionNode *node = m_txn_index->get(key, 0);
  if (!node) {
    node = new TransactionNode(this, key);
    node_created = true;
    // TODO only store when the operation is successful?
    m_txn_index->store(node);
  }

  // check for conflicts of this key
  //
  // !!
  // afterwards, clear the changeset; check_insert_conflicts()
  // checks if a key already exists, and this fills the changeset
  st = check_insert_conflicts(txn, node, key, flags);
  if (st) {
    if (node_created) {
      m_txn_index->remove(node);
      delete node;
    }
    return (st);
  }

  // append a new operation to this node
  op = node->append(txn, flags,
                (flags & HAM_PARTIAL) |
                ((flags & HAM_DUPLICATE)
                    ? TransactionOperation::kInsertDuplicate
                    : (flags & HAM_OVERWRITE)
                        ? TransactionOperation::kInsertOverwrite
                        : TransactionOperation::kInsert),
                get_local_env()->get_incremented_lsn(), key, record);

  // if there's a cursor then couple it to the op; also store the
  // dupecache-index in the op (it's needed for DUPLICATE_INSERT_BEFORE/NEXT) */
  if (cursor) {
    Cursor *c = cursor->get_parent();
    if (c->get_dupecache_index())
      op->set_referenced_dupe(c->get_dupecache_index());

    cursor->couple_to_op(op);

    // all other cursors need to increment their dupe index, if their
    // index is > this cursor's index
    increment_dupe_index(node, c, c->get_dupecache_index());
  }

  // append journal entry
  if (m_env->get_flags() & HAM_ENABLE_RECOVERY
      && m_env->get_flags() & HAM_ENABLE_TRANSACTIONS) {
    Journal *j = get_local_env()->get_journal();
    j->append_insert(this, txn, key, record,
              flags & HAM_DUPLICATE ? flags : flags | HAM_OVERWRITE,
              op->get_lsn());
  }

  ham_assert(st == 0);
  return (0);
}

ham_status_t
LocalDatabase::find_txn(Cursor *cursor, LocalTransaction *txn, ham_key_t *key,
                ham_record_t *record, uint32_t flags)
{
  ham_status_t st = 0;
  TransactionOperation *op = 0;
  bool first_loop = true;
  bool exact_is_erased = false;

  ByteArray *key_arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
                ? &get_key_arena()
                : &txn->get_key_arena();
  ByteArray *record_arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
                ? &get_record_arena()
                : &txn->get_record_arena();

  ham_key_set_intflags(key,
        (ham_key_get_intflags(key) & (~BtreeKey::kApproximate)));

  /* get the node for this key (but don't create a new one if it does
   * not yet exist) */
  TransactionNode *node = m_txn_index->get(key, flags);

  /*
   * pick the node of this key, and walk through each operation
   * in reverse chronological order (from newest to oldest):
   * - is this op part of an aborted txn? then skip it
   * - is this op part of a committed txn? then look at the
   *    operation in detail
   * - is this op part of an txn which is still active? return an error
   *    because we've found a conflict
   * - if a committed txn has erased the item then there's no need
   *    to continue checking older, committed txns
   */
retry:
  if (node)
    op = node->get_newest_op();
  while (op) {
    Transaction *optxn = op->get_txn();
    if (optxn->is_aborted())
      ; /* nop */
    else if (optxn->is_committed() || txn == optxn) {
      if (op->get_flags() & TransactionOperation::kIsFlushed)
        ; /* nop */
      /* if key was erased then it doesn't exist and we can return
       * immediately
       *
       * if an approximate match is requested then move to the next
       * or previous node
       */
      else if (op->get_flags() & TransactionOperation::kErase) {
        if (first_loop
            && !(ham_key_get_intflags(key) & BtreeKey::kApproximate))
          exact_is_erased = true;
        first_loop = false;
        if (flags & HAM_FIND_LT_MATCH) {
          node = node->get_previous_sibling();
          if (!node)
            break;
          ham_key_set_intflags(key,
              (ham_key_get_intflags(key) | BtreeKey::kApproximate));
          goto retry;
        }
        else if (flags & HAM_FIND_GT_MATCH) {
          node = node->get_next_sibling();
          if (!node)
            break;
          ham_key_set_intflags(key,
              (ham_key_get_intflags(key) | BtreeKey::kApproximate));
          goto retry;
        }
        /* if a duplicate was deleted then check if there are other duplicates
         * left */
        st = HAM_KEY_NOT_FOUND;
        // TODO merge both calls
        if (cursor) {
          cursor->get_txn_cursor()->couple_to_op(op);
          cursor->couple_to_txnop();
        }
      }
    }

    /* set a flag that the cursor just completed an Insert-or-find
     * operation; this information is needed in ham_cursor_move */
    cursor->set_lastop(Cursor::kLookupOrInsert);
  }
  else {
    if (cursor->is_coupled_to_txnop() && record)
      cursor->get_txn_cursor()->copy_coupled_record(record);
  }

ham_status_t
LocalDatabase::erase(Cursor *cursor, Transaction *txn, ham_key_t *key,
                uint32_t flags)
{
  ham_status_t st = 0;
  LocalTransaction *local_txn = 0;

  if (cursor) {
    if (cursor->is_nil())
      throw Exception(HAM_CURSOR_IS_NIL);
    if (cursor->is_coupled_to_txnop()) // TODO rewrite the next line, it's ugly
      key = cursor->get_txn_cursor()->get_coupled_op()->get_node()->get_key();
    else // cursor->is_coupled_to_btree()
      key = 0;
  }

  if (key) {
    if (m_config.key_size != HAM_KEY_SIZE_UNLIMITED
        && key->size != m_config.key_size) {
      ham_trace(("invalid key size (%u instead of %u)",
            key->size, m_config.key_size));
      return (HAM_INV_KEY_SIZE);
    }
  }

  if (!txn && (get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    local_txn = (LocalTransaction *)get_local_env()->get_txn_manager()->begin(
                        0, HAM_TXN_TEMPORARY);
    txn = local_txn;
  }

  FINALIZE_ON_SCOPE_EXIT(this, st, local_txn);

  return (erase_impl(cursor, txn, key, flags));
}

ham_status_t
LocalDatabase::find(Cursor *cursor, Transaction *txn, ham_key_t *key,
            ham_record_t *record, uint32_t flags)
{
  ham_status_t st = 0;
  LocalTransaction *local_txn = 0;

  /* Duplicates AND Transactions require a Cursor because only
   * Cursors can build lists of duplicates.
   * TODO not exception safe - if find() throws then the cursor is not closed
   */
  if (!cursor && txn && get_rt_flags() & HAM_ENABLE_DUPLICATE_KEYS) {
    Cursor *c = cursor_create(txn, 0);
    st = find(c, txn, key, record, flags);
    cursor_close(c);
    return (st);
  }

  if (m_config.key_size != HAM_KEY_SIZE_UNLIMITED
      && key->size != m_config.key_size) {
    ham_trace(("invalid key size (%u instead of %u)",
          key->size, m_config.key_size));
    return (HAM_INV_KEY_SIZE);
  }

  // cursor: reset the dupecache, set to nil
  // TODO merge both calls, only set to nil if find() was successful
  if (cursor) {
    cursor->clear_dupecache();
    cursor->set_to_nil(Cursor::kBoth);
  }

  // create a temporary transaction, if necessary
  if (!txn && (get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    local_txn = (LocalTransaction *)get_local_env()->get_txn_manager()->begin(
                        0, HAM_TXN_TEMPORARY);
    txn = local_txn;
  }

  FINALIZE_ON_SCOPE_EXIT(this, st, local_txn);

  st = find_impl(cursor, (LocalTransaction *)txn, key, record, flags);
>>>>>>> Merging Database::erase and Database::cursor_erase
  if (st)
    return (st);

  /* approximate matching? then also copy the key */
  if (ham_key_get_intflags(key) & BtreeKey::kApproximate) {
    if (cursor->is_coupled_to_txnop())
      cursor->get_txn_cursor()->copy_coupled_key(key);
  }
#endif
=======
  return (st);
}

Cursor *
LocalDatabase::cursor_create_impl(Transaction *txn, uint32_t flags)
{
  return (new Cursor(this, txn, flags));
}

Cursor *
LocalDatabase::cursor_clone_impl(Cursor *src)
{
  return (new Cursor(*src));
}

ham_status_t
LocalDatabase::cursor_insert(Cursor *cursor, ham_key_t *key,
        ham_record_t *record, uint32_t flags)
{
  return (insert_impl(cursor, cursor->get_txn(), key, record, flags));
>>>>>>> Merging Database::find and Database::cursor_find
}

uint32_t
LocalDatabase::cursor_get_record_count(Cursor *cursor, uint32_t flags)
{
  return (cursor->get_record_count(flags));
}

uint32_t
LocalDatabase::cursor_get_duplicate_position(Cursor *cursor)
{
  return (cursor->get_duplicate_position());
}

uint64_t
LocalDatabase::cursor_get_record_size(Cursor *cursor)
{
  return (cursor->get_record_size());
}

ham_status_t
LocalDatabase::cursor_overwrite(Cursor *cursor,
                ham_record_t *record, uint32_t flags)
{
  ham_status_t st = 0;
  Transaction *local_txn = 0;

  /* purge cache if necessary */
  get_local_env()->get_page_manager()->purge_cache();

  /* if user did not specify a transaction, but transactions are enabled:
   * create a temporary one */
  if (!cursor->get_txn() && (get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    local_txn = get_local_env()->get_txn_manager()->begin(0, HAM_TXN_TEMPORARY);
  }

  FINALIZE_ON_SCOPE_EXIT(this, st, local_txn);

  /* this function will do all the work */
  st = cursor->overwrite(cursor->get_txn()
                                  ? cursor->get_txn()
                                  : local_txn,
                                record, flags);

  return (st);
}

ham_status_t
LocalDatabase::cursor_move(Cursor *cursor, ham_key_t *key,
                ham_record_t *record, uint32_t flags)
{
  /* purge cache if necessary */
  get_local_env()->get_page_manager()->purge_cache();

  /*
   * if the cursor was never used before and the user requests a NEXT then
   * move the cursor to FIRST; if the user requests a PREVIOUS we set it
   * to LAST, resp.
   *
   * if the cursor was already used but is nil then we've reached EOF,
   * and a NEXT actually tries to move to the LAST key (and PREVIOUS
   * moves to FIRST)
   *
   * TODO the btree-cursor has identical code which can be removed
   */
  if (cursor->is_nil(0)) {
    if (flags & HAM_CURSOR_NEXT) {
      flags &= ~HAM_CURSOR_NEXT;
      if (cursor->is_first_use())
        flags |= HAM_CURSOR_FIRST;
      else
        flags |= HAM_CURSOR_LAST;
    }
    else if (flags & HAM_CURSOR_PREVIOUS) {
      flags &= ~HAM_CURSOR_PREVIOUS;
      if (cursor->is_first_use())
        flags |= HAM_CURSOR_LAST;
      else
        flags |= HAM_CURSOR_FIRST;
    }
  }

  ham_status_t st = 0;

  /* in non-transactional mode - just call the btree function and return */
  if (!(get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    return (cursor->get_btree_cursor()->move(key, &get_key_arena(), record,
                    &get_record_arena(), flags));
  }

  /* everything else is handled by the cursor function */
  st = cursor->move(key, record, flags);

  /* store the direction */
  if (flags & HAM_CURSOR_NEXT)
    cursor->set_lastop(HAM_CURSOR_NEXT);
  else if (flags & HAM_CURSOR_PREVIOUS)
    cursor->set_lastop(HAM_CURSOR_PREVIOUS);
  else
    cursor->set_lastop(0);

  if (st) {
    if (st == HAM_KEY_ERASED_IN_TXN)
      st = HAM_KEY_NOT_FOUND;
    /* trigger a sync when the function is called again */
    cursor->set_lastop(0);
    return (st);
  }

  return (0);
}

void
LocalDatabase::cursor_close_impl(Cursor *cursor)
{
  cursor->close();
}

ham_status_t
LocalDatabase::close_impl(uint32_t flags)
{
  /* check if this database is modified by an active transaction */
  if (m_txn_index) {
    TransactionNode *node = m_txn_index->get_first();
    while (node) {
      TransactionOperation *op = node->get_newest_op();
      while (op) {
        Transaction *optxn = op->get_txn();
        if (!optxn->is_committed() && !optxn->is_aborted()) {
          ham_trace(("cannot close a Database that is modified by "
                 "a currently active Transaction"));
          return (set_error(HAM_TXN_STILL_OPEN));
        }
        op = op->get_previous_in_node();
      }
      node = node->get_next_sibling();
    }
  }

  /* flush all committed transactions */
  if (get_local_env()->get_txn_manager())
    get_local_env()->get_txn_manager()->flush_committed_txns();

  /* in-memory-database: free all allocated blobs */
  if (m_btree_index && m_env->get_flags() & HAM_IN_MEMORY)
   m_btree_index->release();

  /*
   * flush all pages of this database (but not the header page,
   * it's still required and will be flushed below)
   */
  get_local_env()->get_page_manager()->close_database(this);

  return (0);
}

void 
LocalDatabase::increment_dupe_index(TransactionNode *node,
                Cursor *skip, uint32_t start)
{
  Cursor *c = m_cursor_list;

  while (c) {
    bool hit = false;

    if (c == skip || c->is_nil(0))
      goto next;

    /* if cursor is coupled to an op in the same node: increment
     * duplicate index (if required) */
    if (c->is_coupled_to_txnop()) {
      TransactionCursor *txnc = c->get_txn_cursor();
      TransactionNode *n = txnc->get_coupled_op()->get_node();
      if (n == node)
        hit = true;
    }
    /* if cursor is coupled to the same key in the btree: increment
     * duplicate index (if required) */
    else if (c->get_btree_cursor()->points_to(node->get_key())) {
      hit = true;
    }

    if (hit) {
      if (c->get_dupecache_index() > start)
        c->set_dupecache_index(c->get_dupecache_index() + 1);
    }

next:
    c = c->get_next();
  }
}

void
LocalDatabase::nil_all_cursors_in_node(LocalTransaction *txn, Cursor *current,
                TransactionNode *node)
{
  TransactionOperation *op = node->get_newest_op();
  while (op) {
    TransactionCursor *cursor = op->get_cursor_list();
    while (cursor) {
      Cursor *parent = cursor->get_parent();
      // is the current cursor to a duplicate? then adjust the
      // coupled duplicate index of all cursors which point to a duplicate
      if (current) {
        if (current->get_dupecache_index()) {
          if (current->get_dupecache_index() < parent->get_dupecache_index()) {
            parent->set_dupecache_index(parent->get_dupecache_index() - 1);
            cursor = cursor->get_coupled_next();
            continue;
          }
          else if (current->get_dupecache_index() > parent->get_dupecache_index()) {
            cursor = cursor->get_coupled_next();
            continue;
          }
          // else fall through
        }
      }
      parent->couple_to_btree(); // TODO merge these two lines
      parent->set_to_nil(Cursor::kTxn);
      // set a flag that the cursor just completed an Insert-or-find
      // operation; this information is needed in ham_cursor_move
      // (in this aspect, an erase is the same as insert/find)
      parent->set_lastop(Cursor::kLookupOrInsert);

      cursor = op->get_cursor_list();
    }

    op = op->get_previous_in_node();
  }
}

ham_status_t
LocalDatabase::copy_record(LocalDatabase *db, Transaction *txn,
                TransactionOperation *op, ham_record_t *record)
{
  ByteArray *arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
            ? &db->get_record_arena()
            : &txn->get_record_arena();

  if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
    arena->resize(op->get_record()->size);
    record->data = arena->get_ptr();
  }
  memcpy(record->data, op->get_record()->data, op->get_record()->size);
  record->size = op->get_record()->size;
  return (0);
}

void
LocalDatabase::nil_all_cursors_in_btree(Cursor *current, ham_key_t *key)
{
  Cursor *c = m_cursor_list;

  /* foreach cursor in this database:
   *  if it's nil or coupled to the txn: skip it
   *  if it's coupled to btree AND uncoupled: compare keys; set to nil
   *    if keys are identical
   *  if it's uncoupled to btree AND coupled: compare keys; set to nil
   *    if keys are identical; (TODO - improve performance by nil'ling
   *    all other cursors from the same btree page)
   *
   *  do NOT nil the current cursor - it's coupled to the key, and the
   *  coupled key is still needed by the caller
   */
  while (c) {
    if (c->is_nil(0) || c == current)
      goto next;
    if (c->is_coupled_to_txnop())
      goto next;

    if (c->get_btree_cursor()->points_to(key)) {
      /* is the current cursor to a duplicate? then adjust the
       * coupled duplicate index of all cursors which point to a
       * duplicate */
      if (current) {
        if (current->get_dupecache_index()) {
          if (current->get_dupecache_index() < c->get_dupecache_index()) {
            c->set_dupecache_index(c->get_dupecache_index() - 1);
            goto next;
          }
          else if (current->get_dupecache_index() > c->get_dupecache_index()) {
            goto next;
          }
          /* else fall through */
        }
      }
      c->set_to_nil(0);
    }
next:
    c = c->get_next();
  }
}

ham_status_t
LocalDatabase::flush_txn_operation(LocalTransaction *txn,
                TransactionOperation *op)
{
  ham_status_t st = 0;
  TransactionNode *node = op->get_node();

  /*
   * depending on the type of the operation: actually perform the
   * operation on the btree
   *
   * if the txn-op has a cursor attached, then all (txn)cursors
   * which are coupled to this op have to be uncoupled, and their
   * parent (btree) cursor must be coupled to the btree item instead.
   */
  if ((op->get_flags() & TransactionOperation::kInsert)
      || (op->get_flags() & TransactionOperation::kInsertOverwrite)
      || (op->get_flags() & TransactionOperation::kInsertDuplicate)) {
    uint32_t additional_flag = 
      (op->get_flags() & TransactionOperation::kInsertDuplicate)
          ? HAM_DUPLICATE
          : HAM_OVERWRITE;
    if (!op->get_cursor_list()) {
      st = m_btree_index->insert(0, node->get_key(), op->get_record(),
                  op->get_orig_flags() | additional_flag);
    }
    else {
      TransactionCursor *tc2, *tc1 = op->get_cursor_list();
      Cursor *c2, *c1 = tc1->get_parent();
      /* pick the first cursor, get the parent/btree cursor and
       * insert the key/record pair in the btree. The btree cursor
       * then will be coupled to this item. */
      st = m_btree_index->insert(c1, node->get_key(), op->get_record(),
                  op->get_orig_flags() | additional_flag);
      if (!st) {
        /* uncouple the cursor from the txn-op, and remove it */
        c1->couple_to_btree(); // TODO merge these two calls
        c1->set_to_nil(Cursor::kTxn);

        /* all other (btree) cursors need to be coupled to the same
         * item as the first one. */
        while ((tc2 = op->get_cursor_list())) {
          c2 = tc2->get_parent();
          c2->get_btree_cursor()->clone(c1->get_btree_cursor());
          c2->couple_to_btree(); // TODO merge these two calls
          c2->set_to_nil(Cursor::kTxn);
        }
      }
    }
  }
  else if (op->get_flags() & TransactionOperation::kErase) {
    st = m_btree_index->erase(0, node->get_key(),
                  op->get_referenced_dupe(), op->get_flags());
    if (st == HAM_KEY_NOT_FOUND)
      st = 0;
  }

  return (st);
}

void
LocalDatabase::erase_me()
{
  m_btree_index->release();
}

ham_status_t
LocalDatabase::insert_impl(Cursor *cursor, Transaction *htxn, ham_key_t *key,
            ham_record_t *record, uint32_t flags)
{
  ham_status_t st = 0;
  LocalTransaction *local_txn = 0;

  LocalTransaction *txn = dynamic_cast<LocalTransaction *>(htxn);

  if (m_config.flags & (HAM_RECORD_NUMBER32 | HAM_RECORD_NUMBER64)) {
    if (key->size == 0 && key->data == 0) {
      // ok!
    }
    else if (key->size == 0 && key->data != 0) {
      ham_trace(("for record number keys set key size to 0, "
                             "key->data to null"));
      return (HAM_INV_PARAMETER);
    }
    else if (key->size != m_config.key_size) {
      ham_trace(("invalid key size (%u instead of %u)",
            key->size, m_config.key_size));
      return (HAM_INV_KEY_SIZE);
    }
  }
  else if (m_config.key_size != HAM_KEY_SIZE_UNLIMITED
      && key->size != m_config.key_size) {
    ham_trace(("invalid key size (%u instead of %u)",
          key->size, m_config.key_size));
    return (HAM_INV_KEY_SIZE);
  }
  if (m_config.record_size != HAM_RECORD_SIZE_UNLIMITED
      && record->size != m_config.record_size) {
    ham_trace(("invalid record size (%u instead of %u)",
          record->size, m_config.record_size));
    return (HAM_INV_RECORD_SIZE);
  }

  ByteArray *arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
            ? &get_key_arena()
            : &txn->get_key_arena();

  /*
   * record number: make sure that we have a valid key structure,
   * and lazy load the last used record number
   *
   * TODO TODO
   * too much duplicated code
   */
  uint64_t recno = 0;
  if (get_rt_flags() & HAM_RECORD_NUMBER64) {
    if (flags & HAM_OVERWRITE) {
      ham_assert(key->size == sizeof(uint64_t));
      ham_assert(key->data != 0);
      recno = *(uint64_t *)key->data;
    }
    else {
      /* get the record number and increment it */
      recno = get_incremented_recno();
    }

    /* allocate memory for the key */
    if (!key->data) {
      arena->resize(sizeof(uint64_t));
      key->data = arena->get_ptr();
    }
    key->size = sizeof(uint64_t);
    *(uint64_t *)key->data = recno;

    /* A recno key is always appended sequentially */
    flags |= HAM_HINT_APPEND;
  }
  else if (get_rt_flags() & HAM_RECORD_NUMBER32) {
    if (flags & HAM_OVERWRITE) {
      ham_assert(key->size == sizeof(uint32_t));
      ham_assert(key->data != 0);
      recno = *(uint32_t *)key->data;
    }
    else {
      /* get the record number and increment it */
      recno = get_incremented_recno();
    }

    /* allocate memory for the key */
    if (!key->data) {
      arena->resize(sizeof(uint32_t));
      key->data = arena->get_ptr();
    }
    key->size = sizeof(uint32_t);
    *(uint32_t *)key->data = recno;

    /* A recno key is always appended sequentially */
    flags |= HAM_HINT_APPEND;
  }

  /* purge cache if necessary */
  get_local_env()->get_page_manager()->purge_cache();

  if (!txn && (get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    local_txn = (LocalTransaction *)get_local_env()->get_txn_manager()->begin(
                        0, HAM_TXN_TEMPORARY);
    txn = local_txn;
  }

  FINALIZE_ON_SCOPE_EXIT(this, st, local_txn);

  /*
   * if transactions are enabled: only insert the key/record pair into
   * the Transaction structure. Otherwise immediately write to the btree.
   */
  if (txn)
    st = insert_txn(txn, key, record, flags, cursor
                                                ? cursor->get_txn_cursor()
                                                : 0);
  else
    st = m_btree_index->insert(cursor, key, record, flags);

  // couple the cursor to the inserted key
  if (st == 0 && cursor) {
    if (m_env->get_flags() & HAM_ENABLE_TRANSACTIONS) {
      DupeCache *dc = cursor->get_dupecache();
      // TODO required? should have happened in insert_txn
      cursor->couple_to_txnop();
      /* the cursor is coupled to the txn-op; nil the btree-cursor to
       * trigger a sync() call when fetching the duplicates */
      // TODO merge with the line above
      cursor->set_to_nil(Cursor::kBtree);

      /* reset the dupecache, otherwise cursor->get_dupecache_count()
       * does not update the dupecache correctly */
      dc->clear();
      
      /* if duplicate keys are enabled: set the duplicate index of
       * the new key  */
      if (st == 0 && cursor->get_dupecache_count()) {
        TransactionOperation *op = cursor->get_txn_cursor()->get_coupled_op();
        ham_assert(op != 0);

        for (uint32_t i = 0; i < dc->get_count(); i++) {
          DupeCacheLine *l = dc->get_element(i);
          if (!l->use_btree() && l->get_txn_op() == op) {
            cursor->set_dupecache_index(i + 1);
            break;
          }
        }
      }
    }
    else {
      // TODO required? should have happened in BtreeInsertAction
      cursor->couple_to_btree();
    }

    /* set a flag that the cursor just completed an Insert-or-find
     * operation; this information is needed in ham_cursor_move */
    cursor->set_lastop(Cursor::kLookupOrInsert);
  }

  return (st);
}

ham_status_t
LocalDatabase::find_impl(Cursor *cursor, LocalTransaction *txn, ham_key_t *key,
            ham_record_t *record, uint32_t flags)
{
  /* purge cache if necessary */
  get_local_env()->get_page_manager()->purge_cache();

  /*
   * if transactions are enabled: read keys from transaction trees,
   * otherwise read immediately from disk
   */
  if (txn)
    return (find_txn(cursor, txn, key, record, flags));

  ByteArray *key_arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
                ? &get_key_arena()
                : &txn->get_key_arena();
  ByteArray *rec_arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
                ? &get_record_arena()
                : &txn->get_record_arena();
  return (m_btree_index->find(cursor, key, key_arena, record,
                          rec_arena, flags));
}

ham_status_t
LocalDatabase::erase_impl(Cursor *cursor, Transaction *htxn, ham_key_t *key,
                uint32_t flags)
{
  ham_status_t st = 0;

  LocalTransaction *txn = dynamic_cast<LocalTransaction *>(htxn);

  /*
   * if transactions are enabled: append a 'erase key' operation into
   * the txn tree; otherwise immediately erase the key from disk
   */
  if (txn == 0) {
    st = m_btree_index->erase(cursor, key, 0, flags);
  }
  else {
    if (cursor) {
      /*
       * !!
       * we have two cases:
       *
       * 1. the cursor is coupled to a btree item (or uncoupled, but not nil)
       *    and the txn_cursor is nil; in that case, we have to
       *    - uncouple the btree cursor
       *    - insert the erase-op for the key which is used by the btree cursor
       *
       * 2. the cursor is coupled to a txn-op; in this case, we have to
       *    - insert the erase-op for the key which is used by the txn-op
       *
       * TODO clean up this whole mess. code should be like
       *
       *   if (txn)
       *     erase_txn(txn, cursor->get_key(), 0, cursor->get_txn_cursor());
       */
      /* case 1 described above */
      if (cursor->is_coupled_to_btree()) {
        cursor->set_to_nil(Cursor::kTxn);
        cursor->get_btree_cursor()->uncouple_from_page();
        st = erase_txn(txn, cursor->get_btree_cursor()->get_uncoupled_key(),
                        0, cursor->get_txn_cursor());
      }
      /* case 2 described above */
      else {
        // TODO this line is ugly
        st = erase_txn(txn, cursor->get_txn_cursor()->get_coupled_op()->get_key(),
                        0, cursor->get_txn_cursor());
      }
    }
    else {
      st = erase_txn(txn, key, flags, 0);
    }
  }

  /* on success: verify that cursor is now nil */
  if (cursor && st == 0) {
    cursor->set_to_nil(0);
    cursor->couple_to_btree(); // TODO why?
    ham_assert(cursor->get_txn_cursor()->is_nil());
    ham_assert(cursor->is_nil(0));
    cursor->clear_dupecache(); // TODO merge with set_to_nil()
  }

  return (st);
}

ham_status_t
LocalDatabase::finalize(ham_status_t status, Transaction *local_txn)
{
  LocalEnvironment *env = get_local_env();

  if (status) {
    if (local_txn) {
      env->get_changeset()->clear();
      env->get_txn_manager()->abort(local_txn);
    }
    return (status);
  }

  if (local_txn) {
    env->get_changeset()->clear();
    env->get_txn_manager()->commit(local_txn);
  }
  else if (env->get_flags() & HAM_ENABLE_RECOVERY
      && !(env->get_flags() & HAM_ENABLE_TRANSACTIONS)) {
    env->get_changeset()->flush(env->get_incremented_lsn());
  }
  return (0);
}

ham_status_t
LocalDatabase::find_impl(Cursor *cursor, Transaction *htxn, ham_key_t *key,
            ham_record_t *record, uint32_t flags)
{
  ham_status_t st;
  LocalTransaction *local_txn = 0;
  LocalTransaction *txn = dynamic_cast<LocalTransaction *>(htxn);

  if (m_config.key_size != HAM_KEY_SIZE_UNLIMITED
      && key->size != m_config.key_size) {
    ham_trace(("invalid key size (%u instead of %u)",
          key->size, m_config.key_size));
    return (HAM_INV_KEY_SIZE);
  }

  /* purge cache if necessary */
  get_local_env()->get_page_manager()->purge_cache();

  if (!txn && (get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    local_txn = (LocalTransaction *)get_local_env()->get_txn_manager()->begin(
                        0, HAM_TXN_TEMPORARY);
    if (cursor)
      cursor->set_txn(local_txn);
    txn = local_txn;
  }

#if 0
  /* if this database has duplicates, then we use ham_cursor_find
   * because we have to build a duplicate list, and this is currently
   * only available in ham_cursor_find
   *
   * TODO create cursor on the stack and avoid the memory allocation!
   * TODO or move this to 5hamsterdb/hamsterdb.cc?
   */
  if (txn && get_rt_flags() & HAM_ENABLE_DUPLICATE_KEYS) {
    Cursor *c;
    st = ham_cursor_create((ham_cursor_t **)&c, (ham_db_t *)this,
            (ham_txn_t *)txn, HAM_DONT_LOCK);
    if (st)
      return (st);
    st = ham_cursor_find((ham_cursor_t *)c, key, record, flags | HAM_DONT_LOCK);
    cursor_close(c);
    get_local_env()->get_changeset().clear();
    return (st);
  }
#endif

  /*
   * if transactions are enabled: read keys from transaction trees,
   * otherwise read immediately from disk
   */
  if (txn)
    st = find_txn(cursor, txn, key, record, flags);
  else
    st = m_btree_index->find(0, cursor, key, record, flags);

  if (local_txn) {
    get_local_env()->get_txn_manager()->abort(local_txn);
    if (cursor)
      cursor->set_txn(0);
  }
  get_local_env()->get_changeset().clear();

  return (st);
}

ham_status_t
LocalDatabase::erase_impl(Cursor *cursor, Transaction *htxn, ham_key_t *key,
                uint32_t flags)
{
  LocalTransaction *local_txn = 0;
  LocalTransaction *txn = dynamic_cast<LocalTransaction *>(htxn);

  if (key) {
    if (m_config.key_size != HAM_KEY_SIZE_UNLIMITED
        && key->size != m_config.key_size) {
      ham_trace(("invalid key size (%u instead of %u)",
            key->size, m_config.key_size));
      return (HAM_INV_KEY_SIZE);
    }

    /* record number: make sure that we have a valid key structure */
    if (get_rt_flags() & HAM_RECORD_NUMBER) {
      if (key->size != sizeof(uint64_t) || !key->data) {
        ham_trace(("key->size must be 8, key->data must not be NULL"));
        return (HAM_INV_PARAMETER);
      }
    }
  }

  if (!txn && (get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    local_txn = (LocalTransaction *)get_local_env()->get_txn_manager()->begin(
                        0, HAM_TXN_TEMPORARY);
    if (cursor)
      cursor->set_txn(local_txn);
    txn = local_txn;
  }

  /*
   * if transactions are enabled: append a 'erase key' operation into
   * the txn tree; otherwise immediately erase the key from disk
   */
  ham_status_t st;
  if (txn == 0) {
    st = m_btree_index->erase(txn, cursor, key, 0, flags);
  }
  else {
    if (cursor) {
      /*
       * !!
       * we have two cases:
       *
       * 1. the cursor is coupled to a btree item (or uncoupled, but not nil)
       *    and the txn_cursor is nil; in that case, we have to
       *    - uncouple the btree cursor
       *    - insert the erase-op for the key which is used by the btree cursor
       *
       * 2. the cursor is coupled to a txn-op; in this case, we have to
       *    - insert the erase-op for the key which is used by the txn-op
       *
       * TODO clean up this whole mess. code should be like
       *
       *   if (txn)
       *     erase_txn(txn, cursor->get_key(), 0, cursor->get_txn_cursor());
       */
      /* case 1 described above */
      if (cursor->is_coupled_to_btree()) {
        cursor->set_to_nil(Cursor::kTxn);
        cursor->get_btree_cursor()->uncouple_from_page();
        st = erase_txn(txn, cursor->get_btree_cursor()->get_uncoupled_key(),
                        0, cursor->get_txn_cursor());
      }
      /* case 2 described above */
      else {
        st = erase_txn(txn, cursor->get_txn_cursor()->get_coupled_op()->get_node()->get_key(),
                        0, cursor->get_txn_cursor());
      }

      if (local_txn)
        cursor->set_txn(0);
    }
    else {
      st = erase_txn(txn, key, flags, 0);
    }
  }

  /* on success: verify that cursor is now nil */
  if (cursor && st == 0) {
    cursor->set_to_nil(0);
    cursor->couple_to_btree(); // TODO why?
    ham_assert(cursor->get_txn_cursor()->is_nil());
    ham_assert(cursor->is_nil(0));
    cursor->clear_dupecache(); // TODO merge with set_to_nil()
  }

  if (st) {
    if (local_txn)
      get_local_env()->get_txn_manager()->abort(local_txn);

    get_local_env()->get_changeset().clear();
    return (st);
  }

  if (local_txn)
    get_local_env()->get_txn_manager()->commit(local_txn);
  else if (m_env->get_flags() & HAM_ENABLE_RECOVERY
      && !(m_env->get_flags() & HAM_ENABLE_TRANSACTIONS))
    get_local_env()->get_changeset().flush();

  return (0);
}

} // namespace hamsterdb
