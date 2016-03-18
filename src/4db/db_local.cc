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
#include "1globals/callbacks.h"
#include "3page_manager/page_manager.h"
#include "3journal/journal.h"
#include "3blob_manager/blob_manager.h"
#include "3btree/btree_index.h"
#include "3btree/btree_index_factory.h"
#include "4db/db_local.h"
#include "4context/context.h"
#include "4cursor/cursor_local.h"
#include "4txn/txn_local.h"
#include "4txn/txn_cursor.h"
#include "4uqi/statements.h"
#include "4uqi/scanvisitorfactory.h"
#include "4uqi/result.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

enum {
  // The default threshold for inline records
  kInlineRecordThreshold = 32
};

// Returns the LocalEnv instance
static inline LocalEnv *
lenv(LocalDb *db)
{
  return (LocalEnv *)db->env;
}

static inline void
copy_record(LocalDb *db, Txn *txn, TxnOperation *op, ups_record_t *record)
{
  ByteArray *arena = &db->record_arena(txn);

  record->size = op->record.size;

  if (notset(record->flags, UPS_RECORD_USER_ALLOC)) {
    arena->resize(record->size);
    record->data = arena->data();
  }
  ::memcpy(record->data, op->record.data, record->size);
}

static inline LocalTxn *
begin_temp_txn(LocalEnv *env)
{
  LocalTxn *txn;
  ups_status_t st = env->txn_begin((Txn **)&txn, 0,
                            UPS_TXN_TEMPORARY | UPS_DONT_LOCK);
  if (unlikely(st))
    throw Exception(st);
  return txn;
}

static inline ups_status_t
finalize(Context *context, ups_status_t status, Txn *local_txn)
{
  LocalEnv *env = context->env;

  if (unlikely(status)) {
    if (local_txn) {
      context->changeset.clear();
      env->txn_manager()->abort(local_txn);
    }
    return status;
  }

  if (local_txn) {
    context->changeset.clear();
    env->txn_manager()->commit(local_txn);
  }
  return 0;
}

// Returns true if this database is modified by an active transaction
static inline bool
is_modified_by_active_transaction(TxnIndex *txn_index)
{
  if (txn_index) {
    TxnNode *node = txn_index->first();
    while (node) {
      TxnOperation *op = node->newest_op;
      while (op) {
        Txn *optxn = op->txn;
        // ignore aborted transactions
        // if the transaction is still active, or if it is committed
        // but was not yet flushed then return an error
        if (!optxn->is_aborted()) {
          if (!optxn->is_committed()
              || !(op->flags & TxnOperation::kIsFlushed)) {
            ups_trace(("cannot close a Database that is modified by "
                   "a currently active Txn"));
            return (true);
          }
        }
        op = op->previous_in_node;
      }
      node = node->next_sibling();
    }
  }
  return (false);
}

static inline bool
is_key_erased(Context *context, TxnIndex *txn_index, ups_key_t *key)
{
  /* get the node for this key (but don't create a new one if it does
   * not yet exist) */
  TxnNode *node = txn_index->get(key, 0);
  if (!node)
    return (false);

  /* now traverse the tree, check if the key was erased */
  TxnOperation *op = node->newest_op;
  while (op) {
    Txn *optxn = op->txn;
    if (optxn->is_aborted())
      ; /* nop */
    else if (optxn->is_committed() || context->txn == optxn) {
      if (op->flags & TxnOperation::kIsFlushed)
        ; /* continue */
      else if (op->flags & TxnOperation::kErase) {
        /* TODO does not check duplicates!! */
        return (true);
      }
      else if ((op->flags & TxnOperation::kInsert)
          || (op->flags & TxnOperation::kInsertOverwrite)
          || (op->flags & TxnOperation::kInsertDuplicate)) {
        return (false);
      }
    }

    op = op->previous_in_node;
  }

  return (false);
}

static inline void 
increment_duplicate_index(LocalDb *db, Context *context, TxnNode *node,
                LocalCursor *current_cursor)
{
  LocalCursor *c = (LocalCursor *)db->cursor_list;
  uint32_t start = current_cursor->duplicate_cache_index();

  while (c) {
    bool hit = false;

    if (c == current_cursor || c->is_nil(0))
      goto next;

    /* if cursor is coupled to an op in the same node: increment
     * duplicate index (if required) */
    if (c->is_coupled_to_txnop()) {
      TxnCursor *txnc = c->get_txn_cursor();
      TxnNode *n = txnc->get_coupled_op()->node;
      if (n == node)
        hit = true;
    }
    /* if cursor is coupled to the same key in the btree: increment
     * duplicate index (if required) */
    else if (c->get_btree_cursor()->points_to(context, node->key())) {
      hit = true;
    }

    if (hit) {
      if (c->duplicate_cache_index() > start)
        c->set_duplicate_cache_index(c->duplicate_cache_index() + 1);
    }

next:
    c = (LocalCursor *)c->next;
  }
}

// Sets all cursors attached to a TxnNode to nil
static inline void
nil_all_cursors_in_node(LocalTxn *txn, LocalCursor *current, TxnNode *node)
{
  TxnOperation *op = node->newest_op;
  while (op) {
    TxnCursor *cursor = op->cursor_list;
    while (cursor) {
      LocalCursor *parent = cursor->parent();
      // is the current cursor to a duplicate? then adjust the
      // coupled duplicate index of all cursors which point to a duplicate
      if (current) {
        if (current->duplicate_cache_index()) {
          if (current->duplicate_cache_index() < parent->duplicate_cache_index()) {
            parent->set_duplicate_cache_index(parent->duplicate_cache_index() - 1);
            cursor = cursor->next();
            continue;
          }
          else if (current->duplicate_cache_index() > parent->duplicate_cache_index()) {
            cursor = cursor->next();
            continue;
          }
          // else fall through
        }
      }
      parent->couple_to_btree(); // TODO merge these two lines
      parent->set_to_nil(LocalCursor::kTxn);
      // set a flag that the cursor just completed an Insert-or-find
      // operation; this information is needed in ups_cursor_move
      // (in this aspect, an erase is the same as insert/find)
      parent->set_last_operation(LocalCursor::kLookupOrInsert);

      cursor = op->cursor_list;
    }

    op = op->previous_in_node;
  }
}

// Sets all cursors to nil if they point to |key| in the btree index
static inline void
nil_all_cursors_in_btree(LocalDb *db, Context *context,
                LocalCursor *current, ups_key_t *key)
{
  LocalCursor *c = (LocalCursor *)db->cursor_list;

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

    if (c->get_btree_cursor()->points_to(context, key)) {
      /* is the current cursor to a duplicate? then adjust the
       * coupled duplicate index of all cursors which point to a
       * duplicate */
      if (current) {
        if (current->duplicate_cache_index()) {
          if (current->duplicate_cache_index() < c->duplicate_cache_index()) {
            c->set_duplicate_cache_index(c->duplicate_cache_index() - 1);
            goto next;
          }
          else if (current->duplicate_cache_index() > c->duplicate_cache_index()) {
            goto next;
          }
          /* else fall through */
        }
      }
      c->set_to_nil(0);
    }
next:
    c = (LocalCursor *)c->next;
  }
}

// Checks if an erase operation conflicts with another txn; this is the
// case if the same key is modified by another active txn.
static inline ups_status_t
check_erase_conflicts(LocalDb *db, Context *context, TxnNode *node,
                    ups_key_t *key, uint32_t flags)
{
  TxnOperation *op = 0;

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
  op = node->newest_op;
  while (op) {
    Txn *optxn = op->txn;
    if (optxn->is_aborted())
      ; /* nop */
    else if (optxn->is_committed() || context->txn == optxn) {
      if (op->flags & TxnOperation::kIsFlushed)
        ; /* nop */
      /* if key was erased then it doesn't exist and we fail with
       * an error */
      else if (op->flags & TxnOperation::kErase)
        return (UPS_KEY_NOT_FOUND);
      /* if the key exists then we're successful */
      else if ((op->flags & TxnOperation::kInsert)
          || (op->flags & TxnOperation::kInsertOverwrite)
          || (op->flags & TxnOperation::kInsertDuplicate)) {
        return (0);
      }
      else if (!(op->flags & TxnOperation::kNop)) {
        assert(!"shouldn't be here");
        return (UPS_KEY_NOT_FOUND);
      }
    }
    else { /* txn is still active */
      return (UPS_TXN_CONFLICT);
    }

    op = op->previous_in_node;
  }

  /*
   * we've successfully checked all un-flushed transactions and there
   * were no conflicts. Now check all transactions which are already
   * flushed - basically that's identical to a btree lookup.
   */
  return (db->btree_index->find(context, 0, key, 0, 0, 0, flags));
}

// Checks if an insert operation conflicts with another txn; this is the
// case if the same key is modified by another active txn.
static inline ups_status_t
check_insert_conflicts(LocalDb *db, Context *context, TxnNode *node,
                    ups_key_t *key, uint32_t flags)
{
  TxnOperation *op = 0;

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
  op = node->newest_op;
  while (op) {
    LocalTxn *optxn = op->txn;
    if (optxn->is_aborted())
      ; /* nop */
    else if (optxn->is_committed() || context->txn == optxn) {
      /* if key was erased then it doesn't exist and can be
       * inserted without problems */
      if (op->flags & TxnOperation::kIsFlushed)
        ; /* nop */
      else if (op->flags & TxnOperation::kErase)
        return (0);
      /* if the key already exists then we can only continue if
       * we're allowed to overwrite it or to insert a duplicate */
      else if ((op->flags & TxnOperation::kInsert)
          || (op->flags & TxnOperation::kInsertOverwrite)
          || (op->flags & TxnOperation::kInsertDuplicate)) {
        if ((flags & UPS_OVERWRITE) || (flags & UPS_DUPLICATE))
          return (0);
        else
          return (UPS_DUPLICATE_KEY);
      }
      else if (!(op->flags & TxnOperation::kNop)) {
        assert(!"shouldn't be here");
        return (UPS_DUPLICATE_KEY);
      }
    }
    else { /* txn is still active */
      return (UPS_TXN_CONFLICT);
    }

    op = op->previous_in_node;
  }

  /*
   * we've successfully checked all un-flushed transactions and there
   * were no conflicts. Now check all transactions which are already
   * flushed - basically that's identical to a btree lookup.
   *
   * however we can skip this check if we do not care about duplicates.
   */
  if ((flags & UPS_OVERWRITE)
          || (flags & UPS_DUPLICATE)
          || (db->flags() & (UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64)))
    return (0);

  ups_status_t st = db->btree_index->find(context, 0, key, 0, 0, 0, flags);
  switch (st) {
    case UPS_KEY_NOT_FOUND:
      return (0);
    case UPS_SUCCESS:
      return (UPS_DUPLICATE_KEY);
    default:
      return (st);
  }
}

// returns the next record number
static inline uint64_t
next_record_number(LocalDb *db)
{
  db->_current_record_number++;
  if (unlikely(isset(db->config.flags, UPS_RECORD_NUMBER32)
        && db->_current_record_number > std::numeric_limits<uint32_t>::max()))
    throw Exception(UPS_LIMITS_REACHED);

  if (unlikely(db->_current_record_number == 0))
    throw Exception(UPS_LIMITS_REACHED);

  return db->_current_record_number;
}

// Inserts a key/record pair in a txn node; if cursor is not NULL it will
// be attached to the new txn_op structure
static inline ups_status_t
insert_txn(LocalDb *db, Context *context, ups_key_t *key, ups_record_t *record,
                uint32_t flags, TxnCursor *cursor)
{
  ups_status_t st = 0;
  TxnOperation *op;
  bool node_created = false;

  /* get (or create) the node for this key */
  TxnNode *node = db->txn_index->get(key, 0);
  if (!node) {
    node = new TxnNode(db, key);
    node_created = true;
    // TODO only store when the operation is successful?
    db->txn_index->store(node);
  }

  // check for conflicts of this key
  //
  // !!
  // afterwards, clear the changeset; check_insert_conflicts()
  // checks if a key already exists, and this fills the changeset
  st = check_insert_conflicts(db, context, node, key, flags);
  if (st) {
    if (node_created) {
      db->txn_index->remove(node);
      delete node;
    }
    return (st);
  }

  // append a new operation to this node
  op = node->append(context->txn, flags,
                ((flags & UPS_DUPLICATE)
                    ? TxnOperation::kInsertDuplicate
                    : (flags & UPS_OVERWRITE)
                        ? TxnOperation::kInsertOverwrite
                        : TxnOperation::kInsert),
                lenv(db)->next_lsn(), key, record);

  // if there's a cursor then couple it to the op; also store the
  // dupecache-index in the op (it's needed for DUPLICATE_INSERT_BEFORE/NEXT) */
  if (cursor) {
    LocalCursor *c = cursor->parent();
    if (c->duplicate_cache_index())
      op->referenced_duplicate = c->duplicate_cache_index();

    cursor->couple_to_op(op);

    // all other cursors need to increment their dupe index, if their
    // index is > this cursor's index
    increment_duplicate_index(db, context, node, c);
  }

  // append journal entry
  if (lenv(db)->journal()) {
    lenv(db)->journal()->append_insert(db, context->txn, key, record,
              flags & UPS_DUPLICATE ? flags : flags | UPS_OVERWRITE,
              op->lsn);
  }

  assert(st == 0);
  return (0);
}

// Lookup of a key/record pair in the Txn index and in the btree,
// if transactions are disabled/not successful; copies the
// record into |record|. Also performs approx. matching.
static inline ups_status_t
find_txn(LocalDb *db, Context *context, LocalCursor *cursor, ups_key_t *key,
                ups_record_t *record, uint32_t flags)
{
  ups_status_t st = 0;
  TxnOperation *op = 0;
  bool first_loop = true;
  bool exact_is_erased = false;

  ByteArray *pkey_arena = &db->key_arena(context->txn);
  ByteArray *precord_arena = &db->record_arena(context->txn);

  ups_key_set_intflags(key,
        (ups_key_get_intflags(key) & (~BtreeKey::kApproximate)));

  /* get the node for this key (but don't create a new one if it does
   * not yet exist) */
  TxnNode *node = db->txn_index->get(key, flags);

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
    op = node->newest_op;
  while (op) {
    Txn *optxn = op->txn;
    if (optxn->is_aborted())
      ; /* nop */
    else if (optxn->is_committed() || context->txn == optxn) {
      if (op->flags & TxnOperation::kIsFlushed)
        ; /* nop */
      /* if key was erased then it doesn't exist and we can return
       * immediately
       *
       * if an approximate match is requested then move to the next
       * or previous node
       */
      else if (op->flags & TxnOperation::kErase) {
        if (first_loop
            && !(ups_key_get_intflags(key) & BtreeKey::kApproximate))
          exact_is_erased = true;
        first_loop = false;
        if (flags & UPS_FIND_LT_MATCH) {
          node = node->previous_sibling();
          if (!node)
            break;
          ups_key_set_intflags(key,
              (ups_key_get_intflags(key) | BtreeKey::kApproximate));
          goto retry;
        }
        else if (flags & UPS_FIND_GT_MATCH) {
          node = node->next_sibling();
          if (!node)
            break;
          ups_key_set_intflags(key,
              (ups_key_get_intflags(key) | BtreeKey::kApproximate));
          goto retry;
        }
        /* if a duplicate was deleted then check if there are other duplicates
         * left */
        st = UPS_KEY_NOT_FOUND;
        // TODO merge both calls
        if (cursor) {
          cursor->get_txn_cursor()->couple_to_op(op);
          cursor->couple_to_txnop();
        }
        if (op->referenced_duplicate > 1) {
          // not the first dupe - there are other dupes
          st = 0;
        }
        else if (op->referenced_duplicate == 1) {
          // check if there are other dupes
          bool is_equal;
          (void)cursor->synchronize(context, LocalCursor::kSyncOnlyEqualKeys,
                          &is_equal);
          if (!is_equal) // TODO merge w/ line above?
            cursor->set_to_nil(LocalCursor::kBtree);
          st = cursor->duplicate_cache_count(context) ? 0 : UPS_KEY_NOT_FOUND;
        }
        return (st);
      }
      /* if the key already exists then return its record; do not
       * return pointers to TxnOperation::get_record, because it may be
       * flushed and the user's pointers would be invalid */
      else if ((op->flags & TxnOperation::kInsert)
          || (op->flags & TxnOperation::kInsertOverwrite)
          || (op->flags & TxnOperation::kInsertDuplicate)) {
        if (cursor) { // TODO merge those calls
          cursor->get_txn_cursor()->couple_to_op(op);
          cursor->couple_to_txnop();
        }
        // approx match? leave the loop and continue
        // with the btree
        if (ups_key_get_intflags(key) & BtreeKey::kApproximate)
          break;
        // otherwise copy the record and return
        if (record)
          copy_record(db, context->txn, op, record);
        return (0);
      }
      else if (!(op->flags & TxnOperation::kNop)) {
        assert(!"shouldn't be here");
        return (UPS_KEY_NOT_FOUND);
      }
    }
    else { /* txn is still active */
      return (UPS_TXN_CONFLICT);
    }

    op = op->previous_in_node;
  }

  /*
   * if there was an approximate match: check if the btree provides
   * a better match
   */
  if (op && ups_key_get_intflags(key) & BtreeKey::kApproximate) {
    ups_key_t *k = op->node->key();

    ups_key_t txnkey = ups_make_key(::alloca(k->size), k->size);
    txnkey._flags = BtreeKey::kApproximate;
    ::memcpy(txnkey.data, k->data, k->size);

    ups_key_set_intflags(key, 0);

    // now lookup in the btree, but make sure that the retrieved key was
    // not deleted or overwritten in a transaction
    bool first_run = true;
    do {
      uint32_t new_flags = flags; 

      // the "exact match" key was erased? then don't fetch it again
      if (!first_run || exact_is_erased) {
        first_run = false;
        new_flags = flags & (~UPS_FIND_EQ_MATCH);
      }

      if (cursor)
        cursor->set_to_nil(LocalCursor::kBtree);
      st = db->btree_index->find(context, cursor, key, pkey_arena, record,
                      precord_arena, new_flags);
    } while (st == 0 && is_key_erased(context, db->txn_index.get(), key));

    // if the key was not found in the btree: return the key which was found
    // in the transaction tree
    if (st == UPS_KEY_NOT_FOUND) {
      if (!(key->flags & UPS_KEY_USER_ALLOC) && txnkey.data) {
        pkey_arena->resize(txnkey.size);
        key->data = pkey_arena->data();
      }
      if (txnkey.data)
        ::memcpy(key->data, txnkey.data, txnkey.size);
      key->size = txnkey.size;
      key->_flags = txnkey._flags;

      if (cursor) { // TODO merge those calls
        cursor->get_txn_cursor()->couple_to_op(op);
        cursor->couple_to_txnop();
      }
      if (record)
        copy_record(db, context->txn, op, record);
      return (0);
    }
    else if (st)
      return (st);

    // the btree key is a direct match? then return it
    if ((!(ups_key_get_intflags(key) & BtreeKey::kApproximate))
          && (flags & UPS_FIND_EQ_MATCH)
          && !exact_is_erased) {
      if (cursor)
        cursor->couple_to_btree();
      return (0);
    }

    // if there's an approx match in the btree: compare both keys and
    // use the one that is closer. if the btree is closer: make sure
    // that it was not erased or overwritten in a transaction
    int cmp = db->btree_index->compare_keys(key, &txnkey);
    bool use_btree = false;
    if (flags & UPS_FIND_GT_MATCH) {
      if (cmp < 0)
        use_btree = true;
    }
    else if (flags & UPS_FIND_LT_MATCH) {
      if (cmp > 0)
        use_btree = true;
    }
    else
      assert(!"shouldn't be here");

    if (use_btree) {
      // lookup again, with the same flags and the btree key.
      // this will check if the key was erased or overwritten
      // in a transaction
      st = find_txn(db, context, cursor, key, record, flags | UPS_FIND_EQ_MATCH);
      if (st == 0)
        ups_key_set_intflags(key,
          (ups_key_get_intflags(key) | BtreeKey::kApproximate));
      return (st);
    }
    else { // use txn
      if (!(key->flags & UPS_KEY_USER_ALLOC) && txnkey.data) {
        pkey_arena->resize(txnkey.size);
        key->data = pkey_arena->data();
      }
      if (txnkey.data) {
        ::memcpy(key->data, txnkey.data, txnkey.size);
      }
      key->size = txnkey.size;
      key->_flags = txnkey._flags;

      if (cursor) { // TODO merge those calls
        cursor->get_txn_cursor()->couple_to_op(op);
        cursor->couple_to_txnop();
      }
      if (record)
        copy_record(db, context->txn, op, record);
      return (0);
    }
  }

  /*
   * no approximate match:
   *
   * we've successfully checked all un-flushed transactions and there
   * were no conflicts, and we have not found the key: now try to
   * lookup the key in the btree.
   */
  return (db->btree_index->find(context, cursor, key, pkey_arena, record,
                          precord_arena, flags));
}

// Erases a key/record pair from a txn; on success, cursor will be set to
// nil
static inline ups_status_t
erase_txn(LocalDb *db, Context *context, ups_key_t *key, uint32_t flags,
                TxnCursor *cursor)
{
  ups_status_t st = 0;
  TxnOperation *op;
  bool node_created = false;
  LocalCursor *pc = 0;
  if (cursor)
    pc = cursor->parent();

  /* get (or create) the node for this key */
  TxnNode *node = db->txn_index->get(key, 0);
  if (!node) {
    node = new TxnNode(db, key);
    node_created = true;
    // TODO only store when the operation is successful?
    db->txn_index->store(node);
  }

  /*
   * check for conflicts of this key - but only if we're not erasing a
   * duplicate key. dupes are checked for conflicts in LocalCursor::move
   */
  if (!pc || (!pc->duplicate_cache_index())) {
    st = check_erase_conflicts(db, context, node, key, flags);
    if (st) {
      if (node_created) {
        db->txn_index->remove(node);
        delete node;
      }
      return (st);
    }
  }

  /* append a new operation to this node */
  op = node->append(context->txn, flags, TxnOperation::kErase,
                  lenv(db)->next_lsn(), key, 0);

  /* is this function called through ups_cursor_erase? then add the
   * duplicate ID */
  if (cursor) {
    if (pc->duplicate_cache_index())
      op->referenced_duplicate = pc->duplicate_cache_index();
  }

  /* the current op has no cursors attached; but if there are any
   * other ops in this node and in this transaction, then they have to
   * be set to nil. This only nil's txn-cursors! */
  nil_all_cursors_in_node(context->txn, pc, node);

  /* in addition we nil all btree cursors which are coupled to this key */
  nil_all_cursors_in_btree(db, context, pc, node->key());

  /* append journal entry */
  if (lenv(db)->journal())
    lenv(db)->journal()->append_erase(db, context->txn, key, 0,
                    flags | UPS_ERASE_ALL_DUPLICATES, op->lsn);

  assert(st == 0);
  return (0);
}

// The actual implementation of insert()
static inline ups_status_t
insert_impl(LocalDb *db, Context *context, LocalCursor *cursor,
                ups_key_t *key, ups_record_t *record, uint32_t flags)
{
  ups_status_t st = 0;

  lenv(db)->page_manager()->purge_cache(context);

  /*
   * if transactions are enabled: only insert the key/record pair into
   * the Txn structure. Otherwise immediately write to the btree.
   */
  if (context->txn || db->env->flags() & UPS_ENABLE_TRANSACTIONS)
    st = insert_txn(db, context, key, record, flags, cursor
                                                ? cursor->get_txn_cursor()
                                                : 0);
  else
    st = db->btree_index->insert(context, cursor, key, record, flags);

  // couple the cursor to the inserted key
  if (st == 0 && cursor) {
    if (db->env->flags() & UPS_ENABLE_TRANSACTIONS) {
      DuplicateCache &dc = cursor->duplicate_cache();
      // TODO required? should have happened in insert_txn
      cursor->couple_to_txnop();
      /* the cursor is coupled to the txn-op; nil the btree-cursor to
       * trigger a synchronize() call when fetching the duplicates */
      // TODO merge with the line above
      cursor->set_to_nil(LocalCursor::kBtree);

      /* if duplicate keys are enabled: set the duplicate index of
       * the new key  */
      if (st == 0 && cursor->duplicate_cache_count(context, true)) {
        TxnOperation *op = cursor->get_txn_cursor()->get_coupled_op();
        assert(op != 0);

        for (uint32_t i = 0; i < dc.size(); i++) {
          DuplicateCacheLine *l = &dc[i];
          if (!l->use_btree() && l->txn_op() == op) {
            cursor->set_duplicate_cache_index(i + 1);
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
     * operation; this information is needed in ups_cursor_move */
    cursor->set_last_operation(LocalCursor::kLookupOrInsert);
  }

  return (st);
}

// The actual implementation of find()
static inline ups_status_t
find_impl(LocalDb *db, Context *context, LocalCursor *cursor,
                ups_key_t *key, ups_record_t *record, uint32_t flags)
{
  /* purge cache if necessary */
  lenv(db)->page_manager()->purge_cache(context);

  /*
   * if transactions are enabled: read keys from transaction trees,
   * otherwise read immediately from disk
   */
  if (context->txn || db->env->flags() & UPS_ENABLE_TRANSACTIONS)
    return (find_txn(db, context, cursor, key, record, flags));

  return (db->btree_index->find(context, cursor, key,
                          &db->key_arena(context->txn), record,
                          &db->record_arena(context->txn), flags));
}

// The actual implementation of erase()
static inline ups_status_t
erase_impl(LocalDb *db, Context *context, LocalCursor *cursor, ups_key_t *key,
                uint32_t flags)
{
  ups_status_t st = 0;

  /*
   * if transactions are enabled: append a 'erase key' operation into
   * the txn tree; otherwise immediately erase the key from disk
   */
  if (context->txn || db->env->flags() & UPS_ENABLE_TRANSACTIONS) {
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
        cursor->set_to_nil(LocalCursor::kTxn);
        cursor->get_btree_cursor()->uncouple_from_page(context);
        st = erase_txn(db, context, cursor->get_btree_cursor()->uncoupled_key(),
                        0, cursor->get_txn_cursor());
      }
      /* case 2 described above */
      else {
        // TODO this line is ugly
        st = erase_txn(db, context, 
                        &cursor->get_txn_cursor()->get_coupled_op()->key,
                        0, cursor->get_txn_cursor());
      }
    }
    else {
      st = erase_txn(db, context, key, flags, 0);
    }
  }
  else {
    st = db->btree_index->erase(context, cursor, key, 0, flags);
  }

  /* on success: 'nil' the cursor */
  if (cursor && st == 0) {
    cursor->set_to_nil(0);
    assert(cursor->get_txn_cursor()->is_nil());
    assert(cursor->is_nil(0));
  }

  return (st);
}

ups_status_t
LocalDb::create(Context *context, PBtreeHeader *btree_header)
{
  /* the header page is now modified */
  Page *header = lenv(this)->page_manager()->fetch(context, 0);
  header->set_dirty(true);

  /* set the flags; strip off run-time (per session) flags for the btree */
  uint32_t persistent_flags = flags();
  persistent_flags &= ~(UPS_CACHE_UNLIMITED
            | UPS_DISABLE_MMAP
            | UPS_ENABLE_FSYNC
            | UPS_READ_ONLY
            | UPS_AUTO_RECOVERY
            | UPS_ENABLE_TRANSACTIONS);

  switch (config.key_type) {
    case UPS_TYPE_UINT8:
      config.key_size = 1;
      break;
    case UPS_TYPE_UINT16:
      config.key_size = 2;
      break;
    case UPS_TYPE_REAL32:
    case UPS_TYPE_UINT32:
      config.key_size = 4;
      break;
    case UPS_TYPE_REAL64:
    case UPS_TYPE_UINT64:
      config.key_size = 8;
      break;
  }

  switch (config.record_type) {
    case UPS_TYPE_UINT8:
      config.record_size = 1;
      break;
    case UPS_TYPE_UINT16:
      config.record_size = 2;
      break;
    case UPS_TYPE_REAL32:
    case UPS_TYPE_UINT32:
      config.record_size = 4;
      break;
    case UPS_TYPE_REAL64:
    case UPS_TYPE_UINT64:
      config.record_size = 8;
      break;
  }

  // if we cannot fit at least 10 keys in a page then refuse to continue
  if (config.key_size != UPS_KEY_SIZE_UNLIMITED) {
    if (lenv(this)->config.page_size_bytes / (config.key_size + 8) < 10) {
      ups_trace(("key size too large; either increase page_size or decrease "
                "key size"));
      return (UPS_INV_KEY_SIZE);
    }
  }

  // fixed length records:
  //
  // if records are <= 8 bytes OR if we can fit at least 500 keys AND
  // records into the leaf then store the records in the leaf;
  // otherwise they're allocated as a blob
  if (config.record_size != UPS_RECORD_SIZE_UNLIMITED) {
    if (config.record_size <= 8
        || (config.record_size <= kInlineRecordThreshold
          && lenv(this)->config.page_size_bytes
                / (config.key_size + config.record_size) > 500)) {
      persistent_flags |= UPS_FORCE_RECORDS_INLINE;
      config.flags |= UPS_FORCE_RECORDS_INLINE;
    }
  }

  // create the btree
  btree_index.reset(new BtreeIndex(this));

  /* initialize the btree */
  btree_index->create(context, btree_header, &config);

  if (config.record_compressor) {
    record_compressor.reset(CompressorFactory::create(
                                    config.record_compressor));
  }

  /* load the custom compare function? */
  if (config.key_type == UPS_TYPE_CUSTOM) {
    ups_compare_func_t func = CallbackManager::get(btree_index->compare_hash());
    // silently ignore errors as long as db_set_compare_func is in place
    if (func != 0)
      compare_function = func;
  }

  /* the header page is now dirty */
  header->set_dirty(true);

  /* and the TxnIndex */
  txn_index.reset(new TxnIndex(this));

  return (0);
}

ups_status_t
LocalDb::open(Context *context, PBtreeHeader *btree_header)
{
  /* create the BtreeIndex */
  btree_index.reset(new BtreeIndex(this));

  /* initialize the btree */
  btree_index->open(btree_header, &config);

  /* merge the persistent flags with the flags supplied by the user */
  config.flags |= flags();

  /* create the TxnIndex - TODO only if txn's are enabled? */
  txn_index.reset(new TxnIndex(this));

  /* load the custom compare function? */
  if (config.key_type == UPS_TYPE_CUSTOM) {
    ups_compare_func_t f = CallbackManager::get(btree_index->compare_hash());
    if (f == 0 && notset(flags(), UPS_IGNORE_MISSING_CALLBACK)) {
      ups_trace(("custom compare function is not yet registered"));
      return (UPS_NOT_READY);
    }
    compare_function = f;
  }

  /* is record compression enabled? */
  if (config.record_compressor) {
    record_compressor.reset(CompressorFactory::create(
                                    config.record_compressor));
  }

  /* fetch the current record number */
  if ((flags() & (UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64))) {
    ups_key_t key = {};
    LocalCursor *c = new LocalCursor(this, 0);
    ups_status_t st = c->move(context, &key, 0, UPS_CURSOR_LAST);
    c->close();
    delete c;
    if (st)
      return (st == UPS_KEY_NOT_FOUND ? 0 : st);

    if (flags() & UPS_RECORD_NUMBER32)
      _current_record_number = *(uint32_t *)key.data;
    else
      _current_record_number = *(uint64_t *)key.data;
  }

  return (0);
}

struct MetricsVisitor : public BtreeVisitor {
  MetricsVisitor(ups_env_metrics_t *metrics)
    : m_metrics(metrics) {
  }

  // Specifies if the visitor modifies the node
  virtual bool is_read_only() const {
    return (true);
  }

  // called for each node
  virtual void operator()(Context *context, BtreeNodeProxy *node) {
    if (node->is_leaf())
      node->fill_metrics(&m_metrics->btree_leaf_metrics);
    else
      node->fill_metrics(&m_metrics->btree_internal_metrics);
  }
  
  ups_env_metrics_t *m_metrics;
};

void
LocalDb::fill_metrics(ups_env_metrics_t *metrics)
{
  metrics->btree_leaf_metrics.database_name = name();
  metrics->btree_internal_metrics.database_name = name();

  MetricsVisitor visitor(metrics);
  Context context(lenv(this), 0, this);
  btree_index->visit_nodes(&context, visitor, true);

  // calculate the "avg" values
  BtreeStatistics::finalize_metrics(&metrics->btree_leaf_metrics);
  BtreeStatistics::finalize_metrics(&metrics->btree_internal_metrics);
}

ups_status_t
LocalDb::get_parameters(ups_parameter_t *param)
{
  Context context(lenv(this), 0, this);

  Page *page = 0;
  ups_parameter_t *p = param;

  if (p) {
    for (; p->name; p++) {
      switch (p->name) {
      case UPS_PARAM_KEY_TYPE:
        p->value = config.key_type;
        break;
      case UPS_PARAM_KEY_SIZE:
        p->value = config.key_size;
        break;
      case UPS_PARAM_RECORD_TYPE:
        p->value = config.record_type;
        break;
      case UPS_PARAM_RECORD_SIZE:
        p->value = config.record_size;
        break;
      case UPS_PARAM_FLAGS:
        p->value = (uint64_t)flags();
        break;
      case UPS_PARAM_DATABASE_NAME:
        p->value = (uint64_t)name();
        break;
      case UPS_PARAM_MAX_KEYS_PER_PAGE:
        p->value = 0;
        page = btree_index->root_page(&context);
        if (page) {
          BtreeNodeProxy *node = btree_index->get_node_from_page(page);
          p->value = node->estimate_capacity();
        }
        break;
      case UPS_PARAM_RECORD_COMPRESSION:
        p->value = config.record_compressor;
        break;
      case UPS_PARAM_KEY_COMPRESSION:
        p->value = config.key_compressor;
        break;
      default:
        ups_trace(("unknown parameter %d", (int)p->name));
        throw Exception(UPS_INV_PARAMETER);
      }
    }
  }
  return (0);
}

ups_status_t
LocalDb::check_integrity(uint32_t flags)
{
  Context context(lenv(this), 0, this);

  /* purge cache if necessary */
  lenv(this)->page_manager()->purge_cache(&context);

  /* call the btree function */
  btree_index->check_integrity(&context, flags);

  /* call the txn function */
  //txn_index->check_integrity(flags);
  return (0);
}

uint64_t
LocalDb::count(Txn *htxn, bool distinct)
{
  LocalTxn *txn = dynamic_cast<LocalTxn *>(htxn);

  Context context(lenv(this), txn, this);

  /* purge cache if necessary */
  lenv(this)->page_manager()->purge_cache(&context);

  /*
   * call the btree function - this will retrieve the number of keys
   * in the btree
   */
  uint64_t keycount = btree_index->count(&context, distinct);

  /*
   * if transactions are enabled, then also sum up the number of keys
   * from the transaction tree
   */
  if (flags() & UPS_ENABLE_TRANSACTIONS)
    keycount += txn_index->count(&context, txn, distinct);

  return keycount;
}

ups_status_t
LocalDb::insert(Cursor *hcursor, Txn *txn, ups_key_t *key,
                ups_record_t *record, uint32_t flags)
{
  LocalCursor *cursor = (LocalCursor *)hcursor;
  Context context(lenv(this), (LocalTxn *)txn, this);

  if (config.flags & (UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64)) {
    if (key->size == 0 && key->data == 0) {
      // ok!
    }
    else if (key->size == 0 && key->data != 0) {
      ups_trace(("for record number keys set key size to 0, "
                             "key->data to null"));
      return (UPS_INV_PARAMETER);
    }
    else if (key->size != config.key_size) {
      ups_trace(("invalid key size (%u instead of %u)",
            key->size, config.key_size));
      return (UPS_INV_KEY_SIZE);
    }
  }
  else if (config.key_size != UPS_KEY_SIZE_UNLIMITED
      && key->size != config.key_size) {
    ups_trace(("invalid key size (%u instead of %u)",
          key->size, config.key_size));
    return (UPS_INV_KEY_SIZE);
  }
  if (config.record_size != UPS_RECORD_SIZE_UNLIMITED
      && record->size != config.record_size) {
    ups_trace(("invalid record size (%u instead of %u)",
          record->size, config.record_size));
    return (UPS_INV_RECORD_SIZE);
  }

  ByteArray *arena = &key_arena(txn);

  /*
   * record number: make sure that we have a valid key structure,
   * and lazy load the last used record number
   *
   * TODO TODO
   * too much duplicated code
   */
  uint64_t recno = 0;
  if (this->flags() & UPS_RECORD_NUMBER64) {
    if (flags & UPS_OVERWRITE) {
      assert(key->size == sizeof(uint64_t));
      assert(key->data != 0);
      recno = *(uint64_t *)key->data;
    }
    else {
      /* get the record number and increment it */
      recno = next_record_number(this);
    }

    /* allocate memory for the key */
    if (!key->data) {
      arena->resize(sizeof(uint64_t));
      key->data = arena->data();
    }
    key->size = sizeof(uint64_t);
    *(uint64_t *)key->data = recno;

    /* A recno key is always appended sequentially */
    flags |= UPS_HINT_APPEND;
  }
  else if (this->flags() & UPS_RECORD_NUMBER32) {
    if (flags & UPS_OVERWRITE) {
      assert(key->size == sizeof(uint32_t));
      assert(key->data != 0);
      recno = *(uint32_t *)key->data;
    }
    else {
      /* get the record number and increment it */
      recno = next_record_number(this);
    }

    /* allocate memory for the key */
    if (!key->data) {
      arena->resize(sizeof(uint32_t));
      key->data = arena->data();
    }
    key->size = sizeof(uint32_t);
    *(uint32_t *)key->data = (uint32_t)recno;

    /* A recno key is always appended sequentially */
    flags |= UPS_HINT_APPEND;
  }

  ups_status_t st = 0;
  LocalTxn *local_txn = 0;

  /* purge cache if necessary */
  if (!txn && (this->flags() & UPS_ENABLE_TRANSACTIONS)) {
    local_txn = begin_temp_txn(lenv(this));
    context.txn = local_txn;
  }

  st = insert_impl(this, &context, cursor, key, record, flags);
  return (finalize(&context, st, local_txn));
}

ups_status_t
LocalDb::erase(Cursor *hcursor, Txn *txn, ups_key_t *key, uint32_t flags)
{
  LocalCursor *cursor = (LocalCursor *)hcursor;
  Context context(lenv(this), (LocalTxn *)txn, this);

  ups_status_t st = 0;
  LocalTxn *local_txn = 0;

  if (cursor) {
    if (cursor->is_nil())
      throw Exception(UPS_CURSOR_IS_NIL);
    if (cursor->is_coupled_to_txnop()) // TODO rewrite the next line, it's ugly
      key = cursor->get_txn_cursor()->get_coupled_op()->node->key();
    else // cursor->is_coupled_to_btree()
      key = 0;
  }

  if (key) {
    if (config.key_size != UPS_KEY_SIZE_UNLIMITED
        && key->size != config.key_size) {
      ups_trace(("invalid key size (%u instead of %u)",
            key->size, config.key_size));
      return (UPS_INV_KEY_SIZE);
    }
  }

  if (!txn && (this->flags() & UPS_ENABLE_TRANSACTIONS)) {
    local_txn = begin_temp_txn(lenv(this));
    context.txn = local_txn;
  }

  st = erase_impl(this, &context, cursor, key, flags);
  return (finalize(&context, st, local_txn));
}

ups_status_t
LocalDb::find(Cursor *hcursor, Txn *txn, ups_key_t *key,
            ups_record_t *record, uint32_t flags)
{
  LocalCursor *cursor = (LocalCursor *)hcursor;
  Context context(lenv(this), (LocalTxn *)txn, this);

  ups_status_t st = 0;

  /* Duplicates AND Txns require a Cursor because only
   * Cursors can build lists of duplicates.
   * TODO not exception safe - if find() throws then the cursor is not closed
   */
  if (!cursor
        && (this->flags() & (UPS_ENABLE_DUPLICATE_KEYS|UPS_ENABLE_TRANSACTIONS))) {
    LocalCursor *c = new LocalCursor(this, txn);
    st = find(c, txn, key, record, flags);
    c->close();
    delete c;
    return (st);
  }

  if (config.key_size != UPS_KEY_SIZE_UNLIMITED
      && key->size != config.key_size) {
    ups_trace(("invalid key size (%u instead of %u)",
          key->size, config.key_size));
    return (UPS_INV_KEY_SIZE);
  }

  // cursor: reset the dupecache, set to nil
  if (cursor)
    cursor->set_to_nil(LocalCursor::kBoth);

  st = find_impl(this, &context, cursor, key, record, flags);
  if (st)
    return (finalize(&context, st, 0));

  if (cursor) {
    // make sure that txn-cursor and btree-cursor point to the same keys
    if (this->flags() & UPS_ENABLE_TRANSACTIONS) {
      bool is_equal;
      (void)cursor->synchronize(&context, LocalCursor::kSyncOnlyEqualKeys,
                      &is_equal);
      if (!is_equal && cursor->is_coupled_to_txnop())
        cursor->set_to_nil(LocalCursor::kBtree);
    }

    /* if the key has duplicates: build a duplicate table, then couple to the
     * first/oldest duplicate */
    if (cursor->duplicate_cache_count(&context, true)) {
      cursor->couple_to_duplicate(1); // 1-based index!
      if (record) { // TODO don't copy record if it was already
                    // copied in find_impl
        if (cursor->is_coupled_to_txnop())
          cursor->get_txn_cursor()->copy_coupled_record(record);
        else {
          Txn *txn = cursor->txn;
          st = cursor->get_btree_cursor()->move(&context, 0, 0, record,
                        &record_arena(txn), 0);
        }
      }
    }

    /* set a flag that the cursor just completed an Insert-or-find
     * operation; this information is needed in ups_cursor_move */
    cursor->set_last_operation(LocalCursor::kLookupOrInsert);
  }

  return (finalize(&context, st, 0));
}

Cursor *
LocalDb::cursor_create(Txn *txn, uint32_t)
{
  return (new LocalCursor(this, txn));
}

Cursor *
LocalDb::cursor_clone(Cursor *hsrc)
{
  return (new LocalCursor(*(LocalCursor *)hsrc));
}

ups_status_t
LocalDb::cursor_move(Cursor *hcursor, ups_key_t *key,
                ups_record_t *record, uint32_t flags)
{
  LocalCursor *cursor = (LocalCursor *)hcursor;

  Context context(lenv(this), (LocalTxn *)cursor->txn, this);

  /* purge cache if necessary */
  lenv(this)->page_manager()->purge_cache(&context);

  /*
   * if the cursor was never used before and the user requests a NEXT then
   * move the cursor to FIRST; if the user requests a PREVIOUS we set it
   * to LAST, resp.
   *
   * if the cursor was already used but is nil then we've reached EOF,
   * and a NEXT actually tries to move to the LAST key (and PREVIOUS
   * moves to FIRST)
   */
  if (cursor->is_nil(0)) {
    if (flags & UPS_CURSOR_NEXT) {
      flags &= ~UPS_CURSOR_NEXT;
      if (cursor->is_first_use())
        flags |= UPS_CURSOR_FIRST;
      else
        flags |= UPS_CURSOR_LAST;
    }
    else if (flags & UPS_CURSOR_PREVIOUS) {
      flags &= ~UPS_CURSOR_PREVIOUS;
      if (cursor->is_first_use())
        flags |= UPS_CURSOR_LAST;
      else
        flags |= UPS_CURSOR_FIRST;
    }
  }

  ups_status_t st = 0;

  /* everything else is handled by the cursor function */
  st = cursor->move(&context, key, record, flags);

  if (st) {
    if (st == UPS_KEY_ERASED_IN_TXN)
      st = UPS_KEY_NOT_FOUND;
    /* trigger a synchronize when the function is called again */
    cursor->set_last_operation(0);
    return (st);
  }

  /* store the direction */
  if (flags & UPS_CURSOR_NEXT)
    cursor->set_last_operation(UPS_CURSOR_NEXT);
  else if (flags & UPS_CURSOR_PREVIOUS)
    cursor->set_last_operation(UPS_CURSOR_PREVIOUS);
  else
    cursor->set_last_operation(0);

  return (0);
}

ups_status_t
LocalDb::close(uint32_t flags)
{
  Context context(lenv(this), 0, this);

  if (is_modified_by_active_transaction(txn_index.get())) {
    ups_trace(("cannot close a Database that is modified by "
               "a currently active Txn"));
    return (UPS_TXN_STILL_OPEN);
  }

  /* in-memory-database: free all allocated blobs */
  if (btree_index && env->flags() & UPS_IN_MEMORY)
   btree_index->drop(&context);

  /*
   * flush all pages of this database (but not the header page,
   * it's still required and will be flushed below)
   */
  lenv(this)->page_manager()->close_database(&context, this);

  env = 0;

  return (0);
}

static bool
are_cursors_identical(LocalCursor *c1, LocalCursor *c2)
{
  assert(!c1->is_nil());
  assert(!c2->is_nil());

  if (c1->is_coupled_to_btree()) {
    if (c2->is_coupled_to_txnop())
      return (false);

    int s1, s2;
    Page *p1, *p2;
    c1->get_btree_cursor()->coupled_key(&p1, &s1);
    c2->get_btree_cursor()->coupled_key(&p2, &s2);
    return (p1 == p2 && s1 == s2);
  }

  ups_key_t *k1 = c1->get_txn_cursor()->get_coupled_op()->node->key();
  ups_key_t *k2 = c2->get_txn_cursor()->get_coupled_op()->node->key();
  return (k1 == k2);
}

ups_status_t
LocalDb::select_range(SelectStatement *stmt, LocalCursor *begin,
                LocalCursor *end, Result **presult)
{
  ups_status_t st = 0;
  Page *page = 0;
  int slot;
  ups_key_t key = {0};
  ups_record_t record = {0};
 
  LocalCursor *cursor = begin;
  if (cursor && cursor->is_nil())
    return (UPS_CURSOR_IS_NIL);

  if (end && end->is_nil())
    return (UPS_CURSOR_IS_NIL);

  std::auto_ptr<ScanVisitor> visitor(ScanVisitorFactory::from_select(stmt,
                                                this));
  if (!visitor.get())
    return (UPS_PARSER_ERROR);

  Context context(lenv(this), 0, this);

  Result *result = new Result;

  /* purge cache if necessary */
  lenv(this)->page_manager()->purge_cache(&context);

  /* create a cursor, move it to the first key */
  if (!cursor) {
    cursor = new LocalCursor(this, 0);
    st = cursor->move(&context, &key, &record, UPS_CURSOR_FIRST);
    if (st)
      goto bail;
  }

  /* process transactional keys at the beginning */
  while (!cursor->is_coupled_to_btree()) {
    /* check if we reached the 'end' cursor */
    if (end && are_cursors_identical(cursor, end))
      goto bail;
    /* process the key */
    (*visitor)(key.data, key.size, record.data, record.size);
    st = cursor->move(&context, &key, 0, UPS_CURSOR_NEXT);
    if (st)
      goto bail;
  }

  /*
   * now jump from leaf to leaf, and from transactional cursor to
   * transactional cursor.
   *
   * if there are transactional keys BEFORE a page then process them
   * if there are transactional keys IN a page then use a cursor for
   *      the page
   * if there are NO transactional keys IN a page then ask the
   *      Btree to process the request (this is the fastest code path)
   *
   * afterwards, pick up any transactional stragglers that are still left.
   */
  while (true) {
    cursor->get_btree_cursor()->coupled_key(&page, &slot);
    BtreeNodeProxy *node = btree_index->get_node_from_page(page);

    bool use_cursors = false;

    /*
     * in a few cases we're forced to use a cursor to iterate over the
     * page. these cases are:
     *
     * 1) an 'end' cursor is specified, and it is positioned "in" this page
     * 2) the page is modified by one (or more) transactions
     */

    /* case 1) - if an 'end' cursor is specified then check if it modifies
     * the current page */
    if (end) {
      if (end->is_coupled_to_btree()) {
        int end_slot;
        Page *end_page;
        end->get_btree_cursor()->coupled_key(&end_page, &end_slot);
        if (page == end_page)
          use_cursors = true;
      }
      else {
        ups_key_t *k = end->get_txn_cursor()->get_coupled_op()->
                              node->key();
        if (node->compare(&context, k, 0) >= 0
            && node->compare(&context, k, node->length() - 1) <= 0)
          use_cursors = true;
      }
    }

    /* case 2) - take a peek at the next transactional key and check
     * if it modifies the current page */
    if (!use_cursors && (flags() & UPS_ENABLE_TRANSACTIONS)) {
      TxnCursor tc(cursor);
      tc.clone(cursor->get_txn_cursor());
      if (tc.is_nil())
        st = tc.move(UPS_CURSOR_FIRST);
      else
        st = tc.move(UPS_CURSOR_NEXT);
      if (st == 0) {
        ups_key_t *txnkey = 0;
        if (tc.get_coupled_op())
          txnkey = tc.get_coupled_op()->node->key();
        if (node->compare(&context, txnkey, 0) >= 0
            && node->compare(&context, txnkey, node->length() - 1) <= 0)
          use_cursors = true;
      }
    }

    /* no transactional data: the Btree will do the work. This is the */
    /* fastest code path */
    if (use_cursors == false) {
      node->scan(&context, visitor.get(), stmt, slot, stmt->distinct);
      st = cursor->get_btree_cursor()->move_to_next_page(&context);
      if (st == UPS_KEY_NOT_FOUND)
        break;
      if (st)
        goto bail;
    }
    /* mixed txn/btree load? if there are leafs which are NOT modified */
    /* in a transaction then move the scan to the btree node. Otherwise use */
    /* a regular cursor */
    else {
      do {
        /* check if we reached the 'end' cursor */
        if (end && are_cursors_identical(cursor, end))
          goto bail;

        Page *new_page = 0;
        if (cursor->is_coupled_to_btree())
          cursor->get_btree_cursor()->coupled_key(&new_page);
        /* break the loop if we've reached the next page */
        if (new_page && new_page != page) {
          page = new_page;
          break;
        }
        /* process the key */
        (*visitor)(key.data, key.size, record.data, record.size);
        st = cursor->move(&context, &key, &record, UPS_CURSOR_NEXT);
      } while (st == 0);
    }

    if (st == UPS_KEY_NOT_FOUND)
      goto bail;
    if (st)
      return (st);
  }

  /* pick up the remaining transactional keys */
  while ((st = cursor->move(&context, &key, &record, UPS_CURSOR_NEXT)) == 0) {
    /* check if we reached the 'end' cursor */
    if (end && are_cursors_identical(cursor, end))
      goto bail;

    (*visitor)(key.data, key.size, record.data, record.size);
  }

bail:
  /* now fetch the results */
  visitor->assign_result((uqi_result_t *)result);

  if (cursor && begin == 0) {
    cursor->close();
    delete cursor;
  }

  *presult = result;

  // TODO leaks cursor if exception is thrown
  return (st == UPS_KEY_NOT_FOUND ? 0 : st);
}

ups_status_t
LocalDb::flush_txn_operation(Context *context, LocalTxn *txn,
                TxnOperation *op)
{
  ups_status_t st = 0;
  TxnNode *node = op->node;

  /*
   * depending on the type of the operation: actually perform the
   * operation on the btree
   *
   * if the txn-op has a cursor attached, then all (txn)cursors
   * which are coupled to this op have to be uncoupled, and their
   * parent (btree) cursor must be coupled to the btree item instead.
   */
  if ((op->flags & TxnOperation::kInsert)
      || (op->flags & TxnOperation::kInsertOverwrite)
      || (op->flags & TxnOperation::kInsertDuplicate)) {
    uint32_t additional_flag = 
      (op->flags & TxnOperation::kInsertDuplicate)
          ? UPS_DUPLICATE
          : UPS_OVERWRITE;

    LocalCursor *c1 = op->cursor_list
                            ? op->cursor_list->parent()
                            : 0;

    /* ignore cursor if it's coupled to btree */
    if (!c1 || c1->is_coupled_to_btree()) {
      st = btree_index->insert(context, 0, node->key(), &op->record,
                  op->original_flags | additional_flag);
    }
    else {
      /* pick the first cursor, get the parent/btree cursor and
       * insert the key/record pair in the btree. The btree cursor
       * then will be coupled to this item. */
      st = btree_index->insert(context, c1, node->key(), &op->record,
                  op->original_flags | additional_flag);
      if (!st) {
        /* uncouple the cursor from the txn-op, and remove it */
        c1->couple_to_btree(); // TODO merge these two calls
        c1->set_to_nil(LocalCursor::kTxn);

        /* all other (txn) cursors need to be coupled to the same
         * item as the first one. */
        TxnCursor *tc2;
        while ((tc2 = op->cursor_list)) {
          LocalCursor *c2 = tc2->parent();
          c2->get_btree_cursor()->clone(c1->get_btree_cursor());
          c2->couple_to_btree(); // TODO merge these two calls
          c2->set_to_nil(LocalCursor::kTxn);
        }
      }
    }
  }
  else if (op->flags & TxnOperation::kErase) {
    st = btree_index->erase(context, 0, node->key(),
                  op->referenced_duplicate, op->flags);
    if (st == UPS_KEY_NOT_FOUND)
      st = 0;
  }

  return (st);
}

ups_status_t
LocalDb::drop(Context *context)
{
  btree_index->drop(context);
  return (0);
}

} // namespace upscaledb
