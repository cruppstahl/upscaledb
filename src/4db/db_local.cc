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
#include "5upscaledb/statements.h"
#include "5upscaledb/scanvisitorfactory.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

ups_status_t
LocalDatabase::check_insert_conflicts(Context *context, TransactionNode *node,
                    ups_key_t *key, uint32_t flags)
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
    else if (optxn->is_committed() || context->txn == optxn) {
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
        if ((flags & UPS_OVERWRITE) || (flags & UPS_DUPLICATE))
          return (0);
        else
          return (UPS_DUPLICATE_KEY);
      }
      else if (!(op->get_flags() & TransactionOperation::kNop)) {
        ups_assert(!"shouldn't be here");
        return (UPS_DUPLICATE_KEY);
      }
    }
    else { /* txn is still active */
      return (UPS_TXN_CONFLICT);
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
  if ((flags & UPS_OVERWRITE)
          || (flags & UPS_DUPLICATE)
          || (get_flags() & (UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64)))
    return (0);

  ups_status_t st = m_btree_index->find(context, 0, key, 0, 0, 0, flags);
  switch (st) {
    case UPS_KEY_NOT_FOUND:
      return (0);
    case UPS_SUCCESS:
      return (UPS_DUPLICATE_KEY);
    default:
      return (st);
  }
}

ups_status_t
LocalDatabase::check_erase_conflicts(Context *context, TransactionNode *node,
                    ups_key_t *key, uint32_t flags)
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
    else if (optxn->is_committed() || context->txn == optxn) {
      if (op->get_flags() & TransactionOperation::kIsFlushed)
        ; /* nop */
      /* if key was erased then it doesn't exist and we fail with
       * an error */
      else if (op->get_flags() & TransactionOperation::kErase)
        return (UPS_KEY_NOT_FOUND);
      /* if the key exists then we're successful */
      else if ((op->get_flags() & TransactionOperation::kInsert)
          || (op->get_flags() & TransactionOperation::kInsertOverwrite)
          || (op->get_flags() & TransactionOperation::kInsertDuplicate)) {
        return (0);
      }
      else if (!(op->get_flags() & TransactionOperation::kNop)) {
        ups_assert(!"shouldn't be here");
        return (UPS_KEY_NOT_FOUND);
      }
    }
    else { /* txn is still active */
      return (UPS_TXN_CONFLICT);
    }

    op = op->get_previous_in_node();
  }

  /*
   * we've successfully checked all un-flushed transactions and there
   * were no conflicts. Now check all transactions which are already
   * flushed - basically that's identical to a btree lookup.
   */
  return (m_btree_index->find(context, 0, key, 0, 0, 0, flags));
}

ups_status_t
LocalDatabase::insert_txn(Context *context, ups_key_t *key,
                ups_record_t *record, uint32_t flags, TransactionCursor *cursor)
{
  ups_status_t st = 0;
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
  st = check_insert_conflicts(context, node, key, flags);
  if (st) {
    if (node_created) {
      m_txn_index->remove(node);
      delete node;
    }
    return (st);
  }

  // append a new operation to this node
  op = node->append(context->txn, flags,
                (flags & UPS_PARTIAL) |
                ((flags & UPS_DUPLICATE)
                    ? TransactionOperation::kInsertDuplicate
                    : (flags & UPS_OVERWRITE)
                        ? TransactionOperation::kInsertOverwrite
                        : TransactionOperation::kInsert),
                lenv()->next_lsn(), key, record);

  // if there's a cursor then couple it to the op; also store the
  // dupecache-index in the op (it's needed for DUPLICATE_INSERT_BEFORE/NEXT) */
  if (cursor) {
    LocalCursor *c = cursor->get_parent();
    if (c->get_dupecache_index())
      op->set_referenced_dupe(c->get_dupecache_index());

    cursor->couple_to_op(op);

    // all other cursors need to increment their dupe index, if their
    // index is > this cursor's index
    increment_dupe_index(context, node, c, c->get_dupecache_index());
  }

  // append journal entry
  if (lenv()->journal()) {
    lenv()->journal()->append_insert(this, context->txn, key, record,
              flags & UPS_DUPLICATE ? flags : flags | UPS_OVERWRITE,
              op->get_lsn());
  }

  ups_assert(st == 0);
  return (0);
}

bool
LocalDatabase::is_modified_by_active_transaction()
{
  if (m_txn_index) {
    TransactionNode *node = m_txn_index->get_first();
    while (node) {
      TransactionOperation *op = node->get_newest_op();
      while (op) {
        Transaction *optxn = op->get_txn();
        // ignore aborted transactions
        // if the transaction is still active, or if it is committed
        // but was not yet flushed then return an error
        if (!optxn->is_aborted()) {
          if (!optxn->is_committed()
              || !(op->get_flags() & TransactionOperation::kIsFlushed)) {
            ups_trace(("cannot close a Database that is modified by "
                   "a currently active Transaction"));
            return (true);
          }
        }
        op = op->get_previous_in_node();
      }
      node = node->get_next_sibling();
    }
  }
  return (false);
}

bool
LocalDatabase::is_key_erased(Context *context, ups_key_t *key)
{
  /* get the node for this key (but don't create a new one if it does
   * not yet exist) */
  TransactionNode *node = m_txn_index->get(key, 0);
  if (!node)
    return (false);

  /* now traverse the tree, check if the key was erased */
  TransactionOperation *op = node->get_newest_op();
  while (op) {
    Transaction *optxn = op->get_txn();
    if (optxn->is_aborted())
      ; /* nop */
    else if (optxn->is_committed() || context->txn == optxn) {
      if (op->get_flags() & TransactionOperation::kIsFlushed)
        ; /* continue */
      else if (op->get_flags() & TransactionOperation::kErase) {
        /* TODO does not check duplicates!! */
        return (true);
      }
      else if ((op->get_flags() & TransactionOperation::kInsert)
          || (op->get_flags() & TransactionOperation::kInsertOverwrite)
          || (op->get_flags() & TransactionOperation::kInsertDuplicate)) {
        return (false);
      }
    }

    op = op->get_previous_in_node();
  }

  return (false);
}

ups_status_t
LocalDatabase::find_txn(Context *context, LocalCursor *cursor,
                ups_key_t *key, ups_record_t *record, uint32_t flags)
{
  ups_status_t st = 0;
  TransactionOperation *op = 0;
  bool first_loop = true;
  bool exact_is_erased = false;

  ByteArray *pkey_arena = &key_arena(context->txn);
  ByteArray *precord_arena = &record_arena(context->txn);

  ups_key_set_intflags(key,
        (ups_key_get_intflags(key) & (~BtreeKey::kApproximate)));

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
    else if (optxn->is_committed() || context->txn == optxn) {
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
            && !(ups_key_get_intflags(key) & BtreeKey::kApproximate))
          exact_is_erased = true;
        first_loop = false;
        if (flags & UPS_FIND_LT_MATCH) {
          node = node->get_previous_sibling();
          if (!node)
            break;
          ups_key_set_intflags(key,
              (ups_key_get_intflags(key) | BtreeKey::kApproximate));
          goto retry;
        }
        else if (flags & UPS_FIND_GT_MATCH) {
          node = node->get_next_sibling();
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
        if (op->get_referenced_dupe() > 1) {
          // not the first dupe - there are other dupes
          st = 0;
        }
        else if (op->get_referenced_dupe() == 1) {
          // check if there are other dupes
          bool is_equal;
          (void)cursor->sync(context, LocalCursor::kSyncOnlyEqualKeys, &is_equal);
          if (!is_equal) // TODO merge w/ line above?
            cursor->set_to_nil(LocalCursor::kBtree);
          st = cursor->get_dupecache_count(context) ? 0 : UPS_KEY_NOT_FOUND;
        }
        return (st);
      }
      /* if the key already exists then return its record; do not
       * return pointers to TransactionOperation::get_record, because it may be
       * flushed and the user's pointers would be invalid */
      else if ((op->get_flags() & TransactionOperation::kInsert)
          || (op->get_flags() & TransactionOperation::kInsertOverwrite)
          || (op->get_flags() & TransactionOperation::kInsertDuplicate)) {
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
          return (LocalDatabase::copy_record(this, context->txn, op, record));
        return (0);
      }
      else if (!(op->get_flags() & TransactionOperation::kNop)) {
        ups_assert(!"shouldn't be here");
        return (UPS_KEY_NOT_FOUND);
      }
    }
    else { /* txn is still active */
      return (UPS_TXN_CONFLICT);
    }

    op = op->get_previous_in_node();
  }

  /*
   * if there was an approximate match: check if the btree provides
   * a better match
   */
  if (op && ups_key_get_intflags(key) & BtreeKey::kApproximate) {
    ups_key_t *k = op->get_node()->get_key();

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
      st = m_btree_index->find(context, cursor, key, pkey_arena, record,
                      precord_arena, new_flags);
    } while (st == 0 && is_key_erased(context, key));

    // if the key was not found in the btree: return the key which was found
    // in the transaction tree
    if (st == UPS_KEY_NOT_FOUND) {
      if (!(key->flags & UPS_KEY_USER_ALLOC) && txnkey.data) {
        pkey_arena->resize(txnkey.size);
        key->data = pkey_arena->get_ptr();
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
        return (LocalDatabase::copy_record(this, context->txn, op, record));
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
    int cmp = m_btree_index->compare_keys(key, &txnkey);
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
      ups_assert(!"shouldn't be here");

    if (use_btree) {
      // lookup again, with the same flags and the btree key.
      // this will check if the key was erased or overwritten
      // in a transaction
      st = find_txn(context, cursor, key, record, flags | UPS_FIND_EQ_MATCH);
      if (st == 0)
        ups_key_set_intflags(key,
          (ups_key_get_intflags(key) | BtreeKey::kApproximate));
      return (st);
    }
    else { // use txn
      if (!(key->flags & UPS_KEY_USER_ALLOC) && txnkey.data) {
        pkey_arena->resize(txnkey.size);
        key->data = pkey_arena->get_ptr();
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
        return (LocalDatabase::copy_record(this, context->txn, op, record));
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
  return (m_btree_index->find(context, cursor, key, pkey_arena, record,
                          precord_arena, flags));
}

ups_status_t
LocalDatabase::erase_txn(Context *context, ups_key_t *key, uint32_t flags,
                TransactionCursor *cursor)
{
  ups_status_t st = 0;
  TransactionOperation *op;
  bool node_created = false;
  LocalCursor *pc = 0;
  if (cursor)
    pc = cursor->get_parent();

  /* get (or create) the node for this key */
  TransactionNode *node = m_txn_index->get(key, 0);
  if (!node) {
    node = new TransactionNode(this, key);
    node_created = true;
    // TODO only store when the operation is successful?
    m_txn_index->store(node);
  }

  /* check for conflicts of this key - but only if we're not erasing a
   * duplicate key. dupes are checked for conflicts in _local_cursor_move TODO that function no longer exists */
  if (!pc || (!pc->get_dupecache_index())) {
    st = check_erase_conflicts(context, node, key, flags);
    if (st) {
      if (node_created) {
        m_txn_index->remove(node);
        delete node;
      }
      return (st);
    }
  }

  /* append a new operation to this node */
  op = node->append(context->txn, flags, TransactionOperation::kErase,
                  lenv()->next_lsn(), key, 0);

  /* is this function called through ups_cursor_erase? then add the
   * duplicate ID */
  if (cursor) {
    if (pc->get_dupecache_index())
      op->set_referenced_dupe(pc->get_dupecache_index());
  }

  /* the current op has no cursors attached; but if there are any
   * other ops in this node and in this transaction, then they have to
   * be set to nil. This only nil's txn-cursors! */
  nil_all_cursors_in_node(context->txn, pc, node);

  /* in addition we nil all btree cursors which are coupled to this key */
  nil_all_cursors_in_btree(context, pc, node->get_key());

  /* append journal entry */
  if (lenv()->journal()) {
    lenv()->journal()->append_erase(this, context->txn, key, 0,
                    flags | UPS_ERASE_ALL_DUPLICATES, op->get_lsn());
  }

  ups_assert(st == 0);
  return (0);
}

ups_status_t
LocalDatabase::create(Context *context, PBtreeHeader *btree_header)
{
  /* the header page is now modified */
  Page *header = lenv()->page_manager()->fetch(context, 0);
  header->set_dirty(true);

  /* set the flags; strip off run-time (per session) flags for the btree */
  uint32_t persistent_flags = get_flags();
  persistent_flags &= ~(UPS_CACHE_UNLIMITED
            | UPS_DISABLE_MMAP
            | UPS_ENABLE_FSYNC
            | UPS_READ_ONLY
            | UPS_AUTO_RECOVERY
            | UPS_ENABLE_TRANSACTIONS);

  switch (m_config.key_type) {
    case UPS_TYPE_UINT8:
      m_config.key_size = 1;
      break;
    case UPS_TYPE_UINT16:
      m_config.key_size = 2;
      break;
    case UPS_TYPE_REAL32:
    case UPS_TYPE_UINT32:
      m_config.key_size = 4;
      break;
    case UPS_TYPE_REAL64:
    case UPS_TYPE_UINT64:
      m_config.key_size = 8;
      break;
  }

  // if we cannot fit at least 10 keys in a page then refuse to continue
  if (m_config.key_size != UPS_KEY_SIZE_UNLIMITED) {
    if (lenv()->config().page_size_bytes / (m_config.key_size + 8) < 10) {
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
  if (m_config.record_size != UPS_RECORD_SIZE_UNLIMITED) {
    if (m_config.record_size <= 8
        || (m_config.record_size <= kInlineRecordThreshold
          && lenv()->config().page_size_bytes
                / (m_config.key_size + m_config.record_size) > 500)) {
      persistent_flags |= UPS_FORCE_RECORDS_INLINE;
      m_config.flags |= UPS_FORCE_RECORDS_INLINE;
    }
  }

  // create the btree
  m_btree_index.reset(new BtreeIndex(this, btree_header, persistent_flags,
                        m_config.key_type, m_config.key_size));

  if (m_config.key_compressor)
    enable_key_compression(context, m_config.key_compressor);
  if (m_config.record_compressor)
    enable_record_compression(context, m_config.record_compressor);

  /* initialize the btree */
  m_btree_index->create(context, m_config.key_type, m_config.key_size,
                  m_config.record_size, m_config.compare_name);

  /* load the custom compare function? */
  if (m_config.key_type == UPS_TYPE_CUSTOM) {
    ups_compare_func_t func = CallbackManager::get(m_btree_index->compare_hash());
    // silently ignore errors as long as db_set_compare_func is in place
    if (func != 0)
      set_compare_func(func);
  }

  /* the header page is now dirty */
  header->set_dirty(true);

  /* and the TransactionIndex */
  m_txn_index.reset(new TransactionIndex(this));

  return (0);
}

ups_status_t
LocalDatabase::open(Context *context, PBtreeHeader *btree_header)
{
  /*
   * set the database flags; strip off the persistent flags that may have been
   * set by the caller, before mixing in the persistent flags as obtained
   * from the btree.
   */
  uint32_t flags = get_flags();
  flags &= ~(UPS_CACHE_UNLIMITED
            | UPS_DISABLE_MMAP
            | UPS_ENABLE_FSYNC
            | UPS_READ_ONLY
            | UPS_AUTO_RECOVERY
            | UPS_ENABLE_TRANSACTIONS);

  m_config.key_type = btree_header->key_type();
  m_config.key_size = btree_header->key_size();

  /* is key compression enabled? */
  m_config.key_compressor = btree_header->key_compression();

  /* create the BtreeIndex */
  m_btree_index.reset(new BtreeIndex(this, btree_header,
                            flags | btree_header->flags(),
                            btree_header->key_type(),
                            btree_header->key_size()));

  ups_assert(!(m_btree_index->flags() & UPS_CACHE_UNLIMITED));
  ups_assert(!(m_btree_index->flags() & UPS_DISABLE_MMAP));
  ups_assert(!(m_btree_index->flags() & UPS_ENABLE_FSYNC));
  ups_assert(!(m_btree_index->flags() & UPS_READ_ONLY));
  ups_assert(!(m_btree_index->flags() & UPS_AUTO_RECOVERY));
  ups_assert(!(m_btree_index->flags() & UPS_ENABLE_TRANSACTIONS));

  /* initialize the btree */
  m_btree_index->open();

  /* create the TransactionIndex - TODO only if txn's are enabled? */
  m_txn_index.reset(new TransactionIndex(this));

  /* merge the non-persistent database flag with the persistent flags from
   * the btree index */
  m_config.flags = config().flags | m_btree_index->flags();
  m_config.key_size = m_btree_index->key_size();
  m_config.key_type = m_btree_index->key_type();
  m_config.record_size = m_btree_index->record_size();

  /* load the custom compare function? */
  if (m_config.key_type == UPS_TYPE_CUSTOM) {
    ups_compare_func_t func = CallbackManager::get(m_btree_index->compare_hash());
    if (func == 0) {
      ups_trace(("custom compare function is not yet registered"));
      return (UPS_NOT_READY);
    }
    set_compare_func(func);
  }

  /* is record compression enabled? */
  int algo = btree_header->record_compression();
  if (algo) {
    enable_record_compression(context, algo);
    m_record_compressor.reset(CompressorFactory::create(algo));
  }

  // fetch the current record number
  if ((get_flags() & (UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64))) {
    ups_key_t key = {};
    LocalCursor *c = new LocalCursor(this, 0);
    ups_status_t st = cursor_move_impl(context, c, &key, 0, UPS_CURSOR_LAST);
    cursor_close(c);
    if (st)
      return (st == UPS_KEY_NOT_FOUND ? 0 : st);

    if (get_flags() & UPS_RECORD_NUMBER32)
      m_recno = *(uint32_t *)key.data;
    else
      m_recno = *(uint64_t *)key.data;
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
LocalDatabase::fill_metrics(ups_env_metrics_t *metrics)
{
  metrics->btree_leaf_metrics.database_name = name();
  metrics->btree_internal_metrics.database_name = name();

  try {
    MetricsVisitor visitor(metrics);
    Context context(lenv(), 0, this);
    m_btree_index->visit_nodes(&context, visitor, true);

    // calculate the "avg" values
    BtreeStatistics::finalize_metrics(&metrics->btree_leaf_metrics);
    BtreeStatistics::finalize_metrics(&metrics->btree_internal_metrics);
  }
  catch (Exception &) {
  }
}

ups_status_t
LocalDatabase::get_parameters(ups_parameter_t *param)
{
  try {
    Context context(lenv(), 0, this);

    Page *page = 0;
    ups_parameter_t *p = param;

    if (p) {
      for (; p->name; p++) {
        switch (p->name) {
        case UPS_PARAM_KEY_SIZE:
          p->value = m_config.key_size;
          break;
        case UPS_PARAM_KEY_TYPE:
          p->value = m_config.key_type;
          break;
        case UPS_PARAM_RECORD_SIZE:
          p->value = m_config.record_size;
          break;
        case UPS_PARAM_FLAGS:
          p->value = (uint64_t)get_flags();
          break;
        case UPS_PARAM_DATABASE_NAME:
          p->value = (uint64_t)name();
          break;
        case UPS_PARAM_MAX_KEYS_PER_PAGE:
          p->value = 0;
          page = lenv()->page_manager()->fetch(&context,
                          m_btree_index->root_address(),
                        PageManager::kReadOnly);
          if (page) {
            BtreeNodeProxy *node = m_btree_index->get_node_from_page(page);
            p->value = node->estimate_capacity();
          }
          break;
        case UPS_PARAM_RECORD_COMPRESSION:
          p->value = btree_index()->record_compression();
          break;
        case UPS_PARAM_KEY_COMPRESSION:
          p->value = btree_index()->key_compression();
          break;
        default:
          ups_trace(("unknown parameter %d", (int)p->name));
          throw Exception(UPS_INV_PARAMETER);
        }
      }
    }
  }
  catch (Exception &ex) {
    return (ex.code);
  }
  return (0);
}

ups_status_t
LocalDatabase::check_integrity(uint32_t flags)
{
  try {
    Context context(lenv(), 0, this);

    /* purge cache if necessary */
    lenv()->page_manager()->purge_cache(&context);

    /* call the btree function */
    m_btree_index->check_integrity(&context, flags);

    /* call the txn function */
    //m_txn_index->check_integrity(flags);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
  return (0);
}

ups_status_t
LocalDatabase::count(Transaction *htxn, bool distinct, uint64_t *pcount)
{
  LocalTransaction *txn = dynamic_cast<LocalTransaction *>(htxn);

  try {
    Context context(lenv(), txn, this);

    /* purge cache if necessary */
    lenv()->page_manager()->purge_cache(&context);

    /*
     * call the btree function - this will retrieve the number of keys
     * in the btree
     */
    uint64_t keycount = m_btree_index->count(&context, distinct);

    /*
     * if transactions are enabled, then also sum up the number of keys
     * from the transaction tree
     */
    if (get_flags() & UPS_ENABLE_TRANSACTIONS)
      keycount += m_txn_index->count(&context, txn, distinct);

    *pcount = keycount;
    return (0);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ups_status_t
LocalDatabase::scan(Transaction *txn, ScanVisitor *visitor, bool distinct)
{
  ups_status_t st = 0;
  LocalCursor *cursor = 0;

  if (!(get_flags() & UPS_ENABLE_DUPLICATE_KEYS))
    distinct = true;

  try {
    Context context(lenv(), (LocalTransaction *)txn, this);

    Page *page;
    ups_key_t key = {0};

    /* purge cache if necessary */
    lenv()->page_manager()->purge_cache(&context);

    /* create a cursor, move it to the first key */
    cursor = (LocalCursor *)cursor_create_impl(txn);

    st = cursor_move_impl(&context, cursor, &key, 0, UPS_CURSOR_FIRST);
    if (st)
      goto bail;

    /* only transaction keys? then use a regular cursor */
    if (!cursor->is_coupled_to_btree()) {
      do {
        /* process the key */
        (*visitor)(key.data, key.size, distinct
                                        ? cursor->get_duplicate_count(&context)
                                        : 1);
      } while ((st = cursor_move_impl(&context, cursor, &key,
                            0, UPS_CURSOR_NEXT)) == 0);
      goto bail;
    }

    /* only btree keys? then traverse page by page */
    if (!(get_flags() & UPS_ENABLE_TRANSACTIONS)) {
      ups_assert(cursor->is_coupled_to_btree());

      do {
        // get the coupled page
        cursor->get_btree_cursor()->get_coupled_key(&page);
        BtreeNodeProxy *node = m_btree_index->get_node_from_page(page);
        // and let the btree node perform the remaining work
        node->scan(&context, visitor, 0, distinct);
      } while (cursor->get_btree_cursor()->move_to_next_page(&context) == 0);

      goto bail;
    }

    /* mixed txn/btree load? if there are btree nodes which are NOT modified
     * in transactions then move the scan to the btree node. Otherwise use
     * a regular cursor */
    while (true) {
      if (!cursor->is_coupled_to_btree())
        break;

      int slot;
      cursor->get_btree_cursor()->get_coupled_key(&page, &slot);
      BtreeNodeProxy *node = m_btree_index->get_node_from_page(page);

      /* are transactions present? then check if the next txn key is >= btree[0]
       * and <= btree[n] */
      ups_key_t *txnkey = 0;
      if (cursor->get_txn_cursor()->get_coupled_op())
        txnkey = cursor->get_txn_cursor()->get_coupled_op()->get_node()->get_key();
      // no (more) transactional keys left - process the current key, then
      // scan the remaining keys directly in the btree
      if (!txnkey) {
        /* process the key */
        (*visitor)(key.data, key.size, distinct
                                        ? cursor->get_duplicate_count(&context)
                                        : 1);
        break;
      }

      /* if yes: use the cursor to traverse the page */
      if (node->compare(&context, txnkey, 0) >= 0
          && node->compare(&context, txnkey, node->get_count() - 1) <= 0) {
        do {
          Page *new_page = 0;
          if (cursor->is_coupled_to_btree())
            cursor->get_btree_cursor()->get_coupled_key(&new_page);
          /* break the loop if we've reached the next page */
          if (new_page && new_page != page) {
            page = new_page;
            break;
          }
          /* process the key */
          (*visitor)(key.data, key.size, distinct
                                        ? cursor->get_duplicate_count(&context)
                                        : 1);
        } while ((st = cursor_move_impl(&context, cursor, &key,
                                0, UPS_CURSOR_NEXT)) == 0);

        if (st != UPS_SUCCESS)
          goto bail;
      }
      else {
        /* Otherwise traverse directly in the btree page. This is the fastest
         * code path. */
        node->scan(&context, visitor, slot, distinct);
        /* and then move to the next page */
        if (cursor->get_btree_cursor()->move_to_next_page(&context) != 0)
          break;
      }
    }

    /* pick up the remaining transactional keys */
    while ((st = cursor_move_impl(&context, cursor, &key,
                            0, UPS_CURSOR_NEXT)) == 0) {
      (*visitor)(key.data, key.size, distinct
                                     ? cursor->get_duplicate_count(&context)
                                     : 1);
    }

bail:
    if (cursor) {
      cursor->close();
      delete cursor;
    }
    return (st == UPS_KEY_NOT_FOUND ? 0 : st);
  }
  catch (Exception &ex) {
    if (cursor) {
      cursor->close();
      delete cursor;
    }
    return (ex.code);
  }
}

ups_status_t
LocalDatabase::insert(Cursor *hcursor, Transaction *txn, ups_key_t *key,
            ups_record_t *record, uint32_t flags)
{
  LocalCursor *cursor = (LocalCursor *)hcursor;
  Context context(lenv(), (LocalTransaction *)txn, this);

  try {
    if (m_config.flags & (UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64)) {
      if (key->size == 0 && key->data == 0) {
        // ok!
      }
      else if (key->size == 0 && key->data != 0) {
        ups_trace(("for record number keys set key size to 0, "
                               "key->data to null"));
        return (UPS_INV_PARAMETER);
      }
      else if (key->size != m_config.key_size) {
        ups_trace(("invalid key size (%u instead of %u)",
              key->size, m_config.key_size));
        return (UPS_INV_KEY_SIZE);
      }
    }
    else if (m_config.key_size != UPS_KEY_SIZE_UNLIMITED
        && key->size != m_config.key_size) {
      ups_trace(("invalid key size (%u instead of %u)",
            key->size, m_config.key_size));
      return (UPS_INV_KEY_SIZE);
    }
    if (m_config.record_size != UPS_RECORD_SIZE_UNLIMITED
        && record->size != m_config.record_size) {
      ups_trace(("invalid record size (%u instead of %u)",
            record->size, m_config.record_size));
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
    if (get_flags() & UPS_RECORD_NUMBER64) {
      if (flags & UPS_OVERWRITE) {
        ups_assert(key->size == sizeof(uint64_t));
        ups_assert(key->data != 0);
        recno = *(uint64_t *)key->data;
      }
      else {
        /* get the record number and increment it */
        recno = next_record_number();
      }

      /* allocate memory for the key */
      if (!key->data) {
        arena->resize(sizeof(uint64_t));
        key->data = arena->get_ptr();
      }
      key->size = sizeof(uint64_t);
      *(uint64_t *)key->data = recno;

      /* A recno key is always appended sequentially */
      flags |= UPS_HINT_APPEND;
    }
    else if (get_flags() & UPS_RECORD_NUMBER32) {
      if (flags & UPS_OVERWRITE) {
        ups_assert(key->size == sizeof(uint32_t));
        ups_assert(key->data != 0);
        recno = *(uint32_t *)key->data;
      }
      else {
        /* get the record number and increment it */
        recno = next_record_number();
      }
  
      /* allocate memory for the key */
      if (!key->data) {
        arena->resize(sizeof(uint32_t));
        key->data = arena->get_ptr();
      }
      key->size = sizeof(uint32_t);
      *(uint32_t *)key->data = (uint32_t)recno;

      /* A recno key is always appended sequentially */
      flags |= UPS_HINT_APPEND;
    }

    ups_status_t st = 0;
    LocalTransaction *local_txn = 0;

    /* purge cache if necessary */
    if (!txn && (get_flags() & UPS_ENABLE_TRANSACTIONS)) {
      local_txn = begin_temp_txn();
      context.txn = local_txn;
    }

    st = insert_impl(&context, cursor, key, record, flags);
    return (finalize(&context, st, local_txn));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ups_status_t
LocalDatabase::erase(Cursor *hcursor, Transaction *txn, ups_key_t *key,
                uint32_t flags)
{
  LocalCursor *cursor = (LocalCursor *)hcursor;
  Context context(lenv(), (LocalTransaction *)txn, this);

  try {
    ups_status_t st = 0;
    LocalTransaction *local_txn = 0;

    if (cursor) {
      if (cursor->is_nil())
        throw Exception(UPS_CURSOR_IS_NIL);
      if (cursor->is_coupled_to_txnop()) // TODO rewrite the next line, it's ugly
        key = cursor->get_txn_cursor()->get_coupled_op()->get_node()->get_key();
      else // cursor->is_coupled_to_btree()
        key = 0;
    }

    if (key) {
      if (m_config.key_size != UPS_KEY_SIZE_UNLIMITED
          && key->size != m_config.key_size) {
        ups_trace(("invalid key size (%u instead of %u)",
              key->size, m_config.key_size));
        return (UPS_INV_KEY_SIZE);
      }
    }

    if (!txn && (get_flags() & UPS_ENABLE_TRANSACTIONS)) {
      local_txn = begin_temp_txn();
      context.txn = local_txn;
    }

    st = erase_impl(&context, cursor, key, flags);
    return (finalize(&context, st, local_txn));
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ups_status_t
LocalDatabase::find(Cursor *hcursor, Transaction *txn, ups_key_t *key,
            ups_record_t *record, uint32_t flags)
{
  LocalCursor *cursor = (LocalCursor *)hcursor;
  Context context(lenv(), (LocalTransaction *)txn, this);

  try {
    ups_status_t st = 0;

    /* Duplicates AND Transactions require a Cursor because only
     * Cursors can build lists of duplicates.
     * TODO not exception safe - if find() throws then the cursor is not closed
     */
    if (!cursor
          && (get_flags() & (UPS_ENABLE_DUPLICATE_KEYS|UPS_ENABLE_TRANSACTIONS))) {
      LocalCursor *c = (LocalCursor *)cursor_create_impl(txn);
      st = find(c, txn, key, record, flags);
      c->close();
      delete c;
      return (st);
    }

    if (m_config.key_size != UPS_KEY_SIZE_UNLIMITED
        && key->size != m_config.key_size) {
      ups_trace(("invalid key size (%u instead of %u)",
            key->size, m_config.key_size));
      return (UPS_INV_KEY_SIZE);
    }

    // cursor: reset the dupecache, set to nil
    if (cursor)
      cursor->set_to_nil(LocalCursor::kBoth);

    st = find_impl(&context, cursor, key, record, flags);
    if (st)
      return (finalize(&context, st, 0));

    if (cursor) {
      // make sure that txn-cursor and btree-cursor point to the same keys
      if (get_flags() & UPS_ENABLE_TRANSACTIONS) {
        bool is_equal;
        (void)cursor->sync(&context, LocalCursor::kSyncOnlyEqualKeys, &is_equal);
        if (!is_equal && cursor->is_coupled_to_txnop())
          cursor->set_to_nil(LocalCursor::kBtree);
      }

      /* if the key has duplicates: build a duplicate table, then couple to the
       * first/oldest duplicate */
      if (cursor->get_dupecache_count(&context, true)) {
        cursor->couple_to_dupe(1); // 1-based index!
        if (record) { // TODO don't copy record if it was already
                      // copied in find_impl
          if (cursor->is_coupled_to_txnop())
            cursor->get_txn_cursor()->copy_coupled_record(record);
          else {
            Transaction *txn = cursor->get_txn();
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
  catch (Exception &ex) {
    return (ex.code);
  }
}

Cursor *
LocalDatabase::cursor_create_impl(Transaction *txn)
{
  return (new LocalCursor(this, txn));
}

Cursor *
LocalDatabase::cursor_clone_impl(Cursor *hsrc)
{
  return (new LocalCursor(*(LocalCursor *)hsrc));
}

ups_status_t
LocalDatabase::cursor_move(Cursor *hcursor, ups_key_t *key,
                ups_record_t *record, uint32_t flags)
{
  LocalCursor *cursor = (LocalCursor *)hcursor;

  try {
    Context context(lenv(), (LocalTransaction *)cursor->get_txn(),
            this);

    return (cursor_move_impl(&context, cursor, key, record, flags));
  }
  catch (Exception &ex) {
    return (ex.code);
  } 
}

ups_status_t
LocalDatabase::cursor_move_impl(Context *context, LocalCursor *cursor,
                ups_key_t *key, ups_record_t *record, uint32_t flags)
{
  /* purge cache if necessary */
  lenv()->page_manager()->purge_cache(context);

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
  st = cursor->move(context, key, record, flags);

  /* store the direction */
  if (flags & UPS_CURSOR_NEXT)
    cursor->set_last_operation(UPS_CURSOR_NEXT);
  else if (flags & UPS_CURSOR_PREVIOUS)
    cursor->set_last_operation(UPS_CURSOR_PREVIOUS);
  else
    cursor->set_last_operation(0);

  if (st) {
    if (st == UPS_KEY_ERASED_IN_TXN)
      st = UPS_KEY_NOT_FOUND;
    /* trigger a sync when the function is called again */
    cursor->set_last_operation(0);
    return (st);
  }

  return (0);
}

ups_status_t
LocalDatabase::close_impl(uint32_t flags)
{
  Context context(lenv(), 0, this);

  if (is_modified_by_active_transaction()) {
    ups_trace(("cannot close a Database that is modified by "
               "a currently active Transaction"));
    return (UPS_TXN_STILL_OPEN);
  }

  /* in-memory-database: free all allocated blobs */
  if (m_btree_index && m_env->get_flags() & UPS_IN_MEMORY)
   m_btree_index->drop(&context);

  /*
   * flush all pages of this database (but not the header page,
   * it's still required and will be flushed below)
   */
  lenv()->page_manager()->close_database(&context, this);

  return (0);
}

void 
LocalDatabase::increment_dupe_index(Context *context, TransactionNode *node,
                LocalCursor *skip, uint32_t start)
{
  LocalCursor *c = (LocalCursor *)m_cursor_list;

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
    else if (c->get_btree_cursor()->points_to(context, node->get_key())) {
      hit = true;
    }

    if (hit) {
      if (c->get_dupecache_index() > start)
        c->set_dupecache_index(c->get_dupecache_index() + 1);
    }

next:
    c = (LocalCursor *)c->get_next();
  }
}

void
LocalDatabase::nil_all_cursors_in_node(LocalTransaction *txn,
                LocalCursor *current, TransactionNode *node)
{
  TransactionOperation *op = node->get_newest_op();
  while (op) {
    TransactionCursor *cursor = op->cursor_list();
    while (cursor) {
      LocalCursor *parent = cursor->get_parent();
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
      parent->set_to_nil(LocalCursor::kTxn);
      // set a flag that the cursor just completed an Insert-or-find
      // operation; this information is needed in ups_cursor_move
      // (in this aspect, an erase is the same as insert/find)
      parent->set_last_operation(LocalCursor::kLookupOrInsert);

      cursor = op->cursor_list();
    }

    op = op->get_previous_in_node();
  }
}

ups_status_t
LocalDatabase::copy_record(LocalDatabase *db, Transaction *txn,
                TransactionOperation *op, ups_record_t *record)
{
  ByteArray *arena = &db->record_arena(txn);

  if (!(record->flags & UPS_RECORD_USER_ALLOC)) {
    arena->resize(op->get_record()->size);
    record->data = arena->get_ptr();
  }
  memcpy(record->data, op->get_record()->data, op->get_record()->size);
  record->size = op->get_record()->size;
  return (0);
}

void
LocalDatabase::nil_all_cursors_in_btree(Context *context, LocalCursor *current,
                ups_key_t *key)
{
  LocalCursor *c = (LocalCursor *)m_cursor_list;

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
    c = (LocalCursor *)c->get_next();
  }
}

ups_status_t
LocalDatabase::select_range(SelectStatement *stmt, LocalCursor **begin,
                const LocalCursor *end, uqi_result_t *result)
{
  ups_status_t st = 0;
  Context context(lenv(), 0, this);
  Page *page = 0;
  int slot;
  ups_key_t key = {0};
  LocalCursor *cursor = *begin;
  std::auto_ptr<ScanVisitor> visitor(ScanVisitorFactory::from_select(stmt,
                                                this));
  if (!visitor.get())
    return (UPS_PARSER_ERROR);

  try {
    /* purge cache if necessary */
    lenv()->page_manager()->purge_cache(&context);

    /* create a cursor, move it to the first key */
    if (!cursor) {
      cursor = (LocalCursor *)cursor_create_impl(0);
      st = cursor_move_impl(&context, cursor, &key, 0, UPS_CURSOR_FIRST);
      if (st)
        goto bail;
    }

    /* process transactional keys at the beginning */
    if ((get_flags() & UPS_ENABLE_TRANSACTIONS) != 0) {
      while (!cursor->is_coupled_to_btree()) {
        /* process the key */
        (*visitor)(key.data, key.size, stmt->distinct
                                ? cursor->get_duplicate_count(&context)
                                : 1);
        st = cursor_move_impl(&context, cursor, &key, 0, UPS_CURSOR_NEXT);
        if (st)
          goto bail;
      }
    }

    /*
     * now move forward over all leaf pages; if any key in the page is modified
     * in a transaction then use a cursor to traverse the page.
     * otherwise let the btree do the processing.
     */
    while (true) {
      bool mixed_load = false;

      cursor->get_btree_cursor()->get_coupled_key(&page, &slot);
      BtreeNodeProxy *node = m_btree_index->get_node_from_page(page);

      if (get_flags() & UPS_ENABLE_TRANSACTIONS) {
        ups_key_t *txnkey = 0;
        if (cursor->get_txn_cursor()->get_coupled_op())
          txnkey = cursor->get_txn_cursor()->get_coupled_op()->
                                get_node()->get_key();
        // no (more) transactional keys left - process the current key, then
        // scan the remaining keys directly in the btree
        if (!txnkey) {
          /* process the key */
          (*visitor)(key.data, key.size, stmt->distinct
                                ? cursor->get_duplicate_count(&context)
                                : 1);
        }
        else if (node->compare(&context, txnkey, 0) >= 0
            && node->compare(&context, txnkey, node->get_count() - 1) <= 0)
          mixed_load = true;
      }

      // no transactional data: the Btree will do the work. This is the
      // fastest code path
      if (mixed_load == false) {
        node->scan(&context, visitor.get(), 0, stmt->distinct);
        st = cursor->get_btree_cursor()->move_to_next_page(&context);
        if (st)
          goto bail;
      }
      // mixed txn/btree load? if there are btree nodes which are NOT modified
      // in transactions then move the scan to the btree node. Otherwise use
      // a regular cursor
      else {
        do {
          Page *new_page = 0;
          if (cursor->is_coupled_to_btree())
            cursor->get_btree_cursor()->get_coupled_key(&new_page);
          /* break the loop if we've reached the next page */
          if (new_page && new_page != page) {
            page = new_page;
            break;
          }
          /* process the key */
          (*visitor)(key.data, key.size, stmt->distinct
                                ? cursor->get_duplicate_count(&context)
                                : 1);
        } while ((st = cursor_move_impl(&context, cursor, &key,
                                0, UPS_CURSOR_NEXT)) == 0);
      }
    }

    /* pick up the remaining transactional keys */
    while ((st = cursor_move_impl(&context, cursor, &key,
                            0, UPS_CURSOR_NEXT)) == 0) {
      (*visitor)(key.data, key.size, stmt->distinct
                            ? cursor->get_duplicate_count(&context)
                            : 1);
    }

    /* now fetch the results */
    visitor->assign_result(result);

bail:
    if (cursor && *begin == 0) {
      cursor->close();
      delete cursor;
    }
    return (st == UPS_KEY_NOT_FOUND ? 0 : st);
  }
  catch (Exception &ex) {
    if (cursor) {
      cursor->close();
      delete cursor;
    }
    return (ex.code);
  }
}

ups_status_t
LocalDatabase::flush_txn_operation(Context *context, LocalTransaction *txn,
                TransactionOperation *op)
{
  ups_status_t st = 0;
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
          ? UPS_DUPLICATE
          : UPS_OVERWRITE;

    LocalCursor *c1 = op->cursor_list()
                            ? op->cursor_list()->get_parent()
                            : 0;

    /* ignore cursor if it's coupled to btree */
    if (!c1 || c1->is_coupled_to_btree()) {
      st = m_btree_index->insert(context, 0, node->get_key(), op->get_record(),
                  op->get_orig_flags() | additional_flag);
    }
    else {
      /* pick the first cursor, get the parent/btree cursor and
       * insert the key/record pair in the btree. The btree cursor
       * then will be coupled to this item. */
      st = m_btree_index->insert(context, c1, node->get_key(), op->get_record(),
                  op->get_orig_flags() | additional_flag);
      if (!st) {
        /* uncouple the cursor from the txn-op, and remove it */
        c1->couple_to_btree(); // TODO merge these two calls
        c1->set_to_nil(LocalCursor::kTxn);

        /* all other (txn) cursors need to be coupled to the same
         * item as the first one. */
        TransactionCursor *tc2;
        while ((tc2 = op->cursor_list())) {
          LocalCursor *c2 = tc2->get_parent();
          c2->get_btree_cursor()->clone(c1->get_btree_cursor());
          c2->couple_to_btree(); // TODO merge these two calls
          c2->set_to_nil(LocalCursor::kTxn);
        }
      }
    }
  }
  else if (op->get_flags() & TransactionOperation::kErase) {
    st = m_btree_index->erase(context, 0, node->get_key(),
                  op->get_referenced_dupe(), op->get_flags());
    if (st == UPS_KEY_NOT_FOUND)
      st = 0;
  }

  return (st);
}

ups_status_t
LocalDatabase::drop(Context *context)
{
  m_btree_index->drop(context);
  return (0);
}

ups_status_t
LocalDatabase::insert_impl(Context *context, LocalCursor *cursor,
                ups_key_t *key, ups_record_t *record, uint32_t flags)
{
  ups_status_t st = 0;

  lenv()->page_manager()->purge_cache(context);

  /*
   * if transactions are enabled: only insert the key/record pair into
   * the Transaction structure. Otherwise immediately write to the btree.
   */
  if (context->txn || m_env->get_flags() & UPS_ENABLE_TRANSACTIONS)
    st = insert_txn(context, key, record, flags, cursor
                                                ? cursor->get_txn_cursor()
                                                : 0);
  else
    st = m_btree_index->insert(context, cursor, key, record, flags);

  // couple the cursor to the inserted key
  if (st == 0 && cursor) {
    if (m_env->get_flags() & UPS_ENABLE_TRANSACTIONS) {
      DupeCache *dc = cursor->get_dupecache();
      // TODO required? should have happened in insert_txn
      cursor->couple_to_txnop();
      /* the cursor is coupled to the txn-op; nil the btree-cursor to
       * trigger a sync() call when fetching the duplicates */
      // TODO merge with the line above
      cursor->set_to_nil(LocalCursor::kBtree);

      /* if duplicate keys are enabled: set the duplicate index of
       * the new key  */
      if (st == 0 && cursor->get_dupecache_count(context, true)) {
        TransactionOperation *op = cursor->get_txn_cursor()->get_coupled_op();
        ups_assert(op != 0);

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
     * operation; this information is needed in ups_cursor_move */
    cursor->set_last_operation(LocalCursor::kLookupOrInsert);
  }

  return (st);
}

ups_status_t
LocalDatabase::find_impl(Context *context, LocalCursor *cursor,
                ups_key_t *key, ups_record_t *record, uint32_t flags)
{
  /* purge cache if necessary */
  lenv()->page_manager()->purge_cache(context);

  /*
   * if transactions are enabled: read keys from transaction trees,
   * otherwise read immediately from disk
   */
  if (context->txn || m_env->get_flags() & UPS_ENABLE_TRANSACTIONS)
    return (find_txn(context, cursor, key, record, flags));

  return (m_btree_index->find(context, cursor, key, &key_arena(context->txn),
                          record, &record_arena(context->txn), flags));
}

ups_status_t
LocalDatabase::erase_impl(Context *context, LocalCursor *cursor, ups_key_t *key,
                uint32_t flags)
{
  ups_status_t st = 0;

  /*
   * if transactions are enabled: append a 'erase key' operation into
   * the txn tree; otherwise immediately erase the key from disk
   */
  if (context->txn || m_env->get_flags() & UPS_ENABLE_TRANSACTIONS) {
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
        st = erase_txn(context, cursor->get_btree_cursor()->get_uncoupled_key(),
                        0, cursor->get_txn_cursor());
      }
      /* case 2 described above */
      else {
        // TODO this line is ugly
        st = erase_txn(context, 
                        cursor->get_txn_cursor()->get_coupled_op()->get_key(),
                        0, cursor->get_txn_cursor());
      }
    }
    else {
      st = erase_txn(context, key, flags, 0);
    }
  }
  else {
    st = m_btree_index->erase(context, cursor, key, 0, flags);
  }

  /* on success: 'nil' the cursor */
  if (cursor && st == 0) {
    cursor->set_to_nil(0);
    ups_assert(cursor->get_txn_cursor()->is_nil());
    ups_assert(cursor->is_nil(0));
  }

  return (st);
}

ups_status_t
LocalDatabase::finalize(Context *context, ups_status_t status,
                Transaction *local_txn)
{
  LocalEnvironment *env = lenv();

  if (status) {
    if (local_txn) {
      context->changeset.clear();
      env->txn_manager()->abort(local_txn);
    }
    return (status);
  }

  if (local_txn) {
    context->changeset.clear();
    env->txn_manager()->commit(local_txn);
  }
  return (0);
}

LocalTransaction *
LocalDatabase::begin_temp_txn()
{
  LocalTransaction *txn;
  ups_status_t st = lenv()->txn_begin((Transaction **)&txn, 0,
                        UPS_TXN_TEMPORARY | UPS_DONT_LOCK);
  if (st)
    throw Exception(st);
  return (txn);
}

void
LocalDatabase::enable_record_compression(Context *context, int algo)
{
  m_record_compressor.reset(CompressorFactory::create(algo));
  m_btree_index->set_record_compression(context, algo);
}

void
LocalDatabase::enable_key_compression(Context *context, int algo)
{
  m_key_compression_algo = algo;
  m_btree_index->set_key_compression(context, algo);
}

} // namespace upscaledb
