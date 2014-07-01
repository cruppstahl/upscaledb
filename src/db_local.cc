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

#include "config.h"

#include "blob_manager.h"
#include "btree_index.h"
#include "btree_index_factory.h"
#include "btree_cursor.h"
#include "btree_stats.h"
#include "cursor.h"
#include "db_local.h"
#include "device.h"
#include "journal.h"
#include "mem.h"
#include "os.h"
#include "page.h"
#include "page_manager.h"
#include "txn_local.h"
#include "txn_cursor.h"
#include "version.h"

namespace hamsterdb {

ham_status_t
LocalDatabase::check_insert_conflicts(LocalTransaction *txn,
            TransactionNode *node, ham_key_t *key, ham_u32_t flags)
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
      /* TODO txn_set_conflict_txn(txn, optxn); */
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
  if ((flags & HAM_OVERWRITE) || (flags & HAM_DUPLICATE))
    return (0);

  ham_status_t st = m_btree_index->find(0, 0, key, 0, flags);

  get_local_env()->get_changeset().clear();

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
            TransactionNode *node, ham_key_t *key, ham_u32_t flags)
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
      /* TODO txn_set_conflict_txn(txn, optxn); */
      return (HAM_TXN_CONFLICT);
    }

    op = op->get_previous_in_node();
  }

  /*
   * we've successfully checked all un-flushed transactions and there
   * were no conflicts. Now check all transactions which are already
   * flushed - basically that's identical to a btree lookup.
   */
  ham_status_t st = m_btree_index->find(0, 0, key, 0, flags);
  get_local_env()->get_changeset().clear();
  return (st);
}

ham_status_t
LocalDatabase::insert_txn(LocalTransaction *txn, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags,
            TransactionCursor *cursor)
{
  ham_status_t st = 0;
  TransactionOperation *op;
  bool node_created = false;

  /* get (or create) the node for this key */
  TransactionNode *node = get_txn_index()->get(key, 0);
  if (!node) {
    node = new TransactionNode(this, key);
    node_created = true;
    // TODO only store when the operation is successful?
    get_txn_index()->store(node);
  }

  // check for conflicts of this key
  //
  // !!
  // afterwards, clear the changeset; check_insert_conflicts()
  // checks if a key already exists, and this fills the changeset
  st = check_insert_conflicts(txn, node, key, flags);
  if (st) {
    if (node_created) {
      get_txn_index()->remove(node);
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
LocalDatabase::find_txn(LocalTransaction *txn, ham_key_t *key,
                ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st = 0;
  TransactionOperation *op = 0;
  BtreeIndex *be = get_btree_index();
  bool first_loop = true;
  bool exact_is_erased = false;

  ByteArray *arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
            ? &get_key_arena()
            : &txn->get_key_arena();

  ham_key_set_intflags(key,
        (ham_key_get_intflags(key) & (~BtreeKey::kApproximate)));

  /* get the node for this key (but don't create a new one if it does
   * not yet exist) */
  TransactionNode *node = get_txn_index()->get(key, flags);

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
          ham_key_set_intflags(key,
              (ham_key_get_intflags(key) | BtreeKey::kApproximate));
          goto retry;
        }
        else if (flags & HAM_FIND_GT_MATCH) {
          node = node->get_next_sibling();
          ham_key_set_intflags(key,
              (ham_key_get_intflags(key) | BtreeKey::kApproximate));
          goto retry;
        }
        return (HAM_KEY_NOT_FOUND);
      }
      /* if the key already exists then return its record; do not
       * return pointers to TransactionOperation::get_record, because it may be
       * flushed and the user's pointers would be invalid */
      else if ((op->get_flags() & TransactionOperation::kInsert)
          || (op->get_flags() & TransactionOperation::kInsertOverwrite)
          || (op->get_flags() & TransactionOperation::kInsertDuplicate)) {
        // approx match? leave the loop and continue
        // with the btree
        if (ham_key_get_intflags(key) & BtreeKey::kApproximate)
          break;
        // otherwise copy the record and return
        return (LocalDatabase::copy_record(this, txn, op, record));
      }
      else if (!(op->get_flags() & TransactionOperation::kNop)) {
        ham_assert(!"shouldn't be here");
        return (HAM_KEY_NOT_FOUND);
      }
    }
    else { /* txn is still active */
      /* TODO txn_set_conflict_txn(txn, optxn); */
      return (HAM_TXN_CONFLICT);
    }

    op = op->get_previous_in_node();
  }

  /*
   * if there was an approximate match: check if the btree provides
   * a better match
   */
  if (op && ham_key_get_intflags(key) & BtreeKey::kApproximate) {
    ham_key_t txnkey = {0};
    ham_key_t *k = op->get_node()->get_key();
    txnkey.size = k->size;
    txnkey._flags = BtreeKey::kApproximate;
    txnkey.data = Memory::allocate<ham_u8_t>(txnkey.size);
    memcpy(txnkey.data, k->data, txnkey.size);

    ham_key_set_intflags(key, 0);

    // the "exact match" key was erased? then don't fetch it again
    if (exact_is_erased)
      flags = flags & (~HAM_FIND_EXACT_MATCH);

    // now lookup in the btree
    st = be->find(txn, 0, key, record, flags);
    if (st == HAM_KEY_NOT_FOUND) {
      if (!(key->flags & HAM_KEY_USER_ALLOC) && txnkey.data) {
        arena->resize(txnkey.size);
        key->data = arena->get_ptr();
      }
      if (txnkey.data) {
        memcpy(key->data, txnkey.data, txnkey.size);
        Memory::release(txnkey.data);
      }
      key->size = txnkey.size;
      key->_flags = txnkey._flags;

      return (LocalDatabase::copy_record(this, txn, op, record));
    }
    else if (st)
      return (st);
    // the btree key is a direct match? then return it
    if ((!(ham_key_get_intflags(key) & BtreeKey::kApproximate))
        && (flags & HAM_FIND_EXACT_MATCH)) {
      Memory::release(txnkey.data);
      return (0);
    }
    // if there's an approx match in the btree: compare both keys and
    // use the one that is closer. if the btree is closer: make sure
    // that it was not erased or overwritten in a transaction
    int cmp = get_btree_index()->compare_keys(key, &txnkey);
    bool use_btree = false;
    if (flags & HAM_FIND_GT_MATCH) {
      if (cmp < 0)
        use_btree = true;
    }
    else if (flags & HAM_FIND_LT_MATCH) {
      if (cmp > 0)
        use_btree = true;
    }
    else
      ham_assert(!"shouldn't be here");

    if (use_btree) {
      Memory::release(txnkey.data);
      // lookup again, with the same flags and the btree key.
      // this will check if the key was erased or overwritten
      // in a transaction
      st = find_txn(txn, key, record, flags | HAM_FIND_EXACT_MATCH);
      if (st == 0)
        ham_key_set_intflags(key,
          (ham_key_get_intflags(key) | BtreeKey::kApproximate));
      return (st);
    }
    else { // use txn
      if (!(key->flags & HAM_KEY_USER_ALLOC) && txnkey.data) {
        arena->resize(txnkey.size);
        key->data = arena->get_ptr();
      }
      if (txnkey.data) {
        memcpy(key->data, txnkey.data, txnkey.size);
        Memory::release(txnkey.data);
      }
      key->size = txnkey.size;
      key->_flags = txnkey._flags;

      return (LocalDatabase::copy_record(this, txn, op, record));
    }
  }

  /*
   * no approximate match:
   *
   * we've successfully checked all un-flushed transactions and there
   * were no conflicts, and we have not found the key: now try to
   * lookup the key in the btree.
   */
  return (be->find(txn, 0, key, record, flags));
}

ham_status_t
LocalDatabase::erase_txn(LocalTransaction *txn, ham_key_t *key, ham_u32_t flags,
        TransactionCursor *cursor)
{
  ham_status_t st = 0;
  TransactionOperation *op;
  bool node_created = false;
  Cursor *pc = 0;
  if (cursor)
    pc = cursor->get_parent();

  /* get (or create) the node for this key */
  TransactionNode *node = get_txn_index()->get(key, 0);
  if (!node) {
    node = new TransactionNode(this, key);
    node_created = true;
    // TODO only store when the operation is successful?
    get_txn_index()->store(node);
  }

  /* check for conflicts of this key - but only if we're not erasing a
   * duplicate key. dupes are checked for conflicts in _local_cursor_move */
  if (!pc || (!pc->get_dupecache_index())) {
    st = check_erase_conflicts(txn, node, key, flags);
    if (st) {
      if (node_created) {
        get_txn_index()->remove(node);
        delete node;
      }
      return (st);
    }
  }

  /* append a new operation to this node */
  op = node->append(txn, flags, TransactionOperation::kErase,
                  get_local_env()->get_incremented_lsn(), key, 0);

  /* is this function called through ham_cursor_erase? then add the
   * duplicate ID */
  if (cursor) {
    if (pc->get_dupecache_index())
      op->set_referenced_dupe(pc->get_dupecache_index());
  }

  /* the current op has no cursors attached; but if there are any
   * other ops in this node and in this transaction, then they have to
   * be set to nil. This only nil's txn-cursors! */
  nil_all_cursors_in_node(txn, pc, node);

  /* in addition we nil all btree cursors which are coupled to this key */
  nil_all_cursors_in_btree(pc, node->get_key());

  /* append journal entry */
  if (m_env->get_flags() & HAM_ENABLE_RECOVERY
      && m_env->get_flags() & HAM_ENABLE_TRANSACTIONS) {
    Journal *j = get_local_env()->get_journal();
    j->append_erase(this, txn, key, 0, flags | HAM_ERASE_ALL_DUPLICATES,
              op->get_lsn());
  }

  ham_assert(st == 0);
  return (0);
}

ham_status_t
LocalDatabase::open(ham_u16_t descriptor)
{
  /*
   * set the database flags; strip off the persistent flags that may have been
   * set by the caller, before mixing in the persistent flags as obtained
   * from the btree.
   */
  ham_u32_t flags = get_rt_flags();
  flags &= ~(HAM_CACHE_UNLIMITED
            | HAM_DISABLE_MMAP
            | HAM_ENABLE_FSYNC
            | HAM_READ_ONLY
            | HAM_ENABLE_RECOVERY
            | HAM_AUTO_RECOVERY
            | HAM_ENABLE_TRANSACTIONS);

  PBtreeHeader *desc = get_local_env()->get_btree_descriptor(descriptor);

  /* create the BtreeIndex */
  m_btree_index = new BtreeIndex(this, descriptor, flags | desc->get_flags(),
                            desc->get_key_type(), desc->get_key_size());

  ham_assert(!(m_btree_index->get_flags() & HAM_CACHE_UNLIMITED));
  ham_assert(!(m_btree_index->get_flags() & HAM_DISABLE_MMAP));
  ham_assert(!(m_btree_index->get_flags() & HAM_ENABLE_FSYNC));
  ham_assert(!(m_btree_index->get_flags() & HAM_READ_ONLY));
  ham_assert(!(m_btree_index->get_flags() & HAM_ENABLE_RECOVERY));
  ham_assert(!(m_btree_index->get_flags() & HAM_AUTO_RECOVERY));
  ham_assert(!(m_btree_index->get_flags() & HAM_ENABLE_TRANSACTIONS));

  /* initialize the btree */
  m_btree_index->open();

  /* create the TransactionIndex - TODO only if txn's are enabled? */
  m_txn_index = new TransactionIndex(this);

  /* merge the non-persistent database flag with the persistent flags from
   * the btree index */
  m_rt_flags = get_rt_flags(true) | m_btree_index->get_flags();

  if ((get_rt_flags() & HAM_RECORD_NUMBER) == 0)
    return (0);

  ham_key_t key = {};
  Cursor *c = new Cursor(this, 0, 0);
  ham_status_t st = cursor_move(c, &key, 0, HAM_CURSOR_LAST);
  cursor_close(c);
  if (st)
    return (st == HAM_KEY_NOT_FOUND ? 0 : st);

  ham_assert(key.size == sizeof(ham_u64_t));
  m_recno = *(ham_u64_t *)key.data;

  return (0);
}

ham_status_t
LocalDatabase::create(ham_u16_t descriptor, ham_u16_t key_type,
                        ham_u16_t key_size, ham_u32_t rec_size)
{
  /* set the flags; strip off run-time (per session) flags for the btree */
  ham_u32_t persistent_flags = get_rt_flags();
  persistent_flags &= ~(HAM_CACHE_UNLIMITED
            | HAM_DISABLE_MMAP
            | HAM_ENABLE_FSYNC
            | HAM_READ_ONLY
            | HAM_ENABLE_RECOVERY
            | HAM_AUTO_RECOVERY
            | HAM_ENABLE_TRANSACTIONS);

  switch (key_type) {
    case HAM_TYPE_UINT8:
      key_size = 1;
      break;
    case HAM_TYPE_UINT16:
      key_size = 2;
      break;
    case HAM_TYPE_REAL32:
    case HAM_TYPE_UINT32:
      key_size = 4;
      break;
    case HAM_TYPE_REAL64:
    case HAM_TYPE_UINT64:
      key_size = 8;
      break;
  }

  // if we cannot fit at least 10 keys in a page then refuse to continue
  if (key_size != HAM_KEY_SIZE_UNLIMITED) {
    if (get_local_env()->get_page_size() / (key_size + 8) < 10) {
      ham_trace(("key size too large; either increase page_size or decrease "
                "key size"));
      return (HAM_INV_KEY_SIZE);
    }
  }

  // fixed length records:
  //
  // if records are <= 8 bytes OR if we can fit at least 500 keys AND
  // records into the leaf then store the records in the leaf;
  // otherwise they're allocated as a blob
  if (rec_size != HAM_RECORD_SIZE_UNLIMITED) {
    if (rec_size <= 8
        || (rec_size <= kInlineRecordThreshold
          && get_local_env()->get_page_size() / (key_size + rec_size) > 500)) {
      persistent_flags |= HAM_FORCE_RECORDS_INLINE;
      m_rt_flags |= HAM_FORCE_RECORDS_INLINE;
    }
  }

  // create the btree
  m_btree_index = new BtreeIndex(this, descriptor, persistent_flags,
                        key_type, key_size);

  /* initialize the btree */
  m_btree_index->create(key_type, key_size, rec_size);

  /* and the TransactionIndex */
  m_txn_index = new TransactionIndex(this);

  return (0);
}

ham_status_t
LocalDatabase::get_parameters(ham_parameter_t *param)
{
  Page *page = 0;
  ham_parameter_t *p = param;

  ham_assert(get_btree_index() != 0);

  if (p) {
    for (; p->name; p++) {
      switch (p->name) {
      case HAM_PARAM_KEY_SIZE:
        p->value = (ham_u64_t)get_key_size();
        break;
      case HAM_PARAM_KEY_TYPE:
        p->value = (ham_u64_t)get_key_type();
        break;
      case HAM_PARAM_RECORD_SIZE:
        p->value = (ham_u64_t)get_record_size();
        break;
      case HAM_PARAM_FLAGS:
        p->value = (ham_u64_t)get_rt_flags();
        break;
      case HAM_PARAM_DATABASE_NAME:
        p->value = (ham_u64_t)get_name();
        break;
      case HAM_PARAM_MAX_KEYS_PER_PAGE:
        p->value = 0;
        page = get_local_env()->get_page_manager()->fetch_page(this,
                        m_btree_index->get_root_address(),
                        PageManager::kReadOnly);
        if (page) {
          BtreeNodeProxy *node = m_btree_index->get_node_from_page(page);
          p->value = node->get_capacity();
        }
        break;
      case HAM_PARAM_RECORD_COMPRESSION:
        p->value = 0;
        break;
      case HAM_PARAM_KEY_COMPRESSION:
        p->value = 0;
        break;
      default:
        ham_trace(("unknown parameter %d", (int)p->name));
        return (HAM_INV_PARAMETER);
      }
    }
  }

  return (0);
}

ham_status_t
LocalDatabase::check_integrity(ham_u32_t flags)
{
  /* purge cache if necessary */
  get_local_env()->get_page_manager()->purge_cache();

  /* call the btree function */
  m_btree_index->check_integrity(flags);
  get_local_env()->get_changeset().clear();

  return (0);
}

void
LocalDatabase::count(Transaction *htxn, bool distinct,
                ham_u64_t *pkeycount)
{
  LocalTransaction *txn = dynamic_cast<LocalTransaction *>(htxn);

  /* purge cache if necessary */
  get_local_env()->get_page_manager()->purge_cache();

  /*
   * call the btree function - this will retrieve the number of keys
   * in the btree
   */
  *pkeycount = m_btree_index->count(distinct);

  /*
   * if transactions are enabled, then also sum up the number of keys
   * from the transaction tree
   */
  if (get_rt_flags() & HAM_ENABLE_TRANSACTIONS)
    *pkeycount += m_txn_index->count(txn, distinct);

  get_local_env()->get_changeset().clear();
}

void
LocalDatabase::scan(Transaction *txn, ScanVisitor *visitor,
                bool distinct)
{
  Page *page;
  ham_key_t key = {0};

  /* purge cache if necessary */
  get_local_env()->get_page_manager()->purge_cache();

  /* create a cursor, move it to the first key */
  Cursor *cursor = cursor_create(txn, 0);

  ham_status_t st = cursor_move(cursor, &key, 0, HAM_CURSOR_FIRST);
  if (st) {
    cursor_close(cursor);
    throw Exception(st);
  }

  /* only transaction keys? then use a regular cursor */
  if (!cursor->is_coupled_to_btree()) {
    do {
      /* process the key */
      (*visitor)(key.data, key.size, distinct
                                        ? cursor->get_record_count(txn, 0)
                                        : 1);
    } while ((st = cursor_move(cursor, &key, 0, HAM_CURSOR_NEXT)) == 0);
    goto bail;
  }

  /* only btree keys? then traverse page by page */
  if (!(get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    ham_assert(cursor->is_coupled_to_btree());

    do {
      // get the coupled page
      cursor->get_btree_cursor()->get_coupled_key(&page);
      BtreeNodeProxy *node = get_btree_index()->get_node_from_page(page);
      // and let the btree node perform the remaining work
      node->scan(visitor, 0, distinct);

    } while (cursor->get_btree_cursor()->move_to_next_page() == 0);

    goto bail;
  }

  /* mixed txn/btree load? if there are btree nodes which are NOT modified
   * in transactions then move the scan to the btree node. Otherwise use
   * a regular cursor */
  while (true) {
    if (!cursor->is_coupled_to_btree())
      break;

    ham_u32_t slot;
    cursor->get_btree_cursor()->get_coupled_key(&page, &slot);
    BtreeNodeProxy *node = get_btree_index()->get_node_from_page(page);

    /* are transactions present? then check if the next txn key is >= btree[0]
     * and <= btree[n] */
    ham_key_t *txnkey = 0;
    if (cursor->get_txn_cursor()->get_coupled_op())
      txnkey = cursor->get_txn_cursor()->get_coupled_op()->get_node()->get_key();
    // no (more) transactional keys left - process the current key, then
    // scan the remaining keys directly in the btree
    if (!txnkey) {
      /* process the key */
      (*visitor)(key.data, key.size, distinct
                                          ? cursor->get_record_count(txn, 0)
                                          : 1);
      break;
    }

    /* if yes: use the cursor to traverse the page */
    if (node->compare(txnkey, 0) >= 0
        && node->compare(txnkey, node->get_count() - 1) <= 0) {
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
                                          ? cursor->get_record_count(txn, 0)
                                          : 1);
      } while ((st = cursor_move(cursor, &key, 0, HAM_CURSOR_NEXT)) == 0);

      if (st == HAM_KEY_NOT_FOUND)
        goto bail;
      if (st != HAM_SUCCESS) {
        cursor->close();
        throw Exception(st);
      }
    }
    else {
      /* otherwise traverse directly in the btree page */
      node->scan(visitor, slot, distinct);
      /* and then move to the next page */
      if (cursor->get_btree_cursor()->move_to_next_page() != 0)
        break;
    }
  }

  /* pick up the remaining transactional keys */
  while ((st = cursor_move(cursor, &key, 0, HAM_CURSOR_NEXT)) == 0) {
    (*visitor)(key.data, key.size, distinct
                                      ? cursor->get_record_count(txn, 0)
                                      : 1);
  }

bail:
  // TODO not exception safe! call close() in Cursor::~Cursor()?
  cursor_close(cursor);
  get_local_env()->get_changeset().clear();
}

ham_status_t
LocalDatabase::insert(Transaction *htxn, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
  LocalTransaction *local_txn = 0;
  LocalTransaction *txn = dynamic_cast<LocalTransaction *>(htxn);

  if (get_rt_flags() & HAM_RECORD_NUMBER) {
    if (key->size != 0 && key->size != get_key_size()) {
      ham_trace(("invalid record number key size (%u instead of 0 or %u)",
            key->size, get_key_size()));
      return (HAM_INV_KEY_SIZE);
    }
  }
  else if (get_key_size() != HAM_KEY_SIZE_UNLIMITED
      && key->size != get_key_size()) {
    ham_trace(("invalid key size (%u instead of %u)",
          key->size, get_key_size()));
    return (HAM_INV_KEY_SIZE);
  }
  if (get_record_size() != HAM_RECORD_SIZE_UNLIMITED
      && record->size != get_record_size()) {
    ham_trace(("invalid record size (%u instead of %u)",
          record->size, get_record_size()));
    return (HAM_INV_RECORD_SIZE);
  }

  ByteArray *arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
            ? &get_key_arena()
            : &txn->get_key_arena();

  /*
   * record number: make sure that we have a valid key structure,
   * and lazy load the last used record number
   */
  ham_u64_t recno = 0;
  if (get_rt_flags() & HAM_RECORD_NUMBER) {
    if (flags & HAM_OVERWRITE) {
      ham_assert(key->size == sizeof(ham_u64_t));
      ham_assert(key->data != 0);
      recno = *(ham_u64_t *)key->data;
    }
    else {
      /* get the record number and increment it */
      recno = get_incremented_recno();
    }

    /* allocate memory for the key */
    if (!key->data) {
      arena->resize(sizeof(ham_u64_t));
      key->data = arena->get_ptr();
    }
    key->size = sizeof(ham_u64_t);
    memcpy(key->data, &recno, sizeof(ham_u64_t));

    /* we're appending this key sequentially */
    flags |= HAM_HINT_APPEND;

    /* transactions are faster if HAM_OVERWRITE is specified */
    if (txn)
      flags |= HAM_OVERWRITE;
  }

  /* purge cache if necessary */
  get_local_env()->get_page_manager()->purge_cache();

  if (!txn && (get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    local_txn = (LocalTransaction *)get_local_env()->get_txn_manager()->begin(
                        0, HAM_TXN_TEMPORARY);
    txn = local_txn;
  }

  /*
   * if transactions are enabled: only insert the key/record pair into
   * the Transaction structure. Otherwise immediately write to the btree.
   */
  ham_status_t st;
  if (txn)
    st = insert_txn(txn, key, record, flags, 0);
  else
    st = m_btree_index->insert(0, 0, key, record, flags);

  if (st) {
    if (local_txn)
      get_local_env()->get_txn_manager()->abort(local_txn);

    if ((get_rt_flags() & HAM_RECORD_NUMBER)
        && !(flags & HAM_OVERWRITE)) {
      if (!(key->flags & HAM_KEY_USER_ALLOC)) {
        key->data = 0;
        key->size = 0;
      }
      ham_assert(st != HAM_DUPLICATE_KEY);
    }

    get_local_env()->get_changeset().clear();
    return (st);
  }

  // return the incremented record number in the key.
  if (get_rt_flags() & HAM_RECORD_NUMBER)
    key->size = sizeof(ham_u64_t);

  if (local_txn)
    get_local_env()->get_txn_manager()->commit(local_txn);
  else if (m_env->get_flags() & HAM_ENABLE_RECOVERY
      && !(m_env->get_flags() & HAM_ENABLE_TRANSACTIONS))
    get_local_env()->get_changeset().flush();

  return (0);
}

ham_status_t
LocalDatabase::erase(Transaction *htxn, ham_key_t *key, ham_u32_t flags)
{
  LocalTransaction *local_txn = 0;
  LocalTransaction *txn = dynamic_cast<LocalTransaction *>(htxn);
  ham_u64_t recno = 0;

  if (get_key_size() != HAM_KEY_SIZE_UNLIMITED
      && key->size != get_key_size()) {
    ham_trace(("invalid key size (%u instead of %u)",
          key->size, get_key_size()));
    return (HAM_INV_KEY_SIZE);
  }

  /* record number: make sure that we have a valid key structure */
  if (get_rt_flags() & HAM_RECORD_NUMBER) {
    if (key->size != sizeof(ham_u64_t) || !key->data) {
      ham_trace(("key->size must be 8, key->data must not be NULL"));
      return (HAM_INV_PARAMETER);
    }
    recno = *(ham_u64_t *)key->data;
  }

  if (!txn && (get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    local_txn = (LocalTransaction *)get_local_env()->get_txn_manager()->begin(
                        0, HAM_TXN_TEMPORARY);
    txn = local_txn;
  }

  /*
   * if transactions are enabled: append a 'erase key' operation into
   * the txn tree; otherwise immediately erase the key from disk
   */
  ham_status_t st;
  if (txn)
    st = erase_txn(txn, key, flags, 0);
  else
    st = m_btree_index->erase(0, 0, key, 0, flags);

  if (st) {
    if (local_txn)
      get_local_env()->get_txn_manager()->abort(local_txn);

    get_local_env()->get_changeset().clear();
    return (st);
  }

  if (get_rt_flags() & HAM_RECORD_NUMBER)
    *(ham_u64_t *)key->data = recno;

  if (local_txn)
    get_local_env()->get_txn_manager()->commit(local_txn);
  else if (m_env->get_flags() & HAM_ENABLE_RECOVERY
      && !(m_env->get_flags() & HAM_ENABLE_TRANSACTIONS))
    get_local_env()->get_changeset().flush();

  return (0);
}

ham_status_t
LocalDatabase::find(Transaction *htxn, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st;
  LocalTransaction *txn = dynamic_cast<LocalTransaction *>(htxn);

  ham_u64_t recno = 0;

  if (get_key_size() != HAM_KEY_SIZE_UNLIMITED
      && key->size != get_key_size()) {
    ham_trace(("invalid key size (%u instead of %u)",
          key->size, get_key_size()));
    return (HAM_INV_KEY_SIZE);
  }

  /* purge cache if necessary */
  get_local_env()->get_page_manager()->purge_cache();

  /* if this database has duplicates, then we use ham_cursor_find
   * because we have to build a duplicate list, and this is currently
   * only available in ham_cursor_find
   *
   * TODO create cursor on the stack and avoid the memory allocation!
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

  /*
   * if transactions are enabled: read keys from transaction trees,
   * otherwise read immediately from disk
   */
  if (get_rt_flags() & HAM_ENABLE_TRANSACTIONS)
    st = find_txn(txn, key, record, flags);
  else
    st = m_btree_index->find(0, 0, key, record, flags);

  get_local_env()->get_changeset().clear();

  if (get_rt_flags() & HAM_RECORD_NUMBER)
    *(ham_u64_t *)key->data = recno;

  return (st);
}

Cursor *
LocalDatabase::cursor_create_impl(Transaction *txn, ham_u32_t flags)
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
        ham_record_t *record, ham_u32_t flags)
{
  ham_u64_t recno = 0;
  LocalTransaction *local_txn = 0;
  LocalTransaction *txn = dynamic_cast<LocalTransaction *>(cursor->get_txn());

  ByteArray *arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
            ? &get_key_arena()
            : &txn->get_key_arena();

  if (get_rt_flags() & HAM_RECORD_NUMBER) {
    if (key->size != 0 && key->size != get_key_size()) {
      ham_trace(("invalid record number key size (%u instead of 0 or %u)",
            key->size, get_key_size()));
      return (HAM_INV_KEY_SIZE);
    }
  }
  else if (get_key_size() != HAM_KEY_SIZE_UNLIMITED
      && key->size != get_key_size()) {
    ham_trace(("invalid key size (%u instead of %u)",
          key->size, get_key_size()));
    return (HAM_INV_KEY_SIZE);
  }
  if (get_record_size() != HAM_RECORD_SIZE_UNLIMITED
      && record->size != get_record_size()) {
    ham_trace(("invalid record size (%u instead of %u)",
          record->size, get_record_size()));
    return (HAM_INV_RECORD_SIZE);
  }

  /*
   * record number: make sure that we have a valid key structure,
   * and lazy load the last used record number
   */
  if (get_rt_flags() & HAM_RECORD_NUMBER) {
    if (flags & HAM_OVERWRITE) {
      ham_assert(key->size == sizeof(ham_u64_t));
      ham_assert(key->data != 0);
      recno = *(ham_u64_t *)key->data;
    }
    else {
      /* get the record number and increment it */
      recno = get_incremented_recno();
    }

    /* allocate memory for the key */
    if (!key->data) {
      arena->resize(sizeof(ham_u64_t));
      key->data = arena->get_ptr();
      key->size = sizeof(ham_u64_t);
    }

    memcpy(key->data, &recno, sizeof(ham_u64_t));
    key->size = sizeof(ham_u64_t);

    /* we're appending this key sequentially */
    flags |= HAM_HINT_APPEND;

    /* transactions are faster if HAM_OVERWRITE is specified */
    if (cursor->get_txn())
      flags |= HAM_OVERWRITE;
  }

  /* purge cache if necessary */
  get_local_env()->get_page_manager()->purge_cache();

  /* if user did not specify a transaction, but transactions are enabled:
   * create a temporary one */
  if (!cursor->get_txn() && (get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    local_txn = (LocalTransaction *)get_local_env()->get_txn_manager()->begin(
                        0, HAM_TXN_TEMPORARY);
    cursor->set_txn(local_txn);
  }

  ham_status_t st;
  if (cursor->get_txn() || local_txn) {
    st = insert_txn((LocalTransaction *)(cursor->get_txn()
                      ? cursor->get_txn()
                      : local_txn),
                key, record, flags, cursor->get_txn_cursor());
    if (st == 0) {
      DupeCache *dc = cursor->get_dupecache();
      cursor->couple_to_txnop();
      /* reset the dupecache, otherwise cursor->get_dupecache_count()
       * does not update the dupecache correctly */
      dc->clear();
      /* if duplicate keys are enabled: set the duplicate index of
       * the new key  */
      if (st == 0 && cursor->get_dupecache_count()) {
        TransactionCursor *txnc = cursor->get_txn_cursor();
        TransactionOperation *op = txnc->get_coupled_op();
        ham_assert(op != 0);

        for (ham_u32_t i = 0; i < dc->get_count(); i++) {
          DupeCacheLine *l = dc->get_element(i);
          if (!l->use_btree() && l->get_txn_op() == op) {
            cursor->set_dupecache_index(i + 1);
            break;
          }
        }
      }
      get_local_env()->get_changeset().clear();
    }
  }
  else {
    st = cursor->get_btree_cursor()->insert(key, record, flags);
    if (st == 0)
      cursor->couple_to_btree();
  }

  /* if we created a temp. txn then clean it up again */
  if (local_txn)
    cursor->set_txn(0);

  if (st) {
    if (local_txn)
      get_local_env()->get_txn_manager()->abort(local_txn);

    if ((get_rt_flags() & HAM_RECORD_NUMBER) && !(flags & HAM_OVERWRITE)) {
      if (!(key->flags & HAM_KEY_USER_ALLOC)) {
        key->data = 0;
        key->size = 0;
      }
      ham_assert(st != HAM_DUPLICATE_KEY);
      // fall through
    }

    get_local_env()->get_changeset().clear();
    return (st);
  }

  /* no need to append the journal entry - it's appended in insert_txn() */

  /* store the incremented record number */
  if (get_rt_flags() & HAM_RECORD_NUMBER) {
    memcpy(key->data, &recno, sizeof(ham_u64_t));
    key->size = sizeof(ham_u64_t);
  }

  /* set a flag that the cursor just completed an Insert-or-find
   * operation; this information is needed in ham_cursor_move */
  cursor->set_lastop(Cursor::kLookupOrInsert);

  if (local_txn)
    get_local_env()->get_txn_manager()->commit(local_txn);
  else if (m_env->get_flags() & HAM_ENABLE_RECOVERY
      && !(m_env->get_flags() & HAM_ENABLE_TRANSACTIONS))
    get_local_env()->get_changeset().flush();

  return (0);
}

ham_status_t
LocalDatabase::cursor_erase(Cursor *cursor, ham_u32_t flags)
{
  Transaction *local_txn = 0;

  /* if user did not specify a transaction, but transactions are enabled:
   * create a temporary one */
  if (!cursor->get_txn() && (get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    local_txn = get_local_env()->get_txn_manager()->begin(0, HAM_TXN_TEMPORARY);
    cursor->set_txn(local_txn);
  }

  /* this function will do all the work */
  ham_status_t st = cursor->erase(cursor->get_txn()
                                    ? cursor->get_txn()
                                    : local_txn,
                                  flags);

  /* clear the changeset */
  get_local_env()->get_changeset().clear();

  /* if we created a temp. txn then clean it up again */
  if (local_txn)
    cursor->set_txn(0);

  /* on success: verify that cursor is now nil */
  if (st == 0) {
    cursor->couple_to_btree();
    ham_assert(cursor->get_txn_cursor()->is_nil());
    ham_assert(cursor->is_nil(0));
    cursor->clear_dupecache();
  }
  else {
    if (local_txn)
      get_local_env()->get_txn_manager()->abort(local_txn);
    get_local_env()->get_changeset().clear();
    return (st);
  }

  ham_assert(st == 0);

  /* no need to append the journal entry - it's appended in erase_txn(),
   * which is called by txn_cursor_erase() */

  if (local_txn)
    get_local_env()->get_txn_manager()->commit(local_txn);
  else if (m_env->get_flags() & HAM_ENABLE_RECOVERY
      && !(m_env->get_flags() & HAM_ENABLE_TRANSACTIONS))
    get_local_env()->get_changeset().flush();

  return (0);
}

ham_status_t
LocalDatabase::cursor_find(Cursor *cursor, ham_key_t *key,
          ham_record_t *record, ham_u32_t flags)
{
  TransactionCursor *txnc = cursor->get_txn_cursor();

  if (get_key_size() != HAM_KEY_SIZE_UNLIMITED
      && key->size != get_key_size()) {
    ham_trace(("invalid key size (%u instead of %u)",
          key->size, get_key_size()));
    return (HAM_INV_KEY_SIZE);
  }

  if (get_rt_flags() & HAM_RECORD_NUMBER) {
    if (key->size != sizeof(ham_u64_t) || !key->data) {
      ham_trace(("key->size must be 8, key->data must not be NULL"));
      return (HAM_INV_PARAMETER);
    }
  }

  /* purge cache if necessary */
  get_local_env()->get_page_manager()->purge_cache();

  /* reset the dupecache */
  cursor->clear_dupecache();

  cursor->set_to_nil(Cursor::kBoth);

  /*
   * first try to find the key in the transaction tree. If it exists and
   * is NOT a duplicate then return its record. If it does not exist or
   * it has duplicates then lookup the key in the btree.
   *
   * in non-Transaction mode directly search through the btree.
   */
  ham_status_t st;
  if (cursor->get_txn() || (get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    st = cursor->get_txn_cursor()->find(key, flags);
    /* if the key was erased in a transaction then fail with an error
     * (unless we have duplicates - they're checked below) */
    if (st) {
      if (st == HAM_KEY_NOT_FOUND)
        goto btree;
      if (st == HAM_KEY_ERASED_IN_TXN) {
        /* performance hack: if coupled op erases ALL duplicates
         * then we know that the key no longer exists. if coupled op
         * references a single duplicate w/ index > 1 then
         * we know that there are other duplicates. if coupled op
         * references the FIRST duplicate (idx 1) then we have
         * to check if there are other duplicates */
        TransactionOperation *op = txnc->get_coupled_op();
        ham_assert(op->get_flags() & TransactionOperation::kErase);
        if (!op->get_referenced_dupe()) {
          // ALL!
          st = HAM_KEY_NOT_FOUND;
        }
        else if (op->get_referenced_dupe() > 1) {
          // not the first dupe - there are other dupes
          st = 0;
        }
        else if (op->get_referenced_dupe() == 1) {
          // check if there are other dupes
          bool is_equal;
          (void)cursor->sync(Cursor::kSyncOnlyEqualKeys, &is_equal);
          if (!is_equal)
            cursor->set_to_nil(Cursor::kBtree);
          if (!cursor->get_dupecache_count())
            st = HAM_KEY_NOT_FOUND;
          else
            st = 0;
        }
      }
      if (st)
        goto bail;
    }
    else {
      bool is_equal;
      (void)cursor->sync(Cursor::kSyncOnlyEqualKeys, &is_equal);
      if (!is_equal)
        cursor->set_to_nil(Cursor::kBtree);
    }
    cursor->couple_to_txnop();
    if (!cursor->get_dupecache_count()) {
      if (record)
        txnc->copy_coupled_record(record);
      goto bail;
    }
    if (st == 0)
      goto check_dupes;
  }

btree:
  st = cursor->get_btree_cursor()->find(key, record, flags);
  if (st == 0) {
    cursor->couple_to_btree();
    /* if btree keys were found: reset the dupecache. The previous
     * call to cursor_get_dupecache_count() already initialized the
     * dupecache, but only with txn keys because the cursor was only
     * coupled to the txn */
    cursor->clear_dupecache();
  }

check_dupes:
  /* if the key has duplicates: build a duplicate table, then
   * couple to the first/oldest duplicate */
  if (cursor->get_dupecache_count()) {
    DupeCacheLine *e = cursor->get_dupecache()->get_first_element();
    if (e->use_btree())
      cursor->couple_to_btree();
    else
      cursor->couple_to_txnop();
    cursor->couple_to_dupe(1);
    st = 0;

    /* now read the record */
    if (record) {
      /* TODO this works, but in case of the btree key w/ duplicates
      * it's possible that we read the record twice. I'm not sure if
      * this can be avoided, though. */
      if (cursor->is_coupled_to_txnop())
        cursor->get_txn_cursor()->copy_coupled_record(record);
      else
        st = cursor->get_btree_cursor()->move(0, record, 0);
    }
  }
  else {
    if (cursor->is_coupled_to_txnop() && record)
      cursor->get_txn_cursor()->copy_coupled_record(record);
  }

bail:
  get_local_env()->get_changeset().clear();

  if (st)
    return (st);

  /* set a flag that the cursor just completed an Insert-or-find
   * operation; this information is needed in ham_cursor_move */
  cursor->set_lastop(Cursor::kLookupOrInsert);

  return (0);
}

ham_status_t
LocalDatabase::cursor_get_record_count(Cursor *cursor,
          ham_u32_t *count, ham_u32_t flags)
{
  TransactionCursor *txnc = cursor->get_txn_cursor();

  if (cursor->is_nil(0) && txnc->is_nil())
    return (HAM_CURSOR_IS_NIL);

  /* this function will do all the work */
  *count = cursor->get_record_count(
            cursor->get_txn() ? cursor->get_txn() : 0,
            flags);

  /* set a flag that the cursor just completed an Insert-or-find
   * operation; this information is needed in ham_cursor_move */
  cursor->set_lastop(Cursor::kLookupOrInsert);

  get_local_env()->get_changeset().clear();

  return (0);
}

ham_u32_t
LocalDatabase::cursor_get_duplicate_position(Cursor *cursor)
{
  TransactionCursor *txnc = cursor->get_txn_cursor();

  if (cursor->is_nil(0) && txnc->is_nil())
    throw Exception(HAM_CURSOR_IS_NIL);

  /* this function will do all the work */
  return (cursor->get_dupecache_index());
}

ham_status_t
LocalDatabase::cursor_get_record_size(Cursor *cursor, ham_u64_t *size)
{
  TransactionCursor *txnc = cursor->get_txn_cursor();

  /* purge cache if necessary */
  get_local_env()->get_page_manager()->purge_cache();

  if (cursor->is_nil(0) && txnc->is_nil())
    return (HAM_CURSOR_IS_NIL);

  /* this function will do all the work */
  *size = cursor->get_record_size(
                cursor->get_txn() ? cursor->get_txn() : 0);

  get_local_env()->get_changeset().clear();

  /* set a flag that the cursor just completed an Insert-or-find
   * operation; this information is needed in ham_cursor_move */
  cursor->set_lastop(Cursor::kLookupOrInsert);

  return (0);
}

ham_status_t
LocalDatabase::cursor_overwrite(Cursor *cursor,
          ham_record_t *record, ham_u32_t flags)
{
  Transaction *local_txn = 0;

  /* purge cache if necessary */
  get_local_env()->get_page_manager()->purge_cache();

  /* if user did not specify a transaction, but transactions are enabled:
   * create a temporary one */
  if (!cursor->get_txn() && (get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    local_txn = get_local_env()->get_txn_manager()->begin(0, HAM_TXN_TEMPORARY);
    cursor->set_txn(local_txn);
  }

  /* this function will do all the work */
  ham_status_t st = cursor->overwrite(cursor->get_txn()
                                        ? cursor->get_txn()
                                        : local_txn,
                                      record, flags);

  /* if we created a temp. txn then clean it up again */
  if (local_txn)
    cursor->set_txn(0);

  if (st) {
    if (local_txn)
      get_local_env()->get_txn_manager()->abort(local_txn);
    get_local_env()->get_changeset().clear();
    return (st);
  }

  /* the journal entry is appended in insert_txn() */

  if (local_txn)
    get_local_env()->get_txn_manager()->commit(local_txn);
  else if (m_env->get_flags() & HAM_ENABLE_RECOVERY
      && !(m_env->get_flags() & HAM_ENABLE_TRANSACTIONS))
    get_local_env()->get_changeset().flush();

  return (0);
}

ham_status_t
LocalDatabase::cursor_move(Cursor *cursor, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
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
    st = cursor->get_btree_cursor()->move(key, record, flags);
    get_local_env()->get_changeset().clear();
    return (st);
  }

  /* everything else is handled by the cursor function */
  st = cursor->move(key, record, flags);

  get_local_env()->get_changeset().clear();

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
LocalDatabase::close_impl(ham_u32_t flags)
{
  /* check if this database is modified by an active transaction */
  TransactionIndex *tree = get_txn_index();
  if (tree) {
    TransactionNode *node = tree->get_first();
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

  /* clear the changeset */
  get_local_env()->get_changeset().clear();

  /*
   * flush all pages of this database (but not the header page,
   * it's still required and will be flushed below)
   */
  get_local_env()->get_page_manager()->close_database(this);

  /* clean up the transaction tree */
  if (m_txn_index) {
    delete m_txn_index;
    m_txn_index = 0;
  }

  /* close the btree */
  if (m_btree_index) {
    delete m_btree_index;
    m_btree_index = 0;
  }

  return (0);
}

ham_u16_t
LocalDatabase::get_key_size()
{
  return (get_btree_index()->get_key_size());
}

ham_u16_t
LocalDatabase::get_key_type()
{
  return (get_btree_index()->get_key_type());
}

ham_u32_t
LocalDatabase::get_record_size()
{
  return (get_btree_index()->get_record_size());
}

void 
LocalDatabase::increment_dupe_index(TransactionNode *node,
        Cursor *skip, ham_u32_t start)
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
    ham_u32_t additional_flag = 
      (op->get_flags() & TransactionOperation::kInsertDuplicate)
          ? HAM_DUPLICATE
          : HAM_OVERWRITE;
    if (!op->get_cursor_list()) {
      st = m_btree_index->insert(txn, 0, node->get_key(), op->get_record(),
                  op->get_orig_flags() | additional_flag);
    }
    else {
      TransactionCursor *tc2, *tc1 = op->get_cursor_list();
      Cursor *c2, *c1 = tc1->get_parent();
      /* pick the first cursor, get the parent/btree cursor and
       * insert the key/record pair in the btree. The btree cursor
       * then will be coupled to this item. */
      st = c1->get_btree_cursor()->insert(
                  node->get_key(), op->get_record(),
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
    st = m_btree_index->erase(txn, 0, node->get_key(),
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

} // namespace hamsterdb
