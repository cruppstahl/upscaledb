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

  if (NOTSET(record->flags, UPS_RECORD_USER_ALLOC)) {
    arena->resize(record->size);
    record->data = arena->data();
  }
  if (likely(op->record.data != 0))
    ::memcpy(record->data, op->record.data, record->size);
}

static inline void
copy_key(LocalDb *db, Txn *txn, ups_key_t *source, ups_key_t *key)
{
  ByteArray *arena = &db->key_arena(txn);

  key->size = source->size;
  key->_flags = source->_flags;

  if (NOTSET(key->flags, UPS_KEY_USER_ALLOC) && source->data) {
    arena->resize(source->size);
    key->data = arena->data();
  }
  if (likely(source->data != 0))
    ::memcpy(key->data, source->data, source->size);
}

static inline LocalTxn *
begin_temp_txn(LocalEnv *env)
{
  return (LocalTxn *)env->txn_begin(0, UPS_TXN_TEMPORARY | UPS_DONT_LOCK);
}

static inline ups_status_t
finalize(LocalEnv *env, Context *context, ups_status_t status, Txn *local_txn)
{
  if (unlikely(status)) {
    if (local_txn) {
      context->changeset.clear();
      env->txn_manager->abort(local_txn);
    }
    return status;
  }

  if (local_txn) {
    context->changeset.clear();
    return env->txn_manager->commit(local_txn);
  }
  return 0;
}

// Returns true if this database is modified by an active transaction
static inline bool
is_modified_by_active_transaction(TxnIndex *txn_index)
{
  assert(txn_index != 0);

  for (TxnNode *node = txn_index->first();
                  node != 0;
                  node = node->next_sibling()) {
    for (TxnOperation *op = node->newest_op;
                    op != 0;
                    op = op->previous_in_node) {
      Txn *optxn = op->txn;
      // ignore aborted transactions
      // if the transaction is still active, or if it is committed
      // but was not yet flushed then return an error
      if (!optxn->is_aborted() && !optxn->is_committed())
        if (NOTSET(op->flags, TxnOperation::kIsFlushed))
          return true;
    }
  }
  return false;
}

static inline bool
is_key_erased(Context *context, TxnIndex *txn_index, ups_key_t *key)
{
  // get the node for this key (but don't create a new one if it does
  // not yet exist)
  TxnNode *node = txn_index->get(key, 0);
  if (likely(!node))
    return false;

  // now traverse the tree, check if the key was erased
  for (TxnOperation *op = node->newest_op;
                  op != 0;
                  op = op->previous_in_node) {
    Txn *optxn = op->txn;
    if (optxn->is_aborted())
      continue;
    if (optxn->is_committed() || context->txn == optxn) {
      if (ISSET(op->flags, TxnOperation::kIsFlushed))
        continue;
      if (ISSET(op->flags, TxnOperation::kErase)) {
        // TODO does not check duplicates!!
        return true;
      }
      if (ISSETANY(op->flags, TxnOperation::kInsert
                                    | TxnOperation::kInsertOverwrite
                                    | TxnOperation::kInsertDuplicate))
        return false;
    }
  }

  return false;
}

// Checks if an erase operation conflicts with another txn; this is the
// case if the same key is modified by another active txn.
static inline ups_status_t
check_erase_conflicts(LocalDb *db, Context *context, TxnNode *node,
                    ups_key_t *key, uint32_t flags)
{
  //
  // pick the tree_node of this key, and walk through each operation
  // in reverse chronological order (from newest to oldest):
  // - is this op part of an aborted txn? then skip it
  // - is this op part of a committed txn? then look at the
  //    operation in detail
  // - is this op part of an txn which is still active? return an error
  //    because we've found a conflict
  // - if a committed txn has erased the item then there's no need
  //    to continue checking older, committed txns
  //
  for (TxnOperation *op = node->newest_op;
                  op != 0;
                  op = op->previous_in_node) {
    LocalTxn *optxn = op->txn;
    if (optxn->is_aborted())
      continue;

    if (optxn->is_committed() || context->txn == optxn) {
      if (ISSET(op->flags, TxnOperation::kIsFlushed))
        continue;
      // if key was erased then it doesn't exist and can be
      // inserted without problems
      if (ISSET(op->flags, TxnOperation::kErase))
        return UPS_KEY_NOT_FOUND;
      // if the key already exists then we can only continue if
      // we're allowed to overwrite it or to insert a duplicate
      if (ISSETANY(op->flags, TxnOperation::kInsert
                                    | TxnOperation::kInsertOverwrite
                                    | TxnOperation::kInsertDuplicate))
        return 0;
      if (NOTSET(op->flags, TxnOperation::kNop)) {
        assert(!"shouldn't be here");
        return UPS_INTERNAL_ERROR;
      }
      continue;
    }

    // txn is still active
    return UPS_TXN_CONFLICT;
  }

  // we've successfully checked all un-flushed transactions and there
  // were no conflicts. Now check all transactions which are already
  // flushed - basically that's identical to a btree lookup. Fail if the
  // key does not exist.
  return db->btree_index->find(context, 0, key, 0, 0, 0, flags);
}

// Checks if an insert operation conflicts with another txn; this is the
// case if the same key is modified by another active txn.
static inline ups_status_t
check_insert_conflicts(LocalDb *db, Context *context, TxnNode *node,
                    ups_key_t *key, uint32_t flags)
{
  //
  // pick the tree_node of this key, and walk through each operation
  // in reverse chronological order (from newest to oldest):
  // - is this op part of an aborted txn? then skip it
  // - is this op part of a committed txn? then look at the
  //    operation in detail
  // - is this op part of an txn which is still active? return an error
  //    because we've found a conflict
  // - if a committed txn has erased the item then there's no need
  //    to continue checking older, committed txns
  ///
  for (TxnOperation *op = node->newest_op;
                  op != 0;
                  op = op->previous_in_node) {
    LocalTxn *optxn = op->txn;
    if (optxn->is_aborted())
      continue;

    if (optxn->is_committed() || context->txn == optxn) {
      if (ISSET(op->flags, TxnOperation::kIsFlushed))
        continue;
      /* if key was erased then it doesn't exist and can be
       * inserted without problems */
      if (ISSET(op->flags, TxnOperation::kErase))
        return 0;
      /* if the key already exists then we can only continue if
       * we're allowed to overwrite it or to insert a duplicate */
      if (ISSETANY(op->flags, TxnOperation::kInsert
                                    | TxnOperation::kInsertOverwrite
                                    | TxnOperation::kInsertDuplicate)) {
        if (ISSETANY(flags, UPS_OVERWRITE | UPS_DUPLICATE))
          return 0;
        return UPS_DUPLICATE_KEY;
      }
      if (NOTSET(op->flags, TxnOperation::kNop)) {
        assert(!"shouldn't be here");
        return UPS_INTERNAL_ERROR;
      }
      continue;
    }

    // txn is still active
    return UPS_TXN_CONFLICT;
  }

  // we've successfully checked all un-flushed transactions and there
  // were no conflicts. Now check all transactions which are already
  // flushed - basically that's identical to a btree lookup.
  //
  // we can skip this check if we do not care about duplicates.
  if (ISSETANY(flags, UPS_OVERWRITE | UPS_DUPLICATE
                          | UPS_HINT_APPEND | UPS_HINT_PREPEND))
    return 0;

  ByteArray *arena = &db->key_arena(context->txn);
  ups_status_t st = db->btree_index->find(context, 0, key, arena, 0, 0, flags);
  switch (st) {
    case UPS_KEY_NOT_FOUND:
      return 0;
    case UPS_SUCCESS:
      return UPS_DUPLICATE_KEY;
    default:
      return st;
  }
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
  bool exact_is_erased = false;

  ByteArray *key_arena = &db->key_arena(context->txn);
  ByteArray *record_arena = &db->record_arena(context->txn);

  ups_key_set_intflags(key,
        (ups_key_get_intflags(key) & (~BtreeKey::kApproximate)));

  // cursor: reset the dupecache, set to nil
  if (cursor)
    cursor->set_to_nil();

  // get the node for this key (but don't create a new one if it does
  // not yet exist)
  TxnNode *node = db->txn_index->get(key, flags);

  //
  // pick the node of this key, and walk through each operation
  // in reverse chronological order (from newest to oldest):
  // - is this op part of an aborted txn? then skip it
  // - is this op part of a committed txn? then look at the
  //    operation in detail
  // - is this op part of an txn which is still active? return an error
  //    because we've found a conflict
  // - if a committed txn has erased the item then there's no need
  //    to continue checking older, committed txns
  //
retry:
  if (node)
    op = node->newest_op;

  for (; op != 0; op = op->previous_in_node) {
    Txn *optxn = op->txn;
    if (optxn->is_aborted())
      continue;

    if (optxn->is_committed() || context->txn == optxn) {
      if (unlikely(ISSET(op->flags, TxnOperation::kIsFlushed)))
        continue;

      // if the key already exists then return its record; do not
      // return pointers to TxnOperation::get_record, because it may be
      // flushed and the user's pointers would be invalid
      if (ISSETANY(op->flags, TxnOperation::kInsert
                                | TxnOperation::kInsertOverwrite
                                | TxnOperation::kInsertDuplicate)) {
        if (cursor)
          cursor->activate_txn(op);
        // approx match? leave the loop and continue with the btree
        if (ISSETANY(ups_key_get_intflags(key), BtreeKey::kApproximate))
          break;
        // otherwise copy the record and return
        if (likely(record != 0))
          copy_record(db, context->txn, op, record);
        return 0;
      }

      // if key was erased then it doesn't exist and we can return
      // immediately
      //
      // if an approximate match is requested then move to the next
      // or previous node
      if (ISSET(op->flags, TxnOperation::kErase)) {
        if (NOTSET(ups_key_get_intflags(key), BtreeKey::kApproximate))
          exact_is_erased = true;
        if (ISSET(flags, UPS_FIND_LT_MATCH)) {
          node = node->previous_sibling();
          if (!node)
            break;
          ups_key_set_intflags(key,
              (ups_key_get_intflags(key) | BtreeKey::kApproximate));
          goto retry;
        }
        if (ISSET(flags, UPS_FIND_GT_MATCH)) {
          node = node->next_sibling();
          if (!node)
            break;
          ups_key_set_intflags(key,
              (ups_key_get_intflags(key) | BtreeKey::kApproximate));
          goto retry;
        }
        // if a duplicate was deleted then check if there are other duplicates
        // left
        if (cursor)
          cursor->activate_txn(op);
        if (op->referenced_duplicate > 1) {
          // not the first dupe - there are other dupes
          return 0;
        }
        if (op->referenced_duplicate == 1) {
          // check if there are other dupes
          cursor->synchronize(context, LocalCursor::kSyncOnlyEqualKeys);
          return cursor->duplicate_cache_count(context) > 0
                    ? 0
                    : UPS_KEY_NOT_FOUND;
        }
        return UPS_KEY_NOT_FOUND;
      }

      if (unlikely(NOTSET(op->flags, TxnOperation::kNop))) {
        assert(!"shouldn't be here");
        return UPS_KEY_NOT_FOUND;
      }

      continue;
    }

    return UPS_TXN_CONFLICT;
  }

  // if there was an approximate match: check if the btree provides
  // a better match
  if (unlikely(op
          && ISSETANY(ups_key_get_intflags(key), BtreeKey::kApproximate))) {
    ups_key_set_intflags(key, 0);

    // create a duplicate of the key
    ups_key_t *source = op->node->key();
    ups_key_t copy = ups_make_key(::alloca(source->size), source->size);
    copy._flags = BtreeKey::kApproximate;
    ::memcpy(copy.data, source->data, source->size);

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

      st = db->btree_index->find(context, cursor, key, key_arena, record,
                      record_arena, new_flags);
      if (st)
        break;
      exact_is_erased = is_key_erased(context, db->txn_index.get(), key);
    } while (exact_is_erased);

    // if the key was not found in the btree: return the key which was found
    // in the transaction tree
    if (st == UPS_KEY_NOT_FOUND) {
      if (cursor)
        cursor->activate_txn(op);
      copy_key(db, context->txn, &copy, key);
      if (likely(record != 0))
        copy_record(db, context->txn, op, record);
      return 0;
    }

    if (unlikely(st))
      return st;

    // the btree key is a direct match? then return it
    if (NOTSET(ups_key_get_intflags(key), BtreeKey::kApproximate)
          && ISSET(flags, UPS_FIND_EQ_MATCH)
          && !exact_is_erased) {
      if (cursor)
        cursor->activate_btree();
      return 0;
    }

    // if there's an approx match in the btree: compare both keys and
    // use the one that is closer. if the btree is closer: make sure
    // that it was not erased or overwritten in a transaction
    int cmp = db->btree_index->compare_keys(key, &copy);
    bool use_btree = false;
    if (ISSET(flags, UPS_FIND_GT_MATCH)) {
      if (cmp < 0)
        use_btree = true;
    }
    else if (ISSET(flags, UPS_FIND_LT_MATCH)) {
      if (cmp > 0)
        use_btree = true;
    }
    else
      assert(!"shouldn't be here");

    // use the btree key
    if (likely(use_btree)) {
      if (cursor)
        cursor->activate_btree();
      return 0;
    }
    else { // use the txn key
      if (cursor)
        cursor->activate_txn(op);
      copy_key(db, context->txn, &copy, key);
      if (likely(record != 0))
        copy_record(db, context->txn, op, record);
      return 0;
    }
  }

  //
  // no approximate match:
  //
  // we've successfully checked all un-flushed transactions and there
  // were no conflicts, and we have not found the key: now try to
  // lookup the key in the btree.
  //
  st = db->btree_index->find(context, cursor, key, key_arena, record,
                          record_arena, flags);
  if (unlikely(st))
    return st;
  if (cursor)
    cursor->activate_btree();
  return 0;
}

static inline void 
update_other_cursors_after_erase(LocalDb *db, Context *context, TxnNode *node,
                LocalCursor *current_cursor)
{
  uint32_t start = current_cursor ? current_cursor->duplicate_cache_index : 0;

  for (LocalCursor *c = (LocalCursor *)db->cursor_list;
              c != 0;
              c = (LocalCursor *)c->next) {
    if (c == current_cursor || c->is_nil(0))
      continue;

    bool hit = false;

    // if cursor is coupled to an op in the same node: increment
    // duplicate index (if required)
    if (c->is_txn_active()) {
      if (node == c->txn_cursor.get_coupled_op()->node)
        hit = true;
    }
    // if cursor is coupled to the same key in the btree: increment
    // duplicate index (if required) 
    else if (!c->btree_cursor.is_nil()
            && c->btree_cursor.points_to(context, node->key()))
      hit = true;

    if (hit) {
      // is the current cursor coupled to a duplicate? then adjust the
      // coupled duplicate index of all cursors which point to a duplicate
      if (start > 0) {
        if (start < c->duplicate_cache_index) {
          c->duplicate_cache_index--;
          continue;
        }
        if (start > c->duplicate_cache_index) {
          continue;
        }
        // else fall through
      }
      // Do not 'nil' the current cursor - its coupled key is required
      // by the parent!
      if (c != current_cursor)
        c->set_to_nil();
    }
  }
}

// Erases a key/record pair from a txn; on success, cursor will be set to
// nil
static inline ups_status_t
erase_txn(LocalDb *db, Context *context, ups_key_t *key, uint32_t flags,
                LocalCursor *cursor)
{
  // get (or create) the node for this key
  bool node_created = false;
  TxnNode *node = db->txn_index->store(key, &node_created);

  // check for conflicts of this key - but only if we're not erasing a
  // duplicate key. Duplicates are checked for conflicts in LocalCursor::move
  if (!cursor || !cursor->duplicate_cache_index) {
    ups_status_t st = check_erase_conflicts(db, context, node, key, flags);
    if (unlikely(st)) {
      if (node_created) {
        db->txn_index->remove(node);
        delete node;
      }
      return st;
    }
  }

  uint64_t lsn = lenv(db)->lsn_manager.next();

  // append a new operation to this node
  TxnOperation *op = node->append(context->txn, flags, TxnOperation::kErase,
                  lsn, key, 0);

  // is this function called through ups_cursor_erase? then add the
  // duplicate ID
  if (cursor && cursor->duplicate_cache_index)
    op->referenced_duplicate = cursor->duplicate_cache_index;

  // all other cursors need to adjust their duplicate index, if they
  // point to the same key
  // TODO similar code is run in the btree - remove it!
  // TODO yes, but this code is required for btree-only mode.
  // Can we fix this? The btree code is ugly
  update_other_cursors_after_erase(db, context, node, cursor);

  return 0;
}

// The actual implementation of erase()
static inline ups_status_t
erase_impl(LocalDb *db, Context *context, LocalCursor *cursor, ups_key_t *key,
                uint32_t flags)
{
  // No transactions? Then delete the key/value pair from the Btree
  if (NOTSET(db->env->flags(), UPS_ENABLE_TRANSACTIONS))
    return db->btree_index->erase(context, cursor, key, 0, flags);

  // if transactions are enabled: append a 'erase key' operation into
  // the txn tree; otherwise immediately erase the key from disk
  //
  // !!
  // If a cursor was specified then we have two cases:
  //
  // 1. the cursor is coupled to a btree item (or uncoupled, but not nil)
  //    and the txn_cursor is nil; in that case, we have to
  //    - uncouple the btree cursor
  //    - insert the erase-op for the key which is used by the btree cursor
  //
  // 2. the cursor is coupled to a txn-op; in this case, we have to
  //    - insert the erase-op for the key which is used by the txn-op
  if (cursor) {
    // case 1 described above
    if (cursor->is_btree_active()) {
      cursor->btree_cursor.uncouple_from_page(context);
      key = cursor->btree_cursor.uncoupled_key();
    }
    // case 2 described above
    else {
      // TODO this line is ugly
      key = &cursor->txn_cursor.get_coupled_op()->key;
    }
  }

  // update the "histogram" and reset cached values, if necessary
  db->histogram.reset_if_equal(key);

  return erase_txn(db, context, key, flags, cursor);
}

ups_status_t
LocalDb::create(Context *context, PBtreeHeader *btree_header)
{
  // the header page is now modified
  Page *header = lenv(this)->page_manager->fetch(context, 0);
  header->set_dirty(true);

  // set the flags; strip off run-time (per session) flags for the btree
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
      return UPS_INV_KEY_SIZE;
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

  // initialize the btree
  btree_index->create(context, btree_header, &config);

  if (config.record_compressor) {
    record_compressor.reset(CompressorFactory::create(
                                    config.record_compressor));
  }

  // load the custom compare function?
  if (config.key_type == UPS_TYPE_CUSTOM) {
    ups_compare_func_t func = CallbackManager::get(btree_index->compare_hash());
    // silently ignore errors as long as db_set_compare_func is in place
    if (func != 0)
      compare_function = func;
  }

  // the header page is now dirty
  header->set_dirty(true);

  // and the TxnIndex
  txn_index.reset(new TxnIndex(this));

  return 0;
}

static inline ups_status_t
fetch_record_number(Context *context, LocalDb *db)
{
  ups_key_t key = {0};
  ScopedPtr<LocalCursor> c(new LocalCursor(db, 0));
  ups_status_t st = c->move(context, &key, 0, UPS_CURSOR_LAST);
  if (unlikely(st))
    return st == UPS_KEY_NOT_FOUND ? 0 : st;

  if (ISSET(db->flags(), UPS_RECORD_NUMBER32))
    db->_current_record_number = *(uint32_t *)key.data;
  else
    db->_current_record_number = *(uint64_t *)key.data;
  return 0;
}

ups_status_t
LocalDb::open(Context *context, PBtreeHeader *btree_header)
{
  // create the BtreeIndex
  btree_index.reset(new BtreeIndex(this));

  // initialize the btree
  btree_index->open(btree_header, &config);

  // merge the persistent flags with the flags supplied by the user
  config.flags |= flags();

  // create the TxnIndex
  txn_index.reset(new TxnIndex(this));

  // load the custom compare function?
  if (config.key_type == UPS_TYPE_CUSTOM) {
    ups_compare_func_t f = CallbackManager::get(btree_index->compare_hash());
    if (f == 0 && NOTSET(flags(), UPS_IGNORE_MISSING_CALLBACK)) {
      ups_trace(("custom compare function is not yet registered"));
      return UPS_NOT_READY;
    }
    compare_function = f;
  }

  // is record compression enabled?
  if (config.record_compressor) {
    record_compressor.reset(CompressorFactory::create(
                                    config.record_compressor));
  }

  // fetch the current record number
  if (ISSETANY(flags(), UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64))
    return fetch_record_number(context, this);

  return 0;
}

struct MetricsVisitor : public BtreeVisitor {
  MetricsVisitor(ups_env_metrics_t *metrics_)
    : metrics(metrics_) {
  }

  // Specifies if the visitor modifies the node
  virtual bool is_read_only() const {
    return true;
  }

  // called for each node
  virtual void operator()(Context *context, BtreeNodeProxy *node) {
    if (likely(node->is_leaf()))
      node->fill_metrics(&metrics->btree_leaf_metrics);
    else
      node->fill_metrics(&metrics->btree_internal_metrics);
  }
  
  ups_env_metrics_t *metrics;
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
  assert(param != 0);

  ups_parameter_t *p = param;

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
    case UPS_PARAM_MAX_KEYS_PER_PAGE: {
      Context context(lenv(this), 0, this);
      Page *page = btree_index->root_page(&context);
      if (likely(page != 0)) {
        BtreeNodeProxy *node = btree_index->get_node_from_page(page);
        p->value = node->estimate_capacity();
      }
      else
        p->value = 0;
      break;
    }
    case UPS_PARAM_RECORD_COMPRESSION:
      p->value = config.record_compressor;
      break;
    case UPS_PARAM_KEY_COMPRESSION:
      p->value = config.key_compressor;
      break;
    default:
      ups_trace(("unknown parameter %d", (int)p->name));
      return UPS_INV_PARAMETER;
    }
  }
  return 0;
}

ups_status_t
LocalDb::check_integrity(uint32_t flags)
{
  Context context(lenv(this), 0, this);

  // purge cache if necessary
  lenv(this)->page_manager->purge_cache(&context);

  // call the btree function
  btree_index->check_integrity(&context, flags);

  return 0;
}

uint64_t
LocalDb::count(Txn *htxn, bool distinct)
{
  LocalTxn *txn = dynamic_cast<LocalTxn *>(htxn);

  Context context(lenv(this), txn, this);

  // purge cache if necessary
  lenv(this)->page_manager->purge_cache(&context);

  // call the btree function - this will retrieve the number of keys
  // in the btree
  uint64_t keycount = btree_index->count(&context, distinct);

  // if transactions are enabled, then also sum up the number of keys
  // from the transaction tree
  if (ISSET(flags(), UPS_ENABLE_TRANSACTIONS))
    keycount += txn_index->count(&context, txn, distinct);

  return keycount;
}

// returns the next record number
template<typename T>
static inline T
next_record_number(LocalDb *db)
{
  if (unlikely(db->_current_record_number >= std::numeric_limits<T>::max()))
    throw Exception(UPS_LIMITS_REACHED);

  return (T) ++db->_current_record_number;
}

template<typename T>
static inline void
prepare_record_number(LocalDb *db, ups_key_t *key, ByteArray *arena,
                uint32_t flags)
{
  T record_number = 0;

  if (unlikely(ISSET(flags, UPS_OVERWRITE))) {
    assert(key->size == sizeof(T));
    assert(key->data != 0);
    record_number = *(T *)key->data;
  }
  else {
    // get the record number and increment it
    record_number = next_record_number<T>(db);
  }

  // allocate memory for the key
  if (!key->data) {
    arena->resize(sizeof(T));
    key->data = arena->data();
  }
  key->size = sizeof(T);
  *(T *)key->data = record_number;
}

static inline void 
update_other_cursors_after_insert(LocalDb *db, Context *context, TxnNode *node,
                LocalCursor *current_cursor)
{
  uint32_t start = current_cursor->duplicate_cache_index;

  for (LocalCursor *c = (LocalCursor *)db->cursor_list;
              c != 0;
              c = (LocalCursor *)c->next) {
    if (c == current_cursor || c->is_nil(0))
      continue;

    bool hit = false;

    // if cursor is coupled to an op in the same node: increment
    // duplicate index (if required)
    if (c->is_txn_active()) {
      if (node == c->txn_cursor.get_coupled_op()->node)
        hit = true;
    }
    // if cursor is coupled to the same key in the btree: increment
    // duplicate index (if required) 
    else if (c->btree_cursor.points_to(context, node->key()))
      hit = true;

    if (hit) {
      if (c->duplicate_cache_index > start)
        c->duplicate_cache_index++;
    }
  }
}

// Inserts a key/record pair in a txn node; if cursor is not NULL it will
// be attached to the new txn_op structure
static inline ups_status_t
insert_txn(LocalDb *db, Context *context, ups_key_t *key, ups_record_t *record,
                uint32_t flags, LocalCursor *cursor)
{
  // get (or create) the node for this key
  bool node_created = false;
  TxnNode *node = db->txn_index->store(key, &node_created);

  // check for conflicts of this key
  ups_status_t st = check_insert_conflicts(db, context, node, key, flags);
  if (unlikely(st)) {
    if (node_created) {
      db->txn_index->remove(node);
      delete node;
    }
    return st;
  }

  uint64_t lsn = lenv(db)->lsn_manager.next();

  // append a new operation to this node
  TxnOperation *op = node->append(context->txn, flags,
                (ISSET(flags, UPS_DUPLICATE)
                    ? TxnOperation::kInsertDuplicate
                    : ISSET(flags, UPS_OVERWRITE)
                        ? TxnOperation::kInsertOverwrite
                        : TxnOperation::kInsert),
                lsn, key, record);

  // if there's a cursor then couple it to the op; also store the
  // dupecache-index in the op (it's needed for DUPLICATE_INSERT_BEFORE/NEXT)
  if (cursor) {
    if (cursor->duplicate_cache_index > 0)
      op->referenced_duplicate = cursor->duplicate_cache_index;

    cursor->activate_txn(op);

    // all other cursors need to increment their duplicate index, if their
    // index is > this cursor's index
    update_other_cursors_after_insert(db, context, node, cursor);
  }

  return 0;
}

// The actual implementation of insert()
static inline ups_status_t
insert_impl(LocalDb *db, Context *context, LocalCursor *cursor,
                ups_key_t *key, ups_record_t *record, uint32_t flags)
{
  ups_status_t st;

  // if Transactions are disabled: directly insert the new key/record pair
  // in the Btree, then return
  if (NOTSET(db->env->flags(), UPS_ENABLE_TRANSACTIONS)) {
    st = db->btree_index->insert(context, cursor, key, record, flags);
    if (likely(st == 0) && cursor)
      cursor->activate_btree();
    return st;
  }

  // Otherwise insert the key/record pair into the TxnIndex
  st = insert_txn(db, context, key, record, flags, cursor);
  if (unlikely(st != 0))
    return st;

  if (cursor) {
    DuplicateCache &dc = cursor->duplicate_cache;
    // if duplicate keys are enabled: set the duplicate index of
    // the new key  */
    if (cursor->duplicate_cache_count(context, true)) {
      TxnOperation *op = cursor->txn_cursor.get_coupled_op();
      assert(op != 0);

      for (uint32_t i = 0; i < dc.size(); i++) {
        DuplicateCacheLine *l = &dc[i];
        if (!l->use_btree() && l->txn_op() == op) {
          cursor->duplicate_cache_index = i + 1;
          break;
        }
      }
    }

    // set a flag that the cursor just completed an Insert-or-find
    // operation; this information is needed in ups_cursor_move
    cursor->last_operation = LocalCursor::kLookupOrInsert;
  }

  return 0;
}

ups_status_t
LocalDb::insert(Cursor *hcursor, Txn *txn, ups_key_t *key,
                ups_record_t *record, uint32_t flags)
{
  if (config.flags & (UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64)) {
    if (unlikely(key->size == 0 && key->data != 0)) {
      ups_trace(("for record number keys set key size to 0, "
                             "key->data to null"));
      return UPS_INV_PARAMETER;
    }
    if (unlikely(key->size > 0 && key->size != config.key_size)) {
      ups_trace(("invalid key size (%u instead of %u)",
            key->size, config.key_size));
      return UPS_INV_KEY_SIZE;
    }

    if (ISSET(config.flags, UPS_RECORD_NUMBER32))
      prepare_record_number<uint32_t>(this, key, &key_arena(txn), flags);
    else
      prepare_record_number<uint64_t>(this, key, &key_arena(txn), flags);
  }

  if (unlikely(config.key_size != UPS_KEY_SIZE_UNLIMITED
                          && key->size != config.key_size)) {
    ups_trace(("invalid key size (%u instead of %u)",
          key->size, config.key_size));
    return UPS_INV_KEY_SIZE;
  }

  if (unlikely(config.record_size != UPS_RECORD_SIZE_UNLIMITED
                          && record->size != config.record_size)) {
    ups_trace(("invalid record size (%u instead of %u)",
          record->size, config.record_size));
    return UPS_INV_RECORD_SIZE;
  }

  LocalTxn *local_txn = 0;
  LocalCursor *cursor = (LocalCursor *)hcursor;
  Context context(lenv(this), (LocalTxn *)txn, this);

  if (cursor && NOTSET(flags, UPS_DUPLICATE) && NOTSET(flags, UPS_OVERWRITE))
    cursor->duplicate_cache_index = 0;

  // create temporary transaction, if neccessary
  if (!txn && ISSET(this->flags(), UPS_ENABLE_TRANSACTIONS)) {
    local_txn = begin_temp_txn(lenv(this));
    context.txn = local_txn;
  }

  // check the "histogram" to avoid further btree lookups
  // (only if transactions are enabled and if record numbers are disabled -
  // otherwise we overwrite the internal memory used by the record number)
  if (ISSET(this->flags(), UPS_ENABLE_TRANSACTIONS)
      && !ISSETANY(this->flags(), UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64)) {
    if (histogram.test_and_update_if_lower(context.txn, key))
      flags |= UPS_HINT_PREPEND;
    if (histogram.test_and_update_if_greater(context.txn, key))
      flags |= UPS_HINT_APPEND;
  }

  // purge the cache
  lenv(this)->page_manager->purge_cache(&context);

  ups_status_t st = insert_impl(this, &context, cursor, key, record, flags);
  return finalize(lenv(this), &context, st, local_txn);
}

ups_status_t
LocalDb::erase(Cursor *hcursor, Txn *txn, ups_key_t *key, uint32_t flags)
{
  LocalCursor *cursor = (LocalCursor *)hcursor;

  if (unlikely(cursor && cursor->is_nil()))
    return UPS_CURSOR_IS_NIL;

  if (key) {
    if (unlikely(config.key_size != UPS_KEY_SIZE_UNLIMITED
        && key->size != config.key_size)) {
      ups_trace(("invalid key size (%u instead of %u)",
            key->size, config.key_size));
      return UPS_INV_KEY_SIZE;
    }
  }

  LocalTxn *local_txn = 0;
  Context context(lenv(this), (LocalTxn *)txn, this);

  if (!txn && ISSET(this->flags(), UPS_ENABLE_TRANSACTIONS)) {
    local_txn = begin_temp_txn(lenv(this));
    context.txn = local_txn;
  }

  ups_status_t st = erase_impl(this, &context, cursor, key, flags);
  // on success: 'nil' the cursor
  if (likely(st == 0)) {
    if (cursor)
      cursor->set_to_nil();
  }

  return finalize(lenv(this), &context, st, local_txn);
}

ups_status_t
LocalDb::find(Cursor *hcursor, Txn *txn, ups_key_t *key,
                ups_record_t *record, uint32_t flags)
{
  if (unlikely(config.key_size != UPS_KEY_SIZE_UNLIMITED
        && key->size != config.key_size)) {
    ups_trace(("invalid key size (%u instead of %u)",
          key->size, config.key_size));
    return UPS_INV_KEY_SIZE;
  }

  LocalCursor *cursor = (LocalCursor *)hcursor;

  // Transactions require a Cursor because only Cursors can build lists
  // of duplicates.
  if (!cursor
          && ISSET(this->flags(), UPS_ENABLE_TRANSACTIONS
                                    | UPS_ENABLE_DUPLICATES)) {
    ScopedPtr<LocalCursor> c(new LocalCursor(this, txn));
    return find(c.get(), txn, key, record, flags);
  }

  Context context(lenv(this), (LocalTxn *)txn, this);

  // purge cache if necessary
  lenv(this)->page_manager->purge_cache(&context);

  // if Transactions are disabled then read from the Btree
  if (NOTSET(this->flags(), UPS_ENABLE_TRANSACTIONS)) {
    ups_status_t st = btree_index->find(&context, cursor, key, &key_arena(txn),
                          record, &record_arena(txn), flags);
    if (likely(st == 0) && cursor)
      cursor->activate_btree();
    return finalize(lenv(this), &context, st, 0);
  }

  // Otherwise fetch the record from the Transaction index
  ups_status_t st = find_txn(this, &context, cursor, key, record, flags);
  if (unlikely(st))
    return finalize(lenv(this), &context, st, 0);

  // if the key has duplicates: build a duplicate table, then couple to the
  // first/oldest duplicate
  if (cursor) {
    if (cursor->duplicate_cache_count(&context, false)) {
      cursor->couple_to_duplicate(1); // 1-based index!
      if (likely(record != 0)) {
        if (cursor->is_txn_active())
          cursor->txn_cursor.copy_coupled_record(record);
        else {
          Txn *txn = cursor->txn;
          st = cursor->btree_cursor.move(&context, 0, 0, record,
                        &record_arena(txn), 0);
        }
      }
    }

    // set a flag that the cursor just completed an Insert-or-find
    // operation; this information is required in ups_cursor_move
    cursor->last_operation = LocalCursor::kLookupOrInsert;
  }

  return finalize(lenv(this), &context, st, 0);
}

Cursor *
LocalDb::cursor_create(Txn *txn, uint32_t)
{
  return new LocalCursor(this, txn);
}

Cursor *
LocalDb::cursor_clone(Cursor *src)
{
  return new LocalCursor(*(LocalCursor *)src);
}

ups_status_t
LocalDb::bulk_operations(Txn *txn, ups_operation_t *ops, size_t ops_length,
                uint32_t /* unused */)
{
  ByteArray ka, ra;
  ups_operation_t *initial_ops = ops;

  // The |ByteArray| uses realloc to grow, and existing pointers will
  // be invalidated. Therefore we will use two loops: the first one
  // accumulates all results in |ka| and |ra|, the second one lets key->data
  // and record->data pointers point into |ka| and |ra|.
  for (size_t i = 0; i < ops_length; i++, ops++) {
    switch (ops->type) {
      case UPS_OP_INSERT:
        ops->result = insert(0, txn, &ops->key, &ops->record, ops->flags);
        // if this a record number database? then we might have to copy the key
        if (likely(ops->result == 0)
                && ISSETANY(flags(), UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64)
                && NOTSET(ops->key.flags, UPS_KEY_USER_ALLOC)) {
          ka.append((uint8_t *)ops->key.data, ops->key.size);
        }
        break;
      case UPS_OP_FIND:
        ops->result = find(0, txn, &ops->key, &ops->record, ops->flags);
        if (likely(ops->result == 0)) {
          // copy key if approx. matching was used
          if (ISSETANY(ups_key_get_intflags(&ops->key), BtreeKey::kApproximate)
                  && NOTSET(ops->key.flags, UPS_KEY_USER_ALLOC)) {
            ka.append((uint8_t *)ops->key.data, ops->key.size);
          }
          // copy record unless it's allocated by the user
          if (NOTSET(ops->record.flags, UPS_RECORD_USER_ALLOC)) {
            ra.append((uint8_t *)ops->record.data, ops->record.size);
          }
        }
        break;
      case UPS_OP_ERASE:
        ops->result = erase(0, txn, &ops->key, ops->flags);
        break;
      default:
        return UPS_INV_PARAMETER;
    }
  }

  if (ka.is_empty() && ra.is_empty())
    return 0;

  uint8_t *kptr = ka.data();
  uint8_t *rptr = ra.data();
  ops = initial_ops;
  for (size_t i = 0; i < ops_length; i++, ops++) {
    if (unlikely(ops->result != 0))
      continue;

    switch (ops->type) {
      case UPS_OP_INSERT:
        // if this a record number database? then we might have to copy the key
        if (ISSETANY(flags(), UPS_RECORD_NUMBER32 | UPS_RECORD_NUMBER64)
                && NOTSET(ops->key.flags, UPS_KEY_USER_ALLOC)) {
          ops->key.data = kptr;
          kptr += ops->key.size;
        }
        break;
      case UPS_OP_FIND:
        // copy key if approx. matching was used
        if (ISSETANY(ups_key_get_intflags(&ops->key), BtreeKey::kApproximate)
                  && NOTSET(ops->key.flags, UPS_KEY_USER_ALLOC)) {
          ops->key.data = kptr;
          kptr += ops->key.size;
        }
        // copy record unless it's allocated by the user
        if (NOTSET(ops->record.flags, UPS_RECORD_USER_ALLOC)) {
          ops->record.data = rptr;
          rptr += ops->record.size;
        }
        break;
      default:
        break;
    }
  }

  // swap key and record buffers
  key_arena(txn).steal_from(ka);
  record_arena(txn).steal_from(ra);

  return 0;
}

ups_status_t
LocalDb::cursor_move(Cursor *hcursor, ups_key_t *key,
                ups_record_t *record, uint32_t flags)
{
  LocalCursor *cursor = (LocalCursor *)hcursor;

  Context context(lenv(this), (LocalTxn *)cursor->txn, this);

  // purge cache if necessary
  lenv(this)->page_manager->purge_cache(&context);

  //
  // if the cursor was never used before and the user requests a NEXT then
  // move the cursor to FIRST; if the user requests a PREVIOUS we set it
  // to LAST, resp.
  //
  // if the cursor was already used but is nil then we've reached EOF,
  // and a NEXT actually tries to move to the LAST key (and PREVIOUS
  // moves to FIRST)
  //
  if (unlikely(cursor->is_nil(0))) {
    if (ISSET(flags, UPS_CURSOR_NEXT)) {
      flags &= ~UPS_CURSOR_NEXT;
      flags |= UPS_CURSOR_FIRST;
    }
    else if (ISSET(flags, UPS_CURSOR_PREVIOUS)) {
      flags &= ~UPS_CURSOR_PREVIOUS;
      flags |= UPS_CURSOR_LAST;
    }
  }

  // everything else is handled by the cursor function
  ups_status_t st = cursor->move(&context, key, record, flags);
  if (unlikely(st))
    return st;

  // store the direction
  cursor->last_operation = flags & (UPS_CURSOR_NEXT | UPS_CURSOR_PREVIOUS);

  return 0;
}

ups_status_t
LocalDb::close(uint32_t flags)
{
  Context context(lenv(this), 0, this);

  if (unlikely(is_modified_by_active_transaction(txn_index.get()))) {
    ups_trace(("cannot close a Database that is modified by "
               "a currently active Txn"));
    return UPS_TXN_STILL_OPEN;
  }

  // in-memory-database: free all allocated blobs
  if (btree_index && ISSET(env->flags(), UPS_IN_MEMORY))
   btree_index->drop(&context);

  // write all pages of this database to disk
  lenv(this)->page_manager->close_database(&context, this);

  env = 0;

  return 0;
}

static bool
are_cursors_identical(LocalCursor *c1, LocalCursor *c2)
{
  assert(!c1->is_nil());
  assert(!c2->is_nil());

  if (c1->is_btree_active()) {
    if (c2->is_txn_active())
      return false;

    Page *p1 = c1->btree_cursor.coupled_page();
    Page *p2 = c2->btree_cursor.coupled_page();
    int s1 = c1->btree_cursor.coupled_slot();
    int s2 = c2->btree_cursor.coupled_slot();

    return p1 == p2 && s1 == s2;
  }

  ups_key_t *k1 = c1->txn_cursor.get_coupled_op()->node->key();
  ups_key_t *k2 = c2->txn_cursor.get_coupled_op()->node->key();
  return k1 == k2;
}

ups_status_t
LocalDb::select_range(SelectStatement *stmt, LocalCursor *begin,
                LocalCursor *end, Result **presult)
{
  Page *page = 0;
  int slot;
  ups_key_t key = {0};
  ups_record_t record = {0};
  ScopedPtr<LocalCursor> tmpcursor;
 
  LocalCursor *cursor = begin;
  if (unlikely(cursor && cursor->is_nil()))
    return UPS_CURSOR_IS_NIL;

  if (unlikely(end && end->is_nil()))
    return UPS_CURSOR_IS_NIL;

  ScopedPtr<ScanVisitor> visitor(ScanVisitorFactory::from_select(stmt, this));
  if (unlikely(!visitor.get()))
    return UPS_PARSER_ERROR;

  Context context(lenv(this), 0, this);

  Result *result = new Result;

  // purge cache if necessary
  lenv(this)->page_manager->purge_cache(&context);

  // create a cursor, move it to the first key
  ups_status_t st = 0;
  if (!cursor) {
    tmpcursor.reset(new LocalCursor(this, 0));
    cursor = tmpcursor.get();
    st = cursor->move(&context, &key, &record, UPS_CURSOR_FIRST);
    if (unlikely(st))
      goto bail;
  }

  // process transactional keys at the beginning
  while (!cursor->is_btree_active()) {
    // check if we reached the 'end' cursor
    if (unlikely(end && are_cursors_identical(cursor, end)))
      goto bail;
    // now process the key
    (*visitor)(key.data, key.size, record.data, record.size);
    st = cursor->move(&context, &key, 0, UPS_CURSOR_NEXT);
    if (unlikely(st))
      goto bail;
  }

  //
  // now jump from leaf to leaf, and from transactional cursor to
  // transactional cursor.
  //
  // if there are transactional keys BEFORE a page then process them
  // if there are transactional keys IN a page then use a cursor for
  //      the page
  // if there are NO transactional keys IN a page then ask the
  //      Btree to process the request (this is the fastest code path)
  //
  // afterwards, pick up any transactional stragglers that are still left.
  //
  while (true) {
    page = cursor->btree_cursor.coupled_page();
    slot = cursor->btree_cursor.coupled_slot();
    BtreeNodeProxy *node = btree_index->get_node_from_page(page);

    bool use_cursors = false;

    //
    // in a few cases we're forced to use a cursor to iterate over the
    // page. these cases are:
    //
    // 1) an 'end' cursor is specified, and it is positioned "in" this page
    // 2) the page is modified by one (or more) transactions
    //

    // case 1) - if an 'end' cursor is specified then check if it modifies
    // the current page
    if (end) {
      if (end->is_btree_active()) {
        Page *end_page = end->btree_cursor.coupled_page();
        if (page == end_page)
          use_cursors = true;
      }
      else {
        ups_key_t *k = end->txn_cursor.get_coupled_op()->node->key();
        if (node->compare(&context, k, 0) >= 0
            && node->compare(&context, k, node->length() - 1) <= 0)
          use_cursors = true;
      }
    }

    // case 2) - take a peek at the next transactional key and check
    // if it modifies the current page
    if (!use_cursors && ISSET(flags(), UPS_ENABLE_TRANSACTIONS)) {
      TxnCursor tc(cursor);
      tc.clone(&cursor->txn_cursor);
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

    // no transactional data: the Btree will do the work. This is the
    // fastest code path
    if (use_cursors == false) {
      node->scan(&context, visitor.get(), stmt, slot, stmt->distinct);
      st = cursor->btree_cursor.move_to_next_page(&context);
      if (unlikely(st == UPS_KEY_NOT_FOUND))
        break;
      if (unlikely(st))
        goto bail;
    }
    // mixed txn/btree load? if there are leafs which are NOT modified
    // in a transaction then move the scan to the btree node. Otherwise use
    // a regular cursor
    else {
      do {
        // check if we reached the 'end' cursor
        if (unlikely(end && are_cursors_identical(cursor, end)))
          goto bail;

        Page *new_page = 0;
        if (cursor->is_btree_active())
          new_page = cursor->btree_cursor.coupled_page();
        // break the loop if we've reached the next page
        if (new_page && new_page != page) {
          page = new_page;
          break;
        }
        // process the key
        (*visitor)(key.data, key.size, record.data, record.size);
        st = cursor->move(&context, &key, &record, UPS_CURSOR_NEXT);
      } while (st == 0);
    }

    if (unlikely(st == UPS_KEY_NOT_FOUND))
      goto bail;
    if (unlikely(st))
      return st;
  }

  // pick up the remaining transactional keys
  while ((st = cursor->move(&context, &key, &record, UPS_CURSOR_NEXT)) == 0) {
    // check if we reached the 'end' cursor
    if (end && are_cursors_identical(cursor, end))
      goto bail;

    (*visitor)(key.data, key.size, record.data, record.size);
  }

bail:
  // now fetch the results
  visitor->assign_result((uqi_result_t *)result);

  *presult = result;

  return st == UPS_KEY_NOT_FOUND ? 0 : st;
}

ups_status_t
LocalDb::flush_txn_operation(Context *context, LocalTxn *txn, TxnOperation *op)
{
  ups_status_t st = 0;
  TxnNode *node = op->node;

  //
  // depending on the type of the operation: actually perform the
  // operation on the btree
  //
  // if the txn-op has a cursor attached, then all (txn)cursors
  // which are coupled to this op have to be uncoupled, and must be coupled
  // to the btree item instead.
  //
  if (ISSETANY(op->flags, TxnOperation::kInsert
                                | TxnOperation::kInsertOverwrite
                                | TxnOperation::kInsertDuplicate)) {
    uint32_t additional_flag = 
      ISSET(op->flags, TxnOperation::kInsertDuplicate)
          ? UPS_DUPLICATE
          : UPS_OVERWRITE;

    LocalCursor *c1 = op->cursor_list
                            ? op->cursor_list->parent()
                            : 0;

    // ignore cursor if it's coupled to btree
    if (!c1 || c1->is_btree_active()) {
      st = btree_index->insert(context, 0, node->key(), &op->record,
                  op->original_flags | additional_flag);
    }
    else {
      // pick the first cursor, get the parent/btree cursor and
      // insert the key/record pair in the btree. The btree cursor
      // then will be coupled to this item.
      st = btree_index->insert(context, c1, node->key(), &op->record,
                  op->original_flags | additional_flag);
      if (likely(st == 0)) {
        // uncouple the cursor from the txn-op, and remove it
        c1->activate_btree(true);

        // all other (txn) cursors need to be coupled to the same
        // item as the first one.
        TxnCursor *tc2;
        while ((tc2 = op->cursor_list)) {
          LocalCursor *c2 = tc2->parent();
          if (unlikely(c1 != c2)) {
            c2->btree_cursor.clone(&c1->btree_cursor);
            c2->activate_btree(true);
          }
        }
      }
    }
  }
  else if (ISSET(op->flags, TxnOperation::kErase)) {
    st = btree_index->erase(context, 0, node->key(),
                  op->referenced_duplicate, op->flags);
    if (unlikely(st == UPS_KEY_NOT_FOUND))
      st = 0;
  }

  if (likely(st == 0))
    op->set_flushed();

  return st;
}

ups_status_t
LocalDb::drop(Context *context)
{
  btree_index->drop(context);
  return 0;
}

} // namespace upscaledb
