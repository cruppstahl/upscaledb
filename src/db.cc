/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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
#include <math.h>
#include <float.h>

#include "blob.h"
#include "btree.h"
#include "cache.h"
#include "cursor.h"
#include "device.h"
#include "btree_cursor.h"
#include "db.h"
#include "device.h"
#include "env.h"
#include "error.h"
#include "extkeys.h"
#include "freelist.h"
#include "log.h"
#include "journal.h"
#include "mem.h"
#include "os.h"
#include "page.h"
#include "btree_stats.h"
#include "txn.h"
#include "txn_cursor.h"
#include "version.h"

namespace hamsterdb {

#define DUMMY_LSN                 1

typedef struct
{
  Database *db;         /* [in] */
  ham_u32_t flags;      /* [in] */
  ham_u64_t total_count;   /* [out] */
  bool is_leaf;     /* [scratch] */
}  calckeys_context_t;

/*
 * callback function for estimating / counting the number of keys stored
 * in the database
 */
static ham_status_t
__calc_keys_cb(int event, void *param1, void *param2, void *context)
{
  BtreeKey *key;
  calckeys_context_t *c;
  ham_size_t count;

  c=(calckeys_context_t *)context;

  switch (event) {
  case HAM_ENUM_EVENT_DESCEND:
    break;

  case HAM_ENUM_EVENT_PAGE_START:
    c->is_leaf=*(bool *)param2;
    break;

  case HAM_ENUM_EVENT_PAGE_STOP:
    break;

  case HAM_ENUM_EVENT_ITEM:
    key = (BtreeKey *)param1;
    count = *(ham_size_t *)param2;

    if (c->is_leaf) {
      ham_size_t dupcount = 1;

      if (c->flags & HAM_SKIP_DUPLICATES
          || (c->db->get_rt_flags() & HAM_ENABLE_DUPLICATES) == 0) {
        c->total_count+=count;
        return (HAM_ENUM_DO_NOT_DESCEND);
      }
      if (key->get_flags() & BtreeKey::KEY_HAS_DUPLICATES) {
        ham_status_t st = c->db->get_env()->get_duplicate_manager()->get_count(
              key->get_ptr(), &dupcount, 0);
        if (st)
          return (st);
        c->total_count+=dupcount;
      }
      else {
        c->total_count++;
      }
    }
    break;

  default:
    ham_assert(!"unknown callback event");
    break;
  }

  return (HAM_ENUM_CONTINUE);
}

typedef struct free_cb_context_t
{
  Database *db;
  bool is_leaf;

} free_cb_context_t;

/*
 * callback function for freeing blobs of an in-memory-database
 */
ham_status_t
__free_inmemory_blobs_cb(int event, void *param1, void *param2, void *context)
{
  ham_status_t st;
  BtreeKey *key;
  free_cb_context_t *c;

  c = (free_cb_context_t *)context;

  switch (event) {
  case HAM_ENUM_EVENT_DESCEND:
    break;

  case HAM_ENUM_EVENT_PAGE_START:
    c->is_leaf = *(bool *)param2;
    break;

  case HAM_ENUM_EVENT_PAGE_STOP:
    /* nop */
    break;

  case HAM_ENUM_EVENT_ITEM:
    key = (BtreeKey *)param1;

    if (key->get_flags()&BtreeKey::KEY_IS_EXTENDED) {
      ham_u64_t blobid = key->get_extended_rid(c->db);
      /* delete the extended key */
      st = c->db->remove_extkey(blobid);
      if (st)
        return (st);
    }

    if (key->get_flags() & (BtreeKey::KEY_BLOB_SIZE_TINY
              | BtreeKey::KEY_BLOB_SIZE_SMALL
              | BtreeKey::KEY_BLOB_SIZE_EMPTY))
      break;

    /* if we're in the leaf page, delete the blob */
    if (c->is_leaf) {
      st = key->erase_record(c->db, 0, 0, true);
      if (st)
        return (st);
    }
    break;

  default:
    ham_assert(!"unknown callback event");
    return (HAM_ENUM_STOP);
  }

  return (HAM_ENUM_CONTINUE);
}

Database::Database(Environment *env, ham_u16_t name, ham_u32_t flags)
  : m_env(env), m_name(name), m_error(0), m_context(0), m_btree(0),
    m_cursors(0), m_prefix_func(0), m_cmp_func(0), m_rt_flags(flags),
    m_extkey_cache(0), m_optree(this)
{
  m_key_arena.set_allocator(env->get_allocator());
  m_record_arena.set_allocator(env->get_allocator());
}

ham_status_t
Database::remove_extkey(ham_u64_t blobid)
{
  if (get_extkey_cache())
    get_extkey_cache()->remove(blobid);
  return (m_env->get_blob_manager()->free(this, blobid, 0));
}

int HAM_CALLCONV
Database::default_prefix_compare(ham_db_t *db,
           const ham_u8_t *lhs, ham_size_t lhs_length,
           ham_size_t lhs_real_length,
           const ham_u8_t *rhs, ham_size_t rhs_length,
           ham_size_t rhs_real_length)
{
  (void)db;

  /*
   * the default compare uses memcmp. shorter strings are "greater".
   *
   * when one of the keys is NOT extended we don't need to request the other
   * (extended) key because shorter keys are "greater" anyway.
   */
  if (lhs_length < rhs_length) {
    int m = memcmp(lhs, rhs, lhs_length);
    if (m < 0)
      return (-1);
    if (m > 0)
      return (+1);

    if (lhs_length == lhs_real_length) {
      ham_assert(lhs_real_length < rhs_real_length);
      return (-1);
    }
  }
  else if (rhs_length < lhs_length) {
    int m = memcmp(lhs, rhs, rhs_length);
    if (m < 0)
      return (-1);
    if (m > 0)
      return (+1);

    if (rhs_length == rhs_real_length) {
      ham_assert(lhs_real_length > rhs_real_length);
      return (+1);
    }
  }
  else {
    int m = memcmp(lhs, rhs, lhs_length);
    if (m < 0)
      return (-1);
    if (m > 0)
      return (+1);

    if (lhs_length == lhs_real_length) {
      if (lhs_real_length < rhs_real_length)
        return (-1);
    }
    else if (rhs_length == rhs_real_length) {
      if (lhs_real_length > rhs_real_length)
        return (+1);
    }
  }

  return (HAM_PREFIX_REQUEST_FULLKEY);
}

int HAM_CALLCONV
Database::default_compare(ham_db_t *db,
           const ham_u8_t *lhs, ham_size_t lhs_length,
           const ham_u8_t *rhs, ham_size_t rhs_length)
{
  (void)db;

  /* the default compare uses memcmp. treat shorter strings as "higher" */
  if (lhs_length < rhs_length) {
    int m = memcmp(lhs, rhs, lhs_length);
    if (m < 0)
      return (-1);
    if (m > 0)
      return (+1);
    return (-1);
  }
  else if (rhs_length < lhs_length) {
    int m = memcmp(lhs, rhs, rhs_length);
    if (m < 0)
      return (-1);
    if (m > 0)
      return (+1);
    return (+1);
  }
  else {
    int m = memcmp(lhs, rhs, lhs_length);
    if (m < 0)
      return (-1);
    if (m > 0)
      return (+1);
    return (0);
  }
}

int HAM_CALLCONV
Database::default_recno_compare(ham_db_t *db,
           const ham_u8_t *lhs, ham_size_t lhs_length,
           const ham_u8_t *rhs, ham_size_t rhs_length)
{
  ham_u64_t ulhs, urhs;

  (void)db;

  memcpy(&ulhs, lhs, 8);
  memcpy(&urhs, rhs, 8);

  ulhs = ham_db2h64(ulhs);
  urhs = ham_db2h64(urhs);

  if (ulhs < urhs)
    return (-1);
  if (ulhs == urhs)
    return (0);
  return (1);
}

ham_status_t
Database::get_extended_key(ham_u8_t *key_data, ham_size_t key_length,
            ham_u32_t key_flags, ham_key_t *ext_key)
{
  ham_u64_t blobid;
  ham_status_t st;
  ham_size_t temp;
  ham_record_t record;
  ham_u8_t *ptr;
  Allocator *alloc = m_env->get_allocator();

  ham_assert(key_flags & BtreeKey::KEY_IS_EXTENDED);

  /*
   * make sure that we have an extended key-cache
   *
   * in in-memory-db, the extkey-cache doesn't lead to performance
   * advantages; it only duplicates the data and wastes memory.
   * therefore we don't use it.
   */
  if (!(get_env()->get_flags() & HAM_IN_MEMORY)) {
    if (!get_extkey_cache())
      set_extkey_cache(new ExtKeyCache(this));
  }

  /* almost the same as: blobid = key->get_extended_rid(db); */
  memcpy(&blobid, key_data + (get_keysize() - sizeof(ham_u64_t)),
      sizeof(blobid));
  blobid = ham_db2h_offset(blobid);

  /* fetch from the cache */
  if (!(m_env->get_flags() & HAM_IN_MEMORY)) {
    st = get_extkey_cache()->fetch(blobid, &temp, &ptr);
    if (!st) {
      ham_assert(temp == key_length);

      if (!(ext_key->flags & HAM_KEY_USER_ALLOC)) {
        ext_key->data = (ham_u8_t *)alloc->alloc(key_length);
        if (!ext_key->data)
          return (HAM_OUT_OF_MEMORY);
      }
      memcpy(ext_key->data, ptr, key_length);
      ext_key->size = (ham_u16_t)key_length;
      return (0);
    }
    else if (st != HAM_KEY_NOT_FOUND)
      return (st);
  }

  /*
   * not cached - fetch from disk;
   * we allocate the memory here to avoid that the global record
   * pointer is overwritten
   *
   * The key is fetched in two parts: we already have the front
   * part of the key in key_data and now we only need to fetch the blob
   * remainder, which size is:
   *  key_length - (get_keysize() - sizeof(ham_u64_t))
   *
   * To prevent another round of memcpy and heap allocation here, we
   * simply allocate sufficient space for the entire key as it should be
   * passed back through (*ext_key) and adjust the pointer into that
   * memory space for the faked record-based blob_read() below.
   */
  if (!(ext_key->flags & HAM_KEY_USER_ALLOC)) {
    ext_key->data = (ham_u8_t *)alloc->alloc(key_length);
    if (!ext_key->data)
      return (HAM_OUT_OF_MEMORY);
  }

  memmove(ext_key->data, key_data, get_keysize() - sizeof(ham_u64_t));

  /* now read the remainder of the key */
  memset(&record, 0, sizeof(record));
  record.data = (((ham_u8_t *)ext_key->data) +
          get_keysize() - sizeof(ham_u64_t));
  record.size = key_length - (get_keysize() - sizeof(ham_u64_t));
  record.flags = HAM_RECORD_USER_ALLOC;

  st = m_env->get_blob_manager()->read(this, 0, blobid, &record, 0);
  if (st)
    return (st);

  /* insert the FULL key in the extkey-cache */
  if (get_extkey_cache()) {
    ExtKeyCache *cache = get_extkey_cache();
    cache->insert(blobid, key_length, (ham_u8_t *)ext_key->data);
  }

  ext_key->size = (ham_u16_t)key_length;
  return (0);
}

ham_status_t
Database::alloc_page(Page **page, ham_u32_t type, ham_u32_t flags)
{
  ham_status_t st;
  st = m_env->alloc_page(page, this, type, flags);
  if (st)
    return (st);

  /* hack: prior to 2.0, the type of btree root pages was not set
   * correctly */
  if ((*page)->get_self() == m_btree->get_rootpage()
      && !((*page)->get_db()->get_rt_flags() & HAM_READ_ONLY))
    (*page)->set_type(Page::TYPE_B_ROOT);
  return (0);
}

ham_status_t
Database::fetch_page(Page **page, ham_u64_t address, bool only_from_cache)
{
  return (m_env->fetch_page(page, this, address, only_from_cache));
}

Cursor *
Database::cursor_clone(Cursor *src)
{
  Cursor *dest = cursor_clone_impl(src);

  /* fix the linked list of cursors */
  dest->set_previous(0);
  dest->set_next(get_cursors());
  ham_assert(get_cursors() != 0);
  get_cursors()->set_previous(dest);
  set_cursors(dest);

  /* initialize the remaining fields */
  dest->set_txn(src->get_txn());

  if (src->get_txn())
    src->get_txn()->set_cursor_refcount(
            src->get_txn()->get_cursor_refcount() + 1);

  return (dest);
}

void
Database::cursor_close(Cursor *cursor)
{
  Cursor *p, *n;

  /* decrease the transaction refcount; the refcount specifies how many
   * cursors are attached to the transaction */
  if (cursor->get_txn()) {
    ham_assert(cursor->get_txn()->get_cursor_refcount() > 0);
    cursor->get_txn()->set_cursor_refcount(
            cursor->get_txn()->get_cursor_refcount() - 1);
  }

  /* now finally close the cursor */
  cursor_close_impl(cursor);

  /* fix the linked list of cursors */
  p = cursor->get_previous();
  n = cursor->get_next();

  if (p)
    p->set_next(n);
  else
    set_cursors(n);

  if (n)
    n->set_previous(p);

  cursor->set_next(0);
  cursor->set_previous(0);

  delete cursor;
}

ham_status_t
Database::close(ham_u32_t flags)
{
  /* auto-cleanup cursors?  */
  if (flags & HAM_AUTO_CLEANUP) {
    Cursor *cursor;
    while ((cursor = get_cursors()))
      cursor_close(cursor);
  }
  else if (get_cursors()) {
    ham_trace(("cannot close Database if Cursors are still open"));
    return (set_error(HAM_CURSOR_STILL_OPEN));
  }

  /* the derived classes can now do the bulk of the work */
  ham_status_t st = close_impl(flags);
  if (st)
    return (set_error(st));

  /* remove from the Environment's list */
  m_env->get_database_map().erase(m_name);

  /* free cached memory */
  get_key_arena().clear();
  get_record_arena().clear();

  m_env = 0;
  return (0);
}

struct keycount_t
{
  ham_u64_t c;
  ham_u32_t flags;
  Transaction *txn;
  Database *db;
};

static void
__get_key_count_txn(TransactionNode *node, void *data)
{
  struct keycount_t *kc = (struct keycount_t *)data;
  BtreeIndex *be = kc->db->get_btree();
  TransactionOperation *op;

  /*
   * look at each tree_node and walk through each operation
   * in reverse chronological order (from newest to oldest):
   * - is this op part of an aborted txn? then skip it
   * - is this op part of a committed txn? then include it
   * - is this op part of an txn which is still active? then include it
   * - if a committed txn has erased the item then there's no need
   *    to continue checking older, committed txns of the same key
   *
   * !!
   * if keys are overwritten or a duplicate key is inserted, then
   * we have to consolidate the btree keys with the txn-tree keys.
   */
  op = node->get_newest_op();
  while (op) {
    Transaction *optxn = op->get_txn();
    if (optxn->is_aborted())
      ; /* nop */
    else if (optxn->is_committed() || kc->txn==optxn) {
      if (op->get_flags() & TransactionOperation::TXN_OP_FLUSHED)
        ; /* nop */
      /* if key was erased then it doesn't exist */
      else if (op->get_flags() & TransactionOperation::TXN_OP_ERASE)
        return;
      else if (op->get_flags() & TransactionOperation::TXN_OP_NOP)
        ; /* nop */
      else if (op->get_flags() & TransactionOperation::TXN_OP_INSERT) {
        kc->c++;
        return;
      }
      /* key exists - include it */
      else if ((op->get_flags() & TransactionOperation::TXN_OP_INSERT)
          || (op->get_flags() & TransactionOperation::TXN_OP_INSERT_OW)) {
        /* check if the key already exists in the btree - if yes,
         * we do not count it (it will be counted later) */
        if (HAM_KEY_NOT_FOUND == be->find(0, 0, node->get_key(), 0, 0))
          kc->c++;
        return;
      }
      else if (op->get_flags() & TransactionOperation::TXN_OP_INSERT_DUP) {
        /* check if btree has other duplicates */
        if (0 == be->find(0, 0, node->get_key(), 0, 0)) {
          /* yes, there's another one */
          if (kc->flags & HAM_SKIP_DUPLICATES)
            return;
          else
            kc->c++;
        }
        else {
          /* check if other key is in this node */
          kc->c++;
          if (kc->flags & HAM_SKIP_DUPLICATES)
            return;
        }
      }
      else {
        ham_assert(!"shouldn't be here");
        return;
      }
    }
    else { /* txn is still active */
      kc->c++;
    }

    op = op->get_previous_in_node();
  }
}

ham_status_t
LocalDatabase::check_insert_conflicts(Transaction *txn,
        TransactionNode *node, ham_key_t *key, ham_u32_t flags)
{
  ham_status_t st;
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
      /* if key was erased then it doesn't exist and can be
       * inserted without problems */
      if (op->get_flags() & TransactionOperation::TXN_OP_FLUSHED)
        ; /* nop */
      else if (op->get_flags() & TransactionOperation::TXN_OP_ERASE)
        return (0);
      else if (op->get_flags() & TransactionOperation::TXN_OP_NOP)
        ; /* nop */
      /* if the key already exists then we can only continue if
       * we're allowed to overwrite it or to insert a duplicate */
      else if ((op->get_flags() & TransactionOperation::TXN_OP_INSERT)
          || (op->get_flags() & TransactionOperation::TXN_OP_INSERT_OW)
          || (op->get_flags() & TransactionOperation::TXN_OP_INSERT_DUP)) {
        if ((flags & HAM_OVERWRITE) || (flags & HAM_DUPLICATE))
          return (0);
        else
          return (HAM_DUPLICATE_KEY);
      }
      else {
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
  st = m_btree->find(0, 0, key, 0, flags);
  if (st == HAM_KEY_NOT_FOUND)
    return (0);
  if (st == HAM_SUCCESS)
    return (HAM_DUPLICATE_KEY);
  return (st);
}

ham_status_t
LocalDatabase::check_erase_conflicts(Transaction *txn,
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
      if (op->get_flags() & TransactionOperation::TXN_OP_FLUSHED)
        ; /* nop */
      /* if key was erased then it doesn't exist and we fail with
       * an error */
      else if (op->get_flags() & TransactionOperation::TXN_OP_ERASE)
        return (HAM_KEY_NOT_FOUND);
      else if (op->get_flags() & TransactionOperation::TXN_OP_NOP)
        ; /* nop */
      /* if the key exists then we're successful */
      else if ((op->get_flags() & TransactionOperation::TXN_OP_INSERT)
          || (op->get_flags() & TransactionOperation::TXN_OP_INSERT_OW)
          || (op->get_flags() & TransactionOperation::TXN_OP_INSERT_DUP)) {
        return (0);
      }
      else {
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
  return (m_btree->find(0, 0, key, 0, flags));
}

static void
__increment_dupe_index(Database *db, TransactionNode *node,
        Cursor *skip, ham_u32_t start)
{
  Cursor *c = db->get_cursors();

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

ham_status_t
LocalDatabase::insert_txn(Transaction *txn, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags,
            struct TransactionCursor *cursor)
{
  ham_status_t st = 0;
  TransactionOperation *op;
  bool node_created = false;
  ham_u64_t lsn = 0;

  /* get (or create) the node for this key */
  TransactionNode *node = get_optree()->get(key, 0);
  if (!node) {
    node = new TransactionNode(this, key);
    node_created = true;
  }

  /* check for conflicts of this key
   *
   * !!
   * afterwards, clear the changeset; check_insert_conflicts() sometimes
   * checks if a key already exists, and this fills the changeset
   */
  st = check_insert_conflicts(txn, node, key, flags);
  m_env->get_changeset().clear();
  if (st) {
    if (node_created)
      delete node;
    return (st);
  }

  /* get the next lsn */
  st = m_env->get_incremented_lsn(&lsn);
  if (st) {
    if (node_created)
      delete node;
    return (st);
  }

  /* append a new operation to this node */
  op = node->append(txn, flags,
          (flags & HAM_PARTIAL) |
          ((flags & HAM_DUPLICATE)
            ? TransactionOperation::TXN_OP_INSERT_DUP
            : (flags & HAM_OVERWRITE)
              ? TransactionOperation::TXN_OP_INSERT_OW
              : TransactionOperation::TXN_OP_INSERT),
          lsn, record);
  if (!op)
    return (HAM_OUT_OF_MEMORY);

  /* if there's a cursor then couple it to the op; also store the
   * dupecache-index in the op (it's needed for
   * DUPLICATE_INSERT_BEFORE/NEXT) */
  if (cursor) {
    Cursor *c = cursor->get_parent();
    if (c->get_dupecache_index())
      op->set_referenced_dupe(c->get_dupecache_index());

    c->set_to_nil(Cursor::CURSOR_TXN);
    cursor->couple(op);

    /* all other cursors need to increment their dupe index, if their
     * index is > this cursor's index */
    __increment_dupe_index(this, node, c, c->get_dupecache_index());
  }

  /* append journal entry */
  if (m_env->get_flags() & HAM_ENABLE_RECOVERY
      && m_env->get_flags() & HAM_ENABLE_TRANSACTIONS) {
    Journal *j = m_env->get_journal();
    st = j->append_insert(this, txn, key, record,
              flags & HAM_DUPLICATE ? flags : flags | HAM_OVERWRITE,
              op->get_lsn());
  }

  return (st);
}

static void
__nil_all_cursors_in_node(Transaction *txn, Cursor *current,
        TransactionNode *node)
{
  TransactionOperation *op = node->get_newest_op();
  while (op) {
    TransactionCursor *cursor = op->get_cursors();
    while (cursor) {
      Cursor *pc = cursor->get_parent();
      /* is the current cursor to a duplicate? then adjust the
       * coupled duplicate index of all cursors which point to a
       * duplicate */
      if (current) {
        if (current->get_dupecache_index()) {
          if (current->get_dupecache_index()
              < pc->get_dupecache_index()) {
            pc->set_dupecache_index(pc->get_dupecache_index() - 1);
            cursor = cursor->get_coupled_next();
            continue;
          }
          else if (current->get_dupecache_index()
              > pc->get_dupecache_index()) {
            cursor = cursor->get_coupled_next();
            continue;
          }
          /* else fall through */
        }
      }
      pc->couple_to_btree();
      pc->set_to_nil(Cursor::CURSOR_TXN);
      cursor = op->get_cursors();
      /* set a flag that the cursor just completed an Insert-or-find
       * operation; this information is needed in ham_cursor_move
       * (in this aspect, an erase is the same as insert/find) */
      pc->set_lastop(Cursor::CURSOR_LOOKUP_INSERT);
    }

    op = op->get_previous_in_node();
  }
}

static void
__nil_all_cursors_in_btree(Database *db, Cursor *current, ham_key_t *key)
{
  Cursor *c = db->get_cursors();

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
          if (current->get_dupecache_index()
              < c->get_dupecache_index()) {
            c->set_dupecache_index(c->get_dupecache_index() - 1);
            goto next;
          }
          else if (current->get_dupecache_index()
              > c->get_dupecache_index()) {
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
LocalDatabase::erase_txn(Transaction *txn, ham_key_t *key, ham_u32_t flags,
        TransactionCursor *cursor)
{
  ham_status_t st = 0;
  TransactionOperation *op;
  bool node_created = false;
  ham_u64_t lsn = 0;
  Cursor *pc = 0;
  if (cursor)
    pc = cursor->get_parent();

  /* get (or create) the node for this key */
  TransactionNode *node = get_optree()->get(key, 0);
  if (!node) {
    node = new TransactionNode(this, key);
    node_created = true;
  }

  /* check for conflicts of this key - but only if we're not erasing a
   * duplicate key. dupes are checked for conflicts in _local_cursor_move */
  if (!pc || (!pc->get_dupecache_index())) {
    st = check_erase_conflicts(txn, node, key, flags);
    m_env->get_changeset().clear();
    if (st) {
      if (node_created)
        delete node;
      return (st);
    }
  }

  /* get the next lsn */
  st = m_env->get_incremented_lsn(&lsn);
  if (st) {
    if (node_created)
      delete node;
    return (st);
  }

  /* append a new operation to this node */
  op = node->append(txn, flags, TransactionOperation::TXN_OP_ERASE, lsn, 0);
  if (!op)
    return (HAM_OUT_OF_MEMORY);

  /* is this function called through ham_cursor_erase? then add the
   * duplicate ID */
  if (cursor) {
    if (pc->get_dupecache_index())
      op->set_referenced_dupe(pc->get_dupecache_index());
  }

  /* the current op has no cursors attached; but if there are any
   * other ops in this node and in this transaction, then they have to
   * be set to nil. This only nil's txn-cursors! */
  __nil_all_cursors_in_node(txn, pc, node);

  /* in addition we nil all btree cursors which are coupled to this key */
  __nil_all_cursors_in_btree(this, pc, node->get_key());

  /* append journal entry */
  if (m_env->get_flags() & HAM_ENABLE_RECOVERY
      && m_env->get_flags() & HAM_ENABLE_TRANSACTIONS) {
    Journal *j = m_env->get_journal();
    st = j->append_erase(this, txn, key, 0, flags | HAM_ERASE_ALL_DUPLICATES,
              op->get_lsn());
  }

  return (st);
}

static ham_status_t
copy_record(Database *db, Transaction *txn, TransactionOperation *op, ham_record_t *record)
{
  ByteArray *arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
            ? &db->get_record_arena()
            : &txn->get_record_arena();

  if (!(record->flags & HAM_RECORD_USER_ALLOC)) {
    arena->resize(op->get_record()->size);
    record->data = arena->get_ptr();
  }
  memcpy(record->data, op->get_record()->data,
        op->get_record()->size);
  record->size = op->get_record()->size;
  return (0);
}

static ham_status_t
db_find_txn(Database *db, Transaction *txn,
        ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st = 0;
  TransactionOperation *op = 0;
  BtreeIndex *be = db->get_btree();
  bool first_loop = true;
  bool exact_is_erased = false;

  ByteArray *arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
            ? &db->get_key_arena()
            : &txn->get_key_arena();

  ham_key_set_intflags(key,
        (ham_key_get_intflags(key) & (~BtreeKey::KEY_IS_APPROXIMATE)));

  /* get the node for this key (but don't create a new one if it does
   * not yet exist) */
  TransactionNode *node = db->get_optree()->get(key, flags);

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
      if (op->get_flags() & TransactionOperation::TXN_OP_FLUSHED)
        ; /* nop */
      /* if key was erased then it doesn't exist and we can return
       * immediately
       *
       * if an approximate match is requested then move to the next
       * or previous node
       */
      else if (op->get_flags() & TransactionOperation::TXN_OP_ERASE) {
        if (first_loop
            && !(ham_key_get_intflags(key) & BtreeKey::KEY_IS_APPROXIMATE))
          exact_is_erased = true;
        first_loop = false;
        if (flags & HAM_FIND_LT_MATCH) {
          node = node->get_previous_sibling();
          ham_key_set_intflags(key,
              (ham_key_get_intflags(key) | BtreeKey::KEY_IS_APPROXIMATE));
          goto retry;
        }
        else if (flags & HAM_FIND_GT_MATCH) {
          node = node->get_next_sibling();
          ham_key_set_intflags(key,
              (ham_key_get_intflags(key) | BtreeKey::KEY_IS_APPROXIMATE));
          goto retry;
        }
        return (HAM_KEY_NOT_FOUND);
      }
      else if (op->get_flags() & TransactionOperation::TXN_OP_NOP)
        ; /* nop */
      /* if the key already exists then return its record; do not
       * return pointers to TransactionOperation::get_record, because it may be
       * flushed and the user's pointers would be invalid */
      else if ((op->get_flags() & TransactionOperation::TXN_OP_INSERT)
          || (op->get_flags() & TransactionOperation::TXN_OP_INSERT_OW)
          || (op->get_flags() & TransactionOperation::TXN_OP_INSERT_DUP)) {
        // approx match? leave the loop and continue
        // with the btree
        if (ham_key_get_intflags(key) & BtreeKey::KEY_IS_APPROXIMATE)
          break;
        // otherwise copy the record and return
        return (copy_record(db, txn, op, record));
      }
      else {
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
  if (op && ham_key_get_intflags(key) & BtreeKey::KEY_IS_APPROXIMATE) {
    ham_key_t txnkey = {0};
    ham_key_t *k = op->get_node()->get_key();
    txnkey.size = k->size;
    txnkey._flags = BtreeKey::KEY_IS_APPROXIMATE;
    txnkey.data = db->get_env()->get_allocator()->alloc(txnkey.size);
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
        db->get_env()->get_allocator()->free(txnkey.data);
      }
      key->size = txnkey.size;
      key->_flags = txnkey._flags;

      return (copy_record(db, txn, op, record));
    }
    else if (st)
      return (st);
    // the btree key is a direct match? then return it
    if ((!(ham_key_get_intflags(key) & BtreeKey::KEY_IS_APPROXIMATE))
        && (flags & HAM_FIND_EXACT_MATCH)) {
      if (txnkey.data)
        db->get_env()->get_allocator()->free(txnkey.data);
      return (0);
    }
    // if there's an approx match in the btree: compare both keys and
    // use the one that is closer. if the btree is closer: make sure
    // that it was not erased or overwritten in a transaction
    int cmp = db->compare_keys(key, &txnkey);
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
      if (txnkey.data)
        db->get_env()->get_allocator()->free(txnkey.data);
      // lookup again, with the same flags and the btree key.
      // this will check if the key was erased or overwritten
      // in a transaction
      st=db_find_txn(db, txn, key, record, flags | HAM_FIND_EXACT_MATCH);
      if (st == 0)
        ham_key_set_intflags(key,
          (ham_key_get_intflags(key) | BtreeKey::KEY_IS_APPROXIMATE));
      return (st);
    }
    else { // use txn
      if (!(key->flags & HAM_KEY_USER_ALLOC) && txnkey.data) {
        arena->resize(txnkey.size);
        key->data = arena->get_ptr();
      }
      if (txnkey.data) {
        memcpy(key->data, txnkey.data, txnkey.size);
        db->get_env()->get_allocator()->free(txnkey.data);
      }
      key->size = txnkey.size;
      key->_flags = txnkey._flags;

      return (copy_record(db, txn, op, record));
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
LocalDatabase::open(ham_u16_t descriptor)
{
  /*
   * set the database flags; strip off the persistent flags that may have been
   * set by the caller, before mixing in the persistent flags as obtained
   * from the btree.
   */
  ham_u32_t flags = get_rt_flags();
  flags &= ~(HAM_DISABLE_VAR_KEYLEN
            | HAM_CACHE_STRICT
            | HAM_CACHE_UNLIMITED
            | HAM_DISABLE_MMAP
            | HAM_ENABLE_FSYNC
            | HAM_READ_ONLY
            | HAM_ENABLE_RECOVERY
            | HAM_AUTO_RECOVERY
            | HAM_ENABLE_TRANSACTIONS);

  /* create the btree */
  BtreeIndex *bt = new BtreeIndex(this, descriptor, flags);

  ham_assert(!(bt->get_flags() & HAM_DISABLE_VAR_KEYLEN));
  ham_assert(!(bt->get_flags() & HAM_CACHE_STRICT));
  ham_assert(!(bt->get_flags() & HAM_CACHE_UNLIMITED));
  ham_assert(!(bt->get_flags() & HAM_DISABLE_MMAP));
  ham_assert(!(bt->get_flags() & HAM_ENABLE_FSYNC));
  ham_assert(!(bt->get_flags() & HAM_READ_ONLY));
  ham_assert(!(bt->get_flags() & HAM_ENABLE_RECOVERY));
  ham_assert(!(bt->get_flags() & HAM_AUTO_RECOVERY));
  ham_assert(!(bt->get_flags() & HAM_ENABLE_TRANSACTIONS));

  /* initialize the btree */
  ham_status_t st = bt->open();
  if (st) {
    delete bt;
    return (st);
  }

  set_btree(bt);

  /* set the compare functions */
  if (get_rt_flags() & HAM_RECORD_NUMBER) {
    set_compare_func(Database::default_recno_compare);
  }
  else {
    set_compare_func(Database::default_compare);
    set_prefix_compare_func(Database::default_prefix_compare);
  }

  /* merge the non-persistent database flag with the persistent flags from
   * the btree index */
  set_rt_flags(get_rt_flags(true) | bt->get_flags());

  if ((get_rt_flags() & HAM_RECORD_NUMBER) == 0)
    return (0);

  ham_key_t key = {};
  Cursor *c = new Cursor(this, 0, 0);
  st = cursor_move(c, &key, 0, HAM_CURSOR_LAST);
  cursor_close(c);
  if (st)
    return (st == HAM_KEY_NOT_FOUND ? 0 : st);

  ham_assert(key.size == sizeof(ham_u64_t));
  m_recno = *(ham_u64_t *)key.data;
  m_recno = ham_h2db64(m_recno);

  return (0);
}

ham_status_t
LocalDatabase::create(ham_u16_t descriptor, ham_u16_t keysize)
{
  /* set the flags; strip off run-time (per session) flags for the btree */
  ham_u32_t persistent_flags = get_rt_flags();
  persistent_flags &= ~(HAM_DISABLE_VAR_KEYLEN
            | HAM_CACHE_STRICT
            | HAM_CACHE_UNLIMITED
            | HAM_DISABLE_MMAP
            | HAM_ENABLE_FSYNC
            | HAM_READ_ONLY
            | HAM_ENABLE_RECOVERY
            | HAM_AUTO_RECOVERY
            | HAM_ENABLE_TRANSACTIONS);

  /* create the btree */
  BtreeIndex *bt = new BtreeIndex(this, descriptor, persistent_flags);
  set_btree(bt);

  /* initialize the btree */
  ham_status_t st = bt->create(keysize);
  if (st) {
    delete bt;
    return (st);
  }

  /* set the default key compare functions */
  if (get_rt_flags() & HAM_RECORD_NUMBER) {
    set_compare_func(Database::default_recno_compare);
  }
  else {
    set_compare_func(Database::default_compare);
    set_prefix_compare_func(Database::default_prefix_compare);
  }

  return (0);
}

ham_status_t
LocalDatabase::get_parameters(ham_parameter_t *param)
{
  ham_parameter_t *p = param;

  if (p) {
    for (; p->name; p++) {
      switch (p->name) {
      case HAM_PARAM_KEYSIZE:
        p->value = get_btree() ? get_keysize() : 21;
        break;
      case HAM_PARAM_FLAGS:
        p->value = (ham_u64_t)get_rt_flags();
        break;
      case HAM_PARAM_DATABASE_NAME:
        p->value = (ham_u64_t)get_name();
        break;
      case HAM_PARAM_MAX_KEYS_PER_PAGE:
        if (get_btree()) {
          ham_size_t count = 0, size = get_keysize();
          BtreeIndex *be = get_btree();
          ham_status_t st;

          st = be->calc_keycount_per_page(&count, size);
          if (st)
            return (st);
          p->value = count;
        }
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
LocalDatabase::check_integrity(Transaction *txn)
{
  ham_status_t st;

  /* check the cache integrity */
  if (!(get_rt_flags()&HAM_IN_MEMORY)) {
    st = m_env->get_cache()->check_integrity();
    if (st)
      return (st);
  }

  /* purge cache if necessary */
  st = get_env()->purge_cache();
  if (st)
    return (st);

  /* call the btree function */
  st = m_btree->check_integrity();
  m_env->get_changeset().clear();

  return (st);
}

ham_status_t
LocalDatabase::get_key_count(Transaction *txn, ham_u32_t flags,
        ham_u64_t *keycount)
{
  ham_status_t st;

  calckeys_context_t ctx = {this, flags, 0, HAM_FALSE};

  if (flags & ~(HAM_SKIP_DUPLICATES)) {
    ham_trace(("parameter 'flag' contains unsupported flag bits: %08x",
          flags & ~(HAM_SKIP_DUPLICATES)));
    return (HAM_INV_PARAMETER);
  }

  /* purge cache if necessary */
  st = get_env()->purge_cache();
  if (st)
    return (st);

  /*
   * call the btree function - this will retrieve the number of keys
   * in the btree
   */
  st = m_btree->enumerate(__calc_keys_cb, &ctx);
  if (st)
    goto bail;
  *keycount = ctx.total_count;

  /*
   * if transactions are enabled, then also sum up the number of keys
   * from the transaction tree
   */
  if ((get_rt_flags() & HAM_ENABLE_TRANSACTIONS) && (get_optree())) {
    struct keycount_t k;
    k.c = 0;
    k.flags = flags;
    k.txn = txn;
    k.db = this;
    txn_tree_enumerate(get_optree(), __get_key_count_txn, (void *)&k);
    *keycount += k.c;
  }

bail:
  m_env->get_changeset().clear();
  return (st);
}

ham_status_t
LocalDatabase::insert(Transaction *txn, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
  Transaction *local_txn = 0;
  ham_status_t st;
  ham_u64_t recno = 0;

  ByteArray *arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
            ? &get_key_arena()
            : &txn->get_key_arena();

  if (key->size > get_keysize()
      && !(get_rt_flags() & HAM_ENABLE_EXTENDED_KEYS)) {
    ham_trace(("database does not support extended keys "
          "(see HAM_ENABLE_EXTENDED_KEYS)"));
    return (HAM_INV_KEYSIZE);
  }

  /* purge cache if necessary */
  st = get_env()->purge_cache();
  if (st)
    return (st);

  if (!txn && (get_rt_flags() & HAM_ENABLE_TRANSACTIONS))
    local_txn = new Transaction(m_env, 0, HAM_TXN_TEMPORARY);

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
      /* get the record number (host endian) and increment it */
      recno = get_incremented_recno();
    }

    /* allocate memory for the key */
    if (!key->data) {
      arena->resize(sizeof(ham_u64_t));
      key->data = arena->get_ptr();
      key->size = sizeof(ham_u64_t);
    }

    /* store it in db endian */
    recno = ham_h2db64(recno);
    memcpy(key->data, &recno, sizeof(ham_u64_t));
    key->size = sizeof(ham_u64_t);

    /* we're appending this key sequentially */
    flags |= HAM_HINT_APPEND;

    /* transactions are faster if HAM_OVERWRITE is specified */
    if (txn)
      flags |= HAM_OVERWRITE;
  }

  /*
   * if transactions are enabled: only insert the key/record pair into
   * the Transaction structure. Otherwise immediately write to the btree.
   */
  if (txn || local_txn) {
    st = insert_txn(txn ? txn : local_txn, key, record, flags, 0);
  }
  else
    st = m_btree->insert(txn, key, record, flags);

  if (st) {
    if (local_txn)
      local_txn->abort();

    if ((get_rt_flags() & HAM_RECORD_NUMBER)
        && !(flags & HAM_OVERWRITE)) {
      if (!(key->flags & HAM_KEY_USER_ALLOC)) {
        key->data = 0;
        key->size = 0;
      }
      ham_assert(st != HAM_DUPLICATE_KEY);
    }

    m_env->get_changeset().clear();
    return (st);
  }

  /*
   * record numbers: return key in host endian! and store the incremented
   * record number
   */
  if (get_rt_flags() & HAM_RECORD_NUMBER) {
    recno = ham_db2h64(recno);
    memcpy(key->data, &recno, sizeof(ham_u64_t));
    key->size = sizeof(ham_u64_t);
  }

  ham_assert(st == 0);

  if (local_txn)
    return (local_txn->commit());
  else if (m_env->get_flags() & HAM_ENABLE_RECOVERY
      && !(m_env->get_flags() & HAM_ENABLE_TRANSACTIONS))
    return (m_env->get_changeset().flush(DUMMY_LSN));
  else
    return (st);
}

ham_status_t
LocalDatabase::erase(Transaction *txn, ham_key_t *key, ham_u32_t flags)
{
  ham_status_t st;
  Transaction *local_txn = 0;
  ham_u64_t recno = 0;

  if (get_rt_flags() & HAM_READ_ONLY) {
    ham_trace(("cannot erase from a read-only database"));
    return (HAM_WRITE_PROTECTED);
  }

  /* record number: make sure that we have a valid key structure */
  if (get_rt_flags() & HAM_RECORD_NUMBER) {
    if (key->size != sizeof(ham_u64_t) || !key->data) {
      ham_trace(("key->size must be 8, key->data must not be NULL"));
      return (HAM_INV_PARAMETER);
    }
    recno = *(ham_u64_t *)key->data;
    recno = ham_h2db64(recno);
    *(ham_u64_t *)key->data = recno;
  }

  if (!txn && (get_rt_flags() & HAM_ENABLE_TRANSACTIONS))
    local_txn = new Transaction(m_env, 0, HAM_TXN_TEMPORARY);

  /*
   * if transactions are enabled: append a 'erase key' operation into
   * the txn tree; otherwise immediately erase the key from disk
   */
  if (txn || local_txn)
    st = erase_txn(txn ? txn : local_txn, key, flags, 0);
  else
    st = m_btree->erase(txn, key, flags);

  if (st) {
    if (local_txn)
      local_txn->abort();

    m_env->get_changeset().clear();
    return (st);
  }

  /* record number: re-translate the number to host endian */
  if (get_rt_flags() & HAM_RECORD_NUMBER)
    *(ham_u64_t *)key->data = ham_db2h64(recno);

  ham_assert(st == 0);

  if (local_txn) {
    m_env->get_changeset().clear();
    return (local_txn->commit());
  }
  else if (m_env->get_flags() & HAM_ENABLE_RECOVERY
      && !(m_env->get_flags() & HAM_ENABLE_TRANSACTIONS))
    return (m_env->get_changeset().flush(DUMMY_LSN));
  else
    return (st);
}

ham_status_t
LocalDatabase::find(Transaction *txn, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
  Transaction *local_txn = 0;
  ham_status_t st;

  ham_u64_t recno = 0;

  /* purge cache if necessary */
  st = get_env()->purge_cache();
  if (st)
    return (st);

  if ((get_keysize() < sizeof(ham_u64_t)) &&
      (key->size > get_keysize())) {
    ham_trace(("database does not support variable length keys"));
    return (HAM_INV_KEYSIZE);
  }

  /* if this database has duplicates, then we use ham_cursor_find
   * because we have to build a duplicate list, and this is currently
   * only available in ham_cursor_find */
  if (get_rt_flags() & HAM_ENABLE_DUPLICATES) {
    Cursor *c;
    st = ham_cursor_create((ham_cursor_t **)&c, (ham_db_t *)this,
            (ham_txn_t *)txn, HAM_DONT_LOCK);
    if (st)
      return (st);
    st = ham_cursor_find((ham_cursor_t *)c, key, record, flags | HAM_DONT_LOCK);
    cursor_close(c);
    return (st);
  }

  /* record number: make sure we have a number in little endian */
  if (get_rt_flags() & HAM_RECORD_NUMBER) {
    ham_assert(key->size == sizeof(ham_u64_t));
    ham_assert(key->data != 0);
    recno = *(ham_u64_t *)key->data;
    recno = ham_h2db64(recno);
    *(ham_u64_t *)key->data = recno;
  }

  /* if user did not specify a transaction, but transactions are enabled:
   * create a temporary one */
  if (!txn && (get_rt_flags() & HAM_ENABLE_TRANSACTIONS))
    local_txn = new Transaction(m_env, 0, HAM_TXN_READ_ONLY | HAM_TXN_TEMPORARY);

  /*
   * if transactions are enabled: read keys from transaction trees,
   * otherwise read immediately from disk
   */
  if (txn || local_txn)
    st = db_find_txn(this, txn ? txn : local_txn, key, record, flags);
  else
    st = m_btree->find(txn, 0, key, record, flags);

  if (st) {
    if (local_txn)
      local_txn->abort();

    m_env->get_changeset().clear();
    return (st);
  }

  /* record number: re-translate the number to host endian */
  if (get_rt_flags() & HAM_RECORD_NUMBER)
    *(ham_u64_t *)key->data = ham_db2h64(recno);

  m_env->get_changeset().clear();

  if (local_txn)
    return (local_txn->commit());
  else if (m_env->get_flags() & HAM_ENABLE_RECOVERY
      && !(m_env->get_flags() & HAM_ENABLE_TRANSACTIONS))
    return (m_env->get_changeset().flush(DUMMY_LSN));
  else
    return (st);
}

Cursor *
LocalDatabase::cursor_create(Transaction *txn, ham_u32_t flags)
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
  ham_status_t st;
  ham_u64_t recno = 0;
  Transaction *local_txn = 0;
  Transaction *txn = cursor->get_txn();

  ByteArray *arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
            ? &get_key_arena()
            : &txn->get_key_arena();

  if (key->size > get_keysize()
      && !(get_rt_flags() & HAM_ENABLE_EXTENDED_KEYS)) {
    ham_trace(("database does not support extended keys "
          "(see HAM_ENABLE_EXTENDED_KEYS)"));
    return (HAM_INV_KEYSIZE);
  }

  if ((get_keysize() < sizeof(ham_u64_t)) && (key->size > get_keysize())) {
    ham_trace(("database does not support variable length keys"));
    return (HAM_INV_KEYSIZE);
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
      /* get the record number (host endian) and increment it */
      recno = get_incremented_recno();
    }

    /* allocate memory for the key */
    if (!key->data) {
      arena->resize(sizeof(ham_u64_t));
      key->data = arena->get_ptr();
      key->size = sizeof(ham_u64_t);
    }

    /* store it in db endian */
    recno = ham_h2db64(recno);
    memcpy(key->data, &recno, sizeof(ham_u64_t));
    key->size = sizeof(ham_u64_t);

    /* we're appending this key sequentially */
    flags |= HAM_HINT_APPEND;

    /* transactions are faster if HAM_OVERWRITE is specified */
    if (cursor->get_txn())
      flags |= HAM_OVERWRITE;
  }

  /* purge cache if necessary */
  st = get_env()->purge_cache();
  if (st)
    return (st);

  /* if user did not specify a transaction, but transactions are enabled:
   * create a temporary one */
  if (!cursor->get_txn() && (get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    local_txn = new Transaction(m_env, 0, HAM_TXN_TEMPORARY);
    cursor->set_txn(local_txn);
  }

  if (cursor->get_txn() || local_txn) {
    st = insert_txn(cursor->get_txn()
              ? cursor->get_txn()
              : local_txn,
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
        ham_size_t i;
        TransactionCursor *txnc = cursor->get_txn_cursor();
        TransactionOperation *op = txnc->get_coupled_op();
        ham_assert(op != 0);

        for (i = 0; i < dc->get_count(); i++) {
          DupeCacheLine *l = dc->get_element(i);
          if (!l->use_btree() && l->get_txn_op() == op) {
            cursor->set_dupecache_index(i + 1);
            break;
          }
        }
      }
      m_env->get_changeset().clear();
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
      local_txn->abort();
    if ((get_rt_flags() & HAM_RECORD_NUMBER) && !(flags & HAM_OVERWRITE)) {
      if (!(key->flags & HAM_KEY_USER_ALLOC)) {
        key->data = 0;
        key->size = 0;
      }
      ham_assert(st != HAM_DUPLICATE_KEY);
    }

    m_env->get_changeset().clear();
    return (st);
  }

  /* no need to append the journal entry - it's appended in insert_txn(),
   * which is called by insert_txn() */

  /*
   * record numbers: return key in host endian! and store the incremented
   * record number
   */
  if (get_rt_flags() & HAM_RECORD_NUMBER) {
    recno = ham_db2h64(recno);
    memcpy(key->data, &recno, sizeof(ham_u64_t));
    key->size = sizeof(ham_u64_t);
  }

  ham_assert(st == 0);

  /* set a flag that the cursor just completed an Insert-or-find
   * operation; this information is needed in ham_cursor_move */
  cursor->set_lastop(Cursor::CURSOR_LOOKUP_INSERT);

  if (local_txn) {
    m_env->get_changeset().clear();
    return (local_txn->commit());
  }
  else if (m_env->get_flags() & HAM_ENABLE_RECOVERY
      && !(m_env->get_flags() & HAM_ENABLE_TRANSACTIONS))
    return (m_env->get_changeset().flush(DUMMY_LSN));
  else
    return (st);
}

ham_status_t
LocalDatabase::cursor_erase(Cursor *cursor, ham_u32_t flags)
{
  ham_status_t st;
  Transaction *local_txn = 0;

  /* if user did not specify a transaction, but transactions are enabled:
   * create a temporary one */
  if (!cursor->get_txn() && (get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    local_txn = new Transaction(m_env, 0, HAM_TXN_TEMPORARY);
    cursor->set_txn(local_txn);
  }

  /* this function will do all the work */
  st=cursor->erase(cursor->get_txn() ? cursor->get_txn() : local_txn, flags);

  /* clear the changeset */
  m_env->get_changeset().clear();

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
      local_txn->abort();
    m_env->get_changeset().clear();
    return (st);
  }

  ham_assert(st == 0);

  /* no need to append the journal entry - it's appended in erase_txn(),
   * which is called by txn_cursor_erase() */

  if (local_txn) {
    m_env->get_changeset().clear();
    return (local_txn->commit());
  }
  else if (m_env->get_flags() & HAM_ENABLE_RECOVERY
      && !(m_env->get_flags() & HAM_ENABLE_TRANSACTIONS))
    return (m_env->get_changeset().flush(DUMMY_LSN));
  else
    return (st);
}

ham_status_t
LocalDatabase::cursor_find(Cursor *cursor, ham_key_t *key,
          ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st;
  ham_u64_t recno = 0;
  Transaction *local_txn = 0;
  TransactionCursor *txnc = cursor->get_txn_cursor();

  /*
   * record number: make sure that we have a valid key structure,
   * and translate the record number to database endian
   */
  if (get_rt_flags() & HAM_RECORD_NUMBER) {
    if (key->size != sizeof(ham_u64_t) || !key->data) {
      ham_trace(("key->size must be 8, key->data must not be NULL"));
      return (HAM_INV_PARAMETER);
    }
    recno = *(ham_u64_t *)key->data;
    recno = ham_h2db64(recno);
    *(ham_u64_t *)key->data = recno;
  }

  /* purge cache if necessary */
  st = get_env()->purge_cache();
  if (st)
    return (st);

  /* if user did not specify a transaction, but transactions are enabled:
   * create a temporary one */
  if (!cursor->get_txn() && (get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    local_txn = new Transaction(m_env, 0, HAM_TXN_TEMPORARY);
    cursor->set_txn(local_txn);
  }

  /* reset the dupecache */
  cursor->clear_dupecache();

  /*
   * first try to find the key in the transaction tree. If it exists and
   * is NOT a duplicate then return its record. If it does not exist or
   * it has duplicates then lookup the key in the btree.
   *
   * in non-Transaction mode directly search through the btree.
   */
  if (cursor->get_txn() || local_txn) {
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
        ham_assert(op->get_flags() & TransactionOperation::TXN_OP_ERASE);
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
          (void)cursor->sync(Cursor::CURSOR_SYNC_ONLY_EQUAL_KEY,
                  &is_equal);
          if (!is_equal)
            cursor->set_to_nil(Cursor::CURSOR_BTREE);
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
      (void)cursor->sync(Cursor::CURSOR_SYNC_ONLY_EQUAL_KEY, &is_equal);
      if (!is_equal)
        cursor->set_to_nil(Cursor::CURSOR_BTREE);
    }
    cursor->couple_to_txnop();
    if (!cursor->get_dupecache_count()) {
      if (record)
        st = txnc->get_record(record);
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
        st = cursor->get_txn_cursor()->get_record(record);
      else
        st = cursor->get_btree_cursor()->move(0, record, 0);
    }
  }
  else {
    if (cursor->is_coupled_to_txnop() && record)
      st = cursor->get_txn_cursor()->get_record(record);
  }

bail:
  /* if we created a temp. txn then clean it up again */
  if (local_txn)
    cursor->set_txn(0);

  if (st) {
    if (local_txn)
      local_txn->abort();
    m_env->get_changeset().clear();
    return (st);
  }

  /* record number: re-translate the number to host endian */
  if (get_rt_flags() & HAM_RECORD_NUMBER)
    *(ham_u64_t *)key->data = ham_db2h64(recno);

  /* set a flag that the cursor just completed an Insert-or-find
   * operation; this information is needed in ham_cursor_move */
  cursor->set_lastop(Cursor::CURSOR_LOOKUP_INSERT);

  if (local_txn) {
    m_env->get_changeset().clear();
    return (local_txn->commit());
  }
  else if (m_env->get_flags() & HAM_ENABLE_RECOVERY
      && !(m_env->get_flags() & HAM_ENABLE_TRANSACTIONS))
    return (m_env->get_changeset().flush(DUMMY_LSN));
  else
    return (st);
}

ham_status_t
LocalDatabase::cursor_get_duplicate_count(Cursor *cursor,
          ham_size_t *count, ham_u32_t flags)
{
  ham_status_t st = 0;
  Transaction *local_txn = 0;
  TransactionCursor *txnc = cursor->get_txn_cursor();

  /* purge cache if necessary */
  st = get_env()->purge_cache();
  if (st)
    return (st);

  if (cursor->is_nil(0) && txnc->is_nil())
    return (HAM_CURSOR_IS_NIL);

  /* if user did not specify a transaction, but transactions are enabled:
   * create a temporary one */
  if (!cursor->get_txn() && (get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    local_txn = new Transaction(m_env, 0, HAM_TXN_TEMPORARY);
    cursor->set_txn(local_txn);
  }

  /* this function will do all the work */
  st = cursor->get_duplicate_count(
            cursor->get_txn() ? cursor->get_txn() : local_txn,
            count, flags);

  /* if we created a temp. txn then clean it up again */
  if (local_txn)
    cursor->set_txn(0);

  if (st) {
    if (local_txn)
      local_txn->abort();
    m_env->get_changeset().clear();
    return (st);
  }

  ham_assert(st == 0);

  /* set a flag that the cursor just completed an Insert-or-find
   * operation; this information is needed in ham_cursor_move */
  cursor->set_lastop(Cursor::CURSOR_LOOKUP_INSERT);

  if (local_txn) {
    m_env->get_changeset().clear();
    return (local_txn->commit());
  }
  else if (m_env->get_flags() & HAM_ENABLE_RECOVERY
      && !(m_env->get_flags() & HAM_ENABLE_TRANSACTIONS))
    return (m_env->get_changeset().flush(DUMMY_LSN));
  else
    return (st);
}

ham_status_t
LocalDatabase::cursor_get_record_size(Cursor *cursor, ham_u64_t *size)
{
  ham_status_t st = 0;
  Transaction *local_txn = 0;
  TransactionCursor *txnc = cursor->get_txn_cursor();

  /* purge cache if necessary */
  st = get_env()->purge_cache();
  if (st)
    return (st);

  if (cursor->is_nil(0) && txnc->is_nil())
    return (HAM_CURSOR_IS_NIL);

  /* if user did not specify a transaction, but transactions are enabled:
   * create a temporary one */
  if (!cursor->get_txn() && (get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    local_txn = new Transaction(m_env, 0, HAM_TXN_TEMPORARY);
    cursor->set_txn(local_txn);
  }

  /* this function will do all the work */
  st = cursor->get_record_size(
                cursor->get_txn() ? cursor->get_txn() : local_txn,
                size);

  /* if we created a temp. txn then clean it up again */
  if (local_txn)
    cursor->set_txn(0);

  m_env->get_changeset().clear();

  if (st) {
    if (local_txn)
      local_txn->abort();
    return (st);
  }

  ham_assert(st == 0);

  /* set a flag that the cursor just completed an Insert-or-find
   * operation; this information is needed in ham_cursor_move */
  cursor->set_lastop(Cursor::CURSOR_LOOKUP_INSERT);

  if (local_txn) {
    m_env->get_changeset().clear();
    return (local_txn->commit());
  }
  else if (m_env->get_flags() & HAM_ENABLE_RECOVERY
      && !(m_env->get_flags() & HAM_ENABLE_TRANSACTIONS))
    return (m_env->get_changeset().flush(DUMMY_LSN));
  else
    return (st);
}

ham_status_t
LocalDatabase::cursor_overwrite(Cursor *cursor,
          ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st;
  Transaction *local_txn = 0;

  /* purge cache if necessary */
  st = get_env()->purge_cache();
  if (st)
    return (st);

  /* if user did not specify a transaction, but transactions are enabled:
   * create a temporary one */
  if (!cursor->get_txn() && (get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    local_txn = new Transaction(m_env, 0, HAM_TXN_TEMPORARY);
    cursor->set_txn(local_txn);
  }

  /* this function will do all the work */
  st = cursor->overwrite(
            cursor->get_txn() ? cursor->get_txn() : local_txn,
            record, flags);

  /* if we created a temp. txn then clean it up again */
  if (local_txn)
    cursor->set_txn(0);

  if (st) {
    if (local_txn)
      local_txn->abort();
    m_env->get_changeset().clear();
    return (st);
  }

  ham_assert(st == 0);

  /* the journal entry is appended in insert_txn() */

  if (local_txn) {
    m_env->get_changeset().clear();
    return (local_txn->commit());
  }
  else if (m_env->get_flags() & HAM_ENABLE_RECOVERY
      && !(m_env->get_flags() & HAM_ENABLE_TRANSACTIONS))
    return (m_env->get_changeset().flush(DUMMY_LSN));
  else
    return (st);
}

ham_status_t
LocalDatabase::cursor_move(Cursor *cursor, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st = 0;
  Transaction *local_txn = 0;

  /* purge cache if necessary */
  st = get_env()->purge_cache();
  if (st)
    return (st);

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

  /* in non-transactional mode - just call the btree function and return */
  if (!(get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    st = cursor->get_btree_cursor()->move(key, record, flags);
    m_env->get_changeset().clear();
    return (st);
  }

  /* if user did not specify a transaction, but transactions are enabled:
   * create a temporary one */
  if (!cursor->get_txn() && (get_rt_flags() & HAM_ENABLE_TRANSACTIONS)) {
    local_txn = new Transaction(m_env, 0, HAM_TXN_TEMPORARY);
    cursor->set_txn(local_txn);
  }

  /* everything else is handled by the cursor function */
  st = cursor->move(key, record, flags);

  /* if we created a temp. txn then clean it up again */
  if (local_txn)
    cursor->set_txn(0);

  m_env->get_changeset().clear();

  /* store the direction */
  if (flags & HAM_CURSOR_NEXT)
    cursor->set_lastop(HAM_CURSOR_NEXT);
  else if (flags & HAM_CURSOR_PREVIOUS)
    cursor->set_lastop(HAM_CURSOR_PREVIOUS);
  else
    cursor->set_lastop(0);

  if (st) {
    if (local_txn)
      local_txn->abort();
    if (st == HAM_KEY_ERASED_IN_TXN)
      st = HAM_KEY_NOT_FOUND;
    /* trigger a sync when the function is called again */
    cursor->set_lastop(0);
    return (st);
  }

  if (local_txn) {
    m_env->get_changeset().clear();
    return (local_txn->commit());
  }
  else
    return (st);
}

void
LocalDatabase::cursor_close_impl(Cursor *cursor)
{
  cursor->close();
}

static bool
db_close_callback(Page *page, Database *db, ham_u32_t flags)
{
  Environment *env = page->get_device()->get_env();

  if (page->get_db() == db && page != env->get_header_page()) {
    (void)page->flush();
    (void)page->uncouple_all_cursors();

    /*
     * if this page has a header, and it's either a B-Tree root page or
     * a B-Tree index page: remove all extended keys from the cache,
     * and/or free their blobs
     *
     * TODO move to btree
     */
    if (page->get_pers() &&
        (!(page->get_flags() & Page::NPERS_NO_HEADER)) &&
          (page->get_type() == Page::TYPE_B_ROOT ||
            page->get_type() == Page::TYPE_B_INDEX)) {
      ham_assert(page->get_db());
      BtreeIndex *be = page->get_db()->get_btree();
      if (be)
        (void)be->free_page_extkeys(page, flags);
    }

    /* free the page */
    (void)page->free();
    return (true);
  }

  return (false);
}

ham_status_t
LocalDatabase::close_impl(ham_u32_t flags)
{
  /* check if this database is modified by an active transaction */
  TransactionIndex *tree = get_optree();
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

  /* get rid of the extkey-cache */
  if (get_extkey_cache()) {
    delete get_extkey_cache();
    set_extkey_cache(0);
  }

  BtreeIndex *be = get_btree();

  /* in-memory-database: free all allocated blobs */
  if (be && m_env->get_flags() & HAM_IN_MEMORY) {
    Transaction *txn;
    free_cb_context_t context = {0};
    context.db = this;
    txn = new Transaction(m_env, 0, HAM_TXN_TEMPORARY);
    (void)be->enumerate(__free_inmemory_blobs_cb, &context);
    (void)txn->commit();
  }

  /* clear the changeset */
  m_env->get_changeset().clear();

  /*
   * flush all pages of this database (but not the header page,
   * it's still required and will be flushed below)
   */
  if (m_env->get_cache())
    (void)m_env->get_cache()->visit(db_close_callback, this, 0);

  /* clean up the transaction tree */
  get_optree()->close();

  /* close the btree */
  if (be) {
    delete be;
    set_btree(0);
  }

  return (0);
}

ham_u16_t
Database::get_keysize()
{
  return (get_btree()->get_keysize());
}

} // namespace hamsterdb
