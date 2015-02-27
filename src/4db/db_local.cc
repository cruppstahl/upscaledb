/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
#include "3delta/delta_factory.h"
#include "3delta/delta_binding.h"
#include "3delta/delta_updates_sorted.h"
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

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

ham_status_t
LocalDatabase::insert_txn(Context *context, ham_key_t *key,
                ham_record_t *record, uint32_t flags, LocalCursor *cursor)
{
  bool is_erased = false;

  // Fetch the BtreeNode which stores the key
  Page *page = m_btree_index->find_leaf(context, key);
  ham_assert(page != 0);
  BtreeNodeProxy *node = m_btree_index->get_node_from_page(page);

  // Check for txn conflicts of this key
  SortedDeltaUpdates::Iterator it = node->deltas().find(key, this);
  if (it != node->deltas().end()) {
    for (DeltaAction *action = (*it)->actions();
            action != 0;
            action = action->next()) {
      // Ignore aborted Transactions
      if (isset(action->flags(), DeltaAction::kIsAborted))
        continue;

      // Is the DeltaUpdate modified by a different Transaction? Then report a
      // conflict.
      if (notset(action->flags(), DeltaAction::kIsCommitted)
          && action->txn_id() != context->txn->get_id())
        return (HAM_TXN_CONFLICT);

      // Otherwise - if the key already exists - return an error unless the key
      // is overwritten or a duplicate is inserted
      if (notset(action->flags(), DeltaAction::kErase)) {
        if (notset(flags, (HAM_OVERWRITE | HAM_DUPLICATE)))
          return (HAM_DUPLICATE_KEY);
      }
      else {
        is_erased = true;
        break;
      }
    }
  }

  // Still here - no conflicts. Check the BtreeNode if the key exists
  if (is_erased == false
      && notset(flags, (HAM_OVERWRITE | HAM_DUPLICATE))
      && node->find(context, key) >= 0)
    return (HAM_DUPLICATE_KEY);

  // If not then finally append a new DeltaUpdate to the node
  DeltaUpdate *du;
  if (it == node->deltas().end()) {
    du = DeltaUpdateFactory::create_delta_update(this, key);
    node->deltas().insert(du, this);
  }
  else
    du = *it;

  // Append a new DeltaAction to the DeltaUpdate
  DeltaAction *da = DeltaUpdateFactory::create_delta_action(du,
                        context->txn ? context->txn->get_id() : 0,
                        lenv()->next_lsn(), 
                        issetany(flags, HAM_PARTIAL | HAM_DUPLICATE)
                            ? DeltaAction::kInsertDuplicate
                            : (isset(flags, HAM_OVERWRITE)
                                ? DeltaAction::kInsertOverwrite
                                : DeltaAction::kInsert),
                        flags,
                        cursor ? cursor->m_dupecache_index : -1,
                        record);
  du->append(da);

  // if there's a cursor then couple it to the DeltaUpdate
  if (cursor) {
    cursor->clear_duplicate_cache();
    du->binding().attach(cursor->get_btree_cursor());
    cursor->get_btree_cursor()->couple_to_page(page, 0, 0); // TODO
    cursor->get_btree_cursor()->attach_to_deltaaction(da); // TODO
    cursor->m_currently_using = LocalCursor::kDeltaUpdate; // TODO
    // if there (maybe) are duplicates then build a DuplicateCache and
    // attach the cursor to the inserted duplicate

    // TODO can we remove this loop and directly insert the DeltaAction
    // at the correct position??
    cursor->update_duplicate_cache(context);
    for (size_t i = 0; i < cursor->m_duplicate_cache.size(); i++) {
      if (cursor->m_duplicate_cache[i].action() == da) {
        cursor->m_dupecache_index = (int)i;
        break;
      }
    }
  }

  if (context->txn)
    context->txn->add_delta_action(da);

  // append journal entry
  if (isset(m_env->get_flags(),
              HAM_ENABLE_RECOVERY | HAM_ENABLE_TRANSACTIONS)) {
    Journal *j = lenv()->journal();
    j->append_insert(this, context->txn, key, record,
              isset(flags, HAM_DUPLICATE) ? flags : flags | HAM_OVERWRITE,
              da->lsn());
  }

  return (0);
}

static SortedDeltaUpdates::Iterator
adjust_iterator(SortedDeltaUpdates &deltas, SortedDeltaUpdates::Iterator it,
                    uint32_t flags)
{
  if (isset(flags, HAM_FIND_LT_MATCH) && it > deltas.begin())
    return (--it);

  if (isset(flags, HAM_FIND_GT_MATCH) && it < deltas.end() - 1)
    return (++it);

  return (deltas.end());
}

bool
LocalDatabase::is_key_erased(Context *context, BtreeNodeProxy *node,
                    int slot, ham_key_t *key)
{
  SortedDeltaUpdates::Iterator it = node->deltas().find(key, context->db);
  if (it == node->deltas().end())
    return (false);

  // retrieve number of records in the Btree
  int inserted = node->get_record_count(context, slot);

  // now add the DeltaUpdates
  DeltaUpdate *du = *it;
  for (DeltaAction *action = du->actions();
                  action != 0;
                  action = action->next()) {
    if (notset(action->flags(), DeltaAction::kIsCommitted)
              && action->txn_id() != context->txn->get_id())
      continue;
    if (isset(action->flags(), DeltaAction::kErase)) {
      if (action->referenced_duplicate() == -1)
        inserted = 0;
      else
        inserted--;
    }
    else if (isset(action->flags(), DeltaAction::kInsert)) {
      inserted = 1;
    }
    else if (isset(action->flags(), DeltaAction::kInsertDuplicate)) {
      inserted++;
    }
  }

  // No records left? then the key was deleted
  ham_assert(inserted >= 0);
  return (inserted == 0);
} 

ham_status_t
LocalDatabase::find_approx_txn(Context *context, LocalCursor *cursor,
                ham_key_t *key, ham_record_t *record, uint32_t flags)
{
  DeltaAction *found_action = 0;
  ByteArray *pkey_arena = &key_arena(context->txn);
  ByteArray *precord_arena = &record_arena(context->txn);

  ham_assert(cursor != 0);

  ham_key_set_intflags(key,
        (ham_key_get_intflags(key) & (~BtreeKey::kApproximate)));

  // Fetch the BtreeNode which stores the key
  Page *page = m_btree_index->find_leaf(context, key);
  ham_assert(page != 0);
  BtreeNodeProxy *node = m_btree_index->get_node_from_page(page);

  // Check for txn conflicts of this key
  //
  // TODO
  // it would be nice if the following two blocks would be comined in a
  // call to node->deltas().find_approx(key, this, flags), which returns
  // an adjusted iterator
  SortedDeltaUpdates::Iterator it = node->deltas().find(key, this, flags);

  if (notset(flags, HAM_FIND_EXACT_MATCH)
        && it != node->deltas().end()
        && !m_btree_index->compare_keys((*it)->key(), key)) {
    it = adjust_iterator(node->deltas(), it, flags);
    ham_key_set_intflags(key,
          (ham_key_get_intflags(key) | BtreeKey::kApproximate));
  }

  if (it == node->deltas().end()) {
    cursor->m_btree_cursor.detach_from_deltaupdate();
    goto find_in_btree;
  }

find_in_deltas:
  for (DeltaAction *action = (*it)->actions();
          action != 0;
          action = action->next()) {
    // Ignore aborted Transactions
    if (isset(action->flags(), DeltaAction::kIsAborted))
      continue;

    // Is the DeltaUpdate modified by a different Transaction? Then move
    // to the next key
    if (notset(action->flags(), DeltaAction::kIsCommitted)
          && action->txn_id() != context->txn->get_id()) {
      it = adjust_iterator(node->deltas(), it, flags);
      if (it != node->deltas().end()) {
        ham_key_set_intflags(key,
            (ham_key_get_intflags(key) | BtreeKey::kApproximate));
        found_action = 0;
        goto find_in_deltas;
      }
      break;
    }

    // Otherwise - if the key already exists - return an error unless the key
    // is overwritten or a duplicate is inserted
    if (isset(action->flags(), DeltaAction::kErase)) {
      if (action->referenced_duplicate() == -1) {
        it = adjust_iterator(node->deltas(), it, flags);
        if (it != node->deltas().end()) {
          found_action = 0;
          goto find_in_deltas;
        }
      }
      // the caller will check the duplicate cache for a valid duplicate
      // TODO
      break;
    }

    // If the key exists then return its record. A deep copy is required
    // since action->record() might be invalidated
    if (issetany(action->flags(), DeltaAction::kInsert
                                    | DeltaAction::kInsertOverwrite
                                    | DeltaAction::kInsertDuplicate)) {
      found_action = action;
      continue;
    }
  }

  if (it != node->deltas().end()) {
    (*it)->binding().attach(cursor->get_btree_cursor());
    cursor->get_btree_cursor()->couple_to_page(page, 0, 0); // TODO
    cursor->m_currently_using = LocalCursor::kDeltaUpdate; // TODO
  }

  if (found_action == 0)
    goto find_in_btree;

  // successfully found a DeltaUpdate? Then return without looking at the
  // btree
  if (isset(flags, HAM_FIND_EXACT_MATCH)
        && !m_btree_index->compare_keys((*it)->key(), key)) {
    if (record)
      copy_record(found_action->record(), &record_arena(context->txn), record);
    return (0);
  }

  if (HAM_KEY_NOT_FOUND == m_btree_index->find_in_leaf(context, cursor,
                        page, key, pkey_arena, 0, 0, flags)) {
    // TODO pretend that txn/btree are not equal.
    // This will trigger a call to LocalCursor::sync() when moving 
    // forward/backward with the cursor
    cursor->m_last_cmp = 1;
  }
  else {
    // Btree key exists - check if it's deleted
    Page *page;
    int slot;
    cursor->m_btree_cursor.get_coupled_key(&page, &slot, 0);
    node = m_btree_index->get_node_from_page(page);
    if (slot < (int)node->get_count()
        && !is_key_erased(context, node, slot, key)) {
      // Check which key is "closer" to the requested key; the B-Tree key
      // or the DeltaUpdate key?
      cursor->m_last_cmp = m_btree_index->compare_keys((*it)->key(), key);
      if (isset(flags, HAM_FIND_LT_MATCH)) {
        if (cursor->m_last_cmp < 0)
          cursor->m_currently_using = LocalCursor::kBtree; // TODO
        else
          cursor->m_currently_using = LocalCursor::kDeltaUpdate; // TODO
      }
      else if (isset(flags, HAM_FIND_GT_MATCH)) {
        if (cursor->m_last_cmp < 0)
          cursor->m_currently_using = LocalCursor::kDeltaUpdate; // TODO
        else
          cursor->m_currently_using = LocalCursor::kBtree; // TODO
      }
    }
  }

  // If duplicate keys are enabled: build a duplicate table
  // Now copy the record and return
  // TODO only if duplicates are disabled; otherwise the caller will
  // build a duplicate table and copy the record
  if (record) {
    if (cursor->m_currently_using == LocalCursor::kDeltaUpdate) {
      copy_key((*it)->key(), &key_arena(context->txn), key);
      copy_record(found_action->record(), &record_arena(context->txn), record);
    }
    else
      cursor->move(context, 0, record, 0);
  }
  return (0);

find_in_btree:
  // Still here? then either there is no DeltaUpdate, or all of them were
  // ignored. Fetch the key and the record from the btree
  cursor->m_currently_using = LocalCursor::kBtree; // TODO
  cursor->m_last_cmp = 1; // TODO see comment above

  // Search through the btree till we found a key which was not erased
  ham_status_t st = 0;
  int slot = -1;
  uint32_t btree_flags = flags & ~HAM_FIND_EXACT_MATCH;
  if (isset(flags, HAM_FIND_LT_MATCH))
    btree_flags |= HAM_CURSOR_PREVIOUS;
  else
    btree_flags |= HAM_CURSOR_PREVIOUS;

  do {
    if (slot == -1)
      st = cursor->m_btree_cursor.find(context, key, pkey_arena,
                                    record, precord_arena, flags);
    else
      st = cursor->m_btree_cursor.move(context, key, pkey_arena,
                                    record, precord_arena, btree_flags);
    if (st)
      break;

    Page *page;
    cursor->m_btree_cursor.get_coupled_key(&page, &slot, 0);
    node = m_btree_index->get_node_from_page(page);
  } while (is_key_erased(context, node, slot, key));

  return (st);
}

ham_status_t
LocalDatabase::find_txn(Context *context, LocalCursor *cursor,
                ham_key_t *key, ham_record_t *record, uint32_t flags)
{
  ByteArray *pkey_arena = &key_arena(context->txn);
  ByteArray *precord_arena = &record_arena(context->txn);

  ham_key_set_intflags(key,
        (ham_key_get_intflags(key) & (~BtreeKey::kApproximate)));

  // Fetch the BtreeNode which stores the key
  Page *page = m_btree_index->find_leaf(context, key);
  ham_assert(page != 0);
  BtreeNodeProxy *node = m_btree_index->get_node_from_page(page);

  // Check for txn conflicts of this key
  SortedDeltaUpdates::Iterator it = node->deltas().find(key, this);
  if (it == node->deltas().end()) {
    cursor->m_btree_cursor.detach_from_deltaupdate();
  }
  else {
    DeltaAction *found_action = 0;

    for (DeltaAction *action = (*it)->actions();
            action != 0;
            action = action->next()) {
      // Ignore aborted Transactions
      if (isset(action->flags(), DeltaAction::kIsAborted))
        continue;

      // Is the DeltaUpdate modified by a different Transaction? Then report a
      // conflict.
      if (notset(action->flags(), DeltaAction::kIsCommitted)
          && action->txn_id() != context->txn->get_id())
        return (HAM_TXN_CONFLICT);

      // Otherwise - if the key already exists - return an error unless the key
      // is overwritten or a duplicate is inserted
      if (isset(action->flags(), DeltaAction::kErase)) {
        if (action->referenced_duplicate() == -1) {
          found_action = 0;
          continue;
        }
        // the caller will check the duplicate cache for a valid duplicate
        break;
      }

      // If the key exists then return its record. A deep copy is required
      // since action->record() might be invalidated
      if (issetany(action->flags(), DeltaAction::kInsert
                                      | DeltaAction::kInsertOverwrite
                                      | DeltaAction::kInsertDuplicate)) {
        found_action = action;
        continue;
      }
    }

    if (cursor) {
      (*it)->binding().attach(cursor->get_btree_cursor());
      cursor->get_btree_cursor()->couple_to_page(page, 0, 0); // TODO
      cursor->m_currently_using = LocalCursor::kDeltaUpdate; // TODO
    }

    if (found_action == 0)
      goto find_in_btree;

    if (cursor) {
      if (HAM_KEY_NOT_FOUND == m_btree_index->find_in_leaf(context, cursor,
                            page, key, pkey_arena, 0, 0, flags))
        // TODO pretend that txn/btree are not equal.
        // This will trigger a call to LocalCursor::sync() when moving 
        // forward/backward with the cursor
        cursor->m_last_cmp = 1;
    }
    // If duplicate keys are enabled: build a duplicate table
    // Now copy the record and return
    // TODO only if duplicates are disabled; otherwise the caller will
    // build a duplicate table and copy the record
    if (record)
      copy_record(found_action->record(), &record_arena(context->txn), record);
    return (0);
  }

find_in_btree:
  // Still here? then either there is no DeltaUpdate, or all of them were
  // ignored. Fetch the key and the record from the btree
  if (cursor) {
    cursor->m_currently_using = LocalCursor::kBtree; // TODO
    cursor->m_last_cmp = 1; // TODO see comment above
  }
  return (m_btree_index->find_in_leaf(context, cursor, page, key,
                                pkey_arena, record, precord_arena, flags));
}

ham_status_t
LocalDatabase::erase_txn(Context *context, ham_key_t *key, uint32_t flags,
                LocalCursor *cursor)
{
  // Fetch the BtreeNode which stores the key
  Page *page = m_btree_index->find_leaf(context, key);
  ham_assert(page != 0);
  BtreeNodeProxy *node = m_btree_index->get_node_from_page(page);

  DeltaAction *insert_action = 0;

  // Check for txn conflicts of this key
  SortedDeltaUpdates::Iterator it = node->deltas().find(key, this);
  if (it != node->deltas().end()) {
    for (DeltaAction *action = (*it)->actions();
            action != 0;
            action = action->next()) {
      // Ignore aborted Transactions
      if (isset(action->flags(), DeltaAction::kIsAborted))
        continue;

      // Is the DeltaUpdate modified by a different Transaction? Then report a
      // conflict.
      if (notset(action->flags(), DeltaAction::kIsCommitted)
          && action->txn_id() != context->txn->get_id())
        return (HAM_TXN_CONFLICT);

      // If the key was already erased then return an error
      if (isset(action->flags(), DeltaAction::kErase)) {
        if (action->referenced_duplicate() == -1)
          insert_action = 0;
      }
      else if (issetany(action->flags(), DeltaAction::kInsert
                                      | DeltaAction::kInsertOverwrite
                                      | DeltaAction::kInsertDuplicate)) {
        insert_action = action;
      }
    }
  }

  // Still here - no conflicts. If a key was not yet found then check the
  // BtreeNode if the key exists
  if (insert_action == 0 && node->find(context, key) == -1)
    return (HAM_KEY_NOT_FOUND);

  // If not then finally append a new DeltaUpdate to the node
  DeltaUpdate *du;
  if (it == node->deltas().end()) {
    du = DeltaUpdateFactory::create_delta_update(this, key);
    node->deltas().insert(du, this);
  }
  else
    du = *it;

  // Append a new DeltaAction to the DeltaUpdate
  DeltaAction *da = DeltaUpdateFactory::create_delta_action(du,
                        context->txn ? context->txn->get_id() : 0,
                        lenv()->next_lsn(), 
                        DeltaAction::kErase,
                        flags,
                        cursor ? cursor->m_dupecache_index : -1, 0);
  du->append(da);

  if (context->txn)
    context->txn->add_delta_action(da);

  // TODO combine both calls
  if (cursor) {
    cursor->set_to_nil();
    cursor->get_btree_cursor()->detach_from_deltaupdate();
  }

  /* append journal entry */
  if (isset(m_env->get_flags(),
              HAM_ENABLE_RECOVERY | HAM_ENABLE_TRANSACTIONS)) {
    Journal *j = lenv()->journal();
    j->append_erase(this, context->txn, key, 0,
                    flags | HAM_ERASE_ALL_DUPLICATES, da->lsn());
  }

  return (0);
}

ham_status_t
LocalDatabase::create(Context *context, PBtreeHeader *btree_header)
{
  /* the header page is now modified */
  Page *header = lenv()->page_manager()->fetch(context, 0);
  header->set_dirty(true);

  /* set the flags; strip off run-time (per session) flags for the btree */
  uint32_t persistent_flags = get_flags();
  persistent_flags &= ~(HAM_CACHE_UNLIMITED
            | HAM_DISABLE_MMAP
            | HAM_ENABLE_FSYNC
            | HAM_READ_ONLY
            | HAM_ENABLE_RECOVERY
            | HAM_AUTO_RECOVERY
            | HAM_ENABLE_TRANSACTIONS);

  switch (m_config.key_type) {
    case HAM_TYPE_UINT8:
      m_config.key_size = 1;
      break;
    case HAM_TYPE_UINT16:
      m_config.key_size = 2;
      break;
    case HAM_TYPE_REAL32:
    case HAM_TYPE_UINT32:
      m_config.key_size = 4;
      break;
    case HAM_TYPE_REAL64:
    case HAM_TYPE_UINT64:
      m_config.key_size = 8;
      break;
  }

  // if we cannot fit at least 10 keys in a page then refuse to continue
  if (m_config.key_size != HAM_KEY_SIZE_UNLIMITED) {
    if (lenv()->config().page_size_bytes / (m_config.key_size + 8) < 10) {
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
  if (m_config.record_size != HAM_RECORD_SIZE_UNLIMITED) {
    if (m_config.record_size <= 8
        || (m_config.record_size <= kInlineRecordThreshold
          && lenv()->config().page_size_bytes
                / (m_config.key_size + m_config.record_size) > 500)) {
      persistent_flags |= HAM_FORCE_RECORDS_INLINE;
      m_config.flags |= HAM_FORCE_RECORDS_INLINE;
    }
  }

  // create the btree
  m_btree_index.reset(new BtreeIndex(this, btree_header, persistent_flags,
                        m_config.key_type, m_config.key_size));

  /* initialize the btree */
  m_btree_index->create(context, m_config.key_type, m_config.key_size,
                  m_config.record_size);

  /* and the TransactionIndex */
  m_txn_index.reset(new TransactionIndex(this));

  return (0);
}

ham_status_t
LocalDatabase::open(Context *context, PBtreeHeader *btree_header)
{
  /*
   * set the database flags; strip off the persistent flags that may have been
   * set by the caller, before mixing in the persistent flags as obtained
   * from the btree.
   */
  uint32_t flags = get_flags();
  flags &= ~(HAM_CACHE_UNLIMITED
            | HAM_DISABLE_MMAP
            | HAM_ENABLE_FSYNC
            | HAM_READ_ONLY
            | HAM_ENABLE_RECOVERY
            | HAM_AUTO_RECOVERY
            | HAM_ENABLE_TRANSACTIONS);

  m_config.key_type = btree_header->key_type();
  m_config.key_size = btree_header->key_size();

  /* create the BtreeIndex */
  m_btree_index.reset(new BtreeIndex(this, btree_header,
                            flags | btree_header->flags(),
                            btree_header->key_type(),
                            btree_header->key_size()));

  ham_assert(!(m_btree_index->flags() & HAM_CACHE_UNLIMITED));
  ham_assert(!(m_btree_index->flags() & HAM_DISABLE_MMAP));
  ham_assert(!(m_btree_index->flags() & HAM_ENABLE_FSYNC));
  ham_assert(!(m_btree_index->flags() & HAM_READ_ONLY));
  ham_assert(!(m_btree_index->flags() & HAM_ENABLE_RECOVERY));
  ham_assert(!(m_btree_index->flags() & HAM_AUTO_RECOVERY));
  ham_assert(!(m_btree_index->flags() & HAM_ENABLE_TRANSACTIONS));

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

  // fetch the current record number
  if ((get_flags() & (HAM_RECORD_NUMBER32 | HAM_RECORD_NUMBER64))) {
    ham_key_t key = {};
    LocalCursor *c = new LocalCursor(this, 0);
    ham_status_t st = cursor_move_impl(context, c, &key, 0, HAM_CURSOR_LAST);
    cursor_close(c);
    delete c;
    if (st)
      return (st == HAM_KEY_NOT_FOUND ? 0 : st);

    if (get_flags() & HAM_RECORD_NUMBER32)
      m_recno = *(uint32_t *)key.data;
    else
      m_recno = *(uint64_t *)key.data;
  }

  return (0);
}

struct MetricsVisitor : public BtreeVisitor {
  MetricsVisitor(ham_env_metrics_t *metrics)
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
  
  ham_env_metrics_t *m_metrics;
};

void
LocalDatabase::fill_metrics(ham_env_metrics_t *metrics)
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

ham_status_t
LocalDatabase::get_parameters(ham_parameter_t *param)
{
  try {
    Context context(lenv(), 0, this);

    Page *page = 0;
    ham_parameter_t *p = param;

    if (p) {
      for (; p->name; p++) {
        switch (p->name) {
        case HAM_PARAM_KEY_SIZE:
          p->value = m_config.key_size;
          break;
        case HAM_PARAM_KEY_TYPE:
          p->value = m_config.key_type;
          break;
        case HAM_PARAM_RECORD_SIZE:
          p->value = m_config.record_size;
          break;
        case HAM_PARAM_FLAGS:
          p->value = (uint64_t)get_flags();
          break;
        case HAM_PARAM_DATABASE_NAME:
          p->value = (uint64_t)name();
          break;
        case HAM_PARAM_MAX_KEYS_PER_PAGE:
          p->value = 0;
          page = lenv()->page_manager()->fetch(&context,
                          m_btree_index->root_address(),
                        PageManager::kReadOnly);
          if (page) {
            BtreeNodeProxy *node = m_btree_index->get_node_from_page(page);
            p->value = node->estimate_capacity();
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
          throw Exception(HAM_INV_PARAMETER);
        }
      }
    }
  }
  catch (Exception &ex) {
    return (ex.code);
  }
  return (0);
}

ham_status_t
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

ham_status_t
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
    if (get_flags() & HAM_ENABLE_TRANSACTIONS)
      keycount += m_txn_index->count(&context, txn, distinct);

    *pcount = keycount;
    return (0);
  }
  catch (Exception &ex) {
    return (ex.code);
  }
}

ham_status_t
LocalDatabase::scan(Transaction *txn, ScanVisitor *visitor, bool distinct)
{
  ham_status_t st = 0;
  LocalCursor *cursor = 0;

  if (!(get_flags() & HAM_ENABLE_DUPLICATE_KEYS))
    distinct = true;

  try {
    Context context(lenv(), (LocalTransaction *)txn, this);

    Page *page;
    ham_key_t key = {0};

    /* purge cache if necessary */
    lenv()->page_manager()->purge_cache(&context);

    /* create a cursor, move it to the first key */
    cursor = (LocalCursor *)cursor_create_impl(txn);

    st = cursor_move_impl(&context, cursor, &key, 0, HAM_CURSOR_FIRST);
    if (st)
      goto bail;

    /* only transaction keys? then use a regular cursor */
    if (!cursor->is_coupled_to_btree()) {
      do {
        /* process the key */
        (*visitor)(key.data, key.size, distinct
                                        ? cursor->duplicate_count(&context)
                                        : 1);
      } while ((st = cursor_move_impl(&context, cursor, &key,
                            0, HAM_CURSOR_NEXT)) == 0);
      goto bail;
    }

    /* only btree keys? then traverse page by page */
    if (!(get_flags() & HAM_ENABLE_TRANSACTIONS)) {
      ham_assert(cursor->is_coupled_to_btree());

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
      ham_key_t *txnkey = 0;
      if (cursor->get_txn_cursor()->get_coupled_op())
        txnkey = cursor->get_txn_cursor()->get_coupled_op()->get_node()->get_key();
      // no (more) transactional keys left - process the current key, then
      // scan the remaining keys directly in the btree
      if (!txnkey) {
        /* process the key */
        (*visitor)(key.data, key.size, distinct
                                        ? cursor->duplicate_count(&context)
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
                                        ? cursor->duplicate_count(&context)
                                        : 1);
        } while ((st = cursor_move_impl(&context, cursor, &key,
                                0, HAM_CURSOR_NEXT)) == 0);

        if (st != HAM_SUCCESS)
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
                            0, HAM_CURSOR_NEXT)) == 0) {
      (*visitor)(key.data, key.size, distinct
                                     ? cursor->duplicate_count(&context)
                                     : 1);
    }

bail:
    if (cursor) {
      cursor->close();
      delete cursor;
    }
    return (st == HAM_KEY_NOT_FOUND ? 0 : st);
  }
  catch (Exception &ex) {
    if (cursor) {
      cursor->close();
      delete cursor;
    }
    return (ex.code);
  }
}

ham_status_t
LocalDatabase::insert(Cursor *hcursor, Transaction *txn, ham_key_t *key,
            ham_record_t *record, uint32_t flags)
{
  LocalCursor *cursor = (LocalCursor *)hcursor;
  Context context(lenv(), (LocalTransaction *)txn, this);

  try {
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

    ByteArray *arena = &key_arena(txn);

    /*
     * record number: make sure that we have a valid key structure,
     * and lazy load the last used record number
     *
     * TODO TODO
     * too much duplicated code
     */
    uint64_t recno = 0;
    if (get_flags() & HAM_RECORD_NUMBER64) {
      if (flags & HAM_OVERWRITE) {
        ham_assert(key->size == sizeof(uint64_t));
        ham_assert(key->data != 0);
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
      flags |= HAM_HINT_APPEND;
    }
    else if (get_flags() & HAM_RECORD_NUMBER32) {
      if (flags & HAM_OVERWRITE) {
        ham_assert(key->size == sizeof(uint32_t));
        ham_assert(key->data != 0);
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
      flags |= HAM_HINT_APPEND;
    }

    ham_status_t st = 0;
    LocalTransaction *local_txn = 0;

    /* purge cache if necessary */
    if (!txn && (get_flags() & HAM_ENABLE_TRANSACTIONS)) {
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

ham_status_t
LocalDatabase::erase(Cursor *hcursor, Transaction *txn, ham_key_t *key,
                uint32_t flags)
{
  LocalCursor *cursor = (LocalCursor *)hcursor;
  Context context(lenv(), (LocalTransaction *)txn, this);

  try {
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

    if (!txn && (get_flags() & HAM_ENABLE_TRANSACTIONS)) {
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

ham_status_t
LocalDatabase::find(Cursor *hcursor, Transaction *txn, ham_key_t *key,
            ham_record_t *record, uint32_t flags)
{
  LocalCursor *cursor = (LocalCursor *)hcursor;
  Context context(lenv(), (LocalTransaction *)txn, this);

  try {
    ham_status_t st = 0;

    /* Duplicates AND Transactions require a Cursor because only
     * Cursors can build lists of duplicates.
     * TODO not exception safe - if find() throws then the cursor is not closed
     */
    if (!cursor
          && (get_flags() & (HAM_ENABLE_DUPLICATE_KEYS | HAM_ENABLE_TRANSACTIONS))) {
      LocalCursor *c = (LocalCursor *)cursor_create_impl(txn);
      st = find(c, txn, key, record, flags);
      c->close();
      delete c;
      return (st);
    }

    if (m_config.key_size != HAM_KEY_SIZE_UNLIMITED
        && key->size != m_config.key_size) {
      ham_trace(("invalid key size (%u instead of %u)",
            key->size, m_config.key_size));
      return (HAM_INV_KEY_SIZE);
    }

    // cursor: reset the dupecache, set to nil
    if (cursor)
      cursor->set_to_nil(LocalCursor::kBoth);

    st = find_impl(&context, cursor, key, record, flags);
    if (st)
      return (finalize(&context, st, 0));

    // if a cursor is used w/ duplicates and transactions (DeltaUpdates)
    // enabled then the duplicates have to be consolidated in a DuplicateCache
    if (cursor != 0
            && isset(get_flags(),
                        HAM_ENABLE_DUPLICATE_KEYS | HAM_ENABLE_TRANSACTIONS)) {
      cursor->update_duplicate_cache(&context);
      if (cursor->m_duplicate_cache.size() == 0)
        return (finalize(&context, HAM_KEY_NOT_FOUND, 0));
      cursor->couple_to_duplicate(0);
      if (record)
        cursor->move(&context, 0, record, 0);
    }

    /* set a flag that the cursor just completed an Insert-or-find
     * operation; this information is needed in ham_cursor_move */
    if (cursor)
      cursor->set_last_operation(LocalCursor::kLookupOrInsert);

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

ham_status_t
LocalDatabase::cursor_move(Cursor *hcursor, ham_key_t *key,
                ham_record_t *record, uint32_t flags)
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

ham_status_t
LocalDatabase::cursor_move_impl(Context *context, LocalCursor *cursor,
                ham_key_t *key, ham_record_t *record, uint32_t flags)
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

  /* everything else is handled by the cursor function */
  st = cursor->move(context, key, record, flags);

  /* store the direction */
  if (flags & HAM_CURSOR_NEXT)
    cursor->set_last_operation(HAM_CURSOR_NEXT);
  else if (flags & HAM_CURSOR_PREVIOUS)
    cursor->set_last_operation(HAM_CURSOR_PREVIOUS);
  else
    cursor->set_last_operation(0);

  if (st) {
    if (st == HAM_KEY_ERASED_IN_TXN)
      st = HAM_KEY_NOT_FOUND;
    /* trigger a sync when the function is called again */
    cursor->set_last_operation(0);
    return (st);
  }

  return (0);
}

ham_status_t
LocalDatabase::close_impl(uint32_t flags)
{
  Context context(lenv(), 0, this);

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

  /* in-memory-database: free all allocated blobs */
  if (m_btree_index && m_env->get_flags() & HAM_IN_MEMORY)
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
      if (c->get_dupecache_index() > (int)start)
        c->set_dupecache_index(c->get_dupecache_index() + 1);
    }

next:
    c = (LocalCursor *)c->get_next();
  }
}

void
LocalDatabase::copy_key(ham_key_t *source, ByteArray *arena,
                ham_key_t *dest)
{
  if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
    arena->resize(source->size);
    dest->data = arena->get_ptr();
  }
  ::memcpy(dest->data, source->data, source->size);
  dest->size = source->size;
}

void
LocalDatabase::copy_record(ham_record_t *source, ByteArray *arena,
                ham_record_t *dest)
{
  if (!(dest->flags & HAM_RECORD_USER_ALLOC)) {
    arena->resize(source->size);
    dest->data = arena->get_ptr();
  }
  ::memcpy(dest->data, source->data, source->size);
  dest->size = source->size;
}

ham_status_t
LocalDatabase::flush_txn_operation(Context *context, LocalTransaction *txn,
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
    if (!op->cursor_list()) {
      st = m_btree_index->insert(context, 0, node->get_key(), op->get_record(),
                  op->get_orig_flags() | additional_flag);
    }
    else {
      TransactionCursor *tc1 = op->cursor_list();
      LocalCursor *c1 = tc1->get_parent();
      /* pick the first cursor, get the parent/btree cursor and
       * insert the key/record pair in the btree. The btree cursor
       * then will be coupled to this item. */
      st = m_btree_index->insert(context, c1, node->get_key(), op->get_record(),
                  op->get_orig_flags() | additional_flag);
      if (!st) {
        /* uncouple the cursor from the txn-op, and remove it */
        c1->couple_to_btree(); // TODO merge these two calls
        c1->set_to_nil(LocalCursor::kTxn);

        /* all other (btree) cursors need to be coupled to the same
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
    if (st == HAM_KEY_NOT_FOUND)
      st = 0;
  }

  return (st);
}

struct UncoupleBtreeCursor
{
  void operator()(DeltaUpdate *update, BtreeCursor *cursor) {
    cursor->uncouple_from_deltaupdate(update);
  }
};

void
LocalDatabase::flush_delta_action(Context *context, DeltaAction *action)
{
  DeltaUpdate *update = action->delta_update();

  /*
   * Depending on the type of the operation: actually perform the
   * operation on the btree.
   */
  if (issetany(action->flags(), 
            DeltaAction::kInsert
             | DeltaAction::kInsertOverwrite
             | DeltaAction::kInsertDuplicate)) {
    uint32_t additional_flag = 0;
    if (isset(action->flags(), TransactionOperation::kInsertDuplicate))
      additional_flag |= HAM_DUPLICATE;

    // Now insert the key, and couple the *first* cursor to the inserted
    // key. In 99% of all cases there will NOT be more than one cursor. If
    // it is, then simply uncouple all other cursors. They will be
    // coupled when they are accessed again.
    BtreeCursor *btree_cursor = update->binding().any();
    m_btree_index->insert(context,
                    btree_cursor ? btree_cursor->parent() : 0,
                    update->key(), action->record(),
                    action->original_flags() | additional_flag);
    if (update->binding().size() > 1)
      update->binding().perform(UncoupleBtreeCursor());
    return;
  }

  if (isset(action->flags(), DeltaAction::kErase)) {
    ham_status_t st = m_btree_index->erase(context, 0, update->key(),
                  action->referenced_duplicate(), action->original_flags());
    if (st && st != HAM_KEY_NOT_FOUND)
      throw Exception(st);

    return;
  }
}

ham_status_t
LocalDatabase::drop(Context *context)
{
  m_btree_index->drop(context);
  return (0);
}

ham_status_t
LocalDatabase::insert_impl(Context *context, LocalCursor *cursor,
                ham_key_t *key, ham_record_t *record, uint32_t flags)
{
  lenv()->page_manager()->purge_cache(context);

  /*
   * if transactions are enabled: only insert the key/record pair into
   * the Transaction structure. Otherwise immediately write to the btree.
   */
  ham_status_t st = 0;
  if (context->txn || m_env->get_flags() & HAM_ENABLE_TRANSACTIONS)
    st = insert_txn(context, key, record, flags, cursor);
  else
    st = m_btree_index->insert(context, cursor, key, record, flags);

  /*
   * set a flag that the cursor just completed an Insert-or-find
   * operation; this information is needed in ham_cursor_move()
   */
  if (st == 0 && cursor) {
    cursor->set_last_operation(LocalCursor::kLookupOrInsert);
  }

  return (st);
}

ham_status_t
LocalDatabase::find_impl(Context *context, LocalCursor *cursor,
                ham_key_t *key, ham_record_t *record, uint32_t flags)
{
  /* purge cache if necessary */
  lenv()->page_manager()->purge_cache(context);

  /*
   * if transactions are enabled: read keys from transaction trees,
   * otherwise read immediately from disk
   */
  if (context->txn || m_env->get_flags() & HAM_ENABLE_TRANSACTIONS) {
    if (flags & (HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH))
      return (find_approx_txn(context, cursor, key, record, flags));
    else
      return (find_txn(context, cursor, key, record, flags));
  }

  return (m_btree_index->find(context, cursor, key, &key_arena(context->txn),
                          record, &record_arena(context->txn), flags));
}

ham_status_t
LocalDatabase::erase_impl(Context *context, LocalCursor *cursor, ham_key_t *key,
                uint32_t flags)
{
  ham_status_t st = 0;

  /*
   * if transactions are enabled: append a 'erase key' operation into
   * the txn tree; otherwise immediately erase the key from disk
   */
  if (context->txn || m_env->get_flags() & HAM_ENABLE_TRANSACTIONS) {
    if (cursor) {
      /*
       * !!
       * we have two cases:
       *
       * 1. the cursor is coupled to a btree item (or uncoupled, but not nil)
       *    and NOT to a DeltaUpdate:
       *    - uncouple the btree cursor
       *    - insert an "Erase"-DeltaUpdate for the current key
       *
       * 2. the cursor is attached to a DeltaUpdate; in this case, we have to
       *    - insert an "Erase"-DeltaUpdate for the current key
       *
       * TODO clean up this whole mess. code should be like
       *
       *   if (txn)
       *     erase_txn(txn, cursor->key(), 0, cursor->txn_cursor());
       */
      /* case 1 described above */
      if (cursor->m_currently_using == LocalCursor::kDeltaUpdate) {
        Page *page;
        cursor->get_btree_cursor()->get_coupled_key(&page, 0, 0);
        ham_key_t *key = cursor->get_btree_cursor()->deltaupdate()->key();
        st = erase_txn(context, key, 0, cursor);
      }
      /* case 2 described above */
      else {
        cursor->get_btree_cursor()->uncouple_from_page(context);
        st = erase_txn(context, cursor->get_btree_cursor()->get_uncoupled_key(),
                        0, cursor);
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
    ham_assert(cursor->get_txn_cursor()->is_nil());
    ham_assert(cursor->is_nil(0));
  }

  return (st);
}

ham_status_t
LocalDatabase::finalize(Context *context, ham_status_t status,
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
  else if (env->get_flags() & HAM_ENABLE_RECOVERY
      && !(env->get_flags() & HAM_ENABLE_TRANSACTIONS)) {
    context->changeset.flush(env->next_lsn());
  }
  return (0);
}

LocalTransaction *
LocalDatabase::begin_temp_txn()
{
  LocalTransaction *txn;
  ham_status_t st = lenv()->txn_begin((Transaction **)&txn, 0,
                        HAM_TXN_TEMPORARY | HAM_DONT_LOCK);
  if (st)
    throw Exception(st);
  return (txn);
}

} // namespace hamsterdb
