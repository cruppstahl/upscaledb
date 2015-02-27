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

#include <string.h>

// Always verify that a file of level N does not include headers > N!
#include "3btree/btree_cursor.h"
#include "3btree/btree_index.h"
#include "3btree/btree_node_proxy.h"
#include "3delta/delta_binding.h"
#include "3page_manager/page_manager.h"
#include "4cursor/cursor_local.h"
#include "4env/env_local.h"
#include "4txn/txn_local.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

using namespace hamsterdb;

LocalCursor::LocalCursor(LocalDatabase *db, Transaction *txn)
  : Cursor(db, txn), m_txn_cursor(this), m_btree_cursor(this),
    m_dupecache_index(-1), m_last_operation(0), m_flags(0), m_last_cmp(0),
    m_is_first_use(true), m_currently_using(0), m_btree_eof(false)
{
  m_duplicate_cache.reserve(8);
}

LocalCursor::LocalCursor(LocalCursor &other)
  : Cursor(other), m_txn_cursor(this), m_btree_cursor(this)
{
  m_txn = other.m_txn;
  m_next = other.m_next;
  m_previous = other.m_previous;
  m_dupecache_index = other.m_dupecache_index;
  m_duplicate_cache = other.m_duplicate_cache;
  m_last_operation = other.m_last_operation;
  m_last_cmp = other.m_last_cmp;
  m_currently_using = other.m_currently_using;
  m_flags = other.m_flags;
  m_is_first_use = other.m_is_first_use;
  m_btree_eof = other.m_btree_eof;

  m_btree_cursor.clone(&other.m_btree_cursor);
  m_txn_cursor.clone(&other.m_txn_cursor);

  if (m_db->get_flags() & HAM_ENABLE_DUPLICATE_KEYS)
    other.m_duplicate_cache = m_duplicate_cache;
}

void
LocalCursor::update_duplicate_cache(Context *context, bool force_sync)
{
  if (notset(m_db->get_flags(), HAM_ENABLE_DUPLICATE_KEYS))
    return;

  /* if the cache already exists: no need to continue, it should be
   * up to date */
  if (!m_duplicate_cache.empty())
    return;

  /* clone the cursor, otherwise this function would modify its state */
  ScopedPtr<LocalCursor> clone(new LocalCursor(*this));

  Page *page;
  int slot;
  m_btree_cursor.get_coupled_key(&page, &slot, 0);

  // the clone is not automatically coupled
  // TODO why?
  clone->m_btree_cursor.couple_to_page(page, slot, 0);
  BtreeNodeProxy *node = ldb()->btree_index()->get_node_from_page(page);

  /* synchronize both cursors if necessary */
  if (clone->m_last_cmp != 0 || force_sync)
    clone->sync(context);

  /* first collect all duplicates from the btree. They're already sorted,
   * therefore we can just append them to the DuplicateCache */
  if (clone->m_currently_using == kBtree
        || (clone->m_last_cmp == 0
                && (slot >= 0 && slot < (int)node->get_count()))) {
    int count = node->get_record_count(context, slot);
    for (int i = 0; i < count; i++)
      m_duplicate_cache.push_back(Duplicate(i));
  }

  /* locate the DeltaUpdates, merge them with the Btree Duplicates */
  if ((clone->m_currently_using == kDeltaUpdate || clone->m_last_cmp == 0)
      && clone->m_btree_cursor.deltaupdate() != 0) {
    SortedDeltaUpdates::Iterator it = node->deltas().get(clone->m_btree_cursor.deltaupdate());
    for (DeltaAction *action = (*it)->actions();
            action != 0;
            action = action->next()) {
      // Ignore aborted Transactions
      if (isset(action->flags(), DeltaAction::kIsAborted))
        continue;

      // Is the DeltaUpdate modified by a different Transaction? Then skip
      // it as well, because it's "conflicting"
      // conflict.
      if (notset(action->flags(), DeltaAction::kIsCommitted)
          && action->txn_id() != context->txn->get_id())
        continue;

      // handle deleted duplicates
      if (isset(action->flags(), DeltaAction::kErase)) {
        if (action->referenced_duplicate() >= 0)
          m_duplicate_cache.erase(m_duplicate_cache.begin()
                                    + action->referenced_duplicate());
        else
          m_duplicate_cache.clear();
        continue;
      }

      // all duplicates are overwritten by a new key?
      if (issetany(action->flags(), DeltaAction::kInsert)) {
        m_duplicate_cache.clear();
        m_duplicate_cache.push_back(Duplicate(action));
        continue;
      }

      // a duplicate is overwritten?
      if (issetany(action->flags(), DeltaAction::kInsertOverwrite)) {
        int ref = action->referenced_duplicate();
        if (ref >= 0) {
          ham_assert(ref < (int)m_duplicate_cache.size());
          m_duplicate_cache[ref] = Duplicate(action);
        }
        else {
          m_duplicate_cache.clear();
          m_duplicate_cache.push_back(Duplicate(action));
        }
        continue;
      }

      // another duplicate is inserted?
      if (issetany(action->flags(), DeltaAction::kInsertDuplicate)) {
        uint32_t of = action->original_flags();
        int ref = action->referenced_duplicate();
        if (isset(of, HAM_DUPLICATE_INSERT_FIRST)) {
          m_duplicate_cache.insert(m_duplicate_cache.begin(),
                                Duplicate(action));
        }
        else if (isset(of, HAM_DUPLICATE_INSERT_BEFORE)) {
          m_duplicate_cache.insert(m_duplicate_cache.begin() + ref,
                                Duplicate(action));
        }
        else if (isset(of, HAM_DUPLICATE_INSERT_AFTER)) {
          if (ref + 1 >= (int)m_duplicate_cache.size() - 1)
            m_duplicate_cache.push_back(Duplicate(action));
          else
            m_duplicate_cache.insert(m_duplicate_cache.begin() + ref + 1,
                                Duplicate(action));
        }
        else /* default is HAM_DUPLICATE_INSERT_LAST */
          m_duplicate_cache.push_back(Duplicate(action));
        continue;
      }
    }
  }
}

void
LocalCursor::update_duplicate_cache(Context *context, BtreeNodeProxy *node,
                        int slot, DeltaUpdate *du)
{
  ham_assert(isset(m_db->get_flags(), HAM_ENABLE_DUPLICATE_KEYS));
  ham_assert(m_duplicate_cache.empty());

  /* first collect all duplicates from the btree. They're already sorted,
   * therefore we can just append them to the DuplicateCache */
  int count = node->get_record_count(context, slot);
  for (int i = 0; i < count; i++)
    m_duplicate_cache.push_back(Duplicate(i));

  /* Now merge the DeltaUpdates */
  if (!du)
    return;

  for (DeltaAction *action = du->actions();
          action != 0;
          action = action->next()) {
    // Ignore aborted Transactions
    if (isset(action->flags(), DeltaAction::kIsAborted))
      continue;

    // Is the DeltaUpdate modified by a different Transaction? Then skip
    // it as well, because it's "conflicting"
    // conflict.
    if (notset(action->flags(), DeltaAction::kIsCommitted)
        && action->txn_id() != context->txn->get_id())
      continue;

    // handle deleted duplicates
    if (isset(action->flags(), DeltaAction::kErase)) {
      if (action->referenced_duplicate() >= 0)
        m_duplicate_cache.erase(m_duplicate_cache.begin()
                                  + action->referenced_duplicate());
      else
        m_duplicate_cache.clear();
      continue;
    }

    // all duplicates are overwritten by a new key?
    if (issetany(action->flags(), DeltaAction::kInsert)) {
      m_duplicate_cache.clear();
      m_duplicate_cache.push_back(Duplicate(action));
      continue;
    }

    // a duplicate is overwritten?
    if (issetany(action->flags(), DeltaAction::kInsertOverwrite)) {
      int ref = action->referenced_duplicate();
      if (ref >= 0) {
        ham_assert(ref < (int)m_duplicate_cache.size());
        m_duplicate_cache[ref] = Duplicate(action);
      }
      else {
        m_duplicate_cache.clear();
        m_duplicate_cache.push_back(Duplicate(action));
      }
      continue;
    }

    // another duplicate is inserted?
    if (issetany(action->flags(), DeltaAction::kInsertDuplicate)) {
      uint32_t of = action->original_flags();
      int ref = action->referenced_duplicate();
      if (isset(of, HAM_DUPLICATE_INSERT_FIRST)) {
        m_duplicate_cache.insert(m_duplicate_cache.begin(),
                              Duplicate(action));
      }
      else if (isset(of, HAM_DUPLICATE_INSERT_BEFORE)) {
        m_duplicate_cache.insert(m_duplicate_cache.begin() + ref,
                              Duplicate(action));
      }
      else if (isset(of, HAM_DUPLICATE_INSERT_AFTER)) {
        if (ref + 1 >= (int)m_duplicate_cache.size() - 1)
          m_duplicate_cache.push_back(Duplicate(action));
        else
          m_duplicate_cache.insert(m_duplicate_cache.begin() + ref + 1,
                              Duplicate(action));
      }
      else /* default is HAM_DUPLICATE_INSERT_LAST */
        m_duplicate_cache.push_back(Duplicate(action));
      continue;
    }
  }
}

void
LocalCursor::sync(Context *context)
{
  Page *page;
  int slot;
  m_btree_cursor.get_coupled_key(&page, &slot, 0);
  BtreeNodeProxy *node = ldb()->btree_index()->get_node_from_page(page);

  /* if cursor is attached to a DeltaUpdate: search for the same key
   * in the btree node */
  if (m_currently_using == kDeltaUpdate) {
    ham_key_t *key = m_btree_cursor.deltaupdate()->key();
    int slot = node->find_lower_bound(context, key, 0, &m_last_cmp);
    if (slot >= 0)
      m_btree_cursor.couple_to_page(page, slot, 0);
  }
  /* if the cursor is coupled to a btree slot then search for the
   * corresponding DeltaUpdate */
  else {
    ham_key_t key = {0};
    ByteArray *arena = &ldb()->key_arena(context->txn);
    node->get_key(context, slot, arena, &key); // TODO avoid deep copy

    SortedDeltaUpdates::Iterator it = node->deltas().find_lower_bound(&key, ldb());
    if (it != node->deltas().end()) {
      m_last_cmp = node->compare(context, (*it)->key(), slot);
      m_btree_cursor.attach_to_deltaupdate(*it);
    }
  }

  m_duplicate_cache.clear();
}

// TODO where is this required?
int
LocalCursor::compare(Context *context)
{
  BtreeCursor *btrc = get_btree_cursor();
  BtreeIndex *btree = ldb()->btree_index();

  TransactionNode *node = m_txn_cursor.get_coupled_op()->get_node();
  ham_key_t *txnk = node->get_key();

  ham_assert(!is_nil(0));
  ham_assert(!m_txn_cursor.is_nil());

  if (btrc->state() == BtreeCursor::kStateCoupled) {
    Page *page;
    int slot;
    btrc->get_coupled_key(&page, &slot, 0);
    m_last_cmp = btree->get_node_from_page(page)->compare(context, txnk, slot);

    // need to fix the sort order - we compare txnk vs page[slot], but the
    // caller expects m_last_cmp to be the comparison of page[slot] vs txnk
    if (m_last_cmp < 0)
      m_last_cmp = +1;
    else if (m_last_cmp > 0)
      m_last_cmp = -1;

    return (m_last_cmp);
  }
  else if (btrc->state() == BtreeCursor::kStateUncoupled) {
    m_last_cmp = btree->compare_keys(btrc->get_uncoupled_key(), txnk);
    return (m_last_cmp);
  }

  ham_assert(!"shouldn't be here");
  return (0);
}

ham_status_t
LocalCursor::move_next_key(Context *context, uint32_t flags)
{
  bool force_sync = false;
  Page *page;
  int slot;
  BtreeNodeProxy *node = 0;

  // If duplicates are enabled: load the DuplicateCache (if necessary),
  // then try to move to the next duplicate
  if (isset(m_db->get_flags(), HAM_ENABLE_DUPLICATE_KEYS)) {
    if (notset(flags, HAM_SKIP_DUPLICATES) && !m_duplicate_cache.empty()) {
      if (m_dupecache_index < (int)m_duplicate_cache.size() - 1) {
        m_dupecache_index++;
        couple_to_duplicate(m_dupecache_index);
        return (0);
      }
    }
    // clear the DuplicateCache before moving to the next key
    m_duplicate_cache.clear();
  }

  // Fetch the current page and slot
  m_btree_cursor.get_coupled_key(&page, &slot);
  if (page == 0) { // TODO merge all three calls
    m_btree_cursor.couple(context);
    m_btree_cursor.get_coupled_key(&page, &slot);
  }
  node = ldb()->btree_index()->get_node_from_page(page);

  bool use_delta_key;
  bool use_btree_key;

  SortedDeltaUpdates::Iterator it = node->deltas().begin();
  if (m_btree_cursor.deltaupdate() != 0)
    it = node->deltas().get(m_btree_cursor.deltaupdate());
  if (m_last_cmp <= 0 || m_currently_using == kDeltaUpdate) {
    it++;
    m_btree_cursor.attach_to_deltaupdate(*it);
  }

  while (true) {
    DeltaAction *action = 0;
    use_btree_key = node->get_count() > 0;
    use_delta_key = true;

    // If the Btree key was consumed: move to the next Btree key
    if (m_last_cmp >= 0 || m_currently_using == kBtree) {
      m_btree_cursor.couple_to_page(page, slot, 0);
      if (m_btree_cursor.move(context, 0, 0, 0, 0,
                              HAM_CURSOR_NEXT | HAM_SKIP_DUPLICATES) != 0)
        use_btree_key = false;

      // TODO this moves node to the next page; but what if the previous node
      // still had valid Delta updates? they will not get picked up!
      m_btree_cursor.get_coupled_key(&page, &slot);
      node = ldb()->btree_index()->get_node_from_page(page);
    }

    if (node->deltas().size() == 0 || it >= node->deltas().end()) {
      // attach "out of bounds", otherwise DU will be picked up again
      m_btree_cursor.attach_to_deltaupdate(*node->deltas().end());
      use_delta_key = false;
    }
    else {
      for (; it != node->deltas().end(); it++) {
        // check if this key has valid (i.e. non-aborted) actions
        for (action = (*it)->actions();
                        action != 0;
                        action = action->next()) {
          if (isset(action->flags(), DeltaAction::kIsAborted))
            continue;
          use_delta_key = true;
          break;
        }
        if (use_delta_key)
          break;
      }
    }

    // Neither Btree key nor Delta key available?
    if (!use_btree_key && !use_delta_key) {
      return (HAM_KEY_NOT_FOUND);
    }

    // Only Btree keys remaining, no more Delta key available?
    if (use_btree_key && !use_delta_key) {
      m_btree_cursor.couple_to_page(page, slot, 0);
      m_currently_using = kBtree;
      force_sync = true;
      goto maybe_update_duplicate_cache;
    }

    bool is_erased = false;
    bool is_conflict = false;
    for (; action != 0; action = action->next()) {
      if (isset(action->flags(), DeltaAction::kErase)) {
        is_erased = true;
        continue;
      }
      if (context->txn
              && notset(action->flags(), DeltaAction::kIsCommitted)
              && action->txn_id() != context->txn->get_id()) {
        is_conflict = true;
        break;
      }

      is_erased = false;
    }

    // Only Delta keys remaining, no more Btree key available?
    if (!use_btree_key && use_delta_key) {
      // TODO might have duplicates!?
      // Cursor-longtxn/moveNextWhileErasingTest
      if (is_erased || is_conflict) {
        it++;
        m_btree_cursor.attach_to_deltaupdate(*it);
        m_currently_using = kDeltaUpdate;
        continue;
      }
      m_currently_using = kDeltaUpdate;
      m_btree_cursor.attach_to_deltaupdate(*it);
      force_sync = true;
      goto maybe_update_duplicate_cache;
    }

    // Btree key *and* Delta key are available - use the smaller one
    // Btree key is < DeltaUpdate? then use the Btree key
    ham_assert(slot >= 0 && slot < (int)node->get_count());
    m_last_cmp = node->compare(context, (*it)->key(), slot);

    if (m_last_cmp > 0) {
      m_btree_cursor.couple_to_page(page, slot, 0);
      m_currently_using = kBtree;
      use_delta_key = false;
      break;
    }

    // Btree key is equal to DeltaUpdate? check for conflict or if the Btree
    // key was erased; if not then attach to the DeltaUpdate, otherwise
    // move next in the Btree and continue
    if (is_erased || is_conflict) {
      it++;
      m_btree_cursor.attach_to_deltaupdate(*it);
      m_currently_using = kDeltaUpdate;
      continue;
    }

    if (m_last_cmp == 0) {
      m_btree_cursor.couple_to_deltaupdate(page, *it);
      m_currently_using = kDeltaUpdate;
      use_btree_key = false;
      break;
    }

    // DeltaUpdate is < Btree key? Use the DeltaUpdate if possible
    m_btree_cursor.couple_to_deltaupdate(page, *it);
    m_currently_using = kDeltaUpdate;
    use_btree_key = false;
    break;
  }

maybe_update_duplicate_cache:
  // If required: build a DuplicateCache, return its first duplicate
  if (isset(m_db->get_flags(), HAM_ENABLE_DUPLICATE_KEYS)) {
    ham_assert(m_duplicate_cache.empty());
    update_duplicate_cache(context, force_sync);
    couple_to_duplicate(0);
  }
  return (0);
}

/*
 * TODO
 * this method uses an integer index in the deltas to move backwards.
 * this is cumbersome - better use a reverse iterator!
 */
ham_status_t
LocalCursor::move_previous_key(Context *context, uint32_t flags)
{
  bool force_sync = false;
  Page *page;
  int slot;
  BtreeNodeProxy *node = 0;

  // If duplicates are enabled: load the DuplicateCache (if necessary),
  // then try to move to the next duplicate
  if (isset(m_db->get_flags(), HAM_ENABLE_DUPLICATE_KEYS)) {
    if (notset(flags, HAM_SKIP_DUPLICATES) && !m_duplicate_cache.empty()) {
      if (m_dupecache_index > 0) {
        m_dupecache_index--;
        couple_to_duplicate(m_dupecache_index);
        return (0);
      }
    }
    // clear the DuplicateCache before moving to the next key
    m_duplicate_cache.clear();
  }

  // Fetch the current page and slot
  m_btree_cursor.get_coupled_key(&page, &slot);
  if (page == 0) { // TODO merge all three calls
    m_btree_cursor.couple(context);
    m_btree_cursor.get_coupled_key(&page, &slot);
  }
  node = ldb()->btree_index()->get_node_from_page(page);

  bool use_delta_key;
  bool use_btree_key;

  int delta_slot = node->deltas().index_of(m_btree_cursor.deltaupdate());
  SortedDeltaUpdates::Iterator it = node->deltas().begin();
  if (delta_slot >= (int)node->deltas().size())
    delta_slot = node->deltas().size() - 1;
  if (m_last_cmp >= 0 || m_currently_using == kDeltaUpdate) {
    delta_slot--;
    m_btree_cursor.attach_to_deltaupdate(node->deltas().at(delta_slot));
  }

  while (true) {
    DeltaAction *action = 0;
    use_btree_key = m_btree_eof ? false : node->get_count() > 0;
    use_delta_key = true;

    // If the Btree key was consumed: move to the next Btree key
    if (m_last_cmp <= 0 || m_currently_using == kBtree) {
      m_btree_cursor.couple_to_page(page, slot, 0);
      if (m_btree_cursor.move(context, 0, 0, 0, 0,
                              HAM_CURSOR_PREVIOUS | HAM_SKIP_DUPLICATES) != 0) {
        use_btree_key = false;
        m_btree_eof = true;
      }

      m_btree_cursor.get_coupled_key(&page, &slot);
      node = ldb()->btree_index()->get_node_from_page(page);
    }

    if (node->deltas().size() == 0 || delta_slot < 0) {
      // attach "out of bounds", otherwise DU will be picked up again
      m_btree_cursor.detach_from_deltaupdate();
      use_delta_key = false;
    }
    else {
      for (; delta_slot >= 0; delta_slot--) {
        it = node->deltas().begin() + delta_slot;
        // check if this key has valid (i.e. non-aborted) actions
        for (action = (*it)->actions();
                        action != 0;
                        action = action->next()) {
          if (isset(action->flags(), DeltaAction::kIsAborted))
            continue;
          use_delta_key = true;
          break;
        }
        if (use_delta_key)
          break;
      }
    }

    // Neither Btree key nor Delta key available?
    if (!use_btree_key && !use_delta_key) {
      return (HAM_KEY_NOT_FOUND);
    }

    // Only Btree keys remaining, no more Delta key available?
    if (use_btree_key && !use_delta_key) {
      m_btree_cursor.couple_to_page(page, slot, 0);
      m_currently_using = kBtree;
      force_sync = true;
      goto maybe_update_duplicate_cache;
    }

    bool is_erased = false;
    bool is_conflict = false;
    for (; action != 0; action = action->next()) {
      if (isset(action->flags(), DeltaAction::kErase)) {
        is_erased = true;
        continue;
      }
      if (context->txn
              && notset(action->flags(), DeltaAction::kIsCommitted)
              && action->txn_id() != context->txn->get_id()) {
        is_conflict = true;
        break;
      }

      is_erased = false;
    }

    // Only Delta keys remaining, no more Btree key available?
    if (!use_btree_key && use_delta_key) {
      if (is_erased || is_conflict) {
        it--;
        delta_slot--;
        m_btree_cursor.attach_to_deltaupdate(node->deltas().at(delta_slot));
        m_currently_using = kDeltaUpdate;
        continue;
      }
      m_btree_cursor.attach_to_deltaupdate(node->deltas().at(delta_slot));
      m_currently_using = kDeltaUpdate;
      force_sync = true;
      goto maybe_update_duplicate_cache;
    }

    // Btree key *and* Delta key are available - use the smaller one
    // Btree key is < DeltaUpdate? then use the Btree key
    ham_assert(slot >= 0 && slot < (int)node->get_count());
    m_last_cmp = node->compare(context, (*it)->key(), slot);

    if (m_last_cmp < 0) {
      m_btree_cursor.couple_to_page(page, slot, 0);
      m_currently_using = kBtree;
      use_delta_key = false;
      break;
    }

    // Btree key is equal to DeltaUpdate? check for conflict or if the Btree
    // key was erased; if not then attach to the DeltaUpdate, otherwise
    // move next in the Btree and continue
    if (is_erased || is_conflict) {
      it--;
      delta_slot--;
      m_btree_cursor.attach_to_deltaupdate(node->deltas().at(delta_slot));
      m_currently_using = kDeltaUpdate;
      continue;
    }

    if (m_last_cmp == 0) {
      m_btree_cursor.couple_to_deltaupdate(page, node->deltas().at(delta_slot));
      m_currently_using = kDeltaUpdate;
      use_btree_key = false;
      break;
    }

    // DeltaUpdate is < Btree key? Use the DeltaUpdate if possible
    m_btree_cursor.couple_to_deltaupdate(page, node->deltas().at(delta_slot));
    m_currently_using = kDeltaUpdate;
    use_btree_key = false;
    break;
  }

maybe_update_duplicate_cache:
  // If required: build a DuplicateCache, return its last duplicate
  if (isset(m_db->get_flags(), HAM_ENABLE_DUPLICATE_KEYS)) {
    ham_assert(m_duplicate_cache.empty());
    update_duplicate_cache(context, force_sync);
    if (!m_duplicate_cache.empty())
      couple_to_duplicate(m_duplicate_cache.size() - 1);
  }
  return (0);
}

ham_status_t
LocalCursor::move_first_key(Context *context, uint32_t flags)
{
  Page *page;
  BtreeNodeProxy *node = 0;

  // Reset the cursor
  m_btree_cursor.detach_from_deltaupdate();

  // Fetch the BtreeNode which stores the first key
  m_btree_cursor.move(context, 0, 0, 0, 0,
                    HAM_CURSOR_FIRST | HAM_SKIP_DUPLICATES);
  m_btree_cursor.get_coupled_key(&page);
  node = ldb()->btree_index()->get_node_from_page(page);

  int slot = 0;
  SortedDeltaUpdates::Iterator start = node->deltas().begin();
  for (SortedDeltaUpdates::Iterator it = start;
          it != node->deltas().end();
          it++) {

    // check if this key has valid (i.e. non-aborted) actions
    DeltaAction *action = locate_valid_action(*it);
    if (action == 0)
      continue;

    // compare the current Btree key to the key of the DeltaUpdate
    if (slot < (int)node->get_count()) {
      m_last_cmp = node->compare(context, (*it)->key(), slot);

      // Btree key is < DeltaUpdate? then use the Btree key. Btree keys are
      // always sorted, therefore it's not necessary to use a DuplicateCache
      if (m_last_cmp > 0) {
        m_btree_cursor.couple_to_page(page, slot, 0);
        m_currently_using = kBtree;

        if (isset(m_db->get_flags(), HAM_ENABLE_DUPLICATE_KEYS)) {
          update_duplicate_cache(context);
          couple_to_duplicate(0);
        }
        return (0);
      }

      if (context->txn
              && notset(action->flags(), DeltaAction::kIsCommitted)
              && action->txn_id() != context->txn->get_id())
        throw Exception(HAM_TXN_CONFLICT);

      // Btree key is equal to DeltaUpdate? check for conflict or if the Btree
      // key was erased; if not then attach to the DeltaUpdate, otherwise
      // move next in the Btree and continue
      if (m_last_cmp == 0) {
        // handle txn conflict
        if (context->txn
            && notset(action->flags(), DeltaAction::kIsCommitted)
            && action->txn_id() != context->txn->get_id())
          throw Exception(HAM_TXN_CONFLICT);

        bool is_erased = false;

        if (isset(m_db->get_flags(), HAM_ENABLE_DUPLICATE_KEYS)) {
          update_duplicate_cache(context, node, slot, *it);
          is_erased = m_duplicate_cache.empty();
          if (!is_erased)
            couple_to_duplicate(0);
        }
        else {
          for (; action != 0; action = action->next()) {
            if (notset(action->flags(), DeltaAction::kIsCommitted)
                  && action->txn_id() != context->txn->get_id())
              continue;
            if (isset(action->flags(), DeltaAction::kErase)) {
              is_erased = true;
              continue;
            }
            is_erased = false;
          }
        }

        if (is_erased) {
          slot++;
          m_btree_cursor.couple_to_page(page, slot, 0);
          continue;
        }

        m_currently_using = kDeltaUpdate;
        m_btree_cursor.couple_to_deltaupdate(page, *it);
        return (0);
      }
    }
    else {
      // continue with the sibling page
      if (node->get_right()) {
        page = lenv()->page_manager()->fetch(context, node->get_right());
        node = ldb()->btree_index()->get_node_from_page(page);
        slot = 0;
        m_btree_cursor.couple_to_page(page, slot, 0);
        continue;
      }
    }

    if (context->txn
            && notset(action->flags(), DeltaAction::kIsCommitted)
            && action->txn_id() != context->txn->get_id())
      throw Exception(HAM_TXN_CONFLICT);

    // DeltaUpdate is < Btree key? Use the DeltaUpdate if possible
    m_btree_cursor.couple_to_deltaupdate(page, *it);
    m_currently_using = kDeltaUpdate;

    // Locate the first duplicate
    if (isset(m_db->get_flags(), HAM_ENABLE_DUPLICATE_KEYS)) {
      update_duplicate_cache(context);
      if (m_duplicate_cache.empty())
        continue;
      couple_to_duplicate(0);
    }
    else if (isset(action->flags(), DeltaAction::kErase))
      continue;
    return (0);
  }

  // still here? then all DeltaUpdates have been processed. If there are still
  // Btree keys available then use them.
  if (slot >= (int)node->get_count())
    return (HAM_KEY_NOT_FOUND);

  m_btree_cursor.couple_to_page(page, slot, 0);
  m_currently_using = kBtree;
  // Locate the first duplicate
  if (isset(m_db->get_flags(), HAM_ENABLE_DUPLICATE_KEYS)) {
    update_duplicate_cache(context);
    couple_to_duplicate(0);
  }
  return (0);
}

ham_status_t
LocalCursor::move_last_key(Context *context, uint32_t flags)
{
  Page *page;
  BtreeNodeProxy *node = 0;

  // Reset the cursor
  m_btree_cursor.detach_from_deltaupdate();

  // Fetch the BtreeNode which stores the first key
  m_btree_cursor.move(context, 0, 0, 0, 0,
                    HAM_CURSOR_LAST | HAM_SKIP_DUPLICATES);
  m_btree_cursor.get_coupled_key(&page);
  node = ldb()->btree_index()->get_node_from_page(page);

  int slot = node->get_count() - 1;
  int delta_slot = node->deltas().size() - 1;
  SortedDeltaUpdates::Iterator it = node->deltas().begin();
  for (; delta_slot >= 0; delta_slot--) {
    it = node->deltas().begin() + delta_slot;

    // check if this key has valid (i.e. non-aborted) actions
    DeltaAction *action = locate_valid_action(*it);
    if (action == 0)
      continue;

    // compare the current Btree key to the key of the DeltaUpdate
    if (slot < (int)node->get_count()) {
      m_last_cmp = node->compare(context, (*it)->key(), slot);

      // Btree key is > DeltaUpdate? then use the Btree key
      if (m_last_cmp < 0) {
        m_btree_cursor.attach_to_deltaupdate(*it);
        m_btree_cursor.couple_to_page(page, slot, 0);
        m_currently_using = kBtree;

        if (isset(m_db->get_flags(), HAM_ENABLE_DUPLICATE_KEYS)) {
          update_duplicate_cache(context);
          couple_to_duplicate(0);
        }
        return (0);
      }

      if (context->txn
              && notset(action->flags(), DeltaAction::kIsCommitted)
              && action->txn_id() != context->txn->get_id())
        throw Exception(HAM_TXN_CONFLICT);

      // Btree key is equal to DeltaUpdate? check for conflict or if the Btree
      // key was erased; if not then attach to the DeltaUpdate, otherwise
      // move next in the Btree and continue
      if (m_last_cmp == 0) {
        bool is_erased = false;

        if (isset(m_db->get_flags(), HAM_ENABLE_DUPLICATE_KEYS)) {
          update_duplicate_cache(context, node, slot, *it);
          is_erased = m_duplicate_cache.empty();
          if (!is_erased)
            couple_to_duplicate(m_duplicate_cache.size() - 1);
        }
        else {
          for (; action != 0; action = action->next()) {
            if (notset(action->flags(), DeltaAction::kIsCommitted)
                  && action->txn_id() != context->txn->get_id())
              continue;
            if (isset(action->flags(), DeltaAction::kErase)) {
              is_erased = true;
              continue;
            }
            is_erased = false;
          }
        }

        if (is_erased) {
          slot--;
          m_btree_cursor.couple_to_page(page, slot, 0);
          continue;
        }

        m_currently_using = kDeltaUpdate;
        m_btree_cursor.couple_to_deltaupdate(page, node->deltas().at(delta_slot));
        return (0);
      }
    }
    else {
      // continue with the left page
      if (node->get_left()) {
        page = lenv()->page_manager()->fetch(context, node->get_left());
        node = ldb()->btree_index()->get_node_from_page(page);
        slot = node->get_count() - 1;
        m_btree_cursor.couple_to_page(page, slot, 0);
        continue;
      }
    }

    if (context->txn
            && notset(action->flags(), DeltaAction::kIsCommitted)
            && action->txn_id() != context->txn->get_id())
      throw Exception(HAM_TXN_CONFLICT);

    // DeltaUpdate is > Btree key? Use the DeltaUpdate if possible
    m_btree_cursor.couple_to_deltaupdate(page, node->deltas().at(delta_slot));
    m_currently_using = kDeltaUpdate;

    // Locate the first duplicate
    if (isset(m_db->get_flags(), HAM_ENABLE_DUPLICATE_KEYS)) {
      update_duplicate_cache(context);
      if (m_duplicate_cache.empty())
        continue;
      couple_to_duplicate(m_duplicate_cache.size() - 1);
    }
    else if (isset(action->flags(), DeltaAction::kErase))
      continue;
    return (0);
  }

  // still here? then all DeltaUpdates have been processed. If there are still
  // Btree keys available then use them.
  if (slot >= (int)node->get_count() || slot < 0)
    return (HAM_KEY_NOT_FOUND);

  m_btree_cursor.couple_to_page(page, slot, node->get_count() - 1);
  m_currently_using = kBtree;
  // Locate the first duplicate
  if (isset(m_db->get_flags(), HAM_ENABLE_DUPLICATE_KEYS)) {
    update_duplicate_cache(context);
    couple_to_duplicate(m_duplicate_cache.size() - 1);
  }
  return (0);
}

bool
LocalCursor::is_key_erased(Context *context, ham_key_t *key)
{
  Page *page;
  int slot;
  m_btree_cursor.get_coupled_key(&page, &slot);
  if (page == 0)
    return (false);

  BtreeNodeProxy *node = ldb()->btree_index()->get_node_from_page(page);
  SortedDeltaUpdates::Iterator it = node->deltas().find(key, ldb());
  if (it == node->deltas().end())
    return (false);

  return (is_key_erased(context, *it));
}

bool
LocalCursor::is_key_erased(Context *context, DeltaUpdate *du)
{
  int inserted = 0;

  // retrieve number of records in the btree
  if (m_last_cmp == 0) {
    Page *page;
    int slot;
    m_btree_cursor.get_coupled_key(&page, &slot);
    if (page != 0) {
      BtreeNodeProxy *node = ldb()->btree_index()->get_node_from_page(page);
      if (slot >= 0 && slot < (int)node->get_count())
        inserted += node->get_record_count(context, slot);
    }
  }

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

  ham_assert(inserted >= 0);
  return (inserted == 0);
} 

ham_status_t
LocalCursor::move(Context *context, ham_key_t *key, ham_record_t *record,
                uint32_t flags)
{
  ham_status_t st = 0;
  bool changed_dir = false;

  /* in non-transactional mode - just call the btree function and return */
  if (!(lenv()->get_flags() & HAM_ENABLE_TRANSACTIONS)) {
    return (m_btree_cursor.move(context,
                            key, &ldb()->key_arena(context->txn),
                            record, &ldb()->record_arena(context->txn), flags));
  }

  /* no movement requested? directly retrieve key/record */
  if (!flags)
    goto retrieve_key_and_record;

  /* synchronize the btree and transaction cursor if the last operation was
   * not a move next/previous OR if the direction changed */
  if ((m_last_operation == HAM_CURSOR_PREVIOUS)
        && (flags & HAM_CURSOR_NEXT))
    changed_dir = true;
  else if ((m_last_operation == HAM_CURSOR_NEXT)
        && (flags & HAM_CURSOR_PREVIOUS))
    changed_dir = true;
  if (((flags & HAM_CURSOR_NEXT) || (flags & HAM_CURSOR_PREVIOUS))
        && (m_last_operation == LocalCursor::kLookupOrInsert || changed_dir)) {
    if (is_coupled_to_txnop()) // TODO raus!
      set_to_nil(kBtree);
    else
      set_to_nil(kTxn);

    // TODO loses flags re approx matching
    if (m_last_cmp != 0 || m_duplicate_cache.empty())
      sync(context);

    m_btree_eof = false;
  }

  /* we have either skipped duplicates or reached the end of the duplicate
   * list. btree cursor and txn cursor are synced and as close to
   * each other as possible. Move the cursor in the requested direction. */
  if (flags & HAM_CURSOR_NEXT) {
    st = move_next_key(context, flags);
  }
  else if (flags & HAM_CURSOR_PREVIOUS) {
    st = move_previous_key(context, flags);
  }
  else if (flags & HAM_CURSOR_FIRST) {
    clear_duplicate_cache();
    m_btree_eof = false;
    st = move_first_key(context, flags);
  }
  else {
    ham_assert(flags & HAM_CURSOR_LAST);
    clear_duplicate_cache();
    m_btree_eof = false;
    st = move_last_key(context, flags);
  }

  if (st)
    return (st);

retrieve_key_and_record:
  ham_assert(st == 0);

  if (m_currently_using == 0)
    return (HAM_CURSOR_IS_NIL);

  ByteArray *key_arena = &ldb()->key_arena(context->txn);
  ByteArray *record_arena = &ldb()->record_arena(context->txn);

  /* if duplicate keys are enabled then fetch the duplicate record */
  if (m_dupecache_index >= 0
        && isset(m_db->get_flags(), HAM_ENABLE_DUPLICATE_KEYS)) {
    Duplicate &dup = m_duplicate_cache[m_dupecache_index];
    m_btree_cursor.attach_to_deltaaction(dup.action());

    if (record) {
      if (dup.action()) {
        record->size = dup.action()->record()->size;
        if (record->size > 0) {
          if (!(record->flags & HAM_KEY_USER_ALLOC)) {
            record_arena->resize(record->size);
            record->data = record_arena->get_ptr();
          }
          ::memcpy(record->data, dup.action()->record()->data, record->size);
        }
        else
          record->data = 0;
      }
      else {
        m_btree_cursor.move(context, 0, 0, record, record_arena, 0);
      }
      record = 0; // do not overwrite below
    }
  }

  if (m_currently_using == kBtree) {
    ham_status_t st = m_btree_cursor.move(context, key, key_arena, record,
                            record_arena, 0);
    /* don't forget to check if the key still exists */
    /* TODO perform the check even if |key| is null! */
    if (st == 0 && flags == 0 && key && is_key_erased(context, key))
      return (HAM_KEY_NOT_FOUND);
    return (st);
  }

  if (m_currently_using == kDeltaUpdate) {
    Page *page;
    m_btree_cursor.get_coupled_key(&page);

    /* page is not assigned if key was deleted */
    if (!page)
      return (HAM_CURSOR_IS_NIL);

    DeltaUpdate *du = m_btree_cursor.deltaupdate();

    /* don't forget to check if the key still exists */
    if (flags == 0 && is_key_erased(context, du))
      return (HAM_KEY_NOT_FOUND);

    if (key) {
      key->size = du->key()->size;
      if (key->size > 0) {
        if (!(key->flags & HAM_KEY_USER_ALLOC)) {
          key_arena->resize(key->size);
          key->data = key_arena->get_ptr();
        }
        ::memcpy(key->data, du->key()->data, key->size);
      }
      else
        key->data = 0;
    }

    // pick the first action with a record
    DeltaAction *action = m_btree_cursor.deltaupdate_action();
    if (action == 0) {
      for (action = du->actions(); action != 0; action = action->next()) {
        if (isset(action->flags(), DeltaAction::kIsAborted))
          continue;
        if (context->txn && action->txn_id() != context->txn->get_id())
          continue;
        break;
      }
    }
    m_btree_cursor.attach_to_deltaaction(action);

    if (record) {
      record->size = action->record()->size;
      if (record->size > 0) {
        if (!(record->flags & HAM_KEY_USER_ALLOC)) {
          record_arena->resize(record->size);
          record->data = record_arena->get_ptr();
        }
        ::memcpy(record->data, action->record()->data, record->size);
      }
      else
        record->data = 0;
    }
  }

  return (0);
}

bool
LocalCursor::is_nil(int what)
{
  switch (what) {
    case kBtree:
      return (m_btree_cursor.state() == BtreeCursor::kStateNil);
    case kTxn:
      return (m_txn_cursor.is_nil());
    default:
      ham_assert(what == 0);
      return (m_btree_cursor.state() == BtreeCursor::kStateNil
                      && m_txn_cursor.is_nil());
  }
}

void
LocalCursor::set_to_nil(int what)
{
  switch (what) {
    case kBtree:
      m_btree_cursor.set_to_nil();
      break;
    case kTxn:
      m_txn_cursor.set_to_nil();
      couple_to_btree(); /* reset flag */
      break;
    default:
      ham_assert(what == 0);
      m_btree_cursor.set_to_nil();
      m_txn_cursor.set_to_nil();
      couple_to_btree(); /* reset flag */
      m_is_first_use = true;
      clear_duplicate_cache();
      break;
  }
}

void
LocalCursor::close()
{
  m_btree_cursor.close();
  m_duplicate_cache.clear();
}

ham_status_t
LocalCursor::do_overwrite(ham_record_t *record, uint32_t flags)
{
  Context context(lenv(), (LocalTransaction *)m_txn, ldb());

  if (is_nil())
    return (HAM_CURSOR_IS_NIL);

  ham_status_t st = 0;
  Transaction *local_txn = 0;

  /* if user did not specify a transaction, but transactions are enabled:
   * create a temporary one */
  if (!m_txn && (m_db->get_flags() & HAM_ENABLE_TRANSACTIONS)) {
    local_txn = ldb()->begin_temp_txn();
    context.txn = (LocalTransaction *)local_txn;
  }

  if (m_currently_using == kDeltaUpdate) {
    Page *page;
    m_btree_cursor.get_coupled_key(&page);

    st = ldb()->insert_txn(&context, m_btree_cursor.deltaupdate()->key(),
                        record, flags | HAM_OVERWRITE, this);
  }
  else {
    m_btree_cursor.overwrite(&context, record, flags);
    couple_to_btree();
    st = 0;
  }

  return (ldb()->finalize(&context, st, local_txn));
}

ham_status_t
LocalCursor::do_get_duplicate_position(uint32_t *pposition)
{
  if (is_nil())
    return (HAM_CURSOR_IS_NIL);

  // use btree cursor?
  if (m_txn_cursor.is_nil())
    *pposition = m_btree_cursor.duplicate_index();
  // otherwise return the index in the duplicate cache
  else
    *pposition = get_dupecache_index() - 1;

  return (0);
}

uint32_t
LocalCursor::duplicate_count(Context *context)
{
  ham_assert(!is_nil());

  // if duplicates are disabled then there's only one record
  if (notset(m_db->get_flags(), HAM_ENABLE_DUPLICATE_KEYS))
    return (1);

  // only update the DuplicateCache if required
  if (isset(ldb()->get_flags(), HAM_ENABLE_TRANSACTIONS)) {
    if (m_last_cmp != 0 || m_duplicate_cache.empty())
      update_duplicate_cache(context);
    return (m_duplicate_cache.size());
  }

  return (m_btree_cursor.get_record_count(context, 0));
}

ham_status_t
LocalCursor::do_get_duplicate_count(uint32_t flags, uint32_t *pcount)
{
  if (is_nil()) {
    *pcount = 0;
    return (HAM_CURSOR_IS_NIL);
  }

  Context context(ldb()->lenv(), (LocalTransaction *)m_txn, ldb());

  *pcount = duplicate_count(&context);
  return (0);
}

ham_status_t
LocalCursor::do_get_record_size(uint64_t *psize)
{
  Context context(ldb()->lenv(), (LocalTransaction *)m_txn, ldb());

  if (is_nil())
    return (HAM_CURSOR_IS_NIL);

  if (is_coupled_to_txnop())
    *psize = m_txn_cursor.get_record_size();
  else if (m_currently_using == kDeltaUpdate
            && m_btree_cursor.deltaupdate_action())
    *psize = m_btree_cursor.deltaupdate_action()->record()->size;
  else
    *psize = m_btree_cursor.get_record_size(&context);

  return (0);
}

LocalEnvironment *
LocalCursor::lenv()
{
  return ((LocalEnvironment *)ldb()->get_env());
}

void
LocalCursor::couple_to_duplicate(int index)
{
  ham_assert(index >= 0 && index < (int)m_duplicate_cache.size());

  Duplicate &dup = m_duplicate_cache[index];
  if (!dup.action()) {
    m_btree_cursor.set_duplicate_index(dup.duplicate_index());
    m_btree_cursor.set_state(BtreeCursor::kStateCoupled);
  }
  else {
    m_btree_cursor.attach_to_deltaaction(dup.action());
  }

  m_dupecache_index = index;
}

DeltaAction *
LocalCursor::locate_valid_action(DeltaUpdate *du)
{
  DeltaAction *action = 0;
  for (DeltaAction *da = du->actions(); da != 0; da = da->next()) {
    if (isset(da->flags(), DeltaAction::kIsAborted))
      continue;
    action = da;
  }

  return (action);
}

