/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include "config.h"

#include <string.h>

#include "error.h"
#include "mem.h"
#include "page.h"
#include "txn.h"
#include "cursor.h"
#include "page_manager.h"
#include "btree_index.h"
#include "btree_cursor.h"
#include "btree_node_proxy.h"

namespace hamsterdb {

BtreeCursor::BtreeCursor(Cursor *parent)
  : m_parent(parent), m_state(0), m_duplicate_index(0), m_dupe_cache(),
    m_coupled_page(0), m_coupled_index(0), m_next_in_page(0),
    m_previous_in_page(0)
{
  memset(&m_uncoupled_key, 0, sizeof(m_uncoupled_key));
  m_btree = parent->get_db()->get_btree_index();
}

void
BtreeCursor::set_to_nil()
{
  // uncoupled cursor: free the cached pointer
  if (m_state == kStateUncoupled)
    memset(&m_uncoupled_key, 0, sizeof(m_uncoupled_key));
  // coupled cursor: remove from page
  else if (m_state == kStateCoupled)
    remove_cursor_from_page(m_coupled_page);

  m_state = BtreeCursor::kStateNil;
  m_duplicate_index = 0;
  memset(&m_dupe_cache, 0, sizeof(PDupeEntry));
}

ham_status_t
BtreeCursor::uncouple_from_page()
{
  if (m_state == kStateUncoupled || m_state == kStateNil)
    return (0);

  ham_assert(m_coupled_page != 0);

  // get the btree-entry of this key
  BtreeNodeProxy *node = m_btree->get_node_from_page(m_coupled_page);
  ham_assert(node->is_leaf());
  ham_status_t st = node->get_key(m_coupled_index, &m_uncoupled_arena,
                  &m_uncoupled_key);
  if (st)
    return (st);

  // uncouple the page
  remove_cursor_from_page(m_coupled_page);

  // set the state and the uncoupled key
  m_state = BtreeCursor::kStateUncoupled;

  return (0);
}

void
BtreeCursor::clone(BtreeCursor *other)
{
  m_duplicate_index = other->m_duplicate_index;

  // if the old cursor is coupled: couple the new cursor, too
  if (other->m_state == kStateCoupled) {
     couple_to_page(other->m_coupled_page, other->m_coupled_index);
  }
  // otherwise, if the src cursor is uncoupled: copy the key
  else if (other->m_state == kStateUncoupled) {
    memset(&m_uncoupled_key, 0, sizeof(m_uncoupled_key));

    m_uncoupled_arena.copy(other->m_uncoupled_arena.get_ptr(),
                   other->m_uncoupled_arena.get_size());
    m_uncoupled_key.data = m_uncoupled_arena.get_ptr();
    m_uncoupled_key.size = m_uncoupled_arena.get_size();
    m_state = kStateUncoupled;
  }
  else {
    set_to_nil();
  }
}

ham_status_t
BtreeCursor::overwrite(ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st;
  Transaction *txn = m_parent->get_txn();

  // uncoupled cursor: couple it
  if (m_state == kStateUncoupled) {
    st = couple();
    if (st)
      return (st);
  }
  else if (m_state != kStateCoupled)
    return (HAM_CURSOR_IS_NIL);

  // delete the cache of the current duplicate
  memset(&m_dupe_cache, 0, sizeof(PDupeEntry));

  // copy the key flags, and remove all flags concerning the key size
  BtreeNodeProxy *node = m_btree->get_node_from_page(m_coupled_page);
  st = node->set_record(m_coupled_index, txn, record, m_duplicate_index,
                    flags | HAM_OVERWRITE, 0);
  if (st)
    return (st);

  m_coupled_page->set_dirty(true);

  return (0);
}

ham_status_t
BtreeCursor::move(ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st = 0;
  LocalDatabase *db = m_parent->get_db();
  Transaction *txn = m_parent->get_txn();

  // delete the cache of the current duplicate
  memset(&m_dupe_cache, 0, sizeof(PDupeEntry));

  if (flags & HAM_CURSOR_FIRST)
    st = move_first(flags);
  else if (flags & HAM_CURSOR_LAST)
    st = move_last(flags);
  else if (flags & HAM_CURSOR_NEXT)
    st = move_next(flags);
  else if (flags & HAM_CURSOR_PREVIOUS)
    st = move_previous(flags);
  // no move, but cursor is nil? return error
  else if (m_state == kStateNil) {
    if (key || record)
      return (HAM_CURSOR_IS_NIL);
    else
      return (0);
  }
  // no move, but cursor is not coupled? couple it
  else if (m_state == kStateUncoupled)
    st = couple();

  if (st)
    return (st);

  ham_assert(m_state == kStateCoupled);

  BtreeNodeProxy *node = m_btree->get_node_from_page(m_coupled_page);
  ham_assert(node->is_leaf());

  if (key) {
    ByteArray *arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
            ? &db->get_key_arena()
            : &txn->get_key_arena();

    st = node->get_key(m_coupled_index, arena, key);
    if (st)
      return (st);
  }

  if (record) {
    ByteArray *arena = (txn == 0 || (txn->get_flags() & HAM_TXN_TEMPORARY))
           ? &db->get_record_arena()
           : &txn->get_record_arena();

    st = node->get_record(m_coupled_index, arena, record, flags,
                    m_duplicate_index, &m_dupe_cache);
    if (st)
      return (st);
  }

  return (0);
}

ham_status_t
BtreeCursor::find(ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
  Transaction *txn = m_parent->get_txn();

  ham_assert(key);

  set_to_nil();

  return (m_btree->find(txn, m_parent, key, record, flags));
}

ham_status_t
BtreeCursor::insert(ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
  Transaction *txn = m_parent->get_txn();

  ham_assert(key);
  ham_assert(record);

  // call the btree insert function
  return (m_btree->insert(txn, m_parent, key, record, flags));
}

ham_status_t
BtreeCursor::erase(ham_u32_t flags)
{
  Transaction *txn = m_parent->get_txn();

  if (m_state != kStateUncoupled && m_state != kStateCoupled)
    return (HAM_CURSOR_IS_NIL);

  return (m_btree->erase(txn, m_parent, 0, 0, flags));
}

bool
BtreeCursor::points_to(Page *page, ham_u32_t slot)
{
  ham_status_t st;

  if (m_state == kStateUncoupled) {
    st = couple();
    if (st)
      return (false);
  }

  if (m_state == kStateCoupled)
    return (m_coupled_page == page && m_coupled_index == slot);

  return (false);
}

bool
BtreeCursor::points_to(ham_key_t *key)
{
  if (m_state == kStateUncoupled) {
    if (m_uncoupled_key.size != key->size)
      return (false);
    return (0 == m_btree->compare_keys(key, &m_uncoupled_key));
  }

  if (m_state == kStateCoupled) {
    BtreeNodeProxy *node = m_btree->get_node_from_page(m_coupled_page);
    return (node->equals(key, m_coupled_index));
  }

  ham_assert(!"shouldn't be here");
  return (false);
}

ham_status_t
BtreeCursor::get_duplicate_count(ham_u32_t *count, ham_u32_t flags)
{
  // uncoupled cursor: couple it
  if (m_state == kStateUncoupled) {
    ham_status_t st = couple();
    if (st)
      return (st);
  }
  else if (m_state != kStateCoupled)
    return (HAM_CURSOR_IS_NIL);

  BtreeNodeProxy *node = m_btree->get_node_from_page(m_coupled_page);
  return (node->get_duplicate_count(m_coupled_index, count));
}

ham_status_t
BtreeCursor::get_record_size(ham_u64_t *size)
{
  // uncoupled cursor: couple it
  if (m_state == kStateUncoupled) {
    ham_status_t st = couple();
    if (st)
      return (st);
  }
  else if (m_state != kStateCoupled)
    return (HAM_CURSOR_IS_NIL);

  BtreeNodeProxy *node = m_btree->get_node_from_page(m_coupled_page);
  return (node->get_record_size(m_coupled_index, m_duplicate_index, size));
}

ham_status_t
BtreeCursor::couple()
{
  ham_assert(m_state == kStateUncoupled);

  /*
   * Make a 'find' on the cached key; if we succeed, the cursor
   * is automatically coupled. Since |find()| overwrites and modifies
   * the cursor's state, keep a backup and restore it afterwards.
   */
  ham_u32_t duplicate_index = m_duplicate_index;
  ByteArray uncoupled_arena = m_uncoupled_arena;
  ham_key_t uncoupled_key = m_uncoupled_key;
  m_uncoupled_arena = ByteArray();

  ham_status_t st = find(&uncoupled_key, 0, 0);

  m_duplicate_index = duplicate_index;
  m_uncoupled_key = uncoupled_key;
  m_uncoupled_arena = uncoupled_arena;
  uncoupled_arena.disown(); // do not free when going out of scope

  return (st);
}

ham_status_t
BtreeCursor::move_first(ham_u32_t flags)
{
  LocalDatabase *db = m_parent->get_db();
  LocalEnvironment *env = db->get_local_env();

  // get a NIL cursor
  set_to_nil();

  // get the root page
  if (!m_btree->get_root_address())
    return (HAM_KEY_NOT_FOUND);

  Page *page = env->get_page_manager()->fetch_page(db,
          m_btree->get_root_address());

  // while we've not reached the leaf: pick the smallest element
  // and traverse down
  while (1) {
    BtreeNodeProxy *node = m_btree->get_node_from_page(page);
    // check for an empty root page
    if (node->get_count() == 0)
      return (HAM_KEY_NOT_FOUND);
    // leave the loop when we've reached the leaf page
    if (node->is_leaf())
      break;

    page = env->get_page_manager()->fetch_page(db, node->get_ptr_down());
  }

  // couple this cursor to the smallest key in this page
  couple_to_page(page, 0, 0);

  return (0);
}

ham_status_t
BtreeCursor::move_next(ham_u32_t flags)
{
  ham_status_t st;
  LocalDatabase *db = m_parent->get_db();
  LocalEnvironment *env = db->get_local_env();

  // uncoupled cursor: couple it
  if (m_state == kStateUncoupled) {
    st = couple();
    if (st)
      return (st);
  }
  else if (m_state != kStateCoupled)
    return (HAM_CURSOR_IS_NIL);

  BtreeNodeProxy *node = m_btree->get_node_from_page(m_coupled_page);

  // if this key has duplicates: get the next duplicate; otherwise
  // (and if there's no duplicate): fall through
  if (node->has_duplicates(m_coupled_index)
              && (!(flags & HAM_SKIP_DUPLICATES))) {
    m_duplicate_index++;
    st = env->get_duplicate_manager()->get(node->get_record_id(m_coupled_index),
                    m_duplicate_index, &m_dupe_cache);
    if (st) {
      m_duplicate_index--; // undo the increment above
      if (st != HAM_KEY_NOT_FOUND)
        return (st);
    }
    else if (!st)
      return (0);
  }

  // don't continue if ONLY_DUPLICATES is set
  if (flags & HAM_ONLY_DUPLICATES)
    return (HAM_KEY_NOT_FOUND);

  // if the index+1 is still in the coupled page, just increment the index
  if (m_coupled_index + 1 < node->get_count()) {
    couple_to_page(m_coupled_page, m_coupled_index + 1, 0);
    return (0);
  }

  // otherwise uncouple the cursor and load the right sibling page
  if (!node->get_right())
    return (HAM_KEY_NOT_FOUND);

  remove_cursor_from_page(m_coupled_page);

  Page *page = env->get_page_manager()->fetch_page(db, node->get_right());

  // couple this cursor to the smallest key in this page
  couple_to_page(page, 0, 0);

  return (0);
}

ham_status_t
BtreeCursor::move_previous(ham_u32_t flags)
{
  ham_status_t st;
  LocalDatabase *db = m_parent->get_db();
  LocalEnvironment *env = db->get_local_env();

  // uncoupled cursor: couple it
  if (m_state == kStateUncoupled) {
    st = couple();
    if (st)
      return (st);
  }
  else if (m_state != kStateCoupled)
    return (HAM_CURSOR_IS_NIL);

  BtreeNodeProxy *node = m_btree->get_node_from_page(m_coupled_page);

  // if this key has duplicates: get the previous duplicate; otherwise
  // (and if there's no duplicate): fall through
  if (node->has_duplicates(m_coupled_index)
      && (!(flags & HAM_SKIP_DUPLICATES))
      && m_duplicate_index > 0) {
    m_duplicate_index--;
    st = env->get_duplicate_manager()->get(node->get_record_id(m_coupled_index),
                    m_duplicate_index, &m_dupe_cache);
    if (st) {
      m_duplicate_index++; // undo the decrement above
      if (st != HAM_KEY_NOT_FOUND)
        return (st);
    }
    else if (!st)
      return (0);
  }

  // don't continue if ONLY_DUPLICATES is set
  if (flags & HAM_ONLY_DUPLICATES)
    return (HAM_KEY_NOT_FOUND);

  // if the index-1 is till in the coupled page, just decrement the index
  if (m_coupled_index != 0) {
    couple_to_page(m_coupled_page, m_coupled_index - 1);
  }
  // otherwise load the left sibling page
  else {
    if (!node->get_left())
      return (HAM_KEY_NOT_FOUND);

    remove_cursor_from_page(m_coupled_page);

    Page *page = env->get_page_manager()->fetch_page(db, node->get_left());
    node = m_btree->get_node_from_page(page);

    // couple this cursor to the highest key in this page
    couple_to_page(page, node->get_count() - 1);
  }
  m_duplicate_index = 0;

  // if duplicates are enabled: move to the end of the duplicate-list
  if (node->has_duplicates(m_coupled_index)
      && !(flags & HAM_SKIP_DUPLICATES)) {
    ham_u32_t count;
    st = env->get_duplicate_manager()->get_count(node->get_record_id(
                            m_coupled_index),
                    &count, &m_dupe_cache);
    if (st)
      return (st);
    m_duplicate_index = count - 1;
  }

  return (0);
}

ham_status_t
BtreeCursor::move_last(ham_u32_t flags)
{
  LocalDatabase *db = m_parent->get_db();
  LocalEnvironment *env = db->get_local_env();

  // get a NIL cursor
  set_to_nil();

  // get the root page
  if (!m_btree->get_root_address())
    return (HAM_KEY_NOT_FOUND);

  Page *page = env->get_page_manager()->fetch_page(db,
          m_btree->get_root_address());

  // while we've not reached the leaf: pick the largest element
  // and traverse down
  BtreeNodeProxy *node;
  while (1) {
    node = m_btree->get_node_from_page(page);
    // check for an empty root page
    if (node->get_count() == 0)
      return (HAM_KEY_NOT_FOUND);
    // leave the loop when we've reached a leaf page
    if (node->is_leaf())
      break;

    page = env->get_page_manager()->fetch_page(db,
            node->get_record_id(node->get_count() - 1));
  }

  // couple this cursor to the largest key in this page
  couple_to_page(page, node->get_count() - 1, 0);

  // if duplicates are enabled: move to the end of the duplicate-list
  if (node->has_duplicates(m_coupled_index) && !(flags & HAM_SKIP_DUPLICATES)) {
    ham_u32_t count;
    ham_status_t st = env->get_duplicate_manager()->get_count(
                    node->get_record_id(m_coupled_index),
                    &count, &m_dupe_cache);
    if (st)
      return (st);
    m_duplicate_index = count - 1;
  }

  return (0);
}

void
BtreeCursor::couple_to_page(Page *page, ham_u32_t index)
{
  m_coupled_index = index;
  m_state = kStateCoupled;
  if (m_coupled_page == page)
    return;

  m_coupled_page = page;

  // add the cursor to the page
  if (page->get_cursor_list()) {
    m_next_in_page = page->get_cursor_list();
    m_previous_in_page = 0;
    page->get_cursor_list()->m_previous_in_page = this;
  }
  page->set_cursor_list(this);
}

void
BtreeCursor::remove_cursor_from_page(Page *page)
{
  BtreeCursor *n, *p;

  if (this == page->get_cursor_list()) {
    n = m_next_in_page;
    if (n)
      n->m_previous_in_page = 0;
    page->set_cursor_list(n);
  }
  else {
    n = m_next_in_page;
    p = m_previous_in_page;
    if (p)
      p->m_next_in_page = n;
    if (n)
      n->m_previous_in_page = p;
  }

  m_coupled_page = 0;
  m_next_in_page = 0;
  m_previous_in_page = 0;
}

ham_status_t
BtreeCursor::uncouple_all_cursors(Page *page, ham_u32_t start)
{
  ham_status_t st;
  bool skipped = false;
  Cursor *cursors = page->get_cursor_list()
          ? page->get_cursor_list()->get_parent()
          : 0;

  while (cursors) {
    BtreeCursor *btc = cursors->get_btree_cursor();
    BtreeCursor *next = btc->m_next_in_page;

    // ignore all cursors which are already uncoupled or which are
    // coupled to a key in the Transaction
    if (btc->m_state == kStateCoupled) {
      // skip this cursor if its position is < start
      if (btc->m_coupled_index < start) {
        cursors = next ? next->m_parent : 0;
        skipped = true;
        continue;
      }

      // otherwise: uncouple the cursor from the page
      st = btc->uncouple_from_page();
      if (st)
        return (st);
    }

    cursors = next ? next->m_parent : 0;
  }

  if (!skipped)
    page->set_cursor_list(0);

  return (0);
}

} // namespace hamsterdb
