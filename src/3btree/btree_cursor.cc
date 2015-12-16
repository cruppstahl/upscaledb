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

#include <string.h>

// Always verify that a file of level N does not include headers > N!
#include "1base/error.h"
#include "2page/page.h"
#include "3page_manager/page_manager.h"
#include "3btree/btree_index.h"
#include "3btree/btree_cursor.h"
#include "3btree/btree_node_proxy.h"
#include "4cursor/cursor_local.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

BtreeCursor::BtreeCursor(LocalCursor *parent)
  : m_parent(parent), m_state(0), m_duplicate_index(0),
    m_coupled_page(0), m_coupled_index(0), m_next_in_page(0),
    m_previous_in_page(0)
{
  memset(&m_uncoupled_key, 0, sizeof(m_uncoupled_key));
  m_btree = parent->ldb()->btree_index();
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
}

void
BtreeCursor::uncouple_from_page(Context *context)
{
  if (m_state == kStateUncoupled || m_state == kStateNil)
    return;

  ups_assert(m_coupled_page != 0);

  // get the btree-entry of this key
  BtreeNodeProxy *node = m_btree->get_node_from_page(m_coupled_page);
  ups_assert(node->is_leaf());
  node->get_key(context, m_coupled_index, &m_uncoupled_arena, &m_uncoupled_key);

  // uncouple the page
  remove_cursor_from_page(m_coupled_page);

  // set the state and the uncoupled key
  m_state = BtreeCursor::kStateUncoupled;
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

void
BtreeCursor::overwrite(Context *context, ups_record_t *record, uint32_t flags)
{
  // uncoupled cursor: couple it
  if (m_state == kStateUncoupled)
    couple(context);
  else if (m_state != kStateCoupled)
    throw Exception(UPS_CURSOR_IS_NIL);

  // copy the key flags, and remove all flags concerning the key size
  BtreeNodeProxy *node = m_btree->get_node_from_page(m_coupled_page);
  node->set_record(context, m_coupled_index, record, m_duplicate_index,
                    flags | UPS_OVERWRITE, 0);

  m_coupled_page->set_dirty(true);
}

ups_status_t
BtreeCursor::move(Context *context, ups_key_t *key, ByteArray *key_arena,
                ups_record_t *record, ByteArray *record_arena, uint32_t flags)
{
  ups_status_t st = 0;

  if (flags & UPS_CURSOR_FIRST)
    st = move_first(context, flags);
  else if (flags & UPS_CURSOR_LAST)
    st = move_last(context, flags);
  else if (flags & UPS_CURSOR_NEXT)
    st = move_next(context, flags);
  else if (flags & UPS_CURSOR_PREVIOUS)
    st = move_previous(context, flags);
  // no move, but cursor is nil? return error
  else if (m_state == kStateNil) {
    if (key || record)
      return (UPS_CURSOR_IS_NIL);
    else
      return (0);
  }
  // no move, but cursor is not coupled? couple it
  else if (m_state == kStateUncoupled)
    couple(context);

  if (st)
    return (st);

  ups_assert(m_state == kStateCoupled);

  BtreeNodeProxy *node = m_btree->get_node_from_page(m_coupled_page);
  ups_assert(node->is_leaf());

  if (key)
    node->get_key(context, m_coupled_index, key_arena, key);

  if (record)
    node->get_record(context, m_coupled_index, record_arena, record,
                    flags, m_duplicate_index);

  return (0);
}

ups_status_t
BtreeCursor::find(Context *context, ups_key_t *key, ByteArray *key_arena,
                ups_record_t *record, ByteArray *record_arena, uint32_t flags)
{
  set_to_nil();

  return (m_btree->find(context, m_parent, key, key_arena, record,
                          record_arena, flags));
}

bool
BtreeCursor::points_to(Context *context, Page *page, int slot)
{
  if (m_state == kStateUncoupled)
    couple(context);

  if (m_state == kStateCoupled)
    return (m_coupled_page == page && m_coupled_index == slot);

  return (false);
}

bool
BtreeCursor::points_to(Context *context, ups_key_t *key)
{
  if (m_state == kStateUncoupled) {
    if (m_uncoupled_key.size != key->size)
      return (false);
    return (0 == m_btree->compare_keys(key, &m_uncoupled_key));
  }

  if (m_state == kStateCoupled) {
    BtreeNodeProxy *node = m_btree->get_node_from_page(m_coupled_page);
    return (node->equals(context, key, m_coupled_index));
  }

  ups_assert(!"shouldn't be here");
  return (false);
}

ups_status_t
BtreeCursor::move_to_next_page(Context *context)
{
  LocalEnvironment *env = m_parent->ldb()->lenv();

  // uncoupled cursor: couple it
  if (m_state == kStateUncoupled)
    couple(context);
  else if (m_state != kStateCoupled)
    return (UPS_CURSOR_IS_NIL);

  BtreeNodeProxy *node = m_btree->get_node_from_page(m_coupled_page);
  // if there is no right sibling then couple the cursor to the right-most
  // key in the last page and return KEY_NOT_FOUND
  if (!node->get_right()) {
    uint32_t new_slot = node->get_count() - 1;
    uint32_t new_duplicate = node->get_record_count(context, new_slot);
    couple_to_page(m_coupled_page, new_slot, new_duplicate);
    return (UPS_KEY_NOT_FOUND);
  }

  Page *page = env->page_manager()->fetch(context, node->get_right(),
                        PageManager::kReadOnly);
  couple_to_page(page, 0, 0);
  return (0);
}

int
BtreeCursor::get_record_count(Context *context, uint32_t flags)
{
  // uncoupled cursor: couple it
  if (m_state == kStateUncoupled)
    couple(context);
  else if (m_state != kStateCoupled)
    throw Exception(UPS_CURSOR_IS_NIL);

  BtreeNodeProxy *node = m_btree->get_node_from_page(m_coupled_page);
  return (node->get_record_count(context, m_coupled_index));
}

uint64_t
BtreeCursor::get_record_size(Context *context)
{
  // uncoupled cursor: couple it
  if (m_state == kStateUncoupled)
    couple(context);
  else if (m_state != kStateCoupled)
    throw Exception(UPS_CURSOR_IS_NIL);

  BtreeNodeProxy *node = m_btree->get_node_from_page(m_coupled_page);
  return (node->get_record_size(context, m_coupled_index, m_duplicate_index));
}

void
BtreeCursor::couple(Context *context)
{
  ups_assert(m_state == kStateUncoupled);

  /*
   * Make a 'find' on the cached key; if we succeed, the cursor
   * is automatically coupled. Since |find()| overwrites and modifies
   * the cursor's state, keep a backup and restore it afterwards.
   */
  int duplicate_index = m_duplicate_index;
  ByteArray uncoupled_arena = m_uncoupled_arena;
  ups_key_t uncoupled_key = m_uncoupled_key;
  m_uncoupled_arena = ByteArray();

  find(context, &uncoupled_key, 0, 0, 0, 0);

  m_duplicate_index = duplicate_index;
  m_uncoupled_key = uncoupled_key;
  m_uncoupled_arena = uncoupled_arena;
  uncoupled_arena.disown(); // do not free when going out of scope
}

ups_status_t
BtreeCursor::move_first(Context *context, uint32_t flags)
{
  LocalDatabase *db = m_parent->ldb();
  LocalEnvironment *env = db->lenv();

  // get a NIL cursor
  set_to_nil();

  // get the root page
  Page *page = env->page_manager()->fetch(context,
                m_btree->root_address(), PageManager::kReadOnly);
  BtreeNodeProxy *node = m_btree->get_node_from_page(page);

  // traverse down to the leafs
  while (!node->is_leaf()) {
    page = env->page_manager()->fetch(context, node->get_ptr_down(),
                    PageManager::kReadOnly);
    node = m_btree->get_node_from_page(page);
  }

  // and to the next page that is NOT empty
  while (node->get_count() == 0) {
    if (node->get_right() == 0)
      return (UPS_KEY_NOT_FOUND);
    page = env->page_manager()->fetch(context, node->get_right(),
                    PageManager::kReadOnly);
    node = m_btree->get_node_from_page(page);
  }

  // couple this cursor to the smallest key in this page
  couple_to_page(page, 0, 0);

  return (0);
}

ups_status_t
BtreeCursor::move_next(Context *context, uint32_t flags)
{
  LocalDatabase *db = m_parent->ldb();
  LocalEnvironment *env = db->lenv();

  // uncoupled cursor: couple it
  if (m_state == kStateUncoupled)
    couple(context);
  else if (m_state != kStateCoupled)
    return (UPS_CURSOR_IS_NIL);

  BtreeNodeProxy *node = m_btree->get_node_from_page(m_coupled_page);

  // if this key has duplicates: get the next duplicate; otherwise
  // (and if there's no duplicate): fall through
  if (!(flags & UPS_SKIP_DUPLICATES)) {
    if (m_duplicate_index
            < node->get_record_count(context, m_coupled_index) - 1) {
      m_duplicate_index++;
      return (0);
    }
  }

  // don't continue if ONLY_DUPLICATES is set
  if (flags & UPS_ONLY_DUPLICATES)
    return (UPS_KEY_NOT_FOUND);

  // if the index+1 is still in the coupled page, just increment the index
  if (m_coupled_index + 1 < (int)node->get_count()) {
    couple_to_page(m_coupled_page, m_coupled_index + 1, 0);
    return (0);
  }

  // otherwise uncouple the cursor and load the right sibling page
  if (!node->get_right())
    return (UPS_KEY_NOT_FOUND);

  Page *page = env->page_manager()->fetch(context, node->get_right(),
                    PageManager::kReadOnly);
  node = m_btree->get_node_from_page(page);

  // if the right node is empty then continue searching for the next
  // non-empty page
  while (node->get_count() == 0) {
    if (!node->get_right())
      return (UPS_KEY_NOT_FOUND);
    page = env->page_manager()->fetch(context, node->get_right(),
                    PageManager::kReadOnly);
    node = m_btree->get_node_from_page(page);
  }

  // couple this cursor to the smallest key in this page
  couple_to_page(page, 0, 0);

  return (0);
}

ups_status_t
BtreeCursor::move_previous(Context *context, uint32_t flags)
{
  LocalDatabase *db = m_parent->ldb();
  LocalEnvironment *env = db->lenv();

  // uncoupled cursor: couple it
  if (m_state == kStateUncoupled)
    couple(context);
  else if (m_state != kStateCoupled)
    return (UPS_CURSOR_IS_NIL);

  BtreeNodeProxy *node = m_btree->get_node_from_page(m_coupled_page);

  // if this key has duplicates: get the previous duplicate; otherwise
  // (and if there's no duplicate): fall through
  if (!(flags & UPS_SKIP_DUPLICATES) && m_duplicate_index > 0) {
    m_duplicate_index--;
    return (0);
  }

  // don't continue if ONLY_DUPLICATES is set
  if (flags & UPS_ONLY_DUPLICATES)
    return (UPS_KEY_NOT_FOUND);

  // if the index-1 is till in the coupled page, just decrement the index
  if (m_coupled_index != 0) {
    couple_to_page(m_coupled_page, m_coupled_index - 1);
  }
  // otherwise load the left sibling page
  else {
    if (!node->get_left())
      return (UPS_KEY_NOT_FOUND);

    Page *page = env->page_manager()->fetch(context, node->get_left(),
                    PageManager::kReadOnly);
    node = m_btree->get_node_from_page(page);

    // if the left node is empty then continue searching for the next
    // non-empty page
    while (node->get_count() == 0) {
      if (!node->get_left())
        return (UPS_KEY_NOT_FOUND);
      page = env->page_manager()->fetch(context, node->get_left(),
                    PageManager::kReadOnly);
      node = m_btree->get_node_from_page(page);
    }

    // couple this cursor to the highest key in this page
    couple_to_page(page, node->get_count() - 1);
  }
  m_duplicate_index = 0;

  // if duplicates are enabled: move to the end of the duplicate-list
  if (!(flags & UPS_SKIP_DUPLICATES))
    m_duplicate_index = node->get_record_count(context, m_coupled_index) - 1;

  return (0);
}

ups_status_t
BtreeCursor::move_last(Context *context, uint32_t flags)
{
  LocalDatabase *db = m_parent->ldb();
  LocalEnvironment *env = db->lenv();

  // get a NIL cursor
  set_to_nil();

  Page *page = env->page_manager()->fetch(context,
                m_btree->root_address(), PageManager::kReadOnly);
  BtreeNodeProxy *node = m_btree->get_node_from_page(page);

  // traverse down to the leafs
  while (!node->is_leaf()) {
    if (node->get_count() == 0)
      page = env->page_manager()->fetch(context, node->get_ptr_down(),
                    PageManager::kReadOnly);
    else
      page = env->page_manager()->fetch(context,
                        node->get_record_id(context, node->get_count() - 1),
                        PageManager::kReadOnly);
    node = m_btree->get_node_from_page(page);
  }

  // and to the last page that is NOT empty
  while (node->get_count() == 0) {
    if (node->get_left() == 0)
      return (UPS_KEY_NOT_FOUND);
    page = env->page_manager()->fetch(context, node->get_left(),
                        PageManager::kReadOnly);
    node = m_btree->get_node_from_page(page);
  }

  // couple this cursor to the largest key in this page
  couple_to_page(page, node->get_count() - 1, 0);

  // if duplicates are enabled: move to the end of the duplicate-list
  if (!(flags & UPS_SKIP_DUPLICATES))
    m_duplicate_index = node->get_record_count(context, m_coupled_index) - 1;

  return (0);
}

void
BtreeCursor::couple_to_page(Page *page, uint32_t index)
{
  ups_assert(page != 0);

  if (m_state == kStateCoupled && m_coupled_page != page)
    remove_cursor_from_page(m_coupled_page);

  m_coupled_index = index;
  m_state = kStateCoupled;
  if (m_coupled_page == page)
    return;

  m_coupled_page = page;

  // add the cursor to the page
  if (page->cursor_list()) {
    m_next_in_page = page->cursor_list();
    m_previous_in_page = 0;
    page->cursor_list()->m_previous_in_page = this;
  }
  page->set_cursor_list(this);
}

void
BtreeCursor::remove_cursor_from_page(Page *page)
{
  BtreeCursor *n, *p;

  if (this == page->cursor_list()) {
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

void
BtreeCursor::uncouple_all_cursors(Context *context, Page *page, int start)
{
  bool skipped = false;
  LocalCursor *cursors = page->cursor_list()
          ? page->cursor_list()->get_parent()
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
      btc->uncouple_from_page(context);
    }

    cursors = next ? next->m_parent : 0;
  }

  if (!skipped)
    page->set_cursor_list(0);
}

} // namespace upscaledb
