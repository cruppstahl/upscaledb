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

// Removes this cursor from a page
static inline void
remove_cursor_from_page(BtreeCursor *cursor, Page *page)
{
  BtreeCursorState &st_ = cursor->st_;
  BtreeCursor *n, *p;

  if (cursor == page->cursor_list()) {
    n = st_.m_next_in_page;
    if (n)
      n->st_.m_previous_in_page = 0;
    page->set_cursor_list(n);
  }
  else {
    n = st_.m_next_in_page;
    p = st_.m_previous_in_page;
    if (p)
      p->st_.m_next_in_page = n;
    if (n)
      n->st_.m_previous_in_page = p;
  }

  st_.m_coupled_page = 0;
  st_.m_next_in_page = 0;
  st_.m_previous_in_page = 0;
}

// Couples the cursor to the current page/key
// Asserts that the cursor is uncoupled. After this call the cursor
// will be coupled.
static inline void
couple(BtreeCursor *cursor, Context *context)
{
  BtreeCursorState &st_ = cursor->st_;
  assert(st_.m_state == BtreeCursor::kStateUncoupled);

  /*
   * Perform a lookup on the cached key; if we succeed, the cursor
   * is automatically coupled. Since |find()| overwrites and modifies
   * the cursor's state, keep a backup and restore it afterwards.
   */
  int duplicate_index = st_.m_duplicate_index;
  ByteArray uncoupled_arena = st_.m_uncoupled_arena;
  ups_key_t uncoupled_key = st_.m_uncoupled_key;
  st_.m_uncoupled_arena = ByteArray();

  cursor->find(context, &uncoupled_key, 0, 0, 0, 0);

  st_.m_duplicate_index = duplicate_index;
  st_.m_uncoupled_key = uncoupled_key;
  st_.m_uncoupled_arena = uncoupled_arena;
  uncoupled_arena.disown(); // do not free when going out of scope
}

// move cursor to the very first key
static inline ups_status_t
move_first(BtreeCursor *cursor, Context *context, uint32_t flags)
{
  BtreeCursorState &st_ = cursor->st_;
  LocalDatabase *db = st_.m_parent->ldb();
  LocalEnvironment *env = db->lenv();

  // get a NIL cursor
  cursor->set_to_nil();

  // get the root page
  Page *page = env->page_manager()->fetch(context, st_.m_btree->root_address(),
                  PageManager::kReadOnly);
  BtreeNodeProxy *node = st_.m_btree->get_node_from_page(page);

  // traverse down to the leafs
  while (!node->is_leaf()) {
    page = env->page_manager()->fetch(context, node->left_child(),
                    PageManager::kReadOnly);
    node = st_.m_btree->get_node_from_page(page);
  }

  // and to the first page that is NOT empty
  while (node->length() == 0) {
    if (unlikely(node->right_sibling() == 0))
      return UPS_KEY_NOT_FOUND;
    page = env->page_manager()->fetch(context, node->right_sibling(),
                    PageManager::kReadOnly);
    node = st_.m_btree->get_node_from_page(page);
  }

  // couple this cursor to the smallest key in this page
  cursor->couple_to_page(page, 0, 0);
  return 0;
}

// move cursor to the very last key
static inline ups_status_t
move_last(BtreeCursor *cursor, Context *context, uint32_t flags)
{
  BtreeCursorState &st_ = cursor->st_;
  LocalDatabase *db = st_.m_parent->ldb();
  LocalEnvironment *env = db->lenv();

  // get a NIL cursor
  cursor->set_to_nil();

  Page *page = env->page_manager()->fetch(context, st_.m_btree->root_address(),
                  PageManager::kReadOnly);
  BtreeNodeProxy *node = st_.m_btree->get_node_from_page(page);

  // traverse down to the leafs
  while (!node->is_leaf()) {
    if (unlikely(node->length() == 0))
      page = env->page_manager()->fetch(context, node->left_child(),
                    PageManager::kReadOnly);
    else
      page = env->page_manager()->fetch(context,
                        node->record_id(context, node->length() - 1),
                        PageManager::kReadOnly);
    node = st_.m_btree->get_node_from_page(page);
  }

  // and to the last page that is NOT empty
  while (node->length() == 0) {
    if (unlikely(node->left_sibling() == 0))
      return UPS_KEY_NOT_FOUND;
    page = env->page_manager()->fetch(context, node->left_sibling(),
                        PageManager::kReadOnly);
    node = st_.m_btree->get_node_from_page(page);
  }

  // couple this cursor to the largest key in this page
  cursor->couple_to_page(page, node->length() - 1, 0);

  // if duplicates are enabled: move to the end of the duplicate-list
  if (notset(flags, UPS_SKIP_DUPLICATES))
    st_.m_duplicate_index = node->record_count(context,
                    st_.m_coupled_index) - 1;

  return 0;
}

static inline void
couple_or_throw(BtreeCursor *cursor, Context *context)
{
  BtreeCursorState &st_ = cursor->st_;
  if (st_.m_state == BtreeCursor::kStateUncoupled)
    couple(cursor, context);
  else if (st_.m_state != BtreeCursor::kStateCoupled)
    throw UPS_CURSOR_IS_NIL;
}

// move cursor to the next key
static inline ups_status_t
move_next(BtreeCursor *cursor, Context *context, uint32_t flags)
{
  BtreeCursorState &st_ = cursor->st_;
  LocalDatabase *db = st_.m_parent->ldb();
  LocalEnvironment *env = db->lenv();

  // uncoupled cursor: couple it
  couple_or_throw(cursor, context);

  BtreeNodeProxy *node = st_.m_btree->get_node_from_page(st_.m_coupled_page);

  // if this key has duplicates: get the next duplicate; otherwise
  // (and if there's no duplicate): fall through
  if (notset(flags, UPS_SKIP_DUPLICATES)) {
    if (likely(st_.m_duplicate_index
            < node->record_count(context, st_.m_coupled_index) - 1)) {
      st_.m_duplicate_index++;
      return 0;
    }
  }

  // don't continue if ONLY_DUPLICATES is set
  if (isset(flags, UPS_ONLY_DUPLICATES))
    return UPS_KEY_NOT_FOUND;

  // if the index+1 is still in the coupled page, just increment the index
  if (likely(st_.m_coupled_index + 1 < (int)node->length())) {
    cursor->couple_to_page(st_.m_coupled_page, st_.m_coupled_index + 1, 0);
    return 0;
  }

  // otherwise uncouple the cursor and load the right sibling page
  if (unlikely(!node->right_sibling()))
    return UPS_KEY_NOT_FOUND;

  Page *page = env->page_manager()->fetch(context, node->right_sibling(),
                    PageManager::kReadOnly);
  node = st_.m_btree->get_node_from_page(page);

  // if the right node is empty then continue searching for the next
  // non-empty page
  while (node->length() == 0) {
    if (unlikely(!node->right_sibling()))
      return UPS_KEY_NOT_FOUND;
    page = env->page_manager()->fetch(context, node->right_sibling(),
                    PageManager::kReadOnly);
    node = st_.m_btree->get_node_from_page(page);
  }

  // couple this cursor to the smallest key in this page
  cursor->couple_to_page(page, 0, 0);

  return 0;
}

// move cursor to the previous key
static inline ups_status_t
move_previous(BtreeCursor *cursor, Context *context, uint32_t flags)
{
  BtreeCursorState &st_ = cursor->st_;
  LocalDatabase *db = st_.m_parent->ldb();
  LocalEnvironment *env = db->lenv();

  // uncoupled cursor: couple it
  couple_or_throw(cursor, context);

  BtreeNodeProxy *node = st_.m_btree->get_node_from_page(st_.m_coupled_page);

  // if this key has duplicates: get the previous duplicate; otherwise
  // (and if there's no duplicate): fall through
  if (notset(flags, UPS_SKIP_DUPLICATES) && st_.m_duplicate_index > 0) {
    st_.m_duplicate_index--;
    return 0;
  }

  // don't continue if ONLY_DUPLICATES is set
  if (isset(flags, UPS_ONLY_DUPLICATES))
    return UPS_KEY_NOT_FOUND;

  // if the index-1 is till in the coupled page, just decrement the index
  if (likely(st_.m_coupled_index != 0)) {
    cursor->couple_to_page(st_.m_coupled_page, st_.m_coupled_index - 1);
  }
  // otherwise load the left sibling page
  else {
    if (unlikely(!node->left_sibling()))
      return UPS_KEY_NOT_FOUND;

    Page *page = env->page_manager()->fetch(context, node->left_sibling(),
                    PageManager::kReadOnly);
    node = st_.m_btree->get_node_from_page(page);

    // if the left node is empty then continue searching for the next
    // non-empty page
    while (node->length() == 0) {
      if (unlikely(!node->left_sibling()))
        return UPS_KEY_NOT_FOUND;
      page = env->page_manager()->fetch(context, node->left_sibling(),
                    PageManager::kReadOnly);
      node = st_.m_btree->get_node_from_page(page);
    }

    // couple this cursor to the highest key in this page
    cursor->couple_to_page(page, node->length() - 1);
  }
  st_.m_duplicate_index = 0;

  // if duplicates are enabled: move to the end of the duplicate-list
  if (notset(flags, UPS_SKIP_DUPLICATES))
    st_.m_duplicate_index = node->record_count(context,
                    st_.m_coupled_index) - 1;

  return 0;
}


BtreeCursor::BtreeCursor(LocalCursor *parent)
{
  st_.m_parent = parent;
  st_.m_state = 0;
  st_.m_duplicate_index = 0;
  st_.m_coupled_page = 0;
  st_.m_coupled_index = 0;
  st_.m_next_in_page = 0;
  st_.m_previous_in_page = 0;
  ::memset(&st_.m_uncoupled_key, 0, sizeof(st_.m_uncoupled_key));
  st_.m_btree = parent->ldb()->btree_index();
}

void
BtreeCursor::set_to_nil()
{
  // uncoupled cursor: free the cached pointer
  if (st_.m_state == kStateUncoupled)
    ::memset(&st_.m_uncoupled_key, 0, sizeof(st_.m_uncoupled_key));
  // coupled cursor: remove from page
  else if (st_.m_state == BtreeCursor::kStateCoupled)
    remove_cursor_from_page(this, st_.m_coupled_page);

  st_.m_state = BtreeCursor::kStateNil;
  st_.m_duplicate_index = 0;
}

void
BtreeCursor::uncouple_from_page(Context *context)
{
  if (st_.m_state == kStateUncoupled || st_.m_state == kStateNil)
    return;

  assert(st_.m_coupled_page != 0);

  // get the btree-entry of this key
  BtreeNodeProxy *node = st_.m_btree->get_node_from_page(st_.m_coupled_page);
  assert(node->is_leaf());
  node->key(context, st_.m_coupled_index, &st_.m_uncoupled_arena,
                  &st_.m_uncoupled_key);

  // uncouple the page
  remove_cursor_from_page(this, st_.m_coupled_page);

  // set the state and the uncoupled key
  st_.m_state = BtreeCursor::kStateUncoupled;
}

void
BtreeCursor::couple_to_page(Page *page, uint32_t index, int duplicate_index)
{
  assert(page != 0);

  st_.m_duplicate_index = duplicate_index;

  if (st_.m_state == BtreeCursor::kStateCoupled && st_.m_coupled_page != page)
    remove_cursor_from_page(this, st_.m_coupled_page);

  st_.m_coupled_index = index;
  st_.m_state = BtreeCursor::kStateCoupled;
  if (st_.m_coupled_page == page)
    return;

  st_.m_coupled_page = page;

  // add the cursor to the page
  if (page->cursor_list()) {
    st_.m_next_in_page = page->cursor_list();
    st_.m_previous_in_page = 0;
    page->cursor_list()->st_.m_previous_in_page = this;
  }
  page->set_cursor_list(this);
}

void
BtreeCursor::clone(BtreeCursor *other)
{
  // if the old cursor is coupled: couple the new cursor, too
  if (other->st_.m_state == kStateCoupled) {
    couple_to_page(other->st_.m_coupled_page, other->st_.m_coupled_index);
  }
  // otherwise, if the src cursor is uncoupled: copy the key
  else if (other->st_.m_state == kStateUncoupled) {
    ::memset(&st_.m_uncoupled_key, 0, sizeof(st_.m_uncoupled_key));

    st_.m_uncoupled_arena.copy(other->st_.m_uncoupled_arena.data(),
                   other->st_.m_uncoupled_arena.size());
    st_.m_uncoupled_key.data = st_.m_uncoupled_arena.data();
    st_.m_uncoupled_key.size = st_.m_uncoupled_arena.size();
    st_.m_state = kStateUncoupled;
  }
  else {
    set_to_nil();
  }

  st_.m_duplicate_index = other->st_.m_duplicate_index;
}

void
BtreeCursor::overwrite(Context *context, ups_record_t *record, uint32_t flags)
{
  // uncoupled cursor: couple it
  couple_or_throw(this, context);

  // copy the key flags, and remove all flags concerning the key size
  BtreeNodeProxy *node = st_.m_btree->get_node_from_page(st_.m_coupled_page);
  node->set_record(context, st_.m_coupled_index, record, st_.m_duplicate_index,
                    flags | UPS_OVERWRITE, 0);

  st_.m_coupled_page->set_dirty(true);
}

ups_status_t
BtreeCursor::move(Context *context, ups_key_t *key, ByteArray *key_arena,
                ups_record_t *record, ByteArray *record_arena, uint32_t flags)
{
  ups_status_t st = 0;

  if (isset(flags, UPS_CURSOR_FIRST))
    st = move_first(this, context, flags);
  else if (isset(flags, UPS_CURSOR_LAST))
    st = move_last(this, context, flags);
  else if (isset(flags, UPS_CURSOR_NEXT))
    st = move_next(this, context, flags);
  else if (isset(flags, UPS_CURSOR_PREVIOUS))
    st = move_previous(this, context, flags);
  // no move, but cursor is nil? return error
  else if (unlikely(st_.m_state == kStateNil)) {
    if (key || record)
      return UPS_CURSOR_IS_NIL;
    else
      return 0;
  }
  // no move, but cursor is not coupled? couple it
  else if (st_.m_state == kStateUncoupled)
    couple(this, context);

  if (unlikely(st))
    return st;

  assert(st_.m_state == kStateCoupled);

  BtreeNodeProxy *node = st_.m_btree->get_node_from_page(st_.m_coupled_page);
  assert(node->is_leaf());

  if (key)
    node->key(context, st_.m_coupled_index, key_arena, key);

  if (record)
    node->record(context, st_.m_coupled_index, record_arena, record,
                    flags, st_.m_duplicate_index);

  return 0;
}

ups_status_t
BtreeCursor::find(Context *context, ups_key_t *key, ByteArray *key_arena,
                ups_record_t *record, ByteArray *record_arena, uint32_t flags)
{
  set_to_nil();

  return st_.m_btree->find(context, st_.m_parent, key, key_arena, record,
                          record_arena, flags);
}

bool
BtreeCursor::points_to(Context *context, Page *page, int slot)
{
  if (st_.m_state == kStateUncoupled)
    couple(this, context);

  if (st_.m_state == kStateCoupled)
    return st_.m_coupled_page == page && st_.m_coupled_index == slot;

  return false;
}

bool
BtreeCursor::points_to(Context *context, ups_key_t *key)
{
  if (st_.m_state == kStateUncoupled) {
    if (st_.m_uncoupled_key.size != key->size)
      return false;
    return 0 == st_.m_btree->compare_keys(key, &st_.m_uncoupled_key);
  }

  if (st_.m_state == kStateCoupled) {
    BtreeNodeProxy *node = st_.m_btree->get_node_from_page(st_.m_coupled_page);
    return node->equals(context, key, st_.m_coupled_index);
  }

  assert(!"shouldn't be here");
  return false;
}

ups_status_t
BtreeCursor::move_to_next_page(Context *context)
{
  LocalEnvironment *env = st_.m_parent->ldb()->lenv();

  // uncoupled cursor: couple it
  couple_or_throw(this, context);

  BtreeNodeProxy *node = st_.m_btree->get_node_from_page(st_.m_coupled_page);
  // if there is no right sibling then couple the cursor to the right-most
  // key in the last page and return KEY_NOT_FOUND
  if (unlikely(!node->right_sibling())) {
    uint32_t new_slot = node->length() - 1;
    uint32_t new_duplicate = node->record_count(context, new_slot);
    couple_to_page(st_.m_coupled_page, new_slot, new_duplicate);
    return UPS_KEY_NOT_FOUND;
  }

  Page *page = env->page_manager()->fetch(context, node->right_sibling(),
                        PageManager::kReadOnly);
  couple_to_page(page, 0, 0);
  return 0;
}

int
BtreeCursor::record_count(Context *context, uint32_t flags)
{
  // uncoupled cursor: couple it
  couple_or_throw(this, context);

  BtreeNodeProxy *node = st_.m_btree->get_node_from_page(st_.m_coupled_page);
  return node->record_count(context, st_.m_coupled_index);
}

uint32_t
BtreeCursor::record_size(Context *context)
{
  // uncoupled cursor: couple it
  couple_or_throw(this, context);

  BtreeNodeProxy *node = st_.m_btree->get_node_from_page(st_.m_coupled_page);
  return node->record_size(context, st_.m_coupled_index,
                  st_.m_duplicate_index);
}

void
BtreeCursor::uncouple_all_cursors(Context *context, Page *page, int start)
{
  bool skipped = false;
  LocalCursor *cursors = page->cursor_list()
          ? page->cursor_list()->parent()
          : 0;

  while (cursors) {
    BtreeCursor *btc = cursors->get_btree_cursor();
    BtreeCursor *next = btc->st_.m_next_in_page;

    // ignore all cursors which are already uncoupled or which are
    // coupled to a key in the Transaction
    if (btc->st_.m_state == kStateCoupled) {
      // skip this cursor if its position is < start
      if (btc->st_.m_coupled_index < start) {
        cursors = next ? next->st_.m_parent : 0;
        skipped = true;
        continue;
      }

      // otherwise: uncouple the cursor from the page
      btc->uncouple_from_page(context);
    }

    cursors = next ? next->st_.m_parent : 0;
  }

  if (!skipped)
    page->set_cursor_list(0);
}

} // namespace upscaledb
