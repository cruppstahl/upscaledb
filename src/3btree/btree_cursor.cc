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
  page->cursor_list.del(cursor);

  BtreeCursorState &st_ = cursor->st_;
  st_.coupled_page = 0;
}

// Couples the cursor to the current page/key
// Asserts that the cursor is uncoupled. After this call the cursor
// will be coupled.
static inline void
couple(BtreeCursor *cursor, Context *context)
{
  BtreeCursorState &st_ = cursor->st_;
  assert(st_.state == BtreeCursor::kStateUncoupled);

  /*
   * Perform a lookup on the cached key; if we succeed, the cursor
   * is automatically coupled. Since |find()| overwrites and modifies
   * the cursor's state, keep a backup and restore it afterwards.
   */
  int duplicate_index = st_.duplicate_index;
  ByteArray uncoupled_arena = st_.uncoupled_arena;
  ups_key_t uncoupled_key = st_.uncoupled_key;
  st_.uncoupled_arena = ByteArray();

  cursor->find(context, &uncoupled_key, 0, 0, 0, 0);

  st_.duplicate_index = duplicate_index;
  st_.uncoupled_key = uncoupled_key;
  st_.uncoupled_arena = uncoupled_arena;
  uncoupled_arena.disown(); // do not free when going out of scope
}

// move cursor to the very first key
static inline ups_status_t
move_first(BtreeCursor *cursor, Context *context, uint32_t flags)
{
  BtreeCursorState &st_ = cursor->st_;
  LocalDb *db = (LocalDb *)st_.parent->db;
  LocalEnv *env = (LocalEnv *)db->env;

  // get a NIL cursor
  cursor->set_to_nil();

  // get the root page
  Page *page = st_.btree->root_page(context);
  BtreeNodeProxy *node = st_.btree->get_node_from_page(page);

  // traverse down to the leafs
  while (!node->is_leaf()) {
    page = env->page_manager->fetch(context, node->left_child(),
                    PageManager::kReadOnly);
    node = st_.btree->get_node_from_page(page);
  }

  // and to the first page that is NOT empty
  while (node->length() == 0) {
    if (unlikely(node->right_sibling() == 0))
      return UPS_KEY_NOT_FOUND;
    page = env->page_manager->fetch(context, node->right_sibling(),
                    PageManager::kReadOnly);
    node = st_.btree->get_node_from_page(page);
  }

  // couple this cursor to the smallest key in this page
  cursor->couple_to(page, 0, 0);
  return 0;
}

// move cursor to the very last key
static inline ups_status_t
move_last(BtreeCursor *cursor, Context *context, uint32_t flags)
{
  BtreeCursorState &st_ = cursor->st_;
  LocalDb *db = (LocalDb *)st_.parent->db;
  LocalEnv *env = (LocalEnv *)db->env;

  // get a NIL cursor
  cursor->set_to_nil();

  Page *page = st_.btree->root_page(context);
  BtreeNodeProxy *node = st_.btree->get_node_from_page(page);

  // traverse down to the leafs
  while (!node->is_leaf()) {
    if (unlikely(node->length() == 0))
      page = env->page_manager->fetch(context, node->left_child(),
                    PageManager::kReadOnly);
    else
      page = env->page_manager->fetch(context,
                        node->record_id(context, node->length() - 1),
                        PageManager::kReadOnly);
    node = st_.btree->get_node_from_page(page);
  }

  // and to the last page that is NOT empty
  while (node->length() == 0) {
    if (unlikely(node->left_sibling() == 0))
      return UPS_KEY_NOT_FOUND;
    page = env->page_manager->fetch(context, node->left_sibling(),
                        PageManager::kReadOnly);
    node = st_.btree->get_node_from_page(page);
  }

  // couple this cursor to the largest key in this page
  cursor->couple_to(page, node->length() - 1, 0);

  // if duplicates are enabled: move to the end of the duplicate-list
  if (NOTSET(flags, UPS_SKIP_DUPLICATES))
    st_.duplicate_index = node->record_count(context,
                    st_.coupled_index) - 1;

  return 0;
}

static inline void
couple_or_throw(BtreeCursor *cursor, Context *context)
{
  BtreeCursorState &st_ = cursor->st_;
  if (st_.state == BtreeCursor::kStateUncoupled)
    couple(cursor, context);
  else if (st_.state != BtreeCursor::kStateCoupled)
    throw Exception(UPS_CURSOR_IS_NIL);
}

// move cursor to the next key
static inline ups_status_t
move_next(BtreeCursor *cursor, Context *context, uint32_t flags)
{
  BtreeCursorState &st_ = cursor->st_;
  LocalDb *db = (LocalDb *)st_.parent->db;
  LocalEnv *env = (LocalEnv *)db->env;

  // uncoupled cursor: couple it
  couple_or_throw(cursor, context);

  BtreeNodeProxy *node = st_.btree->get_node_from_page(st_.coupled_page);

  // if this key has duplicates: get the next duplicate; otherwise
  // (and if there's no duplicate): fall through
  if (NOTSET(flags, UPS_SKIP_DUPLICATES)) {
    if (likely(st_.duplicate_index
            < node->record_count(context, st_.coupled_index) - 1)) {
      st_.duplicate_index++;
      return 0;
    }
  }

  // don't continue if ONLY_DUPLICATES is set
  if (ISSET(flags, UPS_ONLY_DUPLICATES))
    return UPS_KEY_NOT_FOUND;

  // if the index+1 is still in the coupled page, just increment the index
  if (likely(st_.coupled_index + 1 < (int)node->length())) {
    cursor->couple_to(st_.coupled_page, st_.coupled_index + 1, 0);
    return 0;
  }

  // otherwise uncouple the cursor and load the right sibling page
  if (unlikely(!node->right_sibling()))
    return UPS_KEY_NOT_FOUND;

  Page *page = env->page_manager->fetch(context, node->right_sibling(),
                    PageManager::kReadOnly);
  node = st_.btree->get_node_from_page(page);

  // if the right node is empty then continue searching for the next
  // non-empty page
  while (node->length() == 0) {
    if (unlikely(!node->right_sibling()))
      return UPS_KEY_NOT_FOUND;
    page = env->page_manager->fetch(context, node->right_sibling(),
                    PageManager::kReadOnly);
    node = st_.btree->get_node_from_page(page);
  }

  // couple this cursor to the smallest key in this page
  cursor->couple_to(page, 0, 0);

  return 0;
}

// move cursor to the previous key
static inline ups_status_t
move_previous(BtreeCursor *cursor, Context *context, uint32_t flags)
{
  BtreeCursorState &st_ = cursor->st_;
  LocalDb *db = (LocalDb *)st_.parent->db;
  LocalEnv *env = (LocalEnv *)db->env;

  // uncoupled cursor: couple it
  couple_or_throw(cursor, context);

  BtreeNodeProxy *node = st_.btree->get_node_from_page(st_.coupled_page);

  // if this key has duplicates: get the previous duplicate; otherwise
  // (and if there's no duplicate): fall through
  if (NOTSET(flags, UPS_SKIP_DUPLICATES) && st_.duplicate_index > 0) {
    st_.duplicate_index--;
    return 0;
  }

  // don't continue if ONLY_DUPLICATES is set
  if (ISSET(flags, UPS_ONLY_DUPLICATES))
    return UPS_KEY_NOT_FOUND;

  // if the index-1 is till in the coupled page, just decrement the index
  if (likely(st_.coupled_index != 0)) {
    cursor->couple_to(st_.coupled_page, st_.coupled_index - 1);
  }
  // otherwise load the left sibling page
  else {
    if (unlikely(!node->left_sibling()))
      return UPS_KEY_NOT_FOUND;

    Page *page = env->page_manager->fetch(context, node->left_sibling(),
                    PageManager::kReadOnly);
    node = st_.btree->get_node_from_page(page);

    // if the left node is empty then continue searching for the next
    // non-empty page
    while (node->length() == 0) {
      if (unlikely(!node->left_sibling()))
        return UPS_KEY_NOT_FOUND;
      page = env->page_manager->fetch(context, node->left_sibling(),
                    PageManager::kReadOnly);
      node = st_.btree->get_node_from_page(page);
    }

    // couple this cursor to the highest key in this page
    cursor->couple_to(page, node->length() - 1);
  }
  st_.duplicate_index = 0;

  // if duplicates are enabled: move to the end of the duplicate-list
  if (NOTSET(flags, UPS_SKIP_DUPLICATES))
    st_.duplicate_index = node->record_count(context,
                    st_.coupled_index) - 1;

  return 0;
}


BtreeCursor::BtreeCursor(LocalCursor *parent)
{
  st_.parent = parent;
  st_.state = 0;
  st_.duplicate_index = 0;
  st_.coupled_page = 0;
  st_.coupled_index = 0;
  ::memset(&st_.uncoupled_key, 0, sizeof(st_.uncoupled_key));
  st_.btree = ((LocalDb *)parent->db)->btree_index.get();
}

void
BtreeCursor::clone(BtreeCursor *other)
{
  // if the old cursor is coupled: couple the new cursor, too
  if (other->st_.state == kStateCoupled) {
    couple_to(other->st_.coupled_page, other->st_.coupled_index);
  }
  // otherwise, if the src cursor is uncoupled: copy the key
  else if (other->st_.state == kStateUncoupled) {
    ::memset(&st_.uncoupled_key, 0, sizeof(st_.uncoupled_key));

    st_.uncoupled_arena.copy(other->st_.uncoupled_arena.data(),
                   other->st_.uncoupled_arena.size());
    st_.uncoupled_key.data = st_.uncoupled_arena.data();
    st_.uncoupled_key.size = (uint16_t)st_.uncoupled_arena.size();
    st_.state = kStateUncoupled;
  }
  else {
    set_to_nil();
  }

  st_.duplicate_index = other->st_.duplicate_index;
}

void
BtreeCursor::set_to_nil()
{
  // uncoupled cursor: free the cached pointer
  if (st_.state == kStateUncoupled)
    ::memset(&st_.uncoupled_key, 0, sizeof(st_.uncoupled_key));
  // coupled cursor: remove from page
  else if (st_.state == BtreeCursor::kStateCoupled)
    remove_cursor_from_page(this, st_.coupled_page);

  st_.state = BtreeCursor::kStateNil;
  st_.duplicate_index = 0;
}

void
BtreeCursor::couple_to(Page *page, uint32_t index, int duplicate_index)
{
  assert(page != 0);

  st_.duplicate_index = duplicate_index;

  if (st_.state == BtreeCursor::kStateCoupled && st_.coupled_page != page)
    remove_cursor_from_page(this, st_.coupled_page);

  st_.coupled_index = index;
  st_.state = BtreeCursor::kStateCoupled;
  if (st_.coupled_page == page)
    return;

  st_.coupled_page = page;

  // add the cursor to the page
  page->cursor_list.put(this);
}

void
BtreeCursor::uncouple_from_page(Context *context)
{
  if (st_.state == kStateUncoupled || is_nil())
    return;

  assert(st_.coupled_page != 0);

  // get the btree-entry of this key
  BtreeNodeProxy *node = st_.btree->get_node_from_page(st_.coupled_page);
  assert(node->is_leaf());
  node->key(context, st_.coupled_index, &st_.uncoupled_arena,
                  &st_.uncoupled_key);

  // uncouple the page
  remove_cursor_from_page(this, st_.coupled_page);

  // set the state and the uncoupled key
  st_.state = BtreeCursor::kStateUncoupled;
}

int
BtreeCursor::compare(Context *context, ups_key_t *key)
{
  assert(!is_nil());

  if (st_.state == BtreeCursor::kStateCoupled) {
    Page *page = coupled_page();
    int slot = coupled_slot();
    int rv = st_.btree->get_node_from_page(page)->compare(context, key, slot);

    // need to fix the sort order - we compare key vs page[slot], but the
    // caller expects m_last_cmp to be the comparison of page[slot] vs key
    if (rv < 0)
      rv = +1;
    else if (rv > 0)
      rv = -1;

    return rv;
  }

  // if (state() == BtreeCursor::kStateUncoupled)
  return st_.btree->compare_keys(&st_.uncoupled_key, key);
}

void
BtreeCursor::overwrite(Context *context, ups_record_t *record, uint32_t flags)
{
  // uncoupled cursor: couple it
  couple_or_throw(this, context);

  // copy the key flags, and remove all flags concerning the key size
  BtreeNodeProxy *node = st_.btree->get_node_from_page(st_.coupled_page);
  node->set_record(context, st_.coupled_index, record, st_.duplicate_index,
                    flags | UPS_OVERWRITE, 0);

  st_.coupled_page->set_dirty(true);
}

ups_status_t
BtreeCursor::move(Context *context, ups_key_t *key, ByteArray *key_arena,
                ups_record_t *record, ByteArray *record_arena, uint32_t flags)
{
  ups_status_t st = 0;

  if (ISSET(flags, UPS_CURSOR_FIRST))
    st = move_first(this, context, flags);
  else if (ISSET(flags, UPS_CURSOR_LAST))
    st = move_last(this, context, flags);
  else if (ISSET(flags, UPS_CURSOR_NEXT))
    st = move_next(this, context, flags);
  else if (ISSET(flags, UPS_CURSOR_PREVIOUS))
    st = move_previous(this, context, flags);
  // no move, but cursor is nil? return error
  else if (unlikely(is_nil())) {
    if (key || record)
      return UPS_CURSOR_IS_NIL;
    else
      return 0;
  }
  // no move, but cursor is not coupled? couple it
  else if (st_.state == kStateUncoupled)
    couple(this, context);

  if (unlikely(st))
    return st;

  assert(st_.state == kStateCoupled);

  BtreeNodeProxy *node = st_.btree->get_node_from_page(st_.coupled_page);
  assert(node->is_leaf());

  if (key)
    node->key(context, st_.coupled_index, key_arena, key);

  if (record)
    node->record(context, st_.coupled_index, record_arena, record,
                    flags, st_.duplicate_index);

  return 0;
}

ups_status_t
BtreeCursor::find(Context *context, ups_key_t *key, ByteArray *key_arena,
                ups_record_t *record, ByteArray *record_arena, uint32_t flags)
{
  set_to_nil();

  return st_.btree->find(context, st_.parent, key, key_arena, record,
                          record_arena, flags);
}

bool
BtreeCursor::points_to(Context *context, Page *page, int slot)
{
  if (st_.state == kStateUncoupled)
    couple(this, context);

  if (st_.state == kStateCoupled)
    return st_.coupled_page == page && st_.coupled_index == slot;

  return false;
}

bool
BtreeCursor::points_to(Context *context, ups_key_t *key)
{
  if (st_.state == kStateUncoupled) {
    if (st_.uncoupled_key.size != key->size)
      return false;
    return 0 == st_.btree->compare_keys(key, &st_.uncoupled_key);
  }

  if (st_.state == kStateCoupled) {
    BtreeNodeProxy *node = st_.btree->get_node_from_page(st_.coupled_page);
    return node->equals(context, key, st_.coupled_index);
  }

  assert(!"shouldn't be here");
  return false;
}

ups_status_t
BtreeCursor::move_to_next_page(Context *context)
{
  LocalEnv *env = (LocalEnv *)st_.parent->db->env;

  // uncoupled cursor: couple it
  couple_or_throw(this, context);

  BtreeNodeProxy *node = st_.btree->get_node_from_page(st_.coupled_page);
  // if there is no right sibling then couple the cursor to the right-most
  // key in the last page and return KEY_NOT_FOUND
  if (unlikely(!node->right_sibling())) {
    uint32_t new_slot = node->length() - 1;
    uint32_t new_duplicate = node->record_count(context, new_slot);
    couple_to(st_.coupled_page, new_slot, new_duplicate);
    return UPS_KEY_NOT_FOUND;
  }

  Page *page = env->page_manager->fetch(context, node->right_sibling(),
                        PageManager::kReadOnly);
  couple_to(page, 0, 0);
  return 0;
}

int
BtreeCursor::record_count(Context *context, uint32_t flags)
{
  // uncoupled cursor: couple it
  couple_or_throw(this, context);

  BtreeNodeProxy *node = st_.btree->get_node_from_page(st_.coupled_page);
  return node->record_count(context, st_.coupled_index);
}

uint32_t
BtreeCursor::record_size(Context *context)
{
  // uncoupled cursor: couple it
  couple_or_throw(this, context);

  BtreeNodeProxy *node = st_.btree->get_node_from_page(st_.coupled_page);
  return node->record_size(context, st_.coupled_index,
                  st_.duplicate_index);
}

void
BtreeCursor::uncouple_all_cursors(Context *context, Page *page, int start)
{
  bool skipped = false;
  
  for (BtreeCursor *btc = page->cursor_list.head();
                  btc != 0;
                  btc = btc->list_node.next[0]) {
    // ignore all cursors which are already uncoupled or which are
    // coupled to a key in the Txn
    if (btc->st_.state == kStateCoupled) {
      // skip this cursor if its position is < start
      if (btc->st_.coupled_index < start) {
        skipped = true;
        continue;
      }

      // otherwise: uncouple the cursor from the page
      btc->uncouple_from_page(context);
    }
  }

  if (!skipped)
    page->cursor_list.clear();
}

} // namespace upscaledb
