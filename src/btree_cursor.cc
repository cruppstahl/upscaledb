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
#include "cursor.h"
#include "btree.h"
#include "btree_key.h"
#include "btree_node.h"
#include "btree_cursor.h"
#include "page_manager.h"
#include "txn.h"

namespace hamsterdb {

void
BtreeCursor::set_to_nil()
{
  // uncoupled cursor: free the cached pointer
  if (m_state == kStateUncoupled) {
    ham_key_t *key = get_uncoupled_key();
    Memory::release(key->data);
    Memory::release(key);
    m_uncoupled_key = 0;
  }
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
  LocalDatabase *db = m_parent->get_db();

  if (m_state == kStateUncoupled || m_state == kStateNil)
    return (0);

  ham_assert(m_coupled_page != 0);

  // get the btree-entry of this key
  PBtreeNode *node = PBtreeNode::from_page(m_coupled_page);
  ham_assert(node->is_leaf());
  PBtreeKey *entry = node->get_key(db, m_coupled_index);

  // copy the key
  ham_key_t *key = Memory::callocate<ham_key_t>(sizeof(*key));
  if (!key)
    return (HAM_OUT_OF_MEMORY);

  ham_status_t st = db->get_btree_index()->copy_key(entry, key);
  if (st) {
    Memory::release(key->data);
    Memory::release(key);
    m_uncoupled_key = 0;
    return (st);
  }

  // uncouple the page
  remove_cursor_from_page(m_coupled_page);

  // set the state and the uncoupled key
  m_state = BtreeCursor::kStateUncoupled;
  m_uncoupled_key = key;

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
    ham_key_t *key = m_uncoupled_key;
    if (!key)
      key = Memory::callocate<ham_key_t>(sizeof(*key));
    else {
      Memory::release(key->data);
      key->data = 0;
      key->size = 0;
    }

    m_parent->get_db()->copy_key(other->get_uncoupled_key(), key);
    m_uncoupled_key = key;
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
  LocalDatabase *db = m_parent->get_db();
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

  // get the btree node entry
  PBtreeNode *node = PBtreeNode::from_page(m_coupled_page);
  ham_assert(node->is_leaf());
  PBtreeKey *key = node->get_key(db, m_coupled_index);

  // copy the key flags, and remove all flags concerning the key size
  st = key->set_record(db, txn, record, m_duplicate_index,
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
  LocalEnvironment *env = db->get_local_env();
  Transaction *txn = m_parent->get_txn();
  BtreeIndex *be = (BtreeIndex *)db->get_btree_index();

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

  /*
   * during read_key() and read_record() new pages might be needed,
   * and the page at which we're pointing could be moved out of memory;
   * that would mean that the cursor would be uncoupled, and we're losing
   * the 'entry'-pointer. therefore we 'lock' the page by incrementing
   * the reference counter
   */
  ham_assert(m_state == kStateCoupled);
  PBtreeNode *node = PBtreeNode::from_page(m_coupled_page);
  ham_assert(node->is_leaf());
  PBtreeKey *entry = node->get_key(db, m_coupled_index);

  if (key) {
    st = be->read_key(txn, entry, key);
    if (st)
      return (st);
  }

  if (record) {
    ham_u64_t *ridptr = 0;
    if (entry->get_flags() & PBtreeKey::kDuplicates
        && m_duplicate_index) {
      PDupeEntry *e = &m_dupe_cache;
      if (!dupe_entry_get_rid(e)) {
        st = env->get_duplicate_manager()->get(entry->get_ptr(),
                        m_duplicate_index, &m_dupe_cache);
        if (st)
          return (st);
      }
      record->_intflags = dupe_entry_get_flags(e);
      record->_rid = dupe_entry_get_rid(e);
      ridptr = (ham_u64_t *)&dupe_entry_get_ridptr(e);
    }
    else {
      record->_intflags = entry->get_flags();
      record->_rid = entry->get_ptr();
      ridptr = entry->get_rawptr();
    }
    st = be->read_record(txn, ridptr, record, flags);
    if (st)
      return (st);
  }

  return (0);
}

ham_status_t
BtreeCursor::find(ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st;
  BtreeIndex *be = m_parent->get_db()->get_btree_index();
  Transaction *txn = m_parent->get_txn();

  ham_assert(key);

  set_to_nil();

  st = be->find(txn, m_parent, key, record, flags);
  if (st) {
    // cursor is now NIL
    return (st);
  }

  return (0);
}

ham_status_t
BtreeCursor::insert(ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
  BtreeIndex *be = m_parent->get_db()->get_btree_index();
  Transaction *txn = m_parent->get_txn();

  ham_assert(key);
  ham_assert(record);

  // call the btree insert function
  return (be->insert(txn, m_parent, key, record, flags));
}

ham_status_t
BtreeCursor::erase(ham_u32_t flags)
{
  BtreeIndex *be = m_parent->get_db()->get_btree_index();
  Transaction *txn = m_parent->get_txn();

  if (m_state != kStateUncoupled && m_state != kStateCoupled)
    return (HAM_CURSOR_IS_NIL);

  return (be->erase(txn, m_parent, 0, 0, flags));
}

bool
BtreeCursor::points_to(PBtreeKey *key)
{
  ham_status_t st;

  if (m_state == kStateUncoupled) {
    st = couple();
    if (st)
      return (false);
  }

  if (m_state == kStateCoupled) {
    PBtreeNode *node = PBtreeNode::from_page(m_coupled_page);
    PBtreeKey *entry = node->get_key(m_parent->get_db(), m_coupled_index);

    if (entry == key)
      return (true);
  }

  return (false);
}

bool
BtreeCursor::points_to(ham_key_t *key)
{
  Cursor *parent = m_parent;
  LocalDatabase *db = parent->get_db();
  bool ret = false;

  if (m_state == kStateUncoupled) {
    ham_key_t *k = get_uncoupled_key();
    if (k->size != key->size)
      return (false);
    return (0 == db->compare_keys(key, k));
  }

  if (m_state == kStateCoupled) {
    PBtreeNode *node = PBtreeNode::from_page(m_coupled_page);
    PBtreeKey *entry = node->get_key(db, m_coupled_index);

    if (entry->get_size() != key->size)
      return (false);

    bool ret = false;
    Cursor *clone = db->cursor_clone(parent);
    ham_status_t st = clone->get_btree_cursor()->uncouple_from_page();
    if (st) {
      db->cursor_close(clone);
      return (false);
    }
    if (0 == db->compare_keys(key,
                clone->get_btree_cursor()->get_uncoupled_key()))
      ret = true;
    db->cursor_close(clone);
    return (ret);
  }

  ham_assert(!"shouldn't be here");
  return (ret);
}

ham_status_t
BtreeCursor::get_duplicate_count(ham_size_t *count, ham_u32_t flags)
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

  PBtreeNode *node = PBtreeNode::from_page(m_coupled_page);
  PBtreeKey *entry = node->get_key(db, m_coupled_index);

  if (!(entry->get_flags() & PBtreeKey::kDuplicates)) {
    *count = 1;
  }
  else {
    st = env->get_duplicate_manager()->get_count(entry->get_ptr(), count, 0);
    if (st)
      return (st);
  }

  return (0);
}

ham_status_t
BtreeCursor::get_record_size(ham_u64_t *size)
{
  ham_status_t st;
  LocalDatabase *db = m_parent->get_db();
  LocalEnvironment *env = db->get_local_env();
  ham_u32_t keyflags = 0;
  ham_u64_t *ridptr = 0;
  ham_u64_t rid = 0;
  PDupeEntry dupeentry;

  // uncoupled cursor: couple it
  if (m_state == kStateUncoupled) {
    st = couple();
    if (st)
      return (st);
  }
  else if (m_state != kStateCoupled)
    return (HAM_CURSOR_IS_NIL);

  PBtreeNode *node = PBtreeNode::from_page(m_coupled_page);
  PBtreeKey *entry = node->get_key(db, m_coupled_index);

  if (entry->get_flags() & PBtreeKey::kDuplicates) {
    st = env->get_duplicate_manager()->get(entry->get_ptr(),
                    m_duplicate_index, &dupeentry);
    if (st)
      return (st);
    keyflags = dupe_entry_get_flags(&dupeentry);
    ridptr = &dupeentry._rid;
    rid = dupeentry._rid;
  }
  else {
    keyflags = entry->get_flags();
    ridptr = entry->get_rawptr();
    rid = entry->get_ptr();
  }

  if (keyflags & PBtreeKey::kBlobSizeTiny) {
    // the highest byte of the record id is the size of the blob
    char *p = (char *)ridptr;
    *size = p[sizeof(ham_u64_t) - 1];
  }
  else if (keyflags & PBtreeKey::kBlobSizeSmall) {
    // record size is sizeof(ham_u64_t)
    *size = sizeof(ham_u64_t);
  }
  else if (keyflags & PBtreeKey::kBlobSizeEmpty) {
    // record size is 0
    *size = 0;
  }
  else {
    st = env->get_blob_manager()->get_datasize(db, rid, size);
    if (st)
      return (st);
  }

  return (0);
}

ham_status_t
BtreeCursor::couple()
{
  ham_key_t key = {0};
  LocalDatabase *db = m_parent->get_db();
  ham_u32_t dupe_idx = m_duplicate_index;

  ham_assert(m_state == kStateUncoupled);

  /*
   * make a 'find' on the cached key; if we succeed, the cursor
   * is automatically coupled
   *
   * the dupe ID is overwritten in BtreeCursor::find, therefore save it
   * and restore it afterwards
   */
  ham_status_t st = db->copy_key(get_uncoupled_key(), &key);
  if (st)
    goto bail;

  st = find(&key, 0, 0);
  m_duplicate_index = dupe_idx;

  // free the cached key
bail:
  Memory::release(key.data);

  return (st);
}

ham_status_t
BtreeCursor::move_first(ham_u32_t flags)
{
  ham_status_t st;
  LocalDatabase *db = m_parent->get_db();
  LocalEnvironment *env = db->get_local_env();
  Page *page;
  BtreeIndex *be = db->get_btree_index();

  // get a NIL cursor
  set_to_nil();

  // get the root page
  if (!be->get_root_address())
    return (HAM_KEY_NOT_FOUND);
  st = env->get_page_manager()->fetch_page(&page, db,
                  be->get_root_address());
  if (st)
    return (st);

  // while we've not reached the leaf: pick the smallest element
  // and traverse down
  while (1) {
    PBtreeNode *node = PBtreeNode::from_page(page);
    // check for an empty root page
    if (node->get_count()==0)
      return (HAM_KEY_NOT_FOUND);
    // leave the loop when we've reached the leaf page
    if (node->is_leaf())
      break;

    st = env->get_page_manager()->fetch_page(&page, db,
                  node->get_ptr_left());
    if (st)
      return (st);
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

  PBtreeNode *node = PBtreeNode::from_page(m_coupled_page);
  PBtreeKey *entry = node->get_key(db, m_coupled_index);

  /*
   * if this key has duplicates: get the next duplicate; otherwise
   * (and if there's no duplicate): fall through
   */
  if (entry->get_flags() & PBtreeKey::kDuplicates
      && (!(flags & HAM_SKIP_DUPLICATES))) {
    m_duplicate_index++;
    st = env->get_duplicate_manager()->get(entry->get_ptr(),
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

  Page *page;
  st = env->get_page_manager()->fetch_page(&page, db, node->get_right());
  if (st)
    return (st);

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

  PBtreeNode *node = PBtreeNode::from_page(m_coupled_page);
  PBtreeKey *entry = node->get_key(db, m_coupled_index);

  // if this key has duplicates: get the previous duplicate; otherwise
  // (and if there's no duplicate): fall through
  if (entry->get_flags() & PBtreeKey::kDuplicates
      && (!(flags & HAM_SKIP_DUPLICATES))
      && m_duplicate_index > 0) {
    m_duplicate_index--;
    st = env->get_duplicate_manager()->get(entry->get_ptr(),
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
    entry = node->get_key(db, m_coupled_index);
  }
  // otherwise load the left sibling page
  else {
    if (!node->get_left())
      return (HAM_KEY_NOT_FOUND);

    remove_cursor_from_page(m_coupled_page);

    Page *page;
    st = env->get_page_manager()->fetch_page(&page, db, node->get_left());
    if (st)
      return (st);
    node = PBtreeNode::from_page(page);

    // couple this cursor to the highest key in this page
    couple_to_page(page, node->get_count() - 1);
    entry = node->get_key(db, m_coupled_index);
  }
  m_duplicate_index = 0;

  // if duplicates are enabled: move to the end of the duplicate-list
  if (entry->get_flags() & PBtreeKey::kDuplicates
      && !(flags & HAM_SKIP_DUPLICATES)) {
    ham_size_t count;
    st = env->get_duplicate_manager()->get_count(entry->get_ptr(),
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
  Page *page;
  PBtreeNode *node;
  BtreeIndex *be = db->get_btree_index();

  // get a NIL cursor
  set_to_nil();

  // get the root page
  if (!be->get_root_address())
    return (HAM_KEY_NOT_FOUND);
  ham_status_t st = env->get_page_manager()->fetch_page(&page, db,
                        be->get_root_address());
  if (st)
    return (st);
  // hack: prior to 2.0, the type of btree root pages was not set
  // correctly
  page->set_type(Page::kTypeBroot);

  // while we've not reached the leaf: pick the largest element
  // and traverse down
  while (1) {
    node = PBtreeNode::from_page(page);
    // check for an empty root page
    if (node->get_count() == 0)
      return (HAM_KEY_NOT_FOUND);
    // leave the loop when we've reached a leaf page
    if (node->is_leaf())
      break;

    PBtreeKey *key = node->get_key(db, node->get_count() - 1);
    st = env->get_page_manager()->fetch_page(&page, db, key->get_ptr());
    if (st)
      return (st);
  }

  // couple this cursor to the largest key in this page
  couple_to_page(page, node->get_count() - 1, 0);
  PBtreeKey *entry = node->get_key(db, m_coupled_index);

  // if duplicates are enabled: move to the end of the duplicate-list
  if (entry->get_flags() & PBtreeKey::kDuplicates
      && !(flags & HAM_SKIP_DUPLICATES)) {
    ham_size_t count;
    st = env->get_duplicate_manager()->get_count(entry->get_ptr(),
                    &count, &m_dupe_cache);
    if (st)
      return (st);
    m_duplicate_index = count - 1;
  }

  return (0);
}

void
BtreeCursor::couple_to_page(Page *page, ham_size_t index)
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
BtreeCursor::uncouple_all_cursors(Page *page, ham_size_t start)
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
