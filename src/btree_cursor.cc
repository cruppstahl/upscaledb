/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief btree cursors - implementation
 */

#include "config.h"

#include <string.h>

#include "blob.h"
#include "btree.h"
#include "db.h"
#include "env.h"
#include "error.h"
#include "btree_key.h"
#include "log.h"
#include "mem.h"
#include "page.h"
#include "txn.h"
#include "util.h"
#include "cursor.h"
#include "btree_node.h"
#include "btree_cursor.h"

namespace ham {

void
BtreeCursor::set_to_nil()
{
  Environment *env = get_db()->get_env();

  /* uncoupled cursor: free the cached pointer */
  if (is_uncoupled()) {
    ham_key_t *key = get_uncoupled_key();
    if (key->data)
      env->get_allocator()->free(key->data);
    env->get_allocator()->free(key);
    set_uncoupled_key(0);
  }
  /* coupled cursor: remove from page */
  else if (is_coupled())
    get_coupled_page()->remove_cursor(get_parent());

  ham_key_t *key = get_uncoupled_key();
  if (key) {
    if (key->data)
      env->get_allocator()->free(key->data);
    env->get_allocator()->free(key);
    set_uncoupled_key(0);
  }

  set_state(BtreeCursor::STATE_NIL);
  set_dupe_id(0);
  memset(get_dupe_cache(), 0, sizeof(dupe_entry_t));
}

bool
BtreeCursor::is_nil()
{
  if (is_uncoupled() || is_coupled())
    return (false);
  if (get_parent()->is_coupled_to_txnop())
    return (false);
  return (true);
}

ham_status_t
BtreeCursor::uncouple(ham_u32_t flags)
{
  ham_status_t st;
  Database *db = get_db();
  Environment *env = db->get_env();

  if (is_uncoupled() || is_nil())
    return (0);

  ham_assert(get_coupled_page() != 0);

  /* get the btree-entry of this key */
  BtreeNode *node = BtreeNode::from_page(get_coupled_page());
  ham_assert(node->is_leaf());
  BtreeKey *entry = node->get_key(db, get_coupled_index());

  /* copy the key */
  ham_key_t *key = get_uncoupled_key();
  if (!key) {
    key = (ham_key_t *)env->get_allocator()->calloc(sizeof(*key));
    if (!key)
      return (HAM_OUT_OF_MEMORY);
  }

  st = ((BtreeBackend *)db->get_backend())->copy_key(entry, key);
  if (st) {
    if (key->data)
      env->get_allocator()->free(key->data);
    env->get_allocator()->free(key);
    set_uncoupled_key(0);
    return (st);
  }

  /* uncouple the page */
  get_coupled_page()->remove_cursor(get_parent());

  /* set the flags and the uncoupled key */
  set_state(BtreeCursor::STATE_UNCOUPLED);
  set_uncoupled_key(key);

  return (0);
}

void
BtreeCursor::clone(BtreeCursor *other)
{
  Database *db = other->get_db();
  Environment *env = db->get_env();

  set_dupe_id(other->get_dupe_id());

  /* if the old cursor is coupled: couple the new cursor, too */
  if (other->is_coupled()) {
     Page *page = other->get_coupled_page();
     page->add_cursor(get_parent());
     couple_to(page, other->get_coupled_index());
  }
  /* otherwise, if the src cursor is uncoupled: copy the key */
  else if (other->is_uncoupled()) {
    ham_key_t *key = get_uncoupled_key();
    if (!key)
      key = (ham_key_t *)env->get_allocator()->calloc(sizeof(*key));
    else {
      env->get_allocator()->free(key->data);
      key->data = 0;
      key->size = 0;
    }

    get_db()->copy_key(other->get_uncoupled_key(), key);
    set_uncoupled_key(key);
  }
}

ham_status_t
BtreeCursor::overwrite(ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st;
  Database *db = get_db();
  Transaction *txn = get_parent()->get_txn();

  /* uncoupled cursor: couple it */
  if (is_uncoupled()) {
    st = couple();
    if (st)
      return (st);
  }
  else if (!is_coupled())
    return (HAM_CURSOR_IS_NIL);

  /* delete the cache of the current duplicate */
  memset(get_dupe_cache(), 0, sizeof(dupe_entry_t));

  Page *page = get_coupled_page();

  /* get the btree node entry */
  BtreeNode *node = BtreeNode::from_page(get_coupled_page());
  ham_assert(node->is_leaf());
  BtreeKey *key = node->get_key(db, get_coupled_index());

  /* copy the key flags, and remove all flags concerning the key size */
  st = key->set_record(db, txn, record, get_dupe_id(),
                    flags | HAM_OVERWRITE, 0);
  if (st)
    return (st);

  page->set_dirty(true);

  return (0);
}

ham_status_t
BtreeCursor::move(ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st = 0;
  Database *db = get_db();
  Environment *env = db->get_env();
  Transaction *txn = get_parent()->get_txn();
  BtreeBackend *be = (BtreeBackend *)db->get_backend();

  /* delete the cache of the current duplicate */
  memset(get_dupe_cache(), 0, sizeof(dupe_entry_t));

  if (flags & HAM_CURSOR_FIRST)
    st = move_first(flags);
  else if (flags & HAM_CURSOR_LAST)
    st = move_last(flags);
  else if (flags & HAM_CURSOR_NEXT)
    st = move_next(flags);
  else if (flags & HAM_CURSOR_PREVIOUS)
    st = move_previous(flags);
  /* no move, but cursor is nil? return error */
  else if (is_nil()) {
    if (key || record)
      return (HAM_CURSOR_IS_NIL);
    else
      return (0);
  }
  /* no move, but cursor is not coupled? couple it */
  else if (is_uncoupled())
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
  ham_assert(is_coupled());
  Page *page = get_coupled_page();
  BtreeNode *node = BtreeNode::from_page(page);
  ham_assert(node->is_leaf());
  BtreeKey *entry = node->get_key(db, get_coupled_index());

  if (key) {
    st = be->read_key(txn, entry, key);
    if (st)
      return (st);
  }

  if (record) {
    ham_u64_t *ridptr = 0;
    if (entry->get_flags() & BtreeKey::KEY_HAS_DUPLICATES && get_dupe_id()) {
      dupe_entry_t *e = get_dupe_cache();
      if (!dupe_entry_get_rid(e)) {
        st = env->get_duplicate_manager()->get(entry->get_ptr(), get_dupe_id(),
                                get_dupe_cache());
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
    st = be->read_record(txn, record, ridptr, flags);
    if (st)
      return (st);
  }

  return (0);
}

ham_status_t
BtreeCursor::find(ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
  ham_status_t st;
  BtreeBackend *be = (BtreeBackend *)get_db()->get_backend();
  Transaction *txn = get_parent()->get_txn();

  ham_assert(key);

  set_to_nil();

  st = be->do_find(txn, get_parent(), key, record, flags);
  if (st) {
    /* cursor is now NIL */
    return (st);
  }

  return (0);
}

ham_status_t
BtreeCursor::insert(ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
  BtreeBackend *be = (BtreeBackend *)get_db()->get_backend();
  Transaction *txn = get_parent()->get_txn();

  ham_assert(key);
  ham_assert(record);

  /* call the btree insert function */
  return (be->insert_cursor(txn, key, record, get_parent(), flags));
}

ham_status_t
BtreeCursor::erase(ham_u32_t flags)
{
  BtreeBackend *be = (BtreeBackend *)get_db()->get_backend();
  Transaction *txn = get_parent()->get_txn();

  if (!is_uncoupled() && !is_coupled())
    return (HAM_CURSOR_IS_NIL);

  return (be->erase_cursor(txn, 0, get_parent(), flags));
}

bool
BtreeCursor::points_to(BtreeKey *key)
{
  ham_status_t st;

  if (is_uncoupled()) {
    st = couple();
    if (st)
      return (false);
  }

  if (is_coupled()) {
    BtreeNode *node = BtreeNode::from_page(get_coupled_page());
    BtreeKey *entry = node->get_key(get_db(), get_coupled_index());

    if (entry == key)
      return (true);
  }

  return (false);
}

bool
BtreeCursor::points_to(ham_key_t *key)
{
  Cursor *parent = get_parent();
  Database *db = parent->get_db();
  bool ret = false;

  if (is_uncoupled()) {
    ham_key_t *k = get_uncoupled_key();
    if (k->size != key->size)
      return (false);
    return (0 == db->compare_keys(key, k));
  }

  if (is_coupled()) {
    Page *page = get_coupled_page();
    BtreeNode *node = BtreeNode::from_page(page);
    BtreeKey *entry = node->get_key(db, get_coupled_index());

    if (entry->get_size() != key->size)
      return (false);

    bool ret = false;
    Cursor *clone = db->cursor_clone(parent);
    ham_status_t st = clone->get_btree_cursor()->uncouple();
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
  Database *db = get_db();
  Environment *env = db->get_env();

  /* uncoupled cursor: couple it */
  if (is_uncoupled()) {
    st = couple();
    if (st)
      return (st);
  }
  else if (!(is_coupled()))
    return (HAM_CURSOR_IS_NIL);

  Page *page = get_coupled_page();
  BtreeNode *node = BtreeNode::from_page(page);
  BtreeKey *entry = node->get_key(db, get_coupled_index());

  if (!(entry->get_flags() & BtreeKey::KEY_HAS_DUPLICATES)) {
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
btree_uncouple_all_cursors(Page *page, ham_size_t start)
{
  ham_status_t st;
  bool skipped = false;
  Cursor *cursors = page->get_cursors();

  while (cursors) {
    BtreeCursor *btc = cursors->get_btree_cursor();
    Cursor *next = cursors->get_next_in_page();

    /*
     * ignore all cursors which are already uncoupled or which are
     * coupled to the txn
     */
    if (btc->is_coupled() || cursors->is_coupled_to_txnop()) {
      /* skip this cursor if its position is < start */
      if (btc->get_coupled_index()<start) {
        cursors = next;
        skipped = true;
        continue;
      }

      /* otherwise: uncouple it */
      st = btc->uncouple();
      if (st)
        return (st);
      cursors->set_next_in_page(0);
      cursors->set_previous_in_page(0);
    }

    cursors = next;
  }

  if (!skipped)
    page->set_cursors(0);

  return (0);
}

ham_status_t
BtreeCursor::get_duplicate_table(dupe_table_t **ptable, bool *needs_free)
{
  ham_status_t st;
  Database *db = get_db();
  Environment *env = db->get_env();

  *ptable = 0;

  /* uncoupled cursor: couple it */
  if (is_uncoupled()) {
    st = couple();
    if (st)
      return (st);
  }
  else if (!is_coupled())
    return (HAM_CURSOR_IS_NIL);

  Page *page = get_coupled_page();
  BtreeNode *node = BtreeNode::from_page(page);
  BtreeKey *entry = node->get_key(db, get_coupled_index());

  /* if key has no duplicates: return successfully, but with *ptable=0 */
  if (!(entry->get_flags() & BtreeKey::KEY_HAS_DUPLICATES)) {
    dupe_entry_t *e;
    dupe_table_t *t;
    t = (dupe_table_t *)env->get_allocator()->calloc(sizeof(*t));
    if (!t)
      return (HAM_OUT_OF_MEMORY);
    dupe_table_set_capacity(t, 1);
    dupe_table_set_count(t, 1);
    e = dupe_table_get_entry(t, 0);
    dupe_entry_set_flags(e, entry->get_flags());
    dupe_entry_set_rid(e, *entry->get_rawptr());
    *ptable = t;
    *needs_free = 1;
    return (0);
  }

  return (env->get_duplicate_manager()->get_table(entry->get_ptr(),
                    ptable, needs_free));
}

ham_status_t
BtreeCursor::get_record_size(ham_offset_t *size)
{
  ham_status_t st;
  Database *db = get_db();
  ham_u32_t keyflags = 0;
  ham_u64_t *ridptr = 0;
  ham_u64_t rid = 0;
  dupe_entry_t dupeentry;

  /*
   * uncoupled cursor: couple it
   */
  if (is_uncoupled()) {
    st = couple();
    if (st)
      return (st);
  }
  else if (!is_coupled())
    return (HAM_CURSOR_IS_NIL);

  Page *page = get_coupled_page();
  BtreeNode *node = BtreeNode::from_page(page);
  BtreeKey *entry = node->get_key(db, get_coupled_index());

  if (entry->get_flags() & BtreeKey::KEY_HAS_DUPLICATES) {
    st = db->get_env()->get_duplicate_manager()->get(entry->get_ptr(),
                    get_dupe_id(), &dupeentry);
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

  if (keyflags & BtreeKey::KEY_BLOB_SIZE_TINY) {
    /* the highest byte of the record id is the size of the blob */
    char *p = (char *)ridptr;
    *size = p[sizeof(ham_offset_t) - 1];
  }
  else if (keyflags & BtreeKey::KEY_BLOB_SIZE_SMALL) {
    /* record size is sizeof(ham_offset_t) */
    *size = sizeof(ham_offset_t);
  }
  else if (keyflags & BtreeKey::KEY_BLOB_SIZE_EMPTY) {
    /* record size is 0 */
    *size = 0;
  }
  else {
    st = db->get_env()->get_blob_manager()->get_datasize(db, rid, size);
    if (st)
      return (st);
  }

  return (0);
}

Database *
BtreeCursor::get_db()
{
  return (m_parent->get_db());
}

ham_status_t
BtreeCursor::couple()
{
  ham_key_t key = {0};
  Database *db = get_db();
  Environment *env = db->get_env();

  ham_assert(is_uncoupled());

  /*
   * make a 'find' on the cached key; if we succeed, the cursor
   * is automatically coupled
   *
   * the dupe ID is overwritten in BtreeCursor::find, therefore save it
   * and restore it afterwards
   */
  ham_status_t st = db->copy_key(get_uncoupled_key(), &key);
  if (st) {
    if (key.data)
      env->get_allocator()->free(key.data);
    return (st);
  }

  ham_u32_t dupe_id = get_dupe_id();
  st = find(&key, 0, 0);
  set_dupe_id(dupe_id);

  /* free the cached key */
  if (key.data)
    env->get_allocator()->free(key.data);

  return (st);
}

ham_status_t
BtreeCursor::move_first(ham_u32_t flags)
{
  ham_status_t st;
  Database *db = get_db();
  Page *page;
  BtreeBackend *be = (BtreeBackend *)db->get_backend();

  /* get a NIL cursor */
  set_to_nil();

  /* get the root page */
  if (!be->get_rootpage())
    return (HAM_KEY_NOT_FOUND);
  st = db_fetch_page(&page, db, be->get_rootpage(), 0);
  if (st)
    return (st);

  /*
   * while we've not reached the leaf: pick the smallest element
   * and traverse down
   */
  while (1) {
    BtreeNode *node = BtreeNode::from_page(page);
    /* check for an empty root page */
    if (node->get_count()==0)
      return HAM_KEY_NOT_FOUND;
    /* leave the loop when we've reached the leaf page */
    if (node->is_leaf())
      break;

    st=db_fetch_page(&page, db, node->get_ptr_left(), 0);
    if (st)
      return (st);
  }

  /* couple this cursor to the smallest key in this page */
  page->add_cursor(get_parent());
  couple_to(page, 0);
  set_dupe_id(0);

  return (0);
}

ham_status_t
BtreeCursor::move_next(ham_u32_t flags)
{
  ham_status_t st;
  Database *db = get_db();
  Environment *env = db->get_env();

  /* uncoupled cursor: couple it */
  if (is_uncoupled()) {
    st = couple();
    if (st)
      return (st);
  }
  else if (!is_coupled())
    return (HAM_CURSOR_IS_NIL);

  Page *page = get_coupled_page();
  BtreeNode *node = BtreeNode::from_page(page);
  BtreeKey *entry = node->get_key(db, get_coupled_index());

  /*
   * if this key has duplicates: get the next duplicate; otherwise
   * (and if there's no duplicate): fall through
   */
  if (entry->get_flags() & BtreeKey::KEY_HAS_DUPLICATES
      && (!(flags & HAM_SKIP_DUPLICATES))) {
    ham_status_t st;
    set_dupe_id(get_dupe_id() + 1);
    st = env->get_duplicate_manager()->get(entry->get_ptr(),
                    get_dupe_id(), get_dupe_cache());
    if (st) {
      set_dupe_id(get_dupe_id() - 1);
      if (st != HAM_KEY_NOT_FOUND)
        return (st);
    }
    else if (!st)
      return (0);
  }

  /* don't continue if ONLY_DUPLICATES is set */
  if (flags & HAM_ONLY_DUPLICATES)
    return (HAM_KEY_NOT_FOUND);

  /*
   * if the index+1 is still in the coupled page, just increment the
   * index
   */
  if (get_coupled_index() + 1 < node->get_count()) {
    couple_to(page, get_coupled_index() + 1);
    set_dupe_id(0);
    return (0);
  }

  /*
   * otherwise uncouple the cursor and load the right sibling page
   */
  if (!node->get_right())
    return (HAM_KEY_NOT_FOUND);

  page->remove_cursor(get_parent());

  st = db_fetch_page(&page, db, node->get_right(), 0);
  if (st)
    return (st);

  /* couple this cursor to the smallest key in this page */
  page->add_cursor(get_parent());
  couple_to(page, 0);
  set_dupe_id(0);

  return (0);
}

ham_status_t
BtreeCursor::move_previous(ham_u32_t flags)
{
  ham_status_t st;
  Database *db = get_db();
  Environment *env = db->get_env();

  /* uncoupled cursor: couple it */
  if (is_uncoupled()) {
    st = couple();
    if (st)
      return (st);
  }
  else if (!is_coupled())
    return (HAM_CURSOR_IS_NIL);

  Page *page = get_coupled_page();
  BtreeNode *node = BtreeNode::from_page(page);
  BtreeKey *entry = node->get_key(db, get_coupled_index());

  /*
   * if this key has duplicates: get the previous duplicate; otherwise
   * (and if there's no duplicate): fall through
   */
  if (entry->get_flags() & BtreeKey::KEY_HAS_DUPLICATES
      && (!(flags & HAM_SKIP_DUPLICATES))
      && get_dupe_id() > 0) {
    ham_status_t st;
    set_dupe_id(get_dupe_id() - 1);
    st = env->get_duplicate_manager()->get(entry->get_ptr(),
                    get_dupe_id(), get_dupe_cache());
    if (st) {
      set_dupe_id(get_dupe_id() + 1);
      if (st != HAM_KEY_NOT_FOUND)
        return (st);
    }
    else if (!st)
      return (0);
  }

  /* don't continue if ONLY_DUPLICATES is set */
  if (flags & HAM_ONLY_DUPLICATES)
    return (HAM_KEY_NOT_FOUND);

  /*
   * if the index-1 is till in the coupled page, just decrement the
   * index
   */
  if (get_coupled_index() != 0) {
    couple_to(page, get_coupled_index() - 1);
    entry = node->get_key(db, get_coupled_index());
  }
  /* otherwise load the left sibling page */
  else {
    if (!node->get_left())
      return (HAM_KEY_NOT_FOUND);

    page->remove_cursor(get_parent());

    st = db_fetch_page(&page, db, node->get_left(), 0);
    if (st)
      return (st);
    node = BtreeNode::from_page(page);

    /* couple this cursor to the highest key in this page */
    page->add_cursor(get_parent());
    couple_to(page, node->get_count() - 1);
    entry = node->get_key(db, get_coupled_index());
  }
  set_dupe_id(0);

  /* if duplicates are enabled: move to the end of the duplicate-list */
  if (entry->get_flags() & BtreeKey::KEY_HAS_DUPLICATES
      && !(flags & HAM_SKIP_DUPLICATES)) {
    ham_size_t count;
    ham_status_t st;
    st = env->get_duplicate_manager()->get_count(entry->get_ptr(),
                    &count, get_dupe_cache());
    if (st)
      return (st);
    set_dupe_id(count - 1);
  }

  return (0);
}

ham_status_t
BtreeCursor::move_last(ham_u32_t flags)
{
  Page *page;
  Database *db = get_db();
  BtreeNode *node;
  Environment *env = db->get_env();
  BtreeBackend *be = (BtreeBackend *)db->get_backend();

  /* get a NIL cursor */
  set_to_nil();

  /* get the root page */
  if (!be->get_rootpage())
    return (HAM_KEY_NOT_FOUND);
  ham_status_t st = db_fetch_page(&page, db, be->get_rootpage(), 0);
  if (st)
    return (st);
  /* hack: prior to 2.0, the type of btree root pages was not set
   * correctly */
  page->set_type(Page::TYPE_B_ROOT);

  /*
   * while we've not reached the leaf: pick the largest element
   * and traverse down
   */
  while (1) {
    node = BtreeNode::from_page(page);
    /* check for an empty root page */
    if (node->get_count() == 0)
      return (HAM_KEY_NOT_FOUND);
    /* leave the loop when we've reached a leaf page */
    if (node->is_leaf())
      break;

    BtreeKey *key = node->get_key(db, node->get_count() - 1);
    st = db_fetch_page(&page, db, key->get_ptr(), 0);
    if (st)
      return (st);
  }

  /* couple this cursor to the largest key in this page */
  page->add_cursor(get_parent());
  couple_to(page, node->get_count() - 1);
  BtreeKey *entry = node->get_key(db, get_coupled_index());
  set_dupe_id(0);

  /* if duplicates are enabled: move to the end of the duplicate-list */
  if (entry->get_flags() & BtreeKey::KEY_HAS_DUPLICATES
      && !(flags & HAM_SKIP_DUPLICATES)) {
    ham_size_t count;
    ham_status_t st;
    st = env->get_duplicate_manager()->get_count(entry->get_ptr(),
                    &count, get_dupe_cache());
    if (st)
      return (st);
    set_dupe_id(count - 1);
  }

  return (0);
}

} // namespace ham
