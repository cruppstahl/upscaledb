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
#include "btree_cursor.h"
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

namespace ham {

static ham_status_t
btree_cursor_couple(BtreeCursor *c)
{
    ham_key_t key;
    ham_status_t st;
    Database *db=c->get_db();
    Environment *env = db->get_env();
    ham_u32_t dupe_id;

    ham_assert(c->is_uncoupled());

    /*
     * make a 'find' on the cached key; if we succeed, the cursor
     * is automatically coupled
     *
     * the dupe ID is overwritten in btree_cursor_find, therefore save it
     * and restore it afterwards
     */
    memset(&key, 0, sizeof(key));

    st=db->copy_key(c->get_uncoupled_key(), &key);
    if (st) {
        if (key.data)
            env->get_allocator()->free(key.data);
        return (st);
    }

    dupe_id=c->get_dupe_id();
    st=btree_cursor_find(c, &key, NULL, 0);
    c->set_dupe_id(dupe_id);

    /* free the cached key */
    if (key.data)
        env->get_allocator()->free(key.data);

    return (st);
}

static ham_status_t
__move_first(BtreeBackend *be, BtreeCursor *c, ham_u32_t flags)
{
    ham_status_t st;
    Page *page;
    BtreeNode *node;
    Database *db=c->get_db();

    /* get a NIL cursor */
    c->set_to_nil();

    /* get the root page */
    if (!be->get_rootpage())
        return (HAM_KEY_NOT_FOUND);
    st=db_fetch_page(&page, db, be->get_rootpage(), 0);
    if (st)
        return (st);

    /*
     * while we've not reached the leaf: pick the smallest element
     * and traverse down
     */
    while (1) {
        node=BtreeNode::from_page(page);
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
    page->add_cursor(c->get_parent());
    c->couple_to(page, 0);
    c->set_dupe_id(0);

    return (0);
}

static ham_status_t
__move_next(BtreeBackend *be, BtreeCursor *c, ham_u32_t flags)
{
    ham_status_t st;
    Page *page;
    BtreeNode *node;
    Database *db=c->get_db();
    Environment *env = db->get_env();
    BtreeKey *entry;

    /* uncoupled cursor: couple it */
    if (c->is_uncoupled()) {
        st=btree_cursor_couple(c);
        if (st)
            return (st);
    }
    else if (!c->is_coupled())
        return (HAM_CURSOR_IS_NIL);

    page=c->get_coupled_page();
    node=BtreeNode::from_page(page);
    entry=node->get_key(db, c->get_coupled_index());

    /*
     * if this key has duplicates: get the next duplicate; otherwise
     * (and if there's no duplicate): fall through
     */
    if (entry->get_flags()&BtreeKey::KEY_HAS_DUPLICATES
            && (!(flags&HAM_SKIP_DUPLICATES))) {
        ham_status_t st;
        c->set_dupe_id(c->get_dupe_id()+1);
        st=env->get_duplicate_manager()->get(entry->get_ptr(),
                        c->get_dupe_id(),
                        c->get_dupe_cache());
        if (st) {
            c->set_dupe_id(c->get_dupe_id()-1);
            if (st!=HAM_KEY_NOT_FOUND)
                return (st);
        }
        else if (!st)
            return (0);
    }

    /* don't continue if ONLY_DUPLICATES is set */
    if (flags&HAM_ONLY_DUPLICATES)
        return (HAM_KEY_NOT_FOUND);

    /*
     * if the index+1 is still in the coupled page, just increment the
     * index
     */
    if (c->get_coupled_index()+1<node->get_count()) {
        c->couple_to(page, c->get_coupled_index()+1);
        c->set_dupe_id(0);
        return (HAM_SUCCESS);
    }

    /*
     * otherwise uncouple the cursor and load the right sibling page
     */
    if (!node->get_right())
        return (HAM_KEY_NOT_FOUND);

    page->remove_cursor(c->get_parent());

    st=db_fetch_page(&page, db, node->get_right(), 0);
    if (st)
        return (st);

    /* couple this cursor to the smallest key in this page */
    page->add_cursor(c->get_parent());
    c->couple_to(page, 0);
    c->set_dupe_id(0);

    return (HAM_SUCCESS);
}

static ham_status_t
__move_previous(BtreeBackend *be, BtreeCursor *c, ham_u32_t flags)
{
    ham_status_t st;
    Page *page;
    BtreeNode *node;
    Database *db=c->get_db();
    Environment *env = db->get_env();
    BtreeKey *entry;

    /* uncoupled cursor: couple it */
    if (c->is_uncoupled()) {
        st=btree_cursor_couple(c);
        if (st)
            return (st);
    }
    else if (!c->is_coupled())
        return (HAM_CURSOR_IS_NIL);

    page=c->get_coupled_page();
    node=BtreeNode::from_page(page);
    entry=node->get_key(db, c->get_coupled_index());

    /*
     * if this key has duplicates: get the previous duplicate; otherwise
     * (and if there's no duplicate): fall through
     */
    if (entry->get_flags()&BtreeKey::KEY_HAS_DUPLICATES
            && (!(flags&HAM_SKIP_DUPLICATES))
            && c->get_dupe_id()>0) {
        ham_status_t st;
        c->set_dupe_id(c->get_dupe_id()-1);
        st=env->get_duplicate_manager()->get(entry->get_ptr(),
                        c->get_dupe_id(),
                        c->get_dupe_cache());
        if (st) {
            c->set_dupe_id(c->get_dupe_id()+1);
            if (st!=HAM_KEY_NOT_FOUND)
                return (st);
        }
        else if (!st)
            return (0);
    }

    /* don't continue if ONLY_DUPLICATES is set */
    if (flags&HAM_ONLY_DUPLICATES)
        return (HAM_KEY_NOT_FOUND);

    /*
     * if the index-1 is till in the coupled page, just decrement the
     * index
     */
    if (c->get_coupled_index()!=0) {
        c->couple_to(page, c->get_coupled_index()-1);
        entry=node->get_key(db, c->get_coupled_index());
    }
    /*
     * otherwise load the left sibling page
     */
    else {
        if (!node->get_left())
            return (HAM_KEY_NOT_FOUND);

        page->remove_cursor(c->get_parent());

        st=db_fetch_page(&page, db, node->get_left(), 0);
        if (st)
            return (st);
        node=BtreeNode::from_page(page);

        /* couple this cursor to the highest key in this page */
        page->add_cursor(c->get_parent());
        c->couple_to(page, node->get_count()-1);
        entry=node->get_key(db, c->get_coupled_index());
    }
    c->set_dupe_id(0);

    /* if duplicates are enabled: move to the end of the duplicate-list */
    if (entry->get_flags()&BtreeKey::KEY_HAS_DUPLICATES
            && !(flags&HAM_SKIP_DUPLICATES)) {
        ham_size_t count;
        ham_status_t st;
        st=env->get_duplicate_manager()->get_count(entry->get_ptr(),
                        &count, c->get_dupe_cache());
        if (st)
            return st;
        c->set_dupe_id(count-1);
    }

    return (0);
}

static ham_status_t
__move_last(BtreeBackend *be, BtreeCursor *c, ham_u32_t flags)
{
    ham_status_t st;
    Page *page;
    BtreeNode *node;
    BtreeKey *entry;
    Database *db=c->get_db();
    Environment *env = db->get_env();

    /* get a NIL cursor */
    c->set_to_nil();

    /* get the root page */
    if (!be->get_rootpage())
        return HAM_KEY_NOT_FOUND;
    st=db_fetch_page(&page, db, be->get_rootpage(), 0);
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
        BtreeKey *key;

        node=BtreeNode::from_page(page);
        /* check for an empty root page */
        if (node->get_count()==0)
            return HAM_KEY_NOT_FOUND;
        /* leave the loop when we've reached a leaf page */
        if (node->is_leaf())
            break;

        key=node->get_key(db, node->get_count()-1);

        st=db_fetch_page(&page, db, key->get_ptr(), 0);
        if (st)
            return (st);
    }

    /* couple this cursor to the largest key in this page */
    page->add_cursor(c->get_parent());
    c->couple_to(page, node->get_count()-1);
    entry=node->get_key(db, c->get_coupled_index());
    c->set_dupe_id(0);

    /* if duplicates are enabled: move to the end of the duplicate-list */
    if (entry->get_flags()&BtreeKey::KEY_HAS_DUPLICATES
            && !(flags&HAM_SKIP_DUPLICATES)) {
        ham_size_t count;
        ham_status_t st;
        st=env->get_duplicate_manager()->get_count(entry->get_ptr(),
                        &count, c->get_dupe_cache());
        if (st)
            return (st);
        c->set_dupe_id(count-1);
    }

    return (0);
}

void
BtreeCursor::set_to_nil()
{
    Environment *env = get_db()->get_env();

    /* uncoupled cursor: free the cached pointer */
    if (is_uncoupled()) {
        ham_key_t *key=get_uncoupled_key();
        if (key->data)
            env->get_allocator()->free(key->data);
        env->get_allocator()->free(key);
        set_uncoupled_key(0);
    }
    /* coupled cursor: remove from page */
    else if (is_coupled()) {
        get_coupled_page()->remove_cursor(get_parent());
    }

    set_state(BtreeCursor::STATE_NIL);
    set_dupe_id(0);
    memset(get_dupe_cache(), 0, sizeof(dupe_entry_t));
}

void
btree_cursor_couple_to_other(BtreeCursor *cu, BtreeCursor *other)
{
    ham_assert(other->is_coupled());
    cu->set_to_nil();

    cu->couple_to(other->get_coupled_page(), other->get_coupled_index());
    cu->set_dupe_id(other->get_dupe_id());
    //cu->set_flags(other->get_flags());
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
btree_cursor_uncouple(BtreeCursor *c, ham_u32_t flags)
{
    ham_status_t st;
    BtreeNode *node;
    BtreeKey *entry;
    ham_key_t *key;
    Database *db=c->get_db();
    Environment *env=db->get_env();

    if (c->is_uncoupled() || c->is_nil())
        return (0);

    ham_assert(c->get_coupled_page()!=0);

    /* get the btree-entry of this key */
    node=BtreeNode::from_page(c->get_coupled_page());
    ham_assert(node->is_leaf());
    entry=node->get_key(db, c->get_coupled_index());

    /* copy the key */
    key=(ham_key_t *)env->get_allocator()->calloc(sizeof(*key));
    if (!key)
        return (HAM_OUT_OF_MEMORY);
    st=((BtreeBackend *)db->get_backend())->copy_key(entry, key);
    if (st) {
        if (key->data)
            env->get_allocator()->free(key->data);
        env->get_allocator()->free(key);
        return (st);
    }

    /* uncouple the page */
    if (!(flags&BTREE_CURSOR_UNCOUPLE_NO_REMOVE))
        c->get_coupled_page()->remove_cursor(c->get_parent());

    /* set the flags and the uncoupled key */
    c->set_state(BtreeCursor::STATE_UNCOUPLED);
    c->set_uncoupled_key(key);

    return (0);
}

void
BtreeCursor::clone(BtreeCursor *other)
{
    Database *db=other->get_db();
    Environment *env=db->get_env();

    set_dupe_id(other->get_dupe_id());

    /* if the old cursor is coupled: couple the new cursor, too */
    if (other->is_coupled()) {
         Page *page=other->get_coupled_page();
         page->add_cursor(get_parent());
         couple_to(page, other->get_coupled_index());
    }
    /* otherwise, if the src cursor is uncoupled: copy the key */
    else if (other->is_uncoupled()) {
        ham_key_t *key;
        key=(ham_key_t *)env->get_allocator()->calloc(sizeof(*key));

        get_db()->copy_key(other->get_uncoupled_key(), key);
        set_uncoupled_key(key);
    }
}

void
btree_cursor_close(BtreeCursor *c)
{
    c->set_to_nil();
}

ham_status_t
btree_cursor_overwrite(BtreeCursor *c, ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    BtreeNode *node;
    BtreeKey *key;
    Database *db=c->get_db();
    Transaction *txn=c->get_parent()->get_txn();
    Page *page;

    /* uncoupled cursor: couple it */
    if (c->is_uncoupled()) {
        st=btree_cursor_couple(c);
        if (st)
            return (st);
    }
    else if (!c->is_coupled())
        return (HAM_CURSOR_IS_NIL);

    /* delete the cache of the current duplicate */
    memset(c->get_dupe_cache(), 0, sizeof(dupe_entry_t));

    page=c->get_coupled_page();

    /* get the btree node entry */
    node=BtreeNode::from_page(c->get_coupled_page());
    ham_assert(node->is_leaf());
    key=node->get_key(db, c->get_coupled_index());

    /* copy the key flags, and remove all flags concerning the key size */
    st=key->set_record(db, txn, record,
            c->get_dupe_id(), flags|HAM_OVERWRITE, 0);
    if (st)
        return (st);

    page->set_dirty(true);

    return (0);
}

ham_status_t
btree_cursor_move(BtreeCursor *c, ham_key_t *key,
                ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st=0;
    Page *page;
    BtreeNode *node;
    Database *db=c->get_db();
    Environment *env=db->get_env();
    Transaction *txn=c->get_parent()->get_txn();
    BtreeBackend *be=(BtreeBackend *)db->get_backend();
    BtreeKey *entry;

    if (!be)
        return (HAM_NOT_INITIALIZED);

    /* delete the cache of the current duplicate */
    memset(c->get_dupe_cache(), 0, sizeof(dupe_entry_t));

    if (flags&HAM_CURSOR_FIRST)
        st=__move_first(be, c, flags);
    else if (flags&HAM_CURSOR_LAST)
        st=__move_last(be, c, flags);
    else if (flags&HAM_CURSOR_NEXT)
        st=__move_next(be, c, flags);
    else if (flags&HAM_CURSOR_PREVIOUS)
        st=__move_previous(be, c, flags);
    /* no move, but cursor is nil? return error */
    else if (c->is_nil()) {
        if (key || record)
            return (HAM_CURSOR_IS_NIL);
        else
            return (0);
    }
    /* no move, but cursor is not coupled? couple it */
    else if (c->is_uncoupled())
        st=btree_cursor_couple(c);

    if (st)
        return (st);

    /*
     * during read_key() and read_record() new pages might be needed,
     * and the page at which we're pointing could be moved out of memory;
     * that would mean that the cursor would be uncoupled, and we're losing
     * the 'entry'-pointer. therefore we 'lock' the page by incrementing
     * the reference counter
     */
    ham_assert(c->is_coupled());
    page=c->get_coupled_page();
    node=BtreeNode::from_page(page);
    ham_assert(node->is_leaf());
    entry=node->get_key(db, c->get_coupled_index());

    if (key) {
        st=be->read_key(txn, entry, key);
        if (st)
            return (st);
    }

    if (record) {
        ham_u64_t *ridptr=0;
        if (entry->get_flags()&BtreeKey::KEY_HAS_DUPLICATES
                && c->get_dupe_id()) {
            dupe_entry_t *e=c->get_dupe_cache();
            if (!dupe_entry_get_rid(e)) {
                st=env->get_duplicate_manager()->get(entry->get_ptr(),
                        c->get_dupe_id(), c->get_dupe_cache());
                if (st)
                    return st;
            }
            record->_intflags=dupe_entry_get_flags(e);
            record->_rid=dupe_entry_get_rid(e);
            ridptr=(ham_u64_t *)&dupe_entry_get_ridptr(e);
        }
        else {
            record->_intflags=entry->get_flags();
            record->_rid=entry->get_ptr();
            ridptr=entry->get_rawptr();
        }
        st=be->read_record(txn, record, ridptr, flags);
        if (st)
            return (st);
    }

    return (0);
}

ham_status_t
btree_cursor_find(BtreeCursor *c, ham_key_t *key, ham_record_t *record,
                ham_u32_t flags)
{
    ham_status_t st;
    BtreeBackend *be=(BtreeBackend *)c->get_db()->get_backend();
    Transaction *txn=c->get_parent()->get_txn();

    if (!be)
        return (HAM_NOT_INITIALIZED);
    ham_assert(key);

    c->set_to_nil();

    st=be->do_find(txn, c->get_parent(), key, record, flags);
    if (st) {
        /* cursor is now NIL */
        return (st);
    }

    return (0);
}

ham_status_t
btree_cursor_insert(BtreeCursor *c, ham_key_t *key,
                ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    Database *db=c->get_db();
    BtreeBackend *be=(BtreeBackend *)db->get_backend();
    Transaction *txn=c->get_parent()->get_txn();

    if (!be)
        return (HAM_NOT_INITIALIZED);
    ham_assert(key);
    ham_assert(record);

    /* call the btree insert function */
    st=be->insert_cursor(txn, key, record, c->get_parent(), flags);
    if (st)
        return (st);

    return (0);
}

ham_status_t
btree_cursor_erase(BtreeCursor *c, ham_u32_t flags)
{
    Database *db=c->get_db();
    BtreeBackend *be=(BtreeBackend *)db->get_backend();
    Transaction *txn=c->get_parent()->get_txn();

    if (!be)
        return (HAM_NOT_INITIALIZED);

    if (!c->is_uncoupled() && !c->is_coupled())
        return (HAM_CURSOR_IS_NIL);

    return (be->erase_cursor(txn, 0, c->get_parent(), flags));
}

bool
btree_cursor_points_to(BtreeCursor *cursor, BtreeKey *key)
{
    ham_status_t st;
    Database *db=cursor->get_db();

    if (cursor->is_uncoupled()) {
        st=btree_cursor_couple(cursor);
        if (st)
            return (false);
    }

    if (cursor->is_coupled()) {
        Page *page=cursor->get_coupled_page();
        BtreeNode *node=BtreeNode::from_page(page);
        BtreeKey *entry=node->get_key(db, cursor->get_coupled_index());

        if (entry==key)
            return (true);
    }

    return (false);
}

bool
btree_cursor_points_to_key(BtreeCursor *btc, ham_key_t *key)
{
    Cursor *c=btc->get_parent();
    Database *db=c->get_db();
    bool ret=false;

    if (btc->is_uncoupled()) {
        ham_key_t *k=btc->get_uncoupled_key();
        if (k->size!=key->size)
            return (false);
        return (0==db->compare_keys(key, k));
    }

    if (btc->is_coupled()) {
        Page *page=btc->get_coupled_page();
        BtreeNode *node=BtreeNode::from_page(page);
        BtreeKey *entry=node->get_key(db, btc->get_coupled_index());

        if (entry->get_size()!=key->size)
            return (false);

        bool ret=false;
        Cursor *clone=0;
        db->clone_cursor(c, &clone);
        ham_status_t st=btree_cursor_uncouple(clone->get_btree_cursor(), 0);
        if (st) {
            db->close_cursor(clone);
            return (false);
        }
        if (0==db->compare_keys(key,
               clone->get_btree_cursor()->get_uncoupled_key()))
            ret=true;
        db->close_cursor(clone);
        return (ret);
    }

    else {
        ham_assert(!"shouldn't be here");
    }

    return (ret);
}


ham_status_t
btree_cursor_get_duplicate_count(BtreeCursor *c,
                ham_size_t *count, ham_u32_t flags)
{
    ham_status_t st;
    Database *db=c->get_db();
    Environment *env = db->get_env();
    BtreeBackend *be=(BtreeBackend *)db->get_backend();
    Page *page;
    BtreeNode *node;
    BtreeKey *entry;

    if (!be)
        return (HAM_NOT_INITIALIZED);

    /* uncoupled cursor: couple it */
    if (c->is_uncoupled()) {
        st=btree_cursor_couple(c);
        if (st)
            return (st);
    }
    else if (!(c->is_coupled()))
        return (HAM_CURSOR_IS_NIL);

    page=c->get_coupled_page();
    node=BtreeNode::from_page(page);
    entry=node->get_key(db, c->get_coupled_index());

    if (!(entry->get_flags()&BtreeKey::KEY_HAS_DUPLICATES)) {
        *count=1;
    }
    else {
        st=env->get_duplicate_manager()->get_count(entry->get_ptr(), count, 0);
        if (st)
            return (st);
    }

    return (0);
}

ham_status_t
btree_uncouple_all_cursors(Page *page, ham_size_t start)
{
    ham_status_t st;
    ham_bool_t skipped=HAM_FALSE;
    Cursor *n;
    Cursor *c=page->get_cursors();

    while (c) {
        BtreeCursor *btc=c->get_btree_cursor();
        n=c->get_next_in_page();

        /*
         * ignore all cursors which are already uncoupled or which are
         * coupled to the txn
         */
        if (btc->is_coupled() || c->is_coupled_to_txnop()) {
            /* skip this cursor if its position is < start */
            if (btc->get_coupled_index()<start) {
                c=n;
                skipped=HAM_TRUE;
                continue;
            }

            /* otherwise: uncouple it */
            st=btree_cursor_uncouple(btc, 0);
            if (st)
                return (st);
            c->set_next_in_page(0);
            c->set_previous_in_page(0);
        }

        c=n;
    }

    if (!skipped)
        page->set_cursors(0);

    return (0);
}

ham_status_t
btree_cursor_get_duplicate_table(BtreeCursor *c, dupe_table_t **ptable,
                bool *needs_free)
{
    ham_status_t st;
    Page *page;
    BtreeNode *node;
    BtreeKey *entry;
    Database *db=c->get_db();
    Environment *env = db->get_env();

    *ptable=0;

    /* uncoupled cursor: couple it */
    if (c->is_uncoupled()) {
        st=btree_cursor_couple(c);
        if (st)
            return (st);
    }
    else if (!c->is_coupled())
        return (HAM_CURSOR_IS_NIL);

    page=c->get_coupled_page();
    node=BtreeNode::from_page(page);
    entry=node->get_key(db, c->get_coupled_index());

    /* if key has no duplicates: return successfully, but with *ptable=0 */
    if (!(entry->get_flags()&BtreeKey::KEY_HAS_DUPLICATES)) {
        dupe_entry_t *e;
        dupe_table_t *t;
        t=(dupe_table_t *)env->get_allocator()->calloc(sizeof(*t));
        if (!t)
            return (HAM_OUT_OF_MEMORY);
        dupe_table_set_capacity(t, 1);
        dupe_table_set_count(t, 1);
        e=dupe_table_get_entry(t, 0);
        dupe_entry_set_flags(e, entry->get_flags());
        dupe_entry_set_rid(e, *entry->get_rawptr());
        *ptable=t;
        *needs_free=1;
        return (0);
    }

    return (env->get_duplicate_manager()->get_table(entry->get_ptr(),
                    ptable, needs_free));
}

ham_status_t
btree_cursor_get_record_size(BtreeCursor *c, ham_offset_t *size)
{
    ham_status_t st;
    Database *db=c->get_db();
    BtreeBackend *be=(BtreeBackend *)db->get_backend();
    Page *page;
    BtreeNode *node;
    BtreeKey *entry;
    ham_u32_t keyflags=0;
    ham_u64_t *ridptr=0;
    ham_u64_t rid=0;
    dupe_entry_t dupeentry;

    if (!be)
        return (HAM_NOT_INITIALIZED);

    /*
     * uncoupled cursor: couple it
     */
    if (c->is_uncoupled()) {
        st=btree_cursor_couple(c);
        if (st)
            return (st);
    }
    else if (!c->is_coupled())
        return (HAM_CURSOR_IS_NIL);

    page=c->get_coupled_page();
    node=BtreeNode::from_page(page);
    entry=node->get_key(db, c->get_coupled_index());

    if (entry->get_flags()&BtreeKey::KEY_HAS_DUPLICATES) {
        st=db->get_env()->get_duplicate_manager()->get(entry->get_ptr(),
                        c->get_dupe_id(), &dupeentry);
        if (st)
            return st;
        keyflags=dupe_entry_get_flags(&dupeentry);
        ridptr=&dupeentry._rid;
        rid=dupeentry._rid;
    }
    else {
        keyflags=entry->get_flags();
        ridptr=entry->get_rawptr();
        rid=entry->get_ptr();
    }

    if (keyflags&BtreeKey::KEY_BLOB_SIZE_TINY) {
        /* the highest byte of the record id is the size of the blob */
        char *p=(char *)ridptr;
        *size=p[sizeof(ham_offset_t)-1];
    }
    else if (keyflags&BtreeKey::KEY_BLOB_SIZE_SMALL) {
        /* record size is sizeof(ham_offset_t) */
        *size=sizeof(ham_offset_t);
    }
    else if (keyflags&BtreeKey::KEY_BLOB_SIZE_EMPTY) {
        /* record size is 0 */
        *size=0;
    }
    else {
        st=db->get_env()->get_blob_manager()->get_datasize(db, rid, size);
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

} // namespace ham
