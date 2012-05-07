/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
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

static ham_status_t
btree_cursor_couple(btree_cursor_t *c)
{
    ham_key_t key;
    ham_status_t st;
    Database *db=btree_cursor_get_db(c);
    Environment *env = db->get_env();
    ham_u32_t dupe_id;

    ham_assert(btree_cursor_is_uncoupled(c),
            ("coupling a cursor which is not uncoupled"));

    /*
     * make a 'find' on the cached key; if we succeed, the cursor
     * is automatically coupled
     *
     * the dupe ID is overwritten in btree_cursor_find, therefore save it
     * and restore it afterwards
     */
    memset(&key, 0, sizeof(key));

    st=db->copy_key(btree_cursor_get_uncoupled_key(c), &key);
    if (st) {
        if (key.data)
            env->get_allocator()->free(key.data);
        return (st);
    }

    dupe_id=btree_cursor_get_dupe_id(c);
    st=btree_cursor_find(c, &key, NULL, 0);
    btree_cursor_set_dupe_id(c, dupe_id);

    /* free the cached key */
    if (key.data)
        env->get_allocator()->free(key.data);

    return (st);
}

static ham_status_t
__move_first(BtreeBackend *be, btree_cursor_t *c, ham_u32_t flags)
{
    ham_status_t st;
    Page *page;
    btree_node_t *node;
    Database *db=btree_cursor_get_db(c);

    /* get a NIL cursor */
    btree_cursor_set_to_nil(c);

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
        node=page_get_btree_node(page);
        /* check for an empty root page */
        if (btree_node_get_count(node)==0)
            return HAM_KEY_NOT_FOUND;
        /* leave the loop when we've reached the leaf page */
        if (btree_node_is_leaf(node))
            break;

        st=db_fetch_page(&page, db, btree_node_get_ptr_left(node), 0);
        if (st)
            return (st);
    }

    /* couple this cursor to the smallest key in this page */
    page->add_cursor(btree_cursor_get_parent(c));
    btree_cursor_set_coupled_page(c, page);
    btree_cursor_set_coupled_index(c, 0);
    btree_cursor_set_flags(c,
            btree_cursor_get_flags(c)|BTREE_CURSOR_FLAG_COUPLED);
    btree_cursor_set_dupe_id(c, 0);

    return (0);
}

static ham_status_t
__move_next(BtreeBackend *be, btree_cursor_t *c, ham_u32_t flags)
{
    ham_status_t st;
    Page *page;
    btree_node_t *node;
    Database *db=btree_cursor_get_db(c);
    Environment *env = db->get_env();
    btree_key_t *entry;

    /* uncoupled cursor: couple it */
    if (btree_cursor_is_uncoupled(c)) {
        st=btree_cursor_couple(c);
        if (st)
            return (st);
    }
    else if (!btree_cursor_is_coupled(c))
        return (HAM_CURSOR_IS_NIL);

    page=btree_cursor_get_coupled_page(c);
    node=page_get_btree_node(page);
    entry=btree_node_get_key(db, node, btree_cursor_get_coupled_index(c));

    /*
     * if this key has duplicates: get the next duplicate; otherwise
     * (and if there's no duplicate): fall through
     */
    if (key_get_flags(entry)&KEY_HAS_DUPLICATES
            && (!(flags&HAM_SKIP_DUPLICATES))) {
        ham_status_t st;
        btree_cursor_set_dupe_id(c, btree_cursor_get_dupe_id(c)+1);
        st=blob_duplicate_get(env, key_get_ptr(entry),
                        btree_cursor_get_dupe_id(c),
                        btree_cursor_get_dupe_cache(c));
        if (st) {
            btree_cursor_set_dupe_id(c, btree_cursor_get_dupe_id(c)-1);
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
    if (btree_cursor_get_coupled_index(c)+1<btree_node_get_count(node)) {
        btree_cursor_set_coupled_index(c, btree_cursor_get_coupled_index(c)+1);
        btree_cursor_set_dupe_id(c, 0);
        return (HAM_SUCCESS);
    }

    /*
     * otherwise uncouple the cursor and load the right sibling page
     */
    if (!btree_node_get_right(node))
        return (HAM_KEY_NOT_FOUND);

    page->remove_cursor(btree_cursor_get_parent(c));
    btree_cursor_set_flags(c,
                    btree_cursor_get_flags(c)&(~BTREE_CURSOR_FLAG_COUPLED));

    st=db_fetch_page(&page, db, btree_node_get_right(node), 0);
    if (st)
        return (st);

    /* couple this cursor to the smallest key in this page */
    page->add_cursor(btree_cursor_get_parent(c));
    btree_cursor_set_coupled_page(c, page);
    btree_cursor_set_coupled_index(c, 0);
    btree_cursor_set_flags(c,
            btree_cursor_get_flags(c)|BTREE_CURSOR_FLAG_COUPLED);
    btree_cursor_set_dupe_id(c, 0);

    return (HAM_SUCCESS);
}

static ham_status_t
__move_previous(BtreeBackend *be, btree_cursor_t *c, ham_u32_t flags)
{
    ham_status_t st;
    Page *page;
    btree_node_t *node;
    Database *db=btree_cursor_get_db(c);
    Environment *env = db->get_env();
    btree_key_t *entry;

    /* uncoupled cursor: couple it */
    if (btree_cursor_is_uncoupled(c)) {
        st=btree_cursor_couple(c);
        if (st)
            return (st);
    }
    else if (!btree_cursor_is_coupled(c))
        return (HAM_CURSOR_IS_NIL);

    page=btree_cursor_get_coupled_page(c);
    node=page_get_btree_node(page);
    entry=btree_node_get_key(db, node, btree_cursor_get_coupled_index(c));

    /*
     * if this key has duplicates: get the previous duplicate; otherwise
     * (and if there's no duplicate): fall through
     */
    if (key_get_flags(entry)&KEY_HAS_DUPLICATES
            && (!(flags&HAM_SKIP_DUPLICATES))
            && btree_cursor_get_dupe_id(c)>0) {
        ham_status_t st;
        btree_cursor_set_dupe_id(c, btree_cursor_get_dupe_id(c)-1);
        st=blob_duplicate_get(env, key_get_ptr(entry),
                        btree_cursor_get_dupe_id(c),
                        btree_cursor_get_dupe_cache(c));
        if (st) {
            btree_cursor_set_dupe_id(c, btree_cursor_get_dupe_id(c)+1);
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
    if (btree_cursor_get_coupled_index(c)!=0) {
        btree_cursor_set_coupled_index(c, btree_cursor_get_coupled_index(c)-1);
        entry=btree_node_get_key(db, node, btree_cursor_get_coupled_index(c));
    }
    /*
     * otherwise load the left sibling page
     */
    else {
        if (!btree_node_get_left(node))
            return (HAM_KEY_NOT_FOUND);

        page->remove_cursor(btree_cursor_get_parent(c));
        btree_cursor_set_flags(c,
                btree_cursor_get_flags(c)&(~BTREE_CURSOR_FLAG_COUPLED));

        st=db_fetch_page(&page, db, btree_node_get_left(node), 0);
        if (st)
            return (st);
        node=page_get_btree_node(page);

        /* couple this cursor to the highest key in this page */
        page->add_cursor(btree_cursor_get_parent(c));
        btree_cursor_set_coupled_page(c, page);
        btree_cursor_set_coupled_index(c, btree_node_get_count(node)-1);
        btree_cursor_set_flags(c,
                btree_cursor_get_flags(c)|BTREE_CURSOR_FLAG_COUPLED);
        entry=btree_node_get_key(db, node, btree_cursor_get_coupled_index(c));
    }
    btree_cursor_set_dupe_id(c, 0);

    /* if duplicates are enabled: move to the end of the duplicate-list */
    if (key_get_flags(entry)&KEY_HAS_DUPLICATES
            && !(flags&HAM_SKIP_DUPLICATES)) {
        ham_size_t count;
        ham_status_t st;
        st=blob_duplicate_get_count(env, key_get_ptr(entry),
                        &count, btree_cursor_get_dupe_cache(c));
        if (st)
            return st;
        btree_cursor_set_dupe_id(c, count-1);
    }

    return (0);
}

static ham_status_t
__move_last(BtreeBackend *be, btree_cursor_t *c, ham_u32_t flags)
{
    ham_status_t st;
    Page *page;
    btree_node_t *node;
    btree_key_t *entry;
    Database *db=btree_cursor_get_db(c);
    Environment *env = db->get_env();

    /* get a NIL cursor */
    st=btree_cursor_set_to_nil(c);
    if (st)
        return (st);

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
        btree_key_t *key;

        node=page_get_btree_node(page);
        /* check for an empty root page */
        if (btree_node_get_count(node)==0)
            return HAM_KEY_NOT_FOUND;
        /* leave the loop when we've reached a leaf page */
        if (btree_node_is_leaf(node))
            break;

        key=btree_node_get_key(db, node, btree_node_get_count(node)-1);

        st=db_fetch_page(&page, db, key_get_ptr(key), 0);
        if (st)
            return (st);
    }

    /* couple this cursor to the largest key in this page */
    page->add_cursor(btree_cursor_get_parent(c));
    btree_cursor_set_coupled_page(c, page);
    btree_cursor_set_coupled_index(c, btree_node_get_count(node)-1);
    btree_cursor_set_flags(c,
            btree_cursor_get_flags(c)|BTREE_CURSOR_FLAG_COUPLED);
    entry=btree_node_get_key(db, node, btree_cursor_get_coupled_index(c));
    btree_cursor_set_dupe_id(c, 0);

    /* if duplicates are enabled: move to the end of the duplicate-list */
    if (key_get_flags(entry)&KEY_HAS_DUPLICATES
            && !(flags&HAM_SKIP_DUPLICATES)) {
        ham_size_t count;
        ham_status_t st;
        st=blob_duplicate_get_count(env, key_get_ptr(entry),
                        &count, btree_cursor_get_dupe_cache(c));
        if (st)
            return (st);
        btree_cursor_set_dupe_id(c, count-1);
    }

    return (0);
}

ham_status_t
btree_cursor_set_to_nil(btree_cursor_t *c)
{
    Environment *env = btree_cursor_get_db(c)->get_env();

    /* uncoupled cursor: free the cached pointer */
    if (btree_cursor_is_uncoupled(c)) {
        ham_key_t *key=btree_cursor_get_uncoupled_key(c);
        if (key->data)
            env->get_allocator()->free(key->data);
        env->get_allocator()->free(key);
        btree_cursor_set_uncoupled_key(c, 0);
        btree_cursor_set_flags(c,
                btree_cursor_get_flags(c)&(~BTREE_CURSOR_FLAG_UNCOUPLED));
    }
    /* coupled cursor: uncouple, remove from page */
    else if (btree_cursor_is_coupled(c)) {
        btree_cursor_get_coupled_page(c)->remove_cursor(btree_cursor_get_parent(c));
        btree_cursor_set_flags(c,
                btree_cursor_get_flags(c)&(~BTREE_CURSOR_FLAG_COUPLED));
    }

    btree_cursor_set_dupe_id(c, 0);
    memset(btree_cursor_get_dupe_cache(c), 0, sizeof(dupe_entry_t));

    return (0);
}

void
btree_cursor_couple_to_other(btree_cursor_t *cu, btree_cursor_t *other)
{
    ham_assert(btree_cursor_is_coupled(other), (""));
    btree_cursor_set_to_nil(cu);

    btree_cursor_set_coupled_page(cu, btree_cursor_get_coupled_page(other));
    btree_cursor_set_coupled_index(cu, btree_cursor_get_coupled_index(other));
    btree_cursor_set_dupe_id(cu, btree_cursor_get_dupe_id(other));
    btree_cursor_set_flags(cu, btree_cursor_get_flags(other));
}

ham_bool_t
btree_cursor_is_nil(btree_cursor_t *cursor)
{
    if (btree_cursor_is_uncoupled(cursor))
        return (HAM_FALSE);
    if (btree_cursor_is_coupled(cursor))
        return (HAM_FALSE);
    if (btree_cursor_get_parent(cursor)->is_coupled_to_txnop())
        return (HAM_FALSE);
    return (HAM_TRUE);
}

ham_status_t
btree_cursor_uncouple(btree_cursor_t *c, ham_u32_t flags)
{
    ham_status_t st;
    btree_node_t *node;
    btree_key_t *entry;
    ham_key_t *key;
    Database *db=btree_cursor_get_db(c);
    Environment *env=db->get_env();

    if (btree_cursor_is_uncoupled(c) || btree_cursor_is_nil(c))
        return (0);

    ham_assert(btree_cursor_get_coupled_page(c)!=0,
            ("uncoupling a cursor which has no coupled page"));

    /* get the btree-entry of this key */
    node=page_get_btree_node(btree_cursor_get_coupled_page(c));
    ham_assert(btree_node_is_leaf(node), ("iterator points to internal node"));
    entry=btree_node_get_key(db, node, btree_cursor_get_coupled_index(c));

    /* copy the key */
    key=(ham_key_t *)env->get_allocator()->calloc(sizeof(*key));
    if (!key)
        return (HAM_OUT_OF_MEMORY);
    st=btree_copy_key_int2pub(db, entry, key);
    if (st) {
        if (key->data)
            env->get_allocator()->free(key->data);
        env->get_allocator()->free(key);
        return (st);
    }

    /* uncouple the page */
    if (!(flags&BTREE_CURSOR_UNCOUPLE_NO_REMOVE))
        btree_cursor_get_coupled_page(c)->remove_cursor(btree_cursor_get_parent(c));

    /* set the flags and the uncoupled key */
    btree_cursor_set_flags(c,
                    btree_cursor_get_flags(c)&(~BTREE_CURSOR_FLAG_COUPLED));
    btree_cursor_set_flags(c,
                    btree_cursor_get_flags(c)|BTREE_CURSOR_FLAG_UNCOUPLED);
    btree_cursor_set_uncoupled_key(c, key);

    return (0);
}

ham_status_t
btree_cursor_clone(btree_cursor_t *src, btree_cursor_t *dest,
                Cursor *parent)
{
    ham_status_t st;
    Database *db=btree_cursor_get_db(src);
    Environment *env=db->get_env();

    btree_cursor_set_dupe_id(dest, btree_cursor_get_dupe_id(src));
    btree_cursor_set_parent(dest, parent);

    /* if the old cursor is coupled: couple the new cursor, too */
    if (btree_cursor_is_coupled(src)) {
         Page *page=btree_cursor_get_coupled_page(src);
         page->add_cursor(btree_cursor_get_parent(dest));
         btree_cursor_set_coupled_page(dest, page);
    }
    /* otherwise, if the src cursor is uncoupled: copy the key */
    else if (btree_cursor_is_uncoupled(src)) {
        ham_key_t *key;

        key=(ham_key_t *)env->get_allocator()->calloc(sizeof(*key));
        if (!key)
            return (HAM_OUT_OF_MEMORY);

        st=btree_cursor_get_db(dest)->copy_key(
                        btree_cursor_get_uncoupled_key(src), key);
        if (st) {
            if (key->data)
                env->get_allocator()->free(key->data);
            env->get_allocator()->free(key);
            return (st);
        }
        btree_cursor_set_uncoupled_key(dest, key);
    }

    return (0);
}

void
btree_cursor_close(btree_cursor_t *c)
{
    btree_cursor_set_to_nil(c);
}

ham_status_t
btree_cursor_overwrite(btree_cursor_t *c, ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    btree_node_t *node;
    btree_key_t *key;
    Database *db=btree_cursor_get_db(c);
    Transaction *txn=btree_cursor_get_parent(c)->get_txn();
    Page *page;

    /* uncoupled cursor: couple it */
    if (btree_cursor_is_uncoupled(c)) {
        st=btree_cursor_couple(c);
        if (st)
            return (st);
    }
    else if (!btree_cursor_is_coupled(c))
        return (HAM_CURSOR_IS_NIL);

    /* delete the cache of the current duplicate */
    memset(btree_cursor_get_dupe_cache(c), 0, sizeof(dupe_entry_t));

    page=btree_cursor_get_coupled_page(c);

    /* get the btree node entry */
    node=page_get_btree_node(btree_cursor_get_coupled_page(c));
    ham_assert(btree_node_is_leaf(node), ("iterator points to internal node"));
    key=btree_node_get_key(db, node, btree_cursor_get_coupled_index(c));

    /* copy the key flags, and remove all flags concerning the key size */
    st=key_set_record(db, txn, key, record, 
            btree_cursor_get_dupe_id(c), flags|HAM_OVERWRITE, 0);
    if (st)
        return (st);

    page->set_dirty(true);

    return (0);
}

ham_status_t
btree_cursor_move(btree_cursor_t *c, ham_key_t *key,
                ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st=0;
    Page *page;
    btree_node_t *node;
    Database *db=btree_cursor_get_db(c);
    Environment *env=db->get_env();
    Transaction *txn=btree_cursor_get_parent(c)->get_txn();
    BtreeBackend *be=(BtreeBackend *)db->get_backend();
    btree_key_t *entry;

    if (!be)
        return (HAM_NOT_INITIALIZED);

    /* delete the cache of the current duplicate */
    memset(btree_cursor_get_dupe_cache(c), 0, sizeof(dupe_entry_t));

    if (flags&HAM_CURSOR_FIRST)
        st=__move_first(be, c, flags);
    else if (flags&HAM_CURSOR_LAST)
        st=__move_last(be, c, flags);
    else if (flags&HAM_CURSOR_NEXT)
        st=__move_next(be, c, flags);
    else if (flags&HAM_CURSOR_PREVIOUS)
        st=__move_previous(be, c, flags);
    /* no move, but cursor is nil? return error */
    else if (btree_cursor_is_nil(c)) {
        if (key || record)
            return (HAM_CURSOR_IS_NIL);
        else
            return (0);
    }
    /* no move, but cursor is not coupled? couple it */
    else if (btree_cursor_is_uncoupled(c))
        st=btree_cursor_couple(c);

    if (st)
        return (st);

    /*
     * during btree_read_key and btree_read_record, new pages might be needed,
     * and the page at which we're pointing could be moved out of memory;
     * that would mean that the cursor would be uncoupled, and we're losing
     * the 'entry'-pointer. therefore we 'lock' the page by incrementing
     * the reference counter
     */
    ham_assert(btree_cursor_is_coupled(c), ("move: cursor is not coupled"));
    page=btree_cursor_get_coupled_page(c);
    node=page_get_btree_node(page);
    ham_assert(btree_node_is_leaf(node), ("iterator points to internal node"));
    entry=btree_node_get_key(db, node, btree_cursor_get_coupled_index(c));

    if (key) {
        st=btree_read_key(db, txn, entry, key);
        if (st)
            return (st);
    }

    if (record) {
        ham_u64_t *ridptr=0;
        if (key_get_flags(entry)&KEY_HAS_DUPLICATES
                && btree_cursor_get_dupe_id(c)) {
            dupe_entry_t *e=btree_cursor_get_dupe_cache(c);
            if (!dupe_entry_get_rid(e)) {
                st=blob_duplicate_get(env, key_get_ptr(entry),
                        btree_cursor_get_dupe_id(c),
                        btree_cursor_get_dupe_cache(c));
                if (st)
                    return st;
            }
            record->_intflags=dupe_entry_get_flags(e);
            record->_rid=dupe_entry_get_rid(e);
            ridptr=(ham_u64_t *)&dupe_entry_get_ridptr(e);
        }
        else {
            record->_intflags=key_get_flags(entry);
            record->_rid=key_get_ptr(entry);
            ridptr=(ham_u64_t *)&key_get_rawptr(entry);
        }
        st=btree_read_record(db, txn, record, ridptr, flags);
        if (st)
            return (st);
    }

    return (0);
}

ham_status_t
btree_cursor_find(btree_cursor_t *c, ham_key_t *key, ham_record_t *record,
                ham_u32_t flags)
{
    ham_status_t st;
    Backend *be=btree_cursor_get_db(c)->get_backend();
    Transaction *txn=btree_cursor_get_parent(c)->get_txn();

    if (!be)
        return (HAM_NOT_INITIALIZED);
    ham_assert(key, ("invalid parameter"));

    st=btree_cursor_set_to_nil(c);
    if (st)
        return (st);

    st=btree_find_cursor((BtreeBackend *)be, txn, c, key, record, flags);
    if (st) {
        /* cursor is now NIL */
        return (st);
    }

    return (0);
}

ham_status_t
btree_cursor_insert(btree_cursor_t *c, ham_key_t *key,
                ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    Database *db=btree_cursor_get_db(c);
    BtreeBackend *be=(BtreeBackend *)db->get_backend();
    Transaction *txn=btree_cursor_get_parent(c)->get_txn();

    if (!be)
        return (HAM_NOT_INITIALIZED);
    ham_assert(key, (0));
    ham_assert(record, (0));

    /* call the btree insert function */
    st=btree_insert_cursor(be, txn, key, record, c, flags);
    if (st)
        return (st);

    return (0);
}

ham_status_t
btree_cursor_erase(btree_cursor_t *c, ham_u32_t flags)
{
    ham_status_t st;
    Database *db=btree_cursor_get_db(c);
    BtreeBackend *be=(BtreeBackend *)db->get_backend();
    Transaction *txn=btree_cursor_get_parent(c)->get_txn();

    if (!be)
        return (HAM_NOT_INITIALIZED);

    /* coupled cursor: try to remove the key directly from the page.
     * if that's not possible (i.e. because of underflow): uncouple
     * the cursor and process the normal erase algorithm */
    if (btree_cursor_is_coupled(c)) {
        Page *page=btree_cursor_get_coupled_page(c);
        btree_node_t *node=page_get_btree_node(page);
        BtreeBackend *be=(BtreeBackend *)db->get_backend();
        ham_size_t maxkeys=be->get_maxkeys();
        ham_assert(btree_node_is_leaf(node), 
                ("iterator points to internal node"));
        if (btree_cursor_get_coupled_index(c)>0
                && btree_node_get_count(node)>btree_get_minkeys(maxkeys)) {
            /* yes, we can remove the key */
            return (btree_cursor_erase_fasttrack(be, txn, c));
        }
        else {
            st=btree_cursor_uncouple(c, 0);
            if (st)
                return (st);
        }
    }
    else if (!btree_cursor_is_uncoupled(c))
        return (HAM_CURSOR_IS_NIL);

    return (btree_erase_cursor(be, txn,
                btree_cursor_get_uncoupled_key(c), c, flags));
}

bool
btree_cursor_points_to(btree_cursor_t *cursor, btree_key_t *key)
{
    ham_status_t st;
    Database *db=btree_cursor_get_db(cursor);

    if (btree_cursor_is_uncoupled(cursor)) {
        st=btree_cursor_couple(cursor);
        if (st)
            return (false);
    }

    if (btree_cursor_is_coupled(cursor)) {
        Page *page=btree_cursor_get_coupled_page(cursor);
        btree_node_t *node=page_get_btree_node(page);
        btree_key_t *entry=btree_node_get_key(db, node,
                        btree_cursor_get_coupled_index(cursor));

        if (entry==key)
            return (true);
    }

    return (false);
}

bool
btree_cursor_points_to_key(btree_cursor_t *btc, ham_key_t *key)
{
    Cursor *c=btree_cursor_get_parent(btc);
    Database *db=c->get_db();
    bool ret=false;

    if (btree_cursor_is_uncoupled(btc)) {
        ham_key_t *k=btree_cursor_get_uncoupled_key(btc);
        if (k->size!=key->size)
            return (false);
        return (0==db->compare_keys(key, k));
    }

    if (btree_cursor_is_coupled(btc)) {
        Page *page=btree_cursor_get_coupled_page(btc);
        btree_node_t *node=page_get_btree_node(page);
        btree_key_t *entry=btree_node_get_key(db, node,
                        btree_cursor_get_coupled_index(btc));

        if (key_get_size(entry)!=key->size)
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
               btree_cursor_get_uncoupled_key(clone->get_btree_cursor())))
            ret=true;
        db->close_cursor(clone);
        return (ret);
    }

    else {
        ham_assert(!"shouldn't be here", (""));
    }
    
    return (ret);
}


ham_status_t
btree_cursor_get_duplicate_count(btree_cursor_t *c,
                ham_size_t *count, ham_u32_t flags)
{
    ham_status_t st;
    Database *db=btree_cursor_get_db(c);
    Environment *env = db->get_env();
    BtreeBackend *be=(BtreeBackend *)db->get_backend();
    Page *page;
    btree_node_t *node;
    btree_key_t *entry;

    if (!be)
        return (HAM_NOT_INITIALIZED);

    /* uncoupled cursor: couple it */
    if (btree_cursor_is_uncoupled(c)) {
        st=btree_cursor_couple(c);
        if (st)
            return (st);
    }
    else if (!(btree_cursor_is_coupled(c)))
        return (HAM_CURSOR_IS_NIL);

    page=btree_cursor_get_coupled_page(c);
    node=page_get_btree_node(page);
    entry=btree_node_get_key(db, node, btree_cursor_get_coupled_index(c));

    if (!(key_get_flags(entry)&KEY_HAS_DUPLICATES)) {
        *count=1;
    }
    else {
        st=blob_duplicate_get_count(env, key_get_ptr(entry), count, 0);
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
        btree_cursor_t *btc=c->get_btree_cursor();
        n=c->get_next_in_page();

        /*
         * ignore all cursors which are already uncoupled or which are
         * coupled to the txn
         */
        if (btree_cursor_is_coupled(btc) || c->is_coupled_to_txnop()) {
            /* skip this cursor if its position is < start */
            if (btree_cursor_get_coupled_index(btc)<start) {
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

void
btree_cursor_create(Database *db, Transaction *txn, ham_u32_t flags,
                btree_cursor_t *cursor, Cursor *parent)
{
    btree_cursor_set_parent(cursor, parent);
}

ham_status_t
btree_cursor_get_duplicate_table(btree_cursor_t *c, dupe_table_t **ptable,
                ham_bool_t *needs_free)
{
    ham_status_t st;
    Page *page;
    btree_node_t *node;
    btree_key_t *entry;
    Database *db=btree_cursor_get_db(c);
    Environment *env = db->get_env();

    *ptable=0;

    /* uncoupled cursor: couple it */
    if (btree_cursor_is_uncoupled(c)) {
        st=btree_cursor_couple(c);
        if (st)
            return (st);
    }
    else if (!btree_cursor_is_coupled(c))
        return (HAM_CURSOR_IS_NIL);

    page=btree_cursor_get_coupled_page(c);
    node=page_get_btree_node(page);
    entry=btree_node_get_key(db, node, btree_cursor_get_coupled_index(c));

    /* if key has no duplicates: return successfully, but with *ptable=0 */
    if (!(key_get_flags(entry)&KEY_HAS_DUPLICATES)) {
        dupe_entry_t *e;
        dupe_table_t *t;
        t=(dupe_table_t *)env->get_allocator()->calloc(sizeof(*t));
        if (!t)
            return (HAM_OUT_OF_MEMORY);
        dupe_table_set_capacity(t, 1);
        dupe_table_set_count(t, 1);
        e=dupe_table_get_entry(t, 0);
        dupe_entry_set_flags(e, key_get_flags(entry));
        dupe_entry_set_rid(e, key_get_rawptr(entry));
        *ptable=t;
        *needs_free=1;
        return (0);
    }

    return (blob_duplicate_get_table(env, key_get_ptr(entry),
                    ptable, needs_free));
}

ham_status_t
btree_cursor_get_record_size(btree_cursor_t *c, ham_offset_t *size)
{
    ham_status_t st;
    Database *db=btree_cursor_get_db(c);
    BtreeBackend *be=(BtreeBackend *)db->get_backend();
    Page *page;
    btree_node_t *node;
    btree_key_t *entry;
    ham_u32_t keyflags=0;
    ham_u64_t *ridptr=0;
    ham_u64_t rid=0;
    dupe_entry_t dupeentry;

    if (!be)
        return (HAM_NOT_INITIALIZED);

    /*
     * uncoupled cursor: couple it
     */
    if (btree_cursor_is_uncoupled(c)) {
        st=btree_cursor_couple(c);
        if (st)
            return (st);
    }
    else if (!btree_cursor_is_coupled(c))
        return (HAM_CURSOR_IS_NIL);

    page=btree_cursor_get_coupled_page(c);
    node=page_get_btree_node(page);
    entry=btree_node_get_key(db, node, btree_cursor_get_coupled_index(c));

    if (key_get_flags(entry)&KEY_HAS_DUPLICATES) {
        st=blob_duplicate_get(db->get_env(), key_get_ptr(entry),
                        btree_cursor_get_dupe_id(c),
                        &dupeentry);
        if (st)
            return st;
        keyflags=dupe_entry_get_flags(&dupeentry);
        ridptr=&dupeentry._rid;
        rid=dupeentry._rid;
    }
    else {
        keyflags=key_get_flags(entry);
        ridptr=(ham_u64_t *)&key_get_rawptr(entry);
        rid=key_get_ptr(entry);
    }

    if (keyflags&KEY_BLOB_SIZE_TINY) {
        /* the highest byte of the record id is the size of the blob */
        char *p=(char *)ridptr;
        *size=p[sizeof(ham_offset_t)-1];
    }
    else if (keyflags&KEY_BLOB_SIZE_SMALL) {
        /* record size is sizeof(ham_offset_t) */
        *size=sizeof(ham_offset_t);
    }
    else if (keyflags&KEY_BLOB_SIZE_EMPTY) {
        /* record size is 0 */
        *size=0;
    }
    else {
        st=blob_get_datasize(db, rid, size);
        if (st)
            return (st);
    }

    return (0);
}

