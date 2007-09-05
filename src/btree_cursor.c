/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 *
 * btree cursors - implementation
 *
 */

#include <string.h>
#include "btree_cursor.h"
#include "btree.h"
#include "mem.h"
#include "util.h"
#include "error.h"
#include "blob.h"
#include "db.h"
#include "keys.h"

static ham_status_t
my_move_first(ham_btree_t *be, ham_bt_cursor_t *c, ham_u32_t flags)
{
    ham_status_t st;
    ham_page_t *page;
    btree_node_t *node;
    ham_db_t *db=cursor_get_db(c);
    int_key_t *entry;

    /*
     * get a NIL cursor
     */
    st=bt_cursor_set_to_nil(c);
    if (st)
        return (st);

    db_set_error(db, 0);

    /*
     * get the root page
     */
    if (!btree_get_rootpage(be))
        return (db_set_error(db, HAM_KEY_NOT_FOUND));
    page=db_fetch_page(db, btree_get_rootpage(be), 0);
    if (!page)
        return (db_get_error(db));

    /*
     * while we've not reached the leaf: pick the smallest element
     * and traverse down
     */
    while (1) {
        node=ham_page_get_btree_node(page);
        /* check for an empty root page */
        if (btree_node_get_count(node)==0)
            return (db_set_error(db, HAM_KEY_NOT_FOUND));
        /* leave the loop when we've reached the leaf page */
        if (btree_node_is_leaf(node))
            break;

        page=db_fetch_page(db, btree_node_get_ptr_left(node), 0);
        if (!page) {
            if (!db_get_error(db))
                db_set_error(db, HAM_KEY_NOT_FOUND);
            return (db_get_error(db));
        }
    }

    /*
     * couple this cursor to the smallest key in this page
     */
    page_add_cursor(page, (ham_cursor_t *)c);
    bt_cursor_set_coupled_page(c, page);
    bt_cursor_set_coupled_index(c, 0);
    bt_cursor_set_flags(c,
            bt_cursor_get_flags(c)|BT_CURSOR_FLAG_COUPLED);
    entry=btree_node_get_key(db, node, bt_cursor_get_coupled_index(c));
    bt_cursor_set_dupe_id(c, 0);

    return (0);
}

static ham_status_t
my_move_next(ham_btree_t *be, ham_bt_cursor_t *c, ham_u32_t flags)
{
    ham_status_t st;
    ham_page_t *page;
    btree_node_t *node;
    ham_db_t *db=cursor_get_db(c);
    int_key_t *entry;

    /*
     * uncoupled cursor: couple it
     */
    if (bt_cursor_get_flags(c)&BT_CURSOR_FLAG_UNCOUPLED) {
        st=bt_cursor_couple(c);
        if (st)
            return (st);
    }
    else if (!(bt_cursor_get_flags(c)&BT_CURSOR_FLAG_COUPLED))
        return (HAM_CURSOR_IS_NIL);

    db_set_error(db, 0);

    page=bt_cursor_get_coupled_page(c);
    node=ham_page_get_btree_node(page);
    entry=btree_node_get_key(db, node, bt_cursor_get_coupled_index(c));

    /*
     * if this key has duplicates: get the next duplicate; otherwise 
     * (and if there's no duplicate): fall through
     */
    if (key_get_flags(entry)&KEY_HAS_DUPLICATES
            && (!(flags&HAM_SKIP_DUPLICATES))) {
        bt_cursor_set_dupe_id(c, bt_cursor_get_dupe_id(c)+1);
        ham_status_t st=blob_duplicate_get(db, key_get_ptr(entry),
                        bt_cursor_get_dupe_id(c),
                        bt_cursor_get_dupe_cache(c));
        if (st && st!=HAM_KEY_NOT_FOUND)
            return (db_set_error(db, st));
        else if (!st)
            return (0);
    }

    /*
     * don't continue if ONLY_DUPLICATES is set
     */
    if (flags&HAM_ONLY_DUPLICATES)
        return (HAM_KEY_NOT_FOUND);

    /*
     * if the index+1 is till in the coupled page, just increment the
     * index
     */
    if (bt_cursor_get_coupled_index(c)+1<btree_node_get_count(node)) {
        bt_cursor_set_coupled_index(c, bt_cursor_get_coupled_index(c)+1);
        entry=btree_node_get_key(db, node, bt_cursor_get_coupled_index(c));
        bt_cursor_set_dupe_id(c, 0);
        return (0);
    }

    /*
     * otherwise uncouple the cursor and load the right sibling page
     */
    if (!btree_node_get_right(node))
        return db_set_error(db, HAM_KEY_NOT_FOUND);

    page_remove_cursor(page, (ham_cursor_t *)c);
    bt_cursor_set_flags(c, bt_cursor_get_flags(c)&(~BT_CURSOR_FLAG_COUPLED));

    page=db_fetch_page(db, btree_node_get_right(node), 0);
    if (!page)
        return (db_get_error(db));
    node=ham_page_get_btree_node(page);

    /*
     * couple this cursor to the smallest key in this page
     */
    page_add_cursor(page, (ham_cursor_t *)c);
    bt_cursor_set_coupled_page(c, page);
    bt_cursor_set_coupled_index(c, 0);
    bt_cursor_set_flags(c,
            bt_cursor_get_flags(c)|BT_CURSOR_FLAG_COUPLED);
    entry=btree_node_get_key(db, node, bt_cursor_get_coupled_index(c));
    bt_cursor_set_dupe_id(c, 0);

    return (0);
}

static ham_status_t
my_move_previous(ham_btree_t *be, ham_bt_cursor_t *c, ham_u32_t flags)
{
    ham_status_t st;
    ham_page_t *page;
    btree_node_t *node;
    ham_db_t *db=cursor_get_db(c);
    int_key_t *entry;

    /*
     * uncoupled cursor: couple it
     */
    if (bt_cursor_get_flags(c)&BT_CURSOR_FLAG_UNCOUPLED) {
        st=bt_cursor_couple(c);
        if (st)
            return (st);
    }
    else if (!(bt_cursor_get_flags(c)&BT_CURSOR_FLAG_COUPLED))
        return (HAM_CURSOR_IS_NIL);


    db_set_error(db, 0);

    page=bt_cursor_get_coupled_page(c);
    node=ham_page_get_btree_node(page);
    entry=btree_node_get_key(db, node, bt_cursor_get_coupled_index(c));

    /*
     * if this key has duplicates: get the previous duplicate; otherwise 
     * (and if there's no duplicate): fall through
     */
    if (key_get_flags(entry)&KEY_HAS_DUPLICATES
            && (!(flags&HAM_SKIP_DUPLICATES))
            && bt_cursor_get_dupe_id(c)>0) {
        bt_cursor_set_dupe_id(c, bt_cursor_get_dupe_id(c)-1);
        ham_status_t st=blob_duplicate_get(db, key_get_ptr(entry),
                        bt_cursor_get_dupe_id(c), 
                        bt_cursor_get_dupe_cache(c));
        if (st && st!=HAM_KEY_NOT_FOUND)
            return (db_set_error(db, st));
        else if (!st)
            return (0);
    }

    /*
     * don't continue if ONLY_DUPLICATES is set
     */
    if (flags&HAM_ONLY_DUPLICATES)
        return (HAM_KEY_NOT_FOUND);

    /*
     * if the index-1 is till in the coupled page, just decrement the
     * index
     */
    if (bt_cursor_get_coupled_index(c)!=0) {
        bt_cursor_set_coupled_index(c, bt_cursor_get_coupled_index(c)-1);
        entry=btree_node_get_key(db, node, bt_cursor_get_coupled_index(c));
    }
    /*
     * otherwise load the left sibling page
     */
    else {
        if (!btree_node_get_left(node))
            return db_set_error(db, HAM_KEY_NOT_FOUND);

        page_remove_cursor(page, (ham_cursor_t *)c);
        bt_cursor_set_flags(c, 
                bt_cursor_get_flags(c)&(~BT_CURSOR_FLAG_COUPLED));

        page=db_fetch_page(db, btree_node_get_left(node), 0);
        if (!page)
            return (db_get_error(db));
        node=ham_page_get_btree_node(page);

        /*
         * couple this cursor to the highest key in this page
         */
        page_add_cursor(page, (ham_cursor_t *)c);
        bt_cursor_set_coupled_page(c, page);
        bt_cursor_set_coupled_index(c, btree_node_get_count(node)-1);
        bt_cursor_set_flags(c,
                bt_cursor_get_flags(c)|BT_CURSOR_FLAG_COUPLED);
        entry=btree_node_get_key(db, node, bt_cursor_get_coupled_index(c));
    }
    bt_cursor_set_dupe_id(c, 0);

    /*
     * if duplicates are enabled: move to the end of the duplicate-list
     *
     * TODO
     * fill cursor_dupe_cache
     */
    if (key_get_flags(entry)&KEY_HAS_DUPLICATES
            && !(flags&HAM_SKIP_DUPLICATES)) {
        ham_size_t count;
        ham_status_t st=blob_duplicate_get_count(db, key_get_ptr(entry),
                                &count);
        if (st)
            return (db_set_error(db, st));
        bt_cursor_set_dupe_id(c, count-1);
    }

    return (0);
}

static ham_status_t
my_move_last(ham_btree_t *be, ham_bt_cursor_t *c, ham_u32_t flags)
{
    ham_status_t st;
    ham_page_t *page;
    btree_node_t *node;
    int_key_t *entry;
    ham_db_t *db=cursor_get_db(c);

    /*
     * get a NIL cursor
     */
    st=bt_cursor_set_to_nil(c);
    if (st)
        return (st);

    db_set_error(db, 0);

    /*
     * get the root page
     */
    if (!btree_get_rootpage(be))
        return (db_set_error(db, HAM_KEY_NOT_FOUND));
    page=db_fetch_page(db, btree_get_rootpage(be), 0);
    if (!page)
        return (db_get_error(db));

    /*
     * while we've not reached the leaf: pick the largest element
     * and traverse down
     */
    while (1) {
        int_key_t *key;

        node=ham_page_get_btree_node(page);
        /* check for an empty root page */
        if (btree_node_get_count(node)==0)
            return (db_set_error(db, HAM_KEY_NOT_FOUND));
        /* leave the loop when we've reached a leaf page */
        if (btree_node_is_leaf(node))
            break;

        key=btree_node_get_key(db, node, btree_node_get_count(node)-1);

        page=db_fetch_page(db, key_get_ptr(key), 0);
        if (!page) {
            if (!db_get_error(db))
                db_set_error(db, HAM_KEY_NOT_FOUND);
            return (db_get_error(db));
        }
    }

    /*
     * couple this cursor to the largest key in this page
     */
    page_add_cursor(page, (ham_cursor_t *)c);
    bt_cursor_set_coupled_page(c, page);
    bt_cursor_set_coupled_index(c, btree_node_get_count(node)-1);
    bt_cursor_set_flags(c,
            bt_cursor_get_flags(c)|BT_CURSOR_FLAG_COUPLED);
    entry=btree_node_get_key(db, node, bt_cursor_get_coupled_index(c));
    bt_cursor_set_dupe_id(c, 0);

    /*
     * if duplicates are enabled: move to the end of the duplicate-list
     *
     * TODO
     * fill cursor_dupe_cache
     */
    if (key_get_flags(entry)&KEY_HAS_DUPLICATES
            && !(flags&HAM_SKIP_DUPLICATES)) {
        ham_size_t count;
        ham_status_t st=blob_duplicate_get_count(db, key_get_ptr(entry),
                                &count);
        if (st)
            return (db_set_error(db, st));
        bt_cursor_set_dupe_id(c, count-1);
    }

    return (0);
}

ham_status_t
bt_cursor_set_to_nil(ham_bt_cursor_t *c)
{
    /*
     * uncoupled cursor: free the cached pointer
     */
    if (bt_cursor_get_flags(c)&BT_CURSOR_FLAG_UNCOUPLED) {
        ham_key_t *key=bt_cursor_get_uncoupled_key(c);
        if (key->data)
            ham_mem_free(cursor_get_db(c), key->data);
        ham_mem_free(cursor_get_db(c), key);
        bt_cursor_set_uncoupled_key(c, 0);
        bt_cursor_set_flags(c,
                bt_cursor_get_flags(c)&(~BT_CURSOR_FLAG_UNCOUPLED));
    }
    /*
     * coupled cursor: uncouple, remove from page
     */
    else if (bt_cursor_get_flags(c)&BT_CURSOR_FLAG_COUPLED) {
        page_remove_cursor(bt_cursor_get_coupled_page(c),
                (ham_cursor_t *)c);
        bt_cursor_set_flags(c,
                bt_cursor_get_flags(c)&(~BT_CURSOR_FLAG_COUPLED));
    }

    bt_cursor_set_dupe_id(c, 0);

    return (0);
}

ham_status_t
bt_cursor_couple(ham_bt_cursor_t *c)
{
    ham_key_t key;
    ham_status_t st;
    ham_db_t *db=cursor_get_db(c);
    ham_txn_t txn;
    ham_bool_t local_txn=db_get_txn(db) ? HAM_FALSE : HAM_TRUE;

    ham_assert(bt_cursor_get_flags(c)&BT_CURSOR_FLAG_UNCOUPLED,
            ("coupling a cursor which is not uncoupled"));

    if (local_txn) {
        st=ham_txn_begin(&txn, db);
        if (st)
            return (st);
    }

    /*
     * make a 'find' on the cached key; if we succeed, the cursor
     * is automatically coupled
     */
    memset(&key, 0, sizeof(key));

    if (!util_copy_key(db, bt_cursor_get_uncoupled_key(c), &key)) {
        if (local_txn)
            (void)ham_txn_abort(&txn);
        return (db_get_error(db));
    }
    
    st=bt_cursor_find(c, &key, 0);

    /*
     * free the cached key
     */
    if (key.data)
        ham_mem_free(db, key.data);

    if (local_txn) {
        if (st) {
            (void)ham_txn_abort(&txn);
            return (st);
        }
        return (ham_txn_commit(&txn, 0));
    }

    return (st);
}

ham_status_t
bt_cursor_uncouple(ham_bt_cursor_t *c, ham_u32_t flags)
{
    ham_status_t st=0;
    btree_node_t *node;
    int_key_t *entry;
    ham_key_t *key;
    ham_db_t *db=bt_cursor_get_db(c);
    ham_txn_t txn;
    ham_bool_t local_txn=db_get_txn(db) ? HAM_FALSE : HAM_TRUE;

    if (bt_cursor_get_flags(c)&BT_CURSOR_FLAG_UNCOUPLED)
        return (0);
    if (!(bt_cursor_get_flags(c)&BT_CURSOR_FLAG_COUPLED))
        return (0);

    ham_assert(bt_cursor_get_coupled_page(c)!=0,
            ("uncoupling a cursor which has no coupled page"));

    if (local_txn) {
        st=ham_txn_begin(&txn, db);
        if (st)
            return (st);
    }

    /*
     * get the btree-entry of this key
     */
    node=ham_page_get_btree_node(bt_cursor_get_coupled_page(c));
    ham_assert(btree_node_is_leaf(node), ("iterator points to internal node"));
    entry=btree_node_get_key(db, node, bt_cursor_get_coupled_index(c));

    /*
     * copy the key
     */
    key=(ham_key_t *)ham_mem_alloc(db, sizeof(*key));
    if (!key) {
        if (local_txn)
            (void)ham_txn_abort(&txn);
        return (db_set_error(db, HAM_OUT_OF_MEMORY));
    }
    memset(key, 0, sizeof(*key));
    key=util_copy_key_int2pub(db, entry, key);
    if (!key) {
        if (local_txn)
            (void)ham_txn_abort(&txn);
        return (db_get_error(bt_cursor_get_db(c)));
    }

    /*
     * uncouple the page
     */
    if (!(flags&BT_CURSOR_UNCOUPLE_NO_REMOVE))
        page_remove_cursor(bt_cursor_get_coupled_page(c),
                (ham_cursor_t *)c);

    /*
     * set the flags and the uncoupled key
     */
    bt_cursor_set_flags(c, bt_cursor_get_flags(c)&(~BT_CURSOR_FLAG_COUPLED));
    bt_cursor_set_flags(c, bt_cursor_get_flags(c)|BT_CURSOR_FLAG_UNCOUPLED);
    bt_cursor_set_uncoupled_key(c, key);

    if (local_txn)
        return (ham_txn_commit(&txn, 0));

    return (0);
}

ham_status_t
bt_cursor_create(ham_db_t *db, ham_txn_t *txn, ham_u32_t flags,
            ham_bt_cursor_t **cu)
{
    ham_bt_cursor_t *c;

    (void)flags;

    *cu=0;

    c=(ham_bt_cursor_t *)ham_mem_alloc(db, sizeof(*c));
    if (!c)
        return (db_set_error(db, HAM_OUT_OF_MEMORY));

    memset(c, 0, sizeof(*c));
    c->_fun_clone=bt_cursor_clone;
    c->_fun_close=bt_cursor_close;
    c->_fun_overwrite=bt_cursor_overwrite;
    c->_fun_move=bt_cursor_move;
    c->_fun_find=bt_cursor_find;
    c->_fun_insert=bt_cursor_insert;
    c->_fun_erase=bt_cursor_erase;
    bt_cursor_set_db(c, db);
    bt_cursor_set_txn(c, txn);

    /* fix the linked list of cursors */
    cursor_set_next((ham_cursor_t *)c, db_get_cursors(db));
    if (db_get_cursors(db))
        cursor_set_previous(db_get_cursors(db), (ham_cursor_t *)c);
    db_set_cursors(db, (ham_cursor_t *)c);

    *cu=c;
    return (0);
}

ham_status_t
bt_cursor_clone(ham_bt_cursor_t *oldcu, ham_bt_cursor_t **newcu)
{
    ham_status_t st=0;
    ham_bt_cursor_t *c;
    ham_db_t *db=bt_cursor_get_db(oldcu);
    ham_txn_t txn;
    ham_bool_t local_txn=db_get_txn(db) ? HAM_FALSE : HAM_TRUE;

    *newcu=0;

    c=(ham_bt_cursor_t *)ham_mem_alloc(db, sizeof(*c));
    if (!c)
        return (db_set_error(db, HAM_OUT_OF_MEMORY));
    memcpy(c, oldcu, sizeof(*c));
    cursor_set_next_in_page(c, 0);
    cursor_set_previous_in_page(c, 0);

    /* fix the linked list of cursors */
    cursor_set_previous((ham_cursor_t *)c, 0);
    cursor_set_next((ham_cursor_t *)c, db_get_cursors(db));
    if (db_get_cursors(db))
        cursor_set_previous(db_get_cursors(db), (ham_cursor_t *)c);
    db_set_cursors(db, (ham_cursor_t *)c);

    if (local_txn) {
        st=ham_txn_begin(&txn, db);
        if (st)
            return (st);
    }

    /*
     * if the old cursor is coupled: couple the new cursor, too
     */
    if (bt_cursor_get_flags(oldcu)&BT_CURSOR_FLAG_COUPLED) {
         ham_page_t *page=bt_cursor_get_coupled_page(oldcu);
         page_add_cursor(page, (ham_cursor_t *)c);
         bt_cursor_set_coupled_page(c, page);
    }
    /*
     * if the old cursor is uncoupled: copy the key
     */
    else if (bt_cursor_get_flags(oldcu)&BT_CURSOR_FLAG_UNCOUPLED) {
        ham_key_t *key;

        key=(ham_key_t *)ham_mem_alloc(db, sizeof(*key));
        if (!key) {
            if (local_txn)
                (void)ham_txn_abort(&txn);
            return (db_set_error(db, HAM_OUT_OF_MEMORY));
        }
        memset(key, 0, sizeof(*key));

        if (!util_copy_key(bt_cursor_get_db(c), 
                bt_cursor_get_uncoupled_key(oldcu), key)) {
            if (key->data)
                ham_mem_free(db, key->data);
            ham_mem_free(db, key);
            if (local_txn)
                (void)ham_txn_abort(&txn);
            return (db_get_error(bt_cursor_get_db(c)));
        }
        bt_cursor_set_uncoupled_key(c, key);
    }

    bt_cursor_set_dupe_id(c, bt_cursor_get_dupe_id(oldcu));

    *newcu=c;

    if (local_txn)
        return (ham_txn_commit(&txn, 0));

    return (0);
}

ham_status_t
bt_cursor_close(ham_bt_cursor_t *c)
{
    ham_db_t *db=bt_cursor_get_db(c);

    /* fix the linked list of cursors */
    ham_cursor_t *p=(ham_cursor_t *)cursor_get_previous(c);
    ham_cursor_t *n=(ham_cursor_t *)cursor_get_next(c);

    if (p)
        cursor_set_next(p, n);
    else
        db_set_cursors(db, n);

    if (n)
        cursor_set_previous(n, p);
    else
        db_set_cursors(db, 0);

    (void)bt_cursor_set_to_nil(c);

    ham_mem_free(cursor_get_db(c), c);

    return (0);
}

ham_status_t
bt_cursor_overwrite(ham_bt_cursor_t *c, ham_record_t *record,
            ham_u32_t flags)
{
    ham_status_t st;
    btree_node_t *node;
    int_key_t *key;
    ham_db_t *db=bt_cursor_get_db(c);
    ham_txn_t txn;
    ham_bool_t local_txn=db_get_txn(db) ? HAM_FALSE : HAM_TRUE;
    ham_page_t *page;

    if (local_txn) {
        st=ham_txn_begin(&txn, db);
        if (st)
            return (st);
    }

    /*
     * uncoupled cursor: couple it
     */
    if (bt_cursor_get_flags(c)&BT_CURSOR_FLAG_UNCOUPLED) {
        st=bt_cursor_couple(c);
        if (st) {
            if (local_txn)
                (void)ham_txn_abort(&txn);
            return (st);
        }
    }
    else if (!(bt_cursor_get_flags(c)&BT_CURSOR_FLAG_COUPLED)) {
        if (local_txn)
            (void)ham_txn_abort(&txn);
        return (HAM_CURSOR_IS_NIL);
    }

    /*
     * delete the cache of the current duplicate
     */
    memset(bt_cursor_get_dupe_cache(c), 0, sizeof(dupe_entry_t));

    /*
     * make sure that the page is not unloaded
     */
    page=bt_cursor_get_coupled_page(c);
    page_add_ref(page);

    /*
     * get the btree node entry
     */
    node=ham_page_get_btree_node(bt_cursor_get_coupled_page(c));
    ham_assert(btree_node_is_leaf(node), ("iterator points to internal node"));
    key=btree_node_get_key(db, node, bt_cursor_get_coupled_index(c));

    /*
     * copy the key flags, and remove all flags concerning the key size
     */
    st=key_set_record(db, key, record, 
            bt_cursor_get_dupe_id(c), flags|HAM_OVERWRITE);
    if (st) {
        page_release_ref(page);
        if (local_txn)
            return (ham_txn_abort(&txn));
        return (st);
    }

    page_set_dirty(bt_cursor_get_coupled_page(c), 1);
    page_release_ref(page);

    if (local_txn)
        return (ham_txn_commit(&txn, 0));

    return (0);
}

ham_status_t
bt_cursor_move(ham_bt_cursor_t *c, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st=0;
    ham_page_t *page;
    btree_node_t *node;
    ham_db_t *db=bt_cursor_get_db(c);
    ham_btree_t *be=(ham_btree_t *)db_get_backend(db);
    int_key_t *entry;
    ham_txn_t txn;
    ham_bool_t local_txn=db_get_txn(db) ? HAM_FALSE : HAM_TRUE;

    if (!be)
        return (HAM_NOT_INITIALIZED);

    if (local_txn) {
        st=ham_txn_begin(&txn, db);
        if (st)
            return (st);
    }

    /*
     * if the cursor is NIL, and the user requests a NEXT, we set it to FIRST;
     * if the user requests a PREVIOUS, we set it to LAST, resp.
     */
    if (bt_cursor_is_nil(c)) {
        if (flags&HAM_CURSOR_NEXT) {
            flags&=~HAM_CURSOR_NEXT;
            flags|=HAM_CURSOR_FIRST;
        }
        else if (flags&HAM_CURSOR_PREVIOUS) {
            flags&=~HAM_CURSOR_PREVIOUS;
            flags|=HAM_CURSOR_LAST;
        }
    }

    /*
     * delete the cache of the current duplicate
     */
    memset(bt_cursor_get_dupe_cache(c), 0, sizeof(dupe_entry_t));

    if (flags&HAM_CURSOR_FIRST)
        st=my_move_first(be, c, flags);
    else if (flags&HAM_CURSOR_LAST)
        st=my_move_last(be, c, flags);
    else if (flags&HAM_CURSOR_NEXT)
        st=my_move_next(be, c, flags);
    else if (flags&HAM_CURSOR_PREVIOUS)
        st=my_move_previous(be, c, flags);
    /* no move, but cursor is nil? return error */
    else if (bt_cursor_is_nil(c)) {
        if (local_txn)
            (void)ham_txn_abort(&txn);
        if (key || record) 
            return (HAM_CURSOR_IS_NIL);
        else
            return (0);
    }
    /* no move, but cursor is not coupled? couple it */
    else if (bt_cursor_get_flags(c)&BT_CURSOR_FLAG_UNCOUPLED) {
        st=bt_cursor_couple(c);
    }

    if (st) {
        if (local_txn)
            (void)ham_txn_abort(&txn);
        return (st);
    }

    /*
     * during util_read_key and util_read_record, new pages might be needed,
     * and the page at which we're pointing could be moved out of memory; 
     * that would mean that the cursor would be uncoupled, and we're losing
     * the 'entry'-pointer. therefore we 'lock' the page by incrementing 
     * the reference counter
     */
    ham_assert(bt_cursor_get_flags(c)&BT_CURSOR_FLAG_COUPLED, 
            ("move: cursor is not coupled"));
    page=bt_cursor_get_coupled_page(c);
    page_add_ref(page);
    node=ham_page_get_btree_node(page);
    ham_assert(btree_node_is_leaf(node), ("iterator points to internal node"));
    entry=btree_node_get_key(db, node, bt_cursor_get_coupled_index(c));

    if (key) {
        st=util_read_key(db, entry, key, 0);
        if (st) {
            page_release_ref(page);
            if (local_txn)
                (void)ham_txn_abort(&txn);
            return (st);
        }
    }

    if (record) {
        if (key_get_flags(entry)&KEY_HAS_DUPLICATES
                && bt_cursor_get_dupe_id(c)) {
            dupe_entry_t *e=bt_cursor_get_dupe_cache(c);
            record->_intflags=dupe_entry_get_flags(e);
		    record->_rid=dupe_entry_get_rid(e);
        }
        else {
            record->_intflags=key_get_flags(entry);
		    record->_rid=key_get_ptr(entry);
        }
        st=util_read_record(db, record, 0);
        if (st) {
            page_release_ref(page);
            if (local_txn)
                (void)ham_txn_abort(&txn);
            return (st);
        }
    }

    page_release_ref(page);

    if (local_txn)
        return (ham_txn_commit(&txn, 0));

    return (0);
}

ham_status_t
bt_cursor_find(ham_bt_cursor_t *c, ham_key_t *key, ham_u32_t flags)
{
    ham_status_t st;
    ham_backend_t *be=db_get_backend(bt_cursor_get_db(c));
    ham_txn_t txn;
    ham_db_t *db=cursor_get_db(c);
    ham_bool_t local_txn=db_get_txn(db) ? HAM_FALSE : HAM_TRUE;

    if (!be)
        return (HAM_NOT_INITIALIZED);
    if (!key)
        return (HAM_INV_PARAMETER);

    if (local_txn) {
        st=ham_txn_begin(&txn, db);
        if (st)
            return (st);
    }

    st=bt_cursor_set_to_nil(c);
    if (st) {
        if (local_txn)
            (void)ham_txn_abort(&txn);
        return (st);
    }

    st=btree_find_cursor((ham_btree_t *)be, c, key, 0, flags);
    if (st) {
        /* cursor is now NIL */
        if (local_txn)
            (void)ham_txn_abort(&txn);
        return (st);
    }

    if (local_txn)
        return (ham_txn_commit(&txn, 0));

    return (0);
}

ham_status_t
bt_cursor_insert(ham_bt_cursor_t *c, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    ham_db_t *db=bt_cursor_get_db(c);
    ham_btree_t *be=(ham_btree_t *)db_get_backend(db);
    ham_txn_t txn;
    ham_bool_t local_txn=db_get_txn(db) ? HAM_FALSE : HAM_TRUE;

    if (!be)
        return (HAM_NOT_INITIALIZED);
    if (!key)
        return (HAM_INV_PARAMETER);
    if (!record)
        return (HAM_INV_PARAMETER);

    if (local_txn) {
        st=ham_txn_begin(&txn, db);
        if (st)
            return (st);
    }

    /*
     * set the cursor to nil
     */
    st=bt_cursor_set_to_nil(c);
    if (st) {
        if (local_txn)
            (void)ham_txn_abort(&txn);
        return (st);
    }

    /*
     * then call the btree insert function
     */
    st=btree_insert_cursor(be, key, record, c, flags);
    if (st) {
        if (local_txn)
            (void)ham_txn_abort(&txn);
        return (st);
    }

    if (local_txn)
        return (ham_txn_commit(&txn, 0));

    return (0);
}

ham_status_t
bt_cursor_erase(ham_bt_cursor_t *c, ham_u32_t flags)
{
    ham_status_t st;
    ham_db_t *db=bt_cursor_get_db(c);
    ham_btree_t *be=(ham_btree_t *)db_get_backend(db);
    ham_txn_t txn;
    ham_bool_t local_txn=db_get_txn(db) ? HAM_FALSE : HAM_TRUE;

    if (!be)
        return (HAM_NOT_INITIALIZED);

    if (local_txn) {
        st=ham_txn_begin(&txn, db);
        if (st)
            return (st);
    }

    /*
     * coupled cursor: uncouple it
     */
    if (bt_cursor_get_flags(c)&BT_CURSOR_FLAG_COUPLED) {
        st=bt_cursor_uncouple(c, 0);
        if (st)
            return (st);
    }
    else if (!(bt_cursor_get_flags(c)&BT_CURSOR_FLAG_UNCOUPLED))
        return (HAM_CURSOR_IS_NIL);

    st=btree_erase_cursor(be, bt_cursor_get_uncoupled_key(c), c, flags);
    if (st) {
        if (local_txn)
            (void)ham_txn_abort(&txn);
        return (st);
    }

    /*
     * set cursor to nil
     */
    st=bt_cursor_set_to_nil(c);
    if (st)
        return (st);

    if (local_txn)
        return (ham_txn_commit(&txn, 0));

    return (0);
}

