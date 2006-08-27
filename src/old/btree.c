/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 * implementation of btree.h
 *
 */

#include <string.h>
#include <ham/config.h>
#include "db.h"
#include "error.h"
#include "btree.h"
#include "cachemgr.h"

#define offsetof(type, member) ((size_t) &((type *)0)->member)

static ham_size_t
my_calc_maxkeys(ham_db_t *db)
{
    ham_size_t p, k, max;

    /* 
     * a btree page is always P bytes long, where P is the pagesize of 
     * the database. 
     */
    p=db_get_pagesize(db);

    /*
     * every btree page has a header where we can't store entries
     */
    p-=offsetof(btree_node_t, _entries);

    /*
     * compute the size of a key, k. 
     */
    k=db_get_keysize(db)+sizeof(btree_entry_t)-1;

    /* 
     * make sure that MAX is an even number, otherwise we can't calculate
     * MIN (which is MAX/2)
     */
    max=p/k;
    return (max&1 ? max-1 : max);
}

static ham_status_t 
my_fun_create(ham_btree_t *be, ham_u32_t flags)
{
    ham_status_t st;
    ham_txn_t txn;
    ham_page_t *root;
    ham_size_t maxkeys;
    ham_u16_t keysize;
    ham_db_t *db=btree_get_db(be);

    /* 
     * initialize the database with a good default value;
     * 32byte is the size of a first level cache line for most modern
     * processors; adjust the keysize, so the keys are aligned to
     * 32byte (or 16)
     */
    keysize=db_get_keysize(db);
    if (keysize==0)
        keysize=16-sizeof(btree_entry_t)-1;

    if (!db_get_keysize(db) && (st=ham_set_keysize(db, keysize))) {
        ham_log("failed to set keysize: 0x%x", st);
        return (db_get_error(db));
    }

    /*
     * calculate the maximum number of keys for this page, 
     * and make sure that this number is even
     */
    maxkeys=my_calc_maxkeys(db);
    db_set_maxkeys(db, maxkeys);

    /*
     * allocate a new root page
     */
    st=ham_txn_begin(&txn, db, 0);
    if (st)
        return (st);
    root=txn_alloc_page(&txn, 0);
    if (!root)
        return (db_get_error(db));

    /* 
     * set the whole page to zero and flush the page 
     */
    memset(root->_pers._payload, 0, db_get_pagesize(db));
    btree_set_rootpage(be, page_get_self(root));
    return (ham_txn_commit(&txn, 0));
}

static ham_status_t 
my_fun_open(ham_btree_t *be, ham_u32_t flags)
{
    /*
     * nothing to do
     */
    return (0);
}

static ham_status_t
my_fun_close(ham_btree_t *be)
{
    /*
     * nothing to do
     */
    return (0);
}

static void
my_fun_delete(ham_btree_t *be)
{
    /*
     * nothing to do
     */
}

ham_status_t
btree_create(ham_btree_t *btree, ham_db_t *db, ham_u32_t flags)
{
    memset(btree, 0, sizeof(ham_btree_t));
    btree->_db=db;
    btree->_fun_create=my_fun_create;
    btree->_fun_open=my_fun_open;
    btree->_fun_close=my_fun_close;
    btree->_fun_delete=my_fun_delete;
    btree->_fun_find=btree_find;
    btree->_fun_insert=btree_insert;
    btree->_fun_erase=btree_erase;
    btree->_fun_dump=btree_dump;
    btree->_fun_check_integrity=btree_check_integrity;
    return (0);
}

/*
 * TODO - one day, we should optimize this function, i.e. with 
 * binary search
 */
ham_u32_t
btree_node_search_by_key(ham_db_t *db, ham_page_t *page, ham_key_t *key)
{
    int cmp;
    ham_size_t i;
    btree_entry_t *entry;
    btree_node_t *node=ham_page_get_btree_node(page);

    db_set_error(db, 0);

    for (i=0; i<btree_node_get_count(node); i++) {
        entry=btree_node_get_entry(db, node, i);
        cmp=db_compare_keys(db, page,
                i, btree_entry_get_flags(entry), btree_entry_get_key(entry), 
                btree_entry_get_real_size(db, entry), 
                btree_entry_get_size(entry),
                -1, key->_flags, key->data, key->size, key->size);
        if (db_get_error(db))
            return (0);
        if (cmp==0)
            return (i+1);
    }

    return (0);
}

ham_u32_t
btree_node_search_by_ptr(ham_db_t *db, btree_node_t *node, ham_offset_t ptr)
{
    ham_size_t i;
    btree_entry_t *entry;

    for (i=0; i<btree_node_get_count(node); i++) {
        entry=btree_node_get_entry(db, node, i);
        if (btree_entry_get_ptr(entry)==ptr)
            return (i+1);
    }

    return (0);
}
