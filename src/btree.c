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
#include "keys.h"
#include "error.h"
#include "btree.h"

#define offsetof(type, member) ((size_t) &((type *)0)->member)

ham_status_t 
btree_get_slot(ham_db_t *db, ham_txn_t *txn, ham_page_t *page, 
        ham_key_t *key, ham_s32_t *slot)
{
    int cmp;
    btree_node_t *node=ham_page_get_btree_node(page);
    ham_size_t r=btree_node_get_count(node)-1, l=1, i, last;

    /*
     * otherwise perform a binary search for the *smallest* element, which 
     * is >= the key
     */
    last=(ham_size_t)-1;

    ham_assert(btree_node_get_count(node)>0, 
            "node is empty", 0);

    if (r==0) {
        cmp=key_compare_pub_to_int(txn, page, key, 0);
        if (db_get_error(db))
            return (db_get_error(db));
        *slot=cmp<0 ? -1 : 0;
        return (0);
    }

    while (r>=0) {
        /* get the median item; if it's identical with the "last" item, 
         * we've found the slot */
        i=(l+r)/2;

        if (i==last) {
            *slot=i;
            return (0);
        }
        
        /* compare it against the key */
        cmp=key_compare_pub_to_int(txn, page, key, i);
        if (db_get_error(db))
            return (db_get_error(db));

        /* found it? */
        if (cmp==0) {
            *slot=i;
            return (0);
        }

        /* if the key is bigger than the item: search "to the left" */
        if (cmp<0) {
            if (r==0) {
                *slot=-1;
                return (0);
            }
            r=i-1;
        }
        else {
            last=i;
            l=i+1;
        }
    }
    
    return (0);
}

static ham_size_t
my_calc_maxkeys(ham_db_t *db)
{
    union page_union_t u;
    ham_size_t p, k, max;

    /* 
     * a btree page is always P bytes long, where P is the pagesize of 
     * the database. 
     */
    p=db_get_pagesize(db);

    /* every btree page has a header where we can't store entries */
    p-=offsetof(btree_node_t, _entries);

    /* every page has a header where we can't store entries */
    p-=sizeof(u._s)-1;

    /*
     * compute the size of a key, k. 
     */
    k=db_get_keysize(db)+sizeof(key_t)-1;

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
    ham_page_t *root;
    ham_size_t maxkeys;
    ham_u16_t keysize;
    ham_db_t *db=btree_get_db(be);
    ham_u8_t *indexdata=db_get_indexdata(db);

    /* 
     * initialize the database with a good default value;
     * 32byte is the size of a first level cache line for most modern
     * processors; adjust the keysize, so the keys are aligned to
     * 32byte (or 16)
     */
    keysize=db_get_keysize(db);
    if (keysize==0)
        keysize=16-sizeof(key_t)-1;

    if (!db_get_keysize(db) && (st=db_set_keysize(db, keysize))) {
        ham_log("failed to set keysize: 0x%x", st);
        return (db_get_error(db));
    }

    /*
     * calculate the maximum number of keys for this page, 
     * and make sure that this number is even
     */
    maxkeys=my_calc_maxkeys(db);
    btree_set_maxkeys(be, maxkeys);

    /*
     * allocate a new root page
     */
    root=db_alloc_page(db, PAGE_TYPE_ROOT, 0, 0);
    if (!root)
        return (db_get_error(db));
    btree_set_rootpage(be, page_get_self(root));

    /*
     * store root address and maxkeys 
     */
    *(ham_u16_t    *)&indexdata[0]=ham_h2db16(maxkeys);
    *(ham_offset_t *)&indexdata[2]=ham_h2db_offset(page_get_self(root));
    db_set_dirty(db, 1);
    return (0);
}

static ham_status_t 
my_fun_open(ham_btree_t *be, ham_u32_t flags)
{
    ham_offset_t rootadd;
    ham_u16_t maxkeys;
    ham_db_t *db=btree_get_db(be);
    ham_u8_t *indexdata=db_get_indexdata(db);

    /*
     * load root address and maxkeys
     */
    maxkeys=ham_db2h16(*(ham_u16_t *)&indexdata[0]);
    rootadd=ham_db2h_offset(*(ham_offset_t *)&indexdata[2]);
    btree_set_rootpage(be, rootadd);
    btree_set_maxkeys(be, maxkeys);
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
    btree->_fun_enumerate=btree_enumerate;
    btree->_fun_check_integrity=btree_check_integrity;
    return (0);
}

ham_page_t *
btree_traverse_tree(ham_db_t *db, ham_txn_t *txn, ham_page_t *page, 
        ham_key_t *key, ham_s32_t *idxptr)
{
    ham_status_t st;
    ham_s32_t slot;
    key_t *bte;
    btree_node_t *node=ham_page_get_btree_node(page);

    /*
     * make sure that we're not in a leaf page, and that the 
     * page is not empty
     */
    ham_assert(btree_node_get_count(node)>0, 0, 0);
    ham_assert(btree_node_get_ptr_left(node)!=0, 0, 0);

    st=btree_get_slot(db, txn, page, key, &slot);
    if (st)
        return (0);

    if (idxptr)
        *idxptr=slot;

    if (slot==-1)
        return (db_fetch_page(db, txn, btree_node_get_ptr_left(node), 0));
    else {
        bte=btree_node_get_key(db, node, slot);
        ham_assert(key_get_flags(bte)==0 || 
                key_get_flags(bte)==KEY_IS_EXTENDED,
                "invalid key flags 0x%x", key_get_flags(bte));
        return (db_fetch_page(db, txn, key_get_ptr(bte), 0));
    }
}

ham_s32_t 
btree_node_search_by_key(ham_db_t *db, ham_txn_t *txn, 
        ham_page_t *page, ham_key_t *key)
{
    int cmp;
    ham_size_t i;
    btree_node_t *node=ham_page_get_btree_node(page);

    db_set_error(db, 0);

    for (i=0; i<btree_node_get_count(node); i++) {
        cmp=key_compare_int_to_pub(txn, page, i, key);
        if (db_get_error(db))
            return (-1);
        if (cmp==0)
            return (i);
    }

    return (-1);
}
