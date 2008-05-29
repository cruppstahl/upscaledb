/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 *
 * implementation of btree.h
 *
 */

#include <string.h>
#include "config.h"
#include "db.h"
#include "error.h"
#include "btree.h"
#include "keys.h"

ham_status_t 
btree_get_slot(ham_db_t *db, ham_page_t *page, 
        ham_key_t *key, ham_s32_t *slot, int *pcmp)
{
    int cmp;
    btree_node_t *node=ham_page_get_btree_node(page);
    ham_size_t r=btree_node_get_count(node)-1, l=1, i, last;

    /*
     * perform a binary search for the *smallest* element, which 
     * is >= the key
     */
    last=(ham_size_t)-1;

    ham_assert(btree_node_get_count(node)>0, 
            ("node is empty"));

    /*
     * only one element in this node?
     */
    if (r==0) {
        cmp=key_compare_pub_to_int(page, key, 0);
        if (db_get_error(db))
            return (db_get_error(db));
        *slot=cmp<0 ? -1 : 0;
        goto bail;
    }

    while (r>=0) {
        /* get the median item; if it's identical with the "last" item, 
         * we've found the slot */
        i=(l+r)/2;

        if (i==last) {
            *slot=i;
            goto bail;
        }
        
        /* compare it against the key */
        cmp=key_compare_pub_to_int(page, key, (ham_u16_t)i);
        if (db_get_error(db))
            return (db_get_error(db));

        /* found it? */
        if (cmp==0) {
            *slot=i;
            goto bail;
        }

        /* if the key is bigger than the item: search "to the left" */
        if (cmp<0) {
            if (r==0) {
                *slot=-1;
                goto bail;
            }
            r=i-1;
        }
        else {
            last=i;
            l=i+1;
        }
    }
    
bail:
    if (pcmp && *slot!=-1) {
        *pcmp=key_compare_int_to_pub(page, (ham_u16_t)*slot, key);
        if (db_get_error(db))
            return (db_get_error(db));
    }

    return (0);
}

static ham_size_t
my_calc_maxkeys(ham_size_t pagesize, ham_u16_t keysize)
{
    union page_union_t u;
    ham_size_t p, k, max;

    /* 
     * a btree page is always P bytes long, where P is the pagesize of 
     * the database. 
     */
    p=pagesize;

    /* every btree page has a header where we can't store entries */
    p-=OFFSET_OF(btree_node_t, _entries);

    /* every page has a header where we can't store entries */
    p-=sizeof(u._s)-1;

    /*
     * compute the size of a key, k. 
     */
    k=keysize+sizeof(int_key_t)-1;

    /* 
     * make sure that MAX is an even number, otherwise we can't calculate
     * MIN (which is MAX/2)
     */
    max=p/k;
    return (max&1 ? max-1 : max);
}

static ham_status_t 
my_fun_create(ham_btree_t *be, ham_u16_t keysize, ham_u32_t flags)
{
    ham_page_t *root;
    ham_size_t maxkeys;
    ham_db_t *db=btree_get_db(be);
    ham_u8_t *indexdata=db_get_indexdata_at(db, db_get_indexdata_offset(db));

    /*
     * allocate a new root page
     */
    root=db_alloc_page(db, PAGE_TYPE_B_ROOT, PAGE_IGNORE_FREELIST);
    if (!root)
        return (db_get_error(db));

    memset(page_get_raw_payload(root), 0, 
            sizeof(btree_node_t)+sizeof(union page_union_t));

    /*
     * calculate the maximum number of keys for this page, 
     * and make sure that this number is even
     */
    maxkeys=my_calc_maxkeys(db_get_pagesize(db), keysize);
    btree_set_maxkeys(be, maxkeys);
    be_set_dirty(be, HAM_TRUE);
    be_set_keysize(be, keysize);
    be_set_flags(be, flags);

    btree_set_rootpage(be, page_get_self(root));

    *(ham_u16_t    *)&indexdata[ 2]=ham_h2db16(maxkeys);
    *(ham_u16_t    *)&indexdata[ 4]=ham_h2db16(keysize);
    *(ham_offset_t *)&indexdata[ 8]=ham_h2db_offset(page_get_self(root));
    *(ham_u32_t    *)&indexdata[16]=ham_h2db32(flags);
    *(ham_offset_t *)&indexdata[20]=ham_h2db_offset(0);
    db_set_dirty(db, 1);

    return (0);
}

static ham_status_t 
my_fun_open(ham_btree_t *be, ham_u32_t flags)
{
    ham_offset_t rootadd, recno;
    ham_u16_t maxkeys, keysize;
    ham_db_t *db=btree_get_db(be);
    ham_u8_t *indexdata=db_get_indexdata_at(db, db_get_indexdata_offset(db));

    /*
     * load root address and maxkeys (first two bytes are the
     * database name)
     */
    maxkeys=ham_db2h16     (*(ham_u16_t    *)&indexdata[ 2]);
    keysize=ham_db2h16     (*(ham_u16_t    *)&indexdata[ 4]);
    rootadd=ham_db2h_offset(*(ham_offset_t *)&indexdata[ 8]);
    flags  =ham_db2h32     (*(ham_u32_t    *)&indexdata[16]);
    recno  =ham_db2h_offset(*(ham_offset_t *)&indexdata[20]);

    btree_set_rootpage(be, rootadd);
    btree_set_maxkeys(be, maxkeys);
    be_set_keysize(be, keysize);
    be_set_flags(be, flags);
    be_set_recno(be, recno);

    return (0);
}

static ham_status_t
my_fun_flush(ham_btree_t *be)
{
    ham_db_t *db=btree_get_db(be);
    ham_u8_t *indexdata=db_get_indexdata_at(db, db_get_indexdata_offset(db));

    /*
     * nothing todo if the backend was not touched
     */
    if (!be_is_dirty(be))
        return (0);

    /*
     * store root address and maxkeys (first two bytes are the
     * database name)
     */
    *(ham_u16_t    *)&indexdata[ 2]=ham_h2db16(btree_get_maxkeys(be));
    *(ham_u16_t    *)&indexdata[ 4]=ham_h2db16(be_get_keysize(be));
    *(ham_offset_t *)&indexdata[ 8]=ham_h2db_offset(btree_get_rootpage(be));
    *(ham_u32_t    *)&indexdata[16]=ham_h2db32(be_get_flags(be));
    *(ham_offset_t *)&indexdata[20]=ham_h2db_offset(be_get_recno(be));

    db_set_dirty(db, HAM_TRUE);
    be_set_dirty(be, HAM_FALSE);

    return (0);
}

static ham_status_t
my_fun_close(ham_btree_t *be)
{
    /*
     * just flush the backend info if it's dirty
     */
    return (my_fun_flush(be));
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
    btree->_fun_flush=my_fun_flush;
    btree->_fun_delete=my_fun_delete;
    btree->_fun_find=btree_find;
    btree->_fun_insert=btree_insert;
    btree->_fun_erase=btree_erase;
    btree->_fun_enumerate=btree_enumerate;
#ifdef HAM_ENABLE_INTERNAL
    btree->_fun_check_integrity=btree_check_integrity;
#else
    btree->_fun_check_integrity=0;
#endif
    return (0);
}

ham_page_t *
btree_traverse_tree(ham_db_t *db, ham_page_t *page, 
        ham_key_t *key, ham_s32_t *idxptr)
{
    ham_status_t st;
    ham_s32_t slot;
    int_key_t *bte;
    btree_node_t *node=ham_page_get_btree_node(page);

    /*
     * make sure that we're not in a leaf page, and that the 
     * page is not empty
     */
    ham_assert(btree_node_get_count(node)>0, (0));
    ham_assert(btree_node_get_ptr_left(node)!=0, (0));

    st=btree_get_slot(db, page, key, &slot, 0);
    if (st)
        return (0);

    if (idxptr)
        *idxptr=slot;

    if (slot==-1)
        return (db_fetch_page(db, btree_node_get_ptr_left(node), 0));
    else {
        bte=btree_node_get_key(db, node, slot);
        ham_assert(key_get_flags(bte)==0 || 
                key_get_flags(bte)==KEY_IS_EXTENDED,
                ("invalid key flags 0x%x", key_get_flags(bte)));
        return (db_fetch_page(db, key_get_ptr(bte), 0));
    }
}

ham_s32_t 
btree_node_search_by_key(ham_db_t *db, ham_page_t *page, ham_key_t *key)
{
    int cmp=-1;
    ham_s32_t slot;
    ham_status_t st;
    btree_node_t *node=ham_page_get_btree_node(page);

    db_set_error(db, 0);

    if (btree_node_get_count(node)==0)
        return (-1);

    st=btree_get_slot(db, page, key, &slot, &cmp);
    if (st) {
        db_set_error(db, st);
        return (-1);
    }

#if 0 /* not needed... */
    if (slot!=-1) {
        int cmp=key_compare_int_to_pub(page, (ham_u16_t)slot, key);
        if (cmp)
            return (-1);
    }
#endif

    if (cmp)
        return (-1);

    return (slot);
}

