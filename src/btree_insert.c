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
 * btree inserting
 *
 */

#include "config.h"

#include <string.h>
#include "db.h"
#include "error.h"
#include "keys.h"
#include "page.h"
#include "btree.h"
#include "blob.h"
#include "mem.h"
#include "util.h"
#include "btree_cursor.h"

/*
 * the insert_scratchpad_t structure helps us to propagate return values
 * from the bottom of the tree to the root.
 */
typedef struct
{
    /*
     * the backend pointer
     */
    ham_btree_t *be;

    /*
     * the flags of the ham_insert()-call
     */
    ham_u32_t flags;

    /*
     * the record which is inserted
     */
    ham_record_t *record;

    /*
     * a key; this is used to propagate SMOs (structure modification
     * operations) from a child page to a parent page
     */
    ham_key_t key;

    /*
     * a RID; this is used to propagate SMOs (structure modification
     * operations) from a child page to a parent page
     */
    ham_offset_t rid;

    /*
     * a pointer to a cursor; if this is a valid pointer, then this 
     * cursor will point to the new inserted item
     */
    ham_bt_cursor_t *cursor;

} insert_scratchpad_t;

/*
 * return values
 */
#define SPLIT     1

/*
 * flags for my_insert_nosplit()
 */
#define NOFLUSH   0x1000    /* avoid conflicts with public flags */

/*
 * this is the function which does most of the work - traversing to a 
 * leaf, inserting the key using my_insert_in_page() 
 * and performing necessary SMOs. it works recursive.
 */
static ham_status_t
my_insert_recursive(ham_page_t *page, ham_key_t *key, 
        ham_offset_t rid, insert_scratchpad_t *scratchpad);

/*
 * this function inserts a key in a page
 */
static ham_status_t
my_insert_in_page(ham_page_t *page, ham_key_t *key, 
        ham_offset_t rid, ham_u32_t flags, 
        insert_scratchpad_t *scratchpad);

/*
 * insert a key in a page; the page MUST have free slots
 */
static ham_status_t
my_insert_nosplit(ham_page_t *page, ham_key_t *key, 
        ham_offset_t rid, ham_record_t *record, 
        ham_bt_cursor_t *cursor, ham_u32_t flags, ham_bool_t force_append);

/*
 * split a page and insert the new element
 */
static ham_status_t
my_insert_split(ham_page_t *page, ham_key_t *key, 
        ham_offset_t rid, ham_u32_t flags, 
        insert_scratchpad_t *scratchpad);

static ham_status_t
my_append_key(ham_btree_t *be, ham_key_t *key, 
        ham_record_t *record, ham_bt_cursor_t *cursor, ham_u32_t flags)
{
    int cmp;
    ham_status_t st=0;
    ham_page_t *page;
    btree_node_t *node;
    ham_db_t *db=bt_cursor_get_db(cursor);
    int_key_t *entry;

    ham_assert(flags&HAM_HINT_APPEND, ("invalid flags"));
    ham_assert(bt_cursor_is_nil(cursor)==0, ("cursor must not be nil"));

    /*
     * couple the cursor, then fetch the page of the cursor
     */
    if (bt_cursor_get_flags(cursor)&BT_CURSOR_FLAG_UNCOUPLED) {
        st=bt_cursor_couple(cursor);
        if (st)
            return (st);
    }

    db_set_error(db, 0);

    page=bt_cursor_get_coupled_page(cursor);
    page_add_ref(page);
    node=ham_page_get_btree_node(page);
    ham_assert(btree_node_is_leaf(node), ("iterator points to internal node"));
    entry=btree_node_get_key(db, node, bt_cursor_get_coupled_index(cursor));

    /*
     * if the page is already full OR this page is not the right-most page
     * OR the new key is not the highest key: perform a normal insert
     */
    if (btree_node_get_right(node)
            || btree_node_get_count(node)>=btree_get_maxkeys(be)) {
        page_release_ref(page);
        flags &=~ HAM_HINT_APPEND;
        return (btree_insert_cursor(be, key, record, cursor, flags));
    }

    cmp=key_compare_pub_to_int(page, key, btree_node_get_count(node));
    if (db_get_error(db)) {
        page_release_ref(page);
        return (db_get_error(db));
    }
    if (cmp<=0) {
        page_release_ref(page);
        flags &=~ HAM_HINT_APPEND;
        return (btree_insert_cursor(be, key, record, cursor, flags));
    }

    /*
     * the page will be changed - write it to the log (if a log exists)
     */
    st=ham_log_add_page_before(page);
    if (st) {
        page_release_ref(page);
        return (st);
    }

    /*
     * OK - we're really appending the new key. 
     */
    st=my_insert_nosplit(page, key, 0, record, cursor, flags, HAM_TRUE);

    page_release_ref(page);
    return (st);
}

ham_status_t
btree_insert_cursor(ham_btree_t *be, ham_key_t *key, 
        ham_record_t *record, ham_bt_cursor_t *cursor, ham_u32_t flags)
{
    ham_status_t st;
    ham_page_t *root;
    ham_db_t *db=btree_get_db(be);
    insert_scratchpad_t scratchpad;

    /*
     * append the key? my_append_key() will try to append the key; if it 
     * fails because the key is NOT the highest key in the database or
     * because the current page is already full, it will remove the 
     * HINT_APPEND flag and call btree_insert_cursor() again
     */
    if (flags&HAM_HINT_APPEND && !bt_cursor_is_nil(cursor))
         return (my_append_key(be, key, record, cursor, flags));

    /* 
     * initialize the scratchpad 
     */
    memset(&scratchpad, 0, sizeof(scratchpad));
    scratchpad.be=be;
    scratchpad.flags=flags;
    scratchpad.record=record;
    scratchpad.cursor=cursor;

    /* 
     * get the root-page...
     */
    ham_assert(btree_get_rootpage(be)!=0, ("btree has no root page"));
    root=db_fetch_page(db, btree_get_rootpage(be), 0);

    /* 
     * ... and start the recursion 
     */
    st=my_insert_recursive(root, key, 0, &scratchpad);

    /*
     * if the root page was split, we have to create a new
     * root page.
     */
    if (st==SPLIT) {
        ham_page_t *newroot;
        btree_node_t *node;

        /*
         * the root-page will be changed...
         */
        st=ham_log_add_page_before(root);
        if (st)
            return (st);

        /*
         * allocate a new root page
         */
        newroot=db_alloc_page(db, PAGE_TYPE_B_ROOT, 0); 
        if (!newroot)
            return (db_get_error(db));
        /* clear the node header */
        memset(page_get_payload(newroot), 0, sizeof(btree_node_t));

        /* 
         * insert the pivot element and the ptr_left
         */ 
        node=ham_page_get_btree_node(newroot);
        btree_node_set_ptr_left(node, btree_get_rootpage(be));
        st=my_insert_nosplit(newroot, &scratchpad.key, 
                scratchpad.rid, scratchpad.record, scratchpad.cursor, 
                flags|NOFLUSH, HAM_FALSE);
        scratchpad.cursor=0; /* don't overwrite cursor if my_insert_nosplit
                                is called again */
        if (st) {
            if (scratchpad.key.data)
                ham_mem_free(db, scratchpad.key.data);
            return (st);
        }

        /*
         * set the new root page
         *
         * !!
         * do NOT delete the old root page - it's still in use!
         */
        btree_set_rootpage(be, page_get_self(newroot));
        be_set_dirty(be, HAM_TRUE);
        db_set_dirty(db, 1);
        page_set_type(root, PAGE_TYPE_B_INDEX);
        page_set_dirty(root);
        page_set_dirty(newroot);
    }

    /*
     * release the scratchpad-memory and return to caller
     */
    if (scratchpad.key.data)
        ham_mem_free(db, scratchpad.key.data);

    return (st);
}

ham_status_t
btree_insert(ham_btree_t *be, ham_key_t *key, 
        ham_record_t *record, ham_u32_t flags)
{
    return (btree_insert_cursor(be, key, record, 0, flags));
}

static ham_status_t
my_insert_recursive(ham_page_t *page, ham_key_t *key, 
        ham_offset_t rid, insert_scratchpad_t *scratchpad)
{
    ham_status_t st;
    ham_page_t *child;
    ham_db_t *db=page_get_owner(page);
    btree_node_t *node=ham_page_get_btree_node(page);

    /*
     * if we've reached a leaf: insert the key
     */
    if (btree_node_is_leaf(node)) 
        return (my_insert_in_page(page, key, rid, 
                    scratchpad->flags, scratchpad));

    /*
     * otherwise traverse the root down to the leaf
     */
    child=btree_traverse_tree(db, page, key, 0);
    if (!child)
        return (db_get_error(db));

    /*
     * and call this function recursively
     */
    st=my_insert_recursive(child, key, rid, scratchpad);
    switch (st) {
        /*
         * if we're done, we're done
         */
        case HAM_SUCCESS:
            break;

        /*
         * if we tried to insert a duplicate key, we're done, too
         */
        case HAM_DUPLICATE_KEY:
            break;

        /*
         * the child was split, and we have to insert a new key/rid-pair.
         */
        case SPLIT:
            st=my_insert_in_page(page, &scratchpad->key, 
                        scratchpad->rid, scratchpad->flags|HAM_OVERWRITE, 
                        scratchpad);
            break;

        /*
         * every other return value is unexpected and shouldn't happen
         */
        default:
            db_set_error(db, st);
            break;
    }

    return (st);
}

static ham_status_t
my_insert_in_page(ham_page_t *page, ham_key_t *key, 
        ham_offset_t rid, ham_u32_t flags, 
        insert_scratchpad_t *scratchpad)
{
    ham_status_t st;
    ham_size_t maxkeys=btree_get_maxkeys(scratchpad->be);
    btree_node_t *node=ham_page_get_btree_node(page);

    ham_assert(maxkeys>1, 
            ("invalid result of db_get_maxkeys(): %d", maxkeys));

    /*
     * prepare the page for modifications
     */
    st=ham_log_add_page_before(page);
    if (st)
        return (st);

    /*
     * if we can insert the new key without splitting the page: 
     * my_insert_nosplit() will do the work for us
     */
    if (btree_node_get_count(node)<maxkeys) {
        st=my_insert_nosplit(page, key, rid, 
                    scratchpad->record, scratchpad->cursor, flags, HAM_FALSE);
        scratchpad->cursor=0; /* don't overwrite cursor if my_insert_nosplit
                                 is called again */
        return (st);
    }

    /*
     * otherwise, we have to split the page.
     * but BEFORE we split, we check if the key already exists!
     */
    if (btree_node_is_leaf(node)) {
        if (btree_node_search_by_key(page_get_owner(page), page, key, 
                HAM_FIND_EXACT_MATCH)>=0) {
            if ((flags&HAM_OVERWRITE) || (flags&HAM_DUPLICATE)) {
                st=my_insert_nosplit(page, key, rid, 
                        scratchpad->record, scratchpad->cursor, 
                        flags, HAM_FALSE);
                /* don't overwrite cursor if my_insert_nosplit
                   is called again */
                scratchpad->cursor=0; 
                return (st);
            }
            else
                return (HAM_DUPLICATE_KEY);
        }
    }

    return (my_insert_split(page, key, rid, flags, scratchpad));
}

static ham_status_t
my_insert_nosplit(ham_page_t *page, ham_key_t *key, 
        ham_offset_t rid, ham_record_t *record, 
        ham_bt_cursor_t *cursor, ham_u32_t flags, ham_bool_t force_append)
{
    ham_status_t st;
    ham_size_t count, keysize, new_dupe_id=0;
    int_key_t *bte=0;
    btree_node_t *node;
    ham_db_t *db=page_get_owner(page);
    ham_bool_t exists=HAM_FALSE;
    ham_s32_t slot;

    node=ham_page_get_btree_node(page);
    count=btree_node_get_count(node);
    keysize=db_get_keysize(db);

    if (force_append) {
        slot=count;
    } 
    else if (btree_node_get_count(node)==0)
        slot=0;
    else {
        int cmp;

        st=btree_get_slot(db, page, key, &slot, &cmp);
        if (st)
            return (db_set_error(db, st));

        /* insert the new key at the beginning? */
        if (slot==-1) {
            slot++;
            bte=btree_node_get_key(db, node, slot);
            goto shift_elements;
        }

        bte=btree_node_get_key(db, node, slot);

        /*
         * key exists already
         */
        if (cmp==0) {
            if (flags&HAM_OVERWRITE) {
                /* 
                 * no need to overwrite the key - it already exists! 
                 * however, we have to overwrite the data!
                 */
                if (!btree_node_is_leaf(node)) 
                    return (HAM_SUCCESS);
            }
            else if (!(flags&HAM_DUPLICATE))
                return (HAM_DUPLICATE_KEY);

            exists=HAM_TRUE;
        }
        /*
         * otherwise, if the new key is < then the slot key, move to 
         * the next slot
         *
         * in any case, uncouple the cursors and shift all elements to the
         * right
         */
        else {
            if (cmp>0) {
                slot++;
                bte=btree_node_get_key(db, node, slot);
            }

shift_elements:
            /* uncouple all cursors */
            st=db_uncouple_all_cursors(page, slot);
            if (st)
                return (db_set_error(db, st));

            memmove(((char *)bte)+db_get_int_key_header_size()+keysize, bte,
                    (db_get_int_key_header_size()+keysize)*(count-slot));
        }
    }

    if (slot==count)
        bte=btree_node_get_key(db, node, slot);

    /*
     * if a new key is created or inserted: initialize it with zeroes
     */
    if (!exists)
        memset(bte, 0, db_get_int_key_header_size()+keysize);

    /*
     * if we're in the leaf: insert, overwrite or append the blob
     * (depends on the flags)
     */
    if (btree_node_is_leaf(node)) {
        ham_status_t st;

        st=key_set_record(db, bte, record, 
                        cursor
                            ? bt_cursor_get_dupe_id(cursor)
                            : 0, 
                        flags, &new_dupe_id);
        if (st)
            return (db_set_error(db, st));
    }
    else
        key_set_ptr(bte, rid);

    page_set_dirty(page);
    key_set_size(bte, key->size);

    /*
     * set a flag if the key is extended, and does not fit into the 
     * btree
     */
    if (key->size>db_get_keysize(db))
        key_set_flags(bte, key_get_flags(bte)|KEY_IS_EXTENDED);

    /*
     * if we have a cursor: couple it to the new key
     *
     * the cursor always points to NIL.
     */
    if (cursor) {
        if ((st=bt_cursor_set_to_nil(cursor)))
            return (db_set_error(db, st));

        ham_assert(!(bt_cursor_get_flags(cursor)&BT_CURSOR_FLAG_UNCOUPLED), 
                ("coupling an uncoupled cursor, but need a nil-cursor"));
        ham_assert(!(bt_cursor_get_flags(cursor)&BT_CURSOR_FLAG_COUPLED), 
                ("coupling a coupled cursor, but need a nil-cursor"));
        bt_cursor_set_flags(cursor, 
                bt_cursor_get_flags(cursor)|BT_CURSOR_FLAG_COUPLED);
        bt_cursor_set_coupled_page(cursor, page);
        bt_cursor_set_coupled_index(cursor, slot);
        bt_cursor_set_dupe_id(cursor, new_dupe_id);
        memset(bt_cursor_get_dupe_cache(cursor), 0, sizeof(dupe_entry_t));
        page_add_cursor(page, (ham_cursor_t *)cursor);
    }

    /*
     * if we've overwritten a key: no need to continue, we're done
     */
    if (exists)
        return (0);

    /*
     * we insert the extended key, if necessary
     */
    key_set_key(bte, key->data, 
            db_get_keysize(db)<key->size?db_get_keysize(db):key->size);

    /*
     * if we need an extended key, allocate a blob and store
     * the blob-id in the key
     */
    if (key->size>db_get_keysize(db)) {
        ham_offset_t blobid;

        key_set_key(bte, key->data, db_get_keysize(db));

        blobid=key_insert_extended(db, page, key);
        if (!blobid)
            return (db_get_error(db));

        key_set_extended_rid(db, bte, blobid);
    }

    /*
     * update the btree node-header
     */
    btree_node_set_count(node, count+1);

    return (0);
}

static ham_status_t
my_insert_split(ham_page_t *page, ham_key_t *key, 
        ham_offset_t rid, ham_u32_t flags, 
        insert_scratchpad_t *scratchpad)
{
    int cmp;
    ham_status_t st;
    ham_page_t *newpage, *oldsib;
    int_key_t *nbte, *obte;
    btree_node_t *nbtp, *obtp, *sbtp;
    ham_size_t count, keysize;
    ham_db_t *db=page_get_owner(page);
    ham_key_t pivotkey, oldkey;
    ham_offset_t pivotrid;
	ham_u16_t pivot;

    keysize=db_get_keysize(db);

    /*
     * allocate a new page
     */
    newpage=db_alloc_page(db, PAGE_TYPE_B_INDEX, 0); 
    if (!newpage)
        return (db_get_error(db)); 
    /* clear the node header */
    memset(page_get_payload(newpage), 0, sizeof(btree_node_t));

    /*
     * move half of the key/rid-tuples to the new page
     *
     * !! recno: keys are sorted; we do a "lazy split"
     */
    nbtp=ham_page_get_btree_node(newpage);
    nbte=btree_node_get_key(db, nbtp, 0);
    obtp=ham_page_get_btree_node(page);
    obte=btree_node_get_key(db, obtp, 0);
    count=btree_node_get_count(obtp);

    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER && count>8)
        pivot=count-4;
    else
        pivot=count/2;

    /*
     * uncouple all cursors
     */
    st=db_uncouple_all_cursors(page, pivot);
    if (st)
        return (db_set_error(db, st));

    /*
     * if we split a leaf, we'll insert the pivot element in the leaf
     * page, too. in internal nodes, we don't insert it, but propagate
     * it to the parent node only.
     */
    if (btree_node_is_leaf(obtp)) {
        memcpy((char *)nbte,
               ((char *)obte)+(db_get_int_key_header_size()+keysize)*pivot, 
               (db_get_int_key_header_size()+keysize)*(count-pivot));
    }
    else {
        memcpy((char *)nbte,
               ((char *)obte)+(db_get_int_key_header_size()+keysize)*(pivot+1), 
               (db_get_int_key_header_size()+keysize)*(count-pivot-1));
    }
    
    /* 
     * store the pivot element, we'll need it later to propagate it 
     * to the parent page
     */
    nbte=btree_node_get_key(db, obtp, pivot);

    memset(&oldkey, 0, sizeof(oldkey));
    oldkey.data=key_get_key(nbte);
    oldkey.size=key_get_size(nbte);
    oldkey._flags=key_get_flags(nbte);
    if (!util_copy_key(db, &oldkey, &pivotkey)) {
        (void)db_free_page(newpage, DB_MOVE_TO_FREELIST);
        return (db_get_error(db));
    }
    pivotrid=page_get_self(newpage);

    /*
     * adjust the page count
     */
    if (btree_node_is_leaf(obtp)) {
        btree_node_set_count(obtp, pivot);
        btree_node_set_count(nbtp, count-pivot);
    }
    else {
        btree_node_set_count(obtp, pivot);
        btree_node_set_count(nbtp, count-pivot-1);
    }

    /*
     * if we're in an internal page: fix the ptr_left of the new page
     * (it points to the ptr of the pivot key)
     */ 
    if (!btree_node_is_leaf(obtp)) {
        /* 
         * nbte still contains the pivot key 
         */
        btree_node_set_ptr_left(nbtp, key_get_ptr(nbte));
    }

    /*
     * insert the new element
     */
    cmp=key_compare_pub_to_int(page, key, pivot);
    if (db_get_error(db)) 
        return (db_get_error(db));

    if (cmp>=0)
        st=my_insert_nosplit(newpage, key, rid, 
                scratchpad->record, scratchpad->cursor, 
                flags|NOFLUSH, HAM_FALSE);
    else
        st=my_insert_nosplit(page, key, rid, 
                scratchpad->record, scratchpad->cursor,
                flags|NOFLUSH, HAM_FALSE);
    if (st) 
        return (st);
    scratchpad->cursor=0; /* don't overwrite cursor if my_insert_nosplit
                             is called again */

    /*
     * fix the double-linked list of pages, and mark the pages as dirty
     */
    if (btree_node_get_right(obtp)) 
        oldsib=db_fetch_page(db, btree_node_get_right(obtp), 0);
    else
        oldsib=0;

    if (oldsib) {
        st=ham_log_add_page_before(oldsib);
        if (st)
            return (st);
    }

    btree_node_set_left (nbtp, page_get_self(page));
    btree_node_set_right(nbtp, btree_node_get_right(obtp));
    btree_node_set_right(obtp, page_get_self(newpage));
    if (oldsib) {
        sbtp=ham_page_get_btree_node(oldsib);
        btree_node_set_left(sbtp, page_get_self(newpage));
        page_set_dirty(oldsib);
    }
    page_set_dirty(newpage);
    page_set_dirty(page);

    /* 
     * propagate the pivot key to the parent page
     */
    if (scratchpad->key.data)
        ham_mem_free(db, scratchpad->key.data);
    scratchpad->key=pivotkey;
    scratchpad->rid=pivotrid;

    return (SPLIT);
}

