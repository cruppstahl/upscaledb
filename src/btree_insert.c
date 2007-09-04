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
 * btree inserting
 *
 */

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
        ham_bt_cursor_t *cursor, ham_u32_t flags);

/*
 * split a page and insert the new element
 */
static ham_status_t
my_insert_split(ham_page_t *page, ham_key_t *key, 
        ham_offset_t rid, ham_u32_t flags, 
        insert_scratchpad_t *scratchpad);


ham_status_t
btree_insert_cursor(ham_btree_t *be, ham_key_t *key, 
        ham_record_t *record, ham_bt_cursor_t *cursor, ham_u32_t flags)
{
    ham_status_t st;
    ham_page_t *root;
    ham_db_t *db=btree_get_db(be);
    insert_scratchpad_t scratchpad;

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
                flags|NOFLUSH);
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
        btree_set_dirty(be, HAM_TRUE);
        db_set_dirty(db, 1);
        page_set_type(root, PAGE_TYPE_B_INDEX);
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
         * the child was split, and we have to insert a new (key/rid)-tuple.
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
    ham_size_t maxkeys=btree_get_maxkeys(scratchpad->be);
    btree_node_t *node=ham_page_get_btree_node(page);

    ham_assert(maxkeys>1, 
            ("invalid result of db_get_maxkeys(): %d", maxkeys));

    /*
     * if we can insert the new key without splitting the page: 
     * my_insert_nosplit() will do the work for us
     */
    if (btree_node_get_count(node)<maxkeys) {
        ham_status_t st=my_insert_nosplit(page, key, rid, 
                    scratchpad->record, scratchpad->cursor, flags);
        scratchpad->cursor=0; /* don't overwrite cursor if my_insert_nosplit
                                 is called again */
        return (st);
    }

    /*
     * otherwise, we have to split the page.
     * but BEFORE we split, we check if the key already exists!
     *
     * TODO
     * btree_node_search_by_key is called inside my_insert_split; remove
     * at least one of the calls for optimization
     */
    if (btree_node_is_leaf(node)) {
        if (btree_node_search_by_key(page_get_owner(page), page, key)>=0) {
            if ((flags&HAM_OVERWRITE) || (flags&HAM_DUPLICATE)) {
                ham_status_t st=my_insert_nosplit(page, key, rid, 
                        scratchpad->record, scratchpad->cursor, flags);
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
        ham_bt_cursor_t *cursor, ham_u32_t flags)
{
    int cmp;
    ham_status_t st;
    ham_size_t i, count, keysize;
    int_key_t *bte=0;
    btree_node_t *node;
    ham_db_t *db=page_get_owner(page);
    ham_bool_t exists=HAM_FALSE;
    ham_s32_t slot;
    ham_u32_t oldflags;
    ham_offset_t dupeid=0;

    node=ham_page_get_btree_node(page);
    count=btree_node_get_count(node);
    keysize=db_get_keysize(db);

    if (btree_node_get_count(node)==0)
        slot=0;
    else {
        st=btree_get_slot(db, page, key, &slot);
        if (st)
            return (db_set_error(db, st));

        /* insert the new key at the beginning? */
        if (slot==-1) {
            slot++;
            bte=btree_node_get_key(db, node, slot);
            goto shift_elements;
        }

        cmp=key_compare_int_to_pub(page, (ham_u16_t)slot, key);
        if (db_get_error(db))
            return (db_get_error(db));

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
         */
        else if (cmp<0) {
            /* uncouple all cursors */
            st=db_uncouple_all_cursors(page);
            if (st)
                return (db_set_error(db, st));

            slot++;
            bte=btree_node_get_key(db, node, slot);
            memmove(((char *)bte)+sizeof(int_key_t)-1+keysize, bte,
                    (sizeof(int_key_t)-1+keysize)*(count-slot));
        }
        /*
         * otherwise, the current slot is the first key, which is 
         * bigger than the new key; this is where we insert the new key
         */
        else {
shift_elements:
            /* uncouple all cursors */
            st=db_uncouple_all_cursors(page);
            if (st)
                return (db_set_error(db, st));

            /* shift all keys one position to the right */
            memmove(((char *)bte)+sizeof(int_key_t)-1+keysize, bte,
                    (sizeof(int_key_t)-1+keysize)*(count-slot));
        }
    }

    i=slot;

    if (i==count) 
        bte=btree_node_get_key(db, node, count);

    oldflags=key_get_flags(bte);
    key_set_flags(bte, 0);

    /*
     * if we're in the leaf: insert the blob, and append the blob-id 
     * in this node; otherwise just append the entry (rid)
     *
     * if the record's size is <= sizeof(ham_offset_t), we don't allocate
     * a blob but store the record data directly in the offset
     *
     * in an in-memory-database, we don't use the blob management, but 
     * allocate a single chunk of memory, and store the memory address
     * in rid
     */
    if (btree_node_is_leaf(node)) {
        ham_status_t st;

        /*
         * insert a duplicate key? 
         *
         * if the previous key is SMALL, TINY or EMPTY: allocate a blob
         * structure to start a linked list
         *
         * then allocate another blob for the dupe and insert the dupe
         * at the head of the linked list
         */
#if 0
        if (exists && (flags&HAM_DUPLICATE)) {
            ham_offset_t old_blobid=0;

            if ((oldflags&KEY_BLOB_SIZE_TINY) ||
                (oldflags&KEY_BLOB_SIZE_SMALL) ||
                (oldflags&KEY_BLOB_SIZE_EMPTY)) {
                ham_offset_t bid=key_get_ptr(bte);
                ham_size_t size=0;

                if (oldflags&KEY_BLOB_SIZE_TINY) {
                    char *p=(char *)&bid;
                    size=p[sizeof(ham_offset_t)-1];
                }
                if (oldflags&KEY_BLOB_SIZE_SMALL)
                    size=sizeof(ham_offset_t);

                st=blob_allocate(db, size ? (void *)&bid : 0, size, 
                        0, 0, &old_blobid);
                if (st)
                    return (st);
            }
            else {
                old_blobid=key_get_ptr(bte);
            }

            if (flags&HAM_DUPLICATE_INSERT_AFTER) {
                if (bt_cursor_get_dupe_id(cursor)) {
                    st=blob_allocate_after(db, record->data, record->size, 0, 
                            bt_cursor_get_dupe_id(cursor), &rid);
                }
                else {
                    st=blob_allocate(db, record->data, record->size, 0, 
                            old_blobid, &rid);
                }
                dupeid=rid;
            }
            else if (flags&HAM_DUPLICATE_INSERT_LAST) {
                if (bt_cursor_get_dupe_id(cursor)) {
                    st=blob_allocate_last(db, record->data, record->size, 0, 
                            bt_cursor_get_dupe_id(cursor), &rid);
                }
                else {
                    st=blob_allocate_last(db, record->data, record->size, 0, 
                            old_blobid, &rid);
                }
                dupeid=rid;
            }
            else if (flags&HAM_DUPLICATE_INSERT_BEFORE) {
                if (cursor && bt_cursor_get_dupe_id(cursor)) {
                    st=blob_allocate_before(db, record->data, record->size, 0, 
                            bt_cursor_get_dupe_id(cursor), &rid);
                }
                else {
                    st=blob_allocate(db, record->data, record->size, 0, 
                            old_blobid, &rid);
                }
                dupeid=rid;
            }
            else { /* if (flags&HAM_DUPLICATE_INSERT_FIRST) */
                /* allocate the new blob, and set the 'next' pointer */
                st=blob_allocate(db, record->data, record->size, 0, 
                        old_blobid, &rid);
                dupeid=rid;
            }

            if (st)
                return (st);
        }
#endif
if (0) ;

        /*
         * if the record is big: replace the blob
         */
        else if (record->size>sizeof(ham_offset_t)) {
            /*
             * make sure that we only call blob_overwrite(), if there IS a blob
             * to replace! otherwise call blob_allocate()
             */
            if (exists && (flags&HAM_OVERWRITE)) {
                if (!((oldflags&KEY_BLOB_SIZE_TINY) ||
                    (oldflags&KEY_BLOB_SIZE_SMALL) ||
                    (oldflags&KEY_BLOB_SIZE_EMPTY))) {
                    st=blob_overwrite(db, key_get_ptr(bte), record->data, 
                                record->size, 0, &rid);
                    if (st)
                        return (st);

                    /*
                     * if the blob was relocated: update each cursor which
                     * is coupled to this blob
                     */
                    if (key_get_ptr(bte)!=rid) {
                        ham_bt_cursor_t *c=
                            (ham_bt_cursor_t *)db_get_cursors(db);
                        while (c) {
                            if (bt_cursor_get_dupe_id(c)==key_get_ptr(bte))
                                bt_cursor_set_dupe_id(c, (ham_size_t)rid);
                            c=(ham_bt_cursor_t *)cursor_get_next(c);
                        }
                    }
                }
                else {
                    st=blob_allocate(db, record->data, 
                                record->size, 0, 0, &rid);
                    if (st)
                        return (st);
                }
            }
            else {
                if ((st=blob_allocate(db, record->data, 
                                record->size, 0, 0, &rid)))
                    return (st);
            }
        }

        /*
         * if the record's size is <= sizeof(ham_offset_t), store the data
         * directly in the offset
         *
         * if the record's size is < sizeof(ham_offset_t), the last byte
         * in &rid is the size of the data. if the record is empty, we just
         * set the "empty"-flag.
         *
         * before, reset the key-flags
         */
        else {
            /*
             * if a blob exists, free it
             */
            if (exists && (flags&HAM_OVERWRITE)) {
                if (!((oldflags&KEY_BLOB_SIZE_TINY) ||
                    (oldflags&KEY_BLOB_SIZE_SMALL) ||
                    (oldflags&KEY_BLOB_SIZE_EMPTY))) {
                    st=blob_free(db, key_get_ptr(bte), 0);
                    if (st)
                        return (st);
                }
            }

            /*
             * now set the new key flags 
             */
            if (record->size==0) {
                key_set_flags(bte, key_get_flags(bte)|KEY_BLOB_SIZE_EMPTY);
            }
            else if (record->size<=sizeof(ham_offset_t)) {
                memcpy(&rid, record->data, record->size);
                if (record->size<sizeof(ham_offset_t)) {
                    char *p=(char *)&rid;
                    p[sizeof(ham_offset_t)-1]=record->size;
                    key_set_flags(bte, key_get_flags(bte)|KEY_BLOB_SIZE_TINY);
                }
                else {
                    key_set_flags(bte, key_get_flags(bte)|KEY_BLOB_SIZE_SMALL);
                }
            }
        }
    }

    key_set_ptr(bte, rid);
    page_set_dirty(page, 1);
    key_set_size(bte, key->size);

    /*
     * set a flag if the key is extended, and does not fit into the 
     * btree
     */
    if (key->size>db_get_keysize(db))
        key_set_flags(bte, key_get_flags(bte)|KEY_IS_EXTENDED);

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

    btree_node_set_count(node, count+1);

    /*
     * if we have a cursor: couple it to the new key
     */
    if (cursor) {
        ham_assert(!(bt_cursor_get_flags(cursor)&BT_CURSOR_FLAG_UNCOUPLED), 
                ("coupling an uncoupled cursor, but need a nil-cursor"));
        ham_assert(!(bt_cursor_get_flags(cursor)&BT_CURSOR_FLAG_COUPLED), 
                ("coupling a coupled cursor, but need a nil-cursor"));
        bt_cursor_set_flags(cursor, 
                bt_cursor_get_flags(cursor)|BT_CURSOR_FLAG_COUPLED);
        bt_cursor_set_coupled_page(cursor, page);
        bt_cursor_set_coupled_index(cursor, slot);
        bt_cursor_set_dupe_id(cursor, (ham_size_t)dupeid);
        page_add_cursor(page, (ham_cursor_t *)cursor);
    }

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
     * uncouple all cursors
     */
    st=db_uncouple_all_cursors(page);
    if (st)
        return (db_set_error(db, st));

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
     * if we split a leaf, we'll insert the pivot element in the leaf
     * page, too. in internal nodes, we don't insert it, but propagate
     * it to the parent node only.
     */
    if (btree_node_is_leaf(obtp)) {
        memcpy((char *)nbte,
               ((char *)obte)+(sizeof(int_key_t)-1+keysize)*pivot, 
               (sizeof(int_key_t)-1+keysize)*(count-pivot));
    }
    else {
        memcpy((char *)nbte,
               ((char *)obte)+(sizeof(int_key_t)-1+keysize)*(pivot+1), 
               (sizeof(int_key_t)-1+keysize)*(count-pivot-1));
    }
    
    /* 
     * store the pivot element, we'll need it later to propagate it 
     * to the parent page
     */
    nbte=btree_node_get_key(db, obtp, pivot);

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
    cmp=key_compare_int_to_pub(page, pivot, key);
    if (db_get_error(db)) 
        return (db_get_error(db));

    if (cmp<=0)
        st=my_insert_nosplit(newpage, key, rid, 
                scratchpad->record, scratchpad->cursor, flags|NOFLUSH);
    else
        st=my_insert_nosplit(page, key, rid, 
                scratchpad->record, scratchpad->cursor, flags|NOFLUSH);
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
    btree_node_set_left (nbtp, page_get_self(page));
    btree_node_set_right(nbtp, btree_node_get_right(obtp));
    btree_node_set_right(obtp, page_get_self(newpage));
    if (oldsib) {
        sbtp=ham_page_get_btree_node(oldsib);
        btree_node_set_left(sbtp, page_get_self(newpage));
        page_set_dirty(oldsib, 1);
    }
    page_set_dirty(newpage, 1);
    page_set_dirty(page, 1);

    /* 
     * propagate the pivot key to the parent page
     */
    if (scratchpad->key.data)
        ham_mem_free(db, scratchpad->key.data);
    scratchpad->key=pivotkey;
    scratchpad->rid=pivotrid;

    return (SPLIT);
}

#if HAM_DEBUG
void
pp(ham_page_t *page)
{
    ham_size_t i, j, len, count;
    int cmp;
    int_key_t *bte=0;
    btree_node_t *node;
    ham_db_t *db=page_get_owner(page);

    printf("page 0x%llx\n", (unsigned long long)page_get_self(page));

    node=ham_page_get_btree_node(page);
    count=btree_node_get_count(node);

    for (i=0; i<count; i++) {
        bte=btree_node_get_key(db, node, i);

        printf("%03d: ", i);

        if (key_get_flags(bte)&KEY_IS_EXTENDED) 
            len=db_get_keysize(page_get_owner(page))-sizeof(ham_offset_t);
        else
            len=key_get_size(bte);

        for (j=0; j<len; j++)
            /*printf("%02x ", (unsigned char)(key_get_key(bte)[j]));*/
            printf("%c", key_get_key(bte)[j]);

        printf("(%d bytes, 0x%x flags)  -> rid 0x%llx\n", key_get_size(bte), 
                key_get_flags(bte), (unsigned long long)key_get_ptr(bte));
    }

    /*
     * verify page
     */
    for (i=0; i<count-1; i++) {
        cmp=key_compare_int_to_int(page, i, i+1);
        ham_assert(db_get_error(db)==0, ("key compare failed"));
        if (cmp>=0) {
            ham_log(("integrity check failed in page 0x%llx: item #%d "
                    "< item #%d\n", page_get_self(page), i, i+1));
            return;
        }
    }
}
#endif
