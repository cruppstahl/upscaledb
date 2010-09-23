/**
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
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

#include "internal_fwd_decl.h"
#include "blob.h"
#include "btree.h"
#include "btree_cursor.h"
#include "cache.h"
#include "db.h"
#include "device.h"
#include "env.h"
#include "error.h"
#include "btree_key.h"
#include "log.h"
#include "mem.h"
#include "page.h"
#include "statistics.h"
#include "txn.h"
#include "util.h"

/**
 * the insert_scratchpad_t structure helps us to propagate return values
 * from the bottom of the tree to the root.
 */
typedef struct insert_scratchpad_t
{
    /**
     * the backend pointer
     */
    ham_btree_t *be;

    /**
     * the record which is inserted
     */
    ham_record_t *record;

    /**
     * a key; this is used to propagate SMOs (structure modification
     * operations) from a child page to a parent page
     */
    ham_key_t key;

    /**
     * a RID; this is used to propagate SMOs (structure modification
     * operations) from a child page to a parent page
     */
    ham_offset_t rid;

    /**
     * a pointer to a cursor; if this is a valid pointer, then this 
     * cursor will point to the new inserted item
     */
    ham_bt_cursor_t *cursor;

} insert_scratchpad_t;

/**
 * @ref __insert_recursive B+-tree split requirement signaling 
 * return value.
 *
 * @note Shares the value space with the error codes 
 * listed in @ref ham_status_codes .
 */
#define SPLIT     1

/*
 * flags for __insert_nosplit()
 */
/* #define NOFLUSH   0x1000    -- unused */

/**
 * this is the function which does most of the work - traversing to a 
 * leaf, inserting the key using __insert_in_page() 
 * and performing necessary SMOs. it works recursive.
 */
static ham_status_t
__insert_recursive(ham_page_t *page, ham_key_t *key, 
        ham_offset_t rid, insert_scratchpad_t *scratchpad, 
        insert_hints_t *hints);

/**
 * this function inserts a key in a page
 */
static ham_status_t
__insert_in_page(ham_page_t *page, ham_key_t *key, 
        ham_offset_t rid, insert_scratchpad_t *scratchpad, 
        insert_hints_t *hints);

/**
 * insert a key in a page; the page MUST have free slots
 */
static ham_status_t
__insert_nosplit(ham_page_t *page, ham_key_t *key, 
        ham_offset_t rid, ham_record_t *record, 
        ham_bt_cursor_t *cursor, insert_hints_t *hints);

/**
 * split a page and insert the new element
 */
static ham_status_t
__insert_split(ham_page_t *page, ham_key_t *key, 
        ham_offset_t rid, insert_scratchpad_t *scratchpad, 
        insert_hints_t *hints);

static ham_status_t
__insert_cursor(ham_btree_t *be, ham_key_t *key, ham_record_t *record, 
        ham_bt_cursor_t *cursor, insert_hints_t *hints);


static ham_status_t
__append_key(ham_btree_t *be, ham_key_t *key, ham_record_t *record, 
                ham_bt_cursor_t *cursor, insert_hints_t *hints)
{
    ham_status_t st=0;
    ham_page_t *page;
    btree_node_t *node;
    ham_db_t *db;

#ifdef HAM_DEBUG
    if (cursor && !bt_cursor_is_nil(cursor)) {
        ham_assert(be_get_db(be) == bt_cursor_get_db(cursor), (0));
    }
#endif

    db = be_get_db(be);

    /* 
     * see if we get this btree leaf; if not, revert to regular scan 
     *    
     * As this is a speed-improvement hint re-using recent material, the page 
     * should still sit in the cache, or we're using old info, which should be 
     * discarded.
     */
    st = db_fetch_page(&page, db, hints->leaf_page_addr, DB_ONLY_FROM_CACHE);
    if (st)
        return st;
    if (!page) {
        hints->force_append = HAM_FALSE;
        hints->force_prepend = HAM_FALSE;
        return (__insert_cursor(be, key, record, cursor, hints));
    }

    page_lock(page);
    node=page_get_btree_node(page);
    ham_assert(btree_node_is_leaf(node), ("iterator points to internal node"));

    /*
     * if the page is already full OR this page is not the right-most page
     * when we APPEND or the left-most node when we PREPEND
     * OR the new key is not the highest key: perform a normal insert
     */
    if ((hints->force_append && btree_node_get_right(node))
            || (hints->force_prepend && btree_node_get_left(node))
            || btree_node_get_count(node) >= btree_get_maxkeys(be)) {
        page_unlock(page);
        hints->force_append = HAM_FALSE;
        hints->force_prepend = HAM_FALSE;
        return (__insert_cursor(be, key, record, cursor, hints));
    }

    /*
     * if the page is not empty: check if we append the key at the end / start
     * (depending on force_append/force_prepend),
     * or if it's actually inserted in the middle (when neither force_append 
     * or force_prepend is specified: that'd be SEQUENTIAL insertion 
     * hinting somewhere in the middle of the total key range.
     */
    if (btree_node_get_count(node)!=0) {
        int cmp_hi;
        int cmp_lo;

        hints->cost++;
        if (!hints->force_prepend) {
            cmp_hi = btree_compare_keys(db, page, key, 
                                btree_node_get_count(node)-1);
            /* key is in the middle */
            if (cmp_hi < -1) {
                page_unlock(page);
                return (ham_status_t)cmp_hi;
            }
            /* key is at the end */
            if (cmp_hi > 0) {
                if (btree_node_get_right(node)) {
                    /* not at top end of the btree, so we can't do the 
                     * fast track */
                    page_unlock(page);
                    //hints->flags &= ~HAM_HINT_APPEND;
                    hints->force_append = HAM_FALSE;
                    hints->force_prepend = HAM_FALSE;
                    return (__insert_cursor(be, key, record, cursor, hints));
                }

                hints->force_append = HAM_TRUE;
                hints->force_prepend = HAM_FALSE;
            }
        }
        else { /* hints->force_prepend is true */
            /* not bigger than the right-most node while we 
             * were trying to APPEND */
            cmp_hi = -1;
        }

        if (!hints->force_append) {
            cmp_lo = btree_compare_keys(db, page, key, 0);
            /* in the middle range */
            if (cmp_lo < -1) {
                page_unlock(page);
                return ((ham_status_t)cmp_lo);
            }
            /* key is at the start of page */
            if (cmp_lo < 0) {
                if (btree_node_get_left(node)) {
                    /* not at bottom end of the btree, so we can't 
                     * do the fast track */
                    page_unlock(page);
                    //hints->flags &= ~HAM_HINT_PREPEND;
                    hints->force_append = HAM_FALSE;
                    hints->force_prepend = HAM_FALSE;
                    return (__insert_cursor(be, key, record, cursor, hints));
                }

                hints->force_append = HAM_FALSE;
                hints->force_prepend = HAM_TRUE;
            }
        }
        else { /* hints->force_prepend is true */
            /* not smaller than the left-most node while we were 
             * trying to PREPEND */
            cmp_lo = +1;
        }

        /* handle inserts in the middle range */
        if (cmp_lo >= 0 && cmp_hi <= 0) {
            /*
             * Depending on where we are in the btree, the current key either
             * is going to end up in the middle of the given node/page,
             * OR the given key is out of range of the given leaf node.
             */
            if (hints->force_append || hints->force_prepend) {
                /*
                 * when prepend or append is FORCED, we are expected to 
                 * add keys ONLY at the beginning or end of the btree
                 * key range. Clearly the current key does not fit that
                 * criterium.
                 */
                page_unlock(page);
                //hints->flags &= ~HAM_HINT_PREPEND;
                hints->force_append = HAM_FALSE;
                hints->force_prepend = HAM_FALSE;
                return (__insert_cursor(be, key, record, cursor, hints));
            }

            /* 
             * we discovered that the key must be inserted in the middle 
             * of the current leaf.
             * 
             * It does not matter whether the current leaf is at the start or
             * end of the btree range; as we need to add the key in the middle
             * of the current leaf, that info alone is enough to continue with
             * the fast track insert operation.
             */
            ham_assert(!hints->force_prepend && !hints->force_append, (0));
        }

        ham_assert((hints->force_prepend + hints->force_append) < 2, 
                ("Either APPEND or PREPEND flag MAY be set, but not both"));
    }
    else { /* empty page: force insertion in slot 0 */
        hints->force_append = HAM_FALSE;
        hints->force_prepend = HAM_TRUE;
    }

    /*
     * the page will be changed - write it to the log (if a log exists)
     */
    st=ham_log_add_page_before(page);
    if (st) {
        page_unlock(page);
        return (st);
    }

    /*
     * OK - we're really appending/prepending the new key.
     */
    ham_assert(hints->force_append || hints->force_prepend, (0));
    st=__insert_nosplit(page, key, 0, record, cursor, hints);

    page_unlock(page);
    return (st);
}

static ham_status_t
__insert_cursor(ham_btree_t *be, ham_key_t *key, ham_record_t *record, 
                ham_bt_cursor_t *cursor, insert_hints_t *hints)
{
    ham_status_t st;
    ham_page_t *root;
    ham_db_t *db=be_get_db(be);
    ham_env_t *env = db_get_env(db);
    insert_scratchpad_t scratchpad;

    ham_assert(hints->force_append == HAM_FALSE, (0));
    ham_assert(hints->force_prepend == HAM_FALSE, (0));

    /* 
     * initialize the scratchpad 
     */
    memset(&scratchpad, 0, sizeof(scratchpad));
    scratchpad.be=be;
    scratchpad.record=record;
    scratchpad.cursor=cursor;

    /* 
     * get the root-page...
     */
    ham_assert(btree_get_rootpage(be)!=0, ("btree has no root page"));
    st=db_fetch_page(&root, db, btree_get_rootpage(be), 0);
    ham_assert(st ? root == NULL : 1, (0));
    if (st)
        return st;

    /* 
     * ... and start the recursion 
     */
    st=__insert_recursive(root, key, 0, &scratchpad, hints);

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
        st=db_alloc_page(&newroot, db, PAGE_TYPE_B_ROOT, 0); 
        ham_assert(st ? newroot == NULL : 1, (0));
        if (st)
            return (st);
        ham_assert(page_get_owner(newroot), (""));
        /* clear the node header */
        memset(page_get_payload(newroot), 0, sizeof(btree_node_t));

        stats_page_is_nuked(db, root, HAM_TRUE);

        /* 
         * insert the pivot element and the ptr_left
         */ 
        node=page_get_btree_node(newroot);
        btree_node_set_ptr_left(node, btree_get_rootpage(be));
        st=__insert_nosplit(newroot, &scratchpad.key, 
                scratchpad.rid, scratchpad.record, scratchpad.cursor, 
                hints);
        ham_assert(!(scratchpad.key.flags & HAM_KEY_USER_ALLOC), (0));
        scratchpad.cursor=0; /* don't overwrite cursor if __insert_nosplit
                                is called again */
        if (st) {
            ham_assert(!(scratchpad.key.flags & HAM_KEY_USER_ALLOC), (0));
            if (scratchpad.key.data)
                allocator_free(env_get_allocator(env), scratchpad.key.data);
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
        env_set_dirty(env);
        if (env_get_cache(env) && (page_get_type(root)!=PAGE_TYPE_B_INDEX)) 
        {
            /*
             *  As we re-purpose a page, we will reset its pagecounter
             * as well to signal its first use as the new type assigned
             * here.
             */
            cache_update_page_access_counter(root, env_get_cache(env), 0);
        }
        page_set_type(root, PAGE_TYPE_B_INDEX);
        page_set_dirty(root);
        page_set_dirty(newroot);
    }

    /*
     * release the scratchpad-memory and return to caller
     */
    ham_assert(!(scratchpad.key.flags & HAM_KEY_USER_ALLOC), (0));
    if (scratchpad.key.data)
        allocator_free(env_get_allocator(env), scratchpad.key.data);

    return (st);
}

ham_status_t
btree_insert_cursor(ham_btree_t *be, ham_key_t *key, 
        ham_record_t *record, ham_bt_cursor_t *cursor, ham_u32_t flags)
{
    ham_status_t st;
    ham_db_t *db=be_get_db(be);
    insert_hints_t hints = {flags, flags, (ham_cursor_t *)cursor, 0, 
        HAM_FALSE, HAM_FALSE, HAM_FALSE, 0, NULL, -1};

    btree_insert_get_hints(&hints, db, key);

    /*
     * append the key? __append_key() will try to append the key; if it 
     * fails because the key is NOT the highest key in the database or
     * because the current page is already full, it will remove the 
     * HINT_APPEND flag and call btree_insert_cursor() again
     */
    if (hints.force_append || hints.force_prepend) {
        ham_assert(hints.try_fast_track, (0));
        st = __append_key(be, key, record, cursor, &hints);
    }
    else {
        hints.force_append = HAM_FALSE;
        hints.force_prepend = HAM_FALSE;
        st = __insert_cursor(be, key, record, cursor, &hints);
    }

     if (st) {
        stats_update_insert_fail(db, &hints);
     }
     else {
        stats_update_insert(db, hints.processed_leaf_page, &hints);
        stats_update_any_bound(db, hints.processed_leaf_page, 
                key, hints.flags, hints.processed_slot);
     }

    return (st);
}

/**                                                                 
 * insert (or update) a key in the index                            
 *                                                                  
 * the backend is responsible for inserting or updating the         
 * record. (see blob.h for blob management functions)               
 *
 * @note This is a B+-tree 'backend' method.
 */                                                                 
ham_status_t
btree_insert(ham_btree_t *be, ham_key_t *key, 
        ham_record_t *record, ham_u32_t flags)
{
    return (btree_insert_cursor(be, key, record, 0, flags));
}

static ham_status_t
__insert_recursive(ham_page_t *page, ham_key_t *key, 
        ham_offset_t rid, insert_scratchpad_t *scratchpad, 
        insert_hints_t *hints)
{
    ham_status_t st;
    ham_page_t *child;
    ham_db_t *db=page_get_owner(page);
    btree_node_t *node=page_get_btree_node(page);

    /*
     * if we've reached a leaf: insert the key
     */
    if (btree_node_is_leaf(node)) 
        return (__insert_in_page(page, key, rid, scratchpad, hints));

    /*
     * otherwise traverse the root down to the leaf
     */
    hints->cost += 2;
    st=btree_traverse_tree(&child, 0, db, page, key);
    if (!child)
        return st ? st : HAM_INTERNAL_ERROR;

    /*
     * and call this function recursively
     */
    st=__insert_recursive(child, key, rid, scratchpad, hints);
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
            hints->flags |= HAM_OVERWRITE;
            st=__insert_in_page(page, &scratchpad->key, 
                        scratchpad->rid, scratchpad, hints);
            ham_assert(!(scratchpad->key.flags & HAM_KEY_USER_ALLOC), (0));
            hints->flags = hints->original_flags;
            break;

        /*
         * every other return value is unexpected and shouldn't happen
         */
        default:
            break;
    }

    return (st);
}

static ham_status_t
__insert_in_page(ham_page_t *page, ham_key_t *key, 
        ham_offset_t rid, insert_scratchpad_t *scratchpad, 
        insert_hints_t *hints)
{
    ham_status_t st;
    ham_size_t maxkeys=btree_get_maxkeys(scratchpad->be);
    btree_node_t *node=page_get_btree_node(page);

    ham_assert(maxkeys>1, 
            ("invalid result of db_get_maxkeys(): %d", maxkeys));
    ham_assert(hints->force_append == HAM_FALSE, (0));
    ham_assert(hints->force_prepend == HAM_FALSE, (0));

    /*
     * prepare the page for modifications
     */
    st=ham_log_add_page_before(page);
    if (st)
        return (st);

    /*
     * if we can insert the new key without splitting the page: 
     * __insert_nosplit() will do the work for us
     */
    if (btree_node_get_count(node)<maxkeys) {
        st=__insert_nosplit(page, key, rid, 
                    scratchpad->record, scratchpad->cursor, hints);
        scratchpad->cursor=0; /* don't overwrite cursor if __insert_nosplit
                                 is called again */
        return (st);
    }

    /*
     * otherwise, we have to split the page.
     * but BEFORE we split, we check if the key already exists!
     */
    if (btree_node_is_leaf(node)) {
        ham_s32_t idx;

        hints->cost++;
        idx = btree_node_search_by_key(page_get_owner(page), page, key, 
                            HAM_FIND_EXACT_MATCH);
        /* key exists! */
        if (idx>=0) {
            ham_assert((hints->flags & (HAM_DUPLICATE_INSERT_BEFORE
                                |HAM_DUPLICATE_INSERT_AFTER
                                |HAM_DUPLICATE_INSERT_FIRST
                                |HAM_DUPLICATE_INSERT_LAST)) 
                    ? (hints->flags & HAM_DUPLICATE)
                    : 1, (0)); 
            if (!(hints->flags & (HAM_OVERWRITE | HAM_DUPLICATE))) 
                return (HAM_DUPLICATE_KEY);
            st=__insert_nosplit(page, key, rid, 
                    scratchpad->record, scratchpad->cursor, hints);
            /* don't overwrite cursor if __insert_nosplit is called again */
            scratchpad->cursor=0; 
            return (st);
        }
    }

    return (__insert_split(page, key, rid, scratchpad, hints));
}

static ham_status_t
__insert_nosplit(ham_page_t *page, ham_key_t *key, 
        ham_offset_t rid, ham_record_t *record, 
        ham_bt_cursor_t *cursor, insert_hints_t *hints)
{
    ham_status_t st;
    ham_u16_t count;
    ham_size_t keysize;
    ham_size_t new_dupe_id = 0;
    btree_key_t *bte = 0;
    btree_node_t *node;
    ham_db_t *db=page_get_owner(page);
    ham_bool_t exists = HAM_FALSE;
    ham_s32_t slot;

    ham_assert(page_get_owner(page), (0));
    ham_assert(device_get_env(page_get_device(page)) == db_get_env(page_get_owner(page)), (0));

    node=page_get_btree_node(page);
    count=btree_node_get_count(node);
    keysize=db_get_keysize(db);

    if (btree_node_get_count(node)==0)
    {
        slot = 0;
    }
    else if (hints->force_append) 
    {
        slot = count;
    } 
    else if (hints->force_prepend) 
    {
        /* insert at beginning; shift all up by one */
        slot = 0;
    } 
    else 
    {
        int cmp;

        hints->cost++;
        st=btree_get_slot(db, page, key, &slot, &cmp);
        if (st)
            return (st);

        /* insert the new key at the beginning? */
        if (slot == -1) 
        {
            slot = 0;
        }
        else
        {
            /*
             * key exists already
             */
            if (cmp == 0) 
            {
                if (hints->flags & HAM_OVERWRITE) 
                {
                    /* 
                     * no need to overwrite the key - it already exists! 
                     * however, we have to overwrite the data!
                     */
                    if (!btree_node_is_leaf(node)) 
                        return (HAM_SUCCESS);
                }
                else if (!(hints->flags & HAM_DUPLICATE))
                    return (HAM_DUPLICATE_KEY);

                /* do NOT shift keys up to make room; just overwrite the current [slot] */
                exists = HAM_TRUE;
            }
            else 
            {
                /*
                 * otherwise, if the new key is > then the slot key, move to 
                 * the next slot
                 */
                if (cmp > 0) 
                {
                    slot++;
                }
            }
        }
    }

    /*
     * in any case, uncouple the cursors and see if we must shift any elements to the
     * right
     */
    bte=btree_node_get_key(db, node, slot);
    ham_assert(bte, (0));

    if (!exists)
    {
        if (count > slot)
        {
            /* uncouple all cursors & shift any elements following [slot] */
            st=bt_uncouple_all_cursors(page, slot);
            if (st)
                return (st);

            hints->cost += stats_memmove_cost((db_get_int_key_header_size()+keysize)*(count-slot));
            memmove(((char *)bte)+db_get_int_key_header_size()+keysize, bte,
                    (db_get_int_key_header_size()+keysize)*(count-slot));
        }

        /*
         * if a new key is created or inserted: initialize it with zeroes
         */
        memset(bte, 0, db_get_int_key_header_size()+keysize);
    }

    /*
     * if we're in the leaf: insert, overwrite or append the blob
     * (depends on the flags)
     */
    if (btree_node_is_leaf(node)) 
    {
        ham_status_t st;

        hints->cost++;
        st=key_set_record(db, bte, record, 
                        cursor
                            ? bt_cursor_get_dupe_id(cursor)
                            : 0, 
                        hints->flags, &new_dupe_id);
        if (st)
            return (st);
        
        hints->processed_leaf_page = page;
        hints->processed_slot = slot;
    }
    else
    {
        key_set_ptr(bte, rid);
    }

    page_set_dirty(page);
    key_set_size(bte, key->size);

    /*
     * set a flag if the key is extended, and does not fit into the 
     * btree
     */
    if (key->size > db_get_keysize(db))
        key_set_flags(bte, key_get_flags(bte)|KEY_IS_EXTENDED);

    /*
     * if we have a cursor: couple it to the new key
     *
     * the cursor always points to NIL.
     */
    if (cursor) 
    {
        if ((st=bt_cursor_set_to_nil(cursor)))
            return (st);

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
            db_get_keysize(db) < key->size ? db_get_keysize(db) : key->size);

    /*
     * if we need an extended key, allocate a blob and store
     * the blob-id in the key
     */
    if (key->size > db_get_keysize(db)) 
    {
        ham_offset_t blobid;

        key_set_key(bte, key->data, db_get_keysize(db));

        st=key_insert_extended(&blobid, db, page, key);
        ham_assert(st ? blobid == 0 : 1, (0));
        if (!blobid)
            return st ? st : HAM_INTERNAL_ERROR;

        key_set_extended_rid(db, bte, blobid);
    }

    /*
     * update the btree node-header
     */
    btree_node_set_count(node, count+1);

    return (0);
}

static ham_status_t
__insert_split(ham_page_t *page, ham_key_t *key, 
        ham_offset_t rid, insert_scratchpad_t *scratchpad, 
        insert_hints_t *hints)
{
    int cmp;
    ham_status_t st;
    ham_page_t *newpage, *oldsib;
    btree_key_t *nbte, *obte;
    btree_node_t *nbtp, *obtp, *sbtp;
    ham_size_t count, keysize;
    ham_db_t *db=page_get_owner(page);
    ham_env_t *env = db_get_env(db);
    ham_key_t pivotkey, oldkey;
    ham_offset_t pivotrid;
    ham_u16_t pivot;
    ham_bool_t pivot_at_end=HAM_FALSE;

    ham_assert(page_get_owner(page), (0));
    ham_assert(device_get_env(page_get_device(page)) 
            == db_get_env(page_get_owner(page)), (0));

    ham_assert(hints->force_append == HAM_FALSE, (0));

    keysize=db_get_keysize(db);

    /*
     * allocate a new page
     */
    hints->cost++;
    st=db_alloc_page(&newpage, db, PAGE_TYPE_B_INDEX, 0); 
    ham_assert(st ? page == NULL : 1, (0));
    ham_assert(!st ? page  != NULL : 1, (0));
    if (st)
        return st; 
    ham_assert(page_get_owner(newpage), (""));
    /* clear the node header */
    memset(page_get_payload(newpage), 0, sizeof(btree_node_t));

    stats_page_is_nuked(db, page, HAM_TRUE);

    /*
     * move half of the key/rid-tuples to the new page
     *
     * !! recno: keys are sorted; we do a "lazy split"
     */
    nbtp=page_get_btree_node(newpage);
    nbte=btree_node_get_key(db, nbtp, 0);
    obtp=page_get_btree_node(page);
    obte=btree_node_get_key(db, obtp, 0);
    count=btree_node_get_count(obtp);

    /*
     * for databases with sequential access (this includes recno databases):
     * do not split in the middle, but at the very end of the page
     *
     * if this page is the right-most page in the index, and this key is 
     * inserted at the very end, then we select the same pivot as for
     * sequential access
     */
    if (db_get_data_access_mode(db)&HAM_DAM_SEQUENTIAL_INSERT)
        pivot_at_end=HAM_TRUE;
    else if (btree_node_get_right(obtp)==0) {
        cmp=btree_compare_keys(db, page, key, btree_node_get_count(obtp)-1);
        if (cmp>0)
            pivot_at_end=HAM_TRUE;
    }

    /*
     * internal pages set the count of the new page to count-pivot-1 (because
     * the pivot element will become ptr_left of the new page).
     * by using pivot=count-2 we make sure that at least 1 element will remain
     * in the new node.
     */
    if (pivot_at_end) {
        pivot=count-2;
    }
    else {
        pivot=count/2;
    }

    /*
     * uncouple all cursors
     */
    st=bt_uncouple_all_cursors(page, pivot);
    if (st)
        return (st);

    /*
     * if we split a leaf, we'll insert the pivot element in the leaf
     * page, too. in internal nodes, we don't insert it, but propagate
     * it to the parent node only.
     */
    if (btree_node_is_leaf(obtp)) {
        hints->cost += stats_memmove_cost((db_get_int_key_header_size()+keysize)*(count-pivot));
        memcpy((char *)nbte,
               ((char *)obte)+(db_get_int_key_header_size()+keysize)*pivot, 
               (db_get_int_key_header_size()+keysize)*(count-pivot));
    }
    else {
        hints->cost += stats_memmove_cost((db_get_int_key_header_size()+keysize)*(count-pivot-1));
        memcpy((char *)nbte,
               ((char *)obte)+(db_get_int_key_header_size()+keysize)*(pivot+1), 
               (db_get_int_key_header_size()+keysize)*(count-pivot-1));
    }
    
    /* 
     * store the pivot element, we'll need it later to propagate it 
     * to the parent page
     */
    nbte=btree_node_get_key(db, obtp, pivot);

    memset(&pivotkey, 0, sizeof(pivotkey));
    memset(&oldkey, 0, sizeof(oldkey));
    oldkey.data=key_get_key(nbte);
    oldkey.size=key_get_size(nbte);
    oldkey._flags=key_get_flags(nbte);
    st = db_copy_key(db, &oldkey, &pivotkey);
    if (st) {
        (void)db_free_page(newpage, DB_MOVE_TO_FREELIST);
        goto fail_dramatically;
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
    hints->cost++;
    cmp=btree_compare_keys(db, page, key, pivot);
    if (cmp < -1) 
    {
        st = (ham_status_t)cmp;
        goto fail_dramatically;
    }

    if (cmp>=0)
        st=__insert_nosplit(newpage, key, rid, 
                scratchpad->record, scratchpad->cursor, hints);
    else
        st=__insert_nosplit(page, key, rid, 
                scratchpad->record, scratchpad->cursor, hints);
    if (st) 
    {
        goto fail_dramatically;
    }
    scratchpad->cursor=0; /* don't overwrite cursor if __insert_nosplit
                             is called again */

    /*
     * fix the double-linked list of pages, and mark the pages as dirty
     */
    if (btree_node_get_right(obtp)) 
    {
        st=db_fetch_page(&oldsib, db, btree_node_get_right(obtp), 0);
        if (st)
            goto fail_dramatically;
    }
    else
    {
        oldsib=0;
    }

    if (oldsib) {
        st=ham_log_add_page_before(oldsib);
        if (st)
            goto fail_dramatically;
    }

    btree_node_set_left (nbtp, page_get_self(page));
    btree_node_set_right(nbtp, btree_node_get_right(obtp));
    btree_node_set_right(obtp, page_get_self(newpage));
    if (oldsib) {
        sbtp=page_get_btree_node(oldsib);
        btree_node_set_left(sbtp, page_get_self(newpage));
        page_set_dirty(oldsib);
    }
    page_set_dirty(newpage);
    page_set_dirty(page);

    /* 
     * propagate the pivot key to the parent page
     */
    ham_assert(!(scratchpad->key.flags & HAM_KEY_USER_ALLOC), (0));
    if (scratchpad->key.data)
        allocator_free(env_get_allocator(env), scratchpad->key.data);
    scratchpad->key=pivotkey;
    scratchpad->rid=pivotrid;
    ham_assert(!(scratchpad->key.flags & HAM_KEY_USER_ALLOC), (0));

    return (SPLIT);

fail_dramatically:
    ham_assert(!(pivotkey.flags & HAM_KEY_USER_ALLOC), (0));
    if (pivotkey.data)
        allocator_free(env_get_allocator(env), pivotkey.data);
    return st;
}

