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
#include "cursor.h"
#include "cache.h"
#include "db.h"
#include "device.h"
#include "env.h"
#include "error.h"
#include "btree_key.h"
#include "log.h"
#include "mem.h"
#include "page.h"
#include "btree_stats.h"
#include "txn.h"
#include "util.h"

using namespace ham;


/* a unittest hook triggered when a page is split */
void (*g_BTREE_INSERT_SPLIT_HOOK)(void);

/**
 * the insert_scratchpad_t structure helps us to propagate return values
 * from the bottom of the tree to the root.
 */
typedef struct insert_scratchpad_t
{
    /**
     * the backend pointer
     */
    BtreeBackend *be;

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
    btree_cursor_t *cursor;

    /** the current transaction */
    Transaction *txn;

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
__insert_recursive(Page *page, ham_key_t *key,
                ham_offset_t rid, insert_scratchpad_t *scratchpad,
                insert_hints_t *hints);

/**
 * this function inserts a key in a page
 */
static ham_status_t
__insert_in_page(Page *page, ham_key_t *key,
                ham_offset_t rid, insert_scratchpad_t *scratchpad,
                insert_hints_t *hints);

/**
 * insert a key in a page; the page MUST have free slots
 */
static ham_status_t
__insert_nosplit(Page *page, Transaction *txn, ham_key_t *key,
                ham_offset_t rid, ham_record_t *record,
                btree_cursor_t *cursor, insert_hints_t *hints);

/**
 * split a page and insert the new element
 */
static ham_status_t
__insert_split(Page *page, ham_key_t *key,
                ham_offset_t rid, insert_scratchpad_t *scratchpad,
                insert_hints_t *hints);

static ham_status_t
__insert_cursor(BtreeBackend *be, Transaction *txn, ham_key_t *key,
        ham_record_t *record, btree_cursor_t *cursor, insert_hints_t *hints);


static ham_status_t
__append_key(BtreeBackend *be, Transaction *txn, ham_key_t *key,
        ham_record_t *record, btree_cursor_t *cursor, insert_hints_t *hints)
{
    ham_status_t st=0;
    Page *page;
    btree_node_t *node;
    Database *db;

#ifdef HAM_DEBUG
    if (cursor && !btree_cursor_is_nil(cursor)) {
        ham_assert(be->get_db()==btree_cursor_get_db(cursor));
    }
#endif

    db=be->get_db();

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
        return (__insert_cursor(be, txn, key, record, cursor, hints));
    }

    node=page_get_btree_node(page);
    ham_assert(btree_node_is_leaf(node));

    /*
     * if the page is already full OR this page is not the right-most page
     * when we APPEND or the left-most node when we PREPEND
     * OR the new key is not the highest key: perform a normal insert
     */
    if ((hints->force_append && btree_node_get_right(node))
            || (hints->force_prepend && btree_node_get_left(node))
            || btree_node_get_count(node) >= be->get_maxkeys()) {
        hints->force_append = HAM_FALSE;
        hints->force_prepend = HAM_FALSE;
        return (__insert_cursor(be, txn, key, record, cursor, hints));
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
                return (ham_status_t)cmp_hi;
            }
            /* key is at the end */
            if (cmp_hi > 0) {
                if (btree_node_get_right(node)) {
                    /* not at top end of the btree, so we can't do the
                     * fast track */
                    //hints->flags &= ~HAM_HINT_APPEND;
                    hints->force_append = HAM_FALSE;
                    hints->force_prepend = HAM_FALSE;
                    return (__insert_cursor(be, txn, key, record, cursor, hints));
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
                return ((ham_status_t)cmp_lo);
            }
            /* key is at the start of page */
            if (cmp_lo < 0) {
                if (btree_node_get_left(node)) {
                    /* not at bottom end of the btree, so we can't
                     * do the fast track */
                    //hints->flags &= ~HAM_HINT_PREPEND;
                    hints->force_append = HAM_FALSE;
                    hints->force_prepend = HAM_FALSE;
                    return (__insert_cursor(be, txn, key, record, cursor, hints));
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
                //hints->flags &= ~HAM_HINT_PREPEND;
                hints->force_append = HAM_FALSE;
                hints->force_prepend = HAM_FALSE;
                return (__insert_cursor(be, txn, key, record, cursor, hints));
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
            ham_assert(!hints->force_prepend && !hints->force_append);
        }

        ham_assert((hints->force_prepend + hints->force_append) < 2);
    }
    else { /* empty page: force insertion in slot 0 */
        hints->force_append = HAM_FALSE;
        hints->force_prepend = HAM_TRUE;
    }

    /*
     * OK - we're really appending/prepending the new key.
     */
    ham_assert(hints->force_append || hints->force_prepend);
    st=__insert_nosplit(page, txn, key, 0, record, cursor, hints);

    return (st);
}

static ham_status_t
__insert_cursor(BtreeBackend *be, Transaction *txn, ham_key_t *key,
        ham_record_t *record, btree_cursor_t *cursor, insert_hints_t *hints)
{
    ham_status_t st;
    Page *root;
    Database *db=be->get_db();
    Environment *env = db->get_env();
    insert_scratchpad_t scratchpad;

    ham_assert(hints->force_append == HAM_FALSE);
    ham_assert(hints->force_prepend == HAM_FALSE);

    /*
     * initialize the scratchpad
     */
    memset(&scratchpad, 0, sizeof(scratchpad));
    scratchpad.be=be;
    scratchpad.record=record;
    scratchpad.cursor=cursor;
    scratchpad.txn=txn;

    /*
     * get the root-page...
     */
    ham_assert(be->get_rootpage()!=0);
    st=db_fetch_page(&root, db, be->get_rootpage(), 0);
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
        Page *newroot;
        btree_node_t *node;

        /*
         * allocate a new root page
         */
        st=db_alloc_page(&newroot, db, Page::TYPE_B_ROOT, 0);
        if (st)
            return (st);
        ham_assert(newroot->get_db());
        /* clear the node header */
        memset(newroot->get_payload(), 0, sizeof(btree_node_t));

        btree_stats_page_is_nuked(db, root, HAM_TRUE);

        /*
         * insert the pivot element and the ptr_left
         */
        node=page_get_btree_node(newroot);
        btree_node_set_ptr_left(node, be->get_rootpage());
        st=__insert_nosplit(newroot, scratchpad.txn, &scratchpad.key,
                scratchpad.rid, scratchpad.record, scratchpad.cursor, hints);
        ham_assert(!(scratchpad.key.flags & HAM_KEY_USER_ALLOC));
        scratchpad.cursor=0; /* don't overwrite cursor if __insert_nosplit
                                is called again */
        if (st) {
            ham_assert(!(scratchpad.key.flags & HAM_KEY_USER_ALLOC));
            if (scratchpad.key.data)
                env->get_allocator()->free(scratchpad.key.data);
            return (st);
        }

        /*
         * set the new root page
         *
         * !!
         * do NOT delete the old root page - it's still in use! also add the
         * root page to the changeset to make sure that the changes are logged
         */
        be->set_rootpage(newroot->get_self());
        be->do_flush_indexdata();
        if (env->get_flags()&HAM_ENABLE_RECOVERY)
            env->get_changeset().add_page(env->get_header_page());
        root->set_type(Page::TYPE_B_INDEX);
        root->set_dirty(true);
        newroot->set_dirty(true);
    }

    /*
     * release the scratchpad-memory and return to caller
     */
    ham_assert(!(scratchpad.key.flags & HAM_KEY_USER_ALLOC));
    if (scratchpad.key.data)
        env->get_allocator()->free(scratchpad.key.data);

    return (st);
}

ham_status_t
BtreeBackend::do_insert_cursor(Transaction *txn, ham_key_t *key,
                ham_record_t *record, btree_cursor_t *cursor, ham_u32_t flags)
{
    ham_status_t st;
    Database *db=get_db();
    insert_hints_t hints = {flags, flags,
        cursor ? (ham_cursor_t *)btree_cursor_get_parent(cursor) : 0, 0,
        HAM_FALSE, HAM_FALSE, HAM_FALSE, 0, NULL, -1};

    btree_insert_get_hints(&hints, db, key);

    /*
     * append the key? __append_key() will try to append the key; if it
     * fails because the key is NOT the highest key in the database or
     * because the current page is already full, it will remove the
     * HINT_APPEND flag and call this function again
     */
    if (hints.force_append || hints.force_prepend) {
        ham_assert(hints.try_fast_track);
        st = __append_key(this, txn, key, record, cursor, &hints);
    }
    else {
        hints.force_append = HAM_FALSE;
        hints.force_prepend = HAM_FALSE;
        st = __insert_cursor(this, txn, key, record, cursor, &hints);
    }

     if (st) {
        btree_stats_update_insert_fail(db, &hints);
     }
     else {
        btree_stats_update_insert(db, hints.processed_leaf_page, &hints);
        btree_stats_update_any_bound(HAM_OPERATION_STATS_INSERT,
                db, hints.processed_leaf_page,
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
BtreeBackend::do_insert(Transaction *txn, ham_key_t *key,
                ham_record_t *record, ham_u32_t flags)
{
    return (do_insert_cursor(txn, key, record, 0, flags));
}

static ham_status_t
__insert_recursive(Page *page, ham_key_t *key,
                ham_offset_t rid, insert_scratchpad_t *scratchpad,
                insert_hints_t *hints)
{
    ham_status_t st;
    Page *child;
    Database *db=page->get_db();
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
    if (st)
        return (st);

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
            ham_assert(!(scratchpad->key.flags & HAM_KEY_USER_ALLOC));
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
__insert_in_page(Page *page, ham_key_t *key,
                ham_offset_t rid, insert_scratchpad_t *scratchpad,
                insert_hints_t *hints)
{
    ham_status_t st;
    ham_size_t maxkeys=scratchpad->be->get_maxkeys();
    btree_node_t *node=page_get_btree_node(page);

    ham_assert(maxkeys>1);
    ham_assert(hints->force_append == HAM_FALSE);
    ham_assert(hints->force_prepend == HAM_FALSE);

    /*
     * if we can insert the new key without splitting the page:
     * __insert_nosplit() will do the work for us
     */
    if (btree_node_get_count(node)<maxkeys) {
        st=__insert_nosplit(page, scratchpad->txn, key, rid,
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
        idx = btree_node_search_by_key(page->get_db(), page, key,
                            HAM_FIND_EXACT_MATCH);
        /* key exists! */
        if (idx>=0) {
            ham_assert((hints->flags & (HAM_DUPLICATE_INSERT_BEFORE
                                |HAM_DUPLICATE_INSERT_AFTER
                                |HAM_DUPLICATE_INSERT_FIRST
                                |HAM_DUPLICATE_INSERT_LAST))
                    ? (hints->flags & HAM_DUPLICATE)
                    : 1);
            if (!(hints->flags & (HAM_OVERWRITE | HAM_DUPLICATE)))
                return (HAM_DUPLICATE_KEY);
            st=__insert_nosplit(page, scratchpad->txn, key, rid,
                    scratchpad->record, scratchpad->cursor, hints);
            /* don't overwrite cursor if __insert_nosplit is called again */
            scratchpad->cursor=0;
            return (st);
        }
    }

    return (__insert_split(page, key, rid, scratchpad, hints));
}

static ham_status_t
__insert_nosplit(Page *page, Transaction *txn, ham_key_t *key,
                ham_offset_t rid, ham_record_t *record,
                btree_cursor_t *cursor, insert_hints_t *hints)
{
    ham_status_t st;
    ham_u16_t count;
    ham_size_t keysize;
    ham_size_t new_dupe_id = 0;
    btree_key_t *bte = 0;
    btree_node_t *node;
    Database *db=page->get_db();
    ham_bool_t exists = HAM_FALSE;
    ham_s32_t slot;

    ham_assert(page->get_db());

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
    ham_assert(bte);

    if (!exists)
    {
        if (count > slot)
        {
            /* uncouple all cursors & shift any elements following [slot] */
            st=btree_uncouple_all_cursors(page, slot);
            if (st)
                return (st);

            hints->cost += btree_stats_memmove_cost((db_get_int_key_header_size()+keysize)*(count-slot));
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
        st=key_set_record(db, txn, bte, record,
                        cursor
                            ? btree_cursor_get_dupe_id(cursor)
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

    page->set_dirty(true);
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
    if (cursor) {
        btree_cursor_get_parent(cursor)->set_to_nil(Cursor::CURSOR_BTREE);

        ham_assert(!btree_cursor_is_uncoupled(cursor));
        ham_assert(!btree_cursor_is_coupled(cursor));
        btree_cursor_set_flags(cursor,
                btree_cursor_get_flags(cursor)|BTREE_CURSOR_FLAG_COUPLED);
        btree_cursor_set_coupled_page(cursor, page);
        btree_cursor_set_coupled_index(cursor, slot);
        btree_cursor_set_dupe_id(cursor, new_dupe_id);
        memset(btree_cursor_get_dupe_cache(cursor), 0, sizeof(dupe_entry_t));
        page->add_cursor(btree_cursor_get_parent(cursor));
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
        if (st)
            return (st);
        ham_assert(blobid!=0);

        key_set_extended_rid(db, bte, blobid);
    }

    /*
     * update the btree node-header
     */
    btree_node_set_count(node, count+1);

    return (0);
}

static ham_status_t
__insert_split(Page *page, ham_key_t *key,
                ham_offset_t rid, insert_scratchpad_t *scratchpad,
                insert_hints_t *hints)
{
    int cmp;
    ham_status_t st;
    Page *newpage, *oldsib;
    btree_key_t *nbte, *obte;
    btree_node_t *nbtp, *obtp, *sbtp;
    ham_size_t count, keysize;
    Database *db=page->get_db();
    Environment *env = db->get_env();
    ham_key_t pivotkey, oldkey;
    ham_offset_t pivotrid;
    ham_u16_t pivot;
    ham_bool_t pivot_at_end=HAM_FALSE;

    ham_assert(page->get_db());

    ham_assert(hints->force_append == HAM_FALSE);

    keysize=db_get_keysize(db);

    /*
     * allocate a new page
     */
    hints->cost++;
    st=db_alloc_page(&newpage, db, Page::TYPE_B_INDEX, 0);
    ham_assert(st ? page == NULL : 1);
    ham_assert(!st ? page  != NULL : 1);
    if (st)
        return st;
    ham_assert(newpage->get_db());
    /* clear the node header */
    memset(newpage->get_payload(), 0, sizeof(btree_node_t));

    btree_stats_page_is_nuked(db, page, HAM_TRUE);

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
    if (db->get_data_access_mode()&HAM_DAM_SEQUENTIAL_INSERT)
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
    st=btree_uncouple_all_cursors(page, pivot);
    if (st)
        return (st);

    /*
     * if we split a leaf, we'll insert the pivot element in the leaf
     * page, too. in internal nodes, we don't insert it, but propagate
     * it to the parent node only.
     */
    if (btree_node_is_leaf(obtp)) {
        hints->cost += btree_stats_memmove_cost((db_get_int_key_header_size()+keysize)*(count-pivot));
        memcpy((char *)nbte,
               ((char *)obte)+(db_get_int_key_header_size()+keysize)*pivot,
               (db_get_int_key_header_size()+keysize)*(count-pivot));
    }
    else {
        hints->cost += btree_stats_memmove_cost((db_get_int_key_header_size()+keysize)*(count-pivot-1));
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
    st=db->copy_key(&oldkey, &pivotkey);
    if (st)
        goto fail_dramatically;
    pivotrid=newpage->get_self();

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
        st=__insert_nosplit(newpage, scratchpad->txn, key, rid,
                scratchpad->record, scratchpad->cursor, hints);
    else
        st=__insert_nosplit(page, scratchpad->txn, key, rid,
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

    btree_node_set_left (nbtp, page->get_self());
    btree_node_set_right(nbtp, btree_node_get_right(obtp));
    btree_node_set_right(obtp, newpage->get_self());
    if (oldsib) {
        sbtp=page_get_btree_node(oldsib);
        btree_node_set_left(sbtp, newpage->get_self());
        oldsib->set_dirty(true);
    }
    newpage->set_dirty(true);
    page->set_dirty(true);

    /*
     * propagate the pivot key to the parent page
     */
    ham_assert(!(scratchpad->key.flags & HAM_KEY_USER_ALLOC));
    if (scratchpad->key.data)
        env->get_allocator()->free(scratchpad->key.data);
    scratchpad->key=pivotkey;
    scratchpad->rid=pivotrid;
    ham_assert(!(scratchpad->key.flags & HAM_KEY_USER_ALLOC));

    if (g_BTREE_INSERT_SPLIT_HOOK)
        g_BTREE_INSERT_SPLIT_HOOK();

    return (SPLIT);

fail_dramatically:
    ham_assert(!(pivotkey.flags & HAM_KEY_USER_ALLOC));
    if (pivotkey.data)
        env->get_allocator()->free(pivotkey.data);
    return st;
}

