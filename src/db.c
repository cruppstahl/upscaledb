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
 */

#include "config.h"

#include <string.h>
#include <math.h>
#include <float.h>
#include <ham/hamsterdb.h>
#include "error.h"
#include "cache.h"
#include "freelist.h"
#include "mem.h"
#include "os.h"
#include "db.h"
#include "btree.h"
#include "version.h"
#include "txn.h"
#include "blob.h"
#include "extkeys.h"
#include "cursor.h"
#include "btree_cursor.h"
#include "page.h"
#include "statistics.h"




ham_status_t
db_uncouple_all_cursors(ham_page_t *page, ham_size_t start)
{
    ham_status_t st;
    ham_bool_t skipped=HAM_FALSE;
    ham_cursor_t *n;
    ham_cursor_t *c=page_get_cursors(page);

    while (c) {
        ham_bt_cursor_t *btc=(ham_bt_cursor_t *)c;
        n=cursor_get_next_in_page(c);

        /*
         * ignore all cursors which are already uncoupled
         */
        if (bt_cursor_get_flags(btc)&BT_CURSOR_FLAG_COUPLED) {
            /*
             * skip this cursor if its position is < start
             */
            if (bt_cursor_get_coupled_index(btc)<start) {
                c=n;
                skipped=HAM_TRUE;
                continue;
            }

            /*
             * otherwise: uncouple it
             */
            st=bt_cursor_uncouple((ham_bt_cursor_t *)c, 0);
            if (st)
                return (st);
            cursor_set_next_in_page(c, 0);
            cursor_set_previous_in_page(c, 0);
        }

        c=n;
    }

    if (!skipped)
        page_set_cursors(page, 0);

    return (0);
}

int HAM_CALLCONV 
db_default_prefix_compare(ham_db_t *db, 
                   const ham_u8_t *lhs, ham_size_t lhs_length,
                   ham_size_t lhs_real_length,
                   const ham_u8_t *rhs, ham_size_t rhs_length,
                   ham_size_t rhs_real_length)
{
    int m;
    
    (void)db;

    /*
     * the default compare uses memcmp
     *
     * treat shorter strings as "higher"

    when one of the keys is NOT extended we don't need to request the other (extended) key:
    shorter is "higher" anyhow, and one of 'em is short enough to know already. Two scenarios
    here: the simple one and the little less simple one:

    1) when the key lengths differ, one of them is NOT extended for sure.

    2) when one of the key lengths equals their real_length, that one is NOT extended.

    Saves fetching extended keys whenever possible.

    The few extra comparisons in here outweigh the overhead of fetching one extended key by far.


    Note:

    there's a 'tiny' caveat to it all: often these comparisons are between a database key
    (int_key_t based) and a user-specified key (ham_key_t based), where the latter will always
    appear in the LHS and the important part here is: ham_key_t-based comparisons will have
    their key lengths possibly LARGER than the usual 'short' int_key_t key length as the 
    ham_key_t data doesn't need extending - the result is that simply looking at the lhs_length
    is not good enough here to ensure the key is actually shorter than the other.
     */
    if (lhs_length < rhs_length) 
    {
        m=memcmp(lhs, rhs, lhs_length);
        if (m<0)
            return (-1);
        if (m>0)
            return (+1);
        //ham_assert(lhs_real_length < rhs_real_length, (0));

        /* scenario (2) check: */
        if (lhs_length == lhs_real_length) {
            ham_assert(lhs_real_length < rhs_real_length, (0));
            return (-1);
        }
    }
    else if (rhs_length < lhs_length) 
    {
        m=memcmp(lhs, rhs, rhs_length);
        if (m<0)
            return (-1);
        if (m>0)
            return (+1);
        //ham_assert(lhs_real_length > rhs_real_length, (0));

        /* scenario (2) check: */
        if (rhs_length == rhs_real_length) {
            ham_assert(lhs_real_length > rhs_real_length, (0));
            return (+1);
        }
    }
    else
    {
        m=memcmp(lhs, rhs, lhs_length);
        if (m<0)
            return (-1);
        if (m>0)
            return (+1);

        /* scenario (2) check: */
        if (lhs_length == lhs_real_length) {
            if (lhs_real_length < rhs_real_length)
                return (-1);
        }
        else if (rhs_length == rhs_real_length) {
            if (lhs_real_length > rhs_real_length)
                return (+1);
        }
    }

    return (HAM_PREFIX_REQUEST_FULLKEY);
}

int HAM_CALLCONV 
db_default_compare(ham_db_t *db, 
                   const ham_u8_t *lhs, ham_size_t lhs_length,
                   const ham_u8_t *rhs, ham_size_t rhs_length)
{
    int m;

    (void)db;

    /*
     * the default compare uses memcmp
     *
     * treat shorter strings as "higher"
     */
    if (lhs_length<rhs_length) {
        m=memcmp(lhs, rhs, lhs_length);
        if (m<0)
            return (-1);
        if (m>0)
            return (+1);
        return (-1);
    }

    else if (rhs_length<lhs_length) {
        m=memcmp(lhs, rhs, rhs_length);
        if (m<0)
            return (-1);
        if (m>0)
            return (+1);
        return (+1);
    }

    m=memcmp(lhs, rhs, lhs_length);
    if (m<0)
        return (-1);
    if (m>0)
        return (+1);
    return (0);
}

int HAM_CALLCONV 
db_default_recno_compare(ham_db_t *db, 
                   const ham_u8_t *lhs, ham_size_t lhs_length,
                   const ham_u8_t *rhs, ham_size_t rhs_length)
{
    ham_u64_t ulhs, urhs;

    (void)db;

    memcpy(&ulhs, lhs, 8);
    memcpy(&urhs, rhs, 8);

    ulhs=ham_db2h64(ulhs);
    urhs=ham_db2h64(urhs);

    if (ulhs<urhs)
        return -1;
    if (ulhs==urhs)
        return 0;
    return 1;
}

ham_status_t
db_get_extended_key(ham_db_t *db, ham_u8_t *key_data,
                    ham_size_t key_length, ham_u32_t key_flags,
                    ham_key_t *ext_key)
{
    ham_offset_t blobid;
    ham_status_t st;
    ham_size_t temp;
    ham_record_t record;
    ham_u8_t *ptr;

    ham_assert(key_flags&KEY_IS_EXTENDED, ("key is not extended"));

    if (!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB)) {
        /*
         * make sure that we have an extended key-cache
         *
         * in in-memory-db, the extkey-cache doesn't lead to performance
         * advantages; it only duplicates the data and wastes memory.
         * therefore we don't use it.
         */
        if (!db_get_extkey_cache(db)) {
            extkey_cache_t *c=extkey_cache_new(db);
            if (db_get_env(db))
                env_set_extkey_cache(db_get_env(db), c);
            else
                db_set_extkey_cache(db, c);
            if (!db_get_extkey_cache(db))
                return (db_get_error(db));
        }
    }

    /* almost the same as: blobid = key_get_extended_rid(db, key); */
    memcpy(&blobid, key_data+(db_get_keysize(db)-sizeof(ham_offset_t)), 
            sizeof(blobid));
    blobid=ham_db2h_offset(blobid);

    /* fetch from the cache */
    if (!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB)) {
        st=extkey_cache_fetch(db_get_extkey_cache(db), blobid,
                        &temp, &ptr);
        if (!st) {
            ham_assert(temp==key_length, ("invalid key length"));

            if (!(ext_key->flags&HAM_KEY_USER_ALLOC)) {
                ext_key->data=(ham_u8_t *)ham_mem_alloc(db, key_length);
                if (!ext_key->data) {
                    db_set_error(db, HAM_OUT_OF_MEMORY);
                    return (HAM_OUT_OF_MEMORY);
                }
            }
            memcpy(ext_key->data, ptr, key_length);
            ext_key->size=key_length;
            return (0);
        }
        if (st!=HAM_KEY_NOT_FOUND) {
            db_set_error(db, st);
            return (st);
        }
    }

    /*
     * not cached - fetch from disk;
     * we allocate the memory here to avoid that the global record
     * pointer is overwritten
     */
    memset(&record, 0, sizeof(record));
    record.data=ham_mem_alloc(db, key_length+sizeof(ham_offset_t));
    if (!record.data)
        return (db_set_error(db, HAM_OUT_OF_MEMORY));
    record.flags=HAM_RECORD_USER_ALLOC;
    record.size=key_length+sizeof(ham_offset_t);

    st=blob_read(db, blobid, &record, 0);
    if (st) {
        ham_mem_free(db, record.data);
        return (db_set_error(db, st));
    }

    if (!(ext_key->flags&HAM_KEY_USER_ALLOC)) {
        ext_key->data=(ham_u8_t *)ham_mem_alloc(db, key_length);
        if (!ext_key->data) {
            ham_mem_free(db, record.data);
            return (db_set_error(db, HAM_OUT_OF_MEMORY));
        }
    }
    memmove(((char *)ext_key->data), key_data, 
               db_get_keysize(db)-sizeof(ham_offset_t));
    memcpy (((char *)ext_key->data)+(db_get_keysize(db)-sizeof(ham_offset_t)),
               record.data, record.size);

    /* insert the FULL key in the cache */
    if (db_get_extkey_cache(db)) {
        (void)extkey_cache_insert(db_get_extkey_cache(db),
                blobid, key_length, ext_key->data);
    }

    ext_key->size = key_length;
    ham_mem_free(db, record.data);

    return (0);
}

ham_status_t 
db_prepare_ham_key_for_compare(ham_db_t *db, int_key_t *src, ham_key_t *dest)
{
    dest->size = key_get_size(src);
    dest->data = key_get_key(src);
    dest->flags = HAM_KEY_USER_ALLOC;
    dest->_flags = key_get_flags(src);

    if (dest->_flags & KEY_IS_EXTENDED) {
        void *p = ham_mem_alloc(db, dest->size);
        if (!p) {
            dest->data = 0;
            return db_set_error(db, HAM_OUT_OF_MEMORY);
        }
        memcpy(p, dest->data, db_get_keysize(db));
        dest->data = p;
    }

    return (0);
}

void
db_release_ham_key_after_compare(ham_db_t *db, ham_key_t *key)
{
    if (key->data && key->_flags&KEY_IS_EXTENDED) {
        ham_mem_free(db, key->data);
        key->data = 0;
    }
}

int
db_compare_keys(ham_db_t *db, ham_key_t *lhs, ham_key_t *rhs)
{
    int cmp=HAM_PREFIX_REQUEST_FULLKEY;
    ham_compare_func_t foo=db_get_compare_func(db);
    ham_prefix_compare_func_t prefoo=db_get_prefix_compare_func(db);

    db_set_error(db, 0);

    /*
     * need prefix compare?
     */
    if (!(lhs->_flags&KEY_IS_EXTENDED) && !(rhs->_flags&KEY_IS_EXTENDED)) {
        /*
         * no!
         */
        return (foo(db, lhs->data, lhs->size, rhs->data, rhs->size));
    }

    /*
     * yes! - run prefix comparison, but only if we have a prefix
     * comparison function
     */
    if (prefoo) {
        ham_size_t lhsprefixlen, rhsprefixlen;

        if (lhs->_flags&KEY_IS_EXTENDED)
            lhsprefixlen=db_get_keysize(db)-sizeof(ham_offset_t);
        else
            lhsprefixlen=lhs->size;

        if (rhs->_flags&KEY_IS_EXTENDED)
            rhsprefixlen=db_get_keysize(db)-sizeof(ham_offset_t);
        else
            rhsprefixlen=rhs->size;

        cmp=prefoo(db, lhs->data, lhsprefixlen, lhs->size, 
                    rhs->data, rhsprefixlen, rhs->size);
        if (db_get_error(db))
            return (0);
    }

    if (cmp==HAM_PREFIX_REQUEST_FULLKEY) 
    {
        ham_status_t st;

        /*
         * 1. load the first key, if needed
         */
        if (lhs->_flags & KEY_IS_EXTENDED) 
        {
            st = db_get_extended_key(db, lhs->data,
                    lhs->size, lhs->_flags,
                    lhs);
            if (st)
                return (0);
        }

        /*
         * 2. load the second key, if needed
         */
        if (rhs->_flags & KEY_IS_EXTENDED) {
            st = db_get_extended_key(db, rhs->data,
                    rhs->size, rhs->_flags,
                    rhs);
            if (st)
                return (0);
        }

        /*
         * 3. run the comparison function
         */
        cmp=foo(db, lhs->data, lhs->size, rhs->data, rhs->size);
    }

    return (cmp);
}

ham_backend_t *
db_create_backend(ham_db_t *db, ham_u32_t flags)
{
    ham_backend_t *be;
    ham_status_t st;

    /*
     * the default backend is the BTREE
     *
     * create a ham_backend_t with the size of a ham_btree_t
     */
    be=(ham_backend_t *)ham_mem_alloc(db, sizeof(ham_btree_t));
    if (!be)
        return (0);

    /* initialize the backend */
    st=btree_create((ham_btree_t *)be, db, flags);
    if (st) {
        ham_mem_free(db, be);
        return (0);
    }

    return (be);
}

static ham_status_t
my_purge_cache(ham_db_t *db)
{
    ham_status_t st;
    ham_page_t *page;

    /*
     * first, try to delete unused pages from the cache
     */
    if (db_get_cache(db) && !(db_get_rt_flags(db)&HAM_IN_MEMORY_DB)) {
#if defined(HAM_DEBUG)
        if (cache_too_big(db_get_cache(db)))
        {
            (void)cache_check_integrity(db_get_cache(db));
        }
#endif
        while (cache_too_big(db_get_cache(db))) {
            page=cache_get_unused_page(db_get_cache(db));
            if (!page) {
                if (db_get_rt_flags(db)&HAM_CACHE_STRICT) 
                    return (db_set_error(db, HAM_CACHE_FULL));
                else
                    break;
            }
            cache_push_history(page, -100);

            st=db_write_page_and_delete(page, 0);
            if (st)
                return (db_set_error(db, st));
        }
    }

    /*
     * then free unused extended keys. We don't do this too often to 
     * avoid a thrashing of the cache, and freeing unused extkeys
     * is more expensive (performance-wise) than freeing unused pages.
     */
    if (db_get_extkey_cache(db)) {
        if (db_get_txn_id(db)%10==0) {
            st=extkey_cache_purge(db_get_extkey_cache(db));
            if (st)
                return (db_set_error(db, st));
        }
    }

    return (HAM_SUCCESS);
}

ham_status_t
db_free_page(ham_page_t *page, ham_u32_t flags)
{
    ham_status_t st;
    ham_db_t *db=page_get_owner(page);
    ham_assert(0 == (flags & ~DB_MOVE_TO_FREELIST), (0));

    st=db_uncouple_all_cursors(page, 0);
    if (st)
        return (st);

    if (db_get_cache(db)) {
        st=cache_remove_page(db_get_cache(db), page);
        if (st)
            return (st);
    }

    /*
     * if this page has a header, and it's either a B-Tree root page or 
     * a B-Tree index page: remove all extended keys from the cache, 
     * and/or free their blobs
     *
     * TODO move this to the backend!
     * TODO not necessary, if we come from my_free_cb (hamsterdb)
     */
    if (page_get_pers(page) && 
        (!(page_get_npers_flags(page)&PAGE_NPERS_NO_HEADER)) &&
         (page_get_type(page)==PAGE_TYPE_B_ROOT ||
          page_get_type(page)==PAGE_TYPE_B_INDEX)) {
        ham_size_t i;
        ham_offset_t blobid;
        int_key_t *bte;
        btree_node_t *node=ham_page_get_btree_node(page);
        extkey_cache_t *c=db_get_extkey_cache(page_get_owner(page));

        for (i=0; i<btree_node_get_count(node); i++) {
            bte=btree_node_get_key(db, node, i);
            if (key_get_flags(bte)&KEY_IS_EXTENDED) {
                blobid=key_get_extended_rid(db, bte);
                if (db_get_rt_flags(db)&HAM_IN_MEMORY_DB) {
                    /* delete the blobid to prevent that it's freed twice */
                    *(ham_offset_t *)(key_get_key(bte)+
                        (db_get_keysize(db)-sizeof(ham_offset_t)))=0;
                }
                (void)key_erase_record(db, bte, 0, BLOB_FREE_ALL_DUPES);
                (void)extkey_cache_remove(c, blobid);
            }
        }
    }

    /*
     * move the page to the freelist
     */
    if (flags&DB_MOVE_TO_FREELIST) {
        if (!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB))
            (void)freel_mark_free(db, page_get_self(page), 
                    db_get_pagesize(db), HAM_TRUE);
    }

    /*
     * free the page; since it's deleted, we don't need to flush it
     */
    page_set_undirty(page);
    (void)page_free(page);
    (void)page_delete(page);

    return (HAM_SUCCESS);
}

ham_page_t *
db_alloc_page(ham_db_t *db, ham_u32_t type, ham_u32_t flags)
{
    ham_status_t st;
    ham_offset_t tellpos=0;
    ham_page_t *page=0;
    ham_assert(0 == (flags & ~(PAGE_IGNORE_FREELIST 
                        | PAGE_CLEAR_WITH_ZERO)), (0));

    /* freel_alloc_page() can set an error and we want to detect 
     * that unambiguously */
    ham_assert(db_get_error(db) == 0, (0)); 

    /* purge the cache, if necessary */
    if (db_get_cache(db) 
            && !(db_get_rt_flags(db)&HAM_IN_MEMORY_DB)
            && !(db_get_rt_flags(db)&HAM_CACHE_UNLIMITED)) {
        st=my_purge_cache(db);
        if (st)
            return (0);
    }

    /* first, we ask the freelist for a page */
    if (!(flags&PAGE_IGNORE_FREELIST)) {
        tellpos=freel_alloc_page(db);
        if (tellpos) {
            ham_assert(tellpos%db_get_pagesize(db)==0,
                    ("page id %llu is not aligned", tellpos));
            /* try to fetch the page from the txn */
            if (db_get_txn(db)) {
                page=txn_get_page(db_get_txn(db), tellpos);
                if (page)
                    goto done;
            }
            /* try to fetch the page from the cache */
            if (db_get_cache(db)) {
                page=cache_get_page(db_get_cache(db), tellpos, 0);
                if (page)
                    goto done;
            }
            /* allocate a new page structure */
            ham_assert(!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB) 
                    ? 1 
                    : !!db_get_cache(db), ("in-memory DBs MUST have a cache"));
            page=page_new(db);
            if (!page)
                return (0);
            page_set_self(page, tellpos);
            /* fetch the page from disk */
            st=page_fetch(page, db_get_pagesize(db));
            if (st) {
                page_delete(page);
                return (0);
            }
            goto done;
        }
        else if (db_get_error(db)) 
            return (0);
    }

    if (!page) {
        page=page_new(db);
        if (!page)
            return (0);
    }

    st=page_alloc(page, db_get_pagesize(db));
    if (st)
        return (0);

    if (db_get_txn(db))
        page_set_alloc_txn_id(page, txn_get_id(db_get_txn(db)));

    /* 
     * update statistics for the freelist: we'll need to mark this one 
     * down as allocated, so the statistics are up-to-date.
     *
     * This is done further below.
     */

done:
    ham_assert(!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB) 
            ? 1 
            : !!db_get_cache(db), ("in-memory DBs MUST have a cache"));
    /*
     * As we re-purpose a page, we will reset its pagecounter as
     * well to signal its first use as the new type assigned here.
     *
     * only do this if the page is reused - otherwise page_get_type()
     * accesses uninitialized memory, and valgrind complains
     */
    if (tellpos && db_get_cache(db) && (page_get_type(page) != type)) {
        page_set_cache_cntr(page, db_get_cache(db)->_timeslot++);
    }
    page_set_type(page, type);
    page_set_undirty(page);

    st=ham_log_add_page_before(page);
    if (st) 
        return (0);

    /*
     * clear the page with zeroes?
     */
    if (flags&PAGE_CLEAR_WITH_ZERO) {
        memset(page_get_pers(page), 0, db_get_pagesize(db));

        st=ham_log_add_page_after(page);
        if (st) 
            return (0);
    }

    if (db_get_txn(db)) {
        st=txn_add_page(db_get_txn(db), page, HAM_FALSE);
        if (st) {
            db_set_error(db, st);
            /* TODO memleak? */
            return (0);
        }
    }

    if (db_get_cache(db)) {
        st=cache_put_page(db_get_cache(db), page);
        if (st) {
            db_set_error(db, st);
            /* TODO memleak? */
            return (0);
        }
        cache_update_page_access_counter(page, db_get_cache(db));
    }
    cache_check_history(db, page, -1);

    return (page);
}

ham_page_t *
db_fetch_page(ham_db_t *db, ham_offset_t address, ham_u32_t flags)
{
    ham_page_t *page=0;
    ham_status_t st;
    ham_assert(0 == (flags & ~(DB_NEW_PAGE_DOESNT_THRASH_CACHE 
                                | DB_ONLY_FROM_CACHE)), (0));

    /* 
     * check if the cache allows us to allocate another page; if not,
     * purge it
     */
    if (!(flags&DB_ONLY_FROM_CACHE) 
            && db_get_cache(db) 
            && !(db_get_rt_flags(db)&HAM_IN_MEMORY_DB)
            && !(db_get_rt_flags(db)&HAM_CACHE_UNLIMITED)) {
        if (cache_too_big(db_get_cache(db))) {
            st=my_purge_cache(db);
            if (st) {
                db_set_error(db, st);
                return (0);
            }
        }
    }

    if (db_get_txn(db)) {
        page=txn_get_page(db_get_txn(db), address);
        if (page) {
            cache_check_history(db, page, 1);
            return (page);
        }
    }

    /* 
     * fetch the page from the cache
     */
    if (db_get_cache(db)) {
        page=cache_get_page(db_get_cache(db), address, CACHE_NOREMOVE);
        if (page) {
            if (db_get_txn(db)) {
                st=txn_add_page(db_get_txn(db), page, HAM_FALSE);
                if (st) {
                    db_set_error(db, st);
                    return (0);
                }
            }
            cache_check_history(db, page, 2);
            return (page);
        }
    }

    if (flags&DB_ONLY_FROM_CACHE)
        return (0);

#if HAM_DEBUG
    if (db_get_cache(db))
        ham_assert(cache_get_page(db_get_cache(db), address, 0)==0, (""));
#endif
    ham_assert(!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB) 
            ? 1 
            : !!db_get_cache(db), ("in-memory DBs MUST have a cache"));

    page=page_new(db);
    if (!page)
        return (0);

    page_set_self(page, address);
    st=page_fetch(page, db_get_pagesize(db));
    if (st) {
        (void)page_delete(page);
        return (0);
    }

    if (db_get_txn(db)) {
        st=txn_add_page(db_get_txn(db), page, HAM_FALSE);
        if (st) {
            db_set_error(db, st);
            (void)page_delete(page);
            return (0);
        }
    }

    if (db_get_cache(db)) {
        st=cache_put_page(db_get_cache(db), page);
        if (st) {
            db_set_error(db, st);
            (void)page_delete(page);
            return (0);
        }
        if (flags & DB_NEW_PAGE_DOESNT_THRASH_CACHE) {
            /* give it an 'antique' age so this one will get flushed pronto */
            page_set_cache_cntr(page, 1 /* cache->_timeslot - 1000 */ );
        }
        else {
            cache_update_page_access_counter(page, db_get_cache(db));
        }
    }

    cache_check_history(db, page, 3);

    return (page);
}

ham_status_t
db_flush_page(ham_db_t *db, ham_page_t *page, ham_u32_t flags)
{
    ham_status_t st;

    /* write the page, if it's dirty and if write-through is enabled */
    if ((db_get_rt_flags(db)&HAM_WRITE_THROUGH || flags&HAM_WRITE_THROUGH) && 
            page_is_dirty(page)) {
        st=page_flush(page);
        if (st)
            return (st);
    }

    /*
     * put page back into the cache; do NOT update the page_counter, as
     * this flush operation should not be considered an 'additional page
     * access' impacting the page life-time in the cache.
     */
    if (db_get_cache(db))
        return (cache_put_page(db_get_cache(db), page));

    return (0);
}

ham_status_t
db_flush_all(ham_db_t *db, ham_u32_t flags)
{
    ham_page_t *head;

    ham_assert(0 == (flags & ~DB_FLUSH_NODELETE), (0));

    if (!db_get_cache(db)) 
        return (0);

    head=cache_get_totallist(db_get_cache(db)); 
    while (head) {
        ham_page_t *next=page_get_next(head, PAGE_LIST_CACHED);

        /*
         * don't remove the page from the cache, if flag NODELETE
         * is set (this flag is used i.e. in ham_flush())
         */
        if (!(flags&DB_FLUSH_NODELETE)) {
            ham_assert(page_get_refcount(head)==0, 
                ("page is in use, but database is closing"));
            cache_set_totallist(db_get_cache(db), 
                page_list_remove(cache_get_totallist(db_get_cache(db)), 
                PAGE_LIST_CACHED, head));
            cache_push_history(head, -6);
            
            cache_set_cur_elements(db_get_cache(db), 
                cache_get_cur_elements(db_get_cache(db))-1);
        }

        (void)db_write_page_and_delete(head, flags);

        head=next;
    }

    return (HAM_SUCCESS);
}

ham_status_t
db_write_page_and_delete(ham_page_t *page, ham_u32_t flags)
{
    ham_status_t st;
    ham_db_t *db=page_get_owner(page);
    ham_assert(0 == (flags & ~DB_FLUSH_NODELETE), (0));

    /*
     * write page to disk if it's dirty (and if we don't have 
     * an IN-MEMORY DB)
     */
    if (page_is_dirty(page) && !(db_get_rt_flags(db)&HAM_IN_MEMORY_DB)) {
        st=page_flush(page);
        if (st)
            return (db_set_error(db, st));
    }

    /*
     * if the page is deleted, uncouple all cursors, then
     * free the memory of the page
     */
    if (!(flags&DB_FLUSH_NODELETE)) {
        st=db_uncouple_all_cursors(page, 0);
        if (st)
            return (st);
        st=page_free(page);
        if (st)
            return (st);
        page_delete(page);
    }

    return (HAM_SUCCESS);
}

ham_status_t
db_resize_allocdata(ham_db_t *db, ham_size_t size)
{
    if (size==0) {
        if (db_get_record_allocdata(db))
            ham_mem_free(db, db_get_record_allocdata(db));
        db_set_record_allocdata(db, 0);
        db_set_record_allocsize(db, 0);
    }
    else if (size>db_get_record_allocsize(db)) {
        void *newdata=ham_mem_alloc(db, size);
        if (!newdata) 
            return (HAM_OUT_OF_MEMORY);
        if (db_get_record_allocdata(db))
            ham_mem_free(db, db_get_record_allocdata(db));
        db_set_record_allocdata(db, newdata);
        db_set_record_allocsize(db, size);
    }

    return (0);
}
