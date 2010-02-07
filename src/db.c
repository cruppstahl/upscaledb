/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
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

#include "blob.h"
#include "btree.h"
#include "btree_cursor.h"
#include "cache.h"
#include "cursor.h"
#include "db.h"
#include "device.h"
#include "env.h"
#include "error.h"
#include "extkeys.h"
#include "freelist.h"
#include "log.h"
#include "mem.h"
#include "os.h"
#include "page.h"
#include "statistics.h"
#include "txn.h"
#include "version.h"

#if 0 /* B+tree dependent; moved to btree_cursor.c */

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

#else

ham_status_t
db_uncouple_all_cursors(ham_page_t *page, ham_size_t start)
{
    ham_cursor_t *c = page_get_cursors(page);

    if (c)
    {
        ham_db_t *db = cursor_get_db(c);
        if (db)
        {
            ham_backend_t *be = db_get_backend(db);
            
            if (be)
            {
                return (*be->_fun_uncouple_all_cursors)(be, page, start);
            }
        }
    }
    return HAM_SUCCESS;
}


#endif

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
    ham_env_t *env = db_get_env(db);
    mem_allocator_t *alloc=env_get_allocator(env);

    ham_assert(key_flags&KEY_IS_EXTENDED, ("key is not extended"));

    if (!(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)) {
        /*
         * make sure that we have an extended key-cache
         *
         * in in-memory-db, the extkey-cache doesn't lead to performance
         * advantages; it only duplicates the data and wastes memory.
         * therefore we don't use it.
         */
        if (!db_get_extkey_cache(db)) {
            extkey_cache_t *c=extkey_cache_new(db);
            db_set_extkey_cache(db, c);
            if (!c)
                return HAM_OUT_OF_MEMORY;
        }
    }

    /* almost the same as: blobid = key_get_extended_rid(db, key); */
    memcpy(&blobid, key_data+(db_get_keysize(db)-sizeof(ham_offset_t)), 
            sizeof(blobid));
    blobid=ham_db2h_offset(blobid);

    /* fetch from the cache */
    if (!(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)) {
        st=extkey_cache_fetch(db_get_extkey_cache(db), blobid,
                        &temp, &ptr);
        if (!st) {
            ham_assert(temp==key_length, ("invalid key length"));

            if (!(ext_key->flags&HAM_KEY_USER_ALLOC)) {
                if (!ext_key->data || ext_key->size < key_length) {
                    if (ext_key->data)
                        allocator_free(alloc, ext_key->data);
                    ext_key->data = (ham_u8_t *)allocator_alloc(alloc, 
                                            key_length);
                    if (!ext_key->data) 
                        return HAM_OUT_OF_MEMORY;
                }
            }
            memcpy(ext_key->data, ptr, key_length);
            ext_key->size=(ham_u16_t)key_length;
            return (0);
        }
        else if (st!=HAM_KEY_NOT_FOUND) {
            return st;
        }
    }

    /*
     * not cached - fetch from disk;
     * we allocate the memory here to avoid that the global record
     * pointer is overwritten
     * 
     * Note that the key is fetched in two parts: we already have the front 
     * part of the key in key_data and now we only need to fetch the blob 
     * remainder, which size is: 
     *    key_length - (db_get_keysize(db)-sizeof(ham_offset_t))
     *
     * To prevent another round of memcpy and heap allocation here, we 
     * simply allocate sufficient space for the entire key as it should be 
     * passed back through (*ext_key) and adjust the pointer into that 
     * memory space for the faked record-based blob_read() below.
     */
    if (!(ext_key->flags & HAM_KEY_USER_ALLOC)) {
        if (!ext_key->data || ext_key->size < key_length) {
            if (ext_key->data)
                allocator_free(alloc, ext_key->data);
            ext_key->data = (ham_u8_t *)allocator_alloc(alloc, key_length);
            if (!ext_key->data)
                return HAM_OUT_OF_MEMORY;
        }
    }

    memmove(ext_key->data, key_data, db_get_keysize(db)-sizeof(ham_offset_t));

    memset(&record, 0, sizeof(record));
    record.data=(((ham_u8_t *)ext_key->data) + 
                    db_get_keysize(db)-sizeof(ham_offset_t));
    record.size = key_length - (db_get_keysize(db)-sizeof(ham_offset_t));
    record.flags = HAM_RECORD_USER_ALLOC;

    st=blob_read(db, blobid, &record, 0);
    if (st)
        return st;

    /* insert the FULL key in the extkey-cache */
    if (db_get_extkey_cache(db)) {
        st = extkey_cache_insert(db_get_extkey_cache(db),
                blobid, key_length, ext_key->data);
        if (st)
            return st;
    }

    ext_key->size = (ham_u16_t)key_length;

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
        void *p = allocator_alloc(env_get_allocator(db_get_env(db)), dest->size);
        if (!p) {
            dest->data = 0;
            return HAM_OUT_OF_MEMORY;
        }
        memcpy(p, dest->data, db_get_keysize(db));
        dest->data = p;
        dest->flags = 0; /* [i_a] important; discard any USER_ALLOC present due to the copy from src */
    }

    return (0);
}

void
db_release_ham_key_after_compare(ham_db_t *db, ham_key_t *key)
{
    // if (key->data && key->_flags&KEY_IS_EXTENDED) {
    if (key->data && !(key->flags & HAM_KEY_USER_ALLOC)) /* [i_a] check the right flag for free() */
    {
        allocator_free(env_get_allocator(db_get_env(db)), key->data);
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
        if (cmp < -1 && cmp != HAM_PREFIX_REQUEST_FULLKEY)
            return cmp; /* unexpected error! */
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
            {
                ham_assert(st < -1, (0));
                return st;
            }
        }

        /*
         * 2. load the second key, if needed
         */
        if (rhs->_flags & KEY_IS_EXTENDED) {
            st = db_get_extended_key(db, rhs->data,
                    rhs->size, rhs->_flags,
                    rhs);
            if (st)
            {
                ham_assert(st < -1, (0));
                return st;
            }
        }

        /*
         * 3. run the comparison function
         */
        cmp=foo(db, lhs->data, lhs->size, rhs->data, rhs->size);
    }

    return (cmp);
}

ham_status_t
db_create_backend(ham_backend_t **backend_ref, ham_db_t *db, ham_u32_t flags)
{
    *backend_ref = 0;

    /*
     * the default backend is the BTREE
     *
     * create a ham_backend_t with the size of a ham_btree_t
     */
    return btree_create(backend_ref, db, flags);
}

static ham_status_t
my_purge_cache(ham_env_t *env)
{
    ham_status_t st;
    ham_page_t *page;

    /*
     * first, try to delete unused pages from the cache
     */
    if (env_get_cache(env) && !(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)) {
        ham_bool_t lowwater = HAM_FALSE;

#if defined(HAM_DEBUG) && defined(HAM_ENABLE_INTERNAL) && !defined(HAM_LEAN_AND_MEAN_FOR_PROFILING)
        if (cache_too_big(env_get_cache(env), HAM_FALSE))
        {
            (void)cache_check_integrity(env_get_cache(env));
        }
#endif
        while (cache_too_big(env_get_cache(env), lowwater)) {
            lowwater = HAM_TRUE;

            page=cache_get_unused_page(env_get_cache(env));
            if (!page) {
                if (env_get_rt_flags(env)&HAM_CACHE_STRICT) 
                    return HAM_CACHE_FULL;
                else
                    break;
            }
            cache_push_history(page, -100);

            st=db_write_page_and_delete(page, 0);
            if (st)
                return st;
        }
    }

#if 0
    /*
     * then free unused extended keys. We don't do this too often to 
     * avoid a thrashing of the cache, and freeing unused extkeys
     * is more expensive (performance-wise) than freeing unused pages.
     */
    if (db_get_extkey_cache(db)) {
        if (env_get_txn_id(env) % 10 == 0) {
            st=extkey_cache_purge(db_get_extkey_cache(db));
            if (st)
                return st;
        }
    }
#endif

    return (HAM_SUCCESS);
}

ham_status_t
db_free_page(ham_page_t *page, ham_u32_t flags)
{
    ham_status_t st;
    ham_env_t *env=device_get_env(page_get_device(page));
    
    ham_assert(page_get_owner(page) 
                ? device_get_env(page_get_device(page)) 
                        == db_get_env(page_get_owner(page)) 
                : 1, (0));

    ham_assert(0 == (flags & ~DB_MOVE_TO_FREELIST), (0));

    st=db_uncouple_all_cursors(page, 0);
    if (st)
        return (st);

    if (env_get_cache(env)) {
        st=cache_remove_page(env_get_cache(env), page);
        if (st)
            return (st);
    }

    /*
     * if this page has a header, and it's either a B-Tree root page or 
     * a B-Tree index page: remove all extended keys from the cache, 
     * and/or free their blobs
     */
    if (page_get_pers(page) && 
        (!(page_get_npers_flags(page)&PAGE_NPERS_NO_HEADER)) &&
         (page_get_type(page)==PAGE_TYPE_B_ROOT ||
          page_get_type(page)==PAGE_TYPE_B_INDEX)) 
    {
        ham_backend_t *be;
        
        ham_assert(page_get_owner(page), ("Must be set as page owner when this is a Btree page"));
        be = db_get_backend(page_get_owner(page));
        ham_assert(be, (0));
        
        st = be->_fun_free_page_extkeys(be, page, flags);
        if (st)
            return (st);
    }

    /*
     * move the page to the freelist
     */
    if (flags&DB_MOVE_TO_FREELIST) {
        if (!(env_get_rt_flags(env)&HAM_IN_MEMORY_DB))
            (void)freel_mark_free(env, 0, page_get_self(page), 
                    env_get_pagesize(env), HAM_TRUE);
    }

    /*
     * free the page; since it's deleted, we don't need to flush it
     */
    page_set_undirty(page);
    (void)page_free(page);
    (void)page_delete(page);

    return (HAM_SUCCESS);
}

ham_status_t
db_alloc_page(ham_page_t **page_ref, ham_env_t *env, ham_db_t *db, 
                ham_u32_t type, ham_u32_t flags)
{
    ham_status_t st;
    ham_offset_t tellpos=0;
    ham_page_t *page = NULL;

    *page_ref = 0;
    ham_assert(0 == (flags & ~(PAGE_IGNORE_FREELIST 
                        | PAGE_CLEAR_WITH_ZERO
                        | PAGE_DONT_LOG_CONTENT
                        | DB_NEW_PAGE_DOES_THRASH_CACHE)), (0));
    ham_assert(env_get_cache(env) != 0, 
            ("This code will not realize the requested page may already exist "
             "through a previous call to this function or db_fetch_page() "
             "unless page caching is available!"));

    /* purge the cache, if necessary */
    if (env_get_cache(env) 
            && !(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)
            && !(env_get_rt_flags(env)&HAM_CACHE_UNLIMITED)) {
        st=my_purge_cache(env);
        if (st)
            return st;
    }

    /* first, we ask the freelist for a page */
    if (!(flags&PAGE_IGNORE_FREELIST)) {
        st=freel_alloc_page(&tellpos, env, db);
        ham_assert(st ? !tellpos : 1, (0));
        if (tellpos) {
            ham_assert(tellpos%env_get_pagesize(env)==0,
                    ("page id %llu is not aligned", tellpos));
            /* try to fetch the page from the txn */
            if (env_get_txn(env)) {
                page=txn_get_page(env_get_txn(env), tellpos);
                if (page)
                    goto done;
            }
            /* try to fetch the page from the cache */
            if (env_get_cache(env)) {
                page=cache_get_page(env_get_cache(env), tellpos, 0);
                if (page)
                    goto done;
            }
            /* allocate a new page structure */
            ham_assert(!(env_get_rt_flags(env)&HAM_IN_MEMORY_DB) 
                    ? 1 
                    : !!env_get_cache(env), 
                    ("in-memory DBs MUST have a cache"));
            page=page_new(env);
            if (!page)
                return HAM_OUT_OF_MEMORY;
            page_set_self(page, tellpos);
            /* fetch the page from disk */
            st=page_fetch(page);
            if (st) {
                page_delete(page);
                return st;
            }
            goto done;
        }
        else if (st) 
        {
            return st;
        }
    }

    if (!page) {
        page=page_new(env);
        if (!page)
            return HAM_OUT_OF_MEMORY;
    }

    ham_assert(tellpos == 0, (0));
    st=page_alloc(page);
    if (st)
        return st;

    if (env_get_txn(env))
        page_set_alloc_txn_id(page, txn_get_id(env_get_txn(env)));

    /* 
     * update statistics for the freelist: we'll need to mark this one 
     * down as allocated, so the statistics are up-to-date.
     *
     * This is done further below.
     */

done:
    ham_assert(!(env_get_rt_flags(env)&HAM_IN_MEMORY_DB) 
            ? 1 
            : !!env_get_cache(env), ("in-memory DBs MUST have a cache"));

    /* 
     * disable page content logging ONLY when the page is 
     * completely new (contains bogus 'before' data) 
     */
    if (tellpos == 0) /* [i_a] BUG! */
    {
        flags &= ~PAGE_DONT_LOG_CONTENT;
    }

    /*
     * As we re-purpose a page, we will reset its pagecounter as
     * well to signal its first use as the new type assigned here.
     *
     * only do this if the page is reused - otherwise page_get_type()
     * accesses uninitialized memory, and valgrind complains
     */
    if (tellpos && env_get_cache(env) && (page_get_type(page) != type)) {
#if 0 // this is done at the end of this call...
        //page_set_cache_cntr(page, env_get_cache(env)->_timeslot++);
        cache_update_page_access_counter(newroot, env_get_cache(env)); /* bump up */
#endif
    }
    page_set_type(page, type);
    page_set_owner(page, db);
    page_set_undirty(page);

    /*
    CONCERN / TO BE CHECKED

    logging or no logging, when a new page is appended to the file
    and the txn fails, that page is not removed from the file through
    the OS, yet freelist is rolled back, resulting in a 'gap' page
    which would seem allocated for ever as the freelist didn't mark
    it as used (max_bits), while subsequent page adds will append
    new pages.

    EDIT v1.1.2: this is resolved by adding database file size changes
                 to the log: aborting transactions should now abort
                 correctly by also truncating the database filesize
                 to the previously known filesize.

                 This, of course, means trouble for partitioning schemes,
                 where the database has multiple 'growth fronts': which
                 one should we truncate?

                 Answer: this is resolved in the simplest possible way:
                 new pages are flagged as such and each page is 'rewound'
                 by calling the 'shrink' device method, which will thus
                 have access to the appropriate 'rid' and any underlying
                 partitioners can take care of the routing of the resize
                 request.

                 The alternative solution: keeping the file size as is,
                 while the transaction within which it occurred, would
                 cause insurmountable trouble when the freelist is expanded
                 but at the same time the freelist needs to grow itself to
                 contain this expansion: the allocation of the freelist
                 page MAY collide with other freelist bit edits in the
                 same freelist page, so logging would not work as is. Then,
                 the option would be to 'recreate' this particular freelist
                 page allocation, but such 'regeneration' cannot be guaranteed
                 to match the original action as the transaction is rewound
                 and the freelist allocation will end up in another place.

                 Guaranteed fault scenario for regeneration: TXN START >
                 ERASE PAGE-SIZE BLOB (marks freelist section as 'free') >
                 EXPAND DB BY SEVERAL PAGES: needs to expand freelist >
                 FREELIST ALLOCS PAGE in location where the erased BLOB was.

                 Which is perfectly Okay when transaction commits, but txn 
                 abort means the BLOB will exist again and the freelist
                 allocation CANNOT happen at the same spot. Subsequent
                 committing transactions would then see a b0rked freelist
                 image after regeneration-on-txn-abort.


                 WRONG! immediately after ABORT the freelist CAN be regenerated,
                 as long as those results are applied to the REWOUND database
                 AND these new edits are LOGGED after the ABORT TXN using 
                 a 'derived' transaction which is COMMITTED: that TXN will only
                 contain the filesize and freelist edits then!
    */
    if (!(flags & PAGE_DONT_LOG_CONTENT) && (env && env_get_log(env)))
    {
        st=ham_log_add_page_before(page);
        if (st) 
            return st;
    }

    /*
     * clear the page with zeroes?
     */
    if (flags&PAGE_CLEAR_WITH_ZERO) {
        memset(page_get_pers(page), 0, env_get_pagesize(env));

        st=ham_log_add_page_after(page);
        if (st) 
            return st;
    }

    if (env_get_txn(env)) {
        st=txn_add_page(env_get_txn(env), page, HAM_FALSE);
        if (st) {
            return st;
            /* TODO memleak? */
        }
    }

    if (env_get_cache(env)) 
    {
        unsigned int bump = 0;

        st=cache_put_page(env_get_cache(env), page);
        if (st) {
            return st;
            /* TODO memleak? */
        }
#if 0
        /*
        Some quick measurements indicate that this (and the btree lines which
        do the same: bumping the cache age of the given page) is deteriorating
        performance:

        with this it's ~ 17K refetches (reloads of previously cached pages);
        without it's ~ 16K refetches, which means the dumb version without the
        weights reloads less database pages.

        All in all, the conclusion is simple:

        Stick with the simplest cache aging system, unless we can come up with something
        truely fantastic to cut down disc I/O (which is particularly important when the
        database file is located on a remote storage disc (SAN).

        And the simplest system is... 
        
        Count every access as one age point, i.e. age 
        all pages with each cache access by 1 and dicard the oldest bugger.
        
        Don't bother with high/low watermarks in purging either as that didn't help
        neither.
        */
        switch (type)
        {
        default:
        case PAGE_TYPE_UNKNOWN:
        case PAGE_TYPE_HEADER:
            break;

        case PAGE_TYPE_B_ROOT:
        case PAGE_TYPE_B_INDEX:
            bump = (cache_get_max_elements(env_get_cache(env)) + 32 - 1) / 32;
            if (bump > 4)
                bump = 4;
            break;

        case PAGE_TYPE_FREELIST:
            bump = (cache_get_max_elements(env_get_cache(env)) + 8 - 1) / 8;
            break;
        }
#endif
        if (flags & DB_NEW_PAGE_DOES_THRASH_CACHE) {
            /* give it an 'antique' age so this one will get flushed pronto */
            page_set_cache_cntr(page, 1 /* cache->_timeslot - 1000 */ );
        }
        else {
            cache_update_page_access_counter(page, env_get_cache(env), bump);
        }
    }

    cache_check_history(env, page, -99);

    *page_ref = page;
    return HAM_SUCCESS;
}

ham_status_t
db_fetch_page(ham_page_t **page_ref, ham_env_t *env, ham_db_t *db,
                ham_offset_t address, ham_u32_t flags)
{
    ham_page_t *page=0;
    ham_status_t st;

    ham_assert(0 == (flags & ~(DB_NEW_PAGE_DOES_THRASH_CACHE 
                                | HAM_HINTS_MASK
                                | DB_ONLY_FROM_CACHE)), (0));
    ham_assert(!(env_get_rt_flags(env)&HAM_IN_MEMORY_DB) 
            ? 1 
            : !!env_get_cache(env), ("in-memory DBs MUST have a cache"));
    ham_assert((env_get_rt_flags(env)&HAM_IN_MEMORY_DB) 
            ? 1 
            : env_get_cache(env) != 0, 
            ("This code will not realize the requested page may already exist through"
             " a previous call to this function or db_alloc_page() unless page caching"
             " is available!"));

    *page_ref = 0;

    /* 
     * check if the cache allows us to allocate another page; if not,
     * purge it
     */
    if (!(flags&DB_ONLY_FROM_CACHE) 
            && env_get_cache(env) 
            && !(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)
            && !(env_get_rt_flags(env)&HAM_CACHE_UNLIMITED)) {
        if (cache_too_big(env_get_cache(env), HAM_FALSE)) {
            st=my_purge_cache(env);
            if (st) {
                return st;
            }
        }
    }

    if (env_get_txn(env)) {
        page=txn_get_page(env_get_txn(env), address);
        if (page) {
            cache_check_history(env, page, 1);
            *page_ref = page;
            ham_assert(page_get_pers(page), (""));
            if (db) {
                ham_assert(page_get_owner(page)==db, (""));
            }
            return (HAM_SUCCESS);
        }
    }

    /* 
     * fetch the page from the cache
     */
    if (env_get_cache(env)) {
        page=cache_get_page(env_get_cache(env), address, CACHE_NOREMOVE);
        if (page) {
            if (env_get_txn(env)) {
                st=txn_add_page(env_get_txn(env), page, HAM_FALSE);
                if (st) {
                    return st;
                }
            }
            cache_check_history(env, page, 2);
            *page_ref = page;
            ham_assert(page_get_pers(page), (""));
            ham_assert(db ? page_get_owner(page)==db : 1, (""));
            return HAM_SUCCESS;
        }
    }

    if (flags&DB_ONLY_FROM_CACHE)
        return HAM_SUCCESS;

#if HAM_DEBUG
    if (env_get_cache(env)) {
        ham_assert(cache_get_page(env_get_cache(env), address, 0)==0, (0));
    }
#endif
    ham_assert(!(env_get_rt_flags(env)&HAM_IN_MEMORY_DB) 
            ? 1 
            : !!env_get_cache(env), ("in-memory DBs MUST have a cache"));

    page=page_new(env);
    if (!page)
        return HAM_OUT_OF_MEMORY;

    page_set_owner(page, db);
    page_set_self(page, address);
    st=page_fetch(page);
    if (st) {
        (void)page_delete(page);
        return st;
    }

    ham_assert(page_get_pers(page), (""));

    if (env_get_txn(env)) {
        st=txn_add_page(env_get_txn(env), page, HAM_FALSE);
        if (st) {
            (void)page_delete(page);
            return st;
        }
    }

    if (env_get_cache(env)) {
        st=cache_put_page(env_get_cache(env), page);
        if (st) {
            (void)page_delete(page);
            return st;
        }
        if (flags & DB_NEW_PAGE_DOES_THRASH_CACHE) {
            /* give it an 'antique' age so this one will get flushed pronto */
            page_set_cache_cntr(page, 1 /* cache->_timeslot - 1000 */ );
        }
        else {
            cache_update_page_access_counter(page, env_get_cache(env), 0);
        }
    }

    cache_check_history(env, page, 3);

    *page_ref = page;
    return HAM_SUCCESS;
}

ham_status_t
db_flush_page(ham_env_t *env, ham_page_t *page, ham_u32_t flags)
{
    ham_status_t st;

    /* write the page, if it's dirty and if write-through is enabled */
    if ((env_get_rt_flags(env)&HAM_WRITE_THROUGH || flags&HAM_WRITE_THROUGH ||
         !env_get_cache(env)) && page_is_dirty(page)) 
    {
        st=page_flush(page);
        if (st)
            return (st);
    }

    /*
     * put page back into the cache; do NOT update the page_counter, as
     * this flush operation should not be considered an 'additional page
     * access' impacting the page life-time in the cache.
     */
    if (env_get_cache(env))
        return (cache_put_page(env_get_cache(env), page));

    return (0);
}

ham_status_t
db_flush_all(ham_cache_t *cache, ham_u32_t flags)
{
    ham_page_t *head;

    ham_assert(0 == (flags & ~DB_FLUSH_NODELETE), (0));

    if (!cache)
        return (0);

    head=cache_get_totallist(cache);
    while (head) {
        ham_page_t *next=page_get_next(head, PAGE_LIST_CACHED);

        /*
         * don't remove the page from the cache, if flag NODELETE
         * is set (this flag is used i.e. in ham_flush())
         */
        if (!(flags&DB_FLUSH_NODELETE)) {
            ham_assert(page_get_refcount(head)==0, 
                ("page is in use, but database is closing"));
            cache_set_totallist(cache,
                page_list_remove(cache_get_totallist(cache), 
                    PAGE_LIST_CACHED, head));
            cache_push_history(head, -6);
            
            cache_set_cur_elements(cache, cache_get_cur_elements(cache)-1);
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
    ham_env_t *env=device_get_env(page_get_device(page));
    
    ham_assert(0 == (flags & ~DB_FLUSH_NODELETE), (0));

    /*
     * write page to disk if it's dirty (and if we don't have 
     * an IN-MEMORY DB)
     */
    ham_assert(env, (0));
    if (page_is_dirty(page) 
            && !(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)) {
        st=page_flush(page);
        if (st)
            return st;
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
            allocator_free(env_get_allocator(db_get_env(db)), db_get_record_allocdata(db));
        db_set_record_allocdata(db, 0);
        db_set_record_allocsize(db, 0);
    }
    else if (size>db_get_record_allocsize(db)) {
        void *newdata=allocator_alloc(env_get_allocator(db_get_env(db)), size);
        if (!newdata) 
            return (HAM_OUT_OF_MEMORY);
        if (db_get_record_allocdata(db))
            allocator_free(env_get_allocator(db_get_env(db)), db_get_record_allocdata(db));
        db_set_record_allocdata(db, newdata);
        db_set_record_allocsize(db, size);
    }

    return (0);
}
