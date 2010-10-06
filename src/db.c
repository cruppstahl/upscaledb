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
#include "btree_stats.h"
#include "txn.h"
#include "version.h"


#define PURGE_THRESHOLD  (500 * 1024 * 1024) /* 500 mb */

typedef struct
{
    ham_db_t *db;               /* [in] */
    ham_u32_t flags;            /* [in] */
    ham_offset_t total_count;   /* [out] */
    ham_bool_t is_leaf;         /* [scratch] */
}  calckeys_context_t;

/*
 * callback function for estimating / counting the number of keys stored 
 * in the database
 */
static ham_status_t
my_calc_keys_cb(int event, void *param1, void *param2, void *context)
{
    btree_key_t *key;
    calckeys_context_t *c;
    ham_size_t count;

    c = (calckeys_context_t *)context;

    switch (event) 
    {
    case ENUM_EVENT_DESCEND:
        break;

    case ENUM_EVENT_PAGE_START:
        c->is_leaf = *(ham_bool_t *)param2;
        break;

    case ENUM_EVENT_PAGE_STOP:
        break;

    case ENUM_EVENT_ITEM:
        key = (btree_key_t *)param1;
        count = *(ham_size_t *)param2;

        if (c->is_leaf) {
            ham_size_t dupcount = 1;

            if (!(c->flags & HAM_SKIP_DUPLICATES)
                    && (key_get_flags(key) & KEY_HAS_DUPLICATES)) 
            {
                ham_status_t st = blob_duplicate_get_count(db_get_env(c->db), 
                        key_get_ptr(key), &dupcount, 0);
                if (st)
                    return st;
                c->total_count += dupcount;
            }
            else {
                c->total_count++;
            }

            if (c->flags & HAM_FAST_ESTIMATE) {
                /* 
                 * fast mode: just grab the keys-per-page value and 
                 * call it a day for this page.
                 *
                 * Assume all keys in this page have the same number 
                 * of dupes (=1 if no dupes)
                 */
                c->total_count += (count - 1) * dupcount;
                return CB_DO_NOT_DESCEND;
            }
        }
        break;

    default:
        ham_assert(!"unknown callback event", (""));
        break;
    }

    return CB_CONTINUE;
}


typedef struct free_cb_context_t
{
    ham_db_t *db;
    ham_bool_t is_leaf;

} free_cb_context_t;

/*
 * callback function for freeing blobs of an in-memory-database
 */
ham_status_t
free_inmemory_blobs_cb(int event, void *param1, void *param2, void *context)
{
    ham_status_t st;
    btree_key_t *key;
    free_cb_context_t *c;

    c=(free_cb_context_t *)context;

    switch (event) {
    case ENUM_EVENT_DESCEND:
        break;

    case ENUM_EVENT_PAGE_START:
        c->is_leaf=*(ham_bool_t *)param2;
        break;

    case ENUM_EVENT_PAGE_STOP:
        /*
         * if this callback function is called from ham_env_erase_db:
         * move the page to the freelist
         *
         * TODO
         */
        break;

    case ENUM_EVENT_ITEM:
        key=(btree_key_t *)param1;

        if (key_get_flags(key)&KEY_IS_EXTENDED) {
            ham_offset_t blobid=key_get_extended_rid(c->db, key);
            /*
             * delete the extended key
             */
            st = extkey_remove(c->db, blobid);
            if (st)
                return st;
        }

        if (key_get_flags(key)&(KEY_BLOB_SIZE_TINY
                            |KEY_BLOB_SIZE_SMALL
                            |KEY_BLOB_SIZE_EMPTY))
            break;

        /*
         * if we're in the leaf page, delete the blob
         */
        if (c->is_leaf) {
            st = key_erase_record(c->db, key, 0, BLOB_FREE_ALL_DUPES);
            if (st)
                return st;
        }
        break;

    default:
        ham_assert(!"unknown callback event", (0));
        return CB_STOP;
    }

    return CB_CONTINUE;
}

static ham_status_t
__record_filters_before_write(ham_db_t *db, ham_record_t *record)
{
    ham_status_t st=0;
    ham_record_filter_t *record_head;

    record_head=db_get_record_filter(db);
    while (record_head) 
    {
        if (record_head->before_write_cb) 
        {
            st=record_head->before_write_cb(db, record_head, record);
            if (st)
                break;
        }
        record_head=record_head->_next;
    }

    return (st);
}

/*
 * WATCH IT!
 *
 * as with the page filters, there was a bug in PRE-1.1.0 which would execute 
 * a record filter chain in the same order for both write (insert) and read 
 * (find), which means chained record filters would process invalid data in 
 * one of these, as a correct filter chain must traverse the transformation 
 * process IN REVERSE for one of these actions.
 * 
 * As with the page filters, we've chosen the WRITE direction to be the 
 * FORWARD direction, i.e. added filters end up processing data WRITTEN by 
 * the previous filter.
 * 
 * This also means the READ==FIND action must walk this chain in reverse.
 * 
 * See the documentation about the cyclic prev chain: the point is 
 * that FIND must traverse the record filter chain in REVERSE order so we 
 * should start with the LAST filter registered and stop once we've DONE 
 * calling the FIRST.
 */
static ham_status_t
__record_filters_after_find(ham_db_t *db, ham_record_t *record)
{
    ham_status_t st = 0;
    ham_record_filter_t *record_head;

    record_head=db_get_record_filter(db);
    if (record_head)
    {
        record_head = record_head->_prev;
        do
        {
            if (record_head->after_read_cb) 
            {
                st=record_head->after_read_cb(db, record_head, record);
                if (st)
                      break;
            }
            record_head = record_head->_prev;
        } while (record_head->_prev->_next);
    }
    return (st);
}

ham_status_t
db_uncouple_all_cursors(ham_page_t *page, ham_size_t start)
{
    ham_cursor_t *c = page_get_cursors(page);

    if (c) {
        ham_db_t *db = cursor_get_db(c);
        if (db) {
            ham_backend_t *be = db_get_backend(db);
            
            if (be) {
                return (*be->_fun_uncouple_all_cursors)(be, page, start);
            }
        }
    }

    return HAM_SUCCESS;
}

ham_u16_t
db_get_dbname(ham_db_t *db)
{
    ham_env_t *env;
	
	ham_assert(db!=0, (""));
    ham_assert(db_get_env(db), (""));

    env=db_get_env(db);

    if (env_get_header_page(env) && page_get_pers(env_get_header_page(env))) {
        db_indexdata_t *idx=env_get_indexdata_ptr(env, 
            db_get_indexdata_offset(db));
        return (index_get_dbname(idx));
    }
    
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
    (btree_key_t based) and a user-specified key (ham_key_t based), where the latter will always
    appear in the LHS and the important part here is: ham_key_t-based comparisons will have
    their key lengths possibly LARGER than the usual 'short' btree_key_t key length as the 
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

    /*
     * make sure that we have an extended key-cache
     *
     * in in-memory-db, the extkey-cache doesn't lead to performance
     * advantages; it only duplicates the data and wastes memory.
     * therefore we don't use it.
     */
    if (!(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)) {
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
                ext_key->data = (ham_u8_t *)allocator_alloc(alloc, key_length);
                if (!ext_key->data) 
                    return HAM_OUT_OF_MEMORY;
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
        ext_key->data = (ham_u8_t *)allocator_alloc(alloc, key_length);
        if (!ext_key->data)
            return HAM_OUT_OF_MEMORY;
    }

    memmove(ext_key->data, key_data, db_get_keysize(db)-sizeof(ham_offset_t));

    /*
     * now read the remainder of the key
     */
    memset(&record, 0, sizeof(record));
    record.data=(((ham_u8_t *)ext_key->data) + 
                    db_get_keysize(db)-sizeof(ham_offset_t));
    record.size = key_length - (db_get_keysize(db)-sizeof(ham_offset_t));
    record.flags = HAM_RECORD_USER_ALLOC;

    st=blob_read(db, blobid, &record, 0);
    if (st)
        return st;

    /* 
     * insert the FULL key in the extkey-cache 
     */
    if (db_get_extkey_cache(db)) {
        st = extkey_cache_insert(db_get_extkey_cache(db),
                blobid, key_length, ext_key->data);
        if (st)
            return st;
    }

    ext_key->size = (ham_u16_t)key_length;

    return (0);
}

int
db_compare_keys(ham_db_t *db, ham_key_t *lhs, ham_key_t *rhs)
{
    int cmp=HAM_PREFIX_REQUEST_FULLKEY;
    ham_compare_func_t foo=db_get_compare_func(db);
    ham_prefix_compare_func_t prefoo=db_get_prefix_compare_func(db);

    db_set_error(db, 0);

    /*
     * need prefix compare? if no key is extended we can just call the
     * normal compare function
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

#if defined(HAM_DEBUG) && defined(HAM_ENABLE_INTERNAL) && !defined(HAM_LEAN_AND_MEAN_FOR_PROFILING)
        if (cache_too_big(env_get_cache(env))) {
            (void)cache_check_integrity(env_get_cache(env));
        }
#endif

        while (cache_too_big(env_get_cache(env))) {
            page=cache_get_unused_page(env_get_cache(env));
            if (!page) {
                if (env_get_rt_flags(env)&HAM_CACHE_STRICT) 
                    return HAM_CACHE_FULL;
                else
                    break;
            }
            st=db_write_page_and_delete(page, 0);
            if (st)
                return st;
        }
    }

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
db_alloc_page_impl(ham_page_t **page_ref, ham_env_t *env, ham_db_t *db, 
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

    /* purge the cache, if necessary. if cache is unlimited, then we purge very
     * very rarely (but we nevertheless purge to avoid OUT OF MEMORY conditions
     * which can happen on Win32) */
    if (env_get_cache(env) 
            && !(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)) {
        ham_bool_t purge=HAM_TRUE;
#if defined(WIN32) && defined(HAM_32BIT)
        if (env_get_rt_flags(env)&HAM_CACHE_UNLIMITED) {
            ham_cache_t *cache=env_get_cache(env);
            if (cache_get_cur_elements(cache)*env_get_pagesize(env) 
                    > PURGE_THRESHOLD)
                purge=HAM_FALSE;
        }
#endif
        if (purge) {
            st=my_purge_cache(env);
            if (st)
                return st;
        }
    }

    /* first, we ask the freelist for a page */
    if (!(flags&PAGE_IGNORE_FREELIST)) {
        st=freel_alloc_page(&tellpos, env, db);
        ham_assert(st ? !tellpos : 1, (0));
        if (tellpos) {
            ham_assert(tellpos%env_get_pagesize(env)==0,
                    ("page id %llu is not aligned", tellpos));
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

    if (env_get_cache(env)) {
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

    *page_ref = page;
    return HAM_SUCCESS;
}

ham_status_t
db_alloc_page(ham_page_t **page_ref, ham_db_t *db, 
                ham_u32_t type, ham_u32_t flags)
{
    return (db_alloc_page_impl(page_ref, db_get_env(db), db, type, flags));
}

ham_status_t
db_fetch_page_impl(ham_page_t **page_ref, ham_env_t *env, ham_db_t *db,
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

    /* purge the cache, if necessary. if cache is unlimited, then we purge very
     * very rarely (but we nevertheless purge to avoid OUT OF MEMORY conditions
     * which can happen on Win32) */
    if (!(flags&DB_ONLY_FROM_CACHE) 
            && env_get_cache(env) 
            && !(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)) {
        ham_bool_t purge=HAM_TRUE;
#if defined(WIN32) && defined(HAM_32BIT)
        if (env_get_rt_flags(env)&HAM_CACHE_UNLIMITED) {
            ham_cache_t *cache=env_get_cache(env);
            if (cache_get_cur_elements(cache)*env_get_pagesize(env) 
                    > PURGE_THRESHOLD)
                purge=HAM_FALSE;
        }
#endif
        if (purge) {
            st=my_purge_cache(env);
            if (st)
                return st;
        }
    }

    /* 
     * fetch the page from the cache
     */
    if (env_get_cache(env)) {
        page=cache_get_page(env_get_cache(env), address, CACHE_NOREMOVE);
        if (page) {
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

    *page_ref = page;
    return HAM_SUCCESS;
}

ham_status_t
db_fetch_page(ham_page_t **page_ref, ham_db_t *db,
                ham_offset_t address, ham_u32_t flags)
{
    return (db_fetch_page_impl(page_ref, db_get_env(db), db, address, flags));
}

ham_status_t
db_flush_page(ham_env_t *env, ham_page_t *page, ham_u32_t flags)
{
    ham_status_t st;

    /* write the page, if it's dirty and if write-through is enabled */
    if ((env_get_rt_flags(env)&HAM_WRITE_THROUGH 
            || flags&HAM_WRITE_THROUGH 
            || !env_get_cache(env)) 
            && page_is_dirty(page)) {
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
            cache_set_totallist(cache,
                page_list_remove(cache_get_totallist(cache), 
                    PAGE_LIST_CACHED, head));
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
db_resize_record_allocdata(ham_db_t *db, ham_size_t size)
{
    if (size==0) {
        if (db_get_record_allocdata(db))
            allocator_free(env_get_allocator(db_get_env(db)), 
                    db_get_record_allocdata(db));
        db_set_record_allocdata(db, 0);
        db_set_record_allocsize(db, 0);
    }
    else if (size>db_get_record_allocsize(db)) {
        void *newdata=allocator_realloc(env_get_allocator(db_get_env(db)), 
                db_get_record_allocdata(db), size);
        if (!newdata) 
            return (HAM_OUT_OF_MEMORY);
        db_set_record_allocdata(db, newdata);
        db_set_record_allocsize(db, size);
    }

    return (0);
}

ham_status_t
db_resize_key_allocdata(ham_db_t *db, ham_size_t size)
{
    if (size==0) {
        if (db_get_key_allocdata(db))
            allocator_free(env_get_allocator(db_get_env(db)), 
                    db_get_key_allocdata(db));
        db_set_key_allocdata(db, 0);
        db_set_key_allocsize(db, 0);
    }
    else if (size>db_get_key_allocsize(db)) {
        void *newdata=allocator_realloc(env_get_allocator(db_get_env(db)), 
                db_get_key_allocdata(db), size);
        if (!newdata) 
            return (HAM_OUT_OF_MEMORY);
        db_set_key_allocdata(db, newdata);
        db_set_key_allocsize(db, size);
    }

    return (0);
}

ham_status_t
db_copy_key(ham_db_t *db, const ham_key_t *source, ham_key_t *dest)
{
    /*
     * extended key: copy the whole key
     */
    if (source->_flags&KEY_IS_EXTENDED) {
        ham_status_t st=db_get_extended_key(db, source->data,
                    source->size, source->_flags, dest);
        if (st) {
            return st;
        }
        ham_assert(dest->data!=0, ("invalid extended key"));
        /* dest->size is set by db_get_extended_key() */
        ham_assert(dest->size == source->size, (0)); 
        /* the extended flag is set later, when this key is inserted */
        dest->_flags = source->_flags & ~KEY_IS_EXTENDED;
    }
    else if (source->size) {
        if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
			if (!dest->data || dest->size < source->size) {
				if (dest->data)
					allocator_free(env_get_allocator(db_get_env(db)), 
                               dest->data);
				dest->data = (ham_u8_t *)allocator_alloc(
                               env_get_allocator(db_get_env(db)), source->size);
				if (!dest->data) 
					return (HAM_OUT_OF_MEMORY);
			}
		}
        memcpy(dest->data, source->data, source->size);
        dest->size=source->size;
        dest->_flags=source->_flags;
    }
    else { 
        /* key.size is 0 */
        if (!(dest->flags & HAM_KEY_USER_ALLOC)) {
            if (dest->data)
                allocator_free(env_get_allocator(db_get_env(db)), dest->data);
            dest->data=0;
        }
        dest->size=0;
        dest->_flags=source->_flags;
    }

    return (HAM_SUCCESS);
}

static ham_status_t
_local_fun_close(ham_db_t *db, ham_u32_t flags)
{
    ham_env_t *env=db_get_env(db);
    ham_status_t st = HAM_SUCCESS;
    ham_status_t st2 = HAM_SUCCESS;
    ham_backend_t *be;
    ham_bool_t noenv=HAM_FALSE;
    ham_db_t *newowner=0;
    ham_record_filter_t *record_head;

    /*
     * if this Database is the last database in the environment: 
     * delete all environment-members
     *
     * TODO noenv is no longer needed!
     */
    if (env) {
        ham_bool_t has_other=HAM_FALSE;
        ham_db_t *n=env_get_list(env);
        while (n) {
            if (n!=db) {
                has_other=HAM_TRUE;
                break;
            }
            n=db_get_next(n);
        }
        if (!has_other)
            noenv=HAM_TRUE;
    }

    be=db_get_backend(db);

    /* close all open cursors */
    if (be && be->_fun_close_cursors) {
        st = be->_fun_close_cursors(be, flags);
        if (st)
            return (st);
    }
    
    /* 
     * flush all DB performance data 
     */
    if (st2 == HAM_SUCCESS) {
        btree_stats_flush_dbdata(db, db_get_db_perf_data(db), noenv);
    }
    else {
        /* trash all DB performance data */
        btree_stats_trash_dbdata(db, db_get_db_perf_data(db));
    }

    /*
     * if we're not in read-only mode, and not an in-memory-database,
     * and the dirty-flag is true: flush the page-header to disk
     */
    if (env
            && env_get_header_page(env) 
            && noenv
            && !(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)
            && env_get_device(env) 
            && env_get_device(env)->is_open(env_get_device(env)) 
            && (!(db_get_rt_flags(db)&HAM_READ_ONLY))) {
        /* flush the database header, if it's dirty */
        if (env_is_dirty(env)) {
            st=page_flush(env_get_header_page(env));
            if (st) {
                if (st2 == 0) st2 = st;
            }
        }
    }

    /*
     * in-memory-database: free all allocated blobs
     */
    if (be && be_is_active(be) && env_get_rt_flags(env)&HAM_IN_MEMORY_DB) {
        ham_txn_t *txn;
        free_cb_context_t context;
        context.db=db;
        st = txn_begin(&txn, env, 0);
        if (st) {
            if (st2 == 0) st2 = st;
        }
        else {
            (void)be->_fun_enumerate(be, free_inmemory_blobs_cb, &context);
            (void)txn_commit(txn, 0);
        }
    }

    /*
     * immediately flush all pages of this database
     */
    if (env && env_get_cache(env)) {
        ham_page_t *n, *head=cache_get_totallist(env_get_cache(env)); 
        while (head) {
            n=page_get_next(head, PAGE_LIST_CACHED);
            if (page_get_owner(head)==db) {
                if (!(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)) 
                    (void)db_flush_page(env, head, HAM_WRITE_THROUGH);
                (void)db_free_page(head, 0);
            }
            head=n;
        }
    }

    /*
     * free cached memory
     */
    (void)db_resize_record_allocdata(db, 0);
    if (db_get_key_allocdata(db)) {
        allocator_free(env_get_allocator(env), db_get_key_allocdata(db));
        db_set_key_allocdata(db, 0);
        db_set_key_allocsize(db, 0);
    }

    /*
     * free the cache for extended keys
     */
    if (db_get_extkey_cache(db)) {
        extkey_cache_destroy(db_get_extkey_cache(db));
        db_set_extkey_cache(db, 0);
    }

    /* free the transaction tree */
    if (db_get_optree(db)) {
        txn_free_optree(db_get_optree(db));
        db_set_optree(db, 0);
    }

    /* close the backend */
    if (be && be_is_active(be)) {
        st=be->_fun_close(be);
        if (st) {
            if (st2 == 0) st2 = st;
        }
        else {
            ham_assert(!be_is_active(be), (0));
        }
    }
    if (be) {
        ham_assert(!be_is_active(be), (0));

        st = be->_fun_delete(be);
        if (st2 == 0)
            st2 = st;

        /*
         * TODO
         * this free() should move into the backend destructor 
         */
        allocator_free(env_get_allocator(env), be);
        db_set_backend(db, 0);
    }

    /*
     * environment: move the ownership to another database.
     * it's possible that there's no other page, then set the 
     * ownership to 0
     */
    if (env) {
        ham_db_t *head=env_get_list(env);
        while (head) {
            if (head!=db) {
                newowner=head;
                break;
            }
            head=db_get_next(head);
        }
    }
    if (env && env_get_header_page(env)) {
        ham_assert(env_get_header_page(env), (0));
        page_set_owner(env_get_header_page(env), newowner);
    }

    /*
     * close all record-level filters
     */
    record_head=db_get_record_filter(db);
    while (record_head) {
        ham_record_filter_t *next=record_head->_next;

        if (record_head->close_cb)
            record_head->close_cb(db, record_head);
        record_head=next;
    }
    db_set_record_filter(db, 0);

    /* 
     * trash all DB performance data 
     *
     * This must happen before the DB is removed from the ENV as the ENV 
     * (when it exists) provides the required allocator.
     */
    btree_stats_trash_dbdata(db, db_get_db_perf_data(db));

    return (st2);
}

static ham_status_t
_local_fun_get_parameters(ham_db_t *db, ham_parameter_t *param)
{
    ham_parameter_t *p=param;
    ham_env_t *env;

    env=db_get_env(db);

    if (p) {
        for (; p->name; p++) {
            switch (p->name) {
            case HAM_PARAM_CACHESIZE:
                p->value=env_get_cachesize(env);
                break;
            case HAM_PARAM_PAGESIZE:
                p->value=env_get_pagesize(env);
                break;
            case HAM_PARAM_KEYSIZE:
                p->value=db_get_backend(db) ? db_get_keysize(db) : 21;
                break;
            case HAM_PARAM_MAX_ENV_DATABASES:
                p->value=env_get_max_databases(env);
                break;
            case HAM_PARAM_GET_FLAGS:
                p->value=db_get_rt_flags(db);
                break;
            case HAM_PARAM_GET_FILEMODE:
                p->value=env_get_file_mode(db_get_env(db));
                break;
            case HAM_PARAM_GET_FILENAME:
                if (env_get_filename(env))
                    p->value=(ham_u64_t)PTR_TO_U64(env_get_filename(env));
                else
                    p->value=0;
                break;
            case HAM_PARAM_GET_DATABASE_NAME:
                p->value=(ham_offset_t)db_get_dbname(db);
                break;
            case HAM_PARAM_GET_KEYS_PER_PAGE:
                if (db_get_backend(db)) {
                    ham_size_t count=0, size=db_get_keysize(db);
                    ham_backend_t *be = db_get_backend(db);
                    ham_status_t st;

                    if (!be->_fun_calc_keycount_per_page)
                        return (HAM_NOT_IMPLEMENTED);
                    st = be->_fun_calc_keycount_per_page(be, &count, size);
                    if (st)
                        return st;
                    p->value=count;
                }
                break;
            case HAM_PARAM_GET_DATA_ACCESS_MODE:
                p->value=db_get_data_access_mode(db);
                break;
            case HAM_PARAM_GET_STATISTICS:
                if (!p->value) {
                    ham_trace(("the value for parameter "
                               "'HAM_PARAM_GET_STATISTICS' must not be NULL "
                               "and reference a ham_statistics_t data "
                               "structure before invoking "
                               "ham_[env_]get_parameters"));
                    return (HAM_INV_PARAMETER);
                }
                else {
                    ham_status_t st = btree_stats_fill_ham_statistics_t(
                            env, db, (ham_statistics_t *)U64_TO_PTR(p->value));
                    if (st)
                        return st;
                }
                break;
            default:
                ham_trace(("unknown parameter %d", (int)p->name));
                return (HAM_INV_PARAMETER);
            }
        }
    }

    return (0);
}

static ham_status_t
_local_fun_check_integrity(ham_db_t *db, ham_txn_t *txn)
{
#ifdef HAM_ENABLE_INTERNAL
    ham_txn_t *local_txn;
    ham_status_t st;
    ham_backend_t *be;

    /*
     * check the cache integrity
     */
    if (!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB))
    {
        st=cache_check_integrity(env_get_cache(db_get_env(db)));
        if (st)
            return (st);
    }

    be=db_get_backend(db);
    if (!be)
        return (HAM_NOT_INITIALIZED);
    if (!be->_fun_check_integrity)
        return (HAM_NOT_IMPLEMENTED);

    if (!txn) {
        if ((st=txn_begin(&local_txn, db_get_env(db), HAM_TXN_READ_ONLY)))
            return (st);
    }

    /*
     * call the backend function
     */
    st=be->_fun_check_integrity(be);

    if (st) {
        if (!txn)
            (void)txn_abort(local_txn, 0);
        return (st);
    }

    if (!txn)
        return (txn_commit(local_txn, 0));
    else
        return (st);
#else
    return (HAM_NOT_IMPLEMENTED);
#endif /* ifdef HAM_ENABLE_INTERNAL */
}

static ham_status_t
_local_fun_get_key_count(ham_db_t *db, ham_txn_t *txn, ham_u32_t flags,
            ham_offset_t *keycount)
{
    ham_txn_t *local_txn;
    ham_status_t st;
    ham_backend_t *be;
    ham_env_t *env=0;
    calckeys_context_t ctx = {db, flags, 0, HAM_FALSE};

    env=db_get_env(db);

    if (flags & ~(HAM_SKIP_DUPLICATES | HAM_FAST_ESTIMATE)) {
        ham_trace(("parameter 'flag' contains unsupported flag bits: %08x", 
                  flags & ~(HAM_SKIP_DUPLICATES | HAM_FAST_ESTIMATE)));
        return (HAM_INV_PARAMETER);
    }

    be = db_get_backend(db);
    if (!be || !be_is_active(be))
        return (HAM_NOT_INITIALIZED);
    if (!be->_fun_enumerate)
        return (HAM_NOT_IMPLEMENTED);

    if (!txn) {
        st = txn_begin(&local_txn, env, HAM_TXN_READ_ONLY);
        if (st)
            return (st);
    }

    /*
     * call the backend function
     */
    st = be->_fun_enumerate(be, my_calc_keys_cb, &ctx);

    if (st) {
        if (!txn)
            (void)txn_abort(local_txn, 0);
        return (st);
    }

    *keycount = ctx.total_count;

    if (!txn)
        return (txn_commit(local_txn, 0));
    else
        return (st);
}

static ham_status_t
db_check_insert_conflicts(ham_db_t *db, ham_txn_t *txn, 
                txn_optree_node_t *node, ham_key_t *key, ham_u32_t flags)
{
    ham_status_t st;
    txn_op_t *op=0;

    /*
     * pick the tree_node of this key, and walk through each operation 
     * in reverse chronological order (from newest to oldest):
     * - is this op part of an aborted txn? then skip it
     * - is this op part of a committed txn? then look at the
     *      operation in detail
     * - is this op part of an txn which is still active? return an error
     *      because we've found a conflict
     * - if a committed txn has erased the item then there's no need
     *      to continue checking older, committed txns
     */
    op=txn_optree_node_get_newest_op(node);
    while (op) {
        ham_txn_t *optxn=txn_op_get_txn(op);
        if (txn_get_flags(optxn)&TXN_STATE_ABORTED)
            ; /* nop */
        else if (txn_get_flags(optxn)&TXN_STATE_COMMITTED) {
            /* if key was erased then it doesn't exist and can be
             * inserted without problems */
            if (txn_op_get_flags(op)&TXN_OP_ERASE)
                return (0);
            else if (txn_op_get_flags(op)&TXN_OP_NOP)
                ; /* nop */
            /* if the key already exists then we can only continue if
             * we're allowed to overwrite it or to insert a duplicate */
            else if ((txn_op_get_flags(op)&TXN_OP_INSERT_OW)
                    || (txn_op_get_flags(op)&TXN_OP_INSERT_DUP)) {
                if ((flags&HAM_OVERWRITE) || (flags&HAM_DUPLICATE))
                    return (0);
                else
                    return (HAM_DUPLICATE_KEY);
            }
            else {
                ham_assert(!"shouldn't be here", (""));
                return (HAM_DUPLICATE_KEY);
            }
        }
        else { /* txn is still active */
            /* TODO txn_set_conflict_txn(txn, optxn); */
            return (HAM_TXN_CONFLICT);
        }

        op=txn_op_get_next_in_node(op);
    }

    /*
     * we've successfully checked all un-flushed transactions and there
     * were no conflicts. Now check all transactions which are already
     * flushed - basically that's identical to a btree lookup.
     *
     * however we can skip this check if we do not care about duplicates.
     */
    if ((flags&HAM_OVERWRITE) || (flags&HAM_DUPLICATE))
        return (0);
    st=db->_fun_find(db, 0, key, 0, 0);
    if (st==HAM_KEY_NOT_FOUND)
        return (0);
    if (st=HAM_SUCCESS)
        return (HAM_DUPLICATE);
    return (st);
}

static ham_status_t
db_insert_txn(ham_db_t *db, ham_txn_t *txn,
        ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    txn_optree_t *tree;
    txn_optree_node_t *node;
    txn_op_t *op;

    /* get (or create) the txn-tree for this database; we do not need
     * the returned value, but we call the function to trigger the 
     * tree creation if it does not yet exist */
    tree=txn_tree_get_or_create(db);
    if (!tree)
        return (HAM_OUT_OF_MEMORY);

    /* get (or create) the node for this key */
    node=txn_optree_node_get_or_create(db, key);
    if (!node)
        return (HAM_OUT_OF_MEMORY);

    /* check for conflicts of this key */
    st=db_check_insert_conflicts(db, txn, node, key, flags);
    if (st)
        return (st);

    /* append a new operation to this node */
    /* TODO lsn is missing! */
    op=txn_optree_node_append(txn, node, 
                    (flags&HAM_DUPLICATE) 
                        ? TXN_OP_INSERT_DUP 
                        : TXN_OP_INSERT_OW, 0, record);
    if (!op)
        return (HAM_OUT_OF_MEMORY);

    return (0);
}

static ham_status_t
_local_fun_insert(ham_db_t *db, ham_txn_t *txn,
        ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
    ham_env_t *env=db_get_env(db);
    ham_txn_t *local_txn=0;
    ham_status_t st;
    ham_backend_t *be;
    ham_u64_t recno = 0;
    ham_record_t temprec;

    be=db_get_backend(db);
    if (!be || !be_is_active(be))
        return (HAM_NOT_INITIALIZED);
    if (!be->_fun_insert)
        return HAM_NOT_IMPLEMENTED;

    if (!txn && (db_get_rt_flags(db)&HAM_ENABLE_TRANSACTIONS)) {
        st=txn_begin(&local_txn, env, 0);
        if (st)
            return (st);
    }

    /*
     * record number: make sure that we have a valid key structure,
     * and lazy load the last used record number
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        if (flags&HAM_OVERWRITE) {
            ham_assert(key->size==sizeof(ham_u64_t), (""));
            ham_assert(key->data!=0, (""));
            recno=*(ham_u64_t *)key->data;
        }
        else {
            /* get the record number (host endian) and increment it */
            recno=be_get_recno(be);
            recno++;
        }

        /* store it in db endian */
        recno=ham_h2db64(recno);
        memcpy(key->data, &recno, sizeof(ham_u64_t));
        key->size=sizeof(ham_u64_t);
    }

    /*
     * run the record-level filters on a temporary record structure - we
     * don't want to mess up the original structure
     */
    temprec=*record;
    st=__record_filters_before_write(db, &temprec);

    /* 
     * if transactions are enabled: only insert the key/record pair into
     * the Transaction structore. Otherwise immediately write to disk
     */
    if (!st) {
        if (txn || local_txn)
            st=db_insert_txn(db, txn, key, &temprec, flags);
        else
            st=be->_fun_insert(be, key, &temprec, flags);
    }

    if (temprec.data!=record->data)
        allocator_free(env_get_allocator(env), temprec.data);

    if (st) {
        if (!txn)
            (void)txn_abort(local_txn, 0);

        if ((db_get_rt_flags(db)&HAM_RECORD_NUMBER) && !(flags&HAM_OVERWRITE)) {
            if (!(key->flags&HAM_KEY_USER_ALLOC)) {
                key->data=0;
                key->size=0;
            }
            ham_assert(st!=HAM_DUPLICATE_KEY, ("duplicate key in recno db!"));
        }
        return (st);
    }

    /*
     * record numbers: return key in host endian! and store the incremented
     * record number
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        recno=ham_db2h64(recno);
        memcpy(key->data, &recno, sizeof(ham_u64_t));
        key->size=sizeof(ham_u64_t);
        if (!(flags&HAM_OVERWRITE)) {
            be_set_recno(be, recno);
            be_set_dirty(be, HAM_TRUE);
            env_set_dirty(env);
        }
    }

    if (!txn)
        return (txn_commit(local_txn, 0));
    else
        return (st);
}

static ham_status_t
_local_fun_erase(ham_db_t *db, ham_txn_t *txn, ham_key_t *key, ham_u32_t flags)
{
    ham_txn_t *local_txn;
    ham_status_t st;
    ham_env_t *env=db_get_env(db);
    ham_backend_t *be;
    ham_offset_t recno=0;

    be=db_get_backend(db);
    if (!be || !be_is_active(be))
        return (HAM_NOT_INITIALIZED);
    if (!be->_fun_erase)
        return HAM_NOT_IMPLEMENTED;
    if (db_get_rt_flags(db)&HAM_READ_ONLY) {
        ham_trace(("cannot erase from a read-only database"));
        return (HAM_DB_READ_ONLY);
    }

    /*
     * record number: make sure that we have a valid key structure
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        if (key->size!=sizeof(ham_u64_t) || !key->data) {
            ham_trace(("key->size must be 8, key->data must not be NULL"));
            return (HAM_INV_PARAMETER);
        }
        recno=*(ham_offset_t *)key->data;
        recno=ham_h2db64(recno);
        *(ham_offset_t *)key->data=recno;
    }

    if (!txn) {
        if ((st=txn_begin(&local_txn, env, 0)))
            return (st);
    }

    db_update_global_stats_erase_query(db, key->size);

    /*
     * get rid of the entry
     */
    st=be->_fun_erase(be, key, flags);

    if (st) {
        if (!txn)
            (void)txn_abort(local_txn, 0);
        return (st);
    }

    /*
     * record number: re-translate the number to host endian
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        *(ham_offset_t *)key->data=ham_db2h64(recno);
    }

    if (!txn)
        return (txn_commit(local_txn, 0));
    else
        return (st);
}

static ham_status_t
_local_fun_find(ham_db_t *db, ham_txn_t *txn, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
    ham_env_t *env=db_get_env(db);
    ham_txn_t *local_txn;
    ham_status_t st;
    ham_backend_t *be;
    ham_offset_t recno=0;

    if ((db_get_keysize(db)<sizeof(ham_offset_t)) &&
            (key->size>db_get_keysize(db))) {
        ham_trace(("database does not support variable length keys"));
        return (HAM_INV_KEYSIZE);
    }

    /*
     * record number: make sure we have a number in little endian
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        ham_assert(key->size==sizeof(ham_u64_t), (""));
        ham_assert(key->data!=0, (""));

        recno=*(ham_offset_t *)key->data;
        recno=ham_h2db64(recno);
        *(ham_offset_t *)key->data=recno;
    }

    be=db_get_backend(db);
    if (!be || !be_is_active(be))
        return (HAM_NOT_INITIALIZED);

    if (!be->_fun_find)
        return HAM_NOT_IMPLEMENTED;

    if (!txn) {
        st=txn_begin(&local_txn, env, HAM_TXN_READ_ONLY);
        if (st)
            return (st);
    }

    db_update_global_stats_find_query(db, key->size);

    /*
     * first look up the blob id, then fetch the blob
     */
    st=be->_fun_find(be, key, record, flags);

    if (st) {
        if (!txn)
            (void)txn_abort(local_txn, 0);
        return (st);
    }

    /*
     * record number: re-translate the number to host endian
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        *(ham_offset_t *)key->data=ham_db2h64(recno);
    }

    /*
     * run the record-level filters
     */
    st=__record_filters_after_find(db, record);
    if (st) {
        if (!txn)
            (void)txn_abort(local_txn, 0);
        return (st);
    }

    if (!txn)
        return (txn_commit(local_txn, 0));
    else
        return (st);
}

static ham_status_t
_local_cursor_create(ham_db_t *db, ham_txn_t *txn, ham_u32_t flags,
        ham_cursor_t **cursor)
{
    ham_backend_t *be;
    ham_status_t st;

    be=db_get_backend(db);
    if (!be || !be_is_active(be))
        return (HAM_NOT_INITIALIZED);
    if (!be->_fun_cursor_create)
        return (HAM_NOT_IMPLEMENTED);

    st=be->_fun_cursor_create(be, db, txn, flags, cursor);
    if (st)
        return (st);

    if (txn)
        txn_set_cursor_refcount(txn, txn_get_cursor_refcount(txn)+1);

    return (0);
}

static ham_status_t
_local_cursor_clone(ham_cursor_t *src, ham_cursor_t **dest)
{
    ham_status_t st;
    ham_txn_t *local_txn;
    ham_db_t *db=cursor_get_db(src);
    ham_env_t *env;

    env = db_get_env(db);

    if (!cursor_get_txn(src)) {
        st=txn_begin(&local_txn, env, HAM_TXN_READ_ONLY);
        if (st)
            return (st);
    }

    st=src->_fun_clone(src, dest);
    if (st) {
        if (!cursor_get_txn(src))
            (void)txn_abort(local_txn, 0);
        return (st);
    }

    if (cursor_get_txn(src))
        txn_set_cursor_refcount(cursor_get_txn(src), 
                txn_get_cursor_refcount(cursor_get_txn(src))+1);

    if (!cursor_get_txn(src))
        return (txn_commit(local_txn, 0));
    else
        return (0);
}

static ham_status_t
_local_cursor_close(ham_cursor_t *cursor)
{
    return (cursor->_fun_close(cursor));
}

static ham_status_t
_local_cursor_insert(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    ham_backend_t *be;
    ham_u64_t recno = 0;
    ham_record_t temprec;
    ham_txn_t *local_txn;
    ham_db_t *db=cursor_get_db(cursor);
    ham_env_t *env=db_get_env(db);

    be=db_get_backend(db);
    if (!be)
        return (HAM_NOT_INITIALIZED);

    if ((db_get_keysize(db)<sizeof(ham_offset_t)) &&
            (key->size>db_get_keysize(db))) {
        ham_trace(("database does not support variable length keys"));
        return (HAM_INV_KEYSIZE);
    }

    /*
     * record number: make sure that we have a valid key structure,
     * and lazy load the last used record number
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        if (flags&HAM_OVERWRITE) {
            ham_assert(key->size==sizeof(ham_u64_t), (""));
            ham_assert(key->data!=0, (""));
            recno=*(ham_u64_t *)key->data;
        }
        else {
            /*
             * get the record number (host endian) and increment it
             */
            recno=be_get_recno(be);
            recno++;
        }

        /*
         * store it in db endian
         */
        recno=ham_h2db64(recno);
        memcpy(key->data, &recno, sizeof(ham_u64_t));
        key->size=sizeof(ham_u64_t);

        /*
         * we're appending this key sequentially
         */
        flags|=HAM_HINT_APPEND;
    }

    if (!cursor_get_txn(cursor)) {
        if ((st=txn_begin(&local_txn, env, 0)))
            return (st);
    }

    /*
     * run the record-level filters on a temporary record structure - we
     * don't want to mess up the original structure
     */
    temprec=*record;
    st=__record_filters_before_write(db, &temprec);
    if (!st) {
        db_update_global_stats_insert_query(db, key->size, temprec.size);
    }

    if (!st) {
        st=cursor->_fun_insert(cursor, key, &temprec, flags);
    }

    if (temprec.data!=record->data)
        allocator_free(env_get_allocator(env), temprec.data);

    if (st) {
        if (!cursor_get_txn(cursor))
            (void)txn_abort(local_txn, 0);
        if ((db_get_rt_flags(db)&HAM_RECORD_NUMBER) && !(flags&HAM_OVERWRITE)) {
            if (!(key->flags&HAM_KEY_USER_ALLOC)) {
                key->data=0;
                key->size=0;
            }
            ham_assert(st!=HAM_DUPLICATE_KEY, ("duplicate key in recno db!"));
        }
        return (st);
    }

    /*
     * record numbers: return key in host endian! and store the incremented
     * record number
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        recno=ham_db2h64(recno);
        memcpy(key->data, &recno, sizeof(ham_u64_t));
        key->size=sizeof(ham_u64_t);
        if (!(flags&HAM_OVERWRITE)) {
            be_set_recno(be, recno);
            be_set_dirty(be, HAM_TRUE);
            env_set_dirty(env);
        }
    }

    if (!cursor_get_txn(cursor))
        return (txn_commit(local_txn, 0));
    else
        return (st);
}

static ham_status_t 
_local_cursor_erase(ham_cursor_t *cursor, ham_u32_t flags)
{
    ham_status_t st;
    ham_txn_t *local_txn;
    ham_db_t *db=cursor_get_db(cursor);
    ham_env_t *env=db_get_env(db);

    if (!cursor_get_txn(cursor)) {
        st=txn_begin(&local_txn, env, 0);
        if (st)
            return (st);
    }

    db_update_global_stats_erase_query(db, 0);

    st=cursor->_fun_erase(cursor, flags);
    if (st) {
        if (!cursor_get_txn(cursor))
            (void)txn_abort(local_txn, 0);
        return (st);
    }

    if (!cursor_get_txn(cursor))
        return (txn_commit(local_txn, 0));
    else
        return (st);
}

static ham_status_t
_local_cursor_find(ham_cursor_t *cursor, ham_key_t *key, 
            ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    ham_txn_t *local_txn;
    ham_db_t *db=cursor_get_db(cursor);
    ham_env_t *env=db_get_env(db);
    ham_offset_t recno=0;

    /*
     * record number: make sure that we have a valid key structure,
     * and translate the record number to database endian
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        if (key->size!=sizeof(ham_u64_t) || !key->data) {
            ham_trace(("key->size must be 8, key->data must not be NULL"));
            if (!cursor_get_txn(cursor))
                (void)txn_abort(local_txn, 0);
            return (HAM_INV_PARAMETER);
        }
        recno=*(ham_offset_t *)key->data;
        recno=ham_h2db64(recno);
        *(ham_offset_t *)key->data=recno;
    }

    if (!cursor_get_txn(cursor)) {
        st=txn_begin(&local_txn, env, HAM_TXN_READ_ONLY);
        if (st)
            return (st);
    }

    db_update_global_stats_find_query(db, key->size);

    st=cursor->_fun_find(cursor, key, record, flags);
    if (st) {
        if (!cursor_get_txn(cursor))
            (void)txn_abort(local_txn, 0);
        return (st);
    }

    /*
     * record number: re-translate the number to host endian
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        *(ham_offset_t *)key->data=ham_db2h64(recno);
    }

    /*
     * run the record-level filters
     */
    if (record) {
        st=__record_filters_after_find(db, record);
        if (st) {
            if (!cursor_get_txn(cursor))
                (void)txn_abort(local_txn, 0);
            return (st);
        }
    }

    if (!cursor_get_txn(cursor))
        return (txn_commit(local_txn, 0));
    else
        return (0);
}

static ham_status_t
_local_cursor_get_duplicate_count(ham_cursor_t *cursor, 
        ham_size_t *count, ham_u32_t flags)
{
    ham_status_t st;
    ham_txn_t *local_txn;
    ham_db_t *db=cursor_get_db(cursor);
    ham_env_t *env=db_get_env(db);

    if (!cursor_get_txn(cursor)) {
        st=txn_begin(&local_txn, env, HAM_TXN_READ_ONLY);
        if (st)
            return (st);
    }

    st=(*cursor->_fun_get_duplicate_count)(cursor, count, flags);
    if (st) {
        if (!cursor_get_txn(cursor))
            (void)txn_abort(local_txn, 0);
        return (st);
    }

    if (!cursor_get_txn(cursor))
        return (txn_commit(local_txn, 0));
    else
        return (st);
}

static ham_status_t
_local_cursor_overwrite(ham_cursor_t *cursor, ham_record_t *record,
            ham_u32_t flags)
{
    ham_db_t *db=cursor_get_db(cursor);
    ham_env_t *env=db_get_env(db);
    ham_status_t st;
    ham_txn_t *local_txn;
    ham_record_t temprec;

    if (!cursor_get_txn(cursor)) {
        st=txn_begin(&local_txn, env, 0);
        if (st)
            return (st);
    }

    /*
     * run the record-level filters on a temporary record structure - we
     * don't want to mess up the original structure
     */
    temprec=*record;
    st=__record_filters_before_write(db, &temprec);
    if (st) {
        if (!cursor_get_txn(cursor))
            (void)txn_abort(local_txn, 0);
        return (st);
    }

    st=cursor->_fun_overwrite(cursor, &temprec, flags);

    ham_assert(env_get_allocator(env) == cursor_get_allocator(cursor), (0));
    if (temprec.data != record->data)
        allocator_free(env_get_allocator(env), temprec.data);

    if (st) {
        if (!cursor_get_txn(cursor))
            (void)txn_abort(local_txn, 0);
        return (st);
    }

    if (!cursor_get_txn(cursor))
        return (txn_commit(local_txn, 0));
    else
        return (st);
}

static ham_status_t
_local_cursor_move(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    ham_db_t *db=cursor_get_db(cursor);
    ham_env_t *env=db_get_env(db);
    ham_txn_t *local_txn;

    if (!cursor_get_txn(cursor)) {
        st=txn_begin(&local_txn, env, HAM_TXN_READ_ONLY);
        if (st)
            return (st);
    }

    st=cursor->_fun_move(cursor, key, record, flags);
    if (st) {
        if (!cursor_get_txn(cursor))
            (void)txn_abort(local_txn, 0);
        return (st);
    }

    /*
     * run the record-level filters
     */
    if (record) {
        st=__record_filters_after_find(db, record);
        if (st) {
            if (!cursor_get_txn(cursor))
                (void)txn_abort(local_txn, 0);
            return (st);
        }
    }

    if (!cursor_get_txn(cursor))
        return (txn_commit(local_txn, 0));
    else
        return (st);
}


ham_status_t
db_initialize_local(ham_db_t *db)
{
    db->_fun_close          =_local_fun_close;
    db->_fun_get_parameters =_local_fun_get_parameters;
    db->_fun_check_integrity=_local_fun_check_integrity;
    db->_fun_get_key_count  =_local_fun_get_key_count;
    db->_fun_insert         =_local_fun_insert;
    db->_fun_erase          =_local_fun_erase;
    db->_fun_find           =_local_fun_find;
    db->_fun_cursor_create  =_local_cursor_create;
    db->_fun_cursor_clone   =_local_cursor_clone;
    db->_fun_cursor_close   =_local_cursor_close;
    db->_fun_cursor_insert  =_local_cursor_insert;
    db->_fun_cursor_erase   =_local_cursor_erase;
    db->_fun_cursor_find    =_local_cursor_find;
    db->_fun_cursor_get_duplicate_count=_local_cursor_get_duplicate_count;
    db->_fun_cursor_overwrite=_local_cursor_overwrite;
    db->_fun_cursor_move    =_local_cursor_move;

    return (0);
}

