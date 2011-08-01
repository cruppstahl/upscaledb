/*
 * Copyright (C) 2005-2011 Christoph Rupp (chris@crupp.de).
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
#include "cache.h"
#include "cursor.h"
#include "btree_cursor.h"
#include "db.h"
#include "device.h"
#include "env.h"
#include "error.h"
#include "extkeys.h"
#include "freelist.h"
#include "log.h"
#include "journal.h"
#include "mem.h"
#include "os.h"
#include "page.h"
#include "btree_stats.h"
#include "txn.h"
#include "txn_cursor.h"
#include "version.h"


#define PURGE_THRESHOLD       (500 * 1024 * 1024) /* 500 mb */
#define DUMMY_LSN                               1
#define SHITTY_HACK_FIX_ME                    999
#define SHITTY_HACK_DONT_MOVE_DUPLICATE 0xf000000
#define SHITTY_HACK_REACHED_EOF         0xf100000

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
__calc_keys_cb(int event, void *param1, void *param2, void *context)
{
    btree_key_t *key;
    calckeys_context_t *c;
    ham_size_t count;

    c = (calckeys_context_t *)context;

    switch (event) {
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
                    && (key_get_flags(key) & KEY_HAS_DUPLICATES)) {
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
__free_inmemory_blobs_cb(int event, void *param1, void *param2, void *context)
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
        /* nop */
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
            st = key_erase_record(c->db, key, 0, HAM_ERASE_ALL_DUPLICATES);
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

static ham_bool_t
__cache_needs_purge(ham_env_t *env)
{
    ham_cache_t *cache=env_get_cache(env);
    if (!cache)
        return (HAM_FALSE);

    /* purge the cache, if necessary. if cache is unlimited, then we purge very
     * very rarely (but we nevertheless purge to avoid OUT OF MEMORY conditions
     * which can happen on 32bit Windows) */
    if (cache && !(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)) {
        ham_bool_t purge=cache_too_big(cache);
#if defined(WIN32) && defined(HAM_32BIT)
        if (env_get_rt_flags(env)&HAM_CACHE_UNLIMITED) {
            if (cache_get_cur_elements(cache)*env_get_pagesize(env) 
                    > PURGE_THRESHOLD)
                return (HAM_FALSE);
        }
#endif
        return (purge);
    }
    return (HAM_FALSE);
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
        cache_remove_page(env_get_cache(env), page);
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
    ham_page_t *page=NULL;
    ham_bool_t allocated_by_me=HAM_FALSE;

    *page_ref = 0;
    ham_assert(0==(flags&~(PAGE_IGNORE_FREELIST|PAGE_CLEAR_WITH_ZERO)), (0));
    ham_assert(env_get_cache(env) != 0, 
            ("This code will not realize the requested page may already exist "
             "through a previous call to this function or db_fetch_page() "
             "unless page caching is available!"));

    /* first, we ask the freelist for a page */
    if (!(flags&PAGE_IGNORE_FREELIST)) {
        st=freel_alloc_page(&tellpos, env, db);
        ham_assert(st ? !tellpos : 1, (0));
        if (tellpos) {
            ham_assert(tellpos%env_get_pagesize(env)==0,
                    ("page id %llu is not aligned", tellpos));
            /* try to fetch the page from the changeset */
            page=changeset_get_page(env_get_changeset(env), tellpos);
            if (page)
                goto done;
            /* try to fetch the page from the cache */
            if (env_get_cache(env)) {
                page=cache_get_page(env_get_cache(env), tellpos, 0);
                if (page)
                    goto done;
            }
            /* allocate a new page structure */
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
            return st;
    }

    if (!page) {
        page=page_new(env);
        if (!page)
            return HAM_OUT_OF_MEMORY;
        allocated_by_me=HAM_TRUE;
    }

    /* can we allocate a new page for the cache? */
    if (cache_too_big(env_get_cache(env))) {
        if (env_get_rt_flags(env)&HAM_CACHE_STRICT) {
            if (allocated_by_me)
                page_delete(page);
            return (HAM_CACHE_FULL);
        }
    }

    ham_assert(tellpos == 0, (0));
    st=page_alloc(page);
    if (st)
        return st;

done:
    /* initialize the page; also set the 'dirty' flag to force logging */
    page_set_type(page, type);
    page_set_owner(page, db);
    page_set_dirty(page);

    /* clear the page with zeroes?  */
    if (flags&PAGE_CLEAR_WITH_ZERO)
        memset(page_get_pers(page), 0, env_get_pagesize(env));

    /* an allocated page is always flushed if recovery is enabled */
    if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY)
        changeset_add_page(env_get_changeset(env), page);

    /* store the page in the cache */
    if (env_get_cache(env))
        cache_put_page(env_get_cache(env), page);

    *page_ref = page;
    return (HAM_SUCCESS);
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

    ham_assert(0 == (flags & ~(HAM_HINTS_MASK|DB_ONLY_FROM_CACHE)), (0));
    ham_assert(!(env_get_rt_flags(env)&HAM_IN_MEMORY_DB) 
            ? 1 
            : !!env_get_cache(env), ("in-memory DBs MUST have a cache"));
    ham_assert((env_get_rt_flags(env)&HAM_IN_MEMORY_DB) 
            ? 1 
            : env_get_cache(env) != 0, 
            ("This code will not realize the requested page may already "
             "exist through a previous call to this function or "
             "db_alloc_page() unless page caching is available!"));

    *page_ref = 0;

    /* fetch the page from the changeset */
    page=changeset_get_page(env_get_changeset(env), address);
    if (page) {
        *page_ref = page;
        ham_assert(page_get_pers(page), (""));
        ham_assert(db ? page_get_owner(page)==db : 1, (""));
        return (HAM_SUCCESS);
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
            /* store the page in the changeset, but only if recovery is enabled
             * and not if Transactions are enabled. 
             *
             * when inserting a key in a transaction, this can trigger lookups
             * with ham_find(). when ham_find() fetches pages, these pages must
             * not be added to the changeset.
             */
            if ((env_get_rt_flags(env)&HAM_ENABLE_RECOVERY)
                    && !(env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS))
                changeset_add_page(env_get_changeset(env), page);
            return (HAM_SUCCESS);
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

    /* can we allocate a new page for the cache? */
    if (cache_too_big(env_get_cache(env))) {
        if (env_get_rt_flags(env)&HAM_CACHE_STRICT) 
            return (HAM_CACHE_FULL);
    }

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

    /* store the page in the cache */
    if (env_get_cache(env))
        cache_put_page(env_get_cache(env), page);

    /* store the page in the changeset */
    if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY)
        changeset_add_page(env_get_changeset(env), page);

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

    /* write the page if it's dirty and if HAM_WRITE_THROUGH is enabled */
    if ((flags&HAM_WRITE_THROUGH 
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
     *
     * TODO why "put it back"? it's already in the cache
     * be careful - don't store the header page in the cache
     */
    if (env_get_cache(env) && page_get_self(page)!=0)
        cache_put_page(env_get_cache(env), page);

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
        cache_remove_page(env_get_cache(env), page);
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
     * get rid of the extkey-cache
     */
    if (db_get_extkey_cache(db)) {
        (void)extkey_cache_purge_all(db_get_extkey_cache(db));
        extkey_cache_destroy(db_get_extkey_cache(db));
        db_set_extkey_cache(db, 0);
    }

    /*
     * in-memory-database: free all allocated blobs
     * TODO TODO TODO is the temp. transaction required?
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
            (void)be->_fun_enumerate(be, __free_inmemory_blobs_cb, &context);
            (void)txn_commit(txn, 0);
        }
    }

    if (env)
        changeset_clear(env_get_changeset(env));

    /*
     * immediately flush all pages of this database (but not the header page,
     * it's still required and will be flushed below
     */
    if (env && env_get_cache(env)) {
        ham_page_t *n, *head=cache_get_totallist(env_get_cache(env)); 
        while (head) {
            n=page_get_next(head, PAGE_LIST_CACHED);
            if (page_get_owner(head)==db && head!=env_get_header_page(env)) {
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
    ham_status_t st;
    ham_backend_t *be;

    /*
     * check the cache integrity
     */
    if (!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB)) {
        st=cache_check_integrity(env_get_cache(db_get_env(db)));
        if (st)
            return (st);
    }

    be=db_get_backend(db);
    if (!be)
        return (HAM_NOT_INITIALIZED);
    if (!be->_fun_check_integrity)
        return (HAM_NOT_IMPLEMENTED);

    /* purge cache if necessary */
    if (__cache_needs_purge(db_get_env(db))) {
        st=env_purge_cache(db_get_env(db));
        if (st)
            return (st);
    }

    /* call the backend function */
    st=be->_fun_check_integrity(be);
    changeset_clear(env_get_changeset(db_get_env(db)));
    return (st);
}

struct keycount_t 
{
    ham_u64_t c;
    ham_u32_t flags;
    ham_txn_t *txn;
    ham_db_t *db;
};

static void
db_get_key_count_txn(txn_opnode_t *node, void *data)
{
    struct keycount_t *kc=(struct keycount_t *)data;
    ham_backend_t *be=db_get_backend(kc->db);
    txn_op_t *op;

    /*
     * look at each tree_node and walk through each operation 
     * in reverse chronological order (from newest to oldest):
     * - is this op part of an aborted txn? then skip it
     * - is this op part of a committed txn? then include it
     * - is this op part of an txn which is still active? then include it
     * - if a committed txn has erased the item then there's no need
     *      to continue checking older, committed txns of the same key
     *
     * !!
     * if keys are overwritten or a duplicate key is inserted, then 
     * we have to consolidate the btree keys with the txn-tree keys.
     */
    op=txn_opnode_get_newest_op(node);
    while (op) {
        ham_txn_t *optxn=txn_op_get_txn(op);
        if (txn_get_flags(optxn)&TXN_STATE_ABORTED)
            ; /* nop */
        else if ((txn_get_flags(optxn)&TXN_STATE_COMMITTED)
                    || (kc->txn==optxn)) {
            /* if key was erased then it doesn't exist */
            if (txn_op_get_flags(op)&TXN_OP_ERASE)
                return;
            else if (txn_op_get_flags(op)&TXN_OP_NOP)
                ; /* nop */
            else if (txn_op_get_flags(op)&TXN_OP_INSERT) {
                kc->c++;
                return;
            }
            /* key exists - include it */
            else if ((txn_op_get_flags(op)&TXN_OP_INSERT)
                    || (txn_op_get_flags(op)&TXN_OP_INSERT_OW)) {
                /* check if the key already exists in the btree - if yes,
                 * we do not count it (it will be counted later) */
                if (kc->flags&HAM_FAST_ESTIMATE)
                    kc->c++;
                else if (HAM_KEY_NOT_FOUND==be->_fun_find(be, 
                                    txn_opnode_get_key(node), 0, 0))
                    kc->c++;
                return;
            }
            else if (txn_op_get_flags(op)&TXN_OP_INSERT_DUP) {
                /* check if the key already exists in the btree - if yes,
                 * we do not count it (it will be counted later) */
                if (kc->flags&HAM_FAST_ESTIMATE)
                    kc->c++;
                else {
                    /* check if btree has other duplicates */
                    if (0==be->_fun_find(be, txn_opnode_get_key(node), 0, 0)) {
                        /* yes, there's another one */
                        if (kc->flags&HAM_SKIP_DUPLICATES)
                            return;
                        else
                            kc->c++;
                    }
                    else {
                        /* check if other key is in this node */
                        kc->c++;
                        if (kc->flags&HAM_SKIP_DUPLICATES)
                            return;
                    }
                }
            }
            else {
                ham_assert(!"shouldn't be here", (""));
                return;
            }
        }
        else { /* txn is still active */
            kc->c++;
        }

        op=txn_op_get_previous_in_node(op);
    }
}


static ham_status_t
_local_fun_get_key_count(ham_db_t *db, ham_txn_t *txn, ham_u32_t flags,
            ham_offset_t *keycount)
{
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

    /* purge cache if necessary */
    if (__cache_needs_purge(db_get_env(db))) {
        st=env_purge_cache(db_get_env(db));
        if (st)
            return (st);
    }

    /*
     * call the backend function - this will retrieve the number of keys
     * in the btree
     */
    st = be->_fun_enumerate(be, __calc_keys_cb, &ctx);
    if (st)
        return (st);
    *keycount = ctx.total_count;

    /*
     * if transactions are enabled, then also sum up the number of keys
     * from the transaction tree
     */
    if ((db_get_rt_flags(db)&HAM_ENABLE_TRANSACTIONS) && (db_get_optree(db))) {
        struct keycount_t k;
        k.c=0;
        k.flags=flags;
        k.txn=txn;
        k.db=db;
        txn_tree_enumerate(db_get_optree(db), db_get_key_count_txn, (void *)&k);
        *keycount += k.c;
    }

    changeset_clear(env_get_changeset(env));
    return (0);
}

static ham_status_t
db_check_insert_conflicts(ham_db_t *db, ham_txn_t *txn, 
                txn_opnode_t *node, ham_key_t *key, ham_u32_t flags)
{
    ham_status_t st;
    txn_op_t *op=0;
    ham_backend_t *be=db_get_backend(db);

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
    op=txn_opnode_get_newest_op(node);
    while (op) {
        ham_txn_t *optxn=txn_op_get_txn(op);
        if (txn_get_flags(optxn)&TXN_STATE_ABORTED)
            ; /* nop */
        else if ((txn_get_flags(optxn)&TXN_STATE_COMMITTED)
                    || (txn==optxn)) {
            /* if key was erased then it doesn't exist and can be
             * inserted without problems */
            if (txn_op_get_flags(op)&TXN_OP_ERASE)
                return (0);
            else if (txn_op_get_flags(op)&TXN_OP_NOP)
                ; /* nop */
            /* if the key already exists then we can only continue if
             * we're allowed to overwrite it or to insert a duplicate */
            else if ((txn_op_get_flags(op)&TXN_OP_INSERT)
                    || (txn_op_get_flags(op)&TXN_OP_INSERT_OW)
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

        op=txn_op_get_previous_in_node(op);
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
    st=be->_fun_find(be, key, 0, flags);
    if (st==HAM_KEY_NOT_FOUND)
        return (0);
    if (st==HAM_SUCCESS)
        return (HAM_DUPLICATE_KEY);
    return (st);
}

static ham_status_t
db_check_erase_conflicts(ham_db_t *db, ham_txn_t *txn, 
                txn_opnode_t *node, ham_key_t *key, ham_u32_t flags)
{
    txn_op_t *op=0;
    ham_backend_t *be=db_get_backend(db);

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
    op=txn_opnode_get_newest_op(node);
    while (op) {
        ham_txn_t *optxn=txn_op_get_txn(op);
        if (txn_get_flags(optxn)&TXN_STATE_ABORTED)
            ; /* nop */
        else if ((txn_get_flags(optxn)&TXN_STATE_COMMITTED)
                    || (txn==optxn)) {
            /* if key was erased then it doesn't exist and we fail with
             * an error */
            if (txn_op_get_flags(op)&TXN_OP_ERASE)
                return (HAM_KEY_NOT_FOUND);
            else if (txn_op_get_flags(op)&TXN_OP_NOP)
                ; /* nop */
            /* if the key exists then we're successful */
            else if ((txn_op_get_flags(op)&TXN_OP_INSERT)
                    || (txn_op_get_flags(op)&TXN_OP_INSERT_OW)
                    || (txn_op_get_flags(op)&TXN_OP_INSERT_DUP)) {
                return (0);
            }
            else {
                ham_assert(!"shouldn't be here", (""));
                return (HAM_KEY_NOT_FOUND);
            }
        }
        else { /* txn is still active */
            /* TODO txn_set_conflict_txn(txn, optxn); */
            return (HAM_TXN_CONFLICT);
        }

        op=txn_op_get_previous_in_node(op);
    }

    /*
     * we've successfully checked all un-flushed transactions and there
     * were no conflicts. Now check all transactions which are already
     * flushed - basically that's identical to a btree lookup.
     */
    return (be->_fun_find(be, key, 0, flags));
}

static ham_bool_t
__btree_cursor_points_to(ham_cursor_t *c, ham_key_t *key)
{
    ham_bool_t ret=HAM_FALSE;
    ham_db_t *db=cursor_get_db(c);
    btree_cursor_t *btc=(btree_cursor_t *)c;

    if (btree_cursor_get_flags(btc)&BTREE_CURSOR_FLAG_COUPLED) {
        ham_cursor_t *clone;
        ham_status_t st=ham_cursor_clone(c, &clone);
        if (st)
            return (HAM_FALSE);
        st=btree_cursor_uncouple((btree_cursor_t *)clone, 0);
        if (st) {
            ham_cursor_close(clone);
            return (HAM_FALSE);
        }
        if (0==db_compare_keys(db, key, 
                   btree_cursor_get_uncoupled_key((btree_cursor_t *)clone)))
            ret=HAM_TRUE;
        ham_cursor_close(clone);
    }
    else if (btree_cursor_get_flags(btc)&BTREE_CURSOR_FLAG_UNCOUPLED) {
        ham_key_t *k=btree_cursor_get_uncoupled_key(btc);
        if (0==db_compare_keys(db, key, k))
            ret=HAM_TRUE;
    }
    else {
        ham_assert(!"shouldn't be here", (""));
    }
    
    return (ret);
}

static void
__increment_dupe_index(ham_db_t *db, txn_opnode_t *node, ham_u32_t start)
{
    ham_cursor_t *c=db_get_cursors(db);

    while (c) {
        ham_bool_t hit=HAM_FALSE;

        if (c->_fun_is_nil(c))
            goto next;

        /* if cursor is coupled to an op in the same node: increment 
         * duplicate index (if required) */
        if (cursor_get_flags(c)&CURSOR_COUPLED_TO_TXN) {
            txn_cursor_t *txnc=cursor_get_txn_cursor(c);
            txn_opnode_t *n=txn_op_get_node(txn_cursor_get_coupled_op(txnc));
            if (n==node)
                hit=HAM_TRUE;
        }
        /* if cursor is coupled to the same key in the btree: increment
         * duplicate index (if required) */
        else if (__btree_cursor_points_to(c, txn_opnode_get_key(node))) {
            hit=HAM_TRUE;
        }

        if (hit) {
            if (cursor_get_dupecache_index(c)>start) {
                cursor_set_dupecache_index(c, 
                    cursor_get_dupecache_index(c)+1);
            }
        }

next:
        c=cursor_get_next(c);
    }
}

ham_status_t
db_insert_txn(ham_db_t *db, ham_txn_t *txn,
                ham_key_t *key, ham_record_t *record, ham_u32_t flags, 
                struct txn_cursor_t *cursor)
{
    ham_status_t st=0;
    txn_optree_t *tree;
    txn_opnode_t *node;
    txn_op_t *op;
    ham_bool_t node_created=HAM_FALSE;
    ham_u64_t lsn=0;

    /* get (or create) the txn-tree for this database; we do not need
     * the returned value, but we call the function to trigger the 
     * tree creation if it does not yet exist */
    tree=txn_tree_get_or_create(db);
    if (!tree)
        return (HAM_OUT_OF_MEMORY);

    /* get (or create) the node for this key */
    node=txn_opnode_get(db, key, 0);
    if (!node) {
        node=txn_opnode_create(db, key);
        if (!node)
            return (HAM_OUT_OF_MEMORY);
        node_created=HAM_TRUE;
    }

    /* check for conflicts of this key
     *
     * !!
     * afterwards, clear the changeset; db_check_insert_conflicts() sometimes
     * checks if a key already exists, and this fills the changeset
     */
    st=db_check_insert_conflicts(db, txn, node, key, flags);
    changeset_clear(env_get_changeset(db_get_env(db)));
    if (st) {
        if (node_created)
            txn_opnode_free(db_get_env(db), node);
        return (st);
    }

    /* get the next lsn */
    st=env_get_incremented_lsn(db_get_env(db), &lsn);
    if (st) {
        if (node_created)
            txn_opnode_free(db_get_env(db), node);
        return (st);
    }

    /* append a new operation to this node */
    op=txn_opnode_append(txn, node, flags, 
                    (flags&HAM_PARTIAL) | 
                    ((flags&HAM_DUPLICATE) 
                        ? TXN_OP_INSERT_DUP 
                        : (flags&HAM_OVERWRITE)
                            ? TXN_OP_INSERT_OW
                            : TXN_OP_INSERT), 
                    lsn, record);
    if (!op)
        return (HAM_OUT_OF_MEMORY);

    /* if there's a cursor then couple it to the op; also store the 
     * dupecache-index in the op (it's needed for 
     * DUPLICATE_INSERT_BEFORE/NEXT) */
    if (cursor) {
        ham_cursor_t *c=txn_cursor_get_parent(cursor);
        if (cursor_get_dupecache_index(c))
            txn_op_set_referenced_dupe(op, cursor_get_dupecache_index(c));

        txn_cursor_set_to_nil(cursor);
        txn_cursor_set_flags(cursor, 
                    txn_cursor_get_flags(cursor)|TXN_CURSOR_FLAG_COUPLED);
        txn_cursor_set_coupled_op(cursor, op);
        txn_op_add_cursor(op, cursor);

        /* all other cursors need to increment their dupe index, if their
         * index is > this cursor's index */
        __increment_dupe_index(db, node, cursor_get_dupecache_index(c));
    }

    /* append journal entry */
    if (env_get_rt_flags(db_get_env(db))&HAM_ENABLE_RECOVERY
            && env_get_rt_flags(db_get_env(db))&HAM_ENABLE_TRANSACTIONS)
        st=journal_append_insert(env_get_journal(db_get_env(db)), db, txn,
                            key, record, 
                            flags&HAM_DUPLICATE ? flags : flags|HAM_OVERWRITE, 
                            txn_op_get_lsn(op));

    return (st);
}

static void
__nil_all_cursors_in_node(ham_txn_t *txn, ham_cursor_t *current, 
                txn_opnode_t *node)
{
    txn_op_t *op=txn_opnode_get_newest_op(node);
    while (op) {
        txn_cursor_t *cursor=txn_op_get_cursors(op);
        while (cursor) {
            ham_cursor_t *pc=txn_cursor_get_parent(cursor);
            /* is the current cursor to a duplicate? then adjust the 
             * coupled duplicate index of all cursors which point to a
             * duplicate */
            if (current) {
                if (cursor_get_dupecache_index(current)) {
                    if (cursor_get_dupecache_index(current)
                            <cursor_get_dupecache_index(pc)) {
                        cursor_set_dupecache_index(pc, 
                            cursor_get_dupecache_index(pc)-1);
                        cursor=txn_cursor_get_coupled_next(cursor);
                        continue;
                    }
                    else if (cursor_get_dupecache_index(current)
                            >cursor_get_dupecache_index(pc)) {
                        cursor=txn_cursor_get_coupled_next(cursor);
                        continue;
                    }
                    /* else fall through */
                }
            }
            cursor_set_flags(pc, 
                    cursor_get_flags(pc)&(~CURSOR_COUPLED_TO_TXN));
            txn_cursor_set_to_nil(cursor);
            cursor=txn_op_get_cursors(op);
            /* set a flag that the cursor just completed an Insert-or-find 
             * operation; this information is needed in ham_cursor_move 
             * (in this aspect, an erase is the same as insert/find) */
            cursor_set_lastop(pc, CURSOR_LOOKUP_INSERT);
        }

        op=txn_op_get_previous_in_node(op);
    }
}

static void
__nil_all_cursors_in_btree(ham_db_t *db, ham_cursor_t *current, ham_key_t *key)
{
    ham_cursor_t *c=db_get_cursors(db);

    /* foreach cursor in this database:
     *  if it's nil or coupled to the txn: skip it
     *  if it's coupled to btree AND uncoupled: compare keys; set to nil
     *      if keys are identical
     *  if it's uncoupled to btree AND coupled: compare keys; set to nil
     *      if keys are identical; (TODO - improve performance by nil'ling 
     *      all other cursors from the same btree page)
     *
     *  do NOT nil the current cursor - it's coupled to the key, and the
     *  coupled key is still needed by the caller
     */
    while (c) {
        if (c->_fun_is_nil(c) || c==current)
            goto next;
        if (cursor_get_flags(c)&CURSOR_COUPLED_TO_TXN)
            goto next;

        if (__btree_cursor_points_to(c, key)) {
            /* is the current cursor to a duplicate? then adjust the 
             * coupled duplicate index of all cursors which point to a
             * duplicate */
            if (current) {
                if (cursor_get_dupecache_index(current)) {
                    if (cursor_get_dupecache_index(current)
                            <cursor_get_dupecache_index(c)) {
                        cursor_set_dupecache_index(c, 
                            cursor_get_dupecache_index(c)-1);
                        goto next;
                    }
                    else if (cursor_get_dupecache_index(current)
                            >cursor_get_dupecache_index(c)) {
                        goto next;
                    }
                    /* else fall through */
                }
            }
            btree_cursor_set_to_nil((btree_cursor_t *)c);
            txn_cursor_set_to_nil(cursor_get_txn_cursor(c));
        }
next:
        c=cursor_get_next(c);
    }
}

ham_status_t
db_erase_txn(ham_db_t *db, ham_txn_t *txn, ham_key_t *key, ham_u32_t flags,
                txn_cursor_t *cursor)
{
    ham_status_t st=0;
    txn_optree_t *tree;
    txn_opnode_t *node;
    txn_op_t *op;
    ham_bool_t node_created=HAM_FALSE;
    ham_u64_t lsn=0;
    ham_cursor_t *pc=0;
    if (cursor)
        pc=txn_cursor_get_parent(cursor);

    /* get (or create) the txn-tree for this database; we do not need
     * the returned value, but we call the function to trigger the 
     * tree creation if it does not yet exist */
    tree=txn_tree_get_or_create(db);
    if (!tree)
        return (HAM_OUT_OF_MEMORY);

    /* get (or create) the node for this key */
    node=txn_opnode_get(db, key, 0);
    if (!node) {
        node=txn_opnode_create(db, key);
        if (!node)
            return (HAM_OUT_OF_MEMORY);
        node_created=HAM_TRUE;
    }

    /* check for conflicts of this key - but only if we're not erasing a 
     * duplicate key. dupes are checked for conflicts in _local_cursor_move */
    if (!pc || (!cursor_get_dupecache_index(pc))) {
        st=db_check_erase_conflicts(db, txn, node, key, flags);
        if (st) {
            if (node_created)
                txn_opnode_free(db_get_env(db), node);
            return (st);
        }
    }

    /* get the next lsn */
    st=env_get_incremented_lsn(db_get_env(db), &lsn);
    if (st) {
        if (node_created)
            txn_opnode_free(db_get_env(db), node);
        return (st);
    }

    /* append a new operation to this node */
    op=txn_opnode_append(txn, node, flags, TXN_OP_ERASE, lsn, 0);
    if (!op)
        return (HAM_OUT_OF_MEMORY);

    /* is this function called through ham_cursor_erase? then add the 
     * duplicate ID */
    if (cursor) {
        if (cursor_get_dupecache_index(pc))
            txn_op_set_referenced_dupe(op, cursor_get_dupecache_index(pc));
    }

    /* the current op has no cursors attached; but if there are any 
     * other ops in this node and in this transaction, then they have to
     * be set to nil. This only nil's txn-cursors! */
    __nil_all_cursors_in_node(txn, pc, node);

    /* in addition we nil all btree cursors which are coupled to this key */
    __nil_all_cursors_in_btree(db, pc, txn_opnode_get_key(node));

    /* append journal entry */
    if (env_get_rt_flags(db_get_env(db))&HAM_ENABLE_RECOVERY
            && env_get_rt_flags(db_get_env(db))&HAM_ENABLE_TRANSACTIONS)
        st=journal_append_erase(env_get_journal(db_get_env(db)), db, txn,
                            key, 0, flags|HAM_ERASE_ALL_DUPLICATES,
                            txn_op_get_lsn(op));

    return (st);
}

static ham_status_t
db_find_txn(ham_db_t *db, ham_txn_t *txn,
        ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st=0;
    txn_optree_t *tree=0;
    txn_opnode_t *node=0;
    txn_op_t *op=0;
    ham_backend_t *be=db_get_backend(db);

    /* get the txn-tree for this database; if there's no tree then
     * there's no need to create a new one - we'll just skip the whole
     * tree-related code */
    tree=db_get_optree(db);

    /* get the node for this key (but don't create a new one if it does
     * not yet exist) */
    if (tree)
        node=txn_opnode_get(db, key, 0);

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
    if (tree && node)
        op=txn_opnode_get_newest_op(node);
    while (op) {
        ham_txn_t *optxn=txn_op_get_txn(op);
        if (txn_get_flags(optxn)&TXN_STATE_ABORTED)
            ; /* nop */
        else if ((txn_get_flags(optxn)&TXN_STATE_COMMITTED)
                    || (txn==optxn)) {
            /* if key was erased then it doesn't exist and we can return
             * immediately */
            if (txn_op_get_flags(op)&TXN_OP_ERASE)
                return (HAM_KEY_NOT_FOUND);
            else if (txn_op_get_flags(op)&TXN_OP_NOP)
                ; /* nop */
            /* if the key already exists then return its record; do not
             * return pointers to txn_op_get_record, because it may be
             * flushed and the user's pointers would be invalid */
            else if ((txn_op_get_flags(op)&TXN_OP_INSERT)
                    || (txn_op_get_flags(op)&TXN_OP_INSERT_OW)
                    || (txn_op_get_flags(op)&TXN_OP_INSERT_DUP)) {
                if (!(record->flags&HAM_RECORD_USER_ALLOC)) {
                    st=db_resize_record_allocdata(db, 
                                    txn_op_get_record(op)->size);
                    if (st)
                        return (st);
                    record->data=db_get_record_allocdata(db);
                }
                memcpy(record->data, txn_op_get_record(op)->data,
                            txn_op_get_record(op)->size);
                record->size=txn_op_get_record(op)->size;
                return (0);
            }
            else {
                ham_assert(!"shouldn't be here", (""));
                return (HAM_KEY_NOT_FOUND);
            }
        }
        else { /* txn is still active */
            /* TODO txn_set_conflict_txn(txn, optxn); */
            return (HAM_TXN_CONFLICT);
        }

        op=txn_op_get_previous_in_node(op);
    }

    /*
     * we've successfully checked all un-flushed transactions and there
     * were no conflicts, and we have not found the key. Now try to 
     * lookup the key in the btree.
     */
    return (be->_fun_find(be, key, record, flags));
}

/* TODO move this function to cursor.h/cursor.c */
static ham_bool_t
__cursor_has_duplicates(ham_cursor_t *cursor)
{
    ham_db_t *db=cursor_get_db(cursor);
    txn_cursor_t *txnc=cursor_get_txn_cursor(cursor);

    if (!(db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES))
        return (HAM_FALSE);

    if (txn_cursor_get_coupled_op(txnc))
        cursor_update_dupecache(cursor, DUPE_CHECK_BTREE|DUPE_CHECK_TXN);
    else
        cursor_update_dupecache(cursor, DUPE_CHECK_BTREE);

    return (dupecache_get_count(cursor_get_dupecache(cursor)));
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

    /* purge cache if necessary */
    if (__cache_needs_purge(db_get_env(db))) {
        st=env_purge_cache(db_get_env(db));
        if (st)
            return (st);
    }

    /* 
     * if transactions are enabled: only insert the key/record pair into
     * the Transaction structure. Otherwise immediately write to the btree.
     */
    if (!st) {
        if (txn || local_txn) {
            st=db_insert_txn(db, txn ? txn : local_txn, 
                            key, &temprec, flags, 0);
        }
        else
            st=be->_fun_insert(be, key, &temprec, flags);
    }

    if (temprec.data!=record->data)
        allocator_free(env_get_allocator(env), temprec.data);

    if (st) {
        if (local_txn)
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

    if (local_txn)
        return (txn_commit(local_txn, 0));
    else if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY 
            && !(env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS))
        return (changeset_flush(env_get_changeset(env), DUMMY_LSN));
    else
        return (st);
}

static ham_status_t
_local_fun_erase(ham_db_t *db, ham_txn_t *txn, ham_key_t *key, ham_u32_t flags)
{
    ham_status_t st;
    ham_txn_t *local_txn=0;
    ham_env_t *env=db_get_env(db);
    ham_backend_t *be;
    ham_offset_t recno=0;

    be=db_get_backend(db);
    if (!be || !be_is_active(be))
        return (HAM_NOT_INITIALIZED);
    if (!be->_fun_erase)
        return (HAM_NOT_IMPLEMENTED);
    if (db_get_rt_flags(db)&HAM_READ_ONLY) {
        ham_trace(("cannot erase from a read-only database"));
        return (HAM_DB_READ_ONLY);
    }

    /* record number: make sure that we have a valid key structure */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        if (key->size!=sizeof(ham_u64_t) || !key->data) {
            ham_trace(("key->size must be 8, key->data must not be NULL"));
            return (HAM_INV_PARAMETER);
        }
        recno=*(ham_offset_t *)key->data;
        recno=ham_h2db64(recno);
        *(ham_offset_t *)key->data=recno;
    }

    /* purge cache if necessary */
    if (__cache_needs_purge(db_get_env(db))) {
        st=env_purge_cache(db_get_env(db));
        if (st)
            return (st);
    }

    if (!txn && (db_get_rt_flags(db)&HAM_ENABLE_TRANSACTIONS)) {
        if ((st=txn_begin(&local_txn, env, 0)))
            return (st);
    }

    db_update_global_stats_erase_query(db, key->size);

    /* 
     * if transactions are enabled: append a 'erase key' operation into
     * the txn tree; otherwise immediately erase the key from disk
     */
    if (txn || local_txn)
        st=db_erase_txn(db, txn ? txn : local_txn, key, flags, 0);
    else
        st=be->_fun_erase(be, key, flags);

    changeset_clear(env_get_changeset(db_get_env(db)));

    if (st) {
        if (local_txn)
            (void)txn_abort(local_txn, 0);
        return (st);
    }

    /* record number: re-translate the number to host endian */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        *(ham_offset_t *)key->data=ham_db2h64(recno);
    }

    if (local_txn)
        return (txn_commit(local_txn, 0));
    else if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY 
            && !(env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS))
        return (changeset_flush(env_get_changeset(env), DUMMY_LSN));
    else
        return (st);
}

static ham_status_t
_local_fun_find(ham_db_t *db, ham_txn_t *txn, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
    ham_env_t *env=db_get_env(db);
    ham_txn_t *local_txn=0;
    ham_status_t st;
    ham_backend_t *be;
    ham_offset_t recno=0;

    /* if this database has duplicates, then we use ham_cursor_find
     * because we have to build a duplicate list, and this is currently
     * only available in ham_cursor_find */
    if (db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES) {
        ham_cursor_t *c;
        st=ham_cursor_create(db, txn, 0, &c);
        if (st)
            return (st);
        st=ham_cursor_find_ex(c, key, record, flags);
        ham_cursor_close(c);
        return (st);
    }

    if ((db_get_keysize(db)<sizeof(ham_offset_t)) &&
            (key->size>db_get_keysize(db))) {
        ham_trace(("database does not support variable length keys"));
        return (HAM_INV_KEYSIZE);
    }

    /* record number: make sure we have a number in little endian */
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
        return (HAM_NOT_IMPLEMENTED);

    /* purge cache if necessary */
    if (__cache_needs_purge(db_get_env(db))) {
        st=env_purge_cache(db_get_env(db));
        if (st)
            return (st);
    }

    /* if user did not specify a transaction, but transactions are enabled:
     * create a temporary one */
    if (!txn && (db_get_rt_flags(db)&HAM_ENABLE_TRANSACTIONS)) {
        st=txn_begin(&local_txn, env, HAM_TXN_READ_ONLY);
        if (st)
            return (st);
    }

    db_update_global_stats_find_query(db, key->size);

    /* 
     * if transactions are enabled: read keys from transaction trees, 
     * otherwise read immediately from disk
     */
    if (txn || local_txn)
        st=db_find_txn(db, txn ? txn : local_txn, key, record, flags);
    else
        st=be->_fun_find(be, key, record, flags);

    /* TODO this will render the changeset_flush below obsolete */
    changeset_clear(env_get_changeset(env));

    if (st) {
        if (local_txn)
            (void)txn_abort(local_txn, 0);
        return (st);
    }

    /* record number: re-translate the number to host endian */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        *(ham_offset_t *)key->data=ham_db2h64(recno);
    }

    /* run the record-level filters */
    st=__record_filters_after_find(db, record);
    if (st) {
        if (local_txn)
            (void)txn_abort(local_txn, 0);
        return (st);
    }

    if (local_txn)
        return (txn_commit(local_txn, 0));
    else if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY 
            && !(env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS))
        return (changeset_flush(env_get_changeset(env), DUMMY_LSN));
    else
        return (st);
}

static ham_status_t
_local_cursor_create(ham_db_t *db, ham_txn_t *txn, ham_u32_t flags,
        ham_cursor_t **cursor)
{
    ham_backend_t *be;

    be=db_get_backend(db);
    if (!be || !be_is_active(be))
        return (HAM_NOT_INITIALIZED);
    if (!be->_fun_cursor_create)
        return (HAM_NOT_IMPLEMENTED);

    return (be->_fun_cursor_create(be, db, txn, flags, cursor));
}

static ham_status_t
_local_cursor_clone(ham_cursor_t *src, ham_cursor_t **dest)
{
    ham_status_t st;
    ham_db_t *db=cursor_get_db(src);
    ham_env_t *env;

    env = db_get_env(db);

    st=src->_fun_clone(src, dest);
    if (st)
        return (st);

    if (db_get_rt_flags(db)&HAM_ENABLE_TRANSACTIONS) {
        txn_cursor_clone(cursor_get_txn_cursor(src), 
                        cursor_get_txn_cursor(*dest));
    }

    if (db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES) {
        dupecache_clone(cursor_get_dupecache(src), 
                        cursor_get_dupecache(*dest));
    }

    return (0);
}

static ham_status_t
_local_cursor_close(ham_cursor_t *cursor)
{
    /* if the txn_cursor is coupled then uncouple it */
    txn_cursor_t *tc;
    tc=cursor_get_txn_cursor(cursor);
    if (!txn_cursor_is_nil(tc))
        txn_cursor_set_to_nil(tc);

    /* call the backend function */
    cursor->_fun_close(cursor);

    return (0);
}

static ham_status_t
_local_cursor_insert(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    ham_backend_t *be;
    ham_u64_t recno = 0;
    ham_record_t temprec;
    ham_db_t *db=cursor_get_db(cursor);
    ham_env_t *env=db_get_env(db);
    ham_txn_t *local_txn=0;

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
            /* get the record number (host endian) and increment it */
            recno=be_get_recno(be);
            recno++;
        }

        /* store it in db endian */
        recno=ham_h2db64(recno);
        memcpy(key->data, &recno, sizeof(ham_u64_t));
        key->size=sizeof(ham_u64_t);

        /* we're appending this key sequentially */
        flags|=HAM_HINT_APPEND;
    }

    /* purge cache if necessary */
    if (__cache_needs_purge(db_get_env(db))) {
        st=env_purge_cache(db_get_env(db));
        if (st)
            return (st);
    }

    /*
     * run the record-level filters on a temporary record structure - we
     * don't want to mess up the original structure
     */
    temprec=*record;
    st=__record_filters_before_write(db, &temprec);
    if (!st)
        db_update_global_stats_insert_query(db, key->size, temprec.size);
    else
        return (st);

    /* if user did not specify a transaction, but transactions are enabled:
     * create a temporary one */
    if (!cursor_get_txn(cursor) 
            && (db_get_rt_flags(db)&HAM_ENABLE_TRANSACTIONS)) {
        st=txn_begin(&local_txn, env, 0);
        if (st)
            return (st);
        cursor_set_txn(cursor, local_txn);
    }

    if (cursor_get_txn(cursor) || local_txn) {
        st=txn_cursor_insert(cursor_get_txn_cursor(cursor), key, 
                    &temprec, flags);
        if (st==0) {
            dupecache_t *dc=cursor_get_dupecache(cursor);
            cursor_set_flags(cursor, 
                    cursor_get_flags(cursor)|CURSOR_COUPLED_TO_TXN);
            /* reset the dupecache, otherwise __cursor_has_duplicates()
             * does not update the dupecache correctly */
            dupecache_reset(dc);
            /* if duplicate keys are enabled: set the duplicate index of
             * the new key */
            if (st==0 && __cursor_has_duplicates(cursor)) {
                int i;
                txn_cursor_t *txnc=cursor_get_txn_cursor(cursor);
                txn_op_t *op=txn_cursor_get_coupled_op(txnc);
                ham_assert(op!=0, (""));

                for (i=0; i<dupecache_get_count(dc); i++) {
                    dupecache_line_t *l=dupecache_get_elements(dc)+i;
                    if (!dupecache_line_use_btree(l)
                            && dupecache_line_get_txn_op(l)==op) {
                        cursor_set_dupecache_index(cursor, i+1);
                        break;
                    }
                }
            }
        }
    }
    else {
        st=cursor->_fun_insert(cursor, key, &temprec, flags);
        if (st==0)
            cursor_set_flags(cursor, 
                    cursor_get_flags(cursor)&(~CURSOR_COUPLED_TO_TXN));
    }

    /* if we created a temp. txn then clean it up again */
    if (local_txn)
        cursor_set_txn(cursor, 0);

    if (temprec.data!=record->data)
        allocator_free(env_get_allocator(env), temprec.data);

    if (st) {
        if (local_txn)
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

    /* no need to append the journal entry - it's appended in db_insert_txn(),
     * which is called by txn_cursor_insert() */

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

    /* set a flag that the cursor just completed an Insert-or-find 
     * operation; this information is needed in ham_cursor_move */
    cursor_set_lastop(cursor, CURSOR_LOOKUP_INSERT);

    if (local_txn)
        return (txn_commit(local_txn, 0));
    else if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY 
            && !(env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS))
        return (changeset_flush(env_get_changeset(env), DUMMY_LSN));
    else
        return (st);
}

static ham_status_t 
_local_cursor_erase(ham_cursor_t *cursor, ham_u32_t flags)
{
    ham_status_t st;
    ham_db_t *db=cursor_get_db(cursor);
    ham_env_t *env=db_get_env(db);
    ham_txn_t *local_txn=0;

    db_update_global_stats_erase_query(db, 0);

    /* purge cache if necessary */
    if (__cache_needs_purge(db_get_env(db))) {
        st=env_purge_cache(db_get_env(db));
        if (st)
            return (st);
    }

    /* if user did not specify a transaction, but transactions are enabled:
     * create a temporary one */
    if (!cursor_get_txn(cursor) 
            && (db_get_rt_flags(db)&HAM_ENABLE_TRANSACTIONS)) {
        st=txn_begin(&local_txn, env, 0);
        if (st)
            return (st);
        cursor_set_txn(cursor, local_txn);
    }

    /* if transactions are enabled: add a erase-op to the txn-tree */
    if (cursor_get_txn(cursor) || local_txn) {
        /* if cursor is coupled to a btree item: set the txn-cursor to 
         * nil; otherwise txn_cursor_erase() doesn't know which cursor 
         * part is the valid one */
        if (!(cursor_get_flags(cursor)&CURSOR_COUPLED_TO_TXN))
            txn_cursor_set_to_nil(cursor_get_txn_cursor(cursor));
        st=txn_cursor_erase(cursor_get_txn_cursor(cursor));
    }
    else {
        st=cursor->_fun_erase(cursor, flags);
    }

    /* if we created a temp. txn then clean it up again */
    if (local_txn)
        cursor_set_txn(cursor, 0);

    /* on success: cursor was set to nil */
    if (st==0) {
        cursor_set_flags(cursor, 
                cursor_get_flags(cursor)&(~CURSOR_COUPLED_TO_TXN));
        ham_assert(txn_cursor_is_nil(cursor_get_txn_cursor(cursor)), (""));
        ham_assert(cursor->_fun_is_nil(cursor), (""));
        dupecache_reset(cursor_get_dupecache(cursor));
        cursor_set_dupecache_index(cursor, 0);
    }
    else {
        if (local_txn)
            (void)txn_abort(local_txn, 0);
        return (st);
    }

    /* no need to append the journal entry - it's appended in db_erase_txn(),
     * which is called by txn_cursor_erase() */

    if (local_txn)
        return (txn_commit(local_txn, 0));
    else if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY 
            && !(env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS))
        return (changeset_flush(env_get_changeset(env), DUMMY_LSN));
    else
        return (st);
}

static ham_status_t
_local_cursor_find(ham_cursor_t *cursor, ham_key_t *key, 
            ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    ham_db_t *db=cursor_get_db(cursor);
    ham_offset_t recno=0;
    ham_txn_t *local_txn=0;
    ham_env_t *env=db_get_env(db);
    txn_cursor_t *txnc=cursor_get_txn_cursor(cursor);
    dupecache_t *dc=cursor_get_dupecache(cursor);

    /*
     * record number: make sure that we have a valid key structure,
     * and translate the record number to database endian
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

    db_update_global_stats_find_query(db, key->size);

    /* purge cache if necessary */
    if (__cache_needs_purge(db_get_env(db))) {
        st=env_purge_cache(db_get_env(db));
        if (st)
            return (st);
    }

    /* if user did not specify a transaction, but transactions are enabled:
     * create a temporary one */
    if (!cursor_get_txn(cursor) 
            && (db_get_rt_flags(db)&HAM_ENABLE_TRANSACTIONS)) {
        st=txn_begin(&local_txn, env, 0);
        if (st)
            return (st);
        cursor_set_txn(cursor, local_txn);
    }

    /* reset the dupecache */
    dupecache_reset(dc);
    cursor_set_dupecache_index(cursor, 0);

    /* 
     * first try to find the key in the transaction tree. If it exists and 
     * is NOT a duplicate then return its record. If it does not exist or
     * it has duplicates then also find the key in the btree.
     *
     * in non-Transaction mode, we directly search through the btree.
     */
    if (cursor_get_txn(cursor) || local_txn) {
        txn_op_t *op=0;
        st=txn_cursor_find(cursor_get_txn_cursor(cursor), key, flags);
        /* if the key was erased in a transaction then fail with an error 
         * (unless we have duplicates - they're checked below) */
        if (st) {
            if (st==HAM_KEY_NOT_FOUND)
                goto btree;
            if (st==HAM_KEY_ERASED_IN_TXN) {
                ham_bool_t is_equal;
                (void)cursor_sync(cursor, BTREE_CURSOR_ONLY_EQUAL_KEY, &is_equal);
                if (!is_equal)
                    btree_cursor_set_to_nil((btree_cursor_t *)cursor);

                if (!__cursor_has_duplicates(cursor))
                    st=HAM_KEY_NOT_FOUND;
                else
                    st=0;
            }
            if (st)
                goto bail;
        }
        else {
            ham_bool_t is_equal;
            (void)cursor_sync(cursor, BTREE_CURSOR_ONLY_EQUAL_KEY, &is_equal);
            if (!is_equal)
                btree_cursor_set_to_nil((btree_cursor_t *)cursor);
        }
        cursor_set_flags(cursor, 
                cursor_get_flags(cursor)|CURSOR_COUPLED_TO_TXN);
        op=txn_cursor_get_coupled_op(txnc);
        if (!__cursor_has_duplicates(cursor)) {
            if (record)
                st=txn_cursor_get_record(txnc, record);
            goto bail;
        }
        if (st==0)
            goto check_dupes;
    }

btree:
    st=cursor->_fun_find(cursor, key, record, flags);
    if (st==0) {
        cursor_set_flags(cursor, 
                cursor_get_flags(cursor)&(~CURSOR_COUPLED_TO_TXN));
        /* if btree keys were found: reset the dupecache. The previous
         * call to __cursor_has_duplicates() already initialized the
         * dupecache, but only with txn keys because the cursor was only
         * coupled to the txn */
        dupecache_reset(dc);
        cursor_set_dupecache_index(cursor, 0);
    }

check_dupes:
    /* if the key has duplicates: build a duplicate table, then
     * couple to the first/oldest duplicate */
    if (__cursor_has_duplicates(cursor)) {
        dupecache_line_t *e=dupecache_get_elements(
                cursor_get_dupecache(cursor));
        if (dupecache_line_use_btree(e))
            cursor_set_flags(cursor, 
                    cursor_get_flags(cursor)&(~CURSOR_COUPLED_TO_TXN));
        else
            cursor_set_flags(cursor, 
                    cursor_get_flags(cursor)|CURSOR_COUPLED_TO_TXN);
        cursor_couple_to_dupe(cursor, 1);
        st=0;

        /* now read the record */
        if (record) {
            /* TODO this works, but in case of the btree key w/ duplicates
            * it's possible that we read the record twice. I'm not sure if 
            * this can be avoided, though. */
            if (cursor_get_flags(cursor)&CURSOR_COUPLED_TO_TXN)
                st=txn_cursor_get_record(cursor_get_txn_cursor(cursor), 
                        record);
            else
                st=cursor->_fun_move(cursor, 0, record, 0);
        }
    }
    else {
        if ((cursor_get_flags(cursor)&CURSOR_COUPLED_TO_TXN) && record)
            st=txn_cursor_get_record(cursor_get_txn_cursor(cursor), record);
    }

bail:
    /* if we created a temp. txn then clean it up again */
    if (local_txn)
        cursor_set_txn(cursor, 0);

    if (st) {
        if (local_txn)
            (void)txn_abort(local_txn, 0);
        return (st);
    }

    /* record number: re-translate the number to host endian */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER)
        *(ham_offset_t *)key->data=ham_db2h64(recno);

    /* run the record-level filters */
    if (record) {
        st=__record_filters_after_find(db, record);
        if (st) {
            if (local_txn)
                (void)txn_abort(local_txn, 0);
            return (st);
        }
    }

    /* set a flag that the cursor just completed an Insert-or-find 
     * operation; this information is needed in ham_cursor_move */
    cursor_set_lastop(cursor, CURSOR_LOOKUP_INSERT);

    if (local_txn)
        return (txn_commit(local_txn, 0));
    else if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY 
            && !(env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS))
        return (changeset_flush(env_get_changeset(env), DUMMY_LSN));
    else
        return (st);
}

static ham_status_t
_local_cursor_get_duplicate_count(ham_cursor_t *cursor, 
        ham_size_t *count, ham_u32_t flags)
{
    ham_status_t st=0;
    ham_db_t *db=cursor_get_db(cursor);
    ham_env_t *env=db_get_env(db);
    ham_txn_t *local_txn=0;
    txn_cursor_t *txnc=cursor_get_txn_cursor(cursor);

    /* purge cache if necessary */
    if (__cache_needs_purge(db_get_env(db))) {
        st=env_purge_cache(db_get_env(db));
        if (st)
            return (st);
    }

    if (cursor->_fun_is_nil(cursor) && txn_cursor_is_nil(txnc))
        return (HAM_CURSOR_IS_NIL);

    /* if user did not specify a transaction, but transactions are enabled:
     * create a temporary one */
    if (!cursor_get_txn(cursor) 
            && (db_get_rt_flags(db)&HAM_ENABLE_TRANSACTIONS)) {
        st=txn_begin(&local_txn, env, 0);
        if (st)
            return (st);
        cursor_set_txn(cursor, local_txn);
    }

    if (cursor_get_txn(cursor) || local_txn) {
        if (db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES) {
            ham_bool_t dummy;
            dupecache_t *dc=cursor_get_dupecache(cursor);

            (void)cursor_sync(cursor, 0, &dummy);
            st=cursor_update_dupecache(cursor, DUPE_CHECK_TXN|DUPE_CHECK_BTREE);
            if (st)
                return (st);
            *count=dupecache_get_count(dc);
        }
        else {
            /* obviously the key exists, since the cursor is coupled to
             * a valid item */
            *count=1;
        }
    }
    else {
        st=cursor->_fun_get_duplicate_count(cursor, count, flags);
    }

    /* if we created a temp. txn then clean it up again */
    if (local_txn)
        cursor_set_txn(cursor, 0);

    if (st) {
        if (local_txn)
            (void)txn_abort(local_txn, 0);
        return (st);
    }

    /* set a flag that the cursor just completed an Insert-or-find 
     * operation; this information is needed in ham_cursor_move */
    cursor_set_lastop(cursor, CURSOR_LOOKUP_INSERT);

    if (local_txn)
        return (txn_commit(local_txn, 0));
    else if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY 
            && !(env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS))
        return (changeset_flush(env_get_changeset(env), DUMMY_LSN));
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
    ham_record_t temprec;
    ham_txn_t *local_txn=0;

    /* purge cache if necessary */
    if (__cache_needs_purge(db_get_env(db))) {
        st=env_purge_cache(db_get_env(db));
        if (st)
            return (st);
    }

    /*
     * run the record-level filters on a temporary record structure - we
     * don't want to mess up the original structure
     */
    temprec=*record;
    st=__record_filters_before_write(db, &temprec);
    if (st)
        return (st);

    /* if user did not specify a transaction, but transactions are enabled:
     * create a temporary one */
    if (!cursor_get_txn(cursor) 
            && (db_get_rt_flags(db)&HAM_ENABLE_TRANSACTIONS)) {
        st=txn_begin(&local_txn, env, 0);
        if (st)
            return (st);
        cursor_set_txn(cursor, local_txn);
    }

    /*
     * if we're in transactional mode then just append an "insert/OW" operation
     * to the txn-tree. 
     *
     * if the txn_cursor is already coupled to a txn-op, then we can use
     * txn_cursor_overwrite(). Otherwise we have to call txn_cursor_insert().
     *
     * otherwise (transactions are disabled) overwrite the item in the btree.
     */
    if (cursor_get_txn(cursor) || local_txn) {
        if (txn_cursor_is_nil(cursor_get_txn_cursor(cursor))
                && !(cursor->_fun_is_nil(cursor))) {
            st=btree_cursor_uncouple((btree_cursor_t *)cursor, 0);
            if (st==0)
                st=txn_cursor_insert(cursor_get_txn_cursor(cursor), 
                        btree_cursor_get_uncoupled_key((btree_cursor_t *)cursor),
                        &temprec, flags|HAM_OVERWRITE);
        }
        else {
            st=txn_cursor_overwrite(cursor_get_txn_cursor(cursor), &temprec);
        }

        if (st==0)
            cursor_set_flags(cursor, 
                    cursor_get_flags(cursor)|CURSOR_COUPLED_TO_TXN);
    }
    else {
        st=cursor->_fun_overwrite(cursor, &temprec, flags);
        if (st==0)
            cursor_set_flags(cursor, 
                    cursor_get_flags(cursor)&(~CURSOR_COUPLED_TO_TXN));
    }

    /* if we created a temp. txn then clean it up again */
    if (local_txn)
        cursor_set_txn(cursor, 0);

    if (st) {
        if (local_txn)
            (void)txn_abort(local_txn, 0);
        return (st);
    }

    /* the journal entry is appended in txn_cursor_insert() */

    if (temprec.data != record->data)
        allocator_free(env_get_allocator(env), temprec.data);

    if (local_txn)
        return (txn_commit(local_txn, 0));
    else if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY 
            && !(env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS))
        return (changeset_flush(env_get_changeset(env), DUMMY_LSN));
    else
        return (st);
}

/* 
 * this function compares two cursors - or more exactly, the two keys that
 * the cursors point at.
 *
 * we have to distinguish two states for the btree cursor and one state
 * for the txn cursor.
 *
 * both cursors must not be nil.
 */
static ham_status_t
__compare_cursors(btree_cursor_t *btrc, txn_cursor_t *txnc, int *pcmp)
{
    ham_db_t *db=cursor_get_db(btrc);
    ham_cursor_t *cursor=(ham_cursor_t *)btrc;

    txn_opnode_t *node=txn_op_get_node(txn_cursor_get_coupled_op(txnc));
    ham_key_t *txnk=txn_opnode_get_key(node);

    ham_assert(!cursor->_fun_is_nil(cursor), (""));
    ham_assert(!txn_cursor_is_nil(txnc), (""));

    if (cursor_get_flags(btrc)&BTREE_CURSOR_FLAG_COUPLED) {
        /* clone the cursor, then uncouple the clone; get the uncoupled key
         * and discard the clone again */
        
        /* 
         * TODO TODO TODO
         * this is all correct, but of course quite inefficient, because 
         *    a) new structures have to be allocated/released
         *    b) uncoupling fetches the whole extended key, which is often
         *      not necessary
         *  -> fix it!
         */
        int cmp;
        ham_cursor_t *clone;
        ham_status_t st=ham_cursor_clone(cursor, &clone);
        if (st)
            return (st);
        st=btree_cursor_uncouple((btree_cursor_t *)clone, 0);
        if (st) {
            ham_cursor_close(clone);
            return (st);
        }
        /* TODO error codes are swallowed */
        cmp=db_compare_keys(db, 
                btree_cursor_get_uncoupled_key((btree_cursor_t *)clone), txnk);
        ham_cursor_close(clone);
        *pcmp=cmp;
        return (0);
    }
    else if (cursor_get_flags(btrc)&BTREE_CURSOR_FLAG_UNCOUPLED) {
        /* TODO error codes are swallowed */
        *pcmp=db_compare_keys(db, btree_cursor_get_uncoupled_key(btrc), txnk);
        return (0);
    }

    ham_assert(!"shouldn't be here", (""));
    return (0);
}

static ham_bool_t
__btree_cursor_is_nil(btree_cursor_t *btc)
{
    return (!(cursor_get_flags(btc)&BTREE_CURSOR_FLAG_COUPLED) &&
            !(cursor_get_flags(btc)&BTREE_CURSOR_FLAG_UNCOUPLED));
}

static ham_status_t
_local_cursor_move(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags);

/*
 * this is quite a long function, but bear with me - it has lots of comments
 * and the flow should be easy to follow.
 */
static ham_status_t
do_local_cursor_move(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags, ham_bool_t fresh_start,
            ham_u32_t *pwhat)
{
    ham_status_t st=0;
    ham_bool_t changed_dir=fresh_start;
    txn_cursor_t *txnc=cursor_get_txn_cursor(cursor);

    /* was the direction changed? i.e. the previous operation was
     * HAM_CURSOR_NEXT, but now we move to HAM_CURSOR_PREVIOUS? */
    if ((cursor_get_lastop(cursor)==HAM_CURSOR_PREVIOUS)
            && (flags&HAM_CURSOR_NEXT))
        changed_dir=HAM_TRUE;
    else if ((cursor_get_lastop(cursor)==HAM_CURSOR_NEXT)
            && (flags&HAM_CURSOR_PREVIOUS))
        changed_dir=HAM_TRUE;

    /* 
     * Otherwise we have to consolidate the btree cursor and the txn-cursor.
     *
     * Move to the first key?
     */
    if (flags&HAM_CURSOR_FIRST) {
        ham_status_t btrs, txns;
        /* fetch the smallest/first key from the transaction tree. */
        txns=txn_cursor_move(txnc, flags);
        if (txns==HAM_KEY_NOT_FOUND)
            txn_cursor_set_to_nil(txnc);
        /* fetch the smallest/first key from the btree tree. */
        btrs=cursor->_fun_move(cursor, 0, 0, flags);
        if (btrs==HAM_KEY_NOT_FOUND)
            btree_cursor_set_to_nil((btree_cursor_t *)cursor);
        /* now consolidate - if both trees are empty then return */
        if (btrs==HAM_KEY_NOT_FOUND && txns==HAM_KEY_NOT_FOUND) {
            st=HAM_KEY_NOT_FOUND;
            goto bail;
        }
        /* if btree is empty but txn-tree is not: couple to txn */
        else if (btrs==HAM_KEY_NOT_FOUND && txns==0) {
            cursor_set_flags(cursor, 
                    cursor_get_flags(cursor)|CURSOR_COUPLED_TO_TXN);
            if (pwhat)
                *pwhat=DUPE_CHECK_TXN;
        }
        /* if txn-tree is empty but btree is not: couple to btree */
        else if (txns==HAM_KEY_NOT_FOUND && btrs==0) {
            cursor_set_flags(cursor, 
                    cursor_get_flags(cursor)&(~CURSOR_COUPLED_TO_TXN));
            if (pwhat)
                *pwhat=DUPE_CHECK_BTREE;
        }
        /* if both trees are not empty then pick the smaller key, but make
         * sure that it wasn't erased in the txn 
         *
         * !!
         * if the key has duplicates which were erased then return - dupes
         * are handled by the caller */
        else if (btrs==0 
                && (txns==0 
                    || txns==HAM_KEY_ERASED_IN_TXN
                    || txns==HAM_TXN_CONFLICT)) {
            int cmp;

            st=__compare_cursors((btree_cursor_t *)cursor, txnc, &cmp);
            if (st)
                goto bail;
            /* if both keys are equal: make sure that the btree key was not
             * erased in the transaction; otherwise couple to the txn-op
             * (it's chronologically newer and has faster access) 
             *
             * !!
             * only move next if there are no duplicate keys (= if the txn-op
             * does not reference a duplicate). Duplicate keys are handled
             * by the caller
             * */
            if (cmp==0) {
                ham_bool_t has_dupes;
                cursor_set_flags(cursor, 
                        cursor_get_flags(cursor)|CURSOR_COUPLED_TO_TXN);
                has_dupes=__cursor_has_duplicates(cursor);
                if ((txns==HAM_KEY_ERASED_IN_TXN) && !has_dupes) {
                    flags&=~HAM_CURSOR_FIRST;
                    flags|=HAM_CURSOR_NEXT;
                    /* if this btree key was erased or overwritten then couple
                     * to the txn, but already move the btree cursor to the
                     * next item */
                    st=cursor->_fun_move(cursor, 0, 0, flags);
                    if (st==HAM_KEY_NOT_FOUND)
                        btree_cursor_set_to_nil((btree_cursor_t *)cursor);
                    /* if the key was erased: continue moving "next" till 
                     * we find a key or reach the end of the database */
                    st=do_local_cursor_move(cursor, key, record, flags, 0, 0);
                    if (st==HAM_KEY_ERASED_IN_TXN) {
                        btree_cursor_set_to_nil((btree_cursor_t *)cursor);
                        txn_cursor_set_to_nil(txnc);
                        return (SHITTY_HACK_REACHED_EOF);
                    }
                    goto bail;
                }
                else if (txns==HAM_KEY_ERASED_IN_TXN && has_dupes) {
                    if (pwhat)
                        *pwhat=DUPE_CHECK_BTREE|DUPE_CHECK_TXN;
                    return (txns);
                }
                else if (txns==HAM_TXN_CONFLICT) {
                    return (txns);
                }
                /* btree and txn-tree have duplicates of the same key */
                else if (txns==HAM_SUCCESS && btrs==HAM_SUCCESS && has_dupes) {
                    if (pwhat)
                        *pwhat=DUPE_CHECK_BTREE|DUPE_CHECK_TXN;
                    return (txns);
                }
                /* if the btree entry was overwritten in the txn: move the
                 * btree entry to the next key */
                else if (txns==HAM_SUCCESS) {
                    flags&=~HAM_CURSOR_FIRST;
                    flags|=HAM_CURSOR_NEXT;
                    (void)cursor->_fun_move(cursor, 0, 0, flags);
                }
                if (pwhat)
                    *pwhat=DUPE_CHECK_BTREE|DUPE_CHECK_TXN;
                if (txns) {
                    st=txns;
                    goto bail;
                }
            }
            else if (cmp<1) {
                /* couple to btree */
                cursor_set_flags(cursor, 
                        cursor_get_flags(cursor)&(~CURSOR_COUPLED_TO_TXN));
            }
            else {
                if (txns==HAM_TXN_CONFLICT)
                    return (txns);
                /* couple to txn */
                cursor_set_flags(cursor, 
                        cursor_get_flags(cursor)|CURSOR_COUPLED_TO_TXN);
            }
        }
        /* every other error code is returned to the caller */
        else {
            if ((btrs==HAM_KEY_NOT_FOUND) && (txns==HAM_KEY_ERASED_IN_TXN))
                if (pwhat)
                    *pwhat=DUPE_CHECK_TXN;
            st=txns ? txns : btrs;
            goto bail;
        }
    }
    /*
     * move to the last key?
     */
    else if (flags&HAM_CURSOR_LAST) {
        ham_status_t btrs, txns;
        /* fetch the greatest/last key from the transaction tree. */
        txns=txn_cursor_move(txnc, flags);
        if (txns==HAM_KEY_NOT_FOUND)
            txn_cursor_set_to_nil(txnc);
        /* fetch the greatest/last key from the btree tree. */
        btrs=cursor->_fun_move(cursor, 0, 0, flags);
        if (btrs==HAM_KEY_NOT_FOUND)
            btree_cursor_set_to_nil((btree_cursor_t *)cursor);
        /* now consolidate - if both trees are empty then return */
        if (btrs==HAM_KEY_NOT_FOUND && txns==HAM_KEY_NOT_FOUND) {
            st=HAM_KEY_NOT_FOUND;
            goto bail;
        }
        /* if btree is empty but txn-tree is not: couple to txn */
        else if (btrs==HAM_KEY_NOT_FOUND && txns==0) {
            cursor_set_flags(cursor, 
                    cursor_get_flags(cursor)|CURSOR_COUPLED_TO_TXN);
            if (pwhat)
                *pwhat=DUPE_CHECK_TXN;
        }
        /* if txn-tree is empty but btree is not: couple to btree */
        else if (txns==HAM_KEY_NOT_FOUND && btrs==0) {
            cursor_set_flags(cursor, 
                    cursor_get_flags(cursor)&(~CURSOR_COUPLED_TO_TXN));
            if (pwhat)
                *pwhat=DUPE_CHECK_BTREE;
        }
        /* if both trees are not empty then pick the greater key, but make
         * sure that it wasn't erased in the txn 
         *
         * !!
         * if the key has duplicates which were erased then return - dupes
         * are handled by the caller */
        else if (btrs==0 
                && (txns==0 
                    || txns==HAM_KEY_ERASED_IN_TXN
                    || txns==HAM_TXN_CONFLICT)) {
            int cmp;

            st=__compare_cursors((btree_cursor_t *)cursor, txnc, &cmp);
            if (st)
                goto bail;
            /* if both keys are equal: make sure that the btree key was not
             * erased in the transaction; otherwise couple to the txn-op
             * (it's chronologically newer and has faster access)
             *
             * !!
             * only move prev if there are no duplicate keys (= if the txn-op
             * does not reference a duplicate). Duplicate keys are handled
             * by the caller
             * */
            if (cmp==0) {
                ham_bool_t has_dupes;
                cursor_set_flags(cursor, 
                        cursor_get_flags(cursor)|CURSOR_COUPLED_TO_TXN);
                has_dupes=__cursor_has_duplicates(cursor);
                if ((txns==HAM_KEY_ERASED_IN_TXN) && !has_dupes) {
                    flags&=~HAM_CURSOR_LAST;
                    flags|=HAM_CURSOR_PREVIOUS;
                    /* if this btree key was erased or overwritten then couple
                     * to the txn, but already move the btree cursor to the
                     * previous item */
                    st=cursor->_fun_move(cursor, 0, 0, flags);
                    if (st==HAM_KEY_NOT_FOUND)
                        btree_cursor_set_to_nil((btree_cursor_t *)cursor);
                    /* if the key was erased: continue moving "next" till 
                     * we find a key or reach the end of the database */
                    st=do_local_cursor_move(cursor, key, record, flags, 0, 0);
                    if (st==HAM_KEY_ERASED_IN_TXN) {
                        btree_cursor_set_to_nil((btree_cursor_t *)cursor);
                        txn_cursor_set_to_nil(txnc);
                        return (SHITTY_HACK_REACHED_EOF);
                    }
                    goto bail;
                }
                else if ((txns==HAM_KEY_ERASED_IN_TXN) && has_dupes) {
                    if (pwhat)
                        *pwhat=DUPE_CHECK_BTREE|DUPE_CHECK_TXN;
                    return (txns);
                }
                else if (txns==HAM_TXN_CONFLICT) {
                    return (txns);
                }
                /* btree and txn-tree have duplicates of the same key */
                else if (txns==HAM_SUCCESS && btrs==HAM_SUCCESS && has_dupes) {
                    if (pwhat)
                        *pwhat=DUPE_CHECK_BTREE|DUPE_CHECK_TXN;
                    return (txns);
                }
                /* if the btree entry was overwritten in the txn: move the
                 * btree entry to the previous key */
                else if (txns==HAM_SUCCESS) {
                    flags&=~HAM_CURSOR_LAST;
                    flags|=HAM_CURSOR_PREVIOUS;
                    (void)cursor->_fun_move(cursor, 0, 0, flags);
                }
                if (pwhat)
                    *pwhat=DUPE_CHECK_TXN|DUPE_CHECK_BTREE;
                if (txns) {
                    st=txns;
                    goto bail;
                }
            }
            else if (cmp<1) {
                /* couple to txn */
                if (txns==HAM_TXN_CONFLICT)
                    return (txns);
                cursor_set_flags(cursor, 
                        cursor_get_flags(cursor)|CURSOR_COUPLED_TO_TXN);
            }
            else {
                /* couple to btree */
                cursor_set_flags(cursor, 
                        cursor_get_flags(cursor)&(~CURSOR_COUPLED_TO_TXN));
            }
        }
        /* every other error code is returned to the caller */
        else {
            if ((btrs==HAM_KEY_NOT_FOUND) && (txns==HAM_KEY_ERASED_IN_TXN))
                if (pwhat)
                    *pwhat=DUPE_CHECK_TXN;
            st=txns ? txns : btrs;
            goto bail;
        }
    }
    /*
     * move to the next key?
     */
    else if (flags&HAM_CURSOR_NEXT) {
        ham_status_t btrs=0, txns=0;
        /* if the cursor is already bound to a txn-op, then move
         * the cursor to the next item in the txn (unless we change the
         * direction - then always move both cursors) */
        if (changed_dir || (cursor_get_flags(cursor)&CURSOR_COUPLED_TO_TXN)) {
            txns=txn_cursor_move(txnc, 
                            txn_cursor_is_nil(txnc) 
                                ? HAM_CURSOR_FIRST
                                : flags);
            /* if we've reached the end of the txn-tree then set the
             * txn-cursor to nil; otherwise subsequent calls to 
             * ham_cursor_move will not know that the txn-cursor is
             * invalid */
            if (txns==HAM_KEY_NOT_FOUND)
                txn_cursor_set_to_nil(txnc);
        }
        /* otherwise the cursor is bound to the btree, and we move
         * the cursor to the next item in the btree */
        if (changed_dir || !(cursor_get_flags(cursor)&CURSOR_COUPLED_TO_TXN)) {
            do {
                btrs=cursor->_fun_move(cursor, 0, 0, 
                            __btree_cursor_is_nil((btree_cursor_t *)cursor)
                                ? HAM_CURSOR_FIRST
                                : flags);
                /* if we've reached the end of the btree then set the
                 * btree-cursor to nil; otherwise subsequent calls to 
                 * ham_cursor_move will not know that the btree-cursor is
                 * invalid */
                if (btrs==HAM_KEY_NOT_FOUND) {
                    btree_cursor_set_to_nil((btree_cursor_t *)cursor);
                    break;
                }

                /* if the direction was changed: continue moving till we 
                 * found a key that is not erased or overwritten in a 
                 * transaction */
                if (!changed_dir)
                    break;
                st=cursor_check_if_btree_key_is_erased_or_overwritten(cursor);
                if (st==HAM_KEY_ERASED_IN_TXN 
                        && __cursor_has_duplicates(cursor)>1) {
                    st=0;
                    break;
                }
            } while (st==HAM_SUCCESS || st==HAM_KEY_ERASED_IN_TXN);
        }

        /* if any of the cursors is nil then we pretend that this cursor
         * doesn't have any keys to point at */
        if (txn_cursor_is_nil(txnc))
            txns=HAM_KEY_NOT_FOUND;
        if (__btree_cursor_is_nil((btree_cursor_t *)cursor))
            btrs=HAM_KEY_NOT_FOUND;

        /* now consolidate - if we've reached the end of both trees 
         * then return HAM_KEY_NOT_FOUND */
        if (btrs==HAM_KEY_NOT_FOUND && txns==HAM_KEY_NOT_FOUND) { 
            st=HAM_KEY_NOT_FOUND;
            goto bail;
        }
        if (btrs==HAM_KEY_NOT_FOUND && txns==HAM_KEY_ERASED_IN_TXN) { 
            st=HAM_KEY_NOT_FOUND;
            goto bail;
        }
        /* if reached end of btree but not of txn-tree: couple to txn */
        else if (btrs==HAM_KEY_NOT_FOUND && txns==0) {
            cursor_set_flags(cursor, 
                    cursor_get_flags(cursor)|CURSOR_COUPLED_TO_TXN);
            if (txn_cursor_is_erased(txnc)) {
                st=HAM_KEY_ERASED_IN_TXN;
                goto bail;
            }
        }
        /* if reached end of txn-tree but not of btree: couple to btree,
         * but only if btree entry was not erased or overwritten in 
         * the meantime. if it was, then just move to the next key. */
        else if (txns==HAM_KEY_NOT_FOUND && btrs==0) {
            cursor_set_flags(cursor, 
                    cursor_get_flags(cursor)&(~CURSOR_COUPLED_TO_TXN));
            st=cursor_check_if_btree_key_is_erased_or_overwritten(cursor);
            if (st==HAM_SUCCESS 
                    || st==HAM_KEY_ERASED_IN_TXN
                    || st==HAM_TXN_CONFLICT) {
                flags&=~HAM_CURSOR_FIRST;
                flags|=HAM_CURSOR_NEXT;
                /* txns is KEY_NOT_FOUND: if the key was erased then
                 * couple the txn-cursor, otherwise cursor_update_dupecache
                 * ignores the txn part */
                if (st==HAM_KEY_ERASED_IN_TXN) {
                    (void)cursor_sync(cursor, BTREE_CURSOR_ONLY_EQUAL_KEY, 0);
                    /* force re-creating the dupecache */
                    dupecache_reset(cursor_get_dupecache(cursor));
                    cursor_set_dupecache_index(cursor, 0);
                }
                /* if key has duplicates: move to the next duplicate */
                if (__cursor_has_duplicates(cursor)) {
                    st=_local_cursor_move(cursor, key, record, 
                            (flags&(~HAM_SKIP_DUPLICATES))
                                |(st==HAM_TXN_CONFLICT 
                                    ? 0
                                    : SHITTY_HACK_DONT_MOVE_DUPLICATE));
                    if (st)
                        goto bail;
                    if (pwhat)/* do not re-read duplicate lists in the caller */
                        *pwhat=0; 
                    return (SHITTY_HACK_FIX_ME);
                }
                /* otherwise move to the next key */
                else
                    st=do_local_cursor_move(cursor, key, record, flags, 0, 0);
                if (st)
                    goto bail;
            }
        }
        /* otherwise pick the smaller of both keys, but if it's a btree key
         * then make sure that it wasn't erased in the txn */
        else if (btrs==0 && (txns==0 || txns==HAM_KEY_ERASED_IN_TXN)) {
            int cmp;
            st=__compare_cursors((btree_cursor_t *)cursor, txnc, &cmp);
            if (st)
                goto bail;
            /* if both keys are equal: make sure that the btree key was not
             * erased or overwritten in the transaction; if it is, then couple 
             * to the txn-op (it's chronologically newer and has faster access) 
             *
             * only check this if the txn-cursor was not yet verified 
             * (only the btree cursor was moved) */
            if (cmp==0) {
                ham_bool_t erased=(txns==HAM_KEY_ERASED_IN_TXN);
                if (!erased 
                        && !(cursor_get_flags(cursor)&CURSOR_COUPLED_TO_TXN)) {
                    /* check if btree key was erased in txn */
                    if (txn_cursor_is_erased(txnc))
                        erased=HAM_TRUE;
                }
                cursor_set_flags(cursor, 
                        cursor_get_flags(cursor)|CURSOR_COUPLED_TO_TXN);
                flags&=~HAM_CURSOR_FIRST;
                flags|=HAM_CURSOR_NEXT;
                /* if this btree key was erased OR overwritten then couple to
                 * the txn, but already move the btree cursor to the next 
                 * item (unless this btree key has duplicates) */
                if (erased && __cursor_has_duplicates(cursor)>1) {
                    /* the duplicate was erased? move to the next */
                    st=_local_cursor_move(cursor, key, record, 
                            (flags&(~HAM_SKIP_DUPLICATES))
                                |SHITTY_HACK_DONT_MOVE_DUPLICATE);
                    if (st)
                        goto bail;
                    if (pwhat)/* do not re-read duplicate lists in the caller */
                        *pwhat=0; 
                    return (SHITTY_HACK_FIX_ME);
                }
                else if (erased || __cursor_has_duplicates(cursor)<=1) {
                    st=cursor->_fun_move(cursor, 0, 0, flags);
                    if (st==HAM_KEY_NOT_FOUND)
                        btree_cursor_set_to_nil((btree_cursor_t *)cursor);
                    st=0; /* ignore return code */
                }
                /* if the key was erased: continue moving "next" till 
                 * we find a key or reach the end of the database */
                if (erased) {
                    st=do_local_cursor_move(cursor, key, record, flags, 0, 0);
                    goto bail;
                }
                else if (pwhat)
                    *pwhat=DUPE_CHECK_TXN|DUPE_CHECK_BTREE;
            }
            else if (cmp<1) {
                /* couple to btree */
                cursor_set_flags(cursor, 
                        cursor_get_flags(cursor)&(~CURSOR_COUPLED_TO_TXN));
                /* check if this key was erased in the txn */
                st=cursor_check_if_btree_key_is_erased_or_overwritten(cursor);
                if (st==HAM_KEY_ERASED_IN_TXN) {
                    txn_cursor_set_to_nil(txnc);
                    (void)cursor_sync(cursor, BTREE_CURSOR_ONLY_EQUAL_KEY, 0);
                    if (!__cursor_has_duplicates(cursor)) {
                        flags&=~HAM_CURSOR_FIRST;
                        flags|=HAM_CURSOR_NEXT;
                        st=do_local_cursor_move(cursor, key, record, 
                                flags, 0, 0);
                        if (st)
                            goto bail;
                    }
                    else
                        *pwhat=DUPE_CHECK_TXN|DUPE_CHECK_BTREE;
                }
            }
            else {
                /* couple to txn */
                cursor_set_flags(cursor, 
                        cursor_get_flags(cursor)|CURSOR_COUPLED_TO_TXN);
                /* check if the txn-cursor points to an erased key */
                if (txns==0 && txn_cursor_is_erased(txnc)) {
                    flags&=~HAM_CURSOR_FIRST;
                    flags|=HAM_CURSOR_NEXT;
                    st=do_local_cursor_move(cursor, key, record, flags, 0, 0);
                    if (st)
                        goto bail;
                }
            }
        }
        /* every other error code is returned to the caller */
        else {
            st=txns ? txns : btrs;
            goto bail;
        }
    }
    /*
     * move to the previous key?
     */
    else if (flags&HAM_CURSOR_PREVIOUS) {
        ham_status_t btrs=0, txns=0;
        /* if the cursor is already bound to a txn-op, then move
         * the cursor to the previous item in the txn (unless we change the
         * direction - then always move both cursors */
        if (changed_dir || (cursor_get_flags(cursor)&CURSOR_COUPLED_TO_TXN)) {
            txns=txn_cursor_move(txnc, 
                            txn_cursor_is_nil(txnc) 
                                ? HAM_CURSOR_LAST
                                : flags);
            /* if we've reached the end of the txn-tree then set the
             * txn-cursor to nil; otherwise subsequent calls to 
             * ham_cursor_move will not know that the txn-cursor is
             * invalid */
            if (txns==HAM_KEY_NOT_FOUND)
                txn_cursor_set_to_nil(txnc);
        }
        /* otherwise the cursor is bound to the btree, and we move
         * the cursor to the previous item in the btree */
        if (changed_dir || !(cursor_get_flags(cursor)&CURSOR_COUPLED_TO_TXN)) {
            do {
                btrs=cursor->_fun_move(cursor, 0, 0, 
                            __btree_cursor_is_nil((btree_cursor_t *)cursor)
                                ? HAM_CURSOR_LAST
                                : flags);
                /* if we've reached the end of the btree then set the
                 * btree-cursor to nil; otherwise subsequent calls to 
                 * ham_cursor_move will not know that the btree-cursor is
                 * invalid */
                if (btrs==HAM_KEY_NOT_FOUND) {
                    btree_cursor_set_to_nil((btree_cursor_t *)cursor);
                    break;
                }

                /* if the direction was changed: continue moving till we 
                 * found a key that is not erased or overwritten in a 
                 * transaction */
                if (!changed_dir)
                    break;
                st=cursor_check_if_btree_key_is_erased_or_overwritten(cursor);
                if (st==HAM_KEY_ERASED_IN_TXN
                        && __cursor_has_duplicates(cursor)>1) {
                    st=0;
                    break;
                }
            } while (st==HAM_SUCCESS || st==HAM_KEY_ERASED_IN_TXN);
        }

        /* if any of the cursors is nil then we pretend that this cursor
         * doesn't have any keys to point at */
        if (txn_cursor_is_nil(txnc))
            txns=HAM_KEY_NOT_FOUND;
        if (__btree_cursor_is_nil((btree_cursor_t *)cursor))
            btrs=HAM_KEY_NOT_FOUND;

        /* now consolidate - if we've reached the end of both trees 
         * then return HAM_KEY_NOT_FOUND */
        if (btrs==HAM_KEY_NOT_FOUND && txns==HAM_KEY_NOT_FOUND) { 
            st=HAM_KEY_NOT_FOUND;
            goto bail;
        }
        if (btrs==HAM_KEY_NOT_FOUND && txns==HAM_KEY_ERASED_IN_TXN) { 
            st=HAM_KEY_NOT_FOUND;
            goto bail;
        }
        /* if reached end of btree but not of txn-tree: couple to txn */
        else if (btrs==HAM_KEY_NOT_FOUND && txns==0) {
            cursor_set_flags(cursor, 
                    cursor_get_flags(cursor)|CURSOR_COUPLED_TO_TXN);
            if (txn_cursor_is_erased(txnc)) {
                st=HAM_KEY_ERASED_IN_TXN;
                goto bail;
            }
        }
        /* if reached end of txn-tree but not of btree: couple to btree,
         * but only if btree entry was not erased or overwritten in 
         * the meantime. if it was, then just move to the previous key. */
        else if (txns==HAM_KEY_NOT_FOUND && btrs==0) {
            cursor_set_flags(cursor, 
                    cursor_get_flags(cursor)&(~CURSOR_COUPLED_TO_TXN));
            st=cursor_check_if_btree_key_is_erased_or_overwritten(cursor);
            if (st==HAM_SUCCESS 
                    || st==HAM_KEY_ERASED_IN_TXN
                    || st==HAM_TXN_CONFLICT) {
                flags&=~HAM_CURSOR_LAST;
                flags|=HAM_CURSOR_PREVIOUS;
                /* txns is KEY_NOT_FOUND: if the key was erased then
                 * couple the txn-cursor, otherwise cursor_update_dupecache
                 * ignores the txn part */
                if (st==HAM_KEY_ERASED_IN_TXN) {
                    (void)cursor_sync(cursor, BTREE_CURSOR_ONLY_EQUAL_KEY, 0);
                    /* force re-creating the dupecache */
                    dupecache_reset(cursor_get_dupecache(cursor));
                    cursor_set_dupecache_index(cursor, 0);
                }
                /* if key has duplicates: move to the previous duplicate */
                if (__cursor_has_duplicates(cursor)>1) {
                    st=_local_cursor_move(cursor, key, record,
                            (flags&(~HAM_SKIP_DUPLICATES))
                                |(st==HAM_TXN_CONFLICT 
                                    ? 0
                                    : SHITTY_HACK_DONT_MOVE_DUPLICATE));
                    if (st)
                        goto bail;
                    if (pwhat)/* do not re-read duplicate lists in the caller */
                        *pwhat=0;
                    return (SHITTY_HACK_FIX_ME);
                }
                /* otherwise move to the next key */
                else
                    st=do_local_cursor_move(cursor, key, record, flags, 0, 0);
                if (st)
                    goto bail;
            }
        }
        /* otherwise pick the smaller of both keys, but if it's a btree key
         * then make sure that it wasn't erased in the txn */
        else if (btrs==0 && (txns==0 || txns==HAM_KEY_ERASED_IN_TXN)) {
            int cmp;
            st=__compare_cursors((btree_cursor_t *)cursor, txnc, &cmp);
            if (st)
                goto bail;
            /* if both keys are equal: make sure that the btree key was not
             * erased or overwritten in the transaction; if it is, then couple 
             * to the txn-op (it's chronologically newer and has faster access) 
             *
             * only check this if the txn-cursor was not yet verified 
             * (only the btree cursor was moved) */
            if (cmp==0) {
                ham_bool_t erased=(txns==HAM_KEY_ERASED_IN_TXN);
                if (!erased 
                        && !(cursor_get_flags(cursor)&CURSOR_COUPLED_TO_TXN)) {
                    /* check if btree key was erased in txn */
                    if (txn_cursor_is_erased(txnc))
                        erased=HAM_TRUE;
                }
                cursor_set_flags(cursor, 
                        cursor_get_flags(cursor)|CURSOR_COUPLED_TO_TXN);
                flags&=~HAM_CURSOR_LAST;
                flags|=HAM_CURSOR_PREVIOUS;
                /* if this btree key was erased or overwritten then couple to
                 * the txn, but already move the btree cursor to the previous 
                 * item (unless this key has duplicates) */
                if (erased || __cursor_has_duplicates(cursor)>1) {
                    /* the duplicate was erased? move to the previous */
                    cursor_set_dupecache_index(cursor,
                            dupecache_get_count(cursor_get_dupecache(cursor)));
                    st=_local_cursor_move(cursor, key, record,
                            (flags&(~HAM_SKIP_DUPLICATES))
                                |SHITTY_HACK_DONT_MOVE_DUPLICATE);
                    if (st)
                        goto bail;
                    if (pwhat)/* do not re-read duplicate lists in the caller */
                        *pwhat=0;
                    return (SHITTY_HACK_FIX_ME);
                }
                else if (erased || __cursor_has_duplicates(cursor)<=1) {
                    st=cursor->_fun_move(cursor, 0, 0, flags);
                    if (st==HAM_KEY_NOT_FOUND)
                        btree_cursor_set_to_nil((btree_cursor_t *)cursor);
                    st=0; /* ignore return code */
                }
                /* if the key was erased: continue moving "previous" till 
                 * we find a key or reach the end of the database */
                if (erased) {
                    st=do_local_cursor_move(cursor, key, record, flags, 0, 0);
                    goto bail;
                }
                else if (pwhat)
                    *pwhat=DUPE_CHECK_TXN|DUPE_CHECK_BTREE;
            }
            else if (cmp<1) {
                /* couple to txn */
                cursor_set_flags(cursor, 
                        cursor_get_flags(cursor)|CURSOR_COUPLED_TO_TXN);
                /* check if the txn-cursor points to an erased key */
                if (txns==0 && txn_cursor_is_erased(txnc)) {
                    flags&=~HAM_CURSOR_LAST;
                    flags|=HAM_CURSOR_PREVIOUS;
                    st=do_local_cursor_move(cursor, key, record, flags, 0, 0);
                    if (st)
                        goto bail;
                }
            }
            else {
                /* couple to btree */
                cursor_set_flags(cursor, 
                        cursor_get_flags(cursor)&(~CURSOR_COUPLED_TO_TXN));
                /* check if this key was erased in the txn */
                st=cursor_check_if_btree_key_is_erased_or_overwritten(cursor);
                if (st==HAM_KEY_ERASED_IN_TXN) {
                    txn_cursor_set_to_nil(txnc);
                    (void)cursor_sync(cursor, BTREE_CURSOR_ONLY_EQUAL_KEY, 0);
                    if (!__cursor_has_duplicates(cursor)) {
                        flags&=~HAM_CURSOR_LAST;
                        flags|=HAM_CURSOR_PREVIOUS;
                        st=do_local_cursor_move(cursor, key, record, 
                                flags, 0, 0);
                        if (st)
                            goto bail;
                    }
                    else
                        *pwhat=DUPE_CHECK_TXN|DUPE_CHECK_BTREE;
                }
            }
        }
        /* every other error code is returned to the caller */
        else {
            st=txns ? txns : btrs;
            goto bail;
        }
    }

    st=0;

bail:
    /* '*pwhat' specifies if the duplicates should be processed from the
     * txn-cursor, from the btree-cursor or from both.
     *
     * cases with both are handled above. All other cases are handled here */
    if (pwhat && *pwhat==0) {
        if (cursor_get_flags(cursor)&CURSOR_COUPLED_TO_TXN)
            *pwhat|=DUPE_CHECK_TXN;
        else
            *pwhat|=DUPE_CHECK_BTREE;
    }

    return (st);
}

static ham_status_t
_local_cursor_move(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st=0;
    ham_db_t *db=cursor_get_db(cursor);
    ham_env_t *env=db_get_env(db);
    ham_txn_t *local_txn=0;
    txn_cursor_t *txnc=cursor_get_txn_cursor(cursor);
    ham_bool_t fresh_start=HAM_FALSE;
    dupecache_t *dc=cursor_get_dupecache(cursor);
    ham_u32_t what=0;

    /* purge cache if necessary */
    if (__cache_needs_purge(db_get_env(db))) {
        st=env_purge_cache(db_get_env(db));
        if (st)
            return (st);
    }

    /*
     * if the cursor is NIL, and the user requests a NEXT, we set it to FIRST;
     * if the user requests a PREVIOUS, we set it to LAST, resp.
     */
    if (cursor->_fun_is_nil(cursor)) {
        if (flags&HAM_CURSOR_NEXT) {
            flags&=~HAM_CURSOR_NEXT;
            flags|=HAM_CURSOR_FIRST;
        }
        else if (flags&HAM_CURSOR_PREVIOUS) {
            flags&=~HAM_CURSOR_PREVIOUS;
            flags|=HAM_CURSOR_LAST;
        }
    }

    /* in non-transactional mode - just call the btree function and return */
    if (!(db_get_rt_flags(db)&HAM_ENABLE_TRANSACTIONS)) {
        st=cursor->_fun_move(cursor, key, record, flags);
        if (st)
            return (st);

        /* run the record-level filters */
        return (__record_filters_after_find(db, record));
    }

    /* if user did not specify a transaction, but transactions are enabled:
     * create a temporary one */
    if (!cursor_get_txn(cursor)
            && (db_get_rt_flags(db)&HAM_ENABLE_TRANSACTIONS)) {
        st=txn_begin(&local_txn, env, 0);
        if (st)
            return (st);
        cursor_set_txn(cursor, local_txn);
    }

    /* if cursor was not moved (i.e. last op was insert or find) then 
     * most likely ONLY txn-cursor OR btree-cursor are coupled. For moving
     * the cursor, however we need to couple both. */
    if (cursor_get_lastop(cursor)==CURSOR_LOOKUP_INSERT
            && ((flags&HAM_CURSOR_NEXT) || (flags&HAM_CURSOR_PREVIOUS))) {
        st=cursor_sync(cursor, flags, &fresh_start);
    }

    if (st!=0)
        goto bail;

    /*
     * now move forwards, backwards etc
     *
     * first check if we have a duplicate list and we want to move next
     * or previous in the duplicates
     */
    if (flags!=0 
            && (db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES) 
            && (dupecache_get_count(dc))) {
        ham_bool_t both_not_nil=HAM_FALSE;
        if (!txn_cursor_is_nil(txnc) 
                && !__btree_cursor_is_nil((btree_cursor_t *)cursor))
            both_not_nil=HAM_TRUE;

        if (!(flags&HAM_SKIP_DUPLICATES) && (flags&HAM_CURSOR_NEXT)) {
            if (cursor_get_dupecache_index(cursor)) {
                if (cursor_get_dupecache_index(cursor)
                        <dupecache_get_count(dc)) {
                    cursor_set_dupecache_index(cursor, 
                                cursor_get_dupecache_index(cursor)+1);
                    cursor_couple_to_dupe(cursor, 
                                cursor_get_dupecache_index(cursor));
                    goto bail;
                }
            }
            if (flags&SHITTY_HACK_DONT_MOVE_DUPLICATE) {
                if (cursor_get_dupecache_index(cursor)==0) {
                    cursor_set_dupecache_index(cursor, 1);
                    cursor_couple_to_dupe(cursor, 
                                cursor_get_dupecache_index(cursor));
                    goto bail;
                }
            }
        }
        else if (!(flags&HAM_SKIP_DUPLICATES) && (flags&HAM_CURSOR_PREVIOUS)) {
            if (flags&SHITTY_HACK_DONT_MOVE_DUPLICATE) {
                goto bail;
            }
            /* duplicate key? then traverse the duplicate list */
            else if (cursor_get_dupecache_index(cursor)) {
                if (cursor_get_dupecache_index(cursor)>1) {
                    cursor_set_dupecache_index(cursor, 
                                cursor_get_dupecache_index(cursor)-1);
                    cursor_couple_to_dupe(cursor, 
                                cursor_get_dupecache_index(cursor));
                    goto bail;
                }
            }
        }

        if ((flags&HAM_CURSOR_NEXT) || (flags&HAM_CURSOR_PREVIOUS)) {
            /* we've made it through the duplicate list. make sure that
             * both cursors (btree AND txn-tree) are incremented in
             * do_local_cursor_move(), if they both point to the same
             * key */
            if (both_not_nil) {
                int cmp=0;
                st=__compare_cursors((btree_cursor_t *)cursor, txnc, &cmp);
                if (st)
                    goto bail;
                if (cmp==0)
                    fresh_start=HAM_TRUE;
            }
        }

        /* still here? then we don't care about the dupecache anymore */
        dupecache_reset(dc);
        cursor_set_dupecache_index(cursor, 0);
    }

    /* get rid of this flag */
    flags=flags&(~SHITTY_HACK_DONT_MOVE_DUPLICATE);

    /* no move requested? then return key or record */
    if (!flags)
        goto bail;

    /* move to the next key, but skip duplicates. Duplicates are handled
     * below (and above) */
    st=do_local_cursor_move(cursor, key, record, 
                    flags|HAM_SKIP_DUPLICATES, fresh_start, &what);
    if (st==SHITTY_HACK_FIX_ME) {
        st=0;
        goto bail_2;
    }
    if (st==SHITTY_HACK_REACHED_EOF) {
        st=HAM_KEY_NOT_FOUND;
        goto bail_2;
    }
    if (st) {
        /* if we only found an erased key but duplicates are available
         * then we must continue and move to the next possible key
         * in the duplicate table. */
        if (st==HAM_KEY_ERASED_IN_TXN) {
            if (!((flags&HAM_CURSOR_FIRST) || (flags&HAM_CURSOR_LAST)))
                goto bail;
        }
        else
            goto bail;
    }

    /* now check if this key has duplicates; if yes, we have to build up the
     * duplicate table (we even have to do this if duplicates are skipped, 
     * because a txn-op might replace the first/oldest duplicate) */
    if (db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES) {
        if (what) {
            ham_status_t st1;
            dupecache_reset(dc);
            st1=cursor_update_dupecache(cursor, what);
            if (st1)
                return (st1);
        }
    }

    /* this key has duplicates? then make sure we pick the right one - either
     * the first or the last, depending on the flags. */
    if (dupecache_get_count(dc)) {
        ham_txn_t *txn=cursor_get_txn(cursor);
        if (!txn)
            txn=local_txn;
        st=0; /* clear HAM_KEY_ERASED_IN_TXN */

        /* check if ANY duplicate has a conflict - if yes then skip 
         * the whole key (with ALL duplicates) */
        if (txn) {
            ham_u32_t i;
            for (i=0; i<dupecache_get_count(dc); i++) {
                dupecache_line_t *e=dupecache_get_elements(dc)+i;
                if (!dupecache_line_use_btree(e)) {
                    txn_op_t *op=dupecache_line_get_txn_op(e);
                    if (txn_op_conflicts(op, txn)) {
                        /* couple to the first (or last) duplicate and move
                        * to the previous (or next) key */ 
                        if (flags&HAM_CURSOR_NEXT) {
                            cursor_couple_to_dupe(cursor, 
                                    dupecache_get_count(dc));
                            flags|=HAM_CURSOR_FIRST;
                            goto move_next_or_prev;
                        }
                        if (flags&HAM_CURSOR_PREVIOUS) {
                            cursor_couple_to_dupe(cursor, 1);
                            flags|=HAM_CURSOR_LAST;
                            goto move_next_or_prev;
                        }
                        else if (flags&HAM_CURSOR_FIRST) {
                            ham_assert(!"shouldn't be here", (""));
                            st=HAM_TXN_CONFLICT;
                            goto bail;
                        }
                        else if (flags&HAM_CURSOR_LAST) {
                            ham_assert(!"shouldn't be here", (""));
                            st=HAM_TXN_CONFLICT;
                            goto bail;
                        }
                    }
                }
            }
        }

        if ((flags&HAM_CURSOR_FIRST) || (flags&HAM_CURSOR_NEXT)) {
            cursor_couple_to_dupe(cursor, 1);
        }
        else if ((flags&HAM_CURSOR_LAST) || (flags&HAM_CURSOR_PREVIOUS)) {
            cursor_couple_to_dupe(cursor, dupecache_get_count(dc));
        }
        else if (flags!=0)
            ham_assert(!"shouldn't be here", (""));
    }

    /* if the first (or last) key was requested, but it was erased, then
     * continue moving to the next (previous) key 
     *
     * make sure that the cursor is properly coupled - either to the
     * txn- or the b-tree */
    if (st==HAM_KEY_ERASED_IN_TXN) {
        txn_op_t *op=0;
move_next_or_prev:
        op=txn_cursor_get_coupled_op(txnc);
        if (cursor_get_dupecache_index(cursor)) {
            dupecache_line_t *e=dupecache_get_elements(dc)+
                    (cursor_get_dupecache_index(cursor)-1);
            if (dupecache_line_use_btree(e))
                op=0;
        }

        if (op)
            cursor_set_flags(cursor, 
                    cursor_get_flags(cursor)|CURSOR_COUPLED_TO_TXN);
        else
            cursor_set_flags(cursor, 
                    cursor_get_flags(cursor)&(~CURSOR_COUPLED_TO_TXN));

        if (flags&HAM_CURSOR_FIRST) {
            flags&=~HAM_CURSOR_FIRST;
            flags|=HAM_CURSOR_NEXT;
            cursor_set_lastop(cursor, HAM_CURSOR_NEXT);
            st=_local_cursor_move(cursor, 0, 0, flags);
            goto bail;
        }
        if (flags&HAM_CURSOR_LAST) {
            flags&=~HAM_CURSOR_LAST;
            flags|=HAM_CURSOR_PREVIOUS;
            cursor_set_lastop(cursor, HAM_CURSOR_PREVIOUS);
            st=_local_cursor_move(cursor, 0, 0, flags);
            goto bail;
        }
    }

bail:

    /*
     * retrieve key/record, if requested
     */
    if (st==0) {
        if (cursor_get_flags(cursor)&CURSOR_COUPLED_TO_TXN) {
            txn_op_t *op=txn_cursor_get_coupled_op(txnc);
            /* are we coupled to an ERASE-op? then return an error 
             *
             * !! Hack alert
             * in rare scenarios, we have to move to the next or previous
             * key (DupeCursorTest::eraseAllDuplicatesMovePreviousMixedTest3
             * and others). */
            if (txn_op_get_flags(op)&TXN_OP_ERASE) {
                if ((flags&HAM_CURSOR_FIRST) || (flags&HAM_CURSOR_LAST))
                    goto move_next_or_prev;
                st=HAM_KEY_NOT_FOUND;
                goto bail;
            }
            if (key) {
                st=txn_cursor_get_key(txnc, key);
                if (st)
                    goto bail;
            }
            if (record) {
                st=txn_cursor_get_record(txnc, record);
                if (st)
                    goto bail;
            }
        }
        else {
            st=cursor->_fun_move(cursor, key, record, 0);
        }
    }

bail_2:

    /* if we created a temp. txn then clean it up again */
    if (local_txn)
        cursor_set_txn(cursor, 0);

    /* TODO this will render the changeset_flush below obsolete */
    changeset_clear(env_get_changeset(env));

    /*
     * run the record-level filters
     */
    if (st==0 && record)
        st=__record_filters_after_find(db, record);

    if (st) {
        if (local_txn)
            (void)txn_abort(local_txn, 0);
        if (st==HAM_KEY_ERASED_IN_TXN)
            st=HAM_KEY_NOT_FOUND;
        return (st);
    }

    /* store the direction */
    if (flags&HAM_CURSOR_NEXT)
        cursor_set_lastop(cursor, HAM_CURSOR_NEXT);
    else if (flags&HAM_CURSOR_PREVIOUS)
        cursor_set_lastop(cursor, HAM_CURSOR_PREVIOUS);
    else
        cursor_set_lastop(cursor, 0);

    if (local_txn)
        return (txn_commit(local_txn, 0));
    else if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY 
            && !(env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS))
        return (changeset_flush(env_get_changeset(env), DUMMY_LSN));
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

