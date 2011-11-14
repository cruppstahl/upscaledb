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
    Database *db;               /* [in] */
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

    c=(calckeys_context_t *)context;

    switch (event) {
    case ENUM_EVENT_DESCEND:
        break;

    case ENUM_EVENT_PAGE_START:
        c->is_leaf=*(ham_bool_t *)param2;
        break;

    case ENUM_EVENT_PAGE_STOP:
        break;

    case ENUM_EVENT_ITEM:
        key=(btree_key_t *)param1;
        count=*(ham_size_t *)param2;

        if (c->is_leaf) {
            ham_size_t dupcount=1;

            if (!(c->flags&HAM_SKIP_DUPLICATES)
                    && (key_get_flags(key)&KEY_HAS_DUPLICATES)) {
                ham_status_t st=blob_duplicate_get_count(db_get_env(c->db), 
                            key_get_ptr(key), &dupcount, 0);
                if (st)
                    return (st);
                c->total_count+=dupcount;
            }
            else {
                c->total_count++;
            }

            if (c->flags&HAM_FAST_ESTIMATE) {
                /* 
                 * fast mode: just grab the keys-per-page value and 
                 * call it a day for this page.
                 *
                 * Assume all keys in this page have the same number 
                 * of dupes (=1 if no dupes)
                 */
                c->total_count+=(count-1)*dupcount;
                return (CB_DO_NOT_DESCEND);
            }
        }
        break;

    default:
        ham_assert(!"unknown callback event", (""));
        break;
    }

    return (CB_CONTINUE);
}


typedef struct free_cb_context_t
{
    Database *db;
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
            /* delete the extended key */
            st=extkey_remove(c->db, blobid);
            if (st)
                return (st);
        }

        if (key_get_flags(key)&(KEY_BLOB_SIZE_TINY
                            |KEY_BLOB_SIZE_SMALL
                            |KEY_BLOB_SIZE_EMPTY))
            break;

        /*
         * if we're in the leaf page, delete the blob
         */
        if (c->is_leaf) {
            st=key_erase_record(c->db, key, 0, HAM_ERASE_ALL_DUPLICATES);
            if (st)
                return (st);
        }
        break;

    default:
        ham_assert(!"unknown callback event", (0));
        return (CB_STOP);
    }

    return (CB_CONTINUE);
}

inline ham_bool_t
__cache_needs_purge(ham_env_t *env)
{
    Cache *cache=env_get_cache(env);
    if (!cache)
        return (HAM_FALSE);

    /* purge the cache, if necessary. if cache is unlimited, then we purge very
     * very rarely (but we nevertheless purge to avoid OUT OF MEMORY conditions
     * which can happen on 32bit Windows) */
    if (cache && !(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)) {
        ham_bool_t purge=cache->is_too_big();
#if defined(WIN32) && defined(HAM_32BIT)
        if (env_get_rt_flags(env)&HAM_CACHE_UNLIMITED) {
            if (cache->get_cur_elements()*env_get_pagesize(env) 
                    > PURGE_THRESHOLD)
                return (HAM_FALSE);
        }
#endif
        return (purge);
    }
    return (HAM_FALSE);
}

static ham_status_t
__record_filters_before_write(Database *db, ham_record_t *record)
{
    ham_status_t st=0;
    ham_record_filter_t *record_head;

    record_head=db_get_record_filter(db);
    while (record_head) {
        if (record_head->before_write_cb) {
            st=record_head->before_write_cb((ham_db_t *)db, 
                    record_head, record);
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
__record_filters_after_find(Database *db, ham_record_t *record)
{
    ham_status_t st = 0;
    ham_record_filter_t *record_head;

    record_head=db_get_record_filter(db);
    if (record_head) {
        record_head = record_head->_prev;
        do {
            if (record_head->after_read_cb) {
                st=record_head->after_read_cb((ham_db_t *)db, 
                        record_head, record);
                if (st)
                      break;
            }
            record_head = record_head->_prev;
        } while (record_head->_prev->_next);
    }
    return (st);
}

Database::Database()
  : m_error(0), m_context(0), m_backend(0), m_cursors(0),
    m_prefix_func(0), m_cmp_func(0), m_duperec_func(0), _extkey_cache(0),
    _rec_allocsize(0), _rec_allocdata(0), _key_allocsize(0), _key_allocdata(0),
    _rt_flags(0), _indexdata_offset(0), _env(0), _next(0),
    _record_filters(0), _data_access_mode(0), _is_active(0)
{
    memset(&_global_perf_data, 0, sizeof(_global_perf_data));
    memset(&_db_perf_data, 0, sizeof(_db_perf_data));

#if HAM_ENABLE_REMOTE
    _remote_handle=0;
#endif
    txn_tree_init(this, &_optree);

    _fun_get_parameters=0;
    _fun_check_integrity=0;
    _fun_get_key_count=0;
    _fun_insert=0;
    _fun_erase=0;
    _fun_find=0;
    _fun_cursor_create=0;
    _fun_cursor_clone=0;
    _fun_cursor_insert=0;
    _fun_cursor_erase=0;
    _fun_cursor_find=0;
    _fun_cursor_get_duplicate_count=0;
    _fun_cursor_get_record_size=0;
    _fun_cursor_overwrite=0;
    _fun_cursor_move=0;
    _fun_cursor_close=0;
    _fun_close=0;
    _fun_destroy=0;
};


ham_status_t
db_uncouple_all_cursors(ham_page_t *page, ham_size_t start)
{
    Cursor *c = page_get_cursors(page);

    if (c) {
        Database *db = c->get_db();
        if (db) {
            ham_backend_t *be = db->get_backend();
            
            if (be) {
                return (*be->_fun_uncouple_all_cursors)(be, page, start);
            }
        }
    }

    return (HAM_SUCCESS);
}

ham_u16_t
db_get_dbname(Database *db)
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
db_get_extended_key(Database *db, ham_u8_t *key_data,
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
            ExtKeyCache *c=new ExtKeyCache(db);
            if (!c)
                return (HAM_OUT_OF_MEMORY);
            db_set_extkey_cache(db, c);
        }
    }

    /* almost the same as: blobid = key_get_extended_rid(db, key); */
    memcpy(&blobid, key_data+(db_get_keysize(db)-sizeof(ham_offset_t)), 
            sizeof(blobid));
    blobid=ham_db2h_offset(blobid);

    /* fetch from the cache */
    if (!(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)) {
        st=db_get_extkey_cache(db)->fetch(blobid, &temp, &ptr);
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

    /* insert the FULL key in the extkey-cache */
    if (db_get_extkey_cache(db)) {
        ExtKeyCache *cache=db_get_extkey_cache(db);
        cache->insert(blobid, key_length, (ham_u8_t *)ext_key->data);
    }

    ext_key->size = (ham_u16_t)key_length;

    return (0);
}

int
db_compare_keys(Database *db, ham_key_t *lhs, ham_key_t *rhs)
{
    int cmp=HAM_PREFIX_REQUEST_FULLKEY;
    ham_compare_func_t foo=db->get_compare_func();
    ham_prefix_compare_func_t prefoo=db->get_prefix_compare_func();

    db->set_error(0);

    /*
     * need prefix compare? if no key is extended we can just call the
     * normal compare function
     */
    if (!(lhs->_flags&KEY_IS_EXTENDED) && !(rhs->_flags&KEY_IS_EXTENDED)) {
        /*
         * no!
         */
        return (foo((ham_db_t *)db, (ham_u8_t *)lhs->data, lhs->size, 
                        (ham_u8_t *)rhs->data, rhs->size));
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

        cmp=prefoo((ham_db_t *)db, 
                    (ham_u8_t *)lhs->data, lhsprefixlen, lhs->size, 
                    (ham_u8_t *)rhs->data, rhsprefixlen, rhs->size);
        if (cmp < -1 && cmp != HAM_PREFIX_REQUEST_FULLKEY)
            return cmp; /* unexpected error! */
    }

    if (cmp==HAM_PREFIX_REQUEST_FULLKEY) {
        ham_status_t st;

        /* 1. load the first key, if needed */
        if (lhs->_flags&KEY_IS_EXTENDED) {
            st=db_get_extended_key(db, (ham_u8_t *)lhs->data,
                    lhs->size, lhs->_flags, lhs);
            if (st)
                return st;
            lhs->_flags&=~KEY_IS_EXTENDED;
        }

        /* 2. load the second key, if needed */
        if (rhs->_flags&KEY_IS_EXTENDED) {
            st=db_get_extended_key(db, (ham_u8_t *)rhs->data,
                    rhs->size, rhs->_flags, rhs);
            if (st)
                return st;
            rhs->_flags&=~KEY_IS_EXTENDED;
        }

        /* 3. run the comparison function */
        cmp=foo((ham_db_t *)db, (ham_u8_t *)lhs->data, lhs->size, 
                        (ham_u8_t *)rhs->data, rhs->size);
    }

    return (cmp);
}

ham_status_t
db_create_backend(ham_backend_t **backend_ref, Database *db, ham_u32_t flags)
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

    env_get_cache(env)->remove_page(page);

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
        be = page_get_owner(page)->get_backend();
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
db_alloc_page_impl(ham_page_t **page_ref, ham_env_t *env, Database *db, 
                ham_u32_t type, ham_u32_t flags)
{
    ham_status_t st;
    ham_offset_t tellpos=0;
    ham_page_t *page=NULL;
    ham_bool_t allocated_by_me=HAM_FALSE;

    *page_ref = 0;
    ham_assert(0==(flags&~(PAGE_IGNORE_FREELIST|PAGE_CLEAR_WITH_ZERO)), (0));

    /* first, we ask the freelist for a page */
    if (!(flags&PAGE_IGNORE_FREELIST)) {
        st=freel_alloc_page(&tellpos, env, db);
        ham_assert(st ? !tellpos : 1, (0));
        if (tellpos) {
            ham_assert(tellpos%env_get_pagesize(env)==0,
                    ("page id %llu is not aligned", tellpos));
            /* try to fetch the page from the cache */
            page=env_get_cache(env)->get_page(tellpos, 0);
            if (page)
                goto done;
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
    if (env_get_cache(env)->is_too_big()) {
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
        env_get_changeset(env).add_page(page);

    /* store the page in the cache */
    env_get_cache(env)->put_page(page);

    *page_ref = page;
    return (HAM_SUCCESS);
}

ham_status_t
db_alloc_page(ham_page_t **page_ref, Database *db, 
                ham_u32_t type, ham_u32_t flags)
{
    return (db_alloc_page_impl(page_ref, db_get_env(db), db, type, flags));
}

ham_status_t
db_fetch_page_impl(ham_page_t **page_ref, ham_env_t *env, Database *db,
                ham_offset_t address, ham_u32_t flags)
{
    ham_page_t *page=0;
    ham_status_t st;

    ham_assert(0 == (flags & ~(HAM_HINTS_MASK|DB_ONLY_FROM_CACHE)), (0));

    *page_ref = 0;

    /* 
     * fetch the page from the cache
     */
    page=env_get_cache(env)->get_page(address, Cache::NOREMOVE);
    if (page) {
        *page_ref = page;
        ham_assert(page_get_pers(page), (""));
        ham_assert(db ? page_get_owner(page)==db : 1, (""));
        /* store the page in the changeset if recovery is enabled */
        if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY)
            env_get_changeset(env).add_page(page);
        return (HAM_SUCCESS);
    }

    if (flags&DB_ONLY_FROM_CACHE)
        return HAM_SUCCESS;

#if HAM_DEBUG
    ham_assert(env_get_cache(env)->get_page(address)==0, (""));
#endif

    /* can we allocate a new page for the cache? */
    if (env_get_cache(env)->is_too_big()) {
        if (env_get_rt_flags(env)&HAM_CACHE_STRICT) 
            return (HAM_CACHE_FULL);
    }

    page=page_new(env);
    if (!page)
        return (HAM_OUT_OF_MEMORY);

    page_set_owner(page, db);
    page_set_self(page, address);
    st=page_fetch(page);
    if (st) {
        (void)page_delete(page);
        return (st);
    }

    ham_assert(page_get_pers(page), (""));

    /* store the page in the cache */
    env_get_cache(env)->put_page(page);

    /* store the page in the changeset */
    if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY)
        env_get_changeset(env).add_page(page);

    *page_ref = page;
    return HAM_SUCCESS;
}

ham_status_t
db_fetch_page(ham_page_t **page_ref, Database *db,
                ham_offset_t address, ham_u32_t flags)
{
    return (db_fetch_page_impl(page_ref, db_get_env(db), db, address, flags));
}

ham_status_t
db_flush_page(ham_env_t *env, ham_page_t *page)
{
    ham_status_t st;

    /* write the page if it's dirty and if HAM_WRITE_THROUGH is enabled */
    if (page_is_dirty(page)) {
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
    if (page_get_self(page)!=0)
        env_get_cache(env)->put_page(page);

    return (0);
}

ham_status_t
db_flush_all(Cache *cache, ham_u32_t flags)
{
    ham_page_t *head;

    ham_assert(0 == (flags & ~DB_FLUSH_NODELETE), (0));

    if (!cache)
        return (0);

    head=cache->get_totallist();
    while (head) {
        ham_page_t *next=page_get_next(head, PAGE_LIST_CACHED);

        /*
         * don't remove the page from the cache, if flag NODELETE
         * is set (this flag is used i.e. in ham_flush())
         */
        if (!(flags&DB_FLUSH_NODELETE)) {
            cache->set_totallist(page_list_remove(cache->get_totallist(), 
                    PAGE_LIST_CACHED, head));
            cache->dec_cur_elements();
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
        env_get_cache(env)->remove_page(page);
        st=page_free(page);
        if (st)
            return (st);
        page_delete(page);
    }

    return (HAM_SUCCESS);
}

ham_status_t
db_resize_record_allocdata(Database *db, ham_size_t size)
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
db_resize_key_allocdata(Database *db, ham_size_t size)
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
db_copy_key(Database *db, const ham_key_t *source, ham_key_t *dest)
{
    /*
     * extended key: copy the whole key
     */
    if (source->_flags&KEY_IS_EXTENDED) {
        ham_status_t st=db_get_extended_key(db, (ham_u8_t *)source->data,
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
_local_fun_close(Database *db, ham_u32_t flags)
{
    ham_env_t *env=db_get_env(db);
    ham_status_t st = HAM_SUCCESS;
    ham_status_t st2 = HAM_SUCCESS;
    ham_backend_t *be;
    ham_bool_t has_other_db=HAM_FALSE;
    Database *newowner=0;
    ham_record_filter_t *record_head;

    /*
     * if this Database is the last database in the environment: 
     * delete all environment-members
     */
    if (env) {
        Database *n=env_get_list(env);
        while (n) {
            if (n!=db) {
                has_other_db=HAM_TRUE;
                break;
            }
            n=db_get_next(n);
        }
    }

    be=db->get_backend();

    /* close all open cursors */
    if (be && be->_fun_close_cursors) {
        st=be->_fun_close_cursors(be, flags);
        if (st)
            return (st);
    }
    
    btree_stats_flush_dbdata(db, db_get_db_perf_data(db), has_other_db);

    /*
     * if we're not in read-only mode, and not an in-memory-database,
     * and the dirty-flag is true: flush the page-header to disk
     */
    if (env
            && env_get_header_page(env) 
            && !(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)
            && env_get_device(env) 
            && env_get_device(env)->is_open(env_get_device(env)) 
            && (!(db_get_rt_flags(db)&HAM_READ_ONLY))) {
        /* flush the database header, if it's dirty */
        if (env_is_dirty(env)) {
            st=page_flush(env_get_header_page(env));
            if (st && st2==0)
                st2=st;
        }
    }

    /* get rid of the extkey-cache */
    if (db_get_extkey_cache(db)) {
        delete db_get_extkey_cache(db);
        db_set_extkey_cache(db, 0);
    }

    /* in-memory-database: free all allocated blobs */
    if (be && be_is_active(be) && env_get_rt_flags(env)&HAM_IN_MEMORY_DB) {
        ham_txn_t *txn;
        free_cb_context_t context;
        context.db=db;
        st=txn_begin(&txn, env, 0);
        if (st && st2==0)
            st2=st;
        else {
            (void)be->_fun_enumerate(be, __free_inmemory_blobs_cb, &context);
            (void)txn_commit(txn, 0);
        }
    }

    /* clear the changeset */
    if (env)
        env_get_changeset(env).clear();

    /*
     * flush all pages of this database (but not the header page,
     * it's still required and will be flushed below
     */
    if (env && env_get_cache(env)) {
        ham_page_t *n, *head=env_get_cache(env)->get_totallist();
        while (head) {
            n=page_get_next(head, PAGE_LIST_CACHED);
            if (page_get_owner(head)==db && head!=env_get_header_page(env)) {
                if (!(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)) 
                    (void)db_flush_page(env, head);
                (void)db_free_page(head, 0);
            }
            head=n;
        }
    }

    /* free cached memory */
    (void)db_resize_record_allocdata(db, 0);
    if (db_get_key_allocdata(db)) {
        allocator_free(env_get_allocator(env), db_get_key_allocdata(db));
        db_set_key_allocdata(db, 0);
        db_set_key_allocsize(db, 0);
    }

    /* clean up the transaction tree */
    if (db_get_optree(db))
        txn_free_optree(db_get_optree(db));

    /* close the backend */
    if (be && be_is_active(be)) {
        st=be->_fun_close(be);
        if (st && st2==0)
            st2=st;
    }

    if (be) {
        ham_assert(!be_is_active(be), (0));

        st=be->_fun_delete(be);
        if (st2==0)
            st2=st;

        /*
         * TODO
         * this free() should move into the backend destructor 
         */
        allocator_free(env_get_allocator(env), be);
        db->set_backend(0);
    }

    /*
     * environment: move the ownership to another database.
     * it's possible that there's no other page, then set the 
     * ownership to 0
     */
    if (env) {
        Database *head=env_get_list(env);
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

    /* close all record-level filters */
    record_head=db_get_record_filter(db);
    while (record_head) {
        ham_record_filter_t *next=record_head->_next;

        if (record_head->close_cb)
            record_head->close_cb((ham_db_t *)db, record_head);
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
_local_fun_get_parameters(Database *db, ham_parameter_t *param)
{
    ham_parameter_t *p=param;
    ham_env_t *env;

    env=db_get_env(db);

    if (p) {
        for (; p->name; p++) {
            switch (p->name) {
            case HAM_PARAM_CACHESIZE:
                p->value=env_get_cache(env)->get_capacity();
                break;
            case HAM_PARAM_PAGESIZE:
                p->value=env_get_pagesize(env);
                break;
            case HAM_PARAM_KEYSIZE:
                p->value=db->get_backend() ? db_get_keysize(db) : 21;
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
                if (env_get_filename(env).size())
                    p->value=(ham_u64_t)PTR_TO_U64(env_get_filename(env).c_str());
                else
                    p->value=0;
                break;
            case HAM_PARAM_GET_DATABASE_NAME:
                p->value=(ham_offset_t)db_get_dbname(db);
                break;
            case HAM_PARAM_GET_KEYS_PER_PAGE:
                if (db->get_backend()) {
                    ham_size_t count=0, size=db_get_keysize(db);
                    ham_backend_t *be = db->get_backend();
                    ham_status_t st;

                    if (!be->_fun_calc_keycount_per_page)
                        return (HAM_NOT_IMPLEMENTED);
                    st=be->_fun_calc_keycount_per_page(be, &count, size);
                    if (st)
                        return (st);
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
                    ham_status_t st=btree_stats_fill_ham_statistics_t(env,
                            db, (ham_statistics_t *)U64_TO_PTR(p->value));
                    if (st)
                        return (st);
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
_local_fun_check_integrity(Database *db, ham_txn_t *txn)
{
    ham_status_t st;
    ham_backend_t *be;

    be=db->get_backend();
    if (!be)
        return (HAM_NOT_INITIALIZED);
    if (!be->_fun_check_integrity)
        return (HAM_NOT_IMPLEMENTED);

    /* check the cache integrity */
    if (!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB)) {
        st=env_get_cache(db_get_env(db))->check_integrity();
        if (st)
            return (st);
    }

    /* purge cache if necessary */
    if (__cache_needs_purge(db_get_env(db))) {
        st=env_purge_cache(db_get_env(db));
        if (st)
            return (st);
    }

    /* call the backend function */
    st=be->_fun_check_integrity(be);
    env_get_changeset(db_get_env(db)).clear();

    return (st);
}

struct keycount_t 
{
    ham_u64_t c;
    ham_u32_t flags;
    ham_txn_t *txn;
    Database *db;
};

static void
db_get_key_count_txn(txn_opnode_t *node, void *data)
{
    struct keycount_t *kc=(struct keycount_t *)data;
    ham_backend_t *be=kc->db->get_backend();
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
_local_fun_get_key_count(Database *db, ham_txn_t *txn, ham_u32_t flags,
                ham_offset_t *keycount)
{
    ham_status_t st;
    ham_backend_t *be;
    ham_env_t *env=0;
    calckeys_context_t ctx = {db, flags, 0, HAM_FALSE};

    env=db_get_env(db);

    if (flags & ~(HAM_SKIP_DUPLICATES|HAM_FAST_ESTIMATE)) {
        ham_trace(("parameter 'flag' contains unsupported flag bits: %08x", 
                  flags & ~(HAM_SKIP_DUPLICATES|HAM_FAST_ESTIMATE)));
        return (HAM_INV_PARAMETER);
    }

    be = db->get_backend();
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
    st=be->_fun_enumerate(be, __calc_keys_cb, &ctx);
    if (st)
        goto bail;
    *keycount=ctx.total_count;

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
        *keycount+=k.c;
    }

bail:
    env_get_changeset(env).clear();
    return (st);
}

static ham_status_t
db_check_insert_conflicts(Database *db, ham_txn_t *txn, 
                txn_opnode_t *node, ham_key_t *key, ham_u32_t flags)
{
    ham_status_t st;
    txn_op_t *op=0;
    ham_backend_t *be=db->get_backend();

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
db_check_erase_conflicts(Database *db, ham_txn_t *txn, 
                txn_opnode_t *node, ham_key_t *key, ham_u32_t flags)
{
    txn_op_t *op=0;
    ham_backend_t *be=db->get_backend();

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

static void
__increment_dupe_index(Database *db, txn_opnode_t *node, Cursor *skip,
                ham_u32_t start)
{
    Cursor *c=db->get_cursors();

    while (c) {
        ham_bool_t hit=HAM_FALSE;

        if (c==skip || c->is_nil(0))
            goto next;

        /* if cursor is coupled to an op in the same node: increment 
         * duplicate index (if required) */
        if (c->is_coupled_to_txnop()) {
            txn_cursor_t *txnc=c->get_txn_cursor();
            txn_opnode_t *n=txn_op_get_node(txn_cursor_get_coupled_op(txnc));
            if (n==node)
                hit=HAM_TRUE;
        }
        /* if cursor is coupled to the same key in the btree: increment
         * duplicate index (if required) */
        else if (btree_cursor_points_to_key(c->get_btree_cursor(), 
                        txn_opnode_get_key(node))) {
            hit=HAM_TRUE;
        }

        if (hit) {
            if (c->get_dupecache_index()>start)
                c->set_dupecache_index(c->get_dupecache_index()+1);
        }

next:
        c=c->get_next();
    }
}

ham_status_t
db_insert_txn(Database *db, ham_txn_t *txn,
                ham_key_t *key, ham_record_t *record, ham_u32_t flags, 
                struct txn_cursor_t *cursor)
{
    ham_status_t st=0;
    txn_opnode_t *node;
    txn_op_t *op;
    ham_bool_t node_created=HAM_FALSE;
    ham_u64_t lsn=0;
    ham_env_t *env=db_get_env(db);

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
    env_get_changeset(env).clear();
    if (st) {
        if (node_created)
            txn_opnode_free(env, node);
        return (st);
    }

    /* get the next lsn */
    st=env_get_incremented_lsn(env, &lsn);
    if (st) {
        if (node_created)
            txn_opnode_free(env, node);
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
        Cursor *c=txn_cursor_get_parent(cursor);
        if (c->get_dupecache_index())
            txn_op_set_referenced_dupe(op, c->get_dupecache_index());

        c->set_to_nil(Cursor::CURSOR_TXN);
        txn_cursor_couple(cursor, op);

        /* all other cursors need to increment their dupe index, if their
         * index is > this cursor's index */
        __increment_dupe_index(db, node, c, c->get_dupecache_index());
    }

    /* append journal entry */
    if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY
            && env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS) {
        Journal *j=env_get_journal(env);
        st=j->append_insert(db, txn, key, record, 
                            flags&HAM_DUPLICATE ? flags : flags|HAM_OVERWRITE, 
                            txn_op_get_lsn(op));
    }

    return (st);
}

static void
__nil_all_cursors_in_node(ham_txn_t *txn, Cursor *current, 
                txn_opnode_t *node)
{
    txn_op_t *op=txn_opnode_get_newest_op(node);
    while (op) {
        txn_cursor_t *cursor=txn_op_get_cursors(op);
        while (cursor) {
            Cursor *pc=txn_cursor_get_parent(cursor);
            /* is the current cursor to a duplicate? then adjust the 
             * coupled duplicate index of all cursors which point to a
             * duplicate */
            if (current) {
                if (current->get_dupecache_index()) {
                    if (current->get_dupecache_index()
                            <pc->get_dupecache_index()) {
                        pc->set_dupecache_index(pc->get_dupecache_index()-1);
                        cursor=txn_cursor_get_coupled_next(cursor);
                        continue;
                    }
                    else if (current->get_dupecache_index()
                            >pc->get_dupecache_index()) {
                        cursor=txn_cursor_get_coupled_next(cursor);
                        continue;
                    }
                    /* else fall through */
                }
            }
            pc->couple_to_btree();
            pc->set_to_nil(Cursor::CURSOR_TXN);
            cursor=txn_op_get_cursors(op);
            /* set a flag that the cursor just completed an Insert-or-find 
             * operation; this information is needed in ham_cursor_move 
             * (in this aspect, an erase is the same as insert/find) */
            pc->set_lastop(Cursor::CURSOR_LOOKUP_INSERT);
        }

        op=txn_op_get_previous_in_node(op);
    }
}

static void
__nil_all_cursors_in_btree(Database *db, Cursor *current, ham_key_t *key)
{
    Cursor *c=db->get_cursors();

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
        if (c->is_nil(0) || c==current)
            goto next;
        if (c->is_coupled_to_txnop())
            goto next;

        if (btree_cursor_points_to_key(c->get_btree_cursor(), key)) {
            /* is the current cursor to a duplicate? then adjust the 
             * coupled duplicate index of all cursors which point to a
             * duplicate */
            if (current) {
                if (current->get_dupecache_index()) {
                    if (current->get_dupecache_index()
                            <c->get_dupecache_index()) {
                        c->set_dupecache_index(c->get_dupecache_index()-1);
                        goto next;
                    }
                    else if (current->get_dupecache_index()
                            >c->get_dupecache_index()) {
                        goto next;
                    }
                    /* else fall through */
                }
            }
            c->set_to_nil(0);
        }
next:
        c=c->get_next();
    }
}

ham_status_t
db_erase_txn(Database *db, ham_txn_t *txn, ham_key_t *key, ham_u32_t flags,
                txn_cursor_t *cursor)
{
    ham_status_t st=0;
    txn_opnode_t *node;
    txn_op_t *op;
    ham_bool_t node_created=HAM_FALSE;
    ham_u64_t lsn=0;
    ham_env_t *env=db_get_env(db);
    Cursor *pc=0;
    if (cursor)
        pc=txn_cursor_get_parent(cursor);

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
    if (!pc || (!pc->get_dupecache_index())) {
        st=db_check_erase_conflicts(db, txn, node, key, flags);
        if (st) {
            if (node_created)
                txn_opnode_free(env, node);
            return (st);
        }
    }

    /* get the next lsn */
    st=env_get_incremented_lsn(env, &lsn);
    if (st) {
        if (node_created)
            txn_opnode_free(env, node);
        return (st);
    }

    /* append a new operation to this node */
    op=txn_opnode_append(txn, node, flags, TXN_OP_ERASE, lsn, 0);
    if (!op)
        return (HAM_OUT_OF_MEMORY);

    /* is this function called through ham_cursor_erase? then add the 
     * duplicate ID */
    if (cursor) {
        if (pc->get_dupecache_index())
            txn_op_set_referenced_dupe(op, pc->get_dupecache_index());
    }

    /* the current op has no cursors attached; but if there are any 
     * other ops in this node and in this transaction, then they have to
     * be set to nil. This only nil's txn-cursors! */
    __nil_all_cursors_in_node(txn, pc, node);

    /* in addition we nil all btree cursors which are coupled to this key */
    __nil_all_cursors_in_btree(db, pc, txn_opnode_get_key(node));

    /* append journal entry */
    if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY
            && env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS) {
        Journal *j=env_get_journal(env);
        st=j->append_erase(db, txn, key, 0, flags|HAM_ERASE_ALL_DUPLICATES,
                            txn_op_get_lsn(op));
    }

    return (st);
}

static ham_status_t
db_find_txn(Database *db, ham_txn_t *txn,
                ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st=0;
    txn_optree_t *tree=0;
    txn_opnode_t *node=0;
    txn_op_t *op=0;
    ham_backend_t *be=db->get_backend();

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

static ham_status_t
_local_fun_insert(Database *db, ham_txn_t *txn,
                ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
    ham_env_t *env=db_get_env(db);
    ham_txn_t *local_txn=0;
    ham_status_t st;
    ham_backend_t *be;
    ham_u64_t recno = 0;
    ham_record_t temprec;

    be=db->get_backend();
    if (!be || !be_is_active(be))
        return (HAM_NOT_INITIALIZED);
    if (!be->_fun_insert)
        return (HAM_NOT_IMPLEMENTED);

    /* purge cache if necessary */
    if (__cache_needs_purge(db_get_env(db))) {
        st=env_purge_cache(db_get_env(db));
        if (st)
            return (st);
    }

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

        env_get_changeset(db_get_env(db)).clear();

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

    ham_assert(st==0, (""));

    if (local_txn)
        return (txn_commit(local_txn, 0));
    else if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY 
            && !(env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS))
        return (env_get_changeset(env).flush(DUMMY_LSN));
    else
        return (st);
}

static ham_status_t
_local_fun_erase(Database *db, ham_txn_t *txn, ham_key_t *key, ham_u32_t flags)
{
    ham_status_t st;
    ham_txn_t *local_txn=0;
    ham_env_t *env=db_get_env(db);
    ham_backend_t *be;
    ham_offset_t recno=0;

    be=db->get_backend();
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

    if (st) {
        if (local_txn)
            (void)txn_abort(local_txn, 0);
    
        env_get_changeset(db_get_env(db)).clear();
        return (st);
    }

    /* record number: re-translate the number to host endian */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        *(ham_offset_t *)key->data=ham_db2h64(recno);
    }

    ham_assert(st==0, (""));

    env_get_changeset(env).clear();

    if (local_txn)
        return (txn_commit(local_txn, 0));
    else if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY 
            && !(env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS))
        return (env_get_changeset(env).flush(DUMMY_LSN));
    else
        return (st);
}

static ham_status_t
_local_fun_find(Database *db, ham_txn_t *txn, ham_key_t *key,
                ham_record_t *record, ham_u32_t flags)
{
    ham_env_t *env=db_get_env(db);
    ham_txn_t *local_txn=0;
    ham_status_t st;
    ham_backend_t *be;
    ham_offset_t recno=0;

    be=db->get_backend();
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

    if ((db_get_keysize(db)<sizeof(ham_offset_t)) &&
            (key->size>db_get_keysize(db))) {
        ham_trace(("database does not support variable length keys"));
        return (HAM_INV_KEYSIZE);
    }

    /* if this database has duplicates, then we use ham_cursor_find
     * because we have to build a duplicate list, and this is currently
     * only available in ham_cursor_find */
    if (db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES) {
        Cursor *c;
        st=ham_cursor_create((ham_db_t *)db, txn, 0, (ham_cursor_t **)&c);
        if (st)
            return (st);
        st=ham_cursor_find_ex((ham_cursor_t *)c, key, record, flags);
        ham_cursor_close((ham_cursor_t *)c);
        return (st);
    }

    /* record number: make sure we have a number in little endian */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        ham_assert(key->size==sizeof(ham_u64_t), (""));
        ham_assert(key->data!=0, (""));
        recno=*(ham_offset_t *)key->data;
        recno=ham_h2db64(recno);
        *(ham_offset_t *)key->data=recno;
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

    if (st) {
        if (local_txn)
            (void)txn_abort(local_txn, 0);

        env_get_changeset(db_get_env(db)).clear();
        return (st);
    }

    /* record number: re-translate the number to host endian */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER)
        *(ham_offset_t *)key->data=ham_db2h64(recno);

    /* run the record-level filters */
    st=__record_filters_after_find(db, record);
    if (st) {
        if (local_txn)
            (void)txn_abort(local_txn, 0);

        env_get_changeset(env).clear();
        return (st);
    }

    ham_assert(st==0, (""));
    env_get_changeset(env).clear();

    if (local_txn)
        return (txn_commit(local_txn, 0));
    else if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY 
            && !(env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS))
        return (env_get_changeset(env).flush(DUMMY_LSN));
    else
        return (st);
}

static Cursor *
_local_cursor_create(Database *db, ham_txn_t *txn, ham_u32_t flags)
{
    ham_backend_t *be;

    be=db->get_backend();
    if (!be || !be_is_active(be))
        return (0);

    return (new Cursor(db, txn, flags));
}

static Cursor *
_local_cursor_clone(Cursor *src)
{
    return (new Cursor(*src));
}

static void
_local_cursor_close(Cursor *cursor)
{
    cursor->close();
}

static ham_status_t
_local_cursor_insert(Cursor *cursor, ham_key_t *key,
                ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    ham_backend_t *be;
    ham_u64_t recno = 0;
    ham_record_t temprec;
    Database *db=cursor->get_db();
    ham_env_t *env=db_get_env(db);
    ham_txn_t *local_txn=0;

    be=db->get_backend();
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
    if (!cursor->get_txn() 
            && (db_get_rt_flags(db)&HAM_ENABLE_TRANSACTIONS)) {
        st=txn_begin(&local_txn, env, 0);
        if (st)
            return (st);
        cursor->set_txn(local_txn);
    }

    if (cursor->get_txn() || local_txn) {
        st=db_insert_txn(db, 
                    cursor->get_txn() 
                      ? cursor->get_txn() 
                      : local_txn,
                    key, &temprec, flags, cursor->get_txn_cursor());
        if (st==0) {
            DupeCache *dc=cursor->get_dupecache();
            cursor->couple_to_txnop();
            /* reset the dupecache, otherwise cursor->get_dupecache_count()
             * does not update the dupecache correctly */
            dc->clear();
            /* if duplicate keys are enabled: set the duplicate index of
             * the new key  */
            if (st==0 && cursor->get_dupecache_count()) {
                ham_size_t i;
                txn_cursor_t *txnc=cursor->get_txn_cursor();
                txn_op_t *op=txn_cursor_get_coupled_op(txnc);
                ham_assert(op!=0, (""));

                for (i=0; i<dc->get_count(); i++) {
                    DupeCacheLine *l=dc->get_element(i);
                    if (!l->use_btree() && l->get_txn_op()==op) {
                        cursor->set_dupecache_index(i+1);
                        break;
                    }
                }
            }
        }
    }
    else {
        st=btree_cursor_insert(cursor->get_btree_cursor(), 
                        key, &temprec, flags);
        if (st==0)
            cursor->couple_to_btree();
    }

    /* if we created a temp. txn then clean it up again */
    if (local_txn)
        cursor->set_txn(0);

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

        env_get_changeset(env).clear();
        return (st);
    }

    /* no need to append the journal entry - it's appended in db_insert_txn(),
     * which is called by db_insert_txn() */

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

    ham_assert(st==0, (""));

    /* set a flag that the cursor just completed an Insert-or-find 
     * operation; this information is needed in ham_cursor_move */
    cursor->set_lastop(Cursor::CURSOR_LOOKUP_INSERT);

    if (local_txn)
        return (txn_commit(local_txn, 0));
    else if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY 
            && !(env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS))
        return (env_get_changeset(env).flush(DUMMY_LSN));
    else
        return (st);
}

static ham_status_t 
_local_cursor_erase(Cursor *cursor, ham_u32_t flags)
{
    ham_status_t st;
    Database *db=cursor->get_db();
    ham_env_t *env=db_get_env(db);
    ham_txn_t *local_txn=0;

    db_update_global_stats_erase_query(db, 0);

    /* if user did not specify a transaction, but transactions are enabled:
     * create a temporary one */
    if (!cursor->get_txn() 
            && (db_get_rt_flags(db)&HAM_ENABLE_TRANSACTIONS)) {
        st=txn_begin(&local_txn, env, 0);
        if (st)
            return (st);
        cursor->set_txn(local_txn);
    }

    /* this function will do all the work */
    st=cursor->erase(cursor->get_txn() ? cursor->get_txn() : local_txn, flags);

    /* clear the changeset */
    if (env)
        env_get_changeset(env).clear();

    /* if we created a temp. txn then clean it up again */
    if (local_txn)
        cursor->set_txn(0);

    /* on success: verify that cursor is now nil */
    if (st==0) {
        cursor->couple_to_btree();
        ham_assert(txn_cursor_is_nil(cursor->get_txn_cursor()), (""));
        ham_assert(cursor->is_nil(0), (""));
        cursor->clear_dupecache();
    }
    else {
        if (local_txn)
            (void)txn_abort(local_txn, 0);
        env_get_changeset(env).clear();
        return (st);
    }

    ham_assert(st==0, (""));

    /* no need to append the journal entry - it's appended in db_erase_txn(),
     * which is called by txn_cursor_erase() */

    if (local_txn)
        return (txn_commit(local_txn, 0));
    else if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY 
            && !(env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS))
        return (env_get_changeset(env).flush(DUMMY_LSN));
    else
        return (st);
}

static ham_status_t
_local_cursor_find(Cursor *cursor, ham_key_t *key, 
                ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    Database *db=cursor->get_db();
    ham_offset_t recno=0;
    ham_txn_t *local_txn=0;
    ham_env_t *env=db_get_env(db);
    txn_cursor_t *txnc=cursor->get_txn_cursor();

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
    if (!cursor->get_txn() 
            && (db_get_rt_flags(db)&HAM_ENABLE_TRANSACTIONS)) {
        st=txn_begin(&local_txn, env, 0);
        if (st)
            return (st);
        cursor->set_txn(local_txn);
    }

    /* reset the dupecache */
    cursor->clear_dupecache();

    /* 
     * first try to find the key in the transaction tree. If it exists and 
     * is NOT a duplicate then return its record. If it does not exist or
     * it has duplicates then lookup the key in the btree.
     *
     * in non-Transaction mode directly search through the btree.
     */
    if (cursor->get_txn() || local_txn) {
        st=txn_cursor_find(cursor->get_txn_cursor(), key, flags);
        /* if the key was erased in a transaction then fail with an error 
         * (unless we have duplicates - they're checked below) */
        if (st) {
            if (st==HAM_KEY_NOT_FOUND)
                goto btree;
            if (st==HAM_KEY_ERASED_IN_TXN) {
                /* performance hack: if coupled op erases ALL duplicates
                 * then we know that the key no longer exists. if coupled op 
                 * references a single duplicate w/ index > 1 then 
                 * we know that there are other duplicates. if coupled op 
                 * references the FIRST duplicate (idx 1) then we have 
                 * to check if there are other duplicates */
                txn_op_t *op=txn_cursor_get_coupled_op(txnc);
                ham_assert(txn_op_get_flags(op)&TXN_OP_ERASE, (""));
                if (!txn_op_get_referenced_dupe(op)) {
                    // ALL!
                    st=HAM_KEY_NOT_FOUND;
                }
                else if (txn_op_get_referenced_dupe(op)>1) {
                    // not the first dupe - there are other dupes
                    st=0;
                }
                else if (txn_op_get_referenced_dupe(op)==1) {
                    // check if there are other dupes
                    ham_bool_t is_equal;
                    (void)cursor->sync(Cursor::CURSOR_SYNC_ONLY_EQUAL_KEY, 
                                    &is_equal);
                    if (!is_equal)
                        cursor->set_to_nil(Cursor::CURSOR_BTREE);
                    if (!cursor->get_dupecache_count())
                        st=HAM_KEY_NOT_FOUND;
                    else
                        st=0;
                }
            }
            if (st)
                goto bail;
        }
        else {
            ham_bool_t is_equal;
            (void)cursor->sync(Cursor::CURSOR_SYNC_ONLY_EQUAL_KEY, &is_equal);
            if (!is_equal)
                cursor->set_to_nil(Cursor::CURSOR_BTREE);
        }
        cursor->couple_to_txnop();
        if (!cursor->get_dupecache_count()) {
            if (record)
                st=txn_cursor_get_record(txnc, record);
            goto bail;
        }
        if (st==0)
            goto check_dupes;
    }

btree:
    st=btree_cursor_find(cursor->get_btree_cursor(), key, record, flags);
    if (st==0) {
        cursor->couple_to_btree();
        /* if btree keys were found: reset the dupecache. The previous
         * call to cursor_get_dupecache_count() already initialized the
         * dupecache, but only with txn keys because the cursor was only
         * coupled to the txn */
        cursor->clear_dupecache();
    }

check_dupes:
    /* if the key has duplicates: build a duplicate table, then
     * couple to the first/oldest duplicate */
    if (cursor->get_dupecache_count()) {
        DupeCacheLine *e=cursor->get_dupecache()->get_first_element();
        if (e->use_btree())
            cursor->couple_to_btree();
        else
            cursor->couple_to_txnop();
        cursor->couple_to_dupe(1);
        st=0;

        /* now read the record */
        if (record) {
            /* TODO this works, but in case of the btree key w/ duplicates
            * it's possible that we read the record twice. I'm not sure if 
            * this can be avoided, though. */
            if (cursor->is_coupled_to_txnop())
                st=txn_cursor_get_record(cursor->get_txn_cursor(), 
                        record);
            else
                st=btree_cursor_move(cursor->get_btree_cursor(), 
                        0, record, 0);
        }
    }
    else {
        if (cursor->is_coupled_to_txnop() && record)
            st=txn_cursor_get_record(cursor->get_txn_cursor(), record);
    }

bail:
    /* if we created a temp. txn then clean it up again */
    if (local_txn)
        cursor->set_txn(0);

    if (st) {
        if (local_txn)
            (void)txn_abort(local_txn, 0);
        env_get_changeset(env).clear();
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
            env_get_changeset(env).clear();
            return (st);
        }
    }

    ham_assert(st==0, (""));
    env_get_changeset(env).clear();

    /* set a flag that the cursor just completed an Insert-or-find 
     * operation; this information is needed in ham_cursor_move */
    cursor->set_lastop(Cursor::CURSOR_LOOKUP_INSERT);

    if (local_txn)
        return (txn_commit(local_txn, 0));
    else if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY 
            && !(env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS))
        return (env_get_changeset(env).flush(DUMMY_LSN));
    else
        return (st);
}

static ham_status_t
_local_cursor_get_duplicate_count(Cursor *cursor, 
                ham_size_t *count, ham_u32_t flags)
{
    ham_status_t st=0;
    Database *db=cursor->get_db();
    ham_env_t *env=db_get_env(db);
    ham_txn_t *local_txn=0;
    txn_cursor_t *txnc=cursor->get_txn_cursor();

    /* purge cache if necessary */
    if (__cache_needs_purge(db_get_env(db))) {
        st=env_purge_cache(db_get_env(db));
        if (st)
            return (st);
    }

    if (cursor->is_nil(0) && txn_cursor_is_nil(txnc))
        return (HAM_CURSOR_IS_NIL);

    /* if user did not specify a transaction, but transactions are enabled:
     * create a temporary one */
    if (!cursor->get_txn() 
            && (db_get_rt_flags(db)&HAM_ENABLE_TRANSACTIONS)) {
        st=txn_begin(&local_txn, env, 0);
        if (st)
            return (st);
        cursor->set_txn(local_txn);
    }

    /* this function will do all the work */
    st=cursor->get_duplicate_count(
                    cursor->get_txn() ? cursor->get_txn() : local_txn,
                    count, flags);

    /* if we created a temp. txn then clean it up again */
    if (local_txn)
        cursor->set_txn(0);

    if (st) {
        if (local_txn)
            (void)txn_abort(local_txn, 0);
        env_get_changeset(env).clear();
        return (st);
    }

    ham_assert(st==0, (""));

    /* set a flag that the cursor just completed an Insert-or-find 
     * operation; this information is needed in ham_cursor_move */
    cursor->set_lastop(Cursor::CURSOR_LOOKUP_INSERT);

    if (local_txn)
        return (txn_commit(local_txn, 0));
    else if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY 
            && !(env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS))
        return (env_get_changeset(env).flush(DUMMY_LSN));
    else
        return (st);
}

static ham_status_t
_local_cursor_get_record_size(Cursor *cursor, ham_offset_t *size)
{
    ham_status_t st=0;
    Database *db=cursor->get_db();
    ham_env_t *env=db_get_env(db);
    ham_txn_t *local_txn=0;
    txn_cursor_t *txnc=cursor->get_txn_cursor();

    /* purge cache if necessary */
    if (__cache_needs_purge(db_get_env(db))) {
        st=env_purge_cache(db_get_env(db));
        if (st)
            return (st);
    }

    if (cursor->is_nil(0) && txn_cursor_is_nil(txnc))
        return (HAM_CURSOR_IS_NIL);

    /* if user did not specify a transaction, but transactions are enabled:
     * create a temporary one */
    if (!cursor->get_txn() 
            && (db_get_rt_flags(db)&HAM_ENABLE_TRANSACTIONS)) {
        st=txn_begin(&local_txn, env, 0);
        if (st)
            return (st);
        cursor->set_txn(local_txn);
    }

    /* this function will do all the work */
    st=cursor->get_record_size(
                    cursor->get_txn() ? cursor->get_txn() : local_txn,
                    size);

    /* if we created a temp. txn then clean it up again */
    if (local_txn)
        cursor->set_txn(0);

    env_get_changeset(env).clear();

    if (st) {
        if (local_txn)
            (void)txn_abort(local_txn, 0);
        return (st);
    }

    ham_assert(st==0, (""));

    /* set a flag that the cursor just completed an Insert-or-find 
     * operation; this information is needed in ham_cursor_move */
    cursor->set_lastop(Cursor::CURSOR_LOOKUP_INSERT);

    if (local_txn)
        return (txn_commit(local_txn, 0));
    else if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY 
            && !(env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS))
        return (env_get_changeset(env).flush(DUMMY_LSN));
    else
        return (st);
}

static ham_status_t
_local_cursor_overwrite(Cursor *cursor, ham_record_t *record,
                ham_u32_t flags)
{
    Database *db=cursor->get_db();
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
    if (!cursor->get_txn() 
            && (db_get_rt_flags(db)&HAM_ENABLE_TRANSACTIONS)) {
        st=txn_begin(&local_txn, env, 0);
        if (st)
            return (st);
        cursor->set_txn(local_txn);
    }

    /* this function will do all the work */
    st=cursor->overwrite(
                    cursor->get_txn() ? cursor->get_txn() : local_txn,
                    &temprec, flags);

    /* if we created a temp. txn then clean it up again */
    if (local_txn)
        cursor->set_txn(0);

    if (temprec.data != record->data)
        allocator_free(env_get_allocator(env), temprec.data);

    if (st) {
        if (local_txn)
            (void)txn_abort(local_txn, 0);
        env_get_changeset(env).clear();
        return (st);
    }

    ham_assert(st==0, (""));

    /* the journal entry is appended in db_insert_txn() */

    if (local_txn)
        return (txn_commit(local_txn, 0));
    else if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY 
            && !(env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS))
        return (env_get_changeset(env).flush(DUMMY_LSN));
    else
        return (st);
}

static ham_status_t
_local_cursor_move(Cursor *cursor, ham_key_t *key,
                ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st=0;
    Database *db=cursor->get_db();
    ham_env_t *env=db_get_env(db);
    ham_txn_t *local_txn=0;

    /* purge cache if necessary */
    if (__cache_needs_purge(db_get_env(db))) {
        st=env_purge_cache(db_get_env(db));
        if (st)
            return (st);
    }

    /*
     * if the cursor was never used before and the user requests a NEXT then
     * move the cursor to FIRST; if the user requests a PREVIOUS we set it 
     * to LAST, resp.
     *
     * if the cursor was already used but is nil then we've reached EOF, 
     * and a NEXT actually tries to move to the LAST key (and PREVIOUS
     * moves to FIRST)
     *
     * TODO the btree-cursor has identical code which can be removed
     */
    if (cursor->is_nil(0)) {
        if (flags&HAM_CURSOR_NEXT) {
            flags&=~HAM_CURSOR_NEXT;
            if (cursor->is_first_use())
              flags|=HAM_CURSOR_FIRST;
            else
              flags|=HAM_CURSOR_LAST;
        }
        else if (flags&HAM_CURSOR_PREVIOUS) {
            flags&=~HAM_CURSOR_PREVIOUS;
            if (cursor->is_first_use())
              flags|=HAM_CURSOR_LAST;
            else
              flags|=HAM_CURSOR_FIRST;
        }
    }

    /* in non-transactional mode - just call the btree function and return */
    if (!(db_get_rt_flags(db)&HAM_ENABLE_TRANSACTIONS)) {
        st=btree_cursor_move(cursor->get_btree_cursor(), 
                key, record, flags);
        env_get_changeset(env).clear();
        if (st)
            return (st);

        /* run the record-level filters */
        return (__record_filters_after_find(db, record));
    }

    /* if user did not specify a transaction, but transactions are enabled:
     * create a temporary one */
    if (!cursor->get_txn()
            && (db_get_rt_flags(db)&HAM_ENABLE_TRANSACTIONS)) {
        st=txn_begin(&local_txn, env, 0);
        if (st)
            return (st);
        cursor->set_txn(local_txn);
    }

    /* everything else is handled by the cursor function */
    st=cursor->move(key, record, flags);

    /* if we created a temp. txn then clean it up again */
    if (local_txn)
        cursor->set_txn(0);

    env_get_changeset(env).clear();

    /* run the record-level filters */
    if (st==0 && record)
        st=__record_filters_after_find(db, record);

    /* store the direction */
    if (flags&HAM_CURSOR_NEXT)
        cursor->set_lastop(HAM_CURSOR_NEXT);
    else if (flags&HAM_CURSOR_PREVIOUS)
        cursor->set_lastop(HAM_CURSOR_PREVIOUS);
    else
        cursor->set_lastop(0);

    if (st) {
        if (local_txn)
            (void)txn_abort(local_txn, 0);
        if (st==HAM_KEY_ERASED_IN_TXN)
            st=HAM_KEY_NOT_FOUND;
        /* trigger a sync when the function is called again */
        cursor->set_lastop(0);
        return (st);
    }

    if (local_txn)
        return (txn_commit(local_txn, 0));
    else
        return (st);
}

ham_status_t
db_initialize_local(Database *db)
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
    db->_fun_cursor_get_record_size=_local_cursor_get_record_size;
    db->_fun_cursor_overwrite=_local_cursor_overwrite;
    db->_fun_cursor_move    =_local_cursor_move;

    return (0);
}

