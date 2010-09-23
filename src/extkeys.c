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

#include "blob.h"
#include "cache.h"
#include "db.h"
#include "env.h"
#include "error.h"
#include "extkeys.h"
#include "mem.h"


/* EXTKEY_CACHE_BUCKETSIZE should be a prime number or similar, as it is
 * used in a MODULO hash scheme */
#define EXTKEY_CACHE_BUCKETSIZE         179	
#define EXTKEY_MAX_AGE                    5

extkey_cache_t *
extkey_cache_new(ham_db_t *db)
{
    extkey_cache_t *c;
    int memsize;

    memsize=sizeof(extkey_cache_t)+EXTKEY_CACHE_BUCKETSIZE*sizeof(extkey_t *);
    c=(extkey_cache_t *)allocator_calloc(env_get_allocator(db_get_env(db)), 
                            memsize);
    if (!c) {
        // HAM_OUT_OF_MEMORY;
        return (0);
    }

    extkey_cache_set_db(c, db);
    extkey_cache_set_bucketsize(c, EXTKEY_CACHE_BUCKETSIZE);

    return (c);
}

void
extkey_cache_destroy(extkey_cache_t *cache)
{
    ham_size_t i;
    extkey_t *e, *n;
    ham_db_t *db=extkey_cache_get_db(cache);
	ham_env_t *env = db_get_env(db);

    /*
     * make sure that all entries are empty
     */
    for (i=0; i<extkey_cache_get_bucketsize(cache); i++) {
        e=extkey_cache_get_bucket(cache, i);
        while (e) {
#if HAM_DEBUG
            /*
             * make sure that the extkey-cache is empty - only for in-memory
             * databases and DEBUG builds.
             */
            if (env_get_rt_flags(env)&HAM_IN_MEMORY_DB)
                ham_assert(!"extkey-cache is not empty!", (0));
#endif
            n=extkey_get_next(e);
            allocator_free(env_get_allocator(env), e);
            e=n;
        }
    }

    allocator_free(env_get_allocator(env), cache);
}

#define my_calc_hash(cache, o)                                              \
    ((ham_size_t)(extkey_cache_get_bucketsize(cache)==0                     \
        ? 0                                                                 \
        : (((o)%(cache_get_bucketsize(cache))))))

ham_status_t
extkey_cache_insert(extkey_cache_t *cache, ham_offset_t blobid, 
            ham_size_t size, const ham_u8_t *data)
{
    ham_size_t h=my_calc_hash(cache, blobid);
    extkey_t *e;
    ham_db_t *db=extkey_cache_get_db(cache);
	ham_env_t *env = db_get_env(db);

    /*
     * DEBUG build: make sure that the item is not inserted twice!
     */
#ifdef HAM_DEBUG
    e=extkey_cache_get_bucket(cache, h);
    while (e) {
        ham_assert(extkey_get_blobid(e)!=blobid, (0));
        e=extkey_get_next(e);
    }
#endif

    e=(extkey_t *)allocator_alloc(env_get_allocator(env), SIZEOF_EXTKEY_T+size);
    if (!e)
        return HAM_OUT_OF_MEMORY;
    extkey_set_blobid(e, blobid);
    /* TODO do not use txn id but lsn for age */
    extkey_set_age(e, env_get_txn_id(env));
    extkey_set_next(e, extkey_cache_get_bucket(cache, h));
    extkey_set_size(e, size);
    memcpy(extkey_get_data(e), data, size);

    extkey_cache_set_bucket(cache, h, e);
    extkey_cache_set_usedsize(cache, extkey_cache_get_usedsize(cache)+size);

    return (0);
}

ham_status_t
extkey_cache_remove(extkey_cache_t *cache, ham_offset_t blobid)
{
	ham_db_t *db=extkey_cache_get_db(cache);
	ham_env_t *env = db_get_env(db);
    ham_size_t h=my_calc_hash(cache, blobid);
    extkey_t *e, *prev=0;

    e=extkey_cache_get_bucket(cache, h);
    while (e) {
        if (extkey_get_blobid(e)==blobid)
            break;
        prev=e;
        e=extkey_get_next(e);
    }

    if (!e)
        return (HAM_KEY_NOT_FOUND);

    if (prev)
        extkey_set_next(prev, extkey_get_next(e));
    else
        extkey_cache_set_bucket(cache, h, extkey_get_next(e));

    extkey_cache_set_usedsize(cache, 
            extkey_cache_get_usedsize(cache)-extkey_get_size(e));
    allocator_free(env_get_allocator(env), e);

    return (0);
}

ham_status_t
extkey_cache_fetch(extkey_cache_t *cache, ham_offset_t blobid, 
            ham_size_t *size, ham_u8_t **data)
{
    ham_size_t h=my_calc_hash(cache, blobid);
    extkey_t *e;

    e=extkey_cache_get_bucket(cache, h);
    while (e) {
        if (extkey_get_blobid(e)==blobid)
            break;
        e=extkey_get_next(e);
    }

    if (!e)
        return (HAM_KEY_NOT_FOUND);

    *size=extkey_get_size(e);
    *data=extkey_get_data(e);
    /* TODO do not use txn id but lsn for age */
    extkey_set_age(e, env_get_txn_id(db_get_env(extkey_cache_get_db(cache))));

    return (0);
}

ham_status_t
extkey_cache_purge(extkey_cache_t *cache)
{
    ham_size_t i;
    extkey_t *e, *n;
    ham_env_t *env;
	
	ham_assert(extkey_cache_get_db(cache), (0));
	env = db_get_env(extkey_cache_get_db(cache));

    /*
     * delete all entries which are "too old" (were not 
     * used in the last EXTKEY_MAX_AGE transactions)
     */
    for (i=0; i<extkey_cache_get_bucketsize(cache); i++) {
        extkey_t *p=0;
        e=extkey_cache_get_bucket(cache, i);
        while (e) {
            n=extkey_get_next(e);
            /* TODO do not use txn id but lsn for age */
            if (env_get_txn_id(env)-extkey_get_age(e)>EXTKEY_MAX_AGE) {
                /* deleted the head element of the list? */
                if (!p)
                    extkey_cache_set_bucket(cache, i, n);
                else
                    extkey_set_next(p, n);
                allocator_free(env_get_allocator(env), e);
            }
            else
                p=e;
            e=n;
        }
    }

    return (0);
}

ham_status_t
extkey_remove(ham_db_t *db, ham_offset_t blobid)
{
    ham_status_t st;

    if (db_get_extkey_cache(db)) {
        st=extkey_cache_remove(db_get_extkey_cache(db), blobid);
        if (st && st!=HAM_KEY_NOT_FOUND)
            return (st);
    }

    return (blob_free(db_get_env(db), db, blobid, 0));
}
