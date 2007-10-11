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
 */

#include <string.h>
#include <ham/hamsterdb.h>
#include "extkeys.h"
#include "db.h"
#include "mem.h"
#include "error.h"
#include "cache.h"

#define EXTKEY_CACHE_BUCKETSIZE         251
#define EXTKEY_MAX_AGE                    5

extkey_cache_t *
extkey_cache_new(ham_db_t *db)
{
    extkey_cache_t *c;
    int memsize;

    memsize=sizeof(extkey_cache_t)+EXTKEY_CACHE_BUCKETSIZE*sizeof(void *);
    c=(extkey_cache_t *)ham_mem_calloc(db, memsize);
    if (!c) {
        db_set_error(db, HAM_OUT_OF_MEMORY);
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
            if (db_get_rt_flags(db)&HAM_IN_MEMORY_DB)
                ham_assert(!"extkey-cache is not empty!", (""));
#endif
            n=extkey_get_next(e);
            ham_mem_free(db, e);
            e=n;
        }
    }

    ham_mem_free(extkey_cache_get_db(cache), cache);
}

#define my_calc_hash(cache, o)                                              \
    (extkey_cache_get_bucketsize(cache)==0                                  \
        ? 0                                                                 \
        : (((o)%(cache_get_bucketsize(cache)))))

ham_status_t
extkey_cache_insert(extkey_cache_t *cache, ham_offset_t blobid, 
            ham_size_t size, const ham_u8_t *data)
{
    ham_size_t h=(ham_size_t)my_calc_hash(cache, blobid);
    extkey_t *e;
    ham_db_t *db=extkey_cache_get_db(cache);

    /*
     * DEBUG build: make sure that the item is not inserted twice!
     */
#ifdef HAM_DEBUG
    e=extkey_cache_get_bucket(cache, h);
    while (e) {
        ham_assert(extkey_get_blobid(e)!=blobid, 
                ("extkey (blob id %llu) is already in the cache!", 
                (unsigned long long)blobid));
        e=extkey_get_next(e);
    }
#endif

    e=(extkey_t *)ham_mem_alloc(db, SIZEOF_EXTKEY_T+size);
    if (!e)
        return (HAM_OUT_OF_MEMORY);
    extkey_set_blobid(e, blobid);
    extkey_set_txn_id(e, db_get_txn_id(db));
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
    ham_size_t h=(ham_size_t)my_calc_hash(cache, blobid);
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
    ham_mem_free(extkey_cache_get_db(cache), e);

    return (0);
}

ham_status_t
extkey_cache_fetch(extkey_cache_t *cache, ham_offset_t blobid, 
            ham_size_t *size, ham_u8_t **data)
{
    ham_size_t h=(ham_size_t)my_calc_hash(cache, blobid);
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
    extkey_set_txn_id(e, db_get_txn_id(extkey_cache_get_db(cache)));

    return (0);
}

ham_status_t
extkey_cache_purge(extkey_cache_t *cache)
{
    ham_size_t i;
    extkey_t *e, *n;
    ham_db_t *db=extkey_cache_get_db(cache);

    /*
     * delete all entries which are "too old" (were not 
     * used in the last EXTKEY_MAX_AGE transactions)
     */
    for (i=0; i<extkey_cache_get_bucketsize(cache); i++) {
        extkey_t *p=0;
        e=extkey_cache_get_bucket(cache, i);
        while (e) {
            n=extkey_get_next(e);
            if (db_get_txn_id(db)-extkey_get_txn_id(e)>EXTKEY_MAX_AGE) {
                /* deleted the head element of the list? */
                if (!p)
                    extkey_cache_set_bucket(cache, i, n);
                else
                    extkey_set_next(p, n);
                ham_mem_free(db, e);
            }
            else
                p=e;
            e=n;
        }
    }

    return (0);
}
