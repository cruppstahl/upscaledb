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
#include "db.h"
#include "env.h"
#include "error.h"
#include "extkeys.h"
#include "mem.h"


/* EXTKEY_CACHE_BUCKETSIZE should be a prime number or similar, as it is
 * used in a MODULO hash scheme */
#define EXTKEY_CACHE_BUCKETSIZE         1021	
#define EXTKEY_MAX_AGE                     5
#define __calc_hash(o)                  ((o)%(EXTKEY_CACHE_BUCKETSIZE))

/**
 * an extended key
 */
struct extkey_t
{
    /** the blobid of this key */
    ham_offset_t _blobid;

    /** the age of this extkey */
    ham_u64_t _age;

    /** pointer to the next key in the linked list */
    extkey_t *_next;

    /** the size of the extended key */
    ham_size_t _size;

    /** the key data */
    ham_u8_t _data[1];

};

/** the size of an extkey_t, without the single data byte */
#define SIZEOF_EXTKEY_T                     (sizeof(extkey_t)-1)

/** get the blobid */
#define extkey_get_blobid(e)                (e)->_blobid

/** set the blobid */
#define extkey_set_blobid(e, id)            (e)->_blobid=(id)

/** get the age of this extkey */
#define extkey_get_age(e)                   (e)->_age

/** set the age of this extkey */
#define extkey_set_age(e, age)              (e)->_age=(age)

/** get the next-pointer */
#define extkey_get_next(e)                  (e)->_next

/** set the next-pointer */
#define extkey_set_next(e, next)            (e)->_next=(next)

/** get the size */
#define extkey_get_size(e)                  (e)->_size

/** set the size */
#define extkey_set_size(e, size)            (e)->_size=(size)

/** get the data pointer */
#define extkey_get_data(e)                  (e)->_data


extkey_cache_t::extkey_cache_t(ham_db_t *db)
  : m_db(db), m_usedsize(0)
{
    for (ham_size_t i=0; i<EXTKEY_CACHE_BUCKETSIZE; i++)
        m_buckets.push_back(0);
}

extkey_cache_t::~extkey_cache_t()
{
    extkey_t *e, *n;
	ham_env_t *env=db_get_env(m_db);

    /*
     * make sure that all entries are empty
     */
    for (ham_size_t i=0; i<EXTKEY_CACHE_BUCKETSIZE; i++) {
        e=m_buckets[i];
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
}

ham_status_t
extkey_cache_t::insert(ham_offset_t blobid, ham_size_t size, 
                const ham_u8_t *data)
{
    ham_size_t h=__calc_hash(blobid);
    extkey_t *e;
	ham_env_t *env=db_get_env(m_db);

    /*
     * DEBUG build: make sure that the item is not inserted twice!
     */
#ifdef HAM_DEBUG
    e=m_buckets[h];
    while (e) {
        ham_assert(extkey_get_blobid(e)!=blobid, (0));
        e=extkey_get_next(e);
    }
#endif

    e=(extkey_t *)allocator_alloc(env_get_allocator(env), SIZEOF_EXTKEY_T+size);
    if (!e)
        return (HAM_OUT_OF_MEMORY);
    extkey_set_blobid(e, blobid);
    /* TODO do not use txn id but lsn for age */
    extkey_set_age(e, env_get_txn_id(env));
    extkey_set_next(e, m_buckets[h]);
    extkey_set_size(e, size);
    memcpy(extkey_get_data(e), data, size);

    m_buckets[h]=e;
    m_usedsize+=size;

    return (0);
}

void
extkey_cache_t::remove(ham_offset_t blobid)
{
	ham_env_t *env=db_get_env(m_db);
    ham_size_t h=__calc_hash(blobid);
    extkey_t *e, *prev=0;

    e=m_buckets[h];
    while (e) {
        if (extkey_get_blobid(e)==blobid)
            break;
        prev=e;
        e=extkey_get_next(e);
    }

    if (!e)
        return;

    if (prev)
        extkey_set_next(prev, extkey_get_next(e));
    else
        m_buckets[h]=extkey_get_next(e);

    m_usedsize-=extkey_get_size(e);
    allocator_free(env_get_allocator(env), e);
}

ham_status_t
extkey_cache_t::fetch(ham_offset_t blobid, ham_size_t *size, ham_u8_t **data)
{
    ham_size_t h=__calc_hash(blobid);
    extkey_t *e;

    e=m_buckets[h];
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
    extkey_set_age(e, env_get_txn_id(db_get_env(m_db)));

    return (0);
}

void
extkey_cache_t::purge(void)
{
    extkey_t *e, *n;
    ham_env_t *env=db_get_env(m_db);

    /*
     * delete all entries which are "too old" (were not 
     * used in the last EXTKEY_MAX_AGE transactions)
     */
    for (ham_size_t i=0; i<EXTKEY_CACHE_BUCKETSIZE; i++) {
        extkey_t *p=0;
        e=m_buckets[i];
        while (e) {
            n=extkey_get_next(e);
            /* TODO do not use txn id but lsn for age */
            if (env_get_txn_id(env)-extkey_get_age(e)>EXTKEY_MAX_AGE) {
                /* deleted the head element of the list? */
                if (!p)
                    m_buckets[i]=n;
                else
                    extkey_set_next(p, n);
                allocator_free(env_get_allocator(env), e);
            }
            else
                p=e;
            e=n;
        }
    }
}

void
extkey_cache_t::purge_all(void)
{
    extkey_t *e, *n;
    ham_env_t *env=db_get_env(m_db);

    /* delete all entries in the cache */
    for (ham_size_t i=0; i<EXTKEY_CACHE_BUCKETSIZE; i++) {
        e=m_buckets[i];
        while (e) {
            n=extkey_get_next(e);
            allocator_free(env_get_allocator(env), e);
            e=n;
        }
        m_buckets[i]=0;
    }
}

ham_status_t
extkey_remove(ham_db_t *db, ham_offset_t blobid)
{
    if (db_get_extkey_cache(db))
        db_get_extkey_cache(db)->remove(blobid);

    return (blob_free(db_get_env(db), db, blobid, 0));
}

