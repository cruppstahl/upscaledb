/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 * internal macros and headers
 *
 */

#ifndef HAM_ENV_H__
#define HAM_ENV_H__

#ifdef __cplusplus
extern "C" {
#endif

#include <ham/hamsterdb.h>
#include "mem.h"
#include "device.h"
#include "cache.h"
#include "extkeys.h"

/*
 * the environment structure
 */
struct ham_env_t
{
    /* the current transaction ID */
    ham_u64_t _txn_id;

    /* the device (either a file or an in-memory-db) */
    ham_device_t *_device;

    /* the cache */
    ham_cache_t *_cache;

    /* the memory allocator */
    mem_allocator_t *_alloc;

    /* the private txn object used by the freelist */
    ham_txn_t *_freel_txn;

    /* the file header page */
    ham_page_t *_hdrpage;

    /* the active txn */
    ham_txn_t *_txn;

    /* the cache for extended keys */
    extkey_cache_t *_extkey_cache;

    /* the database flags - a combination of the persistent flags
     * and runtime flags */
    ham_u32_t _rt_flags;

    /* a linked list of all open databases */
    ham_db_t *_next;

    /*
     * parameters, which are accepted by env_create_ex, and stored for the 
     * first env_create_db
     */
    ham_size_t _pagesize;
    ham_size_t _cachesize;
    ham_u16_t  _keysize;
};

/*
 * get the current transaction ID
 */
#define env_get_txn_id(env)              (env)->_txn_id

/*
 * set the current transaction ID
 */
#define env_set_txn_id(env, id)          (env)->_txn_id=id

/*
 * get the device
 */
#define env_get_device(env)              (env)->_device

/*
 * set the device
 */
#define env_set_device(env, d)           (env)->_device=(d)

/*
 * get the allocator
 */
#define env_get_allocator(env)           (env)->_alloc

/*
 * set the allocator
 */
#define env_set_allocator(env, a)        (env)->_alloc=(a)

/*
 * get the cache pointer
 */
#define env_get_cache(env)               (env)->_cache

/*
 * set the cache pointer
 */
#define env_set_cache(env, c)            (env)->_cache=c

/*
 * get the freelist's txn
 */
#define env_get_freelist_txn(env)        (env)->_freel_txn

/*
 * set the freelist's txn
 */
#define env_set_freelist_txn(env, txn)   (env)->_freel_txn=txn

/*
 * get the header page
 */
#define env_get_header_page(env)         (env)->_hdrpage

/*
 * set the header page
 */
#define env_set_header_page(env, h)      (env)->_hdrpage=(h)

/*
 * get the currently active transaction
 */
#define env_get_txn(env)                 (env)->_txn

/*
 * set the currently active transaction
 */
#define env_set_txn(env, txn)            (env)->_txn=txn

/*
 * get the cache for extended keys
 */
#define env_get_extkey_cache(env)        (env)->_extkey_cache

/*
 * set the cache for extended keys
 */
#define env_set_extkey_cache(env, c)     (env)->_extkey_cache=c

/*
 * get the runtime-flags
 */
#define env_get_rt_flags(env)            (env)->_rt_flags

/*
 * set the runtime-flags
 */
#define env_set_rt_flags(env, f)         (env)->_rt_flags=(f)

/* 
 * get the linked list of all open databases
 */
#define env_get_list(env)                (env)->_next

/* 
 * set the linked list of all open databases
 */
#define env_set_list(env, db)            (env)->_next=db

/*
 * set the parameter list
 */
#define env_set_pagesize(env, ps)        (env)->_pagesize =ps
#define env_set_keysize(env, ks)         (env)->_keysize  =ks
#define env_set_cachesize(env, cs)       (env)->_cachesize=cs


#ifdef __cplusplus
} // extern "C" {
#endif

#endif /* HAM_ENV_H__ */
