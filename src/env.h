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
#include "freelist.h"
#include "log.h"

/*
 * need packing for msvc x64bit
 */
#include "packstart.h"

/*
 * the environment structure
 */
struct ham_env_t
{
    /* the current transaction ID */
    ham_u64_t _txn_id;

    /* the filename of the environment file */
    const char *_filename;

    /* the 'mode' parameter of ham_env_create_ex */
    ham_u32_t _file_mode;

    /* the device (either a file or an in-memory-db) */
    ham_device_t *_device;

    /* the cache */
    ham_cache_t *_cache;

    /* the memory allocator */
    mem_allocator_t *_alloc;

    /* the file header page */
    ham_page_t *_hdrpage;

    /* the active txn */
    ham_txn_t *_txn;

    /* the log object */
    ham_log_t *_log;

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
    ham_size_t _max_databases;

    /* linked list of all file-level filters */
    ham_file_filter_t *_file_filters;

    /* the freelist cache */
    freelist_cache_t *_freelist_cache;
};

#include "packstop.h"

/*
 * get the current transaction ID
 */
#define env_get_txn_id(env)              (env)->_txn_id

/*
 * set the current transaction ID
 */
#define env_set_txn_id(env, id)          (env)->_txn_id=id

/*
 * get the filename
 */
#define env_get_filename(env)            (env)->_filename

/*
 * set the filename
 */
#define env_set_filename(env, f)         (env)->_filename=f

/*
 * get the unix file mode
 */
#define env_get_file_mode(env)           (env)->_file_mode

/*
 * set the unix file mode
 */
#define env_set_file_mode(env, m)        (env)->_file_mode=m

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
 * get the log object
 */
#define env_get_log(env)                 (env)->_log

/*
 * set the log object
 */
#define env_set_log(env, log)            (env)->_log=log

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
 * get the parameter list
 */
#define env_get_pagesize(env)            (env)->_pagesize
#define env_get_keysize(env)             (env)->_keysize
#define env_get_cachesize(env)           (env)->_cachesize
#define env_get_max_databases(env)       (env)->_max_databases

/*
 * set the parameter list
 */
#define env_set_pagesize(env, ps)        (env)->_pagesize =ps
#define env_set_keysize(env, ks)         (env)->_keysize  =ks
#define env_set_cachesize(env, cs)       (env)->_cachesize=cs
#define env_set_max_databases(env, md)   (env)->_max_databases=md

/*
 * get the linked list of all file-level filters
 */
#define env_get_file_filter(env)         (env)->_file_filters

/*
 * set the linked list of all file-level filters
 */
#define env_set_file_filter(env, f)      (env)->_file_filters=f

/*
 * get the freelist cache
 */
#define env_get_freelist_cache(env)      (env)->_freelist_cache

/*
 * set the freelist cache
 */
#define env_set_freelist_cache(env, c)   (env)->_freelist_cache=c


#ifdef __cplusplus
} // extern "C" {
#endif

#endif /* HAM_ENV_H__ */
