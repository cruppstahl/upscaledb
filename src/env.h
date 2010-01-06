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

#include <ham/hamsterdb.h>
#include "freelist.h"
#include "mem.h"
#include "device.h"
#include "cache.h"
#include "extkeys.h"
#include "log.h"

#ifdef __cplusplus
extern "C" {
#endif

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

    /* the Environment flags - a combination of the persistent flags
     * and runtime flags */
    ham_u32_t _rt_flags;

    /* a linked list of all open databases */
    ham_db_t *_next;

    /* the pagesize which was specified when the env was created */
    ham_size_t _pagesize;

    /* the cachesize which was specified when the env was created/opened */
    ham_size_t _cachesize;

    /* linked list of all file-level filters */
    ham_file_filter_t *_file_filters;

    /* the freelist cache */
    freelist_cache_t *_freelist_cache;

	/**
	 * some freelist algorithm specific run-time data
	 *
	 * This is done as a union as it will reduce code complexity
	 * significantly in the common freelist processing areas.
	 */
	ham_runtime_statistics_globdata_t _perf_data;
};

/*
 * get the current transaction ID
 */
#define env_get_txn_id(env)              (env)->_txn_id

/*
 * set the current transaction ID
 */
#define env_set_txn_id(env, id)          (env)->_txn_id=(id)

/*
 * get the filename
 */
#define env_get_filename(env)            (env)->_filename

/*
 * set the filename
 */
#define env_set_filename(env, f)         (env)->_filename=(f)

/*
 * get the unix file mode
 */
#define env_get_file_mode(env)           (env)->_file_mode

/*
 * set the unix file mode
 */
#define env_set_file_mode(env, m)        (env)->_file_mode=(m)

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
#define env_set_cache(env, c)            (env)->_cache=(c)

/*
 * get the header page
 */
#define env_get_header_page(env)         (env)->_hdrpage

/*
 * get a pointer to the header data
 */
#define env_get_header(env)              ((db_header_t *)(page_get_payload(\
                                          env_get_header_page(env))))

/*
 * set the header page
 */
#define env_set_header_page(env, h)      (env)->_hdrpage=(h)

/*
 * set the dirty-flag - this is the same as db_set_dirty()
 */
#define env_set_dirty(env)              page_set_dirty(env_get_header_page(env))

/*
 * get the private data of the backend; interpretation of the
 * data is up to the backend
 *
 * this is the same as db_get_indexdata_arrptr()
 */
#define env_get_indexdata_arrptr(env)                                         \
    ((db_indexdata_t *)((ham_u8_t *)page_get_payload(                         \
        env_get_header_page(env)) + sizeof(db_header_t)))

/*
 * get the private data of the backend; interpretation of the
 * data is up to the backend
 *
 * this is the same as db_get_indexdata_ptr()
 */
#define env_get_indexdata_ptr(env, i)      (env_get_indexdata_arrptr(env) + (i))

/*
 * get the currently active transaction
 */
#define env_get_txn(env)                 (env)->_txn

/*
 * set the currently active transaction
 */
#define env_set_txn(env, txn)            (env)->_txn=(txn)

/*
 * get the log object
 */
#define env_get_log(env)                 (env)->_log

/*
 * set the log object
 */
#define env_set_log(env, log)            (env)->_log=(log)

/*
 * get the cache for extended keys
 */
#define env_get_extkey_cache(env)        (env)->_extkey_cache

/*
 * set the cache for extended keys
 */
#define env_set_extkey_cache(env, c)     (env)->_extkey_cache=(c)

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
#define env_set_list(env, db)            (env)->_next=(db)

/*
 * get the pagesize as specified in ham_env_create_ex
 */
#define env_get_pagesize(env)            (env)->_pagesize

/*
 * set the pagesize as specified in ham_env_create_ex
 */
#define env_set_pagesize(env, ps)        (env)->_pagesize=(ps)

/*
 * get the cachesize as specified in ham_env_create_ex/ham_env_open_ex
 */
#define env_get_cachesize(env)           (env)->_cachesize

/*
 * set the cachesize as specified in ham_env_create_ex/ham_env_open_ex
 */
#define env_set_cachesize(env, cs)       (env)->_cachesize=(cs)

/*
 * get the maximum number of databases for this file
 */
#define env_get_max_databases(env)       ham_db2h16(env_get_header(env)->_max_databases)

/*
 * set the maximum number of databases for this file
 */
#define env_set_max_databases(env,s)     env_get_header(env)->_max_databases=  \
                                            ham_h2db16(s)

/*
 * get the page size
 */
#define env_get_persistent_pagesize(env) (ham_db2h32(env_get_header(env)->_pagesize))

/*
 * set the page size
 */
#define env_set_persistent_pagesize(env, ps)    env_get_header(env)->_pagesize=ham_h2db32(ps)

/*
 * set the 'magic' field of a file header
 */
#define env_set_magic(env, a,b,c,d)  { env_get_header(env)->_magic[0]=a; \
                                     env_get_header(env)->_magic[1]=b; \
                                     env_get_header(env)->_magic[2]=c; \
                                     env_get_header(env)->_magic[3]=d; }

/*
 * get byte #i of the 'magic'-header
 */
#define env_get_magic(hdr, i)        ((hdr)->_magic[i])

/*
 * set the version of a file header
 */
#define env_set_version(env,a,b,c,d) { env_get_header(env)->_version[0]=a; \
                                     env_get_header(env)->_version[1]=b; \
                                     env_get_header(env)->_version[2]=c; \
                                     env_get_header(env)->_version[3]=d; }

/*
 * get byte #i of the 'version'-header
 */
#define env_get_version(env, i)   (dbheader_get_version(env_get_header(env), i))

/*
 * get the serial number
 */
#define env_get_serialno(env)       (ham_db2h32(env_get_header(env)->_serialno))

/*
 * set the serial number
 */
#define env_set_serialno(env, n)    env_get_header(env)->_serialno=ham_h2db32(n)

/*
 * get the linked list of all file-level filters
 */
#define env_get_file_filter(env)         (env)->_file_filters

/*
 * set the linked list of all file-level filters
 */
#define env_set_file_filter(env, f)      (env)->_file_filters=(f)

/*
 * get the freelist cache
 */
#define env_get_freelist_cache(env)      (env)->_freelist_cache

/*
 * set the freelist cache
 */
#define env_set_freelist_cache(env, c)   (env)->_freelist_cache=(c)

/*
 * get a reference to the DB FILE (global) statistics
 */
#define env_get_global_perf_data(env)    &(env)->_perf_data


#ifdef __cplusplus
} // extern "C" {
#endif

#endif /* HAM_ENV_H__ */
