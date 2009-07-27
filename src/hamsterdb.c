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
 */

#include "config.h"

#ifdef HAVE_MALLOC_H
#  include <malloc.h>
#else
#  include <stdlib.h>
#endif
#include <string.h>
#include <ham/hamsterdb.h>
#include "error.h"
#include "mem.h"
#include "env.h"
#include "db.h"
#include "log.h"
#include "page.h"
#include "version.h"
#include "txn.h"
#include "cache.h"
#include "blob.h"
#include "freelist.h"
#include "extkeys.h"
#include "btree_cursor.h"
#include "cursor.h"
#include "util.h"
#include "keys.h"
#include "btree.h"
#include "serial.h"
#include "log.h"

#ifndef HAM_DISABLE_ENCRYPTION
#  include "../3rdparty/aes/aes.h"
#endif
#ifndef HAM_DISABLE_COMPRESSION
#  ifdef HAM_USE_SYSTEM_ZLIB
#    include <zlib.h>
#  else
#    include "../3rdparty/zlib/zlib.h"
#  endif
#endif

/* private parameter list entry for ham_create_ex */
#define HAM_PARAM_DBNAME          (1000)

/* a reserved database name for those databases, who're created without
 * an environment (and therefore don't have a name) */
#define EMPTY_DATABASE_NAME       (0xf000)

/* a reserved database name for the first database in an environment */
#define FIRST_DATABASE_NAME       (0xf001)

/* a reserved database name for a dummy database which only reads/writes 
 * the header page */
#define DUMMY_DATABASE_NAME       (0xf002)


typedef struct free_cb_context_t
{
    ham_db_t *db;

    ham_bool_t is_leaf;

} free_cb_context_t;

static ham_bool_t
my_check_recovery_flags(ham_u32_t flags)
{
    if (flags&HAM_ENABLE_RECOVERY) {
        if (flags&HAM_IN_MEMORY_DB) {
            ham_trace(("combination of HAM_ENABLE_RECOVERY and "
                       "HAM_IN_MEMORY_DB not allowed"));
            return (HAM_FALSE);
        }
        if (flags&HAM_WRITE_THROUGH) {
            ham_trace(("combination of HAM_ENABLE_RECOVERY and "
                       "HAM_WRITE_THROUGH not allowed"));
            return (HAM_FALSE);
        }
        if (flags&HAM_DISABLE_FREELIST_FLUSH) {
            ham_trace(("combination of HAM_ENABLE_RECOVERY and "
                       "HAM_DISABLE_FREELIST_FLUSH not allowed"));
            return (HAM_FALSE);
        }
    }
    return (HAM_TRUE);
}

/*
 * callback function for freeing blobs of an in-memory-database
 */
static void
my_free_cb(int event, void *param1, void *param2, void *context)
{
    int_key_t *key;
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
         */
        if (!(db_get_rt_flags(c->db)&HAM_IN_MEMORY_DB)) {
            ham_page_t *page=(ham_page_t *)param1;
            (void)txn_free_page(db_get_txn(c->db), page);
        }
        break;

    case ENUM_EVENT_ITEM:
        key=(int_key_t *)param1;

        if (key_get_flags(key)&KEY_IS_EXTENDED) {
            ham_offset_t blobid=key_get_extended_rid(c->db, key);
            /*
             * delete the extended key
             */
            (void)extkey_remove(c->db, blobid);
        }

        if (key_get_flags(key)&KEY_BLOB_SIZE_TINY ||
            key_get_flags(key)&KEY_BLOB_SIZE_SMALL ||
            key_get_flags(key)&KEY_BLOB_SIZE_EMPTY)
            break;

        /*
         * if we're in the leaf page, delete the blob
         */
        if (c->is_leaf)
            (void)key_erase_record(c->db, key, 0, BLOB_FREE_ALL_DUPES);
        break;

    default:
        ham_assert(!"unknown callback event", (""));
        break;
    }
}

static ham_status_t
__record_filters_before_insert(ham_db_t *db, ham_record_t *record)
{
    ham_status_t st=0;
    ham_record_filter_t *record_head;

    record_head=db_get_record_filter(db);
    while (record_head) {
        if (record_head->before_insert_cb) {
            st=record_head->before_insert_cb(db, record_head, record);
            if (st)
                break;
        }
        record_head=record_head->_next;
    }

    return (st);
}

static ham_status_t
__record_filters_after_find(ham_db_t *db, ham_record_t *record)
{
    ham_status_t st=0;
    ham_record_filter_t *record_head;

    record_head=db_get_record_filter(db);
    while (record_head) {
        if (record_head->after_read_cb) {
            st=record_head->after_read_cb(db, record_head, record);
            if (st)
                break;
        }
        record_head=record_head->_next;
    }

    return (st);
}

ham_status_t
ham_txn_begin(ham_txn_t **txn, ham_db_t *db, ham_u32_t flags)
{
    ham_status_t st;

    if (!(db_get_rt_flags(db)&HAM_ENABLE_TRANSACTIONS)) {
        ham_trace(("transactions are disabled (see HAM_ENABLE_TRANSACTIONS)"));
        return (HAM_INV_PARAMETER);
    }

    /* for hamsterdb 1.0.4 - only support one transaction */
    if (db_get_txn(db)) {
        ham_trace(("only one concurrent transaction is supported"));
        return (HAM_LIMITS_REACHED);
    }

    *txn=(ham_txn_t *)ham_mem_alloc(db, sizeof(**txn));
    if (!(*txn))
        return (db_set_error(db, HAM_OUT_OF_MEMORY));

    st=txn_begin(*txn, db, flags);
    if (st) {
        ham_mem_free(db, *txn);
        *txn=0;
    }

    return (st);
}

ham_status_t
ham_txn_commit(ham_txn_t *txn, ham_u32_t flags)
{
    ham_status_t st=txn_commit(txn, flags);
    if (st==0) {
        ham_db_t *db=txn_get_db(txn);
        memset(txn, 0, sizeof(*txn));
        ham_mem_free(db, txn);
    }

    return (st);
}

ham_status_t
ham_txn_abort(ham_txn_t *txn, ham_u32_t flags)
{
    ham_status_t st=txn_abort(txn, flags);
    if (st==0) {
        ham_db_t *db=txn_get_db(txn);
        memset(txn, 0, sizeof(*txn));
        ham_mem_free(db, txn);
    }

    return (st);
}

const char * HAM_CALLCONV
ham_strerror(ham_status_t result)
{
    switch (result) {
        case HAM_SUCCESS:
            return ("Success");
        case HAM_INV_KEYSIZE:
            return ("Invalid key size");
        case HAM_INV_PAGESIZE:
            return ("Invalid page size");
        case HAM_OUT_OF_MEMORY:
            return ("Out of memory");
        case HAM_NOT_INITIALIZED:
            return ("Object not initialized");
        case HAM_INV_PARAMETER:
            return ("Invalid parameter");
        case HAM_INV_FILE_HEADER:
            return ("Invalid database file header");
        case HAM_INV_FILE_VERSION:
            return ("Invalid database file version");
        case HAM_KEY_NOT_FOUND:
            return ("Key not found");
        case HAM_DUPLICATE_KEY:
            return ("Duplicate key");
        case HAM_INTEGRITY_VIOLATED:
            return ("Internal integrity violated");
        case HAM_INTERNAL_ERROR:
            return ("Internal error");
        case HAM_DB_READ_ONLY:
            return ("Database opened in read-only mode");
        case HAM_BLOB_NOT_FOUND:
            return ("Data blob not found");
        case HAM_PREFIX_REQUEST_FULLKEY:
            return ("Comparator function needs more data");
        case HAM_IO_ERROR:
            return ("System I/O error");
        case HAM_CACHE_FULL:
            return ("Database cache is full");
        case HAM_NOT_IMPLEMENTED:
            return ("Operation not implemented");
        case HAM_FILE_NOT_FOUND:
            return ("File not found");
        case HAM_WOULD_BLOCK:
            return ("Operation would block");
        case HAM_NOT_READY:
            return ("Object was not initialized correctly");
        case HAM_CURSOR_STILL_OPEN:
            return ("Cursor must be closed prior to Transaction abort/commit");
        case HAM_CURSOR_IS_NIL:
            return ("Cursor points to NIL");
        case HAM_DATABASE_NOT_FOUND:
            return ("Database not found");
        case HAM_DATABASE_ALREADY_EXISTS:
            return ("Database name already exists");
        case HAM_DATABASE_ALREADY_OPEN:
            return ("Database already open");
        case HAM_LIMITS_REACHED:
            return ("Database limits reached");
        case HAM_ALREADY_INITIALIZED:
            return ("Object was already initialized");
        case HAM_ACCESS_DENIED:
            return ("Encryption key is wrong");
        case HAM_NEED_RECOVERY:
            return ("Database needs recovery");
        case HAM_LOG_INV_FILE_HEADER:
            return ("Invalid log file header");
        default:
            return ("Unknown error");
    }
}

static ham_bool_t
__prepare_key(ham_key_t *key)
{
    if (key->size && !key->data) {
        ham_trace(("key->size != 0, but key->data is NULL"));
        return (0);
    }
    if (key->flags!=0 && key->flags!=HAM_KEY_USER_ALLOC) {
        ham_trace(("invalid flag in key->flags"));
        return (0);
    }
    key->_flags=0;
    return (1);
}

static ham_bool_t
__prepare_record(ham_record_t *record)
{
    if (record->size && !record->data) {
        ham_trace(("record->size != 0, but record->data is NULL"));
        return (0);
    }
    if (record->flags!=0 && record->flags!=HAM_RECORD_USER_ALLOC) {
        ham_trace(("invalid flag in record->flags"));
        return (0);
    }
    record->_intflags=0;
    record->_rid=0;
    return (1);
}

static void
__prepare_db(ham_db_t *db)
{
    ham_env_t *env=db_get_env(db);

    ham_assert(db_get_env(db)!=0, (""));

    if (env_get_header_page(env))
        page_set_owner(env_get_header_page(env), db);
    if (env_get_extkey_cache(env))
        extkey_cache_set_db(env_get_extkey_cache(env), db);
    if (env_get_txn(env))
        txn_set_db(env_get_txn(env), db);
}

static ham_status_t 
__check_create_parameters(ham_bool_t is_env, const char *filename, 
        ham_u32_t *flags, const ham_parameter_t *param, ham_size_t *ppagesize, 
        ham_u16_t *pkeysize, ham_size_t *pcachesize, ham_u16_t *pdbname,
        ham_size_t *maxdbs)
{
    ham_u32_t pagesize=0;
    ham_u16_t keysize=0, dbname=EMPTY_DATABASE_NAME;
    ham_size_t cachesize=0;
    ham_bool_t no_mmap=HAM_FALSE;

    if (maxdbs)
        *maxdbs=DB_MAX_INDICES;

    /* 
     * parse parameters 
     */
    if (param) {
        for (; param->name; param++) {
            switch (param->name) {
            case HAM_PARAM_CACHESIZE:
                cachesize=(ham_size_t)param->value;
                break;
            case HAM_PARAM_KEYSIZE:
                if (is_env) { /* calling from ham_env_create_ex? */
                    ham_trace(("invalid parameter HAM_PARAM_KEYSIZE"));
                    return (HAM_INV_PARAMETER);
                }
                else {
                    keysize=(ham_u16_t)param->value;
                    if ((*flags)&HAM_RECORD_NUMBER) {
                        if (keysize>0 && keysize<sizeof(ham_u64_t)) {
                            ham_trace(("invalid keysize - must be 8 for "
                                       "HAM_RECORD_NUMBER databases"));
                            return (HAM_INV_KEYSIZE);
                        }
                    }
                }
                break;
            case HAM_PARAM_PAGESIZE:
                pagesize=(ham_u32_t)param->value;
                break;
            case HAM_PARAM_MAX_ENV_DATABASES:
                if (is_env && maxdbs) {
                    *maxdbs=(ham_u32_t)param->value;
                    if (*maxdbs==0) {
                        ham_trace(("invalid parameter "
                                   "HAM_PARAM_MAX_ENV_DATABASES"));
                        return (HAM_INV_PARAMETER);
                    }
                }
                else {
                    ham_trace(("invalid parameter "
                               "HAM_PARAM_MAX_ENV_DATABASES"));
                    return (HAM_INV_PARAMETER);
                }
                break;
            case HAM_PARAM_DBNAME:
                dbname=(ham_u16_t)param->value;
                break;
            default:
                ham_trace(("unknown parameter"));
                return (HAM_INV_PARAMETER);
            }
        }
    }

    if (dbname==EMPTY_DATABASE_NAME) {
        if (!filename && !((*flags)&HAM_IN_MEMORY_DB)) {
            ham_trace(("filename is missing"));
            return (HAM_INV_PARAMETER);
        }
    }

    /*
     * make sure that the pagesize is aligned to 1024b
     */
    if (pagesize) {
        if (pagesize%1024) {
            ham_trace(("pagesize must be multiple of 1024"));
            return (HAM_INV_PAGESIZE);
        }
    }

    /*
     * creating a file in READ_ONLY mode? doesn't make sense
     */
    if ((*flags)&HAM_READ_ONLY) {
        ham_trace(("cannot create a file in read-only mode"));
        return (HAM_INV_PARAMETER);
    }

    /*
     * don't allow recovery in combination with some other flags
     */
    if (!my_check_recovery_flags(*flags))
        return (HAM_INV_PARAMETER);

    /*
     * in-memory-db? don't allow cache limits!
     */
    if ((*flags)&HAM_IN_MEMORY_DB) {
        if (((*flags)&HAM_CACHE_STRICT) || cachesize!=0) {
            ham_trace(("combination of HAM_IN_MEMORY_DB and HAM_CACHE_STRICT "
                        "or cachesize != 0 not allowed"));
            return (HAM_INV_PARAMETER);
        }
    }

    /*
     * don't allow cache limits with unlimited cache
     */
    if ((*flags)&HAM_CACHE_UNLIMITED) {
        if (cachesize || ((*flags)&HAM_CACHE_STRICT)) {
            ham_trace(("combination of HAM_CACHE_UNLIMITED and cachesize != 0 "
                        "or HAM_CACHE_STRICT not allowed"));
            return (HAM_INV_PARAMETER);
        }
    }

    /*
     * in-memory-db? use a default pagesize of 16kb
     */
    if ((*flags)&HAM_IN_MEMORY_DB) {
        if (!pagesize) {
            pagesize=16*1024;
            no_mmap=HAM_TRUE;
        }
    }

    /*
     * can we use mmap?
     */
#if HAVE_MMAP
    if (!((*flags)&HAM_DISABLE_MMAP)) {
        if (pagesize) {
            if (pagesize%os_get_granularity()!=0)
                no_mmap=HAM_TRUE;
        }
        else
            pagesize=os_get_pagesize();
    }
#else
    no_mmap=HAM_TRUE;
#endif

    /*
     * if we still don't have a pagesize, try to get a good default value
     */
    if (!pagesize)
        pagesize=os_get_pagesize();

    /*
     * set the database flags if we can't use mmapped I/O
     */
    if (no_mmap) {
        (*flags)&=~DB_USE_MMAP;
        (*flags)|=HAM_DISABLE_MMAP;
    }

    /*
     * initialize the keysize with a good default value;
     * 32byte is the size of a first level cache line for most modern
     * processors; adjust the keysize, so the keys are aligned to
     * 32byte
     */
    if (keysize==0) {
        if ((*flags)&HAM_RECORD_NUMBER)
            keysize = sizeof(ham_u64_t);
        else
            keysize = DB_CHUNKSIZE - (db_get_int_key_header_size());
    }

    /*
     * make sure that the pagesize is big enough for at least 5 keys;
     * record number database: need 8 byte
     *
     * By first calculating the keysize if none was specced, we can
     * quickly discard tiny page sizes as well here:
     */
   if (pagesize / keysize < 5) {
       ham_trace(("pagesize too small, must be at least %d bytes",
                   keysize*6));
       return (HAM_INV_KEYSIZE);
   }

    /*
     * And pagesize should not surpass the space we can occupy in a 
     * page for a freelist, or we'll be introducing gaps there.
     */
#if 0
    {
        /* number of bytes occupied by largest possible freelist. */
        ham_u64_t pl = 1ULL << (8 * sizeof(ham_freel_size_t) - 3);
        /* add header costs */
        pl += db_get_freelist_header_size();
        pl += db_get_persistent_header_size();
        if (pagesize > pl) {
            ham_trace(("pagesize too large, must be at most %d bytes",
                    (int)pl));
            return (HAM_INV_KEYSIZE);
        }
    }
#endif

    /*
     * make sure that max_databases actually fit in a header
     * page!
     * leave at least 128*8 bytes for the freelist and the other header data
     */
    if (maxdbs) {
        ham_size_t l = pagesize - sizeof(db_header_t)
                - db_get_freelist_header_size() - 128;
        l /= DB_INDEX_SIZE;
        if (maxdbs[0] > l) {
            ham_trace(("parameter HAM_PARAM_MAX_ENV_DATABASES too high for "
                        "this pagesize; the maximum allowed is %d", (int)l));
            return (HAM_INV_PARAMETER);
        }
    }

    /*
     * return the fixed parameters
     */
    *pcachesize=cachesize;
    *pkeysize  =keysize;
    *ppagesize =pagesize;
    if (pdbname)
        *pdbname=dbname;

    return (0);
}

void HAM_CALLCONV
ham_get_version(ham_u32_t *major, ham_u32_t *minor,
                ham_u32_t *revision)
{
    if (major)
        *major=HAM_VERSION_MAJ;
    if (minor)
        *minor=HAM_VERSION_MIN;
    if (revision)
        *revision=HAM_VERSION_REV;
}

void HAM_CALLCONV
ham_get_license(const char **licensee, const char **product)
{
    if (licensee)
        *licensee=HAM_LICENSEE;
    if (product)
        *product=HAM_PRODUCT_NAME;
}

ham_status_t HAM_CALLCONV
ham_env_new(ham_env_t **env)
{
    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    /* allocate memory for the ham_db_t-structure;
     * we can't use our allocator because it's not yet created! */
    *env=(ham_env_t *)malloc(sizeof(ham_env_t));
    if (!(*env))
        return (HAM_OUT_OF_MEMORY);

    /* reset the whole structure */
    memset(*env, 0, sizeof(ham_env_t));

    return (0);
}

ham_status_t HAM_CALLCONV
ham_env_delete(ham_env_t *env)
{
    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    /*
     * close the device
     */
    if (env_get_allocator(env)) {
        env_get_allocator(env)->close(env_get_allocator(env));
        env_set_allocator(env, 0);
    }

    /* 
     * close the allocator
     */
    if (env_get_device(env)) {
        if (env_get_device(env)->is_open(env_get_device(env))) {
            (void)env_get_device(env)->flush(env_get_device(env));
            (void)env_get_device(env)->close(env_get_device(env));
        }
        (void)env_get_device(env)->destroy(env_get_device(env));
        env_set_device(env, 0);
    }

    free(env);
    return (0);
}

ham_status_t HAM_CALLCONV
ham_env_create(ham_env_t *env, const char *filename,
        ham_u32_t flags, ham_u32_t mode)
{
    return (ham_env_create_ex(env, filename, flags, mode, 0));
}

ham_status_t HAM_CALLCONV
ham_env_create_ex(ham_env_t *env, const char *filename,
        ham_u32_t flags, ham_u32_t mode, const ham_parameter_t *param)
{
    ham_status_t st;
    ham_size_t pagesize;
    ham_u16_t keysize;
    ham_size_t cachesize;
    ham_size_t maxdbs;
    ham_db_t *dummydb;

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    /*
     * 1.0.4: HAM_ENABLE_TRANSACTIONS implies HAM_ENABLE_RECOVERY
     */
    if (flags&HAM_ENABLE_TRANSACTIONS)
        flags|=HAM_ENABLE_RECOVERY;

    /*
     * check (and modify) the parameters
     */
    st=__check_create_parameters(HAM_TRUE, filename, &flags, param, 
            &pagesize, &keysize, &cachesize, 0, &maxdbs);
    if (st)
        return (st);

    /* 
     * if we do not yet have an allocator: create a new one 
     */
    if (!env_get_allocator(env)) {
        env_set_allocator(env, ham_default_allocator_new());
        if (!env_get_allocator(env))
            return (HAM_OUT_OF_MEMORY);
    }

    /*
     * store the parameters
     */
    env_set_rt_flags(env, flags);
    env_set_pagesize(env, pagesize);
    env_set_keysize(env, keysize);
    env_set_cachesize(env, cachesize);
    env_set_max_databases(env, maxdbs);
    env_set_file_mode(env, mode);
    if (filename) {
        env_set_filename(env, 
                allocator_alloc(env_get_allocator(env), 
                    (ham_size_t)strlen(filename)+1));
        if (!env_get_filename(env)) {
            (void)ham_env_close(env, 0);
            return (HAM_OUT_OF_MEMORY);
        }
        strcpy((char *)env_get_filename(env), filename);
    }

    /*
     * now create a dummy database to create the header page; otherwise
     * all the settings could get lost
     *
     * this dummy database is not written to disk, but creates the header 
     * page and the device object (= the file). otherwise, if the environment
     * is created and then immediately closed, all settings would be lost
     * because no header page is written
     */
    st=ham_new(&dummydb);
    if (st)
        return (st);
    st=ham_env_create_db(env, dummydb, DUMMY_DATABASE_NAME, 0, 0);
    if (!st)
        ham_close(dummydb, 0);
    ham_delete(dummydb);

    return (st);
}

ham_status_t HAM_CALLCONV
ham_env_create_db(ham_env_t *env, ham_db_t *db,
        ham_u16_t name, ham_u32_t flags, const ham_parameter_t *param)
{
    ham_status_t st;
    ham_u16_t keysize;

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    if (!name || (name>=EMPTY_DATABASE_NAME && name!=DUMMY_DATABASE_NAME)) {
        ham_trace(("invalid database name"));
        return (HAM_INV_PARAMETER);
    }

    if (env_get_rt_flags(env)&HAM_READ_ONLY) {
        ham_trace(("cannot create database in read-only mode"));
        return (HAM_DB_READ_ONLY);
    }

    keysize=env_get_keysize(env);

    /*
     * only a few flags are allowed
     */
    if (flags&~(HAM_USE_BTREE|HAM_DISABLE_VAR_KEYLEN
               |HAM_RECORD_NUMBER|HAM_ENABLE_DUPLICATES)) {
        ham_trace(("invalid flags specified"));
        return (HAM_INV_PARAMETER);
    }

    /* 
     * parse parameters
     */
    if (param) {
        for (; param->name; param++) {
            switch (param->name) {
            case HAM_PARAM_KEYSIZE:
                keysize=(ham_u16_t)param->value;
                break;
            default:
                ham_trace(("unknown parameter"));
                return (HAM_INV_PARAMETER);
            }
        }
    }

    /*
     * store the env pointer in the database
     */
    db_set_env(db, env);

    /*
     * now create the database
     */
    {
    ham_parameter_t full_param[]={
        {HAM_PARAM_PAGESIZE,  env_get_pagesize(env)},
        {HAM_PARAM_CACHESIZE, env_get_cachesize(env)},
        {HAM_PARAM_KEYSIZE,   keysize},
        {HAM_PARAM_DBNAME,    name},
        {0, 0}};
    st=ham_create_ex(db, 0, flags|env_get_rt_flags(env), 0644, full_param);
    if (st)
        return (st);
    }

    /*
     * on success: store the open database in the environment's list of
     * opened databases - unless it's a dummy database
     */
    db_set_next(db, env_get_list(env));
    env_set_list(env, db);

    return (0);
}

ham_status_t HAM_CALLCONV
ham_env_open_db(ham_env_t *env, ham_db_t *db,
        ham_u16_t name, ham_u32_t flags, const ham_parameter_t *param)
{
    ham_db_t *head;
    ham_status_t st;
    ham_parameter_t full_param[]={
        {HAM_PARAM_CACHESIZE, 0},
        {HAM_PARAM_DBNAME,    name},
        {0, 0}};

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    if (!name) {
        ham_trace(("parameter 'name' must not be 0"));
        return (HAM_INV_PARAMETER);
    }
    if (name!=FIRST_DATABASE_NAME 
            && (name!=DUMMY_DATABASE_NAME && name>EMPTY_DATABASE_NAME)) {
        ham_trace(("parameter 'name' must be lower than 0xf000"));
        return (HAM_INV_PARAMETER);
    }

    full_param[0].value=env_get_cachesize(env);

    /* parse parameters */
    if (param) {
        for (; param->name; param++) {
            switch (param->name) {
            case HAM_PARAM_CACHESIZE:
                if (param->value > 0) {
                    if (!env_get_cache(env)) {
                        full_param[0].value=param->value;
                    }
                    else {
                        ham_trace(("invalid parameter HAM_PARAM_CACHESIZE - "
                                   "it's illegal to specify a new "
                                   "cache size when the cache has already "
                                   "been initialized"));
                        return (HAM_INV_PARAMETER);
                    }
                }
                else {
                    ham_trace(("invalid parameter "
                               "HAM_PARAM_CACHESIZE"));
                    return (HAM_INV_PARAMETER);
                }
                break;
            default:
                ham_trace(("unknown parameter"));
                return (HAM_INV_PARAMETER);
            }
        }
    }

    /*
     * make sure that this database is not yet open
     */
    head=env_get_list(env);
    while (head) {
        db_indexdata_t *ptr=db_get_indexdata_ptr(head, 
                                db_get_indexdata_offset(head));
        if (index_get_dbname(ptr)==name)
            return (HAM_DATABASE_ALREADY_OPEN);
        head=db_get_next(head);
    }

    /*
     * store the env pointer in the database
     */
    db_set_env(db, env);

    /*
     * now open the database
     */
    st=ham_open_ex(db, 0, flags|env_get_rt_flags(env), full_param);
    if (st==HAM_IO_ERROR)
        st=HAM_DATABASE_NOT_FOUND;
    if (st)
        return (st);

    /*
     * on success: store the open database in the environment's list of
     * opened databases
     */
    db_set_next(db, env_get_list(env));
    env_set_list(env, db);

    return (0);
}

ham_status_t HAM_CALLCONV
ham_env_open(ham_env_t *env, const char *filename, ham_u32_t flags)
{
    return (ham_env_open_ex(env, filename, flags, 0));
}

ham_status_t HAM_CALLCONV
ham_env_open_ex(ham_env_t *env, const char *filename,
        ham_u32_t flags, const ham_parameter_t *param)
{
    ham_status_t st;
    ham_size_t cachesize=0;
    ham_db_t *dummydb;

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    /*
     * 1.0.4: HAM_ENABLE_TRANSACTIONS implies HAM_ENABLE_RECOVERY
     */
    if (flags&HAM_ENABLE_TRANSACTIONS)
        flags|=HAM_ENABLE_RECOVERY;

    /* cannot open an in-memory-db */
    if (flags&HAM_IN_MEMORY_DB) {
        ham_trace(("cannot open an in-memory database"));
        return (HAM_INV_PARAMETER);
    }

    /* flag HAM_AUTO_RECOVERY implies HAM_ENABLE_RECOVERY */
    if (flags&HAM_AUTO_RECOVERY)
        flags|=HAM_ENABLE_RECOVERY;

    /* don't allow recovery in combination with some other flags */
    if (!my_check_recovery_flags(flags))
        return (HAM_INV_PARAMETER);

    /* parse parameters */
    if (param) {
        for (; param->name; param++) {
            switch (param->name) {
            case HAM_PARAM_CACHESIZE:
                cachesize=(ham_size_t)param->value;
                break;
            default:
                ham_trace(("unknown parameter"));
                return (HAM_INV_PARAMETER);
            }
        }
    }

    /* don't allow cache limits with unlimited cache */
    if (flags&HAM_CACHE_UNLIMITED) {
        if (cachesize || (flags&HAM_CACHE_STRICT)) {
            ham_trace(("combination of HAM_CACHE_UNLIMITED and cachesize != 0 "
                        "or HAM_CACHE_STRICT not allowed"));
            return (HAM_INV_PARAMETER);
        }
    }

    /* 
     * if we do not yet have an allocator: create a new one 
     */
    if (!env_get_allocator(env)) {
        env_set_allocator(env, ham_default_allocator_new());
        if (!env_get_allocator(env))
            return (HAM_OUT_OF_MEMORY);
    }

    /*
     * store the parameters
     */
    env_set_pagesize(env, 0);
    env_set_keysize(env, 0);
    env_set_cachesize(env, cachesize);
    env_set_rt_flags(env, flags);
    env_set_file_mode(env, 0644);
    if (filename) {
        env_set_filename(env, 
                allocator_alloc(env_get_allocator(env), 
                    (ham_size_t)strlen(filename)+1));
        if (!env_get_filename(env)) {
            (void)ham_env_close(env, 0);
            return (HAM_OUT_OF_MEMORY);
        }
        strcpy((char *)env_get_filename(env), filename);
    }

    /*
     * now open a dummy database to read the header page
     *
     * this dummy database is not really opened, it only opens the header 
     * page and the device object (= the file). otherwise, it would not be
     * possible to query the database flags/settings after the env is 
     * open
     */
    st=ham_new(&dummydb);
    if (st)
        return (st);
    st=ham_env_open_db(env, dummydb, DUMMY_DATABASE_NAME, 0, 0);
    if (!st)
        ham_close(dummydb, 0);
    ham_delete(dummydb);
    if (st)
        return (st);

    /*
     * open the logfile and check if we need recovery
     */
    if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY
            && env_get_log(env)==0) {
        ham_log_t *log;
        st=ham_log_open(env_get_allocator(env), env_get_filename(env), 0, &log);
        if (!st) { /* success - check if we need recovery */
            ham_bool_t isempty;
            st=ham_log_is_empty(log, &isempty);
            if (st) {
                (void)ham_env_close(env, 0);
                return (st);
            }
            env_set_log(env, log);
            if (!isempty) {
                if (flags&HAM_AUTO_RECOVERY) {
                    st=ham_log_recover(log, env_get_device(env));
                    if (st) {
                        (void)ham_env_close(env, 0);
                        return (st);
                    }
                }
                else {
                    (void)ham_env_close(env, 0);
                    return (HAM_NEED_RECOVERY);
                }
            }
        }
        else if (st && st==HAM_FILE_NOT_FOUND) {
            st=ham_log_create(env_get_allocator(env), filename, 0644, 0, &log);
            if (st) {
                (void)ham_env_close(env, 0);
                return (st);
            }
            env_set_log(env, log);
        }
        else {
            (void)ham_env_close(env, 0);
            return (st);
        }
    }

    return (HAM_SUCCESS);
}

ham_status_t HAM_CALLCONV
ham_env_rename_db(ham_env_t *env, ham_u16_t oldname, 
                ham_u16_t newname, ham_u32_t flags)
{
    ham_u16_t i, slot;
    ham_db_t *db;
    ham_bool_t owner=HAM_FALSE;
    ham_status_t st;

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!oldname) {
        ham_trace(("parameter 'oldname' must not be 0"));
        return (HAM_INV_PARAMETER);
    }
    if (!newname) {
        ham_trace(("parameter 'newname' must not be 0"));
        return (HAM_INV_PARAMETER);
    }
    if (newname>=EMPTY_DATABASE_NAME) {
        ham_trace(("parameter 'newname' must be lower than 0xf000"));
        return (HAM_INV_PARAMETER);
    }

    /*
     * make sure that the environment was either created or opened, and 
     * a valid device exists
     */
    if (!env_get_device(env))
        return (HAM_NOT_READY);

    /*
     * no need to do anything if oldname==newname
     */
    if (oldname==newname)
        return (0);

    /*
     * !!
     * we have to distinguish two cases: in the first case, we have a valid
     * ham_db_t pointer, and can therefore access the header page.
     *
     * In the second case, no db was opened/created, and therefore we 
     * have to temporarily fetch the header page
     */
    if (env_get_list(env)) {
        db=env_get_list(env);
    }
    else {
        owner=HAM_TRUE;
        st=ham_new(&db);
        if (st)
            return (st);
        st=ham_env_open_db(env, db, FIRST_DATABASE_NAME, 0, 0);
        if (st) {
            ham_delete(db);
            return (st);
        }
    }

    /*
     * check if a database with the new name already exists; also search 
     * for the database with the old name
     */
    slot=db_get_max_databases(db);
    ham_assert(db_get_max_databases(db) > 0, (0));
    for (i=0; i<db_get_max_databases(db); i++) {
        ham_u16_t name=index_get_dbname(db_get_indexdata_ptr(db, i));
        if (name==newname) {
            if (owner) {
                (void)ham_close(db, 0);
                (void)ham_delete(db);
            }
            return (HAM_DATABASE_ALREADY_EXISTS);
        }
        if (name==oldname)
            slot=i;
    }

    if (slot==db_get_max_databases(db)) {
        if (owner) {
            (void)ham_close(db, 0);
            (void)ham_delete(db);
        }
        return (HAM_DATABASE_NOT_FOUND);
    }

    /*
     * replace the database name with the new name
     */
    index_set_dbname(db_get_indexdata_ptr(db, slot), newname);

    db_set_dirty(db, HAM_TRUE);
    
    if (owner) {
        (void)ham_close(db, 0);
        (void)ham_delete(db);
    }

    return (0);
}

ham_status_t HAM_CALLCONV
ham_env_erase_db(ham_env_t *env, ham_u16_t name, ham_u32_t flags)
{
    ham_db_t *db;
    ham_status_t st;
    free_cb_context_t context;
    ham_txn_t txn;

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!name) {
        ham_trace(("parameter 'name' must not be 0"));
        return (HAM_INV_PARAMETER);
    }

    /*
     * check if this database is still open
     */
    db=env_get_list(env);
    while (db) {
        ham_u16_t dbname=index_get_dbname(db_get_indexdata_ptr(db,
                            db_get_indexdata_offset(db)));
        if (dbname==name)
            return (HAM_DATABASE_ALREADY_OPEN);
        db=db_get_next(db);
    }

    /*
     * if it's an in-memory environment: no need to go on, if the 
     * database was closed, it does no longer exist
     */
    if (env_get_rt_flags(env)&HAM_IN_MEMORY_DB)
        return (HAM_DATABASE_NOT_FOUND);

    /*
     * temporarily load the database
     */
    st=ham_new(&db);
    if (st)
        return (st);
    st=ham_env_open_db(env, db, name, 0, 0);
    if (st) {
        (void)ham_delete(db);
        return (st);
    }

    /*
     * delete all blobs and extended keys, also from the cache and
     * the extkey_cache
     *
     * also delete all pages and move them to the freelist; if they're 
     * cached, delete them from the cache
     */
    if ((st=txn_begin(&txn, db, 0))) {
        (void)ham_close(db, 0);
        (void)ham_delete(db);
        return (st);
    }
    context.db=db;
    st=db_get_backend(db)->_fun_enumerate(db_get_backend(db), 
            my_free_cb, &context);
    if (st) {
        (void)txn_abort(&txn, 0);
        (void)ham_close(db, 0);
        (void)ham_delete(db);
        return (st);
    }

    st=txn_commit(&txn, 0);
    if (st) {
        (void)ham_close(db, 0);
        (void)ham_delete(db);
        return (st);
    }

    /*
     * set database name to 0
     */
    index_set_dbname(db_get_indexdata_ptr(db, db_get_indexdata_offset(db)), 0);
    db_set_dirty(db, HAM_TRUE);

    /*
     * clean up and return
     */
    (void)ham_close(db, 0);
    (void)ham_delete(db);

    return (0);
}

ham_status_t HAM_CALLCONV
ham_env_add_file_filter(ham_env_t *env, ham_file_filter_t *filter)
{
    ham_file_filter_t *head;

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!filter) {
        ham_trace(("parameter 'filter' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    head=env_get_file_filter(env);

    /*
     * clean up if there are still links from a previous
     * installation
     */
    filter->_next=0;
    filter->_prev=0;

    /*
     * !!
     * add the filter at the end of all filters, then we can process them
     * later in the same order as the insertion
     */
    if (!head) {
        env_set_file_filter(env, filter);
    }
    else {
        while (head->_next)
            head=head->_next;

        filter->_prev=head;
        head->_next=filter;
    }

    return (0);
}

ham_status_t HAM_CALLCONV
ham_env_remove_file_filter(ham_env_t *env, ham_file_filter_t *filter)
{
    ham_file_filter_t *head, *prev;

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!filter) {
        ham_trace(("parameter 'filter' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    head=env_get_file_filter(env);

    if (head==filter) {
        env_set_file_filter(env, head->_next);
        return 0;
    }

    do {
        prev=head;
        head=head->_next;
        if (!head)
            break;
        if (head==filter) {
            prev->_next=head->_next;
            if (head->_next)
                head->_next->_prev=prev;
            break;
        }
    } while(head);

    return (0);
}

ham_status_t HAM_CALLCONV
ham_env_get_database_names(ham_env_t *env, ham_u16_t *names, ham_size_t *count)
{
    ham_db_t *db;
    ham_bool_t temp_db=HAM_FALSE;
    ham_u16_t name;
    ham_size_t i=0;
    ham_size_t max_names;
    ham_status_t st=0;

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!names) {
        ham_trace(("parameter 'names' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!count) {
        ham_trace(("parameter 'count' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    max_names=*count;
    *count=0;

    /*
     * to access the database header, we need a db handle; either use an
     * open handle or load a temporary database
     *
     * in-memory databases: if we don't have a db handle, the environment
     * is empty - return success
     */
    db=env_get_list(env);
    if (!db) {
        if (env_get_rt_flags(env)&HAM_IN_MEMORY_DB)
            return (0);
        st=ham_new(&db);
        if (st)
            return (st);
        st=ham_env_open_db(env, db, FIRST_DATABASE_NAME, 0, 0);
        if (st) {
            (void)ham_delete(db);
            return (st==HAM_DATABASE_NOT_FOUND ? 0 : st);
        }
        temp_db=HAM_TRUE;
    }

    /*
     * copy each database name in the array
     */
    ham_assert(db_get_max_databases(db) > 0, (0));
    for (i=0; i<db_get_max_databases(db); i++) {
        name = index_get_dbname(db_get_indexdata_ptr(db, i));
        if (name==0 || name>EMPTY_DATABASE_NAME)
            continue;

        if (*count>=max_names) {
            st=HAM_LIMITS_REACHED;
            goto bail;
        }

        names[(*count)++]=name;
    }

bail:
    if (temp_db) {
        (void)ham_close(db, 0);
        ham_delete(db);
    }

    return st;
}

ham_status_t HAM_CALLCONV
ham_env_close(ham_env_t *env, ham_u32_t flags)
{
    ham_status_t st;
    ham_file_filter_t *file_head;

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    /*
     * close all databases?
     */
    if (env_get_list(env)) {
        ham_db_t *db=env_get_list(env);
        while (db) {
            ham_db_t *next=db_get_next(db);
            st=ham_close(db, flags);
            if (st)
                return (st);
            db=next;
        }
        env_set_list(env, 0);
    }

    /*
     * close the header page
     *
     * !!
     * the last database, which was closed, has set the owner of the 
     * page to 0, which means that we can't call page_free, page_delete
     * etc. We have to use the device-routines.
     */
    if (env_get_header_page(env)) {
        ham_device_t *device=env_get_device(env);
        ham_page_t *page=env_get_header_page(env);
        if (page_get_pers(page))
            (void)device->free_page(device, page);
        allocator_free(env_get_allocator(env), page);
        env_set_header_page(env, 0);
    }

    /* 
     * close the device
     */
    if (env_get_device(env)) {
        if (env_get_device(env)->is_open(env_get_device(env))) {
            (void)env_get_device(env)->flush(env_get_device(env));
            (void)env_get_device(env)->close(env_get_device(env));
        }
        (void)env_get_device(env)->destroy(env_get_device(env));
        env_set_device(env, 0);
    }

    /*
     * close all file-level filters
     */
    file_head=env_get_file_filter(env);
    while (file_head) {
        ham_file_filter_t *next=file_head->_next;
        if (file_head->close_cb)
            file_head->close_cb(env, file_head);
        file_head=next;
    }
    env_set_file_filter(env, 0);

    /*
     * close the log
     */
    if (env_get_log(env)) {
        (void)ham_log_close(env_get_log(env), (flags&HAM_DONT_CLEAR_LOG));
        env_set_log(env, 0);
    }

    /*
     * close everything else
     */
    if (env_get_filename(env)) {
        allocator_free(env_get_allocator(env), 
                        (ham_u8_t *)env_get_filename(env));
        env_get_filename(env)=0;
    }

    /* 
     * finally, close the memory allocator 
     */
    if (env_get_allocator(env)) {
        env_get_allocator(env)->close(env_get_allocator(env));
        env_set_allocator(env, 0);
    }

    return (HAM_SUCCESS);
}

ham_status_t HAM_CALLCONV
ham_new(ham_db_t **db)
{
    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    /* allocate memory for the ham_db_t-structure;
     * we can't use our allocator because it's not yet created! */
    *db=(ham_db_t *)malloc(sizeof(ham_db_t));
    if (!(*db))
        return (HAM_OUT_OF_MEMORY);

    /* reset the whole structure */
    memset(*db, 0, sizeof(ham_db_t));

    return (0);
}

ham_status_t HAM_CALLCONV
ham_delete(ham_db_t *db)
{
    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    /* free cached data pointers */
    (void)db_resize_allocdata(db, 0);

    /* close the allocator */
    if (db_get_allocator(db)) {
        if (!db_get_env(db)) // [i_a] otherwise you'd blow away the methods for the ENV ... 
        {
            db_get_allocator(db)->close(db_get_allocator(db));
            db_set_allocator(db, 0);
        }
    }

    /* "free" all remaining memory */
    free(db);

    return (0);
}

ham_status_t HAM_CALLCONV
ham_open(ham_db_t *db, const char *filename, ham_u32_t flags)
{
    return (ham_open_ex(db, filename, flags, 0));
}

ham_status_t HAM_CALLCONV
ham_open_ex(ham_db_t *db, const char *filename,
        ham_u32_t flags, const ham_parameter_t *param)
{
    ham_status_t st;
    ham_cache_t *cache;
    ham_backend_t *backend;
    ham_u8_t hdrbuf[512];
    ham_u16_t dbname=FIRST_DATABASE_NAME;
    ham_size_t i, cachesize=0, pagesize=0;
    ham_page_t *page;
    ham_device_t *device;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    db_set_error(db, 0);

    /*
     * 1.0.4: HAM_ENABLE_TRANSACTIONS implies HAM_ENABLE_RECOVERY
     */
    if (flags&HAM_ENABLE_TRANSACTIONS)
        flags|=HAM_ENABLE_RECOVERY;

    /* flag HAM_AUTO_RECOVERY imples HAM_ENABLE_RECOVERY */
    if (flags&HAM_AUTO_RECOVERY)
        flags|=HAM_ENABLE_RECOVERY;

    /* cannot open an in-memory-db */
    if (flags&HAM_IN_MEMORY_DB) {
        ham_trace(("cannot open an in-memory database"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    /* HAM_ENABLE_DUPLICATES has to be specified in ham_create, not 
     * ham_open */
    if (flags&HAM_ENABLE_DUPLICATES) {
        ham_trace(("invalid flag HAM_ENABLE_DUPLICATES (only allowed when "
                    "creating a database"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    /* don't allow recovery in combination with some other flags */
    if (!my_check_recovery_flags(flags))
        return (HAM_INV_PARAMETER);

    /* parse parameters */
    if (param) {
        for (; param->name; param++) {
            switch (param->name) {
            case HAM_PARAM_CACHESIZE:
                cachesize=(ham_size_t)param->value;
                break;
            case HAM_PARAM_DBNAME:
                dbname=(ham_u16_t)param->value;
                break;
            default:
                ham_trace(("unknown parameter"));
                return (db_set_error(db, HAM_INV_PARAMETER));
            }
        }
    }

    /* don't allow cache limits with unlimited cache */
    if (flags&HAM_CACHE_UNLIMITED) {
        if (cachesize || (flags&HAM_CACHE_STRICT)) {
            ham_trace(("combination of HAM_CACHE_UNLIMITED and cachesize != 0 "
                        "or HAM_CACHE_STRICT not allowed"));
            return (HAM_INV_PARAMETER);
        }
    }

    if (!db_get_env(db) && !filename) {
        ham_trace(("parameter 'filename' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    if (!filename)
        filename=env_get_filename(db_get_env(db));

    /* 
     * if we do not yet have an allocator: create a new one 
     */
    if (!db_get_allocator(db)) {
        if (db_get_env(db))
            env_set_allocator(db_get_env(db), ham_default_allocator_new());
        else
            db_set_allocator(db, ham_default_allocator_new());
        if (!db_get_allocator(db))
            return (db_set_error(db, HAM_OUT_OF_MEMORY));
    }

    /* 
     * initialize the device
     */
    if (!db_get_device(db)) {
        device=ham_device_new(db_get_allocator(db), flags&HAM_IN_MEMORY_DB);
        if (!device)
            return (db_get_error(db));

        if (db_get_env(db))
            env_set_device(db_get_env(db), device);
        else
            db_set_device(db, device);

        /* 
         * open the file 
         */
        st=device->open(device, filename, flags);
        if (st) {
            (void)ham_close(db, 0);
            return (db_set_error(db, st));
        }
    }
    else
        device=db_get_device(db);

    /*
     * read the database header
     *
     * !!!
     * now this is an ugly problem - the database header is one page, but
     * how large is one page? chances are good that it's the default
     * page-size, but we really can't be sure.
     *
     * read 512 byte and extract the "real" page size, then read 
     * the real page. (but i really don't like this)
     */
    if (!db_get_header_page(db)) {
        db_header_t *hdr;

        st=device->read(db, device, 0, hdrbuf, sizeof(hdrbuf));
        if (st) {
            (void)ham_close(db, 0);
            return (db_set_error(db, st));
        }
        hdr = (db_header_t *)&hdrbuf[SIZEOF_PAGE_UNION_HEADER];
        pagesize = ham_db2h32(hdr->_pagesize);
        device_set_pagesize(device, pagesize);

        /*
         * can we use mmap?
         */
#if HAVE_MMAP
        if (!(flags&HAM_DISABLE_MMAP)) {
            if (pagesize%os_get_granularity()==0)
                flags|=DB_USE_MMAP;
            else
                device->set_flags(device, flags|HAM_DISABLE_MMAP);
        }
        else
            device->set_flags(device, flags|HAM_DISABLE_MMAP);
        flags&=~HAM_DISABLE_MMAP; /* don't store this flag */
#else
        device->set_flags(device, flags|HAM_DISABLE_MMAP);
#endif
    }

    db_set_error(db, HAM_SUCCESS);

    /*
     * open the logfile and check if we need recovery - but only if we're
     * without an environment. Environment recovery is checked in ham_env_open.
     */
    if (dbname!=DUMMY_DATABASE_NAME
            && db_get_env(db)==0 
            && (flags&HAM_ENABLE_RECOVERY)) {
        ham_log_t *log;
        st=ham_log_open(db_get_allocator(db), filename, 0, &log);
        if (!st) { /* success - check if we need recovery */
            ham_bool_t isempty;
            st=ham_log_is_empty(log, &isempty);
            if (st) {
                (void)ham_close(db, 0);
                return (db_set_error(db, st));
            }
            db_set_log(db, log);
            if (!isempty) {
                if (flags&HAM_AUTO_RECOVERY) {
                    st=ham_log_recover(log, db_get_device(db));
                    if (st) {
                        (void)ham_close(db, 0);
                        return (db_set_error(db, st));
                    }
                }
                else {
                    (void)ham_close(db, 0);
                    return (db_set_error(db, HAM_NEED_RECOVERY));
                }
            }
        }
        else if (st && st==HAM_FILE_NOT_FOUND) {
            st=ham_log_create(db_get_allocator(db), filename, 0644, 0, &log);
            if (st) {
                (void)ham_close(db, 0);
                return (db_set_error(db, st));
            }
            db_set_log(db, log);
        }
        else {
            (void)ham_close(db, 0);
            return (st);
        }
    }

    /*
     * read the header page
     */
    if (!db_get_header_page(db)) {
        page=page_new(db);
        if (!page) {
            (void)ham_close(db, 0);
            return (db_get_error(db));
        }
        st=page_fetch(page, pagesize);
        if (st) {
            if (page_get_pers(page))
                (void)page_free(page);
            (void)page_delete(page);
            (void)ham_close(db, 0);
            return (st);
        }
        if (page_get_type(page) != PAGE_TYPE_HEADER) {
            ham_log(("invalid page header type"));
            if (page_get_pers(page))
                (void)page_free(page);
            (void)page_delete(page);
            (void)ham_close(db, 0);
            db_set_error(db, HAM_INV_FILE_HEADER);
            return (HAM_INV_FILE_HEADER);
        }

        if (db_get_env(db))
            env_set_header_page(db_get_env(db), page);
        else
            db_set_header_page(db, page);

        /* store the max_databases value */
        if (db_get_env(db))
            env_set_max_databases(db_get_env(db), db_get_max_databases(db));

        /* store the pagesize */
        if (db_get_env(db))
            env_set_pagesize(db_get_env(db), pagesize);
    }
    /*
     * otherwise, if a header page already exists (which means that we're
     * in an environment), we transfer the ownership of the header 
     * page to this database
     */
    else {
        page_set_owner(db_get_header_page(db), db);
    }

    /* 
     * check the file magic
     */
    if (db_get_magic(db, 0)!='H' ||
        db_get_magic(db, 1)!='A' ||
        db_get_magic(db, 2)!='M' ||
        db_get_magic(db, 3)!='\0') {
        ham_log(("invalid file type"));
        (void)ham_close(db, 0);
        db_set_error(db, HAM_INV_FILE_HEADER);
        return (HAM_INV_FILE_HEADER);
    }

    /* 
     * check the database version
     */
    if (db_get_version(db, 0)!=HAM_VERSION_MAJ ||
        db_get_version(db, 1)!=HAM_VERSION_MIN) {
        ham_log(("invalid file version"));
        (void)ham_close(db, 0);
        db_set_error(db, HAM_INV_FILE_VERSION);
        return (HAM_INV_FILE_VERSION);
    }

    /*
     * already done? When a new environment is opened, a dummy database
     * is opened only to read the header page. In this case, we can
     * return now
     */
    if (dbname==DUMMY_DATABASE_NAME)
        return (0);

    /*
     * search for a database with this name
     */
    ham_assert(db_get_max_databases(db) > 0, (0));
    ham_assert(0 != db_get_header_page(db), (0));
    for (i=0; i<db_get_max_databases(db); i++) {
        if (index_get_dbname(db_get_indexdata_ptr(db, i))==0)
            continue;
        if (dbname == FIRST_DATABASE_NAME ||
                dbname == index_get_dbname(db_get_indexdata_ptr(db, i))) {
            db_set_indexdata_offset(db, i);
            break;
        }
    }
    if (i==db_get_max_databases(db)) {
        (void)ham_close(db, 0);
        return (db_set_error(db, HAM_DATABASE_NOT_FOUND));
    }

    /* 
     * create the backend
     */
    backend=db_create_backend(db, flags);
    if (!backend) {
        (void)ham_close(db, 0);
        return (db_set_error(db, HAM_NOT_INITIALIZED));
    }
    db_set_backend(db, backend);

    /* 
     * initialize the backend 
     */
    st=backend->_fun_open(backend, flags);
    if (st) {
        db_set_error(db, st);
        (void)ham_close(db, 0);
        return (st);
    }

    /* 
     * set the database flags 
     */
    db_set_rt_flags(db, flags|be_get_flags(backend));
    ham_assert(!(be_get_flags(backend)&HAM_DISABLE_VAR_KEYLEN), 
            ("invalid persistent database flags 0x%x", be_get_flags(backend)));
    ham_assert(!(be_get_flags(backend)&HAM_CACHE_STRICT), 
            ("invalid persistent database flags 0x%x", be_get_flags(backend)));
    ham_assert(!(be_get_flags(backend)&HAM_CACHE_UNLIMITED), 
            ("invalid persistent database flags 0x%x", be_get_flags(backend)));
    ham_assert(!(be_get_flags(backend)&HAM_DISABLE_MMAP), 
            ("invalid persistent database flags 0x%x", be_get_flags(backend)));
    ham_assert(!(be_get_flags(backend)&HAM_WRITE_THROUGH), 
            ("invalid persistent database flags 0x%x", be_get_flags(backend)));
    ham_assert(!(be_get_flags(backend)&HAM_READ_ONLY), 
            ("invalid persistent database flags 0x%x", be_get_flags(backend)));
    ham_assert(!(be_get_flags(backend)&HAM_DISABLE_FREELIST_FLUSH), 
            ("invalid persistent database flags 0x%x", be_get_flags(backend)));
    ham_assert(!(be_get_flags(backend)&HAM_ENABLE_RECOVERY), 
            ("invalid persistent database flags 0x%x", be_get_flags(backend)));
    ham_assert(!(be_get_flags(backend)&HAM_AUTO_RECOVERY), 
            ("invalid persistent database flags 0x%x", be_get_flags(backend)));
    ham_assert(!(be_get_flags(backend)&HAM_ENABLE_TRANSACTIONS), 
            ("invalid persistent database flags 0x%x", be_get_flags(backend)));
    ham_assert(!(be_get_flags(backend)&DB_USE_MMAP), 
            ("invalid persistent database flags 0x%x", be_get_flags(backend)));

    /* 
     * initialize the cache
     */
    if (!db_get_cache(db)) {
        ham_size_t ps=db_get_pagesize(db);
        if (cachesize==0)
            cachesize=HAM_DEFAULT_CACHESIZE; 
        cache=cache_new(db, (cachesize+ps-1)/ps);
        if (!cache) {
            (void)ham_close(db, 0);
            return (db_get_error(db));
        }
        if (db_get_env(db))
            env_set_cache(db_get_env(db), cache);
        else
            db_set_cache(db, cache);
    }

    /* 
     * set the key compare function
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        ham_set_compare_func(db, db_default_recno_compare);
    }
    else {
        ham_set_compare_func(db, db_default_compare);
        ham_set_prefix_compare_func(db, db_default_prefix_compare);
    }

    return (HAM_SUCCESS);
}

ham_status_t HAM_CALLCONV
ham_create(ham_db_t *db, const char *filename, ham_u32_t flags, ham_u32_t mode)
{
    return (ham_create_ex(db, filename, flags, mode, 0));
}

ham_status_t HAM_CALLCONV
ham_create_ex(ham_db_t *db, const char *filename,
        ham_u32_t flags, ham_u32_t mode, const ham_parameter_t *param)
{
    ham_status_t st;
    ham_cache_t *cache;
    ham_backend_t *backend;
    ham_page_t *page;
    ham_u32_t pflags;
    ham_device_t *device;

    ham_size_t pagesize;
    ham_u16_t keysize;
    ham_u16_t dbname=0;
    ham_size_t i;
    ham_size_t cachesize;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    db_set_error(db, 0);

    /*
     * 1.0.4: HAM_ENABLE_TRANSACTIONS implies HAM_ENABLE_RECOVERY
     */
    if (flags&HAM_ENABLE_TRANSACTIONS)
        flags|=HAM_ENABLE_RECOVERY;

    /*
     * check (and modify) the parameters
     */
    st=__check_create_parameters(HAM_FALSE, filename, &flags, param, 
            &pagesize, &keysize, &cachesize, &dbname, 0);
    if (st)
        return (db_set_error(db, st));

    /* 
     * if we do not yet have an allocator: create a new one 
     */
    if (!db_get_allocator(db)) {
        db_set_allocator(db, ham_default_allocator_new());
        if (!db_get_allocator(db))
            return (db_set_error(db, HAM_OUT_OF_MEMORY));
    }

    /* 
     * initialize the device, if it does not yet exist
     */
    if (!db_get_device(db)) {
        device=ham_device_new(db_get_allocator(db), flags&HAM_IN_MEMORY_DB);

        if (!device)
            return (db_get_error(db));
        if (db_get_env(db))
            env_set_device(db_get_env(db), device);
        else
            db_set_device(db, device);
        device->set_flags(device, flags);
        device_set_pagesize(device, pagesize);

        if (!filename && db_get_env(db))
            filename=env_get_filename(db_get_env(db));

        /* create the file */
        st=device->create(device, filename, flags, mode);
        if (st) {
            (void)ham_close(db, 0);
            return (db_set_error(db, st));
        }
    }
    else
        device=db_get_device(db);

    /*
     * create a logging object, if logging is enabled (and if it was not
     * yet created)
     */
    if ((flags&HAM_ENABLE_RECOVERY) && !(db_get_log(db))) {
        ham_log_t *log;
        st=ham_log_create(db_get_allocator(db), 
                db_get_env(db) ? env_get_filename(db_get_env(db)) : filename, 
                db_get_env(db) ? env_get_file_mode(db_get_env(db)) : mode, 
                0, &log);
        if (st) {
            (void)ham_close(db, 0);
            return (db_set_error(db, st));
        }
        if (db_get_env(db))
            env_set_log(db_get_env(db), log);
        else
            db_set_log(db, log);
    }

    /*
     * set the flags
     */
    pflags=flags;
    pflags&=~HAM_DISABLE_VAR_KEYLEN;
    pflags&=~HAM_CACHE_STRICT;
    pflags&=~HAM_CACHE_UNLIMITED;
    pflags&=~HAM_DISABLE_MMAP;
    pflags&=~HAM_WRITE_THROUGH;
    pflags&=~HAM_READ_ONLY;
    pflags&=~HAM_DISABLE_FREELIST_FLUSH;
    pflags&=~HAM_ENABLE_RECOVERY;
    pflags&=~HAM_AUTO_RECOVERY;
    pflags&=~HAM_ENABLE_TRANSACTIONS;
    pflags&=~DB_USE_MMAP;
    db_set_rt_flags(db, flags);

    /* 
     * if there's no header page, allocate one
     */
    if (!db_get_header_page(db)) {
        page=page_new(db);
        if (!page) {
            (void)ham_close(db, 0);
            return (db_get_error(db));
        }
        st=page_alloc(page, pagesize);
        if (st) {
            page_delete(page);
            (void)ham_close(db, 0);
            return (st);
        }
        memset(page_get_pers(page), 0, pagesize);
        page_set_type(page, PAGE_TYPE_HEADER);
        if (db_get_env(db))
            env_set_header_page(db_get_env(db), page);
        else
            db_set_header_page(db, page);

        /* initialize the header */
        db_set_magic(db, 'H', 'A', 'M', '\0');
        db_set_version(db, HAM_VERSION_MAJ, HAM_VERSION_MIN, 
                HAM_VERSION_REV, 0);
        db_set_serialno(db, HAM_SERIALNO);
        db_set_error(db, HAM_SUCCESS);
        db_set_pagesize(db, pagesize);
        if (db_get_env(db)) {
            ham_assert(env_get_max_databases(db_get_env(db)) > 0, (0));
            db_set_max_databases(db, env_get_max_databases(db_get_env(db)));
        }
        else {
            db_set_max_databases(db, DB_MAX_INDICES);
        }

        page_set_dirty(page);
    }
    /*
     * otherwise, if a header page already exists (which means that we're
     * in an environment), we transfer the ownership of the header 
     * page to this database
     */
    else {
        page_set_owner(db_get_header_page(db), db);
    }

    /*
     * already done? When a new environment is created, a dummy database
     * is created only to write the header page. In this case, we can
     * return now
     */
    if (dbname==DUMMY_DATABASE_NAME)
        return (0);

    /*
     * check if this database name is unique
     */
    ham_assert(db_get_max_databases(db) > 0, (0));
    for (i=0; i<db_get_max_databases(db); i++) {
        ham_u16_t name = index_get_dbname(db_get_indexdata_ptr(db, i));
        if (name==dbname) {
            (void)ham_close(db, 0);
            return (db_set_error(db, HAM_DATABASE_ALREADY_EXISTS));
        }
    }

    /*
     * find a free slot in the indexdata array and store the 
     * database name
     */
    ham_assert(db_get_max_databases(db) > 0, (0));
    for (i=0; i<db_get_max_databases(db); i++) {
        ham_u16_t name = index_get_dbname(db_get_indexdata_ptr(db, i));
        if (!name) {
            index_set_dbname(db_get_indexdata_ptr(db, i), dbname);
            db_set_indexdata_offset(db, i);
            break;
        }
    }
    if (i==db_get_max_databases(db)) {
        (void)ham_close(db, 0);
        return (db_set_error(db, HAM_LIMITS_REACHED));
    }

    /* 
     * initialize the cache
     */
    if (!db_get_cache(db)) {
        ham_size_t ps=db_get_pagesize(db);
        if (cachesize==0)
            cachesize=HAM_DEFAULT_CACHESIZE;
        cache=cache_new(db, (cachesize+ps-1)/ps);
        if (!cache) {
            (void)ham_close(db, 0);
            return (db_get_error(db));
        }
        if (db_get_env(db))
            env_set_cache(db_get_env(db), cache);
        else
            db_set_cache(db, cache);
    }

    /* 
     * create the backend
     */
    backend=db_create_backend(db, flags);
    if (!backend) {
        (void)ham_close(db, 0);
        return (db_set_error(db, HAM_NOT_INITIALIZED));
    }

    /* 
     * store the backend in the database
     */
    db_set_backend(db, backend);

    /* 
     * initialize the backend
     */
    st=backend->_fun_create(backend, keysize, pflags);
    if (st) {
        (void)ham_close(db, 0);
        db_set_error(db, st);
        return (st);
    }

    /*
     * set the default key compare functions
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        ham_set_compare_func(db, db_default_recno_compare);
    }
    else {
        ham_set_compare_func(db, db_default_compare);
        ham_set_prefix_compare_func(db, db_default_prefix_compare);
    }
    db_set_dirty(db, HAM_TRUE);

    return (HAM_SUCCESS);
}


static void 
nil_param_values(ham_parameter_t *param)
{
    for (; param->name; param++) 
    {
        param->value = 0;
    }
}

HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_get_parameters(ham_env_t *env, ham_parameter_t *param)
{
    ham_u32_t flags;
    ham_u32_t pagesize;
    ham_u16_t keysize;
    ham_size_t keycount;
    ham_u32_t cachesize;
    ham_u32_t max_databases;
    const char *filename;
    ham_u32_t file_mode;
    ham_cache_t *cache;

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    if (!param) {
        ham_trace(("parameter 'param' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    nil_param_values(param);

    if (!(env_get_rt_flags(env) & HAM_IN_MEMORY_DB)) {
        ham_assert(env_get_header_page(env), (0));
    }

    flags = env_get_rt_flags(env);
    pagesize = env_get_pagesize(env);
    keysize = env_get_keysize(env);
    cachesize = env_get_cachesize(env);
    max_databases = env_get_max_databases(env);
    file_mode = env_get_file_mode(env);
    filename = env_get_filename(env);

    if (!keysize) 
    {
        if (flags & HAM_RECORD_NUMBER)
            keysize = sizeof(ham_u64_t);
        else
            keysize = DB_CHUNKSIZE - (db_get_int_key_header_size());
    }

    cache = env_get_cache(env);
    if (cache) {
        ham_size_t max_elements = cache_get_max_elements(cache);
        cachesize = env_get_pagesize(env) * max_elements;
    }
    if (cachesize < env_get_cachesize(env))
        cachesize = env_get_cachesize(env);
    if (!cachesize)
        cachesize = HAM_DEFAULT_CACHESIZE;

    /* approximation of my_calc_maxkeys(); erring on the safe side */
    keycount = (pagesize - 64 /* (28 + 13) */ ) / (keysize + 11);

    for (; param->name; param++) 
    {
        switch (param->name) 
        {
        case HAM_PARAM_CACHESIZE:
            param->value = cachesize;
            break;
        case HAM_PARAM_KEYSIZE:
            param->value = keysize;
            break;
        case HAM_PARAM_PAGESIZE:
            param->value = pagesize;
            break;
        case HAM_PARAM_MAX_ENV_DATABASES:
            param->value = max_databases;
            break;
        case HAM_PARAM_DBNAME:
            break;
        case HAM_PARAM_GET_FLAGS:
            param->value = flags;
            break;
        case HAM_PARAM_GET_FILEMODE:
            param->value = file_mode;
            break;
        case HAM_PARAM_GET_FILENAME:
            param->value = (ham_u64_t)filename;
            break;
        default:
            break;
        }
    }
    return (HAM_SUCCESS);
}


HAM_EXPORT ham_status_t HAM_CALLCONV
ham_get_parameters(ham_db_t *db, ham_parameter_t *param)
{
    ham_env_t *env;
    ham_u32_t flags;
    ham_u32_t pagesize;
    ham_u16_t keysize;
    ham_size_t keycount;
    ham_u32_t cachesize = 0;
    ham_u32_t max_databases;
    ham_cache_t *cache;
    ham_backend_t *be;
    ham_u16_t dbname = 0;
    ham_status_t st;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    if (!param) {
        ham_trace(("parameter 'param' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    nil_param_values(param);

    env = db_get_env(db);
    if (env)
    {
        ham_u16_t indexdata_offset;

        st = ham_env_get_parameters(env, param);
        if (st)
            return st;

        indexdata_offset = db_get_indexdata_offset(db);
        ham_assert(db_get_max_databases(db) > 0, (0));
        if (indexdata_offset < db_get_max_databases(db))
        {
            dbname = index_get_dbname(db_get_indexdata_ptr(db, 
                            indexdata_offset));
        }
    }

    flags = db_get_rt_flags(db);
    pagesize = 0;
    max_databases = 0;
    if (db_get_header_page(db))
    {
        pagesize = db_get_pagesize(db);
        ham_assert(db_get_max_databases(db) > 0, (0));
        max_databases = db_get_max_databases(db);
    }
    if (!pagesize)
    {
        pagesize=os_get_pagesize();
    }
    if (!max_databases)
    {
        max_databases = DB_MAX_INDICES;
    }
    keysize = 0;
    if (db_get_backend(db))
    {
        keysize = db_get_keysize(db);
    }
    if (!keysize)
    {
        if (flags & HAM_RECORD_NUMBER)
            keysize = sizeof(ham_u64_t);
        else
            keysize = DB_CHUNKSIZE - (db_get_int_key_header_size());
    }

    if (env)
        cache = env_get_cache(env);
    else
        cache = db_get_cache(db);
    if (cache) {
        ham_size_t max_elements = cache_get_max_elements(cache);
        cachesize = db_get_pagesize(db) * max_elements;
    }
    if (env && cachesize < env_get_cachesize(env))
        cachesize = env_get_cachesize(env);
    if (!cachesize)
        cachesize = HAM_DEFAULT_CACHESIZE;

    keycount = 0;
    be=db_get_backend(db);
    if (be && be->_fun_calc_keycount)
    {
        st = be->_fun_calc_keycount(be, &keycount, keysize);
    }
    if (!keycount)
    {
        /* approximation of my_calc_maxkeys(); erring on the safe side */
        keycount = (pagesize - 64 /* (28 + 13) */ ) / (keysize + 11);
    }

    for (; param->name; param++) 
    {
        switch (param->name) 
        {
        case HAM_PARAM_CACHESIZE:
            param->value = cachesize;
            break;
        case HAM_PARAM_KEYSIZE:
            param->value = keysize;
            break;
        case HAM_PARAM_PAGESIZE:
            param->value = pagesize;
            break;
        case HAM_PARAM_MAX_ENV_DATABASES:
            /* == DB_MAX_INDICES for db, when not in an env */
            if (!env)
                param->value = max_databases;
            break;
        case HAM_PARAM_DBNAME:
            param->value = dbname;
            break;
        case HAM_PARAM_GET_FLAGS:
            param->value = flags;
            break;
        case HAM_PARAM_GET_KEYS_PER_PAGE:
            param->value = keycount;
            break;
        default:
            break;
        }
    }

    return (HAM_SUCCESS);
}


ham_status_t HAM_CALLCONV
ham_get_error(ham_db_t *db)
{
    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (0);
    }

    return (db_get_error(db));
}

ham_status_t HAM_CALLCONV
ham_set_prefix_compare_func(ham_db_t *db, ham_prefix_compare_func_t foo)
{
    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    db_set_error(db, 0);
    db_set_prefix_compare_func(db, foo);

    return (HAM_SUCCESS);
}

ham_status_t HAM_CALLCONV
ham_set_compare_func(ham_db_t *db, ham_compare_func_t foo)
{
    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    db_set_error(db, 0);
    db_set_compare_func(db, foo ? foo : db_default_compare);

    return (HAM_SUCCESS);
}

#ifndef HAM_DISABLE_ENCRYPTION
static ham_status_t 
__aes_before_write_cb(ham_env_t *env, ham_file_filter_t *filter, 
        ham_u8_t *page_data, ham_size_t page_size)
{
    ham_size_t i, blocks=page_size/16;

    for (i=0; i<blocks; i++) {
        aes_encrypt(&page_data[i*16], (ham_u8_t *)filter->userdata, 
                &page_data[i*16]);
    }

    return (HAM_SUCCESS);
}

static ham_status_t
__aes_after_read_cb(ham_env_t *env, ham_file_filter_t *filter, 
        ham_u8_t *page_data, ham_size_t page_size)
{
    ham_size_t i, blocks=page_size/16;

    ham_assert(page_size%16==0, ("bogus page size"));

    for (i=0; i<blocks; i++) {
        aes_decrypt(&page_data[i*16], (ham_u8_t *)filter->userdata, 
                &page_data[i*16]);
    }

    return (HAM_SUCCESS);
}

static void
__aes_close_cb(ham_env_t *env, ham_file_filter_t *filter)
{
    mem_allocator_t *alloc=env_get_allocator(env);

    if (filter) {
        if (filter->userdata)
            allocator_free(alloc, filter->userdata);
        allocator_free(alloc, filter);
    }
}
#endif /* !HAM_DISABLE_ENCRYPTION */

ham_status_t HAM_CALLCONV
ham_env_enable_encryption(ham_env_t *env, ham_u8_t key[16], ham_u32_t flags)
{
#ifndef HAM_DISABLE_ENCRYPTION
    ham_file_filter_t *filter;
    mem_allocator_t *alloc;
    ham_u8_t buffer[128];
    ham_device_t *device;
    ham_status_t st;
    ham_db_t *db=0;

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (env_get_list(env)) {
        ham_trace(("cannot enable encryption if databases are already open"));
        return (HAM_DATABASE_ALREADY_OPEN);
    }
    if (env_get_rt_flags(env)&HAM_IN_MEMORY_DB)
        return (0);

    device=env_get_device(env);

    alloc=env_get_allocator(env);

    /*
     * make sure that we don't already have AES filtering
     */
    filter=env_get_file_filter(env);
    while (filter) {
        if (filter->before_write_cb==__aes_before_write_cb)
            return (HAM_ALREADY_INITIALIZED);
        filter=filter->_next;
    }

    filter=(ham_file_filter_t *)allocator_alloc(alloc, sizeof(*filter));
    if (!filter)
        return (HAM_OUT_OF_MEMORY);
    memset(filter, 0, sizeof(*filter));

    filter->userdata=allocator_alloc(alloc, 256);
    if (!filter->userdata) {
        allocator_free(alloc, filter);
        return (HAM_OUT_OF_MEMORY);
    }

    /*
     * need a temporary database handle to read from the device
     */
    st=ham_new(&db);
    if (st)
        return (st);
    st=ham_env_open_db(env, db, FIRST_DATABASE_NAME, 0, 0);
    if (st) {
        ham_delete(db);
        db=0;
    }

    aes_expand_key(key, filter->userdata);
    filter->before_write_cb=__aes_before_write_cb;
    filter->after_read_cb=__aes_after_read_cb;
    filter->close_cb=__aes_close_cb;

    /*
     * if the database file already exists (i.e. if it's larger than
     * one page): try to read the header of the next page and decrypt
     * it; if it's garbage, the key is wrong and we return an error
     */
    if (db) {
        struct page_union_header_t *uh;

        st=device->read(db, device, db_get_pagesize(db),
                buffer, sizeof(buffer));
        if (st==0) {
            st=__aes_after_read_cb(env, filter, buffer, sizeof(buffer));
            if (st)
                goto bail;
            uh=(struct page_union_header_t *)buffer;
            if (uh->_reserved1 || uh->_reserved2) {
                st=HAM_ACCESS_DENIED;
                goto bail;
            }
        }
    }
    else
        st=0;

bail:
    if (db) {
        ham_close(db, 0);
        ham_delete(db);
    }

    if (st) {
        __aes_close_cb(env, filter);
        return (st);
    }

    return (ham_env_add_file_filter(env, filter));
#else /* !HAM_DISABLE_ENCRYPTION */
    ham_trace(("hamsterdb was compiled without support for AES encryption"));
    //if (db)
    //    return (db_set_error(db, HAM_NOT_IMPLEMENTED));
    //else
        return (HAM_NOT_IMPLEMENTED);
#endif
}

#ifndef HAM_DISABLE_COMPRESSION
static ham_status_t 
__zlib_before_insert_cb(ham_db_t *db, 
        ham_record_filter_t *filter, ham_record_t *record)
{
    ham_u8_t *dest;
    unsigned long newsize=0;
    ham_u32_t level=*(ham_u32_t *)filter->userdata;
    int zret;

    if (!record->size)
        return (0);

    /* 
     * we work in a temporary copy of the original data 
     *
     * the first 4 bytes in the record are used for storing the original,
     * uncompressed size; this makes the decompression easier
     */
    do {
        if (!newsize)
            newsize=compressBound(record->size)+sizeof(ham_u32_t);
        else
            newsize+=newsize/4;

        dest=ham_mem_alloc(db, newsize);
        if (!dest)
            return (db_set_error(db, HAM_OUT_OF_MEMORY));

        newsize-=sizeof(ham_u32_t);
        zret=compress2(dest+sizeof(ham_u32_t), &newsize,
                record->data, record->size, level);
    } while (zret==Z_BUF_ERROR);

    newsize+=sizeof(ham_u32_t);
    *(ham_u32_t *)dest=ham_h2db32(record->size);

    if (zret==Z_MEM_ERROR) {
        ham_mem_free(db, dest);
        return (db_set_error(db, HAM_OUT_OF_MEMORY));
    }

    if (zret!=Z_OK) {
        ham_mem_free(db, dest);
        return (db_set_error(db, HAM_INTERNAL_ERROR));
    }

    record->data=dest;
    record->size=(ham_size_t)newsize;
    return (0);
}

static ham_status_t 
__zlib_after_read_cb(ham_db_t *db, 
        ham_record_filter_t *filter, ham_record_t *record)
{
    ham_status_t st=0;
    ham_u8_t *src;
    ham_size_t srcsize=record->size;
    unsigned long newsize=record->size-sizeof(ham_u32_t);
    ham_u32_t origsize;
    int zret;

    if (!record->size)
        return (0);

    origsize=ham_db2h32(*(ham_u32_t *)record->data);

    /* don't allow HAM_RECORD_USER_ALLOC */
    if (record->flags&HAM_RECORD_USER_ALLOC) {
        ham_trace(("compression not allowed in combination with "
                    "HAM_RECORD_USER_ALLOC"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    src=ham_mem_alloc(db, newsize);
    if (!src)
        return (db_set_error(db, HAM_OUT_OF_MEMORY));

    memcpy(src, (char *)record->data+4, newsize);

    st=db_resize_allocdata(db, origsize);
    if (st) {
        ham_mem_free(db, src);
        return (st);
    }
    record->data=db_get_record_allocdata(db);
    newsize=origsize;

    zret=uncompress(record->data, &newsize, src, srcsize);
    if (zret==Z_MEM_ERROR)
        st=HAM_LIMITS_REACHED;
    if (zret==Z_DATA_ERROR)
        st=HAM_INTEGRITY_VIOLATED;
    else if (zret==Z_OK) {
        ham_assert(origsize==newsize, (""));
        st=0;
    }
    else
        st=HAM_INTERNAL_ERROR;

    if (!st)
        record->size=(ham_size_t)newsize;

    ham_mem_free(db, src);
    return (db_set_error(db, st));
}

static void 
__zlib_close_cb(ham_db_t *db, ham_record_filter_t *filter)

{
    if (filter) {
        if (filter->userdata)
            ham_mem_free(db, filter->userdata);
        ham_mem_free(db, filter);
    }
}
#endif /* !HAM_DISABLE_COMPRESSION */

ham_status_t HAM_CALLCONV
ham_enable_compression(ham_db_t *db, ham_u32_t level, ham_u32_t flags)
{
#ifndef HAM_DISABLE_COMPRESSION
    ham_record_filter_t *filter;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (level>9) {
        ham_trace(("parameter 'level' must be lower than or equal to 9"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (!level)
        level=6;

    db_set_error(db, 0);

    filter=(ham_record_filter_t *)ham_mem_calloc(db, sizeof(*filter));
    if (!filter)
        return (db_set_error(db, HAM_OUT_OF_MEMORY));

    filter->userdata=ham_mem_calloc(db, sizeof(level));
    if (!filter->userdata) {
        ham_mem_free(db, filter);
        return (db_set_error(db, HAM_OUT_OF_MEMORY));
    }

    *(ham_u32_t *)filter->userdata=level;
    filter->before_insert_cb=__zlib_before_insert_cb;
    filter->after_read_cb=__zlib_after_read_cb;
    filter->close_cb=__zlib_close_cb;

    return (ham_add_record_filter(db, filter));
#else /* !HAM_DISABLE_COMPRESSION */
    ham_trace(("hamsterdb was compiled without support for zlib compression"));
    if (db)
        return (db_set_error(db, HAM_NOT_IMPLEMENTED));
    else
        return (HAM_NOT_IMPLEMENTED);
#endif
}

ham_status_t HAM_CALLCONV
ham_find(ham_db_t *db, ham_txn_t *txn, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
    ham_txn_t local_txn;
    ham_status_t st;
    ham_backend_t *be;
    ham_offset_t recno=0;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!key) {
        ham_trace(("parameter 'key' must not be NULL"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (!record) {
        ham_trace(("parameter 'record' must not be NULL"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (!__prepare_key(key) || !__prepare_record(record))
        return (db_set_error(db, HAM_INV_PARAMETER));

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    /*
     * record number: make sure that we have a valid key structure
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        if (key->size!=sizeof(ham_u64_t) || !key->data) {
            ham_trace(("key->size must be 8, key->data must not be NULL"));
            return (db_set_error(db, HAM_INV_PARAMETER));
        }
        recno=*(ham_offset_t *)key->data;
        recno=ham_h2db64(recno);
        *(ham_offset_t *)key->data=recno;
    }

    be=db_get_backend(db);
    if (!be)
        return (db_set_error(db, HAM_NOT_INITIALIZED));

    if (!txn) {
        if ((st=txn_begin(&local_txn, db, HAM_TXN_READ_ONLY)))
            return (st);
    }

    /*
     * first look up the blob id, then fetch the blob
     */
    st=be->_fun_find(be, key, record, flags);

    if (st) {
        if (!txn)
            (void)txn_abort(&local_txn, 0);
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
            (void)txn_abort(&local_txn, 0);
        return (st);
    }

    if (!txn)
        return (txn_commit(&local_txn, 0));
    else
        return (st);
}


int HAM_CALLCONV
ham_key_get_approximate_match_type(ham_key_t *key)
{
    if (key && (key_get_flags(key) & KEY_IS_APPROXIMATE))
    {
        int rv = (key_get_flags(key) & KEY_IS_LT) ? -1 : +1;
        return (rv);
    }

    return (0);
}


ham_status_t HAM_CALLCONV
ham_insert(ham_db_t *db, ham_txn_t *txn, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
    ham_txn_t local_txn;
    ham_status_t st;
    ham_backend_t *be;
    ham_u64_t recno = 0;
    ham_record_t temprec;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!key) {
        ham_trace(("parameter 'key' must not be NULL"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (!record) {
        ham_trace(("parameter 'record' must not be NULL"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (!__prepare_key(key) || !__prepare_record(record))
        return (db_set_error(db, HAM_INV_PARAMETER));

    if (db_get_env(db))
        __prepare_db(db);

    be=db_get_backend(db);
    if (!be)
        return (db_set_error(db, HAM_NOT_INITIALIZED));
    if (db_get_rt_flags(db)&HAM_READ_ONLY) {
        ham_trace(("cannot insert to a read-only database"));
        return (db_set_error(db, HAM_DB_READ_ONLY));
    }
    if ((db_get_rt_flags(db)&HAM_DISABLE_VAR_KEYLEN) &&
            (key->size>db_get_keysize(db))) {
        ham_trace(("database does not support variable length keys"));
        return (db_set_error(db, HAM_INV_KEYSIZE));
    }
    if ((db_get_keysize(db)<sizeof(ham_offset_t)) &&
            (key->size>db_get_keysize(db))) {
        ham_trace(("database does not support variable length keys"));
        return (db_set_error(db, HAM_INV_KEYSIZE));
    }
    if ((flags&HAM_DUPLICATE) && (flags&HAM_OVERWRITE)) {
        ham_trace(("cannot combine HAM_DUPLICATE and HAM_OVERWRITE"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if ((flags&HAM_DUPLICATE) && !(db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES)) {
        ham_trace(("database does not support duplicate keys "
                    "(see HAM_ENABLE_DUPLICATES)"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if ((flags&HAM_DUPLICATE_INSERT_AFTER)
            || (flags&HAM_DUPLICATE_INSERT_BEFORE)
            || (flags&HAM_DUPLICATE_INSERT_LAST)
            || (flags&HAM_DUPLICATE_INSERT_FIRST)) {
        ham_trace(("function does not support flags HAM_DUPLICATE_INSERT_*; "
                    "see ham_cursor_insert"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    db_set_error(db, 0);

    if (!txn) {
        if ((st=txn_begin(&local_txn, db, 0)))
            return (st);
    }

    /*
     * record number: make sure that we have a valid key structure,
     * and lazy load the last used record number
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        if (flags&HAM_OVERWRITE) {
            if (key->size!=sizeof(ham_u64_t) || !key->data) {
                ham_trace(("key->size must be 8, key->data must not be NULL"));
                return (HAM_INV_PARAMETER);
            }
            recno=*(ham_u64_t *)key->data;
        }
        else {
            /*
             * get the record number (host endian) and increment it
             */
            recno=be_get_recno(be);
            recno++;
    
            /*
             * allocate memory for the key
             */
            if (key->flags&HAM_KEY_USER_ALLOC) {
                if (!key->data || key->size!=sizeof(ham_u64_t)) {
                    ham_trace(("key->size must be 8, key->data must not "
                                "be NULL"));
                    if (!txn)
                        (void)txn_abort(&local_txn, 0);
                    return (HAM_INV_PARAMETER);
                }
            }
            else {
                if (key->data || key->size) {
                    ham_trace(("key->size must be 0, key->data must be NULL"));
                    if (!txn)
                        (void)txn_abort(&local_txn, 0);
                    return (HAM_INV_PARAMETER);
                }
                /* 
                 * allocate memory for the key
                 */
                if (sizeof(ham_u64_t)>db_get_key_allocsize(db)) {
                    if (db_get_key_allocdata(db))
                        ham_mem_free(db, db_get_key_allocdata(db));
                    db_set_key_allocdata(db, 
                            ham_mem_alloc(db, sizeof(ham_u64_t)));
                    if (!db_get_key_allocdata(db)) {
                        if (!txn)
                            (void)txn_abort(&local_txn, 0);
                        db_set_key_allocsize(db, 0);
                        return (db_set_error(db, HAM_OUT_OF_MEMORY));
                    }
                    else {
                        db_set_key_allocsize(db, sizeof(ham_u64_t));
                    }
                }
                else
                    db_set_key_allocsize(db, sizeof(ham_u64_t));

                key->data=db_get_key_allocdata(db);
            }
        }

        /*
         * store it in db endian
         */
        recno=ham_h2db64(recno);
        memcpy(key->data, &recno, sizeof(ham_u64_t));
        key->size=sizeof(ham_u64_t);
    }

    /*
     * run the record-level filters on a temporary record structure - we
     * don't want to mess up the original structure
     */
    temprec=*record;
    st=__record_filters_before_insert(db, &temprec);

    /*
     * store the index entry; the backend will store the blob
     */
    if (!st)
        st=be->_fun_insert(be, key, &temprec, flags);

    if (temprec.data!=record->data)
        ham_mem_free(db, temprec.data);

    if (st) {
        if (!txn)
            (void)txn_abort(&local_txn, 0);

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
            be_set_dirty(be, 1);
            db_set_dirty(db, 1);
        }
    }

    if (!txn)
        return (txn_commit(&local_txn, 0));
    else
        return (st);
}

ham_status_t HAM_CALLCONV
ham_erase(ham_db_t *db, ham_txn_t *txn, ham_key_t *key, ham_u32_t flags)
{
    ham_txn_t local_txn;
    ham_status_t st;
    ham_backend_t *be;
    ham_offset_t recno=0;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!key) {
        ham_trace(("parameter 'key' must not be NULL"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (!__prepare_key(key))
        return (db_set_error(db, HAM_INV_PARAMETER));

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    be=db_get_backend(db);
    if (!be)
        return (db_set_error(db, HAM_NOT_INITIALIZED));
    if (db_get_rt_flags(db)&HAM_READ_ONLY) {
        ham_trace(("cannot erase from a read-only database"));
        return (db_set_error(db, HAM_DB_READ_ONLY));
    }

    /*
     * record number: make sure that we have a valid key structure
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        if (key->size!=sizeof(ham_u64_t) || !key->data) {
            ham_trace(("key->size must be 8, key->data must not be NULL"));
            return (db_set_error(db, HAM_INV_PARAMETER));
        }
        recno=*(ham_offset_t *)key->data;
        recno=ham_h2db64(recno);
        *(ham_offset_t *)key->data=recno;
    }

    if (!txn) {
        if ((st=txn_begin(&local_txn, db, 0)))
            return (st);
    }

    /*
     * get rid of the entry
     */
    st=be->_fun_erase(be, key, flags);

    if (st) {
        if (!txn)
            (void)txn_abort(&local_txn, 0);
        return (st);
    }

    /*
     * record number: re-translate the number to host endian
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        *(ham_offset_t *)key->data=ham_db2h64(recno);
    }

    if (!txn)
        return (txn_commit(&local_txn, 0));
    else
        return (st);
}

ham_status_t HAM_CALLCONV
ham_check_integrity(ham_db_t *db, ham_txn_t *txn)
{
#ifdef HAM_ENABLE_INTERNAL
    ham_txn_t local_txn;
    ham_status_t st;
    ham_backend_t *be;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    /*
     * check the cache integrity
     */
    st=cache_check_integrity(db_get_cache(db));
    if (st)
        return (st);

    be=db_get_backend(db);
    if (!be)
        return (db_set_error(db, HAM_NOT_INITIALIZED));
    if (!txn) {
        if ((st=txn_begin(&local_txn, db, HAM_TXN_READ_ONLY)))
            return (st);
    }

    /*
     * call the backend function
     */
    st=be->_fun_check_integrity(be);

    if (st) {
        if (!txn)
            (void)txn_abort(&local_txn, 0);
        return (st);
    }

    if (!txn)
        return (txn_commit(&local_txn, 0));
    else
        return (st);
#else /* !HAM_ENABLE_INTERNAL */
    ham_trace(("hamsterdb was compiled without support for internal "
                "functions"));
    return (HAM_NOT_IMPLEMENTED);
#endif
}


ham_status_t HAM_CALLCONV
ham_calc_maxkeys_per_page(ham_db_t *db, ham_size_t *keycount, ham_u16_t keysize)
{
    ham_status_t st;
    ham_backend_t *be;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    if (!keycount) {
        ham_trace(("parameter 'keycount' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    *keycount = 0;

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    be=db_get_backend(db);
    if (!be)
        return (db_set_error(db, HAM_NOT_INITIALIZED));
    if (!be->_fun_calc_keycount)
    {
        ham_trace(("hamsterdb was compiled without support for internal "
                    "functions"));
        return (HAM_NOT_IMPLEMENTED);
    }

    /*
     * call the backend function
     */
    st=be->_fun_calc_keycount(be, keycount, keysize);

    return (st);
}


ham_status_t HAM_CALLCONV
ham_flush(ham_db_t *db, ham_u32_t flags)
{
    ham_status_t st;

    (void)flags;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    /*
     * never flush an in-memory-database
     */
    if (db_get_rt_flags(db)&HAM_IN_MEMORY_DB)
        return (0);

    /*
     * flush the backend
     */
    st=db_get_backend(db)->_fun_flush(db_get_backend(db));
    if (st)
        return (db_set_error(db, st));

    /*
     * update the header page, if necessary
     */
    if (db_is_dirty(db)) {
        st=page_flush(db_get_header_page(db));
        if (st)
            return (db_set_error(db, st));
    }

    st=db_flush_all(db, DB_FLUSH_NODELETE);
    if (st)
        return (db_set_error(db, st));

    st=db_get_device(db)->flush(db_get_device(db));
    if (st)
        return (db_set_error(db, st));

    return (HAM_SUCCESS);
}

ham_status_t HAM_CALLCONV
ham_close(ham_db_t *db, ham_u32_t flags)
{
    ham_status_t st=0;
    ham_backend_t *be;
    ham_bool_t noenv=HAM_FALSE;
    ham_db_t *newowner=0;
    ham_record_filter_t *record_head;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if ((flags&HAM_TXN_AUTO_ABORT) && (flags&HAM_TXN_AUTO_COMMIT)) {
        ham_trace(("invalid combination of flags"));
        return (HAM_INV_PARAMETER);
    }

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    /*
     * auto-cleanup cursors?
     */
    if (db_get_cursors(db)) {
        ham_bt_cursor_t *c=(ham_bt_cursor_t *)db_get_cursors(db);
        while (c) {
            ham_bt_cursor_t *next=(ham_bt_cursor_t *)cursor_get_next(c);
            if (flags&HAM_AUTO_CLEANUP)
                st=ham_cursor_close((ham_cursor_t *)c);
            else
                st=bt_cursor_close(c);
            if (st)
                return (st);
            c=next;
        }
        db_set_cursors(db, 0);
    }

    /*
     * if this database has no environment, or if it's the last
     * database in the environment: delete all environment-members
     */
    if (db_get_env(db)==0
            || env_get_list(db_get_env(db))==0
            || db_get_next(env_get_list(db_get_env(db)))==0)
        noenv=HAM_TRUE;

    /*
     * auto-abort (or commit) a pending transaction?
     */
    if (noenv && db_get_txn(db)) {
        if (flags&HAM_TXN_AUTO_COMMIT)
            st=ham_txn_commit(db_get_txn(db), 0);
        else
            st=ham_txn_abort(db_get_txn(db), 0);
        if (st)
            return (st);
        db_set_txn(db, 0);
    }

    /*
     * if we're in an environment: all pages, which have this page
     * as an owner, must transfer their ownership to the
     * next page!
     */
    if (db_get_env(db)) {
        ham_db_t *head=env_get_list(db_get_env(db));
        while (head) {
            if (head!=db) {
                newowner=head;
                break;
            }
            head=db_get_next(head);
        }
    }
    if (newowner) {
        ham_page_t *head=cache_get_totallist(db_get_cache(db)); 
        while (head) {
            if (page_get_owner(head)==db)
                page_set_owner(head, newowner);
            head=page_get_next(head, PAGE_LIST_CACHED);
        }
    }

    be=db_get_backend(db);

    /*
     * in-memory-database: free all allocated blobs
     */
    if (be && db_get_rt_flags(db)&HAM_IN_MEMORY_DB) {
        ham_txn_t txn;
        free_cb_context_t context;
        context.db=db;
        if (!txn_begin(&txn, db, 0)) {
            (void)be->_fun_enumerate(be, my_free_cb, &context);
            (void)txn_commit(&txn, 0);
        }
    }

    /*
     * free cached memory
     */
    (void)db_resize_allocdata(db, 0);
    if (db_get_key_allocdata(db)) {
        ham_mem_free(db, db_get_key_allocdata(db));
        db_set_key_allocdata(db, 0);
        db_set_key_allocsize(db, 0);
    }

    /*
     * flush the freelist
     */
    if (noenv) {
        st=freel_shutdown(db);
        if (st)
            return (st);
    }

    /*
     * flush all pages
     */
    if (noenv) {
        st=db_flush_all(db, 0);
        if (st)
            return (st);
    }

    /*
     * free the cache for extended keys
     */
    if (noenv && db_get_extkey_cache(db)) {
        extkey_cache_destroy(db_get_extkey_cache(db));
        if (db_get_env(db))
            env_set_extkey_cache(db_get_env(db), 0);
        else
            db_set_extkey_cache(db, 0);
    }

    /* close the backend */
    if (be) {
        st=be->_fun_close(db_get_backend(db));
        if (st)
            return (st);
        ham_mem_free(db, be);
        db_set_backend(db, 0);
    }

    /*
     * if we're not in read-only mode, and not an in-memory-database,
     * and the dirty-flag is true: flush the page-header to disk
     */
    if (db_get_header_page(db) && noenv &&
        !(db_get_rt_flags(db)&HAM_IN_MEMORY_DB) &&
        db_get_device(db) && db_get_device(db)->is_open(db_get_device(db)) &&
        (!(db_get_rt_flags(db)&HAM_READ_ONLY))) {
        /* flush the database header, if it's dirty */
        if (db_is_dirty(db)) {
            st=page_flush(db_get_header_page(db));
            if (st)
                return (db_set_error(db, st));
        }
    }

    /*
     * environment: move the ownership to another database.
     * it's possible that there's no other page, then set the 
     * ownerwhip to 0
     */
    if (db_get_env(db) && db_get_header_page(db)) {
        page_set_owner(db_get_header_page(db), newowner);
    }
    /*
     * otherwise (if there's no environment), free the page
     */
    else if (!db_get_env(db) && db_get_header_page(db)) {
        if (page_get_pers(db_get_header_page(db)))
            (void)page_free(db_get_header_page(db));
        (void)page_delete(db_get_header_page(db));
        db_set_header_page(db, 0);
    }

    /* 
     * get rid of the cache 
     */
    if (noenv && db_get_cache(db)) {
        cache_delete(db, db_get_cache(db));
        if (db_get_env(db))
            env_set_cache(db_get_env(db), 0);
        else
            db_set_cache(db, 0);
    }

    /* 
     * close the device, but not if we're in an environment
     */
    if (!db_get_env(db) && db_get_device(db)) {
        if (db_get_device(db)->is_open(db_get_device(db))) {
            (void)db_get_device(db)->flush(db_get_device(db));
            (void)db_get_device(db)->close(db_get_device(db));
        }
        (void)db_get_device(db)->destroy(db_get_device(db));
        db_set_device(db, 0);
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
     * if we're not in an environment: close the log
     */
    if (!db_get_env(db) && db_get_log(db)) {
        (void)ham_log_close(db_get_log(db), (flags&HAM_DONT_CLEAR_LOG));
        db_set_log(db, 0);
    }

    /*
     * remove this database from the environment
     */
    if (db_get_env(db)) {
        ham_db_t *prev=0, *head=env_get_list(db_get_env(db));
        while (head) {
            if (head==db) {
                if (!prev)
                    env_set_list(db_get_env(db), db_get_next(db));
                else
                    db_set_next(prev, db_get_next(db));
                break;
            }
            prev=head;
            head=db_get_next(head);
        }
        db_set_env(db, 0);
    }

    return (HAM_SUCCESS);
}

ham_status_t HAM_CALLCONV
ham_cursor_create(ham_db_t *db, ham_txn_t *txn, ham_u32_t flags,
        ham_cursor_t **cursor)
{
    ham_status_t st;
    
    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!cursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    st=bt_cursor_create(db, txn, flags, (ham_bt_cursor_t **)cursor);
    if (st)
        return (db_set_error(db, st));

    if (txn)
        txn_set_cursor_refcount(txn, txn_get_cursor_refcount(txn)+1);
    return (0);
}

ham_status_t HAM_CALLCONV
ham_cursor_clone(ham_cursor_t *src, ham_cursor_t **dest)
{
    ham_status_t st;
    ham_txn_t local_txn;
    ham_db_t *db;

    if (!src) {
        ham_trace(("parameter 'src' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!dest) {
        ham_trace(("parameter 'dest' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    db=cursor_get_db(src);

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    if (!cursor_get_txn(src)) {
        if ((st=txn_begin(&local_txn, db, HAM_TXN_READ_ONLY)))
            return (st);
    }

    st=src->_fun_clone(src, dest);
    //st=bt_cursor_clone((ham_bt_cursor_t *)src, (ham_bt_cursor_t **)dest);
    if (st) {
        if (!cursor_get_txn(src))
            (void)txn_abort(&local_txn, 0);
        return (st);
    }

    if (cursor_get_txn(src))
        txn_set_cursor_refcount(cursor_get_txn(src), 
                txn_get_cursor_refcount(cursor_get_txn(src))+1);

    if (!cursor_get_txn(src))
        return (txn_commit(&local_txn, 0));
    else
        return (0);
}

ham_status_t HAM_CALLCONV
ham_cursor_overwrite(ham_cursor_t *cursor, ham_record_t *record,
            ham_u32_t flags)
{
    ham_status_t st;
    ham_txn_t local_txn;
    ham_db_t *db;
    ham_record_t temprec;

    if (!cursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    db=cursor_get_db(cursor);

    if (flags) {
        ham_trace(("function does not support a non-zero flags value; "
                    "see ham_cursor_insert for an alternative then"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    if (!record) {
        ham_trace(("parameter 'record' must not be NULL"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (!__prepare_record(record))
        return (db_set_error(db, HAM_INV_PARAMETER));

    if (db_get_rt_flags(cursor_get_db(cursor))&HAM_READ_ONLY) {
        ham_trace(("cannot overwrite in a read-only database"));
        return (db_set_error(db, HAM_DB_READ_ONLY));
    }

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    if (!cursor_get_txn(cursor)) {
        if ((st=txn_begin(&local_txn, db, 0)))
            return (st);
    }

    /*
     * run the record-level filters on a temporary record structure - we
     * don't want to mess up the original structure
     */
    temprec=*record;
    st=__record_filters_before_insert(db, &temprec);
    if (st) {
        if (!cursor_get_txn(cursor))
            (void)txn_abort(&local_txn, 0);
        return (st);
    }

    st=cursor->_fun_overwrite(cursor, &temprec, flags);

    if (temprec.data!=record->data)
        ham_mem_free(db, temprec.data);

    if (st) {
        if (!cursor_get_txn(cursor))
            (void)txn_abort(&local_txn, 0);
        return (st);
    }

    if (!cursor_get_txn(cursor))
        return (txn_commit(&local_txn, 0));
    else
        return (st);
}

ham_status_t HAM_CALLCONV
ham_cursor_move(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    ham_db_t *db;
    ham_txn_t local_txn;

    if (!cursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    db=cursor_get_db(cursor);

    if ((flags&HAM_ONLY_DUPLICATES) && (flags&HAM_SKIP_DUPLICATES)) {
        ham_trace(("combination of HAM_ONLY_DUPLICATES and "
                    "HAM_SKIP_DUPLICATES not allowed"));
        return (HAM_INV_PARAMETER);
    }
    if (key && !__prepare_key(key))
        return (db_set_error(db, HAM_INV_PARAMETER));
    if (record && !__prepare_record(record))
        return (db_set_error(db, HAM_INV_PARAMETER));

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    if (!cursor_get_txn(cursor)) {
        if ((st=txn_begin(&local_txn, db, HAM_TXN_READ_ONLY)))
            return (st);
    }

    st=cursor->_fun_move(cursor, key, record, flags);
    if (st) {
        if (!cursor_get_txn(cursor))
            (void)txn_abort(&local_txn, 0);
        return (st);
    }

    /*
     * run the record-level filters
     */
    if (record) {
        st=__record_filters_after_find(db, record);
        if (st) {
            if (!cursor_get_txn(cursor))
                (void)txn_abort(&local_txn, 0);
            return (st);
        }
    }

    if (!cursor_get_txn(cursor))
        return (txn_commit(&local_txn, 0));
    else
        return (st);
}

ham_status_t HAM_CALLCONV
ham_cursor_find(ham_cursor_t *cursor, ham_key_t *key, ham_u32_t flags)
{
    return (ham_cursor_find_ex(cursor, key, NULL, flags));
}

HAM_EXPORT ham_status_t HAM_CALLCONV
ham_cursor_find_ex(ham_cursor_t *cursor, ham_key_t *key, 
            ham_record_t *record, ham_u32_t flags)
{
    ham_offset_t recno=0;
    ham_status_t st;
    ham_db_t *db;
    ham_txn_t local_txn;

    if (!cursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    db=cursor_get_db(cursor);

    if (flags & ~(HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH | 
                HAM_FIND_EXACT_MATCH)) {
        ham_trace(("flag values besides any combination of "
                   "HAM_FIND_LT_MATCH, HAM_FIND_GT_MATCH and "
                   "HAM_FIND_EXACT_MATCH are not allowed"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    if (!cursor_get_txn(cursor)) {
        if ((st=txn_begin(&local_txn, db, 0)))
            return (st);
    }

    if (!key) {
        ham_trace(("parameter 'key' must not be NULL"));
        if (!cursor_get_txn(cursor))
            (void)txn_abort(&local_txn, 0);
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    if (!__prepare_key(key))
    {
        if (!cursor_get_txn(cursor))
            (void)txn_abort(&local_txn, 0);
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    /*
     * record number: make sure that we have a valid key structure,
     * and translate the record number to database endian
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        if (key->size!=sizeof(ham_u64_t) || !key->data) {
            ham_trace(("key->size must be 8, key->data must not be NULL"));
            if (!cursor_get_txn(cursor))
                (void)txn_abort(&local_txn, 0);
            return (db_set_error(db, HAM_INV_PARAMETER));
        }
        recno=*(ham_offset_t *)key->data;
        recno=ham_h2db64(recno);
        *(ham_offset_t *)key->data=recno;
    }

    st=cursor->_fun_find(cursor, key, record, flags);
    if (st)
    {
        if (!cursor_get_txn(cursor))
            (void)txn_abort(&local_txn, 0);
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
                (void)txn_abort(&local_txn, 0);
            return (st);
        }
    }

    if (!cursor_get_txn(cursor))
        return (txn_commit(&local_txn, 0));
    else
        return (0);
}

ham_status_t HAM_CALLCONV
ham_cursor_insert(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
    ham_db_t *db;
    ham_status_t st;
    ham_backend_t *be;
    ham_u64_t recno = 0;
    ham_record_t temprec;
    ham_txn_t local_txn;

    if (!cursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    db=cursor_get_db(cursor);

    be=db_get_backend(db);
    if (!be)
        return (db_set_error(db, HAM_NOT_INITIALIZED));

    if (!key) {
        ham_trace(("parameter 'key' must not be NULL"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (!record) {
        ham_trace(("parameter 'record' must not be NULL"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (!__prepare_key(key) || !__prepare_record(record))
        return (db_set_error(db, HAM_INV_PARAMETER));

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    if (db_get_rt_flags(db)&HAM_READ_ONLY) {
        ham_trace(("cannot insert to a read-only database"));
        return (db_set_error(db, HAM_DB_READ_ONLY));
    }
    if ((db_get_rt_flags(db)&HAM_DISABLE_VAR_KEYLEN) &&
            (key->size>db_get_keysize(db))) {
        ham_trace(("database does not support variable length keys"));
        return (db_set_error(db, HAM_INV_KEYSIZE));
    }
    if ((db_get_keysize(db)<sizeof(ham_offset_t)) &&
            (key->size>db_get_keysize(db))) {
        ham_trace(("database does not support variable length keys"));
        return (db_set_error(db, HAM_INV_KEYSIZE));
    }
    if ((flags&HAM_DUPLICATE) && (flags&HAM_OVERWRITE)) {
        ham_trace(("cannot combine HAM_DUPLICATE and HAM_OVERWRITE"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if ((flags&HAM_DUPLICATE) && !(db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES)) {
        ham_trace(("database does not support duplicate keys "
                    "(see HAM_ENABLE_DUPLICATES)"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    /*
     * set flag HAM_DUPLICATE if one of DUPLICATE_INSERT* is set
     */
    if ((flags&HAM_DUPLICATE_INSERT_AFTER)
            || (flags&HAM_DUPLICATE_INSERT_BEFORE)
            || (flags&HAM_DUPLICATE_INSERT_LAST)
            || (flags&HAM_DUPLICATE_INSERT_FIRST))
        flags|=HAM_DUPLICATE;

    /*
     * record number: make sure that we have a valid key structure,
     * and lazy load the last used record number
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        if (flags&HAM_OVERWRITE) {
            if (key->size!=sizeof(ham_u64_t) || !key->data) {
                ham_trace(("key->size must be 8, key->data must not be NULL"));
                return (HAM_INV_PARAMETER);
            }
            recno=*(ham_u64_t *)key->data;
        }
        else {
            /*
             * get the record number (host endian) and increment it
             */
            recno=be_get_recno(be);
            recno++;

            /*
             * allocate memory for the key
             */
            if (key->flags&HAM_KEY_USER_ALLOC) {
                if (!key->data || key->size!=sizeof(ham_u64_t)) {
                    ham_trace(("key->size must be 8, key->data must not "
                                "be NULL"));
                    return (HAM_INV_PARAMETER);
                }
            }
            else {
                if (key->data || key->size) {
                    ham_trace(("key->size must be 0, key->data must be NULL"));
                    return (HAM_INV_PARAMETER);
                }
                /* 
                 * allocate memory for the key
                 */
                if (sizeof(ham_u64_t)>db_get_key_allocsize(db)) {
                    if (db_get_key_allocdata(db))
                        ham_mem_free(db, db_get_key_allocdata(db));
                    db_set_key_allocdata(db, 
                            ham_mem_alloc(db, sizeof(ham_u64_t)));
                    if (!db_get_key_allocdata(db)) {
                        db_set_key_allocsize(db, 0);
                        return (db_set_error(db, HAM_OUT_OF_MEMORY));
                    }
                    else {
                        db_set_key_allocsize(db, sizeof(ham_u64_t));
                    }
                }
                else
                    db_set_key_allocsize(db, sizeof(ham_u64_t));
    
                key->data=db_get_key_allocdata(db);
            }
        }

        /*
         * store it in db endian
         */
        recno=ham_h2db64(recno);
        memcpy(key->data, &recno, sizeof(ham_u64_t));
        key->size=sizeof(ham_u64_t);
    }

    if (!cursor_get_txn(cursor)) {
        if ((st=txn_begin(&local_txn, db, 0)))
            return (st);
    }

    /*
     * run the record-level filters on a temporary record structure - we
     * don't want to mess up the original structure
     */
    temprec=*record;
    st=__record_filters_before_insert(db, &temprec);

    if (!st) {
        st=cursor->_fun_insert(cursor, key, &temprec, flags);
    }

    if (temprec.data!=record->data)
        ham_mem_free(db, temprec.data);

    if (st) {
        if (!cursor_get_txn(cursor))
            (void)txn_abort(&local_txn, 0);
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
            be_set_dirty(be, 1);
            db_set_dirty(db, 1);
        }
    }

    if (!cursor_get_txn(cursor))
        return (txn_commit(&local_txn, 0));
    else
        return (st);
}

ham_status_t HAM_CALLCONV
ham_cursor_erase(ham_cursor_t *cursor, ham_u32_t flags)
{
    ham_status_t st;
    ham_txn_t local_txn;
    ham_db_t *db;

    if (!cursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    if (db_get_env(cursor_get_db(cursor)))
        __prepare_db(cursor_get_db(cursor));

    db=cursor_get_db(cursor);
    db_set_error(db, 0);

    if (db_get_rt_flags(db)&HAM_READ_ONLY) {
        ham_trace(("cannot erase from a read-only database"));
        return (db_set_error(db, HAM_DB_READ_ONLY));
    }

    if (!cursor_get_txn(cursor)) {
        if ((st=txn_begin(&local_txn, db, 0)))
            return (st);
    }

    st=cursor->_fun_erase(cursor, flags);

    if (st) {
        if (!cursor_get_txn(cursor))
            (void)txn_abort(&local_txn, 0);
        return (st);
    }

    if (!cursor_get_txn(cursor))
        return (txn_commit(&local_txn, 0));
    else
        return (st);
}

ham_status_t HAM_CALLCONV
ham_cursor_get_duplicate_count(ham_cursor_t *cursor, 
        ham_size_t *count, ham_u32_t flags)
{
    ham_status_t st;
    ham_txn_t local_txn;

    if (!cursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!count) {
        ham_trace(("parameter 'count' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    *count=0;

    if (db_get_env(cursor_get_db(cursor)))
        __prepare_db(cursor_get_db(cursor));

    db_set_error(cursor_get_db(cursor), 0);

    if (!cursor_get_txn(cursor)) {
        if ((st=txn_begin(&local_txn, cursor_get_db(cursor), 
                        HAM_TXN_READ_ONLY)))
            return (st);
    }

    st=bt_cursor_get_duplicate_count((ham_bt_cursor_t *)cursor, count, flags);
    if (st) {
        if (!cursor_get_txn(cursor))
            (void)txn_abort(&local_txn, 0);
        return (st);
    }

    if (!cursor_get_txn(cursor))
        return (txn_commit(&local_txn, 0));
    else
        return (st);
}

ham_status_t HAM_CALLCONV
ham_cursor_close(ham_cursor_t *cursor)
{
    ham_status_t st;

    if (!cursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    if (db_get_env(cursor_get_db(cursor)))
        __prepare_db(cursor_get_db(cursor));

    db_set_error(cursor_get_db(cursor), 0);

    st=cursor->_fun_close(cursor);
    if (!st) {
        if (cursor_get_txn(cursor))
            txn_set_cursor_refcount(cursor_get_txn(cursor), 
                    txn_get_cursor_refcount(cursor_get_txn(cursor))-1);
        ham_mem_free(cursor_get_db(cursor), cursor);
    }

    return (st);
}

ham_status_t HAM_CALLCONV
ham_add_record_filter(ham_db_t *db, ham_record_filter_t *filter)
{
    ham_record_filter_t *head;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    if (!filter) {
        ham_trace(("parameter 'filter' must not be NULL"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    head=db_get_record_filter(db);

    /*
     * !!
     * add the filter at the end of all filters, then we can process them
     * later in the same order as the insertion
     */
    if (!head) {
        db_set_record_filter(db, filter);
    }
    else {
        while (head->_next)
            head=head->_next;

        filter->_prev=head;
        head->_next=filter;
    }

    return (0);
}

ham_status_t HAM_CALLCONV
ham_remove_record_filter(ham_db_t *db, ham_record_filter_t *filter)
{
    ham_record_filter_t *head, *prev;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    if (!filter) {
        ham_trace(("parameter 'filter' must not be NULL"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    head=db_get_record_filter(db);

    if (head==filter) {
        db_set_record_filter(db, head->_next);
        return 0;
    }

    do {
        prev=head;
        head=head->_next;
        if (!head)
            break;
        if (head==filter) {
            prev->_next=head->_next;
            if (head->_next)
                head->_next->_prev=prev;
            break;
        }
    } while(head);

    return (0);
}

ham_status_t HAM_CALLCONV
ham_env_set_device(ham_env_t *env, void *device)
{
    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!device) {
        ham_trace(("parameter 'device' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    if (env_get_device(env)) {
        ham_trace(("Environment already has a device object attached"));
        return (HAM_ALREADY_INITIALIZED);
    }

    env_set_device(env, device);
    return (0);
}

void HAM_CALLCONV
ham_set_context_data(ham_db_t *db, void *data)
{
    if (db)
        db_set_context_data(db, data);
}

void * HAM_CALLCONV
ham_get_context_data(ham_db_t *db)
{
    if (db)
        return (db_get_context_data(db));
    return (0);
}

ham_db_t * HAM_CALLCONV
ham_cursor_get_database(ham_cursor_t *cursor)
{
    if (cursor)
        return (cursor_get_db(cursor));
    else
        return (0);
}

ham_u32_t
ham_get_flags(ham_db_t *db)
{
    if (db)
        return (db_get_rt_flags(db));
    else
        return (0);
}
