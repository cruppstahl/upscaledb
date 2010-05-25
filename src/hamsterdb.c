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

#ifdef HAVE_MALLOC_H
#  include <malloc.h>
#else
#  include <stdlib.h>
#endif
#include <string.h>

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
#include "keys.h"
#include "log.h"
#include "mem.h"
#include "os.h"
#include "page.h"
#include "serial.h"
#include "statistics.h"
#include "txn.h"
#include "util.h"
#include "version.h"

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


/*
 * return true if the filename is for a local file
 */
static ham_bool_t
__filename_is_local(const char *filename)
{
    if (filename && strstr(filename, "http://")==filename)
        return (HAM_FALSE);
    return (HAM_TRUE);
}

static char *
my_strncat_ex(char *buf, size_t buflen, const char *interject, const char *src)
{
    if (!interject)
        interject = "|";
    if (!src)
        src = "???";
    if (buf && buflen > (*buf ? strlen(interject) : 0) + strlen(src))
    {
        if (*buf)
        {
            strcat(buf, interject);
        }
        strcat(buf, src);
    }
    if (buf && buflen)
    {
        buf[buflen - 1] = 0;
        return buf;
    }
    return "???";
}

static const char *
ham_create_flags2str(char *buf, size_t buflen, ham_u32_t flags)
{
    if (!buf || !buflen) 
    {
        buflen = 0;
        buf = NULL;
    }
    else 
    {
        buf[0] = 0;
    }

    if (flags & HAM_WRITE_THROUGH            )
    {
        flags &= ~HAM_WRITE_THROUGH            ;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_WRITE_THROUGH");
    }
    if (flags & HAM_READ_ONLY                )
    {
        flags &= ~HAM_READ_ONLY                ;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_READ_ONLY");
    }
    if (flags & HAM_USE_BTREE                )
    {
        flags &= ~HAM_USE_BTREE                ;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_USE_BTREE");
    }
    if (flags & HAM_DISABLE_VAR_KEYLEN       )
    {
        flags &= ~HAM_DISABLE_VAR_KEYLEN       ;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_DISABLE_VAR_KEYLEN");
    }
    if (flags & HAM_IN_MEMORY_DB             )
    {
        flags &= ~HAM_IN_MEMORY_DB             ;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_IN_MEMORY_DB");
    }
    if (flags & HAM_DISABLE_MMAP             )
    {
        flags &= ~HAM_DISABLE_MMAP             ;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_DISABLE_MMAP");
    }
    if (flags & HAM_CACHE_STRICT             )
    {
        flags &= ~HAM_CACHE_STRICT             ;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_CACHE_STRICT");
    }
    if (flags & HAM_DISABLE_FREELIST_FLUSH   )
    {
        flags &= ~HAM_DISABLE_FREELIST_FLUSH   ;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_DISABLE_FREELIST_FLUSH");
    }
    if (flags & HAM_LOCK_EXCLUSIVE           )
    {
        flags &= ~HAM_LOCK_EXCLUSIVE           ;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_LOCK_EXCLUSIVE");
    }
    if (flags & HAM_RECORD_NUMBER            )
    {
        flags &= ~HAM_RECORD_NUMBER            ;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_RECORD_NUMBER");
    }
    if (flags & HAM_ENABLE_DUPLICATES        )
    {
        flags &= ~HAM_ENABLE_DUPLICATES        ;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_ENABLE_DUPLICATES");
    }
    if (flags & HAM_SORT_DUPLICATES        )
    {
        flags &= ~HAM_SORT_DUPLICATES        ;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_SORT_DUPLICATES");
    }
    if (flags & HAM_ENABLE_RECOVERY          )
    {
        flags &= ~HAM_ENABLE_RECOVERY          ;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_ENABLE_RECOVERY");
    }
    if (flags & HAM_AUTO_RECOVERY            )
    {
        flags &= ~HAM_AUTO_RECOVERY            ;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_AUTO_RECOVERY");
    }
    if (flags & HAM_ENABLE_TRANSACTIONS      )
    {
        flags &= ~HAM_ENABLE_TRANSACTIONS      ;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_ENABLE_TRANSACTIONS");
    }
    if (flags & HAM_CACHE_UNLIMITED          )
    {
        flags &= ~HAM_CACHE_UNLIMITED          ;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_CACHE_UNLIMITED");
    }

    if (flags)
    {
        if (buf && buflen > 13 && buflen > strlen(buf) + 13 + 1 + 9)
        {
            util_snprintf(buf, buflen, "%sHAM_FLAGS(reserved: 0x%x)", 
                            (*buf ? "|" : ""), (unsigned int)flags);
        }
        else
        {
            buf = "???";
        }
    }
    return buf;
}

const char * HAM_CALLCONV
ham_param2str(char *buf, size_t buflen, ham_u32_t name)
{
    switch (name)
    {
    case HAM_PARAM_CACHESIZE          :
        return "HAM_PARAM_CACHESIZE";

    case HAM_PARAM_PAGESIZE           :
        return "HAM_PARAM_PAGESIZE";

    case HAM_PARAM_KEYSIZE            :
        return "HAM_PARAM_KEYSIZE";

    case HAM_PARAM_MAX_ENV_DATABASES  :
        return "HAM_PARAM_MAX_ENV_DATABASES";

    case HAM_PARAM_DATA_ACCESS_MODE   :
        return "HAM_PARAM_DATA_ACCESS_MODE";

    case HAM_PARAM_GET_FLAGS          :
        return "HAM_PARAM_GET_FLAGS";

    case HAM_PARAM_GET_DATA_ACCESS_MODE:
        return "HAM_PARAM_GET_DATA_ACCESS_MODE";

    case HAM_PARAM_GET_FILEMODE       :
        return "HAM_PARAM_GET_FILEMODE";

    case HAM_PARAM_GET_FILENAME       :
        return "HAM_PARAM_GET_FILENAME";

    case HAM_PARAM_GET_DATABASE_NAME  :
        return "HAM_PARAM_GET_DATABASE_NAME";

    case HAM_PARAM_GET_KEYS_PER_PAGE  :
        return "HAM_PARAM_GET_KEYS_PER_PAGE";

    case HAM_PARAM_GET_STATISTICS        :
        return "HAM_PARAM_GET_STATISTICS";

    default:
        if (buf && buflen > 13) {
            util_snprintf(buf, buflen, "HAM_PARAM(0x%x)", (unsigned int)name);
            return buf;
        }
        break;
    }

    return "???";
}

static ham_bool_t
__check_recovery_flags(ham_u32_t flags)
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
ham_txn_begin(ham_txn_t **txn, ham_db_t *db, ham_u32_t flags)
{
    ham_status_t st;
    ham_env_t *env; 

    if (!txn) {
        ham_trace(("parameter 'txn' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    *txn = NULL;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    env = db_get_env(db);
    if (!env) {
        ham_trace(("parameter 'db' must be linked to a valid (implicit or "
                   "explicit) environment"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    if (!(env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS)) {
        ham_trace(("transactions are disabled (see HAM_ENABLE_TRANSACTIONS)"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    *txn=(ham_txn_t *)allocator_alloc(env_get_allocator(env), sizeof(**txn));
    if (!(*txn))
        return (db_set_error(db, HAM_OUT_OF_MEMORY));

    st=txn_begin(*txn, env, flags);
    if (st) {
        allocator_free(env_get_allocator(env), *txn);
        *txn=0;
    }

    return (db_set_error(db, st));
}

ham_status_t
ham_txn_commit(ham_txn_t *txn, ham_u32_t flags)
{
    if (!txn) {
        ham_trace(("parameter 'txn' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    else
    {
        ham_status_t st=txn_commit(txn, flags);
        if (st==0) {
            ham_env_t *env = txn_get_env(txn);
            memset(txn, 0, sizeof(*txn));
            allocator_free(env_get_allocator(env), txn);
        }

        return (st);
    }
}

ham_status_t
ham_txn_abort(ham_txn_t *txn, ham_u32_t flags)
{
    if (!txn) {
        ham_trace(("parameter 'txn' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    else
    {
        ham_status_t st=txn_abort(txn, flags);
        if (st==0) {
            ham_env_t *env = txn_get_env(txn);
            memset(txn, 0, sizeof(*txn));
            allocator_free(env_get_allocator(env), txn);
        }

        return (st);
    }
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
        case HAM_FILTER_NOT_FOUND:
            return ("Record filter or file filter not found");
        case HAM_CURSOR_IS_NIL:
            return ("Cursor points to NIL");
        case HAM_DATABASE_NOT_FOUND:
            return ("Database not found");
        case HAM_DATABASE_ALREADY_EXISTS:
            return ("Database name already exists");
        case HAM_DATABASE_ALREADY_OPEN:
            return ("Database already open, or: Database handle "
                    "already initialized");
        case HAM_ENVIRONMENT_ALREADY_OPEN:
            return ("Environment already open, or: Environment handle "
                    "already initialized");
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
        case HAM_NETWORK_ERROR:
            return ("Remote I/O error/Network error");
        default:
            return ("Unknown error");
    }
}

/**
 * Prepares a @ref ham_key_t structure for returning key data in.
 * 
 * This function checks whether the @ref ham_key_t structure has been 
 * properly initialized by the user and resets all internal used elements.
 * 
 * @return HAM_TRUE when the @a key structure has been initialized correctly 
 * before.
 * 
 * @return HAM_FALSE when the @a key structure has @e not been initialized 
 * correctly before.
 */
static ham_bool_t
__prepare_key(ham_key_t *key)
{
    if (key->size && !key->data) {
        ham_trace(("key->size != 0, but key->data is NULL"));
        return HAM_FALSE;
    }
    if (key->flags!=0 && key->flags!=HAM_KEY_USER_ALLOC) {
        ham_trace(("invalid flag in key->flags"));
        return HAM_FALSE;
    }
    key->_flags=0;
    return HAM_TRUE;
}

/**
 * Prepares a @ref ham_record_t structure for returning record data in.
 * 
 * This function checks whether the @ref ham_record_t structure has been 
 * properly initialized by the user and resets all internal used elements.
 * 
 * @return HAM_TRUE when the @a record structure has been initialized 
 * correctly before.
 * 
 * @return HAM_FALSE when the @a record structure has @e not been 
 * initialized correctly before.
 */
static ham_bool_t
__prepare_record(ham_record_t *record)
{
    if (record->size && !record->data) {
        ham_trace(("record->size != 0, but record->data is NULL"));
        return HAM_FALSE;
    }
    if (record->flags&HAM_DIRECT_ACCESS)
        record->flags&=~HAM_DIRECT_ACCESS;
    if (record->flags!=0 && record->flags!=HAM_RECORD_USER_ALLOC) {
        ham_trace(("invalid flag in record->flags"));
        return HAM_FALSE;
    }
    record->_intflags=0;
    record->_rid=0;
    return HAM_TRUE;
}

ham_status_t 
__check_create_parameters(ham_env_t *env, ham_db_t *db, const char *filename, 
        ham_u32_t *pflags, const ham_parameter_t *param, 
        ham_size_t *ppagesize, ham_u16_t *pkeysize, 
        ham_size_t *pcachesize, ham_u16_t *pdbname,
        ham_u16_t *pmaxdbs, ham_u16_t *pdata_access_mode, ham_bool_t create)
{
    ham_size_t pagesize=0;
    ham_u16_t keysize=0;
    ham_u16_t dbname=HAM_DEFAULT_DATABASE_NAME;
    ham_size_t cachesize=0;
    ham_bool_t no_mmap=HAM_FALSE;
    ham_u16_t dbs=0;
    ham_u16_t dam=0;
    ham_u32_t flags = 0;
    ham_bool_t set_abs_max_dbs = HAM_FALSE;
    ham_device_t *device = NULL;
    ham_status_t st = 0;

    if (!env && db)
        env = db_get_env(db);

    if (pflags)
        flags = *pflags;
    else if (db)
        flags = db_get_rt_flags(db);
    else if (env)
        flags = env_get_rt_flags(env);

    if (pcachesize)
        cachesize = *pcachesize;
    if (pkeysize)
        keysize = *pkeysize;
    if (ppagesize)
        pagesize = *ppagesize;
    if (pdbname && *pdbname) 
        dbname = *pdbname;
    if (pdata_access_mode && *pdata_access_mode)
        dam = *pdata_access_mode;
    if (pmaxdbs && *pmaxdbs)
        dbs = *pmaxdbs;

    /* 
     * cannot open an in-memory-db 
     */
    if (!create && (flags & HAM_IN_MEMORY_DB)) {
        ham_trace(("cannot open an in-memory database"));
        return (HAM_INV_PARAMETER);
    }

    /*
     * creating a file in READ_ONLY mode? doesn't make sense
     */
    if (create && (flags & HAM_READ_ONLY)) {
        ham_trace(("cannot create a file in read-only mode"));
        return (HAM_INV_PARAMETER);
    }
    if (create && env && db && (env_get_rt_flags(env) & HAM_READ_ONLY)) {
        ham_trace(("cannot create database in read-only mode"));
        return (HAM_DB_READ_ONLY);
    }

    /*
     * HAM_ENABLE_DUPLICATES has to be specified in ham_create, not 
     * ham_open 
     */
    if (!create && (flags & HAM_ENABLE_DUPLICATES)) {
        ham_trace(("invalid flag HAM_ENABLE_DUPLICATES (only allowed when "
                "creating a database"));
        return (HAM_INV_PARAMETER);
    }

    /*
     * when creating a Database, HAM_SORT_DUPLICATES is only allowed in
     * combination with HAM_ENABLE_DUPLICATES
     */
    if (create && (flags & HAM_SORT_DUPLICATES)) {
        if (!(flags & HAM_ENABLE_DUPLICATES)) {
            ham_trace(("flag HAM_SORT_DUPLICATES only allowed in combination "
                        "with HAM_ENABLE_DUPLICATES"));
            return (HAM_INV_PARAMETER);
        }
    }

    /*
     * DB create: only a few flags are allowed
     */
    if (db && (flags & ~((!create ? HAM_READ_ONLY : 0)
                        |(create ? HAM_IN_MEMORY_DB : 0)
                        |(!env ? (HAM_WRITE_THROUGH 
                                |HAM_DISABLE_MMAP 
                                |HAM_DISABLE_FREELIST_FLUSH
                                |HAM_CACHE_UNLIMITED
                                |HAM_LOCK_EXCLUSIVE
                                |HAM_ENABLE_TRANSACTIONS
                                |HAM_ENABLE_RECOVERY) : 0)
                        |(!env && !create ? HAM_AUTO_RECOVERY : 0)
                        |HAM_CACHE_STRICT
                        |HAM_USE_BTREE
                        |HAM_DISABLE_VAR_KEYLEN
                        |HAM_RECORD_NUMBER
                        |HAM_SORT_DUPLICATES
                        |(create ? HAM_ENABLE_DUPLICATES : 0))))
    {
        char msgbuf[2048];
        ham_trace(("invalid flags specified: %s", 
                ham_create_flags2str(msgbuf, sizeof(msgbuf), 
                (flags & ~((!create ? HAM_READ_ONLY : 0)
                        |(create ? HAM_IN_MEMORY_DB : 0)
                        |(!env ? (HAM_WRITE_THROUGH 
                                |HAM_DISABLE_MMAP 
                                |HAM_DISABLE_FREELIST_FLUSH
                                |HAM_CACHE_UNLIMITED
                                |HAM_LOCK_EXCLUSIVE
                                |HAM_ENABLE_TRANSACTIONS
                                |HAM_ENABLE_RECOVERY) : 0)
                        |(!env && !create ? HAM_AUTO_RECOVERY : 0)
                        |HAM_CACHE_STRICT
                        |HAM_USE_BTREE
                        |HAM_DISABLE_VAR_KEYLEN
                        |HAM_RECORD_NUMBER
                        |(create ? HAM_ENABLE_DUPLICATES : 0))))));
        flags &= ((!create ? HAM_READ_ONLY : 0)
                        |(create ? HAM_IN_MEMORY_DB : 0)
                        |(!env ? (HAM_WRITE_THROUGH 
                                |HAM_DISABLE_MMAP 
                                |HAM_DISABLE_FREELIST_FLUSH
                                |HAM_CACHE_UNLIMITED
                                |HAM_LOCK_EXCLUSIVE
                                |HAM_ENABLE_TRANSACTIONS
                                |HAM_ENABLE_RECOVERY) : 0)
                        |(!env && !create ? HAM_AUTO_RECOVERY : 0)
                        |HAM_CACHE_STRICT
                        |HAM_USE_BTREE
                        |HAM_DISABLE_VAR_KEYLEN
                        |HAM_RECORD_NUMBER
                        |(create ? HAM_ENABLE_DUPLICATES : 0));
        return (HAM_INV_PARAMETER);
    }

    if (env)
        flags |= env_get_rt_flags(env);

    /* 
     * parse parameters 
     */
    if (param) {
        for (; param->name; param++) {
            switch (param->name) {
            case HAM_PARAM_CACHESIZE:
                if (pcachesize) {
                    cachesize=(ham_size_t)param->value;
                    if (cachesize > 0) {
                        if (env && env_get_cache(env)
                                && cachesize != env_get_cachesize(env)) {
                            ham_trace(("invalid parameter HAM_PARAM_CACHESIZE - "
                                       "it's illegal to specify a new "
                                       "cache size when the cache has already "
                                       "been initialized"));
                            return (HAM_INV_PARAMETER);
                        }
                    }
                }
                break;

            case HAM_PARAM_KEYSIZE:
                if (!create) {
                    ham_trace(("invalid parameter HAM_PARAM_KEYSIZE"));
                    return (HAM_INV_PARAMETER);
                }
                if (pkeysize) {
                    keysize=(ham_u16_t)param->value;
                    if (flags & HAM_RECORD_NUMBER) {
                        if (keysize > 0 && keysize < sizeof(ham_u64_t)) {
                            ham_trace(("invalid keysize %u - must be 8 for "
                                       "HAM_RECORD_NUMBER databases",
                                       (unsigned)keysize));
                            keysize = sizeof(ham_u64_t); 
                            return (HAM_INV_KEYSIZE);
                        }
                    }
                }
                break;
            case HAM_PARAM_PAGESIZE:
                if (ppagesize) {
                    if (param->value!=1024 && param->value%2048!=0) {
                        ham_trace(("invalid pagesize - must be 1024 or "
                                "a multiple of 2048"));
                        pagesize=0;
                        return (HAM_INV_PAGESIZE);
                    }
                    pagesize=(ham_size_t)param->value;
                    break;
                }
                goto default_case;

            case HAM_PARAM_DATA_ACCESS_MODE:
                /* not allowed for Environments, only for Databases */
                if (!db) {
                    ham_trace(("invalid parameter "
                               "HAM_PARAM_DATA_ACCESS_MODE"));
                    dam=0;
                    return (HAM_INV_PARAMETER);
                }
                if (param->value&HAM_DAM_ENFORCE_PRE110_FORMAT) {
                    ham_trace(("Data access mode HAM_DAM_ENFORCE_PRE110_FORMAT "
                                "must not be specified"));
                    return (HAM_INV_PARAMETER);
                }
                if (pdata_access_mode) { 
                    switch (param->value) {
                    case 0: /* ignore 0 */
                        break;
                    case HAM_DAM_SEQUENTIAL_INSERT:
                    case HAM_DAM_RANDOM_WRITE:
                        dam=(ham_u16_t)param->value;
                        break;
                    default:
                        ham_trace(("invalid value 0x%04x specified for "
                                "parameter HAM_PARAM_DATA_ACCESS_MODE", 
                                (unsigned)param->value));
                        return (HAM_INV_PARAMETER);
                    }
                    break;
                }
                goto default_case;

            case HAM_PARAM_MAX_ENV_DATABASES:
                if (pmaxdbs) {
                    if (param->value==0 || param->value >= HAM_DEFAULT_DATABASE_NAME) {
                        if (param->value==0) {
                            ham_trace(("invalid value %u for parameter "
                                       "HAM_PARAM_MAX_ENV_DATABASES",
                                       (unsigned)param->value));
                            return (HAM_INV_PARAMETER);
                        }
                    }
                    else {
                        dbs=(ham_u16_t)param->value;
                    }
                    break;
                }
                goto default_case;

            case HAM_PARAM_GET_DATABASE_NAME:
                if (pdbname) {
                    if (dbname == HAM_DEFAULT_DATABASE_NAME || dbname == HAM_FIRST_DATABASE_NAME) {
                        dbname=(ham_u16_t)param->value;

                        if (!dbname
                            || (dbname != HAM_FIRST_DATABASE_NAME 
                                && dbname != HAM_DUMMY_DATABASE_NAME 
                                && dbname > HAM_DEFAULT_DATABASE_NAME)) 
                        {
                            ham_trace(("parameter 'HAM_PARAM_GET_DATABASE_NAME' value (0x%04x) must be non-zero and lower than 0xf000", (unsigned)dbname));
                            dbname = HAM_FIRST_DATABASE_NAME;
                            return (HAM_INV_PARAMETER);
                        }
                        break;
                    }
                }
                goto default_case;

            case HAM_PARAM_GET_DATA_ACCESS_MODE:
            case HAM_PARAM_GET_FLAGS:
            case HAM_PARAM_GET_FILEMODE:
            case HAM_PARAM_GET_FILENAME:
            case HAM_PARAM_GET_KEYS_PER_PAGE:
            case HAM_PARAM_GET_STATISTICS:
            default:
default_case:
                ham_trace(("unsupported/unknown parameter %d (%s)", 
                            (int)param->name, 
                            ham_param2str(NULL, 0, param->name)));
                return (HAM_INV_PARAMETER);
            }
        }
    }

    /*
     * when creating a database we can calculate the DAM depending on the
     * create flags; when opening a database, the recno-flag is persistent
     * and not yet loaded, therefore it's handled by the caller
     */
    if (!dam && create) {
        dam=(flags & HAM_RECORD_NUMBER)
            ? HAM_DAM_SEQUENTIAL_INSERT 
            : HAM_DAM_RANDOM_WRITE;
    }

    if ((env && !db) || (!env && db)) {
        if (!filename && !(flags&HAM_IN_MEMORY_DB)) {
            ham_trace(("filename is missing"));
            return (HAM_INV_PARAMETER);
        }
    }

    if (pdbname) {
        if (create && (dbname==0 || dbname>HAM_DUMMY_DATABASE_NAME)) {
            ham_trace(("parameter 'name' (0x%04x) must be lower than "
                "0xf000", (unsigned)dbname));
            return (HAM_INV_PARAMETER);
            dbname = HAM_FIRST_DATABASE_NAME;
        }
        else if (!create && (dbname==0 || dbname>HAM_DUMMY_DATABASE_NAME)) {
            ham_trace(("parameter 'name' (0x%04x) must be lower than "
                "0xf000", (unsigned)dbname));
            return (HAM_INV_PARAMETER);
            dbname = HAM_FIRST_DATABASE_NAME;
        }
    }

    if (db && (pdbname && !dbname)) {
        dbname = HAM_FIRST_DATABASE_NAME;
        ham_trace(("invalid database name 0x%04x", (unsigned)dbname));
        return (HAM_INV_PARAMETER);
    }

    /*
     * make sure that the raw pagesize is aligned to 1024b
     */
    if (pagesize && pagesize%1024) {
        ham_trace(("pagesize must be multiple of 1024"));
        return (HAM_INV_PAGESIZE);
    }

    /*
     * 1.0.4: HAM_ENABLE_TRANSACTIONS implies HAM_ENABLE_RECOVERY
     */
    if (flags&HAM_ENABLE_TRANSACTIONS)
        flags|=HAM_ENABLE_RECOVERY;

    /* 
     * flag HAM_AUTO_RECOVERY implies HAM_ENABLE_RECOVERY 
     */
    if (flags&HAM_AUTO_RECOVERY)
        flags|=HAM_ENABLE_RECOVERY;

    /*
     * don't allow recovery in combination with some other flags
     */
    if (!__check_recovery_flags(flags))
        return (HAM_INV_PARAMETER);

    /*
     * in-memory-db? don't allow cache limits!
     */
    if (flags&HAM_IN_MEMORY_DB) {
        if (flags&HAM_CACHE_STRICT) {
            ham_trace(("combination of HAM_IN_MEMORY_DB and HAM_CACHE_STRICT "
                        "not allowed"));
            flags &= ~HAM_CACHE_STRICT;
            return (HAM_INV_PARAMETER);
        }
        if (cachesize!=0) {
            ham_trace(("combination of HAM_IN_MEMORY_DB and cachesize != 0 "
                        "not allowed"));
            cachesize = 0;
            return (HAM_INV_PARAMETER);
        }
    }

    /*
     * don't allow cache limits with unlimited cache
     */
    if (flags&HAM_CACHE_UNLIMITED) {
        if ((flags&HAM_CACHE_STRICT) || cachesize!=0) {
            ham_trace(("combination of HAM_CACHE_UNLIMITED and cachesize != 0 "
                        "or HAM_CACHE_STRICT not allowed"));
            cachesize = 0;
            flags &= ~HAM_CACHE_STRICT;
            return (HAM_INV_PARAMETER);
        }
    }

    /*
     * if this is not the first database we're creating (or opening), 
     * we'd better copy the pagesize values from the env / device
     */
    if (env)
        device = env_get_device(env);

    /*
     * inherit defaults from ENV for DB
     */
    if (env && env_is_active(env)) {
        if (!cachesize)
            cachesize = env_get_cachesize(env);
        if (!dbs && env->_hdrpage)
            dbs = env_get_max_databases(env);
        if (!pagesize)
            pagesize = env_get_pagesize(env);
    }

    if (!pagesize && device)
        pagesize = device->get_pagesize(device);

    /*
     * in-memory-db? use a default pagesize of 16kb
     */
    if (flags&HAM_IN_MEMORY_DB) {
        if (!pagesize) {
            pagesize = 16*1024;
            no_mmap = HAM_TRUE;
        }
    }

    /*
     * can we use mmap?
     */
#if HAVE_MMAP
    if (!(flags&HAM_DISABLE_MMAP)) {
        if (pagesize) {
            if (pagesize % os_get_granularity() != 0)
                no_mmap=HAM_TRUE;
        }
        else {
            pagesize = os_get_pagesize();
        }
    }
#else
    no_mmap=HAM_TRUE;
#endif

    /*
     * if we still don't have a raw pagesize, try to get a good default
     * value
     */
    if (!pagesize)
        pagesize = os_get_pagesize();

    /*
     * set the database flags if we can't use mmapped I/O
     */
    if (no_mmap) {
        flags &= ~DB_USE_MMAP;
        flags |= HAM_DISABLE_MMAP;
    }

    /*
     * initialize the keysize with a good default value;
     * 32byte is the size of a first level cache line for most modern
     * processors; adjust the keysize, so the keys are aligned to
     * 32byte
     */
    if (keysize==0) {
        if (flags&HAM_RECORD_NUMBER)
            keysize = sizeof(ham_u64_t);
        else
            keysize = DB_CHUNKSIZE - (db_get_int_key_header_size());
    }

    /*
     * make sure that the cooked pagesize is big enough for at least 5 keys;
     * record number database: need 8 byte
     *
     * By first calculating the keysize if none was specced, we can
     * quickly discard tiny page sizes as well here:
     */
    if (pagesize / keysize < 5) {
        ham_trace(("pagesize too small (%u), must be at least %u bytes",
                    (unsigned)pagesize,
                    (unsigned)(keysize*6)));
        pagesize = keysize*6 + DB_CHUNKSIZE - 1;
        pagesize -= pagesize % DB_CHUNKSIZE;
        return (HAM_INV_KEYSIZE);
    }

    /*
     * make sure that max_databases actually fit in a header
     * page!
     * leave at least 128 bytes for the freelist and the other header data
     */
    {
        ham_size_t l = pagesize - sizeof(env_header_t)
                - db_get_freelist_header_size32() - 128;

        l /= sizeof(db_indexdata_t);
        if (dbs > l) {
            ham_trace(("parameter HAM_PARAM_MAX_ENV_DATABASES too high for "
                        "this pagesize; the maximum allowed is %u", 
                        (unsigned)l));
            set_abs_max_dbs = HAM_TRUE;
            return (HAM_INV_PARAMETER);
        }
        /* override assignment when 'env' already has been configured with a 
         * non-default maxdbs value of its own */
        if (env && !db && env->_hdrpage && env_get_max_databases(env) > 0) {
            dbs = env_get_max_databases(env);
        }
        else if (db
            && db_get_env(db)
            && env_get_device(env)
            && env_get_device(env)->is_open(env_get_device(env))) {
            dbs = (env ? env_get_max_databases(env) : 1);
        }
        else if (set_abs_max_dbs) {
            if (l >= HAM_DEFAULT_DATABASE_NAME)
                l = HAM_DEFAULT_DATABASE_NAME - 1;
            dbs = (ham_u16_t)l;
        }
        else if (dbs == 0) {
            if (DB_MAX_INDICES > l)
                dbs = (ham_u16_t)l;  /* small page sizes (e.g. 1K) cannot carry DB_MAX_INDICES databases! */
            else
                dbs = DB_MAX_INDICES;
        }
    }
    ham_assert(dbs != 0, (0));

    /*
     * return the fixed parameters
     */
    if (pflags)
        *pflags = flags;
    if (pcachesize)
        *pcachesize=cachesize;
    if (pkeysize)
        *pkeysize = keysize;
    if (ppagesize)
        *ppagesize = pagesize;
    if (pdbname)
        *pdbname = dbname;
    if (pdata_access_mode)
        *pdata_access_mode = dam;
    if (pmaxdbs)
        *pmaxdbs = dbs;

    return st;
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

static ham_status_t 
__ham_destroy_env(ham_env_t *env)
{
    if (env)
    {
        memset(env, 0, sizeof(*env));
        free(env);
    }
    return HAM_SUCCESS;
}

ham_status_t HAM_CALLCONV
ham_env_new(ham_env_t **env)
{
    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    /*
     * allocate memory for the ham_db_t-structure;
     * we can't use our allocator because it's not yet created! 
     * Also reset the whole structure.
     */
    *env=(ham_env_t *)calloc(1, sizeof(ham_env_t));
    if (!(*env))
        return (HAM_OUT_OF_MEMORY);

    env[0]->destroy = __ham_destroy_env;

    return HAM_SUCCESS;
}

ham_status_t HAM_CALLCONV
ham_env_delete(ham_env_t *env)
{
    ham_status_t st;
    ham_status_t st2 = HAM_SUCCESS;

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return HAM_INV_PARAMETER;
    }

    /* delete all performance data */
    stats_trash_globdata(env, env_get_global_perf_data(env));

    /* 
     * close the device if it still exists
     */
    if (env_get_device(env)) {
        ham_device_t *device = env_get_device(env);
        if (device->is_open(device)) {
            st = device->flush(device);
            if (!st2) 
                st2 = st;
            st = device->close(device);
            if (!st2) 
                st2 = st;
        }
        st = device->destroy(device);
        if (!st2) 
            st2 = st;
        env_set_device(env, 0);
    }

    /*
     * close the allocator
     */
    if (env_get_allocator(env)) {
        env_get_allocator(env)->close(env_get_allocator(env));
        env_set_allocator(env, 0);
    }

    if (env->destroy) {
        st = env->destroy(env);
        if (!st2) 
            st2 = st;
    }

    return st2;
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
    ham_size_t pagesize = 0;
    ham_u16_t keysize = 0;
    ham_size_t cachesize = 0;
    ham_u16_t maxdbs = 0;

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    /*
     * make sure that this environment is not yet open/created
     */
    if (env_is_active(env)) {
        ham_trace(("parameter 'env' is already initialized"));
        return (HAM_ENVIRONMENT_ALREADY_OPEN);
    }

    env_set_rt_flags(env, 0);

    /*
     * check (and modify) the parameters
     */
    st=__check_create_parameters(env, 0, filename, &flags, param, 
            &pagesize, &keysize, &cachesize, 0, &maxdbs, 0, HAM_TRUE);
    if (st)
        return (st);

    if (!cachesize)
        cachesize=HAM_DEFAULT_CACHESIZE;

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
    env_set_cachesize(env, cachesize);
    env_set_file_mode(env, mode);
    env_set_pagesize(env, pagesize);
    env_set_max_databases_cached(env, maxdbs);
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
     * initialize function pointers
     */
    if (__filename_is_local(filename)) {
        st=env_initialize_local(env);
    }
    else {
        st=env_initialize_remote(env);
    }
    if (st)
        return (st);

    /*
     * and finish the initialization of the Environment
     */
    st=env->_fun_create(env, filename, flags, mode, param);
    if (st)
        return (st);

    env_set_active(env, HAM_TRUE);

    return (st);
}

ham_status_t HAM_CALLCONV
ham_env_create_db(ham_env_t *env, ham_db_t *db,
        ham_u16_t dbname, ham_u32_t flags, const ham_parameter_t *param)
{
    ham_status_t st;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    /*
     * make sure that this database is not yet open/created
     */
    if (db_is_active(db)) {
        ham_trace(("parameter 'db' is already initialized"));
        return (db_set_error(db, HAM_DATABASE_ALREADY_OPEN));
    }

    if (!dbname || (dbname>HAM_DEFAULT_DATABASE_NAME 
            && dbname!=HAM_DUMMY_DATABASE_NAME)) {
        ham_trace(("invalid database name"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    /*
     * the function handler will do the rest
     */
    st=env->_fun_create_db(env, db, dbname, flags, param);
    if (st)
        return (st);

    db_set_active(db, HAM_TRUE);

    return (db_set_error(db, st));
}

ham_status_t HAM_CALLCONV
ham_env_open_db(ham_env_t *env, ham_db_t *db,
        ham_u16_t dbname, ham_u32_t flags, const ham_parameter_t *param)
{
    ham_status_t st;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    if (!dbname) {
        ham_trace(("parameter 'dbname' must not be 0"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (dbname!=HAM_FIRST_DATABASE_NAME 
          && (dbname!=HAM_DUMMY_DATABASE_NAME 
                && dbname>HAM_DEFAULT_DATABASE_NAME)) {
        ham_trace(("database name must be lower than 0xf000"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (env_get_rt_flags(env)&HAM_IN_MEMORY_DB) {
        ham_trace(("cannot open a Database in an In-Memory Environment"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    /*
     * the function handler will do the rest
     */
    st=env->_fun_open_db(env, db, dbname, flags, param);
    if (st)
        return (st);

    db_set_active(db, HAM_TRUE);

    return (db_set_error(db, 0));
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

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    /*
     * make sure that this environment is not yet open/created
     */
    if (env_is_active(env)) {
        ham_trace(("parameter 'env' is already initialized"));
        return (HAM_ENVIRONMENT_ALREADY_OPEN);
    }

    /*
     * check for invalid flags
     */
    if (flags&HAM_SORT_DUPLICATES) {
        ham_trace(("flag HAM_SORT_DUPLICATES only allowed when creating/"
                   "opening Databases, not Environments"));
        return (HAM_INV_PARAMETER);
    }

    env_set_rt_flags(env, 0);

    /* parse parameters */
    st=__check_create_parameters(env, 0, filename, &flags, param, 
            0, 0, &cachesize, 0, 0, 0, HAM_FALSE);
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
    env_set_pagesize(env, 0);
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
     * initialize function pointers
     */
    if (__filename_is_local(filename)) {
        st=env_initialize_local(env);
    }
    else {
        st=env_initialize_remote(env);
    }
    if (st)
        return (st);

    /*
     * and finish the initialization of the Environment
     */
    st=env->_fun_open(env, filename, flags, param);
    if (st)
        return (st);

    env_set_active(env, HAM_TRUE);

    return (st);
}

ham_status_t HAM_CALLCONV
ham_env_rename_db(ham_env_t *env, ham_u16_t oldname, 
                ham_u16_t newname, ham_u32_t flags)
{
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
    if (newname>=HAM_DEFAULT_DATABASE_NAME) {
        ham_trace(("parameter 'newname' must be lower than 0xf000"));
        return (HAM_INV_PARAMETER);
    }
    if (!env->_fun_rename_db) {
        ham_trace(("Environment was not initialized"));
        return (HAM_NOT_INITIALIZED);
    }

    /*
     * no need to do anything if oldname==newname
     */
    if (oldname==newname)
        return (0);

    /*
     * rename the database
     */
    return (env->_fun_rename_db(env, oldname, newname, flags));
}

ham_status_t HAM_CALLCONV
ham_env_erase_db(ham_env_t *env, ham_u16_t name, ham_u32_t flags)
{
    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!name) {
        ham_trace(("parameter 'name' must not be 0"));
        return (HAM_INV_PARAMETER);
    }
    if (!env->_fun_erase_db) {
        ham_trace(("Environment was not initialized"));
        return (HAM_NOT_INITIALIZED);
    }

    /*
     * erase the database
     */
    return (env->_fun_erase_db(env, name, flags));
}

ham_status_t HAM_CALLCONV
ham_env_add_file_filter(ham_env_t *env, ham_file_filter_t *filter)
{
    ham_file_filter_t *head;

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (env_get_rt_flags(env)&DB_IS_REMOTE) {
        ham_trace(("ham_env_add_file_filter is not supported by remote "
                "servers"));
        return (HAM_NOT_IMPLEMENTED);
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
     * later in the same order as the insertion.
     *
     *
     * Because we must process filters IN REVERSE ORDER when WRITING to 
     * disc (going from 'cooked' to 'raw' data), we've created a cyclic 
     * -> prev chain: no need to first traverse to the end, then traverse back.
     * 
     * This means that the -> next forward chain is terminating (last->next 
     * == NULL), while the ->prev chain is cyclic (head->prev = last 
     * wrap-around). Therefor, the fastest way to check if the REVERSE 
     * (= ->prev) traversal is done, is by checking node->prev->next==NULL.
     */
    if (!head) {
        env_set_file_filter(env, filter);
        filter->_prev = filter;
    }
    else {
        head->_prev = filter;

        while (head->_next)
            head=head->_next;
        head->_next=filter;
        filter->_prev = head;
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
    if (env_get_rt_flags(env)&DB_IS_REMOTE) {
        ham_trace(("ham_env_add_file_filter is not supported by remote "
                "servers"));
        return (HAM_NOT_IMPLEMENTED);
    }

    head=env_get_file_filter(env);

    if (head == filter) {
        if (head->_next) {
            ham_assert(head->_prev != head, (0));
            head->_next->_prev = head->_prev;
        }
        env_set_file_filter(env, head->_next);
        return 0;
    }
    else if (head) {
        if (head->_prev == filter) {
            head->_prev = head->_prev->_prev;
        }
        for (;;)
        {
            prev = head;
            head = head->_next;
            if (!head)
                return (HAM_FILTER_NOT_FOUND);
            if (head == filter) {
                prev->_next = head->_next;
                if (head->_next)
                    head->_next->_prev = prev;
                break;
            }
        }
    }
    else
        return (HAM_FILTER_NOT_FOUND);

    filter->_next = 0;
    filter->_prev = 0;

    return (0);
}

ham_status_t HAM_CALLCONV
ham_env_get_database_names(ham_env_t *env, ham_u16_t *names, ham_size_t *count)
{
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
    if (!env->_fun_get_database_names) {
        ham_trace(("Environment was not initialized"));
        return (HAM_NOT_INITIALIZED);
    }

    /*
     * get all database names
     */
    return (env->_fun_get_database_names(env, names, count));
}

HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_get_parameters(ham_env_t *env, ham_parameter_t *param)
{
    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return HAM_INV_PARAMETER;
    }
    if (!param) {
        ham_trace(("parameter 'param' must not be NULL"));
        return HAM_INV_PARAMETER;
    }
    if (!env->_fun_get_parameters) {
        ham_trace(("Environment was not initialized"));
        return (HAM_NOT_INITIALIZED);
    }

    /*
     * get the parameters
     */
    return (env->_fun_get_parameters(env, param));
}

ham_status_t HAM_CALLCONV
ham_env_flush(ham_env_t *env, ham_u32_t flags)
{
    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return HAM_INV_PARAMETER;
    }
    if (!env->_fun_flush) {
        ham_trace(("Environment was not initialized"));
        return (HAM_NOT_INITIALIZED);
    }

    /*
     * flush the Environment
     */
    return (env->_fun_flush(env, flags));
}

ham_status_t HAM_CALLCONV
ham_env_close(ham_env_t *env, ham_u32_t flags)
{
    ham_status_t st;
    ham_status_t st2 = HAM_SUCCESS;

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    /*
     * it's ok to close an uninitialized Environment
     */
    if (!env->_fun_close)
        return (0);

    /*
     * close all databases?
     */
    if (env_get_list(env)) {
        ham_db_t *db=env_get_list(env);
        while (db) {
            ham_db_t *next=db_get_next(db);
            st=ham_close(db, flags);
            if (!st2) st2 = st;
            db=next;
        }
        env_set_list(env, 0);
    }

    /*
     * when all transactions have been properly closed... 
     */
    if (!env_get_txn(env))
    {
        /* flush/persist all performance data which we want to persist */
        stats_flush_globdata(env, env_get_global_perf_data(env));
    }
    else if (env_is_active(env))
    {
        //st2 = HAM_TRANSACTION_STILL_OPEN;
        ham_assert(!"Should never get here; the db close loop above "
                    "should've taken care of all TXNs", (0));
    }

    /*
     * close the environment
     */
    st=env->_fun_close(env, flags);
    if (st)
        return (st);

    /*
     * close everything else
     */
    if (env_get_filename(env)) {
        allocator_free(env_get_allocator(env), 
                (ham_u8_t *)env_get_filename(env));
        env_set_filename(env, 0);
    }

    /* delete all performance data */
    stats_trash_globdata(env, env_get_global_perf_data(env));

    /* 
     * finally, close the memory allocator 
     */
    if (env_get_allocator(env)) {
        env_get_allocator(env)->close(env_get_allocator(env));
        env_set_allocator(env, 0);
    }

    env_set_active(env, HAM_FALSE);

    return (0);
}

static ham_status_t 
__ham_destroy_db(ham_db_t *db)
{
    if (db)
    {
        free(db);
    }
    return HAM_SUCCESS;
}

ham_status_t HAM_CALLCONV
ham_new(ham_db_t **db)
{
    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    /*
     * allocate memory for the ham_db_t-structure;
     * we can't use our allocator because it's not yet created!
     * Also reset the whole structure 
     */
    *db=(ham_db_t *)calloc(1, sizeof(ham_db_t));
    if (!(*db))
        return (HAM_OUT_OF_MEMORY);

    db[0]->_fun_destroy = __ham_destroy_db;

    return HAM_SUCCESS;
}

ham_status_t HAM_CALLCONV
ham_delete(ham_db_t *db)
{
    ham_env_t *env;
    ham_status_t st;
    ham_status_t st2 = HAM_SUCCESS;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return HAM_INV_PARAMETER;
    }
    env = db_get_env(db);

    /* free cached data pointers */
    st2 = db_resize_allocdata(db, 0);

    /* trash all DB performance data */
    stats_trash_dbdata(db, db_get_db_perf_data(db));

    /* 
     * close the database
     */
    if (db_is_active(db)) 
    {
        st = ham_close(db, 0);
        if (!st2) 
            st2 = st;
    }

    if (db->_fun_destroy) {
        st = db->_fun_destroy(db);
        if (!st2) 
            st2 = st;
    }

    return st2;
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
    ham_u16_t dbname=HAM_FIRST_DATABASE_NAME;
    ham_size_t cachesize=0;
    ham_u16_t dam = 0;
    ham_env_t *env;
    ham_u32_t env_flags;
    ham_parameter_t env_param[8]={{0, 0}};
    ham_parameter_t db_param[8]={{0, 0}};

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    /*
     * make sure that this database is not yet open/created
     */
    if (db_is_active(db)) {
        ham_trace(("parameter 'db' is already initialized"));
        return (HAM_DATABASE_ALREADY_OPEN);
    }

    /* parse parameters */
    st=__check_create_parameters(db_get_env(db), db, filename, &flags, param, 
            0, 0, &cachesize, &dbname, 0, &dam, HAM_FALSE);
    if (st)
        return (st);

    db_set_error(db, 0);
    db_set_rt_flags(db, 0);

    /*
     * create an Environment handle and open the Environment
     */
    env_param[0].name=HAM_PARAM_CACHESIZE;
    env_param[0].value=cachesize;
    env_param[1].name=0;
    env_flags=flags & ~(HAM_ENABLE_DUPLICATES|HAM_SORT_DUPLICATES);

    st=ham_env_new(&env);
    if (st)
        goto bail;

    st=ham_env_open_ex(env, filename, env_flags, &env_param[0]);
    if (st)
        goto bail;

    /*
     * now open the Database in this Environment
     *
     * for this, we first strip off flags which are not allowed/needed 
     * in ham_env_open_db; then set up the parameter list
     */
    flags &= ~(HAM_WRITE_THROUGH 
            |HAM_READ_ONLY 
            |HAM_DISABLE_MMAP 
            |HAM_DISABLE_FREELIST_FLUSH
            |HAM_CACHE_UNLIMITED
            |HAM_CACHE_STRICT
            |HAM_LOCK_EXCLUSIVE
            |HAM_ENABLE_TRANSACTIONS
            |HAM_ENABLE_RECOVERY
            |HAM_AUTO_RECOVERY
            |DB_USE_MMAP
            |DB_ENV_IS_PRIVATE);

    db_param[0].name=HAM_PARAM_DATA_ACCESS_MODE;
    db_param[0].value=dam;
    db_param[1].name=0;

    /*
     * now open the Database in this Environment
     */
    st=ham_env_open_db(env, db, dbname, flags, db_param);
    if (st)
        goto bail;

    /*
     * this Environment is 0wned by the Database (and will be deleted in
     * ham_close)
     */
    db_set_rt_flags(db, db_get_rt_flags(db)|DB_ENV_IS_PRIVATE);

bail:
    if (st) {
        if (db)
            (void)ham_close(db, 0);
        if (env) {
            /* despite the IS_PRIVATE the env will destroy the DB, 
            which is the responsibility of the caller: detach the DB now. */
            env_set_list(env, 0);
            (void)ham_env_close(env, 0);
            (void)ham_env_delete(env);
        }
    }

    return (st);
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
    ham_u16_t dam=(flags & HAM_RECORD_NUMBER)
        ? HAM_DAM_SEQUENTIAL_INSERT 
        : HAM_DAM_RANDOM_WRITE;

    ham_size_t pagesize = 0;
    ham_u16_t maxdbs = 0;
    ham_u16_t keysize = 0;
    ham_u16_t dbname = HAM_DEFAULT_DATABASE_NAME;
    ham_size_t cachesize = 0;
    ham_env_t *env=0;
    ham_u32_t env_flags;
    ham_parameter_t env_param[8]={{0, 0}};
    ham_parameter_t db_param[5]={{0, 0}};

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    /*
     * make sure that this database is not yet open/created
     */
    if (db_is_active(db)) {
        ham_trace(("parameter 'db' is already initialized"));
        return (db_set_error(db, HAM_DATABASE_ALREADY_OPEN));
    }

    /*
     * check (and modify) the parameters
     */
    st=__check_create_parameters(db_get_env(db), db, filename, &flags, param, 
            &pagesize, &keysize, &cachesize, &dbname, &maxdbs, &dam, HAM_TRUE);
    if (st)
        return (db_set_error(db, st));

    db_set_error(db, 0);
    db_set_rt_flags(db, 0);

    /*
     * setup the parameters for ham_env_create_ex
     */
    env_param[0].name=HAM_PARAM_CACHESIZE;
    env_param[0].value=(flags&HAM_IN_MEMORY_DB) ? 0 : cachesize;
    env_param[1].name=HAM_PARAM_PAGESIZE;
    env_param[1].value=pagesize;
    env_param[2].name=HAM_PARAM_MAX_ENV_DATABASES;
    env_param[2].value=maxdbs;
    env_param[3].name=0;
    env_flags=flags & ~(HAM_ENABLE_DUPLICATES|HAM_SORT_DUPLICATES);

    /*
     * create a new Environment
     */
    st=ham_env_new(&env);
    if (st)
        goto bail;

    st=ham_env_create_ex(env, filename, env_flags, mode, env_param);
    if (st)
        goto bail;

    /*
     * now create the Database in this Environment
     *
     * for this, we first strip off flags which are not allowed/needed 
     * in ham_env_create_db; then set up the parameter list
     */
    flags &= ~(HAM_WRITE_THROUGH 
            |HAM_IN_MEMORY_DB 
            |HAM_DISABLE_MMAP 
            |HAM_DISABLE_FREELIST_FLUSH
            |HAM_CACHE_UNLIMITED
            |HAM_CACHE_STRICT
            |HAM_LOCK_EXCLUSIVE
            |HAM_ENABLE_TRANSACTIONS
            |HAM_ENABLE_RECOVERY
            |HAM_AUTO_RECOVERY
            |DB_USE_MMAP
            |DB_ENV_IS_PRIVATE);

    db_param[0].name=HAM_PARAM_KEYSIZE;
    db_param[0].value=keysize;
    db_param[1].name=HAM_PARAM_DATA_ACCESS_MODE;
    db_param[1].value=dam;
    db_param[2].name=0;

    /*
     * now create the Database
     */
    st=ham_env_create_db(env, db, HAM_DEFAULT_DATABASE_NAME, flags, db_param);
    if (st)
        goto bail;

    /*
     * this Environment is 0wned by the Database (and will be deleted in
     * ham_close)
     */
    db_set_rt_flags(db, db_get_rt_flags(db)|DB_ENV_IS_PRIVATE);

bail:
    if (st) {
        if (db) {
            (void)ham_close(db, 0);
        }
        if (env) 
        {
            /* despite the IS_PRIVATE the env will destroy the DB, 
            which is the responsibility of the caller: detach the DB now. */
            env_set_list(env, 0);
            (void)ham_env_close(env, 0);
            ham_env_delete(env);
        }
    }

    return (db_set_error(db, st));
}

HAM_EXPORT ham_status_t HAM_CALLCONV
ham_get_parameters(ham_db_t *db, ham_parameter_t *param)
{
    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return HAM_INV_PARAMETER;
    }
    if (!param) {
        ham_trace(("parameter 'param' must not be NULL"));
        return HAM_INV_PARAMETER;
    }
    if (!db->_fun_get_parameters) {
        ham_trace(("Database was not initialized"));
        return (HAM_NOT_INITIALIZED);
    }

    /*
     * get the parameters
     */
    return (db->_fun_get_parameters(db, param));
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

    return (db_set_error(db, HAM_SUCCESS));
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

    return (db_set_error(db, HAM_SUCCESS));
}

ham_status_t HAM_CALLCONV
ham_set_duplicate_compare_func(ham_db_t *db, ham_duplicate_compare_func_t foo)
{
    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    db_set_error(db, 0);
    db_set_duplicate_compare_func(db, foo ? foo : db_default_compare);

    return (db_set_error(db, HAM_SUCCESS));
}

#ifndef HAM_DISABLE_ENCRYPTION
static ham_status_t 
__aes_before_write_cb(ham_env_t *env, ham_file_filter_t *filter, 
        ham_u8_t *page_data, ham_size_t page_size)
{
    ham_size_t i;
    ham_size_t blocks=page_size/16;

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
    ham_size_t i;
    ham_size_t blocks=page_size/16;

    ham_assert(page_size%16==0, ("bogus pagesize"));

    for (i = 0; i < blocks; i++) {
        aes_decrypt(&page_data[i*16], (ham_u8_t *)filter->userdata, 
                &page_data[i*16]);
    }

    return (HAM_SUCCESS);
}

static void
__aes_close_cb(ham_env_t *env, ham_file_filter_t *filter)
{
    mem_allocator_t *alloc=env_get_allocator(env);

    ham_assert(alloc, (0));

    if (filter) {
        if (filter->userdata) {
            /*
             * destroy the secret key in RAM (free() won't do that, 
             * so NIL the key space first! 
             */
            memset(filter->userdata, 0, sizeof(ham_u8_t)*16);
            allocator_free(alloc, filter->userdata);
        }
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
    if (env_get_rt_flags(env)&DB_IS_REMOTE) {
        ham_trace(("ham_env_enable_encryption is not supported by remote "
                "servers"));
        return (HAM_NOT_IMPLEMENTED);
    }
    if (env_get_rt_flags(env)&HAM_IN_MEMORY_DB)
        return (0);

    device=env_get_device(env);

    alloc=env_get_allocator(env);
    if (!alloc) {
        ham_trace(("called ham_env_enable_encryption before "
                    "ham_env_create/open"));
        return (HAM_NOT_INITIALIZED);
    }

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
    st=ham_env_open_db(env, db, HAM_FIRST_DATABASE_NAME, 0, 0);
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

        st=device->read(device, env_get_pagesize(env),
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
    return (HAM_NOT_IMPLEMENTED);
#endif
}

#ifndef HAM_DISABLE_COMPRESSION
static ham_status_t 
__zlib_before_write_cb(ham_db_t *db, ham_record_filter_t *filter, 
        ham_record_t *record)
{
    ham_env_t *env = db_get_env(db);
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

        dest=allocator_alloc(env_get_allocator(env), newsize);
        if (!dest)
            return (db_set_error(db, HAM_OUT_OF_MEMORY));

        newsize-=sizeof(ham_u32_t);
        zret=compress2(dest+sizeof(ham_u32_t), &newsize,
                record->data, record->size, level);
    } while (zret==Z_BUF_ERROR);

    newsize+=sizeof(ham_u32_t);
    *(ham_u32_t *)dest=ham_h2db32(record->size);

    if (zret==Z_MEM_ERROR) {
        allocator_free(env_get_allocator(env), dest);
        return (db_set_error(db, HAM_OUT_OF_MEMORY));
    }

    if (zret!=Z_OK) {
        allocator_free(env_get_allocator(env), dest);
        return (db_set_error(db, HAM_INTERNAL_ERROR));
    }

    record->data=dest;
    record->size=(ham_size_t)newsize;

    return (db_set_error(db, 0));
}

static ham_status_t 
__zlib_after_read_cb(ham_db_t *db, ham_record_filter_t *filter, 
        ham_record_t *record)
{
    ham_env_t *env = db_get_env(db);
    ham_status_t st=0;
    ham_u8_t *src;
    ham_size_t srcsize=record->size;
    unsigned long newsize=record->size-sizeof(ham_u32_t);
    ham_u32_t origsize;
    int zret;

    if (!record->size)
        return (db_set_error(db, 0));

    origsize=ham_db2h32(*(ham_u32_t *)record->data);

    /* don't allow HAM_RECORD_USER_ALLOC */
    if (record->flags&HAM_RECORD_USER_ALLOC) {
        ham_trace(("compression not allowed in combination with "
                    "HAM_RECORD_USER_ALLOC"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    src=allocator_alloc(env_get_allocator(env), newsize);
    if (!src)
        return (db_set_error(db, HAM_OUT_OF_MEMORY));

    memcpy(src, (char *)record->data+4, newsize);

    st=db_resize_allocdata(db, origsize);
    if (st) {
        allocator_free(env_get_allocator(env), src);
        return (db_set_error(db, st));
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

    allocator_free(env_get_allocator(env), src);
    return (db_set_error(db, st));
}

static void 
__zlib_close_cb(ham_db_t *db, ham_record_filter_t *filter)
{
    ham_env_t *env = db_get_env(db);

    if (filter) {
        if (filter->userdata)
            allocator_free(env_get_allocator(env), filter->userdata);
        allocator_free(env_get_allocator(env), filter);
    }
}
#endif /* !HAM_DISABLE_COMPRESSION */

ham_status_t HAM_CALLCONV
ham_enable_compression(ham_db_t *db, ham_u32_t level, ham_u32_t flags)
{
#ifndef HAM_DISABLE_COMPRESSION
    ham_record_filter_t *filter;
    ham_env_t *env;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return HAM_INV_PARAMETER;
    }
    env = db_get_env(db);
    if (!env) {
        ham_trace(("parameter 'db' must be linked to a valid (implicit or "
                   "explicit) environment"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (env_get_rt_flags(env)&DB_IS_REMOTE) {
        ham_trace(("ham_enable_compression is not supported by remote "
                "servers"));
        return (HAM_NOT_IMPLEMENTED);
    }
    if (level>9) {
        ham_trace(("parameter 'level' must be lower than or equal to 9"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (!level)
        level=6;

    db_set_error(db, 0);

    filter=(ham_record_filter_t *)allocator_calloc(env_get_allocator(env), sizeof(*filter));
    if (!filter)
        return (db_set_error(db, HAM_OUT_OF_MEMORY));

    filter->userdata=allocator_calloc(env_get_allocator(env), sizeof(level));
    if (!filter->userdata) {
        allocator_free(env_get_allocator(env), filter);
        return (db_set_error(db, HAM_OUT_OF_MEMORY));
    }

    *(ham_u32_t *)filter->userdata=level;
    filter->before_write_cb=__zlib_before_write_cb;
    filter->after_read_cb=__zlib_after_read_cb;
    filter->close_cb=__zlib_close_cb;

    return (ham_add_record_filter(db, filter));
#else /* !HAM_DISABLE_COMPRESSION */
    ham_trace(("hamsterdb was compiled without support for zlib compression"));
    if (db)
        return (db_set_error(db, HAM_NOT_IMPLEMENTED));
#endif /* ifndef HAM_DISABLE_COMPRESSION */
}

ham_status_t HAM_CALLCONV
ham_find(ham_db_t *db, ham_txn_t *txn, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
    ham_env_t *env;
    ham_txn_t local_txn;
    ham_status_t st;
    ham_backend_t *be;
    ham_offset_t recno=0;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return HAM_INV_PARAMETER;
    }
    env = db_get_env(db);
    if (!env) {
        ham_trace(("parameter 'db' must be linked to a valid (implicit "
                   "or explicit) environment"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (!key) {
        ham_trace(("parameter 'key' must not be NULL"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (!record) {
        ham_trace(("parameter 'record' must not be NULL"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (flags&HAM_HINT_PREPEND) {
        ham_trace(("flag HAM_HINT_PREPEND is only allowed in "
                    "ham_cursor_insert"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (flags&HAM_HINT_APPEND) {
        ham_trace(("flag HAM_HINT_APPEND is only allowed in "
                    "ham_cursor_insert"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if ((flags&HAM_DIRECT_ACCESS) 
            && !(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)) {
        ham_trace(("flag HAM_DIRECT_ACCESS is only allowed in "
                    "In-Memory Databases"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (!__prepare_key(key) || !__prepare_record(record))
        return (db_set_error(db, HAM_INV_PARAMETER));

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
    if (!be || !be_is_active(be))
        return (db_set_error(db, HAM_NOT_INITIALIZED));

    if (!be->_fun_find)
        return HAM_NOT_IMPLEMENTED;

    if (!txn) {
        st=txn_begin(&local_txn, env, HAM_TXN_READ_ONLY);
        if (st)
            return (db_set_error(db, st));
    }

    db_update_global_stats_find_query(db, key->size);

    /*
     * first look up the blob id, then fetch the blob
     */
    st=be->_fun_find(be, key, record, flags);

    if (st) {
        if (!txn)
            (void)txn_abort(&local_txn, DO_NOT_NUKE_PAGE_STATS);
        return (db_set_error(db, st));
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
            (void)txn_abort(&local_txn, DO_NOT_NUKE_PAGE_STATS);
        return (db_set_error(db, st));
    }

    if (!txn)
        return (db_set_error(db, txn_commit(&local_txn, 0)));
    else
        return (db_set_error(db, st));
}

int HAM_CALLCONV
ham_key_get_approximate_match_type(ham_key_t *key)
{
    if (key && (ham_key_get_intflags(key) & KEY_IS_APPROXIMATE))
    {
        int rv = (ham_key_get_intflags(key) & KEY_IS_LT) ? -1 : +1;
        return (rv);
    }

    return (0);
}

ham_status_t HAM_CALLCONV
ham_insert(ham_db_t *db, ham_txn_t *txn, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
    ham_env_t *env;
    ham_txn_t local_txn;
    ham_status_t st;
    ham_backend_t *be;
    ham_u64_t recno = 0;
    ham_record_t temprec;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return HAM_INV_PARAMETER;
    }
    env = db_get_env(db);
    if (!env) {
        ham_trace(("parameter 'db' must be linked to a valid (implicit or "
                   "explicit) environment"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (!key) {
        ham_trace(("parameter 'key' must not be NULL"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (!record) {
        ham_trace(("parameter 'record' must not be NULL"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (flags&HAM_HINT_APPEND) {
        ham_trace(("flags HAM_HINT_APPEND is only allowed in "
                    "ham_cursor_insert"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (flags&HAM_HINT_PREPEND) {
        ham_trace(("flags HAM_HINT_PREPEND is only allowed in "
                    "ham_cursor_insert"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if ((flags&HAM_PARTIAL) && (db_get_rt_flags(db)&HAM_SORT_DUPLICATES)) {
        ham_trace(("flag HAM_PARTIAL is not allowed if duplicates "
                    "are sorted"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if ((flags&HAM_PARTIAL) 
            && (record->partial_size+record->partial_offset>record->size)) {
        ham_trace(("partial offset+size is greater than the total "
                    "record size"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    if (!__prepare_key(key) || !__prepare_record(record))
        return (db_set_error(db, HAM_INV_PARAMETER));

    be=db_get_backend(db);
    if (!be || !be_is_active(be))
        return (db_set_error(db, HAM_NOT_INITIALIZED));
    if (!be->_fun_insert)
        return HAM_NOT_IMPLEMENTED;

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
        st=txn_begin(&local_txn, env, 0);
        if (st)
            return (db_set_error(db, st));
    }

    /*
     * record number: make sure that we have a valid key structure,
     * and lazy load the last used record number
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        if (flags&HAM_OVERWRITE) {
            if (key->size!=sizeof(ham_u64_t) || !key->data) {
                if (!txn)
                    (void)txn_abort(&local_txn, 0);
                ham_trace(("key->size must be 8, key->data must not be NULL"));
                return (db_set_error(db, HAM_INV_PARAMETER));
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
                    return (db_set_error(db, HAM_INV_PARAMETER));
                }
            }
            else {
                if (key->data || key->size) {
                    ham_trace(("key->size must be 0, key->data must be NULL"));
                    if (!txn)
                        (void)txn_abort(&local_txn, 0);
                    return (db_set_error(db, HAM_INV_PARAMETER));
                }
                /* 
                 * allocate memory for the key
                 */
                if (sizeof(ham_u64_t)>db_get_key_allocsize(db)) {
                    if (db_get_key_allocdata(db))
                        allocator_free(env_get_allocator(env), 
                                db_get_key_allocdata(db));
                    db_set_key_allocdata(db, 
                            allocator_alloc(env_get_allocator(env), 
                                sizeof(ham_u64_t)));
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
    st=__record_filters_before_write(db, &temprec);

    if (!st) {
        db_update_global_stats_insert_query(db, key->size, temprec.size);
    }

    /*
     * store the index entry; the backend will store the blob
     */
    if (!st)
        st=be->_fun_insert(be, key, &temprec, flags);

    if (temprec.data!=record->data)
        allocator_free(env_get_allocator(env), temprec.data);

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
        return (db_set_error(db, st));
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
        return (db_set_error(db, txn_commit(&local_txn, 0)));
    else
        return (db_set_error(db, st));
}

ham_status_t HAM_CALLCONV
ham_erase(ham_db_t *db, ham_txn_t *txn, ham_key_t *key, ham_u32_t flags)
{
    ham_txn_t local_txn;
    ham_status_t st;
    ham_env_t *env;
    ham_backend_t *be;
    ham_offset_t recno=0;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    env = db_get_env(db);
    if (!env) {
        ham_trace(("parameter 'db' must be linked to a valid (implicit "
                   "or explicit) environment"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (!key) {
        ham_trace(("parameter 'key' must not be NULL"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (flags&HAM_HINT_PREPEND) {
        ham_trace(("flags HAM_HINT_PREPEND is only allowed in "
                    "ham_cursor_insert"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (flags&HAM_HINT_APPEND) {
        ham_trace(("flags HAM_HINT_APPEND is only allowed in "
                    "ham_cursor_insert"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (!__prepare_key(key))
        return (db_set_error(db, HAM_INV_PARAMETER));

    db_set_error(db, 0);

    be=db_get_backend(db);
    if (!be || !be_is_active(be))
        return (db_set_error(db, HAM_NOT_INITIALIZED));
    if (!be->_fun_erase)
        return HAM_NOT_IMPLEMENTED;
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
        if ((st=txn_begin(&local_txn, env, 0)))
            return (db_set_error(db, st));
    }

    db_update_global_stats_erase_query(db, key->size);

    /*
     * get rid of the entry
     */
    st=be->_fun_erase(be, key, flags);

    if (st) {
        if (!txn)
            (void)txn_abort(&local_txn, 0);
        return (db_set_error(db, st));
    }

    /*
     * record number: re-translate the number to host endian
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        *(ham_offset_t *)key->data=ham_db2h64(recno);
    }

    if (!txn)
        return (db_set_error(db, txn_commit(&local_txn, 0)));
    else
        return (db_set_error(db, st));
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

    db_set_error(db, 0);

    /*
     * check the cache integrity
     */
    if (!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB))
    {
        st=cache_check_integrity(env_get_cache(db_get_env(db)));
        if (st)
            return (db_set_error(db, st));
    }

    be=db_get_backend(db);
    if (!be)
        return (db_set_error(db, HAM_NOT_INITIALIZED));
    if (!be->_fun_check_integrity)
        return (db_set_error(db, HAM_NOT_IMPLEMENTED));

    if (!txn) {
        if ((st=txn_begin(&local_txn, db_get_env(db), HAM_TXN_READ_ONLY)))
            return (db_set_error(db, st));
    }

    /*
     * call the backend function
     */
    st=be->_fun_check_integrity(be);

    if (st) {
        if (!txn)
            (void)txn_abort(&local_txn, 0);
        return (db_set_error(db, st));
    }

    if (!txn)
        return (db_set_error(db, txn_commit(&local_txn, 0)));
    else
        return (db_set_error(db, st));
#else /* !HAM_ENABLE_INTERNAL */
    ham_trace(("hamsterdb was compiled without support for internal "
                "functions"));
    if (db)
        return (db_set_error(db, HAM_NOT_IMPLEMENTED));
    else
        return (HAM_NOT_IMPLEMENTED);
#endif /* ifdef HAM_ENABLE_INTERNAL */
}

ham_status_t HAM_CALLCONV
ham_calc_maxkeys_per_page(ham_db_t *db, ham_size_t *keycount, ham_u16_t keysize)
{
#ifdef HAM_ENABLE_INTERNAL
    ham_status_t st;
    ham_backend_t *be;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!keycount) {
        ham_trace(("parameter 'keycount' must not be NULL"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    *keycount = 0;

    db_set_error(db, 0);

    be=db_get_backend(db);
    if (!be)
        return (db_set_error(db, HAM_NOT_INITIALIZED));
    if (!be->_fun_calc_keycount_per_page) {
        ham_trace(("hamsterdb was compiled without support for internal "
                    "functions"));
        return (db_set_error(db, HAM_NOT_IMPLEMENTED));
    }

    /*
     * call the backend function
     */
    st=be->_fun_calc_keycount_per_page(be, keycount, keysize);

    return (db_set_error(db, st));

#else /* !HAM_ENABLE_INTERNAL */
    ham_trace(("hamsterdb was compiled without support for internal "
                "functions"));
    if (db)
        return (db_set_error(db, HAM_NOT_IMPLEMENTED));
    else
        return (HAM_NOT_IMPLEMENTED);
#endif /* ifdef HAM_ENABLE_INTERNAL */
}

ham_status_t HAM_CALLCONV
ham_flush(ham_db_t *db, ham_u32_t flags)
{
    ham_status_t st;
    ham_env_t *env;
    ham_device_t *dev;
    ham_backend_t *be;

    (void)flags;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    env = db_get_env(db);
    if (!env) {
        ham_trace(("parameter 'db' must be linked to a valid (implicit or "
                   "explicit) environment"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    db_set_error(db, 0);

    /*
     * never flush an in-memory-database
     */
    if (env_get_rt_flags(env)&HAM_IN_MEMORY_DB)
        return (db_set_error(db, 0));

    be=db_get_backend(db);
    if (!be || !be_is_active(be))
        return (db_set_error(db, HAM_NOT_INITIALIZED));
    if (!be->_fun_flush)
        return (HAM_NOT_IMPLEMENTED);

    dev = env_get_device(env);
    if (!dev)
        return (db_set_error(db, HAM_NOT_INITIALIZED));

    /*
     * flush the backend
     */
    st=be->_fun_flush(be);
    if (st)
        return (db_set_error(db, st));

    /*
     * update the header page, if necessary
     */
    if (env_is_dirty(env)) {
        st=page_flush(env_get_header_page(env));
        if (st)
            return (db_set_error(db, st));
    }

    st=db_flush_all(env_get_cache(env), DB_FLUSH_NODELETE);
    if (st)
        return (db_set_error(db, st));

    st=dev->flush(dev);
    if (st)
        return (db_set_error(db, st));

    return (db_set_error(db, HAM_SUCCESS));
}

/*
 * always shut down entirely, even when a page flush or other 
 * 'non-essential' element of the process fails.
 */
ham_status_t HAM_CALLCONV
ham_close(ham_db_t *db, ham_u32_t flags)
{
    ham_status_t st = HAM_SUCCESS;
    ham_env_t *env=0;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    /*
     * it's ok to close an uninitialized Database
     */
    if (!db->_fun_close)
        return (0);

    env = db_get_env(db);
    if ((flags&HAM_TXN_AUTO_ABORT) && (flags&HAM_TXN_AUTO_COMMIT)) {
        ham_trace(("invalid combination of flags: HAM_TXN_AUTO_ABORT + "
                    "HAM_TXN_AUTO_COMMIT"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    db_set_error(db, 0);
    
    /*
     * the function pointer will do the actual implementation
     */
    st=db->_fun_close(db, flags);

    db_set_active(db, HAM_FALSE);

    return (db_set_error(db, st));
}

ham_status_t HAM_CALLCONV
ham_cursor_create(ham_db_t *db, ham_txn_t *txn, ham_u32_t flags,
        ham_cursor_t **cursor)
{
    ham_status_t st;
    ham_env_t *env;
    ham_backend_t *be;
    
    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return HAM_INV_PARAMETER;
    }
    if (!cursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    env = db_get_env(db);
    if (!env) {
        ham_trace(("parameter 'db' must be linked to a valid (implicit or "
                   "explicit) environment"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    db_set_error(db, 0);

    be=db_get_backend(db);
    if (!be || !be_is_active(be))
        return (db_set_error(db, HAM_NOT_INITIALIZED));
    if (!be->_fun_cursor_create)
        return (db_set_error(db, HAM_NOT_IMPLEMENTED));

    st=be->_fun_cursor_create(be, db, txn, flags, cursor);
    if (st)
        return (db_set_error(db, st));

    if (txn)
        txn_set_cursor_refcount(txn, txn_get_cursor_refcount(txn)+1);

    return (db_set_error(db, 0));
}

ham_status_t HAM_CALLCONV
ham_cursor_clone(ham_cursor_t *src, ham_cursor_t **dest)
{
    ham_status_t st;
    ham_txn_t local_txn;
    ham_db_t *db;
    ham_env_t *env;

    if (!src) {
        ham_trace(("parameter 'src' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!dest) {
        ham_trace(("parameter 'dest' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    db=cursor_get_db(src);

    if (!db || !db_get_env(db)) {
        ham_trace(("parameter 'cursor' must be linked to a valid database"));
        return HAM_INV_PARAMETER;
    }
    env = db_get_env(db);

    db_set_error(db, 0);

    if (!cursor_get_txn(src)) {
        st=txn_begin(&local_txn, env, HAM_TXN_READ_ONLY);
        if (st)
            return (db_set_error(db, st));
    }

    st=src->_fun_clone(src, dest);
    if (st) {
        if (!cursor_get_txn(src))
            (void)txn_abort(&local_txn, 0);
        return (db_set_error(db, st));
    }

    if (cursor_get_txn(src))
        txn_set_cursor_refcount(cursor_get_txn(src), 
                txn_get_cursor_refcount(cursor_get_txn(src))+1);

    if (!cursor_get_txn(src))
        return (db_set_error(db, txn_commit(&local_txn, 0)));
    else
        return (db_set_error(db, 0));
}

ham_status_t HAM_CALLCONV
ham_cursor_overwrite(ham_cursor_t *cursor, ham_record_t *record,
            ham_u32_t flags)
{
    ham_status_t st;
    ham_txn_t local_txn;
    ham_db_t *db;
    ham_env_t *env;
    ham_record_t temprec;

    if (!cursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    db=cursor_get_db(cursor);
    if (!db || !db_get_env(db)) {
        ham_trace(("parameter 'cursor' must be linked to a valid database"));
        return HAM_INV_PARAMETER;
    }
    env = db_get_env(db);

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

    if (db_get_rt_flags(db)&HAM_READ_ONLY) {
        ham_trace(("cannot overwrite in a read-only database"));
        return (db_set_error(db, HAM_DB_READ_ONLY));
    }
    if (db_get_rt_flags(db)&HAM_SORT_DUPLICATES) {
        ham_trace(("function ham_cursor_overwrite is not allowed if "
                    "duplicate sorting is enabled"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    if (!__prepare_record(record))
        return (db_set_error(db, HAM_INV_PARAMETER));

    db_set_error(db, 0);

    if (!cursor_get_txn(cursor)) {
        st=txn_begin(&local_txn, env, 0);
        if (st)
            return (db_set_error(db, st));
    }

    /*
     * run the record-level filters on a temporary record structure - we
     * don't want to mess up the original structure
     */
    temprec=*record;
    st=__record_filters_before_write(db, &temprec);
    if (st) {
        if (!cursor_get_txn(cursor))
            (void)txn_abort(&local_txn, 0);
        return (db_set_error(db, st));
    }

    st=cursor->_fun_overwrite(cursor, &temprec, flags);

    ham_assert(env_get_allocator(env) == cursor_get_allocator(cursor), (0));
    if (temprec.data != record->data)
        allocator_free(env_get_allocator(env), temprec.data);

    if (st) {
        if (!cursor_get_txn(cursor))
            (void)txn_abort(&local_txn, 0);
        return (db_set_error(db, st));
    }

    if (!cursor_get_txn(cursor))
        return (db_set_error(db, txn_commit(&local_txn, 0)));
    else
        return (db_set_error(db, st));
}

ham_status_t HAM_CALLCONV
ham_cursor_move(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
    ham_status_t st;
    ham_db_t *db;
    ham_env_t *env;
    ham_txn_t local_txn;

    if (!cursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    db=cursor_get_db(cursor);
    if (!db || !db_get_env(db)) {
        ham_trace(("parameter 'cursor' must be linked to a valid database"));
        return HAM_INV_PARAMETER;
    }
    env = db_get_env(db);

    if ((flags&HAM_ONLY_DUPLICATES) && (flags&HAM_SKIP_DUPLICATES)) {
        ham_trace(("combination of HAM_ONLY_DUPLICATES and "
                    "HAM_SKIP_DUPLICATES not allowed"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if ((flags&HAM_DIRECT_ACCESS) 
            && !(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)) {
        ham_trace(("flag HAM_DIRECT_ACCESS is only allowed in "
                   "In-Memory Databases"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    if (key && !__prepare_key(key))
        return (db_set_error(db, HAM_INV_PARAMETER));
    if (record && !__prepare_record(record))
        return (db_set_error(db, HAM_INV_PARAMETER));

    db_set_error(db, 0);

    if (!cursor_get_txn(cursor)) {
        st=txn_begin(&local_txn, env, HAM_TXN_READ_ONLY);
        if (st)
            return (db_set_error(db, st));
    }

    st=cursor->_fun_move(cursor, key, record, flags);
    if (st) {
        if (!cursor_get_txn(cursor))
            (void)txn_abort(&local_txn, 0);
        return (db_set_error(db, st));
    }

    /*
     * run the record-level filters
     */
    if (record) {
        st=__record_filters_after_find(db, record);
        if (st) {
            if (!cursor_get_txn(cursor))
                (void)txn_abort(&local_txn, 0);
            return (db_set_error(db, st));
        }
    }

    if (!cursor_get_txn(cursor))
        return (db_set_error(db, txn_commit(&local_txn, 0)));
    else
        return (db_set_error(db, st));
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
    ham_env_t *env;
    ham_txn_t local_txn;

    if (!cursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    db=cursor_get_db(cursor);
    if (!db || !db_get_env(db)) {
        ham_trace(("parameter 'cursor' must be linked to a valid database"));
        return HAM_INV_PARAMETER;
    }
    env = db_get_env(db);

    if (!key) {
        ham_trace(("parameter 'key' must not be NULL"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    if (flags & ~(HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH | 
                HAM_FIND_EXACT_MATCH | HAM_DIRECT_ACCESS)) {
        ham_trace(("flag values besides any combination of "
                   "HAM_FIND_LT_MATCH, HAM_FIND_GT_MATCH, "
                   "HAM_FIND_EXACT_MATCH and HAM_DIRECT_ACCESS "
                   "are not allowed"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if ((flags&HAM_DIRECT_ACCESS) 
            && !(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)) {
        ham_trace(("flag HAM_DIRECT_ACCESS is only allowed in "
                   "In-Memory Databases"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (flags&HAM_HINT_PREPEND) {
        ham_trace(("flag HAM_HINT_PREPEND is only allowed in "
                   "ham_cursor_insert"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (flags&HAM_HINT_APPEND) {
        ham_trace(("flag HAM_HINT_APPEND is only allowed in "
                   "ham_cursor_insert"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    if (key && !__prepare_key(key))
        return (db_set_error(db, HAM_INV_PARAMETER));
    if (record &&  !__prepare_record(record))
        return (db_set_error(db, HAM_INV_PARAMETER));

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

    if (!cursor_get_txn(cursor)) {
        st=txn_begin(&local_txn, env, HAM_TXN_READ_ONLY);
        if (st)
            return (db_set_error(db, st));
    }

    db_update_global_stats_find_query(db, key->size);

    st=cursor->_fun_find(cursor, key, record, flags);
    if (st)
    {
        if (!cursor_get_txn(cursor))
            (void)txn_abort(&local_txn, DO_NOT_NUKE_PAGE_STATS);
        return (db_set_error(db, st));
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
                (void)txn_abort(&local_txn, DO_NOT_NUKE_PAGE_STATS);
            return (db_set_error(db, st));
        }
    }

    if (!cursor_get_txn(cursor))
        return (db_set_error(db, txn_commit(&local_txn, 0)));
    else
        return (db_set_error(db, 0));
}

ham_status_t HAM_CALLCONV
ham_cursor_insert(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
    ham_db_t *db;
    ham_env_t *env;
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
    if (!db || !db_get_env(db)) {
        ham_trace(("parameter 'cursor' must be linked to a valid database"));
        return HAM_INV_PARAMETER;
    }
    env = db_get_env(db);

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
    if ((flags&HAM_HINT_APPEND) && (flags&HAM_HINT_PREPEND)) {
        ham_trace(("flags HAM_HINT_APPEND and HAM_HINT_PREPEND "
                   "are mutually exclusive"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (!__prepare_key(key) || !__prepare_record(record))
        return (db_set_error(db, HAM_INV_PARAMETER));

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
    if ((flags&HAM_PARTIAL) && (db_get_rt_flags(db)&HAM_SORT_DUPLICATES)) {
        ham_trace(("flag HAM_PARTIAL is not allowed if duplicates "
                    "are sorted"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if ((flags&HAM_PARTIAL) 
            && (record->partial_size+record->partial_offset>record->size)) {
        ham_trace(("partial offset+size is greater than the total "
                    "record size"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    /*
     * set flag HAM_DUPLICATE if one of DUPLICATE_INSERT* is set, but do
     * not allow these flags if duplicate sorting is enabled
     */
    if (flags&(HAM_DUPLICATE_INSERT_AFTER
                |HAM_DUPLICATE_INSERT_BEFORE
                |HAM_DUPLICATE_INSERT_LAST
                |HAM_DUPLICATE_INSERT_FIRST)) {
        if (db_get_rt_flags(db)&HAM_SORT_DUPLICATES) {
            ham_trace(("flag HAM_DUPLICATE_INSERT_* is not allowed if "
                        "duplicate sorting is enabled"));
            return (db_set_error(db, HAM_INV_PARAMETER));
        }
        flags|=HAM_DUPLICATE;
    }

    /*
     * record number: make sure that we have a valid key structure,
     * and lazy load the last used record number.
     * also specify the flag HAM_HINT_APPEND (implicit)
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        if (flags&HAM_OVERWRITE) {
            if (key->size!=sizeof(ham_u64_t) || !key->data) {
                ham_trace(("key->size must be 8, key->data must not be NULL"));
                return (db_set_error(db, HAM_INV_PARAMETER));
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
                    return (db_set_error(db, HAM_INV_PARAMETER));
                }
            }
            else {
                if (key->data || key->size) {
                    ham_trace(("key->size must be 0, key->data must be NULL"));
                    return (db_set_error(db, HAM_INV_PARAMETER));
                }
                /* 
                 * allocate memory for the key
                 */
                if (sizeof(ham_u64_t)>db_get_key_allocsize(db)) {
                    if (db_get_key_allocdata(db))
                        allocator_free(env_get_allocator(env), 
                                        db_get_key_allocdata(db));
                    db_set_key_allocdata(db, 
                            allocator_alloc(env_get_allocator(env), 
                                            sizeof(ham_u64_t)));
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

        /*
         * we're appending this key sequentially
         */
        flags|=HAM_HINT_APPEND;
    }

    if (!cursor_get_txn(cursor)) {
        if ((st=txn_begin(&local_txn, env, 0)))
            return (db_set_error(db, st));
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
            (void)txn_abort(&local_txn, 0);
        if ((db_get_rt_flags(db)&HAM_RECORD_NUMBER) && !(flags&HAM_OVERWRITE)) {
            if (!(key->flags&HAM_KEY_USER_ALLOC)) {
                key->data=0;
                key->size=0;
            }
            ham_assert(st!=HAM_DUPLICATE_KEY, ("duplicate key in recno db!"));
        }
        return (db_set_error(db, st));
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
        return (db_set_error(db, txn_commit(&local_txn, 0)));
    else
        return (db_set_error(db, st));
}

ham_status_t HAM_CALLCONV
ham_cursor_erase(ham_cursor_t *cursor, ham_u32_t flags)
{
    ham_status_t st;
    ham_txn_t local_txn;
    ham_db_t *db;
    ham_env_t *env;

    if (!cursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    db=cursor_get_db(cursor);
    if (!db || !db_get_env(db)) {
        ham_trace(("parameter 'cursor' must be linked to a valid database"));
        return HAM_INV_PARAMETER;
    }
    env = db_get_env(db);

    db=cursor_get_db(cursor);
    db_set_error(db, 0);

    if (db_get_rt_flags(db)&HAM_READ_ONLY) {
        ham_trace(("cannot erase from a read-only database"));
        return (db_set_error(db, HAM_DB_READ_ONLY));
    }
    if (flags&HAM_HINT_PREPEND) {
        ham_trace(("flags HAM_HINT_PREPEND is only allowed in "
                   "ham_cursor_insert"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (flags&HAM_HINT_APPEND) {
        ham_trace(("flags HAM_HINT_APPEND is only allowed in "
                   "ham_cursor_insert"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    if (!cursor_get_txn(cursor)) {
        st=txn_begin(&local_txn, env, 0);
        if (st)
            return (db_set_error(db, st));
    }

    db_update_global_stats_erase_query(db, 0);

    st=cursor->_fun_erase(cursor, flags);

    if (st) {
        if (!cursor_get_txn(cursor))
            (void)txn_abort(&local_txn, 0);
        return (db_set_error(db, st));
    }

    if (!cursor_get_txn(cursor))
        return (db_set_error(db, txn_commit(&local_txn, 0)));
    else
        return (db_set_error(db, st));
}

ham_status_t HAM_CALLCONV
ham_cursor_get_duplicate_count(ham_cursor_t *cursor, 
        ham_size_t *count, ham_u32_t flags)
{
    ham_status_t st;
    ham_txn_t local_txn;
    ham_db_t *db;
    ham_env_t *env;

    if (!cursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return HAM_INV_PARAMETER;
    }
    db=cursor_get_db(cursor);
    if (!db || !db_get_env(db)) {
        ham_trace(("parameter 'cursor' must be linked to a valid database"));
        return HAM_INV_PARAMETER;
    }
    env = db_get_env(db);

    if (!count) {
        ham_trace(("parameter 'count' must not be NULL"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    *count=0;

    db_set_error(cursor_get_db(cursor), 0);

    if (!cursor_get_txn(cursor)) {
        st=txn_begin(&local_txn, env, HAM_TXN_READ_ONLY);
        if (st)
            return (db_set_error(db, st));
    }

    st=(*cursor->_fun_get_duplicate_count)(cursor, count, flags);
    if (st) {
        if (!cursor_get_txn(cursor))
            (void)txn_abort(&local_txn, 0);
        return (db_set_error(db, st));
    }

    if (!cursor_get_txn(cursor))
        return (db_set_error(db, txn_commit(&local_txn, 0)));
    else
        return (db_set_error(db, st));
}

ham_status_t HAM_CALLCONV
ham_cursor_close(ham_cursor_t *cursor)
{
    ham_status_t st;
    ham_db_t *db;
    ham_env_t *env;

    if (!cursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    db = cursor_get_db(cursor);
    if (!db || !db_get_env(db)) {
        ham_trace(("parameter 'cursor' must be linked to a valid database"));
        return HAM_INV_PARAMETER;
    }
    env = db_get_env(db);

    db_set_error(db, 0);

    st=cursor->_fun_close(cursor);
    if (!st) {
        if (cursor_get_txn(cursor))
            txn_set_cursor_refcount(cursor_get_txn(cursor), 
                    txn_get_cursor_refcount(cursor_get_txn(cursor))-1);
        allocator_free(cursor_get_allocator(cursor), cursor);
    }

    return (db_set_error(db, st));
}

ham_status_t HAM_CALLCONV
ham_add_record_filter(ham_db_t *db, ham_record_filter_t *filter)
{
    ham_record_filter_t *head;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

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
    if (!head) 
    {
        db_set_record_filter(db, filter);
        filter->_prev = filter;
    }
    else 
    {
        head->_prev = filter;

        while (head->_next)
            head=head->_next;

        filter->_prev=head;
        head->_next=filter;
    }

    return (db_set_error(db, 0));
}

ham_status_t HAM_CALLCONV
ham_remove_record_filter(ham_db_t *db, ham_record_filter_t *filter)
{
    ham_record_filter_t *head, *prev;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    db_set_error(db, 0);

    if (!filter) {
        ham_trace(("parameter 'filter' must not be NULL"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    head=db_get_record_filter(db);

    if (head == filter) {
        if (head->_next) {
            ham_assert(head->_prev != head, (0));
            head->_next->_prev = head->_prev;
        }
        db_set_record_filter(db, head->_next);
    }
    else if (head) {
        if (head->_prev == filter) {
            head->_prev = head->_prev->_prev;
        }
        for (;;) {
            prev = head;
            head = head->_next;
            if (!head)
                return (HAM_FILTER_NOT_FOUND);
            if (head == filter) {
                prev->_next = head->_next;
                if (head->_next)
                    head->_next->_prev = prev;
                break;
            }
        }
    }
    else
        return (db_set_error(db, HAM_FILTER_NOT_FOUND));

    filter->_prev = 0;
    filter->_next = 0;

    return (db_set_error(db, 0));
}

ham_status_t HAM_CALLCONV
ham_env_set_device(ham_env_t *env, ham_device_t *device)
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

void HAM_CALLCONV
ham_env_set_context_data(ham_env_t *env, void *data)
{
    if (env)
        env_set_context_data(env, data);
}

void * HAM_CALLCONV
ham_env_get_context_data(ham_env_t *env)
{
    if (env)
        return env_get_context_data(env);
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

ham_env_t *
ham_get_env(ham_db_t *db)
{
    if (!db || !db_is_active(db))
        return (0);
    return (db_get_env(db));
}

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
    int_key_t *key;
    calckeys_context_t *c;
    ham_page_t *page;
    ham_u32_t level;
    ham_size_t count1;
    ham_size_t count2;

    c = (calckeys_context_t *)context;

    switch (event) 
    {
    case ENUM_EVENT_DESCEND:
        level = *(ham_u32_t *)param1;
        count1 = *(ham_size_t *)param2;
        break;

    case ENUM_EVENT_PAGE_START:
        c->is_leaf = *(ham_bool_t *)param2;
        page = (ham_page_t *)param1;
        break;

    case ENUM_EVENT_PAGE_STOP:
        break;

    case ENUM_EVENT_ITEM:
        key = (int_key_t *)param1;
        count2 = *(ham_size_t *)param2;

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
                c->total_count += (count2 - 1) * dupcount;
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

ham_status_t HAM_CALLCONV
ham_get_key_count(ham_db_t *db, ham_txn_t *txn, ham_u32_t flags,
            ham_offset_t *keycount)
{
    ham_txn_t local_txn;
    ham_status_t st;
    ham_backend_t *be;
    ham_env_t *env=0;
    calckeys_context_t ctx = {db, flags, 0, HAM_FALSE};

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    
    if (!keycount) {
        ham_trace(("parameter 'keycount' must not be NULL"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    *keycount = 0;
    env=db_get_env(db);

    db_set_error(db, 0);

    if (flags & ~(HAM_SKIP_DUPLICATES | HAM_FAST_ESTIMATE)) {
        ham_trace(("parameter 'flag' contains unsupported flag bits: %08x", 
                  flags & ~(HAM_SKIP_DUPLICATES | HAM_FAST_ESTIMATE)));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    be = db_get_backend(db);
    if (!be || !be_is_active(be))
        return (db_set_error(db, HAM_NOT_INITIALIZED));
    if (!be->_fun_enumerate)
        return (db_set_error(db, HAM_NOT_IMPLEMENTED));

    if (!txn) {
        st = txn_begin(&local_txn, env, HAM_TXN_READ_ONLY);
        if (st)
            return (db_set_error(db, st));
    }

    /*
     * call the backend function
     */
    st = be->_fun_enumerate(be, my_calc_keys_cb, &ctx);

    if (st) {
        if (!txn)
            (void)txn_abort(&local_txn, 0);
        return (db_set_error(db, st));
    }

    *keycount = ctx.total_count;

    if (!txn)
        return (db_set_error(db, txn_commit(&local_txn, 0)));
    else
        return (db_set_error(db, st));
}

ham_status_t HAM_CALLCONV
ham_clean_statistics_datarec(ham_statistics_t *s)
{
    if (!s) {
        ham_trace(("parameter 's' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    if (s->_free_func)
        s->_free_func(s);

    ham_assert(s->_free_func == 0, 
        ("the cleanup function must eradicate itself from the struct"));

    return (0);
}
