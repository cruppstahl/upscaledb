/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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

#if HAM_ENABLE_REMOTE
#  define CURL_STATICLIB /* otherwise libcurl uses wrong __declspec */
#  include <curl/curl.h>
#  include <curl/easy.h>
#  include "protocol/protocol.h"
#endif

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
#include "log.h"
#include "mem.h"
#include "os.h"
#include "page.h"
#include "serial.h"
#include "btree_stats.h"
#include "txn.h"
#include "util.h"
#include "version.h"
#include "worker.h"

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

using namespace ham;


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
    if (buf && buflen > (*buf ? strlen(interject) : 0) + strlen(src)) {
        if (*buf)
            strcat(buf, interject);
        strcat(buf, src);
    }
    if (buf && buflen) {
        buf[buflen - 1] = 0;
        return (buf);
    }
    ham_assert(!"shouldn't be here");
    return ((char *)"???");
}

static const char *
ham_create_flags2str(char *buf, size_t buflen, ham_u32_t flags)
{
    if (!buf || !buflen) {
        buflen = 0;
        buf = NULL;
    }
    else
        buf[0] = 0;

    if (flags & HAM_WRITE_THROUGH) {
        flags &= ~HAM_WRITE_THROUGH            ;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_WRITE_THROUGH");
    }
    if (flags & HAM_READ_ONLY) {
        flags &= ~HAM_READ_ONLY;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_READ_ONLY");
    }
    if (flags & HAM_USE_BTREE) {
        flags &= ~HAM_USE_BTREE;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_USE_BTREE");
    }
    if (flags & HAM_DISABLE_VAR_KEYLEN) {
        flags &= ~HAM_DISABLE_VAR_KEYLEN;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_DISABLE_VAR_KEYLEN");
    }
    if (flags & HAM_ENABLE_ASYNCHRONOUS_FLUSH) {
        flags &= ~HAM_ENABLE_ASYNCHRONOUS_FLUSH;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_ENABLE_ASYNCHRONOUS_FLUSH");
    }
    if (flags & HAM_IN_MEMORY)
    {
        flags &= ~HAM_IN_MEMORY;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_IN_MEMORY");
    }
    if (flags & HAM_DISABLE_MMAP)
    {
        flags &= ~HAM_DISABLE_MMAP;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_DISABLE_MMAP");
    }
    if (flags & HAM_CACHE_STRICT) {
        flags &= ~HAM_CACHE_STRICT;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_CACHE_STRICT");
    }
    if (flags & HAM_DISABLE_FREELIST_FLUSH) {
        flags &= ~HAM_DISABLE_FREELIST_FLUSH;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_DISABLE_FREELIST_FLUSH");
    }
    if (flags & HAM_LOCK_EXCLUSIVE) {
        flags &= ~HAM_LOCK_EXCLUSIVE;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_LOCK_EXCLUSIVE");
    }
    if (flags & HAM_RECORD_NUMBER) {
        flags &= ~HAM_RECORD_NUMBER;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_RECORD_NUMBER");
    }
    if (flags & HAM_ENABLE_DUPLICATES) {
        flags &= ~HAM_ENABLE_DUPLICATES;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_ENABLE_DUPLICATES");
    }
    if (flags & HAM_SORT_DUPLICATES) {
        flags &= ~HAM_SORT_DUPLICATES;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_SORT_DUPLICATES");
    }
    if (flags & HAM_ENABLE_RECOVERY) {
        flags &= ~HAM_ENABLE_RECOVERY;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_ENABLE_RECOVERY");
    }
    if (flags & HAM_AUTO_RECOVERY) {
        flags &= ~HAM_AUTO_RECOVERY;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_AUTO_RECOVERY");
    }
    if (flags & HAM_ENABLE_TRANSACTIONS) {
        flags &= ~HAM_ENABLE_TRANSACTIONS;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_ENABLE_TRANSACTIONS");
    }
    if (flags & HAM_CACHE_UNLIMITED) {
        flags &= ~HAM_CACHE_UNLIMITED;
        buf = my_strncat_ex(buf, buflen, NULL, "HAM_CACHE_UNLIMITED");
    }

    if (flags) {
        if (buf && buflen > 13 && buflen > strlen(buf) + 13 + 1 + 9) {
            util_snprintf(buf, buflen, "%sHAM_FLAGS(reserved: 0x%x)",
                            (*buf ? "|" : ""), (unsigned int)flags);
        }
        else {
            ham_assert(!"shouldn't be here");
            buf = (char *)"???";
        }
    }

    return (buf);
}

const char * HAM_CALLCONV
ham_param2str(char *buf, size_t buflen, ham_u32_t name)
{
    switch (name) {
    case HAM_PARAM_CACHESIZE:
        return "HAM_PARAM_CACHESIZE";

    case HAM_PARAM_PAGESIZE:
        return "HAM_PARAM_PAGESIZE";

    case HAM_PARAM_KEYSIZE:
        return "HAM_PARAM_KEYSIZE";

    case HAM_PARAM_LOG_DIRECTORY:
        return "HAM_PARAM_LOG_DIRECTORY";

    case HAM_PARAM_MAX_ENV_DATABASES:
        return "HAM_PARAM_MAX_ENV_DATABASES";

    case HAM_PARAM_DATA_ACCESS_MODE:
        return "HAM_PARAM_DATA_ACCESS_MODE";

    case HAM_PARAM_GET_FLAGS:
        return "HAM_PARAM_GET_FLAGS";

    case HAM_PARAM_GET_DATA_ACCESS_MODE:
        return "HAM_PARAM_GET_DATA_ACCESS_MODE";

    case HAM_PARAM_GET_FILEMODE:
        return "HAM_PARAM_GET_FILEMODE";

    case HAM_PARAM_GET_FILENAME:
        return "HAM_PARAM_GET_FILENAME";

    case HAM_PARAM_GET_DATABASE_NAME:
        return "HAM_PARAM_GET_DATABASE_NAME";

    case HAM_PARAM_GET_KEYS_PER_PAGE:
        return "HAM_PARAM_GET_KEYS_PER_PAGE";

    default:
        if (buf && buflen > 13) {
            util_snprintf(buf, buflen, "HAM_PARAM(0x%x)", (unsigned int)name);
            return buf;
        }
        break;
    }

    ham_assert(!"shouldn't be here");
    return ("???");
}

static ham_bool_t
__check_recovery_flags(ham_u32_t flags)
{
    if (flags&HAM_ENABLE_RECOVERY) {
        if (flags&HAM_IN_MEMORY) {
            ham_trace(("combination of HAM_ENABLE_RECOVERY and "
                       "HAM_IN_MEMORY not allowed"));
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

ham_status_t
ham_txn_begin(ham_txn_t **htxn, ham_env_t *henv, const char *name,
                void *reserved, ham_u32_t flags)
{
    Transaction **txn=(Transaction **)htxn;

    if (!txn) {
        ham_trace(("parameter 'txn' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    *txn=NULL;

    if (!henv) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    Environment *env=(Environment *)henv;

    ScopedLock lock;
    if (!(flags&HAM_DONT_LOCK))
        lock=ScopedLock(env->get_mutex());

    if (env->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    if (!(env->get_flags()&HAM_ENABLE_TRANSACTIONS)) {
        ham_trace(("transactions are disabled (see HAM_ENABLE_TRANSACTIONS)"));
        return (HAM_INV_PARAMETER);
    }
    if (!env->_fun_txn_begin) {
        ham_trace(("Environment was not initialized"));
        return (HAM_NOT_INITIALIZED);
    }

    /* initialize the txn structure */
    return (env->_fun_txn_begin(env, txn, name, flags));
}

HAM_EXPORT const char *
ham_txn_get_name(ham_txn_t *htxn)
{
    Transaction *txn=(Transaction *)htxn;
    if (!txn)
        return (0);
    ScopedLock lock(txn_get_env(txn)->get_mutex());
    return (txn_get_name(txn));
}

ham_status_t
ham_txn_commit(ham_txn_t *htxn, ham_u32_t flags)
{
    Transaction *txn=(Transaction *)htxn;
    if (!txn) {
        ham_trace(("parameter 'txn' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    Environment *env=txn_get_env(txn);
    if (!env || !env->_fun_txn_commit) {
        ham_trace(("Environment was not initialized"));
        return (HAM_NOT_INITIALIZED);
    }

    ScopedLock lock;
    if (!(flags&HAM_DONT_LOCK))
        lock=ScopedLock(env->get_mutex());

    if (env->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    /* mark this transaction as committed; will also call
     * env->signal_commit() to write committed transactions
     * to disk */
    return (env->_fun_txn_commit(env, txn, flags));
}

ham_status_t
ham_txn_abort(ham_txn_t *htxn, ham_u32_t flags)
{
    Transaction *txn=(Transaction *)htxn;
    if (!txn) {
        ham_trace(("parameter 'txn' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    Environment *env=txn_get_env(txn);
    if (!env || !env->_fun_txn_abort) {
        ham_trace(("Environment was not initialized"));
        return (HAM_NOT_INITIALIZED);
    }

    ScopedLock lock;
    if (!(flags&HAM_DONT_LOCK))
        lock=ScopedLock(env->get_mutex());

    if (env->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    return (env->_fun_txn_abort(env, txn, flags));
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
        case HAM_TXN_CONFLICT:
            return ("Operation conflicts with another Transaction");
        case HAM_TXN_STILL_OPEN:
            return ("Database cannot be closed because it is modified in a "
                    "Transaction");
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
        case HAM_ASYNCHRONOUS_ERROR_PENDING:
            return ("Failure in background thread; see "
                    "ham_env_get_asnychronous_error()");
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
static inline ham_bool_t
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
static inline ham_bool_t
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
__check_create_parameters(Environment *env, Database *db, const char *filename,
        ham_u32_t *pflags, const ham_parameter_t *param,
        ham_size_t *ppagesize, ham_u16_t *pkeysize,
        ham_u64_t *pcachesize, ham_u16_t *pdbname,
        ham_u16_t *pmaxdbs, ham_u16_t *pdata_access_mode,
        std::string &logdir, bool create)
{
    ham_size_t pagesize=0;
    ham_u16_t keysize=0;
    ham_u16_t dbname=HAM_DEFAULT_DATABASE_NAME;
    ham_u64_t cachesize=0;
    ham_bool_t no_mmap=HAM_FALSE;
    ham_u16_t dbs=0;
    ham_u16_t dam=0;
    ham_u32_t flags = 0;
    ham_bool_t set_abs_max_dbs = HAM_FALSE;
    ham_status_t st = 0;
    Device *device = NULL;

    if (!env && db)
        env = db->get_env();

    if (pflags)
        flags = *pflags;
    else if (db)
        flags = db->get_rt_flags();
    else if (env)
        flags = env->get_flags();

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
    if (!create && (flags & HAM_IN_MEMORY)) {
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
    if (create && env && db && (env->get_flags() & HAM_READ_ONLY)) {
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
     * combination with HAM_ENABLE_DUPLICATES, but not with Transactions
     */
    if (create && (flags&HAM_SORT_DUPLICATES)) {
        if (!(flags&HAM_ENABLE_DUPLICATES)) {
            ham_trace(("flag HAM_SORT_DUPLICATES only allowed in combination "
                        "with HAM_ENABLE_DUPLICATES"));
            return (HAM_INV_PARAMETER);
        }
        if (flags&HAM_ENABLE_TRANSACTIONS) {
            ham_trace(("flag HAM_SORT_DUPLICATES not allowed in combination "
                        "with HAM_ENABLE_TRANSACTIONS"));
            return (HAM_INV_PARAMETER);
        }
    }

    /*
     * DB create: only a few flags are allowed
     */
    if (db && (flags & ~((!create ? HAM_READ_ONLY : 0)
                        |(create ? HAM_IN_MEMORY : 0)
                        |(!env ? (HAM_WRITE_THROUGH
                                |HAM_DISABLE_MMAP
                                |HAM_DISABLE_FREELIST_FLUSH
                                |HAM_CACHE_UNLIMITED
                                |HAM_DONT_LOCK
                                |HAM_LOCK_EXCLUSIVE
                                |HAM_ENABLE_TRANSACTIONS
                                |HAM_ENABLE_RECOVERY) : 0)
                        |(!env && !create ? HAM_AUTO_RECOVERY : 0)
                        |HAM_CACHE_STRICT
                        |HAM_USE_BTREE
                        |HAM_DONT_LOCK
                        |HAM_DISABLE_VAR_KEYLEN
                        |HAM_ENABLE_ASYNCHRONOUS_FLUSH
                        |HAM_RECORD_NUMBER
                        |HAM_SORT_DUPLICATES
                        |(create ? HAM_ENABLE_DUPLICATES : 0))))
    {
        char msgbuf[2048];
        ham_trace(("invalid flags specified: %s",
                ham_create_flags2str(msgbuf, sizeof(msgbuf),
                (flags & ~((!create ? HAM_READ_ONLY : 0)
                        |(create ? HAM_IN_MEMORY : 0)
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
                        |HAM_ENABLE_ASYNCHRONOUS_FLUSH
                        |(create ? HAM_ENABLE_DUPLICATES : 0))))));
        return (HAM_INV_PARAMETER);
    }

    if (env)
        flags |= env->get_flags();

    /*
     * parse parameters
     */
    if (param) {
        for (; param->name; param++) {
            switch (param->name) {
            case HAM_PARAM_CACHESIZE:
                if (pcachesize)
                    cachesize=param->value;
                break;

            case HAM_PARAM_LOG_DIRECTORY:
                logdir=(const char *)param->value;
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
                        return (HAM_INV_PAGESIZE);
                    }
                    pagesize=(ham_size_t)param->value;
                    break;
                }
                goto default_case;

            case HAM_PARAM_DATA_ACCESS_MODE:
                /* not allowed for Environments, only for Databases */
                if (!db) {
                    ham_trace(("invalid parameter HAM_PARAM_DATA_ACCESS_MODE"));
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
        if (!filename && !(flags&HAM_IN_MEMORY)) {
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
    if (flags&HAM_IN_MEMORY) {
        if (flags&HAM_CACHE_STRICT) {
            ham_trace(("combination of HAM_IN_MEMORY and HAM_CACHE_STRICT "
                        "not allowed"));
            return (HAM_INV_PARAMETER);
        }
        if (cachesize!=0) {
            ham_trace(("combination of HAM_IN_MEMORY and cachesize != 0 "
                        "not allowed"));
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
            return (HAM_INV_PARAMETER);
        }
    }

    /*
     * if this is not the first database we're creating (or opening),
     * we'd better copy the pagesize values from the env / device
     */
    if (env)
        device = env->get_device();

    /*
     * inherit defaults from ENV for DB
     */
    if (env && env->is_active()) {
        if (!cachesize)
            cachesize = env->get_cachesize();
        if (!dbs && env->get_header_page())
            dbs=env->get_max_databases();
        if (!pagesize)
            pagesize = env->get_pagesize();
    }

    if (!pagesize && device)
        pagesize = device->get_pagesize();

    /*
     * in-memory-db? use a default pagesize of 16kb
     */
    if (flags&HAM_IN_MEMORY) {
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
            keysize = DB_CHUNKSIZE - (BtreeKey::ms_sizeof_overhead);
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
        return (HAM_INV_KEYSIZE);
    }

    /*
     * make sure that max_databases actually fit in a header
     * page!
     * leave at least 128 bytes for the freelist and the other header data
     */
    {
        ham_size_t l = pagesize - sizeof(env_header_t)
                - db_get_freelist_header_size() - 128;

        l /= sizeof(db_indexdata_t);
        if (dbs > l) {
            ham_trace(("parameter HAM_PARAM_MAX_ENV_DATABASES too high for "
                        "this pagesize; the maximum allowed is %u",
                        (unsigned)l));
            return (HAM_INV_PARAMETER);
        }
        /* override assignment when 'env' already has been configured with a
         * non-default maxdbs value of its own */
        if (env && !db && env->get_header_page()
                && env->get_max_databases()>0) {
            dbs = env->get_max_databases();
        }
        else if (db
            && db->get_env()
            && env->get_device()
            && env->get_device()->is_open()) {
            dbs = (env ? env->get_max_databases() : 1);
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
    ham_assert(dbs != 0);

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

ham_status_t HAM_CALLCONV
ham_env_new(ham_env_t **env)
{
    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    *env=(ham_env_t *)new Environment();
    if (!(*env))
        return (HAM_OUT_OF_MEMORY);

    return (HAM_SUCCESS);
}

ham_status_t HAM_CALLCONV
ham_env_delete(ham_env_t *henv)
{
    if (!henv) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    Environment *env=(Environment *)henv;

    delete (Environment *)env;

    return (0);
}

ham_status_t HAM_CALLCONV
ham_env_create(ham_env_t *env, const char *filename,
        ham_u32_t flags, ham_u32_t mode)
{
    return (ham_env_create_ex(env, filename, flags, mode, 0));
}

ham_status_t HAM_CALLCONV
ham_env_create_ex(ham_env_t *henv, const char *filename,
        ham_u32_t flags, ham_u32_t mode, const ham_parameter_t *param)
{
    ham_status_t st;
    ham_size_t pagesize = 0;
    ham_u16_t keysize = 0;
    ham_u64_t cachesize = 0;
    ham_u16_t maxdbs = 0;
    std::string logdir;
    Environment *env=(Environment *)henv;

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    if ((flags&HAM_ENABLE_ASYNCHRONOUS_FLUSH)
            && !(flags&HAM_ENABLE_TRANSACTIONS)) {
        ham_trace(("flag HAM_ENABLE_ASYNCHRONOUS_FLUSH only allowed if "
                "Transactions are enabled"));
        return (HAM_INV_PARAMETER);
    }

    ScopedLock lock(env->get_mutex());

#if HAM_ENABLE_REMOTE
    atexit(curl_global_cleanup);
    atexit(Protocol::shutdown);
#endif

    /*
     * make sure that this environment is not yet open/created
     */
    if (env->is_active()) {
        ham_trace(("parameter 'env' is already initialized"));
        return (HAM_ENVIRONMENT_ALREADY_OPEN);
    }

    env->set_flags(0);

    /* check (and modify) the parameters */
    st=__check_create_parameters(env, 0, filename, &flags, param,
            &pagesize, &keysize, &cachesize, 0, &maxdbs, 0, logdir, true);
    if (st)
        return (st);

    if (!cachesize)
        cachesize=HAM_DEFAULT_CACHESIZE;
    if (logdir.size())
        env->set_log_directory(logdir);

    /*
     * if we do not yet have an allocator: create a new one
     */
    if (!env->get_allocator()) {
        env->set_allocator(Allocator::create());
        if (!env->get_allocator())
            return (HAM_OUT_OF_MEMORY);
    }

    /* store the parameters */
    env->set_flags(flags);
    env->set_pagesize(pagesize);
    env->set_cachesize(cachesize);
    env->set_file_mode(mode);
    env->set_max_databases_cached(maxdbs);
    if (filename)
        env->set_filename(filename);

    /* initialize function pointers */
    if (__filename_is_local(filename)) {
        st=env_initialize_local(env);
    }
    else {
        st=env_initialize_remote(env);
    }
    if (st)
        return (st);

    /* and finish the initialization of the Environment */
    st=env->_fun_create(env, filename, flags, mode, param);
    if (st)
        return (st);

    env->set_active(true);

    /* flush the environment to make sure that the header page is written
     * to disk */
    return (ham_env_flush((ham_env_t *)env, HAM_DONT_LOCK));
}

ham_status_t HAM_CALLCONV
ham_env_create_db(ham_env_t *henv, ham_db_t *hdb,
        ham_u16_t dbname, ham_u32_t flags, const ham_parameter_t *param)
{
    ham_status_t st;
    Database *db=(Database *)hdb;
    Environment *env=(Environment *)henv;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    ScopedLock lock(env->get_mutex());

    if (env->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    if (env->is_private()) {
        ham_trace(("Environment was not properly created with ham_env_create, "
                   "ham_env_open"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    /* make sure that this database is not yet open/created */
    if (db->is_active()) {
        ham_trace(("parameter 'db' is already initialized"));
        return (db->set_error(HAM_DATABASE_ALREADY_OPEN));
    }
    if (!dbname || (dbname>HAM_DEFAULT_DATABASE_NAME
            && dbname!=HAM_DUMMY_DATABASE_NAME)) {
        ham_trace(("invalid database name"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    /*
     * the function handler will do the rest
     */
    st=env->_fun_create_db(env, db, dbname, flags, param);
    if (st)
        return (st);

    db->set_active(HAM_TRUE);

    /* flush the environment to make sure that the header page is written
     * to disk */
    return (ham_env_flush((ham_env_t *)env, HAM_DONT_LOCK));
}

ham_status_t HAM_CALLCONV
ham_env_open_db(ham_env_t *henv, ham_db_t *hdb,
        ham_u16_t dbname, ham_u32_t flags, const ham_parameter_t *param)
{
    ham_status_t st;
    Database *db=(Database *)hdb;
    Environment *env=(Environment *)henv;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    ScopedLock lock;
    if (!(flags&HAM_DONT_LOCK))
        lock=ScopedLock(env->get_mutex());

    if (env->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    if (env->is_private()) {
        ham_trace(("Environment was not properly created with ham_env_create, "
                   "ham_env_open"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if (!dbname) {
        ham_trace(("parameter 'dbname' must not be 0"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if (dbname!=HAM_FIRST_DATABASE_NAME
          && (dbname!=HAM_DUMMY_DATABASE_NAME
                && dbname>HAM_DEFAULT_DATABASE_NAME)) {
        ham_trace(("database name must be lower than 0xf000"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if (env->get_flags()&HAM_IN_MEMORY) {
        ham_trace(("cannot open a Database in an In-Memory Environment"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if (flags&HAM_SORT_DUPLICATES
            && env->get_flags()&HAM_ENABLE_TRANSACTIONS) {
        ham_trace(("flag HAM_SORT_DUPLICATES not allowed in combination "
                    "with HAM_ENABLE_TRANSACTIONS"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    /* the function handler will do the rest */
    st=env->_fun_open_db(env, db, dbname, flags, param);
    if (st)
        return (st);

    db->set_active(HAM_TRUE);

    return (db->set_error(0));
}

ham_status_t HAM_CALLCONV
ham_env_open(ham_env_t *env, const char *filename, ham_u32_t flags)
{
    return (ham_env_open_ex(env, filename, flags, 0));
}

ham_status_t HAM_CALLCONV
ham_env_open_ex(ham_env_t *henv, const char *filename,
        ham_u32_t flags, const ham_parameter_t *param)
{
    ham_status_t st;
    ham_u64_t cachesize=0;
    std::string logdir;
    Environment *env=(Environment *)henv;

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    if ((flags&HAM_ENABLE_ASYNCHRONOUS_FLUSH)
            && !(flags&HAM_ENABLE_TRANSACTIONS)) {
        ham_trace(("flag HAM_ENABLE_ASYNCHRONOUS_FLUSH only allowed if "
                "Transactions are enabled"));
        return (HAM_INV_PARAMETER);
    }

    ScopedLock lock(env->get_mutex());

#if HAM_ENABLE_REMOTE
    atexit(curl_global_cleanup);
    atexit(Protocol::shutdown);
#endif

    /* make sure that this environment is not yet open/created */
    if (env->is_active()) {
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

    env->set_flags(0);

    /* parse parameters */
    st=__check_create_parameters(env, 0, filename, &flags, param,
            0, 0, &cachesize, 0, 0, 0, logdir, false);
    if (st)
        return (st);

    if (logdir.size())
        env->set_log_directory(logdir);

    /*
     * if we do not yet have an allocator: create a new one
     */
    if (!env->get_allocator()) {
        env->set_allocator(Allocator::create());
        if (!env->get_allocator())
            return (HAM_OUT_OF_MEMORY);
    }

    /*
     * store the parameters
     */
    env->set_pagesize(0);
    env->set_cachesize(cachesize);
    env->set_flags(flags);
    env->set_file_mode(0644);
    if (filename)
        env->set_filename(filename);

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

    env->set_active(true);

    return (st);
}

ham_status_t HAM_CALLCONV
ham_env_rename_db(ham_env_t *henv, ham_u16_t oldname,
                ham_u16_t newname, ham_u32_t flags)
{
    Environment *env=(Environment *)henv;
    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    ScopedLock lock(env->get_mutex());

    if (env->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
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
ham_env_erase_db(ham_env_t *henv, ham_u16_t name, ham_u32_t flags)
{
    Environment *env=(Environment *)henv;
    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    ScopedLock lock(env->get_mutex());

    if (env->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
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
ham_env_add_file_filter(ham_env_t *henv, ham_file_filter_t *filter)
{
    Environment *env=(Environment *)henv;
    ham_file_filter_t *head;

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    ScopedLock lock(env->get_mutex());

    if (env->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    if (env->get_flags()&DB_IS_REMOTE) {
        ham_trace(("ham_env_add_file_filter is not supported by remote "
                "servers"));
        return (HAM_NOT_IMPLEMENTED);
    }
    if (!filter) {
        ham_trace(("parameter 'filter' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    head=env->get_file_filter();

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
        env->set_file_filter(filter);
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
ham_env_remove_file_filter(ham_env_t *henv, ham_file_filter_t *filter)
{
    Environment *env=(Environment *)henv;
    ham_file_filter_t *head, *prev;

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    ScopedLock lock(env->get_mutex());

    if (env->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    if (!filter) {
        ham_trace(("parameter 'filter' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (env->get_flags()&DB_IS_REMOTE) {
        ham_trace(("ham_env_add_file_filter is not supported by remote "
                "servers"));
        return (HAM_NOT_IMPLEMENTED);
    }

    head=env->get_file_filter();

    if (head == filter) {
        if (head->_next) {
            ham_assert(head->_prev != head);
            head->_next->_prev = head->_prev;
        }
        env->set_file_filter(head->_next);
        return 0;
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
        return (HAM_FILTER_NOT_FOUND);

    filter->_next = 0;
    filter->_prev = 0;

    return (0);
}

ham_status_t HAM_CALLCONV
ham_env_get_database_names(ham_env_t *henv, ham_u16_t *names, ham_size_t *count)
{
    Environment *env=(Environment *)henv;
    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    ScopedLock lock(env->get_mutex());

    if (env->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
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

    /* get all database names */
    return (env->_fun_get_database_names(env, names, count));
}

HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_get_parameters(ham_env_t *henv, ham_parameter_t *param)
{
    Environment *env=(Environment *)henv;
    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    ScopedLock lock(env->get_mutex());

    if (env->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    if (!param) {
        ham_trace(("parameter 'param' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!env->_fun_get_parameters) {
        ham_trace(("Environment was not initialized"));
        return (HAM_NOT_INITIALIZED);
    }

    /* get the parameters */
    return (env->_fun_get_parameters(env, param));
}

ham_status_t HAM_CALLCONV
ham_env_flush(ham_env_t *henv, ham_u32_t flags)
{
    Environment *env=(Environment *)henv;
    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return HAM_INV_PARAMETER;
    }

    ScopedLock lock;
    if (!(flags&HAM_DONT_LOCK))
        lock=ScopedLock(env->get_mutex());

    if (env->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    if (!env->_fun_flush) {
        ham_trace(("Environment was not initialized"));
        return (HAM_NOT_INITIALIZED);
    }

    /* flush the Environment */
    return (env->_fun_flush(env, flags));
}

ham_status_t HAM_CALLCONV
ham_env_close(ham_env_t *henv, ham_u32_t flags)
{
    Environment *env=(Environment *)henv;
    ham_status_t st;

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    ScopedLock lock;
    if (!(flags&HAM_DONT_LOCK)) {
        // join the worker thread before the mutex is locked
        if (env->get_worker_thread())
            env->get_worker_thread()->join();
        lock=ScopedLock(env->get_mutex());
    }

    if (env->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    if (env->get_worker_thread()) {
        delete env->get_worker_thread();
        env->set_worker_thread(0);
    }

    /* it's ok to close an uninitialized Environment */
    if (!env->_fun_close)
        return (0);

    /* make sure that the changeset is empty */
    ham_assert(env->get_changeset().is_empty());

    /* auto-abort (or commit) all pending transactions */
    if (env && env->get_newest_txn()) {
        Transaction *n, *t=env->get_newest_txn();
        while (t) {
            n=txn_get_older(t);
            if ((txn_get_flags(t)&TXN_STATE_ABORTED)
                    || (txn_get_flags(t)&TXN_STATE_COMMITTED))
                ; /* nop */
            else {
                if (flags&HAM_TXN_AUTO_COMMIT) {
                    if ((st=ham_txn_commit((ham_txn_t *)t, HAM_DONT_LOCK)))
                        return (st);
                }
                else { /* if (flags&HAM_TXN_AUTO_ABORT) */
                    if ((st=ham_txn_abort((ham_txn_t *)t, HAM_DONT_LOCK)))
                        return (st);
                }
            }
            t=n;
        }
    }

    /* flush all committed transactions */
    st=env->flush_committed_txns(true);
    if (st)
        return (st);

    /* close all databases?  */
    if (env->get_databases()) {
        Database *db=env->get_databases();
        while (db) {
            Database *next=db->get_next();
            st=ham_close((ham_db_t *)db, flags|HAM_DONT_LOCK);
            if (st)
                return (st);
            db=next;
        }
        env->set_databases(0);
    }

    st=env->signal_commit();
    if (st)
        return (st);

    /* close the environment */
    st=env->_fun_close(env, flags);
    if (st)
        return (st);

    /* when all transactions have been properly closed... */
    if (env->is_active() && env->get_oldest_txn()) {
        ham_assert(!"Should never get here; the db close loop above "
                    "should've taken care of all TXNs");
        return (HAM_INTERNAL_ERROR);
    }

    /* finally, close the memory allocator */
    if (env->get_allocator()) {
        delete env->get_allocator();
        env->set_allocator(0);
    }

    env->set_active(false);

    return (0);
}

ham_status_t HAM_CALLCONV
ham_new(ham_db_t **hdb)
{
    Database **db=(Database **)hdb;
    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    *db=new Database();
    return (HAM_SUCCESS);
}

ham_status_t HAM_CALLCONV
ham_delete(ham_db_t *hdb)
{
    Database *db=(Database *)hdb;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    delete (Database *)db;
    return (0);
}

ham_status_t HAM_CALLCONV
ham_open(ham_db_t *db, const char *filename, ham_u32_t flags)
{
    return (ham_open_ex(db, filename, flags, 0));
}

ham_status_t HAM_CALLCONV
ham_open_ex(ham_db_t *hdb, const char *filename,
        ham_u32_t flags, const ham_parameter_t *param)
{
    ham_status_t st;
    ham_u16_t dbname=HAM_FIRST_DATABASE_NAME;
    ham_u64_t cachesize=0;
    ham_u16_t dam = 0;
    ham_env_t *env;
    ham_u32_t env_flags;
    std::string logdir;
    ham_parameter_t env_param[8]={{0, 0}};
    ham_parameter_t db_param[8]={{0, 0}};
    Database *db=(Database *)hdb;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    /*
     * make sure that this database is not yet open/created
     */
    if (db->is_active()) {
        ham_trace(("parameter 'db' is already initialized"));
        return (HAM_DATABASE_ALREADY_OPEN);
    }

    /* parse parameters */
    st=__check_create_parameters(db->get_env(), db, filename, &flags, param,
            0, 0, &cachesize, &dbname, 0, &dam, logdir, false);
    if (st)
        return (st);

    db->set_error(0);
    db->set_rt_flags(0);

    /*
     * create an Environment handle and open the Environment
     */
    env_param[0].name=HAM_PARAM_CACHESIZE;
    env_param[0].value=cachesize;
    if (logdir.size()) {
        env_param[1].name=HAM_PARAM_LOG_DIRECTORY;
        env_param[1].value=(ham_u64_t)logdir.c_str();
    }
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

    /* now open the Database in this Environment */
    st=ham_env_open_db(env, (ham_db_t *)db, dbname, flags, db_param);
    if (st)
        goto bail;

    /*
     * this Environment is 0wned by the Database (and will be deleted in
     * ham_close)
     */
    db->set_rt_flags(db->get_rt_flags()|DB_ENV_IS_PRIVATE);

bail:
    if (st) {
        if (db)
            (void)ham_close((ham_db_t *)db, 0);
        if (env) {
            /* despite the IS_PRIVATE the env will destroy the DB,
            which is the responsibility of the caller: detach the DB now. */
            ((Environment *)env)->set_databases(0);
            (void)ham_env_close(env, 0);
            delete (Environment *)env;
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
ham_create_ex(ham_db_t *hdb, const char *filename,
        ham_u32_t flags, ham_u32_t mode, const ham_parameter_t *param)
{
    ham_status_t st;
    ham_u16_t dam=(flags & HAM_RECORD_NUMBER)
        ? HAM_DAM_SEQUENTIAL_INSERT
        : HAM_DAM_RANDOM_WRITE;
    Database *db=(Database *)hdb;

    ham_size_t pagesize = 0;
    ham_u16_t maxdbs = 0;
    ham_u16_t keysize = 0;
    ham_u16_t dbname = HAM_DEFAULT_DATABASE_NAME;
    ham_u64_t cachesize = 0;
    ham_env_t *env=0;
    ham_u32_t env_flags;
    std::string logdir;
    ham_parameter_t env_param[8]={{0, 0}};
    ham_parameter_t db_param[5]={{0, 0}};

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    /*
     * make sure that this database is not yet open/created
     */
    if (db->is_active()) {
        ham_trace(("parameter 'db' is already initialized"));
        return (db->set_error(HAM_DATABASE_ALREADY_OPEN));
    }

    /*
     * check (and modify) the parameters
     */
    st=__check_create_parameters(db->get_env(), db, filename, &flags, param,
            &pagesize, &keysize, &cachesize, &dbname, &maxdbs, &dam,
            logdir, true);
    if (st)
        return (db->set_error(st));

    db->set_error(0);
    db->set_rt_flags(0);

    /*
     * setup the parameters for ham_env_create_ex
     */
    env_param[0].name=HAM_PARAM_CACHESIZE;
    env_param[0].value=(flags&HAM_IN_MEMORY) ? 0 : cachesize;
    env_param[1].name=HAM_PARAM_PAGESIZE;
    env_param[1].value=pagesize;
    env_param[2].name=HAM_PARAM_MAX_ENV_DATABASES;
    env_param[2].value=maxdbs;
    if (logdir.size()) {
        env_param[3].name=HAM_PARAM_LOG_DIRECTORY;
        env_param[3].value=(ham_u64_t)logdir.c_str();
    }
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
            |HAM_IN_MEMORY
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

    /* now create the Database */
    st=ham_env_create_db(env, (ham_db_t *)db,
            HAM_DEFAULT_DATABASE_NAME, flags, db_param);
    if (st)
        goto bail;

    /*
     * this Environment is 0wned by the Database (and will be deleted in
     * ham_close)
     */
    db->set_rt_flags(db->get_rt_flags()|DB_ENV_IS_PRIVATE);

bail:
    if (st) {
        if (db) {
            (void)ham_close((ham_db_t *)db, 0);
        }
        if (env) {
            /* despite the IS_PRIVATE the env will destroy the DB,
            which is the responsibility of the caller: detach the DB now. */
            ((Environment *)env)->set_databases(0);
            (void)ham_env_close(env, 0);
            delete (Environment *)env;
        }
    }

    return (db->set_error(st));
}

HAM_EXPORT ham_status_t HAM_CALLCONV
ham_get_parameters(ham_db_t *hdb, ham_parameter_t *param)
{
    Database *db=(Database *)hdb;
    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return HAM_INV_PARAMETER;
    }

    ScopedLock lock(db->get_env()->get_mutex());

    if (!param) {
        ham_trace(("parameter 'param' must not be NULL"));
        return HAM_INV_PARAMETER;
    }

    /* get the parameters */
    return ((*db)()->get_parameters(param));
}

ham_status_t HAM_CALLCONV
ham_get_error(ham_db_t *hdb)
{
    Database *db=(Database *)hdb;
    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (0);
    }

    ScopedLock lock;
    if (db->get_env())
        lock=ScopedLock(db->get_env()->get_mutex());

    return (db->get_error());
}

ham_status_t HAM_CALLCONV
ham_env_get_asnychronous_error(ham_env_t *henv)
{
    Environment *env=(Environment *)henv;
    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (0);
    }

    ScopedLock lock=ScopedLock(env->get_mutex());
    return (env->get_and_reset_worker_error());
}

ham_status_t HAM_CALLCONV
ham_set_prefix_compare_func(ham_db_t *hdb, ham_prefix_compare_func_t foo)
{
    Database *db=(Database *)hdb;
    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    ScopedLock lock(db->get_env()->get_mutex());

    db->set_error(0);
    db->set_prefix_compare_func(foo);
    return (db->set_error(HAM_SUCCESS));
}

ham_status_t HAM_CALLCONV
ham_set_compare_func(ham_db_t *hdb, ham_compare_func_t foo)
{
    Database *db=(Database *)hdb;
    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    ScopedLock lock;
    if (db->get_env())
        lock=ScopedLock(db->get_env()->get_mutex());

    db->set_error(0);
    db->set_compare_func(foo ? foo : db_default_compare);
    return (db->set_error(HAM_SUCCESS));
}

ham_status_t HAM_CALLCONV
ham_set_duplicate_compare_func(ham_db_t *hdb, ham_duplicate_compare_func_t foo)
{
    Database *db=(Database *)hdb;
    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    ScopedLock lock(db->get_env()->get_mutex());

    db->set_error(0);
    db->set_duplicate_compare_func(foo ? foo : db_default_compare);
    return (db->set_error(HAM_SUCCESS));
}

#ifndef HAM_DISABLE_ENCRYPTION
static ham_status_t
__aes_before_write_cb(ham_env_t *henv, ham_file_filter_t *filter,
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
__aes_after_read_cb(ham_env_t *henv, ham_file_filter_t *filter,
        ham_u8_t *page_data, ham_size_t page_size)
{
    ham_size_t i;
    ham_size_t blocks=page_size/16;

    ham_assert(page_size%16==0);

    for (i = 0; i < blocks; i++) {
        aes_decrypt(&page_data[i*16], (ham_u8_t *)filter->userdata,
                &page_data[i*16]);
    }

    return (HAM_SUCCESS);
}

static void
__aes_close_cb(ham_env_t *henv, ham_file_filter_t *filter)
{
    Environment *env=(Environment *)henv;
    Allocator *alloc=env->get_allocator();

    ham_assert(alloc);

    if (filter) {
        if (filter->userdata) {
            /*
             * destroy the secret key in RAM (free() won't do that,
             * so NIL the key space first!
             */
            memset(filter->userdata, 0, sizeof(ham_u8_t)*16);
            alloc->free(filter->userdata);
        }
        alloc->free(filter);
    }
}
#endif /* !HAM_DISABLE_ENCRYPTION */

ham_status_t HAM_CALLCONV
ham_env_enable_encryption(ham_env_t *henv, ham_u8_t key[16], ham_u32_t flags)
{
#ifndef HAM_DISABLE_ENCRYPTION
    Environment *env=(Environment *)henv;
    ham_file_filter_t *filter;
    Allocator *alloc;
    ham_u8_t buffer[128];
    Device *device;
    ham_status_t st;
    ham_db_t *db=0;

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    {
    ScopedLock lock(env->get_mutex());

    if (env->get_databases()) {
        ham_trace(("cannot enable encryption if databases are already open"));
        return (HAM_DATABASE_ALREADY_OPEN);
    }
    if (env->get_flags()&DB_IS_REMOTE) {
        ham_trace(("ham_env_enable_encryption is not supported by remote "
                "servers"));
        return (HAM_NOT_IMPLEMENTED);
    }
    if (env->get_flags()&HAM_IN_MEMORY)
        return (0);

    device=env->get_device();

    alloc=env->get_allocator();
    if (!alloc) {
        ham_trace(("called ham_env_enable_encryption before "
                    "ham_env_create/open"));
        return (HAM_NOT_INITIALIZED);
    }

    /*
     * make sure that we don't already have AES filtering
     */
    filter=env->get_file_filter();
    while (filter) {
        if (filter->before_write_cb==__aes_before_write_cb)
            return (HAM_ALREADY_INITIALIZED);
        filter=filter->_next;
    }

    filter=(ham_file_filter_t *)alloc->alloc(sizeof(*filter));
    if (!filter)
        return (HAM_OUT_OF_MEMORY);
    memset(filter, 0, sizeof(*filter));

    filter->userdata=alloc->alloc(256);
    if (!filter->userdata) {
        alloc->free(filter);
        return (HAM_OUT_OF_MEMORY);
    }

    /*
     * need a temporary database handle to read from the device
     */
    st=ham_new(&db);
    if (st)
        return (st);
    st=ham_env_open_db((ham_env_t *)env, db, HAM_FIRST_DATABASE_NAME,
                        HAM_DONT_LOCK, 0);
    if (st) {
        delete (Database *)db;
        db=0;
    }

    aes_expand_key(key, (ham_u8_t *)filter->userdata);
    filter->before_write_cb=__aes_before_write_cb;
    filter->after_read_cb=__aes_after_read_cb;
    filter->close_cb=__aes_close_cb;

    /*
     * if the database file already exists (i.e. if it's larger than
     * one page): try to read the header of the next page and decrypt
     * it; if it's garbage, the key is wrong and we return an error
     */
    if (db) {
        PageHeader *ph;

        st=device->read(env->get_pagesize(), buffer, sizeof(buffer));
        if (st==0) {
            st=__aes_after_read_cb((ham_env_t *)env, filter,
                                buffer, sizeof(buffer));
            if (st)
                goto bail;
            ph=(PageHeader *)buffer;
            if (ph->_reserved1 || ph->_reserved2) {
                st=HAM_ACCESS_DENIED;
                goto bail;
            }
        }
    }
    else
        st=0;

bail:
    if (st)
        __aes_close_cb((ham_env_t *)env, filter);

    } // ScopedLock

    if (db) {
        ham_close(db, 0);
        delete (Database *)db;
    }

    if (st)
        return (st);

    return (ham_env_add_file_filter((ham_env_t *)env, filter));
#else /* !HAM_DISABLE_ENCRYPTION */
    ham_trace(("hamsterdb was compiled without support for AES encryption"));
    return (HAM_NOT_IMPLEMENTED);
#endif
}

#ifndef HAM_DISABLE_COMPRESSION
static ham_status_t
__zlib_before_write_cb(ham_db_t *hdb, ham_record_filter_t *filter,
        ham_record_t *record)
{
    Database *db=(Database *)hdb;
    Environment *env=db->get_env();
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

        dest=(ham_u8_t *)env->get_allocator()->alloc(newsize);
        if (!dest)
            return (db->set_error(HAM_OUT_OF_MEMORY));

        newsize-=sizeof(ham_u32_t);
        zret=compress2(dest+sizeof(ham_u32_t), &newsize,
                (ham_u8_t *)record->data, record->size, level);
    } while (zret==Z_BUF_ERROR);

    newsize+=sizeof(ham_u32_t);
    *(ham_u32_t *)dest=ham_h2db32(record->size);

    if (zret==Z_MEM_ERROR) {
        env->get_allocator()->free(dest);
        return (db->set_error(HAM_OUT_OF_MEMORY));
    }

    if (zret!=Z_OK) {
        ham_log(("zlib compression failed with error %d", (int)zret));
        env->get_allocator()->free(dest);
        return (db->set_error(HAM_INTERNAL_ERROR));
    }

    record->data=dest;
    record->size=(ham_size_t)newsize;

    return (db->set_error(0));
}

static ham_status_t
__zlib_after_read_cb(ham_db_t *hdb, ham_record_filter_t *filter,
        ham_record_t *record)
{
    Database *db=(Database *)hdb;
    Environment *env=db->get_env();
    ham_status_t st=0;
    ham_u8_t *src;
    ham_size_t srcsize=record->size;
    unsigned long newsize=record->size-sizeof(ham_u32_t);
    ham_u32_t origsize;
    int zret;

    if (!record->size)
        return (db->set_error(0));

    ByteArray *arena=&db->get_record_arena();

    origsize=ham_db2h32(*(ham_u32_t *)record->data);

    /* don't allow HAM_RECORD_USER_ALLOC */
    if (record->flags&HAM_RECORD_USER_ALLOC) {
        ham_trace(("compression not allowed in combination with "
                    "HAM_RECORD_USER_ALLOC"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    src=(ham_u8_t *)env->get_allocator()->alloc(newsize);
    if (!src)
        return (db->set_error(HAM_OUT_OF_MEMORY));

    memcpy(src, (char *)record->data+4, newsize);

    arena->resize(origsize);
    record->data=arena->get_ptr();
    newsize=origsize;

    zret=uncompress((Bytef *)record->data, &newsize, (Bytef *)src, srcsize);
    if (zret==Z_MEM_ERROR)
        st=HAM_LIMITS_REACHED;
    if (zret==Z_DATA_ERROR)
        st=HAM_INTEGRITY_VIOLATED;
    else if (zret==Z_OK) {
        ham_assert(origsize==newsize);
        st=0;
    }
    else {
        ham_log(("zlib uncompress failed with error %d", (int)zret));
        st=HAM_INTERNAL_ERROR;
    }

    if (!st)
        record->size=(ham_size_t)newsize;

    env->get_allocator()->free(src);
    return (db->set_error(st));
}

static void
__zlib_close_cb(ham_db_t *hdb, ham_record_filter_t *filter)
{
    Database *db=(Database *)hdb;
    Environment *env=db->get_env();

    if (filter) {
        if (filter->userdata)
            env->get_allocator()->free(filter->userdata);
        env->get_allocator()->free(filter);
    }
}
#endif /* !HAM_DISABLE_COMPRESSION */

ham_status_t HAM_CALLCONV
ham_enable_compression(ham_db_t *hdb, ham_u32_t level, ham_u32_t flags)
{
#ifndef HAM_DISABLE_COMPRESSION
    Database *db=(Database *)hdb;
    ham_record_filter_t *filter;
    Environment *env;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return HAM_INV_PARAMETER;
    }
    env = db->get_env();
    if (!env) {
        ham_trace(("parameter 'db' must be linked to a valid (implicit or "
                   "explicit) environment"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    {
    ScopedLock lock(env->get_mutex());

    if (env->get_flags()&DB_IS_REMOTE) {
        ham_trace(("ham_enable_compression is not supported by remote "
                "servers"));
        return (HAM_NOT_IMPLEMENTED);
    }
    if (level>9) {
        ham_trace(("parameter 'level' must be lower than or equal to 9"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if (!level)
        level=6;

    db->set_error(0);

    filter=(ham_record_filter_t *)env->get_allocator()->calloc(sizeof(*filter));
    if (!filter)
        return (db->set_error(HAM_OUT_OF_MEMORY));

    filter->userdata=env->get_allocator()->calloc(sizeof(level));
    if (!filter->userdata) {
        env->get_allocator()->free(filter);
        return (db->set_error(HAM_OUT_OF_MEMORY));
    }

    *(ham_u32_t *)filter->userdata=level;
    filter->before_write_cb=__zlib_before_write_cb;
    filter->after_read_cb=__zlib_after_read_cb;
    filter->close_cb=__zlib_close_cb;

    } // ScopedLock

    return (ham_add_record_filter((ham_db_t *)db, filter));
#else /* !HAM_DISABLE_COMPRESSION */
    ham_trace(("hamsterdb was compiled without support for zlib compression"));
    Database *db=(Database *)hdb;
    if (db)
        return (db->set_error(HAM_NOT_IMPLEMENTED));
#endif /* ifndef HAM_DISABLE_COMPRESSION */
}

ham_status_t HAM_CALLCONV
ham_find(ham_db_t *hdb, ham_txn_t *htxn, ham_key_t *key,
                ham_record_t *record, ham_u32_t flags)
{
    Database *db=(Database *)hdb;
    Transaction *txn=(Transaction *)htxn;
    Environment *env;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return HAM_INV_PARAMETER;
    }
    env = db->get_env();
    if (!env) {
        ham_trace(("parameter 'db' must be linked to a valid (implicit "
                   "or explicit) environment"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    ScopedLock lock(env->get_mutex());

    if (env->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    if (!key) {
        ham_trace(("parameter 'key' must not be NULL"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if (!record) {
        ham_trace(("parameter 'record' must not be NULL"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if (flags&HAM_HINT_PREPEND) {
        ham_trace(("flag HAM_HINT_PREPEND is only allowed in "
                    "ham_cursor_insert"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if (flags&HAM_HINT_APPEND) {
        ham_trace(("flag HAM_HINT_APPEND is only allowed in "
                    "ham_cursor_insert"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags&HAM_DIRECT_ACCESS)
            && !(env->get_flags()&HAM_IN_MEMORY)) {
        ham_trace(("flag HAM_DIRECT_ACCESS is only allowed in "
                    "In-Memory Databases"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags&HAM_DIRECT_ACCESS)
            && (env->get_flags()&HAM_ENABLE_TRANSACTIONS)) {
        ham_trace(("flag HAM_DIRECT_ACCESS is not allowed in "
                    "combination with Transactions"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags&HAM_PARTIAL) && (db->get_rt_flags()&HAM_ENABLE_TRANSACTIONS)) {
        ham_trace(("flag HAM_PARTIAL is not allowed in combination with "
                    "transactions"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    /* record number: make sure that we have a valid key structure */
    if (db->get_rt_flags()&HAM_RECORD_NUMBER) {
        if (key->size!=sizeof(ham_u64_t) || !key->data) {
            ham_trace(("key->size must be 8, key->data must not be NULL"));
            return (db->set_error(HAM_INV_PARAMETER));
        }
    }

    if (!__prepare_key(key) || !__prepare_record(record))
        return (db->set_error(HAM_INV_PARAMETER));

    return (db->set_error((*db)()->find(txn, key, record, flags)));
}

int HAM_CALLCONV
ham_key_get_approximate_match_type(ham_key_t *key)
{
    if (key && (ham_key_get_intflags(key) & BtreeKey::KEY_IS_APPROXIMATE)) {
        int rv = (ham_key_get_intflags(key) & BtreeKey::KEY_IS_LT) ? -1 : +1;
        return (rv);
    }

    return (0);
}

ham_status_t HAM_CALLCONV
ham_insert(ham_db_t *hdb, ham_txn_t *htxn, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
    Database *db=(Database *)hdb;
    Transaction *txn=(Transaction *)htxn;
    Environment *env;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return HAM_INV_PARAMETER;
    }
    env=db->get_env();
    if (!env) {
        ham_trace(("parameter 'db' must be linked to a valid (implicit or "
                   "explicit) environment"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    ScopedLock lock;
    if (!(flags&HAM_DONT_LOCK))
        lock=ScopedLock(env->get_mutex());

    if (env->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    if (!key) {
        ham_trace(("parameter 'key' must not be NULL"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if (!record) {
        ham_trace(("parameter 'record' must not be NULL"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if (flags&HAM_HINT_APPEND) {
        ham_trace(("flags HAM_HINT_APPEND is only allowed in "
                    "ham_cursor_insert"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if (flags&HAM_HINT_PREPEND) {
        ham_trace(("flags HAM_HINT_PREPEND is only allowed in "
                    "ham_cursor_insert"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if (db->get_rt_flags()&HAM_READ_ONLY) {
        ham_trace(("cannot insert in a read-only database"));
        return (db->set_error(HAM_DB_READ_ONLY));
    }
    if ((db->get_rt_flags()&HAM_DISABLE_VAR_KEYLEN) &&
            (key->size>db_get_keysize(db))) {
        ham_trace(("database does not support variable length keys"));
        return (db->set_error(HAM_INV_KEYSIZE));
    }
    if ((flags&HAM_OVERWRITE) && (flags&HAM_DUPLICATE)) {
        ham_trace(("cannot combine HAM_OVERWRITE and HAM_DUPLICATE"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags&HAM_PARTIAL) && (db->get_rt_flags()&HAM_ENABLE_TRANSACTIONS)) {
        ham_trace(("flag HAM_PARTIAL is not allowed in combination with "
                    "transactions"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags&HAM_PARTIAL) && (db->get_rt_flags()&HAM_SORT_DUPLICATES)) {
        ham_trace(("flag HAM_PARTIAL is not allowed if duplicates "
                    "are sorted"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags&HAM_PARTIAL) && (record->size<=sizeof(ham_offset_t))) {
        ham_trace(("flag HAM_PARTIAL is not allowed if record->size "
                    "<= 8"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags&HAM_DUPLICATE) && !(db->get_rt_flags()&HAM_ENABLE_DUPLICATES)) {
        ham_trace(("database does not support duplicate keys "
                    "(see HAM_ENABLE_DUPLICATES)"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags&HAM_DUPLICATE_INSERT_AFTER)
            || (flags&HAM_DUPLICATE_INSERT_BEFORE)
            || (flags&HAM_DUPLICATE_INSERT_LAST)
            || (flags&HAM_DUPLICATE_INSERT_FIRST)) {
        ham_trace(("function does not support flags HAM_DUPLICATE_INSERT_*; "
                    "see ham_cursor_insert"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags&HAM_PARTIAL)
            && (record->partial_size+record->partial_offset>record->size)) {
        ham_trace(("partial offset+size is greater than the total "
                    "record size"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags&HAM_PARTIAL) && (record->size<=sizeof(ham_offset_t))) {
        ham_trace(("flag HAM_PARTIAL is not allowed if record->size "
                    "<= 8"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    if (!__prepare_key(key) || !__prepare_record(record))
        return (db->set_error(HAM_INV_PARAMETER));

    /* allocate temp. storage for a recno key */
    if (db->get_rt_flags()&HAM_RECORD_NUMBER) {
        if (flags&HAM_OVERWRITE) {
            if (key->size!=sizeof(ham_u64_t) || !key->data) {
                ham_trace(("key->size must be 8, key->data must not be NULL"));
                return (db->set_error(HAM_INV_PARAMETER));
            }
        }
        else {
            if (key->flags&HAM_KEY_USER_ALLOC) {
                if (!key->data || key->size!=sizeof(ham_u64_t)) {
                    ham_trace(("key->size must be 8, key->data must not "
                                "be NULL"));
                    return (db->set_error(HAM_INV_PARAMETER));
                }
            }
            else {
                if (key->data || key->size) {
                    ham_trace(("key->size must be 0, key->data must be NULL"));
                    return (db->set_error(HAM_INV_PARAMETER));
                }
            }
        }
    }

    return (db->set_error((*db)()->insert(txn, key, record, flags)));
}

ham_status_t HAM_CALLCONV
ham_erase(ham_db_t *hdb, ham_txn_t *htxn, ham_key_t *key, ham_u32_t flags)
{
    Database *db=(Database *)hdb;
    Transaction *txn=(Transaction *)htxn;
    Environment *env;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    env=db->get_env();
    if (!env) {
        ham_trace(("parameter 'db' must be linked to a valid (implicit "
                   "or explicit) environment"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    ScopedLock lock;
    if (!(flags&HAM_DONT_LOCK))
        lock=ScopedLock(env->get_mutex());

    if (env->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    if (!key) {
        ham_trace(("parameter 'key' must not be NULL"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if (flags&HAM_HINT_PREPEND) {
        ham_trace(("flags HAM_HINT_PREPEND is only allowed in "
                    "ham_cursor_insert"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if (flags&HAM_HINT_APPEND) {
        ham_trace(("flags HAM_HINT_APPEND is only allowed in "
                    "ham_cursor_insert"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    if (!__prepare_key(key))
        return (db->set_error(HAM_INV_PARAMETER));

    return (db->set_error((*db)()->erase(txn, key, flags)));
}

ham_status_t HAM_CALLCONV
ham_check_integrity(ham_db_t *hdb, ham_txn_t *htxn)
{
    Database *db=(Database *)hdb;
    Transaction *txn=(Transaction *)htxn;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    ScopedLock lock(db->get_env()->get_mutex());

    if (db->get_env()->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    return (db->set_error((*db)()->check_integrity(txn)));
}

ham_status_t HAM_CALLCONV
ham_calc_maxkeys_per_page(ham_db_t *hdb, ham_size_t *keycount,
                ham_u16_t keysize)
{
    Database *db=(Database *)hdb;
    Backend *be;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!keycount) {
        ham_trace(("parameter 'keycount' must not be NULL"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if (!db->get_env()) {
        ham_trace(("Database was not initialized"));
        return (db->set_error(HAM_NOT_INITIALIZED));
    }

    ScopedLock lock(db->get_env()->get_mutex());

    if (db->get_env()->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    if (db->get_env()->get_flags()&DB_IS_REMOTE) {
        ham_trace(("ham_calc_maxkeys_per_page is not supported by remote "
                "servers"));
        return (HAM_NOT_IMPLEMENTED);
    }

    *keycount = 0;

    db->set_error(0);

    be=db->get_backend();
    if (!be)
        return (db->set_error(HAM_NOT_INITIALIZED));

    /* call the backend function */
    return (db->set_error(be->calc_keycount_per_page(keycount, keysize)));
}

ham_status_t HAM_CALLCONV
ham_flush(ham_db_t *hdb, ham_u32_t flags)
{
    Database *db=(Database *)hdb;
    Environment *env;

    (void)flags;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    env=db->get_env();
    if (!env) {
        ham_trace(("parameter 'db' must be linked to a valid (implicit or "
                   "explicit) environment"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    /* just call ham_env_flush() */
    return (db->set_error(ham_env_flush((ham_env_t *)env, flags)));
}

/*
 * always shut down entirely, even when a page flush or other
 * 'non-essential' element of the process fails.
 */
ham_status_t HAM_CALLCONV
ham_close(ham_db_t *hdb, ham_u32_t flags)
{
    Database *db=(Database *)hdb;
    ham_status_t st = HAM_SUCCESS;
    Environment *env=0;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    if ((flags&HAM_TXN_AUTO_ABORT) && (flags&HAM_TXN_AUTO_COMMIT)) {
        ham_trace(("invalid combination of flags: HAM_TXN_AUTO_ABORT + "
                    "HAM_TXN_AUTO_COMMIT"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    env=db->get_env();

    /* it's ok to close an uninitialized Database */
    if (!env || !(*db)())
        return (0);

    ScopedLock lock;
    if (!(flags&HAM_DONT_LOCK)) {
        // join the worker thread before the mutex is locked
        if (env->get_worker_thread())
            env->get_worker_thread()->join();
        lock=ScopedLock(env->get_mutex());
    }

    if (env->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    /* check if this database is modified by an active transaction */
    txn_optree_t *tree=db->get_optree();
    if (tree && !(db->get_rt_flags(true)&DB_ENV_IS_PRIVATE)) {
        txn_opnode_t *node=txn_tree_get_first(tree);
        while (node) {
            txn_op_t *op=txn_opnode_get_newest_op(node);
            while (op) {
                ham_u32_t f=txn_get_flags(txn_op_get_txn(op));
                if (!((f&TXN_STATE_COMMITTED) || (f&TXN_STATE_ABORTED))) {
                    ham_trace(("cannot close a Database that is modified by "
                               "a currently active Transaction"));
                    return (HAM_TXN_STILL_OPEN);
                }
                op=txn_op_get_previous_in_node(op);
            }
            node=txn_opnode_get_next_sibling(node);
        }
    }

    /*
     * check if there are Database Cursors - they will be closed
     * in the function handler, but the error check is here
     */
    if (!(flags&HAM_AUTO_CLEANUP)) {
        if (db->get_cursors()) {
            ham_trace(("cannot close Database if Cursors are still open"));
            return (db->set_error(HAM_CURSOR_STILL_OPEN));
        }
    }

    /* auto-abort (or commit) all pending transactions */
    if (env && env->get_newest_txn()
            && db->get_rt_flags(true)&DB_ENV_IS_PRIVATE) {
        Transaction *n, *t=env->get_newest_txn();
        while (t) {
            n=txn_get_older(t);
            if ((txn_get_flags(t)&TXN_STATE_ABORTED)
                    || (txn_get_flags(t)&TXN_STATE_COMMITTED))
                ; /* nop */
            else {
                if (flags&HAM_TXN_AUTO_COMMIT) {
                    if ((st=ham_txn_commit((ham_txn_t *)t, HAM_DONT_LOCK)))
                        return (st);
                }
                else { /* if (flags&HAM_TXN_AUTO_ABORT) */
                    if ((st=ham_txn_abort((ham_txn_t *)t, HAM_DONT_LOCK)))
                        return (st);
                }
            }
            t=n;
        }
    }
    // make sure all Transactions are flushed
    st=env->flush_committed_txns(true);
    if (st)
        return (st);

    db->set_error(0);

    /* the function pointer will do the actual implementation */
    st=(*db)()->close(flags);
    if (st)
        return (db->set_error(st));

    bool delete_env = false;

    /* remove this database from the environment */
    if (env) {
        Database *prev=0;
        Database *head=env->get_databases();
        while (head) {
            if (head==db) {
                if (!prev)
                    db->get_env()->set_databases(db->get_next());
                else
                    prev->set_next(db->get_next());
                break;
            }
            prev=head;
            head=head->get_next();
        }
        if (db->get_rt_flags()&DB_ENV_IS_PRIVATE) {
            (void)ham_env_close((ham_env_t *)db->get_env(),
                            flags|HAM_DONT_LOCK);
            delete_env = true;
        }
        db->set_env(0);
    }

    db->set_active(HAM_FALSE);

    if (!(flags&HAM_DONT_LOCK) && delete_env) {
        lock.unlock();
        delete env;
    }

    return (db->set_error(st));
}

ham_status_t HAM_CALLCONV
ham_cursor_create(ham_db_t *hdb, ham_txn_t *htxn, ham_u32_t flags,
                ham_cursor_t **hcursor)
{
    Database *db=(Database *)hdb;
    Transaction *txn=(Transaction *)htxn;
    Environment *env;
    Cursor **cursor=0;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return HAM_INV_PARAMETER;
    }
    if (!hcursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    cursor=(Cursor **)hcursor;

    env=db->get_env();
    if (!env) {
        ham_trace(("parameter 'db' must be linked to a valid (implicit or "
                   "explicit) environment"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    ScopedLock lock;
    if (!(flags&HAM_DONT_LOCK))
        lock=ScopedLock(env->get_mutex());

    if (env->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    if (!(*db)()) {
        ham_trace(("Database was not initialized"));
        return (db->set_error(HAM_NOT_INITIALIZED));
    }

    *cursor=(*db)()->cursor_create(txn, flags);

    /* fix the linked list of cursors */
    (*cursor)->set_next(db->get_cursors());
    if (db->get_cursors())
        db->get_cursors()->set_previous(*cursor);
    db->set_cursors(*cursor);

    if (txn) {
        txn_set_cursor_refcount(txn, txn_get_cursor_refcount(txn)+1);
        (*cursor)->set_txn(txn);
    }

    return (0);
}

ham_status_t HAM_CALLCONV
ham_cursor_clone(ham_cursor_t *hsrc, ham_cursor_t **hdest)
{
    Database *db;

    if (!hsrc) {
        ham_trace(("parameter 'src' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!hdest) {
        ham_trace(("parameter 'dest' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    Cursor *src, **dest;
    src=(Cursor *)hsrc;
    dest=(Cursor **)hdest;

    db=src->get_db();

    if (!db || !db->get_env()) {
        ham_trace(("parameter 'cursor' must be linked to a valid database"));
        return HAM_INV_PARAMETER;
    }

    ScopedLock lock(db->get_env()->get_mutex());

    if (db->get_env()->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    db->clone_cursor(src, dest);

    return (db->set_error(0));
}

ham_status_t HAM_CALLCONV
ham_cursor_overwrite(ham_cursor_t *hcursor, ham_record_t *record,
            ham_u32_t flags)
{
    Database *db;

    if (!hcursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    Cursor *cursor=(Cursor *)hcursor;

    db=cursor->get_db();

    if (!db || !db->get_env()) {
        ham_trace(("parameter 'cursor' must be linked to a valid database"));
        return (HAM_INV_PARAMETER);
    }

    ScopedLock lock(db->get_env()->get_mutex());

    if (db->get_env()->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    if (flags) {
        ham_trace(("function does not support a non-zero flags value; "
                    "see ham_cursor_insert for an alternative then"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if (!record) {
        ham_trace(("parameter 'record' must not be NULL"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if (!__prepare_record(record))
        return (db->set_error(HAM_INV_PARAMETER));
    if (db->get_rt_flags()&HAM_READ_ONLY) {
        ham_trace(("cannot overwrite in a read-only database"));
        return (db->set_error(HAM_DB_READ_ONLY));
    }
    if (db->get_rt_flags()&HAM_SORT_DUPLICATES) {
        ham_trace(("function ham_cursor_overwrite is not allowed if "
                    "duplicate sorting is enabled"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    return (db->set_error((*db)()->cursor_overwrite(cursor, record, flags)));
}

ham_status_t HAM_CALLCONV
ham_cursor_move(ham_cursor_t *hcursor, ham_key_t *key,
                ham_record_t *record, ham_u32_t flags)
{
    Database *db;
    Environment *env;
    ham_status_t st;

    if (!hcursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    Cursor *cursor=(Cursor *)hcursor;

    db=cursor->get_db();

    if (!db || !db->get_env()) {
        ham_trace(("parameter 'cursor' must be linked to a valid database"));
        return HAM_INV_PARAMETER;
    }

    ScopedLock lock(db->get_env()->get_mutex());

    if (db->get_env()->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    if ((flags&HAM_ONLY_DUPLICATES) && (flags&HAM_SKIP_DUPLICATES)) {
        ham_trace(("combination of HAM_ONLY_DUPLICATES and "
                    "HAM_SKIP_DUPLICATES not allowed"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    env=db->get_env();

    if ((flags&HAM_DIRECT_ACCESS)
            && !(env->get_flags()&HAM_IN_MEMORY)) {
        ham_trace(("flag HAM_DIRECT_ACCESS is only allowed in "
                   "In-Memory Databases"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags&HAM_DIRECT_ACCESS)
            && (env->get_flags()&HAM_ENABLE_TRANSACTIONS)) {
        ham_trace(("flag HAM_DIRECT_ACCESS is not allowed in "
                    "combination with Transactions"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags&HAM_PARTIAL) && (db->get_rt_flags()&HAM_ENABLE_TRANSACTIONS)) {
        ham_trace(("flag HAM_PARTIAL is not allowed in combination with "
                    "transactions"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    if (key && !__prepare_key(key))
        return (db->set_error(HAM_INV_PARAMETER));
    if (record && !__prepare_record(record))
        return (db->set_error(HAM_INV_PARAMETER));

    st=(*db)()->cursor_move(cursor, key, record, flags);

    /* make sure that the changeset is empty */
    ham_assert(env->get_changeset().is_empty());

    return (db->set_error(st));
}

ham_status_t HAM_CALLCONV
ham_cursor_find(ham_cursor_t *hcursor, ham_key_t *key, ham_u32_t flags)
{
    return (ham_cursor_find_ex(hcursor, key, NULL, flags));
}

HAM_EXPORT ham_status_t HAM_CALLCONV
ham_cursor_find_ex(ham_cursor_t *hcursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
    Database *db;
    Environment *env;

    if (!hcursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    Cursor *cursor=(Cursor *)hcursor;

    db=cursor->get_db();
    if (!db || !db->get_env()) {
        ham_trace(("parameter 'cursor' must be linked to a valid database"));
        return HAM_INV_PARAMETER;
    }

    env=db->get_env();
    ScopedLock lock;
    if (!(flags&HAM_DONT_LOCK))
        lock=ScopedLock(env->get_mutex());

    if (env->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    if (!key) {
        ham_trace(("parameter 'key' must not be NULL"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags&HAM_DIRECT_ACCESS)
            && !(env->get_flags()&HAM_IN_MEMORY)) {
        ham_trace(("flag HAM_DIRECT_ACCESS is only allowed in "
                   "In-Memory Databases"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags&HAM_FIND_NEAR_MATCH)
            && (env->get_flags()&HAM_ENABLE_TRANSACTIONS)) {
        ham_trace(("approx. matching is not allowed if Transactions "
                   "are enabled"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags&HAM_DIRECT_ACCESS)
            && (env->get_flags()&HAM_ENABLE_TRANSACTIONS)) {
        ham_trace(("flag HAM_DIRECT_ACCESS is not allowed in "
                    "combination with Transactions"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if (flags&HAM_HINT_PREPEND) {
        ham_trace(("flag HAM_HINT_PREPEND is only allowed in "
                   "ham_cursor_insert"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if (flags&HAM_HINT_APPEND) {
        ham_trace(("flag HAM_HINT_APPEND is only allowed in "
                   "ham_cursor_insert"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags&HAM_PARTIAL) && (db->get_rt_flags()&HAM_ENABLE_TRANSACTIONS)) {
        ham_trace(("flag HAM_PARTIAL is not allowed in combination with "
                    "transactions"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    if (key && !__prepare_key(key))
        return (db->set_error(HAM_INV_PARAMETER));
    if (record &&  !__prepare_record(record))
        return (db->set_error(HAM_INV_PARAMETER));

    return (db->set_error((*db)()->cursor_find(cursor, key, record, flags)));
}

ham_status_t HAM_CALLCONV
ham_cursor_insert(ham_cursor_t *hcursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
    Database *db;

    if (!hcursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    Cursor *cursor=(Cursor *)hcursor;

    db=cursor->get_db();
    ScopedLock lock(db->get_env()->get_mutex());

    if (db->get_env()->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    if (!key) {
        ham_trace(("parameter 'key' must not be NULL"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if (!record) {
        ham_trace(("parameter 'record' must not be NULL"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags&HAM_HINT_APPEND) && (flags&HAM_HINT_PREPEND)) {
        ham_trace(("flags HAM_HINT_APPEND and HAM_HINT_PREPEND "
                   "are mutually exclusive"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if (!__prepare_key(key) || !__prepare_record(record))
        return (db->set_error(HAM_INV_PARAMETER));

    if (!db || !db->get_env()) {
        ham_trace(("parameter 'cursor' must be linked to a valid database"));
        return HAM_INV_PARAMETER;
    }

    if (db->get_rt_flags()&HAM_READ_ONLY) {
        ham_trace(("cannot insert to a read-only database"));
        return (db->set_error(HAM_DB_READ_ONLY));
    }
    if ((db->get_rt_flags()&HAM_DISABLE_VAR_KEYLEN) &&
            (key->size>db_get_keysize(db))) {
        ham_trace(("database does not support variable length keys"));
        return (db->set_error(HAM_INV_KEYSIZE));
    }
    if ((flags&HAM_DUPLICATE) && (flags&HAM_OVERWRITE)) {
        ham_trace(("cannot combine HAM_DUPLICATE and HAM_OVERWRITE"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags&HAM_DUPLICATE) && !(db->get_rt_flags()&HAM_ENABLE_DUPLICATES)) {
        ham_trace(("database does not support duplicate keys "
                    "(see HAM_ENABLE_DUPLICATES)"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags&HAM_PARTIAL) && (db->get_rt_flags()&HAM_ENABLE_TRANSACTIONS)) {
        ham_trace(("flag HAM_PARTIAL is not allowed in combination with "
                    "transactions"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags&HAM_PARTIAL) && (db->get_rt_flags()&HAM_SORT_DUPLICATES)) {
        ham_trace(("flag HAM_PARTIAL is not allowed if duplicates "
                    "are sorted"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags&HAM_PARTIAL)
            && (record->partial_size+record->partial_offset>record->size)) {
        ham_trace(("partial offset+size is greater than the total "
                    "record size"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if ((flags&HAM_PARTIAL) && (record->size<=sizeof(ham_offset_t))) {
        ham_trace(("flag HAM_PARTIAL is not allowed if record->size "
                    "<= 8"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    /*
     * set flag HAM_DUPLICATE if one of DUPLICATE_INSERT* is set, but do
     * not allow these flags if duplicate sorting is enabled
     */
    if (flags&(HAM_DUPLICATE_INSERT_AFTER
                |HAM_DUPLICATE_INSERT_BEFORE
                |HAM_DUPLICATE_INSERT_LAST
                |HAM_DUPLICATE_INSERT_FIRST)) {
        if (db->get_rt_flags()&HAM_SORT_DUPLICATES) {
            ham_trace(("flag HAM_DUPLICATE_INSERT_* is not allowed if "
                        "duplicate sorting is enabled"));
            return (db->set_error(HAM_INV_PARAMETER));
        }
        flags|=HAM_DUPLICATE;
    }

    /* allocate temp. storage for a recno key */
    if (db->get_rt_flags()&HAM_RECORD_NUMBER) {
        if (flags&HAM_OVERWRITE) {
            if (key->size!=sizeof(ham_u64_t) || !key->data) {
                ham_trace(("key->size must be 8, key->data must not be NULL"));
                return (db->set_error(HAM_INV_PARAMETER));
            }
        }
        else {
            if (key->flags&HAM_KEY_USER_ALLOC) {
                if (!key->data || key->size!=sizeof(ham_u64_t)) {
                    ham_trace(("key->size must be 8, key->data must not "
                                "be NULL"));
                    return (db->set_error(HAM_INV_PARAMETER));
                }
            }
            else {
                if (key->data || key->size) {
                    ham_trace(("key->size must be 0, key->data must be NULL"));
                    return (db->set_error(HAM_INV_PARAMETER));
                }
            }
        }
    }

    return (db->set_error((*db)()->cursor_insert(cursor, key, record, flags)));
}

ham_status_t HAM_CALLCONV
ham_cursor_erase(ham_cursor_t *hcursor, ham_u32_t flags)
{
    Database *db;

    if (!hcursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    Cursor *cursor=(Cursor *)hcursor;

    db=cursor->get_db();

    if (!db || !db->get_env()) {
        ham_trace(("parameter 'cursor' must be linked to a valid database"));
        return HAM_INV_PARAMETER;
    }

    ScopedLock lock(db->get_env()->get_mutex());

    if (db->get_env()->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    if (db->get_rt_flags()&HAM_READ_ONLY) {
        ham_trace(("cannot erase from a read-only database"));
        return (db->set_error(HAM_DB_READ_ONLY));
    }
    if (flags&HAM_HINT_PREPEND) {
        ham_trace(("flags HAM_HINT_PREPEND is only allowed in "
                   "ham_cursor_insert"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    if (flags&HAM_HINT_APPEND) {
        ham_trace(("flags HAM_HINT_APPEND is only allowed in "
                   "ham_cursor_insert"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    return (db->set_error((*db)()->cursor_erase(cursor, flags)));
}

ham_status_t HAM_CALLCONV
ham_cursor_get_duplicate_count(ham_cursor_t *hcursor,
                ham_size_t *count, ham_u32_t flags)
{
    Database *db;

    if (!hcursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return HAM_INV_PARAMETER;
    }

    Cursor *cursor=(Cursor *)hcursor;

    db=cursor->get_db();
    if (!db || !db->get_env()) {
        ham_trace(("parameter 'cursor' must be linked to a valid database"));
        return HAM_INV_PARAMETER;
    }

    ScopedLock lock(db->get_env()->get_mutex());

    if (db->get_env()->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    if (!count) {
        ham_trace(("parameter 'count' must not be NULL"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    *count=0;

    return (db->set_error(
                (*db)()->cursor_get_duplicate_count(cursor, count, flags)));
}

ham_status_t HAM_CALLCONV
ham_cursor_get_record_size(ham_cursor_t *hcursor, ham_offset_t *size)
{
    Database *db;

    if (!hcursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    Cursor *cursor=(Cursor *)hcursor;

    db=cursor->get_db();
    if (!db || !db->get_env()) {
        ham_trace(("parameter 'cursor' must be linked to a valid database"));
        return (HAM_INV_PARAMETER);
    }

    ScopedLock lock(db->get_env()->get_mutex());

    if (db->get_env()->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    if (!size) {
        ham_trace(("parameter 'size' must not be NULL"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    *size=0;

    return (db->set_error(
                (*db)()->cursor_get_record_size(cursor, size)));
}

ham_status_t HAM_CALLCONV
ham_cursor_close(ham_cursor_t *hcursor)
{
    Database *db;

    if (!hcursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    Cursor *cursor=(Cursor *)hcursor;

    db=cursor->get_db();
    if (!db || !db->get_env()) {
        ham_trace(("parameter 'cursor' must be linked to a valid database"));
        return HAM_INV_PARAMETER;
    }

    ScopedLock lock(db->get_env()->get_mutex());

    if (db->get_env()->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    db->close_cursor(cursor);

    return (0);
}

ham_status_t HAM_CALLCONV
ham_add_record_filter(ham_db_t *hdb, ham_record_filter_t *filter)
{
    Database *db=(Database *)hdb;
    ham_record_filter_t *head;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    ScopedLock lock(db->get_env()->get_mutex());

    db->set_error(0);

    if (!filter) {
        ham_trace(("parameter 'filter' must not be NULL"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    head=db->get_record_filter();

    /*
     * !!
     * add the filter at the end of all filters, then we can process them
     * later in the same order as the insertion
     */
    if (!head) {
        db->set_record_filter(filter);
        filter->_prev = filter;
    }
    else {
        head->_prev = filter;

        while (head->_next)
            head=head->_next;

        filter->_prev=head;
        head->_next=filter;
    }

    return (db->set_error(0));
}

ham_status_t HAM_CALLCONV
ham_remove_record_filter(ham_db_t *hdb, ham_record_filter_t *filter)
{
    Database *db=(Database *)hdb;
    ham_record_filter_t *head, *prev;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    ScopedLock lock(db->get_env()->get_mutex());

    db->set_error(0);

    if (!filter) {
        ham_trace(("parameter 'filter' must not be NULL"));
        return (db->set_error(HAM_INV_PARAMETER));
    }

    head=db->get_record_filter();

    if (head == filter) {
        if (head->_next) {
            ham_assert(head->_prev != head);
            head->_next->_prev = head->_prev;
        }
        db->set_record_filter(head->_next);
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
        return (db->set_error(HAM_FILTER_NOT_FOUND));

    filter->_prev = 0;
    filter->_next = 0;

    return (db->set_error(0));
}

void HAM_CALLCONV
ham_set_context_data(ham_db_t *hdb, void *data)
{
    Database *db=(Database *)hdb;

    if (!db)
        return;

    ScopedLock lock(db->get_env()->get_mutex());
    db->set_context_data(data);
}

void * HAM_CALLCONV
ham_get_context_data(ham_db_t *hdb)
{
    Database *db=(Database *)hdb;
    if (!db)
        return (0);

    ScopedLock lock(db->get_env()->get_mutex());
    return (db->get_context_data());
}

void HAM_CALLCONV
ham_env_set_context_data(ham_env_t *henv, void *data)
{
    Environment *env=(Environment *)henv;
    if (!env)
        return;

    ScopedLock lock(env->get_mutex());
    env->set_context_data(data);
}

void * HAM_CALLCONV
ham_env_get_context_data(ham_env_t *henv)
{
    Environment *env=(Environment *)henv;
    if (!env)
        return (0);

    ScopedLock lock(env->get_mutex());
    return (env->get_context_data());
}


ham_db_t * HAM_CALLCONV
ham_cursor_get_database(ham_cursor_t *hcursor)
{
    if (hcursor) {
        Cursor *cursor=(Cursor *)hcursor;
        return ((ham_db_t *)cursor->get_db());
    }
    else
        return (0);
}

ham_u32_t
ham_get_flags(ham_db_t *hdb)
{
    Database *db=(Database *)hdb;
    if (!db)
        return (0);

    ScopedLock lock(db->get_env()->get_mutex());
    return (db->get_rt_flags());
}

ham_env_t *
ham_get_env(ham_db_t *hdb)
{
    Database *db=(Database *)hdb;
    if (!db)
        return (0);

    return ((ham_env_t *)db->get_env());
}

ham_status_t HAM_CALLCONV
ham_get_key_count(ham_db_t *hdb, ham_txn_t *htxn, ham_u32_t flags,
            ham_offset_t *keycount)
{
    Database *db=(Database *)hdb;
    Transaction *txn=(Transaction *)htxn;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    if (!keycount) {
        ham_trace(("parameter 'keycount' must not be NULL"));
        return (db->set_error(HAM_INV_PARAMETER));
    }
    *keycount = 0;

    ScopedLock lock(db->get_env()->get_mutex());

    if (db->get_env()->has_worker_error()) {
        ham_trace(("background thread has pending error"));
        return (HAM_ASYNCHRONOUS_ERROR_PENDING);
    }

    return (db->set_error((*db)()->get_key_count(txn, flags, keycount)));
}

ham_status_t HAM_CALLCONV
ham_env_set_device(ham_env_t *henv, ham_device_t *hdevice)
{
    Environment *env=(Environment *)henv;
    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    if (!hdevice) {
        ham_trace(("parameter 'device' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    ScopedLock lock(env->get_mutex());

    if (env->get_device()) {
        ham_trace(("Environment already has a device object attached"));
        return (HAM_ALREADY_INITIALIZED);
    }

    env->set_device((Device *)hdevice);
    return (0);
}

ham_device_t * HAM_CALLCONV
ham_env_get_device(ham_env_t *henv)
{
    Environment *env=(Environment *)henv;
    if (!env)
        return (0);

    ScopedLock lock(env->get_mutex());
    return ((ham_device_t *)env->get_device());
}

ham_status_t HAM_CALLCONV
ham_env_set_allocator(ham_env_t *henv, void *alloc)
{
    Environment *env=(Environment *)henv;
    if (!env || !alloc)
        return (HAM_INV_PARAMETER);

    ScopedLock lock(env->get_mutex());
    env->set_allocator((Allocator *)alloc);
    return (0);
}
