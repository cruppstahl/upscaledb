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


typedef struct free_cb_context_t
{
    ham_db_t *db;

    ham_bool_t is_leaf;

} free_cb_context_t;

static char *my_strncat_ex(char *buf, size_t buflen, const char *interject, const char *src)
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
            util_snprintf(buf, buflen, "%sHAM_FLAGS(reserved: 0x%x)", (*buf ? "|" : ""), (unsigned int)flags);
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

    case HAM_PARAM_GET_DAM            :
        return "HAM_PARAM_GET_DAM";

    case HAM_PARAM_GET_FILEMODE       :
        return "HAM_PARAM_GET_FILEMODE";

    case HAM_PARAM_GET_FILENAME       :
        return "HAM_PARAM_GET_FILENAME";

    case HAM_PARAM_DBNAME                :
        return "HAM_PARAM_DBNAME";

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

/*
 * callback function for freeing blobs of an in-memory-database
 */
static ham_status_t
my_free_cb(int event, void *param1, void *param2, void *context)
{
    ham_status_t st;
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
        if (!(env_get_rt_flags(db_get_env(c->db))&HAM_IN_MEMORY_DB)) 
        {
            ham_page_t *page=(ham_page_t *)param1;
            st = txn_free_page(env_get_txn(db_get_env(c->db)), page);
            if (st)
                return st;
        }
        break;

    case ENUM_EVENT_ITEM:
        key=(int_key_t *)param1;

        if (key_get_flags(key)&KEY_IS_EXTENDED) 
        {
            ham_offset_t blobid=key_get_extended_rid(c->db, key);
            /*
             * delete the extended key
             */
            st = extkey_remove(c->db, blobid);
            if (st)
                return st;
        }

        if (key_get_flags(key)&(KEY_BLOB_SIZE_TINY
                            |KEY_BLOB_SIZE_SMALL
                            |KEY_BLOB_SIZE_EMPTY))
            break;

        /*
         * if we're in the leaf page, delete the blob
         */
        if (c->is_leaf)
        {
            st = key_erase_record(c->db, key, 0, BLOB_FREE_ALL_DUPES);
            if (st)
                return st;
        }
        break;

    default:
        ham_assert(!"unknown callback event", (0));
        return CB_STOP;
    }

    return CB_CONTINUE;
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
 * as with the page filters, there was a bug in PRE-1.1.0 which would execute a record
 * filter chain in the same order for both write (insert) and read (find), which means
 * chained record filters would process invalid data in one of these, as a correct 
 * filter chain must traverse the transformation process IN REVERSE for one of these
 * actions.
 * 
 * As with the page filters, we've chosen the WRITE direction to be the FORWARD direction,
 * i.e. added filters end up processing data WRITTEN by the previous filter.
 * 
 * This also means the READ==FIND action must walk this chain in reverse.
 * 
 * See the documentation about the cyclic prev chain: the point is 
 * that FIND must traverse the record filter chain in REVERSE order so we should 
 * start with the LAST filter registered and stop once we've DONE calling
 * the FIRST.
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
        return HAM_INV_PARAMETER;
    }
    env = db_get_env(db);
    if (!env) {
        ham_trace(("parameter 'db' must be linked to a valid (implicit or explicit) environment"));
        return HAM_INV_PARAMETER;
    }

    if (!(env_get_rt_flags(env)&HAM_ENABLE_TRANSACTIONS)) {
        ham_trace(("transactions are disabled (see HAM_ENABLE_TRANSACTIONS)"));
        return HAM_INV_PARAMETER;
    }

    /* for hamsterdb 1.0.4 - only support one transaction */
    if (env_get_txn(env)) {
        ham_trace(("only one concurrent transaction is supported"));
        return (HAM_LIMITS_REACHED);
    }

    *txn=(ham_txn_t *)allocator_alloc(env_get_allocator(env), sizeof(**txn));
    if (!(*txn))
        return HAM_OUT_OF_MEMORY;

    st=txn_begin(*txn, env, flags);
    if (st) {
        allocator_free(env_get_allocator(env), *txn);
        *txn=0;
    }

    return st;
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
        default:
            return ("Unknown error");
    }
}

/**
Prepares a @ref ham_key_t structure for returning key data in.

This function checks whether the @ref ham_key_t structure has been properly initialized by the user
and resets all internal used elements.

@return HAM_TRUE when the @a key structure has been initialized correctly before.

@return HAM_FALSE when the @a key structure has @e not been initialized correctly before.
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
Prepares a @ref ham_record_t structure for returning record data in.

This function checks whether the @ref ham_record_t structure has been properly initialized by the user
and resets all internal used elements.

@return HAM_TRUE when the @a record structure has been initialized correctly before.

@return HAM_FALSE when the @a record structure has @e not been initialized correctly before.
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

static void
__prepare_db(ham_db_t *db)
{
    ham_env_t *env=db_get_env(db);

    ham_assert(env!=0, (0));

    /* do we need to set ourselves up as owner of the header page? not really! */
    //if (env_get_header_page(env))
    //    page_set_owner(env_get_header_page(env), db);
#if 0
    if (env_get_extkey_cache(env))
        extkey_cache_set_db(env_get_extkey_cache(env), db);
#endif
    //if (env_get_txn(env))
    //    txn_set_env(env_get_txn(env), env);
}


static ham_status_t 
__check_create_parameters(ham_env_t *env, ham_db_t *db, const char *filename, 
        ham_u32_t *pflags, const ham_parameter_t *param, 
        ham_size_t *ppagesize, ham_u16_t *pkeysize, 
        ham_size_t *pcachesize, ham_u16_t *pdbname,
        ham_u16_t *pmaxdbs, ham_u16_t *pdata_access_mode, ham_bool_t create,
        ham_bool_t patching_params_and_dont_fail)
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

#define RETURN(e)                                 \
    do {                                          \
        st = (e);                                 \
        if (!patching_params_and_dont_fail)       \
            goto fail_dramatically;               \
    }   while (0)

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
        RETURN(HAM_INV_PARAMETER);
    }

    /*
     * creating a file in READ_ONLY mode? doesn't make sense
     */
    if (create && (flags & HAM_READ_ONLY)) {
        ham_trace(("cannot create a file in read-only mode"));
        RETURN(HAM_INV_PARAMETER);
    }
    if (create && env && db && (env_get_rt_flags(env) & HAM_READ_ONLY)) {
        ham_trace(("cannot create database in read-only mode"));
        RETURN(HAM_DB_READ_ONLY);
    }

    /*
     * HAM_ENABLE_DUPLICATES has to be specified in ham_create, not 
     * ham_open 
     */
    if (!create && (flags & HAM_ENABLE_DUPLICATES)) {
        ham_trace(("invalid flag HAM_ENABLE_DUPLICATES (only allowed when "
                    "creating a database"));
        RETURN(HAM_INV_PARAMETER);
    }

    /*
     * when creating a Database, HAM_SORT_DUPLICATES is only allowed in
     * combination with HAM_ENABLE_DUPLICATES
     */
    if (create && (flags & HAM_SORT_DUPLICATES)) {
        if (!(flags & HAM_ENABLE_DUPLICATES)) {
            ham_trace(("flag HAM_SORT_DUPLICATES only allowed in combination "
                        "with HAM_ENABLE_DUPLICATES"));
            RETURN(HAM_INV_PARAMETER);
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
        RETURN(HAM_INV_PARAMETER);
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
                            if (patching_params_and_dont_fail) {
                                cachesize = env_get_cachesize(env);
                            }
                            else {
                                ham_trace(("invalid parameter HAM_PARAM_CACHESIZE - "
                                           "it's illegal to specify a new "
                                           "cache size when the cache has already "
                                           "been initialized"));
                                RETURN(HAM_INV_PARAMETER);
                            }
                        }
                    }
                    break;
                }
                goto default_case;

            case HAM_PARAM_KEYSIZE:
                if (!create) {
                    ham_trace(("invalid parameter HAM_PARAM_KEYSIZE"));
                    RETURN(HAM_INV_PARAMETER);
                }
                if (pkeysize) {
                    keysize=(ham_u16_t)param->value;
                    if (flags & HAM_RECORD_NUMBER) {
                        if (keysize > 0 && keysize < sizeof(ham_u64_t)) {
                            ham_trace(("invalid keysize %u - must be 8 for "
                                       "HAM_RECORD_NUMBER databases",
                                       (unsigned)keysize));
                            keysize = sizeof(ham_u64_t); 
                            RETURN(HAM_INV_KEYSIZE);
                        }
                    }
                }
                break;
            case HAM_PARAM_PAGESIZE:
                if (ppagesize) {
                    if (!patching_params_and_dont_fail) {
                        if (param->value!=1024 && param->value%2048!=0) {
                            ham_trace(("invalid pagesize - must be 1024 or "
                                    "a multiple of 2048"));
                            pagesize=0;
                            RETURN(HAM_INV_PAGESIZE);
                        }
                    }
                    pagesize=(ham_size_t)param->value;
                    break;
                }
                goto default_case;

            case HAM_PARAM_DATA_ACCESS_MODE:
                /* not allowed for Environments, only for Databases */
                if (!db && !patching_params_and_dont_fail) {
                    ham_trace(("invalid parameter "
                               "HAM_PARAM_DATA_ACCESS_MODE"));
                    dam=0;
                    RETURN(HAM_INV_PARAMETER);
                }
                if (param->value&HAM_DAM_ENFORCE_PRE110_FORMAT) {
                    ham_trace(("Data access mode HAM_DAM_ENFORCE_PRE110_FORMAT "
                                "must not be specified"));
                    RETURN(HAM_INV_PARAMETER);
                }
                if (pdata_access_mode) { 
                    switch (param->value) {
                    case 0: /* ignore 0 */
                        break;
                    case HAM_DAM_SEQUENTIAL_INSERT:
                    case HAM_DAM_RANDOM_WRITE:
                    case HAM_DAM_FAST_INSERT:
                    /* and all more-or-less viable permutations thereof ... */
                    case HAM_DAM_SEQUENTIAL_INSERT | HAM_DAM_FAST_INSERT:
                    case HAM_DAM_RANDOM_WRITE | HAM_DAM_FAST_INSERT:
                        dam=(ham_u16_t)param->value;
                        break;

                    default:
                        if (!patching_params_and_dont_fail) {
                            ham_trace(("invalid value 0x%04x specified for "
                                    "parameter HAM_PARAM_DATA_ACCESS_MODE", 
                                    (unsigned)param->value));
                            RETURN(HAM_INV_PARAMETER);
                        }
                        else
                            dam=0;
                    }
                    break;
                }
                goto default_case;

            case HAM_PARAM_MAX_ENV_DATABASES:
                if (pmaxdbs) {
                    if (param->value==0 || param->value >= HAM_DEFAULT_DATABASE_NAME) {
                        if (param->value==0 && !patching_params_and_dont_fail) {
                            ham_trace(("invalid value %u for parameter "
                                       "HAM_PARAM_MAX_ENV_DATABASES",
                                       (unsigned)param->value));
                            RETURN(HAM_INV_PARAMETER);
                        }
                        else {
                            /*
                            instruct hamster to report back the absolute maximum number of
                            DBs permitted in this kind of env
                            */
                            set_abs_max_dbs = HAM_TRUE;
                        }
                    }
                    else {
                        dbs=(ham_u16_t)param->value;
                    }
                    break;
                }
                goto default_case;

            case HAM_PARAM_DBNAME:
                if (pdbname) {
                    if (dbname == HAM_DEFAULT_DATABASE_NAME || dbname == HAM_FIRST_DATABASE_NAME) {
                        dbname=(ham_u16_t)param->value;

                        if ((!dbname && !patching_params_and_dont_fail)
                            || (dbname != HAM_FIRST_DATABASE_NAME 
                                && dbname != HAM_DUMMY_DATABASE_NAME 
                                && dbname > HAM_DEFAULT_DATABASE_NAME)) 
                        {
                            ham_trace(("parameter 'HAM_PARAM_DBNAME' value (0x%04x) must be non-zero and lower than 0xf000", (unsigned)dbname));
                            dbname = HAM_FIRST_DATABASE_NAME;
                            RETURN(HAM_INV_PARAMETER);
                        }
                        break;
                    }
                }
                goto default_case;

            case HAM_PARAM_GET_DAM:
            case HAM_PARAM_GET_FLAGS:
            case HAM_PARAM_GET_FILEMODE:
            case HAM_PARAM_GET_FILENAME:
            case HAM_PARAM_GET_KEYS_PER_PAGE:
            case HAM_PARAM_GET_STATISTICS:
                if (patching_params_and_dont_fail)
                    break;
                /* else: fall through! */

            default:
default_case:
                ham_trace(("unsupported/unknown parameter %d (%s)", (int)param->name, ham_param2str(NULL, 0, param->name)));
                RETURN(HAM_INV_PARAMETER);
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
        if (!patching_params_and_dont_fail) {
            if (!filename && !(flags&HAM_IN_MEMORY_DB)) {
                ham_trace(("filename is missing"));
                RETURN(HAM_INV_PARAMETER);
            }
        }
    }

    if (pdbname) {
        if (create && (dbname==0 || dbname>HAM_DUMMY_DATABASE_NAME)) {
            if (!patching_params_and_dont_fail) {
                ham_trace(("parameter 'name' (0x%04x) must be lower than "
                    "0xf000", (unsigned)dbname));
                RETURN(HAM_INV_PARAMETER);
            }
            dbname = HAM_FIRST_DATABASE_NAME;
        }
        else if (!create && (dbname==0 || dbname>HAM_DUMMY_DATABASE_NAME)) {
            if (!patching_params_and_dont_fail) {
                ham_trace(("parameter 'name' (0x%04x) must be lower than "
                    "0xf000", (unsigned)dbname));
                RETURN(HAM_INV_PARAMETER);
            }
            dbname = HAM_FIRST_DATABASE_NAME;
        }
    }

    if (db && (pdbname && !dbname)) {
        dbname = HAM_FIRST_DATABASE_NAME;
        if (!patching_params_and_dont_fail) {
            ham_trace(("invalid database name 0x%04x", (unsigned)dbname));
            RETURN(HAM_INV_PARAMETER);
        }
    }

    /*
     * make sure that the raw pagesize is aligned to 1024b
     */
    if (pagesize && pagesize%1024) {
        ham_trace(("pagesize must be multiple of 1024"));
        RETURN(HAM_INV_PAGESIZE);
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
        RETURN(HAM_INV_PARAMETER);

    /*
     * in-memory-db? don't allow cache limits!
     */
    if (flags&HAM_IN_MEMORY_DB) {
        if (flags&HAM_CACHE_STRICT) {
            ham_trace(("combination of HAM_IN_MEMORY_DB and HAM_CACHE_STRICT "
                        "not allowed"));
            flags &= ~HAM_CACHE_STRICT;
            RETURN(HAM_INV_PARAMETER);
        }
        if (cachesize!=0) {
            ham_trace(("combination of HAM_IN_MEMORY_DB and cachesize != 0 "
                        "not allowed"));
            cachesize = 0;
            RETURN(HAM_INV_PARAMETER);
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
            RETURN(HAM_INV_PARAMETER);
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
        RETURN(HAM_INV_KEYSIZE);
    }

    /*
     * make sure that max_databases actually fit in a header
     * page!
     * leave at least 128 bytes for the freelist and the other header data
     */
    {
        ham_size_t l = pagesize - sizeof(db_header_t)
                - db_get_freelist_header_size32() - 128;

        l /= sizeof(db_indexdata_t);
        if (dbs > l) {
            ham_trace(("parameter HAM_PARAM_MAX_ENV_DATABASES too high for "
                        "this pagesize; the maximum allowed is %u", 
                        (unsigned)l));
            set_abs_max_dbs = HAM_TRUE;
            RETURN(HAM_INV_PARAMETER);
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
     * convert cachesize to the number of cached pages for this platform
     * cachesize is either specified in BYTES or PAGES: low numbers are 
     * assumed to be PAGES 
     */
    ham_assert(pagesize, (0));
    if (cachesize > CACHE_MAX_ELEM)
        cachesize = (cachesize + pagesize - 1) / pagesize;
    if (cachesize == 0) {
        /* if (!(flags&HAM_IN_MEMORY_DB)) */
            cachesize = HAM_DEFAULT_CACHESIZE;
    }

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

#undef RETURN
fail_dramatically:
    
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
     *allocate memory for the ham_db_t-structure;
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
    if (env_is_active(env))
    {
        ham_trace(("parameter 'env' must have been closed before you invoke ham_env_delete()"));
        //return HAM_INV_PARAMETER;
    }

    /* delete all performance data */
    stats_trash_globdata(env, env_get_global_perf_data(env));

    /* 
     * close the device
     */
    if (env_get_device(env)) {
        ham_device_t *device = env_get_device(env);
        if (device->is_open(device)) {
            st = device->flush(device);
            if (!st2) st2 = st;
            st = device->close(device);
            if (!st2) st2 = st;
        }
        st = device->destroy(device);
        if (!st2) st2 = st;
        env_set_device(env, 0);
    }

    /*
    * close the allocator
    */
    if (env_get_allocator(env)) {
        env_get_allocator(env)->close(env_get_allocator(env));
        env_set_allocator(env, 0);
    }

    if (env->destroy)
    {
        st = env->destroy(env);
        if (!st2) st2 = st;
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
    ham_device_t *device=0;

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
            &pagesize, &keysize, &cachesize, 0, &maxdbs, 0, HAM_TRUE,
            HAM_FALSE);
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
    env_set_cachesize(env, cachesize);
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
     * it's the only way to communicate the pagesize to the call 
     * which will actually persist this info to disc! (ham_env_create_db()) 
     */
    env_set_pagesize(env, pagesize);

    /* reset all performance data */
    stats_init_globdata(env, env_get_global_perf_data(env));

    ham_assert(!env_get_header_page(env), (0));

    /* 
     * initialize the device, if it does not yet exist
     */
    if (!env_get_device(env)) 
    {
        device=ham_device_new(env_get_allocator(env), env, ((flags&HAM_IN_MEMORY_DB) ? HAM_DEVTYPE_MEMORY : HAM_DEVTYPE_FILE));

        if (!device)
            return (HAM_OUT_OF_MEMORY);

        env_set_device(env, device);

        device->set_flags(device, flags);
        st = device->set_pagesize(device, pagesize);
        if (st)
            return st;

        /* now make sure the 'cooked' pagesize is a multiple of DB_PAGESIZE_MIN_REQD_ALIGNMENT bytes */
        ham_assert(0 == (pagesize % DB_PAGESIZE_MIN_REQD_ALIGNMENT), (0));

        env_set_pagesize(env, pagesize);
    }
    else
    {
        device=env_get_device(env);
        ham_assert(device->get_pagesize(device), (0));
        ham_assert(env_get_pagesize(env) == device->get_pagesize(device), (0));
    }
    ham_assert(device == env_get_device(env), (0));
    ham_assert(env_get_pagesize(env) == device->get_pagesize(env_get_device(env)), (0));

    /* create the file */
    st=device->create(device, filename, flags, mode);
    if (st) {
        (void)ham_env_close(env, 0);
        return (st);
    }

    /* 
     * allocate the header page
     */
    {
        ham_page_t *page;

        page=page_new(env);
        if (!page) {
            (void)ham_env_close(env, 0);
            return (HAM_OUT_OF_MEMORY);
        }
        /* manually set the device pointer */
        page_set_device(page, device);
        st=page_alloc(page, pagesize);
        if (st) {
            page_delete(page);
            (void)ham_env_close(env, 0);
            return (st);
        }
        memset(page_get_pers(page), 0, pagesize);
        page_set_type(page, PAGE_TYPE_HEADER);
        env_set_header_page(env, page);

        /* initialize the header */
        env_set_magic(env, 'H', 'A', 'M', '\0');
        env_set_version(env, HAM_VERSION_MAJ, HAM_VERSION_MIN, 
                HAM_VERSION_REV, 0);
        env_set_serialno(env, HAM_SERIALNO);
        env_set_persistent_pagesize(env, pagesize);
        env_set_max_databases(env, maxdbs);
        ham_assert(env_get_max_databases(env) > 0, (0));

        page_set_dirty(page, env); /* [i_a] */
    }

    /*
     * create a logfile (if requested)
     */
    if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY) {
        ham_log_t *log;
        st=ham_log_create(env_get_allocator(env), env, env_get_filename(env), 
                0644, 0, &log);
        if (st) { 
            (void)ham_env_close(env, 0);
            return (st);
        }
        env_set_log(env, log);
    }

    /* 
     * initialize the cache
     */
    {
        ham_cache_t *cache;

        /* cachesize is specified in PAGES */
        ham_assert(cachesize, (0));
        cache=cache_new(env, cachesize);
        if (!cache) {
            (void)ham_env_close(env, 0);
            return (HAM_OUT_OF_MEMORY);
        }
        env_set_cache(env, cache);
    }

    env_set_active(env, HAM_TRUE);

    return (st);
}

ham_status_t HAM_CALLCONV
ham_env_create_db(ham_env_t *env, ham_db_t *db,
        ham_u16_t dbname, ham_u32_t flags, const ham_parameter_t *param)
{
    ham_status_t st;
    ham_u16_t keysize = 0;
    ham_size_t cachesize = 0;
    ham_u16_t dam = 0;
    ham_u16_t dbi;
    ham_size_t i;
    ham_backend_t *be;
    ham_u32_t pflags;

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
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

    if (!dbname || (dbname>HAM_DEFAULT_DATABASE_NAME 
            && dbname!=HAM_DUMMY_DATABASE_NAME)) {
        ham_trace(("invalid database name"));
        return (HAM_INV_PARAMETER);
    }

    db_set_rt_flags(db, 0);

    /* 
     * parse parameters
     */
    st=__check_create_parameters(env, db, 0, &flags, param, 
            0, &keysize, &cachesize, &dbname, 0, &dam, HAM_TRUE, HAM_FALSE);
    if (st)
        return (st);

    /*
     * store the env pointer in the database
     */
    db_set_env(db, env);

    /* reset all DB performance data */
    stats_init_dbdata(db, db_get_db_perf_data(db));

    /*
     * set the flags; strip off run-time (per session) flags for the 
     * backend::create() method though.
     */
    db_set_rt_flags(db, flags);
    pflags=flags;
    pflags&=~(HAM_DISABLE_VAR_KEYLEN
             |HAM_CACHE_STRICT
             |HAM_CACHE_UNLIMITED
             |HAM_DISABLE_MMAP
             |HAM_WRITE_THROUGH
             |HAM_READ_ONLY
             |HAM_DISABLE_FREELIST_FLUSH
             |HAM_ENABLE_RECOVERY
             |HAM_AUTO_RECOVERY
             |HAM_ENABLE_TRANSACTIONS
             |HAM_SORT_DUPLICATES
             |DB_USE_MMAP
             |DB_ENV_IS_PRIVATE);

    /*
     * transfer the ownership of the header page to this Database
     */
    page_set_owner(env_get_header_page(env), db);
    ham_assert(env_get_header_page(env), (0));

    /*
     * check if this database name is unique
     */
    ham_assert(env_get_max_databases(env) > 0, (0));
    for (i=0; i<env_get_max_databases(env); i++) {
        ham_u16_t name = index_get_dbname(env_get_indexdata_ptr(env, i));
        if (!name)
            continue;
        if (name==dbname || dbname==HAM_FIRST_DATABASE_NAME) {
            (void)ham_close(db, 0);
            return (db_set_error(db, HAM_DATABASE_ALREADY_EXISTS));
        }
    }

    /*
     * find a free slot in the indexdata array and store the 
     * database name
     */
    ham_assert(env_get_max_databases(env) > 0, (0));
    for (dbi=0; dbi<env_get_max_databases(env); dbi++) {
        ham_u16_t name = index_get_dbname(env_get_indexdata_ptr(env, dbi));
        if (!name) {
            index_set_dbname(env_get_indexdata_ptr(env, dbi), dbname);
            db_set_indexdata_offset(db, dbi);
            break;
        }
    }
    if (dbi==env_get_max_databases(env)) {
        (void)ham_close(db, 0);
        return (db_set_error(db, HAM_LIMITS_REACHED));
    }

    /* 
     * create the backend
     */
    be = db_get_backend(db);
    if (be == NULL)
    {
        st=db_create_backend(&be, db, flags);
        ham_assert(st ? be == NULL : 1, (0));
        if (!be) {
            (void)ham_close(db, 0);
            return st; // HAM_NOT_INITIALIZED
        }

        /* 
         * store the backend in the database
         */
        db_set_backend(db, be);
    }

    /* 
     * initialize the backend
     */
    st=be->_fun_create(be, keysize, pflags);
    if (st) {
        (void)ham_close(db, 0);
        db_set_error(db, st);
        return (st);
    }

    ham_assert(be_is_active(be) != 0, (0));

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
    ham_set_duplicate_compare_func(db, db_default_compare);
    env_set_dirty(env);

    /* 
     * finally calculate and store the data access mode 
     */
    if (env_get_version(env, 0) == 1 &&
        env_get_version(env, 1) == 0 &&
        env_get_version(env, 2) <= 9) {
        dam |= HAM_DAM_ENFORCE_PRE110_FORMAT;
        env_set_legacy(env, 1);
    }
    if (!dam) {
        dam=(flags&HAM_RECORD_NUMBER)
            ? HAM_DAM_SEQUENTIAL_INSERT 
            : HAM_DAM_RANDOM_WRITE;
    }
    db_set_data_access_mode(db, dam);

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
    ham_set_duplicate_compare_func(db, db_default_compare);

    /*
     * on success: store the open database in the environment's list of
     * opened databases
     */
    db_set_next(db, env_get_list(env));
    env_set_list(env, db);

    db_set_active(db, HAM_TRUE);

    return (HAM_SUCCESS);
}

ham_status_t HAM_CALLCONV
ham_env_open_db(ham_env_t *env, ham_db_t *db,
        ham_u16_t name, ham_u32_t flags, const ham_parameter_t *param)
{
    ham_db_t *head;
    ham_status_t st;
    ham_u16_t dam = 0;
    ham_size_t cachesize = 0;
    ham_backend_t *be = 0;
    ham_u16_t dbi;

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
    if (name!=HAM_FIRST_DATABASE_NAME 
          && (name!=HAM_DUMMY_DATABASE_NAME 
                && name>HAM_DEFAULT_DATABASE_NAME)) {
        ham_trace(("parameter 'name' must be lower than 0xf000"));
        return (HAM_INV_PARAMETER);
    }
    if (env_get_rt_flags(env)&HAM_IN_MEMORY_DB) {
        ham_trace(("cannot open a Database in an In-Memory Environment"));
        return (HAM_INV_PARAMETER);
    }

    /*
     * make sure that this database is not yet open/created
     */
    if (db_is_active(db)) {
        ham_trace(("parameter 'db' is already initialized"));
        return (HAM_DATABASE_ALREADY_OPEN);
    }

    db_set_rt_flags(db, 0);

    /* parse parameters */
    st=__check_create_parameters(env, db, 0, &flags, param, 
            0, 0, &cachesize, &name, 0, &dam, HAM_FALSE, HAM_FALSE);
    if (st)
        return (st);

    /*
     * make sure that this database is not yet open
     */
    head=env_get_list(env);
    while (head) {
        db_indexdata_t *ptr=env_get_indexdata_ptr(env, 
                                db_get_indexdata_offset(head));
        if (index_get_dbname(ptr)==name)
            return (HAM_DATABASE_ALREADY_OPEN);
        head=db_get_next(head);
    }

    ham_assert(env_get_allocator(env), (""));
    ham_assert(env_get_device(env), (""));
    ham_assert(0 != env_get_header_page(env), (0));
    ham_assert(env_get_max_databases(env) > 0, (0));

    /*
     * store the env pointer in the database
     */
    db_set_env(db, env);

    /*
     * reset the DB performance data
     */
    stats_init_dbdata(db, db_get_db_perf_data(db));

    /*
     * search for a database with this name
     */
    for (dbi=0; dbi<env_get_max_databases(env); dbi++) {
        db_indexdata_t *idx=env_get_indexdata_ptr(env, dbi);
        ham_u16_t dbname = index_get_dbname(idx);
        if (!dbname)
            continue;
        if (name==HAM_FIRST_DATABASE_NAME || name==dbname) {
            db_set_indexdata_offset(db, dbi);
            break;
        }
    }

    if (dbi==env_get_max_databases(env)) {
        (void)ham_close(db, 0);
        return (db_set_error(db, HAM_DATABASE_NOT_FOUND));
    }

    /* 
     * create the backend
     */
    be = db_get_backend(db);
    if (be == NULL) {
        st=db_create_backend(&be, db, flags);
        ham_assert(st ? be == NULL : 1, (0));
        if (!be) {
            (void)ham_close(db, 0);
            return st; // HAM_NOT_INITIALIZED
        }

        /* 
        * store the backend in the database
        */
        db_set_backend(db, be);
    }

    /* 
     * initialize the backend 
     */
    st=be->_fun_open(be, flags);
    if (st) {
        (void)ham_close(db, 0);
        return (db_set_error(db, st));
    }

    ham_assert(be_is_active(be) != 0, (0));
    /* 
     * set the database flags; strip off the persistent flags that may have been
     * set by the caller, before mixing in the persistent flags as obtained 
     * from the backend.
     */
    flags &= (HAM_DISABLE_VAR_KEYLEN
             |HAM_CACHE_STRICT
             |HAM_CACHE_UNLIMITED
             |HAM_DISABLE_MMAP
             |HAM_WRITE_THROUGH
             |HAM_READ_ONLY
             |HAM_DISABLE_FREELIST_FLUSH
             |HAM_ENABLE_RECOVERY
             |HAM_AUTO_RECOVERY
             |HAM_ENABLE_TRANSACTIONS
             |HAM_SORT_DUPLICATES
             |DB_USE_MMAP
             |DB_ENV_IS_PRIVATE);
    db_set_rt_flags(db, flags|be_get_flags(be));
    ham_assert(!(be_get_flags(be)&HAM_DISABLE_VAR_KEYLEN), 
            ("invalid persistent database flags 0x%x", be_get_flags(be)));
    ham_assert(!(be_get_flags(be)&HAM_CACHE_STRICT), 
            ("invalid persistent database flags 0x%x", be_get_flags(be)));
    ham_assert(!(be_get_flags(be)&HAM_CACHE_UNLIMITED), 
            ("invalid persistent database flags 0x%x", be_get_flags(be)));
    ham_assert(!(be_get_flags(be)&HAM_DISABLE_MMAP), 
            ("invalid persistent database flags 0x%x", be_get_flags(be)));
    ham_assert(!(be_get_flags(be)&HAM_WRITE_THROUGH), 
            ("invalid persistent database flags 0x%x", be_get_flags(be)));
    ham_assert(!(be_get_flags(be)&HAM_READ_ONLY), 
            ("invalid persistent database flags 0x%x", be_get_flags(be)));
    ham_assert(!(be_get_flags(be)&HAM_DISABLE_FREELIST_FLUSH), 
            ("invalid persistent database flags 0x%x", be_get_flags(be)));
    ham_assert(!(be_get_flags(be)&HAM_ENABLE_RECOVERY), 
            ("invalid persistent database flags 0x%x", be_get_flags(be)));
    ham_assert(!(be_get_flags(be)&HAM_AUTO_RECOVERY), 
            ("invalid persistent database flags 0x%x", be_get_flags(be)));
    ham_assert(!(be_get_flags(be)&HAM_ENABLE_TRANSACTIONS), 
            ("invalid persistent database flags 0x%x", be_get_flags(be)));
    ham_assert(!(be_get_flags(be)&DB_USE_MMAP), 
            ("invalid persistent database flags 0x%x", be_get_flags(be)));

    /*
     * SORT_DUPLICATES is only allowed if the Database was created
     * with ENABLE_DUPLICATES!
     */
    if ((flags&HAM_SORT_DUPLICATES) 
            && !(db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES)) {
        ham_trace(("flag HAM_SORT_DUPLICATES set but duplicates are not "
                   "enabled for this Database"));
        (void)ham_close(db, 0);
        return (db_set_error(db, HAM_INV_PARAMETER));
    }

    /* 
     * finally calculate and store the data access mode 
     */
    if (env_get_version(env, 0) == 1 &&
        env_get_version(env, 1) == 0 &&
        env_get_version(env, 2) <= 9) {
        dam |= HAM_DAM_ENFORCE_PRE110_FORMAT;
        env_set_legacy(env, 1);
    }
    if (!dam) {
        dam=(db_get_rt_flags(db)&HAM_RECORD_NUMBER)
            ? HAM_DAM_SEQUENTIAL_INSERT 
            : HAM_DAM_RANDOM_WRITE;
    }
    db_set_data_access_mode(db, dam);

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
    ham_set_duplicate_compare_func(db, db_default_compare);

    /*
     * on success: store the open database in the environment's list of
     * opened databases
     */
    db_set_next(db, env_get_list(env));
    env_set_list(env, db);

    db_set_active(db, HAM_TRUE);

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
    ham_device_t *device=0;
    ham_u32_t pagesize=0;
    ham_u16_t dam = 0;

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
            0, 0, &cachesize, 0, 0, 0, HAM_FALSE, HAM_FALSE);
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

    /* reset all performance data */
    stats_init_globdata(env, env_get_global_perf_data(env));

    /* 
     * initialize/open the device
     */
    device=ham_device_new(env_get_allocator(env), env,
                ((flags&HAM_IN_MEMORY_DB) 
                    ? HAM_DEVTYPE_MEMORY 
                    : HAM_DEVTYPE_FILE));
    if (!device)
        return (HAM_OUT_OF_MEMORY);
    env_set_device(env, device);

    /* 
     * open the file 
     */
    st=device->open(device, filename, flags);
    if (st) {
        (void)ham_env_close(env, 0);
        return (st);
    }

    /*
     * read the database header
     *
     * !!!
     * now this is an ugly problem - the database header is one page, but
     * what's the size of this page? chances are good that it's the default
     * page-size, but we really can't be sure.
     *
     * read 512 byte and extract the "real" page size, then read 
     * the real page. (but i really don't like this)
     */
    {
        ham_page_t *page=0;
        db_header_t *hdr;
        ham_u8_t hdrbuf[512];
        ham_page_t fakepage = {{0}};
        ham_bool_t hdrpage_faked = HAM_FALSE;

        /*
         * in here, we're going to set up a faked headerpage for the 
         * duration of this call; BE VERY CAREFUL: we MUST clean up 
         * at the end of this section or we'll be in BIG trouble!
         */
        hdrpage_faked = HAM_TRUE;
        fakepage._pers = (ham_perm_page_union_t *)hdrbuf;
        env_set_header_page(env, &fakepage);

        /*
         * now fetch the header data we need to get an estimate of what 
         * the database is made of really.
         * 
         * Because we 'faked' a headerpage setup right here, we can now use 
         * the regular hamster macros to obtain data from the file 
         * header -- pre v1.1.0 code used specially modified copies of 
         * those macros here, but with the advent of dual-version database 
         * format support here this was getting hairier and hairier. 
         * So we now fake it all the way instead.
         */
        st=device->read(device, 0, hdrbuf, sizeof(hdrbuf));
        if (st) 
            goto fail_with_fake_cleansing;

        hdr = env_get_header(env);
        ham_assert(hdr == (db_header_t *)(hdrbuf + 
                    page_get_persistent_header_size()), (0));

        pagesize = env_get_persistent_pagesize(env);
        env_set_pagesize(env, pagesize);
        st = device->set_pagesize(device, pagesize);
        if (st) 
            goto fail_with_fake_cleansing;

        /*
         * can we use mmap?
         * TODO really necessary? code is already handled in 
         * __check_parameters() above
         */
#if HAVE_MMAP
        if (!(flags&HAM_DISABLE_MMAP)) {
            if (pagesize % os_get_granularity()==0)
                flags|=DB_USE_MMAP;
            else
                device->set_flags(device, flags|HAM_DISABLE_MMAP);
        }
        else {
            device->set_flags(device, flags|HAM_DISABLE_MMAP);
        }
        flags&=~HAM_DISABLE_MMAP; /* don't store this flag */
#else
        device->set_flags(device, flags|HAM_DISABLE_MMAP);
#endif

        /* 
         * check the file magic
         */
        if (env_get_magic(hdr, 0)!='H' ||
                env_get_magic(hdr, 1)!='A' ||
                env_get_magic(hdr, 2)!='M' ||
                env_get_magic(hdr, 3)!='\0') {
            ham_log(("invalid file type"));
            st = HAM_INV_FILE_HEADER;
            goto fail_with_fake_cleansing;
        }

        /* 
         * check the database version
         *
         * if this Database is from 1.0.x: force the PRE110-DAM
         * TODO this is done again some lines below; remove this and
         * replace it with a function __is_supported_version()
         */
        if (envheader_get_version(hdr, 0)!=HAM_VERSION_MAJ ||
                envheader_get_version(hdr, 1)!=HAM_VERSION_MIN) {
            /* before we yak about a bad DB, see if this feller is 
             * a 'backwards compatible' one (1.0.x - 1.0.9) */
            if (envheader_get_version(hdr, 0) == 1 &&
                envheader_get_version(hdr, 1) == 0 &&
                envheader_get_version(hdr, 2) <= 9) {
                dam |= HAM_DAM_ENFORCE_PRE110_FORMAT;
                env_set_legacy(env, 1);
            }
            else {
                ham_log(("invalid file version"));
                st = HAM_INV_FILE_VERSION;
                goto fail_with_fake_cleansing;
            }
        }

        st = 0;

fail_with_fake_cleansing:

        /* undo the headerpage fake first! */
        if (hdrpage_faked) {
            env_set_header_page(env, 0);
        }

        /* exit when an error was signaled */
        if (st) {
            (void)ham_env_close(env, 0);
            return (st);
        }

        /*
         * now read the "real" header page and store it in the Environment
         */
        page=page_new(env);
        if (!page) {
            (void)ham_env_close(env, 0);
            return (HAM_OUT_OF_MEMORY);
        }
        page_set_device(page, device);
        st=page_fetch(page, pagesize);
        if (st) {
            page_delete(page);
            (void)ham_env_close(env, 0);
            return (st);
        }
        env_set_header_page(env, page);
    }

   /*
     * open the logfile and check if we need recovery
     */
    if (env_get_rt_flags(env)&HAM_ENABLE_RECOVERY
            && env_get_log(env)==0) {
        ham_log_t *log;
        st=ham_log_open(env_get_allocator(env), env, env_get_filename(env), 0, &log);
        if (!st) { 
            /* success - check if we need recovery */
            ham_bool_t isempty;
            st=ham_log_is_empty(log, &isempty);
            if (st) {
                (void)ham_env_close(env, 0);
                return (st);
            }
            env_set_log(env, log);
            if (!isempty) {
                if (flags&HAM_AUTO_RECOVERY) 
                {
                    st=ham_log_recover(log, env_get_device(env), env);
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
        else if (st && st==HAM_FILE_NOT_FOUND)
        {
            st=ham_log_create(env_get_allocator(env), env, env_get_filename(env), 0644, 0, &log);
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

    /* 
     * initialize the cache
     */
    {
        ham_cache_t *cache;

        /* cachesize is specified in PAGES */
        ham_assert(cachesize, (0));
        cache=cache_new(env, cachesize);
        if (!cache) {
            (void)ham_env_close(env, 0);
            return (HAM_OUT_OF_MEMORY);
        }
        env_set_cache(env, cache);
    }

    env_set_active(env, HAM_TRUE);

    return (HAM_SUCCESS);
}

ham_status_t HAM_CALLCONV
ham_env_rename_db(ham_env_t *env, ham_u16_t oldname, 
                ham_u16_t newname, ham_u32_t flags)
{
    ham_u16_t dbi;
    ham_u16_t slot;

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
     * check if a database with the new name already exists; also search 
     * for the database with the old name
     */
    slot=env_get_max_databases(env);
    ham_assert(env_get_max_databases(env) > 0, (0));
    for (dbi=0; dbi<env_get_max_databases(env); dbi++) {
        ham_u16_t name=index_get_dbname(env_get_indexdata_ptr(env, dbi));
        if (name==newname)
            return (HAM_DATABASE_ALREADY_EXISTS);
        if (name==oldname)
            slot=dbi;
    }

    if (slot==env_get_max_databases(env))
        return (HAM_DATABASE_NOT_FOUND);

    /*
     * replace the database name with the new name
     */
    index_set_dbname(env_get_indexdata_ptr(env, slot), newname);

    env_set_dirty(env);
    
    return HAM_SUCCESS;
}

ham_status_t HAM_CALLCONV
ham_env_erase_db(ham_env_t *env, ham_u16_t name, ham_u32_t flags)
{
    ham_db_t *db;
    ham_status_t st;
    free_cb_context_t context;
    ham_txn_t txn;
    ham_backend_t *be;

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
        ham_u16_t dbname=index_get_dbname(env_get_indexdata_ptr(env,
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
    st=txn_begin(&txn, env, 0);
    if (st) {
        (void)ham_close(db, 0);
        (void)ham_delete(db);
        return (st);
    }
    
    context.db=db;
    be=db_get_backend(db);
    if (!be || !be_is_active(be))
        return HAM_INTERNAL_ERROR;

    st=be->_fun_enumerate(be, my_free_cb, &context);
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
     * set database name to 0 and set the header page to dirty
     */
    index_set_dbname(env_get_indexdata_ptr(env, db_get_indexdata_offset(db)), 0);
    page_set_dirty_txn(env_get_header_page(env), 1);

    /*
    TODO

    log??? before + after image of header page???
    */

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
     * later in the same order as the insertion.

     Because we must process filters IN REVERSE ORDER when WRITING to disc (going from 'cooked' to 'raw' data),
     we've created a cyclic -> prev chain: no need to first traverse to the end, then traverse back.

     This means that the -> next forward chain is terminating (last->next == NULL), while the ->prev chain
     is cyclic (head->prev = last wrap-around). Therefor, the fastest way to check if the REVERSE (= ->prev)
     traversal is done, is by checking node->prev->next==NULL.
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
        st=ham_env_open_db(env, db, HAM_FIRST_DATABASE_NAME, 0, 0);
        if (st) {
            (void)ham_delete(db);
            return (st==HAM_DATABASE_NOT_FOUND ? 0 : st);
        }
        temp_db=HAM_TRUE;
    }

    /*
     * copy each database name in the array
     */
    ham_assert(env_get_max_databases(env) > 0, (0));
    for (i=0; i<env_get_max_databases(env); i++) {
        name = index_get_dbname(env_get_indexdata_ptr(env, i));
        if (name==0)
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
        (void)ham_delete(db);
    }

    return st;
}

ham_status_t HAM_CALLCONV
ham_env_close(ham_env_t *env, ham_u32_t flags)
{
    ham_status_t st;
    ham_status_t st2 = HAM_SUCCESS;
    ham_device_t *dev;
    ham_file_filter_t *file_head;

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    /* flush/persist all performance data which we want to persist */
    stats_flush_globdata(env, env_get_global_perf_data(env));

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
     * if we're not in read-only mode, and not an in-memory-database,
     * and the dirty-flag is true: flush the page-header to disk
     */
    if (env_get_header_page(env)
            && !(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)
            && env_get_device(env)
            && env_get_device(env)->is_open(env_get_device(env))
            && (!(env_get_rt_flags(env)&HAM_READ_ONLY))) {
        st=page_flush(env_get_header_page(env));
        if (!st2) st2 = st;
    }

    /*
     * flush the freelist
     */
    st=freel_shutdown(env);
    if (st)
    {
        if (st2 == 0) st2 = st;
    }

    /*
     * close the header page
     *
     * !!
     * the last database, which was closed, has set the owner of the 
     * page to 0, which means that we can't call page_free, page_delete
     * etc. We have to use the device-routines.
     */
    dev=env_get_device(env);
    if (env_get_header_page(env)) 
    {
        ham_page_t *page=env_get_header_page(env);
        ham_assert(dev, (0));
        if (page_get_pers(page))
        {
            st = dev->free_page(dev, page);
            if (!st2) st2 = st;
        }
        allocator_free(env_get_allocator(env), page);
        env_set_header_page(env, 0);
    }

    /* 
     * flush all pages, get rid of the cache 
     */
    if (env_get_cache(env)) {
        (void)db_flush_all(env_get_cache(env), 0);
        cache_delete(env, env_get_cache(env));
        env_set_cache(env, 0);
    }

    /* 
     * close the device
     */
    if (dev) {
        if (dev->is_open(dev)) {
            st = dev->flush(dev);
            if (!st2) st2 = st;
            st = dev->close(dev);
            if (!st2) st2 = st;
        }
        st = dev->destroy(dev);
        if (!st2) st2 = st;
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
        st = ham_log_close(env_get_log(env), (flags&HAM_DONT_CLEAR_LOG));
        if (!st2) st2 = st;
        env_set_log(env, 0);
    }

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

    return st2;
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

    db[0]->destroy = __ham_destroy_db;

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
    if (db_is_active(db))
    {
        ham_trace(("parameter 'db' should have been closed before you invoke ham_delete()"));
        //return HAM_INV_PARAMETER;
    }

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
        if (!st2) st2 = st;
    }

    /*
     * "free" all remaining memory
       */
    if (env && env_get_allocator(env)) 
    {
        allocator_free(env_get_allocator(env), db);
    }

    if (db->destroy)
    {
        st = db->destroy(db);
        if (!st2) st2 = st;
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
            0, 0, &cachesize, &dbname, 0, &dam, HAM_FALSE, HAM_FALSE);
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
        return (HAM_DATABASE_ALREADY_OPEN);
    }

    /*
     * check (and modify) the parameters
     */
    st=__check_create_parameters(db_get_env(db), db, filename, &flags, param, 
            &pagesize, &keysize, &cachesize, &dbname, &maxdbs, &dam, HAM_TRUE,
            HAM_FALSE);
    if (st)
        goto bail;

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

static void 
nil_param_values(ham_parameter_t *param)
{
    for (; param->name; param++) 
    {
        if (param->name != HAM_PARAM_GET_STATISTICS)
        {
            param->value = 0;
        }
    }
}

static ham_status_t
__ham_get_parameters(ham_env_t *env, ham_db_t *db, ham_parameter_t *param)
{
    ham_u32_t flags = 0;
    ham_u16_t keysize = 0;
    ham_size_t keycount = 0;
    ham_size_t cachesize = 0;
    ham_u16_t max_databases = 0;
    ham_u16_t dbname = 0;
    ham_u16_t dam = 0;
    const char *filename = NULL;
    ham_u32_t file_mode = 0;
    ham_size_t pagesize = 0;
    ham_status_t st;

    if (!param) {
        ham_trace(("parameter 'param' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    /* use the presets, while we determine the current values */
    if (env) {
        if (env_get_cache(env)) {
            ham_cache_t *cache = env_get_cache(env);
            cachesize = cache_get_max_elements(cache);
        }

        file_mode = env_get_file_mode(env);
        filename = env_get_filename(env);
    }
    if (db) {
        ham_assert(env == db_get_env(db), (0));
        flags = db_get_env(db) ? db_get_rt_flags(db) : 0;
        if (db_get_backend(db)) {
            keysize = db_get_keysize(db);
        }
        if (db_get_env(db) && env_get_cache(env)) {
            cachesize = cache_get_max_elements(env_get_cache(env));
        }
        max_databases = 1; //env_get_max_databases(env);
    }

    st = __check_create_parameters(env, db, filename, &flags, param, 
            &pagesize, &keysize, &cachesize, &dbname, &max_databases, 
            &dam, HAM_TRUE, HAM_TRUE);
    if (st)
        return st;

    nil_param_values(param);

    /*
     * And cooked pagesize should not surpass the space we can occupy in a 
     * page for a freelist, or we'll be introducing gaps there.
     */
    if (env) {
        if (env_get_cache(env)) {
            ham_cache_t *cache = env_get_cache(env);
            cachesize = cache_get_max_elements(cache);
        }
    }
    ham_assert(cachesize <= CACHE_MAX_ELEM, (0));

    for (; param->name; param++) {
        switch (param->name) {
            case HAM_PARAM_CACHESIZE:
                param->value = cachesize;
                break;
            case HAM_PARAM_PAGESIZE:
                param->value = pagesize;
                break;
            case HAM_PARAM_KEYSIZE:
                param->value = keysize;
                break;
            case HAM_PARAM_MAX_ENV_DATABASES:
                param->value = max_databases;
                break;
            case HAM_PARAM_DATA_ACCESS_MODE:
                param->value = dam;
                break;
            case HAM_PARAM_GET_FLAGS:
                param->value = flags;
                break;
            case HAM_PARAM_GET_DAM:
                param->value = db ? db_get_data_access_mode(db) : 0;
                break;
            case HAM_PARAM_GET_FILEMODE:
                if (env)
                    file_mode = env_get_file_mode(env);
                param->value = file_mode;
                break;
            case HAM_PARAM_GET_FILENAME:
                if (env)
                    filename = env_get_filename(env);
                param->value = (ham_u64_t)filename;
                break;
            case HAM_PARAM_DBNAME:
                /* only set this when the 'db' is initialized properly already */
                if (db
                        && db_get_env(db) 
                        && env_get_header_page(env) 
                        && page_get_pers(env_get_header_page(env))) {
                    db_indexdata_t *indexdata = env_get_indexdata_ptr(env, 
                            db_get_indexdata_offset(db));
                    ham_assert(indexdata, (0));
                    dbname = index_get_dbname(indexdata);

                    param->value = dbname;
                }
                else if (param->value == 0 
                        && dbname != HAM_DEFAULT_DATABASE_NAME 
                        && (env || db)) {
                    param->value = dbname;
                }
                break;
            case HAM_PARAM_GET_KEYS_PER_PAGE:
                if (db && db_get_backend(db)) {
                    ham_backend_t *be = db_get_backend(db);

                    st = be->_fun_calc_keycount_per_page(be, &keycount, keysize);
                    if (st)
                        return st;
                }
                else {
                    /* approximation of btree->_fun_calc_keycount_per_page() */
                    keycount = btree_calc_maxkeys(pagesize, keysize);
                    if (keycount > MAX_KEYS_PER_NODE) {
                        ham_trace(("keysize/pagesize ratio too high"));
                        //return (db_set_error(db, HAM_INV_KEYSIZE));
                    }
                    else if (keycount == 0) {
                        ham_trace(("keysize too large for the current pagesize"));
                        //return (db_set_error(db, HAM_INV_KEYSIZE));
                    }
                }
                param->value = keycount;
                break;
            case HAM_PARAM_GET_STATISTICS:
                if (!param->value) {
                    ham_trace(("the value for parameter "
                               "'HAM_PARAM_GET_STATISTICS' must not be NULL "
                               "and reference a ham_statistics_t data "
                               "structure before invoking "
                               "ham_[env_]get_parameters"));
                    return (HAM_INV_PARAMETER);
                }
                else {
                    st = stats_fill_ham_statistics_t(env, db, 
                            (ham_statistics_t *)param->value);
                    if (st)
                        return st;
                }
                break;
            default:
                break;
        }
    }
    return HAM_SUCCESS;
}

HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_get_parameters(ham_env_t *env, ham_parameter_t *param)
{
    ham_parameter_t *p=param;
    if (p) {
        for (; p->name; p++)
            /* statistics require a pointer to ham_statistics_t */
            if (p->name!=HAM_PARAM_GET_STATISTICS)
                p->value=0;
    }

    return (__ham_get_parameters(env, 0, param));
}

HAM_EXPORT ham_status_t HAM_CALLCONV
ham_get_parameters(ham_db_t *db, ham_parameter_t *param)
{
    ham_parameter_t *p=param;
    if (p) {
        for (; p->name; p++)
            /* statistics require a pointer to ham_statistics_t */
            if (p->name!=HAM_PARAM_GET_STATISTICS)
                p->value=0;
    }

    return (__ham_get_parameters((db ? db_get_env(db) : NULL), db, param));
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

ham_status_t HAM_CALLCONV
ham_set_duplicate_compare_func(ham_db_t *db, ham_duplicate_compare_func_t foo)
{
    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    db_set_error(db, 0);
    db_set_duplicate_compare_func(db, foo ? foo : db_default_compare);

    return (HAM_SUCCESS);
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
        return HAM_OUT_OF_MEMORY;
    }

    if (zret!=Z_OK) {
        allocator_free(env_get_allocator(env), dest);
        return HAM_INTERNAL_ERROR;
    }

    record->data=dest;
    record->size=(ham_size_t)newsize;
    return (0);
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
        return (0);

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
        return st;
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
        ham_trace(("parameter 'db' must be linked to a valid (implicit or explicit) environment"));
        return HAM_INV_PARAMETER;
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
        return HAM_OUT_OF_MEMORY;
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
    else
        return (HAM_NOT_IMPLEMENTED);
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
        ham_trace(("parameter 'db' must be linked to a valid (implicit or explicit) environment"));
        return HAM_INV_PARAMETER;
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
    if ((flags&HAM_DIRECT_ACCESS) && !(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)) {
        ham_trace(("flag HAM_DIRECT_ACCESS is only allowed in "
                    "In-Memory Databases"));
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
    if (!be || !be_is_active(be))
        return HAM_NOT_INITIALIZED;

    if (!txn) {
        st=txn_begin(&local_txn, env, HAM_TXN_READ_ONLY);
        if (st)
            return st;
    }

    db_update_global_stats_find_query(db, key->size);

    /*
     * first look up the blob id, then fetch the blob
     */
    st=be->_fun_find(be, key, record, flags);

    if (st) {
        if (!txn)
            (void)txn_abort(&local_txn, DO_NOT_NUKE_PAGE_STATS);
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
            (void)txn_abort(&local_txn, DO_NOT_NUKE_PAGE_STATS);
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
        ham_trace(("parameter 'db' must be linked to a valid (implicit or explicit) environment"));
        return HAM_INV_PARAMETER;
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
    if (!__prepare_key(key) || !__prepare_record(record))
        return HAM_INV_PARAMETER;

    //if (env)
    __prepare_db(db);

    be=db_get_backend(db);
    if (!be || !be_is_active(be))
        return HAM_NOT_INITIALIZED;

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
            return st;
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
                        allocator_free(env_get_allocator(env), db_get_key_allocdata(db));
                    db_set_key_allocdata(db, 
                            allocator_alloc(env_get_allocator(env), sizeof(ham_u64_t)));
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
    ham_env_t *env;
    ham_backend_t *be;
    ham_offset_t recno=0;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    env = db_get_env(db);
    if (!env) {
        ham_trace(("parameter 'db' must be linked to a valid (implicit or explicit) environment"));
        return HAM_INV_PARAMETER;
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

    //if (env)
    __prepare_db(db);

    db_set_error(db, 0);

    be=db_get_backend(db);
    if (!be || !be_is_active(be))
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
        if ((st=txn_begin(&local_txn, env, 0)))
            return (st);
    }

    db_update_global_stats_erase_query(db, key->size);

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
    if (!(db_get_rt_flags(db)&HAM_IN_MEMORY_DB))
    {
        st=cache_check_integrity(env_get_cache(db_get_env(db)));
        if (st)
            return (st);
    }

    be=db_get_backend(db);
    if (!be)
        return (db_set_error(db, HAM_NOT_INITIALIZED));
    if (!txn) {
        if ((st=txn_begin(&local_txn, db_get_env(db), HAM_TXN_READ_ONLY)))
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
#endif /* ifdef HAM_ENABLE_INTERNAL */
}

ham_status_t HAM_CALLCONV
ham_calc_maxkeys_per_page(ham_db_t *db, ham_size_t *keycount, ham_u16_t keysize)
{
#ifdef HAM_ENABLE_INTERNAL
    ham_status_t st;
    ham_backend_t *be;

    if (!keycount) {
        ham_trace(("parameter 'keycount' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    *keycount = 0;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    be=db_get_backend(db);
    if (!be)
        return (db_set_error(db, HAM_NOT_INITIALIZED));
    if (!be->_fun_calc_keycount_per_page)
    {
        ham_trace(("hamsterdb was compiled without support for internal "
                    "functions"));
        return (HAM_NOT_IMPLEMENTED);
    }

    /*
     * call the backend function
     */
    st=be->_fun_calc_keycount_per_page(be, keycount, keysize);

    return (st);

#else /* !HAM_ENABLE_INTERNAL */
    ham_trace(("hamsterdb was compiled without support for internal "
                "functions"));
    return (HAM_NOT_IMPLEMENTED);
#endif /* ifdef HAM_ENABLE_INTERNAL */
}

ham_status_t HAM_CALLCONV
ham_env_flush(ham_env_t *env, ham_u32_t flags)
{
    ham_status_t st;
    ham_db_t *db;
    ham_device_t *dev;

    (void)flags;

    if (!env) {
        ham_trace(("parameter 'env' must not be NULL"));
        return HAM_INV_PARAMETER;
    }

    //if (env)
    //__prepare_db(db);

    //db_set_error(db, 0);

    /*
    * never flush an in-memory-database
    */
    if (env_get_rt_flags(env)&HAM_IN_MEMORY_DB)
        return (0);

    dev = env_get_device(env);
    if (!dev)
        return HAM_NOT_INITIALIZED;

    /*
    * flush the open backends
    */
    db = env_get_list(env);
    while (db) 
    {
        ham_backend_t *be=db_get_backend(db);

        if (!be || !be_is_active(be))
            return HAM_NOT_INITIALIZED;
        st = be->_fun_flush(be);
        if (st)
            return st;
        db = db_get_next(db);
    }

    /*
    * update the header page, if necessary
    */
    if (env_is_dirty(env)) 
    {
        st=page_flush(env_get_header_page(env));
        if (st)
            return st;
    }

    st=db_flush_all(env_get_cache(env), DB_FLUSH_NODELETE);
    if (st)
        return st;

    st=dev->flush(dev);
    if (st)
        return st;

    return (HAM_SUCCESS);
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
        ham_trace(("parameter 'db' must be linked to a valid (implicit or explicit) environment"));
        return HAM_INV_PARAMETER;
    }

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    /*
     * never flush an in-memory-database
     */
    if (env_get_rt_flags(env)&HAM_IN_MEMORY_DB)
        return (0);

    be=db_get_backend(db);
    if (!be || !be_is_active(be))
        return HAM_NOT_INITIALIZED;

    dev = env_get_device(env);
    if (!dev)
        return HAM_NOT_INITIALIZED;

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

    return (HAM_SUCCESS);
}

/*
 * always shut down entirely, even when a page flush or other 
 * 'non-essential' element of the process fails.
 */
ham_status_t HAM_CALLCONV
ham_close(ham_db_t *db, ham_u32_t flags)
{
    ham_status_t st = HAM_SUCCESS;
    ham_status_t st2 = HAM_SUCCESS;
    ham_backend_t *be;
    ham_env_t *env=0;
    ham_bool_t noenv=HAM_FALSE;
    ham_db_t *newowner=0;
    ham_record_filter_t *record_head;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    env = db_get_env(db);
    if ((flags&HAM_TXN_AUTO_ABORT) && (flags&HAM_TXN_AUTO_COMMIT)) {
        ham_trace(("invalid combination of flags: HAM_TXN_AUTO_ABORT + "
                    "HAM_TXN_AUTO_COMMIT"));
        return (HAM_INV_PARAMETER);
    }

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    /*
     * if this Database is the last database in the environment: 
     * delete all environment-members
     */
    if (env) {
        ham_bool_t has_other=HAM_FALSE;
        ham_db_t *n=env_get_list(env);
        while (n) {
            if (n!=db) {
                has_other=HAM_TRUE;
                break;
            }
            n=db_get_next(n);
        }
        if (!has_other)
            noenv=HAM_TRUE;
    }

    /*
     * immediately abort or commit the database - we have to do
     * this right here to decrement the page's refcount
     */
    if (env && env_get_txn(env)) {
        if (flags&HAM_TXN_AUTO_COMMIT)
            st=ham_txn_commit(env_get_txn(env), 0);
        else
            st=ham_txn_abort(env_get_txn(env), 0);
        if (st)
        {
            if (st2 == 0) st2 = st;
        }
        env_set_txn(env, 0);
    }

    be=db_get_backend(db);
    if (!be || !be_is_active(be))
    {
        /* christoph-- i think it's ok if a database is closed twice 
         * st2 = HAM_NOT_INITIALIZED; */
    }
    else if (flags&HAM_AUTO_CLEANUP)
    {
        /*
         * auto-cleanup cursors?
         */
        st2 = be->_fun_close_cursors(be, flags);
        /* error or not, continue closing the database! */
    }
    else if (db_get_cursors(db))
    {
        return (db_set_error(db, HAM_CURSOR_STILL_OPEN));
    }

    /*
     * auto-abort (or commit) a pending transaction?
     */
    if (noenv && env && env_get_txn(env)) 
    {
        /*
        abort transaction when a cursor failure happened: when such a thing
        happened, we're not in a fully controlled state any more so
        'auto-committing' would be extremely dangerous then.
        */
        if (flags&HAM_TXN_AUTO_COMMIT && st2 == 0)
            st=ham_txn_commit(env_get_txn(env), 0);
        else
            st=ham_txn_abort(env_get_txn(env), 0);
        if (st)
        {
            if (st2 == 0) st2 = st;
        }
        env_set_txn(env, 0);
    }

    /* 
     * flush all DB performance data 
     */
    if (st2 == HAM_SUCCESS)
    {
        stats_flush_dbdata(db, db_get_db_perf_data(db), noenv);
    }
    else
    {
        /* trash all DB performance data */
        stats_trash_dbdata(db, db_get_db_perf_data(db));
    }

    /*
     * if we're not in read-only mode, and not an in-memory-database,
     * and the dirty-flag is true: flush the page-header to disk
     */
    if (env
            && env_get_header_page(env) 
            && noenv
            && !(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)
            && env_get_device(env) 
            && env_get_device(env)->is_open(env_get_device(env)) 
            && (!(db_get_rt_flags(db)&HAM_READ_ONLY))) {
        /* flush the database header, if it's dirty */
        if (env_is_dirty(env)) {
            st=page_flush(env_get_header_page(env));
            if (st)
            {
                //db_set_error(db, st);
                if (st2 == 0) st2 = st;
            }
        }
    }

    /*
     * in-memory-database: free all allocated blobs
     */
    if (be && be_is_active(be) && env_get_rt_flags(env)&HAM_IN_MEMORY_DB) 
    {
        ham_txn_t txn;
        free_cb_context_t context;
        context.db=db;
        st = txn_begin(&txn, env, 0);
        if (st) 
        {
            if (st2 == 0) st2 = st;
        }
        else
        {
            (void)be->_fun_enumerate(be, my_free_cb, &context);
            (void)txn_commit(&txn, 0);
        }
    }

    /*
     * immediately flush all pages of this database
     */
    if (env && env_get_cache(env)) {
        ham_page_t *n, *head=cache_get_totallist(env_get_cache(env)); 
        while (head) {
            n=page_get_next(head, PAGE_LIST_CACHED);
            if (page_get_owner(head)==db) {
                if (!(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)) 
                    (void)db_flush_page(env, head, HAM_WRITE_THROUGH);
                (void)db_free_page(head, 0);
            }
            head=n;
        }
    }
    /* otherwise - if we're in an Environment, and there's no other
     * open Database - flush all open pages; we HAVE to flush/free them
     * because their owner is no longer valid */
    else if (env && noenv && env_get_cache(env)) {
        ham_cache_t *cache=env_get_cache(env);
ham_assert(0, (0));
/* i think we can remove this branch... */
        (void)db_flush_all(cache, 0);
        /* 
         * TODO
         * if we delete all pages we have to clear the cache-buckets, too - it's
         * not enough to set the cache-"totallist" pointer to NULL
         * 
         * -> move this to a new function (i.e. cache_reset())
         */
        cache_set_totallist(cache, 0);
        cache_delete(env, cache);
        cache=cache_new(env, env_get_cachesize(env));
        env_set_cache(env, cache);
    }

    /*
     * free cached memory
     */
    (void)db_resize_allocdata(db, 0);
    if (db_get_key_allocdata(db)) {
        allocator_free(env_get_allocator(env), db_get_key_allocdata(db));
        db_set_key_allocdata(db, 0);
        db_set_key_allocsize(db, 0);
    }

    /*
     * free the cache for extended keys
     */
    if (db_get_extkey_cache(db)) 
    {
        extkey_cache_destroy(db_get_extkey_cache(db));
        db_set_extkey_cache(db, 0);
    }

    /* close the backend */
    if (be && be_is_active(be)) 
    {
        st=be->_fun_close(be);
        if (st)
        {
            if (st2 == 0) st2 = st;
        }
        else
        {
            ham_assert(!be_is_active(be), (0));
        }
    }
    if (be)
    {
        ham_assert(!be_is_active(be), (0));

        st = be->_fun_delete(be);
        if (st2 == 0)
            st2 = st;

        /*
         * TODO
         * this free() should move into the backend destructor 
         */
        allocator_free(env_get_allocator(env), be);
        db_set_backend(db, 0);
    }

    /*
     * environment: move the ownership to another database.
     * it's possible that there's no other page, then set the 
     * ownership to 0
     */
    if (env) {
        ham_db_t *head=env_get_list(env);
        while (head) {
            if (head!=db) {
                newowner=head;
                break;
            }
            head=db_get_next(head);
        }
    }
    if (env && env_get_header_page(env)) 
    {
        ham_assert(env_get_header_page(env), (0));
        page_set_owner(env_get_header_page(env), newowner);
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
     * trash all DB performance data 
     *
     * This must happen before the DB is removed from the ENV as the ENV 
     * (when it exists) provides the required allocator.
     */
    stats_trash_dbdata(db, db_get_db_perf_data(db));

    /*
     * remove this database from the environment
     */
    if (env) 
    {
        ham_db_t *prev=0;
        ham_db_t *head=env_get_list(env);
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
        if (db_get_rt_flags(db)&DB_ENV_IS_PRIVATE) {
            (void)ham_env_close(db_get_env(db), flags);
            ham_env_delete(db_get_env(db));
        }
        db_set_env(db, 0);
    }

    db_set_active(db, HAM_FALSE);

    return st2;
}

ham_status_t HAM_CALLCONV
ham_cursor_create(ham_db_t *db, ham_txn_t *txn, ham_u32_t flags,
        ham_cursor_t **cursor)
{
    ham_status_t st;
    ham_env_t *env;
    ham_backend_t *be;
    
    if (!cursor) {
        ham_trace(("parameter 'cursor' must not be NULL"));
        return HAM_INV_PARAMETER;
    }
    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return HAM_INV_PARAMETER;
    }
    env = db_get_env(db);
    if (!env) {
        ham_trace(("parameter 'db' must be linked to a valid (implicit or explicit) environment"));
        return HAM_INV_PARAMETER;
    }

    //if (env)
    __prepare_db(db);

    db_set_error(db, 0);

    be=db_get_backend(db);
    if (!be || !be_is_active(be))
        return HAM_NOT_INITIALIZED;

    st=be->_fun_cursor_create(be, db, txn, flags, cursor);
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

    //if (env)
    __prepare_db(db);

    db_set_error(db, 0);

    if (!cursor_get_txn(src)) {
        st=txn_begin(&local_txn, env, HAM_TXN_READ_ONLY);
        if (st)
            return st;
    }

    st=src->_fun_clone(src, dest);
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
        return HAM_INV_PARAMETER;
    }
    if (!__prepare_record(record))
        return HAM_INV_PARAMETER;

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

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    if (!cursor_get_txn(cursor)) {
        st=txn_begin(&local_txn, env, 0);
        if (st)
            return st;
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
        return (st);
    }

    st=cursor->_fun_overwrite(cursor, &temprec, flags);

    ham_assert(env_get_allocator(env) == cursor_get_allocator(cursor), (0));
    if (temprec.data != record->data)
        allocator_free(env_get_allocator(env), temprec.data);

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
        return (HAM_INV_PARAMETER);
    }
    if ((flags&HAM_DIRECT_ACCESS) && !(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)) {
        ham_trace(("flag HAM_DIRECT_ACCESS is only allowed in "
                   "In-Memory Databases"));
        return (db_set_error(db, HAM_INV_PARAMETER));
    }
    if (key && !__prepare_key(key))
        return (db_set_error(db, HAM_INV_PARAMETER));
    if (record && !__prepare_record(record))
        return (db_set_error(db, HAM_INV_PARAMETER));

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    if (!cursor_get_txn(cursor)) {
        st=txn_begin(&local_txn, env, HAM_TXN_READ_ONLY);
        if (st)
            return st;
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
        return HAM_INV_PARAMETER;
    }

    if (flags & ~(HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH | 
                HAM_FIND_EXACT_MATCH | HAM_DIRECT_ACCESS)) {
        ham_trace(("flag values besides any combination of "
                   "HAM_FIND_LT_MATCH, HAM_FIND_GT_MATCH, "
                   "HAM_FIND_EXACT_MATCH and HAM_DIRECT_ACCESS are not allowed"));
        return HAM_INV_PARAMETER;
    }
    if ((flags&HAM_DIRECT_ACCESS) && !(env_get_rt_flags(env)&HAM_IN_MEMORY_DB)) {
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
        return HAM_INV_PARAMETER;
    if (record &&  !__prepare_record(record))
        return HAM_INV_PARAMETER;

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

    if (!cursor_get_txn(cursor)) {
        st=txn_begin(&local_txn, env, HAM_TXN_READ_ONLY);
        if (st)
            return st;
    }

    db_update_global_stats_find_query(db, key->size);

    st=cursor->_fun_find(cursor, key, record, flags);
    if (st)
    {
        if (!cursor_get_txn(cursor))
            (void)txn_abort(&local_txn, DO_NOT_NUKE_PAGE_STATS);
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
                (void)txn_abort(&local_txn, DO_NOT_NUKE_PAGE_STATS);
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
                        allocator_free(env_get_allocator(env), db_get_key_allocdata(db));
                    db_set_key_allocdata(db, 
                            allocator_alloc(env_get_allocator(env), sizeof(ham_u64_t)));
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
            return (st);
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

    if (db_get_env(cursor_get_db(cursor)))
        __prepare_db(cursor_get_db(cursor));

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
            return st;
    }

    db_update_global_stats_erase_query(db, 0);

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
        return (HAM_INV_PARAMETER);
    }

    *count=0;

    if (db_get_env(cursor_get_db(cursor)))
        __prepare_db(cursor_get_db(cursor));

    db_set_error(cursor_get_db(cursor), 0);

    if (!cursor_get_txn(cursor)) {
        st=txn_begin(&local_txn, env, HAM_TXN_READ_ONLY);
        if (st)
            return st;
    }

    st=(*cursor->_fun_get_duplicate_count)(cursor, count, flags);
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

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    st=cursor->_fun_close(cursor);
    if (!st) {
        if (cursor_get_txn(cursor))
            txn_set_cursor_refcount(cursor_get_txn(cursor), 
                    txn_get_cursor_refcount(cursor_get_txn(cursor))-1);
        allocator_free(cursor_get_allocator(cursor), cursor);
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
        return (HAM_FILTER_NOT_FOUND);

    filter->_prev = 0;
    filter->_next = 0;
    return 0;
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

typedef struct
{
    ham_db_t *db;               /* [in] */
    ham_u32_t flags;            /* [in] */
    ham_offset_t total_count;   /* [out] */
    ham_bool_t is_leaf;         /* [scratch] */
}  calckeys_context_t;

/*
 * callback function for estimating / counting the number of keys stored in the database
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
    ham_env_t *env;
    calckeys_context_t ctx = {db, flags, 0, HAM_FALSE};

    if (!keycount) {
        ham_trace(("parameter 'keycount' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    *keycount = 0;

    if (!db) {
        ham_trace(("parameter 'db' must not be NULL"));
        return (HAM_INV_PARAMETER);
    }
    
    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    if (flags & ~(HAM_SKIP_DUPLICATES | HAM_FAST_ESTIMATE)) {
        ham_trace(("parameter 'flag' contains unsupported flag bits: %08x", 
                  flags & ~(HAM_SKIP_DUPLICATES | HAM_FAST_ESTIMATE)));
        return (HAM_INV_PARAMETER);
    }

    be = db_get_backend(db);
    if (!be || !be_is_active(be))
        return HAM_NOT_INITIALIZED;
    if (!txn) {
        st = txn_begin(&local_txn, env, HAM_TXN_READ_ONLY);
        if (st)
            return st;
    }

    /*
     * call the backend function
     */
    st = be->_fun_enumerate(be, my_calc_keys_cb, &ctx);

    if (st) {
        if (!txn)
            (void)txn_abort(&local_txn, 0);
        return (st);
    }

    *keycount = ctx.total_count;

    if (!txn)
        return (txn_commit(&local_txn, 0));
    else
        return (st);
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
