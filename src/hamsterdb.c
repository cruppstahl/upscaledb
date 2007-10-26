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

#ifdef HAVE_MALLOC_H
#  include <malloc.h>
#else
#  include <stdlib.h>
#endif
#include <string.h>
#include <ham/hamsterdb.h>
#include "config.h"
#include "error.h"
#include "mem.h"
#include "env.h"
#include "db.h"
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
#include "../3rdparty/aes/aes.h"

/* private parameter list entry for ham_create_ex */
#define HAM_PARAM_DBNAME          (1000)

/* a reserved database name for those databases, who're created without
 * an environment (and therefore don't have a name) */
#define EMPTY_DATABASE_NAME       (0xf000)

/* a reserved database name for the first database in an environment */
#define FIRST_DATABASE_NAME       (0xf001)


typedef struct free_cb_context_t
{
    ham_db_t *db;

    ham_bool_t is_leaf;

} free_cb_context_t;

/*
 * callback function for ham_dump
 */
#ifdef HAM_ENABLE_INTERNAL
static void
my_dump_cb(int event, void *param1, void *param2, void *context)
{
    ham_size_t i, limit, keysize;
    ham_page_t *page;
    int_key_t *key;
    ham_u8_t *data;
    ham_dump_cb_t cb=(ham_dump_cb_t)context;

    switch (event) {
    case ENUM_EVENT_DESCEND:
        break;

    case ENUM_EVENT_PAGE_START:
        page=(ham_page_t *)param1;
        printf("\n------ page 0x%llx ---------------------------------------\n",
				(long long unsigned)page_get_self(page));
        break;

    case ENUM_EVENT_ITEM:
        key=(int_key_t *)param1;
        data=key_get_key(key);
        keysize=key_get_size(key);

        if (cb) {
            cb(data, keysize);
        }
        else {
            printf(" %02d: ", *(int *)param2);
            printf(" key (%2d byte): ", key_get_size(key));

            if (keysize>16)
                limit=16;
            else
                limit=keysize;

            for (i=0; i<limit; i++)
                printf("%02x ", data[i]);

            if (keysize>limit)
                printf("... (%d more bytes)\n", keysize-limit);
            else
                printf("\n");

            printf("      ptr: 0x%llx\n",
                    (long long unsigned)key_get_ptr(key));
        }
        break;

    default:
        ham_assert(!"unknown callback event", (""));
        break;
    }
}
#endif /* HAM_ENABLE_INTERNAL */

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
        if (!db_get_rt_flags(c->db)&HAM_IN_MEMORY_DB) {
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

const char *
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
        case HAM_CURSOR_IS_NIL:
            return ("Cursor points to NIL");
        case HAM_ENV_NOT_EMPTY:
            return ("Not all databases were closed before "
                    "closing the environment");
        case HAM_DATABASE_NOT_FOUND:
            return ("Database not found");
        case HAM_DATABASE_ALREADY_EXISTS:
            return ("Database name already exists");
        case HAM_DATABASE_ALREADY_OPEN:
            return ("Database already open");
        case HAM_LIMITS_REACHED:
            return ("Database limits reached");
        case HAM_DB_NOT_EMPTY:
            return ("Not all cursors were closed before "
                    "closing the database");
        default:
            return ("Unknown error");
    }
}

static ham_bool_t
__prepare_key(ham_key_t *key)
{
    if (key->size && !key->data)
        return (0);
    if (key->flags!=0 && key->flags!=HAM_KEY_USER_ALLOC)
        return (0);
    key->_flags=0;
    return (1);
}

static ham_bool_t
__prepare_record(ham_record_t *record)
{
    if (record->size && !record->data)
        return (0);
    if (record->flags!=0 && record->flags!=HAM_RECORD_USER_ALLOC)
        return (0);
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
        ham_u32_t *flags, ham_parameter_t *param, ham_size_t *ppagesize, 
        ham_u16_t *pkeysize, ham_size_t *pcachesize, ham_u16_t *pdbname)
{
    ham_u32_t pagesize=0;
    ham_u16_t keysize=0, dbname=EMPTY_DATABASE_NAME;
    ham_size_t cachesize=0;
    ham_bool_t no_mmap=HAM_FALSE;

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
                if (is_env) /* calling from ham_env_create_ex? */
                    return (HAM_INV_PARAMETER);
                else {
                    keysize=(ham_u16_t)param->value;
                    if ((*flags)&HAM_RECORD_NUMBER)
                        if (keysize>0 && keysize<sizeof(ham_u64_t))
                            return (HAM_INV_KEYSIZE);
                }
                break;
            case HAM_PARAM_PAGESIZE:
                pagesize=(ham_u32_t)param->value;
                break;
            case HAM_PARAM_DBNAME:
                dbname=(ham_u16_t)param->value;
                break;
            default:
                return (HAM_INV_PARAMETER);
            }
        }
    }

    if (dbname==EMPTY_DATABASE_NAME) {
        if (!filename && !((*flags)&HAM_IN_MEMORY_DB))
            return (HAM_INV_PARAMETER);
    }

    /*
     * make sure that the pagesize is aligned to 1024k
     */
    if (pagesize) {
        if (pagesize%1024)
            return (HAM_INV_PAGESIZE);
    }

    /*
     * creating a file in READ_ONLY mode? doesn't make sense
     */
    if ((*flags)&HAM_READ_ONLY)
        return (HAM_INV_PARAMETER);

    /*
     * in-memory-db? don't allow cache limits!
     */
    if ((*flags)&HAM_IN_MEMORY_DB) {
        if (((*flags)&HAM_CACHE_STRICT) || cachesize!=0)
            return (HAM_INV_PARAMETER);
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
     * make sure that the pagesize is big enough for at least 5 keys;
     * record number database: need 8 byte
     */
    if (keysize) {
        if (pagesize/keysize<5)
            return (HAM_INV_KEYSIZE);
    }

    /*
     * initialize the keysize with a good default value;
     * 32byte is the size of a first level cache line for most modern
     * processors; adjust the keysize, so the keys are aligned to
     * 32byte
     */
    if (keysize==0) {
        if ((*flags)&HAM_RECORD_NUMBER)
            keysize=sizeof(ham_u64_t);
        else
            keysize=32-(sizeof(int_key_t)-1);
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

void
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

ham_status_t
ham_env_new(ham_env_t **env)
{
    if (!env)
        return (HAM_INV_PARAMETER);

    /* allocate memory for the ham_db_t-structure;
     * we can't use our allocator because it's not yet created! */
    *env=(ham_env_t *)malloc(sizeof(ham_env_t));
    if (!(*env))
        return (HAM_OUT_OF_MEMORY);

    /* reset the whole structure */
    memset(*env, 0, sizeof(ham_env_t));

    return (0);
}

ham_status_t
ham_env_delete(ham_env_t *env)
{
    if (!env)
        return (HAM_INV_PARAMETER);

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

ham_status_t
ham_env_create(ham_env_t *env, const char *filename,
        ham_u32_t flags, ham_u32_t mode)
{
    return (ham_env_create_ex(env, filename, flags, mode, 0));
}

ham_status_t
ham_env_create_ex(ham_env_t *env, const char *filename,
        ham_u32_t flags, ham_u32_t mode, ham_parameter_t *param)
{
    ham_status_t st;
    ham_size_t pagesize;
    ham_u16_t keysize;
    ham_size_t cachesize;
    ham_device_t *device;

    if (!env)
        return (HAM_INV_PARAMETER);

    /*
     * check (and modify) the parameters
     */
    st=__check_create_parameters(HAM_TRUE, filename, &flags, param, 
            &pagesize, &keysize, &cachesize, 0);
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
     * initialize the device 
     */
    device=ham_device_new(env_get_allocator(env), flags&HAM_IN_MEMORY_DB);
    if (!device)
        return (HAM_OUT_OF_MEMORY);

    env_set_device(env, device);

    /* 
     * create the file 
     */
    st=device->create(device, filename, flags, mode);
    if (st) {
        (void)ham_env_close(env, 0);
        return (st);
    }

    /*
     * store the parameters
     */
    env_set_rt_flags(env, flags);
    env_set_pagesize(env, pagesize);
    env_set_keysize(env, keysize);
    env_set_cachesize(env, cachesize);

    return (HAM_SUCCESS);
}

ham_status_t
ham_env_create_db(ham_env_t *env, ham_db_t *db,
        ham_u16_t name, ham_u32_t flags, ham_parameter_t *param)
{
    ham_status_t st;
    ham_u16_t keysize=env_get_keysize(env);

    if (!env || !db)
        return (HAM_INV_PARAMETER);

    if (!name || name>=EMPTY_DATABASE_NAME)
        return (HAM_INV_PARAMETER);

    if (env_get_rt_flags(env)&HAM_READ_ONLY)
        return (HAM_DB_READ_ONLY);

    /*
     * only a few flags are allowed
     */
    if (flags&~(HAM_USE_BTREE|HAM_DISABLE_VAR_KEYLEN
               |HAM_RECORD_NUMBER|HAM_ENABLE_DUPLICATES))
        return (HAM_INV_PARAMETER);

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
    st=ham_create_ex(db, 0, flags|env_get_rt_flags(env), 0, full_param);
    if (st)
        return (st);
    }

    /*
     * on success: store the open database in the environment's list of
     * opened databases
     */
    db_set_next(db, env_get_list(env));
    env_set_list(env, db);

    return (0);
}

ham_status_t
ham_env_open_db(ham_env_t *env, ham_db_t *db,
        ham_u16_t name, ham_u32_t flags, ham_parameter_t *params)
{
    ham_db_t *head;
    ham_status_t st;
    ham_parameter_t full_param[]={
        {HAM_PARAM_CACHESIZE, env_get_cachesize(env)},
        {HAM_PARAM_DBNAME,    name},
        {0, 0}};

    if (!env || !db)
        return (HAM_INV_PARAMETER);

    if (!name)
        return (HAM_INV_PARAMETER);
    if (name!=FIRST_DATABASE_NAME && name>=EMPTY_DATABASE_NAME)
        return (HAM_INV_PARAMETER);

    /* 
     * parameters aren't allowed
     */
    if (params)
        return (HAM_INV_PARAMETER);

    /*
     * make sure that this database is not yet open
     */
    head=env_get_list(env);
    while (head) {
        ham_u8_t *ptr=db_get_indexdata(head);
        if (ham_db2h16(*(ham_u16_t *)ptr)==name)
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

ham_status_t
ham_env_open(ham_env_t *env, const char *filename, ham_u32_t flags)
{
    return (ham_env_open_ex(env, filename, flags, 0));
}

ham_status_t
ham_env_open_ex(ham_env_t *env, const char *filename,
        ham_u32_t flags, ham_parameter_t *param)
{
    ham_status_t st;
    ham_size_t cachesize=0;
    ham_device_t *device=0;

    if (!env)
        return (HAM_INV_PARAMETER);

    /* 
     * cannot open an in-memory-db 
     */
    if (flags&HAM_IN_MEMORY_DB)
        return (HAM_INV_PARAMETER);

    /* 
     * parse parameters
     */
    if (param) {
        for (; param->name; param++) {
            switch (param->name) {
            case HAM_PARAM_CACHESIZE:
                cachesize=(ham_size_t)param->value;
                break;
            default:
                return (HAM_INV_PARAMETER);
            }
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
     * initialize the device 
     */
    device=ham_device_new(env_get_allocator(env), flags&HAM_IN_MEMORY_DB);
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
     * store the parameters
     */
    env_set_pagesize(env, 0);
    env_set_keysize(env, 0);
    env_set_cachesize(env, cachesize);
    env_set_rt_flags(env, flags);

    return (HAM_SUCCESS);
}

ham_status_t
ham_env_rename_db(ham_env_t *env, ham_u16_t oldname, 
                ham_u16_t newname, ham_u32_t flags)
{
    ham_u16_t i, slot;
    ham_u8_t *ptr;
    ham_db_t *db;
    ham_bool_t owner=HAM_FALSE;
    ham_status_t st;

    if (!env)
        return (HAM_INV_PARAMETER);
    if (!oldname || !newname || newname>=EMPTY_DATABASE_NAME)
        return (HAM_INV_PARAMETER);

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
    slot=db_get_indexdata_size(db);
    for (i=0; i<db_get_indexdata_size(db); i++) {
        ham_u16_t name=ham_h2db16(*(ham_u16_t *)db_get_indexdata_at(db, i));
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

    if (slot==db_get_indexdata_size(db)) {
        if (owner) {
            (void)ham_close(db, 0);
            (void)ham_delete(db);
        }
        return (HAM_DATABASE_NOT_FOUND);
    }

    /*
     * replace the database name with the new name
     */
    ptr=db_get_indexdata_at(db, slot);
    *(ham_u16_t *)ptr=ham_db2h16(newname);

    db_set_dirty(db, HAM_TRUE);
    
    if (owner) {
        (void)ham_close(db, 0);
        (void)ham_delete(db);
    }

    return (0);
}

ham_status_t
ham_env_erase_db(ham_env_t *env, ham_u16_t name, ham_u32_t flags)
{
    ham_db_t *db;
    ham_status_t st;
    free_cb_context_t context;
    ham_txn_t txn;

    if (!env || !name)
        return (HAM_INV_PARAMETER);

    /*
     * check if this database is still open
     */
    db=env_get_list(env);
    while (db) {
        ham_u16_t dbname=ham_db2h16(*(ham_u16_t *)db_get_indexdata(db));
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
    if ((st=ham_txn_begin(&txn, db))) {
        (void)ham_close(db, 0);
        (void)ham_delete(db);
        return (st);
    }
    context.db=db;
    st=db_get_backend(db)->_fun_enumerate(db_get_backend(db), 
            my_free_cb, &context);
    if (st) {
        (void)ham_txn_abort(&txn);
        (void)ham_close(db, 0);
        (void)ham_delete(db);
        return (st);
    }

    st=ham_txn_commit(&txn, 0);
    if (st) {
        (void)ham_close(db, 0);
        (void)ham_delete(db);
        return (st);
    }

    /*
     * set database name to 0
     */
    *(ham_u16_t *)db_get_indexdata(db)=ham_h2db16(0);
    db_set_dirty(db, HAM_TRUE);

    /*
     * clean up and return
     */
    (void)ham_close(db, 0);
    (void)ham_delete(db);

    return (0);
}

ham_status_t
ham_env_close(ham_env_t *env, ham_u32_t flags)
{
    ham_status_t st;

    if (!env)
        return (HAM_INV_PARAMETER);
    if (env_get_list(env) && !(flags&HAM_AUTO_CLEANUP))
        return (HAM_ENV_NOT_EMPTY);

    /*
     * close all databases?
     */
    if (flags&HAM_AUTO_CLEANUP) {
        ham_db_t *db=env_get_list(env);
        while (db) {
            ham_db_t *next=db_get_next(db);
            st=ham_close(db, HAM_AUTO_CLEANUP);
            if (st)
                return (st);
            db=next;
        }
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
     * close the memory allocator 
     */
    if (env_get_allocator(env)) {
        env_get_allocator(env)->close(env_get_allocator(env));
        env_set_allocator(env, 0);
    }

    return (HAM_SUCCESS);
}

ham_status_t
ham_new(ham_db_t **db)
{
    if (!db)
        return (HAM_INV_PARAMETER);

    /* allocate memory for the ham_db_t-structure;
     * we can't use our allocator because it's not yet created! */
    *db=(ham_db_t *)malloc(sizeof(ham_db_t));
    if (!(*db))
        return (HAM_OUT_OF_MEMORY);

    /* reset the whole structure */
    memset(*db, 0, sizeof(ham_db_t));

    return (0);
}

ham_status_t
ham_delete(ham_db_t *db)
{
    if (!db)
        return (HAM_INV_PARAMETER);

    /* free cached data pointers */
    (void)db_resize_allocdata(db, 0);

    /* "free" all remaining memory */
    free(db);

    return (0);
}

ham_status_t
ham_open(ham_db_t *db, const char *filename, ham_u32_t flags)
{
    return (ham_open_ex(db, filename, flags, 0));
}

ham_status_t
ham_open_ex(ham_db_t *db, const char *filename,
        ham_u32_t flags, ham_parameter_t *param)
{
    ham_status_t st;
    ham_cache_t *cache;
    ham_backend_t *backend;
    ham_u8_t hdrbuf[512];
    ham_u16_t dbname=FIRST_DATABASE_NAME;
    ham_size_t i, cachesize=0, pagesize=0;
    ham_page_t *page;
    ham_device_t *device;

    if (!db)
        return (HAM_INV_PARAMETER);

    db_set_error(db, 0);

    /* cannot open an in-memory-db */
    if (flags&HAM_IN_MEMORY_DB)
        return (db_set_error(db, HAM_INV_PARAMETER));
    /* HAM_ENABLE_DUPLICATES has to be specified in ham_create, not 
     * ham_open */
    if (flags&HAM_ENABLE_DUPLICATES)
        return (db_set_error(db, HAM_INV_PARAMETER));

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
                return (db_set_error(db, HAM_INV_PARAMETER));
            }
        }
    }

    if (!db_get_env(db) && !filename)
        return (HAM_INV_PARAMETER);

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
        st=device->read(device, 0, hdrbuf, sizeof(hdrbuf));
        if (st) {
            ham_log(("os_pread of %s failed with status %d (%s)", filename,
                    st, ham_strerror(st)));
            (void)ham_close(db, 0);
            return (db_set_error(db, st));
        }
        pagesize=ham_db2h32(((db_header_t *)&hdrbuf[12])->_pagesize);
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

        /* 
         * create the freelist - not needed for in-memory-databases
         */
        if (!(flags&HAM_IN_MEMORY_DB)) {
            st=freel_create(db);
            if (st) {
                ham_log(("unable to create freelist"));
                (void)ham_close(db, 0);
                return (st);
            }
        }
    }

    db_set_error(db, HAM_SUCCESS);

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
        ham_assert(page_get_type(page)==PAGE_TYPE_HEADER,
                ("invalid page header type"));

        if (db_get_env(db))
            env_set_header_page(db_get_env(db), page);
        else
            db_set_header_page(db, page);
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
        ham_log(("invalid file type - %s is not a hamster-db", filename));
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
     * search for a database with this name
     */
    for (i=0; i<db_get_indexdata_size(db); i++) {
        ham_u8_t *ptr=db_get_indexdata_at(db, i);
        if (0==ham_h2db16(*(ham_u16_t *)ptr))
            continue;
        if (dbname==FIRST_DATABASE_NAME ||
            dbname==ham_h2db16(*(ham_u16_t *)ptr)) {
            db_set_indexdata_offset(db, i);
            break;
        }
    }
    if (i==db_get_indexdata_size(db)) {
        (void)ham_close(db, 0);
        return (db_set_error(db, HAM_DATABASE_NOT_FOUND));
    }

    /* 
     * create the backend
     */
    backend=db_create_backend(db, flags);
    if (!backend) {
        ham_log(("unable to create backend with flags 0x%x", flags));
        (void)ham_close(db, 0);
        return (db_set_error(db, HAM_NOT_INITIALIZED));
    }
    db_set_backend(db, backend);

    /* 
     * initialize the backend 
     */
    st=backend->_fun_open(backend, flags);
    if (st) {
        ham_log(("backend create() failed with status %d (%s)",
                st, ham_strerror(st)));
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
    ham_assert(!(be_get_flags(backend)&HAM_DISABLE_MMAP), 
            ("invalid persistent database flags 0x%x", be_get_flags(backend)));
    ham_assert(!(be_get_flags(backend)&HAM_WRITE_THROUGH), 
            ("invalid persistent database flags 0x%x", be_get_flags(backend)));
    ham_assert(!(be_get_flags(backend)&HAM_READ_ONLY), 
            ("invalid persistent database flags 0x%x", be_get_flags(backend)));
    ham_assert(!(be_get_flags(backend)&HAM_DISABLE_FREELIST_FLUSH), 
            ("invalid persistent database flags 0x%x", be_get_flags(backend)));
    ham_assert(!(be_get_flags(backend)&DB_USE_MMAP), 
            ("invalid persistent database flags 0x%x", be_get_flags(backend)));

    /* 
     * initialize the cache
     */
    if (!db_get_cache(db)) {
        if (cachesize==0)
            cachesize=HAM_DEFAULT_CACHESIZE; 
        cache=cache_new(db, cachesize/db_get_pagesize(db));
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

ham_status_t
ham_create(ham_db_t *db, const char *filename, ham_u32_t flags, ham_u32_t mode)
{
    return (ham_create_ex(db, filename, flags, mode, 0));
}

ham_status_t
ham_create_ex(ham_db_t *db, const char *filename,
        ham_u32_t flags, ham_u32_t mode, ham_parameter_t *param)
{
    ham_status_t st;
    ham_cache_t *cache;
    ham_backend_t *backend;
    ham_page_t *page;
    ham_u32_t pflags;
    ham_device_t *device;

    ham_size_t pagesize;
    ham_u16_t keysize, dbname=0, i;
    ham_size_t cachesize;

    if (!db)
        return (HAM_INV_PARAMETER);

    db_set_error(db, 0);


    /*
     * check (and modify) the parameters
     */
    st=__check_create_parameters(HAM_FALSE, filename, &flags, param, 
            &pagesize, &keysize, &cachesize, &dbname);
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
        /* 
         * create the file 
         */
        st=device->create(device, filename, flags, mode);
        if (st) {
            (void)ham_close(db, 0);
            return (db_set_error(db, st));
        }
    }
    else
        device=db_get_device(db);

    /*
     * set the flags
     */
    pflags=flags;
    pflags&=~HAM_DISABLE_VAR_KEYLEN;
    pflags&=~HAM_CACHE_STRICT;
    pflags&=~HAM_DISABLE_MMAP;
    pflags&=~HAM_WRITE_THROUGH;
    pflags&=~HAM_READ_ONLY;
    pflags&=~HAM_DISABLE_FREELIST_FLUSH;
    pflags&=~DB_USE_MMAP;
    db_set_rt_flags(db, flags);

    /* 
     * if there's no header page, allocate one
     */
    if (!db_get_header_page(db)) {
        page=page_new(db);
        if (!page) {
            ham_log(("unable to allocate the header page"));
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
        page_set_dirty(page, 1);

        /* initialize the header */
        db_set_magic(db, 'H', 'A', 'M', '\0');
        db_set_version(db, HAM_VERSION_MAJ, HAM_VERSION_MIN, 
                HAM_VERSION_REV, 0);
        db_set_serialno(db, HAM_SERIALNO);
        db_set_error(db, HAM_SUCCESS);
        db_set_pagesize(db, pagesize);
        db_set_indexdata_size(db, DB_MAX_INDICES);

        /* 
         * create the freelist - not needed for in-memory-databases
         */
        if (!(flags&HAM_IN_MEMORY_DB)) {
            st=freel_create(db);
            if (st) {
                ham_log(("unable to create freelist"));
                (void)ham_close(db, 0);
                return (db_set_error(db, st));
            }
        }
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
     * check if this database name is unique
     */
    for (i=0; i<db_get_indexdata_size(db); i++) {
        ham_u16_t name=ham_h2db16(*(ham_u16_t *)db_get_indexdata_at(db, i));
        if (name==dbname) {
            (void)ham_close(db, 0);
            return (db_set_error(db, HAM_DATABASE_ALREADY_EXISTS));
        }
    }

    /*
     * find a free slot in the indexdata array and store the 
     * database name
     */
    for (i=0; i<db_get_indexdata_size(db); i++) {
        ham_u8_t *ptr=db_get_indexdata_at(db, i);
        ham_u16_t name=ham_h2db16(*(ham_u16_t *)ptr);
        if (!name) {
            *(ham_u16_t *)ptr=ham_db2h16(dbname);
            db_set_indexdata_offset(db, i);
            break;
        }
    }
    if (i==db_get_indexdata_size(db)) {
        (void)ham_close(db, 0);
        return (db_set_error(db, HAM_LIMITS_REACHED));
    }

    /* 
     * initialize the cache
     */
    if (!db_get_cache(db)) {
        if (cachesize==0)
            cachesize=HAM_DEFAULT_CACHESIZE;
        cache=cache_new(db, cachesize/db_get_pagesize(db));
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
        ham_log(("unable to create backend with flags 0x%x", flags));
        (void)ham_close(db, 0);
        return (db_set_error(db, HAM_NOT_INITIALIZED));
    }

    /* 
     * initialize the backend
     */
    st=backend->_fun_create(backend, keysize, pflags);
    if (st) {
        ham_log(("unable to create the backend"));
        (void)ham_close(db, 0);
        db_set_error(db, st);
        return (st);
    }

    /* 
     * store the backend in the database
     */
    db_set_backend(db, backend);

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

ham_status_t
ham_get_error(ham_db_t *db)
{
    if (!db)
        return (0);

    return (db_get_error(db));
}

ham_status_t
ham_set_prefix_compare_func(ham_db_t *db, ham_prefix_compare_func_t foo)
{
    if (!db)
        return (HAM_INV_PARAMETER);

    db_set_error(db, 0);
    db_set_prefix_compare_func(db, foo ? foo : db_default_prefix_compare);

    return (HAM_SUCCESS);
}

ham_status_t
ham_set_compare_func(ham_db_t *db, ham_compare_func_t foo)
{
    if (!db)
        return (HAM_INV_PARAMETER);

    db_set_error(db, 0);
    db_set_compare_func(db, foo ? foo : db_default_compare);

    return (HAM_SUCCESS);
}

static ham_status_t
__aes_pre_cb(ham_db_t *db, ham_page_filter_t *filter, 
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
__aes_post_cb(ham_db_t *db, ham_page_filter_t *filter, 
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
__aes_close_cb(ham_db_t *db, ham_page_filter_t *filter)
{
    if (filter) {
        if (filter->userdata)
            ham_mem_free(db, filter->userdata);
        ham_mem_free(db, filter);
    }
}

ham_status_t
ham_enable_encryption(ham_db_t *db, ham_u8_t key[16], ham_u32_t flags)
{
    ham_page_filter_t *filter;

    if (!db)
        return (HAM_INV_PARAMETER);

    db_set_error(db, 0);

    if (!key)
        return (db_set_error(db, HAM_INV_PARAMETER));

    filter=(ham_page_filter_t *)ham_mem_calloc(db, sizeof(*filter));
    if (!filter)
        return (db_set_error(db, HAM_OUT_OF_MEMORY));

    filter->userdata=ham_mem_calloc(db, 256);
    if (!filter->userdata) {
        ham_mem_free(db, filter);
        return (db_set_error(db, HAM_OUT_OF_MEMORY));
    }

    aes_expand_key(key, filter->userdata);
    filter->pre_cb=__aes_pre_cb;
    filter->post_cb=__aes_post_cb;
    filter->close_cb=__aes_close_cb;

    return (ham_add_page_filter(db, filter));
}

ham_status_t
ham_find(ham_db_t *db, void *reserved, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
    ham_txn_t txn;
    ham_status_t st;
    ham_backend_t *be;
    ham_offset_t recno=0;

    if (!db)
        return (HAM_INV_PARAMETER);
    if (!key || !record)
        return (db_set_error(db, HAM_INV_PARAMETER));
    if (!__prepare_key(key) || !__prepare_record(record))
        return (db_set_error(db, HAM_INV_PARAMETER));

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    /*
     * record number: make sure that we have a valid key structure
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        if (key->size!=sizeof(ham_u64_t) || !key->data)
            return (db_set_error(db, HAM_INV_PARAMETER));
        recno=*(ham_offset_t *)key->data;
        recno=ham_h2db64(recno);
        *(ham_offset_t *)key->data=recno;
    }

    be=db_get_backend(db);
    if (!be)
        return (db_set_error(db, HAM_NOT_INITIALIZED));

    if ((st=ham_txn_begin(&txn, db)))
        return (st);

    /*
     * first look up the blob id, then fetch the blob
     */
    st=be->_fun_find(be, key, record, flags);
    if (st==HAM_SUCCESS)
        st=util_read_record(db, record, flags);

    if (st) {
        (void)ham_txn_abort(&txn);
        return (st);
    }

    /*
     * record number: re-translate the number to host endian
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        *(ham_offset_t *)key->data=ham_db2h64(recno);
    }

    return (ham_txn_commit(&txn, 0));
}

ham_status_t
ham_insert(ham_db_t *db, void *reserved, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
    ham_txn_t txn;
    ham_status_t st;
    ham_backend_t *be;
    ham_u64_t recno;

    if (!db)
        return (HAM_INV_PARAMETER);
    if (!key || !record)
        return (db_set_error(db, HAM_INV_PARAMETER));
    if (!__prepare_key(key) || !__prepare_record(record))
        return (db_set_error(db, HAM_INV_PARAMETER));

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    be=db_get_backend(db);
    if (!be)
        return (db_set_error(db, HAM_NOT_INITIALIZED));
    if (db_get_rt_flags(db)&HAM_READ_ONLY)
        return (db_set_error(db, HAM_DB_READ_ONLY));
    if ((db_get_rt_flags(db)&HAM_DISABLE_VAR_KEYLEN) &&
        (key->size>db_get_keysize(db)))
        return (db_set_error(db, HAM_INV_KEYSIZE));
    if ((db_get_keysize(db)<sizeof(ham_offset_t)) &&
        (key->size>db_get_keysize(db)))
        return (db_set_error(db, HAM_INV_KEYSIZE));
    if ((flags&HAM_DUPLICATE) && (flags&HAM_OVERWRITE))
        return (db_set_error(db, HAM_INV_PARAMETER));
    if ((flags&HAM_DUPLICATE) && !(db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES))
        return (db_set_error(db, HAM_INV_PARAMETER));
    if ((flags&HAM_DUPLICATE_INSERT_AFTER)
            || (flags&HAM_DUPLICATE_INSERT_BEFORE)
            || (flags&HAM_DUPLICATE_INSERT_LAST)
            || (flags&HAM_DUPLICATE_INSERT_FIRST))
        return (db_set_error(db, HAM_INV_PARAMETER));
    if ((st=ham_txn_begin(&txn, db)))
        return (st);

    /*
     * record number: make sure that we have a valid key structure,
     * and lazy load the last used record number
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        if (flags&HAM_OVERWRITE) {
            if (key->size!=sizeof(ham_u64_t) || !key->data)
                return (HAM_INV_PARAMETER);
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
                    (void)ham_txn_abort(&txn);
                    return (HAM_INV_PARAMETER);
                }
            }
            else {
                if (key->data || key->size) {
                    (void)ham_txn_abort(&txn);
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
                        (void)ham_txn_abort(&txn);
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
     * store the index entry; the backend will store the blob
     */
    st=be->_fun_insert(be, key, record, flags);

    if (st) {
        (void)ham_txn_abort(&txn);

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

    return (ham_txn_commit(&txn, 0));
}

ham_status_t
ham_erase(ham_db_t *db, void *reserved, ham_key_t *key, ham_u32_t flags)
{
    ham_txn_t txn;
    ham_status_t st;
    ham_backend_t *be;
    ham_offset_t recno=0;

    if (!db)
        return (HAM_INV_PARAMETER);
    if (!key)
        return (db_set_error(db, HAM_INV_PARAMETER));
    if (!__prepare_key(key))
        return (db_set_error(db, HAM_INV_PARAMETER));

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    be=db_get_backend(db);
    if (!be)
        return (db_set_error(db, HAM_NOT_INITIALIZED));
    if (db_get_rt_flags(db)&HAM_READ_ONLY)
        return (db_set_error(db, HAM_DB_READ_ONLY));

    /*
     * record number: make sure that we have a valid key structure
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        if (key->size!=sizeof(ham_u64_t) || !key->data)
            return (db_set_error(db, HAM_INV_PARAMETER));
        recno=*(ham_offset_t *)key->data;
        recno=ham_h2db64(recno);
        *(ham_offset_t *)key->data=recno;
    }

    if ((st=ham_txn_begin(&txn, db)))
        return (st);

    /*
     * get rid of the entry
     */
    st=be->_fun_erase(be, key, flags);

    if (st) {
        (void)ham_txn_abort(&txn);
        return (st);
    }

    /*
     * record number: re-translate the number to host endian
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        *(ham_offset_t *)key->data=ham_db2h64(recno);
    }

    return (ham_txn_commit(&txn, 0));
}

ham_status_t
ham_dump(ham_db_t *db, void *reserved, ham_dump_cb_t cb)
{
#ifdef HAM_ENABLE_INTERNAL
    ham_txn_t txn;
    ham_status_t st;
    ham_backend_t *be;

    if (!db || !cb)
        return (HAM_INV_PARAMETER);

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    be=db_get_backend(db);
    if (!be)
        return (db_set_error(db, HAM_NOT_INITIALIZED));
    if ((st=ham_txn_begin(&txn, db)))
        return (st);

    /*
     * call the backend function
     */
    st=be->_fun_enumerate(be, my_dump_cb, cb);

    if (st) {
        (void)ham_txn_abort(&txn);
        return (st);
    }

    return (ham_txn_commit(&txn, 0));
#else /* !HAM_ENABLE_INTERNAL */
    return (HAM_NOT_IMPLEMENTED);
#endif
}

ham_status_t
ham_check_integrity(ham_db_t *db, void *reserved)
{
#ifdef HAM_ENABLE_INTERNAL
    ham_txn_t txn;
    ham_status_t st;
    ham_backend_t *be;

    if (!db)
        return (HAM_INV_PARAMETER);

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
    if ((st=ham_txn_begin(&txn, db)))
        return (st);

    /*
     * call the backend function
     */
    st=be->_fun_check_integrity(be);

    if (st) {
        (void)ham_txn_abort(&txn);
        return (st);
    }

    return (ham_txn_commit(&txn, 0));
#else /* !HAM_ENABLE_INTERNAL */
    return (HAM_NOT_IMPLEMENTED);
#endif
}

ham_status_t
ham_flush(ham_db_t *db, ham_u32_t flags)
{
    ham_status_t st;

    (void)flags;

    if (!db)
        return (HAM_INV_PARAMETER);

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

ham_status_t
ham_close(ham_db_t *db, ham_u32_t flags)
{
    ham_status_t st=0;
    ham_backend_t *be;
    ham_bool_t noenv=HAM_FALSE;
    ham_db_t *newowner=0;
    ham_page_filter_t *head;

    if (!db)
        return (HAM_INV_PARAMETER);
    if (db_get_cursors(db) && !(flags&HAM_AUTO_CLEANUP))
        return (db_set_error(db, HAM_DB_NOT_EMPTY));

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    /*
     * auto-cleanup cursors?
     */
    if (flags&HAM_AUTO_CLEANUP) {
        ham_bt_cursor_t *c=(ham_bt_cursor_t *)db_get_cursors(db);
        while (c) {
            ham_bt_cursor_t *next=(ham_bt_cursor_t *)cursor_get_next(c);
            st=ham_cursor_close((ham_cursor_t *)c);
            if (st)
                return (st);
            c=next;
        }
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

    /*
     * if this database has no environment, or if it's the last
     * database in the environment: delete all environment-members
     */
    if (db_get_env(db)==0 || 
        env_get_list(db_get_env(db))==0 ||
        db_get_next(env_get_list(db_get_env(db)))==0)
        noenv=HAM_TRUE;

    be=db_get_backend(db);

    /*
     * in-memory-database: free all allocated blobs
     */
    if (be && db_get_rt_flags(db)&HAM_IN_MEMORY_DB) {
        ham_txn_t txn;
        free_cb_context_t context;
        context.db=db;
        if (!ham_txn_begin(&txn, db)) {
            (void)be->_fun_enumerate(be, my_free_cb, &context);
            (void)ham_txn_commit(&txn, 0);
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
        if (st) {
            ham_log(("freel_shutdown() failed with status %d (%s)",
                    st, ham_strerror(st)));
            return (st);
        }
    }

    /*
     * flush all pages
     */
    if (noenv) {
        st=db_flush_all(db, 0);
        if (st) {
            ham_log(("db_flush_all() failed with status %d (%s)",
                    st, ham_strerror(st)));
            return (st);
        }
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
        if (st) {
            ham_log(("backend close() failed with status %d (%s)",
                    st, ham_strerror(st)));
            return (st);
        }
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
            if (st) {
                ham_log(("page_flush() failed with status %d (%s)",
                        st, ham_strerror(st)));
                return (db_set_error(db, st));
            }
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
     * close all page-level filters
     */
    head=db_get_page_filter(db);
    while (head) {
        ham_page_filter_t *next=head->_next;
        if (head->close_cb)
            head->close_cb(db, head);
        head=next;
    }
    db_set_page_filter(db, 0);

    /* 
     * close the allocator, but not if we're in an environment
     */
    if (!db_get_env(db) && db_get_allocator(db)) {
        db_get_allocator(db)->close(db_get_allocator(db));
        db_set_allocator(db, 0);
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

    return (0);
}

ham_status_t
ham_cursor_create(ham_db_t *db, void *reserved, ham_u32_t flags,
        ham_cursor_t **cursor)
{
    if (!db || !cursor)
        return (HAM_INV_PARAMETER);

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    return (bt_cursor_create(db, 0, flags, (ham_bt_cursor_t **)cursor));
}

ham_status_t
ham_cursor_clone(ham_cursor_t *src, ham_cursor_t **dest)
{
    if (!src || !dest)
        return (HAM_INV_PARAMETER);

    if (db_get_env(cursor_get_db(src)))
        __prepare_db(cursor_get_db(src));

    db_set_error(cursor_get_db(src), 0);

    return (bt_cursor_clone((ham_bt_cursor_t *)src, (ham_bt_cursor_t **)dest));
}

ham_status_t
ham_cursor_overwrite(ham_cursor_t *cursor, ham_record_t *record,
            ham_u32_t flags)
{
    ham_db_t *db;

    if (!cursor)
        return (HAM_INV_PARAMETER);

    db=cursor_get_db(cursor);

    if (!record)
        return (db_set_error(db, HAM_INV_PARAMETER));
    if (!__prepare_record(record))
        return (db_set_error(db, HAM_INV_PARAMETER));

    if (db_get_rt_flags(cursor_get_db(cursor))&HAM_READ_ONLY)
        return (db_set_error(db, HAM_DB_READ_ONLY));

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    return (bt_cursor_overwrite((ham_bt_cursor_t *)cursor, record, flags));
}

ham_status_t
ham_cursor_move(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
    ham_db_t *db;
    if (!cursor)
        return (HAM_INV_PARAMETER);

    db=cursor_get_db(cursor);

    if ((flags&HAM_ONLY_DUPLICATES) && (flags&HAM_SKIP_DUPLICATES))
        return (HAM_INV_PARAMETER);
    if (key && !__prepare_key(key))
        return (db_set_error(db, HAM_INV_PARAMETER));
    if (record && !__prepare_record(record))
        return (db_set_error(db, HAM_INV_PARAMETER));

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    return (bt_cursor_move((ham_bt_cursor_t *)cursor, key, record, flags));
}

ham_status_t
ham_cursor_find(ham_cursor_t *cursor, ham_key_t *key, ham_u32_t flags)
{
    ham_offset_t recno=0;
    ham_status_t st;
    ham_db_t *db;

    if (!cursor)
        return (HAM_INV_PARAMETER);

    db=cursor_get_db(cursor);

    if (!key)
        return (db_set_error(db, HAM_INV_PARAMETER));

    if (!__prepare_key(key))
        return (db_set_error(db, HAM_INV_PARAMETER));

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    /*
     * record number: make sure that we have a valid key structure,
     * and translate the record number to database endian
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        if (key->size!=sizeof(ham_u64_t) || !key->data)
            return (db_set_error(db, HAM_INV_PARAMETER));
        recno=*(ham_offset_t *)key->data;
        recno=ham_h2db64(recno);
        *(ham_offset_t *)key->data=recno;
    }

    st=bt_cursor_find((ham_bt_cursor_t *)cursor, key, flags);
    if (st)
        return (st);
    /*
     * record number: re-translate the number to host endian
     */
    if (db_get_rt_flags(db)&HAM_RECORD_NUMBER) {
        *(ham_offset_t *)key->data=ham_db2h64(recno);
    }

    return (0);
}

ham_status_t
ham_cursor_insert(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
    ham_db_t *db;
    ham_status_t st;
    ham_backend_t *be;
    ham_u64_t recno;

    if (!cursor)
        return (HAM_INV_PARAMETER);

    db=cursor_get_db(cursor);

    be=db_get_backend(db);
    if (!be)
        return (db_set_error(db, HAM_NOT_INITIALIZED));

    if (!key || !record)
        return (db_set_error(db, HAM_INV_PARAMETER));
    if (!__prepare_key(key) || !__prepare_record(record))
        return (db_set_error(db, HAM_INV_PARAMETER));

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    if (db_get_rt_flags(db)&HAM_READ_ONLY)
        return (db_set_error(db, HAM_DB_READ_ONLY));
    if ((db_get_rt_flags(db)&HAM_DISABLE_VAR_KEYLEN) &&
        (key->size>db_get_keysize(db)))
        return (db_set_error(db, HAM_INV_KEYSIZE));
    if ((db_get_keysize(db)<sizeof(ham_offset_t)) &&
        (key->size>db_get_keysize(db)))
        return (db_set_error(db, HAM_INV_KEYSIZE));
    if ((flags&HAM_DUPLICATE) && (flags&HAM_OVERWRITE))
        return (db_set_error(db, HAM_INV_PARAMETER));
    if ((flags&HAM_DUPLICATE) && !(db_get_rt_flags(db)&HAM_ENABLE_DUPLICATES))
        return (db_set_error(db, HAM_INV_PARAMETER));

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
            if (key->size!=sizeof(ham_u64_t) || !key->data)
                return (HAM_INV_PARAMETER);
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
                if (!key->data || key->size!=sizeof(ham_u64_t))
                    return (HAM_INV_PARAMETER);
            }
            else {
                if (key->data || key->size)
                    return (HAM_INV_PARAMETER);
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

    st=bt_cursor_insert((ham_bt_cursor_t *)cursor, key, record, flags);

    if (st) {
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

    return (0);
}

ham_status_t
ham_cursor_erase(ham_cursor_t *cursor, ham_u32_t flags)
{
    ham_status_t st;
    ham_txn_t txn;
    ham_db_t *db;

    if (!cursor)
        return (HAM_INV_PARAMETER);

    if (db_get_env(cursor_get_db(cursor)))
        __prepare_db(cursor_get_db(cursor));

    db=cursor_get_db(cursor);
    db_set_error(db, 0);

    if (db_get_rt_flags(db)&HAM_READ_ONLY)
        return (db_set_error(db, HAM_DB_READ_ONLY));

    if ((st=ham_txn_begin(&txn, db)))
        return (st);

    st=bt_cursor_erase((ham_bt_cursor_t *)cursor, flags);

    if (st) {
        (void)ham_txn_abort(&txn);
        return (st);
    }

    return (ham_txn_commit(&txn, 0));
}

ham_status_t
ham_cursor_get_duplicate_count(ham_cursor_t *cursor, 
        ham_size_t *count, ham_u32_t flags)
{
    if (!cursor || !count)
        return (HAM_INV_PARAMETER);

    *count=0;

    if (db_get_env(cursor_get_db(cursor)))
        __prepare_db(cursor_get_db(cursor));

    db_set_error(cursor_get_db(cursor), 0);

    return (bt_cursor_get_duplicate_count((ham_bt_cursor_t *)cursor, 
                count, flags));
}

ham_status_t
ham_cursor_close(ham_cursor_t *cursor)
{
    if (!cursor)
        return (HAM_INV_PARAMETER);

    if (db_get_env(cursor_get_db(cursor)))
        __prepare_db(cursor_get_db(cursor));

    db_set_error(cursor_get_db(cursor), 0);

    return (bt_cursor_close((ham_bt_cursor_t *)cursor));
}

ham_status_t
ham_add_page_filter(ham_db_t *db, ham_page_filter_t *filter)
{
    ham_page_filter_t *head;

    if (!db)
        return (HAM_INV_PARAMETER);

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    if (!filter)
        return (db_set_error(db, HAM_INV_PARAMETER));

    head=db_get_page_filter(db);

    /*
     * !!
     * add the filter at the end of all filters, then we can process them
     * later in the same order as the insertion
     */
    if (!head) {
        db_set_page_filter(db, filter);
    }
    else {
        while (head->_next)
            head=head->_next;

        filter->_prev=head;
        head->_next=filter;
    }

    return (0);
}

ham_status_t
ham_remove_page_filter(ham_db_t *db, ham_page_filter_t *filter)
{
    ham_page_filter_t *head, *prev;

    if (!db)
        return (HAM_INV_PARAMETER);

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    if (!filter)
        return (db_set_error(db, HAM_INV_PARAMETER));

    head=db_get_page_filter(db);

    if (head==filter) {
        db_set_page_filter(db, head->_next);
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
