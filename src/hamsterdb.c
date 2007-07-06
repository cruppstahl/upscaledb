/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
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
             * delete the cached extended key
             */
            if (db_get_extkey_cache(c->db))
                (void)extkey_cache_remove(db_get_extkey_cache(c->db), blobid);

            /*
             * then delete the blob
             */
            (void)blob_free(c->db, blobid, 0);
        }

        if (key_get_flags(key)&KEY_BLOB_SIZE_TINY ||
            key_get_flags(key)&KEY_BLOB_SIZE_SMALL ||
            key_get_flags(key)&KEY_BLOB_SIZE_EMPTY)
            break;

        /*
         * if we're in the leaf page, delete the blob
         */
        if (c->is_leaf)
             (void)blob_free(c->db, key_get_ptr(key), 0);
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
        case HAM_SHORT_READ:
            return ("Short read");
        case HAM_SHORT_WRITE:
            return ("Short write");
        case HAM_INV_KEYSIZE:
            return ("Invalid key size");
        case HAM_INV_PAGESIZE:
            return ("Invalid page size");
        case HAM_DB_ALREADY_OPEN:
            return ("Db already open");
        case HAM_OUT_OF_MEMORY:
            return ("Out of memory");
        case HAM_INV_INDEX:
            return ("Invalid index structure");
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
        case HAM_ENV_FULL:
            return ("Environment is full");
        default:
            return ("Unknown error");
    }
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
    if (env_get_freelist_txn(env))
        txn_set_db(env_get_freelist_txn(env), db);
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
                else
                    keysize=(ham_u16_t)param->value;
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
     * in-memory-db? don't allow cache limits!
     */
    if ((*flags)&HAM_IN_MEMORY_DB) {
        if (((*flags)&HAM_CACHE_STRICT) || cachesize!=0)
            return (HAM_INV_PARAMETER);
    }

    /*
     * in-memory-db? use the default pagesize of the system
     */
    if ((*flags)&HAM_IN_MEMORY_DB) {
        if (!pagesize) {
            pagesize=os_get_pagesize();
            if (!pagesize) {
                pagesize=1024*4;
                no_mmap=HAM_TRUE;
            }
        }
    }

    /*
     * can we use mmap?
     */
#if HAVE_MMAP
    if (!((*flags)&HAM_DISABLE_MMAP)) {
        if (pagesize) {
            if (pagesize%os_get_pagesize()!=0)
                no_mmap=HAM_TRUE;
        }
        else {
            pagesize=os_get_pagesize();
            if (!pagesize) {
                pagesize=1024*4;
                no_mmap=HAM_TRUE;
            }
        }
    }
#else
    no_mmap=HAM_TRUE;
#endif

    /*
     * if we still don't have a pagesize, try to get a good default value
     */
    if (!pagesize) {
        pagesize=os_get_pagesize();
        if (!pagesize) {
            pagesize=1024*4;
            no_mmap=HAM_TRUE;
        }
    }

    /*
     * set the database flags if we can't use mmapped I/O
     */
    if (no_mmap) {
        (*flags)|=DB_USE_MMAP;
        (*flags)|=HAM_DISABLE_MMAP;
    }

    /*
     * make sure that the pagesize is big enough for at least 5 keys
     */
    if (keysize)
        if (pagesize/keysize<5)
            return (HAM_INV_KEYSIZE);

    /*
     * initialize the database with a good default value;
     * 32byte is the size of a first level cache line for most modern
     * processors; adjust the keysize, so the keys are aligned to
     * 32byte
     */
    if (keysize==0)
        keysize=32-(sizeof(int_key_t)-1);

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
        (void)ham_env_close(env);
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

    /*
     * only a few flags are allowed
     */
    if (flags&~(HAM_USE_BTREE|HAM_DISABLE_VAR_KEYLEN))
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
    ham_size_t cachesize;
    ham_device_t *device;

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
        (void)ham_env_close(env);
        return (st);
    }

    /*
     * store the parameters
     */
    env_set_pagesize(env, 0);
    env_set_keysize(env, 0);
    env_set_cachesize(env, cachesize);

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
                (void)ham_close(db);
                (void)ham_delete(db);
            }
            return (HAM_DATABASE_ALREADY_EXISTS);
        }
        if (name==oldname)
            slot=i;
    }

    if (slot==db_get_indexdata_size(db)) {
        if (owner) {
            (void)ham_close(db);
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
        (void)ham_close(db);
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
        (void)ham_close(db);
        (void)ham_delete(db);
        return (st);
    }
    context.db=db;
    st=db_get_backend(db)->_fun_enumerate(db_get_backend(db), 
            my_free_cb, &context);
    if (st) {
        (void)ham_txn_abort(&txn);
        (void)ham_close(db);
        (void)ham_delete(db);
        return (st);
    }

    st=ham_txn_commit(&txn);
    if (st) {
        (void)ham_close(db);
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
    (void)ham_close(db);
    (void)ham_delete(db);

    return (0);
}

ham_status_t
ham_env_close(ham_env_t *env)
{
    if (!env)
        return (HAM_INV_PARAMETER);
    if (env_get_list(env))
        return (HAM_ENV_NOT_EMPTY);

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
        env_set_header_page(env, page);
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
    if (db_get_record_allocdata(db))
        ham_mem_free(db, db_get_record_allocdata(db));
    if (db_get_key_allocdata(db))
        ham_mem_free(db, db_get_key_allocdata(db));

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
    ham_u16_t dbname=EMPTY_DATABASE_NAME;
    ham_size_t i, cachesize=0, pagesize=0;
    ham_page_t *page;
    ham_device_t *device;

    if (!db)
        return (HAM_INV_PARAMETER);

    db_set_error(db, 0);

    /* cannot open an in-memory-db */
    if (flags&HAM_IN_MEMORY_DB)
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

    if (dbname==EMPTY_DATABASE_NAME && !filename)
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
            (void)ham_close(db);
            return (db_set_error(db, st));
        }

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
        st=device->read(device, 0, hdrbuf, sizeof(hdrbuf));
        if (st) {
            ham_log(("os_pread of %s failed with status %d (%s)", filename,
                    st, ham_strerror(st)));
            (void)ham_close(db);
            return (db_set_error(db, st));
        }
        pagesize=ham_db2h32(((db_header_t *)&hdrbuf[12])->_pagesize);
        device_set_pagesize(device, pagesize);

        /*
         * can we use mmap?
         */
#if HAVE_MMAP
        if (!(flags&HAM_DISABLE_MMAP)) {
            if (pagesize%os_get_pagesize()==0)
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
                (void)ham_close(db);
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
            (void)ham_close(db);
            return (db_get_error(db));
        }
        st=page_fetch(page, pagesize);
        if (st) {
            if (page_get_pers(page))
                (void)page_free(page);
            (void)page_delete(page);
            (void)ham_close(db);
            return (st);
        }
        page_set_type(page, PAGE_TYPE_HEADER);

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
        (void)ham_close(db);
        db_set_error(db, HAM_INV_FILE_HEADER);
        return (HAM_INV_FILE_HEADER);
    }

    /* 
     * check the database version
     */
    if (db_get_version(db, 0)!=HAM_VERSION_MAJ ||
        db_get_version(db, 1)!=HAM_VERSION_MIN) {
        ham_log(("invalid file version"));
        (void)ham_close(db);
        db_set_error(db, HAM_INV_FILE_VERSION);
        return (HAM_INV_FILE_VERSION);
    }

    /*
     * search for a database with this name
     */
    for (i=0; i<db_get_indexdata_size(db); i++) {
        ham_u8_t *ptr=db_get_indexdata_at(db, i);
        if (dbname==FIRST_DATABASE_NAME ||
            dbname==ham_h2db16(*(ham_u16_t *)ptr)) {
            db_set_indexdata_offset(db, i);
            break;
        }
    }
    if (i==db_get_indexdata_size(db)) {
        (void)ham_close(db);
        return (db_set_error(db, HAM_DATABASE_NOT_FOUND));
    }

    /* 
     * create the backend
     */
    backend=db_create_backend(db, flags);
    if (!backend) {
        ham_log(("unable to create backend with flags 0x%x", flags));
        (void)ham_close(db);
        db_set_error(db, HAM_INV_INDEX);
        return (HAM_INV_INDEX);
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
        (void)ham_close(db);
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
            (void)ham_close(db);
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
    ham_set_compare_func(db, db_default_compare);
    ham_set_prefix_compare_func(db, db_default_prefix_compare);

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
        flags&=~HAM_DISABLE_MMAP; /* don't store this flag */
        device_set_pagesize(device, pagesize);

        /* 
         * create the file 
         */
        st=device->create(device, filename, flags, mode);
        if (st) {
            (void)ham_close(db);
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
            (void)ham_close(db);
            return (db_get_error(db));
        }
        st=page_alloc(page, pagesize);
        if (st) {
            page_delete(page);
            (void)ham_close(db);
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
         * initialize the freelist structure in the header page;
         * the freelist starts _after_ the header-page, therefore 
         * the start-address of the freelist is the pagesize
         */
        st=freel_prepare(db, db_get_freelist(db), db_get_pagesize(db),
                db_get_usable_pagesize(db)-
                (OFFSET_OF(db_header_t, _freelist_start)+1));
        if (st) {
            ham_log(("unable to setup the freelist"));
            (void)ham_close(db);
            return (db_set_error(db, st));
        }

        /* 
         * create the freelist - not needed for in-memory-databases
         */
        if (!(flags&HAM_IN_MEMORY_DB)) {
            st=freel_create(db);
            if (st) {
                ham_log(("unable to create freelist"));
                (void)ham_close(db);
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
            (void)ham_close(db);
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
        (void)ham_close(db);
        return (db_set_error(db, HAM_INTERNAL_ERROR)); /* TODO */
    }

    /* 
     * initialize the cache
     */
    if (!db_get_cache(db)) {
        if (cachesize==0)
            cachesize=HAM_DEFAULT_CACHESIZE;
        cache=cache_new(db, cachesize/db_get_pagesize(db));
        if (!cache) {
            (void)ham_close(db);
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
        (void)ham_close(db);
        return (db_set_error(db, HAM_INV_INDEX));
    }

    /* 
     * initialize the backend
     */
    st=backend->_fun_create(backend, keysize, pflags);
    if (st) {
        ham_log(("unable to create the backend"));
        (void)ham_close(db);
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
    ham_set_compare_func(db, db_default_compare);
    ham_set_prefix_compare_func(db, db_default_prefix_compare);
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
    db_set_prefix_compare_func(db, foo);

    return (HAM_SUCCESS);
}

ham_status_t
ham_set_compare_func(ham_db_t *db, ham_compare_func_t foo)
{
    if (!db)
        return (HAM_INV_PARAMETER);

    db_set_error(db, 0);
    db_set_compare_func(db, foo);

    return (HAM_SUCCESS);
}

ham_status_t
ham_find(ham_db_t *db, void *reserved, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
    ham_txn_t txn;
    ham_status_t st;
    ham_backend_t *be;

    if (!db || !key || !record)
        return (HAM_INV_PARAMETER);

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    be=db_get_backend(db);
    if (!be)
        return (db_set_error(db, HAM_INV_INDEX));

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

    return (ham_txn_commit(&txn));
}

ham_status_t
ham_insert(ham_db_t *db, void *reserved, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags)
{
    ham_txn_t txn;
    ham_status_t st;
    ham_backend_t *be;

    if (!db || !key || !record)
        return (HAM_INV_PARAMETER);

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    be=db_get_backend(db);
    if (!be)
        return (db_set_error(db, HAM_INV_INDEX));
    if (db_get_rt_flags(db)&HAM_READ_ONLY)
        return (db_set_error(db, HAM_DB_READ_ONLY));
    if ((db_get_rt_flags(db)&HAM_DISABLE_VAR_KEYLEN) &&
        (key->size>db_get_keysize(db)))
        return (db_set_error(db, HAM_INV_KEYSIZE));
    if ((db_get_keysize(db)<sizeof(ham_offset_t)) &&
        (key->size>db_get_keysize(db)))
        return (db_set_error(db, HAM_INV_KEYSIZE));
    if ((st=ham_txn_begin(&txn, db)))
        return (st);

    /*
     * store the index entry; the backend will store the blob
     */
    st=be->_fun_insert(be, key, record, flags);

    if (st) {
#if 0
        (void)ham_txn_abort(&txn);
#endif
        (void)ham_txn_commit(&txn);
        return (st);
    }

    return (ham_txn_commit(&txn));
}

ham_status_t
ham_erase(ham_db_t *db, void *reserved, ham_key_t *key, ham_u32_t flags)
{
    ham_txn_t txn;
    ham_status_t st;
    ham_u32_t intflags=0;
    ham_offset_t blobid=0;
    ham_backend_t *be;

    if (!db || !key)
        return (HAM_INV_PARAMETER);

    if (db_get_env(db))
        __prepare_db(db);

    db_set_error(db, 0);

    be=db_get_backend(db);
    if (!be)
        return (db_set_error(db, HAM_INV_INDEX));
    if (db_get_rt_flags(db)&HAM_READ_ONLY)
        return (db_set_error(db, HAM_DB_READ_ONLY));
    if ((st=ham_txn_begin(&txn, db)))
        return (st);

    /*
     * get rid of the index entry, then free the blob
     */
    st=be->_fun_erase(be, key, &blobid, &intflags, flags);
    if (st==HAM_SUCCESS) {
        if (!((intflags&KEY_BLOB_SIZE_TINY) ||
              (intflags&KEY_BLOB_SIZE_SMALL) ||
              (intflags&KEY_BLOB_SIZE_EMPTY)))
            st=blob_free(db, blobid, flags);
    }

    if (st) {
#if 0
        (void)ham_txn_abort(&txn);
#endif
        (void)ham_txn_commit(&txn);
        return (st);
    }

    return (ham_txn_commit(&txn));
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
        return (db_set_error(db, HAM_INV_INDEX));
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

    return (ham_txn_commit(&txn));
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
        return (db_set_error(db, HAM_INV_INDEX));
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

    return (ham_txn_commit(&txn));
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
ham_close(ham_db_t *db)
{
    ham_status_t st=0;
    ham_backend_t *be;
    ham_bool_t noenv=HAM_FALSE;
    ham_db_t *newowner=0;

    if (!db)
        return (HAM_INV_PARAMETER);

    db_set_error(db, 0);

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
            (void)ham_txn_commit(&txn);
        }
    }

    /*
     * free cached memory
     */
    if (db_get_record_allocdata(db)) {
        ham_mem_free(db, db_get_record_allocdata(db));
        db_set_record_allocdata(db, 0);
        db_set_record_allocsize(db, 0);
    }
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
    if (noenv &&
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
        db_set_header_page(db, db_get_header_page(db));
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
ham_cursor_replace(ham_cursor_t *cursor, ham_record_t *record,
            ham_u32_t flags)
{
    if (!cursor || !record)
        return (HAM_INV_PARAMETER);

    if (db_get_rt_flags(cursor_get_db(cursor))&HAM_READ_ONLY)
        return (db_set_error(cursor_get_db(cursor), HAM_DB_READ_ONLY));

    if (db_get_env(cursor_get_db(cursor)))
        __prepare_db(cursor_get_db(cursor));

    db_set_error(cursor_get_db(cursor), 0);

    return (bt_cursor_replace((ham_bt_cursor_t *)cursor, record, flags));
}

ham_status_t
ham_cursor_move(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
    if (!cursor)
        return (HAM_INV_PARAMETER);

    if (db_get_env(cursor_get_db(cursor)))
        __prepare_db(cursor_get_db(cursor));

    db_set_error(cursor_get_db(cursor), 0);

    return (bt_cursor_move((ham_bt_cursor_t *)cursor, key, record, flags));
}

ham_status_t
ham_cursor_find(ham_cursor_t *cursor, ham_key_t *key, ham_u32_t flags)
{
    if (!cursor || !key)
        return (HAM_INV_PARAMETER);

    if (db_get_env(cursor_get_db(cursor)))
        __prepare_db(cursor_get_db(cursor));

    db_set_error(cursor_get_db(cursor), 0);

    return (bt_cursor_find((ham_bt_cursor_t *)cursor, key, flags));
}

ham_status_t
ham_cursor_insert(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags)
{
    ham_db_t *db;

    if (!cursor || !key || !record)
        return (HAM_INV_PARAMETER);

    db=cursor_get_db(cursor);

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

    return (bt_cursor_insert((ham_bt_cursor_t *)cursor, key, record, flags));
}

ham_status_t
ham_cursor_erase(ham_cursor_t *cursor, ham_u32_t flags)
{
    ham_status_t st;
    ham_offset_t rid;
    ham_u32_t intflags;
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

    st=bt_cursor_erase((ham_bt_cursor_t *)cursor, &rid, &intflags, flags);
    if (st==HAM_SUCCESS) {
        if (!((intflags&KEY_BLOB_SIZE_TINY) ||
              (intflags&KEY_BLOB_SIZE_SMALL) ||
              (intflags&KEY_BLOB_SIZE_EMPTY)))
            st=blob_free(cursor_get_db(cursor), rid, flags);
    }

    if (st) {
        (void)ham_txn_abort(&txn);
        return (st);
    }

    return (ham_txn_commit(&txn));
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

