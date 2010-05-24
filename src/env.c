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

#include <string.h>

#include "db.h"
#include "env.h"
#include "statistics.h"
#include "device.h"
#include "version.h"
#include "serial.h"
#include "txn.h"
#include "mem.h"
#include "freelist.h"
#include "extkeys.h"
#include "backend.h"
#include "cache.h"
#include "log.h"
#include "keys.h"
#include "os.h"
#include "blob.h"

typedef struct free_cb_context_t
{
    ham_db_t *db;
    ham_bool_t is_leaf;

} free_cb_context_t;

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

ham_u16_t
env_get_max_databases(ham_env_t *env)
{
    env_header_t *hdr=(env_header_t*)
                    (page_get_payload(env_get_header_page(env)));
    return (ham_db2h16(hdr->_max_databases));
}

ham_u8_t
env_get_version(ham_env_t *env, ham_size_t idx)
{
    env_header_t *hdr=(env_header_t*)
                    (page_get_payload(env_get_header_page(env)));
    return (envheader_get_version(hdr, idx));
}

ham_u32_t
env_get_serialno(ham_env_t *env)
{
    env_header_t *hdr=(env_header_t*)
                    (page_get_payload(env_get_header_page(env)));
    return (ham_db2h32(hdr->_serialno));
}

void
env_set_serialno(ham_env_t *env, ham_u32_t n)
{
    env_header_t *hdr=(env_header_t*)
                    (page_get_payload(env_get_header_page(env)));
    hdr->_serialno=ham_h2db32(n);
}

env_header_t *
env_get_header(ham_env_t *env)
{
    return ((env_header_t*)(page_get_payload(env_get_header_page(env))));
}

ham_status_t
env_fetch_page(ham_page_t **page_ref, ham_env_t *env, 
        ham_offset_t address, ham_u32_t flags)
{
    return (db_fetch_page_impl(page_ref, env, 0, address, flags));
}

ham_status_t
env_alloc_page(ham_page_t **page_ref, ham_env_t *env,
                ham_u32_t type, ham_u32_t flags)
{
    return (db_alloc_page_impl(page_ref, env, 0, type, flags));
}

static ham_status_t 
_local_fun_create(ham_env_t *env, const char *filename,
            ham_u32_t flags, ham_u32_t mode, const ham_parameter_t *param)
{
    ham_status_t st=0;
    ham_device_t *device=0;
    ham_size_t pagesize=env_get_pagesize(env);

    /* reset all performance data */
    stats_init_globdata(env, env_get_global_perf_data(env));

    ham_assert(!env_get_header_page(env), (0));

    /* 
     * initialize the device if it does not yet exist
     */
    if (!env_get_device(env)) {
        device=ham_device_new(env_get_allocator(env), env, 
                        ((flags&HAM_IN_MEMORY_DB) 
                            ? HAM_DEVTYPE_MEMORY 
                            : HAM_DEVTYPE_FILE));
        if (!device)
            return (HAM_OUT_OF_MEMORY);

        env_set_device(env, device);

        device->set_flags(device, flags);
        st = device->set_pagesize(device, env_get_pagesize(env));
        if (st)
            return st;

        /* now make sure the pagesize is a multiple of 
         * DB_PAGESIZE_MIN_REQD_ALIGNMENT bytes */
        ham_assert(0 == (env_get_pagesize(env) 
                    % DB_PAGESIZE_MIN_REQD_ALIGNMENT), (0));
    }
    else
    {
        device=env_get_device(env);
        ham_assert(device->get_pagesize(device), (0));
        ham_assert(env_get_pagesize(env) == device->get_pagesize(device), (0));
    }
    ham_assert(device == env_get_device(env), (0));
    ham_assert(env_get_pagesize(env) == device->get_pagesize(device), (""));

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
        st=page_alloc(page);
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
        env_set_max_databases(env, env_get_max_databases_cached(env));
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
        ham_size_t cachesize=env_get_cachesize(env);

        /* cachesize is specified in PAGES */
        ham_assert(cachesize, (0));
        cache=cache_new(env, cachesize);
        if (!cache) {
            (void)ham_env_close(env, 0);
            return (HAM_OUT_OF_MEMORY);
        }
        env_set_cache(env, cache);
    }
    return (st);
}

static ham_status_t 
_local_fun_open(ham_env_t *env, const char *filename, ham_u32_t flags, 
        const ham_parameter_t *param)
{
    ham_status_t st;
    ham_device_t *device=0;
    ham_u32_t pagesize=0;
    ham_u16_t dam = 0;

    /* reset all performance data */
    stats_init_globdata(env, env_get_global_perf_data(env));

    /* 
     * initialize the device if it does not yet exist
     */
    if (!env_get_device(env)) {
        device=ham_device_new(env_get_allocator(env), env,
                ((flags&HAM_IN_MEMORY_DB) 
                    ? HAM_DEVTYPE_MEMORY 
                    : HAM_DEVTYPE_FILE));
        if (!device)
            return (HAM_OUT_OF_MEMORY);
        env_set_device(env, device);
    }
    else {
        device=env_get_device(env);
    }

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
        env_header_t *hdr;
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
        ham_assert(hdr == (env_header_t *)(hdrbuf + 
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
        st=page_fetch(page);
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
        st=ham_log_open(env_get_allocator(env), env, 
                    env_get_filename(env), 0, &log);
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
            st=ham_log_create(env_get_allocator(env), env, 
                    env_get_filename(env), 0644, 0, &log);
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
        ham_size_t cachesize=env_get_cachesize(env);

        if (!cachesize)
            cachesize=HAM_DEFAULT_CACHESIZE;
        env_set_cachesize(env, cachesize);

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

static ham_status_t
_local_fun_rename_db(ham_env_t *env, ham_u16_t oldname, 
                ham_u16_t newname, ham_u32_t flags)
{
    ham_u16_t dbi;
    ham_u16_t slot;

    /*
     * make sure that the environment was either created or opened, and 
     * a valid device exists
     */
    if (!env_get_device(env))
        return (HAM_NOT_READY);

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
    
    return (0);
}

static ham_status_t
_local_fun_erase_db(ham_env_t *env, ham_u16_t name, ham_u32_t flags)
{
    ham_db_t *db;
    ham_status_t st;
    free_cb_context_t context;
    ham_txn_t txn;
    ham_backend_t *be;

    /* for hamsterdb 1.0.4 - only support one transaction */
    if (env_get_txn(env)) {
        ham_trace(("only one concurrent transaction is supported"));
        return (HAM_LIMITS_REACHED);
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

    if (!be->_fun_enumerate)
        return HAM_NOT_IMPLEMENTED;

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
    index_set_dbname(env_get_indexdata_ptr(env, 
                        db_get_indexdata_offset(db)), 0);
    page_set_dirty_txn(env_get_header_page(env), 1);

    /*
     * TODO
     * 
     * log??? before + after image of header page???
     */

    /*
     * clean up and return
     */
    (void)ham_close(db, 0);
    (void)ham_delete(db);

    return (0);
}

static ham_status_t
_local_fun_get_database_names(ham_env_t *env, ham_u16_t *names, 
            ham_size_t *count)
{
    ham_u16_t name;
    ham_size_t i=0;
    ham_size_t max_names=0;
    ham_status_t st=0;

    max_names=*count;
    *count=0;

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

    return st;
}

static ham_status_t
_local_fun_close(ham_env_t *env, ham_u32_t flags)
{
    ham_status_t st;
    ham_status_t st2 = HAM_SUCCESS;
    ham_device_t *dev;
    ham_file_filter_t *file_head;

    /* flush/persist all performance data which we want to persist */
    stats_flush_globdata(env, env_get_global_perf_data(env));

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
        if (st2 == 0) 
            st2 = st;
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
            if (!st2) 
                st2 = st;
        }
        allocator_free(env_get_allocator(env), page);
        env_set_header_page(env, 0);
    }

    /* 
     * flush all pages, get rid of the cache 
     */
    if (env_get_cache(env)) {
        (void)db_flush_all(env_get_cache(env), 0);
        cache_delete(env_get_cache(env));
        env_set_cache(env, 0);
    }

    /* 
     * close the device
     */
    if (dev) {
        if (dev->is_open(dev)) {
            if (!(env_get_rt_flags(env)&HAM_READ_ONLY)) {
                st = dev->flush(dev);
                if (!st2) 
                    st2 = st;
            }
            st = dev->close(dev);
            if (!st2) 
                st2 = st;
        }
        st = dev->destroy(dev);
        if (!st2) 
            st2 = st;
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
        if (!st2) 
            st2 = st;
        env_set_log(env, 0);
    }

    return st2;
}

static ham_status_t 
_local_fun_get_parameters(ham_env_t *env, ham_parameter_t *param)
{
    ham_parameter_t *p=param;

    if (p) {
        for (; p->name; p++) {
            switch (p->name) {
            case HAM_PARAM_CACHESIZE:
                p->value=env_get_cachesize(env);
                break;
            case HAM_PARAM_PAGESIZE:
                p->value=env_get_pagesize(env);
                break;
            case HAM_PARAM_MAX_ENV_DATABASES:
                p->value=env_get_max_databases(env);
                break;
            case HAM_PARAM_GET_FLAGS:
                p->value=env_get_rt_flags(env);
                break;
            case HAM_PARAM_GET_FILEMODE:
                p->value=env_get_file_mode(env);
                break;
            case HAM_PARAM_GET_FILENAME:
                p->value=(ham_u64_t)(PTR_TO_U64(env_get_filename(env)));
                break;
            case HAM_PARAM_GET_STATISTICS:
                if (!p->value) {
                    ham_trace(("the value for parameter "
                               "'HAM_PARAM_GET_STATISTICS' must not be NULL "
                               "and reference a ham_statistics_t data "
                               "structure before invoking "
                               "ham_get_parameters"));
                    return (HAM_INV_PARAMETER);
                }
                else {
                    ham_status_t st = stats_fill_ham_statistics_t(env, 0, 
                            (ham_statistics_t *)U64_TO_PTR(p->value));
                    if (st)
                        return st;
                }
                break;
            default:
                ham_trace(("unknown parameter %d", (int)p->name));
                return (HAM_INV_PARAMETER);
            }
        }
    }

    return (0);
}

static ham_status_t
_local_fun_flush(ham_env_t *env, ham_u32_t flags)
{
    ham_status_t st;
    ham_db_t *db;
    ham_device_t *dev;

    (void)flags;

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
        if (!be->_fun_flush)
            return (HAM_NOT_IMPLEMENTED);
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

static ham_status_t 
_local_fun_create_db(ham_env_t *env, ham_db_t *db, 
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

    db_set_rt_flags(db, 0);

    /* 
     * parse parameters
     */
    st=__check_create_parameters(env, db, 0, &flags, param, 
            0, &keysize, &cachesize, &dbname, 0, &dam, HAM_TRUE);
    if (st)
        return (db_set_error(db, st));

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
    if (be == NULL) {
        st=db_create_backend(&be, db, flags);
        ham_assert(st ? be == NULL : 1, (0));
        if (!be) {
            (void)ham_close(db, 0);
            return (db_set_error(db, st));
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
        return (db_set_error(db, st));
    }

    ham_assert(be_is_active(be) != 0, (0));

    /*
     * initialize the remaining function pointers in ham_db_t
     */
    st=db_initialize_local(db);
    if (st) {
        (void)ham_close(db, 0);
        return (db_set_error(db, st));
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

    return (0);
}

static ham_status_t 
_local_fun_open_db(ham_env_t *env, ham_db_t *db, 
        ham_u16_t name, ham_u32_t flags, const ham_parameter_t *param)
{
    ham_db_t *head;
    ham_status_t st;
    ham_u16_t dam = 0;
    ham_size_t cachesize = 0;
    ham_backend_t *be = 0;
    ham_u16_t dbi;

    /*
     * make sure that this database is not yet open/created
     */
    if (db_is_active(db)) {
        ham_trace(("parameter 'db' is already initialized"));
        return (db_set_error(db, HAM_DATABASE_ALREADY_OPEN));
    }

    db_set_rt_flags(db, 0);

    /* parse parameters */
    st=__check_create_parameters(env, db, 0, &flags, param, 
            0, 0, &cachesize, &name, 0, &dam, HAM_FALSE);
    if (st)
        return (db_set_error(db, st));

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
            return (db_set_error(db, st));
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
     * initialize the remaining function pointers in ham_db_t
     */
    st=db_initialize_local(db);
    if (st) {
        (void)ham_close(db, 0);
        return (db_set_error(db, st));
    }

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

    return (0);
}

ham_status_t
env_initialize_local(ham_env_t *env)
{
    env->_fun_create             =_local_fun_create;
    env->_fun_open               =_local_fun_open;
    env->_fun_rename_db          =_local_fun_rename_db;
    env->_fun_erase_db           =_local_fun_erase_db;
    env->_fun_get_database_names =_local_fun_get_database_names;
    env->_fun_get_parameters     =_local_fun_get_parameters;
    env->_fun_create_db          =_local_fun_create_db;
    env->_fun_open_db            =_local_fun_open_db;
    env->_fun_flush              =_local_fun_flush;
    env->_fun_close              =_local_fun_close;

    return (0);
}

