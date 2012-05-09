/**
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

#include <string.h>

#include "db.h"
#include "env.h"
#include "btree_stats.h"
#include "device.h"
#include "version.h"
#include "serial.h"
#include "txn.h"
#include "device.h"
#include "btree.h"
#include "mem.h"
#include "freelist.h"
#include "extkeys.h"
#include "backend.h"
#include "cache.h"
#include "log.h"
#include "journal.h"
#include "btree_key.h"
#include "os.h"
#include "blob.h"
#include "txn_cursor.h"
#include "cursor.h"
#include "btree_cursor.h"

typedef struct free_cb_context_t
{
    Database *db;
    ham_bool_t is_leaf;

} free_cb_context_t;

Environment::Environment()
  : m_file_mode(0), m_txn_id(0), m_context(0), m_device(0), m_cache(0), 
    m_alloc(0), m_hdrpage(0), m_oldest_txn(0), m_newest_txn(0), m_log(0), 
    m_journal(0), m_flags(0), m_databases(0), m_pagesize(0), m_cachesize(0),
    m_max_databases_cached(0), m_is_active(false), m_is_legacy(false),
    m_file_filters(0)
{
#if HAM_ENABLE_REMOTE
    m_curl=0;
#endif

	memset(&m_perf_data, 0, sizeof(m_perf_data));

    _fun_create=0;
    _fun_open=0;
    _fun_rename_db=0;
    _fun_erase_db=0;
    _fun_get_database_names=0;
    _fun_get_parameters=0;
    _fun_flush=0;
    _fun_create_db=0;
    _fun_open_db=0;
    _fun_txn_begin=0;
    _fun_txn_abort=0;
    _fun_txn_commit=0;
    _fun_close=0;
	destroy=0;
}

Environment::~Environment()
{
    /* delete all performance data */
    btree_stats_trash_globdata(this, get_global_perf_data());

    /* close the device if it still exists */
    if (get_device()) {
        Device *device=get_device();
        if (device->is_open()) {
            (void)device->flush();
            (void)device->close();
        }
        delete device;
        set_device(0);
    }

    /* close the allocator */
    if (get_allocator()) {
        delete get_allocator();
        set_allocator(0);
    }
}

bool 
Environment::is_private() 
{
    // must have exactly 1 database with the ENV_IS_PRIVATE flag
    if (!get_databases())
        return (false);

    Database *db=get_databases();
    if (db->get_next())
        return (false);
    return ((db->get_rt_flags()&DB_ENV_IS_PRIVATE) ? true : false);
}

struct db_indexdata_t *
Environment::get_indexdata_ptr(int i) 
{
    db_indexdata_t *dbi=(db_indexdata_t *)
            (get_header_page()->get_payload()+sizeof(env_header_t));
    return (dbi+i);
}

freelist_payload_t *
Environment::get_freelist()
{
    return ((freelist_payload_t *)(get_header_page()->get_payload()+
                        SIZEOF_FULL_HEADER(this)));
}

/* 
 * forward decl - implemented in hamsterdb.cc
 */
extern ham_status_t 
__check_create_parameters(Environment *env, Database *db, const char *filename, 
        ham_u32_t *pflags, const ham_parameter_t *param, 
        ham_size_t *ppagesize, ham_u16_t *pkeysize, 
        ham_u64_t *pcachesize, ham_u16_t *pdbname,
        ham_u16_t *pmaxdbs, ham_u16_t *pdata_access_mode, 
        std::string &logdir, bool create);

/*
 * callback function for freeing blobs of an in-memory-database, implemented 
 * in db.c
 */
extern ham_status_t
__free_inmemory_blobs_cb(int event, void *param1, void *param2, void *context);

ham_status_t
env_fetch_page(Page **page_ref, Environment *env, 
        ham_offset_t address, ham_u32_t flags)
{
    return (db_fetch_page_impl(page_ref, env, 0, address, flags));
}

ham_status_t
env_alloc_page(Page **page_ref, Environment *env,
                ham_u32_t type, ham_u32_t flags)
{
    return (db_alloc_page_impl(page_ref, env, 0, type, flags));
}

static ham_status_t 
_local_fun_create(Environment *env, const char *filename,
            ham_u32_t flags, ham_u32_t mode, const ham_parameter_t *param)
{
    ham_status_t st=0;
    Device *device=0;
    ham_size_t pagesize=env->get_pagesize();

    /* reset all performance data */
    btree_stats_init_globdata(env, env->get_global_perf_data());

    ham_assert(!env->get_header_page(), (0));

    /* initialize the device if it does not yet exist */
    if (!env->get_device()) {
        if (flags&HAM_IN_MEMORY_DB)
            device=new InMemoryDevice(env, flags);
        else
            device=new FileDevice(env, flags);

        device->set_pagesize(env->get_pagesize());
        env->set_device(device);

        /* now make sure the pagesize is a multiple of 
         * DB_PAGESIZE_MIN_REQD_ALIGNMENT bytes */
        ham_assert(0 == (env->get_pagesize() 
                    % DB_PAGESIZE_MIN_REQD_ALIGNMENT), (0));
    }
    else {
        device=env->get_device();
        ham_assert(device->get_pagesize(), (0));
        ham_assert(env->get_pagesize() == device->get_pagesize(), (0));
    }
    ham_assert(device == env->get_device(), (0));
    ham_assert(env->get_pagesize() == device->get_pagesize(), (""));

    /* create the file */
    st=device->create(filename, flags, mode);
    if (st) {
        (void)ham_env_close((ham_env_t *)env, HAM_DONT_LOCK);
        return (st);
    }

    /* allocate the header page */
    {
        Page *page=new Page(env);
        /* manually set the device pointer */
        page->set_device(device);
        st=page->allocate();
        if (st) {
            delete page;
            (void)ham_env_close((ham_env_t *)env, HAM_DONT_LOCK);
            return (st);
        }
        memset(page->get_pers(), 0, pagesize);
        page->set_type(Page::TYPE_HEADER);
        env->set_header_page(page);

        /* initialize the header */
        env->set_magic('H', 'A', 'M', '\0');
        env->set_version(HAM_VERSION_MAJ, HAM_VERSION_MIN, HAM_VERSION_REV, 0);
        env->set_serialno(HAM_SERIALNO);
        env->set_persistent_pagesize(pagesize);
        env->set_max_databases(env->get_max_databases_cached());
        ham_assert(env->get_max_databases() > 0, (0));

        page->set_dirty(true);
    }

    /*
     * create a logfile and a journal (if requested)
     */
    if (env->get_flags()&HAM_ENABLE_RECOVERY) {
        Log *log=new Log(env);
        st=log->create();
        if (st) { 
            delete log;
            (void)ham_env_close((ham_env_t *)env, HAM_DONT_LOCK);
            return (st);
        }
        env->set_log(log);

        Journal *journal=new Journal(env);
        st=journal->create();
        if (st) { 
            (void)ham_env_close((ham_env_t *)env, HAM_DONT_LOCK);
            return (st);
        }
        env->set_journal(journal);
    }

    /* initialize the cache */
    env->set_cache(new Cache(env, env->get_cachesize()));

    /* flush the header page - this will write through disk if logging is
     * enabled */
    if (env->get_flags()&HAM_ENABLE_RECOVERY)
    	return (env->get_header_page()->flush());

    return (0);
}

static ham_status_t
__recover(Environment *env, ham_u32_t flags)
{
    ham_status_t st;
    Log *log=new Log(env);
    Journal *journal=new Journal(env);

    ham_assert(env->get_flags()&HAM_ENABLE_RECOVERY, (""));

    /* open the log and the journal (if transactions are enabled) */
    st=log->open();
    env->set_log(log);
    if (st && st!=HAM_FILE_NOT_FOUND)
        goto bail;
    if (env->get_flags()&HAM_ENABLE_TRANSACTIONS) {
        st=journal->open();
        env->set_journal(journal);
        if (st && st!=HAM_FILE_NOT_FOUND)
            goto bail;
    }

    /* success - check if we need recovery */
    if (!log->is_empty()) {
        if (flags&HAM_AUTO_RECOVERY) {
            st=log->recover();
            if (st)
                goto bail;
        }
        else {
            st=HAM_NEED_RECOVERY;
            goto bail;
        }
    }

    if (env->get_flags()&HAM_ENABLE_TRANSACTIONS) {
        if (!journal->is_empty()) {
            if (flags&HAM_AUTO_RECOVERY) {
                st=journal->recover();
                if (st)
                    goto bail;
            }
            else {
                st=HAM_NEED_RECOVERY;
                goto bail;
            }
        }
    }

goto success;

bail:
    /* in case of errors: close log and journal, but do not delete the files */
    if (log) {
        log->close(true);
        delete log;
        env->set_log(0);
    }
    if (journal) {
        journal->close(true);
        delete journal;
        env->set_journal(0);
    }
    return (st);

success:
    /* done with recovering - if there's no log and/or no journal then
     * create them and store them in the environment */
    if (!log) {
        log=new Log(env);
        st=log->create();
        if (st)
            return (st);
    }
    env->set_log(log);

    if (env->get_flags()&HAM_ENABLE_TRANSACTIONS) {
        if (!journal) {
            journal=new Journal(env);
            st=journal->create();
            if (st)
                return (st);
        }
    	env->set_journal(journal);
    }
    else if (journal)
        delete journal;

    return (0);
}

static ham_status_t 
_local_fun_open(Environment *env, const char *filename, ham_u32_t flags, 
        const ham_parameter_t *param)
{
    ham_status_t st;
    Device *device=0;
    ham_u32_t pagesize=0;

    /* reset all performance data */
    btree_stats_init_globdata(env, env->get_global_perf_data());

    /* initialize the device if it does not yet exist */
    if (!env->get_device()) {
        if (flags&HAM_IN_MEMORY_DB)
            device=new InMemoryDevice(env, flags);
        else
            device=new FileDevice(env, flags);

        env->set_device(device);
    }
    else {
        device=env->get_device();
    }

    /* open the file */
    st=device->open(filename, flags);
    if (st) {
        (void)ham_env_close((ham_env_t *)env, 
                        HAM_DONT_CLEAR_LOG|HAM_DONT_LOCK);
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
        Page *page=0;
        env_header_t *hdr;
        ham_u8_t hdrbuf[512];
        Page fakepage(env);
        ham_bool_t hdrpage_faked = HAM_FALSE;

        /*
         * in here, we're going to set up a faked headerpage for the 
         * duration of this call; BE VERY CAREFUL: we MUST clean up 
         * at the end of this section or we'll be in BIG trouble!
         */
        hdrpage_faked = HAM_TRUE;
        fakepage.set_pers((page_data_t *)hdrbuf);
        env->set_header_page(&fakepage);

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
        st=device->read(0, hdrbuf, sizeof(hdrbuf));
        if (st) 
            goto fail_with_fake_cleansing;

        hdr=env->get_header();

        pagesize=env->get_persistent_pagesize();
        env->set_pagesize(pagesize);
        device->set_pagesize(pagesize);

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
                device->set_flags(flags|HAM_DISABLE_MMAP);
        }
        else {
            device->set_flags(flags|HAM_DISABLE_MMAP);
        }
        flags&=~HAM_DISABLE_MMAP; /* don't store this flag */
#else
        device->set_flags(flags|HAM_DISABLE_MMAP);
#endif

        /** check the file magic */
        if (!env->compare_magic('H', 'A', 'M', '\0')) {
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
        if (envheader_get_version(hdr, 0)>HAM_VERSION_MAJ ||
                (envheader_get_version(hdr, 0)==HAM_VERSION_MAJ 
                    && envheader_get_version(hdr, 1)>HAM_VERSION_MIN)) {
            ham_log(("invalid file version"));
            st = HAM_INV_FILE_VERSION;
            goto fail_with_fake_cleansing;
        }
        else if (envheader_get_version(hdr, 0) == 1 &&
            envheader_get_version(hdr, 1) == 0 &&
            envheader_get_version(hdr, 2) <= 9) {
            env->set_legacy(true);
        }

        st=0;

fail_with_fake_cleansing:

        /* undo the headerpage fake first! */
        if (hdrpage_faked)
            fakepage.set_pers(0);
            env->set_header_page(0);

        /* exit when an error was signaled */
        if (st) {
            (void)ham_env_close((ham_env_t *)env, 
                        HAM_DONT_CLEAR_LOG|HAM_DONT_LOCK);
            return (st);
        }

        /* now read the "real" header page and store it in the Environment */
        page=new Page(env);
        page->set_device(device);
        st=page->fetch(0);
        if (st) {
            delete page;
            (void)ham_env_close((ham_env_t *)env, 
                        HAM_DONT_CLEAR_LOG|HAM_DONT_LOCK);
            return (st);
        }
        env->set_header_page(page);
    }

    /* 
     * initialize the cache; the cache is needed during recovery, therefore
     * we have to create the cache BEFORE we attempt to recover
     */
    env->set_cache(new Cache(env, env->get_cachesize()));

    /*
     * open the logfile and check if we need recovery. first open the 
     * (physical) log and re-apply it. afterwards to the same with the
     * (logical) journal.
     */
    if (env->get_flags()&HAM_ENABLE_RECOVERY) {
        st=__recover(env, flags);
        if (st) {
            (void)ham_env_close((ham_env_t *)env, 
                        HAM_DONT_CLEAR_LOG|HAM_DONT_LOCK);
            return (st);
        }
    }

    return (HAM_SUCCESS);
}

static ham_status_t
_local_fun_rename_db(Environment *env, ham_u16_t oldname, 
                ham_u16_t newname, ham_u32_t flags)
{
    ham_u16_t dbi;
    ham_u16_t slot;

    /*
     * make sure that the environment was either created or opened, and 
     * a valid device exists
     */
    if (!env->get_device())
        return (HAM_NOT_READY);

    /*
     * check if a database with the new name already exists; also search 
     * for the database with the old name
     */
    slot=env->get_max_databases();
    ham_assert(env->get_max_databases() > 0, (0));
    for (dbi=0; dbi<env->get_max_databases(); dbi++) {
        ham_u16_t name=index_get_dbname(env->get_indexdata_ptr(dbi));
        if (name==newname)
            return (HAM_DATABASE_ALREADY_EXISTS);
        if (name==oldname)
            slot=dbi;
    }

    if (slot==env->get_max_databases())
        return (HAM_DATABASE_NOT_FOUND);

    /* replace the database name with the new name */
    index_set_dbname(env->get_indexdata_ptr(slot), newname);

    env->set_dirty(true);

    /* flush the header page if logging is enabled */
    if (env->get_flags()&HAM_ENABLE_RECOVERY)
        return (env->get_header_page()->flush());
    
    return (0);
}

static ham_status_t
_local_fun_erase_db(Environment *env, ham_u16_t name, ham_u32_t flags)
{
    Database *db;
    ham_status_t st;
    free_cb_context_t context;
    Backend *be;

    /*
     * check if this database is still open
     */
    db=env->get_databases();
    while (db) {
        ham_u16_t dbname=index_get_dbname(env->get_indexdata_ptr(
                            db->get_indexdata_offset()));
        if (dbname==name)
            return (HAM_DATABASE_ALREADY_OPEN);
        db=db->get_next();
    }

    /*
     * if it's an in-memory environment: no need to go on, if the 
     * database was closed, it does no longer exist
     */
    if (env->get_flags()&HAM_IN_MEMORY_DB)
        return (HAM_DATABASE_NOT_FOUND);

    /*
     * temporarily load the database
     */
    st=ham_new((ham_db_t **)&db);
    if (st)
        return (st);
    st=ham_env_open_db((ham_env_t *)env, (ham_db_t *)db, name, 
                HAM_DONT_LOCK, 0);
    if (st) {
        delete db;
        return (st);
    }

    /* logging enabled? then the changeset and the log HAS to be empty */
#ifdef HAM_DEBUG
        if (env->get_flags()&HAM_ENABLE_RECOVERY) {
            ham_assert(env->get_changeset().is_empty(), (""));
            ham_assert(env->get_log()->is_empty(), (""));
        }
#endif

    /*
     * delete all blobs and extended keys, also from the cache and
     * the extkey-cache
     *
     * also delete all pages and move them to the freelist; if they're 
     * cached, delete them from the cache
     */
    context.db=db;
    be=db->get_backend();
    if (!be || !be->is_active())
        return (HAM_INTERNAL_ERROR);

    st=be->enumerate(__free_inmemory_blobs_cb, &context);
    if (st) {
        (void)ham_close((ham_db_t *)db, HAM_DONT_LOCK);
        delete db;
        return (st);
    }

    /* set database name to 0 and set the header page to dirty */
    index_set_dbname(env->get_indexdata_ptr(
                        db->get_indexdata_offset()), 0);
    env->get_header_page()->set_dirty(true);

    /* if logging is enabled: flush the changeset and the header page */
    if (st==0 && env->get_flags()&HAM_ENABLE_RECOVERY) {
        ham_u64_t lsn;
        env->get_changeset().add_page(env->get_header_page());
        st=env_get_incremented_lsn(env, &lsn);
        if (st==0)
            st=env->get_changeset().flush(lsn);
    }

    /* clean up and return */
    (void)ham_close((ham_db_t *)db, HAM_DONT_LOCK);
    delete db;

    return (0);
}

static ham_status_t
_local_fun_get_database_names(Environment *env, ham_u16_t *names, 
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
    ham_assert(env->get_max_databases() > 0, (0));
    for (i=0; i<env->get_max_databases(); i++) {
        name = index_get_dbname(env->get_indexdata_ptr(i));
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
_local_fun_close(Environment *env, ham_u32_t flags)
{
    ham_status_t st;
    ham_status_t st2=HAM_SUCCESS;
    Device *device;
    ham_file_filter_t *file_head;

    /*
     * if we're not in read-only mode, and not an in-memory-database,
     * and the dirty-flag is true: flush the page-header to disk
     */
    if (env->get_header_page()
            && !(env->get_flags()&HAM_IN_MEMORY_DB)
            && env->get_device()
            && env->get_device()->is_open()
            && (!(env->get_flags()&HAM_READ_ONLY))) {
        st=env->get_header_page()->flush();
        if (!st2) st2 = st;
    }

    /*
     * flush the freelist
     */
    st=freel_shutdown(env);
    if (st) {
        if (st2 == 0) 
            st2 = st;
    }

    device=env->get_device();

    /*
     * close the header page
     *
     * !!
     * the last database, which was closed, has set the owner of the 
     * page to 0, which means that we can't call Page::free
     * etc. We have to use the device-routines.
     */
    if (env->get_header_page()) {
        Page *page=env->get_header_page();
        ham_assert(device, (0));
        if (page->get_pers()) {
            st=device->free_page(page);
            if (!st2) 
                st2=st;
        }
        delete page;
        env->set_header_page(0);
    }

    /* flush all pages, get rid of the cache */
    if (env->get_cache()) {
        (void)db_flush_all(env->get_cache(), 0);
        delete env->get_cache();
        env->set_cache(0);
    }

    /* close the device */
    if (device) {
        if (device->is_open()) {
            if (!(env->get_flags()&HAM_READ_ONLY)) {
                st=device->flush();
                if (!st2) 
                    st2=st;
            }
            st=device->close();
            if (!st2) 
                st2=st;
        }
        delete device;
        env->set_device(0);
    }

    /* close all file-level filters */
    file_head=env->get_file_filter();
    while (file_head) {
        ham_file_filter_t *next=file_head->_next;
        if (file_head->close_cb)
            file_head->close_cb((ham_env_t *)env, file_head);
        file_head=next;
    }
    env->set_file_filter(0);

    /* close the log and the journal */
    if (env->get_log()) {
        Log *log=env->get_log();
        st=log->close(flags&HAM_DONT_CLEAR_LOG);
        if (!st2) 
            st2 = st;
        delete log;
        env->set_log(0);
    }
    if (env->get_journal()) {
        Journal *journal=env->get_journal();
        st=journal->close(flags&HAM_DONT_CLEAR_LOG);
        if (!st2) 
            st2 = st;
        delete journal;
        env->set_journal(0);
    }

    return st2;
}

static ham_status_t 
_local_fun_get_parameters(Environment *env, ham_parameter_t *param)
{
    ham_parameter_t *p=param;

    if (p) {
        for (; p->name; p++) {
            switch (p->name) {
            case HAM_PARAM_CACHESIZE:
                p->value=env->get_cache()->get_capacity();
                break;
            case HAM_PARAM_PAGESIZE:
                p->value=env->get_pagesize();
                break;
            case HAM_PARAM_MAX_ENV_DATABASES:
                p->value=env->get_max_databases();
                break;
            case HAM_PARAM_GET_FLAGS:
                p->value=env->get_flags();
                break;
            case HAM_PARAM_GET_FILEMODE:
                p->value=env->get_file_mode();
                break;
            case HAM_PARAM_GET_FILENAME:
                if (env->get_filename().size())
                    p->value=(ham_u64_t)(PTR_TO_U64(env->get_filename().c_str()));
                else
                    p->value=0;
                break;
            case HAM_PARAM_LOG_DIRECTORY:
                if (env->get_log_directory().size())
                    p->value=(ham_u64_t)(PTR_TO_U64(env->get_log_directory().c_str()));
                else
                    p->value=0;
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
                    ham_status_t st = btree_stats_fill_ham_statistics_t(env, 0, 
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
_local_fun_flush(Environment *env, ham_u32_t flags)
{
    ham_status_t st;
    Database *db;
    Device *device=env->get_device();

    (void)flags;

    /* never flush an in-memory-database */
    if (env->get_flags()&HAM_IN_MEMORY_DB)
        return (0);

    /*
     * flush the open backends
     */
    db = env->get_databases();
    while (db) {
        Backend *be=db->get_backend();

        if (!be || !be->is_active())
            return HAM_NOT_INITIALIZED;
        st=be->flush();
        if (st)
            return st;
        db=db->get_next();
    }

    /*
     * update the header page, if necessary
     */
    if (env->is_dirty()) {
        st=env->get_header_page()->flush();
        if (st)
            return st;
    }

    /* flush all open pages to disk */
    st=db_flush_all(env->get_cache(), DB_FLUSH_NODELETE);
    if (st)
        return st;

    /* flush the device - this usually causes a fsync() */
    st=device->flush();
    if (st)
        return st;

    return (HAM_SUCCESS);
}

static ham_status_t 
_local_fun_create_db(Environment *env, Database *db, 
        ham_u16_t dbname, ham_u32_t flags, const ham_parameter_t *param)
{
    ham_status_t st;
    ham_u16_t keysize = 0;
    ham_u64_t cachesize = 0;
    ham_u16_t dam = 0;
    ham_u16_t dbi;
    ham_size_t i;
    Backend *be;
    ham_u32_t pflags;
    std::string logdir;

    db->set_rt_flags(0);

    /* parse parameters */
    st=__check_create_parameters(env, db, 0, &flags, param, 
            0, &keysize, &cachesize, &dbname, 0, &dam, logdir, true);
    if (st)
        return (st);

    /* store the env pointer in the database */
    db->set_env(env);

    /* reset all DB performance data */
    btree_stats_init_dbdata(db, db->get_perf_data());

    /*
     * set the flags; strip off run-time (per session) flags for the 
     * backend::create() method though.
     */
    db->set_rt_flags(flags);
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
    env->get_header_page()->set_db(db);
    ham_assert(env->get_header_page(), (0));

    /*
     * check if this database name is unique
     */
    ham_assert(env->get_max_databases() > 0, (0));
    for (i=0; i<env->get_max_databases(); i++) {
        ham_u16_t name = index_get_dbname(env->get_indexdata_ptr(i));
        if (!name)
            continue;
        if (name==dbname || dbname==HAM_FIRST_DATABASE_NAME) {
            (void)ham_close((ham_db_t *)db, HAM_DONT_LOCK);
            return (HAM_DATABASE_ALREADY_EXISTS);
        }
    }

    /*
     * find a free slot in the indexdata array and store the 
     * database name
     */
    ham_assert(env->get_max_databases() > 0, (0));
    for (dbi=0; dbi<env->get_max_databases(); dbi++) {
        ham_u16_t name = index_get_dbname(env->get_indexdata_ptr(dbi));
        if (!name) {
            index_set_dbname(env->get_indexdata_ptr(dbi), dbname);
            db->set_indexdata_offset(dbi);
            break;
        }
    }
    if (dbi==env->get_max_databases()) {
        (void)ham_close((ham_db_t *)db, HAM_DONT_LOCK);
        return (HAM_LIMITS_REACHED);
    }

    /* logging enabled? then the changeset and the log HAS to be empty */
#ifdef HAM_DEBUG
    if (env->get_flags()&HAM_ENABLE_RECOVERY) {
        ham_assert(env->get_changeset().is_empty(), (""));
        ham_assert(env->get_log()->is_empty(), (""));
    }
#endif

    /* create the backend */
    be=db->get_backend();
    if (be==NULL) {
        be=new BtreeBackend(db, flags);
        if (!be) {
            st=HAM_OUT_OF_MEMORY;
            (void)ham_close((ham_db_t *)db, HAM_DONT_LOCK);
            goto bail;
        }
        /* store the backend in the database */
        db->set_backend(be);
    }

    /* initialize the backend */
    st=be->create(keysize, pflags);
    if (st) {
        (void)ham_close((ham_db_t *)db, HAM_DONT_LOCK);
        goto bail;
    }

    ham_assert(be->is_active()!=0, (0));

    /*
     * initialize the remaining function pointers in Database
     */
    st=db->initialize_local();
    if (st) {
        (void)ham_close((ham_db_t *)db, HAM_DONT_LOCK);
        goto bail;
    }

    /*
     * set the default key compare functions
     */
    if (db->get_rt_flags()&HAM_RECORD_NUMBER) {
        db->set_compare_func(db_default_recno_compare);
    }
    else {
        db->set_compare_func(db_default_compare);
        db->set_prefix_compare_func(db_default_prefix_compare);
    }
    db->set_duplicate_compare_func(db_default_compare);
    env->set_dirty(true);

    /* finally calculate and store the data access mode */
    if (env->get_version(0) == 1 &&
        env->get_version(1) == 0 &&
        env->get_version(2) <= 9) {
        dam |= HAM_DAM_ENFORCE_PRE110_FORMAT;
        env->set_legacy(true);
    }
    if (!dam) {
        dam=(flags&HAM_RECORD_NUMBER)
            ? HAM_DAM_SEQUENTIAL_INSERT 
            : HAM_DAM_RANDOM_WRITE;
    }
    db->set_data_access_mode(dam);

    /* 
     * set the key compare function
     */
    if (db->get_rt_flags()&HAM_RECORD_NUMBER) {
        db->set_compare_func(db_default_recno_compare);
    }
    else {
        db->set_compare_func(db_default_compare);
        db->set_prefix_compare_func(db_default_prefix_compare);
    }
    db->set_duplicate_compare_func(db_default_compare);

    /*
     * on success: store the open database in the environment's list of
     * opened databases
     */
    db->set_next(env->get_databases());
    env->set_databases(db);

bail:
    /* if logging is enabled: flush the changeset and the header page */
    if (st==0 && env->get_flags()&HAM_ENABLE_RECOVERY) {
        ham_u64_t lsn;
        env->get_changeset().add_page(env->get_header_page());
        st=env_get_incremented_lsn(env, &lsn);
        if (st==0)
            st=env->get_changeset().flush(lsn);
    }

    return (st);
}

static ham_status_t 
_local_fun_open_db(Environment *env, Database *db, 
        ham_u16_t name, ham_u32_t flags, const ham_parameter_t *param)
{
    Database *head;
    ham_status_t st;
    ham_u16_t dam = 0;
    ham_u64_t cachesize = 0;
    Backend *be = 0;
    ham_u16_t dbi;
    std::string logdir;

    /*
     * make sure that this database is not yet open/created
     */
    if (db->is_active()) {
        ham_trace(("parameter 'db' is already initialized"));
        return (HAM_DATABASE_ALREADY_OPEN);
    }

    db->set_rt_flags(0);

    /* parse parameters */
    st=__check_create_parameters(env, db, 0, &flags, param, 
            0, 0, &cachesize, &name, 0, &dam, logdir, false);
    if (st)
        return (st);

    /*
     * make sure that this database is not yet open
     */
    head=env->get_databases();
    while (head) {
        db_indexdata_t *ptr=env->get_indexdata_ptr(head->get_indexdata_offset());
        if (index_get_dbname(ptr)==name)
            return (HAM_DATABASE_ALREADY_OPEN);
        head=head->get_next();
    }

    ham_assert(env->get_allocator(), (""));
    ham_assert(env->get_device(), (""));
    ham_assert(0 != env->get_header_page(), (0));
    ham_assert(env->get_max_databases() > 0, (0));

    /* store the env pointer in the database */
    db->set_env(env);

    /* reset the DB performance data */
    btree_stats_init_dbdata(db, db->get_perf_data());

    /*
     * search for a database with this name
     */
    for (dbi=0; dbi<env->get_max_databases(); dbi++) {
        db_indexdata_t *idx=env->get_indexdata_ptr(dbi);
        ham_u16_t dbname = index_get_dbname(idx);
        if (!dbname)
            continue;
        if (name==HAM_FIRST_DATABASE_NAME || name==dbname) {
            db->set_indexdata_offset(dbi);
            break;
        }
    }

    if (dbi==env->get_max_databases()) {
        (void)ham_close((ham_db_t *)db, HAM_DONT_LOCK);
        return (HAM_DATABASE_NOT_FOUND);
    }

    /* create the backend */
    be=db->get_backend();
    if (be==NULL) {
        be=new BtreeBackend(db, flags);
        if (!be) {
            (void)ham_close((ham_db_t *)db, HAM_DONT_LOCK);
            return (HAM_OUT_OF_MEMORY);
        }
        /* store the backend in the database */
        db->set_backend(be);
    }

    /* initialize the backend */
    st=be->open(flags);
    if (st) {
        (void)ham_close((ham_db_t *)db, HAM_DONT_LOCK);
        return (st);
    }

    ham_assert(be->is_active()!=0, (0));

    /*
     * initialize the remaining function pointers in Database
     */
    st=db->initialize_local();
    if (st) {
        (void)ham_close((ham_db_t *)db, HAM_DONT_LOCK);
        return (st);
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
    db->set_rt_flags(flags|be->get_flags());
    ham_assert(!(be->get_flags()&HAM_DISABLE_VAR_KEYLEN), 
            ("invalid persistent database flags 0x%x", be->get_flags()));
    ham_assert(!(be->get_flags()&HAM_CACHE_STRICT), 
            ("invalid persistent database flags 0x%x", be->get_flags()));
    ham_assert(!(be->get_flags()&HAM_CACHE_UNLIMITED), 
            ("invalid persistent database flags 0x%x", be->get_flags()));
    ham_assert(!(be->get_flags()&HAM_DISABLE_MMAP), 
            ("invalid persistent database flags 0x%x", be->get_flags()));
    ham_assert(!(be->get_flags()&HAM_WRITE_THROUGH), 
            ("invalid persistent database flags 0x%x", be->get_flags()));
    ham_assert(!(be->get_flags()&HAM_READ_ONLY), 
            ("invalid persistent database flags 0x%x", be->get_flags()));
    ham_assert(!(be->get_flags()&HAM_DISABLE_FREELIST_FLUSH), 
            ("invalid persistent database flags 0x%x", be->get_flags()));
    ham_assert(!(be->get_flags()&HAM_ENABLE_RECOVERY), 
            ("invalid persistent database flags 0x%x", be->get_flags()));
    ham_assert(!(be->get_flags()&HAM_AUTO_RECOVERY), 
            ("invalid persistent database flags 0x%x", be->get_flags()));
    ham_assert(!(be->get_flags()&HAM_ENABLE_TRANSACTIONS), 
            ("invalid persistent database flags 0x%x", be->get_flags()));
    ham_assert(!(be->get_flags()&DB_USE_MMAP), 
            ("invalid persistent database flags 0x%x", be->get_flags()));

    /*
     * SORT_DUPLICATES is only allowed if the Database was created
     * with ENABLE_DUPLICATES!
     */
    if ((flags&HAM_SORT_DUPLICATES) 
            && !(db->get_rt_flags()&HAM_ENABLE_DUPLICATES)) {
        ham_trace(("flag HAM_SORT_DUPLICATES set but duplicates are not "
                   "enabled for this Database"));
        (void)ham_close((ham_db_t *)db, HAM_DONT_LOCK);
        return (HAM_INV_PARAMETER);
    }

    /* finally calculate and store the data access mode */
    if (env->get_version(0) == 1 &&
        env->get_version(1) == 0 &&
        env->get_version(2) <= 9) {
        dam |= HAM_DAM_ENFORCE_PRE110_FORMAT;
        env->set_legacy(true);
    }
    if (!dam) {
        dam=(db->get_rt_flags()&HAM_RECORD_NUMBER)
            ? HAM_DAM_SEQUENTIAL_INSERT 
            : HAM_DAM_RANDOM_WRITE;
    }
    db->set_data_access_mode(dam);

    /* 
     * set the key compare function
     */
    if (db->get_rt_flags()&HAM_RECORD_NUMBER) {
        db->set_compare_func(db_default_recno_compare);
    }
    else {
        db->set_compare_func(db_default_compare);
        db->set_prefix_compare_func(db_default_prefix_compare);
    }
    db->set_duplicate_compare_func(db_default_compare);

    /*
     * on success: store the open database in the environment's list of
     * opened databases
     */
    db->set_next(env->get_databases());
    env->set_databases(db);

    return (0);
}

static ham_status_t 
_local_fun_txn_begin(Environment *env, Transaction **txn, 
                    const char *name, ham_u32_t flags)
{
    ham_status_t st;

    st=txn_begin(txn, env, name, flags);
    if (st) {
        txn_free(*txn);
        *txn=0;
    }

    /* append journal entry */
    if (st==0
            && env->get_flags()&HAM_ENABLE_RECOVERY
            && env->get_flags()&HAM_ENABLE_TRANSACTIONS) {
        ham_u64_t lsn;
        st=env_get_incremented_lsn(env, &lsn);
        if (st==0)
            st=env->get_journal()->append_txn_begin(*txn, env, name, lsn);
    }

    return (st);
}

static ham_status_t
_local_fun_txn_commit(Environment *env, Transaction *txn, ham_u32_t flags)
{
    ham_status_t st;

    /* are cursors attached to this txn? if yes, fail */
    if (txn_get_cursor_refcount(txn)) {
        ham_trace(("Transaction cannot be committed till all attached "
                    "Cursors are closed"));
        return (HAM_CURSOR_STILL_OPEN);
    }

    /* append journal entry */
    if (env->get_flags()&HAM_ENABLE_RECOVERY
            && env->get_flags()&HAM_ENABLE_TRANSACTIONS) {
        ham_u64_t lsn;
        st=env_get_incremented_lsn(env, &lsn);
        if (st)
            return (st);
        st=env->get_journal()->append_txn_commit(txn, lsn);
        if (st)
            return (st);
    }

    st=txn_commit(txn, flags);

    /* on success: flush all open file handles if HAM_WRITE_THROUGH is 
     * enabled; then purge caches */
    if (st==0) {
        if (env->get_flags()&HAM_WRITE_THROUGH) {
            Device *device=env->get_device();
            (void)env->get_log()->flush();
            (void)device->flush();
        }
    }

    return (st);
}

static ham_status_t
_local_fun_txn_abort(Environment *env, Transaction *txn, ham_u32_t flags)
{
    ham_status_t st=0;

    /* are cursors attached to this txn? if yes, fail */
    if (txn_get_cursor_refcount(txn)) {
        ham_trace(("Transaction cannot be aborted till all attached "
                    "Cursors are closed"));
        return (HAM_CURSOR_STILL_OPEN);
    }

    /* append journal entry */
    if (env->get_flags()&HAM_ENABLE_RECOVERY
            && env->get_flags()&HAM_ENABLE_TRANSACTIONS) {
        ham_u64_t lsn;
        st=env_get_incremented_lsn(env, &lsn);
        if (st==0)
            st=env->get_journal()->append_txn_abort(txn, lsn);
    }

    if (st==0)
        st=txn_abort(txn, flags);


    /* on success: flush all open file handles if HAM_WRITE_THROUGH is 
     * enabled; then purge caches */
    if (st==0) {
        if (env->get_flags()&HAM_WRITE_THROUGH) {
            Device *device=env->get_device();
            (void)env->get_log()->flush();
            (void)device->flush();
        }
    }

    return (st);
}

ham_status_t
env_initialize_local(Environment *env)
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
    env->_fun_txn_begin          =_local_fun_txn_begin;
    env->_fun_txn_commit         =_local_fun_txn_commit;
    env->_fun_txn_abort          =_local_fun_txn_abort;

    return (0);
}

void
env_append_txn(Environment *env, Transaction *txn)
{
    txn_set_env(txn, env);

    if (!env->get_newest_txn()) {
        ham_assert(env->get_oldest_txn()==0, (""));
        env->set_oldest_txn(txn);
        env->set_newest_txn(txn);
    }
    else {
        txn_set_older(txn, env->get_newest_txn());
        txn_set_newer(env->get_newest_txn(), txn);
        env->set_newest_txn(txn);
        /* if there's no oldest txn (this means: all txn's but the
         * current one were already flushed) then set this txn as
         * the oldest txn */
        if (!env->get_oldest_txn())
            env->set_oldest_txn(txn);
    }
}

void
env_remove_txn(Environment *env, Transaction *txn)
{
    if (env->get_newest_txn()==txn) {
        env->set_newest_txn(txn_get_older(txn));
    }

    if (env->get_oldest_txn()==txn) {
        Transaction *n=txn_get_newer(txn);
        env->set_oldest_txn(n);
        if (n)
            txn_set_older(n, 0);
    }
    else {
        ham_assert(!"not yet implemented", (""));
    }
}

static ham_status_t
__flush_txn(Environment *env, Transaction *txn)
{
    ham_status_t st=0;
    txn_op_t *op=txn_get_oldest_op(txn);
    txn_cursor_t *cursor=0;

    while (op) {
        txn_opnode_t *node=txn_op_get_node(op);
        Backend *be=txn_opnode_get_db(node)->get_backend();
        if (!be)
            return (HAM_INTERNAL_ERROR);

        /* make sure that this op was not yet flushed - this would be
         * a serious bug */
        ham_assert(txn_op_get_flags(op)!=TXN_OP_FLUSHED, (""));

        /* logging enabled? then the changeset and the log HAS to be empty */
#ifdef HAM_DEBUG
        if (env->get_flags()&HAM_ENABLE_RECOVERY) {
            ham_assert(env->get_changeset().is_empty(), (""));
            ham_assert(env->get_log()->is_empty(), (""));
        }
#endif

        /* 
         * depending on the type of the operation: actually perform the
         * operation on the btree 
         *
         * if the txn-op has a cursor attached, then all (txn)cursors 
         * which are coupled to this op have to be uncoupled, and their 
         * parent (btree) cursor must be coupled to the btree item instead.
         */
        if ((txn_op_get_flags(op)&TXN_OP_INSERT)
                || (txn_op_get_flags(op)&TXN_OP_INSERT_OW)
                || (txn_op_get_flags(op)&TXN_OP_INSERT_DUP)) {
            ham_u32_t additional_flag=
                (txn_op_get_flags(op)&TXN_OP_INSERT_DUP)
                    ? HAM_DUPLICATE
                    : HAM_OVERWRITE;
            if (!txn_op_get_cursors(op)) {
                st=be->insert(txn, txn_opnode_get_key(node), 
                        txn_op_get_record(op), 
                        txn_op_get_orig_flags(op)|additional_flag);
            }
            else {
                txn_cursor_t *tc2, *tc1=txn_op_get_cursors(op);
                Cursor *c2, *c1=txn_cursor_get_parent(tc1);
                /* pick the first cursor, get the parent/btree cursor and
                 * insert the key/record pair in the btree. The btree cursor
                 * then will be coupled to this item. */
                st=btree_cursor_insert(c1->get_btree_cursor(), 
                        txn_opnode_get_key(node), txn_op_get_record(op), 
                        txn_op_get_orig_flags(op)|additional_flag);
                if (st)
                    goto bail;

                /* uncouple the cursor from the txn-op, and remove it */
                txn_op_remove_cursor(op, tc1);
                c1->couple_to_btree();
                c1->set_to_nil(Cursor::CURSOR_TXN);

                /* all other (btree) cursors need to be coupled to the same 
                 * item as the first one. */
                while ((tc2=txn_op_get_cursors(op))) {
                    txn_op_remove_cursor(op, tc2);
                    c2=txn_cursor_get_parent(tc2);
                    btree_cursor_couple_to_other(c2->get_btree_cursor(), 
                                c1->get_btree_cursor());
                    c2->couple_to_btree();
                    c2->set_to_nil(Cursor::CURSOR_TXN);
                }
            }
        }
        else if (txn_op_get_flags(op)&TXN_OP_ERASE) {
            if (txn_op_get_referenced_dupe(op)) {
                st=btree_erase_duplicate((BtreeBackend *)be, txn,
                        txn_opnode_get_key(node), 
                        txn_op_get_referenced_dupe(op), txn_op_get_flags(op));
            }
            else {
                st=be->erase(txn, txn_opnode_get_key(node), 
                        txn_op_get_flags(op));
            }
        }

bail:
        /* now flush the changeset to disk */
        if (env->get_flags()&HAM_ENABLE_RECOVERY) {
            env->get_changeset().add_page(env->get_header_page());
            st=env->get_changeset().flush(txn_op_get_lsn(op));
        }

        if (st) {
            ham_trace(("failed to flush op: %d (%s)", 
                            (int)st, ham_strerror(st)));
            return (st);
        }

        /* 
         * this op is about to be flushed! 
         *
         * as a concequence, all (txn)cursors which are coupled to this op
         * have to be uncoupled, as their parent (btree) cursor was
         * already coupled to the btree item instead
         */
        txn_op_set_flags(op, TXN_OP_FLUSHED);
        while ((cursor=txn_op_get_cursors(op))) {
            Cursor *pc=txn_cursor_get_parent(cursor);
            ham_assert(pc->get_txn_cursor()==cursor, (""));
            pc->couple_to_btree();
            pc->set_to_nil(Cursor::CURSOR_TXN);
        }

        /* continue with the next operation of this txn */
        op=txn_op_get_next_in_txn(op);
    }

    return (0);
}

ham_status_t
env_flush_committed_txns(Environment *env)
{
    Transaction *oldest;

    /* always get the oldest transaction; if it was committed: flush 
     * it; if it was aborted: discard it; otherwise return */
    while ((oldest=env->get_oldest_txn())) {
        if (txn_get_flags(oldest)&TXN_STATE_COMMITTED) {
            ham_status_t st=__flush_txn(env, oldest);
            if (st)
                return (st);
        }
        else if (txn_get_flags(oldest)&TXN_STATE_ABORTED) {
            ; /* nop */
        }
        else
            break;

        /* now remove the txn from the linked list */
        env_remove_txn(env, oldest);

        /* and free the whole memory */
        txn_free(oldest);
    }

    /* clear the changeset; if the loop above was not entered or the 
     * transaction was empty then it may still contain pages */
    env->get_changeset().clear();

    return (0);
}

ham_status_t
env_get_incremented_lsn(Environment *env, ham_u64_t *lsn) 
{
    Journal *j=env->get_journal();
    if (j) {
        if (j->get_lsn()==0xffffffffffffffffull) {
            ham_log(("journal limits reached (lsn overflow) - please reorg"));
            return (HAM_LIMITS_REACHED);
        }
        *lsn=j->get_incremented_lsn();
        return (0);
    }
    else {
        ham_assert(!"need lsn but have no journal!", (""));
        return (HAM_INTERNAL_ERROR);
    }
}

static ham_status_t
purge_callback(Page *page)
{
    ham_status_t st=page->uncouple_all_cursors();
    if (st)
        return (st);

    st=page->flush();
    if (st)
        return (st);

    st=page->free();
    if (st)
        return (st);
    delete page;
    return (0);
}

ham_status_t
env_purge_cache(Environment *env)
{
    Cache *cache=env->get_cache();

    /* in-memory-db: don't remove the pages or they would be lost */
    if (env->get_flags()&HAM_IN_MEMORY_DB)
        return (0);

    return (cache->purge(purge_callback,
                (env->get_flags()&HAM_CACHE_STRICT) != 0));
}
