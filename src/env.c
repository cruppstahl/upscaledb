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
#include "cache.h"
#include "log.h"
#include "os.h"

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

ham_status_t 
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
     * initialize the device, if it does not yet exist
     */
    if (!env_get_device(env)) 
    {
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

ham_status_t 
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

ham_status_t
env_initialize_local(ham_env_t *env)
{
    env->_fun_create=_local_fun_create;
    env->_fun_open  =_local_fun_open;
    return (0);
}

