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

#include <string.h>

#include "db.h"
#include "device.h"
#include "env.h"
#include "error.h"
#include "log.h"
#include "mem.h"
#include "os.h"
#include "txn.h"
#include "util.h"


static ham_size_t 
__get_aligned_entry_size(ham_size_t data_size)
{
    ham_size_t s=sizeof(log_entry_t)+data_size;
    s += 8-1;
    s -= (s % 8);
    return (s);
}

ham_status_t
log_create(ham_env_t *env, ham_u32_t mode, ham_u32_t flags, ham_log_t **plog)
{
    log_header_t header;
    ham_status_t st;
    const char *dbpath=env_get_filename(env);
    mem_allocator_t *alloc=env_get_allocator(env);
    char filename[HAM_OS_MAX_PATH];
    ham_log_t *log=(ham_log_t *)allocator_alloc(alloc, sizeof(ham_log_t));
    if (!log)
        return (HAM_OUT_OF_MEMORY);
    memset(log, 0, sizeof(ham_log_t));
    log_set_fd(log, HAM_INVALID_FD);

    *plog=0;

    ham_assert(env, (0));

    log_set_allocator(log, alloc);
    log_set_env(log, env);
    log_set_flags(log, flags);

    /* create the files */
    util_snprintf(filename, sizeof(filename), "%s.log%d", dbpath, 0);
    st=os_create(filename, 0, mode, &log_get_fd(log));
    if (st) {
        allocator_free(alloc, log);
        return (st);
    }

    /* write the file header with the magic */
    memset(&header, 0, sizeof(header));
    log_header_set_magic(&header, HAM_LOG_HEADER_MAGIC);

    st=os_write(log_get_fd(log), &header, sizeof(header));
    if (st) {
        (void)log_close(log, HAM_FALSE);
        return (st);
    }

    *plog=log;
    return (0);
}

ham_status_t
log_open(ham_env_t *env, ham_u32_t flags, ham_log_t **plog)
{
    log_header_t header;
    const char *dbpath=env_get_filename(env);
    mem_allocator_t *alloc=env_get_allocator(env);
    ham_status_t st;
    char filename[HAM_OS_MAX_PATH];
    ham_log_t *log=(ham_log_t *)allocator_alloc(alloc, sizeof(ham_log_t));
    if (!log)
        return (HAM_OUT_OF_MEMORY);
    memset(log, 0, sizeof(ham_log_t));
    log_set_fd(log, HAM_INVALID_FD);

    *plog=0;

    ham_assert(env, (0));

    log_set_allocator(log, alloc);
    log_set_env(log, env);
    log_set_flags(log, flags);

    /* open the file */
    util_snprintf(filename, sizeof(filename), "%s.log%d", dbpath, 0);
    st=os_open(filename, 0, &log_get_fd(log));
    if (st) {
        allocator_free(alloc, log);
        return (st);
    }

    /* check the file header with the magic */
    memset(&header, 0, sizeof(header));
    st=os_pread(log_get_fd(log), 0, &header, sizeof(header));
    if (st) {
        (void)log_close(log, HAM_FALSE);
        return (st);
    }
    if (log_header_get_magic(&header)!=HAM_LOG_HEADER_MAGIC) {
        ham_trace(("logfile has unknown magic or is corrupt"));
        (void)log_close(log, HAM_FALSE);
        return (HAM_LOG_INV_FILE_HEADER);
    }

    /* store the lsn */
    log_set_lsn(log, log_header_get_lsn(&header));

    *plog=log;
    return (0);
}

ham_status_t
log_is_empty(ham_log_t *log, ham_bool_t *isempty)
{
    ham_status_t st; 
    ham_offset_t size;

    st=os_get_filesize(log_get_fd(log), &size);
    if (st)
        return (st);
    if (size && size!=sizeof(log_header_t)) {
        *isempty=HAM_FALSE;
        return (0);
    }

    *isempty=HAM_TRUE;
    return (0);
}

ham_status_t
log_append_entry(ham_log_t *log, log_entry_t *entry, ham_size_t size)
{
    ham_status_t st;

    st=os_write(log_get_fd(log), entry, size);
    if (st)
        return (st);

    return (os_flush(log_get_fd(log)));
}

ham_status_t
log_append_write(ham_log_t *log, ham_txn_t *txn, ham_u64_t lsn,
        ham_offset_t offset, ham_u8_t *data, ham_size_t size)
{
    ham_status_t st;
    ham_size_t alloc_size=__get_aligned_entry_size(size);
    log_entry_t *entry;
    ham_u8_t *alloc_buf;

    alloc_buf=allocator_alloc(log_get_allocator(log), alloc_size);
    if (!alloc_buf)
        return (HAM_OUT_OF_MEMORY);

    /* store the lsn - it will be needed later when the log file is closed */
    if (lsn)
        log_set_lsn(log, lsn);

    entry=(log_entry_t *)(alloc_buf+alloc_size-sizeof(log_entry_t));

    memset(entry, 0, sizeof(*entry));
    log_entry_set_lsn(entry, lsn);
    if (txn)
        log_entry_set_txn_id(entry, txn_get_id(txn));
    log_entry_set_type(entry, LOG_ENTRY_TYPE_WRITE);
    log_entry_set_offset(entry, offset);
    log_entry_set_data_size(entry, size);
    memcpy(alloc_buf, data, size);

    st=log_append_entry(log, (log_entry_t *)alloc_buf, alloc_size);
    allocator_free(log_get_allocator(log), alloc_buf);
    return (st);
}

ham_status_t
log_clear(ham_log_t *log)
{
    ham_status_t st;

    st=os_truncate(log_get_fd(log), sizeof(log_header_t));
    if (st)
        return (st);

    /* after truncate, the file pointer is far beyond the new end of file;
     * reset the file pointer, or the next write will resize the file to
     * the original size */
    return (os_seek(log_get_fd(log), sizeof(log_header_t), HAM_OS_SEEK_SET));
}

ham_status_t
log_get_entry(ham_log_t *log, log_iterator_t *iter, log_entry_t *entry,
                ham_u8_t **data)
{
    ham_status_t st;

    *data=0;

    /* if the iterator is initialized and was never used before: read
     * the file size */
    if (!iter->_offset) {
        st=os_get_filesize(log_get_fd(log), &iter->_offset);
        if (st)
            return (st);
    }

    /* if the current file is empty: no need to continue */
    if (iter->_offset<=sizeof(log_header_t)) {
        log_entry_set_lsn(entry, 0);
        return (0);
    }

    /* otherwise read the log_entry_t header (without extended data) 
     * from the file */
    iter->_offset-=sizeof(log_entry_t);

    st=os_pread(log_get_fd(log), iter->_offset, entry, sizeof(*entry));
    if (st)
        return (st);

    /* now read the extended data, if it's available */
    if (log_entry_get_data_size(entry)) {
        ham_offset_t pos=iter->_offset-log_entry_get_data_size(entry);
        // pos += 8-1;
        pos -= (pos % 8);

        *data=allocator_alloc(log_get_allocator(log), 
                        (ham_size_t)log_entry_get_data_size(entry));
        if (!*data)
            return (HAM_OUT_OF_MEMORY);

        st=os_pread(log_get_fd(log), pos, *data, 
                    (ham_size_t)log_entry_get_data_size(entry));
        if (st) {
            allocator_free(log_get_allocator(log), *data);
            *data=0;
            return (st);
        }

        iter->_offset=pos;
    }
    else
        *data=0;

    return (0);
}

ham_status_t
log_close(ham_log_t *log, ham_bool_t noclear)
{
    ham_status_t st=0;
    log_header_t header;

    /* write the file header with the magic and the last used lsn */
    memset(&header, 0, sizeof(header));
    log_header_set_magic(&header, HAM_LOG_HEADER_MAGIC);
    log_header_set_lsn(&header, log_get_lsn(log));

    st=os_pwrite(log_get_fd(log), 0, &header, sizeof(header));
    if (st)
        return (st);

    if (!noclear)
        (void)log_clear(log);

    if (log_get_fd(log)!=HAM_INVALID_FD) {
        if ((st=os_close(log_get_fd(log), 0)))
            return (st);
        log_set_fd(log, HAM_INVALID_FD);
    }

    allocator_free(log_get_allocator(log), log);
    return (st);
}

ham_status_t
log_append_page(ham_log_t *log, ham_page_t *page, ham_u64_t lsn)
{
    ham_status_t st=0;
    ham_env_t *env=device_get_env(page_get_device(page));
    ham_file_filter_t *head=env_get_file_filter(env);
    ham_u8_t *p;
    ham_size_t size=env_get_pagesize(env);

    if (!log)
        return (0);

    /*
     * run page through page-level filters, but not for the 
     * root-page!
     */
    if (head && page_get_self(page)!=0) {
        p=(ham_u8_t *)allocator_alloc(log_get_allocator(log), 
                env_get_pagesize(env));
        if (!p)
            return (HAM_OUT_OF_MEMORY);
        memcpy(p, page_get_raw_payload(page), size);

        while (head) {
            if (head->before_write_cb) {
                st=head->before_write_cb(env, head, p, size);
                if (st) 
                    break;
            }
            head=head->_next;
        }
    }
    else
        p=(ham_u8_t *)page_get_raw_payload(page);

    if (st==0)
        st=log_append_write(log, env_get_flushed_txn(env), lsn, 
                        page_get_self(page), p, size);

    if (p!=page_get_raw_payload(page))
        allocator_free(log_get_allocator(log), p);

    return (st);
}

ham_status_t
log_recover(ham_log_t *log)
{
    ham_status_t st;
    ham_page_t *page;
    ham_env_t *env=log_get_env(log);
    ham_device_t *device=env_get_device(env);
    log_entry_t entry;
    log_iterator_t it={0};
    ham_u8_t *data;
    ham_offset_t filesize;

    /* get the file size of the database; otherwise we do not know if we
     * modify an existing page or if one of the pages has to be allocated */
    st=device->get_filesize(device, &filesize);
    if (st)
        return (st);

    /* temporarily disable logging */
    env_set_rt_flags(env, env_get_rt_flags(env)&~HAM_ENABLE_RECOVERY);

    while (1) {
        /* get the next entry in the logfile */
        st=log_get_entry(log, &it, &entry, &data);
        if (st)
            return (st);

        /* reached end of the log file? */
        if (log_entry_get_lsn(&entry)==0)
            break;

        /* currently we only have support for WRITEs */
        ham_assert(log_entry_get_type(&entry)==LOG_ENTRY_TYPE_WRITE, (""));

        /* 
         * Was the page appended or overwritten? 
         *
         * Either way we have to bypass the cache and all upper layers. We
         * cannot call db_alloc_page() or db_fetch_page() since we do not have
         * a ham_db_t handle. env_alloc_page()/env_fetch_page() would work,
         * but then the page ownership is not set correctly (but the 
         * ownership is verified later, and this would fail).
         */
        if (log_entry_get_offset(&entry)==filesize) {
            /* appended... */
            filesize+=log_entry_get_offset(&entry);

            page=page_new(env);
            if (st)
                return (st);
            st=page_alloc(page);
            if (st)
                return (st);
            ham_assert(page_get_self(page)==log_entry_get_offset(&entry), 
                        (""));
            ham_assert(env_get_pagesize(env)==log_entry_get_data_size(&entry), 
                        (""));

            memcpy(page_get_pers(page), data, log_entry_get_data_size(&entry));

            st=page_flush(page);
            if (st)
                return (st);
            page_delete(page);
        }
        else {
            /* overwritten... */
            page=page_new(env);
            if (st)
                return (st);
            page_set_self(page, log_entry_get_offset(&entry));
            st=page_fetch(page);
            if (st)
                return (st);
            ham_assert(env_get_pagesize(env)==log_entry_get_data_size(&entry), 
                        (""));

            memcpy(page_get_pers(page), data, log_entry_get_data_size(&entry));

            st=page_flush(page);
            if (st)
                return (st);
            page_delete(page);
        }

        /* store the lsn in the log - will be needed later when recovering
         * the journal */
        log_set_lsn(log, log_entry_get_data_size(&entry));
    }

    /* re-enable the logging */
    env_set_rt_flags(env, env_get_rt_flags(env)|HAM_ENABLE_RECOVERY);
    
    /* and finally clear the log */
    st=log_clear(log);
    if (st) {
        ham_log(("unable to clear logfiles; please manually delete the "
                "log files before re-opening the Database"));
        return (st);
    }

    return (0);
}

