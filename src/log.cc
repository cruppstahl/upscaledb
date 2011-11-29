/*
 * Copyright (C) 2005-2011 Christoph Rupp (chris@crupp.de).
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


Log::Log(Environment *env, ham_u32_t flags)
: m_env(env), m_flags(flags), m_lsn(0), m_fd(HAM_INVALID_FD)
{
}

ham_status_t
Log::create(void)
{
    Log::Header header;
    ham_status_t st;
    const char *dbpath=env_get_filename(m_env).c_str();
    char filename[HAM_OS_MAX_PATH];

    ham_assert(m_env, (0));

    /* create the files */
    util_snprintf(filename, sizeof(filename), "%s.log%d", dbpath, 0);
    st=os_create(filename, 0, env_get_file_mode(m_env), &m_fd);
    if (st)
        return (st);

    /* write the file header with the magic */
    header.magic=HEADER_MAGIC;

    st=os_write(m_fd, &header, sizeof(header));
    if (st) {
        close();
        return (st);
    }

    return (0);
}

ham_status_t
Log::open(void)
{
    Log::Header header;
    const char *dbpath=env_get_filename(m_env).c_str();
    ham_status_t st;
    char filename[HAM_OS_MAX_PATH];

    /* open the file */
    util_snprintf(filename, sizeof(filename), "%s.log%d", dbpath, 0);
    st=os_open(filename, 0, &m_fd);
    if (st) {
        close();
        return (st);
    }

    /* check the file header with the magic */
    st=os_pread(m_fd, 0, &header, sizeof(header));
    if (st) {
        close();
        return (st);
    }
    if (header.magic!=HEADER_MAGIC) {
        ham_trace(("logfile has unknown magic or is corrupt"));
        close();
        return (HAM_LOG_INV_FILE_HEADER);
    }

    /* store the lsn */
    m_lsn=header.lsn;

    return (0);
}

bool
Log::is_empty(void)
{
    ham_status_t st; 
    ham_offset_t size;

    st=os_get_filesize(m_fd, &size);
    if (st)
		return (st ? false : true); /* TODO throw */
    if (size && size!=sizeof(Log::Header))
        return (false);

    return (true);
}

ham_status_t
Log::clear(void)
{
    ham_status_t st;

    st=os_truncate(m_fd, sizeof(Log::Header));
    if (st)
        return (st);

    /* after truncate, the file pointer is far beyond the new end of file;
     * reset the file pointer, or the next write will resize the file to
     * the original size */
    return (os_seek(m_fd, sizeof(Log::Header), HAM_OS_SEEK_SET));
}

ham_status_t
Log::get_entry(Log::Iterator *iter, Log::Entry *entry, ham_u8_t **data)
{
    ham_status_t st;

    *data=0;

    /* if the iterator is initialized and was never used before: read
     * the file size */
    if (!*iter) {
        st=os_get_filesize(m_fd, iter);
        if (st)
            return (st);
    }

    /* if the current file is empty: no need to continue */
    if (*iter<=sizeof(Log::Header)) {
        entry->lsn=0;
        return (0);
    }

    /* otherwise read the Log::Entry header (without extended data) 
     * from the file */
    *iter-=sizeof(Log::Entry);

    st=os_pread(m_fd, *iter, entry, sizeof(*entry));
    if (st)
        return (st);

    /* now read the extended data, if it's available */
    if (entry->data_size) {
        ham_offset_t pos=(*iter)-entry->data_size;
        // pos += 8-1;
        pos -= (pos % 8);

        *data=(ham_u8_t *)allocator_alloc(env_get_allocator(m_env), 
                        (ham_size_t)entry->data_size);
        if (!*data)
            return (HAM_OUT_OF_MEMORY);

        st=os_pread(m_fd, pos, *data, (ham_size_t)entry->data_size);
        if (st) {
            allocator_free(env_get_allocator(m_env), *data);
            *data=0;
            return (st);
        }

        *iter=pos;
    }
    else
        *data=0;

    return (0);
}

ham_status_t
Log::close(ham_bool_t noclear)
{
    ham_status_t st=0;
    Log::Header header;

    /* write the file header with the magic and the last used lsn */
    header.magic=HEADER_MAGIC;
    header.lsn=m_lsn;

    st=os_pwrite(m_fd, 0, &header, sizeof(header));
    if (st)
        return (st);

    if (!noclear)
        clear();

    if (m_fd!=HAM_INVALID_FD) {
        if ((st=os_close(m_fd, 0)))
            return (st);
        m_fd=HAM_INVALID_FD;
    }

    return (0);
}

ham_status_t
Log::append_page(ham_page_t *page, ham_u64_t lsn)
{
    ham_status_t st=0;
    ham_file_filter_t *head=env_get_file_filter(m_env);
    ham_u8_t *p;
    ham_size_t size=env_get_pagesize(m_env);

    /*
     * run page through page-level filters, but not for the 
     * root-page!
     */
    if (head && page_get_self(page)!=0) {
        p=(ham_u8_t *)allocator_alloc(env_get_allocator(m_env), 
                env_get_pagesize(m_env));
        if (!p)
            return (HAM_OUT_OF_MEMORY);
        memcpy(p, page_get_raw_payload(page), size);

        while (head) {
            if (head->before_write_cb) {
                st=head->before_write_cb((ham_env_t *)m_env, head, p, size);
                if (st) 
                    break;
            }
            head=head->_next;
        }
    }
    else
        p=(ham_u8_t *)page_get_raw_payload(page);

    if (st==0)
        st=append_write(lsn, page_get_self(page), p, size);

    if (p!=page_get_raw_payload(page))
        allocator_free(env_get_allocator(m_env), p);

    return (st);
}

ham_status_t
Log::recover()
{
    ham_status_t st;
    ham_page_t *page;
    ham_device_t *device=env_get_device(m_env);
    Log::Entry entry;
    Iterator it=0;
    ham_u8_t *data=0;
    ham_offset_t filesize;

    /* get the file size of the database; otherwise we do not know if we
     * modify an existing page or if one of the pages has to be allocated */
    st=device->get_filesize(device, &filesize);
    if (st)
        return (st);

    /* temporarily disable logging */
    env_set_rt_flags(m_env, env_get_rt_flags(m_env)&~HAM_ENABLE_RECOVERY);

    while (1) {
        /* clean up memory of the previous loop */
        if (data) {
            allocator_free(env_get_allocator(m_env), data);
            data=0;
        }

        /* get the next entry in the logfile */
        st=get_entry(&it, &entry, &data);
        if (st)
            goto bail;

        /* reached end of the log file? */
        if (entry.lsn==0)
            break;

        /* currently we only have support for WRITEs */
        ham_assert(entry.type==ENTRY_TYPE_WRITE, (""));

        /* 
         * Was the page appended or overwritten? 
         *
         * Either way we have to bypass the cache and all upper layers. We
         * cannot call db_alloc_page() or db_fetch_page() since we do not have
         * a Database handle. env_alloc_page()/env_fetch_page() would work,
         * but then the page ownership is not set correctly (but the 
         * ownership is verified later, and this would fail).
         */
        if (entry.offset==filesize) {
            /* appended... */
            filesize+=entry.data_size;

            page=page_new(m_env);
            if (st)
                goto bail;
            st=page_alloc(page);
            if (st)
                goto bail;
        }
        else {
            /* overwritten... */
            page=page_new(m_env);
            if (st)
                goto bail;
            page_set_self(page, entry.offset);
            st=page_fetch(page);
            if (st)
                goto bail;
        }

        ham_assert(page_get_self(page)==entry.offset, (""));
        ham_assert(env_get_pagesize(m_env)==entry.data_size, (""));

        /* overwrite the page data */
        memcpy(page_get_pers(page), data, entry.data_size);

        /* flush the modified page to disk */
        page_set_dirty(page);
        st=page_flush(page);
        if (st)
            goto bail;
        st=page_free(page);
        if (st)
            goto bail;
        page_delete(page);

        /* store the lsn in the log - will be needed later when recovering
         * the journal */
        m_lsn=entry.lsn;
    }

    /* and finally clear the log */
    st=clear();
    if (st) {
        ham_log(("unable to clear logfiles; please manually delete the "
                ".log0 file of this Database, then open again."));
        goto bail;
    }

bail:
    /* re-enable the logging */
    env_set_rt_flags(m_env, env_get_rt_flags(m_env)|HAM_ENABLE_RECOVERY);
    
    /* clean up memory */
    if (data) {
        allocator_free(env_get_allocator(m_env), data);
        data=0;
    }

    return (st);
}

ham_status_t
Log::flush(void)
{
    return (os_flush(m_fd));
}

ham_status_t
Log::append_write(ham_u64_t lsn, ham_offset_t offset, 
                    ham_u8_t *data, ham_size_t size)
{
    Log::Entry entry;

    /* store the lsn - it will be needed later when the log file is closed */
    if (lsn)
        m_lsn=lsn;

    entry.lsn=lsn;
    entry.type=ENTRY_TYPE_WRITE;
    entry.offset=offset;
    entry.data_size=size;

    return (os_writev(m_fd, data, size, &entry, sizeof(entry)));
}

