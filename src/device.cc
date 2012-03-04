/**
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
#include "error.h"
#include "mem.h"
#include "os.h"
#include "page.h"
#include "env.h"


ham_status_t
FileDevice::read(ham_offset_t offset, void *buffer, ham_offset_t size)
{
    ham_file_filter_t *head=0;
    ham_status_t st;

    st=os_pread(m_fd, offset, buffer, size);
    if (st)
        return (st);

    /*
     * we're done unless there are file filters (or if we're reading the
     * header page - the header page is not filtered)
     */
    head=m_env->get_file_filter();
    if (!head || offset==0)
        return (0);

    /*
     * otherwise run the filters
     */
    while (head) {
        if (head->after_read_cb) {
            st=head->after_read_cb((ham_env_t *)m_env, head,
                        (ham_u8_t *)buffer, (ham_size_t)size);
            if (st)
                return (st);
        }
        head=head->_next;
    }

    return (0);
}

ham_status_t
FileDevice::read_page(Page *page)
{
    ham_u8_t *buffer;
    ham_status_t st;
    ham_file_filter_t *head=0;
    ham_size_t size=get_pagesize();
    
    head=m_env->get_file_filter();

    /*
     * first, try to mmap the file (if mmap is available/enabled).
     *
     * however, in some scenarios on win32, mmap can fail because resources
     * are exceeded (non-paged memory pool).
     * in such a case, the os_mmap function will return HAM_LIMITS_REACHED
     * and we force a fallback to read/write.
     */
    if (!(m_flags&HAM_DISABLE_MMAP)) {
        st=os_mmap(m_fd, page->get_mmap_handle_ptr(),
                page->get_self(), size, m_flags&HAM_READ_ONLY, &buffer);
        if (st && st!=HAM_LIMITS_REACHED)
            return (st);
        if (st==HAM_LIMITS_REACHED) {
            set_flags(get_flags()|HAM_DISABLE_MMAP);
            goto fallback_rw;
        }
    }
    else {
fallback_rw:
        if (page->get_pers()==0) {
            buffer=(ham_u8_t *)m_env->get_allocator()->alloc(size);
            if (!buffer)
                return (HAM_OUT_OF_MEMORY);
            page->set_pers((page_data_t *)buffer);
            page->set_flags(page->get_flags()|Page::NPERS_MALLOC);
        }
        else
            ham_assert(!(page->get_flags()&Page::NPERS_MALLOC), (0));

        st=FileDevice::read(page->get_self(), page->get_pers(), size);
        if (st)
            return (st);
    }

    /*
     * we're done unless there are file filters (or if we're reading the
     * header page - the header page is not filtered)
     */
    if (!head || page->is_header()) {
        page->set_pers((page_data_t *)buffer);
        return (0);
    }

    /* otherwise run the filters */
    while (head) {
        if (head->after_read_cb) {
            st=head->after_read_cb((ham_env_t *)m_env, head, buffer, size);
            if (st)
                return (st);
        }
        head=head->_next;
    }

    page->set_pers((page_data_t *)buffer);
    return (0);
}

ham_status_t
FileDevice::write(ham_offset_t offset, void *buffer, ham_offset_t size)
{
    ham_u8_t *tempdata=0;
    ham_status_t st=0;
    ham_file_filter_t *head=0;

    /*
     * run page through page-level filters, but not for the
     * root-page!
     */
    head=m_env->get_file_filter();
    if (!head || offset==0)
        return (os_pwrite(m_fd, offset, buffer, size));

    /* don't modify the data in-place!  */
    tempdata=(ham_u8_t *)m_env->get_allocator()->alloc((ham_size_t)size);
    if (!tempdata)
        return (HAM_OUT_OF_MEMORY);
    memcpy(tempdata, buffer, size);

    while (head) {
        if (head->before_write_cb) {
            st=head->before_write_cb((ham_env_t *)m_env, head, tempdata,
                            (ham_size_t)size);
            if (st)
                break;
        }
        head=head->_next;
    }

    if (!st)
        st=os_pwrite(m_fd, offset, tempdata, size);

    m_env->get_allocator()->free(tempdata);
    return (st);
}

ham_status_t
FileDevice::free_page(Page *page)
{
    ham_status_t st;

    if (page->get_pers()) {
        if (page->get_flags()&Page::NPERS_MALLOC) {
            m_env->get_allocator()->free(page->get_pers());
            page->set_flags(page->get_flags()&~Page::NPERS_MALLOC);
        }
        else {
            st=os_munmap(page->get_mmap_handle_ptr(),
                    page->get_pers(), get_pagesize());
            if (st)
                return (st);
        }
    }

    page->set_pers(0);
    return (0);
}

