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

#include <string.h>
#include "device.h"
#include "error.h"
#include "os.h"
#include "mem.h"
#include "page.h"
#include "error.h"
#include "db.h"

typedef struct 
{
    ham_bool_t is_open;
} dev_inmem_t;

typedef struct 
{
    ham_fd_t fd;
} dev_file_t;

static ham_status_t 
__f_create(ham_device_t *self, const char *fname, ham_u32_t flags, 
            ham_u32_t mode)
{
    dev_file_t *t=(dev_file_t *)device_get_private(self);

	device_set_flags(self, flags);

    return (os_create(fname, flags, mode, &t->fd));
}

static ham_status_t 
__f_open(ham_device_t *self, const char *fname, ham_u32_t flags)
{
    dev_file_t *t=(dev_file_t *)device_get_private(self);

	device_set_flags(self, flags);

    return (os_open(fname, flags, &t->fd));
}

static ham_status_t
__f_close(ham_device_t *self)
{
    ham_status_t st;
    dev_file_t *t=(dev_file_t *)device_get_private(self);

    st=os_close(t->fd, device_get_flags(self));
    if (st==HAM_SUCCESS)
        t->fd=HAM_INVALID_FD;
    return (st);
}

static ham_status_t 
__f_flush(ham_device_t *self)
{
    dev_file_t *t=(dev_file_t *)device_get_private(self);

    return (os_flush(t->fd));
}

static ham_status_t 
__f_truncate(ham_device_t *self, ham_offset_t newsize)
{
    dev_file_t *t=(dev_file_t *)device_get_private(self);

    return (os_truncate(t->fd, newsize));
}

static ham_bool_t 
__f_is_open(ham_device_t *self)
{
    dev_file_t *t=(dev_file_t *)device_get_private(self);
    return (HAM_INVALID_FD!=t->fd);
}

static ham_size_t 
__f_get_pagesize(ham_device_t *self)
{
    (void)self;
    return (os_get_pagesize());
}

static ham_status_t 
__f_read(ham_db_t *db, ham_device_t *self, ham_offset_t offset, 
        void *buffer, ham_size_t size)
{
    dev_file_t *t=(dev_file_t *)device_get_private(self);
    ham_file_filter_t *head=db_get_file_filter(db);
    ham_status_t st;

    st=os_pread(t->fd, offset, buffer, size);
    if (st)
        return (db_set_error(db, st));

    /*
     * we're done unless there are file filters (or if we're reading the
     * header page - the header page is not filtered)
     */
    if (!head || offset==0)
        return (0);

    /*
     * otherwise run the filters
     */
    while (head) {
        if (head->after_read_cb) {
            st=head->after_read_cb(db, head, buffer, size);
            if (st)
                return (db_set_error(db, st));
        }
        head=head->_next;
    }

    return (0);
}

static ham_status_t
__f_read_page(ham_device_t *self, ham_page_t *page, ham_size_t size)
{
    ham_u8_t *buffer;
    ham_status_t st;
    dev_file_t *t=(dev_file_t *)device_get_private(self);
    ham_db_t *db=page_get_owner(page);
    ham_file_filter_t *head=db_get_file_filter(db);

    if (!size)
        size=device_get_pagesize(self);

    if (device_get_flags(self)&HAM_DISABLE_MMAP) {
		if (page_get_pers(page)==0) {
            buffer=allocator_alloc(device_get_allocator(self), size);
            if (!buffer)
                return (HAM_OUT_OF_MEMORY);
            page_set_pers(page, (union page_union_t *)buffer);
            page_set_npers_flags(page, 
                page_get_npers_flags(page)|PAGE_NPERS_MALLOC);
        }
        else
            ham_assert(!(page_get_npers_flags(page)&PAGE_NPERS_MALLOC), (0));

        return (__f_read(db, self, page_get_self(page), 
                    page_get_pers(page), size));
    }

    ham_assert(page_get_pers(page)==0, (""));
    ham_assert(!(page_get_npers_flags(page)&PAGE_NPERS_MALLOC), (""));

    st=os_mmap(t->fd, page_get_mmap_handle_ptr(page), 
            page_get_self(page), size, 
            device_get_flags(self)&HAM_READ_ONLY, &buffer);
    if (st)
        return (st);

    /*
     * we're done unless there are file filters (or if we're reading the
     * header page - the header page is not filtered)
     */
    if (!head || page_get_self(page)==0) {
        page_set_pers(page, (union page_union_t *)buffer);
        return (0);
    }

    /*
     * otherwise run the filters
     */
    while (head) {
        if (head->after_read_cb) {
            st=head->after_read_cb(db, head, buffer, size);
            if (st)
                return (db_set_error(db, st));
        }
        head=head->_next;
    }

    page_set_pers(page, (union page_union_t *)buffer);
    return (0);
}

static ham_status_t 
__f_alloc(ham_device_t *self, ham_size_t size, ham_offset_t *address)
{
    ham_status_t st;
    dev_file_t *t=(dev_file_t *)device_get_private(self);

    st=os_get_filesize(t->fd, address);
    if (st)
        return (st);
    st=os_truncate(t->fd, (*address)+size);
    if (st)
        return (st);

    return (0);
}

static ham_status_t 
__f_alloc_page(ham_device_t *self, ham_page_t *page, ham_size_t size)
{
    ham_status_t st;
    ham_offset_t pos;
    dev_file_t *t=(dev_file_t *)device_get_private(self);

    st=os_get_filesize(t->fd, &pos);
    if (st)
        return (st);

	st=os_truncate(t->fd, pos+size);
    if (st)
        return (st);

    page_set_self(page, pos);
    return (__f_read_page(self, page, size));
}

static ham_status_t 
__f_write(ham_db_t *db, ham_device_t *self, ham_offset_t offset, void *buffer, 
            ham_size_t size)
{
    dev_file_t *t=(dev_file_t *)device_get_private(self);
    ham_file_filter_t *head=db_get_file_filter(db);
    ham_u8_t *tempdata=0;
    ham_status_t st=0;

    /*
     * run page through page-level filters, but not for the 
     * root-page!
     */
    if (!head || offset==0)
        return (os_pwrite(t->fd, offset, buffer, size));

    /*
     * don't modify the data in-place!
     */
    tempdata=ham_mem_alloc(db, size);
    if (!tempdata)
        return (db_set_error(db, HAM_OUT_OF_MEMORY));
    memcpy(tempdata, buffer, size);

    while (head) {
        if (head->before_write_cb) {
            st=head->before_write_cb(db, head, tempdata, size);
            if (st) 
                break;
        }
        head=head->_next;
    }

    if (!st)
        st=os_pwrite(t->fd, offset, tempdata, size);

    ham_mem_free(db, tempdata);
    return (st);
}

static ham_status_t 
__f_write_page(ham_device_t *self, ham_page_t *page)
{
    return (__f_write(page_get_owner(page), self, page_get_self(page), 
                page_get_pers(page), device_get_pagesize(self)));
}

static ham_status_t 
__f_free_page(ham_device_t *self, ham_page_t *page)
{
    ham_status_t st;

    if (page_get_pers(page)) {
        if (page_get_npers_flags(page)&PAGE_NPERS_MALLOC) {
            allocator_free(device_get_allocator(self), page_get_pers(page));
            page_set_npers_flags(page, 
                page_get_npers_flags(page)&~PAGE_NPERS_MALLOC);
        }
        else {
            st=os_munmap(page_get_mmap_handle_ptr(page), 
                    page_get_pers(page), device_get_pagesize(self));
            if (st)
                return (st);
        }
    }

    page_set_pers(page, 0);
    return (0);
}

static ham_status_t 
__f_destroy(ham_device_t *self)
{
    ham_assert(!__f_is_open(self), ("destroying a device which is open"));

    allocator_free(device_get_allocator(self), device_get_private(self));
    allocator_free(device_get_allocator(self), self);
    return (0);
}

static ham_status_t 
__m_create(ham_device_t *self, const char *fname, ham_u32_t flags, 
            ham_u32_t mode)
{
    dev_inmem_t *t=(dev_inmem_t *)device_get_private(self);

    (void)fname;
    (void)flags;
    (void)mode;

    ham_assert(!t->is_open, (0));
    t->is_open=HAM_TRUE;
    return (0);
}

static ham_status_t 
__m_open(ham_device_t *self, const char *fname, ham_u32_t flags)
{
    (void)fname;
    (void)flags;
    ham_assert(!"can't open an in-memory-device", (0));
    return (HAM_NOT_IMPLEMENTED);
}

static ham_status_t
__m_close(ham_device_t *self)
{
    dev_inmem_t *t=(dev_inmem_t *)device_get_private(self);
    ham_assert(t->is_open, (0));
    t->is_open=HAM_FALSE;
    return (HAM_SUCCESS);
}

static ham_status_t 
__m_flush(ham_device_t *self)
{
    (void)self;
    return (HAM_SUCCESS);
}

static ham_status_t 
__m_truncate(ham_device_t *self, ham_offset_t newsize)
{
    (void)self;
    (void)newsize;
    return (HAM_SUCCESS);
}

static ham_bool_t 
__m_is_open(ham_device_t *self)
{
    dev_inmem_t *t=(dev_inmem_t *)device_get_private(self);
    return (t->is_open);
}

static ham_size_t 
__m_get_pagesize(ham_device_t *self)
{
    (void)self;
    return (1024*4);
}

static ham_status_t 
__m_alloc(ham_device_t *self, ham_size_t size, ham_offset_t *address)
{
    (void)self;
    (void)size;
    (void)address;
    ham_assert(!"can't alloc from an in-memory-device", (0));
    return (HAM_NOT_IMPLEMENTED);
}

static ham_status_t 
__m_alloc_page(ham_device_t *self, ham_page_t *page, ham_size_t size)
{
    ham_u8_t *buffer;

    ham_assert(page_get_pers(page)==0, (0));

    buffer=allocator_alloc(device_get_allocator(self), size);
    if (!buffer)
        return (HAM_OUT_OF_MEMORY);
    page_set_pers(page, (union page_union_t *)buffer);
    page_set_npers_flags(page, 
        page_get_npers_flags(page)|PAGE_NPERS_MALLOC);
    page_set_self(page, (ham_offset_t)buffer);

    return (HAM_SUCCESS);
}

static ham_status_t 
__m_read(ham_db_t *db, ham_device_t *self, ham_offset_t offset, 
        void *buffer, ham_size_t size)
{
    (void)db;
    (void)self;
    (void)offset;
    (void)buffer;
    (void)size;
    ham_assert(!"this operation is not possible for in-memory-databases", (0));
    return (HAM_SUCCESS);
}


static ham_status_t 
__m_write(ham_db_t *db, ham_device_t *self, ham_offset_t offset, void *buffer, 
            ham_size_t size)
{
    (void)db;
    (void)self;
    (void)offset;
    (void)buffer;
    (void)size;
    ham_assert(!"this operation is not possible for in-memory-databases", (0));
    return (HAM_SUCCESS);
}

static ham_status_t
__m_read_page(ham_device_t *self, ham_page_t *page, ham_size_t size)
{
    (void)self;
    (void)page;
    ham_assert(!"this operation is not possible for in-memory-databases", (0));
    return (HAM_SUCCESS);
}

static ham_status_t 
__m_write_page(ham_device_t *self, ham_page_t *page)
{
    (void)self;
    (void)page;
    ham_assert(!"this operation is not possible for in-memory-databases", (0));
    return (HAM_SUCCESS);
}

static ham_status_t 
__m_free_page(ham_device_t *self, ham_page_t *page)
{
    ham_assert(page_get_pers(page)!=0, (0));
    ham_assert(page_get_npers_flags(page)|PAGE_NPERS_MALLOC, (0));

    allocator_free(device_get_allocator(self), page_get_pers(page));
    page_set_pers(page, 0);
    page_set_npers_flags(page, 
        page_get_npers_flags(page)&~PAGE_NPERS_MALLOC);

    return (HAM_SUCCESS);
}

static ham_status_t 
__m_destroy(ham_device_t *self)
{
    ham_assert(!__m_is_open(self), ("destroying a device which is open"));

    allocator_free(device_get_allocator(self), device_get_private(self));
    allocator_free(device_get_allocator(self), self);
    return (HAM_SUCCESS);
}

static void 
__set_flags(ham_device_t *self, ham_u32_t flags)
{
    device_set_flags(self, flags);
}

static ham_u32_t 
__get_flags(ham_device_t *self)
{
    return (device_get_flags(self));
}

ham_device_t *
ham_device_new(mem_allocator_t *alloc, ham_bool_t inmemorydb)
{
    ham_device_t *dev=(ham_device_t *)allocator_alloc(alloc, sizeof(*dev));
    if (!dev)
        return (0);

    memset(dev, 0, sizeof(*dev));
    device_set_allocator(dev, alloc);

    if (inmemorydb) {
        dev_inmem_t *t=(dev_inmem_t *)allocator_alloc(alloc, sizeof(*t));
        if (!t)
            return (0);
        t->is_open=0;
        device_set_private(dev, t);

        dev->create       = __m_create;
        dev->open         = __m_open;
        dev->close        = __m_close;
        dev->flush        = __m_flush;
        dev->truncate     = __m_truncate;
        dev->is_open      = __m_is_open;
        dev->get_pagesize = __m_get_pagesize;
        dev->set_flags    = __set_flags;
        dev->get_flags    = __get_flags;
        dev->alloc        = __m_alloc;
        dev->alloc_page   = __m_alloc_page;
        dev->read         = __m_read;
        dev->write        = __m_write;
        dev->read_page    = __m_read_page;
        dev->write_page   = __m_write_page;
        dev->free_page    = __m_free_page;
        dev->destroy      = __m_destroy;
    }
    else {
        dev_file_t *t=(dev_file_t *)allocator_alloc(alloc, sizeof(*t));
        if (!t)
            return (0);
        t->fd=HAM_INVALID_FD;
        device_set_private(dev, t);

        dev->create       = __f_create;
        dev->open         = __f_open;
        dev->close        = __f_close;
        dev->flush        = __f_flush;
        dev->truncate     = __f_truncate;
        dev->is_open      = __f_is_open;
        dev->get_pagesize = __f_get_pagesize;
        dev->set_flags    = __set_flags;
        dev->get_flags    = __get_flags;
        dev->alloc        = __f_alloc;
        dev->alloc_page   = __f_alloc_page;
        dev->read         = __f_read;
        dev->write        = __f_write;
        dev->read_page    = __f_read_page;
        dev->write_page   = __f_write_page;
        dev->free_page    = __f_free_page;
        dev->destroy      = __f_destroy;
    }

    /*
     * initialize the pagesize with a default value - this will be
     * overwritten i.e. by ham_open, ham_create when the pagesize 
     * of the file is known
     */
    device_set_pagesize(dev, dev->get_pagesize(dev));

    return (dev);
}

