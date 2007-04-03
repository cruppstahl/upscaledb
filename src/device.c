/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 */

#include <string.h>
#include "device.h"
#include "error.h"
#include "os.h"
#include "mem.h"
#include "db.h"
#include "page.h"
#include "error.h"

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
    ham_status_t st;
    dev_file_t *t=(dev_file_t *)device_get_private(self);

    st=os_create(fname, flags, mode, &t->fd);
    return (db_set_error(device_get_db(self), st));
}

static ham_status_t 
__f_open(ham_device_t *self, const char *fname, ham_u32_t flags)
{
    ham_status_t st;
    dev_file_t *t=(dev_file_t *)device_get_private(self);

    st=os_open(fname, flags, &t->fd);
    return (db_set_error(device_get_db(self), st));
}

static ham_status_t
__f_close(ham_device_t *self)
{
    ham_status_t st;
    dev_file_t *t=(dev_file_t *)device_get_private(self);

    st=os_close(t->fd);
    if (st==HAM_SUCCESS)
        t->fd=HAM_INVALID_FD;
    return (db_set_error(device_get_db(self), st));
}

static ham_status_t 
__f_flush(ham_device_t *self)
{
    ham_status_t st;
    dev_file_t *t=(dev_file_t *)device_get_private(self);

    st=os_flush(t->fd);
    return (db_set_error(device_get_db(self), st));
}

static ham_status_t 
__f_truncate(ham_device_t *self, ham_offset_t newsize)
{
    ham_status_t st;
    dev_file_t *t=(dev_file_t *)device_get_private(self);

    st=os_truncate(t->fd, newsize);
    return (db_set_error(device_get_db(self), st));
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
__f_read(ham_device_t *self, ham_offset_t offset, void *buffer, ham_size_t size)
{
    ham_status_t st;
    dev_file_t *t=(dev_file_t *)device_get_private(self);

    st=os_pread(t->fd, offset, buffer, size);
    return (db_set_error(device_get_db(self), st));
}

static ham_status_t
__f_read_page(ham_device_t *self, ham_page_t *page)
{
    ham_u8_t *buffer;
    ham_status_t st;
    ham_db_t *db=device_get_db(self);
    dev_file_t *t=(dev_file_t *)device_get_private(self);

    if (device_get_flags(self)&DEVICE_NO_MMAP) {
        if (page_get_pers(page)==0) {
            buffer=ham_mem_alloc(db, db_get_pagesize(db));
            if (!buffer)
                return (db_set_error(db, HAM_OUT_OF_MEMORY));
            page_set_pers(page, (union page_union_t *)buffer);
            page_set_npers_flags(page, 
                page_get_npers_flags(page)|PAGE_NPERS_MALLOC);
        }
        else
            ham_assert(!(page_get_npers_flags(page)&PAGE_NPERS_MALLOC), (0));

        st=__f_read(self, page_get_self(page), page_get_pers(page), 
                    db_get_pagesize(db));
        return (db_set_error(db, st));
    }

    ham_assert(page_get_pers(page)==0, (""));
    ham_assert(!(page_get_npers_flags(page)&PAGE_NPERS_MALLOC), (""));

    st=os_mmap(t->fd, page_get_mmap_handle_ptr(page), 
            page_get_self(page), db_get_pagesize(db), &buffer);
    if (st)
        return (db_set_error(db, st));
    page_set_pers(page, (union page_union_t *)buffer);
    return (db_set_error(db, HAM_SUCCESS));
}

static ham_status_t 
__f_alloc(ham_device_t *self, ham_size_t size, ham_offset_t *address)
{
    ham_status_t st;
    dev_file_t *t=(dev_file_t *)device_get_private(self);

    st=os_seek(t->fd, 0, HAM_OS_SEEK_END);
    if (st)
        return (db_set_error(device_get_db(self), st));
    st=os_tell(t->fd, address);
    if (st)
        return (db_set_error(device_get_db(self), st));
    st=os_truncate(t->fd, (*address)+size);
    if (st)
        return (db_set_error(device_get_db(self), st));

    return (db_set_error(device_get_db(self), st));
}

static ham_status_t 
__f_alloc_page(ham_device_t *self, ham_page_t *page)
{
    ham_status_t st;
    ham_offset_t pos;
    dev_file_t *t=(dev_file_t *)device_get_private(self);

    st=os_seek(t->fd, 0, HAM_OS_SEEK_END);
    if (st)
        return (db_set_error(device_get_db(self), st));
    st=os_tell(t->fd, &pos);
    if (st)
        return (db_set_error(device_get_db(self), st));
    st=os_truncate(t->fd, pos+db_get_pagesize(device_get_db(self)));
    if (st)
        return (db_set_error(device_get_db(self), st));

    page_set_self(page, pos);
    st=__f_read_page(self, page);
    return (db_set_error(device_get_db(self), st));
}

static ham_status_t 
__f_write(ham_device_t *self, ham_offset_t offset, void *buffer, 
            ham_size_t size)
{
    ham_status_t st;
    dev_file_t *t=(dev_file_t *)device_get_private(self);

    st=os_pwrite(t->fd, offset, buffer, size);
    return (db_set_error(device_get_db(self), st));
}

static ham_status_t 
__f_write_page(ham_device_t *self, ham_page_t *page)
{
    ham_db_t *db=device_get_db(self);

    return (__f_write(self, page_get_self(page), page_get_pers(page), 
            db_get_pagesize(db)));
}

static ham_status_t 
__f_free_page(ham_device_t *self, ham_page_t *page)
{
    ham_status_t st;
    ham_db_t *db=device_get_db(self);

    if (page_get_pers(page)) {
        if (page_get_npers_flags(page)&PAGE_NPERS_MALLOC) {
            ham_mem_free(db, page_get_pers(page));
            page_set_npers_flags(page, 
                page_get_npers_flags(page)&~PAGE_NPERS_MALLOC);
        }
        else {
            st=os_munmap(page_get_mmap_handle_ptr(page), 
                    page_get_pers(page), db_get_pagesize(db));
            if (st)
                return (db_set_error(db, st));
        }
    }

    page_set_pers(page, 0);
    return (db_set_error(db, HAM_SUCCESS));
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
    return (HAM_SUCCESS);
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
__m_alloc_page(ham_device_t *self, ham_page_t *page)
{
    ham_u8_t *buffer;
    ham_db_t *db=device_get_db(self);

    ham_assert(page_get_pers(page)==0, (0));

    buffer=ham_mem_alloc(db, db_get_pagesize(db));
    if (!buffer)
        return (db_set_error(db, HAM_OUT_OF_MEMORY));
    page_set_pers(page, (union page_union_t *)buffer);
    page_set_npers_flags(page, 
        page_get_npers_flags(page)|PAGE_NPERS_MALLOC);
    page_set_self(page, (ham_offset_t)buffer);

    return (HAM_SUCCESS);
}

static ham_status_t 
__m_read(ham_device_t *self, ham_offset_t offset, void *buffer, ham_size_t size)
{
    (void)self;
    (void)offset;
    (void)buffer;
    (void)size;
    ham_assert(!"this operation is not possible for in-memory-databases", (0));
    return (HAM_SUCCESS);
}


static ham_status_t 
__m_write(ham_device_t *self, ham_offset_t offset, void *buffer, 
            ham_size_t size)
{
    (void)self;
    (void)offset;
    (void)buffer;
    (void)size;
    ham_assert(!"this operation is not possible for in-memory-databases", (0));
    return (HAM_SUCCESS);
}

static ham_status_t
__m_read_page(ham_device_t *self, ham_page_t *page)
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
    ham_db_t *db=device_get_db(self);

    ham_assert(page_get_pers(page)!=0, (0));
    ham_assert(page_get_npers_flags(page)|PAGE_NPERS_MALLOC, (0));

    ham_mem_free(db, page_get_pers(page));
    page_set_pers(page, 0);
    page_set_npers_flags(page, 
        page_get_npers_flags(page)&~PAGE_NPERS_MALLOC);

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


static void 
__destroy(ham_device_t *self)
{
    ham_assert(!__f_is_open(self), ("destroying an opened device"));
}

ham_device_t *
ham_device_new(ham_db_t *db, ham_bool_t inmemorydb)
{
    static ham_device_t dev;
    memset(&dev, 0, sizeof(dev));
    device_set_db(&dev, db);

    if (inmemorydb) {
        static dev_inmem_t t;
        t.is_open=0;
        device_set_private(&dev, &t);

        dev.create       = __m_create;
        dev.open         = __m_open;
        dev.close        = __m_close;
        dev.flush        = __m_flush;
        dev.truncate     = __m_truncate;
        dev.is_open      = __m_is_open;
        dev.get_pagesize = __m_get_pagesize;
        dev.set_flags    = __set_flags;
        dev.get_flags    = __get_flags;
        dev.alloc        = __m_alloc;
        dev.alloc_page   = __m_alloc_page;
        dev.read         = __m_read;
        dev.write        = __m_write;
        dev.read_page    = __m_read_page;
        dev.write_page   = __m_write_page;
        dev.free_page    = __m_free_page;
        dev.destroy      = __destroy;
    }
    else {
        static dev_file_t t;
        t.fd=HAM_INVALID_FD;
        device_set_private(&dev, &t);

        dev.create       = __f_create;
        dev.open         = __f_open;
        dev.close        = __f_close;
        dev.flush        = __f_flush;
        dev.truncate     = __f_truncate;
        dev.is_open      = __f_is_open;
        dev.get_pagesize = __f_get_pagesize;
        dev.set_flags    = __set_flags;
        dev.get_flags    = __get_flags;
        dev.alloc        = __f_alloc;
        dev.alloc_page   = __f_alloc_page;
        dev.read         = __f_read;
        dev.write        = __f_write;
        dev.read_page    = __f_read_page;
        dev.write_page   = __f_write_page;
        dev.free_page    = __f_free_page;
        dev.destroy      = __destroy;
    }

    return (&dev);
}

