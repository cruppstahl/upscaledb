/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 */

#include "device.h"
#include "error.h"
#include "os.h"
#include "mem.h"

static ham_status_t 
__f_create(ham_dev_t *self, const char *fname, ham_u32_t flags, ham_u32_t mode)
{
    return (os_create(fname, flags, mode, 
                (ham_fd_t *)device_get_private(self)));
}

static ham_status_t 
__f_open(ham_dev_t *self, const char *fname, ham_u32_t flags)
{
    return (os_open(fname, flags, 
                (ham_fd_t *)device_get_private(self)));
}

static ham_status_t
__f_close(ham_dev_t *self)
{
    return (os_close(*(ham_fd_t *)device_get_private(self)));
}

static ham_status_t 
__f_flush(ham_dev_t *self)
{
    return (os_flush(*(ham_fd_t *)device_get_private(self)));
}

static ham_bool_t 
__f_is_open(ham_dev_t *self)
{
    return (HAM_INVALID_FD!=os_flush(*(ham_fd_t *)device_get_private(self)));
}

static ham_size_t 
__f_get_pagesize(ham_dev_t *self)
{
    (void)self;
    return (os_get_pagesize());
}

static ham_status_t 
__f_alloc_page(ham_dev_t *self, ham_page_t *page)
{
}

static ham_status_t 
__f_read(ham_dev_t *self, ham_offset_t offset, void *buffer, ham_size_t size)
{
}


static ham_status_t 
__f_write(ham_dev_t *self, ham_offset_t offset, void *buffer, ham_size_t size)
{
}

static ham_status_t
__f_read_page(ham_dev_t *self, ham_page_t *page)
{
}

static ham_status_t 
__f_write_page(ham_dev_t *self, ham_page_t *page)
{
}

static ham_status_t 
__f_free(ham_dev_t *self, void *buffer, ham_size_t size)
{
    (void)self;
    (void)buffer;
    (void)size;
    return (0);
}

static ham_status_t 
__f_free_page(ham_dev_t *self, ham_page_t *page)
{
    (void)self;
    (void)page;
    return (0);
}

static ham_status_t 
__m_create(ham_dev_t *self, const char *fname, ham_u32_t flags, ham_u32_t mode)
{
}

static ham_status_t 
__m_open(ham_dev_t *self, const char *fname, ham_u32_t flags)
{
}

static ham_status_t
__m_close(ham_dev_t *self)
{
}

static ham_status_t 
__m_flush(ham_dev_t *self)
{
}

static ham_bool_t 
__m_is_open(ham_dev_t *self)
{
}

static ham_size_t 
__m_get_pagesize(ham_dev_t *self)
{
    (void)self;
    return (1024*4);
}

static ham_status_t 
__f_alloc_page(ham_dev_t *self, ham_page_t *page)
{
}

static ham_status_t 
__f_read(ham_dev_t *self, ham_offset_t offset, void *buffer, ham_size_t size)
{
}


static ham_status_t 
__f_write(ham_dev_t *self, ham_offset_t offset, void *buffer, ham_size_t size)
{
}

static ham_status_t
__f_read_page(ham_dev_t *self, ham_page_t *page)
{
}

static ham_status_t 
__f_write_page(ham_dev_t *self, ham_page_t *page)
{
}

static ham_status_t 
__f_free(ham_dev_t *self, void *buffer, ham_size_t size)
{
}

static ham_status_t 
__f_free_page(ham_dev_t *self, ham_page_t *page)
{
}

static void 
__set_flags(ham_dev_t *self, ham_u32_t flags)
{
    device_set_flags(self, flags);
}

static ham_u32_t 
__get_flags(ham_dev_t *self)
{
    return (device_get_flags(self));
}


static void 
__destroy(ham_dev_t *self)
{
    ham_assert(!__f_is_open(self), "destroying an opened device");
    return (0);
}

ham_device_t *
ham_device_new(ham_db_t *db, ham_bool_t inmemorydb)
{
    static ham_device_t dev;
    memset(&dev, 0, sizeof(dev));
    device_set_db(&dev, db);

    if (inmemorydb) {
        device_set_private(&dev, 0);
        dev.create       = __m_create;
        dev.open         = __m_open;
        dev.close        = __m_close;
        dev.flush        = __m_flush;
        dev.is_open      = __m_is_open;
        dev.get_pagesize = __m_get_pagesize;
        dev.set_flags    = __set_flags;
        dev.get_flags    = __get_flags;
        dev.alloc_page   = __m_alloc_page;
        dev.read         = __m_read;
        dev.write        = __m_write;
        dev.read_page    = __m_read_page;
        dev.write_page   = __m_write_page;
        dev.free         = __m_free;
        dev.free_page    = __m_free_page;
        dev.destroy      = __destroy;
    }
    else {
        device_set_private(&dev, (void *)HAM_INVALID_FD);
        dev.create       = __f_create;
        dev.open         = __f_open;
        dev.close        = __f_close;
        dev.flush        = __f_flush;
        dev.is_open      = __f_is_open;
        dev.get_pagesize = __f_get_pagesize;
        dev.set_flags    = __set_flags;
        dev.get_flags    = __get_flags;
        dev.alloc_page   = __f_alloc_page;
        dev.read         = __f_read;
        dev.write        = __f_write;
        dev.read_page    = __f_read_page;
        dev.write_page   = __f_write_page;
        dev.free         = __f_free;
        dev.free_page    = __f_free_page;
        dev.destroy      = __destroy;
    }

    return (&dev);
}
