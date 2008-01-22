/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 * 
 *
 * device management; a device encapsulates the physical device, either a 
 * file or memory chunks (for in-memory-databases)
 *
 */

#ifndef HAM_DEVICE_H__
#define HAM_DEVICE_H__

#ifdef __cplusplus
extern "C" {
#endif 

#include <ham/hamsterdb.h>
#include "page.h"
#include "mem.h"

/*
 * the device structure
 */

struct ham_device_t;
typedef struct ham_device_t ham_device_t; 

struct ham_device_t {
    /*
     * create a new device 
     */
    ham_status_t (*create)(ham_device_t *self, const char *fname, 
            ham_u32_t flags, ham_u32_t mode);

    /*
     * open an existing device
     */
    ham_status_t (*open)(ham_device_t *self, const char *fname, 
            ham_u32_t flags);

    /*
     * close the device; also calls self->del
     */
    ham_status_t (*close)(ham_device_t *self);

    /*
     * flushes the device
     */
    ham_status_t (*flush)(ham_device_t *self);

    /*
     * truncate/resize the device
     */
    ham_status_t (*truncate)(ham_device_t *self, ham_offset_t newsize);

    /*
     * returns true if the device is open
     */
    ham_bool_t (*is_open)(ham_device_t *self);

    /*
     * get the default pagesize
     */
    ham_size_t (*get_pagesize)(ham_device_t *self);

    /*
     * set the device flags
     */
    void (*set_flags)(ham_device_t *self, ham_u32_t flags);

    /*
     * get the device flags
     */
    ham_u32_t (*get_flags)(ham_device_t *self);

    /*
     * allocate storage from this device; this function 
     * will *NOT* use mmap.
     */
    ham_status_t (*alloc)(ham_device_t *self, ham_size_t size, 
            ham_offset_t *address);

    /*
     * allocate storage for a page from this device; this function 
     * *can* use mmap.
     *
     * !!
     * The caller is responsible for flushing the page; the function will 
     * assert that the page is not dirty.
     */
    ham_status_t (*alloc_page)(ham_device_t *self, ham_page_t *page, 
            ham_size_t size);

    /*
     * reads from the device; this function does not use mmap
     */
    ham_status_t (*read)(ham_db_t *db, ham_device_t *self, 
            ham_offset_t offset, void *buffer, ham_size_t size);

    /*
     * writes to the device; this function does not use mmap
     */
    ham_status_t (*write)(ham_db_t *db, ham_device_t *self, 
            ham_offset_t offset, void *buffer, ham_size_t size);

    /*
     * reads a page from the device; this function CAN use mmap
     */
    ham_status_t (*read_page)(ham_device_t *self, ham_page_t *page,
            ham_size_t size);

    /*
     * writes a page to the device
     */
    ham_status_t (*write_page)(ham_device_t *self, ham_page_t *page);

    /*
     * frees a page
     */
    ham_status_t (*free_page)(ham_device_t *self, ham_page_t *page);

    /*
     * destroy the device object, free all memory
     */
    ham_status_t (*destroy)(ham_device_t *self);

    /*
     * the memory allocator
     */
    mem_allocator_t *_malloc;

    /*
     * the pagesize
     */
    ham_size_t _pagesize;

    /*
     * flags of this device 
     */
    ham_u32_t _flags;

    /*
     * some private data for this device
     */
    void *_private;

};

/*
 * currently, the only flag means: do not use mmap but pread
 */
#define DEVICE_NO_MMAP                     1

/*
 * get the allocator of this device
 */
#define device_get_allocator(dev)          (dev)->_malloc

/*
 * set the allocator of this device
 */
#define device_set_allocator(dev, a)       (dev)->_malloc=a

/*
 * get the pagesize
 */
#define device_get_pagesize(dev)           (dev)->_pagesize

/*
 * set the pagesize
 */
#define device_set_pagesize(dev, ps)       (dev)->_pagesize=ps

/*
 * get the flags of this device
 */
#define device_get_flags(dev)              (dev)->_flags

/*
 * set the flags of this device
 */
#define device_set_flags(dev, f)           (dev)->_flags=f

/*
 * get the private data of this device
 */
#define device_get_private(dev)            (dev)->_private

/*
 * set the private data of this device
 */
#define device_set_private(dev, p)         (dev)->_private=p

/*
 * create a new device structure; either for in-memory or file-based
 */
extern ham_device_t *
ham_device_new(mem_allocator_t *alloc, ham_bool_t inmemorydb);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_DEVICE_H__ */
