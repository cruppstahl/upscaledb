/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * device management; a device encapsulates the physical device, either a 
 * file or memory chunks (for in-memory-databases)
 *
 */

#ifndef HAM_DEVICE_H__
#define HAM_DEVICE_H__

#include "internal_fwd_decl.h"


/**
 * the device structure
 */
struct ham_device_t {
    /**
     * create a new device 
     */
    ham_status_t (*create)(ham_device_t *self, const char *fname, 
            ham_u32_t flags, ham_u32_t mode);

    /**
     * open an existing device
     */
    ham_status_t (*open)(ham_device_t *self, const char *fname, 
            ham_u32_t flags);

    /**
     * close the device
     */
    ham_status_t (*close)(ham_device_t *self);

    /**
     * flushes the device
     */
    ham_status_t (*flush)(ham_device_t *self);

    /**
     * truncate/resize the device
     */
    ham_status_t (*truncate)(ham_device_t *self, ham_offset_t newsize);

    /**
     * returns true if the device is open
     */
    ham_bool_t (*is_open)(ham_device_t *self);

    /**
     * get the current file/storage size
     */
    ham_status_t (*get_filesize)(ham_device_t *self, ham_offset_t *length);

    /**
     * seek position in a file
     */
    ham_status_t (*seek)(ham_device_t *self, ham_offset_t offset, int whence);

    /**
     * tell the position in a file
     */
    ham_status_t (*tell)(ham_device_t *self, ham_offset_t *offset);

    /**
     * reads from the device; this function does not use mmap
     */
    ham_status_t (*read)(ham_device_t *self, 
            ham_offset_t offset, void *buffer, ham_offset_t size);

    /**
     * writes to the device; this function does not use mmap,
     * the data is run through the file filters
     */
    ham_status_t (*write)(ham_device_t *self, 
            ham_offset_t offset, void *buffer, ham_offset_t size);

    /**
     * reads a page from the device; this function CAN use mmap
     */
    ham_status_t (*read_page)(ham_device_t *self, ham_page_t *page);

    /**
     * writes a page to the device
     */
    ham_status_t (*write_page)(ham_device_t *self, ham_page_t *page);

    /**
     * get the pagesize for this device
     */
    ham_size_t (*get_pagesize)(ham_device_t *self);

    /**
     * set the pagesize for this device
     */
    ham_status_t (*set_pagesize)(ham_device_t *self, ham_size_t pagesize);

    /**
     * set the device flags
     */
    void (*set_flags)(ham_device_t *self, ham_u32_t flags);

    /**
     * get the device flags
     */
    ham_u32_t (*get_flags)(ham_device_t *self);

    /**
     * allocate storage from this device; this function 
     * will *NOT* use mmap.
     */
    ham_status_t (*alloc)(ham_device_t *self, ham_size_t size, 
            ham_offset_t *address);

    /**
     * allocate storage for a page from this device; this function 
     * @e can use mmap.
     *
     * @note
     * The caller is responsible for flushing the page; the @ref free_page 
     * function will assert that the page is not dirty.
     */
    ham_status_t (*alloc_page)(ham_device_t *self, ham_page_t *page);

    /**
     * frees a page on the device; plays counterpoint to @ref alloc_page.
     */
    ham_status_t (*free_page)(ham_device_t *self, ham_page_t *page);

    /**
     * destroy the device object, free all memory
     */
    ham_status_t (*destroy)(ham_device_t *self);

    /** the memory allocator */
    mem_allocator_t *_malloc;

    /** the environment which employs this device */
    Environment *_env;

    /** Flags of this device
     *
     * Currently, these flags are used (at leas):
     * - @ref HAM_DISABLE_MMAP do not use mmap but pread/pwrite
     * - @ref DB_USE_MMAP use memory mapped I/O (this bit is 
     *		not observed through here, though)
     * - @ref HAM_READ_ONLY this is a read-only device
     */
    ham_u32_t _flags;

    /** some private data for this device */
    void *_private;

    /** the pagesize */
    ham_size_t _pagesize;

    /** the freelist cache is managed by the device */
    freelist_cache_t *_freelist_cache;
};

/* get the allocator of this device */
#define device_get_allocator(dev)          (dev)->_malloc

/* set the allocator of this device */
#define device_set_allocator(dev, a)       (dev)->_malloc = (a)

/* get the environment of this device */
#define device_get_env(dev)          (dev)->_env

/* set the environment of this device */
#define device_set_env(dev, e)          (dev)->_env = (e)

/* get the flags of this device */
#define device_get_flags(dev)              (dev)->_flags

/* set the flags of this device */
#define device_set_flags(dev, f)           (dev)->_flags=(f)

/* get the private data of this device */
#define device_get_private(dev)            (dev)->_private

/* set the private data of this device */
#define device_set_private(dev, p)         (dev)->_private=(p)

#define device_set_freelist_cache(dev, cache) (dev)->_freelist_cache=(cache)

#define device_get_freelist_cache(dev)       (dev)->_freelist_cache


/*
 * create a new device structure; either for in-memory or file-based
 */
extern ham_device_t *
ham_device_new(mem_allocator_t *alloc, Environment *env, int devtype);

/**
 * Devices: device type IDs
 */
#define HAM_DEVTYPE_FILE     0
#define HAM_DEVTYPE_MEMORY   1
#define HAM_DEVTYPE_CUSTOM   2


#endif /* HAM_DEVICE_H__ */
