/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
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
#include <ham/hamsterdb_int.h>

/*
 * the device structure
 */
typedef struct {
    /*
     * create a new device 
     */
    ham_status_t (*_fun_create)(ham_dev_t *self, const char *fname, 
            ham_u32_t flags, ham_u32_t mode);

    /*
     * open an existing device
     */
    ham_status_t (*_fun_open)(ham_dev_t *self, const char *fname, 
            ham_u32_t flags);

    /*
     * close the device; also calls self->del
     */
    ham_status_t (*_fun_close)(ham_dev_t *self);

    /*
     * flushes the device
     */
    ham_status_t (*_fun_flush)(ham_dev_t *self);

    /*
     * returns true if the device is open
     */
    ham_bool_t (*_fun_is_open)(ham_dev_t *self);

    /*
     * get the default pagesize
     */
    ham_size_t (*_fun_get_pagesize)(ham_dev_t *self);

    /*
     * set the device flags
     */
    void (*_fun_set_flags)(ham_dev_t *self, ham_u32_t flags);

    /*
     * get the device flags
     */
    ham_u32_t (*_fun_get_flags)(ham_dev_t *self);

    /*
     * allocate storage for a page from this device; this function 
     * *can* use mmap.
     *
     * !!
     * The caller is responsible for flushing the page; the function will 
     * assert that the page is not dirty.
     */
    ham_status_t (*_fun_alloc_page)(ham_dev_t *self, ham_page_t *page);

    /*
     * reads from the device; this function does not use mmap
     */
    ham_status_t (*_fun_read)(ham_dev_t *self, ham_offset_t offset, 
            void *buffer, ham_size_t size);

    /*
     * writes to the device; this function does not use mmap
     */
    ham_status_t (*_fun_write)(ham_dev_t *self, ham_offset_t offset, 
            void *buffer, ham_size_t size);

    /*
     * reads a page from the device; this function CAN use mmap
     */
    ham_status_t (*_fun_read_page)(ham_dev_t *self, ham_page_t *page);

    /*
     * writes a page to the device
     */
    ham_status_t (*_fun_write_page)(ham_dev_t *self, ham_page_t *page);

    /*
     * frees the memory
     */
    ham_status_t (*_fun_free)(ham_dev_t *self, void *buffer, ham_size_t size);

    /*
     * frees a page
     */
    ham_status_t (*_fun_free_page)(ham_dev_t *self, ham_page_t *page);

    /*
     * destructor for this device structure
     */
    void (*_fun_destroy)(ham_dev_t *self);

    /*
     * the database
     */
    ham_db_t *_db;

    /*
     * flags of this device 
     */
    ham_u32_t _flags;

    /*
     * some private data for this device
     */
    void *m_private;

} ham_dev_t;

/*
 * get the database of this device
 */
#define device_get_db(dev)                 (dev)->_db

/*
 * set the database of this device
 */
#define device_set_db(dev, db)             (dev)->_db=db

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
ham_device_new(ham_db_t *db, ham_bool_t inmemorydb);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_DEVICE_H__ */
