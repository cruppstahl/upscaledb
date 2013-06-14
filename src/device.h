/*
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */
 

/*
 * device management; a device encapsulates the physical device, either a
 * file or memory chunks (for in-memory-databases)
 *
 */
 

#ifndef HAM_DEVICE_H__
#define HAM_DEVICE_H__

#include <ham/hamsterdb.h>

namespace hamsterdb {

class Page;
class Environment;

class Device {
  public:
    // Constructor
    //
    // initialize the pagesize with a default value - this will be
    // overwritten i.e. by ham_env_open, ham_env_create when the pagesize
    // of the file is known
    Device(Environment *env, ham_u32_t flags)
      : m_env(env), m_flags(flags), m_pagesize(HAM_DEFAULT_PAGESIZE) {
    }

    // virtual destructor
    virtual ~Device() {
    }

    // Create a new device - called in ham_env_create
    virtual ham_status_t create(const char *filename, ham_u32_t flags,
                ham_u32_t mode) = 0;

    // opens an existing device - called in ham_env_open
    virtual ham_status_t open(const char *filename, ham_u32_t flags) = 0;

    // closes the device - called in ham_env_close
    virtual ham_status_t close() = 0;

    // flushes the device - called in ham_env_flush
    virtual ham_status_t flush() = 0;

    // truncate/resize the device
    virtual ham_status_t truncate(ham_u64_t newsize) = 0;

    // returns true if the device is open
    virtual bool is_open() = 0;

    // get the current file/storage size
    virtual ham_status_t get_filesize(ham_u64_t *length) = 0;

    // seek position in a file
    virtual ham_status_t seek(ham_u64_t offset, int whence) = 0;

    // tell the position in a file
    virtual ham_status_t tell(ham_u64_t *offset) = 0;

    // reads from the device; this function does not use mmap
    virtual ham_status_t read(ham_u64_t offset, void *buffer,
                ham_u64_t size) = 0;

    // writes to the device; this function does not use mmap
    virtual ham_status_t write(ham_u64_t offset, void *buffer,
                ham_u64_t size) = 0;

    // writes to the device; this function does not use mmap
    virtual ham_status_t writev(ham_u64_t offset, void *buffer1,
                ham_u64_t size1, void *buffer2, ham_u64_t size2) = 0;

    // reads a page from the device; this function CAN use mmap
    virtual ham_status_t read_page(Page *page) = 0;

    // writes a page to the device
    virtual ham_status_t write_page(Page *page) = 0;

    // allocate storage from this device; this function
    // will *NOT* use mmap.
    virtual ham_status_t alloc(ham_size_t size, ham_u64_t *address) = 0;

    // allocate storage for a page from this device; this function
    // can use mmap if available
    virtual ham_status_t alloc_page(Page *page) = 0;

    // frees a page on the device
    //
    // The caller is responsible for flushing the page; the @ref free_page
    // function will assert that the page is not dirty.
    virtual void free_page(Page *page) = 0;

    // get the Environment
    //
    // TODO get rid of this function. It's only used in the PageManager.
    Environment *get_env() {
      return (m_env);
    }

    // set the pagesize for this device 
    void set_pagesize(ham_size_t pagesize) {
      m_pagesize = pagesize;
    }

    // disable memory mapped I/O - used for testing
    void test_disable_mmap() {
      m_flags |= HAM_DISABLE_MMAP;
    }

  protected:
    // the environment which employs this device 
    Environment *m_env;

    // the device flags 
    ham_u32_t m_flags;

    // the page size 
    ham_size_t m_pagesize;

    friend class DeviceTest;
    friend class InMemoryDeviceTest;
};

} // namespace hamsterdb

#endif /* HAM_DEVICE_H__ */
