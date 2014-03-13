/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
 */

/*
 * device management; a device encapsulates the physical device, either a
 * file or memory chunks (for in-memory-databases)
 *
 */
 
#ifndef HAM_DEVICE_H__
#define HAM_DEVICE_H__

#include <ham/hamsterdb.h>

#include "config.h"

namespace hamsterdb {

class Page;
class LocalEnvironment;

class Device {
  public:
    // Constructor
    Device(LocalEnvironment *env, ham_u32_t flags)
      : m_env(env), m_flags(flags) {
    }

    // virtual destructor
    virtual ~Device() {
    }

    // Create a new device - called in ham_env_create
    virtual void create(const char *filename, ham_u32_t flags,
                ham_u32_t mode) = 0;

    // opens an existing device - called in ham_env_open
    virtual void open(const char *filename, ham_u32_t flags) = 0;

    // closes the device - called in ham_env_close
    virtual void close() = 0;

    // flushes the device - called in ham_env_flush
    virtual void flush() = 0;

    // truncate/resize the device
    virtual void truncate(ham_u64_t newsize) = 0;

    // returns true if the device is open
    virtual bool is_open() = 0;

    // get the current file/storage size
    virtual ham_u64_t get_file_size() = 0;

    // seek position in a file
    virtual void seek(ham_u64_t offset, int whence) = 0;

    // tell the position in a file
    virtual ham_u64_t tell() = 0;

    // reads from the device; this function does not use mmap
    virtual void read(ham_u64_t offset, void *buffer,
                ham_u64_t size) = 0;

    // writes to the device; this function does not use mmap
    virtual void write(ham_u64_t offset, void *buffer,
                ham_u64_t size) = 0;

    // reads a page from the device; this function CAN use mmap
    virtual void read_page(Page *page, ham_u32_t page_size) = 0;

    // writes a page to the device
    virtual void write_page(Page *page) = 0;

    // allocate storage from this device; this function
    // will *NOT* use mmap.
    virtual ham_u64_t alloc(ham_u32_t size) = 0;

    // allocate storage for a page from this device; this function
    // can use mmap if available
    virtual void alloc_page(Page *page, ham_u32_t page_size) = 0;

    // frees a page on the device
    //
    // The caller is responsible for flushing the page; the @ref free_page
    // function will assert that the page is not dirty.
    virtual void free_page(Page *page) = 0;

    // get the Environment
    //
    // TODO get rid of this function. It's only used in the PageManager.
    LocalEnvironment *get_env() {
      return (m_env);
    }

    // disable memory mapped I/O - used for testing
    void test_disable_mmap() {
      m_flags |= HAM_DISABLE_MMAP;
    }

  protected:
    // the environment which employs this device 
    LocalEnvironment *m_env;

    // the device flags 
    ham_u32_t m_flags;

    friend class DeviceTest;
    friend class InMemoryDeviceTest;
};

} // namespace hamsterdb

#endif /* HAM_DEVICE_H__ */
