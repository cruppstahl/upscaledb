/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Device management; a device encapsulates the physical device, either a
 * file or memory chunks (for in-memory-databases)
 *
 * @exception_safe: nothrow
 * @thread_safe: yes
 */
 
#ifndef HAM_DEVICE_H
#define HAM_DEVICE_H

#include "0root/root.h"

#include "ham/hamsterdb.h"

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class Page;

class Device {
  public:
    // Constructor
    Device(ham_u32_t flags, size_t page_size, ham_u64_t file_size_limit)
      : m_flags(flags), m_page_size(page_size),
        m_file_size_limit(file_size_limit) {
    }

    // virtual destructor
    virtual ~Device() {
    }

    // Create a new device - called in ham_env_create
    virtual void create(const char *filename, ham_u32_t flags,
                ham_u32_t mode) = 0;

    // opens an existing device - called in ham_env_open
    virtual void open(const char *filename, ham_u32_t flags) = 0;

    // sets the page size (used when opening the device)
    void set_page_size(size_t page_size) {
      m_page_size = page_size;
    }

    // returns the page size
    size_t get_page_size() const {
      return (m_page_size);
    }

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
    virtual void read(ham_u64_t offset, void *buffer, size_t len) = 0;

    // writes to the device; this function does not use mmap
    virtual void write(ham_u64_t offset, void *buffer, size_t len) = 0;

    // reads a page from the device; this function CAN use mmap
    virtual void read_page(Page *page, ham_u64_t address, size_t page_size) = 0;

    // writes a page to the device
    virtual void write_page(Page *page) = 0;

    // allocate storage from this device; this function
    // will *NOT* use mmap. returns the offset of the allocated storage.
    virtual ham_u64_t alloc(size_t len) = 0;

    // allocate storage for a page from this device; this function
    // can use mmap if available
    virtual void alloc_page(Page *page, size_t page_size) = 0;

    // frees a page on the device
    //
    // The caller is responsible for flushing the page; the @ref free_page
    // function will assert that the page is not dirty.
    virtual void free_page(Page *page) = 0;

    // disable memory mapped I/O - used for testing
    void test_disable_mmap() {
      m_flags |= HAM_DISABLE_MMAP;
    }

  protected:
    // the device flags 
    ham_u32_t m_flags;

    // the page size
    size_t m_page_size;

    // the file size limit (in bytes)
    ham_u64_t m_file_size_limit;

    friend class DeviceTest;
    friend class InMemoryDeviceTest;
};

} // namespace hamsterdb

#endif /* HAM_DEVICE_H */
