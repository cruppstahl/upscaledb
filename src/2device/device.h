/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
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
#include "2config/env_config.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

class Page;

class Device {
  public:
    // Constructor
    Device(const EnvironmentConfiguration &config)
      : m_config(config) {
    }

    // virtual destructor
    virtual ~Device() {
    }

    // Returns the current configuration
    const EnvironmentConfiguration &config() const {
      return (m_config);
    }

    // Returns the current page size
    size_t page_size() const {
      return (m_config.page_size_bytes);
    }

    // Create a new device - called in ham_env_create
    virtual void create() = 0;

    // Opens an existing device - called in ham_env_open
    virtual void open() = 0;

    // Returns true if the device is open
    virtual bool is_open() = 0;

    // Closes the device - called in ham_env_close
    virtual void close() = 0;

    // Flushes the device - called in ham_env_flush
    virtual void flush() = 0;

    // Truncate/resize the device
    virtual void truncate(uint64_t new_size) = 0;

    // Returns the current file/storage size
    virtual uint64_t file_size() = 0;

    // Seek position in a file
    virtual void seek(uint64_t offset, int whence) = 0;

    // Tell the position in a file
    virtual uint64_t tell() = 0;

    // Reads from the device; this function does not use mmap
    virtual void read(uint64_t offset, void *buffer, size_t len) = 0;

    // Writes to the device; this function does not use mmap
    virtual void write(uint64_t offset, void *buffer, size_t len) = 0;

    // Allocate storage from this device; this function
    // will *NOT* use mmap. returns the offset of the allocated storage.
    virtual uint64_t alloc(size_t len) = 0;

    // Reads a page from the device; this function CAN use mmap
    virtual void read_page(Page *page, uint64_t address) = 0;

    // Allocate storage for a page from this device; this function
    // can use mmap if available
    virtual void alloc_page(Page *page) = 0;

    // Frees a page on the device.
    // The caller is responsible for flushing the page; the @ref free_page
    // function will assert that the page is not dirty.
    virtual void free_page(Page *page) = 0;

    // Returns true if the specified range is in mapped memory
    virtual bool is_mapped(uint64_t file_offset, size_t size) const = 0;

    // Removes unused space at the end of the file
    virtual void reclaim_space() = 0;

  protected:
    // the Environment configuration settings
    const EnvironmentConfiguration &m_config;

    friend class DeviceTest;
    friend class InMemoryDeviceTest;
};

} // namespace hamsterdb

#endif /* HAM_DEVICE_H */
