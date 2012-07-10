/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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
#include "os.h"
#include "mem.h"
#include "db.h"

class Page;
class Device;
class DeviceImplInMemory;
class DeviceImplDisk;


class DeviceImplementation {
  public:
    /** constructor */
    DeviceImplementation(Device *device)
      : m_device(device), m_pagesize(0) {
    }

    /** Create a new device */
    virtual ham_status_t create(const char *filename, ham_u32_t flags, 
                ham_u32_t mode) = 0;

    /** opens an existing device */
    virtual ham_status_t open(const char *filename, ham_u32_t flags) = 0;

    /** closes the device */
    virtual ham_status_t close() = 0;

    /** flushes the device */
    virtual ham_status_t flush() = 0;

    /** truncate/resize the device */
    virtual ham_status_t truncate(ham_offset_t newsize) = 0;

    /** returns true if the device is open */
    virtual bool is_open() = 0;

    /** get the current file/storage size */
    virtual ham_status_t get_filesize(ham_offset_t *length) = 0;

    /** seek position in a file */
    virtual ham_status_t seek(ham_offset_t offset, int whence) = 0;

    /** tell the position in a file */
    virtual ham_status_t tell(ham_offset_t *offset) = 0;

    /** reads from the device; this function does not use mmap */
    virtual ham_status_t read(ham_offset_t offset, void *buffer, 
                ham_offset_t size) = 0;

    /** writes to the device; this function does not use mmap,
     * and is responsible for writing the data is run through the file 
     * filters */
    virtual ham_status_t write(ham_offset_t offset, void *buffer, 
                ham_offset_t size) = 0;

    /** reads a page from the device; this function CAN use mmap */
    virtual ham_status_t read_page(Page *page) = 0;

    /** writes a page to the device */
    virtual ham_status_t write_page(Page *page) = 0;

    /** allocate storage from this device; this function 
     * will *NOT* use mmap.  */
    virtual ham_status_t alloc(ham_size_t size, ham_offset_t *address) = 0;

    /**
     * allocate storage for a page from this device; this function 
     * can use mmap if available
     *
     * @note
     * The caller is responsible for flushing the page; the @ref free_page 
     * function will assert that the page is not dirty.
     */
    virtual ham_status_t alloc_page(Page *page) = 0;

    /** frees a page on the device; plays counterpoint to @ref alloc_page */
    virtual ham_status_t free_page(Page *page) = 0;

    /** sets the pagesize */
    void set_pagesize(ham_size_t ps) {
      m_pagesize=ps;
    }

    /** gets the pagesize */
    ham_size_t get_pagesize() {
      return (m_pagesize);
    }

  protected:
    /** the Device object which created this DeviceImplementation */
    Device *m_device;

    /** the pagesize */
    ham_size_t m_pagesize;
};


/**
 * a File-based device
 */
class DeviceImplDisk : public DeviceImplementation {
  public:
    DeviceImplDisk(Device *device)
      : DeviceImplementation(device) {
    }

    /** Create a new device */
    virtual ham_status_t create(const char *filename, ham_u32_t flags, 
                ham_u32_t mode) {
      return (os_create(filename, flags, mode, &m_fd));
    }

    /** opens an existing device */
    virtual ham_status_t open(const char *filename, ham_u32_t flags) {
      return (os_open(filename, flags, &m_fd));
    }

    /** closes the device */
    virtual ham_status_t close() {
      ham_status_t st = os_close(m_fd);
      if (st == HAM_SUCCESS)
        m_fd = HAM_INVALID_FD;
      return (st);
    }

    /** flushes the device */
    virtual ham_status_t flush() {
      return (os_flush(m_fd));
    }

    /** truncate/resize the device */
    virtual ham_status_t truncate(ham_offset_t newsize) {
      return (os_truncate(m_fd, newsize));
    }

    /** returns true if the device is open */
    virtual bool is_open() {
      return (HAM_INVALID_FD != m_fd);
    }

    /** get the current file/storage size */
    virtual ham_status_t get_filesize(ham_offset_t *length) {
      *length = 0;
      return (os_get_filesize(m_fd, length));
    }

    /** seek position in a file */
    virtual ham_status_t seek(ham_offset_t offset, int whence) {
      return (os_seek(m_fd, offset, whence));
    }

    /** tell the position in a file */
    virtual ham_status_t tell(ham_offset_t *offset) {
      return (os_tell(m_fd, offset));
    }

    /** reads from the device; this function does not use mmap */
    virtual ham_status_t read(ham_offset_t offset, void *buffer, 
                ham_offset_t size);

    /** writes to the device; this function does not use mmap,
     * and is responsible for writing the data is run through the file 
     * filters */
    virtual ham_status_t write(ham_offset_t offset, void *buffer, 
                ham_offset_t size);

    /** reads a page from the device; this function CAN use mmap */
    virtual ham_status_t read_page(Page *page);

    /** writes a page to the device */
    virtual ham_status_t write_page(Page *page);

    /** allocate storage from this device; this function 
     * will *NOT* use mmap.  */
    virtual ham_status_t alloc(ham_size_t size, ham_offset_t *address) {
      ham_status_t st = os_get_filesize(m_fd, address);
      if (st)
        return (st);
      return (os_truncate(m_fd, (*address)+size));
    }

    /**
     * allocate storage for a page from this device; this function 
     * can use mmap if available
     *
     * @note
     * The caller is responsible for flushing the page; the @ref free_page 
     * function will assert that the page is not dirty.
     */
    virtual ham_status_t alloc_page(Page *page);

    /** frees a page on the device; plays counterpoint to @ref alloc_page */
    virtual ham_status_t free_page(Page *page);

  private:
    ham_fd_t m_fd;
};


/**
 * an In-Memory device
 */
class DeviceImplInMemory : public DeviceImplementation {
  public:
    /** constructor */
    DeviceImplInMemory(Device *device)
      : DeviceImplementation(device) {
    }

    /** Create a new device */
    virtual ham_status_t create(const char *filename, ham_u32_t flags, 
                ham_u32_t mode) {
      m_is_open = true;
      return (0);
    }

    /** opens an existing device */
    virtual ham_status_t open(const char *filename, ham_u32_t flags) {
      ham_assert(!"can't open an in-memory-device");
      return (HAM_NOT_IMPLEMENTED);
    }

    /** closes the device */
    virtual ham_status_t close() {
      ham_assert(m_is_open);
      m_is_open = false;
      return (HAM_SUCCESS);
    }

    /** flushes the device */
    virtual ham_status_t flush() {
      return (HAM_SUCCESS);
    }

    /** truncate/resize the device */
    virtual ham_status_t truncate(ham_offset_t newsize) {
      return (HAM_SUCCESS);
    }

    /** returns true if the device is open */
    virtual bool is_open() {
      return (m_is_open);
    }

    /** get the current file/storage size */
    virtual ham_status_t get_filesize(ham_offset_t *length) {
      ham_assert(!"this operation is not possible for in-memory-databases");
      return (HAM_NOT_IMPLEMENTED);
    }

    /** seek position in a file */
    virtual ham_status_t seek(ham_offset_t offset, int whence) {
      ham_assert(!"can't seek in an in-memory-device");
      return (HAM_NOT_IMPLEMENTED);
    }

    /** tell the position in a file */
    virtual ham_status_t tell(ham_offset_t *offset) {
      ham_assert(!"can't tell in an in-memory-device");
      return (HAM_NOT_IMPLEMENTED);
    }

    /** reads from the device; this function does not use mmap */
    virtual ham_status_t read(ham_offset_t offset, void *buffer, 
                ham_offset_t size) {
      ham_assert(!"operation is not possible for in-memory-databases");
      return (HAM_NOT_IMPLEMENTED);
    }

    /** writes to the device; this function does not use mmap,
     * and is responsible for writing the data is run through the file 
     * filters */
    virtual ham_status_t write(ham_offset_t offset, void *buffer, 
                ham_offset_t size) {
      ham_assert(!"operation is not possible for in-memory-databases");
      return (HAM_NOT_IMPLEMENTED);
    }

    /** reads a page from the device; this function CAN use mmap */
    virtual ham_status_t read_page(Page *page) {
      ham_assert(!"operation is not possible for in-memory-databases");
      return (HAM_NOT_IMPLEMENTED);
    }

    /** writes a page to the device */
    virtual ham_status_t write_page(Page *page) {
      ham_assert(!"operation is not possible for in-memory-databases");
      return (HAM_NOT_IMPLEMENTED);
    }

    /** allocate storage from this device; this function 
     * will *NOT* use mmap.  */
    virtual ham_status_t alloc(ham_size_t size, ham_offset_t *address) {
      ham_assert(!"can't alloc from an in-memory-device");
      return (HAM_NOT_IMPLEMENTED);
    }

    /**
     * allocate storage for a page from this device; this function 
     * can use mmap if available
     *
     * @note
     * The caller is responsible for flushing the page; the @ref free_page 
     * function will assert that the page is not dirty.
     */
    virtual ham_status_t alloc_page(Page *page);

    /** frees a page on the device; plays counterpoint to @ref alloc_page */
    virtual ham_status_t free_page(Page *page);

  private:
    bool m_is_open;
};


class Device {
  public:
    /** constructor */
    Device(Environment *env, ham_u32_t flags) 
      : m_env(env), m_flags(flags) { 
      if (flags & HAM_IN_MEMORY_DB)
        m_impl = new DeviceImplInMemory(this);
      else
        m_impl = new DeviceImplDisk(this);

      /*
       * initialize the pagesize with a default value - this will be
       * overwritten i.e. by ham_open, ham_create when the pagesize 
       * of the file is known
       */
      set_pagesize(get_pagesize());
    }

    /** virtual destructor */
    ~Device() { 
      ScopedLock lock(m_mutex);
      delete m_impl;
      m_impl = 0;
    }

    /** set the flags */
    void set_flags(ham_u32_t flags) {
      ScopedLock lock(m_mutex);
      m_flags = flags;
    }

    /** get the flags */
    ham_u32_t get_flags() {
      ScopedLock lock(m_mutex);
      return (m_flags);
    }

    /** Create a new device */
    ham_status_t create(const char *filename, ham_u32_t flags, 
                ham_u32_t mode) {
      ScopedLock lock(m_mutex);
      m_flags = flags;
      return (m_impl->create(filename, flags, mode));
    }

    /** opens an existing device */
    ham_status_t open(const char *filename, ham_u32_t flags) {
      ScopedLock lock(m_mutex);
      m_flags = flags;
      return (m_impl->open(filename, flags));
    }

    /** closes the device */
    ham_status_t close() {
      ScopedLock lock(m_mutex);
      return (m_impl->close());
    }

    /** flushes the device */
    ham_status_t flush() {
      ScopedLock lock(m_mutex);
      return (m_impl->flush());
    }

    /** truncate/resize the device */
    ham_status_t truncate(ham_offset_t newsize) {
      ScopedLock lock(m_mutex);
      return (m_impl->truncate(newsize));
    }

    /** returns true if the device is open */
    bool is_open() {
      ScopedLock lock(m_mutex);
      return (m_impl->is_open());
    }

    /** get the current file/storage size */
    ham_status_t get_filesize(ham_offset_t *length) {
      ScopedLock lock(m_mutex);
      return (m_impl->get_filesize(length));
    }

    /** seek position in a file */
    ham_status_t seek(ham_offset_t offset, int whence) {
      ScopedLock lock(m_mutex);
      return (m_impl->seek(offset, whence));
    }

    /** tell the position in a file */
    ham_status_t tell(ham_offset_t *offset) {
      ScopedLock lock(m_mutex);
      return (m_impl->tell(offset));
    }

    /** reads from the device; this function does not use mmap */
    ham_status_t read(ham_offset_t offset, void *buffer, ham_offset_t size) {
      ScopedLock lock(m_mutex);
      return (m_impl->read(offset, buffer, size));
    }

    /** writes to the device; this function does not use mmap,
     * and is responsible for writing the data is run through the file 
     * filters */
    ham_status_t write(ham_offset_t offset, void *buffer, ham_offset_t size) {
      ScopedLock lock(m_mutex);
      return (m_impl->write(offset, buffer, size));
    }

    /** reads a page from the device; this function CAN use mmap */
    ham_status_t read_page(Page *page) {
      ScopedLock lock(m_mutex);
      return (m_impl->read_page(page));
    }

    /** writes a page to the device */
    ham_status_t write_page(Page *page) {
      ScopedLock lock(m_mutex);
      return (m_impl->write_page(page));
    }

    /** allocate storage from this device; this function 
     * will *NOT* use mmap.  */
    ham_status_t alloc(ham_size_t size, ham_offset_t *address) {
      ScopedLock lock(m_mutex);
      return (m_impl->alloc(size, address));
    }

    /**
     * allocate storage for a page from this device; this function 
     * can use mmap if available
     *
     * @note
     * The caller is responsible for flushing the page; the @ref free_page 
     * function will assert that the page is not dirty.
     */
    ham_status_t alloc_page(Page *page) {
      ScopedLock lock(m_mutex);
      return (m_impl->alloc_page(page));
    }

    /** frees a page on the device; plays counterpoint to @ref alloc_page */
    ham_status_t free_page(Page *page) {
      ScopedLock lock(m_mutex);
      return (m_impl->free_page(page));
    }

    /** get the Environment */
    Environment *get_env() {
      ScopedLock lock(m_mutex);
      return (m_env);
    }

    /** set the pagesize for this device */
    void set_pagesize(ham_size_t pagesize) {
      ScopedLock lock(m_mutex);
      m_impl->set_pagesize(pagesize);
    }

    /** get the pagesize for this device */
    ham_size_t get_pagesize() {
      ScopedLock lock(m_mutex);
      return (m_impl->get_pagesize());
    }

  protected:
    friend class DeviceImplDisk;
    friend class DeviceImplInMemory;

    /** a mutex to protect the device */
    Mutex m_mutex;

    /** The actual implementation */
    DeviceImplementation *m_impl;

    /** the environment which employs this device */
    Environment *m_env;

    /** the device flags */
    ham_u32_t m_flags;
};


#endif /* HAM_DEVICE_H__ */
