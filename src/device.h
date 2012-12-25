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

namespace hamsterdb {

class Page;

class Device {
  public:
    /** constructor */
    Device(Environment *env, ham_u32_t flags)
      : m_env(env), m_flags(flags) {
      /*
       * initialize the pagesize with a default value - this will be
       * overwritten i.e. by ham_env_open, ham_env_create when the pagesize
       * of the file is known
       */
      set_pagesize(get_pagesize());
    }

    /** virtual destructor */
    virtual ~Device() {
    }

    /** set the flags */
    void set_flags(ham_u32_t flags) {
      m_flags = flags;
    }

    /** get the flags */
    ham_u32_t get_flags() {
      return (m_flags);
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
    virtual ham_status_t truncate(ham_u64_t newsize) = 0;

    /** returns true if the device is open */
    virtual bool is_open() = 0;

    /** get the current file/storage size */
    virtual ham_status_t get_filesize(ham_u64_t *length) = 0;

    /** seek position in a file */
    virtual ham_status_t seek(ham_u64_t offset, int whence) = 0;

    /** tell the position in a file */
    virtual ham_status_t tell(ham_u64_t *offset) = 0;

    /** reads from the device; this function does not use mmap */
    virtual ham_status_t read(ham_u64_t offset, void *buffer,
                ham_u64_t size) = 0;

    /** writes to the device; this function does not use mmap */
    virtual ham_status_t write(ham_u64_t offset, void *buffer,
                ham_u64_t size) = 0;

    /** reads a page from the device; this function CAN use mmap */
    virtual ham_status_t read_page(Page *page) = 0;

    /** writes a page to the device */
    virtual ham_status_t write_page(Page *page) = 0;

    /** allocate storage from this device; this function
     * will *NOT* use mmap.  */
    virtual ham_status_t alloc(ham_size_t size, ham_u64_t *address) = 0;

    /**
     * allocate storage for a page from this device; this function
     * can use mmap if available
     */
    virtual ham_status_t alloc_page(Page *page) = 0;

    /** frees a page on the device
     * @note
     * The caller is responsible for flushing the page; the @ref free_page
     * function will assert that the page is not dirty.
     */
    virtual void free_page(Page *page) = 0;

    /** get the Environment */
    Environment *get_env() {
      return (m_env);
    }

    /** set the pagesize for this device */
    void set_pagesize(ham_size_t pagesize) {
      m_pagesize = pagesize;
    }

    /** get the pagesize for this device */
    ham_size_t get_pagesize() {
      return (m_pagesize);
    }

  protected:
    /** the environment which employs this device */
    Environment *m_env;

    /** the device flags */
    ham_u32_t m_flags;

    /** the page size */
    ham_size_t m_pagesize;
};

/**
 * a File-based device
 */
class DiskDevice : public Device {
  public:
    DiskDevice(Environment *env, ham_u32_t flags)
      : Device(env, flags), m_fd(HAM_INVALID_FD),
        m_win32mmap(HAM_INVALID_FD), m_mmapptr(0),
        m_open_filesize(0), m_mapped_size(0) {
    }

    /** Create a new device */
    virtual ham_status_t create(const char *filename, ham_u32_t flags,
                ham_u32_t mode) {
      set_flags(flags);
      return (os_create(filename, flags, mode, &m_fd));
    }

    /** opens an existing device */
    virtual ham_status_t open(const char *filename, ham_u32_t flags) {
      set_flags(flags);
      ham_status_t st = os_open(filename, flags, &m_fd);
      if (st)
        return (st);

      if (m_flags & HAM_DISABLE_MMAP)
        return (0);

      /* try mmap; if it fails then continue with read/write */
      st = get_filesize(&m_open_filesize);
      if (st)
        return (st);

      /* align the filesize */
      ham_size_t granularity = os_get_granularity();
      if (m_open_filesize % granularity)
        m_mapped_size = (m_open_filesize / granularity) * (granularity + 1);
      else
        m_mapped_size = m_open_filesize;

      return (os_mmap(m_fd, &m_win32mmap, 0, m_mapped_size,
                    flags & HAM_READ_ONLY, &m_mmapptr));
    }

    /** closes the device */
    virtual ham_status_t close() {
      if (m_mmapptr)
        (void)os_munmap(&m_win32mmap, m_mmapptr, m_mapped_size);

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
    virtual ham_status_t truncate(ham_u64_t newsize) {
      return (os_truncate(m_fd, newsize));
    }

    /** returns true if the device is open */
    virtual bool is_open() {
      return (HAM_INVALID_FD != m_fd);
    }

    /** get the current file/storage size */
    virtual ham_status_t get_filesize(ham_u64_t *length) {
      *length = 0;
      return (os_get_filesize(m_fd, length));
    }

    /** seek position in a file */
    virtual ham_status_t seek(ham_u64_t offset, int whence) {
      return (os_seek(m_fd, offset, whence));
    }

    /** tell the position in a file */
    virtual ham_status_t tell(ham_u64_t *offset) {
      return (os_tell(m_fd, offset));
    }

    /** reads from the device; this function does not use mmap */
    virtual ham_status_t read(ham_u64_t offset, void *buffer,
                ham_u64_t size) {
      return (os_pread(m_fd, offset, buffer, size));
    }

    /** writes to the device; this function does not use mmap,
     * and is responsible for writing the data is run through the file
     * filters */
    virtual ham_status_t write(ham_u64_t offset, void *buffer,
                ham_u64_t size) {
      return (os_pwrite(m_fd, offset, buffer, size));
    }

    /** reads a page from the device; this function CAN use mmap */
    virtual ham_status_t read_page(Page *page);

    /** writes a page to the device */
    virtual ham_status_t write_page(Page *page) {
      return (write(page->get_self(), page->get_pers(), m_pagesize));
    }

    /** allocate storage from this device; this function
     * will *NOT* use mmap.  */
    virtual ham_status_t alloc(ham_size_t size, ham_u64_t *address) {
      ham_status_t st = os_get_filesize(m_fd, address);
      if (st)
        return (st);
      return (os_truncate(m_fd, (*address) + size));
    }

    /**
     * allocate storage for a page from this device; this function
     * can use mmap if available
     */
    virtual ham_status_t alloc_page(Page *page);

    /** frees a page on the device; plays counterpoint to @ref alloc_page */
    virtual void free_page(Page *page) {
      if (page->get_pers() && page->get_flags() & Page::NPERS_MALLOC) {
        get_env()->get_allocator()->free(page->get_pers());
        page->set_flags(page->get_flags() & ~Page::NPERS_MALLOC);
      }
      page->set_pers(0);
    }

  private:
    /** the file handle */
    ham_fd_t m_fd;

    /** the win32 mmap handle */
    ham_fd_t m_win32mmap;

    /** the mmapped data */
    ham_u8_t *m_mmapptr;

    /** the file size which backs the mapped ptr */
    ham_u64_t m_open_filesize;

    /** the size of m_mmapptr as used in os_mmap */
    ham_u64_t m_mapped_size;
};


/**
 * an In-Memory device
 */
class InMemoryDevice : public Device {
  public:
    /** constructor */
    InMemoryDevice(Environment *env, ham_u32_t flags)
      : Device(env, flags), m_is_open(false) {
    }

    /** Create a new device */
    virtual ham_status_t create(const char *filename, ham_u32_t flags,
                ham_u32_t mode) {
      set_flags(flags);
      m_is_open = true;
      return (0);
    }

    /** opens an existing device */
    virtual ham_status_t open(const char *filename, ham_u32_t flags) {
      ham_assert(!"can't open an in-memory-device");
      set_flags(flags);
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
    virtual ham_status_t truncate(ham_u64_t newsize) {
      return (HAM_SUCCESS);
    }

    /** returns true if the device is open */
    virtual bool is_open() {
      return (m_is_open);
    }

    /** get the current file/storage size */
    virtual ham_status_t get_filesize(ham_u64_t *length) {
      ham_assert(!"this operation is not possible for in-memory-databases");
      return (HAM_NOT_IMPLEMENTED);
    }

    /** seek position in a file */
    virtual ham_status_t seek(ham_u64_t offset, int whence) {
      ham_assert(!"can't seek in an in-memory-device");
      return (HAM_NOT_IMPLEMENTED);
    }

    /** tell the position in a file */
    virtual ham_status_t tell(ham_u64_t *offset) {
      ham_assert(!"can't tell in an in-memory-device");
      return (HAM_NOT_IMPLEMENTED);
    }

    /** reads from the device; this function does not use mmap */
    virtual ham_status_t read(ham_u64_t offset, void *buffer,
                ham_u64_t size) {
      ham_assert(!"operation is not possible for in-memory-databases");
      return (HAM_NOT_IMPLEMENTED);
    }

    /** writes to the device */
    virtual ham_status_t write(ham_u64_t offset, void *buffer,
                ham_u64_t size) {
      ham_assert(!"operation is not possible for in-memory-databases");
      return (HAM_NOT_IMPLEMENTED);
    }

    /** reads a page from the device */
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
    virtual ham_status_t alloc(ham_size_t size, ham_u64_t *address) {
      ham_assert(!"can't alloc from an in-memory-device");
      return (HAM_NOT_IMPLEMENTED);
    }

    /** allocate storage for a page from this device */
    virtual ham_status_t alloc_page(Page *page) {
      ham_assert(page->get_pers() == 0);

      ham_u8_t *p = (ham_u8_t *)get_env()->get_allocator()->alloc(m_pagesize);
      if (!p)
          return (HAM_OUT_OF_MEMORY);
      page->set_pers((PageData *)p);
      page->set_flags(page->get_flags() | Page::NPERS_MALLOC);
      page->set_self((ham_u64_t)PTR_TO_U64(p));
      return (HAM_SUCCESS);
    }

    /** frees a page on the device; plays counterpoint to @ref alloc_page */
    virtual void free_page(Page *page) {
      ham_assert(page->get_pers() != 0);
      ham_assert(page->get_flags() | Page::NPERS_MALLOC);

      page->set_flags(page->get_flags() & ~Page::NPERS_MALLOC);
      get_env()->get_allocator()->free(page->get_pers());
      page->set_pers(0);
    }

  private:
    bool m_is_open;
};

} // namespace hamsterdb

#endif /* HAM_DEVICE_H__ */
