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

class Device {
  public:
    /** constructor */
    Device(Environment *env, ham_u32_t flags) 
      : m_env(env), m_flags(flags), m_freelist_cache(0) { 
        /*
         * initialize the pagesize with a default value - this will be
         * overwritten i.e. by ham_open, ham_create when the pagesize 
         * of the file is known
         */
        set_pagesize(get_pagesize());
    }

    /** virtual destructor */
    virtual ~Device() { 
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

    /** get the Environment */
    Environment *get_env() {
        return (m_env);
    }

    /** get the pagesize for this device */
    ham_size_t get_pagesize() {
        return (m_pagesize);
    }

    /** set the pagesize for this device */
    void set_pagesize(ham_size_t pagesize) {
        m_pagesize=pagesize;
    }

    /** set the device flags */
    void set_flags(ham_u32_t flags) {
        m_flags=flags;
    }

    /** get the device flags */
    ham_u32_t get_flags() {
        return (m_flags);
    }

    /** set the freelist cache */
    void set_freelist_cache(freelist_cache_t *cache) {
        m_freelist_cache=cache;
    }

    /** get the freelist cache */
    freelist_cache_t *get_freelist_cache() {
        return (m_freelist_cache);
    }

  protected:
    /** the environment which employs this device */
    Environment *m_env;

    /** Flags of this device
     *
     * Currently, these flags are used (at least):
     * - @ref HAM_DISABLE_MMAP do not use mmap but pread/pwrite
     * - @ref DB_USE_MMAP use memory mapped I/O (this bit is 
     *		not observed through here, though)
     * - @ref HAM_READ_ONLY this is a read-only device
     */
    ham_u32_t m_flags;

    /** the pagesize */
    ham_size_t m_pagesize;

    /** the freelist cache is managed by the device */
    freelist_cache_t *m_freelist_cache;
};

/**
 * a File-based device
 */
class FileDevice : public Device {
  public:
    /** constructor */
    FileDevice(Environment *env, ham_u32_t flags) 
      : Device(env, flags), m_fd(HAM_INVALID_FD) {
        m_pagesize=os_get_pagesize();
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
        return (os_open(filename, flags, &m_fd));
    }

    /** closes the device */
    virtual ham_status_t close() {
        ham_status_t st=os_close(m_fd, get_flags());
        if (st==HAM_SUCCESS)
            m_fd=HAM_INVALID_FD;
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
        return (HAM_INVALID_FD!=m_fd);
    }

    /** get the current file/storage size */
    virtual ham_status_t get_filesize(ham_offset_t *length) {
        *length=0;
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
    virtual ham_status_t write_page(Page *page) {
        return (write(page->get_self(), page->get_pers(), get_pagesize()));
    }

    /** allocate storage from this device; this function 
     * will *NOT* use mmap.  */
    virtual ham_status_t alloc(ham_size_t size, ham_offset_t *address) {
        ham_status_t st=os_get_filesize(m_fd, address);
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
    virtual ham_status_t alloc_page(Page *page) {
        ham_status_t st;
        ham_offset_t pos;
        ham_size_t size=get_pagesize();

        st=os_get_filesize(m_fd, &pos);
        if (st)
            return (st);

	    st=os_truncate(m_fd, pos+size);
        if (st)
            return (st);

        page->set_self(pos);
        return (FileDevice::read_page(page));
    }


    /** frees a page on the device; plays counterpoint to @ref alloc_page */
    virtual ham_status_t free_page(Page *page);

  private:
    ham_fd_t m_fd;
};

/**
 * an In-Memory device
 */
class InMemoryDevice : public Device {
  public:
    /** constructor */
    InMemoryDevice(Environment *env, ham_u32_t flags) 
      : Device(env, flags), m_is_open(false) {
        m_pagesize=1024*4;
    }

    /** Create a new device */
    virtual ham_status_t create(const char *filename, ham_u32_t flags, 
                ham_u32_t mode) {
        m_is_open=true;
        return (0);
    }

    /** opens an existing device */
    virtual ham_status_t open(const char *filename, ham_u32_t flags) {
        ham_assert(!"can't open an in-memory-device", (0));
        return (HAM_NOT_IMPLEMENTED);
    }

    /** closes the device */
    virtual ham_status_t close() {
        ham_assert(m_is_open, (0));
        m_is_open=false;
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
        ham_assert(!"this operation is not possible for in-memory-databases", 
                    (0));
        return (HAM_NOT_IMPLEMENTED);
    }

    /** seek position in a file */
    virtual ham_status_t seek(ham_offset_t offset, int whence) {
        ham_assert(!"can't seek in an in-memory-device", (0));
        return (HAM_NOT_IMPLEMENTED);
    }

    /** tell the position in a file */
    virtual ham_status_t tell(ham_offset_t *offset) {
        ham_assert(!"can't tell in an in-memory-device", (0));
        return (HAM_NOT_IMPLEMENTED);
    }

    /** reads from the device; this function does not use mmap */
    virtual ham_status_t read(ham_offset_t offset, void *buffer, 
                ham_offset_t size) {
        ham_assert(!"operation is not possible for in-memory-databases", (0));
        return (HAM_NOT_IMPLEMENTED);
    }

    /** writes to the device; this function does not use mmap,
     * and is responsible for writing the data is run through the file 
     * filters */
    virtual ham_status_t write(ham_offset_t offset, void *buffer, 
                ham_offset_t size) {
        ham_assert(!"operation is not possible for in-memory-databases", (0));
        return (HAM_NOT_IMPLEMENTED);
    }

    /** reads a page from the device; this function CAN use mmap */
    virtual ham_status_t read_page(Page *page) {
        ham_assert(!"operation is not possible for in-memory-databases", (0));
        return (HAM_NOT_IMPLEMENTED);
    }

    /** writes a page to the device */
    virtual ham_status_t write_page(Page *page) {
        ham_assert(!"operation is not possible for in-memory-databases", (0));
        return (HAM_NOT_IMPLEMENTED);
    }

    /** allocate storage from this device; this function 
     * will *NOT* use mmap.  */
    virtual ham_status_t alloc(ham_size_t size, ham_offset_t *address) {
        ham_assert(!"can't alloc from an in-memory-device", (0));
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
    virtual ham_status_t alloc_page(Page *page) {
        ham_u8_t *buffer;
        ham_size_t size=get_pagesize();

        ham_assert(page->get_pers()==0, (0));

        buffer=(ham_u8_t *)m_env->get_allocator()->alloc(size);
        if (!buffer)
            return (HAM_OUT_OF_MEMORY);
        page->set_pers((page_data_t *)buffer);
        page->set_flags(page->get_flags()|Page::NPERS_MALLOC);
        page->set_self((ham_offset_t)PTR_TO_U64(buffer));

        return (HAM_SUCCESS);
    }


    /** frees a page on the device; plays counterpoint to @ref alloc_page */
    virtual ham_status_t free_page(Page *page) {
        ham_assert(page->get_pers()!=0, (0));
        ham_assert(page->get_flags()|Page::NPERS_MALLOC, (0));

        m_env->get_allocator()->free(page->get_pers());
        page->set_pers(0);
        page->set_flags(page->get_flags()&~Page::NPERS_MALLOC);

        return (HAM_SUCCESS);
    }


  private:
    bool m_is_open;
};


#endif /* HAM_DEVICE_H__ */
