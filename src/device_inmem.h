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
 

#ifndef HAM_DEVICE_INMEM_H__
#define HAM_DEVICE_INMEM_H__

#include "mem.h"
#include "db.h"
#include "device.h"

namespace hamsterdb {

/*
 * an In-Memory device
 */ 
class InMemoryDevice : public Device {
  public:
    // constructor
    InMemoryDevice(LocalEnvironment *env, ham_u32_t flags)
      : Device(env, flags), m_is_open(false) {
    }

    // Create a new device
    virtual ham_status_t create(const char *filename, ham_u32_t flags,
                ham_u32_t mode) {
      m_flags = flags;
      m_is_open = true;
      return (0);
    }

    // opens an existing device 
    virtual ham_status_t open(const char *filename, ham_u32_t flags) {
      ham_assert(!"can't open an in-memory-device");
      m_flags = flags;
      return (HAM_NOT_IMPLEMENTED);
    }

    // closes the device 
    virtual ham_status_t close() {
      ham_assert(m_is_open);
      m_is_open = false;
      return (HAM_SUCCESS);
    }

    // flushes the device 
    virtual ham_status_t flush() {
      return (HAM_SUCCESS);
    }

    // truncate/resize the device 
    virtual ham_status_t truncate(ham_u64_t newsize) {
      return (HAM_SUCCESS);
    }

    // returns true if the device is open 
    virtual bool is_open() {
      return (m_is_open);
    }

    // get the current file/storage size 
    virtual ham_status_t get_filesize(ham_u64_t *length) {
      ham_assert(!"this operation is not possible for in-memory-databases");
      return (HAM_NOT_IMPLEMENTED);
    }

    // seek position in a file 
    virtual ham_status_t seek(ham_u64_t offset, int whence) {
      ham_assert(!"can't seek in an in-memory-device");
      return (HAM_NOT_IMPLEMENTED);
    }

    // tell the position in a file 
    virtual ham_status_t tell(ham_u64_t *offset) {
      ham_assert(!"can't tell in an in-memory-device");
      return (HAM_NOT_IMPLEMENTED);
    }

    // reads from the device; this function does not use mmap 
    virtual ham_status_t read(ham_u64_t offset, void *buffer,
                ham_u64_t size) {
      ham_assert(!"operation is not possible for in-memory-databases");
      return (HAM_NOT_IMPLEMENTED);
    }

    // writes to the device 
    virtual ham_status_t write(ham_u64_t offset, void *buffer,
                ham_u64_t size) {
      ham_assert(!"operation is not possible for in-memory-databases");
      return (HAM_NOT_IMPLEMENTED);
    }

    virtual ham_status_t writev(ham_u64_t offset, void *buffer1,
                ham_u64_t size1, void *buffer2, ham_u64_t size2) {
      ham_assert(!"operation is not possible for in-memory-databases");
      return (HAM_NOT_IMPLEMENTED);
    }

    // reads a page from the device 
    virtual ham_status_t read_page(Page *page) {
      ham_assert(!"operation is not possible for in-memory-databases");
      return (HAM_NOT_IMPLEMENTED);
    }

    // writes a page to the device 
    virtual ham_status_t write_page(Page *page) {
      return (0);
    }

    // allocate storage from this device; this function
    // will *NOT* use mmap.  
    virtual ham_status_t alloc(ham_size_t size, ham_u64_t *address) {
      ham_assert(!"can't alloc from an in-memory-device");
      return (HAM_NOT_IMPLEMENTED);
    }

    // allocate storage for a page from this device 
    virtual ham_status_t alloc_page(Page *page) {
      ham_assert(page->get_data() == 0);

      ham_u8_t *p = Memory::allocate<ham_u8_t>(m_pagesize);
      if (!p)
        return (HAM_OUT_OF_MEMORY);
      page->set_data((PPageData *)p);
      page->set_flags(page->get_flags() | Page::kNpersMalloc);
      page->set_address((ham_u64_t)PTR_TO_U64(p));
      return (HAM_SUCCESS);
    }

    // frees a page on the device; plays counterpoint to @ref alloc_page 
    virtual void free_page(Page *page) {
      ham_assert(page->get_data() != 0);
      ham_assert(page->get_flags() | Page::kNpersMalloc);

      page->set_flags(page->get_flags() & ~Page::kNpersMalloc);
      Memory::release(page->get_data());
      page->set_data(0);
    }

  private:
    bool m_is_open;
};

} // namespace hamsterdb

#endif /* HAM_DEVICE_INMEM_H__ */
