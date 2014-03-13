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
    virtual void create(const char *filename, ham_u32_t flags, ham_u32_t mode) {
      m_flags = flags;
      m_is_open = true;
    }

    // opens an existing device 
    virtual void open(const char *filename, ham_u32_t flags) {
      ham_assert(!"can't open an in-memory-device");
      m_flags = flags;
      throw Exception(HAM_NOT_IMPLEMENTED);
    }

    // closes the device 
    virtual void close() {
      ham_assert(m_is_open);
      m_is_open = false;
    }

    // flushes the device 
    virtual void flush() {
    }

    // truncate/resize the device 
    virtual void truncate(ham_u64_t newsize) {
    }

    // returns true if the device is open 
    virtual bool is_open() {
      return (m_is_open);
    }

    // get the current file/storage size 
    virtual ham_u64_t get_file_size() {
      ham_assert(!"this operation is not possible for in-memory-databases");
      throw Exception(HAM_NOT_IMPLEMENTED);
    }

    // seek position in a file 
    virtual void seek(ham_u64_t offset, int whence) {
      ham_assert(!"can't seek in an in-memory-device");
      throw Exception(HAM_NOT_IMPLEMENTED);
    }

    // tell the position in a file 
    virtual ham_u64_t tell() {
      ham_assert(!"can't tell in an in-memory-device");
      throw Exception(HAM_NOT_IMPLEMENTED);
    }

    // reads from the device; this function does not use mmap 
    virtual void read(ham_u64_t offset, void *buffer, ham_u64_t size) {
      ham_assert(!"operation is not possible for in-memory-databases");
      throw Exception(HAM_NOT_IMPLEMENTED);
    }

    // writes to the device 
    virtual void write(ham_u64_t offset, void *buffer, ham_u64_t size) {
      ham_assert(!"operation is not possible for in-memory-databases");
      throw Exception(HAM_NOT_IMPLEMENTED);
    }

    // reads a page from the device 
    virtual void read_page(Page *page, ham_u32_t page_size) {
      ham_assert(!"operation is not possible for in-memory-databases");
      throw Exception(HAM_NOT_IMPLEMENTED);
    }

    // writes a page to the device 
    virtual void write_page(Page *page) {
    }

    // allocate storage from this device; this function
    // will *NOT* use mmap.  
    virtual ham_u64_t alloc(ham_u32_t size) {
      ham_assert(!"can't alloc from an in-memory-device");
      throw Exception(HAM_NOT_IMPLEMENTED);
    }

    // allocate storage for a page from this device 
    virtual void alloc_page(Page *page, ham_u32_t page_size) {
      ham_assert(page->get_data() == 0);

      ham_u8_t *p = Memory::allocate<ham_u8_t>(page_size);
      page->set_data((PPageData *)p);
      page->set_flags(page->get_flags() | Page::kNpersMalloc);
      page->set_address((ham_u64_t)PTR_TO_U64(p));
    }

    // frees a page on the device; plays counterpoint to @ref alloc_page 
    virtual void free_page(Page *page) {
      ham_assert(page->get_data() != 0);
      ham_assert(page->get_flags() & Page::kNpersMalloc);

      page->set_flags(page->get_flags() & ~Page::kNpersMalloc);
      Memory::release(page->get_data());
      page->set_data(0);
    }

  private:
    bool m_is_open;
};

} // namespace hamsterdb

#endif /* HAM_DEVICE_INMEM_H__ */
