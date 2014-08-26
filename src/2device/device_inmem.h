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

#ifndef HAM_DEVICE_INMEM_H
#define HAM_DEVICE_INMEM_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "1mem/mem.h"
#include "2device/device.h"
#include "2page/page.h"
#include "4db/db.h"
#include "4env/env_local.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

/*
 * an In-Memory device
 */ 
class InMemoryDevice : public Device {
  public:
    // constructor
    InMemoryDevice(LocalEnvironment *env, ham_u32_t flags,
                    ham_u64_t file_size_limit)
      : Device(env, flags, file_size_limit), m_is_open(false), m_file_size(0) {
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
    virtual void read(ham_u64_t offset, void *buffer, size_t len) {
      ham_assert(!"operation is not possible for in-memory-databases");
      throw Exception(HAM_NOT_IMPLEMENTED);
    }

    // writes to the device 
    virtual void write(ham_u64_t offset, void *buffer, size_t len) {
      ham_assert(!"operation is not possible for in-memory-databases");
      throw Exception(HAM_NOT_IMPLEMENTED);
    }

    // reads a page from the device 
    virtual void read_page(Page *page, size_t page_size) {
      ham_assert(!"operation is not possible for in-memory-databases");
      throw Exception(HAM_NOT_IMPLEMENTED);
    }

    // writes a page to the device 
    virtual void write_page(Page *page) {
    }

    // allocate storage from this device; this function
    // will *NOT* use mmap.  
    virtual ham_u64_t alloc(size_t size) {
      m_file_size += size;
      if (m_file_size > m_file_size_limit)
        throw Exception(HAM_LIMITS_REACHED);

      return ((ham_u64_t)Memory::allocate<ham_u8_t>(size));
    }

    // allocate storage for a page from this device 
    virtual void alloc_page(Page *page, size_t page_size) {
      ham_assert(page->get_data() == 0);

      m_file_size += page_size;
      if (m_file_size > m_file_size_limit)
        throw Exception(HAM_LIMITS_REACHED);

      ham_u8_t *p = Memory::allocate<ham_u8_t>(page_size);
      page->set_data((PPageData *)p);
      page->set_flags(page->get_flags() | Page::kNpersMalloc);
      page->set_address((ham_u64_t)PTR_TO_U64(p));
    }

    // frees a page on the device; plays counterpoint to @ref alloc_page 
    virtual void free_page(Page *page) {
      ham_assert(page->get_data() != 0);
      ham_assert(page->get_flags() & Page::kNpersMalloc);

      ham_assert(m_file_size >= m_env->get_page_size());
      m_file_size -= m_env->get_page_size();

      page->set_flags(page->get_flags() & ~Page::kNpersMalloc);
      Memory::release(page->get_data());
      page->set_data(0);
    }

    // releases a chunk of memory previously allocated with alloc()
    void release(void *ptr, size_t size) {
      ham_assert(m_file_size >= size);
      m_file_size -= size;
      Memory::release(ptr);
    }

  private:
    // flag whether this device was "opened" or is uninitialized
    bool m_is_open;

    // the allocated bytes
    ham_u64_t m_file_size;
};

} // namespace hamsterdb

#endif /* HAM_DEVICE_INMEM_H */
