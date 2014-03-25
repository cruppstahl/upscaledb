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

#ifndef HAM_DEVICE_DISK_H__
#define HAM_DEVICE_DISK_H__

#include "os.h"
#include "mem.h"
#include "db.h"
#include "device.h"
#include "env_local.h"

namespace hamsterdb {

/*
 * a File-based device
 */
class DiskDevice : public Device {
  public:
    DiskDevice(LocalEnvironment *env, ham_u32_t flags)
      : Device(env, flags), m_fd(HAM_INVALID_FD), m_win32mmap(HAM_INVALID_FD),
        m_mmapptr(0), m_mapped_size(0) {
    }

    // Create a new device
    virtual void create(const char *filename, ham_u32_t flags, ham_u32_t mode) {
      m_flags = flags;
      m_fd = os_create(filename, flags, mode);
    }

    // opens an existing device
    //
    // tries to map the file; if it fails then continue with read/write 
    virtual void open(const char *filename, ham_u32_t flags) {
      m_flags = flags;
      m_fd = os_open(filename, flags);

      if (m_flags & HAM_DISABLE_MMAP)
        return;

      // the file size which backs the mapped ptr
      ham_u64_t open_filesize = get_file_size();

      // make sure we do not exceed the "real" size of the file, otherwise
      // we run into issues when accessing that memory (at least on windows)
      ham_u32_t granularity = os_get_granularity();
      if (open_filesize == 0 || open_filesize % granularity)
        return;

      m_mapped_size = open_filesize;

      os_mmap(m_fd, &m_win32mmap, 0, m_mapped_size,
                    (flags & HAM_READ_ONLY) != 0, &m_mmapptr);
    }

    // closes the device
    virtual void close() {
      if (m_mmapptr)
        os_munmap(&m_win32mmap, m_mmapptr, m_mapped_size);

      os_close(m_fd);
      m_fd = HAM_INVALID_FD;
    }

    // flushes the device
    virtual void flush() {
      os_flush(m_fd);
    }

    // truncate/resize the device
    virtual void truncate(ham_u64_t newsize) {
      os_truncate(m_fd, newsize);
    }

    // returns true if the device is open
    virtual bool is_open() {
      return (HAM_INVALID_FD != m_fd);
    }

    // get the current file/storage size
    virtual ham_u64_t get_file_size() {
      return (os_get_file_size(m_fd));
    }

    // seek to a position in a file
    virtual void seek(ham_u64_t offset, int whence) {
      os_seek(m_fd, offset, whence);
    }

    // tell the position in a file
    virtual ham_u64_t tell() {
      return (os_tell(m_fd));
    }

    // reads from the device; this function does NOT use mmap
    virtual void read(ham_u64_t offset, void *buffer, ham_u64_t size) {
      os_pread(m_fd, offset, buffer, size);
    }

    // writes to the device; this function does not use mmap,
    // and is responsible for writing the data is run through the file
    // filters
    virtual void write(ham_u64_t offset, void *buffer, ham_u64_t size) {
      os_pwrite(m_fd, offset, buffer, size);
    }

    // reads a page from the device; this function CAN return a
	// pointer to mmapped memory
    virtual void read_page(Page *page, ham_u32_t page_size) {
      // if this page is in the mapped area: return a pointer into that area.
      // otherwise fall back to read/write.
      if (page->get_address() < m_mapped_size && m_mmapptr != 0) {
        // ok, this page is mapped. If the Page object has a memory buffer:
        // free it
        ham_assert(m_env->is_encryption_enabled() == false);
        Memory::release(page->get_data());
        page->set_flags(page->get_flags() & ~Page::kNpersMalloc);
        page->set_data((PPageData *)&m_mmapptr[page->get_address()]);
        return;
      }

      // this page is not in the mapped area; allocate a buffer
      if (page->get_data() == 0) {
        ham_u8_t *p = Memory::allocate<ham_u8_t>(page_size);
        page->set_data((PPageData *)p);
        page->set_flags(page->get_flags() | Page::kNpersMalloc);
      }

      os_pread(m_fd, page->get_address(), page->get_data(), page_size);
    }

    // writes a page to the device
    virtual void write_page(Page *page) {
      write(page->get_address(), page->get_data(), m_env->get_page_size());
    }

    // allocate storage from this device; this function
    // will *NOT* return mmapped memory
    virtual ham_u64_t alloc(ham_u32_t size) {
      ham_u64_t address = os_get_file_size(m_fd);
      os_truncate(m_fd, address + size);
      return (address);
    }

    // Allocates storage for a page from this device; this function
    // will *NOT* return mmapped memory
    virtual void alloc_page(Page *page, ham_u32_t page_size) {
      ham_u64_t pos = os_get_file_size(m_fd);

      os_truncate(m_fd, pos + page_size);
      page->set_address(pos);
      read_page(page, page_size);
    }

    // Frees a page on the device; plays counterpoint to |ref alloc_page|
    virtual void free_page(Page *page) {
      if (page->get_data() && page->get_flags() & Page::kNpersMalloc) {
        Memory::release(page->get_data());
        page->set_flags(page->get_flags() & ~Page::kNpersMalloc);
      }
      page->set_data(0);
    }

  private:
    // the file handle
    ham_fd_t m_fd;

    // the win32 mmap handle
    ham_fd_t m_win32mmap;

    // pointer to the the mmapped data
    ham_u8_t *m_mmapptr;

    // the size of m_mmapptr as used in os_mmap
    ham_u64_t m_mapped_size;

    // dynamic byte array providing temporary space for encryption
    ByteArray m_encryption_buffer;
};

} // namespace hamsterdb

#endif /* HAM_DEVICE_DISK_H__ */
