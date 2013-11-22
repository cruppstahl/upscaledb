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
 

#ifndef HAM_DEVICE_DISK_H__
#define HAM_DEVICE_DISK_H__

#include "os.h"
#include "mem.h"
#include "db.h"
#include "device.h"
#include "env_local.h"
#ifdef HAM_ENABLE_ENCRYPTION
#  include "aes.h"
#endif

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
    virtual ham_status_t create(const char *filename, ham_u32_t flags,
                ham_u32_t mode) {
      m_flags = flags;
      return (os_create(filename, flags, mode, &m_fd));
    }

    // opens an existing device
    //
    // tries to map the file; if it fails then continue with read/write 
    virtual ham_status_t open(const char *filename, ham_u32_t flags) {
      m_flags = flags;
      ham_status_t st = os_open(filename, flags, &m_fd);
      if (st)
        return (st);

      if (m_flags & HAM_DISABLE_MMAP)
        return (0);

      // the file size which backs the mapped ptr
      ham_u64_t open_filesize;

      st = get_filesize(&open_filesize);
      if (st)
        return (st);

      // make sure we do not exceed the "real" size of the file, otherwise
      // we run into issues when accessing that memory (at least on windows)
      ham_u32_t granularity = os_get_granularity();
      if (open_filesize == 0 || open_filesize % granularity)
        return (0);

      m_mapped_size = open_filesize;

      return (os_mmap(m_fd, &m_win32mmap, 0, m_mapped_size,
                    (flags & HAM_READ_ONLY) != 0, &m_mmapptr));
    }

    // closes the device
    virtual ham_status_t close() {
      if (m_mmapptr)
        (void)os_munmap(&m_win32mmap, m_mmapptr, m_mapped_size);

      ham_status_t st = os_close(m_fd);
      if (st == HAM_SUCCESS)
        m_fd = HAM_INVALID_FD;
      return (st);
    }

    // flushes the device
    virtual ham_status_t flush() {
      return (os_flush(m_fd));
    }

    // truncate/resize the device
    virtual ham_status_t truncate(ham_u64_t newsize) {
      return (os_truncate(m_fd, newsize));
    }

    // returns true if the device is open
    virtual bool is_open() {
      return (HAM_INVALID_FD != m_fd);
    }

    // get the current file/storage size
    virtual ham_status_t get_filesize(ham_u64_t *length) {
      *length = 0;
      return (os_get_filesize(m_fd, length));
    }

    // seek to a position in a file
    virtual ham_status_t seek(ham_u64_t offset, int whence) {
      return (os_seek(m_fd, offset, whence));
    }

    // tell the position in a file
    virtual ham_status_t tell(ham_u64_t *offset) {
      return (os_tell(m_fd, offset));
    }

    // reads from the device; this function does NOT use mmap
    virtual ham_status_t read(ham_u64_t offset, void *buffer,
                ham_u64_t size) {
      ham_status_t st = os_pread(m_fd, offset, buffer, size);
#ifdef HAM_ENABLE_ENCRYPTION
      if (m_env->is_encryption_enabled()) {
        AesCipher aes(m_env->get_encryption_key(), offset);
        aes.decrypt((ham_u8_t *)buffer, (ham_u8_t *)buffer, size);
      }
#endif
      return (st);
    }

    // writes to the device; this function does not use mmap,
    // and is responsible for writing the data is run through the file
    // filters
    virtual ham_status_t write(ham_u64_t offset, void *buffer,
                ham_u64_t size) {
#ifdef HAM_ENABLE_ENCRYPTION
      if (m_env->is_encryption_enabled()) {
        // encryption disables direct I/O -> only full pages are allowed
        ham_assert(size == m_page_size);
        ham_assert(offset % m_page_size == 0);

        m_encryption_buffer.resize(size);
        AesCipher aes(m_env->get_encryption_key(), offset);
        aes.encrypt((ham_u8_t *)buffer,
                        (ham_u8_t *)m_encryption_buffer.get_ptr(), size);
        buffer = m_encryption_buffer.get_ptr();
      }
#endif
      return (os_pwrite(m_fd, offset, buffer, size));
    }

    // writes to the device; this function does not use mmap
    virtual ham_status_t writev(ham_u64_t offset, void *buffer1,
                ham_u64_t size1, void *buffer2, ham_u64_t size2) {
      ham_status_t st = seek(offset, HAM_OS_SEEK_SET);
      if (st)
        return (st);
      return (os_writev(m_fd, buffer1, size1, buffer2, size2));
    }

    // reads a page from the device; this function CAN return a
	// pointer to mmapped memory
    virtual ham_status_t read_page(Page *page) {
      // if this page is in the mapped area: return a pointer into that area.
      // otherwise fall back to read/write.
      if (page->get_address() < m_mapped_size && m_mmapptr != 0) {
        // ok, this page is mapped. If the Page object has a memory buffer:
        // free it
        ham_assert(m_env->is_encryption_enabled() == false);
        Memory::release(page->get_data());
        page->set_flags(page->get_flags() & ~Page::kNpersMalloc);
        page->set_data((PPageData *)&m_mmapptr[page->get_address()]);
        return (0);
      }

      // this page is not in the mapped area; allocate a buffer
      if (page->get_data() == 0) {
        ham_u8_t *p = Memory::allocate<ham_u8_t>(m_page_size);
        if (!p)
          return (HAM_OUT_OF_MEMORY);
        page->set_data((PPageData *)p);
        page->set_flags(page->get_flags() | Page::kNpersMalloc);
      }

      ham_status_t st = os_pread(m_fd, page->get_address(), page->get_data(),
                      m_page_size);
      if (st == 0) {
#ifdef HAM_ENABLE_ENCRYPTION
        if (m_env->is_encryption_enabled()) {
          AesCipher aes(m_env->get_encryption_key(), page->get_address());
          aes.decrypt((ham_u8_t *)page->get_data(),
                          (ham_u8_t *)page->get_data(), m_page_size);
        }
#endif
        return (0);
      }

      Memory::release(page->get_data());
      page->set_data((PPageData *)0);
      return (st);
    }

    // writes a page to the device
    virtual ham_status_t write_page(Page *page) {
      return (write(page->get_address(), page->get_data(), m_page_size));
    }

    // allocate storage from this device; this function
    // will *NOT* return mmapped memory
    virtual ham_status_t alloc(ham_u32_t size, ham_u64_t *address) {
      ham_status_t st = os_get_filesize(m_fd, address);
      if (st)
        return (st);
      return (os_truncate(m_fd, (*address) + size));
    }

    // Allocates storage for a page from this device; this function
    // will *NOT* return mmapped memory
    virtual ham_status_t alloc_page(Page *page) {
      ham_u64_t pos;
      ham_u32_t size = m_page_size;

      ham_status_t st = os_get_filesize(m_fd, &pos);
      if (st)
        return (st);

      st = os_truncate(m_fd, pos + size);
      if (st)
        return (st);

      page->set_address(pos);
      return (read_page(page));
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
