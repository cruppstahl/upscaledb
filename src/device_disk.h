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
      : Device(env, flags), m_mmapptr(0), m_mapped_size(0) {
    }

    // Create a new device
    virtual void create(const char *filename, ham_u32_t flags, ham_u32_t mode) {
      m_flags = flags;
      m_file.create(filename, flags, mode);
    }

    // opens an existing device
    //
    // tries to map the file; if it fails then continue with read/write 
    virtual void open(const char *filename, ham_u32_t flags) {
      m_flags = flags;
      m_file.open(filename, flags);

      if (m_flags & HAM_DISABLE_MMAP)
        return;

      // the file size which backs the mapped ptr
      ham_u64_t open_filesize = get_file_size();

      // make sure we do not exceed the "real" size of the file, otherwise
      // we run into issues when accessing that memory (at least on windows)
      size_t granularity = File::get_granularity();
      if (open_filesize == 0 || open_filesize % granularity)
        return;

      m_mapped_size = open_filesize;

      m_file.mmap(0, m_mapped_size, (flags & HAM_READ_ONLY) != 0, &m_mmapptr);
    }

    // closes the device
    virtual void close() {
      if (m_mmapptr)
        m_file.munmap(m_mmapptr, m_mapped_size);

      m_file.close();
    }

    // flushes the device
    virtual void flush() {
      m_file.flush();
    }

    // truncate/resize the device
    virtual void truncate(ham_u64_t newsize) {
      m_file.truncate(newsize);
    }

    // returns true if the device is open
    virtual bool is_open() {
      return (m_file.is_open());
    }

    // get the current file/storage size
    virtual ham_u64_t get_file_size() {
      return (m_file.get_file_size());
    }

    // seek to a position in a file
    virtual void seek(ham_u64_t offset, int whence) {
      m_file.seek(offset, whence);
    }

    // tell the position in a file
    virtual ham_u64_t tell() {
      return (m_file.tell());
    }

    // reads from the device; this function does NOT use mmap
    virtual void read(ham_u64_t offset, void *buffer, size_t len) {
      m_file.pread(offset, buffer, len);
    }

    // writes to the device; this function does not use mmap,
    // and is responsible for writing the data is run through the file
    // filters
    virtual void write(ham_u64_t offset, void *buffer, size_t len) {
      m_file.pwrite(offset, buffer, len);
    }

    // reads a page from the device; this function CAN return a
	// pointer to mmapped memory
    virtual void read_page(Page *page, size_t page_size) {
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

      m_file.pread(page->get_address(), page->get_data(), page_size);
    }

    // writes a page to the device
    virtual void write_page(Page *page) {
      write(page->get_address(), page->get_data(), m_env->get_page_size());
    }

    // allocate storage from this device; this function
    // will *NOT* return mmapped memory
    virtual ham_u64_t alloc(size_t len) {
      ham_u64_t address = m_file.get_file_size();
      m_file.truncate(address + len);
      return (address);
    }

    // Allocates storage for a page from this device; this function
    // will *NOT* return mmapped memory
    virtual void alloc_page(Page *page, size_t page_size) {
      ham_u64_t pos = m_file.get_file_size();

      m_file.truncate(pos + page_size);
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
    // the database file
    File m_file;

    // pointer to the the mmapped data
    ham_u8_t *m_mmapptr;

    // the size of m_mmapptr as used in mmap
    ham_u64_t m_mapped_size;

    // dynamic byte array providing temporary space for encryption
    ByteArray m_encryption_buffer;
};

} // namespace hamsterdb

#endif /* HAM_DEVICE_DISK_H__ */
