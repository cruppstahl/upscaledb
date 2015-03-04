/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

/*
 * Device-implementation for disk-based files. Exception safety is "strong"
 * for most operations, but currently it's possible that the Page is modified
 * if DiskDevice::read_page fails in the middle.
 *
 * @exception_safe: basic/strong
 * @thread_safe: no
 */

#ifndef HAM_DEVICE_DISK_H
#define HAM_DEVICE_DISK_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "1os/file.h"
#include "1mem/mem.h"
#include "2device/device.h"
#include "2page/page.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

/*
 * a File-based device
 */
class DiskDevice : public Device {
    struct State {
      // the database file
      File file;

      // pointer to the the mmapped data
      uint8_t *mmapptr;

      // the size of mmapptr as used in mmap
      uint64_t mapped_size;

      // the (cached) size of the file
      uint64_t file_size;
    };

  public:
    DiskDevice(const EnvironmentConfiguration &config)
      : Device(config) {
      State state;
      state.mmapptr = 0;
      state.mapped_size = 0;
      state.file_size = 0;
      std::swap(m_state, state);
    }

    // Create a new device
    virtual void create() {
      File file;
      file.create(m_config.filename.c_str(), m_config.file_mode);
      file.set_posix_advice(m_config.posix_advice);
      m_state.file = file;
    }

    // opens an existing device
    //
    // tries to map the file; if it fails then continue with read/write 
    virtual void open() {
      bool read_only = (m_config.flags & HAM_READ_ONLY) != 0;

      State state = m_state;
      state.file.open(m_config.filename.c_str(), read_only);
      state.file.set_posix_advice(m_config.posix_advice);

      // the file size which backs the mapped ptr
      state.file_size = state.file.get_file_size();

      if (m_config.flags & HAM_DISABLE_MMAP) {
        std::swap(m_state, state);
        return;
      }

      // make sure we do not exceed the "real" size of the file, otherwise
      // we crash when accessing memory which exceeds the mapping (at least
      // on Win32)
      size_t granularity = File::get_granularity();
      if (state.file_size == 0 || state.file_size % granularity) {
        std::swap(m_state, state);
        return;
      }

      state.mapped_size = state.file_size;
      state.file.mmap(0, state.mapped_size, read_only, &state.mmapptr);
      std::swap(m_state, state);
    }

    // returns true if the device is open
    virtual bool is_open() {
      return (m_state.file.is_open());
    }

    // closes the device
    virtual void close() {
      State state = m_state;
      if (state.mmapptr)
        state.file.munmap(state.mmapptr, state.mapped_size);
      state.file.close();

      std::swap(m_state, state);
    }

    // flushes the device
    virtual void flush() {
      m_state.file.flush();
    }

    // truncate/resize the device
    virtual void truncate(uint64_t new_file_size) {
      if (new_file_size > m_config.file_size_limit_bytes)
        throw Exception(HAM_LIMITS_REACHED);
      m_state.file.truncate(new_file_size);
      m_state.file_size = new_file_size;
    }

    // get the current file/storage size
    virtual uint64_t file_size() {
      ham_assert(m_state.file_size == m_state.file.get_file_size());
      return (m_state.file_size);
    }

    // seek to a position in a file
    virtual void seek(uint64_t offset, int whence) {
      m_state.file.seek(offset, whence);
    }

    // tell the position in a file
    virtual uint64_t tell() {
      return (m_state.file.tell());
    }

    // reads from the device; this function does NOT use mmap
    virtual void read(uint64_t offset, void *buffer, size_t len) {
      m_state.file.pread(offset, buffer, len);
    }

    // writes to the device; this function does not use mmap,
    // and is responsible for writing the data is run through the file
    // filters
    virtual void write(uint64_t offset, void *buffer, size_t len) {
      m_state.file.pwrite(offset, buffer, len);
    }

    // allocate storage from this device; this function
    // will *NOT* return mmapped memory
    virtual uint64_t alloc(size_t len) {
      uint64_t address = m_state.file_size;
      truncate(address + len);
      return ((uint64_t)address);
    }

    // reads a page from the device; this function CAN return a
	// pointer to mmapped memory
    virtual void read_page(Page *page, uint64_t address) {
      // if this page is in the mapped area: return a pointer into that area.
      // otherwise fall back to read/write.
      if (address < m_state.mapped_size && m_state.mmapptr != 0) {
        // ok, this page is mapped. If the Page object has a memory buffer
        // then free it; afterwards return a pointer into the mapped memory
        page->free_buffer();
        // the following line will not throw a C++ exception, but can
        // raise a signal. If that's the case then we don't catch it because
        // something is seriously wrong and proper recovery is not possible.
        page->assign_mapped_buffer(&m_state.mmapptr[address], address);
        return;
      }

      // this page is not in the mapped area; allocate a buffer
      if (page->get_data() == 0) {
        // note that |p| will not leak if file.pread() throws; |p| is stored
        // in the |page| object and will be cleaned up by the caller in
        // case of an exception.
        uint8_t *p = Memory::allocate<uint8_t>(m_config.page_size_bytes);
        page->assign_allocated_buffer(p, address);
      }

      m_state.file.pread(address, page->get_data(), m_config.page_size_bytes);
    }

    // writes a page to the device
    virtual void write_page(Page *page) {
      write(page->get_address(), page->get_data(), m_config.page_size_bytes);
    }

    // Allocates storage for a page from this device; this function
    // will *NOT* return mmapped memory
    virtual void alloc_page(Page *page) {
      uint64_t address = m_state.file_size;

      truncate(address + m_config.page_size_bytes);
      page->set_address(address);

      // allocate a memory buffer
      uint8_t *p = Memory::allocate<uint8_t>(m_config.page_size_bytes);
      page->assign_allocated_buffer(p, address);
    }

    // Frees a page on the device; plays counterpoint to |alloc_page|
    virtual void free_page(Page *page) {
      ham_assert(page->get_data() != 0);
      page->free_buffer();
    }

    // Returns true if the specified range is in mapped memory
    virtual bool is_mapped(uint64_t file_offset, size_t size) const {
      return (file_offset + size <= m_state.mapped_size);
    }

  private:
    State m_state;
};

} // namespace hamsterdb

#endif /* HAM_DEVICE_DISK_H */
