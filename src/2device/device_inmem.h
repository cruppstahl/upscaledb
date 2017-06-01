/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

/*
 * @exception_safe: strong
 * @thread_safe: no
 */

#ifndef UPS_DEVICE_INMEM_H
#define UPS_DEVICE_INMEM_H

#include "0root/root.h"

// Always verify that a file of level N does not include headers > N!
#include "1mem/mem.h"
#include "2device/device.h"
#include "2page/page.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

/*
 * an In-Memory device
 */ 
struct InMemoryDevice : public Device {
  // constructor
  InMemoryDevice(const EnvConfig &config)
    : Device(config) {
    is_open_ = false;
    allocated_size_ = 0;
  }

  // Create a new device
  virtual void create() {
    is_open_ = true;
  }

  // opens an existing device 
  virtual void open() {
    assert(!"can't open an in-memory-device");
    throw Exception(UPS_NOT_IMPLEMENTED);
  }

  // returns true if the device is open 
  virtual bool is_open() {
    return is_open_;
  }

  // closes the device 
  virtual void close() {
    assert(is_open_);
    is_open_ = false;
  }

  // flushes the device 
  virtual void flush() {
  }

  // truncate/resize the device 
  virtual void truncate(uint64_t newsize) {
  }

  // get the current file/storage size 
  virtual uint64_t file_size() {
    assert(!"this operation is not possible for in-memory-databases");
    throw Exception(UPS_NOT_IMPLEMENTED);
  }

  // seek position in a file 
  virtual void seek(uint64_t offset, int whence) {
    assert(!"can't seek in an in-memory-device");
    throw Exception(UPS_NOT_IMPLEMENTED);
  }

  // tell the position in a file 
  virtual uint64_t tell() {
    assert(!"can't tell in an in-memory-device");
    throw Exception(UPS_NOT_IMPLEMENTED);
  }

  // reads from the device; this function does not use mmap 
  virtual void read(uint64_t offset, void *buffer, size_t len) {
    assert(!"operation is not possible for in-memory-databases");
    throw Exception(UPS_NOT_IMPLEMENTED);
  }

  // writes to the device 
  virtual void write(uint64_t offset, void *buffer, size_t len) {
  }

  // reads a page from the device 
  virtual void read_page(Page *page, uint64_t address) {
    assert(!"operation is not possible for in-memory-databases");
    throw Exception(UPS_NOT_IMPLEMENTED);
  }

  // allocate storage from this device; this function
  // will *NOT* use mmap.  
  virtual uint64_t alloc(size_t size) {
    if (allocated_size_ + size > config.file_size_limit_bytes)
      throw Exception(UPS_LIMITS_REACHED);

    uint64_t retval = (uint64_t)Memory::allocate<uint8_t>(size);
    allocated_size_ += size;
    return retval;
  }

  // allocate storage for a page from this device 
  virtual void alloc_page(Page *page) {
    size_t page_size = config.page_size_bytes;
    if (allocated_size_ + page_size > config.file_size_limit_bytes)
      throw Exception(UPS_LIMITS_REACHED);

    uint8_t *p = Memory::allocate<uint8_t>(page_size);
    page->assign_allocated_buffer(p, (uint64_t)p);

    allocated_size_ += page_size;
  }

  // frees a page on the device; plays counterpoint to @ref alloc_page 
  virtual void free_page(Page *page) {
    page->free_buffer();

    assert(allocated_size_ >= config.page_size_bytes);
    allocated_size_ -= config.page_size_bytes;
  }

  // Returns true if the specified range is in mapped memory
  virtual bool is_mapped(uint64_t file_offset, size_t size) const {
    return false;
  }

  // Removes unused space at the end of the file
  virtual void reclaim_space() {
  }

  // releases a chunk of memory previously allocated with alloc()
  void release(void *ptr, size_t size) {
    Memory::release(ptr);
    assert(allocated_size_ >= size);
    allocated_size_ -= size;
  }

  // flag whether this device was "opened" or is uninitialized
  bool is_open_;

  // the allocated bytes
  uint64_t allocated_size_;
};

} // namespace upscaledb

#endif /* UPS_DEVICE_INMEM_H */
