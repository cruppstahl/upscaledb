/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

/*
 * A class managing a dynamically sized array for arbitrary types
 */

#ifndef UPS_DYNAMIC_ARRAY_H
#define UPS_DYNAMIC_ARRAY_H

#include "0root/root.h"

#include <stdlib.h>
#include <string.h>

// Always verify that a file of level N does not include headers > N!
#include "1mem/mem.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

/*
 * The DynamicArray class is a dynamic, resizable array. The internal memory
 * is released when the DynamicArray instance is destructed.
 *
 * Unlike std::vector, the DynamicArray uses libc functions for constructing,
 * copying and initializing elements.
 */
template<typename T>
class DynamicArray
{
  public:
    typedef T value_t;
    typedef T *pointer_t;

    DynamicArray(size_t size = 0)
      : ptr_(0), size_(0), own_(true) {
      resize(size);
    }

    DynamicArray(size_t size, uint8_t fill_byte)
      : ptr_(0), size_(0), own_(true) {
      resize(size);
      if (ptr_)
        ::memset(ptr_, fill_byte, sizeof(T) * size_);
    }

    ~DynamicArray() {
      clear();
    }

    size_t append(const T *ptr, size_t size) {
      size_t old_size = size_;
      T *p = (T *)resize(size_ + size);
      ::memcpy(p + old_size, ptr, sizeof(T) * size);
      return old_size;
    }

    void copy(const T *ptr, size_t size) {
      resize(size);
      ::memcpy(ptr_, ptr, sizeof(T) * size);
      size_ = size;
    }

    void overwrite(uint32_t position, const T *ptr, size_t size) {
      ::memcpy(((uint8_t *)ptr_) + position, ptr, sizeof(T) * size);
    }

    T *resize(size_t size) {
      if (size > size_) {
        ptr_ = Memory::reallocate<T>(ptr_, sizeof(T) * size);
        size_ = size;
      }
      return ptr_;
    }

    T *resize(size_t size, uint8_t fill_byte) {
      resize(size);
      if (ptr_)
        ::memset(ptr_, fill_byte, sizeof(T) * size);
      return ptr_;
    }

    size_t size() const {
      return size_;
    }

    void set_size(size_t size) {
      size_ = size;
    }

    T *data() {
      return ptr_;
    }

    const T *data() const {
      return ptr_;
    }

    void assign(T *ptr, size_t size) {
      clear();
      ptr_ = ptr;
      size_ = size;
    }

    void clear(bool release_memory = true) {
      if (own_ && release_memory)
        Memory::release(ptr_);
      ptr_ = 0;
      size_ = 0;
    }

    bool is_empty() const {
      return size_ == 0;
    }

    void disown() {
      own_ = false;
    }

  private:
    // Pointer to the data
    T *ptr_;

    // The size of the array
    size_t size_;

    // True if the destructor should free the pointer
    bool own_;
};

/*
 * A ByteArray is a DynamicArray for bytes
 */
typedef DynamicArray<uint8_t> ByteArray;

} // namespace upscaledb

#endif // UPS_DYNAMIC_ARRAY_H
