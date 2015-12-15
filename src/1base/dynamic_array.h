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
      : m_ptr(0), m_size(0), m_own(true) {
      resize(size);
    }

    DynamicArray(size_t size, uint8_t fill_byte)
      : m_ptr(0), m_size(0), m_own(true) {
      resize(size);
      if (m_ptr)
        ::memset(m_ptr, fill_byte, sizeof(T) * m_size);
    }

    ~DynamicArray() {
      clear();
    }

    void append(const T *ptr, size_t size) {
      size_t old_size = m_size;
      T *p = (T *)resize(m_size + size);
      ::memcpy(p + old_size, ptr, sizeof(T) * size);
    }

    void copy(const T *ptr, size_t size) {
      resize(size);
      ::memcpy(m_ptr, ptr, sizeof(T) * size);
      m_size = size;
    }

    void overwrite(uint32_t position, const T *ptr, size_t size) {
      ::memcpy(((uint8_t *)m_ptr) + position, ptr, sizeof(T) * size);
    }

    T *resize(size_t size) {
      if (size > m_size) {
        m_ptr = Memory::reallocate<T>(m_ptr, sizeof(T) * size);
        m_size = size;
      }
      return (m_ptr);
    }

    T *resize(size_t size, uint8_t fill_byte) {
      resize(size);
      if (m_ptr)
        ::memset(m_ptr, fill_byte, sizeof(T) * size);
      return (m_ptr);
    }

    size_t get_size() const {
      return (m_size);
    }

    void set_size(size_t size) {
      m_size = size;
    }

    T *get_ptr() {
      return (m_ptr);
    }

    const T *get_ptr() const {
      return (m_ptr);
    }

    void assign(T *ptr, size_t size) {
      clear();
      m_ptr = ptr;
      m_size = size;
    }

    void clear(bool release_memory = true) {
      if (m_own && release_memory)
        Memory::release(m_ptr);
      m_ptr = 0;
      m_size = 0;
    }

    bool is_empty() const {
      return (m_size == 0);
    }

    void disown() {
      m_own = false;
    }

  private:
    // Pointer to the data
    T *m_ptr;

    // The size of the array
    size_t m_size;

    // True if the destructor should free the pointer
    bool m_own;
};

/*
 * A ByteArray is a DynamicArray for bytes
 */
typedef DynamicArray<uint8_t> ByteArray;

} // namespace upscaledb

#endif // UPS_DYNAMIC_ARRAY_H
