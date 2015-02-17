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
 * A class managing a dynamically sized array for arbitrary types
 *
 * @exception_safe: strong
 * @thread_safe: no
 */

#ifndef HAM_DYNAMIC_ARRAY_H
#define HAM_DYNAMIC_ARRAY_H

#include "0root/root.h"

#include <stdlib.h>
#include <string.h>

// Always verify that a file of level N does not include headers > N!
#include "1mem/mem.h"

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

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

} // namespace hamsterdb

#endif // HAM_DYNAMIC_ARRAY_H
