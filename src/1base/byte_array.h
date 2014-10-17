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

/*
 * A class managing a dynamically sized byte array
 *
 * @exception_safe: nothrow
 * @thread_safe: no
 */

#ifndef HAM_BYTE_ARRAY_H
#define HAM_BYTE_ARRAY_H

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
 * The ByteArray class is a dynamic, resizable array. The internal memory
 * is released when the ByteArray instance is destructed.
 */
class ByteArray
{
  public:
    ByteArray(size_t size = 0)
      : m_ptr(0), m_size(0), m_own(true) {
      resize(size);
    }

    ByteArray(size_t size, uint8_t fill_byte)
      : m_ptr(0), m_size(0), m_own(true) {
      resize(size);
      if (m_ptr)
        ::memset(m_ptr, fill_byte, m_size);
    }

    ~ByteArray() {
      clear();
    }

    void append(const void *ptr, size_t size) {
      uint32_t oldsize = m_size;
      char *p = (char *)resize(m_size + size);
      ::memcpy(p + oldsize, ptr, size);
    }

    void copy(const void *ptr, size_t size) {
      resize(size);
      ::memcpy(m_ptr, ptr, size);
      m_size = size;
    }

    void overwrite(uint32_t position, const void *ptr, size_t size) {
      ::memcpy(((uint8_t *)m_ptr) + position, ptr, size);
    }

    void *resize(size_t size) {
      if (size > m_size) {
        m_ptr = Memory::reallocate<void>(m_ptr, size);
        m_size = size;
      }
      return (m_ptr);
    }

    void *resize(size_t size, uint8_t fill_byte) {
      resize(size);
      if (m_ptr)
        memset(m_ptr, fill_byte, size);
      return (m_ptr);
    }

    size_t get_size() const {
      return (m_size);
    }

    void set_size(size_t size) {
      m_size = size;
    }

    void *get_ptr() {
      return (m_ptr);
    }

    const void *get_ptr() const {
      return (m_ptr);
    }

    void assign(void *ptr, size_t size) {
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
    void *m_ptr;
    size_t m_size;
    bool m_own;
};

} // namespace hamsterdb

#endif // HAM_BYTE_ARRAY_H
