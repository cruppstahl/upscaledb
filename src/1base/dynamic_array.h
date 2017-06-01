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
struct DynamicArray {
  typedef T value_t;
  typedef T *pointer_t;

  DynamicArray(size_t size = 0)
    : _ptr(0), _size(0), _own(true) {
    resize(size);
  }

  DynamicArray(size_t size, uint8_t fill_byte)
    : _ptr(0), _size(0), _own(true) {
    resize(size);
    if (_ptr)
      ::memset(_ptr, fill_byte, sizeof(T) * _size);
  }

  ~DynamicArray() {
    clear();
  }

  void steal_from(DynamicArray &other) {
    clear();
    *this = other;
    other.disown();
    other.clear();
  }

  size_t append(const T *ptr, size_t size) {
    size_t old_size = _size;
    T *p = (T *)resize(_size + size);
    ::memcpy(p + old_size, ptr, sizeof(T) * size);
    return old_size;
  }

  void copy(const T *ptr, size_t size) {
    resize(size);
    ::memcpy(_ptr, ptr, sizeof(T) * size);
    _size = size;
  }

  void overwrite(uint32_t position, const T *ptr, size_t size) {
    ::memcpy(((uint8_t *)_ptr) + position, ptr, sizeof(T) * size);
  }

  T *resize(size_t size) {
    if (size > _size) {
      _ptr = Memory::reallocate<T>(_ptr, sizeof(T) * size);
      _size = size;
    }
    return _ptr;
  }

  T *resize(size_t size, uint8_t fill_byte) {
    resize(size);
    if (_ptr)
      ::memset(_ptr, fill_byte, sizeof(T) * size);
    return _ptr;
  }

  size_t size() const {
    return _size;
  }

  void set_size(size_t size) {
    _size = size;
  }

  T *data() {
    return _ptr;
  }

  const T *data() const {
    return _ptr;
  }

  void assign(T *ptr, size_t size) {
    clear();
    _ptr = ptr;
    _size = size;
  }

  void clear(bool release_memory = true) {
    if (_ptr && _own && release_memory)
      Memory::release(_ptr);
    _ptr = 0;
    _size = 0;
  }

  bool is_empty() const {
    return _size == 0;
  }

  void disown() {
    _own = false;
  }

  // Pointer to the data
  T *_ptr;

  // The size of the array
  size_t _size;

  // True if the destructor should free the pointer
  bool _own;
};

//
// A ByteArray is a DynamicArray for bytes
//
typedef DynamicArray<uint8_t> ByteArray;

} // namespace upscaledb

#endif // UPS_DYNAMIC_ARRAY_H
