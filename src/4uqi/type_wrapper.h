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
 * Template wrapper for POD-style types and variable-length arrays
 */

#ifndef UPS_UPSCALEDB_TYPE_WRAPPER_H
#define UPS_UPSCALEDB_TYPE_WRAPPER_H

#include "0root/root.h"

#include "ups/upscaledb_uqi.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

template<typename T>
struct Sequence {
  typedef const T *iterator;

  Sequence(const void *ptr, size_t length)
    : begin_((const T *)ptr), end_(begin_ + length) {
  }

  size_t length() const {
    return end_ - begin_;
  }

  iterator begin() const {
    return begin_;
  }

  iterator end() const {
    return end_;
  }

  iterator begin_;
  iterator end_;
};

template<typename T>
struct TypeWrapper {
  typedef T type;

  TypeWrapper(T t = 0)
    : value(t) {
  }

  TypeWrapper(const void *ptr, size_t size)
    : value(*(T *)ptr) {
    assert(size == sizeof(T));
  }

  TypeWrapper(const TypeWrapper &other)
    : value(other.value) {
  }

  TypeWrapper &operator=(const TypeWrapper &other) {
    value = other.value;
    return *this;
  }

  size_t size() const {
    return sizeof(T);
  }

  const void *ptr() const {
    return &value;
  }

  bool operator<(const TypeWrapper<T> &other) const {
    return value < other.value;
  }

  bool operator>(const TypeWrapper<T> &other) const {
    return value > other.value;
  }

  bool operator==(const TypeWrapper<T> &other) const {
    return value == other.value;
  }

  T value;
};

} // namespace upscaledb

#endif /* UPS_UPSCALEDB_TYPE_WRAPPER_H */
