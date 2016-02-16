/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
