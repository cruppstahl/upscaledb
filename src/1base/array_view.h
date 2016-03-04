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

#ifndef UPS_ARRAY_VIEW_H
#define UPS_ARRAY_VIEW_H

#include "0root/root.h"

#include <stdlib.h>

// Always verify that a file of level N does not include headers > N!
#include "1mem/mem.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

template<typename T>
struct ArrayView
{
  ArrayView()
    : data(0), size(0) {
  }

  ArrayView(T *data_, size_t size_)
    : data(data_), size(size_) {
  }

  ArrayView(const ArrayView &other)
    : data(other.data), size(other.size) {
  }

  T &operator[](size_t index) {
    return data[index];
  }

  const T &operator[](size_t index) const {
    return data[index];
  }

  // Pointer to the data
  T *data;

  // The number of elements in the array
  size_t size;
};

typedef ArrayView<uint8_t> ByteArrayView;

} // namespace upscaledb

#endif // UPS_ARRAY_VIEW_H

