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

