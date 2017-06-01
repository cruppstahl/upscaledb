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
 * A (stupid) smart pointer
 */

#ifndef UPS_SCOPED_PTR_H
#define UPS_SCOPED_PTR_H

#include "0root/root.h"

#define BOOST_ALL_NO_LIB // disable MSVC auto-linking
#include <boost/scoped_ptr.hpp>

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

template <typename T>
struct ScopedPtr : public boost::scoped_ptr<T>
{
  ScopedPtr()
    : boost::scoped_ptr<T>() {
  }

  ScopedPtr(T *t)
    : boost::scoped_ptr<T>(t) {
  }
};

} // namespace upscaledb

#endif /* UPS_SCOPED_PTR_H */
