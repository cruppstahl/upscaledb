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
 * A (stupid) smart pointer
 *
 * @exception_safe: nothrow
 * @thread_safe: yes
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
