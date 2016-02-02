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
 * Returns the demangled name of a class
 */

#ifndef UPS_ABI_H
#define UPS_ABI_H

#include "0root/root.h"

#ifdef HAVE_GCC_ABI_DEMANGLE
#  include <cxxabi.h>
#endif

#include <string>
#include <stdlib.h>

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

template<class T> inline std::string
get_classname(const T& t)
{
#ifdef HAVE_GCC_ABI_DEMANGLE
  int status;
  const std::type_info &ti = typeid(t);
  char *name = abi::__cxa_demangle(ti.name(), 0, 0, &status);
  if (!name)
    return "";
  if (status) {
    ::free(name);
    return "";
  }
  std::string s = name;
  ::free(name);
  return s;
#else
  return "";
#endif
}

} // namespace upscaledb

#endif /* UPS_ABI_H */

