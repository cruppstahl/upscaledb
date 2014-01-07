/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifndef HAM_ABI_H__
#define HAM_ABI_H__

#include <string>
#ifdef HAVE_GCC_ABI_DEMANGLE
#  include <cxxabi.h>
#endif

namespace hamsterdb {

template<class T> inline std::string
get_classname(const T& t)
{
#ifdef HAVE_GCC_ABI_DEMANGLE
  int status;
  const std::type_info &ti = typeid(t);
  char *name = abi::__cxa_demangle(ti.name(), 0, 0, &status);
  if (!name)
    return ("");
  if (status) {
    free(name);
    return ("");
  }
  std::string s = name;
  free(name);
  return (s);
#else
  return ("");
#endif
}

} // namespace hamsterdb

#endif /* HAM_ABI_H__ */

