/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * NOTICE: All information contained herein is, and remains the property
 * of Christoph Rupp and his suppliers, if any. The intellectual and
 * technical concepts contained herein are proprietary to Christoph Rupp
 * and his suppliers and may be covered by Patents, patents in process,
 * and are protected by trade secret or copyright law. Dissemination of
 * this information or reproduction of this material is strictly forbidden
 * unless prior written permission is obtained from Christoph Rupp.
 */

/*
 * Returns the demangled name of a class
 *
 * @exception_safe: nothrow
 * @thread_safe: yes
 */

#ifndef HAM_ABI_H
#define HAM_ABI_H

#include "0root/root.h"

#ifdef HAVE_GCC_ABI_DEMANGLE
#  include <cxxabi.h>
#endif

#include <string>
#include <stdlib.h>

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
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
    ::free(name);
    return ("");
  }
  std::string s = name;
  ::free(name);
  return (s);
#else
  return ("");
#endif
}

} // namespace hamsterdb

#endif /* HAM_ABI_H */

