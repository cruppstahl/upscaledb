/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

