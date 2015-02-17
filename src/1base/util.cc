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

// Always verify that a file of level N does not include headers > N!
#include "1base/util.h"

namespace hamsterdb {

int
util_vsnprintf(char *str, size_t size, const char *format, va_list ap)
{
#if defined(HAM_OS_POSIX)
  return vsnprintf(str, size, format, ap);
#elif defined(HAM_OS_WIN32)
  return _vsnprintf(str, size, format, ap);
#else
  (void)size;
  return (vsprintf(str, format, ap));
#endif
}

} // namespace hamsterdb

