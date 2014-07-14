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

