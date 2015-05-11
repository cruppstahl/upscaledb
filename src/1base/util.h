/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
 * Misc. utility classes and functions
 *
 * @exception_safe: nothrow
 * @thread_safe: yes
 */

#ifndef HAM_UTIL_H
#define HAM_UTIL_H

#include "0root/root.h"

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

// Always verify that a file of level N does not include headers > N!

#ifndef HAM_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

//
// vsnprintf replacement/wrapper
//
// uses vsprintf on platforms which do not define vsnprintf
//
extern int
util_vsnprintf(char *str, size_t size, const char *format, va_list ap);

//
// snprintf replacement/wrapper
//
// uses sprintf on platforms which do not define snprintf
//
#ifndef HAM_OS_POSIX
#  define util_snprintf _snprintf
#else
#  define util_snprintf snprintf
#endif

} // namespace hamsterdb

#endif // HAM_UTIL_H
