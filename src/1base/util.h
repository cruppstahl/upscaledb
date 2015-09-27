/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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
 * Misc. utility classes and functions
 *
 * @exception_safe: nothrow
 * @thread_safe: yes
 */

#ifndef UPS_UTIL_H
#define UPS_UTIL_H

#include "0root/root.h"

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

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
#ifndef UPS_OS_POSIX
#  define util_snprintf _snprintf
#else
#  define util_snprintf snprintf
#endif

} // namespace upscaledb

#endif // UPS_UTIL_H
