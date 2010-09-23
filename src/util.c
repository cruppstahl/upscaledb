/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

#include "config.h"

#include <string.h>
#include <stdio.h>
#include <stdarg.h>

#include "blob.h"
#include "db.h"
#include "env.h"
#include "error.h"
#include "keys.h"
#include "mem.h"
#include "util.h"


int
util_vsnprintf(char *str, size_t size, const char *format, va_list ap)
{
#if defined(HAM_OS_POSIX)
    return vsnprintf(str, size, format, ap);
#elif defined(HAM_OS_WIN32)
    return _vsnprintf(str, size, format, ap);
#else
    (void)size;
    return vsprintf(str, format, ap);
#endif
}


