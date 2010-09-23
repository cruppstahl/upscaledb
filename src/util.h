/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief utility functions
 *
 */

#ifndef HAM_UTIL_H__
#define HAM_UTIL_H__

#include "internal_fwd_decl.h"

#include <stdarg.h>
#include <stdio.h>


#ifdef __cplusplus
extern "C" {
#endif 

/**
 * vsnprintf replacement/wrapper
 * 
 * uses sprintf on platforms which do not define snprintf
 */
extern int
util_vsnprintf(char *str, size_t size, const char *format, va_list ap);

/**
 * snprintf replacement/wrapper
 * 
 * uses sprintf on platforms which do not define snprintf
 */
#ifndef HAM_OS_POSIX
#define util_snprintf _snprintf
#else
#define util_snprintf snprintf
#endif


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_UTIL_H__ */
