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
 * Error handling routines, assert macros, logging facilities
 */

#ifndef UPS_ERROR_H
#define UPS_ERROR_H

#include "0root/root.h"

#include <assert.h>

#include "ups/upscaledb.h"

// Always verify that a file of level N does not include headers > N!

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace upscaledb {

//
// A generic exception for storing a status code
//
struct Exception
{
  Exception(ups_status_t st)
    : code(st) {
  }

  ups_status_t code;
};

// the default error handler
void UPS_CALLCONV
default_errhandler(int level, const char *message);

extern void
dbg_prepare(int level, const char *file, int line, const char *function,
                const char *expr);

extern void
dbg_log(const char *format, ...);

#ifndef NDEBUG
#  define ups_trace(f)     do {                                               \
                upscaledb::dbg_prepare(UPS_DEBUG_LEVEL_DEBUG, __FILE__,       \
                    __LINE__, __FUNCTION__, 0);                               \
                upscaledb::dbg_log f;                                         \
              } while (0)
#else /* NDEBUG */
#   define ups_trace(f)    (void)0
#endif /* NDEBUG */


#define ups_log(f)       do {                                                 \
                upscaledb::dbg_prepare(UPS_DEBUG_LEVEL_NORMAL, __FILE__,      \
                    __LINE__, __FUNCTION__, 0);                               \
                upscaledb::dbg_log f;                                         \
              } while (0)

} // namespace upscaledb

#endif /* UPS_ERROR_H */
