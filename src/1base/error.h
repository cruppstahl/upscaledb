/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
