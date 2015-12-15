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

#define CLANG_ANALYZER_NORETURN
#if __clang__
#  if __has_feature(attribute_analyzer_noreturn)
#    undef CLANG_ANALYZER_NORETURN
#    define CLANG_ANALYZER_NORETURN __attribute__((analyzer_noreturn))
#  endif
#endif

// causes the actual abort()
extern void
dbg_verify_failed(int level, const char *file, int line,
                const char *function, const char *expr) CLANG_ANALYZER_NORETURN;

// a hook for unittests; will be triggered when an assert fails
extern void (*ups_test_abort)();

// if your compiler does not support __FUNCTION__, you can define it here:
//  #define __FUNCTION__ 0

/*
 * in debug mode we write trace()-messages to stderr, and assert()
 * is enabled.
 *
 * not every preprocessor supports ellipsis as macro-arguments -
 * therefore we have to use brackets, so preprocessors treat multiple
 * arguments like a single argument. and we need to lock the output,
 * otherwise we are not thread-safe. this is super-ugly.
 */
#ifdef UPS_DEBUG
#   define ups_assert(e) while (!(e)) {                                       \
                upscaledb::dbg_verify_failed(UPS_DEBUG_LEVEL_FATAL, __FILE__, \
                        __LINE__, __FUNCTION__, #e);                          \
                break;                                                        \
              }
#else /* !UPS_DEBUG */
#   define ups_assert(e)      (void)0
#endif /* UPS_DEBUG */

// ups_log() and ups_verify() are available in every build
#define ups_trace(f)     do {                                                 \
                upscaledb::dbg_prepare(UPS_DEBUG_LEVEL_DEBUG, __FILE__,       \
                    __LINE__, __FUNCTION__, 0);                               \
                upscaledb::dbg_log f;                                         \
              } while (0)

#define ups_log(f)       do {                                                 \
                upscaledb::dbg_prepare(UPS_DEBUG_LEVEL_NORMAL, __FILE__,      \
                    __LINE__, __FUNCTION__, 0);                               \
                upscaledb::dbg_log f;                                         \
              } while (0)

#define ups_verify(e)      if (!(e)) {                                        \
                upscaledb::dbg_verify_failed(UPS_DEBUG_LEVEL_FATAL, __FILE__, \
                        __LINE__, __FUNCTION__, #e);                          \
              }

} // namespace upscaledb

#endif /* UPS_ERROR_H */
