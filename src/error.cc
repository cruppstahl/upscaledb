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

#include "config.h"

#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include "error.h"
#include "util.h"

namespace hamsterdb {

static int         g_level    = 0;
static const char *g_file     = 0;
static int         g_line     = 0;
static const char *g_expr     = 0;
static const char *g_function = 0;

void (*ham_test_abort)(void);

static int
dbg_snprintf(char *str, size_t size, const char *format, ...)
{
  int s;

  va_list ap;
  va_start(ap, format);
  s = util_vsnprintf(str, size, format, ap);
  va_end(ap);

  return (s);
}

void HAM_CALLCONV
default_errhandler(int level, const char *message)
{
#ifndef HAM_DEBUG
  if (level == HAM_DEBUG_LEVEL_DEBUG)
    return;
#endif
  fprintf(stderr, "%s\n", message);
}

ham_errhandler_fun g_handler = default_errhandler;

void
dbg_prepare(int level, const char *file, int line, const char *function,
            const char *expr)
{
  g_level = level;
  g_file = file;
  g_line = line;
  g_expr = expr;
  g_function = function;
}

void
dbg_log(const char *format, ...)
{
  int s = 0;
  char buffer[1024 * 4];

  va_list ap;
  va_start(ap, format);
#ifdef HAM_DEBUG
  s = dbg_snprintf(buffer,   sizeof(buffer), "%s[%d]: ", g_file, g_line);
  util_vsnprintf(buffer + s, sizeof(buffer) - s, format, ap);
#else
  if (g_function)
    s = dbg_snprintf(buffer, sizeof(buffer), "%s: ", g_function);
  util_vsnprintf(buffer + s, sizeof(buffer) - s, format, ap);
#endif
  va_end(ap);

  g_handler(g_level, buffer);
}

/* coverity[+kill] */
void
dbg_verify_failed(const char *format, ...)
{
  int s;
  char buffer[1024 * 4];
  va_list ap;

  if (!g_expr)
    g_expr = "(none)";

  s = dbg_snprintf(buffer, sizeof(buffer),
      "ASSERT FAILED in file %s, line %d:\n\t\"%s\"\n",
      g_file, g_line, g_expr);

  if (format) {
    va_start(ap, format);
    util_vsnprintf(buffer + s, sizeof(buffer) - s, format, ap);
    va_end(ap);
  }

  g_handler(g_level, buffer);

  if (ham_test_abort)
    ham_test_abort();
  else {
    abort();
  }
}

} // namespace hamsterdb

