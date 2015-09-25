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

#include "0root/root.h"

#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

// Always verify that a file of level N does not include headers > N!
#include "1base/util.h"
#include "1globals/globals.h"

#ifndef UPS_ROOT_H
#  error "root.h was not included"
#endif

namespace hamsterdb {

void (*ups_test_abort)(void);

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

void UPS_CALLCONV
default_errhandler(int level, const char *message)
{
#ifndef UPS_DEBUG
  if (level == UPS_DEBUG_LEVEL_DEBUG)
    return;
#endif
  fprintf(stderr, "%s\n", message);
}

void
dbg_prepare(int level, const char *file, int line, const char *function,
            const char *expr)
{
  Globals::ms_error_level = level;
  Globals::ms_error_file = file;
  Globals::ms_error_line = line;
  Globals::ms_error_expr = expr;
  Globals::ms_error_function = function;
}

void
dbg_log(const char *format, ...)
{
  int s = 0;
  char buffer[1024 * 4];

  va_list ap;
  va_start(ap, format);
#ifdef UPS_DEBUG
  s = dbg_snprintf(buffer,   sizeof(buffer), "%s[%d]: ",
                  Globals::ms_error_file, Globals::ms_error_line);
  util_vsnprintf(buffer + s, sizeof(buffer) - s, format, ap);
#else
  if (Globals::ms_error_function)
    s = dbg_snprintf(buffer, sizeof(buffer), "%s: ",
                    Globals::ms_error_function);
  util_vsnprintf(buffer + s, sizeof(buffer) - s, format, ap);
#endif
  va_end(ap);

  Globals::ms_error_handler(Globals::ms_error_level, buffer);
}

/* coverity[+kill] */
void
dbg_verify_failed(int level, const char *file, int line, const char *function,
            const char *expr)
{
  char buffer[1024 * 4];

  if (!expr)
    expr = "(none)";

  dbg_snprintf(buffer, sizeof(buffer),
      "ASSERT FAILED in file %s, line %d:\n\t\"%s\"\n",
      file, line, expr);
  buffer[sizeof(buffer) - 1] = '\0';

  Globals::ms_error_handler(Globals::ms_error_level, buffer);

  if (ups_test_abort)
    ups_test_abort();
  else
    abort();
}

} // namespace hamsterdb

