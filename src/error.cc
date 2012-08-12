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
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>

#include <ham/hamsterdb.h>

#include "error.h"
#include "mem.h"
#include "util.h"
#include "internal_fwd_decl.h"

namespace ham {

static int     g_level   =0;
static const char *g_file  =0;
static int     g_line  =0;
static const char *g_expr  =0;
static const char *g_function=0;

void (*ham_test_abort)(void);

static int
dbg_snprintf(char *str, size_t size, const char *format, ...)
{
  int s;

  va_list ap;
  va_start(ap, format);
  s=util_vsnprintf(str, size, format, ap);
  va_end(ap);
  
  return (s);
}

static void HAM_CALLCONV
dbg_errhandler(int level, const char *message)
{
#ifndef HAM_DEBUG
  if (level == HAM_DEBUG_LEVEL_DEBUG)
    return;
#endif
  fprintf(stderr, "%s\n", message);
}

static ham_errhandler_fun g_hand = dbg_errhandler;

static Mutex dbg_mutex;

void 
dbg_lock(void)
{
  dbg_mutex.lock();
}

void 
dbg_unlock(void)
{
  dbg_mutex.unlock();
}

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
#if HAM_DEBUG
  s = dbg_snprintf(buffer,   sizeof(buffer), "%s[%d]: ", g_file, g_line);
  util_vsnprintf(buffer + s, sizeof(buffer) - s, format, ap);
#else
  if (g_function)
    s = dbg_snprintf(buffer, sizeof(buffer), "%s: ", g_function);
  util_vsnprintf(buffer + s, sizeof(buffer) - s, format, ap);
#endif
  va_end(ap);

  g_hand(g_level, buffer);
} 

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
  
  g_hand(g_level, buffer);

  if (ham_test_abort) {
    ham_test_abort();
  }
  else {
#ifndef HAM_OS_WINCE
    abort();
#else
    ExitProcess(-1);
#endif
  }
}

} // namespace ham

// global namespace...

void HAM_CALLCONV
ham_set_errhandler(ham_errhandler_fun f)
{
  if (f)
    ham::g_hand = f;
  else
    ham::g_hand = ham::dbg_errhandler;
}

