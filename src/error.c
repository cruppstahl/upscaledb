/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 */

#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdio.h>

#include <ham/hamsterdb.h>

#include "db.h"
#include "error.h"

#if HAM_OS_POSIX
extern int vsnprintf(char *str, size_t size, const char *format, va_list ap);
#endif

static int         g_level   =0;
static const char *g_file    =0;
static int         g_line    =0;
static const char *g_expr    =0;
static const char *g_function=0;

void (*ham_test_abort)(void);

static int
my_vsnprintf(char *str, size_t size, const char *format, va_list ap)
{
#if HAM_OS_POSIX
    return vsnprintf(str, size, format, ap);
#else
    (void)size;
    return vsprintf(str, format, ap);
#endif
}

static int
my_snprintf(char *str, size_t size, const char *format, ...)
{
    int s;

    va_list ap;
    va_start(ap, format);
    s=my_vsnprintf(str, size, format, ap);
    va_end(ap);
    
    return (s);
}

static void
my_errhandler(int level, const char *message)
{
#ifndef HAM_DEBUG
    if (level==DBG_LVL_DEBUG)
        return;
#endif
    fprintf(stderr, "%s\n", message);
}

static ham_errhandler_fun g_hand=my_errhandler;

void
ham_set_errhandler(ham_errhandler_fun f)
{
    if (f)
        g_hand=f;
    else
        g_hand=my_errhandler;
}

void 
dbg_lock(void)
{
    /* not yet needed, we do not yet support multithreading */
}

void 
dbg_unlock(void)
{
    /* not yet needed, we do not yet support multithreading */
}

void 
dbg_prepare(int level, const char *file, int line, const char *function,
        const char *expr)
{
    g_level=level;
    g_file=file;
    g_line=line;
    g_expr=expr;
    g_function=function;
}

void 
dbg_log(const char *format, ...)
{
    int s=0;
    char buffer[1024*4];

    va_list ap;
    va_start(ap, format);
#if HAM_DEBUG
    s=my_snprintf(buffer,   sizeof(buffer), "%s[%d]: ", g_file, g_line);
    my_vsnprintf (buffer+s, sizeof(buffer)-s, format, ap);
#else
    if (g_function)
        s=my_snprintf(buffer,   sizeof(buffer), "%s: ", g_function);
    my_vsnprintf (buffer+s, sizeof(buffer)-s, format, ap);
#endif
    va_end(ap);

    g_hand(g_level, buffer);
} 

void 
dbg_verify_failed(const char *format, ...)
{
    int s;
    char buffer[1024*4];
    va_list ap;

    if (!g_expr)
        g_expr="(none)";
    if (!format)
        format="(none)";

    s=my_snprintf(buffer, sizeof(buffer), 
            "ASSERT FAILED in file %s, line %d:\n\t\"%s\"\n", 
            g_file, g_line, g_expr);

    if (format) {
        va_start(ap, format);
        my_vsnprintf(buffer+s, sizeof(buffer)-s, format, ap);
        va_end(ap);
    }
    
    g_hand(g_level, buffer);

#ifndef HAM_OS_WINCE
    if (ham_test_abort)
        ham_test_abort();
    else
        abort();
#else
	ExitProcess(-1);
#endif
}

