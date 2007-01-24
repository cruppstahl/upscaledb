/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
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

static const char *g_file=0;
static int           g_line=0;
static const char *g_expr=0;

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
my_errhandler(const char *message)
{
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
    /* TODO implement this... */
}

void 
dbg_unlock(void)
{
    /* TODO implement this... */
}

void 
dbg_prepare(const char *file, int line, const char *expr)
{
    g_file=file;
    g_line=line;
    g_expr=expr;
}

void 
dbg_log(const char *format, ...)
{
    int s;
    char buffer[1024];

    va_list ap;
    va_start(ap, format);
    s=my_snprintf(buffer,   sizeof(buffer), "%s[%d]: ", g_file, g_line);
    my_vsnprintf (buffer+s, sizeof(buffer)-s, format, ap);
    va_end(ap);

    g_hand(buffer);
} 

void 
dbg_verify_failed(const char *format, ...)
{
    int s;
    char buffer[1024];
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
    
    g_hand(buffer);
    abort();
}

