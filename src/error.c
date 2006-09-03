/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 */

#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdio.h>
#include <ham/hamsterdb.h>

#include "db.h"
#include "error.h"

extern int vsnprintf(char *str, size_t size, const char *format, va_list ap);
extern int snprintf(char *str, size_t size, const char *format, ...);

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
_ham_log(const char *file, int line, const char *format, ...)
{
    int s;
    char buffer[1024];

    va_list ap;
    va_start(ap, format);
    s=snprintf(buffer,   sizeof(buffer), "%s[%d]: ", file, line);
    vsnprintf (buffer+s, sizeof(buffer)-s, format, ap);
    va_end(ap);

    g_hand(buffer);
} 

void 
_ham_verify(const char *file, int line, const char *msg, 
            const char *format, ...)
{
    int s;
    char buffer[1024];
    va_list ap;

    if (!msg)
        msg="(none)";

    s=snprintf(buffer, sizeof(buffer), 
            "ASSERT FAILED in file %s, line %d:\n\t\"%s\"\n", file, line, msg);

    if (format) {
        va_start(ap, format);
        vsnprintf(buffer+s, sizeof(buffer)-s, format, ap);
        va_end(ap);
    }
    
    g_hand(buffer);
    abort();
}

