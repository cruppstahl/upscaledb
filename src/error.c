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

const char *
ham_strerror(ham_status_t result)
{
    switch (result) {
        case HAM_SUCCESS: 
            return ("Success");
        case HAM_SHORT_READ: 
            return ("Short read");
        case HAM_SHORT_WRITE: 
            return ("Short write");
        case HAM_INV_KEYSIZE: 
            return ("Invalid key size");
        case HAM_DB_ALREADY_OPEN: 
            return ("Db already open");
        case HAM_OUT_OF_MEMORY: 
            return ("Out of memory");
        case HAM_INV_BACKEND:
            return ("Invalid backend");
        case HAM_INV_PARAMETER:
            return ("Invalid parameter");
        case HAM_INV_FILE_HEADER:
            return ("Invalid database file header");
        case HAM_INV_FILE_VERSION:
            return ("Invalid database file version");
        case HAM_KEY_NOT_FOUND:
            return ("Key not found");
        case HAM_DUPLICATE_KEY:
            return ("Duplicate key");
        case HAM_INTEGRITY_VIOLATED:
            return ("Internal integrity violated");
        case HAM_INTERNAL_ERROR:
            return ("Internal error");
        case HAM_DB_READ_ONLY:
            return ("Database opened read only");
        case HAM_BLOB_NOT_FOUND:
            return ("Data blob not found");
        case HAM_PREFIX_REQUEST_FULLKEY:
            return ("Comparator needs more data");

        /* fall back to strerror() */
        default: 
            return (strerror(result));
    }
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

