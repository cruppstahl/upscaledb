/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 */

#include "config.h"

#ifdef HAVE_MALLOC_H
#  include <malloc.h>
#else
#  include <stdlib.h>
#endif

#include "mem.h"

void *
_ham_mem_malloc(const char *file, int line, ham_u32_t size)
{
    /* avoid "not used"-warnings */
    file=file;
    line=line;

    return (malloc(size));
}

void 
_ham_mem_free(const char *file, int line, void *ptr)
{
    /* avoid "not used"-warnings */
    file=file;
    line=line;

    if (ptr) 
        free(ptr);
}
