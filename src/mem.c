/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 */

#include <malloc.h>
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
