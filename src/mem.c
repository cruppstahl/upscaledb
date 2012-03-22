/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
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

#ifdef HAVE_MALLOC_H
#  include <malloc.h>
#else
#  include <stdlib.h>
#endif

#include "db.h"
#include "error.h"
#include "mem.h"

ham_u32_t total_allocs;
ham_u32_t max_allocs;
ham_u32_t current_alloc;

void *
alloc_impl(mem_allocator_t *self, const char *file, int line, ham_u32_t size)
{
    void *p=malloc(size+sizeof(ham_u32_t));
    total_allocs+=size;
    if (total_allocs>max_allocs)
        max_allocs=total_allocs;
    current_alloc=size;
    *(ham_u32_t *)p=size;
/*printf("current %8u, peak %8u, size: +%4u\n", total_allocs, max_allocs, 
        current_alloc);*/
    return ((char *)p)+sizeof(ham_u32_t);
}

void 
free_impl(mem_allocator_t *self, const char *file, int line, const void *ptr)
{
    char *p=(char *)ptr;
    p-=sizeof(ham_u32_t);
    total_allocs-=*(ham_u32_t *)p;
/*printf("current %8u, peak %8u, size: -%4u\n", total_allocs, max_allocs, 
        *(ham_u32_t *)p);*/
    free((void *)p);
}

void *
realloc_impl(mem_allocator_t *self, const char *file, int line, 
        const void *ptr, ham_size_t size)
{
    char *p=(char *)ptr;
    if (!ptr)
        return (alloc_impl(self, file, line, size));
    p-=sizeof(ham_u32_t);
    total_allocs-=*(ham_u32_t *)p;
    total_allocs+=size;
    if (total_allocs>max_allocs)
        max_allocs=total_allocs;
/*printf("current %8u, peak %8u, size: -%4u\n", total_allocs, max_allocs,
        *(ham_u32_t *)p);
printf("current %8u, peak %8u, size: +%4u\n", total_allocs, max_allocs,
        size);*/

    p=(char *)realloc((void *)p, size+sizeof(ham_u32_t));
    if (!p)
        return 0;

    *(ham_u32_t *)p=size;
    return ((char *)p)+sizeof(ham_u32_t);
}

void 
close_impl(mem_allocator_t *self)
{
    free(self);
}

mem_allocator_t *
_ham_default_allocator_new(const char *fname, const int lineno)
{
    mem_allocator_t *m;

    m=(mem_allocator_t *)
#if defined(_CRTDBG_MAP_ALLOC)
                    _malloc_dbg(sizeof(*m), _NORMAL_BLOCK, fname, lineno);
#else
                    malloc(sizeof(*m));
#endif
    if (!m)
        return (0);

    memset(m, 0, sizeof(*m));
    m->alloc  =alloc_impl;
    m->free   =free_impl;
    m->realloc=realloc_impl;
    m->close  =close_impl;
     
    return (m);
}

