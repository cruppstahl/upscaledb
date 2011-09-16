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


void *
alloc_impl(mem_allocator_t *self, const char *file, int line, ham_u32_t size)
{
    (void)self;
    (void)file;
    (void)line;

#if defined(_CRTDBG_MAP_ALLOC)
    return (_malloc_dbg(size, _NORMAL_BLOCK, file, line));
#else
    return (malloc(size));
#endif
}

void 
free_impl(mem_allocator_t *self, const char *file, int line, const void *ptr)
{
    (void)self;
    (void)file;
    (void)line;

    ham_assert(ptr, ("freeing NULL pointer in line %s:%d", file, line));
#if defined(_CRTDBG_MAP_ALLOC)
    _free_dbg((void *)ptr, _NORMAL_BLOCK);
#else
    free((void *)ptr);
#endif
}

void *
realloc_impl(mem_allocator_t *self, const char *file, int line, 
        const void *ptr, ham_size_t size)
{
    (void)self;
    (void)file;
    (void)line;

#if defined(_CRTDBG_MAP_ALLOC)
    return (_realloc_dbg((void *)ptr, size, _NORMAL_BLOCK, file, line));
#else
    return (realloc((void *)ptr, size));
#endif
}

void 
close_impl(mem_allocator_t *self)
{
#if defined(_CRTDBG_MAP_ALLOC)
    _free_dbg(self, _NORMAL_BLOCK);
#else
    free(self);
#endif
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

