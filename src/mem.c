/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 */

#include "config.h"

#include <string.h>

#ifdef HAVE_MALLOC_H
#  include <malloc.h>
#else
#  include <stdlib.h>
#endif

#include "mem.h"
#include "error.h"

void *
alloc_impl(mem_allocator_t *self, const char *file, int line, ham_u32_t size)
{
    (void)self;
    (void)file;
    (void)line;

    return (malloc(size));
}

void 
free_impl(mem_allocator_t *self, const char *file, int line, void *ptr)
{
    (void)self;
    (void)file;
    (void)line;

    ham_assert(ptr, ("freeing NULL pointer in line %s:%d", file, line)) 
    free(ptr);
}

void 
close_impl(mem_allocator_t *self)
{
    free(self);
}

mem_allocator_t *
ham_default_allocator_new(void)
{
    mem_allocator_t *m;

    m=(mem_allocator_t *)malloc(sizeof(*m));
    if (!m)
        return (0);

    memset(m, 0, sizeof(*m));
    m->alloc=alloc_impl;
    m->free =free_impl;
    m->close=close_impl;
     
    return (m);
}
