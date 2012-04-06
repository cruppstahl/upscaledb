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

#define MAX                         20
#define LOOKASIDE(s)                                                        \
void *lookaside ## s[MAX];                                                  \
int   used ## s;

LOOKASIDE(1)
LOOKASIDE(4)
LOOKASIDE(16)
LOOKASIDE(104)
LOOKASIDE(136)
LOOKASIDE(264)
LOOKASIDE(392)
LOOKASIDE(520)
LOOKASIDE(4096)

#define PUSH_CASE(p, s)                                     \
case s:                                                     \
    if (used ## s >= MAX)                                   \
        return (HAM_FALSE);                                 \
    lookaside ## s[used ## s]=p;                            \
    used ## s ++;                                           \
    return (HAM_TRUE);

#define POP_CASE(s)                                         \
case s:                                                     \
    if (used ## s > 0) {                                    \
        void *x=lookaside ## s[used ## s -1];               \
        lookaside ## s[used ## s -1]=0;                     \
        used ## s --;                                       \
        return (char *)(x+sizeof(ham_u32_t));               \
    }                                                       \
    return (0);

#define CLOSE(s)                                            \
    for (i=0; i<used ##s; i++) {                            \
        free(lookaside ## s[i]);                            \
        lookaside ## s[i] = 0;                              \
    }                                                       \
    used ## s = 0;

static ham_bool_t
push_ptr(void *p, ham_u32_t size)
{
    switch (size) {
        PUSH_CASE(p, 1)
        PUSH_CASE(p, 4)
        PUSH_CASE(p, 16)
        PUSH_CASE(p, 104)
        PUSH_CASE(p, 136)
        PUSH_CASE(p, 264)
        PUSH_CASE(p, 392)
        PUSH_CASE(p, 520)
        PUSH_CASE(p, 4096)
        default:
            return (HAM_FALSE);
    }
}

static void *
pop_ptr(ham_u32_t size)
{
    switch (size) {
        POP_CASE(1)
        POP_CASE(4)
        POP_CASE(16)
        POP_CASE(104)
        POP_CASE(136)
        POP_CASE(264)
        POP_CASE(392)
        POP_CASE(520)
        POP_CASE(4096)
        default:
            return (0);
    }
}

void *
alloc_impl(mem_allocator_t *self, const char *file, int line, ham_u32_t size)
{
    void *p=pop_ptr(size);
    if (p)
        return (p);
    else
        p=malloc(size+sizeof(ham_u32_t));
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
    if (push_ptr(p, *(ham_u32_t *)p))
        return;
 
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
    int i;
    CLOSE(1)
    CLOSE(4)
    CLOSE(16)
    CLOSE(104)
    CLOSE(136)
    CLOSE(264)
    CLOSE(392)
    CLOSE(520)
    CLOSE(4096)
    total_allocs=0;
    max_allocs=0;
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

