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
#include <stack>

#ifdef HAVE_MALLOC_H
#  include <malloc.h>
#else
#  include <stdlib.h>
#endif

#include "db.h"
#include "error.h"
#include "mem.h"
#include "txn.h"

struct Lookasides
{
    typedef std::stack<void *> LookasideList;

    Lookasides() 
      : max_sizes(2) {
        sizes[0]=sizeof(txn_op_t);
        sizes[1]=sizeof(txn_opnode_t);
    }

    LookasideList lists[2];
    ham_u32_t sizes[2];
    int max_sizes;
};

void *
alloc_impl(mem_allocator_t *self, const char *file, int line, ham_u32_t size)
{
    void *p=0;
    Lookasides *ls=(Lookasides *)self->priv;

    (void)file;
    (void)line;

    for (int i=0; i<ls->max_sizes; i++) {
        if (size==ls->sizes[i] && !ls->lists[i].empty()) {
            p=ls->lists[i].top();
            ls->lists[i].pop();
            break;
        }
    }

    if (p)
        return ((char *)p+sizeof(ham_u32_t));

#if defined(_CRTDBG_MAP_ALLOC)
    p=_malloc_dbg(size+sizeof(ham_u32_t), _NORMAL_BLOCK, file, line);
#else
    p=malloc(size+sizeof(ham_u32_t));
#endif
    if (p) {
        *(ham_u32_t *)p=size;
        return ((char *)p+sizeof(ham_u32_t));
    }
    return (0);
}

void 
free_impl(mem_allocator_t *self, const char *file, int line, const void *ptr)
{
    ham_u32_t size;
    void *p=0;
    Lookasides *ls=(Lookasides *)self->priv;
    (void)file;
    (void)line;

    ham_assert(ptr, ("freeing NULL pointer in line %s:%d", file, line));

    p=(char *)ptr-sizeof(ham_u32_t);
    size=*(ham_u32_t *)p;

    if (ls) {
        for (int i=0; i<ls->max_sizes; i++) {
            if (size==ls->sizes[i] && ls->lists[i].size()<10) {
                ls->lists[i].push(p);
                return;
            }
        }
    }

#if defined(_CRTDBG_MAP_ALLOC)
    _free_dbg((void *)p, _NORMAL_BLOCK);
#else
    free((void *)p);
#endif
}

void *
realloc_impl(mem_allocator_t *self, const char *file, int line, 
        const void *ptr, ham_size_t size)
{
    (void)self;
    (void)file;
    (void)line;

    void *p=ptr ? (char *)ptr-sizeof(ham_u32_t) : 0;

#if defined(_CRTDBG_MAP_ALLOC)
    ptr=_realloc_dbg((void *)p, size+sizeof(ham_u32_t), 
                _NORMAL_BLOCK, file, line);
#else
    ptr=realloc((void *)p, size+sizeof(ham_u32_t));
#endif
    if (ptr) {
        *(ham_u32_t *)ptr=size;
        return ((char *)ptr+sizeof(ham_u32_t));
    }
    return (0);
}

void 
close_impl(mem_allocator_t *self)
{
    Lookasides *ls=(Lookasides *)self->priv;

    // avoid infinite recursion
    self->priv=0;

    for (int i=0; i<ls->max_sizes; i++) {
        while (!ls->lists[i].empty()) {
            void *p=(char *)ls->lists[i].top()+sizeof(ham_u32_t);
            ls->lists[i].pop();
            free_impl(self, __FILE__, __LINE__, p);
        }
    }
    delete ls;

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
    m->priv   =new Lookasides;
     
    return (m);
}

