/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 */

#include <stdexcept>
#include "memtracker.h"

#define MAGIC_START 0x12345678

#define MAGIC_STOP  0x98765432

#define OFFSETOF(type, member) ((size_t) &((type *)0)->member)

static memdesc_t *
get_descriptor(void *p)
{
    return ((memdesc_t *)((char *)p-OFFSETOF(memdesc_t, data)));
}

static void 
verify_mem_desc(memdesc_t *desc)
{
    if (desc->size==0)
        throw std::out_of_range("memory blob size is 0");
    if (desc->magic_start!=MAGIC_START)
        throw std::out_of_range("memory blob descriptor is corrupt");
    if (*(unsigned *)(desc->data+desc->size)!=MAGIC_STOP)
        throw std::out_of_range("memory blob was corrupted after end");
}

void *
alloc_impl(mem_allocator_t *self, const char *file, int line, ham_u32_t size)
{
    int magic=MAGIC_STOP;
    memtracker_t *mt=(memtracker_t *)self;
    memtracker_priv_t *priv=(memtracker_priv_t *)mt->priv;
    memdesc_t *desc=(memdesc_t *)malloc(sizeof(*desc)+size-1+sizeof(int));
    if (!desc)
        return (0);
    memset(desc, 0, sizeof(*desc));
    desc->file=file;
    desc->line=line;
    desc->size=size;
    desc->magic_start=MAGIC_START;
    /* avoid unaligned access on sparc */
    memcpy(desc->data+size, &magic, sizeof(int));

    desc->next=priv->header;
    if (priv->header)
        priv->header->previous=desc;
    priv->header=desc;
    priv->total+=size;

    return (desc->data);
}

void 
free_impl(mem_allocator_t *self, const char *file, int line, void *ptr)
{
    memtracker_t *mt=(memtracker_t *)self;
    memtracker_priv_t *priv=(memtracker_priv_t *)mt->priv;
    memdesc_t *desc, *p, *n;

    if (!ptr)
        throw std::logic_error("tried to free a null-pointer");

    desc=get_descriptor(ptr);
    verify_mem_desc(desc);

    if (priv->header==desc)
        priv->header=desc->next;
    else {
        p=desc->previous;
        n=desc->next;
        if (p)
            p->next=n;
        if (n)
            n->previous=p;
    }

    priv->total-=desc->size;
    free(desc);
}

void 
close_impl(mem_allocator_t *self)
{
    memtracker_t *mt=(memtracker_t *)self;
    (void)mt;

    /* TODO ausgabe machen? */
}

memtracker_t *
memtracker_new(void)
{
    static memtracker_t m;
    static memtracker_priv_t p;
    memset(&m, 0, sizeof(m));
    memset(&p, 0, sizeof(p));
    m.alloc=alloc_impl;
    m.free =free_impl;
    m.close=close_impl;
    m.priv=&p;
    return (&m);
}

unsigned long
memtracker_get_leaks(memtracker_t *mt)
{
    memtracker_priv_t *priv=(memtracker_priv_t *)mt->priv;
    return (priv->total);
}

