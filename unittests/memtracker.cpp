/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 * memory allocator which tracks memory leaks
 *
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
    if (*(int *)(desc->data+desc->size)!=MAGIC_STOP)
        throw std::out_of_range("memory blob was corrupted after end");
}

void *
alloc_impl(mem_allocator_t *self, const char *file, int line, ham_u32_t size)
{
    memtracker_t *mt=(memtracker_t *)self;
    memdesc_t *desc=(memdesc_t *)malloc(sizeof(*desc)+size-1+sizeof(int));
    if (!desc)
        return (0);
    memset(desc, 0, sizeof(*desc));
    desc->file=file;
    desc->line=line;
    desc->size=size;
    desc->magic_start=MAGIC_START;
    *(int *)(desc->data+size)=MAGIC_STOP;

    desc->next=mt->header;
    if (mt->header)
        mt->header->previous=desc;
    mt->header=desc;

    mt->total_size+=desc->size;
    return (desc->data);
}

void 
free_impl(mem_allocator_t *self, const char *file, int line, void *ptr)
{
    memtracker_t *mt=(memtracker_t *)self;
    memdesc_t *desc, *p, *n;

    if (!ptr)
        throw std::logic_error("tried to free a null-pointer");

    desc=get_descriptor(ptr);
    verify_mem_desc(desc);

    if (mt->header==desc)
        mt->header=desc->next;
    else {
        p=desc->previous;
        n=desc->next;
        if (p)
            p->next=n;
        if (n)
            n->previous=p;
    }

    mt->total_size-=desc->size;
    free(desc);
}

void 
close_impl(mem_allocator_t *self)
{
    memtracker_t *mt=(memtracker_t *)self;

    /* TODO ausgabe machen? */
    free(mt);
}

memtracker_t *
memtracker_new(void)
{
    memtracker_t *m=(memtracker_t *)malloc(sizeof(*m));
    if (m) {
        memset(m, 0, sizeof(*m));
        m->alloc=alloc_impl;
        m->free =free_impl;
        m->close=close_impl;
    }
    return (m);
}

unsigned long
memtracker_get_leaks(memtracker_t *mt)
{
    return (mt->total_size);
}

