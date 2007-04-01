/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 * memory allocator which tracks memory leaks
 *
 */

#include "../src/mem.h"

typedef struct memdesc_t
{
    const char *file;
    int line;
    int size;
    struct memdesc_t *next;
    struct memdesc_t *previous;
    int magic_start;
    char data[1];
} memdesc_t;

typedef struct 
{
    alloc_func_t alloc;
    free_func_t  free;
    close_func_t close;

    memdesc_t *header;
    unsigned long total_size;

} memtracker_t;

extern memtracker_t *
memtracker_new(void);

extern unsigned long
memtracker_get_leaks(memtracker_t *mt);


