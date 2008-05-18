/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
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
    memdesc_t *header;
    unsigned long total;
} memtracker_priv_t;

typedef struct 
{
    alloc_func_t alloc;
    free_func_t  free;
    realloc_func_t realloc;
    close_func_t close;

    memtracker_priv_t *priv;

} memtracker_t;

extern memtracker_t *
memtracker_new(void);

extern unsigned long
memtracker_get_leaks(memtracker_t *mt);


