/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
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

#include "cursor.h"
#include "db.h"
#include "env.h"
#include "error.h"
#include "mem.h"

static ham_status_t
__dupecache_resize(dupecache_t *c, ham_size_t capacity)
{
    ham_env_t *env=db_get_env(cursor_get_db(dupecache_get_cursor(c)));
    dupe_entry_t *ptr=dupecache_get_elements(c);
    ptr=(dupe_entry_t *)allocator_realloc(env_get_allocator(env), 
                    ptr, sizeof(dupe_entry_t)*capacity);
    if (ptr) {
        dupecache_set_capacity(c, capacity);
        dupecache_set_elements(c, ptr);
        return (0);
    }

    return (HAM_OUT_OF_MEMORY);
}

ham_status_t
dupecache_create(dupecache_t *c, struct ham_cursor_t *cursor, 
                    ham_size_t capacity)
{
    memset(c, 0, sizeof(*c));
    dupecache_set_cursor(c, cursor);

    return (__dupecache_resize(c, capacity));
}

ham_status_t
dupecache_insert(dupecache_t *c, ham_u32_t position, dupe_entry_t *dupe)
{
    ham_status_t st;
    dupe_entry_t *e;

    ham_assert(position<=dupecache_get_count(c), (""));

    /* append or insert in the middle? */
    if (position==dupecache_get_count(c)-1)
        return (dupecache_append(c, dupe));

    /* resize if necessary */
    if (dupecache_get_count(c)>=dupecache_get_capacity(c)-1) {
        st=__dupecache_resize(c, dupecache_get_capacity(c)*2);
        if (st)
            return (st);
    }

    e=dupecache_get_elements(c);

    /* shift elements to the "right" */
    memmove(&e[position+1], &e[position], 
                    sizeof(dupe_entry_t)*(dupecache_get_count(c)-position));
    e[position]=*dupe;
    dupecache_set_count(c, dupecache_get_count(c)+1);

    return (0);
}

ham_status_t
dupecache_append(dupecache_t *c, dupe_entry_t *dupe)
{
    ham_status_t st;
    dupe_entry_t *e;

    /* resize if necessary */
    if (dupecache_get_count(c)>=dupecache_get_capacity(c)-1) {
        st=__dupecache_resize(c, dupecache_get_capacity(c)*2);
        if (st)
            return (st);
    }

    e=dupecache_get_elements(c);

    e[dupecache_get_count(c)]=*dupe;
    dupecache_set_count(c, dupecache_get_count(c)+1);

    return (0);
}

ham_status_t
dupecache_erase(dupecache_t *c, ham_u32_t position)
{
    ham_assert(position<dupecache_get_count(c), (""));

    dupe_entry_t *e=dupecache_get_elements(c);

    if (position<dupecache_get_count(c)-1) {
        /* shift elements to the "left" */
        memmove(&e[position], &e[position+1], 
                    sizeof(dupe_entry_t)*(dupecache_get_count(c)-position-1));
    }

    dupecache_set_count(c, dupecache_get_count(c)-1);

    return (0);
}

ham_status_t
dupecache_sort(dupecache_t *c)
{
    /* TODO */
    return (0);
}

void
dupecache_clear(dupecache_t *c)
{
    ham_env_t *env=db_get_env(cursor_get_db(dupecache_get_cursor(c)));

    if (dupecache_get_elements(c))
        allocator_free(env_get_allocator(env), dupecache_get_elements(c));

    dupecache_set_elements(c, 0);
    dupecache_set_capacity(c, 0);
    dupecache_set_count(c, 0);
}

