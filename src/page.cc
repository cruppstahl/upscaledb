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

#include "cache.h"
#include "cursor.h"
#include "db.h"
#include "device.h"
#include "env.h"
#include "error.h"
#include "freelist.h"
#include "log.h"
#include "mem.h"
#include "os.h"
#include "page.h"

#ifdef HAM_DEBUG
/*
static ham_bool_t
__is_in_list(ham_page_t *p, int which)
{
    return (p->_npers._next[which] || p->_npers._prev[which]);
}
*/

static void
__validate_page(ham_page_t *p)
{
    /* not allowed: in changeset but not in cache */
    /* disabled - freelist pages can be in a changeset, but are never
     * in a cache bucket; TODO rewrite this check only for non-freelist 
     * pages!
    if (__is_in_list(p, PAGE_LIST_CHANGESET))
        ham_assert(__is_in_list(p, PAGE_LIST_BUCKET),
            ("in changeset but not in cache")); */
}

ham_page_t *
page_get_next(ham_page_t *page, int which)
{
    ham_page_t *p=page->_npers._next[which];
    __validate_page(page);
    if (p)
        __validate_page(p);
    return (p);
}

void
page_set_next(ham_page_t *page, int which, ham_page_t *other)
{
    page->_npers._next[which]=other;
    __validate_page(page);
    if (other)
        __validate_page(other);
}

ham_page_t *
page_get_previous(ham_page_t *page, int which)
{
    ham_page_t *p=page->_npers._prev[which];
    __validate_page(page);
    if (p)
        __validate_page(p);
    return (p);
}

void
page_set_previous(ham_page_t *page, int which, ham_page_t *other)
{
    page->_npers._prev[which]=other;
    __validate_page(page);
    if (other)
        __validate_page(other);
}

ham_bool_t 
page_is_in_list(ham_page_t *head, ham_page_t *page, int which)
{
    if (page_get_next(page, which))
        return (HAM_TRUE);
    if (page_get_previous(page, which))
        return (HAM_TRUE);
    if (head==page)
        return (HAM_TRUE);
    return (HAM_FALSE);
}
#endif /* HAM_DEBUG */

void
page_add_cursor(ham_page_t *page, Cursor *cursor)
{
    if (page_get_cursors(page)) {
        cursor->set_next_in_page(page_get_cursors(page));
        cursor->set_previous_in_page(0);
        page_get_cursors(page)->set_previous_in_page(cursor);
    }
    page_set_cursors(page, cursor);
}

void
page_remove_cursor(ham_page_t *page, Cursor *cursor)
{
    Cursor *n, *p;

    if (cursor==page_get_cursors(page)) {
        n=cursor->get_next_in_page();
        if (n)
            n->set_previous_in_page(0);
        page_set_cursors(page, n);
    }
    else {
        n=cursor->get_next_in_page();
        p=cursor->get_previous_in_page();
        if (p)
            p->set_next_in_page(n);
        if (n)
            n->set_previous_in_page(p);
    }

    cursor->set_next_in_page(0);
    cursor->set_previous_in_page(0);
}

ham_page_t *
page_new(ham_env_t *env)
{
    ham_page_t *page;
    mem_allocator_t *alloc=env_get_allocator(env);

    page=(ham_page_t *)allocator_alloc(alloc, sizeof(*page));
    if (!page)
        return (0);
    memset(page, 0, sizeof(*page));
    page_set_allocator(page, alloc);
    page_set_device(page, env_get_device(env));

    return (page);
}

void
page_delete(ham_page_t *page)
{
    ham_assert(page!=0, (0));
    ham_assert(page_get_pers(page)==0, (0));
    ham_assert(page_get_cursors(page)==0, (0));

    allocator_free(page_get_allocator(page), page);
}

ham_status_t
page_alloc(ham_page_t *page)
{
    ham_device_t *dev=page_get_device(page);

    ham_assert(dev, (0));
    return (dev->alloc_page(dev, page));
}

ham_status_t
page_fetch(ham_page_t *page)
{
    ham_device_t *dev=page_get_device(page);

    ham_assert(dev, (0));
    return (dev->read_page(dev, page));
}

ham_status_t
page_flush(ham_page_t *page)
{
    ham_status_t st;
    ham_device_t *dev=page_get_device(page);

    if (!page_is_dirty(page))
        return (HAM_SUCCESS);

    ham_assert(dev, (0));

    st=dev->write_page(dev, page);
    if (st)
        return (st);

    page_set_undirty(page);
    return (HAM_SUCCESS);
}

ham_status_t
page_free(ham_page_t *page)
{
    ham_device_t *dev=page_get_device(page);

    ham_assert(dev, (0));
    ham_assert(page_get_cursors(page)==0, (0));

    return (dev->free_page(dev, page));
}

