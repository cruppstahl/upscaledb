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
__is_in_list(Page *p, int which)
{
    return (p->m_next[which] || p->m_prev[which]);
}
*/

static void
__validate_page(Page *p)
{
    /* not allowed: in changeset but not in cache */
    /* disabled - freelist pages can be in a changeset, but are never
     * in a cache bucket; TODO rewrite this check only for non-freelist 
     * pages!
    if (__is_in_list(p, Page::LIST_CHANGESET))
        ham_assert(__is_in_list(p, Page::LIST_BUCKET),
            ("in changeset but not in cache")); */
}

Page *
page_get_next(Page *page, int which)
{
    Page *p=page->m_next[which];
    __validate_page(page);
    if (p)
        __validate_page(p);
    return (p);
}

void
page_set_next(Page *page, int which, Page *other)
{
    page->m_next[which]=other;
    __validate_page(page);
    if (other)
        __validate_page(other);
}

Page *
page_get_previous(Page *page, int which)
{
    Page *p=page->m_prev[which];
    __validate_page(page);
    if (p)
        __validate_page(p);
    return (p);
}

void
page_set_previous(Page *page, int which, Page *other)
{
    page->m_prev[which]=other;
    __validate_page(page);
    if (other)
        __validate_page(other);
}

ham_bool_t 
page_is_in_list(Page *head, Page *page, int which)
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
page_add_cursor(Page *page, Cursor *cursor)
{
    if (page_get_cursors(page)) {
        cursor->set_next_in_page(page_get_cursors(page));
        cursor->set_previous_in_page(0);
        page_get_cursors(page)->set_previous_in_page(cursor);
    }
    page_set_cursors(page, cursor);
}

void
page_remove_cursor(Page *page, Cursor *cursor)
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

Page *
page_new(Environment *env)
{
    Page *page;
    Allocator *alloc=env->get_allocator();

    page=(Page *)alloc->alloc(sizeof(*page));
    if (!page)
        return (0);
    memset(page, 0, sizeof(*page));
    page_set_allocator(page, alloc);
    page_set_device(page, env->get_device());

    return (page);
}

void
page_delete(Page *page)
{
    ham_assert(page!=0, (0));
    ham_assert(page_get_pers(page)==0, (0));
    ham_assert(page_get_cursors(page)==0, (0));

    page_get_allocator(page)->free(page);
}

ham_status_t
page_alloc(Page *page)
{
    Device *dev=page_get_device(page);

    ham_assert(dev, (0));
    return (dev->alloc_page(page));
}

ham_status_t
page_fetch(Page *page)
{
    Device *dev=page_get_device(page);

    ham_assert(dev, (0));
    return (dev->read_page(page));
}

ham_status_t
page_flush(Page *page)
{
    ham_status_t st;
    Device *dev=page_get_device(page);

    if (!page_is_dirty(page))
        return (HAM_SUCCESS);

    ham_assert(dev, (0));

    st=dev->write_page(page);
    if (st)
        return (st);

    page_set_undirty(page);
    return (HAM_SUCCESS);
}

ham_status_t
page_free(Page *page)
{
    Device *dev=page_get_device(page);

    ham_assert(dev, (0));
    ham_assert(page_get_cursors(page)==0, (0));

    return (dev->free_page(page));
}

ham_status_t
page_uncouple_all_cursors(Page *page, ham_size_t start)
{
    Cursor *c = page_get_cursors(page);

    if (c) {
        Database *db=c->get_db();
        if (db) {
            ham_backend_t *be=db->get_backend();
            
            if (be)
                return (*be->_fun_uncouple_all_cursors)(be, page, start);
        }
    }

    return (HAM_SUCCESS);
}

