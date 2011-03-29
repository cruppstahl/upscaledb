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
static ham_bool_t
my_is_in_list(ham_page_t *p, int which)
{
    return (p->_npers._next[which] || p->_npers._prev[which]);
}

static void
my_validate_page(ham_page_t *p)
{
    /*
     * not allowed: dirty and in garbage bin
     */
    ham_assert(!(page_is_dirty(p) && my_is_in_list(p, PAGE_LIST_GARBAGE)),
            ("dirty and in garbage bin"));

    /*
     * not allowed: referenced and in garbage bin
     */
    ham_assert(!(page_get_refcount(p) && my_is_in_list(p, PAGE_LIST_GARBAGE)),
            ("referenced and in garbage bin"));

    /*
     * not allowed: in garbage bin and cursors
     */
    ham_assert(!(page_get_cursors(p) && my_is_in_list(p, PAGE_LIST_GARBAGE)),
            ("cursors and in garbage bin"));

    /*
     * not allowed: in transaction and in garbage bin
     */
    ham_assert(!(my_is_in_list(p, PAGE_LIST_TXN) && 
               my_is_in_list(p, PAGE_LIST_GARBAGE)),
            ("in txn and in garbage bin"));

    /*
     * not allowed: in transaction, but not referenced
     */
    if (my_is_in_list(p, PAGE_LIST_TXN))
        ham_assert(page_get_refcount(p)>0,
            ("in txn, but refcount is zero"));

    /*
     * not allowed: cached and in garbage bin
     */
    ham_assert(!(my_is_in_list(p, PAGE_LIST_BUCKET) && 
               my_is_in_list(p, PAGE_LIST_GARBAGE)),
            ("cached and in garbage bin"));
}

ham_page_t *
page_get_next(ham_page_t *page, int which)
{
    ham_page_t *p=page->_npers._next[which];
    my_validate_page(page);
    if (p)
        my_validate_page(p);
    return (p);
}

void
page_set_next(ham_page_t *page, int which, ham_page_t *other)
{
    page->_npers._next[which]=other;
    my_validate_page(page);
    if (other)
        my_validate_page(other);
}

ham_page_t *
page_get_previous(ham_page_t *page, int which)
{
    ham_page_t *p=page->_npers._prev[which];
    my_validate_page(page);
    if (p)
        my_validate_page(p);
    return (p);
}

void
page_set_previous(ham_page_t *page, int which, ham_page_t *other)
{
    page->_npers._prev[which]=other;
    my_validate_page(page);
    if (other)
        my_validate_page(other);
}
#endif /* HAM_DEBUG */

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

ham_page_t *
page_list_insert(ham_page_t *head, int which, ham_page_t *page)
{
    page_set_next(page, which, 0);
    page_set_previous(page, which, 0);

    if (!head)
        return (page);

    page_set_next(page, which, head);
    page_set_previous(head, which, page);
    return (page);
}

ham_page_t *
page_list_remove(ham_page_t *head, int which, ham_page_t *page)
{
    ham_page_t *n, *p;

    if (page==head) {
        n=page_get_next(page, which);
        if (n)
            page_set_previous(n, which, 0);
        page_set_next(page, which, 0);
        page_set_previous(page, which, 0);
        return (n);
    }

    n=page_get_next(page, which);
    p=page_get_previous(page, which);
    if (p)
        page_set_next(p, which, n);
    if (n)
        page_set_previous(n, which, p);
    page_set_next(page, which, 0);
    page_set_previous(page, which, 0);
    return (head);
}

void
page_add_cursor(ham_page_t *page, ham_cursor_t *cursor)
{
    if (page_get_cursors(page)) {
        cursor_set_next_in_page(cursor, page_get_cursors(page));
        cursor_set_previous_in_page(cursor, 0);
        cursor_set_previous_in_page(page_get_cursors(page), cursor);
    }
    page_set_cursors(page, cursor);
}

void
page_remove_cursor(ham_page_t *page, ham_cursor_t *cursor)
{
    ham_cursor_t *n, *p;

    if (cursor==page_get_cursors(page)) {
        n=cursor_get_next_in_page(cursor);
        if (n)
            cursor_set_previous_in_page(n, 0);
        page_set_cursors(page, n);
    }
    else {
        n=cursor_get_next_in_page(cursor);
        p=cursor_get_previous_in_page(cursor);
        if (p)
            cursor_set_next_in_page(p, n);
        if (n)
            cursor_set_previous_in_page(n, p);
    }

    cursor_set_next_in_page(cursor, 0);
    cursor_set_previous_in_page(cursor, 0);
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

    /*
     * initialize the cache counter, 
     * see also cache_increment_page_counter() 
     */
    page_set_cache_cntr(page, 
        (env_get_cache(env) ? env_get_cache(env)->_timeslot++ : 0));

    return (page);
}

void
page_delete(ham_page_t *page)
{
    ham_assert(page!=0, (0));
    ham_assert(page_get_refcount(page)==0, (0));
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
    ham_env_t *env;
    ham_device_t *dev=page_get_device(page);

    if (!page_is_dirty(page))
        return (HAM_SUCCESS);

	ham_assert(dev, (0));
	env = device_get_env(dev);
	ham_assert(env, (0));
	ham_assert(page_get_owner(page) 
                    ? env == db_get_env(page_get_owner(page)) 
                    : 1, (0));

	/* 
	 * as we are about to write a modified page to disc, we MUST flush 
	 * the log before we do write the page in order to assure crash 
	 * recovery:
     *
	 * as this page belongs to us, it may well be a page which was modified
	 * in the pending transaction and any such edits should be REWINDable
	 * after a crash when that page has just been written.
	 */
    if (env && env_get_log(env)) {
        st=ham_log_append_flush_page(env_get_log(env), page);
        if (st)
            return (st);
    }

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

