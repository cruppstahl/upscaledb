/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 */

#include <string.h>
#include <ham/hamsterdb.h>
#include "error.h"
#include "db.h"
#include "page.h"
#include "mem.h"
#include "os.h"
#include "freelist.h"
#include "cursor.h"
#include "device.h"

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
page_new(ham_db_t *db)
{
    ham_page_t *page;

    page=(ham_page_t *)ham_mem_alloc(db, sizeof(ham_page_t));
    if (!page) {
        db_set_error(db, HAM_OUT_OF_MEMORY);
        return (0);
    }

    memset(page, 0, sizeof(*page));
    page_set_owner(page, db);
    /* temporarily initialize the cache counter, just to be on the safe side */
    page_set_cache_cntr(page, 20);

    return (page);
}

void
page_delete(ham_page_t *page)
{
    ham_db_t *db=page_get_owner(page);

    ham_assert(page!=0, (0));
    ham_assert(page_get_refcount(page)==0, (0));
    ham_assert(page_get_pers(page)==0, (0));

    ham_mem_free(db, page);
}

ham_status_t
page_alloc(ham_page_t *page, ham_size_t size)
{
    ham_status_t st;
    ham_db_t *db=page_get_owner(page);
    ham_device_t *dev=db_get_device(db);

	st=dev->alloc_page(dev, page, size);
    if (st)
        return (db_set_error(db, st));

    return (HAM_SUCCESS);
}

ham_status_t
page_fetch(ham_page_t *page, ham_size_t size)
{
    ham_status_t st;
    ham_db_t *db=page_get_owner(page);
    ham_device_t *dev=db_get_device(db);

    st=dev->read_page(dev, page, size);
    if (st)
        return (db_set_error(db, st));
    return (0);
}

ham_status_t
page_flush(ham_page_t *page)
{
    ham_status_t st;
    ham_db_t *db=page_get_owner(page);
    ham_device_t *dev=db_get_device(db);

    if (!page_is_dirty(page))
        return (HAM_SUCCESS);

    st=dev->write_page(dev, page);
    if (st)
        return (st);

    page_set_dirty(page, 0);
    return (HAM_SUCCESS);
}

ham_status_t
page_free(ham_page_t *page)
{
    ham_db_t *db=page_get_owner(page);
    ham_device_t *dev=db_get_device(db);

    return (dev->free_page(dev, page));
}

