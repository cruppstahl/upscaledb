/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
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

#if (HAM_DEBUG)
static ham_bool_t
my_is_in_list(ham_page_t *p, int which)
{
    return (p->_npers._next[which] || p->_npers._prev[which]);
}

static void
my_validage_page(ham_page_t *p)
{
    /*
     * not allowed: dirty and in garbage bin
     */
    ham_assert(!(page_is_dirty(p) && my_is_in_list(p, PAGE_LIST_GARBAGE)),
            "dirty and in garbage bin", 0);

    /*
     * not allowed: in use and in garbage bin
     */
    ham_assert(!(page_is_inuse(p) && my_is_in_list(p, PAGE_LIST_GARBAGE)),
            "referenced and in garbage bin", 0);

    /*
     * not allowed: in transaction and in garbage bin
     */
    ham_assert(!(my_is_in_list(p, PAGE_LIST_TXN) && 
               my_is_in_list(p, PAGE_LIST_GARBAGE)),
            "in txn and in garbage bin", 0);

    /*
     * not allowed: cached and in garbage bin
     */
    ham_assert(!(my_is_in_list(p, PAGE_LIST_BUCKET) && 
               my_is_in_list(p, PAGE_LIST_GARBAGE)),
            "cached and in garbage bin", 0);

    /*
     * not allowed: unknown page types
     */
    ham_assert(page_get_type(p)!=PAGE_TYPE_UNKNOWN, 
            "page type is unknown", 0);
}

ham_page_t *
page_get_next(ham_page_t *page, int which)
{
    ham_page_t *p=page->_npers._next[which];
    my_validage_page(page);
    if (p)
        my_validage_page(p);
    return (p);
}

void
page_set_next(ham_page_t *page, int which, ham_page_t *other)
{
    page->_npers._next[which]=other;
    my_validage_page(page);
    if (other)
        my_validage_page(other);
}

ham_page_t *
page_get_previous(ham_page_t *page, int which)
{
    ham_page_t *p=page->_npers._prev[which];
    my_validage_page(page);
    if (p)
        my_validage_page(p);
    return (p);
}

void
page_set_previous(ham_page_t *page, int which, ham_page_t *other)
{
    page->_npers._prev[which]=other;
    my_validage_page(page);
    if (other)
        my_validage_page(other);
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
page_list_insert_ring(ham_page_t *head, int which, ham_page_t *page)
{
   ham_page_t *last;

    if (!head) {
        page_set_next(page, which, page);
        page_set_previous(page, which, page);
        return (page);
    }

    last=page_get_previous(head, which);
    page_set_previous(page, which, last);
    page_set_previous(head, which, page);
    page_set_next(page, which, head);
    page_set_next(last, which, page);
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
