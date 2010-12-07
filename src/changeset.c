/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include "page.h"
#include "changeset.h"

void
changeset_add_page(changeset_t *cs, ham_page_t *page)
{
    ham_assert(0==changeset_get_page(cs, page_get_self(page)), (""));
    ham_assert(0==page_get_next(page, PAGE_LIST_CHANGESET), (""));
    ham_assert(0==page_get_previous(page, PAGE_LIST_CHANGESET), (""));

    page_set_next(page, PAGE_LIST_CHANGESET, changeset_get_head(cs));
    changeset_set_head(cs, page);
}

ham_page_t *
changeset_get_page(changeset_t *cs, ham_offset_t pageid)
{
    ham_page_t *p=changeset_get_head(cs);
    while (p) {
        if (page_get_self(p)==pageid)
            return (p);
        p=page_get_next(p, PAGE_LIST_CHANGESET);
    }
    return (0);
}

void
changeset_clear(changeset_t *cs)
{
    ham_page_t *n, *p=changeset_get_head(cs);
    while (p) {
        n=page_get_next(p, PAGE_LIST_CHANGESET);

        page_set_next(p, PAGE_LIST_CHANGESET, 0);
        page_set_previous(p, PAGE_LIST_CHANGESET, 0);
        p=n;
    }
    changeset_set_head(cs, 0);
}

