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

ham_bool_t 
page_is_in_list(Page *head, Page *page, int which)
{
    if (page->get_next(which))
        return (HAM_TRUE);
    if (page->get_previous(which))
        return (HAM_TRUE);
    if (head==page)
        return (HAM_TRUE);
    return (HAM_FALSE);
}
#endif /* HAM_DEBUG */

Page::Page(Environment *env, Database *db)
  : m_self(0), m_db(db), m_device(0), m_flags(0), m_dirty(false),
    m_cursors(0), m_pers(0)
{
#if defined(HAM_OS_WIN32) || defined(HAM_OS_WIN64)
    m_win32mmap=0;
#endif
    if (env)
        m_device=env->get_device();
    memset(&m_prev[0], 0, sizeof(m_prev));
    memset(&m_next[0], 0, sizeof(m_next));
}

Page::~Page()
{
    ham_assert(get_pers()==0, (0));
    ham_assert(get_cursors()==0, (0));
}

ham_status_t
Page::allocate()
{
    return (get_device()->alloc_page(this));
}

ham_status_t
Page::fetch(ham_offset_t address)
{
    set_self(address);
    return (get_device()->read_page(this));
}

ham_status_t
Page::flush()
{
    if (!is_dirty())
        return (HAM_SUCCESS);

    ham_status_t st=get_device()->write_page(this);
    if (st)
        return (st);

    set_dirty(false);
    return (HAM_SUCCESS);
}


void
page_add_cursor(Page *page, Cursor *cursor)
{
    if (page->get_cursors()) {
        cursor->set_next_in_page(page->get_cursors());
        cursor->set_previous_in_page(0);
        page->get_cursors()->set_previous_in_page(cursor);
    }
    page->set_cursors(cursor);
}

void
page_remove_cursor(Page *page, Cursor *cursor)
{
    Cursor *n, *p;

    if (cursor==page->get_cursors()) {
        n=cursor->get_next_in_page();
        if (n)
            n->set_previous_in_page(0);
        page->set_cursors(n);
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

ham_status_t
page_free(Page *page)
{
    Device *dev=page->get_device();

    ham_assert(dev, (0));
    ham_assert(page->get_cursors()==0, (0));

    return (dev->free_page(page));
}

ham_status_t
page_uncouple_all_cursors(Page *page, ham_size_t start)
{
    Cursor *c=page->get_cursors();

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

