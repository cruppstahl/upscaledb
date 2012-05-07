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


int Page::sizeof_persistent_header=(OFFSETOF(page_data_t, _s._payload));

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

ham_status_t
Page::free()
{
    ham_assert(get_cursors()==0, (0));

    return (get_device()->free_page(this));
}

void
Page::add_cursor(Cursor *cursor)
{
    if (get_cursors()) {
        cursor->set_next_in_page(get_cursors());
        cursor->set_previous_in_page(0);
        get_cursors()->set_previous_in_page(cursor);
    }
    set_cursors(cursor);
}

void
Page::remove_cursor(Cursor *cursor)
{
    Cursor *n, *p;

    if (cursor==get_cursors()) {
        n=cursor->get_next_in_page();
        if (n)
            n->set_previous_in_page(0);
        set_cursors(n);
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
Page::uncouple_all_cursors(ham_size_t start)
{
    Cursor *c=get_cursors();

    if (c) {
        Database *db=c->get_db();
        if (db) {
            Backend *be=db->get_backend();
            if (be)
                return be->uncouple_all_cursors(this, start);
        }
    }

    return (HAM_SUCCESS);
}

