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
#include "env.h"
#include "log.h"
#include "device.h"

/* a unittest hook for changeset_flush() */
void (*g_CHANGESET_POST_LOG_HOOK)(void);

void
changeset_t::add_page(ham_page_t *page)
{
    if (page_is_in_list(m_head, page, PAGE_LIST_CHANGESET))
        return;

    ham_assert(0==page_get_next(page, PAGE_LIST_CHANGESET), (""));
    ham_assert(0==page_get_previous(page, PAGE_LIST_CHANGESET), (""));
    ham_assert(env_get_rt_flags(device_get_env(page_get_device(page)))
                &HAM_ENABLE_RECOVERY, (""));

    page_set_next(page, PAGE_LIST_CHANGESET, m_head);
    if (m_head)
        page_set_previous(m_head, PAGE_LIST_CHANGESET, page);
    m_head=page;
}

ham_page_t *
changeset_t::get_page(ham_offset_t pageid)
{
    ham_page_t *p=m_head;

    while (p) {
    	ham_assert(env_get_rt_flags(device_get_env(page_get_device(p)))
                &HAM_ENABLE_RECOVERY, (""));

        if (page_get_self(p)==pageid)
            return (p);
        p=page_get_next(p, PAGE_LIST_CHANGESET);
    }

    return (0);
}

void
changeset_t::clear(void)
{
    ham_page_t *n, *p=m_head;
    while (p) {
        n=page_get_next(p, PAGE_LIST_CHANGESET);

        page_set_next(p, PAGE_LIST_CHANGESET, 0);
        page_set_previous(p, PAGE_LIST_CHANGESET, 0);
        p=n;
    }
    m_head=0;
}

ham_status_t
changeset_t::flush(ham_u64_t lsn)
{
    ham_status_t st;
    ham_page_t *p;
    ham_env_t *env;
    ham_log_t *log;

    /* first write all changed pages to the log; if this fails, clear the log
     * again because recovering an incomplete log would break the database 
     * file */
    p=m_head;
    if (!p)
        return (0);

    env=device_get_env(page_get_device(p));
    log=env_get_log(env);

    ham_assert(log!=0, (""));
    ham_assert(env_get_rt_flags(env)&HAM_ENABLE_RECOVERY, (""));

    while (p) {
        if (page_is_dirty(p)) {
            st=log->append_page(p, lsn);
            if (st) {
                log->clear();
                return (st);
            }
        }
        p=page_get_next(p, PAGE_LIST_CHANGESET);
    }

    /* execute a post-log hook; this hook is set by the unittest framework
     * and can be used to make a backup copy of the logfile */
    if (g_CHANGESET_POST_LOG_HOOK)
        g_CHANGESET_POST_LOG_HOOK();
    
    /* now write all the pages to the file; if any of these writes fail, 
     * we can still recover from the log */
    p=m_head;
    while (p) {
        if (page_is_dirty(p)) {
            st=db_flush_page(env, p, HAM_WRITE_THROUGH);
            if (st)
                return (st);
        }
        p=page_get_next(p, PAGE_LIST_CHANGESET);
    }

    /* done - we can now clear the changeset and the log */
    clear();
    return (log->clear());
}

