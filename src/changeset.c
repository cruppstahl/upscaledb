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
static void (*g_POST_LOG_HOOK)(void);

void
changeset_add_page(changeset_t *cs, ham_page_t *page)
{
    if (page_is_in_list(changeset_get_head(cs), page, PAGE_LIST_CHANGESET))
        return;

    ham_assert(0==page_get_next(page, PAGE_LIST_CHANGESET), (""));
    ham_assert(0==page_get_previous(page, PAGE_LIST_CHANGESET), (""));
    ham_assert(env_get_rt_flags(device_get_env(page_get_device(page)))
                &HAM_ENABLE_RECOVERY, (""));

    page_set_next(page, PAGE_LIST_CHANGESET, changeset_get_head(cs));
    changeset_set_head(cs, page);
}

ham_page_t *
changeset_get_page(changeset_t *cs, ham_offset_t pageid)
{
    ham_page_t *p=changeset_get_head(cs);

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

ham_status_t
changeset_flush(changeset_t *cs, ham_u64_t lsn)
{
    ham_status_t st;
    ham_page_t *p;
    ham_env_t *env;
    ham_log_t *log;

    /* first write all changed pages to the log; if this fails, clear the log
     * again because recovering an incomplete log would break the database 
     * file */
    p=changeset_get_head(cs);
    if (!p)
        return (0);

    env=device_get_env(page_get_device(p));
    log=env_get_log(env);

    ham_assert(log!=0, (""));
    ham_assert(env_get_rt_flags(env)&HAM_ENABLE_RECOVERY, (""));

    while (p) {
        st=log_append_page(log, p, lsn);
        if (st) {
            (void)log_clear(log);
            return (st);
        }
        p=page_get_next(p, PAGE_LIST_CHANGESET);
    }

    /* execute a post-log hook; this hook is set by the unittest framework
     * and can be used to make a backup copy of the logfile */
    if (g_POST_LOG_HOOK)
        g_POST_LOG_HOOK();
    
    /* now write all the pages to the file; if any of these writes fail, 
     * we can still recover from the log */
    p=changeset_get_head(cs);
    while (p) {
        st=db_flush_page(env, p, HAM_WRITE_THROUGH);
        if (st)
            return (st);
        p=page_get_next(p, PAGE_LIST_CHANGESET);
    }

    /* done - we can now clear the log */
    return (log_clear(log));
}

