/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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
#include "db.h"
#include "errorinducer.h"

#define induce(id)                                                          \
    while (m_inducer) {                                                     \
        ham_status_t st=m_inducer->induce(id);                              \
        if (st)                                                             \
            return (st);                                                    \
        break;                                                              \
    }

/* a unittest hook for Changeset::flush() */
void (*g_CHANGESET_POST_LOG_HOOK)(void);

void
Changeset::add_page(ham_page_t *page)
{
    if (page_is_in_list(m_head, page, PAGE_LIST_CHANGESET))
        return;

    ham_assert(0==page_get_next(page, PAGE_LIST_CHANGESET), (""));
    ham_assert(0==page_get_previous(page, PAGE_LIST_CHANGESET), (""));
    ham_assert((device_get_env(page_get_device(page)))->get_flags()
                &HAM_ENABLE_RECOVERY, (""));

    page_set_next(page, PAGE_LIST_CHANGESET, m_head);
    if (m_head)
        page_set_previous(m_head, PAGE_LIST_CHANGESET, page);
    m_head=page;
}

ham_page_t *
Changeset::get_page(ham_offset_t pageid)
{
    ham_page_t *p=m_head;

    while (p) {
        ham_assert((device_get_env(page_get_device(p)))->get_flags()
                &HAM_ENABLE_RECOVERY, (""));

        if (page_get_self(p)==pageid)
            return (p);
        p=page_get_next(p, PAGE_LIST_CHANGESET);
    }

    return (0);
}

void
Changeset::clear(void)
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
Changeset::log_bucket(bucket &b, ham_u64_t lsn, ham_size_t &page_count) 
{
    for (bucket::iterator it=b.begin(); it!=b.end(); ++it) {
        ham_assert(page_is_dirty(*it), (""));

        Environment *env=device_get_env(page_get_device(*it));
        Log *log=env->get_log();

        induce(ErrorInducer::CHANGESET_FLUSH);

        ham_assert(page_count>0, (""));

        ham_status_t st=log->append_page(*it, lsn, --page_count);
        if (st)
            return (st);
    }

    return (0);
}

ham_status_t
Changeset::flush(ham_u64_t lsn)
{
    ham_status_t st;
    ham_page_t *n, *p=m_head;
    ham_size_t page_count=0;

    induce(ErrorInducer::CHANGESET_FLUSH);

    m_blobs.clear();
    m_freelists.clear();
    m_indices.clear();
    m_others.clear();

    // first step: remove all pages that are not dirty and sort all others
    // into the buckets
    while (p) {
        n=page_get_next(p, PAGE_LIST_CHANGESET);
        if (!page_is_dirty(p)) {
            p=n;
            continue;
        }

        if (page_get_self(p)==0) {
            m_indices.push_back(p);
        }
        else if (page_get_npers_flags(p)&PAGE_NPERS_NO_HEADER) {
            m_blobs.push_back(p);
        }
        else {
            switch (page_get_type(p)) {
              case PAGE_TYPE_BLOB:
                m_blobs.push_back(p);
                break;
              case PAGE_TYPE_B_ROOT:
              case PAGE_TYPE_B_INDEX:
              case PAGE_TYPE_HEADER:
                m_indices.push_back(p);
                break;
              case PAGE_TYPE_FREELIST:
                m_freelists.push_back(p);
                break;
              default:
                m_others.push_back(p);
                break;
            }
        }
        page_count++;
        p=n;

        induce(ErrorInducer::CHANGESET_FLUSH);
    }

    if (page_count==0) {
        induce(ErrorInducer::CHANGESET_FLUSH);
        clear();
        return (0);
    }

    induce(ErrorInducer::CHANGESET_FLUSH);

    // if "others" is not empty then log everything because we don't really
    // know what's going on in this operation. otherwise we only need to log
    // if there's more than one index page
    //
    // otherwise skip blobs and freelists because they're idempotent (albeit
    // it's possible that some data is lost, but that's no big deal)
    if (m_others.size() || m_indices.size()>1) {
        if ((st=log_bucket(m_blobs, lsn, page_count)))
            return (st);
        if ((st=log_bucket(m_freelists, lsn, page_count)))
            return (st);
        if ((st=log_bucket(m_indices, lsn, page_count)))
            return (st);
        if ((st=log_bucket(m_others, lsn, page_count)))
            return (st);
    }

    induce(ErrorInducer::CHANGESET_FLUSH);

    // now flush all modified pages to disk
    p=m_head;

    Environment *env=device_get_env(page_get_device(p));
    Log *log=env->get_log();

    ham_assert(log!=0, (""));
    ham_assert(env->get_flags()&HAM_ENABLE_RECOVERY, (""));

    /* execute a post-log hook; this hook is set by the unittest framework
     * and can be used to make a backup copy of the logfile */
    if (g_CHANGESET_POST_LOG_HOOK)
        g_CHANGESET_POST_LOG_HOOK();
    
    /* now write all the pages to the file; if any of these writes fail, 
     * we can still recover from the log */
    while (p) {
        if (page_is_dirty(p)) {
            st=db_flush_page(env, p);
            if (st)
                return (st);
        }
        p=page_get_next(p, PAGE_LIST_CHANGESET);

        induce(ErrorInducer::CHANGESET_FLUSH);
    }

    /* done - we can now clear the changeset and the log */
    clear();
    return (log->clear());
}

