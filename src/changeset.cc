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
Changeset::add_page(Page *page)
{
    ScopedLock lock(m_mutex);
    if (page->is_in_list(m_head, Page::LIST_CHANGESET))
        return;

    ham_assert(0==page->get_next(Page::LIST_CHANGESET), (""));
    ham_assert(0==page->get_previous(Page::LIST_CHANGESET), (""));
    ham_assert(page->get_device()->get_env()->get_flags()
                &HAM_ENABLE_RECOVERY, (""));

    page->set_next(Page::LIST_CHANGESET, m_head);
    if (m_head)
        m_head->set_previous(Page::LIST_CHANGESET, page);
    m_head=page;
}

Page *
Changeset::get_page(ham_offset_t pageid)
{
    ScopedLock lock(m_mutex);
    Page *page=m_head;

    while (page) {
        ham_assert(page->get_device()->get_env()->get_flags()
                &HAM_ENABLE_RECOVERY, (""));

        if (page->get_self()==pageid)
            return (page);
        page=page->get_next(Page::LIST_CHANGESET);
    }

    return (0);
}

void
Changeset::clear_nolock()
{
    Page *n, *p=m_head;
    while (p) {
        n=p->get_next(Page::LIST_CHANGESET);
        p->set_next(Page::LIST_CHANGESET, 0);
        p->set_previous(Page::LIST_CHANGESET, 0);
        p=n;
    }
    m_head=0;
}

ham_status_t
Changeset::log_bucket(Page **bucket, ham_size_t bucket_size, 
                      ham_u64_t lsn, ham_size_t &page_count) 
{
    for (ham_size_t i=0; i<bucket_size; i++) {
        ham_assert(bucket[i]->is_dirty(), (""));

        Environment *env=bucket[i]->get_device()->get_env();
        Log *log=env->get_log();

        induce(ErrorInducer::CHANGESET_FLUSH);

        ham_assert(page_count>0, (""));

        ham_status_t st=log->append_page(bucket[i], lsn, --page_count);
        if (st)
            return (st);
    }

    return (0);
}

#define append(b, bs, bc, p)                                                 \
  if (bs+1>=bc) {                                                            \
      bc=bc ? bc*2 : 8;                                                      \
      b=(Page **)::realloc(b, sizeof(void *)*bc);                            \
  }                                                                          \
  b[bs++]=p;

ham_status_t
Changeset::flush(ham_u64_t lsn)
{
    ScopedLock lock(m_mutex);
    ham_status_t st;
    Page *n, *p=m_head;
    ham_size_t page_count=0;

    if (!p)
        return (0);

    induce(ErrorInducer::CHANGESET_FLUSH);

    m_blobs_size=0;
    m_freelists_size=0;
    m_indices_size=0;
    m_others_size=0;

    // first step: remove all pages that are not dirty and sort all others
    // into the buckets
    while (p) {
        n=p->get_next(Page::LIST_CHANGESET);
        if (!p->is_dirty()) {
            p=n;
            continue;
        }

        if (p->is_header()) {
            append(m_indices, m_indices_size, m_indices_capacity, p);
        }
        else if (p->get_flags()&Page::NPERS_NO_HEADER) {
            append(m_blobs, m_blobs_size, m_blobs_capacity, p);
        }
        else {
            switch (p->get_type()) {
              case Page::TYPE_BLOB:
                append(m_blobs, m_blobs_size, m_blobs_capacity, p);
                break;
              case Page::TYPE_B_ROOT:
              case Page::TYPE_B_INDEX:
              case Page::TYPE_HEADER:
                append(m_indices, m_indices_size, m_indices_capacity, p);
                break;
              case Page::TYPE_FREELIST:
                append(m_freelists, m_freelists_size, m_freelists_capacity, p);
                break;
              default:
                append(m_others, m_others_size, m_others_capacity, p);
                break;
            }
        }
        page_count++;
        p=n;

        induce(ErrorInducer::CHANGESET_FLUSH);
    }

    if (page_count==0) {
        induce(ErrorInducer::CHANGESET_FLUSH);
        clear_nolock();
        return (0);
    }

    induce(ErrorInducer::CHANGESET_FLUSH);

    bool log_written=false;

    // if "others" is not empty then log everything because we don't really
    // know what's going on in this operation. otherwise we only need to log
    // if there's more than one page in a bucket:
    //
    // - if there's more than one freelist page modified then the freelist
    //   operation would be huge and we rather not risk to lose that much space
    // - if there's more than one index operation then the operation must 
    //   be atomic
    if (m_others_size || m_indices_size>1 || m_freelists_size>1) {
        if ((st=log_bucket(m_blobs, m_blobs_size, lsn, page_count)))
            return (st);
        if ((st=log_bucket(m_freelists, m_freelists_size, lsn, page_count)))
            return (st);
        if ((st=log_bucket(m_indices, m_indices_size, lsn, page_count)))
            return (st);
        if ((st=log_bucket(m_others, m_others_size, lsn, page_count)))
            return (st);
        log_written=true;
    }

    p=m_head;

    Environment *env=p->get_device()->get_env();
    Log *log=env->get_log();

    /* flush the file handles (if required) */
    if (env->get_flags()&HAM_WRITE_THROUGH && log_written)
        env->get_log()->flush();

    induce(ErrorInducer::CHANGESET_FLUSH);

    // now flush all modified pages to disk
    ham_assert(log!=0, (""));
    ham_assert(env->get_flags()&HAM_ENABLE_RECOVERY, (""));

    /* execute a post-log hook; this hook is set by the unittest framework
     * and can be used to make a backup copy of the logfile */
    if (g_CHANGESET_POST_LOG_HOOK)
        g_CHANGESET_POST_LOG_HOOK();
    
    /* now write all the pages to the file; if any of these writes fail, 
     * we can still recover from the log */
    while (p) {
        st=p->flush();
        if (st)
            return (st);
        p=p->get_next(Page::LIST_CHANGESET);

        induce(ErrorInducer::CHANGESET_FLUSH);
    }

    /* flush the file handle (if required) */
    if (env->get_flags()&HAM_WRITE_THROUGH)
        env->get_device()->flush();

    /* done - we can now clear the changeset and the log */
    clear_nolock();
    return (log->clear());
}

