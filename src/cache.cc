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

/**
 * @brief implementation of the cache manager
 *
 */

#include "config.h"

#include <string.h>

#include "cache.h"
#include "env.h"
#include "error.h"
#include "mem.h"
#include "page.h"
#include "changeset.h"


Cache::Cache(Environment *env, ham_u64_t capacity_bytes)
  : m_env(env), m_capacity(capacity_bytes), m_cur_elements(0), m_totallist(0),
    m_totallist_tail(0)
{
    if (m_capacity==0)
        m_capacity=HAM_DEFAULT_CACHESIZE;

    for (ham_size_t i=0; i<CACHE_BUCKET_SIZE; i++)
        m_buckets.push_back(0);
}

ham_status_t
Cache::check_integrity()
{
    ham_size_t elements=0;
    Page *head;
    Page *tail=m_totallist_tail;

    /* count the cached pages */
    head=m_totallist;
    while (head) {
        elements++;
        head=head->get_next(Page::LIST_CACHED);
    }

    /* did we count the correct numbers? */
    if (m_cur_elements!=elements) {
        ham_trace(("cache's number of elements (%u) != actual number (%u)", 
                m_cur_elements, elements));
        return (HAM_INTEGRITY_VIOLATED);
    }

    /* make sure that the totallist HEAD -> next -> TAIL is set correctly,
     * and that the TAIL is the chronologically oldest page */
    head=m_totallist;
    while (head) {
        if (tail && !head->get_next(Page::LIST_CACHED))
            ham_assert(head==tail, (""));
        head=head->get_next(Page::LIST_CACHED);
    }
    if (tail)
        ham_assert(tail->get_next(Page::LIST_CACHED)==0, (""));

    return (0);
}

