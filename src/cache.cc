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


#define __calc_hash(o)      ((o)%(CACHE_BUCKET_SIZE))


ham_cache_t::ham_cache_t(ham_env_t *env, ham_size_t capacity_bytes)
  : m_env(env), m_capacity(capacity_bytes), m_cur_elements(0), m_totallist(0),
    m_totallist_tail(0)
{
    if (m_capacity==0)
        m_capacity=HAM_DEFAULT_CACHESIZE;

    for (ham_size_t i=0; i<CACHE_BUCKET_SIZE; i++)
        m_buckets.push_back(0);
}

ham_page_t * 
ham_cache_t::get_unused_page(void)
{
    ham_page_t *page;
    ham_page_t *oldest;
    changeset_t &cs=env_get_changeset(m_env);

    /* get the chronologically oldest page */
    oldest=m_totallist_tail;
    if (!oldest)
        return (0);

    /* now iterate through all pages, starting from the oldest
     * (which is the tail of the "totallist", the list of ALL cached 
     * pages) */
    page=oldest;
    do {
        /* pick the first unused page (not in a changeset) */
        if (!page_is_in_list(cs.get_head(), page, PAGE_LIST_CHANGESET))
            break;
        
        page=page_get_previous(page, PAGE_LIST_CACHED);
        ham_assert(page!=oldest, (0));
    } while (page && page!=oldest);
    
    if (!page)
        return (0);

    /* remove the page from the cache and return it */
    remove_page(page);

    return (page);
}

ham_page_t *
ham_cache_t::get_page(ham_offset_t address, ham_u32_t flags)
{
    ham_page_t *page;
    ham_size_t hash=__calc_hash(address);

    page=m_buckets[hash];
    while (page) {
        if (page_get_self(page)==address)
            break;
        page=page_get_next(page, PAGE_LIST_BUCKET);
    }

    /* not found? then return */
    if (!page)
        return (0);

    /* otherwise remove the page from the cache */
    remove_page(page);

    /* if the flag NOREMOVE is set, then re-insert the page. 
     *
     * The remove/insert trick causes the page to be inserted at the
     * head of the "totallist", and therefore it will automatically move
     * far away from the tail. And the pages at the tail are highest 
     * candidates to be deleted when the cache is purged. */
    if (flags&ham_cache_t::NOREMOVE)
        put_page(page);

    return (page);
}

void 
ham_cache_t::put_page(ham_page_t *page)
{
    ham_size_t hash=__calc_hash(page_get_self(page));

    ham_assert(page_get_pers(page), (""));

    /* first remove the page from the cache, if it's already cached
     *
     * we re-insert the page because we want to make sure that the 
     * cache->_totallist_tail pointer is updated and that the page
     * is inserted at the HEAD of the list
     */
    if (page_is_in_list(m_totallist, page, PAGE_LIST_CACHED))
        remove_page(page);

    /* now (re-)insert into the list of all cached pages, and increment
     * the counter */
    ham_assert(!page_is_in_list(m_totallist, page, PAGE_LIST_CACHED), (0));
    m_totallist=page_list_insert(m_totallist, PAGE_LIST_CACHED, page);

    m_cur_elements++;

    /*
     * insert it in the cache buckets
     * !!!
     * to avoid inserting the page twice, we first remove it from the 
     * bucket
     */
    if (page_is_in_list(m_buckets[hash], page, PAGE_LIST_BUCKET))
        m_buckets[hash]=page_list_remove(m_buckets[hash], 
                        PAGE_LIST_BUCKET, page);
    ham_assert(!page_is_in_list(m_buckets[hash], page, 
                PAGE_LIST_BUCKET), (0));
    m_buckets[hash]=page_list_insert(m_buckets[hash], 
                PAGE_LIST_BUCKET, page);

    /* is this the chronologically oldest page? then set the pointer */
    if (!m_totallist_tail)
        m_totallist_tail=page;

    ham_assert(check_integrity()==0, (""));
}

void 
ham_cache_t::remove_page(ham_page_t *page)
{
    ham_bool_t removed = HAM_FALSE;

    /* are we removing the chronologically oldest page? then 
     * update the pointer with the next oldest page */
    if (m_totallist_tail==page)
        m_totallist_tail=page_get_previous(page, PAGE_LIST_CACHED);

    /* remove the page from the cache buckets */
    if (page_get_self(page)) {
        ham_size_t hash=__calc_hash(page_get_self(page));
        if (page_is_in_list(m_buckets[hash], page, 
                PAGE_LIST_BUCKET)) {
            m_buckets[hash]=page_list_remove(m_buckets[hash], 
                    PAGE_LIST_BUCKET, page);
        }
    }

    /* remove it from the list of all cached pages */
    if (page_is_in_list(m_totallist, page, PAGE_LIST_CACHED)) {
        m_totallist=page_list_remove(m_totallist, PAGE_LIST_CACHED, page);
        removed = HAM_TRUE;
    }

    /* decrease the number of cached elements */
    if (removed)
        m_cur_elements--;

    ham_assert(check_integrity()==0, (""));
}

ham_status_t
ham_cache_t::check_integrity(void)
{
    ham_size_t elements=0;
    ham_page_t *head;
    ham_page_t *tail=m_totallist_tail;

    /* count the cached pages */
    head=m_totallist;
    while (head) {
        elements++;
        head=page_get_next(head, PAGE_LIST_CACHED);
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
        if (tail && !page_get_next(head, PAGE_LIST_CACHED))
            ham_assert(head==tail, (""));
        head=page_get_next(head, PAGE_LIST_CACHED);
    }
    if (tail)
        ham_assert(page_get_next(tail, PAGE_LIST_CACHED)==0, (""));

    return (0);
}

