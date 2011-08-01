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


#define __calc_hash(cache, o)      ((o)%(cache_get_bucketsize(cache)))

ham_cache_t *
cache_new(ham_env_t *env, ham_size_t max_size)
{
    ham_cache_t *cache;
    ham_size_t mem, buckets;

    buckets=CACHE_BUCKET_SIZE;
    ham_assert(buckets, (0));
    ham_assert(max_size, (0));
    mem=sizeof(ham_cache_t)+(buckets-1)*sizeof(void *);

    cache=allocator_calloc(env_get_allocator(env), mem);
    if (!cache)
        return (0);
    cache_set_env(cache, env);
    cache_set_capacity(cache, max_size);
    cache_set_bucketsize(cache, buckets);
    return (cache);
}

void
cache_delete(ham_cache_t *cache)
{
    allocator_free(env_get_allocator(cache_get_env(cache)), cache);
}

ham_page_t * 
cache_get_unused_page(ham_cache_t *cache)
{
    ham_page_t *page;
    ham_page_t *oldest;
    changeset_t *cs=env_get_changeset(cache_get_env(cache));

    /* get the chronologically oldest page */
    oldest=cache_get_totallist_tail(cache);
    if (!oldest)
        return (0);

    /* now iterate through all pages, starting from the oldest
     * (which is the tail of the "totallist", the list of ALL cached 
     * pages) */
    page=oldest;
    do {
        /* pick the first unused page (not in a changeset) */
        if (!page_is_in_list(changeset_get_head(cs), page, PAGE_LIST_CHANGESET))
            break;
        
        page=page_get_previous(page, PAGE_LIST_CACHED);
        ham_assert(page!=oldest, (0));
    } while (page && page!=oldest);
    
    if (!page)
        return (0);

    /* remove the page from the cache and return it */
    cache_remove_page(cache, page);

    return (page);
}

ham_page_t *
cache_get_page(ham_cache_t *cache, ham_offset_t address, ham_u32_t flags)
{
    ham_page_t *page;
    ham_size_t hash=__calc_hash(cache, address);

    page=cache_get_bucket(cache, hash);
    while (page) {
        if (page_get_self(page)==address)
            break;
        page=page_get_next(page, PAGE_LIST_BUCKET);
    }

    /* not found? then return */
    if (!page)
        return (0);

    /* otherwise remove the page from the cache */
    cache_remove_page(cache, page);

    /* if the flag NOREMOVE is set, then re-insert the page. 
     *
     * The remove/insert trick causes the page to be inserted at the
     * head of the "totallist", and therefore it will automatically move
     * far away from the tail. And the pages at the tail are highest 
     * candidates to be deleted when the cache is purged. */
    if (flags&CACHE_NOREMOVE)
        cache_put_page(cache, page);

    return (page);
}

void 
cache_put_page(ham_cache_t *cache, ham_page_t *page)
{
    ham_size_t hash=__calc_hash(cache, page_get_self(page));

    ham_assert(page_get_pers(page), (""));

    /* first remove the page from the cache, if it's already cached
     *
     * we re-insert the page because we want to make sure that the 
     * cache->_totallist_tail pointer is updated and that the page
     * is inserted at the HEAD of the list
     */
    if (page_is_in_list(cache_get_totallist(cache), page, PAGE_LIST_CACHED))
        cache_remove_page(cache, page);

    /* now (re-)insert into the list of all cached pages, and increment
     * the counter */
    ham_assert(!page_is_in_list(cache_get_totallist(cache), page, 
                PAGE_LIST_CACHED), (0));
    cache_set_totallist(cache, 
                page_list_insert(cache_get_totallist(cache), 
                    PAGE_LIST_CACHED, page));

    cache_set_cur_elements(cache, 
                cache_get_cur_elements(cache)+1);

    /*
     * insert it in the cache bucket
     * !!!
     * to avoid inserting the page twice, we first remove it from the 
     * bucket
     */
    if (page_is_in_list(cache_get_bucket(cache, hash), page, PAGE_LIST_BUCKET))
        cache_set_bucket(cache, hash, page_list_remove(cache_get_bucket(cache, 
                    hash), PAGE_LIST_BUCKET, page));
    ham_assert(!page_is_in_list(cache_get_bucket(cache, hash), page, 
                PAGE_LIST_BUCKET), (0));
    cache_get_bucket(cache, hash)=
            page_list_insert(cache_get_bucket(cache, 
                    hash), PAGE_LIST_BUCKET, page);

    /* is this the chronologically oldest page? then set the pointer */
    if (!cache_get_totallist_tail(cache))
        cache_set_totallist_tail(cache, page);

    ham_assert(cache_check_integrity(cache)==0, (""));
}

void 
cache_remove_page(ham_cache_t *cache, ham_page_t *page)
{
    ham_bool_t removed = HAM_FALSE;

    /* are we removing the chronologically oldest page? then 
     * update the pointer with the next oldest page */
    if (cache_get_totallist_tail(cache)==page)
        cache_set_totallist_tail(cache, 
                page_get_previous(page, PAGE_LIST_CACHED));

    /* remove the page from the cache bucket */
    if (page_get_self(page)) {
        ham_size_t hash=__calc_hash(cache, page_get_self(page));
        if (page_is_in_list(cache_get_bucket(cache, hash), page, 
                PAGE_LIST_BUCKET)) {
            cache_set_bucket(cache, hash, 
                    page_list_remove(cache_get_bucket(cache, hash), 
                    PAGE_LIST_BUCKET, page));
        }
    }

    /* remove it from the list of all cached pages */
    if (page_is_in_list(cache_get_totallist(cache), page, PAGE_LIST_CACHED)) {
        cache_set_totallist(cache, page_list_remove(cache_get_totallist(cache), 
                PAGE_LIST_CACHED, page));
        removed = HAM_TRUE;
    }

    /* decrease the number of cached elements */
    if (removed)
        cache_set_cur_elements(cache, cache_get_cur_elements(cache)-1);

    ham_assert(cache_check_integrity(cache)==0, (""));
}

ham_status_t
cache_check_integrity(ham_cache_t *cache)
{
    ham_size_t elements=0;
    ham_page_t *head;
    ham_page_t *tail=cache_get_totallist_tail(cache);

    /* count the cached pages */
    head=cache_get_totallist(cache);
    while (head) {
        elements++;
        head=page_get_next(head, PAGE_LIST_CACHED);
    }

    /* did we count the correct numbers? */
    if (cache_get_cur_elements(cache)!=elements) {
        ham_trace(("cache's number of elements (%u) != actual number (%u)", 
                cache_get_cur_elements(cache), elements));
        return (HAM_INTEGRITY_VIOLATED);
    }

    /* make sure that the totallist HEAD -> next -> TAIL is set correctly,
     * and that the TAIL is the chronologically oldest page */
    head=cache_get_totallist(cache);
    while (head) {
        if (tail && !page_get_next(head, PAGE_LIST_CACHED))
            ham_assert(head==tail, (""));
        head=page_get_next(head, PAGE_LIST_CACHED);
    }
    if (tail)
        ham_assert(page_get_next(tail, PAGE_LIST_CACHED)==0, (""));

    return (0);
}
