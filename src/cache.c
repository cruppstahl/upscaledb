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
    cache->_timeslot = 777; /* a reasonable start value; value is related 
                * to the increments applied to active cache pages */
    return (cache);
}

void
cache_delete(ham_cache_t *cache)
{
    allocator_free(env_get_allocator(cache_get_env(cache)), cache);
}

/**
 * Apparently we've hit a high water mark in the counting business and
 * now it's time to cut down those counts to create a bit of fresh
 * headroom.
 *
 * As higher counters represent something akin to a heady mix of young
 * and famous (stardom gets you higher numbers) we're going to do
 * something to age them all, while maintaining their relative ranking:
 *
 * Instead of subtracting a certain amount Z, which would positively
 * benefit the high & mighty (as their distance from the lower life
 * increases disproportionally then), we DIVIDE all counts by a certain
 * number M, so that all counters are scaled down to generate lots of
 * headroom while keeping the picking order as it is.
 *
 * We happen to know that the high water mark is something close to
 * 2^31 - 1K (the largest step up for any page), so we decide to divide
 * by 2^16 - that still leaves us an optimistic resolution of 1:2^16,
 * which is fine.
 */
void
cache_reduce_page_counts(ham_cache_t *cache)
{
    ham_page_t *page=cache_get_totallist(cache);
    while (page) { 
        /* act on ALL pages, including the reference-counted ones.  */
        ham_u32_t count = page_get_cache_cntr(page);

        /* now scale by applying division: */
        count >>= 16;
        page_set_cache_cntr(page, count);

        /* and the next one, please, James. */
        page=page_get_next(page, PAGE_LIST_CACHED);        
    }

    /* and cut down the timeslot value as well: */
    /*
     * to make sure the division by 2^16 keeps the timing counter
     * non-zero, we mix in a little value...
     */
    cache->_timeslot+=(1<<16)-1;
    cache->_timeslot>>=16;
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

/*
 * in order to improve cache activity for access patterns such as
 * AAB.AAB. where a fetch at the '.' would rate both pages A and B as
 * high, we use a increment-counter approach which will cause page A to
 * be rated higher than page B over time as A is accessed/needed more
 * often.
 */
void
cache_update_page_access_counter(ham_page_t *page, ham_cache_t *cache, 
                        ham_u32_t extra_bump)
{
    if (cache->_timeslot > 0xFFFFFFFFU - 1024 - extra_bump)
        cache_reduce_page_counts(cache);

    cache->_timeslot++;
    page_set_cache_cntr(page, cache->_timeslot + extra_bump);
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
