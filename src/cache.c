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
    ham_page_t *page;

    if (!cache)
        return;

    page = cache_get_totallist(cache);
    while (page)
    { 
        /* act on ALL pages, including the reference-counted ones.  */
        ham_u32_t count = page_get_cache_cntr(page);

        /* now scale by applying division: */
        count >>= 16;
        page_set_cache_cntr(page, count);

        /* and the next one, please, James. */
        page = page_get_next(page, PAGE_LIST_CACHED);        
    }

    /* and cut down the timeslot value as well: */
    /*
     * to make sure the division by 2^16 keeps the timing counter
     * non-zero, we mix in a little value...
     */
    cache->_timeslot+=(1<<16)-1;
    cache->_timeslot>>=16;
}

ham_status_t
cache_purge(ham_cache_t *cache)
{
    ham_page_t *page;
    ham_page_t *oldest;
    ham_page_t *next_start;
    ham_status_t st;

#if 0
    page=cache_get_garbagelist(cache);
    if (page) {
        cache_set_garbagelist(cache, 
                page_list_remove(cache_get_garbagelist(cache), 
                    PAGE_LIST_GARBAGE, page));

        cache_set_cur_elements(cache, 
                cache_get_cur_elements(cache)-1);
        return (page);
    }
#endif

    /* we start with the oldest page */
    oldest=cache_get_oldest(cache);
    if (!oldest)
        return (0);

    next_start = oldest;
    do  {
        ham_page_t *min=0;
        page = next_start;
        ham_size_t i=0;
        do {
            /* do not loop too often */
            if (i++>15)
                break;

            if (page_get_cache_cntr(page)==0) {
                min=page;
                break;
            }
            else {
                if (!min)
                    min=page;
                else if (page_get_cache_cntr(page) 
                        <= page_get_cache_cntr(min)) 
                    min=page;
            }
            
            page = page_get_previous(page, PAGE_LIST_CACHED);
            ham_assert(page != oldest, (0));
        } while (page && page!=oldest);

        /* if we did not find a page: return */
        if (!min)
            return (0);

        /* this is where we continue in our loop */
        next_start = page_get_previous(min, PAGE_LIST_CACHED);

        /* otherwise remove the page from the cache... */
        st=cache_remove_page(cache, min);
        if (st)
            return (st);

        /* ... and then flush/free/delete it */
        st=db_write_page_and_delete(min, 0);
        if (st)
            return st;

    } while (next_start && cache_too_big(cache));
    
    return (0);
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

    if (page && flags != CACHE_NOREMOVE) {
        if (cache_get_oldest(cache)==page) {
            cache_set_oldest(cache, page_get_next(page, PAGE_LIST_CACHED));
        }
        if (page_is_in_list(cache_get_totallist(cache), page, PAGE_LIST_CACHED))
        {
            cache_set_totallist(cache, 
                page_list_remove(cache_get_totallist(cache), 
                PAGE_LIST_CACHED, page));
        }
        ham_assert(page_is_in_list(cache_get_bucket(cache, hash), page, 
                PAGE_LIST_BUCKET), (0));
        cache_set_bucket(cache, hash,
            page_list_remove(cache_get_bucket(cache, 
            hash), PAGE_LIST_BUCKET, page));

        cache_set_cur_elements(cache, 
            cache_get_cur_elements(cache)-1);
    }

    return (page);
}

void 
cache_put_page(ham_cache_t *cache, ham_page_t *page)
{
    ham_size_t hash=__calc_hash(cache, page_get_self(page));

    ham_assert(page_get_pers(page), (""));
    ham_assert(page_get_self(page), ("don't put header page in the cache!"));

    if (page_is_in_list(cache_get_totallist(cache), page, PAGE_LIST_CACHED)) {
        cache_set_totallist(cache, 
                page_list_remove(cache_get_totallist(cache), 
                PAGE_LIST_CACHED, page));

        cache_set_cur_elements(cache, 
                cache_get_cur_elements(cache)-1);
    }
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
    {
        cache_set_bucket(cache, hash, page_list_remove(cache_get_bucket(cache, 
                    hash), PAGE_LIST_BUCKET, page));
    }
    ham_assert(!page_is_in_list(cache_get_bucket(cache, hash), page, 
                PAGE_LIST_BUCKET), (0));
    cache_get_bucket(cache, hash)=page_list_insert(cache_get_bucket(cache, 
                hash), PAGE_LIST_BUCKET, page);

    if (!cache_get_oldest(cache)) {
        cache_set_oldest(cache, page);
    }
}

/*
 * in order to improve cache activity for access patterns such as
 * AAB.AAB. where a fetch at the '.' would rate both pages A and B as
 * high, we use a increment-counter approach which will cause page A to
 * be rated higher than page B over time as A is accessed/needed more
 * often.
 */
void
cache_update_page_access_counter(ham_page_t *page, ham_cache_t *cache, ham_u32_t extra_bump)
{
	if (cache->_timeslot > 0xFFFFFFFFU - 1024 - extra_bump)
	{
		cache_reduce_page_counts(cache);
	}
	cache->_timeslot++;
    page_set_cache_cntr(page, cache->_timeslot + extra_bump);
}

ham_status_t 
cache_remove_page(ham_cache_t *cache, ham_page_t *page)
{
    ham_bool_t removed = HAM_FALSE;

    if (cache_get_oldest(cache)==page) {
        cache_set_oldest(cache, page_get_next(page, PAGE_LIST_CACHED));
    }

    if (page_get_self(page)) {
        ham_size_t hash=__calc_hash(cache, page_get_self(page));
        if (page_is_in_list(cache_get_bucket(cache, hash), page, 
                PAGE_LIST_BUCKET)) {
            cache_set_bucket(cache, hash, 
                    page_list_remove(cache_get_bucket(cache, hash), 
                        PAGE_LIST_BUCKET, page));
        }
    }

    if (page_is_in_list(cache_get_totallist(cache), page, PAGE_LIST_CACHED)) {
        cache_set_totallist(cache, page_list_remove(cache_get_totallist(cache), 
                PAGE_LIST_CACHED, page));
        removed = HAM_TRUE;
    }
    if (page_is_in_list(cache_get_garbagelist(cache), page, PAGE_LIST_GARBAGE)){
        cache_set_garbagelist(cache, 
                    page_list_remove(cache_get_garbagelist(cache), 
                    PAGE_LIST_GARBAGE, page));
        removed = HAM_TRUE;
    }
    if (removed) {
        cache_set_cur_elements(cache, 
                cache_get_cur_elements(cache)-1);
    }

    return (0);
}

ham_status_t
cache_check_integrity(ham_cache_t *cache)
{
    ham_size_t elements=0;
    ham_page_t *head;

    /* count the cached pages */
    head=cache_get_totallist(cache);
    while (head) {
        elements++;
        head=page_get_next(head, PAGE_LIST_CACHED);
    }
    head=cache_get_garbagelist(cache);
    while (head) {
        elements++;
        head=page_get_next(head, PAGE_LIST_GARBAGE);
    }

    /* did we count the correct numbers? */
    if (cache_get_cur_elements(cache)!=elements) {
        ham_trace(("cache's number of elements (%u) != actual number (%u)", 
                cache_get_cur_elements(cache), elements));
        return (HAM_INTEGRITY_VIOLATED);
    }

    return (0);
}
