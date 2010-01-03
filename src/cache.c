/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 * implementation of the cache manager
 *
 */

#include "config.h"

#include <string.h>
#include "cache.h"
#include "mem.h"
#include "page.h"
#include "error.h"
#include "env.h"


#define my_calc_hash(cache, o)                                              \
    ((ham_size_t)(cache_get_max_elements(cache)==0                          \
        ? 0                                                                 \
        : (((o)%(cache_get_bucketsize(cache))))))

ham_cache_t *
cache_new(ham_env_t *env, ham_size_t max_elements)
{
    ham_cache_t *cache;
    ham_size_t mem, buckets;

    buckets=CACHE_BUCKET_SIZE;
    ham_assert(buckets, (0));
    mem=sizeof(ham_cache_t)+(buckets-1)*sizeof(void *);

    cache=ham_mem_calloc_env(env, mem);
    if (!cache)
        return (0);
    if (max_elements == 0 || max_elements > CACHE_MAX_ELEM)
        max_elements = CACHE_MAX_ELEM;
    cache_set_max_elements(cache, max_elements);
    cache_set_bucketsize(cache, buckets);
    cache->_timeslot = 777; /* a reasonable start value; value is related 
                * to the increments applied to active cache pages */
    return (cache);
}

void
cache_delete(ham_env_t *env, ham_cache_t *cache)
{
    allocator_free(env_get_allocator(env), cache);
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

ham_page_t * 
cache_get_unused_page(ham_cache_t *cache)
{
    ham_page_t *page;
    ham_page_t *head;
    ham_page_t *min = 0;
    ham_size_t hash;

    page=cache_get_garbagelist(cache);
    if (page) {
        ham_assert(page_get_refcount(page)==0, 
                ("page is in use and in garbage list"));
        cache_set_garbagelist(cache, 
                page_list_remove(cache_get_garbagelist(cache), 
                    PAGE_LIST_GARBAGE, page));

        cache_push_history(page, -2);

        cache_set_cur_elements(cache, 
                cache_get_cur_elements(cache)-1);
        return (page);
    }

    head=cache_get_totallist(cache);
    if (!head)
        return (0);

    /*
     * Oh, this was all so unfair! <grin>
     *
     * As pages are added at the HEAD and NEXT for page P points to the
     * next older item (i.e. the previously added item to the linked
     * list), it means ->NEXT means
     * 'older'.
     *
     * While, in finding the oldest, re-usable page, we should /start/
     * with the oldest and gradually progress towards the 'younger',
     * i.e. traverse the link list in reverse, by traversing the ->PREV chain 
     * instead of the usual ->NEXT chain!
     *
     * And our 'proper' starting point then would be the
     * /oldest/ fella in the chain, and that would've beem HEAD->PREV
     * if we'd had cyclic double linked lists here; alas, we have NOT,
     * so we just travel down the ->NEXT path and pick the oldest geezer we can
     * find; after all that's one traversal with the same result as first
     * traveling all the way down to the endstop, and then reversing
     * through ->PREV... If only our cyclic LL patch hadn't caused such
     * weird bugs  :-(
     */
    page = head;
    do {
        /* only handle unused pages */
        if (page_get_refcount(page)==0) {
            if (page_get_cache_cntr(page)==0) {
                min=page;
                //goto found_page;
            }
            else {
                if (!min)
                    min=page;
                else
                    if (page_get_cache_cntr(page) <= page_get_cache_cntr(min)) 
                            /* oldest! */
                        min=page;
            }
        }
        page=page_get_next(page, PAGE_LIST_CACHED);
        ham_assert(page != head, (0));
    } while (page && page!=head);
    
    if (!min)
        return (0);

    hash=my_calc_hash(cache, page_get_self(min));

    ham_assert(page_is_in_list(cache_get_totallist(cache), min, 
                    PAGE_LIST_CACHED), (0));
    cache_set_totallist(cache, 
            page_list_remove(cache_get_totallist(cache), 
            PAGE_LIST_CACHED, min));
    ham_assert(page_is_in_list(cache_get_bucket(cache, hash), min, 
                    PAGE_LIST_BUCKET), (0));
    cache_set_bucket(cache, hash, 
            page_list_remove(cache_get_bucket(cache, 
            hash), PAGE_LIST_BUCKET, min));

    cache_push_history(min, -3);

    cache_set_cur_elements(cache, 
            cache_get_cur_elements(cache)-1);

    return (min);
}

ham_page_t *
cache_get_page(ham_cache_t *cache, ham_offset_t address, ham_u32_t flags)
{
    ham_page_t *page;
    ham_size_t hash=my_calc_hash(cache, address);

    page=cache_get_bucket(cache, hash);
    while (page) {
        if (page_get_self(page)==address)
            break;
        page=page_get_next(page, PAGE_LIST_BUCKET);
    }

    if (page && flags != CACHE_NOREMOVE) {
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

        cache_push_history(page, -4);

        cache_set_cur_elements(cache, 
            cache_get_cur_elements(cache)-1);
    }

    return (page);
}

ham_status_t 
cache_put_page(ham_cache_t *cache, ham_page_t *page)
{
    ham_size_t hash=my_calc_hash(cache, page_get_self(page));
    ham_bool_t new_page = HAM_TRUE;

    if (page_is_in_list(cache_get_totallist(cache), page, PAGE_LIST_CACHED)) {
        cache_set_totallist(cache, 
                page_list_remove(cache_get_totallist(cache), 
                PAGE_LIST_CACHED, page));

        new_page = HAM_FALSE;

        cache_set_cur_elements(cache, 
                cache_get_cur_elements(cache)-1);
    }
    ham_assert(!page_is_in_list(cache_get_totallist(cache), page, 
                PAGE_LIST_CACHED), (0));
    cache_set_totallist(cache, 
            page_list_insert(cache_get_totallist(cache), 
            PAGE_LIST_CACHED, page));

    cache_push_history(page, new_page ? +2 : 0);

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

    return (0);
}

/*
 * in order to improve cache activity for access patterns such as
 * AAB.AAB. where a fetch at the '.' would rate both pages A and B as
 * high, we use a increment-counter approach which will cause page A to
 * be rated higher than page B over time as A is accessed/needed more
 * often.
 *
 * Note that page_new() initialized every fresh page with a counter
 * value of 10, so that will be our starting point for our increment.
 * The increment steps are symbolic for the
 * relative importance assigned to each page type; we list a
 * counter-upper-bound for each too to prevent counter overflow for
 * oft-used pages, while the upper-bound also limits the maximum
 * 'importance' a page can achieve within the total set of cached pages:
 * the incremental scheme is mainly meant to allow the cache to detect
 * usage differences between pages of identical/similar type, while the
 * overall importance of each page type should remain a major factor.
 */
void
cache_update_page_access_counter(ham_page_t *page, ham_cache_t *cache)
{
    page_set_cache_cntr(page, cache->_timeslot++);
}

ham_status_t 
cache_remove_page(ham_cache_t *cache, ham_page_t *page)
{
    ham_bool_t removed = HAM_FALSE;

    if (page_get_self(page)) 
    {
        ham_size_t hash=my_calc_hash(cache, page_get_self(page));
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
        cache_push_history(page, -1);

        cache_set_cur_elements(cache, 
                cache_get_cur_elements(cache)-1);
    }

    return (0);
}

ham_bool_t 
cache_too_big(ham_cache_t *cache)
{
    if (cache_get_cur_elements(cache)>=cache_get_max_elements(cache)) 
        return (HAM_TRUE);

    return (HAM_FALSE);
}

#ifdef HAM_ENABLE_INTERNAL
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
#endif /* HAM_ENABLE_INTERNAL */

