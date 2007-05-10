/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 * implementation of the cache manager
 *
 */

#include <string.h>
#include "cache.h"
#include "mem.h"
#include "db.h"
#include "page.h"
#include "error.h"

#define BUCKET_SIZE     256

#define my_calc_hash(cache, o)                                              \
    (cache_get_max_elements(cache)==0                                       \
        ? 0                                                                 \
        : (((o)&(cache_get_bucketsize(cache)-1))))

ham_cache_t *
cache_new(ham_db_t *db, ham_size_t max_elements)
{
    ham_cache_t *cache;
    ham_size_t mem, buckets;

    buckets=BUCKET_SIZE;
    if (!buckets)
        buckets=1;
    mem=sizeof(ham_cache_t)+(buckets-1)*sizeof(void *);

    cache=ham_mem_alloc(db, mem);
    if (!cache) {
        db_set_error(db, HAM_OUT_OF_MEMORY);
        return (0);
    }
    memset(cache, 0, mem);
    cache_set_max_elements(cache, max_elements);
    cache_set_bucketsize(cache, buckets);
    return (cache);
}

void
cache_delete(ham_db_t *db, ham_cache_t *cache)
{
    ham_mem_free(db, cache);
}

ham_page_t * 
cache_get_unused_page(ham_cache_t *cache)
{
    ham_page_t *page, *head, *min=0;
    ham_size_t hash;

    page=cache_get_garbagelist(cache);
    if (page) {
        ham_assert(page_get_refcount(page)==0, 
                ("page is in use and in garbage list"));
        cache_set_garbagelist(cache, 
                page_list_remove(cache_get_garbagelist(cache), 
                    PAGE_LIST_GARBAGE, page));
        cache_set_cur_elements(cache, 
                cache_get_cur_elements(cache)-1);
        return (page);
    }

    head=page=cache_get_totallist(cache);
    if (!head)
        return (0);

    do {
        /* only handle unused pages */
        if (page_get_refcount(page)==0) {
            if (page_get_cache_cntr(page)==0) {
                min=page;
                goto found_page;
            }
            else {
                if (!min)
                    min=page;
                else
                    if (page_get_cache_cntr(page)<page_get_cache_cntr(min))
                        min=page;
            }
            page_set_cache_cntr(page, page_get_cache_cntr(page)-1);
        }
        page=page_get_next(page, PAGE_LIST_CACHED);
    } while (page && page!=head);
    
    if (!min)
        return (0);

found_page:
    hash=(ham_size_t)my_calc_hash(cache, page_get_self(min));

    cache_set_totallist(cache, 
            page_list_remove(cache_get_totallist(cache), 
            PAGE_LIST_CACHED, min));
    cache_set_cur_elements(cache, 
            cache_get_cur_elements(cache)-1);
    cache_get_bucket(cache, hash)=
            page_list_remove(cache_get_bucket(cache, 
            hash), PAGE_LIST_BUCKET, min);

    return (min);
}

ham_page_t *
cache_get_page(ham_cache_t *cache, ham_offset_t address)
{
    ham_page_t *page=0;
    ham_size_t hash=(ham_size_t)my_calc_hash(cache, address);

    page=cache_get_bucket(cache, hash);
    while (page) {
        if (page_get_self(page)==address)
            break;
        page=page_get_next(page, PAGE_LIST_BUCKET);
    }

    if (page) {
        cache_set_totallist(cache, 
            page_list_remove(cache_get_totallist(cache), 
            PAGE_LIST_CACHED, page));
        cache_set_cur_elements(cache, 
            cache_get_cur_elements(cache)-1);
        cache_get_bucket(cache, hash)=
            page_list_remove(cache_get_bucket(cache, 
            hash), PAGE_LIST_BUCKET, page);
    }

    return (page);
}

ham_status_t 
cache_put_page(ham_cache_t *cache, ham_page_t *page)
{
    ham_size_t hash=(ham_size_t)my_calc_hash(cache, page_get_self(page));

    if (page_is_in_list(cache_get_totallist(cache), page, PAGE_LIST_CACHED)) {
        cache_set_totallist(cache, 
                page_list_remove(cache_get_totallist(cache), 
                PAGE_LIST_CACHED, page));
        cache_set_cur_elements(cache, 
                cache_get_cur_elements(cache)-1);
    }
    cache_set_totallist(cache, 
            page_list_insert(cache_get_totallist(cache), 
            PAGE_LIST_CACHED, page));
    cache_set_cur_elements(cache, 
            cache_get_cur_elements(cache)+1);

    /* initialize the cache counter with sane values */
    if (page_get_npers_flags(page)&PAGE_NPERS_NO_HEADER) {
          page_set_cache_cntr(page, 10);
    }
    else {
        switch (page_get_type(page)) {
          case PAGE_TYPE_HEADER:
              page_set_cache_cntr(page, 1000);
              break;
          case PAGE_TYPE_B_ROOT:
              page_set_cache_cntr(page, 1000);
              break;
          case PAGE_TYPE_B_INDEX:
          case PAGE_TYPE_FREELIST:
              page_set_cache_cntr(page, 50);
              break;
          default:
              /* ignore all other pages (most likely they're blobs) */
              break;
        }
    }

    /*
     * insert it in the cache bucket
     * !!!
     * to avoid inserting the page twice, we first remove it from the 
     * bucket
     */
    cache_get_bucket(cache, hash)=page_list_remove(cache_get_bucket(cache, 
                hash), PAGE_LIST_BUCKET, page);
    cache_get_bucket(cache, hash)=page_list_insert(cache_get_bucket(cache, 
                hash), PAGE_LIST_BUCKET, page);

    return (0);
}

ham_status_t 
cache_remove_page(ham_cache_t *cache, ham_page_t *page)
{
    ham_size_t hash;
    
    if (page_get_self(page)) {
        hash=(ham_size_t)my_calc_hash(cache, page_get_self(page));
        cache_get_bucket(cache, hash)=page_list_remove(cache_get_bucket(cache, 
                hash), PAGE_LIST_BUCKET, page);
    }

    if (page_is_in_list(cache_get_totallist(cache), page, PAGE_LIST_CACHED)) {
        cache_set_totallist(cache, page_list_remove(cache_get_totallist(cache), 
                PAGE_LIST_CACHED, page));
        cache_set_cur_elements(cache, 
                cache_get_cur_elements(cache)-1);
    }
    cache_get_garbagelist(cache)=page_list_remove(cache_get_garbagelist(cache), 
                PAGE_LIST_GARBAGE, page);

    return (0);
}

ham_status_t
cache_move_to_garbage(ham_cache_t *cache, ham_page_t *page)
{
    ham_size_t hash=(ham_size_t)my_calc_hash(cache, page_get_self(page));

    ham_assert(page_get_refcount(page)==0, 
            ("page 0x%lx is in use", page_get_self(page)));
    ham_assert(page_is_dirty(page)==0, 
            ("page 0x%lx is dirty", page_get_self(page)));

    if (page_is_in_list(cache_get_totallist(cache), page, PAGE_LIST_CACHED)) {
        cache_set_totallist(cache, page_list_remove(cache_get_totallist(cache), 
                PAGE_LIST_CACHED, page));
    }
    cache_get_bucket(cache, hash)=page_list_remove(cache_get_bucket(cache, 
                hash), PAGE_LIST_BUCKET, page);
    cache_get_garbagelist(cache)=page_list_insert(cache_get_garbagelist(cache), 
                PAGE_LIST_GARBAGE, page);

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

