/**
 * Copyright 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
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

#define my_calc_hash(cache, o)                                              \
    (cache_get_cachesize(cache)==0                                          \
        ? 0                                                                 \
        : (((o-SIZEOF_PERS_HEADER)/db_get_pagesize(cache_get_owner(cache))) \
            %cache_get_bucketsize(cache)))


ham_cache_t *
cache_new(ham_db_t *db, ham_u32_t flags, ham_size_t cachesize)
{
    ham_cache_t *cache;
    ham_size_t mem, buckets;

    buckets=(cachesize/db_get_pagesize(db))/4;
    if (!buckets)
        buckets=1;
    mem=sizeof(ham_cache_t)+(buckets-1)*sizeof(void *);

    cache=ham_mem_alloc(mem);
    if (!cache) {
        db_set_error(db, HAM_OUT_OF_MEMORY);
        return (0);
    }
    memset(cache, 0, mem);
    cache_set_owner(cache, db);
    cache_set_flags(cache, flags);
    cache_set_cachesize(cache, cachesize);
    cache_set_bucketsize(cache, buckets);
    return (cache);
}

void
cache_delete(ham_cache_t *cache)
{
    ham_mem_free(cache);
}

ham_page_t * 
cache_get_unused(ham_cache_t *cache)
{
    ham_page_t *page;

    page=cache_get_garbagelist(cache);
    if (page) {
        cache_set_garbagelist(cache, 
                page_list_remove(cache_get_garbagelist(cache), 
                    PAGE_LIST_GARBAGE, page));
        return (page);
    }

    page=cache_get_unreflist(cache);
    if (page) {
        ham_size_t hash=my_calc_hash(cache, page_get_self(page));

        cache_get_bucket(cache, hash)=page_list_remove(cache_get_bucket(cache, 
                hash), PAGE_LIST_BUCKET, page);
        cache_set_unreflist(cache, 
                page_list_remove(cache_get_unreflist(cache), 
                    PAGE_LIST_UNREF, page));
        return (page);
    }

    return (0);
}

ham_page_t *
cache_get(ham_cache_t *cache, ham_offset_t address)
{
    ham_page_t *page=0;
    ham_size_t hash=my_calc_hash(cache, address);

    page=cache_get_bucket(cache, hash);
    while (page) {
        if (page_get_self(page)==address)
            break;
        page=page_get_next(page, PAGE_LIST_BUCKET);
    }

    if (page) {
        cache_set_unreflist(cache, 
                page_list_remove(cache_get_unreflist(cache), 
                    PAGE_LIST_UNREF, page));
    }

    if (page)
        ham_assert(!(page_get_npers_flags(page)&PAGE_NPERS_LOCKED), 
            "trying to get a locked page", 0);

    return (page);
}

ham_status_t 
cache_put(ham_cache_t *cache, ham_page_t *page)
{
    ham_size_t hash=my_calc_hash(cache, page_get_self(page));

    ham_assert(!(page_get_npers_flags(page)&PAGE_NPERS_LOCKED), 
            "trying to put a locked page", 0);

    /*
     * if the page is not referenced: insert it to the unreflist
     */
    if (page_ref_get(page)==0) {
        cache_set_unreflist(cache, page_list_insert(cache_get_unreflist(cache), 
                PAGE_LIST_UNREF, page));
    }

    /*
     * insert it in the cache bucket
     * !!!
     * to avoid inserting the page twice, we remove it from the 
     * bucket
     */
    cache_get_bucket(cache, hash)=page_list_remove(cache_get_bucket(cache, 
                hash), PAGE_LIST_BUCKET, page);
    cache_get_bucket(cache, hash)=page_list_insert(cache_get_bucket(cache, 
                hash), PAGE_LIST_BUCKET, page);

    return (0);
}

ham_status_t
cache_move_to_garbage(ham_cache_t *cache, ham_page_t *page)
{
    ham_size_t hash=my_calc_hash(cache, page_get_self(page));

    ham_assert(page_ref_get(page)==0, "refcount of page 0x%lx is %d", 
            page_get_self(page), page_ref_get(page));

    cache_get_bucket(cache, hash)=page_list_remove(cache_get_bucket(cache, 
                hash), PAGE_LIST_BUCKET, page);
    cache_get_garbagelist(cache)=page_list_insert(cache_get_garbagelist(cache), 
                PAGE_LIST_GARBAGE, page);

    return (0);
}

ham_status_t 
cache_flush_and_delete(ham_cache_t *cache)
{
    ham_size_t i;
    ham_status_t st;
    ham_page_t *head;
    ham_db_t *db=cache_get_owner(cache);

    /*
     * for each bucket in the hash table
     */
    for (i=0; i<cache_get_bucketsize(cache); i++) {
        /*
         * flush all pages in the bucket, and delete the page
         */
        head=cache_get_bucket(cache, i);
        while (head) {
            ham_page_t *next=page_get_next(head, PAGE_LIST_BUCKET);
            st=db_write_page_and_delete(db, head);
            if (st) 
                ham_log("failed to flush page (%d) - ignoring error...", st);
            head=next;
        }
        cache_set_bucket(cache, i, 0);
    }

    /*
     * clear the unref-list and the garbage-list
     */
    cache_set_unreflist(cache, 0);
    cache_set_garbagelist(cache, 0);

    return (0);
}

void 
cache_dump(ham_db_t *db)
{
    ham_size_t i;
    ham_page_t *head;
    ham_cache_t *cache=db_get_cache(db);

    ham_log("cache_dump ---------------------------------------------", 0);

    /*
     * for each bucket in the hash table
     */
    for (i=0; i<cache_get_bucketsize(cache); i++) {
        /*
         * dump the page info
         */
        head=cache_get_bucket(cache, i);
        while (head) {
            ham_log("    %02d: page %lld", i, page_get_self(head));
            head=page_get_next(head, PAGE_LIST_BUCKET);
        }
    }
}
