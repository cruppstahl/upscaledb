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
        : (((o)&(cache_get_bucketsize(cache)-1))))

static ham_bool_t
my_purge(ham_cache_t *cache)
{
    ham_page_t *page;
    ham_db_t *db=cache_get_owner(cache);
    
    /*
     * get an unused page
     */
    page=cache_get_unused(cache);
    if (!page)
        return (HAM_FALSE);

    /*
     * delete it: remove the page from the totallist, decrement the 
     * page size, write and delete the page
     */
    if (page_is_in_list(cache_get_totallist(cache), page, PAGE_LIST_CACHED)) {
        ham_size_t hash=my_calc_hash(cache, page_get_self(page));

        cache_set_totallist(cache, 
                page_list_remove(cache_get_totallist(cache), 
                PAGE_LIST_CACHED, page));
        cache_set_usedsize(cache, 
                cache_get_usedsize(cache)-db_get_pagesize(db));
        cache_get_bucket(cache, hash)=page_list_remove(cache_get_bucket(cache, 
                hash), PAGE_LIST_BUCKET, page);
    }
    (void)db_write_page_and_delete(cache_get_owner(cache), page, 0);
    /*ham_trace("cache purged one page", 0);*/

    return (HAM_FALSE);
}

ham_cache_t *
cache_new(ham_db_t *db, ham_u32_t flags, ham_size_t cachesize)
{
    ham_cache_t *cache;
    ham_size_t mem, buckets;

    buckets=128; /*(cachesize/db_get_pagesize(db))/2;*/
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
    ham_page_t *page, *head, *min=0;
    ham_size_t hash;

    page=cache_get_garbagelist(cache);
    if (page) {
        ham_assert(page_is_inuse(page)==0, 
                "page is in use and in garbage list", 0);
        cache_set_garbagelist(cache, 
                page_list_remove(cache_get_garbagelist(cache), 
                    PAGE_LIST_GARBAGE, page));
        return (page);
    }

    head=page=cache_get_totallist(cache);
    if (!head)
        return (0);

    do {
        /* only handle unused pages */
        if (!page_is_inuse(page)) {
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
    hash=my_calc_hash(cache, page_get_self(min));

    cache_set_totallist(cache, 
            page_list_remove(cache_get_totallist(cache), 
            PAGE_LIST_CACHED, min));
    cache_set_usedsize(cache, 
            cache_get_usedsize(cache)-
            db_get_pagesize(page_get_owner(min)));
    cache_get_bucket(cache, hash)=
            page_list_remove(cache_get_bucket(cache, 
            hash), PAGE_LIST_BUCKET, min);

    return (min);
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

    return (page);
}

ham_status_t 
cache_put(ham_cache_t *cache, ham_page_t *page)
{
    ham_size_t hash=my_calc_hash(cache, page_get_self(page));
    ham_db_t *db=cache_get_owner(cache);

    /*
     * insert the page in the cachelist
     */
    if (page_is_in_list(cache_get_totallist(cache), page, PAGE_LIST_CACHED)) {
        cache_set_totallist(cache, 
                page_list_remove(cache_get_totallist(cache), 
                PAGE_LIST_CACHED, page));
        cache_set_usedsize(cache, 
                cache_get_usedsize(cache)-db_get_pagesize(db));
    }
    cache_set_totallist(cache, 
            page_list_insert(cache_get_totallist(cache), 
            PAGE_LIST_CACHED, page));
    cache_set_usedsize(cache, 
            cache_get_usedsize(cache)+db_get_pagesize(db));

    /*
     * initialize the cache counter with sane values
     */
    switch (page_get_type(page)) {
      case PAGE_TYPE_HEADER:
          page_set_cache_cntr(page, 1000);
          break;
      case PAGE_TYPE_ROOT:
          page_set_cache_cntr(page, 1000);
          break;
      case PAGE_TYPE_INDEX:
          page_set_cache_cntr(page, 50);
          break;
      case PAGE_TYPE_BLOBHDR:
          page_set_cache_cntr(page, 2);
          break;
      case PAGE_TYPE_BLOBDATA:
          page_set_cache_cntr(page, 0);
          break;
      case PAGE_TYPE_FREELIST:
          /* freelist pages should never be in the cache - fall through */
      default:
          ham_assert(!"unknown page type",
                  "type is 0x%08x", page_get_type(page));
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

    /*
     * check if (in non-strict mode) the cache limits were 
     * overstepped - if yes, try to delete some pages
     */
    if (!(cache_get_flags(cache)&HAM_CACHE_STRICT)) {
        while (cache_get_usedsize(cache)>cache_get_cachesize(cache)) {
            /*
            ham_trace("cache limits overstepped - used size %u, cache "
                    "size %u", cache_get_usedsize(cache), 
                    cache_get_cachesize(cache));
                    */
            if (!my_purge(cache))
                break;
        }
    }

    return (0);
}

ham_status_t 
cache_remove_page(ham_cache_t *cache, ham_page_t *page)
{
    ham_size_t hash;
    ham_db_t *db=cache_get_owner(cache);
    
    if (page_get_self(page)) {
        hash=my_calc_hash(cache, page_get_self(page));
        cache_get_bucket(cache, hash)=page_list_remove(cache_get_bucket(cache, 
                hash), PAGE_LIST_BUCKET, page);
    }

    if (page_is_in_list(cache_get_totallist(cache), page, PAGE_LIST_CACHED)) {
        cache_set_totallist(cache, page_list_remove(cache_get_totallist(cache), 
                PAGE_LIST_CACHED, page));
        cache_set_usedsize(cache, 
                cache_get_usedsize(cache)-db_get_pagesize(db));
    }
    cache_get_garbagelist(cache)=page_list_remove(cache_get_garbagelist(cache), 
                PAGE_LIST_GARBAGE, page);

    return (0);
}

ham_status_t
cache_move_to_garbage(ham_cache_t *cache, ham_page_t *page)
{
    ham_size_t hash=my_calc_hash(cache, page_get_self(page));
    ham_db_t *db=cache_get_owner(cache);

    ham_assert(page_is_inuse(page)==0, "page 0x%lx is in use", 
            page_get_self(page));
    ham_assert(page_is_dirty(page)==0, "page 0x%lx is dirty", 
            page_get_self(page));

    if (page_is_in_list(cache_get_totallist(cache), page, PAGE_LIST_CACHED)) {
        cache_set_totallist(cache, page_list_remove(cache_get_totallist(cache), 
                PAGE_LIST_CACHED, page));
        cache_set_usedsize(cache, 
                cache_get_usedsize(cache)-db_get_pagesize(db));
    }
    cache_get_bucket(cache, hash)=page_list_remove(cache_get_bucket(cache, 
                hash), PAGE_LIST_BUCKET, page);
    cache_get_garbagelist(cache)=page_list_insert(cache_get_garbagelist(cache), 
                PAGE_LIST_GARBAGE, page);

    /*
     * check if (in non-strict mode) the cache limits were 
     * overstepped - if yes, try to delete some pages
     */
    if (!(cache_get_flags(cache)&HAM_CACHE_STRICT)) {
        while (cache_get_usedsize(cache)>cache_get_cachesize(cache)) {
            ham_trace("cache limits overstepped - used size %u, cache "
                    "size %u", cache_get_usedsize(cache), 
                    cache_get_cachesize(cache));
            if (!my_purge(cache))
                break;
        }
    }

    return (0);
}

ham_status_t 
cache_flush_and_delete(ham_cache_t *cache, ham_u32_t flags)
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

            /*
             * also delete the page from the totallist and decrement 
             * the cache size
             */
            ham_assert(page_is_in_list(cache_get_totallist(cache), 
                        head, PAGE_LIST_CACHED), "page is not in totallist", 0);
            cache_set_totallist(cache, 
                    page_list_remove(cache_get_totallist(cache), 
                    PAGE_LIST_CACHED, head));
            cache_set_usedsize(cache, 
                    cache_get_usedsize(cache)-db_get_pagesize(db));

            /*
             * now delete the page
             *
             * TODO 
             * ignoring the error - not sure if this is the best idea...
             */
            st=db_write_page_and_delete(db, head, flags);
            if (st) 
                ham_log("failed to flush page (%d) - ignoring error...", st);
            head=next;
        }

        cache_set_bucket(cache, i, 0);
    }

    /*
     * clear the cached-list and the garbage-list
     */
    cache_set_totallist(cache, 0);
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

ham_bool_t 
cache_can_add_page(ham_cache_t *cache)
{
    ham_db_t *db=cache_get_owner(cache);

    /*
     * if usedsize < cachesize: it's ok to allocate another page
     */
    if (cache_get_usedsize(cache)+db_get_pagesize(db)<=
            cache_get_cachesize(cache)) 
        return (HAM_TRUE);

    /*
     * otherwise it's only allowed if policy!=STRICT
     */
    if (cache_get_flags(cache)&HAM_CACHE_STRICT) 
        return (HAM_FALSE);

    return (HAM_TRUE);
}

ham_status_t
cache_check_integrity(ham_cache_t *cache)
{
    ham_size_t usedsize=0;
    ham_page_t *head;

    /*
     * enumerate cached pages of the cache
     */
    head=cache_get_totallist(cache);
    while (head) {
        usedsize+=db_get_pagesize(cache_get_owner(cache));
        head=page_get_next(head, PAGE_LIST_CACHED);
    }

    /*
     * compare page size
     */
    if (cache_get_usedsize(cache)!=usedsize) {
        ham_trace("cache's usedsize (%d) != actual size (%d)", 
                cache_get_usedsize(cache), usedsize);
        return (HAM_INTEGRITY_VIOLATED);
    }

    /*
     * check if the cache is exhausted
     */
    if (cache_get_flags(cache)&HAM_CACHE_STRICT) {
        if (cache_get_usedsize(cache)>cache_get_cachesize(cache)) {
            ham_trace("cache's usedsize (%d) > maximum size (%d)", 
                    cache_get_usedsize(cache), cache_get_cachesize(cache));
            return (HAM_INTEGRITY_VIOLATED);
        }
    }

    return (0);
}
