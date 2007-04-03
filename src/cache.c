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
    (cache_get_cachesize(cache)==0                                          \
        ? 0                                                                 \
        : (((o)&(cache_get_bucketsize(cache)-1))))

#if HAM_DEBUG
static void
my_check_totallist(ham_cache_t *cache)
{
#if 0
    ham_page_t *page, *p, *n;

    page=cache_get_totallist(cache);
    while (page) {
        p=page_get_previous(page, PAGE_LIST_CACHED);
        while (p) {
            if (p==page)
                break;
            ham_assert(page_get_self(p)!=page_get_self(page), 
                    "page %llu is a dupe!", page_get_self(page));
            p=page_get_previous(p, PAGE_LIST_CACHED);
        }

        n=page_get_next(page, PAGE_LIST_CACHED);
        while (n) {
            if (n==page)
                break;
            ham_assert(page_get_self(n)!=page_get_self(page), 
                    "page %llu is a dupe!", page_get_self(page));
            n=page_get_previous(n, PAGE_LIST_CACHED);
        }

        page=page_get_next(page, PAGE_LIST_CACHED);
    }
#endif
}
#endif

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

    ham_assert(page_get_inuse(page)<=1, (0));

    /*
     * delete it: remove the page from the totallist, decrement the 
     * page size, write and delete the page
     */
    if (page_is_in_list(cache_get_totallist(cache), page, PAGE_LIST_CACHED)) {
        ham_size_t hash=(ham_size_t)my_calc_hash(cache, page_get_self(page));

        cache_set_totallist(cache, 
                page_list_remove(cache_get_totallist(cache), 
                PAGE_LIST_CACHED, page));
        cache_set_usedsize(cache, 
                cache_get_usedsize(cache)-db_get_pagesize(db));
        cache_get_bucket(cache, hash)=page_list_remove(cache_get_bucket(cache, 
                hash), PAGE_LIST_BUCKET, page);
    }
    (void)db_write_page_and_delete(page, 0);
    /*ham_trace("cache purged one page", 0);*/

#if HAM_DEBUG
    my_check_totallist(cache);
#endif

    return (HAM_FALSE);
}

ham_cache_t *
cache_new(ham_db_t *db, ham_u32_t flags, ham_size_t cachesize)
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
    cache_set_owner(cache, db);
    cache_set_flags(cache, flags);
    cache_set_cachesize(cache, cachesize);
    cache_set_bucketsize(cache, buckets);
    return (cache);
}

void
cache_delete(ham_cache_t *cache)
{
    ham_mem_free(cache_get_owner(cache), cache);
}

ham_page_t * 
cache_get_unused(ham_cache_t *cache)
{
    ham_page_t *page, *head, *min=0;
    ham_size_t hash;

    page=cache_get_garbagelist(cache);
    if (page) {
        ham_assert(page_get_inuse(page)==1, 
                ("page is in use and in garbage list"));
        cache_set_garbagelist(cache, 
                page_list_remove(cache_get_garbagelist(cache), 
                    PAGE_LIST_GARBAGE, page));
        cache_set_usedsize(cache, 
                cache_get_usedsize(cache)-
                    db_get_pagesize(page_get_owner(page)));
        return (page);
    }

    head=page=cache_get_totallist(cache);
    if (!head)
        return (0);

    do {
        /* only handle unused pages */
        if (page_get_inuse(page)==1) {
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
    cache_set_usedsize(cache, 
            cache_get_usedsize(cache)-
            db_get_pagesize(page_get_owner(min)));
    cache_get_bucket(cache, hash)=
            page_list_remove(cache_get_bucket(cache, 
            hash), PAGE_LIST_BUCKET, min);

#if HAM_DEBUG
    my_check_totallist(cache);
#endif

    return (min);
}

ham_page_t *
cache_get(ham_cache_t *cache, ham_offset_t address)
{
    ham_page_t *page=0;
    ham_size_t hash=(ham_size_t)my_calc_hash(cache, address);

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
    ham_size_t hash=(ham_size_t)my_calc_hash(cache, page_get_self(page));
    ham_db_t *db=cache_get_owner(cache);

    /*
     * check if (in non-strict mode) the cache limits were 
     * overstepped - if yes, try to delete some pages
     */
    if (!(cache_get_flags(cache)&HAM_IN_MEMORY_DB) &&
        !(cache_get_flags(cache)&HAM_CACHE_STRICT)) {
        while (cache_get_usedsize(cache)+db_get_pagesize(db)
                > cache_get_cachesize(cache)) {
            /*
            ham_trace("cache limits overstepped - used size %u, cache "
                    "size %u", cache_get_usedsize(cache), 
                    cache_get_cachesize(cache));
                    */
            if (!my_purge(cache))
                break;
        }
    }

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

#if HAM_DEBUG
    my_check_totallist(cache);
#endif

    /*
     * initialize the cache counter with sane values
     */
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
              page_set_cache_cntr(page, 50);
              break;
          case PAGE_TYPE_FREELIST:
              /* freelist pages should never be in the cache - fall through */
          default:
              break;
              /* ignore unknown pages... otherwise test 220 fails (allocates
               * a page from the freelist and has no way to set the 
               * page type, because the page is immediately added to the cache
              ham_assert(!"unknown page type",
                      "type is 0x%08x", page_get_type(page));
               */ 
        }
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
cache_remove_page(ham_cache_t *cache, ham_page_t *page)
{
    ham_size_t hash;
    ham_db_t *db=cache_get_owner(cache);
    
    if (page_get_self(page)) {
        hash=(ham_size_t)my_calc_hash(cache, page_get_self(page));
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

#if HAM_DEBUG
    my_check_totallist(cache);
#endif

    return (0);
}

ham_status_t
cache_move_to_garbage(ham_cache_t *cache, ham_page_t *page)
{
    ham_size_t hash=(ham_size_t)my_calc_hash(cache, page_get_self(page));

    ham_assert(page_get_inuse(page)==1, 
            ("page 0x%lx is in use", page_get_self(page)));
    ham_assert(page_is_dirty(page)==0, 
            ("page 0x%lx is dirty", page_get_self(page)));

    if (page_is_in_list(cache_get_totallist(cache), page, PAGE_LIST_CACHED)) {
        cache_set_totallist(cache, page_list_remove(cache_get_totallist(cache), 
                PAGE_LIST_CACHED, page));
        /*cache_set_usedsize(cache, 
                cache_get_usedsize(cache)-db_get_pagesize(db));*/
    }
    cache_get_bucket(cache, hash)=page_list_remove(cache_get_bucket(cache, 
                hash), PAGE_LIST_BUCKET, page);
    cache_get_garbagelist(cache)=page_list_insert(cache_get_garbagelist(cache), 
                PAGE_LIST_GARBAGE, page);

#if HAM_DEBUG
    my_check_totallist(cache);
#endif

    /*
     * check if (in non-strict mode) the cache limits were 
     * overstepped - if yes, try to delete some pages
     */
    if (!(cache_get_flags(cache)&HAM_IN_MEMORY_DB) &&
        !(cache_get_flags(cache)&HAM_CACHE_STRICT)) {
        while (cache_get_usedsize(cache)>cache_get_cachesize(cache)) {
            ham_trace(("cache limits overstepped - used size %u, cache "
                    "size %u", cache_get_usedsize(cache), 
                    cache_get_cachesize(cache)));
            if (!my_purge(cache))
                break;
        }
    }

    return (0);
}

ham_status_t 
cache_flush_and_delete(ham_cache_t *cache, ham_u32_t flags)
{
    ham_status_t st;
    ham_page_t *head;
    ham_db_t *db=cache_get_owner(cache);

    /*
     * !!
     * this function flushes all pages; if DB_FLUSH_NODELETE
     * is set, they will not be deleted; otherwise they will.
     *
     * note that the pages are not removed from the cache buckets. 
     * if the pages are deleted, this is a bug; however, pages are 
     * only deleted in ham_close(), and then the cache is no longer
     * needed anyway.
     */
    head=cache_get_totallist(cache); 
    while (head) {
        ham_page_t *next=page_get_next(head, PAGE_LIST_CACHED);

        ham_assert(page_get_inuse(head)<=1, 
                ("page is in use, but database is closing"));

        /*
         * don't remove the page from the cache, if flag NODELETE
         * is set (this flag is used i.e. in ham_flush())
         */
        if (!(flags&DB_FLUSH_NODELETE)) {
            cache_set_totallist(cache, 
                page_list_remove(cache_get_totallist(cache), 
                PAGE_LIST_CACHED, head));
            cache_set_usedsize(cache, 
                cache_get_usedsize(cache)-db_get_pagesize(db));
        }
        
#if HAM_DEBUG
    my_check_totallist(cache);
#endif

        st=db_write_page_and_delete(head, flags);
        if (st) 
            ham_log(("failed to flush page (%d) - ignoring error...", st));

        head=next;
    }

    if (!(flags&DB_FLUSH_NODELETE)) {
        cache_set_totallist(cache, 0);

        head=cache_get_garbagelist(cache); 
        while (head) {
            ham_page_t *next=page_get_next(head, PAGE_LIST_GARBAGE);
    
            ham_assert(page_get_inuse(head)<=1, 
                    ("page is in use, but database is closing"));

            db_free_page(head);
    
            head=next;
        }

        cache_set_garbagelist(cache, 0);
    }

    return (0);
}

void 
cache_dump(ham_db_t *db)
{
    ham_size_t i;
    ham_page_t *head;
    ham_cache_t *cache=db_get_cache(db);

    ham_log(("cache_dump ---------------------------------------------"));

    /*
     * for each bucket in the hash table
     */
    for (i=0; i<cache_get_bucketsize(cache); i++) {
        /*
         * dump the page info
         */
        head=cache_get_bucket(cache, i);
        while (head) {
            ham_log(("    %02d: page %lld", i, page_get_self(head)));
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

#ifdef HAM_ENABLE_INTERNAL
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
    head=cache_get_garbagelist(cache);
    while (head) {
        usedsize+=db_get_pagesize(cache_get_owner(cache));
        head=page_get_next(head, PAGE_LIST_GARBAGE);
    }

    /*
     * compare page size
     */
    if (cache_get_usedsize(cache)!=usedsize) {
        ham_trace(("cache's usedsize (%d) != actual size (%d)", 
                cache_get_usedsize(cache), usedsize));
        return (HAM_INTEGRITY_VIOLATED);
    }

    /*
     * check if the cache is exhausted
     */
    if (cache_get_flags(cache)&HAM_CACHE_STRICT) {
        if (cache_get_usedsize(cache)>cache_get_cachesize(cache)) {
            ham_trace(("cache's usedsize (%d) > maximum size (%d)", 
                    cache_get_usedsize(cache), cache_get_cachesize(cache)));
            return (HAM_INTEGRITY_VIOLATED);
        }
    }

    return (0);
}
#endif /* HAM_ENABLE_INTERNAL */
