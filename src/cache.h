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
 * @brief the cache manager
 *
 */

#ifndef HAM_CACHE_H__
#define HAM_CACHE_H__

#include "internal_fwd_decl.h"

	
#ifdef __cplusplus
extern "C" {
#endif 

/** CACHE_BUCKET_SIZE should be a prime number or similar, as it is used in 
 * a MODULO hash scheme */
#define CACHE_BUCKET_SIZE    10317


/**
 * a cache manager object
 */
struct ham_cache_t
{
    /** the current Environment */
    ham_env_t *_env;

    /** the capacity (in bytes) */
    ham_size_t _capacity;

    /** the current number of cached elements */
    ham_size_t _cur_elements;

    /** the number of buckets */
    ham_size_t _bucketsize;

    /** linked list of ALL cached pages */
    ham_page_t *_totallist;

    /** the tail of the linked "totallist" - this is the oldest element,
     * and therefore the highest candidate for a flush
     */
    ham_page_t *_oldest;

    /** linked list of unused pages */
    ham_page_t *_garbagelist;

    /** 
     * a 'timer' counter used to set/check the age of cache entries:
     * higher values represent newer / more important entries.
     */
    ham_u32_t _timeslot;

    /** the buckets - a linked list of ham_page_t pointers */
    ham_page_t *_buckets[1];

};

/*
 * get the current Environment
 */
#define cache_get_env(cm)                      (cm)->_env

/*
 * set the current Environment
 */
#define cache_set_env(cm, e)                   (cm)->_env=(e)

/*
 * get the capacity (in bytes)
 */
#define cache_get_capacity(cm)                 (cm)->_capacity

/*
 * set the capacity (in bytes)
 */
#define cache_set_capacity(cm, c)              (cm)->_capacity=(c)

/*
 * get the current number of elements
 */
#define cache_get_cur_elements(cm)             (cm)->_cur_elements

/*
 * set the current number of elements
 */
#define cache_set_cur_elements(cm, s)          (cm)->_cur_elements=(s)

/*
 * get the bucket-size
 */
#define cache_get_bucketsize(cm)               (cm)->_bucketsize

/*
 * set the bucket-size
 */
#define cache_set_bucketsize(cm, s)            (cm)->_bucketsize=(s)

/*
 * get the linked list of all pages
 */
#define cache_get_totallist(cm)                (cm)->_totallist

/*
 * set the linked list of all pages
 */
#define cache_set_totallist(cm, l)             (cm)->_totallist=(l)

/*
 * get the oldest page/tail in totallist
 */
#define cache_get_oldest(cm)                   (cm)->_oldest

/*
 * set the oldest page/tail in totallist
 */
#define cache_set_oldest(cm, p)                (cm)->_oldest=(p)

/*
 * get the linked list of unused (garbage collected) pages
 */
#define cache_get_garbagelist(cm)              (cm)->_garbagelist

/*
 * set the linked list of unused pages
 */
#define cache_set_garbagelist(cm, l)           (cm)->_garbagelist=(l)

/*
 * get a bucket
 */
#define cache_get_bucket(cm, i)                (cm)->_buckets[i]

/*
 * set a bucket
 */
#define cache_set_bucket(cm, i, p)             (cm)->_buckets[i]=p

/**
 * increment the cache counter, while watching a global high water mark;
 * once we hit that, we decrement all counters equally, so this remains
 * a equal opportunity design for page aging.
 */
extern void
cache_reduce_page_counts(ham_cache_t *cache);

/**
 * initialize a cache manager object
 *
 * max_size is in bytes!
 */
extern ham_cache_t *
cache_new(ham_env_t *env, ham_size_t max_size);

/**
 * close and destroy a cache manager object
 *
 * @remark this will NOT flush the cache!
 */
extern void
cache_delete(ham_cache_t *cache);

/**
 * get a page from the cache
 *
 * @remark the page is removed from the cache
 *
 * @return 0 if the page was not cached
 */
extern ham_page_t *
cache_get_page(ham_cache_t *cache, ham_offset_t address, ham_u32_t flags);

#define CACHE_NOREMOVE   1      /** don't remove the page from the cache */

/**
 * store a page in the cache
 */
extern ham_status_t 
cache_put_page(ham_cache_t *cache, ham_page_t *page);

/**
 * update the 'access counter' for a page in the cache.
 * (The page is assumed to exist in the cache!)
 */
extern void
cache_update_page_access_counter(ham_page_t *page, ham_cache_t *cache, ham_u32_t extra_bump);

/**
 * remove a page from the cache
 */
extern ham_status_t 
cache_remove_page(ham_cache_t *cache, ham_page_t *page);

/**
 * returns true if the caller should purge the cache
 */
#define cache_too_big(c)                                                      \
    ((cache_get_cur_elements(c)*env_get_pagesize(cache_get_env(c))            \
            >cache_get_capacity(c)) ? HAM_TRUE : HAM_FALSE) 

/**
 * check the cache integrity
 */
extern ham_status_t
cache_check_integrity(ham_cache_t *cache);

/**
 * purge unused pages from the cache till cache limits are no longer exceeded
 * or till no unused pages are found
 */
extern ham_status_t
cache_purge(ham_cache_t *cache);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_CACHE_H__ */
