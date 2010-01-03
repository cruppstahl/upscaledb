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
 * the cache manager
 *
 */

#ifndef HAM_CACHE_H__
#define HAM_CACHE_H__

#include <ham/hamsterdb.h>
#include "page.h"

	
#ifdef __cplusplus
extern "C" {
#endif 

/* CACHE_BUCKET_SIZE should be a prime number or similar, as it is used in 
 * a MODULO hash scheme */
#define CACHE_BUCKET_SIZE     359
#define CACHE_MAX_ELEM        256 /* a power of 2 *below* CACHE_BUCKET_SIZE */


/**
 * a cache manager object
 */
typedef struct ham_cache_t
{
    /** the maximum number of cached elements */
    ham_size_t _max_elements;

    /** the current number of cached elements */
    ham_size_t _cur_elements;

    /** the number of buckets */
    ham_size_t _bucketsize;

    /** linked list of ALL cached pages */
    ham_page_t *_totallist;

    /** linked list of unused pages */
    ham_page_t *_garbagelist;

    /** 
     * a 'timer' counter used to set/check the age of cache entries:
     * higher values represent newer / more important entries.
     */
    ham_u32_t _timeslot;

    /** the buckets - a linked list of ham_page_t pointers */
    ham_page_t *_buckets[1];

} ham_cache_t;

/*
 * get the maximum number of elements
 */
#define cache_get_max_elements(cm)             (cm)->_max_elements

/*
 * set the maximum number of elements
 */
#define cache_set_max_elements(cm, s)          (cm)->_max_elements=(s)

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
 * get the linked list of unused pages
 */
#define cache_get_totallist(cm)                (cm)->_totallist

/*
 * set the linked list of unused pages
 */
#define cache_set_totallist(cm, l)             (cm)->_totallist=(l)

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

static __inline void 
page_increment_cache_cntr(ham_page_t *page, ham_u32_t count, ham_cache_t *cache)
{															
	ham_u32_t _c_v = page_get_cache_cntr(page);								
	ham_u32_t _u_c = (count);     							
	if (_c_v >=	0xFFFFFFFFU - 1024 - _u_c					
		|| (cache)->_timeslot >= 0xFFFFFFFFU - 1024)        
	{														
		cache_reduce_page_counts(cache);					
		_c_v = page_get_cache_cntr(page);
	}														
	_c_v += _u_c;											
	if (_c_v < (cache)->_timeslot)							
		_c_v = (cache)->_timeslot;							
	page_set_cache_cntr(page, _c_v);										
}

/**
 * initialize a cache manager object
 */
extern ham_cache_t *
cache_new(ham_env_t *env, ham_size_t max_elements);

/**
 * close and destroy a cache manager object
 *
 * @remark this will NOT flush the cache!
 */
extern void
cache_delete(ham_env_t *env, ham_cache_t *cache);

/**
 * get an unused page (or an unreferenced page, if no unused page
 * was available
 *
 * @remark if the page is dirty, it's the caller's responsibility to 
 * write it to disk!
 *
 * @remark the page is removed from the cache
 */
extern ham_page_t *
cache_get_unused_page(ham_cache_t *cache);

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
cache_update_page_access_counter(ham_page_t *page, ham_cache_t *cache);

/**
 * remove a page from the cache
 */
extern ham_status_t 
cache_remove_page(ham_cache_t *cache, ham_page_t *page);

/**
 * move a page from the regular cache to the garbage bin
 */
extern ham_status_t
cache_move_to_garbage(ham_cache_t *cache, ham_page_t *page);

/**
 * returns true if the caller should purge the cache
 */
extern ham_bool_t 
cache_too_big(ham_cache_t *cache);

/**
 * check the cache integrity
 */
#ifdef HAM_ENABLE_INTERNAL
extern ham_status_t
cache_check_integrity(ham_cache_t *cache);
#endif


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_CACHE_H__ */
