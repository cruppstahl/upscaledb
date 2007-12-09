/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 * the cache manager
 *
 */

#ifndef HAM_CACHE_H__
#define HAM_CACHE_H__

#ifdef __cplusplus
extern "C" {
#endif 

#include <ham/hamsterdb.h>
#include "page.h"

/**
 * a cache manager object
 */
typedef struct
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
 * initialize a cache manager object
 */
extern ham_cache_t *
cache_new(ham_db_t *db, ham_size_t max_elements);

/**
 * close and destroy a cache manager object
 *
 * @remark this will NOT flush the cache!
 */
extern void
cache_delete(ham_db_t *db, ham_cache_t *cache);

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
