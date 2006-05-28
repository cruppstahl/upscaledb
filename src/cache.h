/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
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
    /** the owner of the cache */
    ham_db_t *_db;

    /** cache policy/cache flags */
    ham_u32_t _flags;

    /** the cache size, in byte */
    ham_size_t _cachesize;

    /** the used size, in byte */
    ham_size_t _usedsize;

    /** the number of buckets */
    ham_size_t _bucketsize;

    /** linked list of unreferenced pages */
    ham_page_t *_unreflist;

    /** linked list of unused pages */
    ham_page_t *_garbagelist;

    /** the buckets - a linked list of ham_page_t pointers */
    ham_page_t *_buckets[1];

} ham_cache_t;

/*
 * get the database owner
 */
#define cache_get_owner(cm)                    (cm)->_db

/*
 * set the database owner
 */
#define cache_set_owner(cm, o)                 (cm)->_db=(o)

/*
 * get the cache manager flags
 */
#define cache_get_flags(cm)                    (cm)->_flags

/*
 * set the cache manager flags
 */
#define cache_set_flags(cm, f)                 (cm)->_flags=(f)

/*
 * get the cache manager cache size
 */
#define cache_get_cachesize(cm)                (cm)->_cachesize

/*
 * set the cache manager cache size
 */
#define cache_set_cachesize(cm, s)             (cm)->_cachesize=(s)

/*
 * get the used size
 */
#define cache_get_bucketsize(cm)               (cm)->_bucketsize

/*
 * set the used size
 */
#define cache_set_bucketsize(cm, s)            (cm)->_bucketsize=(s)

/*
 * get the size of the _buckets-array
 */
#define cache_get_usedsize(cm)                 (cm)->_usedsize

/*
 * set the size of the _buckets-array
 */
#define cache_set_usedsize(cm, s)              (cm)->_usedsize=(s)

/*
 * get the linked list of unreferenced pages
 */
#define cache_get_unreflist(cm)                (cm)->_unreflist

/*
 * set the linked list of unreferenced pages
 */
#define cache_set_unreflist(cm, l)             (cm)->_unreflist=(l)

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
cache_new(ham_db_t *db, ham_u32_t flags, ham_size_t cachesize);

/**
 * close and destroy a cache manager object
 *
 * @remark this will NOT flush the cache! use @a cache_flush_all() 
 */
extern void
cache_delete(ham_cache_t *cm);

/**
 * get an unused page (or an unreferenced page, if no unused page
 * was available
 *
 * @remark if the page is dirty, it's the caller's responsibility to 
 * write it to disk!
 */
extern ham_page_t *
cache_get_unused(ham_cache_t *cm);

/**
 * get a page from the cache
 *
 * @return 0 if the page was not cached
 */
extern ham_page_t *
cache_get(ham_cache_t *cm, ham_offset_t address);

/**
 * store a page in the cache
 */
extern ham_status_t 
cache_put(ham_cache_t *cm, ham_page_t *page);

/**
 * move a page from the regular cache to the garbage bin
 */
extern ham_status_t
cache_move_to_garbage(ham_cache_t *cm, ham_page_t *page);

/**
 * flush all pages, then delete them
 */
extern ham_status_t 
cache_flush_and_delete(ham_cache_t *cm);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_CACHE_H__ */
