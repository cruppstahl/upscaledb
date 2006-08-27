/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 * the cache manager
 *
 */

#ifndef HAM_CACHEMGR_H__
#define HAM_CACHEMGR_H__

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

} ham_cachemgr_t;

/*
 * get the database owner
 */
#define cm_get_owner(cm)                    (cm)->_db

/*
 * set the database owner
 */
#define cm_set_owner(cm, o)                 (cm)->_db=(o)

/*
 * get the cache manager flags
 */
#define cm_get_flags(cm)                    (cm)->_flags

/*
 * set the cache manager flags
 */
#define cm_set_flags(cm, f)                 (cm)->_flags=(f)

/*
 * get the cache manager cache size
 */
#define cm_get_cachesize(cm)                (cm)->_cachesize

/*
 * set the cache manager cache size
 */
#define cm_set_cachesize(cm, s)             (cm)->_cachesize=(s)

/*
 * get the used size
 */
#define cm_get_bucketsize(cm)               (cm)->_bucketsize

/*
 * set the used size
 */
#define cm_set_bucketsize(cm, s)            (cm)->_bucketsize=(s)

/*
 * get the size of the _buckets-array
 */
#define cm_get_usedsize(cm)                 (cm)->_usedsize

/*
 * set the size of the _buckets-array
 */
#define cm_set_usedsize(cm, s)              (cm)->_usedsize=(s)

/*
 * get the linked list of unreferenced pages
 */
#define cm_get_unreflist(cm)                (cm)->_unreflist

/*
 * set the linked list of unreferenced pages
 */
#define cm_set_unreflist(cm, l)             (cm)->_unreflist=(l)

/*
 * get the linked list of unused (garbage collected) pages
 */
#define cm_get_garbagelist(cm)              (cm)->_garbagelist

/*
 * set the linked list of unused pages
 */
#define cm_set_garbagelist(cm, l)           (cm)->_garbagelist=(l)

/*
 * get a bucket
 */
#define cm_get_bucket(cm, i)                (cm)->_buckets[i]

/**
 * initialize a cache manager object
 */
extern ham_cachemgr_t *
cm_new(ham_db_t *db, ham_u32_t flags, ham_size_t cachesize);

/**
 * close and destroy a cache manager object
 *
 * @remark this will NOT flush the cache! use @a cm_flush_all() 
 */
extern void
cm_delete(ham_cachemgr_t *cm);

/**
 * valid flags for the cache manager 
 */

/**
 * allocate as much memory as needed and ignore all size restrictions
 */
#define HAM_CM_MAXSIZE              4

/**
 * fetch a page from the cache
 *
 * @remark this function will load the page from the cache, if 
 * available. in case of an error it will store the error in the
 * database handle (see @a ham_get_error()) and return 0
 */
extern ham_page_t *
cm_fetch(ham_cachemgr_t *cm, ham_offset_t address, ham_u32_t flags);

/**
 * flags for cm_fetch
 */
#define CM_READ_ONLY        1

/**
 * flush a page
 *
 * @remark this function will dereference the page and MAY the page 
 * to the harddrive. The page pointer MAY become invalid.
 *
 * @remark valid flags: HAM_CM_REVERT_CHANGES - delete page, even if it was
 *  dirty, and DON'T write it back!
 */
extern ham_status_t 
cm_flush(ham_cachemgr_t *cm, ham_page_t *page, ham_u32_t flags);

/**
 * allocate a new page
 *
 * @remark set flags to 0
 * @remark reference count of the page will be incremented
 */
extern ham_page_t *
cm_alloc_page(ham_cachemgr_t *cm, ham_txn_t *txn, ham_u32_t flags);

/**
 * flag for cm_flush: delete page, even if it was dirty, and DON'T write
 * it back!
 */
#define HAM_CM_REVERT_CHANGES       1

/*
 * flag for cm_flush: the flushed page's reference counter is not 
 * decremented and the page-structure is not deleted from RAM
 */
#define HAM_CM_NO_UNREF             2

/**
 * flush all pages
 *
 * @remark this function will flush all pages and force a write
 */
extern ham_status_t 
cm_flush_all(ham_cachemgr_t *cm, ham_u32_t flags);

/**
 * move a page from the regular cache to the garbage bin
 *
 * @remark make sure that this page holds no references!
 */
extern ham_status_t
cm_move_to_garbage(ham_cachemgr_t *cm, ham_page_t *page);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_CACHEMGR_H__ */
