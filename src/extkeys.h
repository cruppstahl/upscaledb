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
 *
 * extended key cache
 *
 */

#ifndef HAM_EXTKEYS_H__
#define HAM_EXTKEYS_H__

#ifdef __cplusplus
extern "C" {
#endif 

#include <ham/types.h>

struct ham_db_t;

/**
 * an extended key
 */
typedef struct extkey_t
{
    /** the blobid of this key */
    ham_offset_t _blobid;

    /** the current transaction, which created this extkey; used to 
     * get the age of the extkey */
    ham_u64_t _txn_id;

    /** pointer to the next key in the linked list */
    struct extkey_t *_next;

    /** the size of the extended key */
    ham_size_t _size;

    /** the key data */
    ham_u8_t _data[1];

} extkey_t;

/**
 * the size of an extkey_t, without the data byte
 */
#define SIZEOF_EXTKEY_T                     (sizeof(extkey_t)-1)

/**
 * get the blobid 
 */
#define extkey_get_blobid(e)                (e)->_blobid

/**
 * set the blobid 
 */
#define extkey_set_blobid(e, id)             (e)->_blobid=(id)

/**
 * get the txn id 
 */
#define extkey_get_txn_id(e)                 (e)->_txn_id

/**
 * set the txn id 
 */
#define extkey_set_txn_id(e, id)             (e)->_txn_id=(id)

/**
 * get the next-pointer 
 */
#define extkey_get_next(e)                   (e)->_next

/**
 * set the next-pointer 
 */
#define extkey_set_next(e, next)             (e)->_next=(next)

/**
 * get the size
 */
#define extkey_get_size(e)                   (e)->_size

/**
 * set the size
 */
#define extkey_set_size(e, size)             (e)->_size=(size)

/**
 * get the data pointer
 */
#define extkey_get_data(e)                   (e)->_data

/**
 * a cache for extended keys
 */
typedef struct
{
    /** the owner of the cache */
    struct ham_db_t *_db;

    /** the used size, in byte */
    ham_size_t _usedsize;

    /** the number of buckets */
    ham_size_t _bucketsize;

    /** the buckets - a linked list of extkey_t pointers */
    extkey_t *_buckets[1];

} extkey_cache_t;

/**
 * get the owner of the cache
 */
#define extkey_cache_get_db(c)          (c)->_db

/**
 * set the owner of the cache
 */
#define extkey_cache_set_db(c, db)       (c)->_db=(db)

/**
 * get the used size of the cache
 */
#define extkey_cache_get_usedsize(c)     (c)->_usedsize

/**
 * set the used size of the cache
 */
#define extkey_cache_set_usedsize(c, s)  (c)->_usedsize=(s)

/**
 * get the number of buckets
 */
#define extkey_cache_get_bucketsize(c)     (c)->_bucketsize

/**
 * set the number of buckets
 */
#define extkey_cache_set_bucketsize(c, s)  (c)->_bucketsize=(s)

/**
 * get a bucket
 */
#define extkey_cache_get_bucket(c, i)      (c)->_buckets[i]

/**
 * set a bucket
 */
#define extkey_cache_set_bucket(c, i, p)   (c)->_buckets[i]=(p)

/**
 * create a new extended key-cache
 */
extern extkey_cache_t *
extkey_cache_new(struct ham_db_t *db);

/**
 * destroy the cache; does NOT delete the cache buckets!
 */
extern void
extkey_cache_destroy(extkey_cache_t *cache);

/**
 * insert a new extended key in the cache
 * will assert that there's no duplicate key! 
 */
extern ham_status_t
extkey_cache_insert(extkey_cache_t *cache, ham_offset_t blobid, 
            ham_size_t size, const ham_u8_t *data);

/**
 * remove an extended key from the cache
 * returns HAM_KEY_NOT_FOUND if the extkey was not found
 */
extern ham_status_t
extkey_cache_remove(extkey_cache_t *cache, ham_offset_t blobid);

/**
 * fetches an extended key from the cache
 * returns HAM_KEY_NOT_FOUND if the extkey was not found
 */
extern ham_status_t
extkey_cache_fetch(extkey_cache_t *cache, ham_offset_t blobid, 
            ham_size_t *size, ham_u8_t **data);

/**
 * removes all old keys from the cache
 */
extern ham_status_t
extkey_cache_purge(extkey_cache_t *cache);

/**
 * a combination of extkey_cache_remove and blob_free
 */
extern ham_status_t
extkey_remove(ham_db_t *db, ham_offset_t blobid);

#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_EXTKEYS_H__ */
