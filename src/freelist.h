/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 *
 * freelist structures, functions and macros
 *
 */

#ifndef HAM_FREELIST_H__
#define HAM_FREELIST_H__

#ifdef __cplusplus
extern "C" {
#endif 

/**
 * an entry in the freelist cache
 */
typedef struct freelist_entry_t
{
    /** the start address of this freelist page */
    ham_offset_t _start_address;

    /**
     * maximum bits in this page
     */
    ham_u16_t _max_bits;

    /**
     * allocated bits in this page
     */
    ham_u16_t _allocated_bits;

    /**
     * the page ID
     */
    ham_offset_t _page_id;

} freelist_entry_t;

/* get the start address of a freelist cache entry */
#define freel_entry_get_start_address(f)                (f)->_start_address

/* set the start address of a freelist cache entry */
#define freel_entry_set_start_address(f, s)             (f)->_start_address=s

/* get max number of bits in the cache entry */
#define freel_entry_get_max_bits(f)                     (f)->_max_bits

/* set max number of bits in the cache entry */
#define freel_entry_set_max_bits(f, m)                  (f)->_max_bits=m

/* get number of allocated bits in the cache entry */
#define freel_entry_get_allocated_bits(f)               (f)->_allocated_bits

/* set number of allocated bits in the cache entry */
#define freel_entry_set_allocated_bits(f, b)            (f)->_allocated_bits=b

/* get the page ID of this freelist entry */
#define freel_entry_get_page_id(f)                      (f)->_page_id

/* set the page ID of this freelist entry */
#define freel_entry_set_page_id(f, id)                  (f)->_page_id=id

/**
 * the freelist structure is a table to track the deleted pages and blobs
 */
typedef struct freelist_cache_t
{
    /** the number of cached elements */
    ham_size_t _count;

    /** the cached freelist entries */
    freelist_entry_t *_entries;

} freelist_cache_t;

/* get the number of freelist cache elements */
#define freel_cache_get_count(f)                        (f)->_count

/* set the number of freelist cache elements */
#define freel_cache_set_count(f, c)                     (f)->_count=c

/* get the cached freelist entries */
#define freel_cache_get_entries(f)                      (f)->_entries

/* set the cached freelist entries */
#define freel_cache_set_entries(f, e)                   (f)->_entries=e


#include "packstart.h"

/**
 * a freelist-payload; it spans the persistent part of a ham_page_t
 */
typedef HAM_PACK_0 struct HAM_PACK_1 freelist_payload_t
{
    /**
     * "real" address of the first bit
     */
    ham_u64_t _start_address;

    /**
     * address of the next freelist page
     */
    ham_offset_t _overflow;

    /**
     * maximum number of bits for this page
     */
    ham_u16_t _max_bits;

    /**
     * number of already allocated bits in the page 
     */
    ham_u16_t _allocated_bits;

    /**
     * the bitmap; the size of the bitmap is _max_bits/8
     */
    ham_u8_t _bitmap[1];

} HAM_PACK_2 freelist_payload_t;

#include "packstop.h"

/**
 * get the address of the first bitmap-entry of this page
 */
#define freel_get_start_address(fl)      (ham_db2h64((fl)->_start_address))

/**
 * set the start-address
 */
#define freel_set_start_address(fl, s)   (fl)->_start_address=ham_h2db64(s)

/**
 * get the maximum number of bits which are handled by this bitmap
 */
#define freel_get_max_bits(fl)           (ham_db2h16((fl)->_max_bits))

/**
 * set the maximum number of bits which are handled by this bitmap
 */
#define freel_set_max_bits(fl, m)        (fl)->_max_bits=ham_h2db16(m)

/**
 * get the number of currently used bits which are handled by this bitmap
 */
#define freel_get_allocated_bits(fl)      (ham_db2h16((fl)->_allocated_bits))

/**
 * set the number of currently used bits which are handled by this bitmap
 */
#define freel_set_allocated_bits(fl, u)   (fl)->_allocated_bits=ham_h2db16(u)

/**
 * get the address of the next overflow page
 */
#define freel_get_overflow(fl)           (ham_db2h_offset((fl)->_overflow))

/**
 * set the address of the next overflow page
 */
#define freel_set_overflow(fl, o)        (fl)->_overflow=ham_h2db_offset(o)

/**
 * get a freelist_payload_t from a ham_page_t
 */
#define page_get_freelist(p)     ((freelist_payload_t *)p->_pers->_s._payload)

/**
 * get the bitmap of the freelist
 */
#define freel_get_bitmap(fl)             (fl)->_bitmap

/**
 * create a new freelist
 */
extern ham_status_t
freel_create(ham_db_t *db);

/**
 * flush and release all freelist pages
 */
extern ham_status_t
freel_shutdown(ham_db_t *db);

/**
 * mark an area in the file as "free"
 *
 * !! note
 * will assert that address and size are DB_CHUNKSIZE-aligned!
 */
extern ham_status_t
freel_mark_free(ham_db_t *db, ham_offset_t address, ham_size_t size);

/**
 * try to allocate an (unaligned) space from the freelist
 *
 * returns 0 on failure
 *
 * !! note
 * will assert that size is DB_CHUNKSIZE-aligned!
 */
extern ham_offset_t
freel_alloc_area(ham_db_t *db, ham_size_t size);

/**
 * try to allocate an (aligned) page from the freelist
 *
 * returns 0 on failure
 */
extern ham_offset_t
freel_alloc_page(ham_db_t *db);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_FREELIST_H__ */
