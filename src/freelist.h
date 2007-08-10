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
 * the freelist structure is a linked list of pages, which track
 * the deleted pages and blobs
 */

#include "packstart.h"

/**
 * a freelist-payload; it spans the persistent part of a ham_page_t
 */
typedef HAM_PACK_0 struct HAM_PACK_1 freelist_t
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
     * number of used bits in the page 
     */
    ham_u16_t _used_bits;

    /**
     * the bitmap; the size of the bitmap is _max_bits/8
     */
    ham_u8_t _bitmap[1];

} HAM_PACK_2 freelist_t;

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
#define freel_get_used_bits(fl)          (ham_db2h16((fl)->_used_bits))

/**
 * set the number of currently used bits which are handled by this bitmap
 */
#define freel_set_used_bits(fl, u)       (fl)->_used_bits=ham_h2db16(u)

/**
 * get the address of the next overflow page
 */
#define freel_get_overflow(fl)           (ham_db2h_offset((fl)->_overflow))

/**
 * set the address of the next overflow page
 */
#define freel_set_overflow(fl, o)        (fl)->_overflow=ham_h2db_offset(o)

/**
 * get a freelist_t from a ham_page_t
 */
#define page_get_freelist(p)     ((freelist_t *)p->_pers->_s._payload)

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
 * prepare a page (or a part of the page) for the freelist
 */
extern ham_status_t
freel_prepare(ham_db_t *db, freelist_t *fl, ham_offset_t start_address, 
        ham_size_t size);

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
