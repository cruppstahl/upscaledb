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
 * \file hamsterdb_int.h
 * \brief Internal hamsterdb functions.
 * \author Christoph Rupp, chris@crupp.de
 *
 */

#ifndef HAM_HAMSTERDB_INT_H__
#define HAM_HAMSTERDB_INT_H__

#ifdef __cplusplus
extern "C" {
#endif 

#include <ham/hamsterdb.h>


/**
 * a callback function for dump - dumps a single key to stdout
 */
typedef void (*ham_dump_cb_t)(const ham_u8_t *key, ham_size_t keysize);

/** 
 * dump the whole tree to stdout
 *
 * @remark you can pass a callback function pointer, or NULL for the default
 * function (dumps the first 16 bytes of the key)
 *
 * @remark This function returns HAM_NOT_IMPLEMENTED unless hamsterdb
 * was built with HAM_ENABLE_INTERNAL (run ./configure --enable-internal)
 *
 * @remark set 'reserved' to NULL
 */
HAM_EXPORT ham_status_t
ham_dump(ham_db_t *db, void *reserved, ham_dump_cb_t cb);

/** 
 * verify the whole tree - this is only useful when you debug hamsterdb
 *
 * @remark This function returns HAM_NOT_IMPLEMENTED unless hamsterdb
 * was built with HAM_ENABLE_INTERNAL (run ./configure --enable-internal)
 *
 * @remark set 'reserved' to NULL
 */
HAM_EXPORT ham_status_t
ham_check_integrity(ham_db_t *db, void *reserved);

struct ham_page_filter_t;
typedef struct ham_page_filter_t ham_page_filter_t;

/**
 * A callback function for a page-level filter; called before the 
 * page is written to disk
 */
typedef ham_status_t (*ham_page_filter_pre_cb_t)(ham_db_t *db, 
        ham_page_filter_t *filter, ham_u8_t *page_data, ham_size_t page_size);

/**
 * A callback function for a page-level filter; called immediately after the 
 * page is read from disk
 */
typedef ham_status_t (*ham_page_filter_post_cb_t)(ham_db_t *db, 
        ham_page_filter_t *filter, ham_u8_t *page_data, ham_size_t page_size);

/**
 * A callback function for a page-level filter; called immediately before the
 * database is closed. Can be used to avoid memory leaks.
 */
typedef void (*ham_page_filter_close_cb_t)(ham_db_t *db, 
        ham_page_filter_t *filter);

/**
 * A handle for page-level filtering.
 *
 * Page-level filters can modify the page data before the page is 
 * written to disk, and immediately after it's read from disk.
 *
 * Page-level filters can be used i.e. for writing encryption filters. 
 * See @a ham_add_encryption() to create a filter for AES-based encryption.
 *
 * Each of the three callback functions can be NULL.
 *
 * Before this structure is used, it has to be initialized with zeroes.
 */
struct ham_page_filter_t
{
    /** The user data */
    void *userdata;

    /** The function which is called before the page is written */
    ham_page_filter_pre_cb_t pre_cb;

    /** The function which is called after the page is read */
    ham_page_filter_post_cb_t post_cb;

    /** The function which is when the database is closed */
    ham_page_filter_close_cb_t close_cb;

    /** For internal use */
    ham_u32_t _flags;

    /** For internal use */
    ham_page_filter_t *_next, *_prev;

};

/**
 * A function to install a page-level filter. 
 *
 * Page-level filters are usually installed immediately after the database
 * is created with @a ham_create[_ex] or opened with @a ham_open[_ex].
 */
HAM_EXPORT ham_status_t
ham_add_page_filter(ham_db_t *db, ham_page_filter_t *filter);

/**
 * A function to remove a page-level filter.
 *
 * This function is usually not necessary - the lifetime of a page-filter
 * usually starts before the first database operation, and ends when the
 * database is closed. It is not recommended to use this function.
 */
HAM_EXPORT ham_status_t
ham_remove_page_filter(ham_db_t *db, ham_page_filter_t *filter);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_HAMSTERDB_INT_H__ */
