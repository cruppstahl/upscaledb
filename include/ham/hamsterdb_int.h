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

struct ham_file_filter_t;
typedef struct ham_file_filter_t ham_file_filter_t;

/**
 * A callback function for a page-level filter; called before the 
 * page is written to disk
 */
typedef ham_status_t (*ham_file_filter_before_write_cb_t)(ham_db_t *db, 
        ham_file_filter_t *filter, ham_u8_t *file_data, ham_size_t file_size);

/**
 * A callback function for a page-level filter; called immediately after the 
 * page is read from disk
 */
typedef ham_status_t (*ham_file_filter_after_read_cb_t)(ham_db_t *db, 
        ham_file_filter_t *filter, ham_u8_t *file_data, ham_size_t file_size);

/**
 * A callback function for a page-level filter; called immediately before the
 * database is closed. Can be used to avoid memory leaks.
 */
typedef void (*ham_file_filter_close_cb_t)(ham_db_t *db, 
        ham_file_filter_t *filter);

/**
 * A handle for page-level filtering.
 *
 * File-level filters can modify the page data before some data is 
 * written to disk, and immediately after it's read from disk.
 *
 * File-level filters can be used i.e. for writing encryption filters. 
 * See @a ham_enable_encryption() to create a filter for AES-based encryption.
 *
 * Each of the three callback functions can be NULL.
 *
 * Before this structure is used, it has to be initialized with zeroes.
 */
struct ham_file_filter_t
{
    /** The user data */
    void *userdata;

    /** The function which is called before the page is written */
    ham_file_filter_before_write_cb_t before_write_cb;

    /** The function which is called after the page is read */
    ham_file_filter_after_read_cb_t after_read_cb;

    /** The function which is when the database is closed */
    ham_file_filter_close_cb_t close_cb;

    /** For internal use */
    ham_u32_t _flags;

    /** For internal use */
    ham_file_filter_t *_next, *_prev;

};

/**
 * A function to install a file-level filter. 
 *
 * File-level filters are usually installed immediately after the database
 * is created with @a ham_create[_ex] or opened with @a ham_open[_ex].
 */
HAM_EXPORT ham_status_t
ham_add_file_filter(ham_db_t *db, ham_file_filter_t *filter);

/**
 * A function to remove a file-level filter.
 *
 * This function is usually not necessary - the lifetime of a file-filter
 * usually starts before the first database operation, and ends when the
 * database is closed. It is not recommended to use this function.
 */
HAM_EXPORT ham_status_t
ham_remove_file_filter(ham_db_t *db, ham_file_filter_t *filter);

struct ham_record_filter_t;
typedef struct ham_record_filter_t ham_record_filter_t;

/**
 * A callback function for a record-level filter; called before the 
 * record is inserted
 *
 * @a record_data and @a record_size can be modified or even
 * re-allocated.
 */
typedef ham_status_t (*ham_record_filter_before_insert_cb_t)(ham_db_t *db, 
        ham_record_filter_t *filter, ham_u8_t **record_data, 
        ham_size_t *record_size);

/**
 * A callback function for a record-level filter; called immediately after the 
 * record is read from disk, and before it is returned to the user.
 */
typedef ham_status_t (*ham_record_filter_after_read_cb_t)(ham_db_t *db, 
        ham_record_filter_t *filter, ham_u8_t **record_data, 
        ham_size_t *file_size);

/**
 * A callback function for a record-level filter; called immediately before the
 * database is closed. Can be used to avoid memory leaks.
 */
typedef void (*ham_record_filter_close_cb_t)(ham_db_t *db, 
        ham_record_filter_t *filter);

/**
 * A handle for record-level filtering.
 *
 * Record-level filters can modify and resize the record data before 
 * it is inserted, and before it is returned to the user.
 *
 * Record-level filters can be used i.e. for writing compression filters. 
 * See @a ham_enable_compression() to create a filter for zlib-based
 * compression.
 *
 * Each of the three callback functions can be NULL.
 *
 * Before this structure is used, it has to be initialized with zeroes.
 */
struct ham_record_filter_t
{
    /** The user data */
    void *userdata;

    /** The function which is called before the record is inserted */
    ham_record_filter_before_insert_cb_t before_insert_cb;

    /** The function which is called after the record is read from disk */
    ham_record_filter_after_read_cb_t after_read_cb;

    /** The function which is when the database is closed */
    ham_record_filter_close_cb_t close_cb;

    /** For internal use */
    ham_u32_t _flags;

    /** For internal use */
    ham_record_filter_t *_next, *_prev;

};

/**
 * A function to install a record-level filter. 
 *
 * Record-level filters are usually installed immediately after the database
 * is created with @a ham_create[_ex] or opened with @a ham_open[_ex].
 */
HAM_EXPORT ham_status_t
ham_add_record_filter(ham_db_t *db, ham_record_filter_t *filter);

/**
 * A function to remove a record-level filter.
 *
 * This function is usually not necessary - the lifetime of a record-filter
 * usually starts before the first database operation, and ends when the
 * database is closed. It is not recommended to use this function.
 */
HAM_EXPORT ham_status_t
ham_remove_record_filter(ham_db_t *db, ham_record_filter_t *filter);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_HAMSTERDB_INT_H__ */
