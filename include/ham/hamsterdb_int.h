/**
 * Copyright (C) 2005-2011 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 *
 * @file hamsterdb_int.h
 * @brief Internal hamsterdb Embedded Storage functions.
 * @author Christoph Rupp, chris@crupp.de
 *
 * Please be aware that the interfaces in this file are mostly for internal
 * use. Unlike those in hamsterdb.h they are not stable and can be changed
 * with every new version.
 *
 */

#ifndef HAM_HAMSTERDB_INT_H__
#define HAM_HAMSTERDB_INT_H__

#include <ham/hamsterdb.h>

#ifdef __cplusplus
extern "C" {
#endif 

/**
 * @defgroup ham_extended_api hamsterdb Enhanced API
 * @{
 */

/**
 * @defgroup ham_special_db_names Special Database Names
 * @{
 */

/** 
 * A reserved Database name for those Databases, who are created without
 * an Environment (and therefore do not have a name).
 *
 * Note that this value also serves as the upper bound for allowed 
 * user specified Database names as passed to @a ham_env_create_db 
 * or @a ham_env_open_db.
 */
#define HAM_DEFAULT_DATABASE_NAME     (0xf000)

/** 
 * A reserved Database name which automatically picks the first Database 
 * in an Environment 
 */
#define HAM_FIRST_DATABASE_NAME       (0xf001)

/** 
 * A reserved Database name for a dummy Database which only reads/writes 
 * the header page 
 */
#define HAM_DUMMY_DATABASE_NAME       (0xf002)

/**
@}
*/

/**
 * Retrieve a @ref ham_statistics_t structure with the current statistics.
 * 
 * @warning
 * Please, heed the warnings and notes listed in the @ref ham_statistics_t 
 * documentation section and follow the advice given there to the letter. 
 * Not adhering to these adminishions introduces the risk of hamsterdb 
 * becoming unstable and exhibiting unreliable and downright faulty 
 * behaviour over time. This includes, but is not limited to, core dumps or 
 * comparable system crashes.
 * 
 * @sa ham_statistics_t
 * @sa ham_get_parameters
 * @sa ham_env_get_parameters
 */
#define HAM_PARAM_GET_STATISTICS        0x00000206

/** 
 * Verifies the integrity of the Database
 * 
 * This function is only interesting if you want to debug hamsterdb.
 *
 * @param db A valid Database handle
 * @param txn A Transaction handle, or NULL
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INTEGRITY_VIOLATED if the Database is broken
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_check_integrity(ham_db_t *db, ham_txn_t *txn);

/**
 * Estimates the number of keys stored per page in the Database
 *
 * @param db A valid Database handle
 * @param keycount A reference to a variable which will receive
 *                 the calculated key count per page
 * @param keysize The size of the key
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if @a db or @a keycount is NULL
 * @return @ref HAM_INV_KEYSIZE if the @a keycount turns out to be huge (i.e.
 *         larger than 65535); in this case @a keycount still contains a 
 *         valid value, but this error indicates this keysize won't be
 *         usable with the given Database.
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_calc_maxkeys_per_page(ham_db_t *db, ham_size_t *keycount, 
            ham_u16_t keysize);

/**
 * Set a user-provided context pointer
 *
 * This function sets a user-provided context pointer. This can be any
 * arbitrary pointer; it is stored in the Database handle and can be
 * retrieved with @a ham_get_context_data. It is mainly used by Wrappers
 * and language bindings.
 *
 * @param db A valid Database handle
 * @param data The pointer to the context data
 */
HAM_EXPORT void HAM_CALLCONV
ham_set_context_data(ham_db_t *db, void *data);

/**
 * Retrieves a user-provided context pointer
 *
 * This function retrieves a user-provided context pointer. This can be any
 * arbitrary pointer which was previously stored with @a ham_set_context_data.
 *
 * @param db A valid Database handle
 *
 * @return The pointer to the context data
 */
HAM_EXPORT void * HAM_CALLCONV
ham_get_context_data(ham_db_t *db);

struct ham_file_filter_t;
typedef struct ham_file_filter_t ham_file_filter_t;

/**
 * A callback function for a file-level filter; called before the 
 * data is written to disk
 */
typedef ham_status_t (*ham_file_filter_before_write_cb_t)(ham_env_t *env, 
        ham_file_filter_t *filter, ham_u8_t *file_data, ham_size_t file_size);

/**
 * A callback function for a file-level filter; called immediately after the 
 * data is read from disk
 */
typedef ham_status_t (*ham_file_filter_after_read_cb_t)(ham_env_t *env, 
        ham_file_filter_t *filter, ham_u8_t *file_data, ham_size_t file_size);

/**
 * A callback function for a file-level filter; called immediately before the
 * Environment is closed. Can be used to avoid memory leaks.
 */
typedef void (*ham_file_filter_close_cb_t)(ham_env_t *env, 
        ham_file_filter_t *filter);

/**
 * A handle for file-level filtering
 *
 * File-level filters can modify the page data before some data is 
 * written to disk, and immediately after it's read from disk.
 *
 * File-level filters can be used for example for writing encryption filters. 
 * See @a ham_env_enable_encryption() to create a filter for AES-based 
 * encryption.
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

    /** The function which is when the Database is closed */
    ham_file_filter_close_cb_t close_cb;

    /** For internal use */
    ham_u32_t _flags;

    /** For internal use */
    ham_file_filter_t *_next, *_prev;

};

/**
 * A function to install a file-level filter. 
 *
 * File-level filters are usually installed immediately after the Environment
 * is created with @a ham_env_create[_ex] or opened with @a ham_env_open[_ex].
 *
 * @param env A valid Environment handle
 * @param filter A pointer to a ham_file_filter_t structure
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if @a env or @a filter is NULL
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_add_file_filter(ham_env_t *env, ham_file_filter_t *filter);

/**
 * A function to remove a file-level filter.
 *
 * This function is usually not necessary - the lifetime of a file-filter
 * usually starts before the first Database operation, and ends when the
 * Environment is closed. It is not recommended to use this function.
 *
 * @param env A valid Environment handle
 * @param filter A pointer to a ham_file_filter_t structure
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if @a env or @a filter is NULL
 * @return @a HAM_FILTER_NOT_FOUND if @a filter was not registered
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_remove_file_filter(ham_env_t *env, ham_file_filter_t *filter);

struct ham_record_filter_t;
typedef struct ham_record_filter_t ham_record_filter_t;

/**
 * A callback function for a record-level filter; called before the 
 * record is inserted
 */
typedef ham_status_t (*ham_record_filter_before_insert_cb_t)(ham_db_t *db, 
        ham_record_filter_t *filter, ham_record_t *record);

/**
 * A callback function for a record-level filter; called immediately after the 
 * record is read from disk, and before it is returned to the user.
 */
typedef ham_status_t (*ham_record_filter_after_read_cb_t)(ham_db_t *db, 
        ham_record_filter_t *filter, ham_record_t *record);

/**
 * A callback function for a record-level filter; called immediately before the
 * Database is closed. Can be used to avoid memory leaks.
 */
typedef void (*ham_record_filter_close_cb_t)(ham_db_t *db, 
        ham_record_filter_t *filter);

/**
 * A handle for record-level filtering
 *
 * Record-level filters can modify and resize the record data before 
 * the record is inserted, and before it is returned to the user.
 *
 * Record-level filters can be used for example for writing compression 
 * filters.  See @a ham_enable_compression() to create a filter for zlib-based
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
    ham_record_filter_before_insert_cb_t before_write_cb;

    /** The function which is called after the record is read from disk */
    ham_record_filter_after_read_cb_t after_read_cb;

    /** The function which is when the Database is closed */
    ham_record_filter_close_cb_t close_cb;

    /** For internal use */
    ham_u32_t _flags;

    /** For internal use */
    ham_record_filter_t *_next, *_prev;

};

/**
 * A function to install a record-level filter.
 *
 * Record-level filters are usually installed immediately after the Database
 * is created with @a ham_create[_ex] or opened with @a ham_open[_ex].
 *
 * @param db A valid Database handle
 * @param filter A pointer to a ham_record_filter_t structure
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if @a db or @a filter is NULL
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_add_record_filter(ham_db_t *db, ham_record_filter_t *filter);

/**
 * A function to remove a record-level filter.
 *
 * This function is usually not necessary - the lifetime of a record-filter
 * usually starts before the first Database operation, and ends when the
 * Database is closed. It is not recommended to use this function.
 *
 * @param db A valid Database handle
 * @param filter A pointer to a ham_record_filter_t structure
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if @a db or @a filter is NULL
 * @return @a HAM_FILTER_NOT_FOUND if @a filter was not registered
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_remove_record_filter(ham_db_t *db, ham_record_filter_t *filter);

/**
 * Install a custom device object
 *
 * Custom device objects can be used to overwrite the functions which
 * open, create, read, write etc. to/from the file. 
 *
 * The device structure is defined in src/device.h. The default device
 * objects (for file-based access and for in-memory access) are implemented
 * in src/device.c.
 *
 * This function has to be called after the Environment handle has been
 * allocated (with @a ham_env_new) and before the Environment is created
 * or opened (with @a ham_env_create[_ex] or @a ham_env_open[_ex]).
 *
 * @param env A valid Environment handle
 * @param device A pointer to a ham_device_t structure
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if @a db or @a device is NULL
 * @return @a HAM_ALREADY_INITIALIZED if this function was already called
 *            for this Environment
 */
struct ham_device_t;
typedef struct ham_device_t ham_device_t;
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_set_device(ham_env_t *env, ham_device_t *device);

/**
 * Retrieves the Database handle of a Cursor
 *
 * @param cursor A valid Cursor handle
 *
 * @return @a The Database handle of @a cursor
 */
HAM_EXPORT ham_db_t * HAM_CALLCONV
ham_cursor_get_database(ham_cursor_t *cursor);

/**
 * @}
 */

/* internal flag - do not use!! */
#define HAM_HINT_UBER_FAST_ACCESS     0x00040000
/* internal flag - do not use!! */
#define HAM_HINT_RANDOM_ACCESS        0x00020000
/* internal flag - do not use!! */
#define HAM_HINT_SEQUENTIAL           0x00010000

#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_HAMSTERDB_INT_H__ */
