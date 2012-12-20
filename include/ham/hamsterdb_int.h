/**
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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
#define HAM_DEFAULT_DATABASE_NAME       (0xf000)

/**
 * A reserved Database name which automatically picks the first Database
 * in an Environment
 */
#define HAM_FIRST_DATABASE_NAME         (0xf001)

/**
 * A reserved Database name for a dummy Database which only reads/writes
 * the header page
 */
#define HAM_DUMMY_DATABASE_NAME         (0xf002)

/**
@}
*/

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
ham_db_check_integrity(ham_db_t *db, ham_txn_t *txn);

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
 * @param dont_lock Whether the Environment mutex should be locked or not
 *      this is used to avoid recursive locks when retrieving the context
 *      data in a compare function
 *
 * @return The pointer to the context data
 */
HAM_EXPORT void * HAM_CALLCONV
ham_get_context_data(ham_db_t *db, ham_bool_t dont_lock);

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
 * Retrieves the current device object
 *
 * Custom device objects can be used to overwrite the functions which
 * open, create, read, write etc. to/from the file.
 *
 * The device structure is defined in src/device.h. The default device
 * objects (for file-based access and for in-memory access) are implemented
 * in src/device.c.
 *
 * @param env A valid Environment handle
 *
 * @return A pointer to a ham_device_t structure, or NULL if the device was
 *            not yet initialized
 */
HAM_EXPORT ham_device_t * HAM_CALLCONV
ham_env_get_device(ham_env_t *env);

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
 * Set a custom memory allocator
 *
 * The memory allocator is an abstract C++ class declared in src/mem.h.
 *
 * @param env A valid Environment handle
 * @param allocator A valid Allocator pointer
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if one of the pointers is NULL
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_set_allocator(ham_env_t *env, void *alloc);

/**
 * @}
 */

#ifdef __cplusplus
} // extern "C"
#endif

#endif /* HAM_HAMSTERDB_INT_H__ */
