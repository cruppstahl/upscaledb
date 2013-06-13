/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
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
 * Retrieves the Database handle of a Cursor
 *
 * @param cursor A valid Cursor handle
 *
 * @return @a The Database handle of @a cursor
 */
HAM_EXPORT ham_db_t * HAM_CALLCONV
ham_cursor_get_database(ham_cursor_t *cursor);

/**
 * Retrieves collected metrics from the hamsterdb Environment. Used mainly
 * for testing.
 * See below for the structure with the currently available metrics.
 * This structure will change a lot; the first field is a version indicator
 * that applications can use to verify that the structure layout is compatible.
 *
 * These metrics are NOT persisted to disk.
 */
#define HAM_METRICS_VERSION         1

typedef struct ham_env_metrics_t {
  // the version indicator - must be HAM_METRICS_VERSION
  ham_u16_t version;

  // number of total allocations for the whole lifetime of the process
  ham_u64_t mem_total_allocations;

  // currently active allocations for the whole process
  ham_u64_t mem_current_allocations;

  // current amount of memory allocated and tracked by the process
  // (excludes memory used by the kernel or not allocated with
  // malloc/free)
  ham_u64_t mem_current_usage;

  // peak usage of memory (for the whole process)
  ham_u64_t mem_peak_usage;

  // the heap size of this process
  ham_u64_t mem_heap_size;

  // amount of pages fetched from disk
  ham_u64_t page_count_fetched;

  // amount of pages written to disk
  ham_u64_t page_count_flushed;

  // number of index pages in this Environment
  ham_u64_t page_count_type_index;

  // number of blob pages in this Environment
  ham_u64_t page_count_type_blob;

  // number of freelist pages in this Environment
  ham_u64_t page_count_type_freelist;

  // number of successful freelist hits
  ham_u64_t freelist_hits;

  // number of freelist misses
  ham_u64_t freelist_misses;

  // number of successful cache hits
  ham_u64_t cache_hits;

  // number of cache misses
  ham_u64_t cache_misses;

  // number of blobs allocated
  ham_u64_t blob_total_allocated;

  // number of blobs read
  ham_u64_t blob_total_read;

  // number of direct I/O bytes read (disk only)
  ham_u64_t blob_direct_read;

  // number of direct I/O bytes written (disk only)
  ham_u64_t blob_direct_written;

  // number of direct I/O bytes allocated (disk only)
  ham_u64_t blob_direct_allocated;

} ham_env_metrics_t;

/**
 * Retrieves the current metrics from an Environment
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_get_metrics(ham_env_t *env, ham_env_metrics_t *metrics);

/**
 * @}
 */

#ifdef __cplusplus
} // extern "C"
#endif

#endif /* HAM_HAMSTERDB_INT_H__ */
