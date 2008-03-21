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
 * \mainpage hamsterdb embedded database
 * \brief Include file for hamsterdb
 * \author Christoph Rupp, chris@crupp.de
 * \version 1.0.1
 */

#ifndef HAM_HAMSTERDB_H__
#define HAM_HAMSTERDB_H__

#ifdef __cplusplus
extern "C" {
#endif

#include <ham/types.h>

/**
 * The hamsterdb Database structure
 *
 * This structure is allocated with @a ham_new and deleted with
 * @a ham_delete.
 */
struct ham_db_t;
typedef struct ham_db_t ham_db_t;

/**
 * The hamsterdb Environment structure
 *
 * This structure is allocated with @a ham_env_new and deleted with
 * @a ham_env_delete.
 */
struct ham_env_t;
typedef struct ham_env_t ham_env_t;

/**
 * A Database Cursor
 *
 * A Cursor is used for bi-directionally traversing the Database and
 * for inserting/deleting/searching Database items.
 *
 * This structure is allocated with @a ham_cursor_create and deleted with
 * @a ham_cursor_close.
 */
struct ham_cursor_t;
typedef struct ham_cursor_t ham_cursor_t;

/**
 * A generic record.
 *
 * A record represents data items in hamsterdb. Before using a record, it
 * is important to initialize all record fields with zeroes, i.e. with
 * the C library routines memset(3) or bzero(2).
 *
 * When hamsterdb returns a record structure, the pointer to the record
 * data is provided in @a data. This pointer is only temporary and will be
 * overwritten by subsequent hamsterdb API calls.
 *
 * To avoid this, the calling application can allocate the @a data pointer.
 * In this case, you have to set the flag @a HAM_RECORD_USER_ALLOC. The
 * @a size parameter will then return the size of the record. It's the
 * responsibility of the caller to make sure that the @a data parameter is
 * large enough for the record.
 */
typedef struct
{
    /** The size of the record data, in bytes */
    ham_size_t size;

    /** Pointer to the record data */
    void *data;

    /** The record flags; see @sa HAM_RECORD_USER_ALLOC */
    ham_u32_t flags;

    /** For internal use */
    ham_u32_t _intflags;

    /** For internal use */
    ham_u64_t _rid;

} ham_record_t;

/** Flag for @a ham_record_t */
#define HAM_RECORD_USER_ALLOC   1

/**
 * A generic key.
 *
 * A key represents key items in hamsterdb. Before using a key, it
 * is important to initialize all key fields with zeroes, i.e. with
 * the C library routines memset(3) or bzero(2).
 *
 * hamsterdb usually uses keys to insert, delete or search for items.
 * However, when using Database Cursors and the function @a ham_cursor_move,
 * hamsterdb also returns keys. In this case, the pointer to the key
 * data is provided in @a data. This pointer is only temporary and will be
 * overwritten by subsequent calls to @a ham_cursor_move.
 *
 * To avoid this, the calling application can allocate the @a data pointer.
 * In this case, you have to set the flag @a HAM_KEY_USER_ALLOC. The
 * @a size parameter will then return the size of the key. It's the
 * responsibility of the caller to make sure that the @a data parameter is
 * large enough for the key.
 */
typedef struct
{
    /** The size of the key, in bytes */
    ham_u16_t size;

    /** The data of the key */
    void *data;

    /** The key flags; see @sa HAM_KEY_USER_ALLOC */
    ham_u32_t flags;

    /** For internal use */
    ham_u32_t _flags;
} ham_key_t;

/** Flag for @a ham_key_t (only in combination with @a ham_cursor_move) */
#define HAM_KEY_USER_ALLOC      1

/**
 * A named parameter.
 *
 * These parameter structures are used for functions like @a ham_open_ex,
 * @a ham_create_ex etc to pass variable length parameter lists.
 *
 * The lists are always arrays of type ham_parameter_t, with a terminating
 * element of { 0, NULL}, e.g.
 *
 * <pre>
 *   ham_parameter_t parameters[]={
 *      { HAM_PARAM_CACHESIZE, 1024 },
 *      { HAM_PARAM_PAGESIZE, 1024*4 },
 *      { 0, NULL }
 *   };
 * </pre>
 */
typedef struct {
    /** The name of the parameter; all HAM_PARAM_*-constants */
    ham_u32_t name;

    /** The value of the parameter. */
    ham_u64_t value;

} ham_parameter_t;

/**
 * @defgroup ham_status_codes hamsterdb Status Codes
 * @{
 */

/** Operation completed successfully */
#define HAM_SUCCESS                  (  0)
/** Invalid key size */
#define HAM_INV_KEYSIZE              ( -3)
/** Invalid page size (must be a multiple of 1024) */
#define HAM_INV_PAGESIZE             ( -4)
/** Memory allocation failed - out of memory */
#define HAM_OUT_OF_MEMORY            ( -6)
/** Object not initialized */
#define HAM_NOT_INITIALIZED          ( -7)
/** Invalid function parameter */
#define HAM_INV_PARAMETER            ( -8)
/** Invalid file header */
#define HAM_INV_FILE_HEADER          ( -9)
/** Invalid file version */
#define HAM_INV_FILE_VERSION         (-10)
/** Key was not found */
#define HAM_KEY_NOT_FOUND            (-11)
/** Tried to insert a key which already exists */
#define HAM_DUPLICATE_KEY            (-12)
/** Internal Database integrity violated */
#define HAM_INTEGRITY_VIOLATED       (-13)
/** Internal hamsterdb error */
#define HAM_INTERNAL_ERROR           (-14)
/** Tried to modify the Database, but the file was opened as read-only */
#define HAM_DB_READ_ONLY             (-15)
/** Database record not found */
#define HAM_BLOB_NOT_FOUND           (-16)
/** Prefix comparison function needs more data */
#define HAM_PREFIX_REQUEST_FULLKEY   (-17)
/** Generic file I/O error */
#define HAM_IO_ERROR                 (-18)
/** Database cache is full */
#define HAM_CACHE_FULL               (-19)
/** Function is not yet implemented */
#define HAM_NOT_IMPLEMENTED          (-20)
/** File not found */
#define HAM_FILE_NOT_FOUND           (-21)
/** Operation would block */
#define HAM_WOULD_BLOCK              (-22)
/** Object was not initialized correctly */
#define HAM_NOT_READY                (-23)
/** Database limits reached */
#define HAM_LIMITS_REACHED           (-24)
/** AES encryption key is wrong */
#define HAM_ACCESS_DENIED            (-25)
/** Object was already initialized */
#define HAM_ALREADY_INITIALIZED      (-27)
/** Cursor does not point to a valid item */
#define HAM_CURSOR_IS_NIL           (-100)
/** Database not found */
#define HAM_DATABASE_NOT_FOUND      (-200)
/** Database name already exists */
#define HAM_DATABASE_ALREADY_EXISTS (-201)
/** Database already open */
#define HAM_DATABASE_ALREADY_OPEN   (-202)
/** Invalid log file header */
#define HAM_LOG_INV_FILE_HEADER     (-300)

/**
 * @}
 */

/**
 * @defgroup ham_static hamsterdb Static Functions
 * @{
 */

/**
 * A typedef for a custom error handler function
 *
 * @param message The error message
 * @param level The error level:
 *      <ul>
 *       <li>0</li> a debug message
 *       <li>1</li> a normal error message
 *       <li>2</li> reserved
 *       <li>3</li> a fatal error message
 *      </ul>
 */
typedef void (HAM_CALLCONV *ham_errhandler_fun)
                    (int level, const char *message);

/**
 * Sets the global error handler
 *
 * This handler will receive all debug messages that are emitted
 * by hamsterdb. You can install the default handler by setting @a f to 0.
 *
 * The default error handler prints all messages to stderr. To install a
 * different logging facility, you can provide your own error handler.
 *
 * Note that the callback function must have the same calling convention 
 * as the hamsterdb library.
 *
 * @param f A pointer to the error handler function, or NULL to restore
 *          the default handler
 */
HAM_EXPORT void HAM_CALLCONV
ham_set_errhandler(ham_errhandler_fun f);

/**
 * Translates a hamsterdb status code to a descriptive error string
 *
 * @param status The hamsterdb status code
 *
 * @return A pointer to a descriptive error string
 */
HAM_EXPORT const char * HAM_CALLCONV
ham_strerror(ham_status_t status);

/**
 * Returns the version of the hamsterdb library
 *
 * @param major If not NULL, will point to the major version number
 * @param minor If not NULL, will point to the minor version number
 * @param revision If not NULL, will point to the revision version number
 */
HAM_EXPORT void HAM_CALLCONV
ham_get_version(ham_u32_t *major, ham_u32_t *minor,
        ham_u32_t *revision);

/**
 * Returns the name of the licensee and the name of the licensed product
 *
 * @param licensee If not NULL, will point to the licensee name, or to
 *        an empty string "" for non-commercial versions
 * @param product If not NULL, will point to the product name
 */
HAM_EXPORT void HAM_CALLCONV
ham_get_license(const char **licensee, const char **product);

/**
 * @}
 */

/**
 * @defgroup ham_env hamsterdb Environment Functions
 * @{
 */
/**
 * Allocates a ham_env_t handle
 *
 * @param env Pointer to the pointer which is allocated
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_OUT_OF_MEMORY if memory allocation failed
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_new(ham_env_t **env);

/**
 * Frees a ham_env_t handle
 *
 * Frees the ham_env_t structure, but does not close the
 * Environment. Call this function <b>AFTER</b> you have closed the
 * Environment using @a ham_env_close, or you will lose your data!
 *
 * @param env A valid Environment handle
 *
 * @return This function always returns @a HAM_SUCCESS
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_delete(ham_env_t *env);

/**
 * Creates a Database Environment
 *
 * A Database Environment is a collection of Databases, which are all stored
 * in one physical file (or in-memory). Per default, up to 16 Databases can be
 * stored in one file (see @a ham_env_create_ex on how to store even more
 * Databases). 
 *
 * Each Database is identified by a positive 16bit value (except
 * 0 and values above 0xf000). Databases in an Environment can be created
 * with @a ham_env_create_db or opened with @a ham_env_open_db.
 *
 * @param env A valid Environment handle, which was created with @a ham_env_new
 * @param filename The filename of the Environment file. If the file already
 *          exists, it is overwritten. Can be NULL if an In-Memory 
 *          Environment is created.
 * @param flags Optional flags for opening the Environment, combined with
 *        bitwise OR. For allowed flags, see @a ham_env_create_ex.
 * @param mode File access rights for the new file. This is the @a mode
 *        parameter for creat(2). Ignored on Microsoft Windows.
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if the @a env pointer is NULL or an
 *              invalid combination of flags was specified
 * @return @a HAM_IO_ERROR if the file could not be opened or
 *              reading/writing failed
 * @return @a HAM_INV_FILE_VERSION if the Environment version is not
 *              compatible with the library version
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated
 * @return @a HAM_WOULD_BLOCK if another process has locked the file
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_create(ham_env_t *env, const char *filename,
        ham_u32_t flags, ham_u32_t mode);

/**
 * Creates a Database Environment - extended version
 *
 * A Database Environment is a collection of Databases, which are all stored
 * in one physical file (or in-memory). Per default, up to 16 Databases can be
 * stored in one file, but this setting can be overwritten by specifying
 * the parameter @a HAM_PARAM_MAX_ENV_DATABASES.
 *
 * Each Database is identified by a positive 16bit value (except
 * 0 and values above 0xf000). Databases in an Environment can be created
 * with @a ham_env_create_db or opened with @a ham_env_open_db.
 *
 * @param env A valid Environment handle, which was created with @a ham_env_new
 * @param filename The filename of the Environment file. If the file already
 *          exists, it is overwritten. Can be NULL for an In-Memory
 *          Environment.
 * @param flags Optional flags for opening the Environment, combined with
 *        bitwise OR. Possible flags are:
 *      <ul>
 *       <li>@a HAM_WRITE_THROUGH</li> Immediately write modified pages to the
 *            disk. This slows down all Database operations, but may
 *            save the Database integrity in case of a system crash.
 *       <li>@a HAM_IN_MEMORY_DB</li> Creates an In-Memory Environment. No 
 *            file will be created, and the Database contents are lost after
 *            the Environment is closed. The @a filename parameter can
 *            be NULL. Do <b>NOT</b> use in combination with
 *            @a HAM_CACHE_STRICT and do <b>NOT</b> specify @a cachesize
 *            other than 0.
 *       <li>@a HAM_DISABLE_MMAP</li> Do not use memory mapped files for I/O.
 *            By default, hamsterdb checks if it can use mmap,
 *            since mmap is faster than read/write. For performance
 *            reasons, this flag should not be used.
 *       <li>@a HAM_CACHE_STRICT</li> Do not allow the cache to grow larger
 *            than @a cachesize. If a Database operation needs to resize the
 *            cache, it will return @a HAM_CACHE_FULL.
 *            If the flag is not set, the cache is allowed to allocate
 *            more pages than the maximum cache size, but only if it's
 *            necessary and only for a short time.
 *       <li>@a HAM_DISABLE_FREELIST_FLUSH</li> Do not immediately write back
 *            modified freelist pages. Using this flag leads to small
 *            performance improvements, but may prove to be risky
 *            in case of a system crash or program crash.
 *       <li>@a HAM_LOCK_EXCLUSIVE</li> Place an exclusive lock on the
 *            file. Only one process may hold an exclusive lock for
 *            a given file at a given time. 
 *      </ul>
 *
 * @param mode File access rights for the new file. This is the @a mode
 *        parameter for creat(2). Ignored on Microsoft Windows.
 * @param param An array of ham_parameter_t structures. The following
 *        parameters are available:
 *      <ul>
 *        <li>HAM_PARAM_CACHESIZE</li> The size of the Database cache,
 *            in bytes. The default size is defined in src/config.h
 *            as HAM_DEFAULT_CACHESIZE - usually 256kb.
 *        <li>HAM_PARAM_PAGESIZE</li> The size of a file page, in
 *            bytes. It is recommended not to change the default size. The
 *            default size depends on hardware and operating system.
 *            Page sizes must be a multiple of 1024.
 *        <li>HAM_PARAM_MAX_ENV_DATABASES</li> The number of maximum
 *            Databases in this Environment; default value: 16.
 *      </ul>
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if the @a env pointer is NULL or an
 *              invalid combination of flags or parameters was specified
 * @return @a HAM_INV_PARAMETER if the value for HAM_PARAM_MAX_ENV_DATABASES
 *              is too high (either decrease it or increase the page size)
 * @return @a HAM_IO_ERROR if the file could not be opened or
 *              reading/writing failed
 * @return @a HAM_INV_FILE_VERSION if the Environment version is not
 *              compatible with the library version
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated
 * @return @a HAM_INV_PAGESIZE if @a pagesize is not a multiple of 1024
 * @return @a HAM_INV_KEYSIZE if @a keysize is too large (at least 4
 *              keys must fit in a page)
 * @return @a HAM_WOULD_BLOCK if another process has locked the file
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_create_ex(ham_env_t *env, const char *filename,
        ham_u32_t flags, ham_u32_t mode, ham_parameter_t *param);

/**
 * Opens an existing Database Environment
 *
 * @param filename The filename of the Environment file
 * @param flags Optional flags for opening the Environment, combined with
 *        bitwise OR. See the documentation of @a ham_env_open_ex
 *        for the allowed flags.
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if the @a env pointer is NULL or an
 *              invalid combination of flags was specified
 * @return @a HAM_FILE_NOT_FOUND if the file does not exist
 * @return @a HAM_IO_ERROR if the file could not be opened or reading failed
 * @return @a HAM_INV_FILE_VERSION if the Environment version is not
 *              compatible with the library version
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated
 * @return @a HAM_WOULD_BLOCK if another process has locked the file
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_open(ham_env_t *env, const char *filename, ham_u32_t flags);

/**
 * Opens an existing Database Environment - extended version
 *
 * @param env A valid Environment handle
 * @param filename The filename of the Environment file
 * @param flags Optional flags for opening the Environment, combined with
 *        bitwise OR. Possible flags are:
 *      <ul>
 *       <li>@a HAM_READ_ONLY</li> Opens the file for reading only.
 *            Operations that need write access (i.e. @a ham_insert) will
 *            return @a HAM_DB_READ_ONLY
 *       <li>@a HAM_WRITE_THROUGH</li> Immediately write modified pages
 *            to the disk. This slows down all Database operations, but
 *            could save the Database integrity in case of a system crash.
 *       <li>@a HAM_DISABLE_MMAP</li> Do not use memory mapped files for I/O.
 *            By default, hamsterdb checks if it can use mmap,
 *            since mmap is faster than read/write. For performance
 *            reasons, this flag should not be used.
 *       <li>@a HAM_CACHE_STRICT</li> Do not allow the cache to grow larger
 *            than @a cachesize. If a Database operation needs to resize the
 *            cache, it will return @a HAM_CACHE_FULL.
 *            If the flag is not set, the cache is allowed to allocate
 *            more pages than the maximum cache size, but only if it's
 *            necessary and only for a short time.
 *       <li>@a HAM_DISABLE_FREELIST_FLUSH</li> Do not immediately write back
 *            modified freelist pages. Using this flag leads to small
 *            performance improvements, but may prove to be risky
 *            in case of a system crash or program crash.
 *       <li>@a HAM_LOCK_EXCLUSIVE</li> Place an exclusive lock on the
 *            file. Only one process may hold an exclusive lock for
 *            a given file at a given time.
 *      </ul>
 * @param param An array of ham_parameter_t structures. The following
 *        parameters are available:
 *      <ul>
 *        <li>HAM_PARAM_CACHESIZE</li> The size of the Database cache,
 *            in bytes. The default size is defined in src/config.h
 *            as HAM_DEFAULT_CACHESIZE - usually 256kb.
 *      </ul>
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if the @a env pointer is NULL, an
 *              invalid combination of flags was specified 
 * @return @a HAM_FILE_NOT_FOUND if the file does not exist
 * @return @a HAM_IO_ERROR if the file could not be opened or reading failed
 * @return @a HAM_INV_FILE_VERSION if the Environment version is not
 *              compatible with the library version.
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated
 * @return @a HAM_WOULD_BLOCK if another process has locked the file
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_open_ex(ham_env_t *env, const char *filename,
        ham_u32_t flags, ham_parameter_t *param);

/**
 * Creates a new Database in a Database Environment.
 *
 * @param env A valid Environment handle.
 * @param db A valid Database handle, which will point to the created
 *          Database. To close the handle, use @a ham_close.
 * @param name The name of the Database. If a Database with this name 
 *          already exists, the function will fail with 
 *          @a HAM_DATABASE_ALREADY_EXISTS. Database names from 0xf000 to
 *          0xffff and 0 are reserved.
 * @param flags Optional flags for creating the Database, combined with
 *        bitwise OR. Possible flags are:
 *      <ul>
 *       <li>@a HAM_USE_BTREE</li> Use a B+Tree for the index structure.
 *            Currently enabled by default, but future releases
 *            of hamsterdb will offer additional index structures,
 *            like hash tables.
 *       <li>@a HAM_DISABLE_VAR_KEYLEN</li> Do not allow the use of variable
 *            length keys. Inserting a key, which is larger than the
 *            B+Tree index key size, returns @a HAM_INV_KEYSIZE.
 *       <li>@a HAM_ENABLE_DUPLICATES</li> Enable duplicate keys for this
 *            Database. By default, duplicate keys are disabled.
 *       <li>@a HAM_RECORD_NUMBER</li> Creates an "auto-increment" Database.
 *            Keys in Record Number Databases are automatically assigned an 
 *            incrementing 64bit value. If key->data is not NULL
 *            (and key->flags is @a HAM_KEY_USER_ALLOC and key->size is 8),
 *            the value of the current key is returned in @a key (a 
 *            host-endian 64bit number of type ham_u64_t). If key-data is NULL
 *            and key->size is 0, key->data is temporarily allocated by 
 *            hamsterdb.
 *      </ul>
 *
 * @param params An array of ham_parameter_t structures. The following
 *        parameters are available:
 *      <ul>
 *        <li>HAM_PARAM_KEYSIZE</li> The size of the keys in the B+Tree
 *            index. The default size is 21 bytes.
 *      </ul>
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if the @a env pointer is NULL or an
 *              invalid combination of flags was specified
 * @return @a HAM_DATABASE_ALREADY_EXISTS if a Database with this @a name
 *              already exists in this Environment
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated
 * @return @a HAM_LIMITS_REACHED if the maximum number of Databases per 
 *              Environment was already created
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_create_db(ham_env_t *env, ham_db_t *db,
        ham_u16_t name, ham_u32_t flags, ham_parameter_t *params);

/**
 * Opens a Database in a Database Environment
 *
 * @param env A valid Environment handle
 * @param db A valid Database handle, which will point to the opened
 *          Database. To close the handle, use @see ham_close.
 * @param name The name of the Database. If a Database with this name 
 *          does not exist, the function will fail with 
 *          @a HAM_DATABASE_NOT_FOUND.
 * @param flags Optional flags for opening the Database, combined with
 *        bitwise OR. Possible flags are:
 *     <ul>
 *       <li>@a HAM_DISABLE_VAR_KEYLEN</li> Do not allow the use of variable
 *            length keys. Inserting a key, which is larger than the
 *            B+Tree index key size, returns @a HAM_INV_KEYSIZE.
 *     </ul>
 *
 * @param params An array of ham_parameter_t structures; unused, set 
 *        to NULL.
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if the @a env pointer is NULL or an
 *              invalid combination of flags was specified
 * @return @a HAM_DATABASE_NOT_FOUND if a Database with this @a name
 *              does not exist in this Environment.
 * @return @a HAM_DATABASE_ALREADY_OPEN if this Database was already
 *              opened
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_open_db(ham_env_t *env, ham_db_t *db,
        ham_u16_t name, ham_u32_t flags, ham_parameter_t *params);

/**
 * Renames a Database in an Environment.
 *
 * @param env A valid Environment handle.
 * @param oldname The old name of the existing Database. If a Database
 *          with this name does not exist, the function will fail with 
 *          @a HAM_DATABASE_NOT_FOUND.
 * @param newname The new name of this Database. If a Database 
 *          with this name already exists, the function will fail with 
 *          @a HAM_DATABASE_ALREADY_EXISTS.
 * @param flags Optional flags for renaming the Database, combined with
 *        bitwise OR; unused, set to 0.
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if the @a env pointer is NULL or if
 *              the new Database name is reserved
 * @return @a HAM_DATABASE_NOT_FOUND if a Database with this @a name
 *              does not exist in this Environment
 * @return @a HAM_DATABASE_ALREADY_EXISTS if a Database with the new name
 *              already exists
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated
 * @return @a HAM_NOT_READY if the Environment @a env was not initialized
 *              correctly (i.e. not yet opened or created)
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_rename_db(ham_env_t *env, ham_u16_t oldname, 
                ham_u16_t newname, ham_u32_t flags);

/**
 * Deletes a Database from an Environment
 *
 * @param env A valid Environment handle
 * @param name The name of the Database to delete. If a Database 
 *          with this name does not exist, the function will fail with 
 *          @a HAM_DATABASE_NOT_FOUND. If the Database was already opened,
 *          the function will fail with @a HAM_DATABASE_ALREADY_OPEN.
 * @param flags Optional flags for deleting the Database; unused, set to 0.
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if the @a env pointer is NULL or if
 *              the new Database name is reserved
 * @return @a HAM_DATABASE_NOT_FOUND if a Database with this @a name
 *              does not exist
 * @return @a HAM_DATABASE_ALREADY_OPEN if a Database with this name is
 *              still open
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_erase_db(ham_env_t *env, ham_u16_t name, ham_u32_t flags);

/**
 * Enables AES encryption
 *
 * This function enables AES encryption for every Database in the Environment.
 * The AES key is cached in the Environment handle. The AES 
 * encryption/decryption is only active when file chunks are written to 
 * disk/read from disk; the cached pages in RAM are decrypted. Please read 
 * the FAQ for security relevant notes.
 *
 * The encryption has no effect on In-Memory Environments, but the function
 * will return @a HAM_SUCCESS.
 *
 * The encryption will be active till @a ham_env_close is called. If the 
 * Environment handle is reused after calling @a ham_env_close, the 
 * encryption is no longer active. @a ham_env_enable_encryption should 
 * be called immediately after @a ham_env_create[_ex] or @a ham_env_open[_ex],
 * and MUST be called before @a ham_env_create_db or @a ham_env_open_db.
 *
 * @param env A valid Environment handle
 * @param key A 128bit AES key
 * @param flags Optional flags for encrypting; unused, set to 0
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if one of the parameters is NULL
 * @return @a HAM_DATABASE_ALREADY_OPEN if this function was called AFTER
 *              @a ham_env_open_db or @a ham_env_create_db
 * @return @a HAM_NOT_IMPLEMENTED if hamsterdb was compiled without support
 *              for AES encryption
 * @return @a HAM_ACCESS_DENIED if the key (= password) was wrong
 * @return @a HAM_ALREADY_INITIALIZED if encryption is already enabled
 *              for this Environment
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_enable_encryption(ham_env_t *env, ham_u8_t key[16], ham_u32_t flags);

/**
 * Returns the names of all Databases in an Environment
 *
 * This function returns the names of all Databases and the number of 
 * Databases in an Environment.
 *
 * The memory for @a names must be allocated by the user. @a count
 * must be the size of @a names when calling the function, and will be
 * the number of Databases when the function returns. The function returns
 * @a HAM_LIMITS_REACHED if @a names is not big enough; in this case, the
 * caller should resize the array and call the function again.
 *
 * @param env A valid Environment handle
 * @param names Pointer to an array for the Database names
 * @param count Pointer to the size of the array; will be used to store the
 *          number of Databases when the function returns.
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if @a env, @a names or @a count is NULL
 * @return @a HAM_LIMITS_REACHED if @a names is not large enough to hold
 *          all Database names
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_get_database_names(ham_env_t *env, ham_u16_t *names, ham_size_t *count);

/**
 * Closes the Database Environment
 *
 * This function closes the Database Environment. It does not free the 
 * memory resources allocated in the @a env handle - use @a ham_env_delete 
 * to free @a env.
 *
 * If the flag @a HAM_AUTO_CLEANUP is specified, hamsterdb automatically
 * calls @a ham_close with flag @a HAM_AUTO_CLEANUP on all open Databases
 * (which closes all open Databases and their Cursors). This invalidates the
 * ham_db_t and ham_cursor_t handles!
 *
 * If the flag is not specified, the application must close all Database 
 * handles with @a ham_close to prevent memory leaks.
 *
 * @param env A valid Environment handle
 * @param flags Optional flags for closing the handle. Possible flags are:
 *          <ul>
 *            <li>@a HAM_AUTO_CLEANUP. Calls @a ham_close with the flag 
 *                @a HAM_AUTO_CLEANUP on every open Database
 *          </ul>
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if @a env is NULL
 * @return @a HAM_ENV_NOT_EMPTY if there are still Databases open
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_close(ham_env_t *env, ham_u32_t flags);

/**
 * @}
 */

/**
 * @defgroup ham_db hamsterdb Database Functions
 * @{
 */

/**
 * Allocates a ham_db_t handle
 *
 * @param db Pointer to the pointer which is allocated
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_OUT_OF_MEMORY if memory allocation failed
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_new(ham_db_t **db);

/**
 * Frees a ham_db_t handle
 *
 * Frees the memory and resources of a ham_db_t structure, but does not 
 * close the Database. Call this function <b>AFTER</b> you have closed the
 * Database using @a ham_close, or you will lose your data!
 *
 * @param db A valid Database handle
 *
 * @return This function always returns @a HAM_SUCCESS
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_delete(ham_db_t *db);

/**
 * Creates a Database
 *
 * @param db A valid Database handle
 * @param filename The filename of the Database file. If the file already
 *          exists, it is overwritten. Can be NULL if you create an
 *          In-Memory Database
 * @param flags Optional flags for opening the Database, combined with
 *        bitwise OR. For allowed flags, see @a ham_create_ex.
 * @param mode File access rights for the new file. This is the @a mode
 *        parameter for creat(2). Ignored on Microsoft Windows.
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if the @a db pointer is NULL or an
 *              invalid combination of flags was specified
 * @return @a HAM_IO_ERROR if the file could not be opened or
 *              reading/writing failed
 * @return @a HAM_INV_FILE_VERSION if the Database version is not
 *              compatible with the library version
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated
 * @return @a HAM_WOULD_BLOCK if another process has locked the file
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_create(ham_db_t *db, const char *filename,
        ham_u32_t flags, ham_u32_t mode);

/**
 * Creates a Database - extended version
 *
 * @param db A valid Database handle
 * @param filename The filename of the Database file. If the file already
 *          exists, it will be overwritten. Can be NULL if you create an
 *          In-Memory Database
 * @param flags Optional flags for opening the Database, combined with
 *        bitwise OR. Possible flags are:
 *      <ul>
 *       <li>@a HAM_WRITE_THROUGH</li> Immediately write modified pages to the
 *            disk. This slows down all Database operations, but may
 *            save the Database integrity in case of a system crash.
 *       <li>@a HAM_USE_BTREE</li> Use a B+Tree for the index structure.
 *            Currently enabled by default, but future releases
 *            of hamsterdb will offer additional index structures,
 *            i.e. hash tables.
 *       <li>@a HAM_DISABLE_VAR_KEYLEN</li> Do not allow the use of variable
 *            length keys. Inserting a key, which is larger than the
 *            B+Tree index key size, returns @a HAM_INV_KEYSIZE.
 *       <li>@a HAM_IN_MEMORY_DB</li> Creates an In-Memory Database. No file
 *            will be created, and the Database contents are lost after
 *            the Database is closed. The @a filename parameter can
 *            be NULL. Do <b>NOT</b> use in combination with
 *            @a HAM_CACHE_STRICT and do <b>NOT</b> specify @a cachesize
 *            other than 0.
 *       <li>@a HAM_RECORD_NUMBER</li> Creates an "auto-increment" Database.
 *            Keys in Record Number Databases are automatically assigned an 
 *            incrementing 64bit value. If key->data is not NULL
 *            (and key->flags is @a HAM_KEY_USER_ALLOC and key->size is 8),
 *            the value of the current key is returned in @a key (a 
 *            host-endian 64bit number of type ham_u64_t). If key-data is NULL
 *            and key->size is 0, key->data is temporarily allocated by 
 *            hamsterdb.
 *       <li>@a HAM_ENABLE_DUPLICATES</li> Enable duplicate keys for this
 *            Database. By default, duplicate keys are disabled.
 *       <li>@a HAM_DISABLE_MMAP</li> Do not use memory mapped files for I/O.
 *            By default, hamsterdb checks if it can use mmap,
 *            since mmap is faster than read/write. For performance
 *            reasons, this flag should not be used.
 *       <li>@a HAM_CACHE_STRICT</li> Do not allow the cache to grow larger
 *            than @a cachesize. If a Database operation needs to resize the
 *            cache, it will return @a HAM_CACHE_FULL.
 *            If the flag is not set, the cache is allowed to allocate
 *            more pages than the maximum cache size, but only if it's
 *            necessary and only for a short time.
 *       <li>@a HAM_DISABLE_FREELIST_FLUSH</li> Do not immediately write back
 *            modified freelist pages. Using this flag leads to small
 *            performance improvements, but may prove to be risky
 *            in case of a system crash or program crash.
 *       <li>@a HAM_LOCK_EXCLUSIVE</li> Place an exclusive lock on the
 *            file. Only one process may hold an exclusive lock for
 *            a given file at a given time.
 *      </ul>
 *
 * @param mode File access rights for the new file. This is the @a mode
 *        parameter for creat(2). Ignored on Microsoft Windows.
 * @param param An array of ham_parameter_t structures. The following
 *        parameters are available:
 *      <ul>
 *        <li>HAM_PARAM_CACHESIZE</li> The size of the Database cache,
 *            in bytes. The default size is defined in src/config.h
 *            as HAM_DEFAULT_CACHESIZE - usually 256kb.
 *        <li>HAM_PARAM_PAGESIZE</li> The size of a file page, in
 *            bytes. It is recommended not to change the default size. The
 *            default size depends on hardware and operating system.
 *            Page sizes must be a multiple of 1024.
 *        <li>HAM_PARAM_KEYSIZE</li> The size of the keys in the B+Tree
 *            index. The default size is 21 bytes.
 *      </ul>
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if the @a db pointer is NULL or an
 *              invalid combination of flags was specified
 * @return @a HAM_IO_ERROR if the file could not be opened or
 *              reading/writing failed
 * @return @a HAM_INV_FILE_VERSION if the Database version is not
 *              compatible with the library version
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated
 * @return @a HAM_INV_PAGESIZE if @a pagesize is not a multiple of 1024
 * @return @a HAM_INV_KEYSIZE if @a keysize is too large (at least 4
 *              keys must fit in a page)
 * @return @a HAM_WOULD_BLOCK if another process has locked the file
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_create_ex(ham_db_t *db, const char *filename,
        ham_u32_t flags, ham_u32_t mode, ham_parameter_t *param);

/**
 * Opens an existing Database
 *
 * @param db A valid Database handle
 * @param filename The filename of the Database file
 * @param flags Optional flags for opening the Database, combined with
 *        bitwise OR. See the documentation of @a ham_open_ex
 *        for the allowed flags
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if the @a db pointer is NULL or an
 *              invalid combination of flags was specified
 * @return @a HAM_FILE_NOT_FOUND if the file does not exist
 * @return @a HAM_IO_ERROR if the file could not be opened or reading failed
 * @return @a HAM_INV_FILE_VERSION if the Database version is not
 *              compatible with the library version
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated
 * @return @a HAM_WOULD_BLOCK if another process has locked the file
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_open(ham_db_t *db, const char *filename, ham_u32_t flags);

/**
 * Opens an existing Database - extended version
 *
 * @param db A valid Database handle
 * @param filename The filename of the Database file
 * @param flags Optional flags for opening the Database, combined with
 *        bitwise OR. Possible flags are:
 *      <ul>
 *       <li>@a HAM_READ_ONLY</li> Opens the file for reading only.
 *            Operations which need write access (i.e. @a ham_insert) will
 *            return @a HAM_DB_READ_ONLY.
 *       <li>@a HAM_WRITE_THROUGH</li> Immediately write modified pages
 *            to the disk. This slows down all Database operations, but
 *            could save the Database integrity in case of a system crash.
 *       <li>@a HAM_DISABLE_VAR_KEYLEN</li> Do not allow the use of variable
 *            length keys. Inserting a key, which is larger than the
 *            B+Tree index key size, returns @a HAM_INV_KEYSIZE.
 *       <li>@a HAM_DISABLE_MMAP</li> Do not use memory mapped files for I/O.
 *            By default, hamsterdb checks if it can use mmap,
 *            since mmap is faster than read/write. For performance
 *            reasons, this flag should not be used.
 *       <li>@a HAM_CACHE_STRICT</li> Do not allow the cache to grow larger
 *            than @a cachesize. If a Database operation needs to resize the
 *            cache, it will return @a HAM_CACHE_FULL.
 *            If the flag is not set, the cache is allowed to allocate
 *            more pages than the maximum cache size, but only if it's
 *            necessary and only for a short time.
 *       <li>@a HAM_DISABLE_FREELIST_FLUSH</li> Do not immediately write back
 *            modified freelist pages. Using this flag leads to small
 *            performance improvements, but may prove to be risky
 *            in case of a system crash or program crash.
 *       <li>@a HAM_LOCK_EXCLUSIVE</li> Place an exclusive lock on the
 *            file. Only one process may hold an exclusive lock for
 *            a given file at a given time.
 *      </ul>
 *
 * @param param An array of ham_parameter_t structures. The following
 *        parameters are available:
 *      <ul>
 *        <li>HAM_PARAM_CACHESIZE</li> The size of the Database cache,
 *            in bytes. The default size is defined in src/config.h
 *            as HAM_DEFAULT_CACHESIZE - usually 256kb.
 *      </ul>
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if the @a db pointer is NULL or an
 *              invalid combination of flags was specified
 * @return @a HAM_FILE_NOT_FOUND if the file does not exist
 * @return @a HAM_IO_ERROR if the file could not be opened or reading failed
 * @return @a HAM_INV_FILE_VERSION if the Database version is not
 *              compatible with the library version
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated
 * @return @a HAM_WOULD_BLOCK if another process has locked the file
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_open_ex(ham_db_t *db, const char *filename,
        ham_u32_t flags, ham_parameter_t *param);

/** Flag for @a ham_open, @a ham_open_ex, @a ham_create, @a ham_create_ex */
#define HAM_WRITE_THROUGH            0x00000001

/** Flag for @a ham_open, @a ham_open_ex */
#define HAM_READ_ONLY                0x00000004

/* unused                            0x00000008 */

/** Flag for @a ham_create, @a ham_create_ex */
#define HAM_USE_BTREE                0x00000010

/* reserved                          0x00000020 */

/** Flag for @a ham_create, @a ham_create_ex */
#define HAM_DISABLE_VAR_KEYLEN       0x00000040

/** Flag for @a ham_create, @a ham_create_ex */
#define HAM_IN_MEMORY_DB             0x00000080

/* reserved                          0x00000100 */

/** Flag for @a ham_open, @a ham_open_ex, @a ham_create, @a ham_create_ex */
#define HAM_DISABLE_MMAP             0x00000200

/** Flag for @a ham_open, @a ham_open_ex, @a ham_create, @a ham_create_ex */
#define HAM_CACHE_STRICT             0x00000400

/** Flag for @a ham_open, @a ham_open_ex, @a ham_create, @a ham_create_ex */
#define HAM_DISABLE_FREELIST_FLUSH   0x00000800

/** Flag for @a ham_open, @a ham_open_ex, @a ham_create, @a ham_create_ex */
#define HAM_LOCK_EXCLUSIVE           0x00001000

/** Flag for @a ham_create, @a ham_create_ex, @a ham_env_create_db */
#define HAM_RECORD_NUMBER            0x00002000

/** Flag for @a ham_create, @a ham_create_ex */
#define HAM_ENABLE_DUPLICATES        0x00004000

/** Parameter name for @a ham_open_ex, @a ham_create_ex; sets the cache
 * size */
#define HAM_PARAM_CACHESIZE          0x00000100

/** Parameter name for @a ham_open_ex, @a ham_create_ex; sets the page
 * size */
#define HAM_PARAM_PAGESIZE           0x00000101

/** Parameter name for @a ham_create_ex; sets the key size */
#define HAM_PARAM_KEYSIZE            0x00000102

/** Parameter name for @a ham_env_create_ex; sets the number of maximum
 * Databases */
#define HAM_PARAM_MAX_ENV_DATABASES  0x00000103

/**
 * Returns the last error code
 *
 * @param db A valid Database handle
 *
 * @return The last error code which was returned by one of the
 *         hamsterdb API functions. Use @a ham_strerror to translate
 *         this code to a descriptive string
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_get_error(ham_db_t *db);

/**
 * Typedef for a prefix comparison function
 *
 * @remark This function compares two index keys. It returns -1 if @a lhs
 * ("left-hand side", the paramter on the left side) is smaller than 
 * @a rhs ("right-hand side"), 0 if both keys are equal, and 1 if @a lhs 
 * is larger than @a rhs.
 *
 * @remark If one of the keys is only partially loaded, but the comparison
 * function needs the full key, the return value should be
 * HAM_PREFIX_REQUEST_FULLKEY.
 */
typedef int (*ham_prefix_compare_func_t)
                                 (ham_db_t *db, 
                                  const ham_u8_t *lhs, ham_size_t lhs_length, 
                                  ham_size_t lhs_real_length,
                                  const ham_u8_t *rhs, ham_size_t rhs_length,
                                  ham_size_t rhs_real_length);

/**
 * Sets the prefix comparison function
 *
 * The prefix comparison function is called when an index uses
 * keys with variable length and at least one of the two keys is loaded
 * only partially.
 *
 * If @a foo is NULL, hamsterdb will not use any prefix comparison.
 *
 * @param db A valid Database handle
 * @param foo A pointer to the prefix compare function
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if the @a db parameter is NULL
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_set_prefix_compare_func(ham_db_t *db, ham_prefix_compare_func_t foo);

/**
 * Typedef for a comparison function
 *
 * @remark This function compares two index keys. It returns -1, if @a lhs
 * ("left-hand side", the paramter on the left side) is smaller than 
 * @a rhs ("right-hand side"), 0 if both keys are equal, and 1 if @a lhs 
 * is larger than @a rhs.
 */
typedef int (*ham_compare_func_t)(ham_db_t *db, 
                                  const ham_u8_t *lhs, ham_size_t lhs_length, 
                                  const ham_u8_t *rhs, ham_size_t rhs_length);

/**
 * Sets the comparison function
 *
 * The comparison function compares two index keys. It returns -1 if the
 * first key is smaller, +1 if the second key is smaller or 0 if both
 * keys are equal.
 *
 * If @a foo is NULL, hamsterdb will use the default compare
 * function (which is based on memcmp(3)).
 *
 * Note that if you use a custom comparison routine in combination with
 * extended keys, it might be useful to disable the prefix comparison, which
 * is based on memcmp(3). See @sa ham_set_prefix_compare_func for details.
 *
 * @param db A valid Database handle
 * @param foo A pointer to the compare function
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if one of the parameters is NULL
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_set_compare_func(ham_db_t *db, ham_compare_func_t foo);

/**
 * Enables zlib compression for all inserted records
 *
 * This function enables zlib compression for all inserted Database records.
 *
 * The compression will be active till @a ham_close is called. If the Database
 * handle is reused after calling @a ham_close, the compression is no longer
 * active. @a ham_enable_compression should be called immediately after
 * @a ham_create[_ex] or @a ham_open[_ex].
 *
 * Note that zlib usually has an overhead and often is not effective if the
 * records are small (i.e. < 128byte), but this highly depends
 * on the data that is inserted.
 *
 * The zlib compression filter does not allow queries (i.e. with @a ham_find)
 * with user-allocated records and the flag @a HAM_RECORD_USER_ALLOC. In this
 * case, the query-function will return @a HAM_INV_PARAMETER.
 *
 * @param db A valid Database handle
 * @param level The compression level. 0 for the zlib default, 1 for
 *      best speed and 9 for minimum size
 * @param flags Optional flags for the compression; unused, set to 0
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if @a db is NULL or @a level is not between
 *      0 and 9
 * @return @a HAM_NOT_IMPLEMENTED if hamsterdb was compiled without support
 *      for compression
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_enable_compression(ham_db_t *db, ham_u32_t level, ham_u32_t flags);

/**
 * Searches an item in the Database
 *
 * This function searches the Database for @a key. If the key
 * is found, @a record will receive the record of this item and
 * @a HAM_SUCCESS is returned. If the key is not found, the function
 * returns @a HAM_KEY_NOT_FOUND.
 *
 * A ham_record_t structure should be initialized with
 * zeroes before it is being used. This can be done with the C library
 * routines memset(3) or bzero(2).
 *
 * If the function completes successfully, the @a record pointer is
 * initialized with the size of the record (in @a record.size) and the
 * actual record data (in @a record.data). If the record is empty, 
 * @a size is 0 and @a data points to NULL.
 *
 * The @a data pointer is a temporary pointer and will be overwritten
 * by subsequent hamsterdb API calls. You can alter this behaviour by
 * allocating the @a data pointer in the application and setting
 * @a record.flags to @a HAM_RECORD_USER_ALLOC. Make sure that the allocated
 * buffer is large enough.
 *
 * @a ham_find can not search for duplicate keys. If @a key has
 * multiple duplicates, only the first duplicate is returned.
 *
 * @param db A valid Database handle
 * @param reserved A reserved value. Set to NULL.
 * @param key The key of the item
 * @param record The record of the item
 * @param flags Optional flags for searching; unused, set to 0
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if @a db, @a key or @a record is NULL
 * @return @a HAM_KEY_NOT_FOUND if the @a key does not exist
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_find(ham_db_t *db, void *reserved, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags);

/**
 * Inserts a Database item
 *
 * This function inserts a key/record pair as a new Database item.
 * 
 * If the key already exists in the Database, error @a HAM_DUPLICATE_KEY
 * is returned.
 *
 * If you wish to overwrite an existing entry specify the 
 * flag @a HAM_OVERWRITE. 
 *
 * If you wish to insert a duplicate key specify the flag @a HAM_DUPLICATE. 
 * (Note that the Database has to be created with @a HAM_ENABLE_DUPLICATES
 * in order to use duplicate keys.)
 * The duplicate key is inserted after all other duplicate keys (see
 * @a HAM_DUPLICATE_INSERT_LAST).
 *
 * Record Number Databases (created with @a HAM_RECORD_NUMBER) expect 
 * either an empty @a key (with a size of 0 and data pointing to NULL),
 * or a user-supplied key (with key.flag @a HAM_KEY_USER_ALLOC, a size
 * of 8 and a valid data pointer). 
 * If key.size is 0 and key.data is NULL, hamsterdb will temporarily 
 * allocate memory for key->data, which will then point to an 8-byte
 * unsigned integer in host-endian.
 *
 * @param db A valid Database handle
 * @param reserved A reserved value. Set to NULL.
 * @param key The key of the new item
 * @param record The record of the new item
 * @param flags Optional flags for inserting. Possible flags are:
 *      <ul>
 *        <li>@a HAM_OVERWRITE. If the @a key already exists, the record is
 *              overwritten. Otherwise, the key is inserted.
 *        <li>@a HAM_DUPLICATE. If the @a key already exists, a duplicate 
 *              key is inserted. The key is inserted before the already
 *              existing key.
 *      </ul>
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if @a db, @a key or @a record is NULL
 * @return @a HAM_INV_PARAMETER if the Database is a Record Number Database
 *              and the key is invalid (see above)
 * @return @a HAM_INV_PARAMETER if the flags @a HAM_OVERWRITE <b>and</b>
 *              @a HAM_DUPLICATE were specified, or if @a HAM_DUPLICATE
 *              was specified, but the Database was not created with 
 *              flag @a HAM_ENABLE_DUPLICATES.
 * @return @a HAM_DB_READ_ONLY if you tried to insert a key in a read-only
 *              Database
 * @return @a HAM_INV_KEYSIZE if the key size is larger than the @a keysize
 *              parameter specified for @a ham_create_ex and variable
 *              key sizes are disabled (see @a HAM_DISABLE_VAR_KEYLEN)
 *              OR if the @a keysize parameter specified for @a ham_create_ex
 *              is smaller than 8
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_insert(ham_db_t *db, void *reserved, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags);

/** Flag for @a ham_insert and @a ham_cursor_insert */
#define HAM_OVERWRITE                   1

/** Flag for @a ham_insert and @a ham_cursor_insert */
#define HAM_DUPLICATE                   2

/** Flag for @a ham_cursor_insert */
#define HAM_DUPLICATE_INSERT_BEFORE     4

/** Flag for @a ham_cursor_insert */
#define HAM_DUPLICATE_INSERT_AFTER      8

/** Flag for @a ham_cursor_insert */
#define HAM_DUPLICATE_INSERT_FIRST     16

/** Flag for @a ham_cursor_insert */
#define HAM_DUPLICATE_INSERT_LAST      32

/**
 * Erases a Database item
 *
 * This function erases a Database item. If the item @a key
 * does not exist, @a HAM_KEY_NOT_FOUND is returned.
 *
 * Note that ham_erase can not erase a single duplicate key. If the key 
 * has multiple duplicates, all duplicates of this key will be erased. Use 
 * @a ham_cursor_erase to erase a specific duplicate key.
 *
 * @param db A valid Database handle
 * @param reserved A reserved value. Set to NULL.
 * @param key The key to delete
 * @param flags Optional flags for erasing; unused, set to 0
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if @a db or @a key is NULL
 * @return @a HAM_DB_READ_ONLY if you tried to erase a key from a read-only
 *              Database
 * @return @a HAM_KEY_NOT_FOUND if @a key was not found
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_erase(ham_db_t *db, void *reserved, ham_key_t *key, ham_u32_t flags);

/**
 * Flushes the Database
 *
 * This function flushes the Database cache and writes the whole file
 * to disk. If this Database was opened in an Environment, all other
 * Databases of this Environment are flushed as well.
 *
 * Since In-Memory Databases do not have a file on disk, the
 * function will have no effect and will return @a HAM_SUCCESS.
 *
 * @param db A valid Database handle
 * @param flags Optional flags for flushing; unused, set to 0
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if @a db is NULL
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_flush(ham_db_t *db, ham_u32_t flags);

/**
 * Closes the Database
 *
 * This function flushes the Database and then closes the file handle.
 * It does not free the memory resources allocated in the @a db handle -
 * use @a ham_delete to free @a db.
 *
 * If the flag @a HAM_AUTO_CLEANUP is specified, hamsterdb automatically
 * calls @a ham_cursor_close on all open Cursors. This invalidates the
 * ham_cursor_t handle! 
 *
 * If the flag is not specified, the application must close all Database 
 * Cursors with @a ham_cursor_close to prevent memory leaks.
 *
 * This function removes all file-level filters installed 
 * with @a ham_add_file_filter.
 *
 * @param db A valid Database handle
 * @param flags Optional flags for closing the Database. Possible values are:
 *      <ul>
 *       <li>@a HAM_AUTO_CLEANUP. Automatically closes all open Cursors
 *      </ul>
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if @a db is NULL
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_close(ham_db_t *db, ham_u32_t flags);

/** Flag for @a ham_close, @a ham_env_close */
#define HAM_AUTO_CLEANUP            1

/**
 * @}
 */

/**
 * @defgroup ham_cursor hamsterdb Cursor Functions
 * @{
 */

/**
 * Creates a Database Cursor
 *
 * Creates a new Database Cursor. Cursors can be used to
 * traverse the Database from start to end or vice versa. Cursors
 * can also be used to insert, delete or search Database items.
 *
 * A newly created Cursor does not point to any item in the Database.
 *
 * The application should close all Database Cursors before closing
 * the Database.
 *
 * @param db A valid Database handle
 * @param reserved A reserved value. Set to NULL.
 * @param flags Optional flags for creating the Cursor; unused, set to 0
 * @param cursor A pointer to a pointer which is allocated for the
 *          new Cursor handle
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if @a db or @a cursor is NULL
 * @return @a HAM_OUT_OF_MEMORY if the new structure could not be allocated
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_cursor_create(ham_db_t *db, void *reserved, ham_u32_t flags,
        ham_cursor_t **cursor);

/**
 * Clones a Database Cursor
 *
 * Clones an existing Cursor. The new Cursor will point to
 * exactly the same item as the old Cursor. If the old Cursor did not point
 * to any item, so will the new Cursor.
 *
 * @param src The existing Cursor
 * @param dest A pointer to a pointer, which is allocated for the
 *          cloned Cursor handle
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if @a src or @a dest is NULL
 * @return @a HAM_OUT_OF_MEMORY if the new structure could not be allocated
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_cursor_clone(ham_cursor_t *src, ham_cursor_t **dest);

/**
 * Moves the Cursor
 *
 * Moves the Cursor. Use the @a flags to specify the direction.
 * After the move, key and record of the item are returned, if @a key
 * and/or @a record are valid pointers.
 *
 * If the direction is not specified, the Cursor will not move. Do not
 * specify a direction if you want to fetch the key and/or record of
 * the current item.
 *
 * @param cursor A valid Cursor handle
 * @param key An optional pointer to a @a ham_key_t structure. If this
 *      pointer is not NULL, the key of the new item is returned.
 *      Note that key->data will point to temporary data. This pointer
 *      will be invalidated by subsequent hamsterdb API calls. See
 *      @a HAM_KEY_USER_ALLOC on how to change this behaviour.
 * @param record An optional pointer to a @a ham_record_t structure. If this
 *      pointer is not NULL, the record of the new item is returned.
 *      Note that record->data will point to temporary data. This pointer
 *      will be invalidated by subsequent hamsterdb API calls. See
 *      @a HAM_RECORD_USER_ALLOC on how to change this behaviour.
 * @param flags The flags for this operation. They are used to specify
 *      the direction for the "move". If you do not specify a direction,
 *      the Cursor will remain on the current position.
 *          @a HAM_CURSOR_FIRST positions the Cursor on the first item
 *              in the Database
 *          @a HAM_CURSOR_LAST positions the Cursor on the last item
 *              in the Database
 *          @a HAM_CURSOR_NEXT positions the Cursor on the next item
 *              in the Database; if the Cursor does not point to any
 *              item, the function behaves as if direction was
 *              @a HAM_CURSOR_FIRST.
 *          @a HAM_CURSOR_PREVIOUS positions the Cursor on the previous item
 *              in the Database; if the Cursor does not point to any
 *              item, the function behaves as if direction was
 *              @a HAM_CURSOR_LAST.
 *          @a HAM_SKIP_DUPLICATES: skip duplicate keys of the current key.
 *              Not allowed in combination with @a HAM_ONLY_DUPLICATES.
 *          @a HAM_ONLY_DUPLICATES: only move through duplicate keys of the
 *              current key. Not allowed in combination with 
 *              @a HAM_SKIP_DUPLICATES.
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if @a cursor is NULL, or if an invalid
 *              combination of flags was specified
 * @return @a HAM_CURSOR_IS_NIL if the Cursor does not point to an item, but
 *              key and/or record were requested
 * @return @a HAM_KEY_NOT_FOUND if @a cursor points to the first (or last)
 *              item, and a move to the previous (or next) item was
 *              requested
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_cursor_move(ham_cursor_t *cursor, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags);

/** Flag for @a ham_cursor_move */
#define HAM_CURSOR_FIRST            1

/** Flag for @a ham_cursor_move */
#define HAM_CURSOR_LAST             2

/** Flag for @a ham_cursor_move */
#define HAM_CURSOR_NEXT             4

/** Flag for @a ham_cursor_move */
#define HAM_CURSOR_PREVIOUS         8

/** Flag for @a ham_cursor_move */
#define HAM_SKIP_DUPLICATES        16

/** Flag for @a ham_cursor_move */
#define HAM_ONLY_DUPLICATES        32

/**
 * Overwrites the current record
 *
 * This function overwrites the record of the current item.
 *
 * @param cursor A valid Cursor handle
 * @param record A valid record structure
 * @param flags Optional flags for overwriting the item; unused, set to 0
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if @a cursor or @a record is NULL
 * @return @a HAM_CURSOR_IS_NIL if the Cursor does not point to an item
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_cursor_overwrite(ham_cursor_t *cursor, ham_record_t *record,
            ham_u32_t flags);

/**
 * Searches a key and points the Cursor to this key
 *
 * Searches for an item in the Database and points the
 * Cursor to this item. If the item could not be found, the Cursor is
 * not modified.
 *
 * Note that @a ham_cursor_find can not search for duplicate keys. If @a key 
 * has multiple duplicates, only the first duplicate is returned.
 *
 * @param cursor A valid Cursor handle
 * @param key A valid key structure
 * @param flags Optional flags for searching the item; unused, set to 0
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if @a cursor or @a key is NULL
 * @return @a HAM_KEY_NOT_FOUND if the requested key was not found
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_cursor_find(ham_cursor_t *cursor, ham_key_t *key, ham_u32_t flags);

/**
 * Inserts a Database item and points the Cursor to the inserted item
 *
 * This function inserts a key/record pair as a new Database item.
 * If the key already exists in the Database, error @a HAM_DUPLICATE_KEY
 * is returned. 
 *
 * If you wish to overwrite an existing entry specify the 
 * flag @a HAM_OVERWRITE. 
 *
 * If you wish to insert a duplicate key specify the flag @a HAM_DUPLICATE. 
 * (Note that the Database has to be created with @a HAM_ENABLE_DUPLICATES, 
 * in order to use duplicate keys.)
 * By default, the duplicate key is inserted after all other duplicate keys 
 * (see @a HAM_DUPLICATE_INSERT_LAST). This behaviour can be overwritten by
 * specifying @a HAM_DUPLICATE_INSERT_FIRST, @a HAM_DUPLICATE_INSERT_BEFORE
 * or @a HAM_DUPLICATE_INSERT_AFTER.
 *
 * After inserting, the Cursor will point to the new item. If inserting
 * the item failed, the Cursor is not modified.
 *
 * Record Number Databases (created with @a HAM_RECORD_NUMBER) expect 
 * either an empty @a key (with a size of 0 and data pointing to NULL),
 * or a user-supplied key (with key.flag @a HAM_KEY_USER_ALLOC, a size
 * of 8 and a valid data pointer). 
 * If key.size is 0 and key.data is NULL, hamsterdb will temporarily 
 * allocate memory for key->data, which will then point to an 8-byte
 * unsigned integer in host-endian.
 *
 * @param cursor A valid Cursor handle
 * @param key A valid key structure
 * @param record A valid record structure
 * @param flags Optional flags for inserting the item, combined with
 *        bitwise OR. Possible flags are:
 *      <ul>
 *        <li>@a HAM_OVERWRITE. If the @a key already exists, the record is
 *              overwritten. Otherwise, the key is inserted.
 *        <li>@a HAM_DUPLICATE. If the @a key already exists, a duplicate 
 *              key is inserted. Same as @a HAM_DUPLICATE_INSERT_LAST.
 *        <li>@a HAM_DUPLICATE_INSERT_BEFORE. If the @a key already exists, 
 *              a duplicate key is inserted before the duplicate pointed
 *              to by the Cursor.
 *        <li>@a HAM_DUPLICATE_INSERT_AFTER. If the @a key already exists, 
 *              a duplicate key is inserted after the duplicate pointed
 *              to by the Cursor.
 *        <li>@a HAM_DUPLICATE_INSERT_FIRST. If the @a key already exists, 
 *              a duplicate key is inserted as the first duplicate of 
 *              the current key.
 *        <li>@a HAM_DUPLICATE_INSERT_LAST. If the @a key already exists, 
 *              a duplicate key is inserted as the last duplicate of 
 *              the current key.
 *      </ul>
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if @a key or @a record is NULL
 * @return @a HAM_INV_PARAMETER if the Database is a Record Number Database
 *              and the key is invalid (see above)
 * @return @a HAM_INV_PARAMETER if the flags @a HAM_OVERWRITE <b>and</b>
 *              @a HAM_DUPLICATE were specified, or if @a HAM_DUPLICATE
 *              was specified, but the Database was not created with 
 *              flag @a HAM_ENABLE_DUPLICATES.
 * @return @a HAM_DB_READ_ONLY if you tried to insert a key to a read-only
 *              Database.
 * @return @a HAM_INV_KEYSIZE if the key's size is larger than the @a keysize
 *              parameter specified for @a ham_create_ex and variable
 *              key sizes are disabled (see @a HAM_DISABLE_VAR_KEYLEN)
 *              OR if the @a keysize parameter specified for @a ham_create_ex
 *              is smaller than 8.
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_cursor_insert(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags);

/**
 * Erases the current key
 *
 * Erases a key from the Database. If the erase was
 * successful, the Cursor is invalidated and does no longer point to
 * any item. In case of an error, the Cursor is not modified.
 *
 * If the Database was opened with the flag @a HAM_ENABLE_DUPLICATES,
 * this function erases only the duplicate item to which the Cursor refers.
 *
 * @param cursor A valid Cursor handle
 * @param flags Optional flags for erasing the key; unused, set to 0.
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if @a cursor is NULL
 * @return @a HAM_DB_READ_ONLY if you tried to erase a key from a read-only
 *              Database
 * @return @a HAM_CURSOR_IS_NIL if the Cursor does not point to an item
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_cursor_erase(ham_cursor_t *cursor, ham_u32_t flags);

/**
 * Gets the number of duplicate keys
 *
 * Returns the number of duplicate keys of the item to which the
 * Cursor currently refers.
 * Returns 1 if the key has no duplicates.
 *
 * @param cursor A valid Cursor handle
 * @param count Returns the number of duplicate keys
 * @param flags Optional flags; unused, set to 0.
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_CURSOR_IS_NIL if the Cursor does not point to an item
 * @return @a HAM_INV_PARAMETER if @a cursor or @a count is NULL
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_cursor_get_duplicate_count(ham_cursor_t *cursor, 
        ham_size_t *count, ham_u32_t flags);

/**
 * Closes a Database Cursor
 *
 * Closes a Cursor and frees allocated memory. All Cursors
 * should be closed before closing the Database (see @a ham_close).
 *
 * @param cursor A valid Cursor handle
 *
 * @return @a HAM_SUCCESS upon success
 * @return @a HAM_INV_PARAMETER if @a cursor is NULL
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_cursor_close(ham_cursor_t *cursor);

/*
 * @}
 */

#ifdef __cplusplus
} // extern "C"
#endif

#endif /* HAM_HAMSTERDB_H__ */
