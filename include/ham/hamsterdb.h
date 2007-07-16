/**
 * \mainpage hamsterdb embeddable database
 * \file hamsterdb.h
 * \brief Include file for hamsterdb.
 * \author Christoph Rupp, chris@crupp.de
 * \version 0.4.4
 *
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 */

#ifndef HAM_HAMSTERDB_H__
#define HAM_HAMSTERDB_H__

#ifdef __cplusplus
extern "C" {
#endif

#include <ham/types.h>

/**
 * The hamsterdb database structure.
 *
 * This structure is allocated with @a ham_new and deleted with
 * @a ham_delete.
 */
struct ham_db_t;
typedef struct ham_db_t ham_db_t;

/**
 * The hamsterdb environment structure.
 *
 * This structure is allocated with @a ham_env_new and deleted with
 * @a ham_env_delete.
 */
struct ham_env_t;
typedef struct ham_env_t ham_env_t;

/**
 * A database cursor.
 *
 * A cursor is used for bi-directionally traversing the database and
 * for inserting/deleting/searching database items.
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
 *
 */
typedef struct
{
    /** The size of the record data, in bytes */
    ham_size_t size;

    /** The record data, usually a temporary pointer allocated by hamsterdb */
    void *data;

    /** The record flags */
    ham_u32_t flags;

    /** For internal use */
    ham_size_t _allocsize;

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
 * However, when using database cursors and the function @a ham_cursor_move,
 * hamsterdb also returns keys. In this case, the pointer to the key
 * data is provided in @a data. This pointer is only temporary and will be
 * overwritten by subsequent calls to @a ham_cursor_move.
 *
 * To avoid this, the calling application can allocate the @a data pointer.
 * In this case, you have to set the flag @a HAM_KEY_USER_ALLOC. The
 * @a size parameter will then return the size of the key. It's the
 * responsibility of the caller to make sure that the @a data parameter is
 * large enough for the key.
 *
 */
typedef struct
{
    /** The size of the key, in bytes */
    ham_u16_t size;

    /** The data of the key */
    void *data;

    /** The flags of the key */
    ham_u32_t flags;

    /** For internal use */
    ham_u32_t _flags;
} ham_key_t;

/** Flag for @a ham_key_t in combination with @a ham_cursor_move */
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

    /** The value of the parameter. Pointer values are casted into
     * @a value like this:
     * <pre>
     *   // a string value - cast the pointer to ham_u64_t:
     *   value=(ham_u64_t)"hello world";
     * </pre>
     */
    ham_u64_t value;

} ham_parameter_t;

/**
 * @defgroup ham_status_codes hamsterdb Status Codes
 * @{
 */

/** Operation completed successfully */
#define HAM_SUCCESS                  (  0)
/** Failed to read the database file */
#define HAM_SHORT_READ               ( -1)
/** Failed to write the database file */
#define HAM_SHORT_WRITE              ( -2)
/** Invalid key size */
#define HAM_INV_KEYSIZE              ( -3)
/** Invalid page size (must be a not a multiple of 1024) */
#define HAM_INV_PAGESIZE             ( -4)
/** Database is already open */
#define HAM_DB_ALREADY_OPEN          ( -5)
/** Memory allocation failed - out of memory */
#define HAM_OUT_OF_MEMORY            ( -6)
/** Invalid backend index */
#define HAM_INV_INDEX                ( -7)
/** Invalid function parameter */
#define HAM_INV_PARAMETER            ( -8)
/** Invalid database file header */
#define HAM_INV_FILE_HEADER          ( -9)
/** Invalid database file version */
#define HAM_INV_FILE_VERSION         (-10)
/** Key was not found */
#define HAM_KEY_NOT_FOUND            (-11)
/** Tried to insert a key which already exists */
#define HAM_DUPLICATE_KEY            (-12)
/** Internal database integrity violated */
#define HAM_INTEGRITY_VIOLATED       (-13)
/** Internal hamsterdb error */
#define HAM_INTERNAL_ERROR           (-14)
/** Tried to modify the database, but the file was opened as read-only */
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
/** Database file not found */
#define HAM_FILE_NOT_FOUND           (-21)
/** Operation would block */
#define HAM_WOULD_BLOCK              (-22)
/** Object was not initialized correctly */
#define HAM_NOT_READY                (-23)
/** Cursor does not point to a valid database item */
#define HAM_CURSOR_IS_NIL           (-100)
/** Not all databases were closed before closing the environment */
#define HAM_ENV_NOT_EMPTY           (-200)
/** Database not found */
#define HAM_DATABASE_NOT_FOUND      (-201)
/** Database name already exists */
#define HAM_DATABASE_ALREADY_EXISTS (-202)
/** Database already open */
#define HAM_DATABASE_ALREADY_OPEN   (-203)
/** Environment is full */
#define HAM_ENV_FULL                (-204)

/**
 * @}
 */

/**
 * @defgroup ham_static hamsterdb Static Functions
 * @{
 */

/**
 * A typedef for a custom error handler function.
 *
 * @param message The error message.
 */
typedef void (*ham_errhandler_fun)(const char *message);

/**
 * Sets the global error handler.
 *
 * This handler will receive <b>all</b> debug messages that are emitted
 * by hamsterdb. You can install the default handler by setting @a f to 0.
 *
 * The default error handler prints all messages to stderr. To install a
 * different logging facility, you can provide your own error handler.
 *
 * @param f A pointer to the error handler function, or NULL to restore
 *          the default handler.
 */
HAM_EXPORT void
ham_set_errhandler(ham_errhandler_fun f);

/**
 * Translates a hamsterdb status code to a descriptive error string.
 *
 * @param status The hamsterdb status code.
 *
 * @return Returns a pointer to a descriptive error string.
 */
HAM_EXPORT const char *
ham_strerror(ham_status_t status);

/**
 * Returns the version of the hamsterdb library.
 */
HAM_EXPORT void
ham_get_version(ham_u32_t *major, ham_u32_t *minor,
        ham_u32_t *revision);

/**
 * @}
 */

/**
 * @defgroup ham_env hamsterdb Environment Functions
 * @{
 */
/**
 * Allocates a ham_env_t handle.
 *
 * @param env Pointer to the pointer which is allocated.
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_OUT_OF_MEMORY if memory allocation failed.
 */
HAM_EXPORT ham_status_t
ham_env_new(ham_env_t **env);

/**
 * Deletes a ham_env_t handle.
 *
 * Frees the ham_env_t structure, but does not close the
 * environment. Call this function <b>AFTER</b> you have closed the
 * environment using @a ham_env_close, or you will lose your data!
 *
 * @param env A valid environment handle.
 *
 * @return This function always returns @a HAM_SUCCESS.
 */
HAM_EXPORT ham_status_t
ham_env_delete(ham_env_t *env);

/**
 * Creates a database environment.
 *
 * @param env A valid environment handle.
 * @param filename The filename of the environment file. If the file already
 *          exists, it is overwritten. Can be NULL if you create an
 *          in-memory environment.
 * @param flags Optional flags for opening the environment, combined with
 *        bitwise OR. For allowed flags, see @a ham_env_create_ex.
 * @param mode File access rights for the new file. This is the @a mode
 *        parameter for creat(2). Ignored on Microsoft Windows.
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if the @a env pointer is NULL or an
 *              invalid combination of flags was specified.
 * @return @a HAM_IO_ERROR if the file could not be opened or
 *              reading/writing failed.
 * @return @a HAM_INV_FILE_VERSION if the environment version is not
 *              compatible with the library version.
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated.
 * @return @a HAM_WOULD_BLOCK if another process has locked the file.
 *
 */
HAM_EXPORT ham_status_t
ham_env_create(ham_env_t *env, const char *filename,
        ham_u32_t flags, ham_u32_t mode);

/**
 * Creates a database environment - extended version.
 *
 * @param env A valid environment handle.
 * @param filename The filename of the environment file. If the file already
 *          exists, it is overwritten. Can be NULL if you create an
 *          in-memory environment.
 * @param flags Optional flags for opening the database, combined with
 *        bitwise OR. Possible flags are:
 *
 *      <ul>
 *       <li>@a HAM_WRITE_THROUGH</li> Immediately write modified pages to the
 *            disk. This slows down all database operations, but may
 *            save the database integrity in case of a system crash.
 *       <li>@a HAM_IN_MEMORY_DB</li> Creates an in-memory environment. No 
 *            file will be created, and the database contents are lost after
 *            the environment is closed. The @a filename parameter can
 *            be NULL. Do <b>NOT</b> use in combination with
 *            @a HAM_CACHE_STRICT and do <b>NOT</b> specify @a cachesize
 *            other than 0.
 *       <li>@a HAM_DISABLE_MMAP</li> Do not use memory mapped files for I/O.
 *            By default, hamsterdb checks if it can use mmap,
 *            since mmap is faster than read/write. For performance
 *            reasons, this flag should not be used.
 *       <li>@a HAM_CACHE_STRICT</li> Do not allow the cache to grow larger
 *            than @a cachesize. If a database operation needs to resize the
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
 *            a given file at a given time. This flag will block the
 *            operation if the lock is held by another process.
 *      </ul>
 *
 * @param mode File access rights for the new file. This is the @a mode
 *        parameter for creat(2). Ignored on Microsoft Windows.
 * @param param An array of ham_parameter_t structures. The following
 *        parameters are available:
 *      <ul>
 *        <li>HAM_PARAM_CACHESIZE</li> The size of the database cache,
 *            in bytes. The default size is defined in src/config.h
 *            as HAM_DEFAULT_CACHESIZE - usually 256kb.
 *        <li>HAM_PARAM_PAGESIZE</li> The size of the file page, in
 *            bytes. It is recommended not to change the default size. The
 *            default size depends on your hardware and operating system.
 *            Page sizes must be a multiple of 1024.
 *      </ul>
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if the @a env pointer is NULL or an
 *              invalid combination of flags or parameters was specified.
 * @return @a HAM_IO_ERROR if the file could not be opened or
 *              reading/writing failed.
 * @return @a HAM_INV_FILE_VERSION if the environment version is not
 *              compatible with the library version.
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated.
 * @return @a HAM_WOULD_BLOCK if another process has locked the file.
 *
 */
HAM_EXPORT ham_status_t
ham_env_create_ex(ham_env_t *env, const char *filename,
        ham_u32_t flags, ham_u32_t mode, ham_parameter_t *param);

/**
 * Opens an existing database environment.
 *
 * @param env A valid environment handle.
 * @param filename The filename of the environment file.
 * @param flags Optional flags for opening the environment, combined with
 *        bitwise OR. See the documentation of @a ham_env_open_ex
 *        for the allowed flags.
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if the @a env pointer is NULL or an
 *              invalid combination of flags was specified.
 * @return @a HAM_FILE_NOT_FOUND if the file does not exist.
 * @return @a HAM_IO_ERROR if the file could not be opened or reading failed.
 * @return @a HAM_INV_FILE_VERSION if the database version is not
 *              compatible with the library version.
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated.
 * @return @a HAM_WOULD_BLOCK if another process has locked the file
 */
HAM_EXPORT ham_status_t
ham_env_open(ham_env_t *env, const char *filename, ham_u32_t flags);

/**
 * Opens an existing database environment - extended version.
 *
 * @param env A valid environment handle.
 * @param filename The filename of the environment file.
 * @param flags Optional flags for opening the environment, combined with
 *        bitwise OR. Possible flags are:
 *      <ul>
 *       <li>@a HAM_READ_ONLY</li> Opens the file for reading only.
 *            Operations that need write access (i.e. @a ham_insert) will
 *            return @a HAM_DB_READ_ONLY.
 *       <li>@a HAM_WRITE_THROUGH</li> Immediately write modified pages
 *            to the disk. This slows down all database operations, but
 *            could save the database integrity in case of a system crash.
 *       <li>@a HAM_DISABLE_MMAP</li> Do not use memory mapped files for I/O.
 *            By default, hamsterdb checks if it can use mmap,
 *            since mmap is faster than read/write. For performance
 *            reasons, this flag should not be used.
 *       <li>@a HAM_CACHE_STRICT</li> Do not allow the cache to grow larger
 *            than @a cachesize. If a database operation needs to resize the
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
 *            a given file at a given time. This flag will block the
 *            operation if the lock is held by another process.
 *      </ul>
 *
 * @param param An array of ham_parameter_t structures. The following
 *        parameters are available:
 *      <ul>
 *        <li>HAM_PARAM_CACHESIZE</li> The size of the database cache,
 *            in bytes. The default size is defined in src/config.h
 *            as HAM_DEFAULT_CACHESIZE - usually 256kb.
 *      </ul>
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if the @a env pointer is NULL, an
 *              invalid combination of flags was specified or if
 *              the database name is invalid (i.e. 0 or in the reserved
 *              range, see above).
 * @return @a HAM_FILE_NOT_FOUND if the file does not exist.
 * @return @a HAM_IO_ERROR if the file could not be opened or reading failed.
 * @return @a HAM_INV_FILE_VERSION if the database version is not
 *              compatible with the library version.
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated.
 * @return @a HAM_WOULD_BLOCK if another process has locked the file
 */
HAM_EXPORT ham_status_t
ham_env_open_ex(ham_env_t *env, const char *filename,
        ham_u32_t flags, ham_parameter_t *param);

/**
 * Creates a new database in a database environment.
 *
 * @param env A valid environment handle.
 * @param env A valid database handle, which will point to the created
 *          database. To close the handle, use @see ham_close.
 * @param name The name of the database. If a database with this name 
 *          already exists, the function will fail with 
 *          @a HAM_DATABASE_ALREADY_EXISTS. Database names from 0xf000 to
 *          0xffff and 0 are reserved.
 * @param flags Optional flags for creating the database, combined with
 *        bitwise OR. Possible flags are:
 *
 *      <ul>
 *       <li>@a HAM_USE_BTREE</li> Use a B+Tree for the index structure.
 *            Currently enabled by default, but future releases
 *            of hamsterdb will offer additional index structures,
 *            like hash tables.
 *       <li>@a HAM_DISABLE_VAR_KEYLEN</li> Do not allow the use of variable
 *            length keys. Inserting a key, which is larger than the
 *            B+Tree index key size, returns @a HAM_INV_KEYSIZE.
 *      </ul>
 *
 * @param param An array of ham_parameter_t structures. The following
 *        parameters are available:
 *      <ul>
 *        <li>HAM_PARAM_KEYSIZE</li> The size of the keys in the B+Tree
 *            index. The default size is 21 bytes.
 *      </ul>
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if the @a env pointer is NULL or an
 *              invalid combination of flags was specified.
 * @return @a HAM_DATABASE_ALREADY_EXISTS if a database with this @a name
 *              already exists in this environment.
 * @return @a HAM_OUT_OF_MEMORY if memory could not be
 *              allocated.
 * @return @a HAM_ENV_FULL if the maximum number of databases per 
 *              environment was already created
 */
HAM_EXPORT ham_status_t
ham_env_create_db(ham_env_t *env, ham_db_t *db,
        ham_u16_t name, ham_u32_t flags, ham_parameter_t *params);

/**
 * Opens a database in a database environment.
 *
 * @param env A valid environment handle.
 * @param env A valid database handle, which will point to the created
 *          database. To close the handle, use @see ham_close.
 * @param name The name of the database. If a database with this name 
 *          does not exist, the function will fail with 
 *          @a HAM_DATABASE_NOT_FOUND.
 * @param flags Optional flags for opening the database, combined with
 *        bitwise OR. Possible flags are:
 *     <ul>
 *       <li>@a HAM_DISABLE_VAR_KEYLEN</li> Do not allow the use of variable
 *            length keys. Inserting a key, which is larger than the
 *            B+Tree index key size, returns @a HAM_INV_KEYSIZE.
 *     </ul>
 * @param param An array of ham_parameter_t structures. Unused, set 
 *        to NULL.
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if the @a env pointer is NULL or an
 *              invalid combination of flags was specified.
 * @return @a HAM_DATABASE_NOT_FOUND if a database with this @a name
 *              does not exist in this environment.
 * @return @a HAM_DATABASE_ALREADY_OPEN if this database was already
 *              opened
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated.
 *
 */
HAM_EXPORT ham_status_t
ham_env_open_db(ham_env_t *env, ham_db_t *db,
        ham_u16_t name, ham_u32_t flags, ham_parameter_t *params);

/**
 * Renames a database in an environment.
 *
 * @param env A valid environment handle.
 * @param oldname The old name of the existing database. If a database 
 *          with this name does not exist, the function will fail with 
 *          @a HAM_DATABASE_NOT_FOUND.
 * @param newname The new name of this database. If a database 
 *          with this name already exists, the function will fail with 
 *          @a HAM_DATABASE_ALREADY_EXISTS.
 * @param flags Optional flags for renaming the database, combined with
 *        bitwise OR. Unused, set to 0.
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if the @a env pointer is NULL or if
 *              the new database name is reserved.
 * @return @a HAM_DATABASE_NOT_FOUND if a database with this @a name
 *              does not exist in this environment.
 * @return @a HAM_DATABASE_ALREADY_EXISTS if a database with the new name
 *              already exists
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated.
 * @return @a HAM_NOT_READY if the environment @env was not initialized
 *              correctly (i.e. not yet opened or created)
 */
HAM_EXPORT ham_status_t
ham_env_rename_db(ham_env_t *env, ham_u16_t oldname, 
                ham_u16_t newname, ham_u32_t flags);

/**
 * Deletes a database from an environment.
 *
 * @param env A valid environment handle.
 * @param name The name of the database, which is deleted. If a database 
 *          with this name does not exist, the function will fail with 
 *          @a HAM_DATABASE_NOT_FOUND. If the database was already opened,
 *          the function will fail with @a HAM_DATABASE_ALREADY_OPEN.
 * @param flags Optional flags for renaming the database, combined with
 *        bitwise OR. Unused, set to 0.
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if the @a env pointer is NULL or if
 *              the new database name is reserved.
 * @return @a HAM_DATABASE_NOT_FOUND if a database with this @a name
 *              does not exists in this environment.
 * @return @a HAM_DATABASE_ALREADY_OPEN if a database with this name is
 *              still open.
 */
HAM_EXPORT ham_status_t
ham_env_erase_db(ham_env_t *env, ham_u16_t name, ham_u32_t flags);

/**
 * Closes the database environment.
 *
 * This function closes the database environment. It does not free the 
 * memory resources allocated in the @a env handle - use @a ham_env_delete 
 * to free @a env.
 *
 * The application must close all databases before closing
 * the environment, or this function will fail (@see ham_close).
 *
 * @param env A valid environment handle.
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if @a env is NULL.
 * @return @a HAM_ENV_NOT_EMPTY if there are still databases open.
 */
HAM_EXPORT ham_status_t
ham_env_close(ham_env_t *env);

/**
 * @}
 */

/**
 * @defgroup ham_db hamsterdb Database Functions
 * @{
 */

/**
 * Allocates a ham_db_t handle.
 *
 * @param db Pointer to the pointer which is allocated.
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_OUT_OF_MEMORY if memory allocation failed.
 */
HAM_EXPORT ham_status_t
ham_new(ham_db_t **db);

/**
 * Deletes a ham_db_t handle.
 *
 * Frees the ham_db_t structure, but does not close the
 * database. Call this function <b>AFTER</b> you have closed the
 * database using @a ham_close, or you will lose your data!
 *
 * @param db A valid database handle.
 *
 * @return This function always returns @a HAM_SUCCESS.
 */
HAM_EXPORT ham_status_t
ham_delete(ham_db_t *db);

/**
 * Creates a database.
 *
 * @param db A valid database handle.
 * @param filename The filename of the database file. If the file already
 *          exists, it is overwritten. Can be NULL if you create an
 *          in-memory database.
 * @param flags Optional flags for opening the database, combined with
 *        bitwise OR. For allowed flags, see @a ham_create_ex.
 * @param mode File access rights for the new file. This is the @a mode
 *        parameter for creat(2). Ignored on Microsoft Windows.
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if the @a db pointer is NULL or an
 *              invalid combination of flags was specified.
 * @return @a HAM_IO_ERROR if the file could not be opened or
 *              reading/writing failed.
 * @return @a HAM_INV_FILE_VERSION if the database version is not
 *              compatible with the library version.
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated.
 * @return @a HAM_WOULD_BLOCK if another process has locked the file
 *
 */
HAM_EXPORT ham_status_t
ham_create(ham_db_t *db, const char *filename,
        ham_u32_t flags, ham_u32_t mode);

/**
 * Creates a database - extended version.
 *
 * @param db A valid database handle.
 * @param filename The filename of the database file. If the file already
 *          exists, it will be overwritten. Can be NULL if you create an
 *          in-memory database.
 * @param flags Optional flags for opening the database, combined with
 *        bitwise OR. Possible flags are:
 *
 *      <ul>
 *       <li>@a HAM_WRITE_THROUGH</li> Immediately write modified pages to the
 *            disk. This slows down all database operations, but may
 *            save the database integrity in case of a system crash.
 *       <li>@a HAM_USE_BTREE</li> Use a B+Tree for the index structure.
 *            Currently enabled by default, but future releases
 *            of hamsterdb will offer additional index structures,
 *            i.e. hash tables.
 *       <li>@a HAM_DISABLE_VAR_KEYLEN</li> Do not allow the use of variable
 *            length keys. Inserting a key, which is larger than the
 *            B+Tree index key size, returns @a HAM_INV_KEYSIZE.
 *       <li>@a HAM_IN_MEMORY_DB</li> Creates an in-memory database. No file
 *            will be created, and the database contents are lost after
 *            the database is closed. The @a filename parameter can
 *            be NULL. Do <b>NOT</b> use in combination with
 *            @a HAM_CACHE_STRICT and do <b>NOT</b> specify @a cachesize
 *            other than 0.
 *       <li>@a HAM_DISABLE_MMAP</li> Do not use memory mapped files for I/O.
 *            By default, hamsterdb checks if it can use mmap,
 *            since mmap is faster than read/write. For performance
 *            reasons, this flag should not be used.
 *       <li>@a HAM_CACHE_STRICT</li> Do not allow the cache to grow larger
 *            than @a cachesize. If a database operation needs to resize the
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
 *            a given file at a given time. This flag will block the
 *            operation if the lock is held by another process.
 *      </ul>
 *
 * @param mode File access rights for the new file. This is the @a mode
 *        parameter for creat(2). Ignored on Microsoft Windows.
 * @param param An array of ham_parameter_t structures. The following
 *        parameters are available:
 *      <ul>
 *        <li>HAM_PARAM_CACHESIZE</li> The size of the database cache,
 *            in bytes. The default size is defined in src/config.h
 *            as HAM_DEFAULT_CACHESIZE - usually 256kb.
 *        <li>HAM_PARAM_PAGESIZE</li> The size of the file page, in
 *            bytes. It is recommended not to change the default size. The
 *            default size depends on your hardware and operating system.
 *            Page sizes must be a multiple of 1024.
 *        <li>HAM_PARAM_KEYSIZE</li> The size of the keys in the B+Tree
 *            index. The default size is 21 bytes.
 *      </ul>
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if the @a db pointer is NULL or an
 *              invalid combination of flags was specified.
 * @return @a HAM_IO_ERROR if the file could not be opened or
 *              reading/writing failed.
 * @return @a HAM_INV_FILE_VERSION if the database version is not
 *              compatible with the library version.
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated.
 * @return @a HAM_INV_PAGESIZE if @a pagesize is not a multiple of 1024.
 * @return @a HAM_INV_KEYSIZE if @a keysize is too large (at least 4
 *              keys must fit in a page).
 * @return @a HAM_WOULD_BLOCK if another process has locked the file
 *
 */
HAM_EXPORT ham_status_t
ham_create_ex(ham_db_t *db, const char *filename,
        ham_u32_t flags, ham_u32_t mode, ham_parameter_t *param);

/**
 * Opens an existing database.
 *
 * @param db A valid database handle.
 * @param filename The filename of the database file.
 * @param flags Optional flags for opening the database, combined with
 *        bitwise OR. See the documentation of @a ham_open_ex
 *        for the allowed flags.
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if the @a db pointer is NULL or an
 *              invalid combination of flags was specified.
 * @return @a HAM_FILE_NOT_FOUND if the file does not exist.
 * @return @a HAM_IO_ERROR if the file could not be opened or reading failed.
 * @return @a HAM_INV_FILE_VERSION if the database version is not
 *              compatible with the library version.
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated.
 * @return @a HAM_WOULD_BLOCK if another process has locked the file
 */
HAM_EXPORT ham_status_t
ham_open(ham_db_t *db, const char *filename, ham_u32_t flags);

/**
 * Opens an existing database - extended version.
 *
 * @param db A valid database handle.
 * @param filename The filename of the database file.
 * @param flags Optional flags for opening the database, combined with
 *        bitwise OR. Possible flags are:
 *      <ul>
 *       <li>@a HAM_READ_ONLY</li> Opens the file for reading only.
 *            Operations which need write access (i.e. @a ham_insert) will
 *            return @a HAM_DB_READ_ONLY.
 *       <li>@a HAM_WRITE_THROUGH</li> Immediately write modified pages
 *            to the disk. This slows down all database operations, but
 *            could save the database integrity in case of a system crash.
 *       <li>@a HAM_DISABLE_VAR_KEYLEN</li> Do not allow the use of variable
 *            length keys. Inserting a key, which is larger than the
 *            B+Tree index key size, returns @a HAM_INV_KEYSIZE.
 *       <li>@a HAM_DISABLE_MMAP</li> Do not use memory mapped files for I/O.
 *            By default, hamsterdb checks if it can use mmap,
 *            since mmap is faster than read/write. For performance
 *            reasons, this flag should not be used.
 *       <li>@a HAM_CACHE_STRICT</li> Do not allow the cache to grow larger
 *            than @a cachesize. If a database operation needs to resize the
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
 *            a given file at a given time. This flag will block the
 *            operation if the lock is held by another process.
 *      </ul>
 *
 * @param param An array of ham_parameter_t structures. The following
 *        parameters are available:
 *      <ul>
 *        <li>HAM_PARAM_CACHESIZE</li> The size of the database cache,
 *            in bytes. The default size is defined in src/config.h
 *            as HAM_DEFAULT_CACHESIZE - usually 256kb.
 *      </ul>
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if the @a db pointer is NULL or an
 *              invalid combination of flags was specified.
 * @return @a HAM_FILE_NOT_FOUND if the file does not exist.
 * @return @a HAM_IO_ERROR if the file could not be opened or reading failed.
 * @return @a HAM_INV_FILE_VERSION if the database version is not
 *              compatible with the library version.
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated.
 * @return @a HAM_WOULD_BLOCK if another process has locked the file
 */
HAM_EXPORT ham_status_t
ham_open_ex(ham_db_t *db, const char *filename,
        ham_u32_t flags, ham_parameter_t *param);

/** Flag for @a ham_open, @a ham_open_ex, @a ham_create, @a ham_create_ex */
#define HAM_WRITE_THROUGH            0x00000001

/** Flag for @a ham_open, @a ham_open_ex */
#define HAM_READ_ONLY                0x00000004

/* unused                            0x00000008 */

/** Flag for @a ham_create, @a ham_create_ex */
#define HAM_USE_BTREE                0x00000010

/* Use a hash database as the index structure
#define HAM_USE_HASH                 0x00000020
 */

/** Flag for @a ham_create, @a ham_create_ex */
#define HAM_DISABLE_VAR_KEYLEN       0x00000040

/** Flag for @a ham_create, @a ham_create_ex */
#define HAM_IN_MEMORY_DB             0x00000080

/* 0x100 is a reserved value         0x00000100 */

/** Flag for @a ham_open, @a ham_open_ex, @a ham_create, @a ham_create_ex */
#define HAM_DISABLE_MMAP             0x00000200

/** Flag for @a ham_open, @a ham_open_ex, @a ham_create, @a ham_create_ex */
#define HAM_CACHE_STRICT             0x00000400

/** Flag for @a ham_open, @a ham_open_ex, @a ham_create, @a ham_create_ex */
#define HAM_DISABLE_FREELIST_FLUSH   0x00000800

/** Flag for @a ham_open, @a ham_open_ex, @a ham_create, @a ham_create_ex */
#define HAM_LOCK_EXCLUSIVE           0x00001000

/** Parameter name for @a ham_open_ex, @a ham_create_ex; sets the cache
 * size*/
#define HAM_PARAM_CACHESIZE          0x00000100

/** Parameter name for @a ham_open_ex, @a ham_create_ex; sets the page
 * size*/
#define HAM_PARAM_PAGESIZE           0x00000101

/** Parameter name for @a ham_open_ex, @a ham_create_ex; sets the key
 * size*/
#define HAM_PARAM_KEYSIZE            0x00000102

/**
 * Returns the last error code.
 *
 * @param db A valid database handle.
 *
 * @return The last error code which was returned by one of the
 *         hamsterdb API functions. Use @a ham_strerror to translate
 *         this code to a descriptive string.
 */
HAM_EXPORT ham_status_t
ham_get_error(ham_db_t *db);

/**
 * Sets the prefix comparison function.
 *
 * The prefix comparison function is called when an index uses
 * keys with variable length, and one of the two keys is loaded only
 * partially.
 *
 * @param db A valid database handle.
 * @param foo A pointer to the prefix compare function.
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if one of the parameters is NULL.
 */
HAM_EXPORT ham_status_t
ham_set_prefix_compare_func(ham_db_t *db, ham_prefix_compare_func_t foo);

/**
 * Sets the comparison function.
 *
 * The comparison function compares two index keys. It returns -1 if the
 * first key is smaller, +1 if the second key is smaller or 0 if
 * keys are equal.
 *
 * The default comparison function uses memcmp to compare the keys.
 *
 * @param db A valid database handle.
 * @param foo A pointer to the compare function.
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if one of the parameters is NULL.
 */
HAM_EXPORT ham_status_t
ham_set_compare_func(ham_db_t *db, ham_compare_func_t foo);

/**
 * Searches an item in the database.
 *
 * This function searches the database for @a key. If the key
 * is found, @a record will hold the record of this item and
 * @a HAM_SUCCESS is returned. If the key is not found, the function
 * returns with @a HAM_KEY_NOT_FOUND.
 *
 * A ham_record_t structure should be initialized with
 * zeroes before it is being used. This can be done with the C library
 * routines memset(3) or bzero(2).
 *
 * If the function completes successfully, the @a record pointer is
 * initialized with the size of the record (in @a record.size) and a
 * pointer to the actual record data (in @a record.data). If the record
 * is empty, @a size is 0 and @a data is NULL.
 *
 * The @a data pointer is a temporary pointer and will be overwritten
 * by subsequent hamsterdb API calls. You can alter this behaviour by
 * allocating the @a data pointer in the application and setting
 * @a record.flags to @a HAM_RECORD_USER_ALLOC. Make sure that the allocated
 * buffer is large enough.
 *
 * @param db A valid database handle.
 * @param reserved A reserved value; set to NULL.
 * @param key The key of the item.
 * @param record The record of the item.
 * @param flags Search flags; unused, set to 0.
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if @a db, @a key or @a record is NULL.
 * @return @a HAM_KEY_NOT_FOUND if the @a key does not exist.
 */
HAM_EXPORT ham_status_t
ham_find(ham_db_t *db, void *reserved, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags);

/**
 * Inserts a database item.
 *
 * This function inserts a key/record pair as a new database item.
 *
 * If the key already exists in the database, error @a HAM_DUPLICATE_KEY
 * is returned. If you wish to overwrite an existing entry specify the
 * flag @a HAM_OVERWRITE.
 *
 * @param db A valid database handle.
 * @param reserved A reserved value; set to NULL.
 * @param key The key of the new item.
 * @param record The record of the new item.
 * @param flags Insert flags. Currently, only one flag is available:
 *         @a HAM_OVERWRITE. If the @a key already exists, the record is
 *              overwritten. Otherwise, the key is inserted.
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if @a db, @a key or @a record is NULL.
 * @return @a HAM_DB_READ_ONLY if you tried to insert a key in a read-only
 *              database.
 * @return @a HAM_INV_KEYSIZE if the key's size is larger than the @a keysize
 *              parameter specified for @a ham_create_ex and variable
 *              key sizes are disabled (see @a HAM_DISABLE_VAR_KEYLEN)
 *              OR if the @a keysize parameter specified for @a ham_create_ex
 *              is smaller than 8.
 */
HAM_EXPORT ham_status_t
ham_insert(ham_db_t *db, void *reserved, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags);

/** Flag for @a ham_insert and @a ham_cursor_insert */
#define HAM_OVERWRITE               1

/**
 * Erases a database item.
 *
 * This function erases a database item. If the item @a key
 * does not exist, @a HAM_KEY_NOT_FOUND is returned.
 *
 * @param db A valid database handle.
 * @param reserved A reserved value; set to NULL.
 * @param key The key to delete.
 * @param flags Erase flags; unused, set to 0.
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if @a db or @a key is NULL.
 * @return @a HAM_DB_READ_ONLY if you tried to erase a key from a read-only
 *              database.
 * @return @a HAM_KEY_NOT_FOUND if @a key was not found.
 */
HAM_EXPORT ham_status_t
ham_erase(ham_db_t *db, void *reserved, ham_key_t *key,
        ham_u32_t flags);

/**
 * Flushes the database.
 *
 * This function flushes the database cache and writes the whole file
 * to disk.
 *
 * Since in-memory databases do not have a file on disk, the
 * function will have no effect and will return @a HAM_SUCCESS.
 *
 * @param db A valid database handle.
 * @param flags Flush flags; unused, set to 0.
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if @a db is NULL.
 */
HAM_EXPORT ham_status_t
ham_flush(ham_db_t *db, ham_u32_t flags);

/**
 * Closes the database.
 *
 * This function flushes the database and then closes the file handle.
 * It does not free the memory resources allocated in the @a db handle -
 * use @a ham_delete to free @a db.
 *
 * The application should close all database cursors before closing
 * the database.
 *
 * @param db A valid database handle.
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if @a db is NULL.
 */
HAM_EXPORT ham_status_t
ham_close(ham_db_t *db);

/**
 * @}
 */

/**
 * @defgroup ham_cursor hamsterdb Cursor Functions
 * @{
 */

/**
 * Creates a database cursor.
 *
 * Creates a new database cursor. Cursors can be used to
 * traverse the database from start to end or vice versa. Cursors
 * can also be used to insert, delete or search database items.
 * A created cursor does not point to any item in the database.
 *
 * The application should close all database cursors before closing
 * the database.
 *
 * @param db A valid database handle.
 * @param reserved A reserved value; set to NULL
 * @param flags Flags for creating the cursor; unused, set to 0
 * @param cursor A pointer to a pointer which is allocated for the
 *          new cursor handle
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if @a db or @a cursor is NULL.
 * @return @a HAM_OUT_OF_MEMORY if the new structure could not be allocated.
 */
HAM_EXPORT ham_status_t
ham_cursor_create(ham_db_t *db, void *reserved, ham_u32_t flags,
        ham_cursor_t **cursor);

/**
 * Clones a database cursor.
 *
 * Clones an existing cursor. The new cursor will point to
 * exactly the same item as the old cursor. If the old cursor did not point
 * to any item, so will the new cursor.
 *
 * @param src The existing cursor.
 * @param dest A pointer to a pointer, which is allocated for the
 *          cloned cursor handle.
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if @a src or @a dest is NULL.
 * @return @a HAM_OUT_OF_MEMORY if the new structure could not be allocated.
 */
HAM_EXPORT ham_status_t
ham_cursor_clone(ham_cursor_t *src, ham_cursor_t **dest);

/**
 * Moves the cursor.
 *
 * Moves the cursor. You may specify the direction in the @a flags.
 * After the move, key and record of the item are returned.
 *
 * If the direction is not specified, the cursor will not move. Do not
 * specify a direction, if you want to fetch the key and/or record of
 * the current item.
 *
 * @param cursor A valid cursor handle.
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
 *      the cursor will remain on the current position.
 *          @a HAM_CURSOR_FIRST positions the cursor on the first item
 *              in the database
 *          @a HAM_CURSOR_LAST positions the cursor on the last item
 *              in the database
 *          @a HAM_CURSOR_NEXT positions the cursor on the next item
 *              in the database; if the cursor does not point to any
 *              item, the function behaves as if direction was
 *              HAM_CURSOR_FIRST.
 *          @a HAM_CURSOR_PREVIOUS positions the cursor on the previous item
 *              in the database; if the cursor does not point to any
 *              item, the function behaves as if direction was
 *              HAM_CURSOR_LAST.
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if @a cursor is NULL.
 * @return @a HAM_CURSOR_IS_NIL if the cursor does not point to an item, but
 *              key and/or record were requested.
 * @return @a HAM_KEY_NOT_FOUND if @a cursor points to the first (or last)
 *              item, and a move to the previous (or next) item was
 *              requested.
 */
HAM_EXPORT ham_status_t
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

/**
 * Replaces the current record.
 *
 * This function replaces the record of the current cursor item.
 *
 * @param cursor A valid cursor handle.
 * @param record A valid record structure.
 * @param flags Flags for replacing the item; unused, set to 0.
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if @a cursor or @a record is NULL.
 * @return @a HAM_CURSOR_IS_NIL if the cursor does not point to an item.
 */
HAM_EXPORT ham_status_t
ham_cursor_replace(ham_cursor_t *cursor, ham_record_t *record,
            ham_u32_t flags);

/**
 * Searches a key and points the cursor to this key.
 *
 * Searches for an item in the database and points the
 * cursor to this item. If the item could not be found, the cursor is
 * not modified.
 *
 * @param cursor A valid cursor handle.
 * @param key A valid key structure.
 * @param flags Flags for searching the item; unused, set to 0.
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if @a cursor or @a key is NULL.
 * @return @a HAM_KEY_NOT_FOUND if the requested key was not found.
 */
HAM_EXPORT ham_status_t
ham_cursor_find(ham_cursor_t *cursor, ham_key_t *key, ham_u32_t flags);

/**
 * Inserts (or updates) a key.
 *
 * Inserts a key to the database. If the flag @a HAM_OVERWRITE
 * is specified, a pre-existing item with this key is overwritten.
 * Otherwise, @a HAM_DUPLICATE_ITEM is returned.
 * In case of an error the cursor is not modified.
 *
 * After insertion, the cursor will point to the new item. If inserting
 * the item failed, the cursor is not modified.
 *
 * @param cursor A valid cursor handle.
 * @param key A valid key structure.
 * @param record A valid record structure.
 * @param flags Flags for inserting the item.
 *         Specify @a HAM_OVERWRITE to overwrite an existing
 *              record. Otherwise a new key will be inserted.
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if @a key or @a record is NULL.
 * @return @a HAM_DB_READ_ONLY if you tried to insert a key to a read-only
 *              database.
 * @return @a HAM_INV_KEYSIZE if the key's size is larger than the @a keysize
 *              parameter specified for @a ham_create_ex and variable
 *              key sizes are disabled (see @a HAM_DISABLE_VAR_KEYLEN)
 *              OR if the @a keysize parameter specified for @a ham_create_ex
 *              is smaller than 8.
 */
HAM_EXPORT ham_status_t
ham_cursor_insert(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags);

/**
 * Erases the key.
 *
 * Erases a key from the database. If the erase was
 * successfull, the cursor is invalidated, and does no longer point to
 * any item. In case of an error, the cursor is not modified.
 *
 * @param cursor A valid cursor handle.
 * @param flags Erase flags; unused, set to 0.
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if @a cursor is NULL.
 * @return @a HAM_DB_READ_ONLY if you tried to erase a key from a read-only
 *              database.
 * @return @a HAM_KEY_NOT_FOUND if the key was not found.
 */
HAM_EXPORT ham_status_t
ham_cursor_erase(ham_cursor_t *cursor, ham_u32_t flags);

/**
 * Closes a database cursor.
 *
 * Closes a cursor and frees allocated memory. All cursors
 * should be closed before closing the database (see @a ham_close).
 *
 * @param cursor A valid cursor handle.
 *
 * @return @a HAM_SUCCESS upon success.
 * @return @a HAM_INV_PARAMETER if @a cursor is NULL.
 *
 */
HAM_EXPORT ham_status_t
ham_cursor_close(ham_cursor_t *cursor);

/*
 * @}
 */

#ifdef __cplusplus
} // extern "C"
#endif

#endif /* HAM_HAMSTERDB_H__ */
