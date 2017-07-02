/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

/**
 * @file upscaledb.h
 * @brief Include file for upscaledb embedded database
 * @author Christoph Rupp, chris@crupp.de
 * @version 2.2.1
 *
 * @mainpage
 *
 * <b>This is the commercial closed-source version of upscaledb!</b>
 *
 * This manual documents the upscaledb C API. upscaledb is a key/value database
 * that is linked directly into your application, avoiding all the overhead
 * that is related to external databases and RDBMS systems.
 *
 * This header file declares all functions and macros that are needed to use
 * upscaledb. The comments are formatted in Doxygen style and can be extracted
 * to automagically generate documentation. The documentation is also available
 * online here: <a href="http://files.upscaledb.com/documentation/html">
  http://files.upscaledb.com/documentation/html</a>.
 *
 * In addition, there's a tutorial book hosted on upscaledb.com:
 * <a href="http://http://upscaledb.com/tutorial.html">
  http://upscaledb.com/tutorial.html</a>.
 *
 * If you want to create or open Databases or Environments (a collection of
 * multiple Databases), the following functions will be interesting for you:
 * <table>
 * <tr><td>@ref ups_env_create</td><td>Creates an Environment</td></tr>
 * <tr><td>@ref ups_env_open</td><td>Opens an Environment</td></tr>
 * <tr><td>@ref ups_env_close</td><td>Closes an Environment</td></tr>
 * <tr><td>@ref ups_env_create_db</td><td>Creates a Database in an
  Environment</td></tr>
 * <tr><td>@ref ups_env_open_db</td><td>Opens a Database from an
  Environment</td></tr>
 * <tr><td>@ref ups_db_close</td><td>Closes a Database</td></tr>
 * </table>
 *
 * To insert, lookup or delete key/value pairs, the following functions are
 * used:
 * <table>
 * <tr><td>@ref ups_db_insert</td><td>Inserts a key/value pair into a
  Database</td></tr>
 * <tr><td>@ref ups_db_find</td><td>Lookup of a key/value pair in a
  Database</td></tr>
 * <tr><td>@ref ups_db_erase</td><td>Erases a key/value pair from a
  Database</td></tr>
 * </table>
 *
 * Alternatively, you can use Cursors to iterate over a Database:
 * <table>
 * <tr><td>@ref ups_cursor_create</td><td>Creates a new Cursor</td></tr>
 * <tr><td>@ref ups_cursor_find</td><td>Positions the Cursor on a key</td></tr>
 * <tr><td>@ref ups_cursor_insert</td><td>Inserts a new key/value pair with a
  Cursor</td></tr>
 * <tr><td>@ref ups_cursor_erase</td><td>Deletes the key/value pair that
  the Cursor points to</td></tr>
 * <tr><td>@ref ups_cursor_overwrite</td><td>Overwrites the value of the current  key</td></tr>
 * <tr><td>@ref ups_cursor_move</td><td>Moves the Cursor to the first, next,
  previous or last key in the Database</td></tr>
 * <tr><td>@ref ups_cursor_close</td><td>Closes the Cursor</td></tr>
 * </table>
 *
 * If you want to use Transactions, then the following functions are required:
 * <table>
 * <tr><td>@ref ups_txn_begin</td><td>Begins a new Txn</td></tr>
 * <tr><td>@ref ups_txn_commit</td><td>Commits the current
  Txn</td></tr>
 * <tr><td>@ref ups_txn_abort</td><td>Aborts the current Txn</td></tr>
 * </table>
 *
 * upscaledb supports remote Databases. The server can be embedded
 * into your application or run standalone (see tools/upszilla for a Unix
 * daemon or Win32 service which hosts Databases). If you want to embed the
 * server then the following functions have to be used:
 * <table>
 * <tr><td>@ref ups_srv_init</td><td>Initializes the server</td></tr>
 * <tr><td>@ref ups_srv_add_env</td><td>Adds an Environment to the
  server. The Environment with all its Databases will then be available
  remotely.</td></tr>
 * <tr><td>@ref ups_srv_close</td><td>Closes the server and frees all allocated
  resources</td></tr>
 * </table>
 *
 * If you need help then you're always welcome to use the <a
  href="https://groups.google.com/forum/?fromgroups#!forum/upscaledb-user">
    mailing list</a>,
 * drop a message (chris at crupp dot de) or use the <a
  href="http://upscaledb.com/index/contact">contact form</a>.
 *
 * Have fun!
 */

#ifndef UPS_UPSCALEDB_H
#define UPS_UPSCALEDB_H

#include <ups/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/* deprecated */
#define UPS_API_REVISION        4

/**
 * The version numbers
 *
 * @remark A change of the major revision means a significant update
 * with a lot of new features and API changes.
 * 
 * The minor version means a significant update without API changes, and the 
 * revision is incremented for each release with minor improvements only.
 *
 * The file version describes the version of the binary database format.
 * upscaledb is neither backwards- nor forwards-compatible regarding file
 * format changes. 
 *
 * History of file versions:
 *   2.1.0:  introduced the file version; version is 0
 *   2.1.3:  new btree format, file format cleanups; version is 1
 *   2.1.4:  new btree format for duplicate keys/var. length keys; version is 2
 *   2.1.5:  new freelist; version is 3
 *   2.1.9:  changes in btree node format; version is 4
 *   2.1.13: changes in btree node format; version is 5
 */
#define UPS_VERSION_MAJ     2
#define UPS_VERSION_MIN     2
#define UPS_VERSION_REV     1
#define UPS_FILE_VERSION    5

/**
 * The upscaledb Database structure
 *
 * This structure is allocated in @ref ups_env_create_db and
 * @ref ups_env_open_db. It is deleted in @a ups_db_close.
 */
struct ups_db_t;
typedef struct ups_db_t ups_db_t;

/**
 * The upscaledb Environment structure
 *
 * This structure is allocated with @ref ups_env_create and @ref ups_env_open
 * and is deleted in @ref ups_env_close.
 */
struct ups_env_t;
typedef struct ups_env_t ups_env_t;

/**
 * A Database Cursor
 *
 * A Cursor is used for bi-directionally traversing the Database and
 * for inserting/deleting/searching Database items.
 *
 * This structure is allocated with @ref ups_cursor_create and deleted with
 * @ref ups_cursor_close.
 */
struct ups_cursor_t;
typedef struct ups_cursor_t ups_cursor_t;

/**
 * A generic record.
 *
 * A record represents data items in upscaledb. Before using a record, it
 * is important to initialize all record fields with zeroes, i.e. with
 * the C library routines memset(3) or bzero(2).
 *
 * When upscaledb returns a record structure, the pointer to the record
 * data is provided in @a data. This pointer is only temporary and will be
 * overwritten by subsequent upscaledb API calls using the same Txn
 * (or, if Transactions are disabled, using the same Database). The pointer
 * will also be invalidated after the Txn is aborted or committed.
 *
 * To avoid this, the calling application can allocate the @a data pointer.
 * In this case, you have to set the flag @ref UPS_RECORD_USER_ALLOC. The
 * @a size parameter will then return the size of the record. It's the
 * responsibility of the caller to make sure that the @a data parameter is
 * large enough for the record.
 *
 * The record->data pointer is not threadsafe. For threadsafe access it is
 * recommended to use @a UPS_RECORD_USER_ALLOC or have each thread manage its
 * own Txn.
 */
typedef struct {
  /** The size of the record data, in bytes */
  uint32_t size;

  /** Pointer to the record data */
  void *data;

  /** The record flags; see @ref UPS_RECORD_USER_ALLOC */
  uint32_t flags;

} ups_record_t;

/** Flag for @ref ups_record_t (only really useful in combination with
 * @ref ups_cursor_move, @ref ups_cursor_find and @ref ups_db_find)
 */
#define UPS_RECORD_USER_ALLOC   1

/**
 * A macro to statically initialize a @ref ups_record_t structure.
 *
 * Usage:
 *    ups_record_t rec = ups_make_record(ptr, size);
 */
#define ups_make_record(PTR, SIZE) { SIZE, PTR, 0 }

/**
 * A generic key.
 *
 * A key represents key items in upscaledb. Before using a key, it
 * is important to initialize all key fields with zeroes, i.e. with
 * the C library routines memset(3) or bzero(2).
 *
 * upscaledb usually uses keys to insert, delete or search for items.
 * However, when using Database Cursors and the function @ref ups_cursor_move,
 * upscaledb also returns keys. In this case, the pointer to the key
 * data is provided in @a data. This pointer is only temporary and will be
 * overwritten by subsequent calls to @ref ups_cursor_move using the
 * same Txn (or, if Transactions are disabled, using the same Database).
 * The pointer will also be invalidated after the Txn is aborted
 * or committed.
 *
 * To avoid this, the calling application can allocate the @a data pointer.
 * In this case, you have to set the flag @ref UPS_KEY_USER_ALLOC. The
 * @a size parameter will then return the size of the key. It's the
 * responsibility of the caller to make sure that the @a data parameter is
 * large enough for the key.
 *
 * The key->data pointer is not threadsafe. For threadsafe access it is
 * recommended to use @a UPS_KEY_USER_ALLOC or have each thread manage its
 * own Txn.
 */
typedef struct {
  /** The size of the key, in bytes */
  uint16_t size;

  /** The data of the key */
  void *data;

  /** The key flags; see @ref UPS_KEY_USER_ALLOC */
  uint32_t flags;

  /** For internal use */
  uint32_t _flags;

} ups_key_t;

/**
 * A macro to statically initialize a @ref ups_key_t structure.
 *
 * Usage:
 *    ups_key_t key = ups_make_key(ptr, size);
 */
#define ups_make_key(PTR, SIZE) { SIZE, PTR, 0, 0 }

/** Flag for @ref ups_key_t (only really useful in combination with
 * @ref ups_cursor_move, @ref ups_cursor_find and @ref ups_db_find)
 */
#define UPS_KEY_USER_ALLOC    1

/**
 * A named parameter.
 *
 * These parameter structures are used for functions like @ref ups_env_open,
 * @ref ups_env_create, etc. to pass variable length parameter lists.
 *
 * The lists are always arrays of type ups_parameter_t, with a terminating
 * element of { 0, NULL}, e.g.
 *
 * <pre>
 *   ups_parameter_t parameters[] = {
 *    { UPS_PARAM_CACHE_SIZE, 2 * 1024 * 1024 }, // set cache size to 2 mb
 *    { UPS_PARAM_PAGE_SIZE, 4096 }, // set page size to 4 kb
 *    { 0, NULL }
 *   };
 * </pre>
 */
typedef struct {
  /** The name of the parameter; all UPS_PARAM_*-constants */
  uint32_t name;

  /** The value of the parameter. */
  uint64_t value;

} ups_parameter_t;


/**
 * @defgroup ups_key_types upscaledb Key Types
 * @{
 */

/** A binary blob without type; sorted by memcmp */
#define UPS_TYPE_BINARY                      0
/** A binary blob without type; sorted by callback function */
#define UPS_TYPE_CUSTOM                      1
/** An unsigned 8-bit integer */
#define UPS_TYPE_UINT8                       3
/** An unsigned 16-bit integer */
#define UPS_TYPE_UINT16                      5
/** An unsigned 32-bit integer */
#define UPS_TYPE_UINT32                      7
/** An unsigned 64-bit integer */
#define UPS_TYPE_UINT64                      9
/** An 32-bit float */
#define UPS_TYPE_REAL32                     11
/** An 64-bit double */
#define UPS_TYPE_REAL64                     12

/**
 * @}
 */


/**
 * @defgroup ups_status_codes upscaledb Status Codes
 * @{
 */

/** Operation completed successfully */
#define UPS_SUCCESS                     (  0)
/** Invalid record size */
#define UPS_INV_RECORD_SIZE             ( -2)
/** Invalid key size */
#define UPS_INV_KEY_SIZE                ( -3)
/* deprecated */
#define UPS_INV_KEYSIZE                 UPS_INV_KEY_SIZE
/** Invalid page size (must be 1024 or a multiple of 2048) */
#define UPS_INV_PAGE_SIZE               ( -4)
/* deprecated */
#define UPS_INV_PAGESIZE                UPS_INV_PAGE_SIZE
/** Memory allocation failed - out of memory */
#define UPS_OUT_OF_MEMORY               ( -6)
/** Invalid function parameter */
#define UPS_INV_PARAMETER               ( -8)
/** Invalid file header */
#define UPS_INV_FILE_HEADER             ( -9)
/** Invalid file version */
#define UPS_INV_FILE_VERSION            (-10)
/** Key was not found */
#define UPS_KEY_NOT_FOUND               (-11)
/** Tried to insert a key which already exists */
#define UPS_DUPLICATE_KEY               (-12)
/** Internal Database integrity violated */
#define UPS_INTEGRITY_VIOLATED          (-13)
/** Internal upscaledb error */
#define UPS_INTERNAL_ERROR              (-14)
/** Tried to modify the Database, but the file was opened as read-only */
#define UPS_WRITE_PROTECTED             (-15)
/** Database record not found */
#define UPS_BLOB_NOT_FOUND              (-16)
/** Generic file I/O error */
#define UPS_IO_ERROR                    (-18)
/** Function is not yet implemented */
#define UPS_NOT_IMPLEMENTED             (-20)
/** File not found */
#define UPS_FILE_NOT_FOUND              (-21)
/** Operation would block */
#define UPS_WOULD_BLOCK                 (-22)
/** Object was not initialized correctly */
#define UPS_NOT_READY                   (-23)
/** Database limits reached */
#define UPS_LIMITS_REACHED              (-24)
/** Object was already initialized */
#define UPS_ALREADY_INITIALIZED         (-27)
/** Database needs recovery */
#define UPS_NEED_RECOVERY               (-28)
/** Cursor must be closed prior to Txn abort/commit */
#define UPS_CURSOR_STILL_OPEN           (-29)
/** Record filter or file filter not found */
#define UPS_FILTER_NOT_FOUND            (-30)
/** Operation conflicts with another Txn */
#define UPS_TXN_CONFLICT                (-31)
/* internal use: key was erased in a Txn */
#define UPS_KEY_ERASED_IN_TXN           (-32)
/** Database cannot be closed because it is modified in a Txn */
#define UPS_TXN_STILL_OPEN              (-33)
/** Cursor does not point to a valid item */
#define UPS_CURSOR_IS_NIL               (-100)
/** Database not found */
#define UPS_DATABASE_NOT_FOUND          (-200)
/** Database name already exists */
#define UPS_DATABASE_ALREADY_EXISTS     (-201)
/** Database already open, or: Database handle is already initialized */
#define UPS_DATABASE_ALREADY_OPEN       (-202)
/** Environment already open, or: Environment handle is already initialized */
#define UPS_ENVIRONMENT_ALREADY_OPEN    (-203)
/** Invalid log file header */
#define UPS_LOG_INV_FILE_HEADER         (-300)
/** Remote I/O error/Network error */
#define UPS_NETWORK_ERROR               (-400)
/** UQI: plugin not found or unable to load */
#define UPS_PLUGIN_NOT_FOUND            (-500)
/** UQI: failed to parse a query command */
#define UPS_PARSER_ERROR                (-501)
/** UQI: a plugin with the given name is already registered */
#define UPS_PLUGIN_ALREADY_EXISTS       (-502)

/**
 * @}
 */


/**
 * @defgroup ups_static upscaledb Static Functions
 * @{
 */

/**
 * A typedef for a custom error handler function
 *
 * This error handler can be used in combination with
 * @ref ups_set_error_handler().
 *
 * @param message The error message
 * @param level The error level:
 *    <ul>
 *     <li>@ref UPS_DEBUG_LEVEL_DEBUG (0) </li> a debug message
 *     <li>@ref UPS_DEBUG_LEVEL_NORMAL (1) </li> a normal error message
 *     <li>2</li> reserved
 *     <li>@ref UPS_DEBUG_LEVEL_FATAL (3) </li> a fatal error message
 *    </ul>
 *
 * @sa error_levels
 */
typedef void UPS_CALLCONV (*ups_error_handler_fun)(int level, const char *message);

/** A debug message */
#define UPS_DEBUG_LEVEL_DEBUG     0

/** A normal error message */
#define UPS_DEBUG_LEVEL_NORMAL    1

/** A fatal error message */
#define UPS_DEBUG_LEVEL_FATAL     3

/**
 * Sets the global error handler
 *
 * This handler will receive all debug messages that are emitted
 * by upscaledb. You can install the default handler by setting @a f to 0.
 *
 * The default error handler prints all messages to stderr. To install a
 * different logging facility, you can provide your own error handler.
 *
 * Note that the callback function must have the same calling convention
 * as the upscaledb library.
 *
 * @param f A pointer to the error handler function, or NULL to restore
 *      the default handler
 */
UPS_EXPORT void UPS_CALLCONV
ups_set_error_handler(ups_error_handler_fun f);

/**
 * Translates a upscaledb status code to a descriptive error string
 *
 * @param status The upscaledb status code
 *
 * @return A pointer to a descriptive error string
 */
UPS_EXPORT const char * UPS_CALLCONV
ups_strerror(ups_status_t status);

/**
 * Returns the version of the upscaledb library
 *
 * @param major If not NULL, will return the major version number
 * @param minor If not NULL, will return the minor version number
 * @param revision If not NULL, will return the revision version number
 */
UPS_EXPORT void UPS_CALLCONV
ups_get_version(uint32_t *major, uint32_t *minor,
            uint32_t *revision);

/**
 * @}
 */


/**
 * @defgroup ups_env upscaledb Environment Functions
 * @{
 */

/**
 * Creates a Database Environment
 *
 * A Database Environment is a collection of Databases, which are all stored
 * in one physical file (or in-memory). The maximum number of Databases
 * depends on the page size; the default is above 600.
 *
 * Each Database in an Environment is identified by a positive 16bit
 * value (except 0 and values at or above 0xf000).
 * Databases in an Environment can be created with @ref ups_env_create_db
 * or opened with @ref ups_env_open_db.
 *
 * Specify a URL instead of a filename (i.e.
 * "ups://localhost:8080/customers.db") to access a remote upscaledb Server.
 *
 * To enable ACID Transactions, supply the flag @ref UPS_ENABLE_TRANSACTIONS.
 * By default, upscaledb will use a Journal for recovering the Environment
 * and its data in case of a crash, and also to re-apply committed Transactions
 * which were not yet flushed to disk. This Journalling can be disabled
 * with the flag @ref UPS_DISABLE_RECOVERY. (It is disabled if the Environment
 * is in-memory.)
 *
 * For performance reasons the Journal does not use fsync(2) (or
 * FlushFileBuffers on Win32) to flush modified buffers to disk. Use the flag
 * @ref UPS_ENABLE_FSYNC to force the use of fsync.
 *
 * If Transactions are enabled, a journal file is written in order
 * to provide recovery if the system crashes. These journal files can be
 * compressed by supplying the parameter
 * @ref UPS_PARAM_ENABLE_JOURNAL_COMPRESSION. Values are one of
 * @ref UPS_COMPRESSOR_ZLIB, @ref UPS_COMPRESSOR_SNAPPY etc. See the
 * upscaledb documentation for more details. This parameter is not
 * persisted.
 *
 * Upscaledb can transparently encrypt the generated file using
 * 128bit AES in CBC mode. The transactional journal is not encrypted.
 * Encryption can be enabled by specifying @ref UPS_PARAM_ENCRYPTION_KEY
 * (see below). The identical key has to be provided in @ref ups_env_open
 * as well. Ignored for remote Environments.
 *
 * CRC32 checksums are stored when a page is flushed, and verified
 * when it is fetched from disk if the flag @ref UPS_ENABLE_CRC32 is set.
 * API functions will return @ref UPS_INTEGRITY_VIOLATED in case of failed
 * verifications. Not allowed in In-Memory Environments. This flag is not
 * persisted.
 *
 * @param env A pointer to an Environment handle
 * @param filename The filename of the Environment file. If the file already
 *      exists, it is overwritten. Can be NULL for an In-Memory
 *      Environment. Can be a URL ("ups://<hostname>:<port>/<environment>")
 *      for remote access.
 * @param flags Optional flags for opening the Environment, combined with
 *      bitwise OR. Possible flags are:
 *    <ul>
 *     <li>@ref UPS_ENABLE_FSYNC</li> Flushes all file handles after
 *      committing or aborting a Txn using fsync(), fdatasync()
 *      or FlushFileBuffers(). This file has no effect
 *      if Transactions are disabled. Slows down performance but makes
 *      sure that all file handles and operating system caches are
 *      transferred to disk, thus providing a stronger durability.
 *     <li>@ref UPS_IN_MEMORY</li> Creates an In-Memory Environment. No
 *      file will be created, and the Database contents are lost after
 *      the Environment is closed. The @a filename parameter can
 *      be NULL. Do <b>NOT</b> specify @a cache_size other than 0.
 *     <li>@ref UPS_DISABLE_MMAP</li> Do not use memory mapped files for I/O.
 *      By default, upscaledb checks if it can use mmap,
 *      since mmap is faster than read/write. For performance
 *      reasons, this flag should not be used.
 *     <li>@ref UPS_CACHE_UNLIMITED</li> Do not limit the cache. Nearly as
 *      fast as an In-Memory Database. Not allowed in combination
 *      with a limited cache size.
 *     <li>@ref UPS_ENABLE_TRANSACTIONS</li> Enables Transactions for this
 *      Environment.
 *     <li>@ref UPS_DISABLE_RECOVERY</li> Disables logging/recovery for this
 *      Environment.
 *     <li>@ref UPS_ENABLE_CRC32</li> Stores (and verifies) CRC32
 *      checksums. Not allowed in combination with @ref UPS_IN_MEMORY.
 *    </ul>
 *
 * @param mode File access rights for the new file. This is the @a mode
 *      parameter for creat(2). Ignored on Microsoft Windows. Default
 *      is 0644.
 * @param param An array of ups_parameter_t structures. The following
 *      parameters are available:
 *    <ul>
 *    <li>@ref UPS_PARAM_CACHE_SIZE</li> The size of the Database cache,
 *      in bytes. The default size is defined in src/config.h
 *      as @a UPS_DEFAULT_CACHE_SIZE - usually 2MB
 *    <li>@ref UPS_PARAM_POSIX_FADVISE</li> Sets the "advice" for
 *      posix_fadvise(). Only on supported platforms. Allowed values are
 *      @ref UPS_POSIX_FADVICE_NORMAL (which is the default) or
 *      @ref UPS_POSIX_FADVICE_RANDOM.
 *    <li>@ref UPS_PARAM_PAGE_SIZE</li> The size of a file page, in
 *      bytes. It is recommended not to change the default size. The
 *      default size depends on hardware and operating system.
 *      Page sizes must be 1024 or a multiple of 2048.
 *    <li>@ref UPS_PARAM_FILE_SIZE_LIMIT</li> Sets a file size limit (in bytes).
 *      Disabled by default. Not allowed in combination with @ref UPS_IN_MEMORY.
 *      If the limit is exceeded, API functions return @ref UPS_LIMITS_REACHED.
 *    <li>@ref UPS_PARAM_LOG_DIRECTORY</li> The path of the log file
 *      and the journal files; default is the same path as the database
 *      file. Ignored for remote Environments.
 *    <li>@ref UPS_PARAM_NETWORK_TIMEOUT_SEC</li> Timeout (in seconds) when
 *      waiting for data from a remote server. By default, no timeout is set.
 *    <li>@ref UPS_PARAM_ENABLE_JOURNAL_COMPRESSION</li> Compresses
 *      the journal files to reduce I/O. See notes above.
 *    <li>@ref UPS_PARAM_ENCRYPTION_KEY</li> The 16 byte long AES
 *      encryption key; enables AES encryption for the Environment file. Not
 *      allowed for In-Memory Environments. Ignored for remote Environments.
 *    </ul>
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if the @a env pointer is NULL or an
 *        invalid combination of flags or parameters was specified
 * @return @ref UPS_IO_ERROR if the file could not be opened or
 *        reading/writing failed
 * @return @ref UPS_INV_FILE_VERSION if the Environment version is not
 *        compatible with the library version
 * @return @ref UPS_OUT_OF_MEMORY if memory could not be allocated
 * @return @ref UPS_INV_PAGE_SIZE if @a page_size is not 1024 or
 *        a multiple of 2048
 * @return @ref UPS_INV_KEY_SIZE if @a key_size is too large (at least 4
 *        keys must fit in a page)
 * @return @ref UPS_WOULD_BLOCK if another process has locked the file
 * @return @ref UPS_ENVIRONMENT_ALREADY_OPEN if @a env is already in use
 *
 * @sa ups_env_create
 * @sa ups_env_close
 * @sa ups_env_open
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_env_create(ups_env_t **env, const char *filename,
            uint32_t flags, uint32_t mode, const ups_parameter_t *param);

/**
 * Opens an existing Database Environment
 *
 * This function opens an existing Database Environment.
 *
 * A Database Environment is a collection of Databases, which are all stored
 * in one physical file (or in-memory).
 *
 * Each Database in an Environment is identified by a positive 16bit
 * value (except 0 and values at or above 0xf000).
 * Databases in an Environment can be created with @ref ups_env_create_db
 * or opened with @ref ups_env_open_db.
 *
 * Specify a URL instead of a filename (i.e.
 * "ups://localhost:8080/customers.db") to access a remote upscaledb Server.
 *
 * Also see the documentation @ref ups_env_create about Transactions, Recovery,
 * AES encryption and the use of fsync.
 *
 * If Transactions are enabled, a journal file is written in order
 * to provide recovery if the system crashes. These journal files can be
 * compressed by supplying the parameter
 * @ref UPS_PARAM_JOURNAL_COMPRESSION. Values are one of
 * @ref UPS_COMPRESSOR_ZLIB, @ref UPS_COMPRESSOR_SNAPPY etc. See the
 * upscaledb documentation for more details. This parameter is not
 * persisted.
 *
 * CRC32 checksums are stored when a page is flushed, and verified
 * when it is fetched from disk if the flag @ref UPS_ENABLE_CRC32 is set.
 * API functions will return @ref UPS_INTEGRITY_VIOLATED in case of failed
 * verifications. This flag is not persisted.
 *
 * @param env A valid Environment handle
 * @param filename The filename of the Environment file, or URL of a upscaledb
 *      Server
 * @param flags Optional flags for opening the Environment, combined with
 *      bitwise OR. Possible flags are:
 *    <ul>
 *     <li>@ref UPS_READ_ONLY </li> Opens the file for reading only.
 *      Operations that need write access (i.e. @ref ups_db_insert) will
 *      return @ref UPS_WRITE_PROTECTED.
 *     <li>@ref UPS_ENABLE_FSYNC</li> Flushes all file handles after
 *      committing or aborting a Txn using fsync(), fdatasync()
 *      or FlushFileBuffers(). This file has no effect
 *      if Transactions are disabled. Slows down performance but makes
 *      sure that all file handles and operating system caches are
 *      transferred to disk, thus providing a stronger durability.
 *     <li>@ref UPS_DISABLE_MMAP </li> Do not use memory mapped files for I/O.
 *      By default, upscaledb checks if it can use mmap,
 *      since mmap is faster than read/write. For performance
 *      reasons, this flag should not be used.
 *     <li>@ref UPS_CACHE_UNLIMITED </li> Do not limit the cache. Nearly as
 *      fast as an In-Memory Database. Not allowed in combination
 *      with a limited cache size.
 *     <li>@ref UPS_ENABLE_TRANSACTIONS </li> Enables Transactions for this
 *      Environment.
 *     <li>@ref UPS_DISABLE_RECOVERY</li> Disables logging/recovery for this
 *      Environment.
 *     <li>@ref UPS_AUTO_RECOVERY </li> Automatically recover the Environment,
 *      if necessary.
 *     <li>@ref UPS_ENABLE_CRC32</li> Stores (and verifies) CRC32
 *      checksums.
 *    </ul>
 * @param param An array of ups_parameter_t structures. The following
 *      parameters are available:
 *    <ul>
 *    <li>@ref UPS_PARAM_CACHE_SIZE </li> The size of the Database cache,
 *      in bytes. The default size is defined in src/config.h
 *      as @a UPS_DEFAULT_CACHE_SIZE - usually 2MB
 *    <li>@ref UPS_PARAM_POSIX_FADVISE</li> Sets the "advice" for
 *      posix_fadvise(). Only on supported platforms. Allowed values are
 *      @ref UPS_POSIX_FADVICE_NORMAL (which is the default) or
 *      @ref UPS_POSIX_FADVICE_RANDOM.
 *    <li>@ref UPS_PARAM_FILE_SIZE_LIMIT</li> Sets a file size limit (in bytes).
 *      Disabled by default. If the limit is exceeded, API functions
 *      return @ref UPS_LIMITS_REACHED.
 *    <li>@ref UPS_PARAM_LOG_DIRECTORY</li> The path of the log file
 *      and the journal files; default is the same path as the database
 *      file. Ignored for remote Environments.
 *    <li>@ref UPS_PARAM_NETWORK_TIMEOUT_SEC</li> Timeout (in seconds) when
 *      waiting for data from a remote server. By default, no timeout is set.
 *    <li>@ref UPS_PARAM_JOURNAL_COMPRESSION</li> Compresses
 *      the journal files to reduce I/O. See notes above.
 *    <li>@ref UPS_PARAM_ENCRYPTION_KEY</li> The 16 byte long AES
 *      encryption key; enables AES encryption for the Environment file. Not
 *      allowed for In-Memory Environments. Ignored for remote Environments.
 *    </ul>
 *
 * @return @ref UPS_SUCCESS upon success.
 * @return @ref UPS_INV_PARAMETER if the @a env pointer is NULL, an
 *        invalid combination of flags was specified
 * @return @ref UPS_FILE_NOT_FOUND if the file does not exist
 * @return @ref UPS_IO_ERROR if the file could not be opened or reading failed
 * @return @ref UPS_INV_FILE_VERSION if the Environment version is not
 *        compatible with the library version.
 * @return @ref UPS_OUT_OF_MEMORY if memory could not be allocated
 * @return @ref UPS_WOULD_BLOCK if another process has locked the file
 * @return @ref UPS_NEED_RECOVERY if the Database is in an inconsistent state
 * @return @ref UPS_LOG_INV_FILE_HEADER if the logfile is corrupt
 * @return @ref UPS_ENVIRONMENT_ALREADY_OPEN if @a env is already in use
 * @return @ref UPS_NETWORK_ERROR if a remote server is not reachable
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_env_open(ups_env_t **env, const char *filename,
            uint32_t flags, const ups_parameter_t *param);

/**
 * Retrieve the current value for a given Environment setting
 *
 * Only those values requested by the parameter array will be stored.
 *
 * The following parameters are supported:
 *    <ul>
 *    <li>UPS_PARAM_CACHE_SIZE</li> returns the cache size
 *    <li>UPS_PARAM_PAGE_SIZE</li> returns the page size
 *    <li>UPS_PARAM_MAX_DATABASES</li> returns the max. number of
 *        Databases of this Database's Environment
 *    <li>UPS_PARAM_FLAGS</li> returns the flags which were used to
 *        open or create this Database
 *    <li>UPS_PARAM_FILEMODE</li> returns the @a mode parameter which
 *        was specified when creating this Database
 *    <li>UPS_PARAM_FILENAME</li> returns the filename (the @a value
 *        of this parameter is a const char * pointer casted to a
 *        uint64_t variable)
 *    <li>@ref UPS_PARAM_LOG_DIRECTORY</li> The path of the log file
 *        and the journal files. Ignored for remote Environments.
 *    <li>@ref UPS_PARAM_JOURNAL_COMPRESSION</li> Returns the
 *        selected algorithm for journal compression, or 0 if compression
 *        is disabled
 *    </ul>
 *
 * @param env A valid Environment handle
 * @param param An array of ups_parameter_t structures
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if the @a env pointer is NULL or
 *        @a param is NULL
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_env_get_parameters(ups_env_t *env, ups_parameter_t *param);

/**
 * Creates a new Database in a Database Environment
 *
 * An Environment can contain a (limited) amount of Databases; the exact
 * limit depends on the page size and is above 600.
 *
 * Each Database in an Environment is identified by a positive 16bit
 * value. 0 and values at or above 0xf000 are reserved.
 *
 * This function initializes the ups_db_t handle (the second parameter).
 * When the handle is no longer in use, it should be closed with
 * @ref ups_db_close. Alternatively, the Database handle is closed
 * automatically if @ref ups_env_close is called with the flag
 * @ref UPS_AUTO_CLEANUP.
 *
 * A Database can (and should) be configured and optimized for the data that
 * is inserted. The data is described through flags and parameters. upscaledb
 * differentiates between several data characteristics, and offers predefined
 * "types" to describe the keys. In general, the default key type
 * (@ref UPS_TYPE_BINARY) is slower than the other types, and
 * fixed-length binary keys (@ref UPS_TYPE_BINARY in combination with
 * @ref UPS_PARAM_KEY_SIZE) is faster than variable-length binary
 * keys. It is therefore recommended to always set the key size and record size,
 * although it is not required.
 *
 * Internally, upscaledb uses two different layouts ("default" and "pax)
 * depending on the settings specified by the user. The "default" layout
 * is enabled for variable-length keys or if duplicate keys are enabled.
 * For fixed-length keys (without duplicates) the "pax" layout is chosen.
 * The "pax" layout is more compact and usually faster.
 *
 * A word of warning regarding the use of fixed length binary keys
 * (@ref UPS_TYPE_CUSTOM or @ref UPS_TYPE_BINARY in combination with
 * @ref UPS_PARAM_KEY_SIZE): if your key size is too large, only few keys
 * will fit in a Btree node. The Btree fanout will be very high, which will
 * decrease performance. In such cases it might be better to NOT specify
 * the key size; then upscaledb will store keys as blobs if they are too large.
 *
 * See the Wiki documentation for <a href=
   "https://github.com/cruppstahl/upscaledb/wiki/Evaluating-and-Benchmarking">
 * Evaluating and Benchmarking</a> on how to test different configurations and
 * optimize for performance.
 *
 * The key type is set with @ref UPS_PARAM_KEY_TYPE and the record type
 * is set with @ref UPS_PARAM_RECORD_TYPE. The types can have one of the
 * following values:
 *
 * <ul>
 *   <li>UPS_TYPE_BINARY</li> This is the default typef or keys and
 *   records: a binary blob with fixed or variable length. Internally,
 *   upscaledb uses memcmp(3) for the sort order of binary keys. Key size
 *   depends on @ref UPS_PARAM_KEY_SIZE and is unlimited
 *   (@ref UPS_KEY_SIZE_UNLIMITED) by default. Record size depends on
 *   @ref UPS_PARAM_RECORD_SIZE and is unlimited
 *   (@ref UPS_RECORD_SIZE_UNLIMITED) by default.
 *   <li>UPS_TYPE_CUSTOM</li> Only for keys: similar to @ref UPS_TYPE_BINARY,
 *   but uses a callback function for the sort order. This function is set
 *   with the parameter @ref UPS_PARAM_CUSTOM_COMPARE_NAME.
 *   <li>UPS_TYPE_UINT8</li> Key is a 8bit (1 byte) unsigned integer
 *   <li>UPS_TYPE_UINT16</li> Key is a 16bit (2 byte) unsigned integer
 *   <li>UPS_TYPE_UINT32</li> Key is a 32bit (4 byte) unsigned integer
 *   <li>UPS_TYPE_UINT64</li> Key is a 64bit (8 byte) unsigned integer
 *   <li>UPS_TYPE_REAL32</li> Key is a 32bit (4 byte) float
 *   <li>UPS_TYPE_REAL64</li> Key is a 64bit (8 byte) double
 * </ul>
 *
 * If the type is ommitted then @ref UPS_TYPE_BINARY is the default.
 *
 * If binary/custom keys are so big that they cannot be stored in the Btree,
 * then the full key will be stored in an overflow area, which has
 * performance implications when accessing such keys.
 *
 * In addition to the flags above, you can specify @a UPS_ENABLE_DUPLICATE_KEYS
 * to insert duplicate keys, i.e. to model 1:n or n:m relationships.
 *
 * If the size of the records is always constant, then
 * @ref UPS_PARAM_RECORD_SIZE should be used to specify this size. This allows
 * upscaledb to optimize the record storage, and small records will
 * automatically be stored in the Btree's leaf nodes instead of a separately
 * allocated blob, allowing faster access.
 * Setting a record size to 0 is valid and suited for boolean values
 * ("key exists" vs "key doesn't exist"). The default record size is
 * @ref UPS_RECORD_SIZE_UNLIMITED.
 *
 * Records can be compressed transparently in order to reduce
 * I/O and disk space. Compression is enabled with
 * @ref UPS_PARAM_RECORD_COMPRESSION. Values are one of
 * @ref UPS_COMPRESSOR_ZLIB, @ref UPS_COMPRESSOR_SNAPPY etc. See the
 * upscaledb documentation for more details.
 *
 * Keys can also be compressed by setting the parameter
 * @ref UPS_PARAM_KEY_COMPRESSION. See the upscaledb documentation
 * for more details.
 *
 * In addition, several integer compression algorithms are available
 * for Databases created with the type @ref UPS_TYPE_UINT32. Note that
 * integer compression only works with the default page size of 16kb.
 *
 * <ul>
 *   <li>@ref UPS_COMPRESSOR_UINT32_SIMDCOMP: fast and extremely good
 *      compression for dense keys (i.e. record number keys).</li>
 *   <li>@ref UPS_COMPRESSOR_UINT32_SIMDFOR: faster than
 *      @ref UPS_COMPRESSOR_UINT32_SIMDCOMP but with slightly worse
 *      compression. Only for Intel platforms, requires SSE3. <b>Not</b>
 *      compatible to @ref UPS_COMPRESSOR_UINT32_FOR.</li>
 *   <li>@ref UPS_COMPRESSOR_UINT32_FOR: Pure C implementation, slightly
 *      slower than @ref UPS_COMPRESSOR_UINT32_SIMDFOR. <b>Not</b>
 *      compatible to @ref UPS_COMPRESSOR_UINT32_SIMDFOR.</li>
 *   <li>@ref UPS_COMPRESSOR_UINT32_VARBYTE: Good compression and performance,
 *      especially for keys that are not dense (i.e. with "gaps"). If possible,
 *      uses AVX-instructions based on MaskedVbyte. Otherwise falls back to
 *      a plain C implementation.</li>
 * </ul>
 *
 * @param env A valid Environment handle.
 * @param db A valid Database handle, which will point to the created
 *      Database. To close the handle, use @ref ups_db_close.
 * @param name The name of the Database. If a Database with this name
 *      already exists, the function will fail with
 *      @ref UPS_DATABASE_ALREADY_EXISTS. Database names from 0xf000 to
 *      0xffff and 0 are reserved.
 * @param flags Optional flags for creating the Database, combined with
 *    bitwise OR. Possible flags are:
 *    <ul>
 *     <li>@ref UPS_ENABLE_DUPLICATE_KEYS </li> Enable duplicate keys for this
 *      Database. By default, duplicate keys are disabled.
 *     <li>@ref UPS_RECORD_NUMBER32 </li> Creates an "auto-increment" Database.
 *      Keys in Record Number Databases are automatically assigned an
 *      incrementing 32bit value. If key->data is not NULL
 *      (and key->flags is @ref UPS_KEY_USER_ALLOC), the value of the current
 *      key is returned in @a key. If key-data is NULL and key->size is 0,
 *      key->data is temporarily allocated by upscaledb.
 *     <li>@ref UPS_RECORD_NUMBER64 </li> Creates an "auto-increment" Database.
 *      Keys in Record Number Databases are automatically assigned an
 *      incrementing 64bit value. If key->data is not NULL
 *      (and key->flags is @ref UPS_KEY_USER_ALLOC), the value of the current
 *      key is returned in @a key. If key-data is NULL and key->size is 0,
 *      key->data is temporarily allocated by upscaledb.
 *    </ul>
 *
 * @param params An array of ups_parameter_t structures. The following
 *    parameters are available:
 *    <ul>
 *    <li>@ref UPS_PARAM_KEY_TYPE </li> The type of the keys in the B+Tree
 *      index. The default is @ref UPS_TYPE_BINARY. See above for more
 *      information.
 *    <li>@ref UPS_PARAM_KEY_SIZE </li> The (fixed) size of the keys in
 *      the B+Tree index; or @ref UPS_KEY_SIZE_UNLIMITED for unlimited and
 *      variable keys (this is the default).
 *    <li>@ref UPS_PARAM_RECORD_TYPE </li> The type of the records in the B+Tree
 *      index. The default is @ref UPS_TYPE_BINARY. See above for more
 *      information.
 *    <li>@ref UPS_PARAM_RECORD_SIZE </li> The (fixed) size of the records;
 *      or @ref UPS_RECORD_SIZE_UNLIMITED if there was no fixed record size
 *      specified (this is the default).
 *    <li>@ref UPS_PARAM_RECORD_COMPRESSION</li> Compresses
 *      the records.
 *    <li>@ref UPS_PARAM_KEY_COMPRESSION</li> Compresses
 *      the keys.
 *    <li>@ref UPS_PARAM_CUSTOM_COMPARE_NAME</li> Specifies the name of the
 *      custom compare function (only if @a UPS_PARAM_KEY_TYPE is @a
 *      UPS_TYPE_CUSTOM).
 *    </ul>
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if the @a env pointer is NULL or an
 *        invalid combination of flags was specified
 * @return @ref UPS_DATABASE_ALREADY_EXISTS if a Database with this @a name
 *        already exists in this Environment
 * @return @ref UPS_OUT_OF_MEMORY if memory could not be allocated
 * @return @ref UPS_LIMITS_REACHED if the maximum number of Databases per
 *        Environment was already created
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_env_create_db(ups_env_t *env, ups_db_t **db,
            uint16_t name, uint32_t flags, const ups_parameter_t *params);

/**
 * Opens a Database in a Database Environment
 *
 * Each Database in an Environment is identified by a positive 16bit
 * value (except 0 and values at or above 0xf000).
 *
 * This function initializes the ups_db_t handle (the second parameter).
 * When the handle is no longer in use, it should be closed with
 * @ref ups_db_close. Alternatively, the Database handle is closed
 * automatically if @ref ups_env_close is called with the flag
 * @ref UPS_AUTO_CLEANUP.
 *
 * If this database stores keys with a custom compare function then you
 * have to install the compare function (@ref ups_register_compare) prior
 * to opening the database.
 *
 * Records can be compressed transparently in order to reduce
 * I/O and disk space. Compression is enabled with
 * @ref UPS_PARAM_ENABLE_RECORD_COMPRESSION. Values are one of
 * @ref UPS_COMPRESSOR_ZLIB, @ref UPS_COMPRESSOR_SNAPPY etc. See the
 * upscaledb documentation for more details. This parameter is not
 * persisted.
 *
 * @param env A valid Environment handle
 * @param db A valid Database handle, which will point to the opened
 *      Database. To close the handle, use @see ups_db_close.
 * @param name The name of the Database. If a Database with this name
 *      does not exist, the function will fail with
 *      @ref UPS_DATABASE_NOT_FOUND.
 * @param flags Optional flags for opening the Database, combined with
 *    bitwise OR. Possible flags are:
 *   <ul>
 *     <li>@ref UPS_READ_ONLY </li> Opens the Database for reading only.
 *      Operations that need write access (i.e. @ref ups_db_insert) will
 *      return @ref UPS_WRITE_PROTECTED.
 *   </ul>
 * @param params Reserved; set to NULL
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if the @a env pointer is NULL or an
 *        invalid combination of flags was specified
 * @return @ref UPS_DATABASE_NOT_FOUND if a Database with this @a name
 *        does not exist in this Environment.
 * @return @ref UPS_DATABASE_ALREADY_OPEN if this Database was already
 *        opened
 * @return @ref UPS_OUT_OF_MEMORY if memory could not be allocated
 * @return @ref UPS_NOT_READY if the database requires a custom callback
 *        function, but this function was not yet registered
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_env_open_db(ups_env_t *env, ups_db_t **db,
            uint16_t name, uint32_t flags, const ups_parameter_t *params);

/**
 * Renames a Database in an Environment.
 *
 * @param env A valid Environment handle.
 * @param oldname The old name of the existing Database. If a Database
 *      with this name does not exist, the function will fail with
 *      @ref UPS_DATABASE_NOT_FOUND.
 * @param newname The new name of this Database. If a Database
 *      with this name already exists, the function will fail with
 *      @ref UPS_DATABASE_ALREADY_EXISTS.
 * @param flags Optional flags for renaming the Database, combined with
 *    bitwise OR; unused, set to 0.
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if the @a env pointer is NULL or if
 *        the new Database name is reserved
 * @return @ref UPS_DATABASE_NOT_FOUND if a Database with this @a name
 *        does not exist in this Environment
 * @return @ref UPS_DATABASE_ALREADY_EXISTS if a Database with the new name
 *        already exists
 * @return @ref UPS_OUT_OF_MEMORY if memory could not be allocated
 * @return @ref UPS_NOT_READY if the Environment @a env was not initialized
 *        correctly (i.e. not yet opened or created)
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_env_rename_db(ups_env_t *env, uint16_t oldname,
            uint16_t newname, uint32_t flags);

/**
 * Deletes a Database from an Environment
 *
 * @param env A valid Environment handle
 * @param name The name of the Database to delete. If a Database
 *      with this name does not exist, the function will fail with
 *      @ref UPS_DATABASE_NOT_FOUND. If the Database was already opened,
 *      the function will fail with @ref UPS_DATABASE_ALREADY_OPEN.
 * @param flags Optional flags for deleting the Database; unused, set to 0.
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if the @a env pointer is NULL or if
 *        the new Database name is reserved
 * @return @ref UPS_DATABASE_NOT_FOUND if a Database with this @a name
 *        does not exist
 * @return @ref UPS_DATABASE_ALREADY_OPEN if a Database with this name is
 *        still open
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_env_erase_db(ups_env_t *env, uint16_t name, uint32_t flags);

/* internal flag - only flush committed transactions, not the btree pages */
#define UPS_FLUSH_COMMITTED_TRANSACTIONS    1

/**
 * Flushes the Environment
 *
 * This function flushes the Environment caches and writes the whole file
 * to disk. All Databases of this Environment are flushed as well.
 *
 * Since In-Memory Databases do not have a file on disk, the
 * function will have no effect and will return @ref UPS_SUCCESS.
 *
 * @param env A valid Environment handle
 * @param flags Optional flags for flushing; unused, set to 0
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if @a db is NULL
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_env_flush(ups_env_t *env, uint32_t flags);

/* internal use only - don't lock mutex */
#define UPS_DONT_LOCK        0xf0000000

/**
 * Returns the names of all Databases in an Environment
 *
 * This function returns the names of all Databases and the number of
 * Databases in an Environment.
 *
 * The memory for @a names must be allocated by the user. @a length
 * must be the length of @a names when calling the function, and will be
 * the number of Databases when the function returns. The function returns
 * @ref UPS_LIMITS_REACHED if @a names is not big enough; in this case, the
 * caller should resize the array and call the function again.
 *
 * @param env A valid Environment handle
 * @param names Pointer to an array for the Database names
 * @param length Pointer to the length of the array; will be used to store the
 *      number of Databases when the function returns.
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if @a env, @a names or @a length is NULL
 * @return @ref UPS_LIMITS_REACHED if @a names is not large enough to hold
 *      all Database names
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_env_get_database_names(ups_env_t *env, uint16_t *names,
            uint32_t *length);

/**
 * Closes the Database Environment
 *
 * This function closes the Database Environment. It also frees the
 * memory resources allocated in the @a env handle, and tries to truncate
 * the file (see below).
 *
 * If the flag @ref UPS_AUTO_CLEANUP is specified, upscaledb automatically
 * calls @ref ups_db_close with flag @ref UPS_AUTO_CLEANUP on all open
 * Databases (which closes all open Databases and their Cursors). This
 * invalidates the ups_db_t and ups_cursor_t handles!
 *
 * If the flag is not specified, the application must close all Database
 * handles with @ref ups_db_close to prevent memory leaks.
 *
 * This function also aborts all Transactions which were not yet committed,
 * and therefore renders all Txn handles invalid. If the flag
 * @ref UPS_TXN_AUTO_COMMIT is specified, all Transactions will be committed.
 *
 * This function also tries to truncate the file and "cut off" unused space
 * at the end of the file to reduce the file size. This feature is disabled
 * on Win32 if memory mapped I/O is used (see @ref UPS_DISABLE_MMAP).
 *
 * @param env A valid Environment handle
 * @param flags Optional flags for closing the handle. Possible flags are:
 *      <ul>
 *      <li>@ref UPS_AUTO_CLEANUP. Calls @ref ups_db_close with the flag
 *        @ref UPS_AUTO_CLEANUP on every open Database
 *      <li>@ref UPS_TXN_AUTO_COMMIT. Automatically commit all open
 *         Transactions
 *      <li>@ref UPS_TXN_AUTO_ABORT. Automatically abort all open
 *         Transactions; this is the default behaviour
 *      </ul>
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if @a env is NULL
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_env_close(ups_env_t *env, uint32_t flags);

/**
 * @}
 */


/**
 * @defgroup ups_txn upscaledb Txn Functions
 * @{
 */

/**
 * The upscaledb Txn structure
 *
 * This structure is allocated with @ref ups_txn_begin and deleted with
 * @ref ups_txn_commit or @ref ups_txn_abort.
 */
struct ups_txn_t;
typedef struct ups_txn_t ups_txn_t;

/**
 * Begins a new Txn
 *
 * A Txn is an atomic sequence of Database operations. With @ref
 * ups_txn_begin such a new sequence is started. To write all operations of this
 * sequence to the Database use @ref ups_txn_commit. To abort and cancel
 * this sequence use @ref ups_txn_abort.
 *
 * In order to use Transactions, the Environment has to be created or
 * opened with the flag @ref UPS_ENABLE_TRANSACTIONS.
 *
 * You can create as many Transactions as you want (older versions of
 * upscaledb did not allow to create more than one Txn in parallel).
 *
 * @param txn Pointer to a pointer of a Txn structure
 * @param env A valid Environment handle
 * @param name An optional Txn name
 * @param reserved A reserved pointer; always set to NULL
 * @param flags Optional flags for beginning the Txn, combined with
 *    bitwise OR. Possible flags are:
 *    <ul>
 *     <li>@ref UPS_TXN_READ_ONLY </li> This Txn is read-only and
 *      will not modify the Database.
 *    </ul>
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_OUT_OF_MEMORY if memory allocation failed
 */
UPS_EXPORT ups_status_t
ups_txn_begin(ups_txn_t **txn, ups_env_t *env, const char *name,
            void *reserved, uint32_t flags);

/** Flag for @ref ups_txn_begin */
#define UPS_TXN_READ_ONLY                     1

/* Internal flag for @ref ups_txn_begin */
#define UPS_TXN_TEMPORARY                     2

/**
 * Retrieves the Txn name
 *
 * @returns NULL if the name was not assigned or if @a txn is invalid
 */
UPS_EXPORT const char *
ups_txn_get_name(ups_txn_t *txn);

/**
 * Commits a Txn
 *
 * This function applies the sequence of Database operations.
 *
 * Note that the function will fail with @ref UPS_CURSOR_STILL_OPEN if
 * a Cursor was attached to this Txn (with @ref ups_cursor_create
 * or @ref ups_cursor_clone), and the Cursor was not closed.
 *
 * @param txn Pointer to a Txn structure
 * @param flags Optional flags for committing the Txn, combined with
 *    bitwise OR. Unused, set to 0.
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_IO_ERROR if writing to the file failed
 * @return @ref UPS_CURSOR_STILL_OPEN if there are Cursors attached to this
 *      Txn
 */
UPS_EXPORT ups_status_t
ups_txn_commit(ups_txn_t *txn, uint32_t flags);

/**
 * Aborts a Txn
 *
 * This function aborts (= cancels) the sequence of Database operations.
 *
 * Note that the function will fail with @ref UPS_CURSOR_STILL_OPEN if
 * a Cursor was attached to this Txn (with @ref ups_cursor_create
 * or @ref ups_cursor_clone), and the Cursor was not closed.
 *
 * @param txn Pointer to a Txn structure
 * @param flags Optional flags for aborting the Txn, combined with
 *    bitwise OR. Unused, set to 0.
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_IO_ERROR if writing to the Database file or logfile failed
 * @return @ref UPS_CURSOR_STILL_OPEN if there are Cursors attached to this
 *      Txn
 */
UPS_EXPORT ups_status_t
ups_txn_abort(ups_txn_t *txn, uint32_t flags);

/**
 * @}
 */


/**
 * @defgroup ups_database upscaledb Database Functions
 * @{
 */

/** Flag for @ref ups_env_open, @ref ups_env_create.
 * This flag is non persistent. */
#define UPS_ENABLE_FSYNC                            0x00000001

/* internal flag */
#define UPS_IGNORE_MISSING_CALLBACK                 0x00000002 

/** Flag for @ref ups_env_open, @ref ups_env_open_db.
 * This flag is non persistent. */
#define UPS_READ_ONLY                               0x00000004

/* unused                                           0x00000008 */

/* unused                                           0x00000010 */

/* reserved                                         0x00000020 */

/* unused                                           0x00000040 */

/** Flag for @ref ups_env_create.
 * This flag is non persistent. */
#define UPS_IN_MEMORY                               0x00000080

/* reserved: DB_USE_MMAP (not persistent)           0x00000100 */

/** Flag for @ref ups_env_open, @ref ups_env_create.
 * This flag is non persistent. */
#define UPS_DISABLE_MMAP                            0x00000200

/* deprecated */
#define UPS_RECORD_NUMBER                           UPS_RECORD_NUMBER64

/** Flag for @ref ups_env_create_db.
 * This flag is persisted in the Database. */
#define UPS_RECORD_NUMBER32                         0x00001000

/** Flag for @ref ups_env_create_db.
 * This flag is persisted in the Database. */
#define UPS_RECORD_NUMBER64                         0x00002000

/** Flag for @ref ups_env_create_db.
 * This flag is persisted in the Database. */
#define UPS_ENABLE_DUPLICATE_KEYS                   0x00004000
/* deprecated */
#define UPS_ENABLE_DUPLICATES                       UPS_ENABLE_DUPLICATE_KEYS

/* deprecated */
#define UPS_ENABLE_RECOVERY                         UPS_ENABLE_TRANSACTIONS

/** Flag for @ref ups_env_open.
 * This flag is non persistent. */
#define UPS_AUTO_RECOVERY                           0x00010000

/** Flag for @ref ups_env_create, @ref ups_env_open.
 * This flag is non persistent. */
#define UPS_ENABLE_TRANSACTIONS                     0x00020000

/** Flag for @ref ups_env_open, @ref ups_env_create.
 * This flag is non persistent. */
#define UPS_CACHE_UNLIMITED                         0x00040000

/** Flag for @ref ups_env_create, @ref ups_env_open.
 * This flag is non persistent. */
#define UPS_DISABLE_RECOVERY                        0x00080000

/* internal use only! (not persistent) */
#define UPS_IS_REMOTE_INTERNAL                      0x00200000

/* internal use only! (not persistent) */
#define UPS_DISABLE_RECLAIM_INTERNAL                0x00400000

/* internal use only! (persistent) */
#define UPS_FORCE_RECORDS_INLINE                    0x00800000

/** Flag for @ref ups_env_open, @ref ups_env_create.
 * This flag is non persistent. */
#define UPS_ENABLE_CRC32                            0x02000000

/* internal use only! (not persistent) */
#define UPS_DONT_FLUSH_TRANSACTIONS                 0x04000000

/** Flag for @ref ups_env_open, @ref ups_env_create.
 * This flag is non persistent. */
#define UPS_FLUSH_TRANSACTIONS_IMMEDIATELY          0x08000000

/**
 * Typedef for a key comparison function
 *
 * @remark This function compares two index keys. It returns -1, if @a lhs
 * ("left-hand side", the parameter on the left side) is smaller than
 * @a rhs ("right-hand side"), 0 if both keys are equal, and 1 if @a lhs
 * is larger than @a rhs.
 */
typedef int UPS_CALLCONV (*ups_compare_func_t)(ups_db_t *db,
                  const uint8_t *lhs, uint32_t lhs_length,
                  const uint8_t *rhs, uint32_t rhs_length);

/**
 * Globally registers a function to compare custom keys
 *
 * Supplying a comparison function is only allowed for the key type
 * @ref UPS_TYPE_CUSTOM; see the documentation of @sa ups_env_create_db 
 * for more information.
 *
 * When creating a database (@sa ups_env_create_db), the name of the
 * comparison function has to be specified as an extended parameter
 * (@sa UPS_PARAM_CUSTOM_COMPARE_NAME). It is valid to register a compare
 * function multiple times under the same name.
 *
 * !!!
 * The compare functions should be registered PRIOR to opening or
 * creating Environments!
 *
 * @param name A (case-insensitive) name of the callback function
 * @param func A pointer to the compare function
 *
 * @return @ref UPS_SUCCESS
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_register_compare(const char *name, ups_compare_func_t func);

/* deprecated */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_set_compare_func(ups_db_t *db, ups_compare_func_t foo);

/**
 * Searches an item in the Database
 *
 * This function searches the Database for @a key. If the key
 * is found, @a record will receive the record of this item and
 * @ref UPS_SUCCESS is returned. If the key is not found, the function
 * returns @ref UPS_KEY_NOT_FOUND.
 *
 * A ups_record_t structure should be initialized with
 * zeroes before it is being used. This can be done with the C library
 * routines memset(3) or bzero(2).
 *
 * If the function completes successfully, the @a record pointer is
 * initialized with the size of the record (in @a record.size) and the
 * actual record data (in @a record.data). If the record is empty,
 * @a size is 0 and @a data points to NULL.
 *
 * The @a data pointer is a temporary pointer and will be overwritten
 * by subsequent upscaledb API calls using the same Txn
 * (or, if Transactions are disabled, using the same Database).
 * You can alter this behaviour by allocating the @a data pointer in
 * the application and setting @a record.flags to @ref UPS_RECORD_USER_ALLOC.
 * Make sure that the allocated buffer is large enough.
 *
 * @ref ups_db_find can not search for duplicate keys. If @a key has
 * multiple duplicates, only the first duplicate is returned.
 *
 * If Transactions are enabled (see @ref UPS_ENABLE_TRANSACTIONS) and
 * @a txn is NULL then upscaledb will create a temporary Txn.
 * When moving the Cursor, and the new key is currently modified in an
 * active Txn (one that is not yet committed or aborted) then
 * upscaledb will skip this key and move to the next/previous one. However if
 * @a flags are 0 (and the Cursor is not moved), and @a key or @a rec
 * is NOT NULL, then upscaledb will return error @ref UPS_TXN_CONFLICT.
 *
 * @param db A valid Database handle
 * @param txn A Txn handle, or NULL
 * @param key The key of the item
 * @param record The record of the item
 * @param flags Optional flags for searching, which can be combined with
 *    bitwise OR. Possible flags are:
 *    <ul>
 *    <li>@ref UPS_FIND_LT_MATCH </li> Cursor 'find' flag 'Less Than': the
 *        cursor is moved to point at the last record which' key
 *        is less than the specified key. When such a record cannot
 *        be located, an error is returned.
 *    <li>@ref UPS_FIND_GT_MATCH </li> Cursor 'find' flag 'Greater Than':
 *        the cursor is moved to point at the first record which' key is
 *        larger than the specified key. When such a record cannot be
 *        located, an error is returned.
 *    <li>@ref UPS_FIND_LEQ_MATCH </li> Cursor 'find' flag 'Less or EQual':
 *        the cursor is moved to point at the record which' key matches
 *        the specified key and when such a record is not available
 *        the cursor is moved to point at the last record which' key
 *        is less than the specified key. When such a record cannot be
 *        located, an error is returned.
 *    <li>@ref UPS_FIND_GEQ_MATCH </li> Cursor 'find' flag 'Greater or
 *        Equal': the cursor is moved to point at the record which' key
 *        matches the specified key and when such a record
 *        is not available the cursor is moved to point at the first
 *        record which' key is larger than the specified key.
 *        When such a record cannot be located, an error is returned.
 *    <li>@ref UPS_FIND_NEAR_MATCH </li> Cursor 'find' flag 'Any Near Or
 *        Equal': the cursor is moved to point at the record which'
 *        key matches the specified key and when such a record is
 *        not available the cursor is moved to point at either the
 *        last record which' key is less than the specified key or
 *        the first record which' key is larger than the specified
 *        key, whichever of these records is located first.
 *        When such records cannot be located, an error is returned.
 *    </ul>
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if @a db, @a key or @a record is NULL
 * @return @ref UPS_KEY_NOT_FOUND if the @a key does not exist
 * @return @ref UPS_TXN_CONFLICT if the same key was inserted in another
 *        Txn which was not yet committed or aborted
 *
 * @remark When either or both @ref UPS_FIND_LT_MATCH and/or @ref
 *    UPS_FIND_GT_MATCH have been specified as flags, the @a key structure
 *    will be overwritten when an approximate match was found: the
 *    @a key and @a record structures will then point at the located
 *    @a key and @a record. In this case the caller should ensure @a key
 *    points at a structure which must adhere to the same restrictions
 *    and conditions as specified for @ref ups_cursor_move(...,
 *    UPS_CURSOR_NEXT).
 *
 * @sa UPS_RECORD_USER_ALLOC
 * @sa UPS_KEY_USER_ALLOC
 * @sa ups_record_t
 * @sa ups_key_t
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_find(ups_db_t *db, ups_txn_t *txn, ups_key_t *key,
            ups_record_t *record, uint32_t flags);

/**
 * Inserts a Database item
 *
 * This function inserts a key/record pair as a new Database item.
 *
 * If the key already exists in the Database, error @ref UPS_DUPLICATE_KEY
 * is returned.
 *
 * If you wish to overwrite an existing entry specify the
 * flag @ref UPS_OVERWRITE.
 *
 * If you wish to insert a duplicate key specify the flag @ref UPS_DUPLICATE.
 * (Note that the Database has to be created with @ref UPS_ENABLE_DUPLICATE_KEYS
 * in order to use duplicate keys.)
 * The duplicate key is inserted after all other duplicate keys (see
 * @ref UPS_DUPLICATE_INSERT_LAST).
 *
 * Record Number Databases (created with @ref UPS_RECORD_NUMBER32 or
 * @ref UPS_RECORD_NUMBER64) expect either an empty @a key (with a size of
 * 0 and data pointing to NULL), or a user-supplied key (with key.flag
 * @ref UPS_KEY_USER_ALLOC and a valid data pointer).
 * If key.size is 0 and key.data is NULL, upscaledb will temporarily
 * allocate memory for key->data, which will then point to an 4-byte (or 8-byte)
 * unsigned integer.
 *
 * @param db A valid Database handle
 * @param txn A Txn handle, or NULL
 * @param key The key of the new item
 * @param record The record of the new item
 * @param flags Optional flags for inserting. Possible flags are:
 *    <ul>
 *    <li>@ref UPS_OVERWRITE. If the @a key already exists, the record is
 *        overwritten. Otherwise, the key is inserted. Flag is not
 *        allowed in combination with @ref UPS_DUPLICATE.
 *    <li>@ref UPS_DUPLICATE. If the @a key already exists, a duplicate
 *        key is inserted. The key is inserted before the already
 *        existing key. Flag is not allowed in combination with
 *        @ref UPS_OVERWRITE.
 *    </ul>
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if @a db, @a key or @a record is NULL
 * @return @ref UPS_INV_PARAMETER if the Database is a Record Number Database
 *        and the key is invalid (see above)
 * @return @ref UPS_INV_PARAMETER if the flags @ref UPS_OVERWRITE <b>and</b>
 *        @ref UPS_DUPLICATE were specified, or if @ref UPS_DUPLICATE
 *        was specified, but the Database was not created with
 *        flag @ref UPS_ENABLE_DUPLICATE_KEYS.
 * @return @ref UPS_WRITE_PROTECTED if you tried to insert a key in a read-only
 *        Database
 * @return @ref UPS_TXN_CONFLICT if the same key was inserted in another
 *        Txn which was not yet committed or aborted
 * @return @ref UPS_INV_KEY_SIZE if the key size is larger than the
 *        @a UPS_PARAMETER_KEY_SIZE parameter specified for
 *        @ref ups_env_create_db
 *        OR if the key's size is greater than the Btree key size (see
 *        @ref UPS_PARAM_KEY_SIZE).
 * @return @ref UPS_INV_RECORD_SIZE if the record size is different from
 *        the one specified with @a UPS_PARAM_RECORD_SIZE
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_insert(ups_db_t *db, ups_txn_t *txn, ups_key_t *key,
            ups_record_t *record, uint32_t flags);

/**
 * Flag for @ref ups_db_insert and @ref ups_cursor_insert
 *
 * When specified with @ref ups_db_insert and in case a key
 * is specified which stores duplicates in the Database, the first
 * duplicate record will be overwritten.
 *
 * When used with @ref ups_cursor_insert and assuming the same
 * conditions, the duplicate currently referenced by the Cursor
 * will be overwritten.
*/
#define UPS_OVERWRITE                   0x0001

/** Flag for @ref ups_db_insert and @ref ups_cursor_insert */
#define UPS_DUPLICATE                   0x0002

/** Flag for @ref ups_cursor_insert */
#define UPS_DUPLICATE_INSERT_BEFORE     0x0004

/** Flag for @ref ups_cursor_insert */
#define UPS_DUPLICATE_INSERT_AFTER      0x0008

/** FlagFlag for @ref ups_cursor_insert */
#define UPS_DUPLICATE_INSERT_FIRST      0x0010

/** Flag for @ref ups_cursor_insert */
#define UPS_DUPLICATE_INSERT_LAST       0x0020

/* internal flag */
#define UPS_DIRECT_ACCESS               0x0040

/* internal flag */
#define UPS_FORCE_DEEP_COPY             0x0100

/* internal flag */
#define UPS_HINT_APPEND                 0x00080000

/* internal flag */
#define UPS_HINT_PREPEND                0x00100000

/**
 * Erases a Database item
 *
 * This function erases a Database item. If the item @a key
 * does not exist, @ref UPS_KEY_NOT_FOUND is returned.
 *
 * Note that ups_db_erase can not erase a single duplicate key. If the key
 * has multiple duplicates, all duplicates of this key will be erased. Use
 * @ref ups_cursor_erase to erase a specific duplicate key.
 *
 * @param db A valid Database handle
 * @param txn A Txn handle, or NULL
 * @param key The key to delete
 * @param flags Optional flags for erasing; unused, set to 0
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if @a db or @a key is NULL
 * @return @ref UPS_WRITE_PROTECTED if you tried to erase a key from a read-only
 *        Database
 * @return @ref UPS_KEY_NOT_FOUND if @a key was not found
 * @return @ref UPS_TXN_CONFLICT if the same key was inserted in another
 *        Txn which was not yet committed or aborted
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_erase(ups_db_t *db, ups_txn_t *txn, ups_key_t *key, uint32_t flags);

/* internal flag for ups_db_erase() - do not use */
#define UPS_ERASE_ALL_DUPLICATES                1

/**
 * Returns the number of keys stored in the Database
 *
 * You can specify the @ref UPS_SKIP_DUPLICATES if you do now want
 * to include any duplicates in the count. This will also speed up the
 * counting.
 *
 * @param db A valid Database handle
 * @param txn A Txn handle, or NULL
 * @param flags Optional flags:
 *     <ul>
 *     <li>@ref UPS_SKIP_DUPLICATES. Excludes any duplicates from
 *       the count
 *     </ul>
 * @param count A pointer to a variable which will receive
 *         the calculated key count per page
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if @a db or @a keycount is NULL or when
 *     @a flags contains an invalid flag set
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_count(ups_db_t *db, ups_txn_t *txn, uint32_t flags, uint64_t *count);

/**
 * Retrieve the current value for a given Database setting
 *
 * Only those values requested by the parameter array will be stored.
 *
 * The following parameters are supported:
 *    <ul>
 *    <li>UPS_PARAM_FLAGS</li> returns the flags which were used to
 *        open or create this Database
 *    <li>UPS_PARAM_DATABASE_NAME</li> returns the Database name
 *    <li>UPS_PARAM_KEY_TYPE</li> returns the Btree key type
 *    <li>UPS_PARAM_KEY_SIZE</li> returns the Btree key size
 *        or @ref UPS_KEY_SIZE_UNLIMITED if there was no fixed key size
 *        specified.
 *    <li>UPS_PARAM_RECORD_TYPE</li> returns the Btree record type
 *    <li>UPS_PARAM_RECORD_SIZE</li> returns the record size,
 *        or @ref UPS_RECORD_SIZE_UNLIMITED if there was no fixed record size
 *        specified.
 *    <li>UPS_PARAM_MAX_KEYS_PER_PAGE</li> returns the maximum number
 *        of keys per page. This number is precise if the key size is fixed
 *        and duplicates are disabled; otherwise it's an estimate.
 *    <li>@ref UPS_PARAM_RECORD_COMPRESSION</li> Returns the
 *        selected algorithm for record compression, or 0 if compression
 *        is disabled
 *    <li>@ref UPS_PARAM_KEY_COMPRESSION</li> Returns the
 *        selected algorithm for key compression, or 0 if compression
 *        is disabled
 *    </ul>
 *
 * @param db A valid Database handle
 * @param param An array of ups_parameter_t structures
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if the @a db pointer is NULL or
 *        @a param is NULL
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_get_parameters(ups_db_t *db, ups_parameter_t *param);

/** Parameter name for @ref ups_env_open, @ref ups_env_create;
 * Journal files are switched whenever the number of new Transactions exceeds
 * this threshold. */
#define UPS_PARAM_JOURNAL_SWITCH_THRESHOLD 0x00001

/** Parameter name for @ref ups_env_open, @ref ups_env_create;
 * sets the cache size */
#define UPS_PARAM_CACHE_SIZE            0x00000100
/* deprecated */
#define UPS_PARAM_CACHESIZE             UPS_PARAM_CACHE_SIZE

/** Parameter name for @ref ups_env_create; sets the page size */
#define UPS_PARAM_PAGE_SIZE             0x00000101
/* deprecated */
#define UPS_PARAM_PAGESIZE              UPS_PARAM_PAGE_SIZE

/** Parameter name for @ref ups_env_create_db; sets the key size */
#define UPS_PARAM_KEY_SIZE              0x00000102
/* deprecated */
#define UPS_PARAM_KEYSIZE               UPS_PARAM_KEY_SIZE

/** Parameter name for @ref ups_env_get_parameters; retrieves the number
 * of maximum Databases */
#define UPS_PARAM_MAX_DATABASES         0x00000103

/** Parameter name for @ref ups_env_create_db; sets the key type */
#define UPS_PARAM_KEY_TYPE              0x00000104

/** Parameter name for @ref ups_env_open, @ref ups_env_create;
 * sets the path of the log files */
#define UPS_PARAM_LOG_DIRECTORY         0x00000105

/** Parameter name for @ref ups_env_open, @ref ups_env_create;
 * sets the AES encryption key */
#define UPS_PARAM_ENCRYPTION_KEY        0x00000106

/** Parameter name for @ref ups_env_open, @ref ups_env_create;
 * sets the network timeout (in seconds) */
#define UPS_PARAM_NETWORK_TIMEOUT_SEC   0x00000107

/** Parameter name for @ref ups_env_create_db; sets the key size */
#define UPS_PARAM_RECORD_SIZE           0x00000108

/** Parameter name for @ref ups_env_create, @ref ups_env_open; sets a
 * limit for the file size (in bytes) */
#define UPS_PARAM_FILE_SIZE_LIMIT       0x00000109

/** Parameter name for @ref ups_env_create, @ref ups_env_open; sets the
 * parameter for posix_fadvise() */
#define UPS_PARAM_POSIX_FADVISE         0x00000110

/** Parameter name for @ref ups_env_create_db */
#define UPS_PARAM_CUSTOM_COMPARE_NAME   0x00000111

/** Parameter name for @ref ups_env_create_db; sets the record type */
#define UPS_PARAM_RECORD_TYPE           0x00000112

/** Value for @ref UPS_PARAM_POSIX_FADVISE */
#define UPS_POSIX_FADVICE_NORMAL                 0

/** Value for @ref UPS_PARAM_POSIX_FADVISE */
#define UPS_POSIX_FADVICE_RANDOM                 1

/** Value for unlimited record sizes */
#define UPS_RECORD_SIZE_UNLIMITED       ((uint32_t)-1)

/** Value for unlimited key sizes */
#define UPS_KEY_SIZE_UNLIMITED          ((uint16_t)-1)

/** Retrieves the Database/Environment flags as were specified at the time of
 * @ref ups_env_create/@ref ups_env_open invocation. */
#define UPS_PARAM_FLAGS                 0x00000200

/** Retrieves the filesystem file access mode as was specified at the time
 * of @ref ups_env_create/@ref ups_env_open invocation. */
#define UPS_PARAM_FILEMODE              0x00000201

/**
 * Return a <code>const char *</code> pointer to the current
 * Environment/Database file name in the @ref uint64_t value
 * member, when the Database is actually stored on disc.
 *
 * In-memory Databases will return a NULL (0) pointer instead.
 */
#define UPS_PARAM_FILENAME              0x00000202

/**
 * Retrieve the Database 'name' number of this @ref ups_db_t Database within
 * the current @ref ups_env_t Environment.
*/
#define UPS_PARAM_DATABASE_NAME         0x00000203

/**
 * Retrieve the maximum number of keys per page; this number depends on the
 * currently active page and key sizes. Can be an estimate if keys do not
 * have constant sizes or if duplicate keys are used.
 */
#define UPS_PARAM_MAX_KEYS_PER_PAGE     0x00000204

/**
 * Parameter name for @ref ups_env_create, @ref ups_env_open;
 * enables compression for the journal.
 */
#define UPS_PARAM_JOURNAL_COMPRESSION   0x00001000

/**
 * Parameter name for @ref ups_env_create_db,
 * @ref ups_env_open_db; enables compression for the records of
 * a Database.
 */
#define UPS_PARAM_RECORD_COMPRESSION    0x00001001

/**
 * Parameter name for @ref ups_env_create_db,
 * @ref ups_env_open_db; enables compression for the records of
 * a Database.
 */
#define UPS_PARAM_KEY_COMPRESSION       0x00001002

/** helper macro for disabling compression */
#define UPS_COMPRESSOR_NONE         0

/**
 * selects zlib compression
 * http://www.zlib.net/
 */
#define UPS_COMPRESSOR_ZLIB         1

/**
 * selects google snappy compression
 * http://code.google.com/p/snappy
 */
#define UPS_COMPRESSOR_SNAPPY       2

/**
 * selects lzf compression
 * http://oldhome.schmorp.de/marc/liblzf.html
 */
#define UPS_COMPRESSOR_LZF          3

/** uint32 key compression (varbyte) */
#define UPS_COMPRESSOR_UINT32_VARBYTE       5
#define UPS_COMPRESSOR_UINT32_MASKEDVBYTE   UPS_COMPRESSOR_UINT32_VARBYTE

/** uint32 key compression (BP128) */
#define UPS_COMPRESSOR_UINT32_SIMDCOMP      6

/* deprecated */
#define UPS_COMPRESSOR_UINT32_GROUPVARINT   7

/* deprecated */
#define UPS_COMPRESSOR_UINT32_STREAMVBYTE   8

/** uint32 key compression (libfor - Frame Of Reference) */
#define UPS_COMPRESSOR_UINT32_FOR          10

/** uint32 key compression (SIMDFOR - Frame Of Reference w/ SIMD) */
#define UPS_COMPRESSOR_UINT32_SIMDFOR      11

/**
 * Retrieves the Environment handle of a Database
 *
 * @param db A valid Database handle
 *
 * @return The Environment handle
 */
UPS_EXPORT ups_env_t *UPS_CALLCONV
ups_db_get_env(ups_db_t *db);

/**
 * Closes the Database
 *
 * This function flushes the Database and then closes the file handle.
 * It also free the memory resources allocated in the @a db handle.
 *
 * If the flag @ref UPS_AUTO_CLEANUP is specified, upscaledb automatically
 * calls @ref ups_cursor_close on all open Cursors. This invalidates the
 * ups_cursor_t handle!
 *
 * If the flag is not specified, the application must close all Database
 * Cursors with @ref ups_cursor_close to prevent memory leaks.
 *
 * @param db A valid Database handle
 * @param flags Optional flags for closing the Database. Possible values are:
 *    <ul>
 *     <li>@ref UPS_AUTO_CLEANUP. Automatically closes all open Cursors
 *    </ul>
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if @a db is NULL
 * @return @ref UPS_CURSOR_STILL_OPEN if not all Cursors of this Database
 *    were closed, and @ref UPS_AUTO_CLEANUP was not specified
 * @return @ref UPS_TXN_STILL_OPEN if this Database is modified by a
 *    currently active Txn
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_db_close(ups_db_t *db, uint32_t flags);

/** Flag for @ref ups_db_close, @ref ups_env_close */
#define UPS_AUTO_CLEANUP                1

/** @internal (Internal) flag for @ref ups_db_close, @ref ups_env_close */
#define UPS_DONT_CLEAR_LOG              2

/** Automatically abort all open Transactions (the default) */
#define UPS_TXN_AUTO_ABORT              4

/** Automatically commit all open Transactions */
#define UPS_TXN_AUTO_COMMIT             8

/**
 * @}
 */

/**
 * @defgroup ups_cursor upscaledb Cursor Functions
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
 * The application should close all Cursors of a Database before closing
 * the Database.
 *
 * If Transactions are enabled (@ref UPS_ENABLE_TRANSACTIONS), but @a txn
 * is NULL, then each Cursor operation (i.e. @ref ups_cursor_insert,
 * @ref ups_cursor_find etc) will create its own, temporary Txn
 * <b>only</b> for the lifetime of this operation and not for the lifetime
 * of the whole Cursor!
 *
 * @param db A valid Database handle
 * @param txn A Txn handle, or NULL
 * @param flags Optional flags for creating the Cursor; unused, set to 0
 * @param cursor A pointer to a pointer which is allocated for the
 *      new Cursor handle
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if @a db or @a cursor is NULL
 * @return @ref UPS_OUT_OF_MEMORY if the new structure could not be allocated
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_cursor_create(ups_cursor_t **cursor, ups_db_t *db, ups_txn_t *txn,
            uint32_t flags);

/**
 * Clones a Database Cursor
 *
 * Clones an existing Cursor. The new Cursor will point to
 * exactly the same item as the old Cursor. If the old Cursor did not point
 * to any item, so will the new Cursor.
 *
 * If the old Cursor is bound to a Txn, then the new Cursor will
 * also be bound to this Txn.
 *
 * @param src The existing Cursor
 * @param dest A pointer to a pointer, which is allocated for the
 *      cloned Cursor handle
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if @a src or @a dest is NULL
 * @return @ref UPS_OUT_OF_MEMORY if the new structure could not be allocated
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_cursor_clone(ups_cursor_t *src, ups_cursor_t **dest);

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
 * If Transactions are enabled (see @ref UPS_ENABLE_TRANSACTIONS), and
 * the Cursor moves next or previous to a key which is currently modified
 * in an active Txn (one that is not yet committed or aborted), then
 * upscaledb will skip the modified key. (This behavior is different from i.e.
 * @a ups_cursor_find, which would return the error @ref UPS_TXN_CONFLICT).
 *
 * If a key has duplicates and any of the duplicates is currently modified
 * in another active Txn, then ALL duplicate keys are skipped when
 * moving to the next or previous key.
 *
 * If the first (@ref UPS_CURSOR_FIRST) or last (@ref UPS_CURSOR_LAST) key
 * is requested, and the current key (or any of its duplicates) is currently
 * modified in an active Txn, then @ref UPS_TXN_CONFLICT is
 * returned.
 *
 * If this Cursor is nil (i.e. because it was not yet used or the Cursor's
 * item was erased) then the flag @a UPS_CURSOR_NEXT (or @a
 * UPS_CURSOR_PREVIOUS) will be identical to @a UPS_CURSOR_FIRST (or
 * @a UPS_CURSOR_LAST).
 *
 * @param cursor A valid Cursor handle
 * @param key An optional pointer to a @ref ups_key_t structure. If this
 *    pointer is not NULL, the key of the new item is returned.
 *    Note that key->data will point to temporary data. This pointer
 *    will be invalidated by subsequent upscaledb API calls. See
 *    @ref UPS_KEY_USER_ALLOC on how to change this behaviour.
 * @param record An optional pointer to a @ref ups_record_t structure. If this
 *    pointer is not NULL, the record of the new item is returned.
 *    Note that record->data will point to temporary data. This pointer
 *    will be invalidated by subsequent upscaledb API calls. See
 *    @ref UPS_RECORD_USER_ALLOC on how to change this behaviour.
 * @param flags The flags for this operation. They are used to specify
 *    the direction for the "move". If you do not specify a direction,
 *    the Cursor will remain on the current position.
 *    <ul>
 *      <li>@ref UPS_CURSOR_FIRST </li> positions the Cursor on the first
 *        item in the Database
 *      <li>@ref UPS_CURSOR_LAST </li> positions the Cursor on the last
 *        item in the Database
 *      <li>@ref UPS_CURSOR_NEXT </li> positions the Cursor on the next
 *        item in the Database; if the Cursor does not point to any
 *        item, the function behaves as if direction was
 *        @ref UPS_CURSOR_FIRST.
 *      <li>@ref UPS_CURSOR_PREVIOUS </li> positions the Cursor on the
 *        previous item in the Database; if the Cursor does not point to
 *        any item, the function behaves as if direction was
 *        @ref UPS_CURSOR_LAST.
 *      <li>@ref UPS_SKIP_DUPLICATES </li> skips duplicate keys of the
 *        current key. Not allowed in combination with
 *        @ref UPS_ONLY_DUPLICATES.
 *      <li>@ref UPS_ONLY_DUPLICATES </li> only move through duplicate keys
 *        of the current key. Not allowed in combination with
 *        @ref UPS_SKIP_DUPLICATES.
 *   </ul>
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if @a cursor is NULL, or if an invalid
 *        combination of flags was specified
 * @return @ref UPS_CURSOR_IS_NIL if the Cursor does not point to an item, but
 *        key and/or record were requested
 * @return @ref UPS_KEY_NOT_FOUND if @a cursor points to the first (or last)
 *        item, and a move to the previous (or next) item was
 *        requested
 * @return @ref UPS_TXN_CONFLICT if @ref UPS_CURSOR_FIRST or @ref
 *        UPS_CURSOR_LAST is specified but the first (or last) key or
 *        any of its duplicates is currently modified in an active
 *        Txn
 *
 * @sa UPS_RECORD_USER_ALLOC
 * @sa UPS_KEY_USER_ALLOC
 * @sa ups_record_t
 * @sa ups_key_t
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_cursor_move(ups_cursor_t *cursor, ups_key_t *key,
            ups_record_t *record, uint32_t flags);

/** Flag for @ref ups_cursor_move */
#define UPS_CURSOR_FIRST                0x0001

/** Flag for @ref ups_cursor_move */
#define UPS_CURSOR_LAST                 0x0002

/** Flag for @ref ups_cursor_move */
#define UPS_CURSOR_NEXT                 0x0004

/** Flag for @ref ups_cursor_move */
#define UPS_CURSOR_PREVIOUS             0x0008

/** Flag for @ref ups_cursor_move and @ref ups_db_count() */
#define UPS_SKIP_DUPLICATES             0x0010

/** Flag for @ref ups_cursor_move */
#define UPS_ONLY_DUPLICATES             0x0020

/**
 * Overwrites the current record
 *
 * This function overwrites the record of the current item.
 *
 * @param cursor A valid Cursor handle
 * @param record A valid record structure
 * @param flags Optional flags for overwriting the item; unused, set to 0
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if @a cursor or @a record is NULL
 * @return @ref UPS_CURSOR_IS_NIL if the Cursor does not point to an item
 * @return @ref UPS_TXN_CONFLICT if the same key was inserted in another
 *        Txn which was not yet committed or aborted
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_cursor_overwrite(ups_cursor_t *cursor, ups_record_t *record,
            uint32_t flags);

/**
 * Searches with a key and points the Cursor to the key found, retrieves
 * the located record
 *
 * Searches for an item in the Database and points the Cursor to this item.
 * If the item could not be found, the Cursor is not modified.
 *
 * Note that @ref ups_cursor_find can not search for duplicate keys. If @a key
 * has multiple duplicates, only the first duplicate is returned.
 *
 * When either or both @ref UPS_FIND_LT_MATCH and/or @ref UPS_FIND_GT_MATCH
 * have been specified as flags, the @a key structure will be overwritten
 * when an approximate match was found: the @a key and @a record
 * structures will then point at the located @a key (and @a record).
 * In this case the caller should ensure @a key points at a structure
 * which must adhere to the same restrictions and conditions as specified
 * for @ref ups_cursor_move(...,UPS_CURSOR_*):
 * key->data will point to temporary data upon return. This pointer
 * will be invalidated by subsequent upscaledb API calls using the same
 * Txn (or the same Database, if Transactions are disabled). See
 * @ref UPS_KEY_USER_ALLOC on how to change this behaviour.
 *
 * Further note that the @a key structure must be non-const at all times as its
 * internal flag bits may be written to.
 *
 * @param cursor A valid Cursor handle
 * @param key A pointer to a @ref ups_key_t structure. If this
 *    pointer is not NULL, the key of the new item is returned.
 *    Note that key->data will point to temporary data. This pointer
 *    will be invalidated by subsequent upscaledb API calls. See
 *    @a UPS_KEY_USER_ALLOC on how to change this behaviour.
 * @param record Optional pointer to a @ref ups_record_t structure. If this
 *    pointer is not NULL, the record of the new item is returned.
 *    Note that record->data will point to temporary data. This pointer
 *    will be invalidated by subsequent upscaledb API calls. See
 *    @ref UPS_RECORD_USER_ALLOC on how to change this behaviour.
 * @param flags Optional flags for searching, which can be combined with
 *    bitwise OR. Possible flags are:
 *    <ul>
 *    <li>@ref UPS_FIND_LT_MATCH </li> Cursor 'find' flag 'Less Than': the
 *        cursor is moved to point at the last record which' key
 *        is less than the specified key. When such a record cannot
 *        be located, an error is returned.
 *    <li>@ref UPS_FIND_GT_MATCH </li> Cursor 'find' flag 'Greater Than':
 *        the cursor is moved to point at the first record which' key is
 *        larger than the specified key. When such a record cannot be
 *        located, an error is returned.
 *    <li>@ref UPS_FIND_LEQ_MATCH </li> Cursor 'find' flag 'Less or EQual':
 *        the cursor is moved to point at the record which' key matches
 *        the specified key and when such a record is not available
 *        the cursor is moved to point at the last record which' key
 *        is less than the specified key. When such a record cannot be
 *        located, an error is returned.
 *    <li>@ref UPS_FIND_GEQ_MATCH </li> Cursor 'find' flag 'Greater or
 *        Equal': the cursor is moved to point at the record which' key
 *        matches the specified key and when such a record
 *        is not available the cursor is moved to point at the first
 *        record which' key is larger than the specified key.
 *        When such a record cannot be located, an error is returned.
 *    <li>@ref UPS_FIND_NEAR_MATCH </li> Cursor 'find' flag 'Any Near Or
 *        Equal': the cursor is moved to point at the record which'
 *        key matches the specified key and when such a record is
 *        not available the cursor is moved to point at either the
 *        last record which' key is less than the specified key or
 *        the first record which' key is larger than the specified
 *        key, whichever of these records is located first.
 *        When such records cannot be located, an error is returned.
 *    </ul>
 *
 * <b>Remark</b>
 * For Approximate Matching the returned match will either match the
 * key exactly or is either the first key available above or below the
 * given key when an exact match could not be found; 'find' does NOT
 * spend any effort, in the sense of determining which of both is the
 * 'nearest' to the given key, when both a key above and a key below the
 * one given exist; 'find' will simply return the first of both found.
 * As such, this flag is the simplest possible combination of the
 * combined @ref UPS_FIND_LEQ_MATCH and @ref UPS_FIND_GEQ_MATCH flags.
 *
 * Note that these flags may be bitwise OR-ed to form functional combinations.
 *
 * @ref UPS_FIND_LEQ_MATCH, @ref UPS_FIND_GEQ_MATCH and
 * @ref UPS_FIND_LT_MATCH, @ref UPS_FIND_GT_MATCH
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if @a db, @a key or @a record is NULL
 * @return @ref UPS_CURSOR_IS_NIL if the Cursor does not point to an item
 * @return @ref UPS_KEY_NOT_FOUND if no suitable @a key (record) exists
 * @return @ref UPS_TXN_CONFLICT if the same key was inserted in another
 *        Txn which was not yet committed or aborted
 *
 * @sa UPS_KEY_USER_ALLOC
 * @sa ups_key_t
 * @sa UPS_RECORD_USER_ALLOC
 * @sa ups_record_t
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_cursor_find(ups_cursor_t *cursor, ups_key_t *key,
            ups_record_t *record, uint32_t flags);

/* internal flag */
#define UPS_FIND_EQ_MATCH            0x4000

/**
 * Cursor 'find' flag 'Less Than': return the nearest match below the
 * given key, whether an exact match exists or not.
 */
#define UPS_FIND_LT_MATCH               0x1000

/**
 * Cursor 'find' flag 'Greater Than': return the nearest match above the
 * given key, whether an exact match exists or not.
 */
#define UPS_FIND_GT_MATCH               0x2000

/**
 * Cursor 'find' flag 'Less or EQual': return the nearest match below the
 * given key, when an exact match does not exist.
 *
 * May be combined with @ref UPS_FIND_GEQ_MATCH to accept any 'near' key, or
 * you can use the @ref UPS_FIND_NEAR_MATCH constant as a shorthand for that.
 */
#define UPS_FIND_LEQ_MATCH      (UPS_FIND_LT_MATCH | UPS_FIND_EQ_MATCH)

/**
 * Cursor 'find' flag 'Greater or Equal': return the nearest match above
 * the given key, when an exact match does not exist.
 *
 * May be combined with @ref UPS_FIND_LEQ_MATCH to accept any 'near' key,
 * or you can use the @ref UPS_FIND_NEAR_MATCH constant as a shorthand for that.
 */
#define UPS_FIND_GEQ_MATCH      (UPS_FIND_GT_MATCH | UPS_FIND_EQ_MATCH)

/**
 * Cursor 'find' flag 'Any Near Or Equal': return a match directly below or
 * above the given key, when an exact match does not exist.
 *
 * Be aware that the returned match will either match the key exactly or
 * is either the first key available above or below the given key when an
 * exact match could not be found; 'find' does NOT spend any effort, in the
 * sense of determining which of both is the 'nearest' to the given key,
 * when both a key above and a key below the one given exist; 'find' will
 * simply return the first of both found. As such, this flag is the simplest
 * possible combination of the combined @ref UPS_FIND_LEQ_MATCH and
 * @ref UPS_FIND_GEQ_MATCH flags.
 */
#define UPS_FIND_NEAR_MATCH     (UPS_FIND_LT_MATCH | UPS_FIND_GT_MATCH  \
                                  | UPS_FIND_EQ_MATCH)

/**
 * Inserts a Database item and points the Cursor to the inserted item
 *
 * This function inserts a key/record pair as a new Database item.
 * If the key already exists in the Database, error @ref UPS_DUPLICATE_KEY
 * is returned.
 *
 * If you wish to overwrite an existing entry specify the
 * flag @ref UPS_OVERWRITE. The use of this flag is not allowed in combination
 * with @ref UPS_DUPLICATE.
 *
 * If you wish to insert a duplicate key specify the flag @ref UPS_DUPLICATE.
 * (In order to use duplicate keys, the Database has to be created with
 * @ref UPS_ENABLE_DUPLICATE_KEYS.)
 * By default, the duplicate key is inserted after all other duplicate keys
 * (see @ref UPS_DUPLICATE_INSERT_LAST). This behaviour can be overwritten by
 * specifying @ref UPS_DUPLICATE_INSERT_FIRST, @ref UPS_DUPLICATE_INSERT_BEFORE
 * or @ref UPS_DUPLICATE_INSERT_AFTER.
 *
 * Specify the flag @ref UPS_HINT_APPEND if you insert sequential data
 * and the current @a key is greater than any other key in this Database.
 * In this case upscaledb will optimize the insert algorithm. upscaledb will
 * verify that this key is the greatest; if not, it will perform a normal
 * insert. This flag is the default for Record Number Databases.
 *
 * Specify the flag @ref UPS_HINT_PREPEND if you insert sequential data
 * and the current @a key is lower than any other key in this Database.
 * In this case upscaledb will optimize the insert algorithm. upscaledb will
 * verify that this key is the lowest; if not, it will perform a normal
 * insert.
 *
 * After inserting, the Cursor will point to the new item. If inserting
 * the item failed, the Cursor is not modified.
 *
 * Record Number Databases (created with @ref UPS_RECORD_NUMBER32 or
 * @ref UPS_RECORD_NUMBER64) expect either an empty @a key (with a size of
 * 0 and data pointing to NULL), or a user-supplied key (with key.flag
 * @ref UPS_KEY_USER_ALLOC and a valid data pointer).
 * If key.size is 0 and key.data is NULL, upscaledb will temporarily
 * allocate memory for key->data, which will then point to an 4-byte (or 8-byte)
 * unsigned integer.
 *
 * @param cursor A valid Cursor handle
 * @param key A valid key structure
 * @param record A valid record structure
 * @param flags Optional flags for inserting the item, combined with
 *    bitwise OR. Possible flags are:
 *    <ul>
 *    <li>@ref UPS_OVERWRITE. If the @a key already exists, the record is
 *        overwritten. Otherwise, the key is inserted. Not allowed in
 *        combination with @ref UPS_DUPLICATE.
 *    <li>@ref UPS_DUPLICATE. If the @a key already exists, a duplicate
 *        key is inserted. Same as @ref UPS_DUPLICATE_INSERT_LAST. Not
 *        allowed in combination with @ref UPS_DUPLICATE.
 *    <li>@ref UPS_DUPLICATE_INSERT_BEFORE. If the @a key already exists,
 *        a duplicate key is inserted before the duplicate pointed
 *        to by the Cursor
 *    <li>@ref UPS_DUPLICATE_INSERT_AFTER. If the @a key already exists,
 *        a duplicate key is inserted after the duplicate pointed
 *        to by the Cursor
 *    <li>@ref UPS_DUPLICATE_INSERT_FIRST. If the @a key already exists,
 *        a duplicate key is inserted as the first duplicate of
 *        the current key
 *    <li>@ref UPS_DUPLICATE_INSERT_LAST. If the @a key already exists,
 *        a duplicate key is inserted as the last duplicate of
 *        the current key
 *    <li>@ref UPS_HINT_APPEND. Hints the upscaledb engine that the
 *        current key will compare as @e larger than any key already
 *        existing in the Database. The upscaledb engine will verify
 *        this postulation and when found not to be true, will revert
 *        to a regular insert operation as if this flag was not
 *        specified. The incurred cost then is only one additional key
 *        comparison. Mutually exclusive with flag @ref UPS_HINT_PREPEND.
 *        This is the default for Record Number Databases.
 *    <li>@ref UPS_HINT_PREPEND. Hints the upscaledb engine that the
 *        current key will compare as @e lower than any key already
 *        existing in the Database. The upscaledb engine will verify
 *        this postulation and when found not to be true, will revert
 *        to a regular insert operation as if this flag was not
 *        specified. The incurred cost then is only one additional key
 *        comparison. Mutually exclusive with flag @ref UPS_HINT_APPEND.
 *    </ul>
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if @a key or @a record is NULL
 * @return @ref UPS_INV_PARAMETER if the Database is a Record Number Database
 *        and the key is invalid (see above)
 * @return @ref UPS_INV_PARAMETER if the flags @ref UPS_OVERWRITE <b>and</b>
 *        @ref UPS_DUPLICATE were specified, or if @ref UPS_DUPLICATE
 *        was specified, but the Database was not created with
 *        flag @ref UPS_ENABLE_DUPLICATE_KEYS.
 * @return @ref UPS_WRITE_PROTECTED if you tried to insert a key to a read-only
 *        Database.
 * @return @ref UPS_INV_KEY_SIZE if the key size is different from
 *        the one specified with @a UPS_PARAM_KEY_SIZE
 * @return @ref UPS_INV_RECORD_SIZE if the record size is different from
 *        the one specified with @a UPS_PARAM_RECORD_SIZE
 * @return @ref UPS_CURSOR_IS_NIL if the Cursor does not point to an item
 * @return @ref UPS_TXN_CONFLICT if the same key was inserted in another
 *        Txn which was not yet committed or aborted
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_cursor_insert(ups_cursor_t *cursor, ups_key_t *key,
            ups_record_t *record, uint32_t flags);

/**
 * Erases the current key
 *
 * Erases a key from the Database. If the erase was
 * successful, the Cursor is invalidated and does no longer point to
 * any item. In case of an error, the Cursor is not modified.
 *
 * If the Database was opened with the flag @ref UPS_ENABLE_DUPLICATE_KEYS,
 * this function erases only the duplicate item to which the Cursor refers.
 *
 * @param cursor A valid Cursor handle
 * @param flags Unused, set to 0
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_INV_PARAMETER if @a cursor is NULL
 * @return @ref UPS_WRITE_PROTECTED if you tried to erase a key from a read-only
 *        Database
 * @return @ref UPS_CURSOR_IS_NIL if the Cursor does not point to an item
 * @return @ref UPS_TXN_CONFLICT if the same key was inserted in another
 *        Txn which was not yet committed or aborted
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_cursor_erase(ups_cursor_t *cursor, uint32_t flags);

/**
 * Returns the number of duplicate keys
 *
 * Returns the number of duplicate keys of the item to which the
 * Cursor currently refers.
 * Returns 1 if the key has no duplicates.
 *
 * @param cursor A valid Cursor handle
 * @param count Returns the number of duplicate keys
 * @param flags Optional flags; unused, set to 0.
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_CURSOR_IS_NIL if the Cursor does not point to an item
 * @return @ref UPS_INV_PARAMETER if @a cursor or @a count is NULL
 * @return @ref UPS_TXN_CONFLICT if the same key was inserted in another
 *        Txn which was not yet committed or aborted
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_cursor_get_duplicate_count(ups_cursor_t *cursor,
            uint32_t *count, uint32_t flags);

/**
 * Returns the current cursor position in the duplicate list
 *
 * Returns the position in the duplicate list of the current key. The position
 * is 0-based.
 *
 * @param cursor A valid Cursor handle
 * @param position Returns the duplicate position
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_CURSOR_IS_NIL if the Cursor does not point to an item
 * @return @ref UPS_INV_PARAMETER if @a cursor or @a position is NULL
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_cursor_get_duplicate_position(ups_cursor_t *cursor,
            uint32_t *position);

/**
 * Returns the record size of the current key
 *
 * Returns the record size of the item to which the Cursor currently refers.
 *
 * @param cursor A valid Cursor handle
 * @param size Returns the record size, in bytes
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_CURSOR_IS_NIL if the Cursor does not point to an item
 * @return @ref UPS_INV_PARAMETER if @a cursor or @a size is NULL
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_cursor_get_record_size(ups_cursor_t *cursor, uint32_t *size);

/**
 * Closes a Database Cursor
 *
 * Closes a Cursor and frees allocated memory. All Cursors
 * should be closed before closing the Database (see @ref ups_db_close).
 *
 * @param cursor A valid Cursor handle
 *
 * @return @ref UPS_SUCCESS upon success
 * @return @ref UPS_CURSOR_IS_NIL if the Cursor does not point to an item
 * @return @ref UPS_INV_PARAMETER if @a cursor is NULL
 *
 * @sa ups_db_close
 */
UPS_EXPORT ups_status_t UPS_CALLCONV
ups_cursor_close(ups_cursor_t *cursor);

/**
 * @}
 */

#ifdef __cplusplus
} // extern "C"
#endif

#endif /* UPS_UPSCALEDB_H */
