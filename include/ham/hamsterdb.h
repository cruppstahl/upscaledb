/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @file hamsterdb.h
 * @brief Include file for hamsterdb Embedded Storage
 * @author Christoph Rupp, chris@crupp.de
 * @version 2.1.10
 *
 * @mainpage
 *
 * This manual documents the hamsterdb C API. hamsterdb is a key/value database
 * that is linked directly into your application, avoiding all the overhead
 * that is related to external databases and RDBMS systems.
 *
 * This header file declares all functions and macros that are needed to use
 * hamsterdb. The comments are formatted in Doxygen style and can be extracted
 * to automagically generate documentation. The documentation is also available
 * online here: <a href="http://hamsterdb.com/public/scripts/html_www">
  http://hamsterdb.com/public/scripts/html_www</a>.
 *
 * In addition, there's a tutorial book hosted on github:
 * <a href="http://github.com/cruppstahl/hamsterdb/wiki/Tutorial">
  http://github.com/cruppstahl/hamsterdb/wiki/Tutorial</a>.
 *
 * If you want to create or open Databases or Environments (a collection of
 * multiple Databases), the following functions will be interesting for you:
 * <table>
 * <tr><td>@ref ham_env_create</td><td>Creates an Environment</td></tr>
 * <tr><td>@ref ham_env_open</td><td>Opens an Environment</td></tr>
 * <tr><td>@ref ham_env_close</td><td>Closes an Environment</td></tr>
 * <tr><td>@ref ham_env_create_db</td><td>Creates a Database in an
  Environment</td></tr>
 * <tr><td>@ref ham_env_open_db</td><td>Opens a Database from an
  Environment</td></tr>
 * <tr><td>@ref ham_db_close</td><td>Closes a Database</td></tr>
 * </table>
 *
 * To insert, lookup or delete key/value pairs, the following functions are
 * used:
 * <table>
 * <tr><td>@ref ham_db_insert</td><td>Inserts a key/value pair into a
  Database</td></tr>
 * <tr><td>@ref ham_db_find</td><td>Lookup of a key/value pair in a
  Database</td></tr>
 * <tr><td>@ref ham_db_erase</td><td>Erases a key/value pair from a
  Database</td></tr>
 * </table>
 *
 * Alternatively, you can use Cursors to iterate over a Database:
 * <table>
 * <tr><td>@ref ham_cursor_create</td><td>Creates a new Cursor</td></tr>
 * <tr><td>@ref ham_cursor_find</td><td>Positions the Cursor on a key</td></tr>
 * <tr><td>@ref ham_cursor_insert</td><td>Inserts a new key/value pair with a
  Cursor</td></tr>
 * <tr><td>@ref ham_cursor_erase</td><td>Deletes the key/value pair that
  the Cursor points to</td></tr>
 * <tr><td>@ref ham_cursor_overwrite</td><td>Overwrites the value of the current  key</td></tr>
 * <tr><td>@ref ham_cursor_move</td><td>Moves the Cursor to the first, next,
  previous or last key in the Database</td></tr>
 * <tr><td>@ref ham_cursor_close</td><td>Closes the Cursor</td></tr>
 * </table>
 *
 * If you want to use Transactions, then the following functions are required:
 * <table>
 * <tr><td>@ref ham_txn_begin</td><td>Begins a new Transaction</td></tr>
 * <tr><td>@ref ham_txn_commit</td><td>Commits the current
  Transaction</td></tr>
 * <tr><td>@ref ham_txn_abort</td><td>Aborts the current Transaction</td></tr>
 * </table>
 *
 * hamsterdb supports remote Databases. The server can be embedded
 * into your application or run standalone (see tools/hamzilla for a Unix
 * daemon or Win32 service which hosts Databases). If you want to embed the
 * server then the following functions have to be used:
 * <table>
 * <tr><td>@ref ham_srv_init</td><td>Initializes the server</td></tr>
 * <tr><td>@ref ham_srv_add_env</td><td>Adds an Environment to the
  server. The Environment with all its Databases will then be available
  remotely.</td></tr>
 * <tr><td>@ref ham_srv_close</td><td>Closes the server and frees all allocated
  resources</td></tr>
 * </table>
 *
 * If you need help then you're always welcome to use the <a
  href="https://groups.google.com/forum/?fromgroups#!forum/hamsterdb-user">
    mailing list</a>,
 * drop a message (chris at crupp dot de) or use the <a
  href="http://hamsterdb.com/index/contact">contact form</a>.
 *
 * Have fun!
 */

#ifndef HAM_HAMSTERDB_H
#define HAM_HAMSTERDB_H

#include <ham/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/* deprecated */
#define HAM_API_REVISION        3

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
 * hamsterdb is neither backwards- nor forwards-compatible regarding file
 * format changes. 
 *
 * If a file was created with hamsterdb pro then the msb of the file version
 * is set. hamsterdb pro is able to open files created with hamsterdb (APL
 * version), but not vice versa.
 *
 * History of file versions:
 *   2.1.0: introduced the file version; version is 0
 *   2.1.3: new btree format, file format cleanups; version is 1
 *   2.1.4: new btree format for duplicate keys/var. length keys; version is 2
 *   2.1.5: new freelist; version is 3
 *   2.1.10: changes in btree node format; version is 4
 */
#define HAM_VERSION_MAJ     2
#define HAM_VERSION_MIN     1
#define HAM_VERSION_REV     10
#define HAM_FILE_VERSION    4

/**
 * The hamsterdb Database structure
 *
 * This structure is allocated in @ref ham_env_create_db and
 * @ref ham_env_open_db. It is deleted in @a ham_db_close.
 */
struct ham_db_t;
typedef struct ham_db_t ham_db_t;

/**
 * The hamsterdb Environment structure
 *
 * This structure is allocated with @ref ham_env_create and @ref ham_env_open
 * and is deleted in @ref ham_env_close.
 */
struct ham_env_t;
typedef struct ham_env_t ham_env_t;

/**
 * A Database Cursor
 *
 * A Cursor is used for bi-directionally traversing the Database and
 * for inserting/deleting/searching Database items.
 *
 * This structure is allocated with @ref ham_cursor_create and deleted with
 * @ref ham_cursor_close.
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
 * overwritten by subsequent hamsterdb API calls using the same Transaction
 * (or, if Transactions are disabled, using the same Database). The pointer
 * will also be invalidated after the Transaction is aborted or committed.
 *
 * To avoid this, the calling application can allocate the @a data pointer.
 * In this case, you have to set the flag @ref HAM_RECORD_USER_ALLOC. The
 * @a size parameter will then return the size of the record. It's the
 * responsibility of the caller to make sure that the @a data parameter is
 * large enough for the record.
 *
 * The record->data pointer is not threadsafe. For threadsafe access it is
 * recommended to use @a HAM_RECORD_USER_ALLOC or have each thread manage its
 * own Transaction.
 */
typedef struct {
  /** The size of the record data, in bytes */
  uint32_t size;

  /** Pointer to the record data */
  void *data;

  /** The record flags; see @ref HAM_RECORD_USER_ALLOC */
  uint32_t flags;

  /** Offset for partial reading/writing; see @ref HAM_PARTIAL */
  uint32_t partial_offset;

  /** Size for partial reading/writing; see @ref HAM_PARTIAL */
  uint32_t partial_size;

} ham_record_t;

/** Flag for @ref ham_record_t (only really useful in combination with
 * @ref ham_cursor_move, @ref ham_cursor_find and @ref ham_db_find)
 */
#define HAM_RECORD_USER_ALLOC   1

/**
 * A macro to statically initialize a @ref ham_record_t structure.
 *
 * Usage:
 *    ham_record_t rec = ham_make_record(ptr, size);
 */
#define ham_make_record(PTR, SIZE) { SIZE, PTR, 0 }

/**
 * A generic key.
 *
 * A key represents key items in hamsterdb. Before using a key, it
 * is important to initialize all key fields with zeroes, i.e. with
 * the C library routines memset(3) or bzero(2).
 *
 * hamsterdb usually uses keys to insert, delete or search for items.
 * However, when using Database Cursors and the function @ref ham_cursor_move,
 * hamsterdb also returns keys. In this case, the pointer to the key
 * data is provided in @a data. This pointer is only temporary and will be
 * overwritten by subsequent calls to @ref ham_cursor_move using the
 * same Transaction (or, if Transactions are disabled, using the same Database).
 * The pointer will also be invalidated after the Transaction is aborted
 * or committed.
 *
 * To avoid this, the calling application can allocate the @a data pointer.
 * In this case, you have to set the flag @ref HAM_KEY_USER_ALLOC. The
 * @a size parameter will then return the size of the key. It's the
 * responsibility of the caller to make sure that the @a data parameter is
 * large enough for the key.
 *
 * The key->data pointer is not threadsafe. For threadsafe access it is
 * recommended to use @a HAM_KEY_USER_ALLOC or have each thread manage its
 * own Transaction.
 */
typedef struct {
  /** The size of the key, in bytes */
  uint16_t size;

  /** The data of the key */
  void *data;

  /** The key flags; see @ref HAM_KEY_USER_ALLOC */
  uint32_t flags;

  /** For internal use */
  uint32_t _flags;

} ham_key_t;

/**
 * A macro to statically initialize a @ref ham_key_t structure.
 *
 * Usage:
 *    ham_key_t key = ham_make_key(ptr, size);
 */
#define ham_make_key(PTR, SIZE) { SIZE, PTR, 0 }

/** Flag for @ref ham_key_t (only really useful in combination with
 * @ref ham_cursor_move, @ref ham_cursor_find and @ref ham_db_find)
 */
#define HAM_KEY_USER_ALLOC    1

/**
 * A named parameter.
 *
 * These parameter structures are used for functions like @ref ham_env_open,
 * @ref ham_env_create, etc. to pass variable length parameter lists.
 *
 * The lists are always arrays of type ham_parameter_t, with a terminating
 * element of { 0, NULL}, e.g.
 *
 * <pre>
 *   ham_parameter_t parameters[] = {
 *    { HAM_PARAM_CACHE_SIZE, 2 * 1024 * 1024 }, // set cache size to 2 mb
 *    { HAM_PARAM_PAGE_SIZE, 4096 }, // set page size to 4 kb
 *    { 0, NULL }
 *   };
 * </pre>
 */
typedef struct {
  /** The name of the parameter; all HAM_PARAM_*-constants */
  uint32_t name;

  /** The value of the parameter. */
  uint64_t value;

} ham_parameter_t;


/**
 * @defgroup ham_key_types hamsterdb Key Types
 * @{
 */

/** A binary blob without type; sorted by memcmp */
#define HAM_TYPE_BINARY                      0
/** A binary blob without type; sorted by callback function */
#define HAM_TYPE_CUSTOM                      1
/** An unsigned 8-bit integer */
#define HAM_TYPE_UINT8                       3
/** An unsigned 16-bit integer */
#define HAM_TYPE_UINT16                      5
/** An unsigned 32-bit integer */
#define HAM_TYPE_UINT32                      7
/** An unsigned 64-bit integer */
#define HAM_TYPE_UINT64                      9
/** An 32-bit float */
#define HAM_TYPE_REAL32                     11
/** An 64-bit double */
#define HAM_TYPE_REAL64                     12

/**
 * @}
 */


/**
 * @defgroup ham_status_codes hamsterdb Status Codes
 * @{
 */

/** Operation completed successfully */
#define HAM_SUCCESS                     (  0)
/** Invalid record size */
#define HAM_INV_RECORD_SIZE             ( -2)
/** Invalid key size */
#define HAM_INV_KEY_SIZE                ( -3)
/* deprecated */
#define HAM_INV_KEYSIZE                 HAM_INV_KEY_SIZE
/** Invalid page size (must be 1024 or a multiple of 2048) */
#define HAM_INV_PAGE_SIZE               ( -4)
/* deprecated */
#define HAM_INV_PAGESIZE                HAM_INV_PAGE_SIZE
/** Memory allocation failed - out of memory */
#define HAM_OUT_OF_MEMORY               ( -6)
/** Invalid function parameter */
#define HAM_INV_PARAMETER               ( -8)
/** Invalid file header */
#define HAM_INV_FILE_HEADER             ( -9)
/** Invalid file version */
#define HAM_INV_FILE_VERSION            (-10)
/** Key was not found */
#define HAM_KEY_NOT_FOUND               (-11)
/** Tried to insert a key which already exists */
#define HAM_DUPLICATE_KEY               (-12)
/** Internal Database integrity violated */
#define HAM_INTEGRITY_VIOLATED          (-13)
/** Internal hamsterdb error */
#define HAM_INTERNAL_ERROR              (-14)
/** Tried to modify the Database, but the file was opened as read-only */
#define HAM_WRITE_PROTECTED             (-15)
/** Database record not found */
#define HAM_BLOB_NOT_FOUND              (-16)
/** Generic file I/O error */
#define HAM_IO_ERROR                    (-18)
/** Function is not yet implemented */
#define HAM_NOT_IMPLEMENTED             (-20)
/** File not found */
#define HAM_FILE_NOT_FOUND              (-21)
/** Operation would block */
#define HAM_WOULD_BLOCK                 (-22)
/** Object was not initialized correctly */
#define HAM_NOT_READY                   (-23)
/** Database limits reached */
#define HAM_LIMITS_REACHED              (-24)
/** Object was already initialized */
#define HAM_ALREADY_INITIALIZED         (-27)
/** Database needs recovery */
#define HAM_NEED_RECOVERY               (-28)
/** Cursor must be closed prior to Transaction abort/commit */
#define HAM_CURSOR_STILL_OPEN           (-29)
/** Record filter or file filter not found */
#define HAM_FILTER_NOT_FOUND            (-30)
/** Operation conflicts with another Transaction */
#define HAM_TXN_CONFLICT                (-31)
/* internal use: key was erased in a Transaction */
#define HAM_KEY_ERASED_IN_TXN           (-32)
/** Database cannot be closed because it is modified in a Transaction */
#define HAM_TXN_STILL_OPEN              (-33)
/** Cursor does not point to a valid item */
#define HAM_CURSOR_IS_NIL               (-100)
/** Database not found */
#define HAM_DATABASE_NOT_FOUND          (-200)
/** Database name already exists */
#define HAM_DATABASE_ALREADY_EXISTS     (-201)
/** Database already open, or: Database handle is already initialized */
#define HAM_DATABASE_ALREADY_OPEN       (-202)
/** Environment already open, or: Environment handle is already initialized */
#define HAM_ENVIRONMENT_ALREADY_OPEN    (-203)
/** Invalid log file header */
#define HAM_LOG_INV_FILE_HEADER         (-300)
/** Remote I/O error/Network error */
#define HAM_NETWORK_ERROR               (-400)

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
 * This error handler can be used in combination with
 * @ref ham_set_errhandler().
 *
 * @param message The error message
 * @param level The error level:
 *    <ul>
 *     <li>@ref HAM_DEBUG_LEVEL_DEBUG (0) </li> a debug message
 *     <li>@ref HAM_DEBUG_LEVEL_NORMAL (1) </li> a normal error message
 *     <li>2</li> reserved
 *     <li>@ref HAM_DEBUG_LEVEL_FATAL (3) </li> a fatal error message
 *    </ul>
 *
 * @sa error_levels
 */
typedef void HAM_CALLCONV (*ham_errhandler_fun)(int level, const char *message);

/** A debug message */
#define HAM_DEBUG_LEVEL_DEBUG     0

/** A normal error message */
#define HAM_DEBUG_LEVEL_NORMAL    1

/** A fatal error message */
#define HAM_DEBUG_LEVEL_FATAL     3

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
 *      the default handler
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
 * @param major If not NULL, will return the major version number
 * @param minor If not NULL, will return the minor version number
 * @param revision If not NULL, will return the revision version number
 */
HAM_EXPORT void HAM_CALLCONV
ham_get_version(uint32_t *major, uint32_t *minor,
            uint32_t *revision);

/**
 * @}
 */


/**
 * @defgroup ham_env hamsterdb Environment Functions
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
 * Databases in an Environment can be created with @ref ham_env_create_db
 * or opened with @ref ham_env_open_db.
 *
 * Specify a URL instead of a filename (i.e.
 * "ham://localhost:8080/customers.db") to access a remote hamsterdb Server.
 *
 * To enable ACID Transactions, supply the flag @ref HAM_ENABLE_TRANSACTIONS.
 * By default, hamsterdb will use a Journal for recovering the Environment
 * and its data in case of a crash, and also to re-apply committed Transactions
 * which were not yet flushed to disk. This Journalling can be disabled
 * with the flag @ref HAM_DISABLE_RECOVERY. (It is disabled if the Environment
 * is in-memory.)
 *
 * If Transactions are not required, but hamsterdb should still be able to
 * recover in case of a crash or power outage, then the flag
 * @ref HAM_ENABLE_RECOVERY will enable the Journal (without allowing
 * Transactions.)
 *
 * For performance reasons the Journal does not use fsync(2) (or
 * FlushFileBuffers on Win32) to flush modified buffers to disk. Use the flag
 * @ref HAM_ENABLE_FSYNC to force the use of fsync.
 *
 * @param env A pointer to an Environment handle
 * @param filename The filename of the Environment file. If the file already
 *      exists, it is overwritten. Can be NULL for an In-Memory
 *      Environment. Can be a URL ("ham://<hostname>:<port>/<environment>")
 *      for remote access.
 * @param flags Optional flags for opening the Environment, combined with
 *      bitwise OR. Possible flags are:
 *    <ul>
 *     <li>@ref HAM_ENABLE_FSYNC</li> Flushes all file handles after
 *      committing or aborting a Transaction using fsync(), fdatasync()
 *      or FlushFileBuffers(). This file has no effect
 *      if Transactions are disabled. Slows down performance but makes
 *      sure that all file handles and operating system caches are
 *      transferred to disk, thus providing a stronger durability.
 *     <li>@ref HAM_IN_MEMORY</li> Creates an In-Memory Environment. No
 *      file will be created, and the Database contents are lost after
 *      the Environment is closed. The @a filename parameter can
 *      be NULL. Do <b>NOT</b> specify @a cache_size other than 0.
 *     <li>@ref HAM_DISABLE_MMAP</li> Do not use memory mapped files for I/O.
 *      By default, hamsterdb checks if it can use mmap,
 *      since mmap is faster than read/write. For performance
 *      reasons, this flag should not be used.
 *     <li>@ref HAM_CACHE_UNLIMITED</li> Do not limit the cache. Nearly as
 *      fast as an In-Memory Database. Not allowed in combination
 *      with a limited cache size.
 *     <li>@ref HAM_ENABLE_TRANSACTIONS</li> Enables Transactions for this
 *      Environment. This flag implies @ref HAM_ENABLE_RECOVERY.
 *     <li>@ref HAM_ENABLE_RECOVERY</li> Enables logging/recovery for this
 *      Environment. Not allowed in combination with @ref HAM_IN_MEMORY.
 *     <li>@ref HAM_DISABLE_RECOVERY</li> Disables logging/recovery for this
 *      Environment.
 *     <li>@ref HAM_FLUSH_WHEN_COMMITTED</li> Immediately flushes committed
 *      Transactions and writes them to the Btree. Disabled by default. If
 *      disabled then hamsterdb buffers committed Transactions and only starts
 *      flushing when too many Transactions were committed.  
 *    </ul>
 *
 * @param mode File access rights for the new file. This is the @a mode
 *      parameter for creat(2). Ignored on Microsoft Windows. Default
 *      is 0644.
 * @param param An array of ham_parameter_t structures. The following
 *      parameters are available:
 *    <ul>
 *    <li>@ref HAM_PARAM_CACHE_SIZE</li> The size of the Database cache,
 *      in bytes. The default size is defined in src/config.h
 *      as @a HAM_DEFAULT_CACHE_SIZE - usually 2MB
 *    <li>@ref HAM_PARAM_POSIX_FADVISE</li> Sets the "advice" for
 *      posix_fadvise(). Only on supported platforms. Allowed values are
 *      @ref HAM_POSIX_FADVICE_NORMAL (which is the default) or
 *      @ref HAM_POSIX_FADVICE_RANDOM.
 *    <li>@ref HAM_PARAM_PAGE_SIZE</li> The size of a file page, in
 *      bytes. It is recommended not to change the default size. The
 *      default size depends on hardware and operating system.
 *      Page sizes must be 1024 or a multiple of 2048.
 *    <li>@ref HAM_PARAM_FILE_SIZE_LIMIT</li> Sets a file size limit (in bytes).
 *      Disabled by default. Not allowed in combination with @ref HAM_IN_MEMORY.
 *      If the limit is exceeded, API functions return @ref HAM_LIMITS_REACHED.
 *    <li>@ref HAM_PARAM_LOG_DIRECTORY</li> The path of the log file
 *      and the journal files; default is the same path as the database
 *      file. Ignored for remote Environments.
 *    <li>@ref HAM_PARAM_NETWORK_TIMEOUT_SEC</li> Timeout (in seconds) when
 *      waiting for data from a remote server. By default, no timeout is set.
 *    </ul>
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if the @a env pointer is NULL or an
 *        invalid combination of flags or parameters was specified
 * @return @ref HAM_IO_ERROR if the file could not be opened or
 *        reading/writing failed
 * @return @ref HAM_INV_FILE_VERSION if the Environment version is not
 *        compatible with the library version
 * @return @ref HAM_OUT_OF_MEMORY if memory could not be allocated
 * @return @ref HAM_INV_PAGE_SIZE if @a page_size is not 1024 or
 *        a multiple of 2048
 * @return @ref HAM_INV_KEY_SIZE if @a key_size is too large (at least 4
 *        keys must fit in a page)
 * @return @ref HAM_WOULD_BLOCK if another process has locked the file
 * @return @ref HAM_ENVIRONMENT_ALREADY_OPEN if @a env is already in use
 *
 * @sa ham_env_create
 * @sa ham_env_close
 * @sa ham_env_open
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_create(ham_env_t **env, const char *filename,
            uint32_t flags, uint32_t mode, const ham_parameter_t *param);

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
 * Databases in an Environment can be created with @ref ham_env_create_db
 * or opened with @ref ham_env_open_db.
 *
 * Specify a URL instead of a filename (i.e.
 * "ham://localhost:8080/customers.db") to access a remote hamsterdb Server.
 *
 * Also see the documentation @ref ham_env_create about Transactions, Recovery
 * and the use of fsync.
 *
 * @param env A valid Environment handle
 * @param filename The filename of the Environment file, or URL of a hamsterdb
 *      Server
 * @param flags Optional flags for opening the Environment, combined with
 *      bitwise OR. Possible flags are:
 *    <ul>
 *     <li>@ref HAM_READ_ONLY </li> Opens the file for reading only.
 *      Operations that need write access (i.e. @ref ham_db_insert) will
 *      return @ref HAM_WRITE_PROTECTED.
 *     <li>@ref HAM_ENABLE_FSYNC</li> Flushes all file handles after
 *      committing or aborting a Transaction using fsync(), fdatasync()
 *      or FlushFileBuffers(). This file has no effect
 *      if Transactions are disabled. Slows down performance but makes
 *      sure that all file handles and operating system caches are
 *      transferred to disk, thus providing a stronger durability.
 *     <li>@ref HAM_DISABLE_MMAP </li> Do not use memory mapped files for I/O.
 *      By default, hamsterdb checks if it can use mmap,
 *      since mmap is faster than read/write. For performance
 *      reasons, this flag should not be used.
 *     <li>@ref HAM_CACHE_UNLIMITED </li> Do not limit the cache. Nearly as
 *      fast as an In-Memory Database. Not allowed in combination
 *      with a limited cache size.
 *     <li>@ref HAM_ENABLE_TRANSACTIONS </li> Enables Transactions for this
 *      Environment. This flag imples @ref HAM_ENABLE_RECOVERY.
 *     <li>@ref HAM_ENABLE_RECOVERY </li> Enables logging/recovery for this
 *      Environment. Will return @ref HAM_NEED_RECOVERY, if the Environment
 *      is in an inconsistent state. Not allowed in combination
 *      with @ref HAM_IN_MEMORY.
 *     <li>@ref HAM_DISABLE_RECOVERY</li> Disables logging/recovery for this
 *      Environment.
 *     <li>@ref HAM_AUTO_RECOVERY </li> Automatically recover the Environment,
 *      if necessary. This flag implies @ref HAM_ENABLE_RECOVERY.
 *     <li>@ref HAM_FLUSH_WHEN_COMMITTED</li> Immediately flushes committed
 *      Transactions and writes them to the Btree. Disabled by default. If
 *      disabled then hamsterdb buffers committed Transactions and only starts
 *      flushing when too many Transactions were committed.  
 *    </ul>
 * @param param An array of ham_parameter_t structures. The following
 *      parameters are available:
 *    <ul>
 *    <li>@ref HAM_PARAM_CACHE_SIZE </li> The size of the Database cache,
 *      in bytes. The default size is defined in src/config.h
 *      as @a HAM_DEFAULT_CACHE_SIZE - usually 2MB
 *    <li>@ref HAM_PARAM_POSIX_FADVISE</li> Sets the "advice" for
 *      posix_fadvise(). Only on supported platforms. Allowed values are
 *      @ref HAM_POSIX_FADVICE_NORMAL (which is the default) or
 *      @ref HAM_POSIX_FADVICE_RANDOM.
 *    <li>@ref HAM_PARAM_FILE_SIZE_LIMIT</li> Sets a file size limit (in bytes).
 *      Disabled by default. If the limit is exceeded, API functions
 *      return @ref HAM_LIMITS_REACHED.
 *    <li>@ref HAM_PARAM_LOG_DIRECTORY</li> The path of the log file
 *      and the journal files; default is the same path as the database
 *      file. Ignored for remote Environments.
 *    <li>@ref HAM_PARAM_NETWORK_TIMEOUT_SEC</li> Timeout (in seconds) when
 *      waiting for data from a remote server. By default, no timeout is set.
 *    </ul>
 *
 * @return @ref HAM_SUCCESS upon success.
 * @return @ref HAM_INV_PARAMETER if the @a env pointer is NULL, an
 *        invalid combination of flags was specified
 * @return @ref HAM_FILE_NOT_FOUND if the file does not exist
 * @return @ref HAM_IO_ERROR if the file could not be opened or reading failed
 * @return @ref HAM_INV_FILE_VERSION if the Environment version is not
 *        compatible with the library version.
 * @return @ref HAM_OUT_OF_MEMORY if memory could not be allocated
 * @return @ref HAM_WOULD_BLOCK if another process has locked the file
 * @return @ref HAM_NEED_RECOVERY if the Database is in an inconsistent state
 * @return @ref HAM_LOG_INV_FILE_HEADER if the logfile is corrupt
 * @return @ref HAM_ENVIRONMENT_ALREADY_OPEN if @a env is already in use
 * @return @ref HAM_NETWORK_ERROR if a remote server is not reachable
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_open(ham_env_t **env, const char *filename,
            uint32_t flags, const ham_parameter_t *param);

/**
 * Retrieve the current value for a given Environment setting
 *
 * Only those values requested by the parameter array will be stored.
 *
 * The following parameters are supported:
 *    <ul>
 *    <li>HAM_PARAM_CACHE_SIZE</li> returns the cache size
 *    <li>HAM_PARAM_PAGE_SIZE</li> returns the page size
 *    <li>HAM_PARAM_MAX_DATABASES</li> returns the max. number of
 *        Databases of this Database's Environment
 *    <li>HAM_PARAM_FLAGS</li> returns the flags which were used to
 *        open or create this Database
 *    <li>HAM_PARAM_FILEMODE</li> returns the @a mode parameter which
 *        was specified when creating this Database
 *    <li>HAM_PARAM_FILENAME</li> returns the filename (the @a value
 *        of this parameter is a const char * pointer casted to a
 *        uint64_t variable)
 *    <li>@ref HAM_PARAM_LOG_DIRECTORY</li> The path of the log file
 *        and the journal files. Ignored for remote Environments.
 *    <li>@ref HAM_PARAM_JOURNAL_COMPRESSION</li> Returns the
 *        selected algorithm for journal compression, or 0 if compression
 *        is disabled
 *    </ul>
 *
 * @param env A valid Environment handle
 * @param param An array of ham_parameter_t structures
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if the @a env pointer is NULL or
 *        @a param is NULL
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_get_parameters(ham_env_t *env, ham_parameter_t *param);

/**
 * Creates a new Database in a Database Environment
 *
 * An Environment can contain a (limited) amount of Databases; the exact
 * limit depends on the page size and is above 600.
 *
 * Each Database in an Environment is identified by a positive 16bit
 * value. 0 and values at or above 0xf000 are reserved.
 *
 * This function initializes the ham_db_t handle (the second parameter).
 * When the handle is no longer in use, it should be closed with
 * @ref ham_db_close. Alternatively, the Database handle is closed
 * automatically if @ref ham_env_close is called with the flag
 * @ref HAM_AUTO_CLEANUP.
 *
 * A Database can (and should) be configured and optimized for the data that
 * is inserted. The data is described through flags and parameters. hamsterdb
 * differentiates between several data characteristics, and offers predefined
 * "types" to describe the keys. In general, the default key type
 * (@ref HAM_TYPE_BINARY) is slower than the other types, and
 * fixed-length binary keys (@ref HAM_TYPE_BINARY in combination with
 * @ref HAM_PARAM_KEY_SIZE) is faster than variable-length binary
 * keys. It is therefore recommended to always set the key size and record size,
 * although it is not required.
 *
 * Internally, hamsterdb uses two different layouts ("default" and "pax)
 * depending on the settings specified by the user. The "default" layout
 * is enabled for variable-length keys or if duplicate keys are enabled.
 * For fixed-length keys (without duplicates) the "pax" layout is chosen.
 * The "pax" layout is more compact and usually faster.
 *
 * A word of warning regarding the use of fixed length binary keys
 * (@ref HAM_TYPE_CUSTOM or @ref HAM_TYPE_BINARY in combination with
 * @ref HAM_PARAM_KEY_SIZE): if your key size is too large, only few keys
 * will fit in a Btree node. The Btree fanout will be very high, which will
 * decrease performance. In such cases it might be better to NOT specify
 * the key size; then hamsterdb will store keys as blobs if they are too large.
 *
 * See the Wiki documentation for <a href=
   "https://github.com/cruppstahl/hamsterdb/wiki/Evaluating-and-Benchmarking">
 * Evaluating and Benchmarking</a> on how to test different configurations and
 * optimize for performance.
 *
 * The key type is set with @ref HAM_PARAM_KEY_TYPE and can have either
 * of the following values:
 *
 * <ul>
 *   <li>HAM_TYPE_BINARY</li> This is the default key type: a binary blob.
 *   Internally, hamsterdb uses memcmp(3) for the sort order. Key size depends
 *   on @ref HAM_PARAM_KEY_SIZE and is unlimited (@ref HAM_KEY_SIZE_UNLIMITED)
 *   by default.
 *   <li>HAM_TYPE_CUSTOM</li> Similar to @ref HAM_TYPE_BINARY, but
 *   uses a callback function for the sort order. This function is supplied
 *   by the application with @sa ham_db_set_compare_func.
 *   <li>HAM_TYPE_UINT8</li> Key is a 8bit (1 byte) unsigned integer
 *   <li>HAM_TYPE_UINT16</li> Key is a 16bit (2 byte) unsigned integer
 *   <li>HAM_TYPE_UINT32</li> Key is a 32bit (4 byte) unsigned integer
 *   <li>HAM_TYPE_UINT64</li> Key is a 64bit (8 byte) unsigned integer
 *   <li>HAM_TYPE_REAL32</li> Key is a 32bit (4 byte) float
 *   <li>HAM_TYPE_REAL64</li> Key is a 64bit (8 byte) double
 * </ul>
 *
 * If the key type is ommitted then @ref HAM_TYPE_BINARY is the default.
 *
 * If binary/custom keys are so big that they cannot be stored in the Btree,
 * then the full key will be stored in an overflow area, which has
 * performance implications when accessing such keys.
 *
 * In addition to the flags above, you can specify @a HAM_ENABLE_DUPLICATE_KEYS
 * to insert duplicate keys, i.e. to model 1:n or n:m relationships.
 *
 * If the size of the records is always constant, then
 * @ref HAM_PARAM_RECORD_SIZE should be used to specify this size. This allows
 * hamsterdb to optimize the record storage, and small records will
 * automatically be stored in the Btree's leaf nodes instead of a separately
 * allocated blob, allowing faster access.
 * A record size of 0 is valid and suited for boolean values ("key exists"
 * vs "key doesn't exist"). The default record size is
 * @ref HAM_RECORD_SIZE_UNLIMITED.
 *
 * @param env A valid Environment handle.
 * @param db A valid Database handle, which will point to the created
 *      Database. To close the handle, use @ref ham_db_close.
 * @param name The name of the Database. If a Database with this name
 *      already exists, the function will fail with
 *      @ref HAM_DATABASE_ALREADY_EXISTS. Database names from 0xf000 to
 *      0xffff and 0 are reserved.
 * @param flags Optional flags for creating the Database, combined with
 *    bitwise OR. Possible flags are:
 *    <ul>
 *     <li>@ref HAM_ENABLE_DUPLICATE_KEYS </li> Enable duplicate keys for this
 *      Database. By default, duplicate keys are disabled.
 *     <li>@ref HAM_RECORD_NUMBER32 </li> Creates an "auto-increment" Database.
 *      Keys in Record Number Databases are automatically assigned an
 *      incrementing 32bit value. If key->data is not NULL
 *      (and key->flags is @ref HAM_KEY_USER_ALLOC), the value of the current
 *      key is returned in @a key. If key-data is NULL and key->size is 0,
 *      key->data is temporarily allocated by hamsterdb.
 *     <li>@ref HAM_RECORD_NUMBER64 </li> Creates an "auto-increment" Database.
 *      Keys in Record Number Databases are automatically assigned an
 *      incrementing 64bit value. If key->data is not NULL
 *      (and key->flags is @ref HAM_KEY_USER_ALLOC), the value of the current
 *      key is returned in @a key. If key-data is NULL and key->size is 0,
 *      key->data is temporarily allocated by hamsterdb.
 *    </ul>
 *
 * @param params An array of ham_parameter_t structures. The following
 *    parameters are available:
 *    <ul>
 *    <li>@ref HAM_PARAM_KEY_TYPE </li> The type of the keys in the B+Tree
 *      index. The default is @ref HAM_TYPE_BINARY. See above for more
 *      information.
 *    <li>@ref HAM_PARAM_KEY_SIZE </li> The (fixed) size of the keys in
 *      the B+Tree index; or @ref HAM_KEY_SIZE_UNLIMITED for unlimited and
 *      variable keys (this is the default).
 *    <li>@ref HAM_PARAM_RECORD_SIZE </li> The (fixed) size of the records;
 *      or @ref HAM_RECORD_SIZE_UNLIMITED if there was no fixed record size
 *      specified (this is the default).
 *    </ul>
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if the @a env pointer is NULL or an
 *        invalid combination of flags was specified
 * @return @ref HAM_DATABASE_ALREADY_EXISTS if a Database with this @a name
 *        already exists in this Environment
 * @return @ref HAM_OUT_OF_MEMORY if memory could not be allocated
 * @return @ref HAM_LIMITS_REACHED if the maximum number of Databases per
 *        Environment was already created
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_create_db(ham_env_t *env, ham_db_t **db,
            uint16_t name, uint32_t flags, const ham_parameter_t *params);

/**
 * Opens a Database in a Database Environment
 *
 * Each Database in an Environment is identified by a positive 16bit
 * value (except 0 and values at or above 0xf000).
 *
 * This function initializes the ham_db_t handle (the second parameter).
 * When the handle is no longer in use, it should be closed with
 * @ref ham_db_close. Alternatively, the Database handle is closed
 * automatically if @ref ham_env_close is called with the flag
 * @ref HAM_AUTO_CLEANUP.
 *
 * @param env A valid Environment handle
 * @param db A valid Database handle, which will point to the opened
 *      Database. To close the handle, use @see ham_db_close.
 * @param name The name of the Database. If a Database with this name
 *      does not exist, the function will fail with
 *      @ref HAM_DATABASE_NOT_FOUND.
 * @param flags Optional flags for opening the Database, combined with
 *    bitwise OR. Possible flags are:
 *   <ul>
 *     <li>@ref HAM_READ_ONLY </li> Opens the Database for reading only.
 *      Operations that need write access (i.e. @ref ham_db_insert) will
 *      return @ref HAM_WRITE_PROTECTED.
 *   </ul>
 * @param params Reserved; set to NULL
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if the @a env pointer is NULL or an
 *        invalid combination of flags was specified
 * @return @ref HAM_DATABASE_NOT_FOUND if a Database with this @a name
 *        does not exist in this Environment.
 * @return @ref HAM_DATABASE_ALREADY_OPEN if this Database was already
 *        opened
 * @return @ref HAM_OUT_OF_MEMORY if memory could not be allocated
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_open_db(ham_env_t *env, ham_db_t **db,
            uint16_t name, uint32_t flags, const ham_parameter_t *params);

/**
 * Renames a Database in an Environment.
 *
 * @param env A valid Environment handle.
 * @param oldname The old name of the existing Database. If a Database
 *      with this name does not exist, the function will fail with
 *      @ref HAM_DATABASE_NOT_FOUND.
 * @param newname The new name of this Database. If a Database
 *      with this name already exists, the function will fail with
 *      @ref HAM_DATABASE_ALREADY_EXISTS.
 * @param flags Optional flags for renaming the Database, combined with
 *    bitwise OR; unused, set to 0.
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if the @a env pointer is NULL or if
 *        the new Database name is reserved
 * @return @ref HAM_DATABASE_NOT_FOUND if a Database with this @a name
 *        does not exist in this Environment
 * @return @ref HAM_DATABASE_ALREADY_EXISTS if a Database with the new name
 *        already exists
 * @return @ref HAM_OUT_OF_MEMORY if memory could not be allocated
 * @return @ref HAM_NOT_READY if the Environment @a env was not initialized
 *        correctly (i.e. not yet opened or created)
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_rename_db(ham_env_t *env, uint16_t oldname,
            uint16_t newname, uint32_t flags);

/**
 * Deletes a Database from an Environment
 *
 * @param env A valid Environment handle
 * @param name The name of the Database to delete. If a Database
 *      with this name does not exist, the function will fail with
 *      @ref HAM_DATABASE_NOT_FOUND. If the Database was already opened,
 *      the function will fail with @ref HAM_DATABASE_ALREADY_OPEN.
 * @param flags Optional flags for deleting the Database; unused, set to 0.
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if the @a env pointer is NULL or if
 *        the new Database name is reserved
 * @return @ref HAM_DATABASE_NOT_FOUND if a Database with this @a name
 *        does not exist
 * @return @ref HAM_DATABASE_ALREADY_OPEN if a Database with this name is
 *        still open
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_erase_db(ham_env_t *env, uint16_t name, uint32_t flags);

/* internal flag - only flush committed transactions, not the btree pages */
#define HAM_FLUSH_COMMITTED_TRANSACTIONS    1

/**
 * Flushes the Environment
 *
 * This function flushes the Environment caches and writes the whole file
 * to disk. All Databases of this Environment are flushed as well.
 *
 * Since In-Memory Databases do not have a file on disk, the
 * function will have no effect and will return @ref HAM_SUCCESS.
 *
 * @param env A valid Environment handle
 * @param flags Optional flags for flushing; unused, set to 0
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if @a db is NULL
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_flush(ham_env_t *env, uint32_t flags);

/* internal use only - don't lock mutex */
#define HAM_DONT_LOCK        0xf0000000

/**
 * Returns the names of all Databases in an Environment
 *
 * This function returns the names of all Databases and the number of
 * Databases in an Environment.
 *
 * The memory for @a names must be allocated by the user. @a count
 * must be the size of @a names when calling the function, and will be
 * the number of Databases when the function returns. The function returns
 * @ref HAM_LIMITS_REACHED if @a names is not big enough; in this case, the
 * caller should resize the array and call the function again.
 *
 * @param env A valid Environment handle
 * @param names Pointer to an array for the Database names
 * @param count Pointer to the size of the array; will be used to store the
 *      number of Databases when the function returns.
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if @a env, @a names or @a count is NULL
 * @return @ref HAM_LIMITS_REACHED if @a names is not large enough to hold
 *      all Database names
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_get_database_names(ham_env_t *env, uint16_t *names,
            uint32_t *count);

/**
 * Closes the Database Environment
 *
 * This function closes the Database Environment. It also frees the
 * memory resources allocated in the @a env handle, and tries to truncate
 * the file (see below).
 *
 * If the flag @ref HAM_AUTO_CLEANUP is specified, hamsterdb automatically
 * calls @ref ham_db_close with flag @ref HAM_AUTO_CLEANUP on all open
 * Databases (which closes all open Databases and their Cursors). This
 * invalidates the ham_db_t and ham_cursor_t handles!
 *
 * If the flag is not specified, the application must close all Database
 * handles with @ref ham_db_close to prevent memory leaks.
 *
 * This function also aborts all Transactions which were not yet committed,
 * and therefore renders all Transaction handles invalid. If the flag
 * @ref HAM_TXN_AUTO_COMMIT is specified, all Transactions will be committed.
 *
 * This function also tries to truncate the file and "cut off" unused space
 * at the end of the file to reduce the file size. This feature is disabled
 * on Win32 if memory mapped I/O is used (see @ref HAM_DISABLE_MMAP).
 *
 * @param env A valid Environment handle
 * @param flags Optional flags for closing the handle. Possible flags are:
 *      <ul>
 *      <li>@ref HAM_AUTO_CLEANUP. Calls @ref ham_db_close with the flag
 *        @ref HAM_AUTO_CLEANUP on every open Database
 *      <li>@ref HAM_TXN_AUTO_COMMIT. Automatically commit all open
 *         Transactions
 *      <li>@ref HAM_TXN_AUTO_ABORT. Automatically abort all open
 *         Transactions; this is the default behaviour
 *      </ul>
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if @a env is NULL
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_env_close(ham_env_t *env, uint32_t flags);

/**
 * @}
 */


/**
 * @defgroup ham_txn hamsterdb Transaction Functions
 * @{
 */

/**
 * The hamsterdb Transaction structure
 *
 * This structure is allocated with @ref ham_txn_begin and deleted with
 * @ref ham_txn_commit or @ref ham_txn_abort.
 */
struct ham_txn_t;
typedef struct ham_txn_t ham_txn_t;

/**
 * Begins a new Transaction
 *
 * A Transaction is an atomic sequence of Database operations. With @ref
 * ham_txn_begin such a new sequence is started. To write all operations of this
 * sequence to the Database use @ref ham_txn_commit. To abort and cancel
 * this sequence use @ref ham_txn_abort.
 *
 * In order to use Transactions, the Environment has to be created or
 * opened with the flag @ref HAM_ENABLE_TRANSACTIONS.
 *
 * You can create as many Transactions as you want (older versions of
 * hamsterdb did not allow to create more than one Transaction in parallel).
 *
 * @param txn Pointer to a pointer of a Transaction structure
 * @param env A valid Environment handle
 * @param name An optional Transaction name
 * @param reserved A reserved pointer; always set to NULL
 * @param flags Optional flags for beginning the Transaction, combined with
 *    bitwise OR. Possible flags are:
 *    <ul>
 *     <li>@ref HAM_TXN_READ_ONLY </li> This Transaction is read-only and
 *      will not modify the Database.
 *    </ul>
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_OUT_OF_MEMORY if memory allocation failed
 */
HAM_EXPORT ham_status_t
ham_txn_begin(ham_txn_t **txn, ham_env_t *env, const char *name,
            void *reserved, uint32_t flags);

/** Flag for @ref ham_txn_begin */
#define HAM_TXN_READ_ONLY                     1

/* Internal flag for @ref ham_txn_begin */
#define HAM_TXN_TEMPORARY                     2

/**
 * Retrieves the Transaction name
 *
 * @returns NULL if the name was not assigned or if @a txn is invalid
 */
HAM_EXPORT const char *
ham_txn_get_name(ham_txn_t *txn);

/**
 * Commits a Transaction
 *
 * This function applies the sequence of Database operations.
 *
 * Note that the function will fail with @ref HAM_CURSOR_STILL_OPEN if
 * a Cursor was attached to this Transaction (with @ref ham_cursor_create
 * or @ref ham_cursor_clone), and the Cursor was not closed.
 *
 * @param txn Pointer to a Transaction structure
 * @param flags Optional flags for committing the Transaction, combined with
 *    bitwise OR. Unused, set to 0.
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_IO_ERROR if writing to the file failed
 * @return @ref HAM_CURSOR_STILL_OPEN if there are Cursors attached to this
 *      Transaction
 */
HAM_EXPORT ham_status_t
ham_txn_commit(ham_txn_t *txn, uint32_t flags);

/**
 * Aborts a Transaction
 *
 * This function aborts (= cancels) the sequence of Database operations.
 *
 * Note that the function will fail with @ref HAM_CURSOR_STILL_OPEN if
 * a Cursor was attached to this Transaction (with @ref ham_cursor_create
 * or @ref ham_cursor_clone), and the Cursor was not closed.
 *
 * @param txn Pointer to a Transaction structure
 * @param flags Optional flags for aborting the Transaction, combined with
 *    bitwise OR. Unused, set to 0.
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_IO_ERROR if writing to the Database file or logfile failed
 * @return @ref HAM_CURSOR_STILL_OPEN if there are Cursors attached to this
 *      Transaction
 */
HAM_EXPORT ham_status_t
ham_txn_abort(ham_txn_t *txn, uint32_t flags);

/**
 * @}
 */


/**
 * @defgroup ham_database hamsterdb Database Functions
 * @{
 */

/** Flag for @ref ham_env_open, @ref ham_env_create.
 * This flag is non persistent. */
#define HAM_ENABLE_FSYNC                            0x00000001

/* unused                                           0x00000002 */

/** Flag for @ref ham_env_open, @ref ham_env_open_db.
 * This flag is non persistent. */
#define HAM_READ_ONLY                               0x00000004

/* unused                                           0x00000008 */

/* unused                                           0x00000010 */

/* reserved                                         0x00000020 */

/* unused                                           0x00000040 */

/** Flag for @ref ham_env_create.
 * This flag is non persistent. */
#define HAM_IN_MEMORY                               0x00000080

/* reserved: DB_USE_MMAP (not persistent)           0x00000100 */

/** Flag for @ref ham_env_open, @ref ham_env_create.
 * This flag is non persistent. */
#define HAM_DISABLE_MMAP                            0x00000200

/* deprecated */
#define HAM_RECORD_NUMBER                           HAM_RECORD_NUMBER64

/** Flag for @ref ham_env_create_db.
 * This flag is persisted in the Database. */
#define HAM_RECORD_NUMBER32                         0x00001000

/** Flag for @ref ham_env_create_db.
 * This flag is persisted in the Database. */
#define HAM_RECORD_NUMBER64                         0x00002000

/** Flag for @ref ham_env_create_db.
 * This flag is persisted in the Database. */
#define HAM_ENABLE_DUPLICATE_KEYS                   0x00004000
/* deprecated */
#define HAM_ENABLE_DUPLICATES                       HAM_ENABLE_DUPLICATE_KEYS

/** Flag for @ref ham_env_create, @ref ham_env_open.
 * This flag is non persistent. */
#define HAM_ENABLE_RECOVERY                         0x00008000

/** Flag for @ref ham_env_open.
 * This flag is non persistent. */
#define HAM_AUTO_RECOVERY                           0x00010000

/** Flag for @ref ham_env_create, @ref ham_env_open.
 * This flag is non persistent. */
#define HAM_ENABLE_TRANSACTIONS                     0x00020000

/** Flag for @ref ham_env_open, @ref ham_env_create.
 * This flag is non persistent. */
#define HAM_CACHE_UNLIMITED                         0x00040000

/** Flag for @ref ham_env_create, @ref ham_env_open.
 * This flag is non persistent. */
#define HAM_DISABLE_RECOVERY                        0x00080000

/* internal use only! (not persistent) */
#define HAM_IS_REMOTE_INTERNAL                      0x00200000

/* internal use only! (not persistent) */
#define HAM_DISABLE_RECLAIM_INTERNAL                0x00400000

/* internal use only! (persistent) */
#define HAM_FORCE_RECORDS_INLINE                    0x00800000

/** Flag for @ref ham_env_open, @ref ham_env_create.
 * This flag is non persistent. */
#define HAM_FLUSH_WHEN_COMMITTED                    0x01000000

/** Pro: Flag for @ref ham_env_open, @ref ham_env_create.
 * This flag is non persistent. */
#define HAM_ENABLE_CRC32                            0x02000000

/**
 * Returns the last error code
 *
 * @note This API is deprecated! It will be removed in one of the
 * next versions.
 *
 * @param db A valid Database handle
 *
 * @return The last error code which was returned by one of the
 *     hamsterdb API functions. Use @ref ham_strerror to translate
 *     this code to a descriptive string
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_db_get_error(ham_db_t *db);

/**
 * Typedef for a key comparison function
 *
 * @remark This function compares two index keys. It returns -1, if @a lhs
 * ("left-hand side", the parameter on the left side) is smaller than
 * @a rhs ("right-hand side"), 0 if both keys are equal, and 1 if @a lhs
 * is larger than @a rhs.
 */
typedef int HAM_CALLCONV (*ham_compare_func_t)(ham_db_t *db,
                  const uint8_t *lhs, uint32_t lhs_length,
                  const uint8_t *rhs, uint32_t rhs_length);

/**
 * Sets the comparison function
 *
 * The comparison function compares two index keys. It returns -1 if the
 * first key is smaller, +1 if the second key is smaller or 0 if both
 * keys are equal.
 *
 * Supplying a comparison function is only allowed for the key type
 * @ref HAM_TYPE_CUSTOM; see the documentation of @sa ham_env_create_db 
 * for more information.
 *
 * @param db A valid Database handle
 * @param foo A pointer to the compare function
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if one of the parameters is NULL
 * @return @ref HAM_INV_PARAMETER if the database's key type was not
 *          specified as @ref HAM_TYPE_CUSTOM
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_db_set_compare_func(ham_db_t *db, ham_compare_func_t foo);

/**
 * Searches an item in the Database
 *
 * This function searches the Database for @a key. If the key
 * is found, @a record will receive the record of this item and
 * @ref HAM_SUCCESS is returned. If the key is not found, the function
 * returns @ref HAM_KEY_NOT_FOUND.
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
 * by subsequent hamsterdb API calls using the same Transaction
 * (or, if Transactions are disabled, using the same Database).
 * You can alter this behaviour by allocating the @a data pointer in
 * the application and setting @a record.flags to @ref HAM_RECORD_USER_ALLOC.
 * Make sure that the allocated buffer is large enough.
 *
 * When specifying @ref HAM_DIRECT_ACCESS, the @a data pointer will point
 * directly to the record that is stored in hamsterdb; the data can be modified,
 * but the pointer must not be reallocated or freed. The flag @ref
 * HAM_DIRECT_ACCESS is only allowed in In-Memory Databases and not if
 * Transactions are enabled.
 *
 * @ref ham_db_find can not search for duplicate keys. If @a key has
 * multiple duplicates, only the first duplicate is returned.
 *
 * You can read only portions of the record by specifying the flag
 * @ref HAM_PARTIAL. In this case, hamsterdb will read
 * <b>record->partial_size</b> bytes of the record data at offset
 * <b>record->partial_offset</b>. If necessary, the record data will
 * be limited to the original record size. The number of actually read
 * bytes is returned in <b>record->partial_size</b>. The original size of
 * the record is stored in <b>record->size</b>.
 *
 * @ref HAM_PARTIAL is not allowed if record->size is <= 8 or if Transactions
 * are enabled. In such a case, @ref HAM_INV_PARAMETER is returned.
 *
 * If Transactions are enabled (see @ref HAM_ENABLE_TRANSACTIONS) and
 * @a txn is NULL then hamsterdb will create a temporary Transaction.
 * When moving the Cursor, and the new key is currently modified in an
 * active Transaction (one that is not yet committed or aborted) then
 * hamsterdb will skip this key and move to the next/previous one. However if
 * @a flags are 0 (and the Cursor is not moved), and @a key or @a rec
 * is NOT NULL, then hamsterdb will return error @ref HAM_TXN_CONFLICT.
 *
 * @param db A valid Database handle
 * @param txn A Transaction handle, or NULL
 * @param key The key of the item
 * @param record The record of the item
 * @param flags Optional flags for searching, which can be combined with
 *    bitwise OR. Possible flags are:
 *    <ul>
 *    <li>@ref HAM_FIND_LT_MATCH </li> Cursor 'find' flag 'Less Than': the
 *        cursor is moved to point at the last record which' key
 *        is less than the specified key. When such a record cannot
 *        be located, an error is returned.
 *    <li>@ref HAM_FIND_GT_MATCH </li> Cursor 'find' flag 'Greater Than':
 *        the cursor is moved to point at the first record which' key is
 *        larger than the specified key. When such a record cannot be
 *        located, an error is returned.
 *    <li>@ref HAM_FIND_LEQ_MATCH </li> Cursor 'find' flag 'Less or EQual':
 *        the cursor is moved to point at the record which' key matches
 *        the specified key and when such a record is not available
 *        the cursor is moved to point at the last record which' key
 *        is less than the specified key. When such a record cannot be
 *        located, an error is returned.
 *    <li>@ref HAM_FIND_GEQ_MATCH </li> Cursor 'find' flag 'Greater or
 *        Equal': the cursor is moved to point at the record which' key
 *        matches the specified key and when such a record
 *        is not available the cursor is moved to point at the first
 *        record which' key is larger than the specified key.
 *        When such a record cannot be located, an error is returned.
 *    <li>@ref HAM_FIND_NEAR_MATCH </li> Cursor 'find' flag 'Any Near Or
 *        Equal': the cursor is moved to point at the record which'
 *        key matches the specified key and when such a record is
 *        not available the cursor is moved to point at either the
 *        last record which' key is less than the specified key or
 *        the first record which' key is larger than the specified
 *        key, whichever of these records is located first.
 *        When such records cannot be located, an error is returned.
 *    <li>@ref HAM_DIRECT_ACCESS </li> Only for In-Memory Databases
 *        and not if Transactions are enabled!
 *        Returns a direct pointer to the data blob stored by the
 *        hamsterdb engine. This pointer must not be resized or freed,
 *        but the data in this memory can be modified.
 *    </ul>
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if @a db, @a key or @a record is NULL
 * @return @ref HAM_INV_PARAMETER if @a HAM_DIRECT_ACCESS is specified,
 *      but the Database is not an In-Memory Database.
 * @return @ref HAM_INV_PARAMETER if @a HAM_DIRECT_ACCESS and
 *      @a HAM_ENABLE_TRANSACTIONS were both specified.
 * @return @ref HAM_INV_PARAMETER if @ref HAM_PARTIAL is set but record
 *      size is <= 8 or Transactions are enabled
 * @return @ref HAM_KEY_NOT_FOUND if the @a key does not exist
 * @return @ref HAM_TXN_CONFLICT if the same key was inserted in another
 *        Transaction which was not yet committed or aborted
 *
 * @remark When either or both @ref HAM_FIND_LT_MATCH and/or @ref
 *    HAM_FIND_GT_MATCH have been specified as flags, the @a key structure
 *    will be overwritten when an approximate match was found: the
 *    @a key and @a record structures will then point at the located
 *    @a key and @a record. In this case the caller should ensure @a key
 *    points at a structure which must adhere to the same restrictions
 *    and conditions as specified for @ref ham_cursor_move(...,
 *    HAM_CURSOR_NEXT).
 *
 * @sa HAM_RECORD_USER_ALLOC
 * @sa HAM_KEY_USER_ALLOC
 * @sa ham_record_t
 * @sa ham_key_t
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_db_find(ham_db_t *db, ham_txn_t *txn, ham_key_t *key,
            ham_record_t *record, uint32_t flags);

/**
 * Inserts a Database item
 *
 * This function inserts a key/record pair as a new Database item.
 *
 * If the key already exists in the Database, error @ref HAM_DUPLICATE_KEY
 * is returned.
 *
 * If you wish to overwrite an existing entry specify the
 * flag @ref HAM_OVERWRITE.
 *
 * You can write only portions of the record by specifying the flag
 * @ref HAM_PARTIAL. In this case, hamsterdb will write <b>partial_size</b>
 * bytes of the record data at offset <b>partial_offset</b>. The full record
 * size will always be given in <b>record->size</b>! If
 * partial_size+partial_offset exceed record->size then partial_size will
 * be limited. To shrink or grow the record, adjust record->size.
 * @ref HAM_PARTIAL automatically overwrites existing records.
 * Gaps will be filled with null-bytes if the record did not yet exist.
 *
 * @ref HAM_PARTIAL is not allowed if record->size is <= 8 or if Transactions
 * are enabled. In such a case, @ref HAM_INV_PARAMETER is returned.
 *
 * If you wish to insert a duplicate key specify the flag @ref HAM_DUPLICATE.
 * (Note that the Database has to be created with @ref HAM_ENABLE_DUPLICATE_KEYS
 * in order to use duplicate keys.)
 * The duplicate key is inserted after all other duplicate keys (see
 * @ref HAM_DUPLICATE_INSERT_LAST).
 *
 * Record Number Databases (created with @ref HAM_RECORD_NUMBER32 or
 * @ref HAM_RECORD_NUMBER64) expect either an empty @a key (with a size of
 * 0 and data pointing to NULL), or a user-supplied key (with key.flag
 * @ref HAM_KEY_USER_ALLOC and a valid data pointer).
 * If key.size is 0 and key.data is NULL, hamsterdb will temporarily
 * allocate memory for key->data, which will then point to an 4-byte (or 8-byte)
 * unsigned integer.
 *
 * For very fast sequential inserts please use @ref ham_cursor_insert in
 * combination with the flag @ref HAM_HINT_APPEND.
 *
 * @param db A valid Database handle
 * @param txn A Transaction handle, or NULL
 * @param key The key of the new item
 * @param record The record of the new item
 * @param flags Optional flags for inserting. Possible flags are:
 *    <ul>
 *    <li>@ref HAM_OVERWRITE. If the @a key already exists, the record is
 *        overwritten. Otherwise, the key is inserted. Flag is not
 *        allowed in combination with @ref HAM_DUPLICATE.
 *    <li>@ref HAM_DUPLICATE. If the @a key already exists, a duplicate
 *        key is inserted. The key is inserted before the already
 *        existing key, or according to the sort order. Flag is not
 *        allowed in combination with @ref HAM_OVERWRITE.
 *    </ul>
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if @a db, @a key or @a record is NULL
 * @return @ref HAM_INV_PARAMETER if the Database is a Record Number Database
 *        and the key is invalid (see above)
 * @return @ref HAM_INV_PARAMETER if @ref HAM_PARTIAL is set but record
 *        size is <= 8 or Transactions are enabled
 * @return @ref HAM_INV_PARAMETER if the flags @ref HAM_OVERWRITE <b>and</b>
 *        @ref HAM_DUPLICATE were specified, or if @ref HAM_DUPLICATE
 *        was specified, but the Database was not created with
 *        flag @ref HAM_ENABLE_DUPLICATE_KEYS.
 * @return @ref HAM_INV_PARAMETER if @ref HAM_PARTIAL is specified and
 *        record->partial_offset+record->partial_size exceeds the
 *        record->size
 * @return @ref HAM_WRITE_PROTECTED if you tried to insert a key in a read-only
 *        Database
 * @return @ref HAM_TXN_CONFLICT if the same key was inserted in another
 *        Transaction which was not yet committed or aborted
 * @return @ref HAM_INV_KEY_SIZE if the key size is larger than the
 *        @a HAM_PARAMETER_KEY_SIZE parameter specified for
 *        @ref ham_env_create_db
 *        OR if the key's size is greater than the Btree key size (see
 *        @ref HAM_PARAM_KEY_SIZE).
 * @return @ref HAM_INV_RECORD_SIZE if the record size is different from
 *        the one specified with @a HAM_PARAM_RECORD_SIZE
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_db_insert(ham_db_t *db, ham_txn_t *txn, ham_key_t *key,
            ham_record_t *record, uint32_t flags);

/**
 * Flag for @ref ham_db_insert and @ref ham_cursor_insert
 *
 * When specified with @ref ham_db_insert and in case a key
 * is specified which stores duplicates in the Database, the first
 * duplicate record will be overwritten.
 *
 * When used with @ref ham_cursor_insert and assuming the same
 * conditions, the duplicate currently referenced by the Cursor
 * will be overwritten.
*/
#define HAM_OVERWRITE                   0x0001

/** Flag for @ref ham_db_insert and @ref ham_cursor_insert */
#define HAM_DUPLICATE                   0x0002

/** Flag for @ref ham_cursor_insert */
#define HAM_DUPLICATE_INSERT_BEFORE     0x0004

/** Flag for @ref ham_cursor_insert */
#define HAM_DUPLICATE_INSERT_AFTER      0x0008

/** Flag for @ref ham_cursor_insert */
#define HAM_DUPLICATE_INSERT_FIRST      0x0010

/** Flag for @ref ham_cursor_insert */
#define HAM_DUPLICATE_INSERT_LAST       0x0020

/** Flag for @ref ham_db_find, @ref ham_cursor_find, @ref ham_cursor_move */
#define HAM_DIRECT_ACCESS               0x0040

/** Flag for @ref ham_db_insert, @ref ham_cursor_insert, @ref ham_db_find,
 * @ref ham_cursor_find, @ref ham_cursor_move */
#define HAM_PARTIAL                     0x0080

/* Internal flag for @ref ham_db_find, @ref ham_cursor_find,
 * @ref ham_cursor_move */
#define HAM_FORCE_DEEP_COPY             0x0100

/**
 * Flag for @ref ham_cursor_insert
 *
 * Mutually exclusive with flag @ref HAM_HINT_PREPEND.
 *
 * Hints the hamsterdb engine that the current key will
 * compare as @e larger than any key already existing in the Database.
 * The hamsterdb engine will verify this postulation and when found not
 * to be true, will revert to a regular insert operation
 * as if this flag was not specified. The incurred cost then is only one
 * additional key comparison.
 */
#define HAM_HINT_APPEND                 0x00080000

/**
 * Flag for @ref ham_cursor_insert
 *
 * Mutually exclusive with flag @ref HAM_HINT_APPEND.
 *
 * Hints the hamsterdb engine that the current key will
 * compare as @e smaller than any key already existing in the Database.
 * The hamsterdb engine will verify this postulation and when found not
 * to be true, will revert to a regular insert operation
 * as if this flag was not specified. The incurred cost then is only one
 * additional key comparison.
 */
#define HAM_HINT_PREPEND                0x00100000

/**
 * Flag mask to extract the common hint flags from a find/move/insert/erase
 * flag value.
 */
#define HAM_HINTS_MASK                  0x001F0000

/**
 * Erases a Database item
 *
 * This function erases a Database item. If the item @a key
 * does not exist, @ref HAM_KEY_NOT_FOUND is returned.
 *
 * Note that ham_db_erase can not erase a single duplicate key. If the key
 * has multiple duplicates, all duplicates of this key will be erased. Use
 * @ref ham_cursor_erase to erase a specific duplicate key.
 *
 * @param db A valid Database handle
 * @param txn A Transaction handle, or NULL
 * @param key The key to delete
 * @param flags Optional flags for erasing; unused, set to 0
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if @a db or @a key is NULL
 * @return @ref HAM_WRITE_PROTECTED if you tried to erase a key from a read-only
 *        Database
 * @return @ref HAM_KEY_NOT_FOUND if @a key was not found
 * @return @ref HAM_TXN_CONFLICT if the same key was inserted in another
 *        Transaction which was not yet committed or aborted
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_db_erase(ham_db_t *db, ham_txn_t *txn, ham_key_t *key, uint32_t flags);

/* internal flag for ham_db_erase() - do not use */
#define HAM_ERASE_ALL_DUPLICATES                1

/**
 * Returns the number of keys stored in the Database
 *
 * You can specify the @ref HAM_SKIP_DUPLICATES if you do now want
 * to include any duplicates in the count. This will also speed up the
 * counting.
 *
 * @param db A valid Database handle
 * @param txn A Transaction handle, or NULL
 * @param flags Optional flags:
 *     <ul>
 *     <li>@ref HAM_SKIP_DUPLICATES. Excludes any duplicates from
 *       the count
 *     </ul>
 * @param keycount A reference to a variable which will receive
 *         the calculated key count per page
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if @a db or @a keycount is NULL or when
 *     @a flags contains an invalid flag set
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_db_get_key_count(ham_db_t *db, ham_txn_t *txn, uint32_t flags,
            uint64_t *keycount);

/**
 * Retrieve the current value for a given Database setting
 *
 * Only those values requested by the parameter array will be stored.
 *
 * The following parameters are supported:
 *    <ul>
 *    <li>HAM_PARAM_FLAGS</li> returns the flags which were used to
 *        open or create this Database
 *    <li>HAM_PARAM_DATABASE_NAME</li> returns the Database name
 *    <li>HAM_PARAM_KEY_TYPE</li> returns the Btree key type
 *    <li>HAM_PARAM_KEY_SIZE</li> returns the Btree key size
 *        or @ref HAM_KEY_SIZE_UNLIMITED if there was no fixed key size
 *        specified.
 *    <li>HAM_PARAM_RECORD_SIZE</li> returns the record size,
 *        or @ref HAM_RECORD_SIZE_UNLIMITED if there was no fixed record size
 *        specified.
 *    <li>HAM_PARAM_MAX_KEYS_PER_PAGE</li> returns the maximum number
 *        of keys per page. This number is precise if the key size is fixed
 *        and duplicates are disabled; otherwise it's an estimate.
 *    <li>@ref HAM_PARAM_RECORD_COMPRESSION</li> Returns the
 *        selected algorithm for record compression, or 0 if compression
 *        is disabled
 *    <li>@ref HAM_PARAM_KEY_COMPRESSION</li> Returns the
 *        selected algorithm for key compression, or 0 if compression
 *        is disabled
 *    </ul>
 *
 * @param db A valid Database handle
 * @param param An array of ham_parameter_t structures
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if the @a db pointer is NULL or
 *        @a param is NULL
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_db_get_parameters(ham_db_t *db, ham_parameter_t *param);

/** Parameter name for @ref ham_env_open, @ref ham_env_create;
 * Journal files are switched whenever the number of new Transactions exceeds
 * this threshold. */
#define HAM_PARAM_JOURNAL_SWITCH_THRESHOLD 0x00001

/** Parameter name for @ref ham_env_open, @ref ham_env_create;
 * sets the cache size */
#define HAM_PARAM_CACHE_SIZE            0x00000100
/* deprecated */
#define HAM_PARAM_CACHESIZE             HAM_PARAM_CACHE_SIZE

/** Parameter name for @ref ham_env_create; sets the page size */
#define HAM_PARAM_PAGE_SIZE             0x00000101
/* deprecated */
#define HAM_PARAM_PAGESIZE              HAM_PARAM_PAGE_SIZE

/** Parameter name for @ref ham_env_create_db; sets the key size */
#define HAM_PARAM_KEY_SIZE              0x00000102
/* deprecated */
#define HAM_PARAM_KEYSIZE               HAM_PARAM_KEY_SIZE

/** Parameter name for @ref ham_env_get_parameters; retrieves the number
 * of maximum Databases */
#define HAM_PARAM_MAX_DATABASES         0x00000103

/** Parameter name for @ref ham_env_create_db; sets the key type */
#define HAM_PARAM_KEY_TYPE              0x00000104

/** Parameter name for @ref ham_env_open, @ref ham_env_create;
 * sets the path of the log files */
#define HAM_PARAM_LOG_DIRECTORY         0x00000105

/** hamsterdb pro: Parameter name for @ref ham_env_open, @ref ham_env_create;
 * sets the AES encryption key */
#define HAM_PARAM_ENCRYPTION_KEY        0x00000106

/** Parameter name for @ref ham_env_open, @ref ham_env_create;
 * sets the network timeout (in seconds) */
#define HAM_PARAM_NETWORK_TIMEOUT_SEC   0x00000107

/** Parameter name for @ref ham_env_create_db; sets the key size */
#define HAM_PARAM_RECORD_SIZE           0x00000108

/** Parameter name for @ref ham_env_create, @ref ham_env_open; sets a
 * limit for the file size (in bytes) */
#define HAM_PARAM_FILE_SIZE_LIMIT       0x00000109

/** Parameter name for @ref ham_env_create, @ref ham_env_open; sets the
 * parameter for posix_fadvise() */
#define HAM_PARAM_POSIX_FADVISE         0x00000110

/** Value for @ref HAM_PARAM_POSIX_FADVISE */
#define HAM_POSIX_FADVICE_NORMAL                 0

/** Value for @ref HAM_PARAM_POSIX_FADVISE */
#define HAM_POSIX_FADVICE_RANDOM                 1

/** Value for unlimited record sizes */
#define HAM_RECORD_SIZE_UNLIMITED       ((uint32_t)-1)

/** Value for unlimited key sizes */
#define HAM_KEY_SIZE_UNLIMITED          ((uint16_t)-1)

/** Retrieves the Database/Environment flags as were specified at the time of
 * @ref ham_env_create/@ref ham_env_open invocation. */
#define HAM_PARAM_FLAGS                 0x00000200

/** Retrieves the filesystem file access mode as was specified at the time
 * of @ref ham_env_create/@ref ham_env_open invocation. */
#define HAM_PARAM_FILEMODE              0x00000201

/**
 * Return a <code>const char *</code> pointer to the current
 * Environment/Database file name in the @ref uint64_t value
 * member, when the Database is actually stored on disc.
 *
 * In-memory Databases will return a NULL (0) pointer instead.
 */
#define HAM_PARAM_FILENAME              0x00000202

/**
 * Retrieve the Database 'name' number of this @ref ham_db_t Database within
 * the current @ref ham_env_t Environment.
*/
#define HAM_PARAM_DATABASE_NAME         0x00000203

/**
 * Retrieve the maximum number of keys per page; this number depends on the
 * currently active page and key sizes. Can be an estimate if keys do not
 * have constant sizes or if duplicate keys are used.
 */
#define HAM_PARAM_MAX_KEYS_PER_PAGE     0x00000204

/**
 * hamsterdb pro: Parameter name for @ref ham_env_create, @ref ham_env_open;
 * enables compression for the journal.
 */
#define HAM_PARAM_JOURNAL_COMPRESSION   0x00001000

/**
 * hamsterdb pro: Parameter name for @ref ham_env_create_db,
 * @ref ham_env_open_db; enables compression for the records of
 * a Database.
 */
#define HAM_PARAM_RECORD_COMPRESSION    0x00001001

/**
 * hamsterdb pro: Parameter name for @ref ham_env_create_db,
 * @ref ham_env_open_db; enables compression for the records of
 * a Database.
 */
#define HAM_PARAM_KEY_COMPRESSION       0x00001002

/** hamsterdb pro: helper macro for disabling compression */
#define HAM_COMPRESSOR_NONE         0

/**
 * hamsterdb pro: selects zlib compression
 * http://www.zlib.net/
 */
#define HAM_COMPRESSOR_ZLIB         1

/**
 * hamsterdb pro: selects google snappy compression
 * http://code.google.com/p/snappy
 */
#define HAM_COMPRESSOR_SNAPPY       2

/**
 * hamsterdb pro: selects lzf compression
 * http://oldhome.schmorp.de/marc/liblzf.html
 */
#define HAM_COMPRESSOR_LZF          3

/**
 * hamsterdb pro: selects lzo compression
 * http://www.oberhumer.com/opensource/lzo
 */
#define HAM_COMPRESSOR_LZO          4

/**
 * Retrieves the Environment handle of a Database
 *
 * @param db A valid Database handle
 *
 * @return The Environment handle
 */
HAM_EXPORT ham_env_t *HAM_CALLCONV
ham_db_get_env(ham_db_t *db);

/**
 * Returns the kind of key match which produced this key as it was
 * returned by one of the @ref ham_db_find() and @ref ham_cursor_find().
 *
 * This routine assumes the key was passed back by one of the @ref ham_db_find
 * and @ref ham_cursor_find functions and not used by any other hamsterdb
 * functions after that.
 *
 * As such, this function produces an answer akin to the 'sign' of the
 * specified key as it was returned by the find operation.
 *
 * @param key A valid key
 *
 * @return 1 (greater than) or -1 (less than) when the given key is an
 *    approximate result / zero (0) otherwise. Specifically:
 *    <ul>
 *    <li>+1 when the key is greater than the item searched for (key
 *        was a GT match)
 *    <li>-1 when the key is less than the item searched for (key was
 *        a LT match)
 *    <li>zero (0) otherwise (key was an EQ (EXACT) match)
 *    </ul>
 */
HAM_EXPORT int HAM_CALLCONV
ham_key_get_approximate_match_type(ham_key_t *key);

/**
 * Closes the Database
 *
 * This function flushes the Database and then closes the file handle.
 * It also free the memory resources allocated in the @a db handle.
 *
 * If the flag @ref HAM_AUTO_CLEANUP is specified, hamsterdb automatically
 * calls @ref ham_cursor_close on all open Cursors. This invalidates the
 * ham_cursor_t handle!
 *
 * If the flag is not specified, the application must close all Database
 * Cursors with @ref ham_cursor_close to prevent memory leaks.
 *
 * This function also aborts all Transactions which were not yet committed,
 * and therefore renders all Transaction handles invalid. If the flag
 * @ref HAM_TXN_AUTO_COMMIT is specified, all Transactions will be committed.
 *
 * @param db A valid Database handle
 * @param flags Optional flags for closing the Database. Possible values are:
 *    <ul>
 *     <li>@ref HAM_AUTO_CLEANUP. Automatically closes all open Cursors
 *     <li>@ref HAM_TXN_AUTO_COMMIT. Automatically commit all open
 *      Transactions
 *     <li>@ref HAM_TXN_AUTO_ABORT. Automatically abort all open
 *      Transactions; this is the default behaviour
 *    </ul>
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if @a db is NULL
 * @return @ref HAM_CURSOR_STILL_OPEN if not all Cursors of this Database
 *    were closed, and @ref HAM_AUTO_CLEANUP was not specified
 * @return @ref HAM_TXN_STILL_OPEN if this Database is modified by a
 *    currently active Transaction
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_db_close(ham_db_t *db, uint32_t flags);

/** Flag for @ref ham_db_close, @ref ham_env_close */
#define HAM_AUTO_CLEANUP                1

/** @internal (Internal) flag for @ref ham_db_close, @ref ham_env_close */
#define HAM_DONT_CLEAR_LOG              2

/** Automatically abort all open Transactions (the default) */
#define HAM_TXN_AUTO_ABORT              4

/** Automatically commit all open Transactions */
#define HAM_TXN_AUTO_COMMIT             8

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
 * The application should close all Cursors of a Database before closing
 * the Database.
 *
 * If Transactions are enabled (@ref HAM_ENABLE_TRANSACTIONS), but @a txn
 * is NULL, then each Cursor operation (i.e. @ref ham_cursor_insert,
 * @ref ham_cursor_find etc) will create its own, temporary Transaction
 * <b>only</b> for the lifetime of this operation and not for the lifetime
 * of the whole Cursor!
 *
 * @param db A valid Database handle
 * @param txn A Transaction handle, or NULL
 * @param flags Optional flags for creating the Cursor; unused, set to 0
 * @param cursor A pointer to a pointer which is allocated for the
 *      new Cursor handle
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if @a db or @a cursor is NULL
 * @return @ref HAM_OUT_OF_MEMORY if the new structure could not be allocated
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_cursor_create(ham_cursor_t **cursor, ham_db_t *db, ham_txn_t *txn,
            uint32_t flags);

/**
 * Clones a Database Cursor
 *
 * Clones an existing Cursor. The new Cursor will point to
 * exactly the same item as the old Cursor. If the old Cursor did not point
 * to any item, so will the new Cursor.
 *
 * If the old Cursor is bound to a Transaction, then the new Cursor will
 * also be bound to this Transaction.
 *
 * @param src The existing Cursor
 * @param dest A pointer to a pointer, which is allocated for the
 *      cloned Cursor handle
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if @a src or @a dest is NULL
 * @return @ref HAM_OUT_OF_MEMORY if the new structure could not be allocated
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
 * When specifying @ref HAM_DIRECT_ACCESS, the @a data pointer will point
 * directly to the record that is stored in hamsterdb; the data can be modified,
 * but the pointer must not be reallocated or freed. The flag @ref
 * HAM_DIRECT_ACCESS is only allowed in In-Memory Databases and not if
 * Transactions are enabled.
 *
 * You can read only portions of the record by specifying the flag
 * @ref HAM_PARTIAL. In this case, hamsterdb will read
 * <b>record->partial_size</b> bytes of the record data at offset
 * <b>record->partial_offset</b>. If necessary, the record data will
 * be limited to the original record size. The number of actually read
 * bytes is returned in <b>record->partial_size</b>. The original size of
 * the record is stored in <b>record->size</b>.
 *
 * @ref HAM_PARTIAL is not allowed if record->size is <= 8 or if Transactions
 * are enabled. In such a case, @ref HAM_INV_PARAMETER is returned.
 *
 * If Transactions are enabled (see @ref HAM_ENABLE_TRANSACTIONS), and
 * the Cursor moves next or previous to a key which is currently modified
 * in an active Transaction (one that is not yet committed or aborted), then
 * hamsterdb will skip the modified key. (This behavior is different from i.e.
 * @a ham_cursor_find, which would return the error @ref HAM_TXN_CONFLICT).
 *
 * If a key has duplicates and any of the duplicates is currently modified
 * in another active Transaction, then ALL duplicate keys are skipped when
 * moving to the next or previous key.
 *
 * If the first (@ref HAM_CURSOR_FIRST) or last (@ref HAM_CURSOR_LAST) key
 * is requested, and the current key (or any of its duplicates) is currently
 * modified in an active Transaction, then @ref HAM_TXN_CONFLICT is
 * returned.
 *
 * If this Cursor is nil (i.e. because it was not yet used or the Cursor's
 * item was erased) then the flag @a HAM_CURSOR_NEXT (or @a
 * HAM_CURSOR_PREVIOUS) will be identical to @a HAM_CURSOR_FIRST (or
 * @a HAM_CURSOR_LAST).
 *
 * @param cursor A valid Cursor handle
 * @param key An optional pointer to a @ref ham_key_t structure. If this
 *    pointer is not NULL, the key of the new item is returned.
 *    Note that key->data will point to temporary data. This pointer
 *    will be invalidated by subsequent hamsterdb API calls. See
 *    @ref HAM_KEY_USER_ALLOC on how to change this behaviour.
 * @param record An optional pointer to a @ref ham_record_t structure. If this
 *    pointer is not NULL, the record of the new item is returned.
 *    Note that record->data will point to temporary data. This pointer
 *    will be invalidated by subsequent hamsterdb API calls. See
 *    @ref HAM_RECORD_USER_ALLOC on how to change this behaviour.
 * @param flags The flags for this operation. They are used to specify
 *    the direction for the "move". If you do not specify a direction,
 *    the Cursor will remain on the current position.
 *    <ul>
 *      <li>@ref HAM_CURSOR_FIRST </li> positions the Cursor on the first
 *        item in the Database
 *      <li>@ref HAM_CURSOR_LAST </li> positions the Cursor on the last
 *        item in the Database
 *      <li>@ref HAM_CURSOR_NEXT </li> positions the Cursor on the next
 *        item in the Database; if the Cursor does not point to any
 *        item, the function behaves as if direction was
 *        @ref HAM_CURSOR_FIRST.
 *      <li>@ref HAM_CURSOR_PREVIOUS </li> positions the Cursor on the
 *        previous item in the Database; if the Cursor does not point to
 *        any item, the function behaves as if direction was
 *        @ref HAM_CURSOR_LAST.
 *      <li>@ref HAM_SKIP_DUPLICATES </li> skips duplicate keys of the
 *        current key. Not allowed in combination with
 *        @ref HAM_ONLY_DUPLICATES.
 *      <li>@ref HAM_ONLY_DUPLICATES </li> only move through duplicate keys
 *        of the current key. Not allowed in combination with
 *        @ref HAM_SKIP_DUPLICATES.
 *    <li>@ref HAM_DIRECT_ACCESS </li> Only for In-Memory Databases and
 *        not if Transactions are enabled!
 *        Returns a direct pointer to the data blob stored by the
 *        hamsterdb engine. This pointer must not be resized or freed,
 *        but the data in this memory can be modified.
 *   </ul>
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if @a cursor is NULL, or if an invalid
 *        combination of flags was specified
 * @return @ref HAM_INV_PARAMETER if @ref HAM_PARTIAL is set but record
 *        size is <= 8 or Transactions are enabled
 * @return @ref HAM_CURSOR_IS_NIL if the Cursor does not point to an item, but
 *        key and/or record were requested
 * @return @ref HAM_KEY_NOT_FOUND if @a cursor points to the first (or last)
 *        item, and a move to the previous (or next) item was
 *        requested
 * @return @ref HAM_INV_PARAMETER if @a HAM_DIRECT_ACCESS is specified,
 *        but the Database is not an In-Memory Database.
 * @return @ref HAM_INV_PARAMETER if @a HAM_DIRECT_ACCESS and
 *        @a HAM_ENABLE_TRANSACTIONS were both specified.
 * @return @ref HAM_INV_PARAMETER if @ref HAM_PARTIAL is specified and
 *        record->partial_offset+record->partial_size exceeds the
 *        record->size
 * @return @ref HAM_TXN_CONFLICT if @ref HAM_CURSOR_FIRST or @ref
 *        HAM_CURSOR_LAST is specified but the first (or last) key or
 *        any of its duplicates is currently modified in an active
 *        Transaction
 *
 * @sa HAM_RECORD_USER_ALLOC
 * @sa HAM_KEY_USER_ALLOC
 * @sa ham_record_t
 * @sa ham_key_t
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_cursor_move(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, uint32_t flags);

/** Flag for @ref ham_cursor_move */
#define HAM_CURSOR_FIRST                0x0001

/** Flag for @ref ham_cursor_move */
#define HAM_CURSOR_LAST                 0x0002

/** Flag for @ref ham_cursor_move */
#define HAM_CURSOR_NEXT                 0x0004

/** Flag for @ref ham_cursor_move */
#define HAM_CURSOR_PREVIOUS             0x0008

/** Flag for @ref ham_cursor_move and @ref ham_db_get_key_count */
#define HAM_SKIP_DUPLICATES             0x0010

/** Flag for @ref ham_cursor_move */
#define HAM_ONLY_DUPLICATES             0x0020

/**
 * Overwrites the current record
 *
 * This function overwrites the record of the current item.
 *
 * @param cursor A valid Cursor handle
 * @param record A valid record structure
 * @param flags Optional flags for overwriting the item; unused, set to 0
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if @a cursor or @a record is NULL
 * @return @ref HAM_INV_PARAMETER if @a cursor points to an item with
 *      duplicates and duplicate sorting is enabled
 * @return @ref HAM_INV_PARAMETER if duplicate sorting is enabled
 * @return @ref HAM_CURSOR_IS_NIL if the Cursor does not point to an item
 * @return @ref HAM_TXN_CONFLICT if the same key was inserted in another
 *        Transaction which was not yet committed or aborted
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_cursor_overwrite(ham_cursor_t *cursor, ham_record_t *record,
            uint32_t flags);

/**
 * Searches with a key and points the Cursor to the key found, retrieves
 * the located record
 *
 * Searches for an item in the Database and points the Cursor to this item.
 * If the item could not be found, the Cursor is not modified.
 *
 * Note that @ref ham_cursor_find can not search for duplicate keys. If @a key
 * has multiple duplicates, only the first duplicate is returned.
 *
 * When specifying @ref HAM_DIRECT_ACCESS, the @a data pointer will point
 * directly to the record that is stored in hamsterdb; the data can be modified,
 * but the pointer must not be reallocated or freed. The flag @ref
 * HAM_DIRECT_ACCESS is only allowed in In-Memory Databases and not if
 * Transactions are enabled.
 *
 * You can read only portions of the record by specifying the flag
 * @ref HAM_PARTIAL. In this case, hamsterdb will read
 * <b>record->partial_size</b> bytes of the record data at offset
 * <b>record->partial_offset</b>. If necessary, the record data will
 * be limited to the original record size. The number of actually read
 * bytes is returned in <b>record->partial_size</b>. The original size of
 * the record is stored in <b>record->size</b>.
 *
 * @ref HAM_PARTIAL is not allowed if record->size is <= 8 or if Transactions
 * are enabled. In such a case, @ref HAM_INV_PARAMETER is returned.
 *
 * When either or both @ref HAM_FIND_LT_MATCH and/or @ref HAM_FIND_GT_MATCH
 * have been specified as flags, the @a key structure will be overwritten
 * when an approximate match was found: the @a key and @a record
 * structures will then point at the located @a key (and @a record).
 * In this case the caller should ensure @a key points at a structure
 * which must adhere to the same restrictions and conditions as specified
 * for @ref ham_cursor_move(...,HAM_CURSOR_*):
 * key->data will point to temporary data upon return. This pointer
 * will be invalidated by subsequent hamsterdb API calls using the same
 * Transaction (or the same Database, if Transactions are disabled). See
 * @ref HAM_KEY_USER_ALLOC on how to change this behaviour.
 *
 * Further note that the @a key structure must be non-const at all times as its
 * internal flag bits may be written to. This is done for your benefit, as
 * you may pass the returned @a key structure to
 * @ref ham_key_get_approximate_match_type() to retrieve additional info about
 * the precise nature of the returned key: the sign value produced
 * by @ref ham_key_get_approximate_match_type() tells you which kind of match
 * (equal, less than, greater than) occurred. This is very useful to
 * discern between the various possible successful answers produced by the
 * combinations of @ref HAM_FIND_LT_MATCH and @ref HAM_FIND_GT_MATCH.
 *
 * @param cursor A valid Cursor handle
 * @param key A pointer to a @ref ham_key_t structure. If this
 *    pointer is not NULL, the key of the new item is returned.
 *    Note that key->data will point to temporary data. This pointer
 *    will be invalidated by subsequent hamsterdb API calls. See
 *    @a HAM_KEY_USER_ALLOC on how to change this behaviour.
 * @param record Optional pointer to a @ref ham_record_t structure. If this
 *    pointer is not NULL, the record of the new item is returned.
 *    Note that record->data will point to temporary data. This pointer
 *    will be invalidated by subsequent hamsterdb API calls. See
 *    @ref HAM_RECORD_USER_ALLOC on how to change this behaviour.
 * @param flags Optional flags for searching, which can be combined with
 *    bitwise OR. Possible flags are:
 *    <ul>
 *    <li>@ref HAM_FIND_LT_MATCH </li> Cursor 'find' flag 'Less Than': the
 *        cursor is moved to point at the last record which' key
 *        is less than the specified key. When such a record cannot
 *        be located, an error is returned.
 *    <li>@ref HAM_FIND_GT_MATCH </li> Cursor 'find' flag 'Greater Than':
 *        the cursor is moved to point at the first record which' key is
 *        larger than the specified key. When such a record cannot be
 *        located, an error is returned.
 *    <li>@ref HAM_FIND_LEQ_MATCH </li> Cursor 'find' flag 'Less or EQual':
 *        the cursor is moved to point at the record which' key matches
 *        the specified key and when such a record is not available
 *        the cursor is moved to point at the last record which' key
 *        is less than the specified key. When such a record cannot be
 *        located, an error is returned.
 *    <li>@ref HAM_FIND_GEQ_MATCH </li> Cursor 'find' flag 'Greater or
 *        Equal': the cursor is moved to point at the record which' key
 *        matches the specified key and when such a record
 *        is not available the cursor is moved to point at the first
 *        record which' key is larger than the specified key.
 *        When such a record cannot be located, an error is returned.
 *    <li>@ref HAM_FIND_NEAR_MATCH </li> Cursor 'find' flag 'Any Near Or
 *        Equal': the cursor is moved to point at the record which'
 *        key matches the specified key and when such a record is
 *        not available the cursor is moved to point at either the
 *        last record which' key is less than the specified key or
 *        the first record which' key is larger than the specified
 *        key, whichever of these records is located first.
 *        When such records cannot be located, an error is returned.
 *    <li>@ref HAM_DIRECT_ACCESS </li> Only for In-Memory Databases and
 *        not if Transactions are enabled!
 *        Returns a direct pointer to the data blob stored by the
 *        hamsterdb engine. This pointer must not be resized or freed,
 *        but the data in this memory can be modified.
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
 * combined @ref HAM_FIND_LEQ_MATCH and @ref HAM_FIND_GEQ_MATCH flags.
 *
 * Note that these flags may be bitwise OR-ed to form functional combinations.
 *
 * @ref HAM_FIND_LEQ_MATCH, @ref HAM_FIND_GEQ_MATCH and
 * @ref HAM_FIND_LT_MATCH, @ref HAM_FIND_GT_MATCH
 *
 * @return @ref HAM_SUCCESS upon success. Mind the remarks about the
 *     @a key flags being adjusted and the useful invocation of
 *     @ref ham_key_get_approximate_match_type() afterwards.
 * @return @ref HAM_INV_PARAMETER if @a db, @a key or @a record is NULL
 * @return @ref HAM_CURSOR_IS_NIL if the Cursor does not point to an item
 * @return @ref HAM_KEY_NOT_FOUND if no suitable @a key (record) exists
 * @return @ref HAM_INV_PARAMETER if @a HAM_DIRECT_ACCESS is specified,
 *        but the Database is not an In-Memory Database.
 * @return @ref HAM_INV_PARAMETER if @a HAM_DIRECT_ACCESS and
 *        @a HAM_ENABLE_TRANSACTIONS were both specified.
 * @return @ref HAM_INV_PARAMETER if @ref HAM_PARTIAL is set but record
 *        size is <= 8 or Transactions are enabled
 * @return @ref HAM_TXN_CONFLICT if the same key was inserted in another
 *        Transaction which was not yet committed or aborted
 *
 * @sa HAM_KEY_USER_ALLOC
 * @sa ham_key_t
 * @sa HAM_RECORD_USER_ALLOC
 * @sa ham_record_t
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_cursor_find(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, uint32_t flags);

/* internal flag */
#define HAM_FIND_EXACT_MATCH            0x4000

/**
 * Cursor 'find' flag 'Less Than': return the nearest match below the
 * given key, whether an exact match exists or not.
 */
#define HAM_FIND_LT_MATCH               0x1000

/**
 * Cursor 'find' flag 'Greater Than': return the nearest match above the
 * given key, whether an exact match exists or not.
 */
#define HAM_FIND_GT_MATCH               0x2000

/**
 * Cursor 'find' flag 'Less or EQual': return the nearest match below the
 * given key, when an exact match does not exist.
 *
 * May be combined with @ref HAM_FIND_GEQ_MATCH to accept any 'near' key, or
 * you can use the @ref HAM_FIND_NEAR_MATCH constant as a shorthand for that.
 */
#define HAM_FIND_LEQ_MATCH      (HAM_FIND_LT_MATCH | HAM_FIND_EXACT_MATCH)

/**
 * Cursor 'find' flag 'Greater or Equal': return the nearest match above
 * the given key, when an exact match does not exist.
 *
 * May be combined with @ref HAM_FIND_LEQ_MATCH to accept any 'near' key,
 * or you can use the @ref HAM_FIND_NEAR_MATCH constant as a shorthand for that.
 */
#define HAM_FIND_GEQ_MATCH      (HAM_FIND_GT_MATCH | HAM_FIND_EXACT_MATCH)

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
 * possible combination of the combined @ref HAM_FIND_LEQ_MATCH and
 * @ref HAM_FIND_GEQ_MATCH flags.
 */
#define HAM_FIND_NEAR_MATCH     (HAM_FIND_LT_MATCH | HAM_FIND_GT_MATCH  \
                                  | HAM_FIND_EXACT_MATCH)

/**
 * Inserts a Database item and points the Cursor to the inserted item
 *
 * This function inserts a key/record pair as a new Database item.
 * If the key already exists in the Database, error @ref HAM_DUPLICATE_KEY
 * is returned.
 *
 * If you wish to overwrite an existing entry specify the
 * flag @ref HAM_OVERWRITE. The use of this flag is not allowed in combination
 * with @ref HAM_DUPLICATE.
 *
 * If you wish to insert a duplicate key specify the flag @ref HAM_DUPLICATE.
 * (In order to use duplicate keys, the Database has to be created with
 * @ref HAM_ENABLE_DUPLICATE_KEYS.)
 * By default, the duplicate key is inserted after all other duplicate keys
 * (see @ref HAM_DUPLICATE_INSERT_LAST). This behaviour can be overwritten by
 * specifying @ref HAM_DUPLICATE_INSERT_FIRST, @ref HAM_DUPLICATE_INSERT_BEFORE
 * or @ref HAM_DUPLICATE_INSERT_AFTER.
 *
 * You can write only portions of the record by specifying the flag
 * @ref HAM_PARTIAL. In this case, hamsterdb will write <b>partial_size</b>
 * bytes of the record data at offset <b>partial_offset</b>. If necessary, the
 * record data will grow. Gaps will be filled with null-bytes, if the record
 * did not yet exist.
 *
 * @ref HAM_PARTIAL is not allowed if record->size is <= 8 or if Transactions
 * are enabled. In such a case, @ref HAM_INV_PARAMETER is returned.
 *
 * Specify the flag @ref HAM_HINT_APPEND if you insert sequential data
 * and the current @a key is greater than any other key in this Database.
 * In this case hamsterdb will optimize the insert algorithm. hamsterdb will
 * verify that this key is the greatest; if not, it will perform a normal
 * insert. This flag is the default for Record Number Databases.
 *
 * Specify the flag @ref HAM_HINT_PREPEND if you insert sequential data
 * and the current @a key is lower than any other key in this Database.
 * In this case hamsterdb will optimize the insert algorithm. hamsterdb will
 * verify that this key is the lowest; if not, it will perform a normal
 * insert.
 *
 * After inserting, the Cursor will point to the new item. If inserting
 * the item failed, the Cursor is not modified.
 *
 * Record Number Databases (created with @ref HAM_RECORD_NUMBER32 or
 * @ref HAM_RECORD_NUMBER64) expect either an empty @a key (with a size of
 * 0 and data pointing to NULL), or a user-supplied key (with key.flag
 * @ref HAM_KEY_USER_ALLOC and a valid data pointer).
 * If key.size is 0 and key.data is NULL, hamsterdb will temporarily
 * allocate memory for key->data, which will then point to an 4-byte (or 8-byte)
 * unsigned integer.
 *
 * @param cursor A valid Cursor handle
 * @param key A valid key structure
 * @param record A valid record structure
 * @param flags Optional flags for inserting the item, combined with
 *    bitwise OR. Possible flags are:
 *    <ul>
 *    <li>@ref HAM_OVERWRITE. If the @a key already exists, the record is
 *        overwritten. Otherwise, the key is inserted. Not allowed in
 *        combination with @ref HAM_DUPLICATE.
 *    <li>@ref HAM_DUPLICATE. If the @a key already exists, a duplicate
 *        key is inserted. Same as @ref HAM_DUPLICATE_INSERT_LAST. Not
 *        allowed in combination with @ref HAM_DUPLICATE.
 *    <li>@ref HAM_DUPLICATE_INSERT_BEFORE. If the @a key already exists,
 *        a duplicate key is inserted before the duplicate pointed
 *        to by the Cursor. Not allowed if duplicate sorting is enabled.
 *    <li>@ref HAM_DUPLICATE_INSERT_AFTER. If the @a key already exists,
 *        a duplicate key is inserted after the duplicate pointed
 *        to by the Cursor. Not allowed if duplicate sorting is enabled.
 *    <li>@ref HAM_DUPLICATE_INSERT_FIRST. If the @a key already exists,
 *        a duplicate key is inserted as the first duplicate of
 *        the current key. Not allowed if duplicate sorting is enabled.
 *    <li>@ref HAM_DUPLICATE_INSERT_LAST. If the @a key already exists,
 *        a duplicate key is inserted as the last duplicate of
 *        the current key. Not allowed if duplicate sorting is enabled.
 *    <li>@ref HAM_HINT_APPEND. Hints the hamsterdb engine that the
 *        current key will compare as @e larger than any key already
 *        existing in the Database. The hamsterdb engine will verify
 *        this postulation and when found not to be true, will revert
 *        to a regular insert operation as if this flag was not
 *        specified. The incurred cost then is only one additional key
 *        comparison. Mutually exclusive with flag @ref HAM_HINT_PREPEND.
 *        This is the default for Record Number Databases.
 *    <li>@ref HAM_HINT_PREPEND. Hints the hamsterdb engine that the
 *        current key will compare as @e lower than any key already
 *        existing in the Database. The hamsterdb engine will verify
 *        this postulation and when found not to be true, will revert
 *        to a regular insert operation as if this flag was not
 *        specified. The incurred cost then is only one additional key
 *        comparison. Mutually exclusive with flag @ref HAM_HINT_APPEND.
 *    </ul>
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if @a key or @a record is NULL
 * @return @ref HAM_INV_PARAMETER if the Database is a Record Number Database
 *        and the key is invalid (see above)
 * @return @ref HAM_INV_PARAMETER if @ref HAM_PARTIAL is set but record
 *        size is <= 8 or Transactions are enabled
 * @return @ref HAM_INV_PARAMETER if the flags @ref HAM_OVERWRITE <b>and</b>
 *        @ref HAM_DUPLICATE were specified, or if @ref HAM_DUPLICATE
 *        was specified, but the Database was not created with
 *        flag @ref HAM_ENABLE_DUPLICATE_KEYS.
 * @return @ref HAM_WRITE_PROTECTED if you tried to insert a key to a read-only
 *        Database.
 * @return @ref HAM_INV_KEY_SIZE if the key size is different from
 *        the one specified with @a HAM_PARAM_KEY_SIZE
 * @return @ref HAM_INV_RECORD_SIZE if the record size is different from
 *        the one specified with @a HAM_PARAM_RECORD_SIZE
 * @return @ref HAM_CURSOR_IS_NIL if the Cursor does not point to an item
 * @return @ref HAM_TXN_CONFLICT if the same key was inserted in another
 *        Transaction which was not yet committed or aborted
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_cursor_insert(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, uint32_t flags);

/**
 * Erases the current key
 *
 * Erases a key from the Database. If the erase was
 * successful, the Cursor is invalidated and does no longer point to
 * any item. In case of an error, the Cursor is not modified.
 *
 * If the Database was opened with the flag @ref HAM_ENABLE_DUPLICATE_KEYS,
 * this function erases only the duplicate item to which the Cursor refers.
 *
 * @param cursor A valid Cursor handle
 * @param flags Unused, set to 0
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_INV_PARAMETER if @a cursor is NULL
 * @return @ref HAM_WRITE_PROTECTED if you tried to erase a key from a read-only
 *        Database
 * @return @ref HAM_CURSOR_IS_NIL if the Cursor does not point to an item
 * @return @ref HAM_TXN_CONFLICT if the same key was inserted in another
 *        Transaction which was not yet committed or aborted
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_cursor_erase(ham_cursor_t *cursor, uint32_t flags);

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
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_CURSOR_IS_NIL if the Cursor does not point to an item
 * @return @ref HAM_INV_PARAMETER if @a cursor or @a count is NULL
 * @return @ref HAM_TXN_CONFLICT if the same key was inserted in another
 *        Transaction which was not yet committed or aborted
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_cursor_get_duplicate_count(ham_cursor_t *cursor,
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
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_CURSOR_IS_NIL if the Cursor does not point to an item
 * @return @ref HAM_INV_PARAMETER if @a cursor or @a position is NULL
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_cursor_get_duplicate_position(ham_cursor_t *cursor,
            uint32_t *position);

/**
 * Returns the record size of the current key
 *
 * Returns the record size of the item to which the Cursor currently refers.
 *
 * @param cursor A valid Cursor handle
 * @param size Returns the record size, in bytes
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_CURSOR_IS_NIL if the Cursor does not point to an item
 * @return @ref HAM_INV_PARAMETER if @a cursor or @a size is NULL
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_cursor_get_record_size(ham_cursor_t *cursor, uint64_t *size);

/**
 * Closes a Database Cursor
 *
 * Closes a Cursor and frees allocated memory. All Cursors
 * should be closed before closing the Database (see @ref ham_db_close).
 *
 * @param cursor A valid Cursor handle
 *
 * @return @ref HAM_SUCCESS upon success
 * @return @ref HAM_CURSOR_IS_NIL if the Cursor does not point to an item
 * @return @ref HAM_INV_PARAMETER if @a cursor is NULL
 *
 * @sa ham_db_close
 */
HAM_EXPORT ham_status_t HAM_CALLCONV
ham_cursor_close(ham_cursor_t *cursor);

/**
 * @}
 */

#ifdef __cplusplus
} // extern "C"
#endif

#endif /* HAM_HAMSTERDB_H */
