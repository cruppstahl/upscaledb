/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 * Include file for hamsterdb.
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
    ham_size_t size;

    /** The data of the key */
    void *data;

    /** The flags of the key */
    ham_u32_t flags;

    /** For internal use */
    ham_u32_t _flags;
} ham_key_t;

/** Flag for @a ham_key_t in combination with  @a ham_cursor_move) */
#define HAM_KEY_USER_ALLOC      1

/*
 * hamsterdb error- and status codes.
 * These codes are always negative, so we have no conflicts with errno.h
 *
 * @defgroup ham_status_codes hamsterdb Status Codes
 * @{
 */

/** Success, no error */
#define HAM_SUCCESS                  (  0)
/** Failed to read the database file */
#define HAM_SHORT_READ               ( -1)
/** Failed to write the database file */
#define HAM_SHORT_WRITE              ( -2)
/** Invalid key size */
#define HAM_INV_KEYSIZE              ( -3)
/** Invalid page size (not a multiple of 1024?) */
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
/** Tried to modify the database, but the file was opened read-only */
#define HAM_DB_READ_ONLY             (-15)
/** Database record not found */
#define HAM_BLOB_NOT_FOUND           (-16)
/** Prefix comparison function needs more data */
#define HAM_PREFIX_REQUEST_FULLKEY   (-17)
/** Generic file I/O error */
#define HAM_IO_ERROR                 (-18)
/** Database cache is full */
#define HAM_CACHE_FULL               (-19)
/** Function is not implemented */
#define HAM_NOT_IMPLEMENTED          (-20)
/** Database file not found */
#define HAM_FILE_NOT_FOUND           (-21)
/** Cursor does not point to a valid database item */
#define HAM_CURSOR_IS_NIL           (-100)

/*
 * @}
 */

/*
 * @defgroup ham_static hamsterdb Static Functions
 * @{
 */

/**
 * A typedef for a custom error handler function
 * 
 * @param message The error message.
 */
typedef void (*ham_errhandler_fun)(const char *message);

/**
 * Set a global error handler
 * 
 * This handler will receive <b>all</b> debug messages which are emitted 
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
 * Get a descriptive error string from a hamsterdb status code
 *
 * @param status The hamsterdb status code
 *
 * @return Returns a pointer to a descriptive error string.
 */
HAM_EXPORT const char *
ham_strerror(ham_status_t status);

/**
 * Get the version of the hamsterdb library
 */
HAM_EXPORT void
ham_get_version(ham_u32_t *major, ham_u32_t *minor,
        ham_u32_t *revision);

/*
 * @}
 */

/*
 * @defgroup ham_db hamsterdb Database Functions
 * @{
 */

/**
 * Allocate a ham_db_t handle
 *
 * @param db Pointer to a pointer which is allocated 
 *
 * @return @a HAM_SUCCESS on success
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated
 */
HAM_EXPORT ham_status_t
ham_new(ham_db_t **db);

/**
 * Delete a ham_db_t handle
 *
 * This function frees the ham_db_t structure. It does not close the 
 * database. Call this function <b>AFTER</b> you have closed the 
 * database with @a ham_close, or you will lose your data!
 *
 * @param db A valid database handle.
 *
 * @return This function always returns @a HAM_SUCCESS.
 */
HAM_EXPORT ham_status_t
ham_delete(ham_db_t *db);

/**
 * Open a database
 *
 * @param db A valid database handle.
 * @param filename The filename of the database file.
 * @param flags Optional flags for opening the database, combined with 
 *        bitwise OR. See the documentation of @a ham_open_ex
 *        for the allowed flags.
 *
 * @return @a HAM_SUCCESS on success
 * @return @a HAM_INV_PARAMETER if the @a db pointer is NULL or an 
 *              invalid combination of flags was specified
 * @return @a HAM_FILE_NOT_FOUND if the file does not exist
 * @return @a HAM_IO_ERROR if the file could not be opened or reading failed
 * @return @a HAM_INV_FILE_VERSION if the database version is not 
 *              compatible with the library version
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated
 */
HAM_EXPORT ham_status_t
ham_open(ham_db_t *db, const char *filename, ham_u32_t flags);

/**
 * Open a database - extended version
 *
 * @param db A valid database handle.
 * @param filename The filename of the database file.
 * @param flags Optional flags for opening the database, combined with 
 *        bitwise OR. Possible flags are:
 *      <ul>
 *       <li>@a HAM_READ_ONLY</li> Opens the file for reading only. 
 *            Operations which need write access (i.e. @a ham_insert) will 
 *            return error @a HAM_DB_READ_ONLY.
 *       <li>@a HAM_WRITE_THROUGH</li> Immediately write modified pages 
 *            to the disk. This slows down all database operations, but 
 *            could save the database integrity in case of a system crash. 
 *       <li>@a HAM_DISABLE_VAR_KEYLEN</li> Do not allow the use of variable 
 *            length keys. Inserting a key, which is larger than the 
 *            B+Tree index key size, returns error @a HAM_INV_KEYSIZE.
 *       <li>@a HAM_DISABLE_MMAP</li> Do not use memory mapped files for I/O. 
 *            Per default, hamsterdb checks if it can use mmap, 
 *            since mmap is faster then read/write. For performance 
 *            reasons, this flag should not be used.
 *       <li>@a HAM_CACHE_STRICT</li> Do not allow the cache to grow larger 
 *            than the cachesize. If a database operation needs to resize the 
 *            cache, it will return with error @a HAM_CACHE_FULL.
 *            If the flag is not set, the cache is allowed to allocate
 *            more pages then the maximum cachesize, but only if it's 
 *            necessary and only for a short time. 
 *       <li>@a HAM_DISABLE_FREELIST_FLUSH</li> Do not immediately writeback 
 *            modified freelist pages. This flag leads to small 
 *            performance improvements, but comes with additional
 *            risk in case of a system crash or program crash.
 *      </ul>
 *
 * @param cachesize The size of the database cache, in bytes. Set to 0
 *        for the default size (defined in src/config.h as
 *        HAM_DEFAULT_CACHESIZE - usually 256kb)
 *
 * @return @a HAM_SUCCESS on success
 * @return @a HAM_INV_PARAMETER if the @a db pointer is NULL or an 
 *              invalid combination of flags was specified
 * @return @a HAM_FILE_NOT_FOUND if the file does not exist
 * @return @a HAM_IO_ERROR if the file could not be opened or reading failed
 * @return @a HAM_INV_FILE_VERSION if the database version is not 
 *              compatible with the library version
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated
 */
HAM_EXPORT ham_status_t
ham_open_ex(ham_db_t *db, const char *filename,
        ham_u32_t flags, ham_size_t cachesize);

/**
 * Create a database
 *
 * @param db A valid database handle.
 * @param filename The filename of the database file. If the file already
 *          exists, it is overwritten. Can be NULL if you create an 
 *          in-memory-database.
 * @param flags Optional flags for opening the database, combined with 
 *        bitwise OR. For allowed flags, see @a ham_create_ex.
 *
 * @return @a HAM_SUCCESS on success
 * @return @a HAM_INV_PARAMETER if the @a db pointer is NULL or an 
 *              invalid combination of flags was specified
 * @return @a HAM_IO_ERROR if the file could not be opened or 
 *              reading/writing failed
 * @return @a HAM_INV_FILE_VERSION if the database version is not 
 *              compatible with the library version
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated
 *
 */
HAM_EXPORT ham_status_t
ham_create(ham_db_t *db, const char *filename,
        ham_u32_t flags, ham_u32_t mode);

/**
 * Create a database - extended version
 *
 * @param db A valid database handle.
 * @param filename The filename of the database file. If the file already
 *          exists, it will be overwritten. Can be NULL if you create an
 *          in-memory-database.
 * @param flags Optional flags for opening the database, combined with 
 *        bitwise OR. Possible flags are:
 *
 *      <ul>
 *       <li>@a HAM_WRITE_THROUGH</li> Immediately write modified pages to the 
 *            disk. This slows down all database operations, but might
 *            save the database integrity in case of a system crash. 
 *       <li>@a HAM_USE_BTREE</li> Use a B+Tree for the index structure. 
 *            Currently, this is your only choice, but future releases 
 *            of hamsterdb will offer additional index structures, 
 *            i.e. hash tables. Enabled by default.
 *       <li>@a HAM_DISABLE_VAR_KEYLEN</li> Do not allow the use of variable 
 *            length keys. Inserting a key, which is larger than the 
 *            B+Tree index key size, returns error @a HAM_INV_KEYSIZE.
 *       <li>@a HAM_IN_MEMORY_DB</li> Create an in-memory-database. No file 
 *            will be created, and the database contents are lost after 
 *            the database is closed. The @a filename parameter can 
 *            be NULL. Do <b>NOT</b> use in combination with 
 *            @a HAM_CACHE_STRICT and do NOT specify a cachesize other 
 *            than 0.
 *       <li>@a HAM_DISABLE_MMAP</li> Do not use memory mapped files for I/O. 
 *            Per default, hamsterdb checks if it can use mmap, 
 *            since mmap is faster then read/write. For performance 
 *            reasons, this flag should not be used.
 *       <li>@a HAM_CACHE_STRICT</li> Do not allow the cache to grow larger 
 *            than the cachesize. If a database operation needs to resize the 
 *            cache, it will return with error @a HAM_CACHE_FULL.
 *            If the flag is not set, the cache is allowed to allocate
 *            more pages then the maximum cachesize, but only if it's 
 *            necessary and only for a short time. 
 *       <li>@a HAM_DISABLE_FREELIST_FLUSH</li> Do not immediately writeback 
 *            modified freelist pages. This flag leads to small 
 *            performance improvements, but comes with additional
 *            risk in case of a system crash or program crash.
 *      </ul>
 *
 * @param keysize The size of the keys in the B+Tree index. Set to 0 for 
 *        the default size (default size: 21 bytes).
 * @param pagesize The size of the file page, in bytes. Set to 0 for 
 *        the default size (recommended). The default size depends on 
 *        your hardware and operating system. Page sizes must be a multiple
 *        of 1024.
 * @param cachesize The size of the database cache, in bytes. Set to 0
 *        for the default size (defined in src/config.h as
 *        HAM_DEFAULT_CACHESIZE - usually 256kb)
 *
 * @return @a HAM_SUCCESS on success
 * @return @a HAM_INV_PARAMETER if the @a db pointer is NULL or an 
 *              invalid combination of flags was specified
 * @return @a HAM_IO_ERROR if the file could not be opened or 
 *              reading/writing failed
 * @return @a HAM_INV_FILE_VERSION if the database version is not 
 *              compatible with the library version
 * @return @a HAM_OUT_OF_MEMORY if memory could not be allocated
 * @return @a HAM_INV_PAGESIZE if the pagesize is not a multiple of 1024
 * @return @a HAM_INV_KEYSIZE if the keysize is too large (at least 4 
 *              keys must fit in a page)
 *
 */
HAM_EXPORT ham_status_t
ham_create_ex(ham_db_t *db, const char *filename,
        ham_u32_t flags, ham_u32_t mode, ham_u32_t pagesize,
        ham_u16_t keysize, ham_size_t cachesize);

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

/**
 * Get the last error code
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
 * Set the prefix comparison function
 *
 * The prefix comparison function is called when an index uses
 * keys with variable length, and one of the two keys is loaded only
 * partially.
 *
 * @param db A valid database handle.
 * @param foo A pointer to the prefix compare function.
 *
 * @return @a HAM_SUCCESS on success
 * @return @a HAM_INV_PARAMETER if one of the parameters is NULL
 */
HAM_EXPORT ham_status_t
ham_set_prefix_compare_func(ham_db_t *db, ham_prefix_compare_func_t foo);

/**
 * Set the comparison function
 *
 * The comparison function compares two index keys. It returns -1 if the 
 * first key is smaller, +1 if the second key is smaller or 0 if both
 * keys are equal.
 *
 * The default comparison function uses memcmp to compare the keys.
 *
 * @param db A valid database handle.
 * @param foo A pointer to the compare function.
 *
 * @return @a HAM_SUCCESS on success
 * @return @a HAM_INV_PARAMETER if one of the parameters is NULL
 */
HAM_EXPORT ham_status_t
ham_set_compare_func(ham_db_t *db, ham_compare_func_t foo);

/**
 * Search an item in the database
 *
 * This function searches the database for the @a key; if the key
 * is found, the record of this item is returned in @a record and 
 * @a HAM_SUCCESS is returned. If the key is not found, the function
 * returns with @a HAM_KEY_NOT_FOUND.
 *
 * Before using a ham_record_t structure, it should be initialized with 
 * zeroes, i.e. with the C library routines memset(3) or bzero(2). 
 *
 * If the function returns successfully, the @a record pointer is 
 * initialized with the size of the record (in record.size) and a 
 * pointer to the actual record data (in record.data). If the record 
 * is empty, @a size is 0 and @a data is NULL.
 *
 * The @a data pointer is a temporary pointer and will be overwritten 
 * by subsequent hamsterdb API calls. You can alter this behaviour by 
 * allocating the @a data pointer in the application and setting 
 * record.flags to @a HAM_RECORD_USER_ALLOC. Make sure that the allocated
 * buffer is large enough. 
 *
 * @param db A valid database handle.
 * @param reserved A reserved value; set to NULL.
 * @param key The key of the item.
 * @param record The record of the item.
 * @param flags Search flags; unused, set to 0.
 *
 * @return @a HAM_SUCCESS on success
 * @return @a HAM_INV_PARAMETER if @a db or @a key or @a record is NULL
 * @return @a HAM_KEY_NOT_FOUND if the @a key does not exist
 */
HAM_EXPORT ham_status_t
ham_find(ham_db_t *db, void *reserved, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags);

/**
 * Insert a database item
 *
 * This function inserts a key/record pair as a new database item.
 *
 * If the key already exists in the database, error @a HAM_DUPLICATE_KEY
 * is returned. If you wish to overwrite an existing entry, specify the
 * flag @a HAM_OVERWRITE.
 *
 * @param db A valid database handle.
 * @param reserved A reserved value; set to NULL.
 * @param key The key of the new item.
 * @param record The record of the new item.
 * @param flags Insert flags. Currently, there's only one flag:
 *         @a HAM_OVERWRITE If the @a key already exists, the record is 
 *              overwritten; otherwise, the key is inserted.
 *
 * @return @a HAM_SUCCESS on success
 * @return @a HAM_INV_PARAMETER if @a db or @a key or @a record is NULL
 * @return @a HAM_DB_READ_ONLY if you tried to insert a key in a read-only
 *              database
 * @return @a HAM_INV_KEYSIZE if the key's size is larger than the keysize
 *              parameter specified for @a ham_create_ex and variable 
 *              key sizes are disabled (see @a HAM_DISABLE_VAR_KEYLEN)
 *              OR if the keysize parameter specified for @a ham_create_ex 
 *              is smaller than 8
 */
HAM_EXPORT ham_status_t
ham_insert(ham_db_t *db, void *reserved, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags);

/** Flag for @a ham_insert and @a ham_cursor_insert */
#define HAM_OVERWRITE               1

/**
 * Erase a database item
 *
 * This function erases a database item. If the item with the @a key
 * does not exist, @a HAM_KEY_NOT_FOUND is returned.
 *
 * @param db A valid database handle.
 * @param reserved A reserved value; set to NULL.
 * @param key The key of the new item.
 * @param flags Erase flags; unused, set to 0.
 *
 * @return @a HAM_SUCCESS on success
 * @return @a HAM_INV_PARAMETER if @a db or @a key is NULL
 * @return @a HAM_DB_READ_ONLY if you tried to erase a key from a read-only
 *              database
 * @return @a HAM_KEY_NOT_FOUND if the key was not found
 */
HAM_EXPORT ham_status_t
ham_erase(ham_db_t *db, void *reserved, ham_key_t *key,
        ham_u32_t flags);

/**
 * Flush the database
 *
 * This function flushes the database cache and writes the whole file 
 * to disk. 
 *
 * @remark Since in-memory-databases do not have a file on disk, the 
 * function will have no effect and return @a HAM_SUCCESS.
 *
 * @param db A valid database handle.
 * @param flags Flush flags; unused, set to 0.
 *
 * @return @a HAM_SUCCESS on success
 * @return @a HAM_INV_PARAMETER if @a db is NULL
 */
HAM_EXPORT ham_status_t
ham_flush(ham_db_t *db, ham_u32_t flags);

/**
 * Close a database
 *
 * This function flushes the database, and then closes the file handle. 
 * It does not free the memory resources allocated in the @a db handle - 
 * use @a ham_delete to free @a db. 
 *
 * The application should close all database cursors before closing 
 * the database.
 *
 * @param db A valid database handle.
 *
 * @return @a HAM_SUCCESS on success
 * @return @a HAM_INV_PARAMETER if @a db is NULL
 */
HAM_EXPORT ham_status_t
ham_close(ham_db_t *db);

/*
 * @}
 */

/*
 * @defgroup ham_cursor hamsterdb Cursor Functions
 * @{
 */

/**
 * Create a database cursor
 *
 * This function creates a new database cursor. Cursors can be used to 
 * traverse the database from start to end or from end to start. 
 * A created cursor does not point to any item in the database. 
 *
 * The application should close all database cursors before closing 
 * the database.
 *
 * @param db A valid database handle.
 * @param reserved A reserved value; set to NULL
 * @param flags Flags for creating the cursor; unused, set to 0
 * @param cursor A pointer to a pointer, which is allocated with the
 *          new cursor handle
 *
 * @return @a HAM_SUCCESS on success
 * @return @a HAM_INV_PARAMETER if @a db or cursor is NULL
 * @return @a HAM_OUT_OF_MEMORY if the new structure could not be allocated
 */
HAM_EXPORT ham_status_t
ham_cursor_create(ham_db_t *db, void *reserved, ham_u32_t flags,
        ham_cursor_t **cursor);

/**
 * Clone a database cursor
 *
 * This function clones an existing cursor. The new cursor will point to 
 * exactly the same item as the old cursor.
 *
 * @param src The existing cursor
 * @param dest A pointer to a pointer, which is allocated with the
 *          cloned cursor handle
 *
 * @return @a HAM_SUCCESS on success
 * @return @a HAM_INV_PARAMETER if @a src or dest is NULL
 * @return @a HAM_OUT_OF_MEMORY if the new structure could not be allocated
 */
HAM_EXPORT ham_status_t
ham_cursor_clone(ham_cursor_t *src, ham_cursor_t **dest);

/**
 * Moves the cursor
 *
 * This function moves the cursor. You can specify the direction in the 
 * flags. After the move, it returns the key and the record of the item.
 *
 * @param cursor A valid cursor handle.
 * @param key An optional pointer to a @a ham_key_t structure; if this 
 *      pointer is not NULL, the key of the new item is returned. 
 *      Note that key->data will point to temporary data; this pointer
 *      will be invalidated by subsequent hamsterdb API calls. See 
 *      @a HAM_KEY_USER_ALLOC on how to change this behaviour.
 * @param record An optional pointer to a @a ham_record_t structure; if this 
 *      pointer is not NULL, the record of the new item is returned. 
 *      Note that record->data will point to temporary data; this pointer
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
 * @return @a HAM_SUCCESS on success
 * @return @a HAM_INV_PARAMETER if @a cursor is NULL
 * @return @a HAM_CURSOR_IS_NIL if the cursor does not point to an item, but
 *              key and/or record were requested
 * @return @a HAM_KEY_NOT_FOUND if the cursor points to the first (or last)
 *              item, and a move to the previous (or next) item was 
 *              attempted
 */
HAM_EXPORT ham_status_t
ham_cursor_move(ham_cursor_t *cursor, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags);

/** flag for @a ham_cursor_move */
#define HAM_CURSOR_FIRST            1

/** flag for @a ham_cursor_move */
#define HAM_CURSOR_LAST             2

/** flag for @a ham_cursor_move */
#define HAM_CURSOR_NEXT             4

/** flag for @a ham_cursor_move */
#define HAM_CURSOR_PREVIOUS         8

/**
 * Replace the current record
 *
 * This function replaces the record of the current item. 
 *
 * @param cursor A valid cursor handle.
 * @param record A valid record structure.
 * @param flags Flags for replacing the item; unused, set to 0
 *
 * @return @a HAM_SUCCESS on success
 * @return @a HAM_INV_PARAMETER if @a cursor or @a record is NULL
 * @return @a HAM_CURSOR_IS_NIL if the cursor does not point to an item, but
 *              key and/or record were requested
 */
HAM_EXPORT ham_status_t
ham_cursor_replace(ham_cursor_t *cursor, ham_record_t *record,
            ham_u32_t flags);

/**
 * Find a key and position the cursor on this key
 *
 * This function searches for an item in the database and positions the 
 * cursor on this item. If the item could not be found, the cursor is 
 * not modified.
 *
 * @param cursor A valid cursor handle.
 * @param key A valid key structure.
 * @param flags Flags for searching the item; unused, set to 0
 *
 * @return @a HAM_SUCCESS on success
 * @return @a HAM_INV_PARAMETER if @a cursor or @a key is NULL
 * @return @a HAM_KEY_NOT_FOUND if the requested key was not found
 */
HAM_EXPORT ham_status_t
ham_cursor_find(ham_cursor_t *cursor, ham_key_t *key, ham_u32_t flags);

/**
 * Insert (or update) a key 
 *
 * This function inserts a key in the database. If the flag @a HAM_OVERWRITE
 * is specified, an already existing item with this key is overwritten;
 * otherwise, error @a HAM_DUPLICATE_ITEM is returned. 
 * In case of an error, the cursor is not modified.
 *
 * @param cursor A valid cursor handle.
 * @param key A valid key structure.
 * @param record A valid record structure.
 * @param flags Flags for inserting the item. 
 *         @a HAM_OVERWRITE If the @a key already exists, the record is 
 *              overwritten; otherwise, the key is inserted.
 *
 * @return @a HAM_SUCCESS on success
 * @return @a HAM_INV_PARAMETER if @a db or @a key or @a record is NULL
 * @return @a HAM_DB_READ_ONLY if you tried to insert a key in a read-only
 *              database
 * @return @a HAM_INV_KEYSIZE if the key's size is larger than the keysize
 *              parameter specified for @a ham_create_ex and variable 
 *              key sizes are disabled (see @a HAM_DISABLE_VAR_KEYLEN)
 *              OR if the keysize parameter specified for @a ham_create_ex 
 *              is smaller than 8
 */
HAM_EXPORT ham_status_t
ham_cursor_insert(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags);

/**
 * Erases the key
 *
 * This function deletes a key from the database. If the erase was 
 * successfull, the cursor is invalidated. On error, the cursor is not
 * modified.
 *
 * @param cursor A valid cursor handle.
 * @param flags Erase flags; unused, set to 0.
 *
 * @return @a HAM_SUCCESS on success
 * @return @a HAM_INV_PARAMETER if @a db or @a key is NULL
 * @return @a HAM_DB_READ_ONLY if you tried to erase a key from a read-only
 *              database
 * @return @a HAM_KEY_NOT_FOUND if the key was not found
 */
HAM_EXPORT ham_status_t
ham_cursor_erase(ham_cursor_t *cursor, ham_u32_t flags);

/**
 * Close a database cursor
 *
 * This function closes a cursor and frees allocated memory. All cursors 
 * should be closed before closing the database (see @a ham_close);
 *
 * @param cursor A valid cursor handle.
 * @return @a HAM_SUCCESS on success
 * @return @a HAM_INV_PARAMETER if @a db or @a key is NULL
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
