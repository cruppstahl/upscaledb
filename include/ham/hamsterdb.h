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

/**
 * Flag for @a ham_record_t
 * 
 * Data points to memory which is allocated by the user
 */
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

/**
 * Flag for @a ham_key_t
 * 
 * Data points to memory which is allocated by the user (only makes sense
 * when querying keys with @a ham_cursor_move).
 */
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
/** Failed to read the file */
#define HAM_SHORT_READ               ( -1)
/** Failed to write the file */
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
/** Invalid file header */
#define HAM_INV_FILE_HEADER          ( -9)
/** Invalid file version */
#define HAM_INV_FILE_VERSION         (-10)
/** Database key not found */
#define HAM_KEY_NOT_FOUND            (-11)
/** Key already exists */
#define HAM_DUPLICATE_KEY            (-12)
/** Internal database integrity violated */
#define HAM_INTEGRITY_VIOLATED       (-13)
/** Internal hamsterdb error */
#define HAM_INTERNAL_ERROR           (-14)
/** Database was opened read-only */
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
 * @return Returns a C string with a descriptive error string.
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
 * @param db Pointer to a pointer which is allocated with the structure
 *
 * @return @a HAM_SUCCESS on success
 * TODO TODO TODO mehr fehlercodes?
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
 * @return @a HAM_SUCCESS on success
 * TODO TODO TODO mehr fehlercodes?
 */
HAM_EXPORT ham_status_t
ham_delete(ham_db_t *db);

/**
 * Open a database
 *
 * @param db A valid database handle.
 * @param filename The filename of the database file.
 * @param flags Optional flags for opening the database, combined with 
 *        bitwise OR. 
 *
 * @return @a HAM_SUCCESS on success
 * TODO TODO TODO mehr fehlercodes?
 * TODO TODO TODO flags beschreiben
 */
HAM_EXPORT ham_status_t
ham_open(ham_db_t *db, const char *filename, ham_u32_t flags);

/**
 * Open a database - extended version
 *
 * @param db A valid database handle.
 * @param filename The filename of the database file.
 * @param flags Optional flags for opening the database, combined with 
 *        bitwise OR. 
 * @param cachesize The size of the database cache, in bytes.
 *
 * @return @a HAM_SUCCESS on success
 * TODO TODO TODO mehr fehlercodes?
 * TODO TODO TODO flags beschreiben
 */
HAM_EXPORT ham_status_t
ham_open_ex(ham_db_t *db, const char *filename,
        ham_u32_t flags, ham_size_t cachesize);

/**
 * Create a database
 *
 * @param db A valid database handle.
 * @param filename The filename of the database file.
 * @param flags Optional flags for opening the database, combined with 
 *        bitwise OR. 
 *
 * @return @a HAM_SUCCESS on success
 * TODO TODO TODO mehr fehlercodes?
 * TODO TODO TODO flags beschreiben
 * TODO TODO TODO was passiert wenn die datei schon existiert? 
 *    wird sie überschrieben?
 *
 * @remark If you create an in-memory-database (flag HAM_IN_MEMORY_DB),
 * you are NOT allowed to set the flag HAM_CACHE_STRICT or to use
 * a cache size != 0!
 */
HAM_EXPORT ham_status_t
ham_create(ham_db_t *db, const char *filename,
        ham_u32_t flags, ham_u32_t mode);

/**
 * create a new database - extended version
 *
 * Create a database - extended version
 *
 * @param db A valid database handle.
 * @param filename The filename of the database file.
 * @param flags Optional flags for opening the database, combined with 
 *        bitwise OR. 
 * @param keysize The size of the keys in the B+Tree index. Set to 0 for 
 *        the default size (usually about 9 to 15 bytes). TODO 
 * @param pagesize The size of the file page, in bytes. Set to 0 for 
 *        the default size (recommended). The default size depends on 
 *        your hardware and operating system. Page sizes must be a multiple
 *        of 1024.
 * @param cachesize The size of the database cache, in bytes.
 *
 * @return @a HAM_SUCCESS on success
 * TODO TODO TODO mehr fehlercodes?
 * TODO TODO TODO flags beschreiben
 * TODO TODO TODO was passiert wenn die datei schon existiert? 
 *    wird sie überschrieben?
 *
 * @remark If you create an in-memory-database (flag HAM_IN_MEMORY_DB),
 * you are NOT allowed to set the flag HAM_CACHE_STRICT or to use
 * a cache size != 0!
 *
 */
HAM_EXPORT ham_status_t
ham_create_ex(ham_db_t *db, const char *filename,
        ham_u32_t flags, ham_u32_t mode, ham_u32_t pagesize,
        ham_u16_t keysize, ham_size_t cachesize);

/**
 * Flag for @a ham_open, @a ham_open_ex, @a ham_create, @a ham_create_ex
 *
 * When file pages are modified, write them imediately to the file. This 
 * slows down all database operations, but might save the database integrity
 * in case of a system crash. 
 * 
 * The flag is disabled by default.
 */
#define HAM_WRITE_THROUGH            0x00000001

/**
 * Flag for @a ham_open, @a ham_open_ex
 *
 * Open the file for reading only. Operations which need write access 
 * (i.e. @a ham_insert) will return error @a HAM_DB_READ_ONLY.
 * 
 * The flag is disabled by default.
 */
#define HAM_READ_ONLY                0x00000004

/**
 * Flag for @a ham_open, @a ham_open_ex
 *
 * Opens the file exclusively by specifying O_EXCL (Posix). TODO win32?
 *
 * This is broken on nfs and most likely on other network filesystems, too.
 *
 * The flag is disabled by default.
 */
#define HAM_OPEN_EXCLUSIVELY         0x00000008

/**
 * Flag for @a ham_create, @a ham_create_ex
 *
 * Use a B+Tree for the index structure. Currently, this is your only choice,
 * but future releases of hamsterdb will offer additional index structures, 
 * i.e. hash tables.
 *
 * This flag is enabled by default.
 */
#define HAM_USE_BTREE                0x00000010

/*
 * Use a hash database as the index structure
 * This flag is disabled by default.
#define HAM_USE_HASH                 0x00000020
 */

/**
 * Flag for @a ham_open, @a ham_open_ex, @a ham_create, @a ham_create_ex
 * TODO stimmt das? open und create?
 *
 * Do not allow the use of variable length keys. Inserting a key, which is 
 * larger than the B+Tree index key size, returns error @a HAM_INV_KEYSIZE.
 *
 * This flag is disabled by default.
 */
#define HAM_DISABLE_VAR_KEYLEN       0x00000040

/**
 * Flag for @a ham_create, @a ham_create_ex
 *
 * Create an in-memory-database. No file will be created, and the
 * database contents are lost after the database is closed.
 *
 * This flag is disabled by default.
 */
#define HAM_IN_MEMORY_DB             0x00000080

/*
 * 0x100 is a reserved value         0x00000100
 */

/**
 * Flag for @a ham_open, @a ham_open_ex, @a ham_create, @a ham_create_ex
 *
 * Do not use memory mapped files for I/O. Per default, hamsterdb
 * checks if it can use mmap, since mmap is faster then read/write.
 * It's not recommended to use this flag.
 * 
 * This flag is disabled by default.
 */
#define HAM_DISABLE_MMAP             0x00000200

/**
 * Flag for @a ham_open, @a ham_open_ex, @a ham_create, @a ham_create_ex
 *
 * If this flag is set, the cache is never allowed to grow larger than 
 * the maximum cachesize. If a database operation needs to resize the 
 * cache, it will return with error @a HAM_CACHE_FULL.
 * 
 * If the flag is not set, the cache is allowed to allocate
 * more pages then the maximum cachesize, but only if it's necessary and
 * only for a short time. 
 *
 * This flag is disabled by default.
 */
#define HAM_CACHE_STRICT             0x00000400

/**
 * Flag for @a ham_open, @a ham_open_ex, @a ham_create, @a ham_create_ex
 *
 * Do not immediately writeback modified freelist pages. This flag
 * leads to small performance improvements, but comes with additional
 * risk in case of a system crash or program crash.
 *
 * This flag is disabled by default.
 */
#define HAM_DISABLE_FREELIST_FLUSH   0x00000800

/**
 * Flag for @a ham_open, @a ham_open_ex, @a ham_create, @a ham_create_ex
 *
 * Optimize for smaller database files; hamsterdb will try to merge freelist
 * entries, whenever possible. Files can become significantly smaller,
 * but it costs performance, especially when the application deletes a 
 * lot of items.
 *
 * In-memory-databases ignore this flag, because they don't use a 
 * freelist.
 * 
 * This flag is disabled by default.
 */
#define HAM_OPTIMIZE_SIZE            0x00001000

/**
 * get the last error code
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
 * TODO TODO TODO mehr fehlercodes?
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
 * TODO TODO TODO mehr fehlercodes?
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
 * initialized with the size of the record (in record->size) and a 
 * pointer to the actual record data (in record->data). If the record 
 * is empty, @a size is 0 and @a data is NULL.
 *
 * The @a data pointer is a temporary pointer and will be overwritten 
 * by subsequent hamsterdb API calls. You can alter this behaviour by 
 * allocating the @a data pointer in the application and setting 
 * record->flags to HAM_RECORD_USER_ALLOC. Make sure that the allocated
 * buffer is large enough. 
 *
 * @param db A valid database handle.
 * @param reserved A reserved value; set to NULL.
 * @param key The key of the item.
 * @param record The record of the item.
 * @param flags Search flags; unused, set to 0.
 *
 * @return @a HAM_SUCCESS on success
 * TODO TODO TODO mehr fehlercodes?
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
 * @param flags Insert flags TODO 
 *
 * @return @a HAM_SUCCESS on success
 * TODO TODO TODO mehr fehlercodes?
 */
HAM_EXPORT ham_status_t
ham_insert(ham_db_t *db, void *reserved, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags);

/** 
 * Flag for @a ham_insert and @a ham_cursor_insert
 *
 * When inserting a key/record pair, and the key already exists, then
 * overwrite the existing item.
 */
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
 * TODO TODO TODO mehr fehlercodes?
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
 * TODO TODO TODO mehr fehlercodes?
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
 * TODO TODO TODO mehr fehlercodes?
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
 * create a database cursor
 *
 * @remark set @a reserved to NULL and @a flags to 0
 */
HAM_EXPORT ham_status_t
ham_cursor_create(ham_db_t *db, void *reserved, ham_u32_t flags,
        ham_cursor_t **cursor);

/**
 * clone a database cursor
 */
HAM_EXPORT ham_status_t
ham_cursor_clone(ham_cursor_t *src, ham_cursor_t **dest);

/**
 * moves the cursor
 *
 * @remark @a key and/or @a record can be NULL; if @a flag is neither
 * HAM_CURSOR_FIRST, HAM_CURSOR_LAST, HAM_CURSOR_NEXT or HAM_CURSOR_PREVIOUS,
 * the current key/record is returned
 */
HAM_EXPORT ham_status_t
ham_cursor_move(ham_cursor_t *cursor, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags);

#define HAM_CURSOR_FIRST            1
#define HAM_CURSOR_LAST             2
#define HAM_CURSOR_NEXT             4
#define HAM_CURSOR_PREVIOUS         8

/**
 * replace the current record
 */
HAM_EXPORT ham_status_t
ham_cursor_replace(ham_cursor_t *cursor, ham_record_t *record,
            ham_u32_t flags);

/**
 * find a key in the index and positions the cursor
 * on this key
 */
HAM_EXPORT ham_status_t
ham_cursor_find(ham_cursor_t *cursor, ham_key_t *key, ham_u32_t flags);

/**
 * insert (or update) a key in the index
 */
HAM_EXPORT ham_status_t
ham_cursor_insert(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags);

/**
 * erases the key from the index; after the erase, the cursor 
 * is invalid
 */
HAM_EXPORT ham_status_t
ham_cursor_erase(ham_cursor_t *cursor, ham_u32_t flags);

/**
 * close a database cursor
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
