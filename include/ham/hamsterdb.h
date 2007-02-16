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
extern void
ham_set_errhandler(ham_errhandler_fun f);

/**
 * Get a descriptive error string from a hamsterdb status code
 *
 * @param status The hamsterdb status code
 *
 * @return Returns a C string with a descriptive error string.
 */
extern const char *
ham_strerror(ham_status_t status);

/*
 * hamsterdb error- and status codes.
 * These codes are always negative, so we have no conflicts with errno.h
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

/**
 * get the version of the hamsterdb library
 */
extern void
ham_get_version(ham_u32_t *major, ham_u32_t *minor,
        ham_u32_t *revision);

/**
 * create a new ham_db_t handle
 */
extern ham_status_t
ham_new(ham_db_t **db);

/**
 * delete a ham_db_t handle
 */
extern ham_status_t
ham_delete(ham_db_t *db);

/**
 * open an (existing) database
 */
extern ham_status_t
ham_open(ham_db_t *db, const char *filename, ham_u32_t flags);

/**
 * open an (existing) database - extended version
 */
extern ham_status_t
ham_open_ex(ham_db_t *db, const char *filename,
        ham_u32_t flags, ham_size_t cachesize);

/**
 * create a new database
 */
extern ham_status_t
ham_create(ham_db_t *db, const char *filename,
        ham_u32_t flags, ham_u32_t mode);

/**
 * create a new database - extended version
 *
 * !!
 * If you create an in-memory-database (flag HAM_IN_MEMORY_DB),
 * you are NOT allowed to set the flag HAM_CACHE_STRICT or to use
 * a cache size != 0!
 */
extern ham_status_t
ham_create_ex(ham_db_t *db, const char *filename,
        ham_u32_t flags, ham_u32_t mode, ham_u16_t pagesize,
        ham_u16_t keysize, ham_size_t cachesize);

/**
 * Flags for opening and creating the database
 *
 * When file pages are modified, write them imediately to the file.
 * The flag is disabled by default.
 */
#define HAM_WRITE_THROUGH            0x00000001

/**
 * Open the file for reading only
 * The flag is disabled by default.
 */
#define HAM_READ_ONLY                0x00000004

/**
 * Open the file exclusively
 * This is broken on nfs and most likely on other network filesystems, too.
 * The flag is disabled by default.
 */
#define HAM_OPEN_EXCLUSIVELY         0x00000008

/**
 * use a B+tree as the index structure
 * This flag is enabled by default.
 */
#define HAM_USE_BTREE                0x00000010

/**
 * use a hash database as the index structure
 * This flag is disabled by default.
 */
#define HAM_USE_HASH                 0x00000020

/**
 * do not allow the use of variable length keys
 * This flag is disabled by default.
 */
#define HAM_DISABLE_VAR_KEYLEN       0x00000040

/**
 * create an in-memory-database. No file will be written, and the
 * database contents are lost after the database is closed.
 * This flag is disabled by default.
 */
#define HAM_IN_MEMORY_DB             0x00000080

/*
 * 0x100 is a reserved value         0x00000100
 */

/**
 * do not use memory mapped files for I/O. Per default, hamsterdb
 * checks if it can use mmap, since mmap is faster then read/write.
 * It's not recommended to use this flag.
 */
#define HAM_DISABLE_MMAP             0x00000200

/**
 * Flags and policies for the database cache
 *
 * If this flag is true, the cache must *never* be bigger then
 * the maximum cachesize; otherwise, the cache is allowed to allocate
 * more pages then the maximum cachesize, but only if it's necessary and
 * only for a short time. Default: flag is off
 *
 * !!
 * This flag is not allowed in combination with HAM_IN_MEMORY_DB!
 */
#define HAM_CACHE_STRICT             0x00000400

/**
 * Do not immediately writeback modified freelist pages.
 * leads to small performance improvements, but comes with additional
 * risk in case of a system crash or program crash.
 * Default: flag is off.
 */
#define HAM_DISABLE_FREELIST_FLUSH   0x00000800

/**
 * Optimize for smaller database files; this will try to merge freelist
 * entries, whenever possible. Files can become significantly smaller,
 * but it costs performance, especially when ham_erase() is called
 * frequently. Default: flag is off.
 */
#define HAM_OPTIMIZE_SIZE            0x00001000

/**
 * create a database cursor
 *
 * @remark set reserved and flags to 0
 */
extern ham_status_t
ham_create_cursor(ham_db_t *db, void *reserved, ham_u32_t flags,
            ham_cursor_t **cursor);

/**
 * get the last error code
 */
extern ham_status_t
ham_get_error(ham_db_t *db);

/**
 * set the prefix comparison function
 *
 * @remark the prefix comparison function is called when an index uses
 * keys with variable length, and one of the two keys is loaded only
 * partially.
 */
extern ham_status_t
ham_set_prefix_compare_func(ham_db_t *db, ham_prefix_compare_func_t foo);

/**
 * set the default comparison function
 *
 * @remark the default comparison function is called when an index does NOT
 * use keys with variable length, or if both keys are loaded completely.
 */
extern ham_status_t
ham_set_compare_func(ham_db_t *db, ham_compare_func_t foo);

/**
 * find a key in the database
 *
 * @remark set 'reserved' to NULL
 */
extern ham_status_t
ham_find(ham_db_t *db, void *reserved, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags);

/**
 * insert a database entry
 *
 * @remark set 'reserved' to NULL
 *
 * @remark see below for valid flags
 */
extern ham_status_t
ham_insert(ham_db_t *db, void *reserved, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags);

/** insert-flag: overwrite the key/record pair, if it exists */
#define HAM_OVERWRITE               1

/**
 * erase a database entry
 *
 * @remark set 'reserved' to NULL
 */
extern ham_status_t
ham_erase(ham_db_t *db, void *reserved, ham_key_t *key,
        ham_u32_t flags);

/**
 * flush all open pages and write them to disk
 *
 * this function has no effect on in-memory-databases
 */
extern ham_status_t
ham_flush(ham_db_t *db, ham_u32_t flags);

/**
 * close a database
 */
extern ham_status_t
ham_close(ham_db_t *db);

/**
 * create a database cursor
 *
 * @remark set @a reserved to NULL and @a flags to 0
 */
extern ham_status_t
ham_cursor_create(ham_db_t *db, void *reserved, ham_u32_t flags,
        ham_cursor_t **cursor);

/**
 * clone a database cursor
 */
extern ham_status_t
ham_cursor_clone(ham_cursor_t *src, ham_cursor_t **dest);

/**
 * moves the cursor
 *
 * @remark @a key and/or @a record can be NULL; if @a flag is neither
 * HAM_CURSOR_FIRST, HAM_CURSOR_LAST, HAM_CURSOR_NEXT or HAM_CURSOR_PREVIOUS,
 * the current key/record is returned
 */
extern ham_status_t
ham_cursor_move(ham_cursor_t *cursor, ham_key_t *key,
        ham_record_t *record, ham_u32_t flags);

#define HAM_CURSOR_FIRST            1
#define HAM_CURSOR_LAST             2
#define HAM_CURSOR_NEXT             4
#define HAM_CURSOR_PREVIOUS         8

/**
 * replace the current record
 */
extern ham_status_t
ham_cursor_replace(ham_cursor_t *cursor, ham_record_t *record,
            ham_u32_t flags);

/**
 * find a key in the index and positions the cursor
 * on this key
 */
extern ham_status_t
ham_cursor_find(ham_cursor_t *cursor, ham_key_t *key, ham_u32_t flags);

/**
 * insert (or update) a key in the index
 */
extern ham_status_t
ham_cursor_insert(ham_cursor_t *cursor, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags);

/**
 * erases the key from the index; after the erase, the cursor 
 * is invalid
 */
extern ham_status_t
ham_cursor_erase(ham_cursor_t *cursor, ham_u32_t flags);

/**
 * close a database cursor
 */
extern ham_status_t
ham_cursor_close(ham_cursor_t *cursor);


#ifdef __cplusplus
} // extern "C"
#endif

#endif /* HAM_HAMSTERDB_H__ */
