/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file COPYING for licence information
 *
 * include file for hamster-db
 *
 */

#ifndef HAM_HAMSTERDB_H__
#define HAM_HAMSTERDB_H__

#ifdef __cplusplus
extern "C" {
#endif 

#include <ham/config.h>
#include <ham/types.h>

/**
 * the database structure
 */
struct ham_db_t; 
typedef struct ham_db_t ham_db_t;

/**
 * a generic record
 */
typedef struct
{
    ham_size_t size;
    ham_u32_t flags;
    void *data;
    ham_size_t _allocsize;
    ham_u32_t _intflags;
    ham_u64_t _rid;
} ham_record_t;

/**
 * flags for ham_record_t: data points to memory which is allocated 
 * by the user
 */
#define HAM_RECORD_USER_ALLOC   1

/**
 * a generic key
 */
typedef struct
{
    ham_u32_t _flags;
    ham_size_t size;
    void *data;
} ham_key_t;

/**
 * a typedef for a custom error handler function
 */
typedef void (*ham_errhandler_fun)(const char *message);

/**
 * set a global error handler; this handler will receive <b>all</b> debug 
 * messages which are emitted by hamster-db. you can remove the handler by
 * setting @a f to 0.
 */
extern void
ham_set_errhandler(ham_errhandler_fun f);

/** 
 * get a descriptive error string from a hamster status code
 */
extern const char *
ham_strerror(ham_status_t result);

/**
 * hamster error- and status codes.
 * hamster-error codes are always negative, so we have no conflicts with 
 * errno.h
 */
#define HAM_SUCCESS                  (  0)
#define HAM_SHORT_READ               ( -1)
#define HAM_SHORT_WRITE              ( -2)
#define HAM_INV_KEYSIZE              ( -3)
#define HAM_INV_PAGESIZE             ( -4)
#define HAM_DB_ALREADY_OPEN          ( -5)
#define HAM_OUT_OF_MEMORY            ( -6)
#define HAM_INV_BACKEND              ( -7)
#define HAM_INV_PARAMETER            ( -8)
#define HAM_INV_FILE_HEADER          ( -9)
#define HAM_INV_FILE_VERSION         (-10)
#define HAM_KEY_NOT_FOUND            (-11)
#define HAM_DUPLICATE_KEY            (-12)
#define HAM_INTEGRITY_VIOLATED       (-13)
#define HAM_INTERNAL_ERROR           (-14)
#define HAM_DB_READ_ONLY             (-15)
#define HAM_BLOB_NOT_FOUND           (-16)
#define HAM_PREFIX_REQUEST_FULLKEY   (-17)
#define HAM_IO_ERROR                 (-18)
#define HAM_CACHE_FULL               (-19)

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
 * If ham_open() fails, if a file with the given name does not 
 * exist. If you set the flag HAM_OPEN_CREATE, the file will
 * be created if it does not exist.
 * The flag is disabled by default.
 */
#define HAM_OPEN_CREATE              0x00000002

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
 * use a B+tree as the backend
 * This flag is enabled by default.
 */
#define HAM_BE_BTREE                 0x00000010

/**
 * use a hash database as the backend
 * This flag is disabled by default.
 */
#define HAM_BE_HASH                  0x00000020

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
 */
extern ham_status_t
ham_insert(ham_db_t *db, void *reserved, ham_key_t *key, 
        ham_record_t *record, ham_u32_t flags);

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
ham_flush(ham_db_t *db);

/**
 * close a database
 */
extern ham_status_t
ham_close(ham_db_t *db);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_HAMSTERDB_H__ */
