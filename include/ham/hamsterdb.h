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
 * a transaction handle
 */
struct ham_txn_t;
typedef struct ham_txn_t ham_txn_t; 

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
 * create a new database
 */
extern ham_status_t
ham_create(ham_db_t *db, const char *filename, 
        ham_u32_t flags, ham_u32_t mode);

/**
 * create a new database - extended version
 */
extern ham_status_t
ham_create_ex(ham_db_t *db, const char *filename, 
        ham_u32_t flags, ham_u32_t mode, ham_u16_t pagesize, 
        ham_u16_t keysize, ham_size_t cachesize);

/**
 * close a database
 */
extern ham_status_t
ham_close(ham_db_t *db);

/**
 * Flags for opening and creating the database
 */

/**
 * If new pages are allocated on the hard drive, they are aligned to
 * 1024 byte-boundaries. You can disable this alignment by setting 
 * the HAM_NO_PAGE_ALIGN-flag when you create the database. Files
 * become a bit smaller then, for the price of a speed tradeoff.
 * TODO benchmark it!
 * The alignment is enabled by default.
 */
#define HAM_NO_PAGE_ALIGN            0x00000001

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
 * get the key size
 */
extern ham_u16_t 
ham_get_keysize(ham_db_t *db);

/** 
 * set key size
 *
 * @remark This function fails if the database was already created or
 * is already open.
 */
extern ham_status_t
ham_set_keysize(ham_db_t *db, ham_u16_t size);

/** 
 * get page size
 */
extern ham_u16_t
ham_get_pagesize(ham_db_t *db);

/** 
 * set page size
 *
 * @remark This function fails if the database was already created or
 * is already open.
 */
extern ham_u16_t
ham_set_pagesize(ham_db_t *db, ham_u16_t size);

/** 
 * get the flags
 */
extern ham_u32_t
ham_get_flags(ham_db_t *db);

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
 * check if the database is open
 */
extern ham_bool_t
ham_is_open(ham_db_t *db);

/**
 * a typedef for a custom error handler function
 */
typedef void (*ham_errhandler_fun)(const char *message);

/**
 * set an error handler; this handler will receive <b>all</b> debug messages
 * which are emitted by hamster-db. you can remove the handler by
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
#define HAM_DB_ALREADY_OPEN          ( -4)
#define HAM_OUT_OF_MEMORY            ( -5)
#define HAM_INV_BACKEND              ( -6)
#define HAM_INV_PARAMETER            ( -7)
#define HAM_INV_FILE_HEADER          ( -8)
#define HAM_INV_FILE_VERSION         ( -9)
#define HAM_KEY_NOT_FOUND            (-10)
#define HAM_DUPLICATE_KEY            (-11)
#define HAM_INTEGRITY_VIOLATED       (-12)
#define HAM_INTERNAL_ERROR           (-13)
#define HAM_DB_READ_ONLY             (-14)
#define HAM_BLOB_NOT_FOUND           (-15)
#define HAM_PREFIX_REQUEST_FULLKEY   (-16)

/** 
 * find a key in the database
 *
 * @remark not yet frozen...
 */
extern ham_status_t
ham_find(ham_db_t *db, ham_key_t *key, ham_record_t *record, ham_u32_t flags);

/** 
 * insert a database entry
 *
 * @remark not yet frozen...
 */
extern ham_status_t
ham_insert(ham_db_t *db, ham_key_t *key, ham_record_t *record, ham_u32_t flags);

/** 
 * erase a database entry
 *
 * @remark not yet frozen...
 */
extern ham_status_t
ham_erase(ham_db_t *db, ham_key_t *key, ham_u32_t flags);

/**
 * a callback function for dump - dumps a single key to stdout
 *
 * @remark see the default implementation in db.c for an example
 */
typedef void (*ham_dump_cb_t)(const ham_u8_t *key, ham_size_t keysize);

/** 
 * dump the whole tree to stdout
 *
 * @remark you can pass a callback function pointer, or NULL for the default
 * function (dumps the first 16 bytes of the key)
 */
extern ham_status_t
ham_dump(ham_db_t *db, ham_dump_cb_t cb);

/** 
 * verify the whole tree
 */
extern ham_status_t
ham_check_integrity(ham_db_t *db);

#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_HAMSTERDB_H__ */
