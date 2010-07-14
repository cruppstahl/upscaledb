/*
 * Copyright (C) 2005-2010 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

/**
 * @brief internal macros and headers
 *
 */

#ifndef HAM_DB_H__
#define HAM_DB_H__

#include "internal_fwd_decl.h"

#include <ham/hamsterdb_stats.h>

#include "endian.h"
#include "error.h"
#include "util.h"


#ifdef __cplusplus
extern "C" {
#endif

/**
 * a macro to cast pointers to u64 and vice versa to avoid compiler
 * warnings if the sizes of ptr and u64 are not equal
 */
#if defined(HAM_32BIT) && (!defined(_MSC_VER))
#   define U64_TO_PTR(p)  (ham_u8_t *)(int)p
#   define PTR_TO_U64(p)  (ham_offset_t)(int)p
#else
#   define U64_TO_PTR(p)  p
#   define PTR_TO_U64(p)  p
#endif

/**
 * a magic and version indicator for the remote protocol
 */
#define HAM_TRANSFER_MAGIC_V1   (('h'<<24)|('a'<<16)|('m'<<8)|'1')

/**
 * the maximum number of indices (if this file is an environment with 
 * multiple indices)
 */
#define DB_MAX_INDICES      16  /* 16*32 = 512 byte wasted */

/*
 * the size of an index data
 */
#define DB_INDEX_SIZE       sizeof(db_indexdata_t) /* 32 */

/**
 * get the key size
 */
#define db_get_keysize(db)         be_get_keysize(db_get_backend(db))


#include "packstart.h"

/**
 * the persistent database index header
 */
typedef HAM_PACK_0 union HAM_PACK_1 
{
    HAM_PACK_0 struct HAM_PACK_1 {
        /** name of the DB: 1..HAM_DEFAULT_DATABASE_NAME-1 */
        ham_u16_t _dbname;

        /** maximum keys in an internal page */
        ham_u16_t _maxkeys;

        /** key size in this page */
        ham_u16_t _keysize;

        /* reserved */
        ham_u16_t  _reserved1;
    
        /** address of this page */
        ham_offset_t _self;

        /** flags for this database */
        ham_u32_t _flags;

        /** last used record number value */
        ham_offset_t _recno;

        /* reserved */
        ham_u32_t _reserved2;
    } HAM_PACK_2 b;

    ham_u8_t _space[32];
} HAM_PACK_2 db_indexdata_t;

#include "packstop.h"


#define index_get_dbname(p)               ham_db2h16((p)->b._dbname)
#define index_set_dbname(p, n)            (p)->b._dbname = ham_h2db16(n)

#define index_get_max_keys(p)             ham_db2h16((p)->b._maxkeys)
#define index_set_max_keys(p, n)          (p)->b._maxkeys = ham_h2db16(n)

#define index_get_keysize(p)              ham_db2h16((p)->b._keysize)
#define index_set_keysize(p, n)           (p)->b._keysize = ham_h2db16(n)

#define index_get_self(p)                 ham_db2h_offset((p)->b._self)
#define index_set_self(p, n)              (p)->b._self=ham_h2db_offset(n)

#define index_get_flags(p)                ham_db2h32((p)->b._flags)
#define index_set_flags(p, n)             (p)->b._flags = ham_h2db32(n)

#define index_get_recno(p)                ham_db2h_offset((p)->b._recno)
#define index_set_recno(p, n)             (p)->b._recno=ham_h2db_offset(n)

#define index_clear_reserved(p)           { (p)->b._reserved1 = 0;            \
                                            (p)->b._reserved2 = 0; }

/**
 * get the cache for extended keys
 */
#define db_get_extkey_cache(db)    (db)->_extkey_cache

/**
 * set the cache for extended keys
 */
#define db_set_extkey_cache(db, c) (db)->_extkey_cache=(c)
 
/**
 * the database structure
 */
struct ham_db_t
{
    /** the current transaction ID */
    ham_u64_t _txn_id;

    /** the last recno-value */
    ham_u64_t _recno;

    /** the last error code */
    ham_status_t _error;

    /** a custom error handler */
    ham_errhandler_fun _errh;

    /** the user-provided context data */
    void *_context;

    /** the backend pointer - btree, hashtable etc */
    ham_backend_t *_backend;

    /** linked list of all cursors */
    ham_cursor_t *_cursors;

    /** the size of the last allocated data pointer for records */
    ham_size_t _rec_allocsize;

    /** the last allocated data pointer for records */
    void *_rec_allocdata;

    /** the size of the last allocated data pointer for keys */
    ham_size_t _key_allocsize;

    /** the last allocated data pointer for keys */
    void *_key_allocdata;

    /** the prefix-comparison function */
    ham_prefix_compare_func_t _prefix_func;

    /** the comparison function */
    ham_compare_func_t _cmp_func;

    /** the duplicate keys record comparison function */
    ham_compare_func_t _duperec_func;

    /** the cache for extended keys */
    extkey_cache_t *_extkey_cache;

    /** the database flags - a combination of the persistent flags
     * and runtime flags */
    ham_u32_t _rt_flags;

    /** the offset of this database in the environment _indexdata */
    ham_u16_t _indexdata_offset;

    /** the environment of this database - can be NULL */
    ham_env_t *_env;

    /** the next database in a linked list of databases */
    ham_db_t *_next;

    /** linked list of all record-level filters */
    ham_record_filter_t *_record_filters;

    /** current data access mode (DAM) */
    ham_u16_t _data_access_mode;

    /** non-zero after this istem has been opened/created */
    unsigned _is_active: 1;

    /** some freelist algorithm specific run-time data */
    ham_runtime_statistics_globdata_t _global_perf_data;

    /** some database specific run-time data */
    ham_runtime_statistics_dbdata_t _db_perf_data;

#if HAM_ENABLE_REMOTE
    /** the remote database handle */
    ham_u64_t _remote_handle;
#endif

    /**
     * get Database parameters
     */
    ham_status_t (*_fun_get_parameters)(ham_db_t *db, ham_parameter_t *param);

    /**
     * flush the Database
     */
    ham_status_t (*_fun_flush)(ham_db_t *db, ham_u32_t flags);

    /**
     * check Database integrity
     */
    ham_status_t (*_fun_check_integrity)(ham_db_t *db, ham_txn_t *txn);

    /**
     * get number of keys
     */
    ham_status_t (*_fun_get_key_count)(ham_db_t *db, ham_txn_t *txn, 
                    ham_u32_t flags, ham_offset_t *keycount);

    /**
     * insert a key/value pair
     */
    ham_status_t (*_fun_insert)(ham_db_t *db, ham_txn_t *txn, 
                    ham_key_t *key, ham_record_t *record, ham_u32_t flags);

    /**
     * erase a key/value pair
     */
    ham_status_t (*_fun_erase)(ham_db_t *db, ham_txn_t *txn, 
                    ham_key_t *key, ham_u32_t flags);

    /**
     * lookup of a key/value pair
     */
    ham_status_t (*_fun_find)(ham_db_t *db, ham_txn_t *txn, 
                    ham_key_t *key, ham_record_t *record, ham_u32_t flags);

    /**
     * create a cursor
     */
    ham_status_t (*_fun_cursor_create)(ham_db_t *db, ham_txn_t *txn, 
                    ham_u32_t flags, ham_cursor_t **cursor);

    /**
     * clone a cursor
     */
    ham_status_t (*_fun_cursor_clone)(ham_cursor_t *src, ham_cursor_t **dest);

    /**
     * insert a key with a cursor
     */
    ham_status_t (*_fun_cursor_insert)(ham_cursor_t *cursor, 
                    ham_key_t *key, ham_record_t *record, ham_u32_t flags);

    /**
     * erase the key of a cursor
     */
    ham_status_t (*_fun_cursor_erase)(ham_cursor_t *cursor, ham_u32_t flags);

    /**
     * position the cursor on a key and return the record
     */
    ham_status_t (*_fun_cursor_find)(ham_cursor_t *cursor, ham_key_t *key, 
                    ham_record_t *record, ham_u32_t flags);

    /**
     * get number of duplicates
     */
    ham_status_t (*_fun_cursor_get_duplicate_count)(ham_cursor_t *cursor, 
                    ham_size_t *count, ham_u32_t flags);

    /**
     * overwrite a cursor
     */
    ham_status_t (*_fun_cursor_overwrite)(ham_cursor_t *cursor, 
                    ham_record_t *record, ham_u32_t flags);

    /**
     * move a cursor, return key and/or record
     */
    ham_status_t (*_fun_cursor_move)(ham_cursor_t *cursor, 
                    ham_key_t *key, ham_record_t *record, ham_u32_t flags);

    /**
     * close a cursor
     */
    ham_status_t (*_fun_cursor_close)(ham_cursor_t *cursor);

    /**
     * close the Database
     */
    ham_status_t (*_fun_close)(ham_db_t *db, ham_u32_t flags);

    /**
    * destroy the database object, free all memory
    */
    ham_status_t (*_fun_destroy)(ham_db_t *self);
};

/**
 * get the last recno value
 */
#define db_get_recno(db)               (db)->_recno

/**
 * set the last recno value
 */
#define db_set_recno(db, r)            (db)->_recno=(r)

/**
 * get the last error code
 */
#define db_get_error(db)               (db)->_error

/**
 * set the last error code
 */
#define db_set_error(db, e)            (db)->_error=(e)

/**
 * get the user-provided context pointer
 */
#define db_get_context_data(db)        (db)->_context

/**
 * set the user-provided context pointer
 */
#define db_set_context_data(db, ctxt)  (db)->_context=(ctxt)

/**
 * get the backend pointer
 */
#define db_get_backend(db)             (db)->_backend

/**
 * set the backend pointer
 */
#define db_set_backend(db, be)         (db)->_backend=(be)

/**
 * get the prefix comparison function
 */
#define db_get_prefix_compare_func(db) (db)->_prefix_func

/**
 * set the prefix comparison function
 */
#define db_set_prefix_compare_func(db, f) (db)->_prefix_func=(f)

/**
 * get the default comparison function
 */
#define db_get_compare_func(db)        (db)->_cmp_func

/**
 * set the default comparison function
 */
#define db_set_compare_func(db, f)     (db)->_cmp_func=(f)

/**
 * get the duplicate record comparison function
 */
#define db_get_duplicate_compare_func(db)        (db)->_duperec_func

/**
 * set the duplicate record comparison function
 */
#define db_set_duplicate_compare_func(db, f)     (db)->_duperec_func=(f)

/**
 * get the runtime-flags - the flags are "mixed" with the flags from 
 * the Environment
 */
#define db_get_rt_flags(db)            (env_get_rt_flags(db_get_env(db))      \
                                            | (db)->_rt_flags)

/**
 * set the runtime-flags - NOT setting environment flags!
 */
#define db_set_rt_flags(db, f)         (db)->_rt_flags=(f)

/**
 * get the index of this database in the indexdata array
 *
 * @sa env_get_indexdata_ptr
 */
#define db_get_indexdata_offset(db)    (db)->_indexdata_offset

/**
 * set the index of this database in the indexdata array
 *
 * @sa env_get_indexdata_ptr
 */
#define db_set_indexdata_offset(db, o) (db)->_indexdata_offset=(o)

/**
 * get the environment pointer
 */
#define db_get_env(db)                 (db)->_env

/**
 * set the environment pointer
 */
#define db_set_env(db, env)            (db)->_env=(env)

/**
 * get the next database in a linked list of databases
 */
#define db_get_next(db)                (db)->_next

/**
 * set the pointer to the next database
 */
#define db_set_next(db, next)          (db)->_next=(next)

/**
 * get the linked list of all record-level filters
 */
#define db_get_record_filter(db)       (db)->_record_filters

/**
 * set the linked list of all record-level filters
 */
#define db_set_record_filter(db, f)    (db)->_record_filters=(f)

/**
 * get the linked list of all cursors
 */
#define db_get_cursors(db)             (db)->_cursors

/**
 * set the linked list of all cursors
 */
#define db_set_cursors(db, c)          (db)->_cursors=(c)

/**
 * get the size of the last allocated data blob
 */
#define db_get_record_allocsize(db)    (db)->_rec_allocsize

/**
 * set the size of the last allocated data blob
 */
#define db_set_record_allocsize(db, s) (db)->_rec_allocsize=(s)

/**
 * get the pointer to the last allocated data blob
 */
#define db_get_record_allocdata(db)    (db)->_rec_allocdata

/**
 * set the pointer to the last allocated data blob
 */
#define db_set_record_allocdata(db, p) (db)->_rec_allocdata=(p)

/**
 * get the size of the last allocated key blob
 */
#define db_get_key_allocsize(db)       (db)->_key_allocsize

/**
 * set the size of the last allocated key blob
 */
#define db_set_key_allocsize(db, s)    (db)->_key_allocsize=(s)

/**
 * get the pointer to the last allocated key blob
 */
#define db_get_key_allocdata(db)       (db)->_key_allocdata

/**
 * set the pointer to the last allocated key blob
 */
#define db_set_key_allocdata(db, p)    (db)->_key_allocdata=(p)

/**
 * get the expected data access mode for this database
 */
#define db_get_data_access_mode(db)   (db)->_data_access_mode

/**
 * set the expected data access mode for this database
 */
#define db_set_data_access_mode(db,s)  (db)->_data_access_mode=(s)

/**
 * Mix a set of flag bits into the data access mode, according to the rule
 * 
 * <pre>
 * DAM(new) = (DAM & and_mask) | or_mask
 * </pre>
 * 
 * This is a quick qay to set or unset particular DAM bits.
 * 
 * @sa ham_data_access_modes
 */
#define db_set_data_access_mode_masked(db, or_mask, and_mask)                \
    (db)->_data_access_mode=(((db)->_data_access_mode & (and_mask))            \
                             | (or_mask))

/**
 * check if a given data access mode / mode-set has been set
 */
#define db_is_mgt_mode_set(mode_collective, mask)                \
    (((mode_collective) & (mask)) == (mask))

/**
 * check whether this database has been opened/created.
 */
#define db_is_active(db)   (db)->_is_active

/**
 * set the 'active' flag of the database: a non-zero value 
 * for @a s sets the @a db to 'active', zero(0) sets the @a db 
 * to 'inactive' (closed)
 */
#define db_set_active(db,s)  (db)->_is_active=!!(s)

/**
 * get a reference to the per-database statistics
 */
#define db_get_db_perf_data(db)      &(db)->_db_perf_data

/**
 * get the remote database handle
 */
#define db_get_remote_handle(db)        (db)->_remote_handle

/**
 * set the remote database handle
 */
#define db_set_remote_handle(db, h)     (db)->_remote_handle=(h)

/**
 * get the database name
 */
extern ham_u16_t
db_get_dbname(ham_db_t *db);

/**
 * uncouple all cursors from a page
 *
 * @remark this is called whenever the page is deleted or becoming invalid
 */
extern ham_status_t
db_uncouple_all_cursors(ham_page_t *page, ham_size_t start);

/**
 * compare two keys
 *
 * this function will call the prefix-compare function and the
 * default compare function whenever it's necessary.
 *
 * This is the default key compare function, which uses memcmp to compare two keys.
 *
 * @return -1, 0, +1 or higher positive values are the result of a successful 
 *         key comparison (0 if both keys match, -1 when LHS < RHS key, +1 
 *         when LHS > RHS key).
 *
 * @return values less than -1 are @ref ham_status_t error codes and indicate 
 *         a failed comparison execution: these are listed in 
 *         @ref ham_status_codes .
 *
 * @sa ham_status_codes 
 */
extern int HAM_CALLCONV 
db_default_compare(ham_db_t *db,
        const ham_u8_t *lhs, ham_size_t lhs_length,
        const ham_u8_t *rhs, ham_size_t rhs_length);

/**
 * compare two recno-keys
 *
 * this function compares two record numbers
 *
 * @return -1, 0, +1 or higher positive values are the result of a successful 
 *         key comparison (0 if both keys match, -1 when LHS < RHS key, +1 
 *         when LHS > RHS key).
 *
 * @return values less than -1 are @ref ham_status_t error codes and indicate 
 *         a failed comparison execution: these are listed in 
 *         @ref ham_status_codes .
 *
 * @sa ham_status_codes 
 */
extern int HAM_CALLCONV 
db_default_recno_compare(ham_db_t *db,
        const ham_u8_t *lhs, ham_size_t lhs_length,
        const ham_u8_t *rhs, ham_size_t rhs_length);

/**
 * the default prefix compare function - uses memcmp
 *
 * compares the prefix of two keys
 *
 * @return -1, 0, +1 or higher positive values are the result of a successful 
 *         key comparison (0 if both keys match, -1 when LHS < RHS key, +1 
 *         when LHS > RHS key).
 *
 * @return values less than -1 are @ref ham_status_t error codes and indicate 
 *         a failed comparison execution: these are listed in 
 *         @ref ham_status_codes .
 *
 * @sa ham_status_codes 
 */
extern int HAM_CALLCONV 
db_default_prefix_compare(ham_db_t *db,
        const ham_u8_t *lhs, ham_size_t lhs_length,
        ham_size_t lhs_real_length,
        const ham_u8_t *rhs, ham_size_t rhs_length,
        ham_size_t rhs_real_length);

/**
 * compare two records for a duplicate key
 *
 * @return -1, 0, +1 or higher positive values are the result of a successful 
 *         key comparison (0 if both keys match, -1 when LHS < RHS key, +1 
 *         when LHS > RHS key).
 *
 * @return values less than -1 are @ref ham_status_t error codes and indicate 
 *         a failed comparison execution: these are listed in 
 *         @ref ham_status_codes .
 *
 * @sa ham_status_codes 
 */
extern int HAM_CALLCONV 
db_default_dupe_compare(ham_db_t *db,
        const ham_u8_t *lhs, ham_size_t lhs_length,
        const ham_u8_t *rhs, ham_size_t rhs_length);

/**
 * load an extended key
 * returns the full data of the extended key in ext_key
 * 'ext_key' must have been initialized before calling this function.
 *
 * @note
 * This routine can cope with @ref HAM_KEY_USER_ALLOC-ated 'dest'-inations.
 */
extern ham_status_t
db_get_extended_key(ham_db_t *db, ham_u8_t *key_data,
                    ham_size_t key_length, ham_u32_t key_flags,
                    ham_key_t *ext_key);

/**
 * function which compares two keys
 *
 * calls the comparison function
 *
 * @return -1, 0, +1 or higher positive values are the result of a successful 
 *         key comparison (0 if both keys match, -1 when LHS < RHS key, +1 
 *         when LHS > RHS key).
 *
 * @return values less than -1 are @ref ham_status_t error codes and indicate 
 *         a failed comparison execution: these are listed in 
 *         @ref ham_status_codes .
 *
 * @sa ham_status_codes 
 */
extern int
db_compare_keys(ham_db_t *db, ham_key_t *lsh, ham_key_t *rhs);

/**
 * create a preliminary copy of an @ref int_key_t key to a @ref ham_key_t
 * in such a way that @ref db_compare_keys can use the data and optionally
 * call @ref db_get_extended_key on this key to obtain all key data, when this
 * is an extended key.
 * 
 * Used in conjunction with @ref db_release_ham_key_after_compare
 */
extern ham_status_t 
db_prepare_ham_key_for_compare(ham_db_t *db, int_key_t *src, ham_key_t *dest);

/**
 * @sa db_prepare_ham_key_for_compare
 */
#define db_release_ham_key_after_compare(db, key)                              \
    while ((key)->data && ((key)->_flags & KEY_IS_ALLOCATED)) {                \
        allocator_free(env_get_allocator(db_get_env((db))), (key)->data);      \
        (key)->data = 0;                                                       \
        (key)->size = 0;                                                       \
        break;                                                                 \
    }

/**
 * create a backend object according to the database flags.
 */
extern ham_status_t
db_create_backend(ham_backend_t **backend_ref, ham_db_t *db, ham_u32_t flags);

/**
 * fetch a page.
 *
 * @param page_ref call-by-reference variable which will be set to 
 *      point to the retrieved @ref ham_page_t instance.
 * @param db the database handle - if it's not available then please
 *      use env_fetch_page()
 * @param address the storage address (a.k.a. 'RID') where the page is 
 *      located in the device store (file, memory, ...). 
 * @param flags An optional, bit-wise combined set of the 
 *      @ref db_fetch_page_flags flag collection.
 *
 * @return the retrieved page in @a *page_ref and HAM_SUCCESS as a 
 *      function return value.
 * @return a NULL value in @a *page_ref and HAM_SUCCESS when the page 
 *      could not be retrieved because the set conditions were not be 
 *      met (see @ref DB_ONLY_FROM_CACHE) 
 * @return one of the @ref ham_status_codes error codes as an error occurred.
 */
extern ham_status_t
db_fetch_page(ham_page_t **page_ref, ham_db_t *db, 
        ham_offset_t address, ham_u32_t flags);

/*
 * this is an internal function. do not use it unless you know what you're
 * doing.
 */
extern ham_status_t
db_fetch_page_impl(ham_page_t **page_ref, ham_env_t *env, ham_db_t *db, 
        ham_offset_t address, ham_u32_t flags);

/**
 * @defgroup db_fetch_page_flags @ref db_fetch_page Flags
 * @{
 *
 * These flags can be bitwise-OR mixed with the @ref HAM_HINTS_MASK flags, 
 * i.e. the hint bits as listed in @ref ham_hinting_flags
 *
 * @sa ham_hinting_flags 
 */

/**
 * Force @ref db_fetch_page to only return a valid @ref ham_page_t instance 
 * reference when it is still stored in the cache, otherwise a NULL pointer 
 * will be returned instead (and no error code)!
 */
#define DB_ONLY_FROM_CACHE                0x0002

/**
 * Register new pages in the cache, but give them an 'old' age upon first 
 * creation, so they are flushed before anything else.
 *
 * This is a hacky way to ensure code simplicity while blob I/O does not 
 * thrash the cache but meanwhile still gets added to the activity log in 
 * a proper fashion.
 */
#define DB_NEW_PAGE_DOES_THRASH_CACHE    0x0004

/**
 * @}
 */


/**
 * flush a page
 */
extern ham_status_t
db_flush_page(ham_env_t *env, ham_page_t *page, ham_u32_t flags);

/**
 * Flush all pages, and clear the cache.
 *
 * @param flags Set to DB_FLUSH_NODELETE if you do NOT want the cache to
 * be cleared
 * @param cache 
 */
extern ham_status_t
db_flush_all(ham_cache_t *cache, ham_u32_t flags);

#define DB_FLUSH_NODELETE       1

/**
 * Allocate a new page.
 *
 * @param page_ref call-by-reference result: will store the @ref ham_page_t 
 *        instance reference.
 * @param db the database; if the database handle is not available, you
 *        can use env_alloc_page
 * @param type the page type of the new page. See @ref page_type_codes for
 *        a list of supported types.
 * @param flags optional allocation request flags. @a flags can be a mix
 *        of the following bits:
 *        - PAGE_IGNORE_FREELIST        ignores all freelist-operations
 *        - PAGE_CLEAR_WITH_ZERO        memset the persistent page with 0
 *        - DB_NEW_PAGE_DOES_THRASH_CACHE
 *        - PAGE_DONT_LOG_CONTENT       do not log page content for new 
 *                                      pages
 *
 * @note The page will be aligned at the current page size. Any wasted
 * space (due to the alignment) is added to the freelist.
 */
extern ham_status_t
db_alloc_page(ham_page_t **page_ref, ham_db_t *db, 
                ham_u32_t type, ham_u32_t flags);

/*
 * this is an internal function. do not use it unless you know what you're
 * doing.
 */
extern ham_status_t
db_alloc_page_impl(ham_page_t **page_ref, ham_env_t *env, ham_db_t *db, 
                ham_u32_t type, ham_u32_t flags);

#define PAGE_IGNORE_FREELIST          8
#define PAGE_CLEAR_WITH_ZERO         16
#define PAGE_DONT_LOG_CONTENT         32

/**
 * Free a page.
 *
 * @remark will mark the page as deleted; the page will be deleted
 * when the transaction is committed (or not deleted if the transaction
 * is aborted).
 *
 * @remark valid flag: DB_MOVE_TO_FREELIST; marks the page as 'deleted'
 * in the freelist. Ignored in in-memory databases.
 */
extern ham_status_t
db_free_page(ham_page_t *page, ham_u32_t flags);

#define DB_MOVE_TO_FREELIST         1

/**
 * Write a page, then delete the page from memory.
 *
 * @remark This function is used by the cache; it shouldn't be used
 * anywhere else.
 */
extern ham_status_t
db_write_page_and_delete(ham_page_t *page, ham_u32_t flags);

/**
 * Resize the record data.
 *
 * Set the size to 0, and the data is freed.
 */
extern ham_status_t
db_resize_allocdata(ham_db_t *db, ham_size_t size);

/**
* @defgroup ham_database_flags 
* @{
*/

/**
 * An internal database flag - use mmap instead of read(2).
 */
#define DB_USE_MMAP                  0x00000100

/**
 * An internal database flag - env handle is private to 
 * the @ref ham_db_t instance
 */
#define DB_ENV_IS_PRIVATE            0x00080000

/**
 * An internal database flag - env handle is remote
 */
#define DB_IS_REMOTE                 0x00200000

/**
 * @}
 */

/**
 * Ensure that the environment occupies a minimum number of pages.
 * 
 * This is useful with various storage devices to prevent / reduce
 * fragmentation.
 * 
 * @param env the environment reference.
 * 
 * @param minimum_page_count The desired minimum number of storage pages 
        * available to the environment/database.
 * 
 * process: 
 * 
 * <ol>
 * <li> detect how many pages we already have in the environment
 * 
 * <li> calculate how many pages we should have
 * 
 * <li> when this is more than what we've got so far, tell
 *      the device driver to allocate the remainder and mark
 *      them all as 'free'.
 * </ol>
 * 
 * @remark Caveat:
 *    The required size may be so large that it does not
 *    fit in the current freelist, so one or more of
 *    the allocated 'free' pages will be used for the
 *    extended freelist.
 */
extern ham_status_t
env_reserve_space(ham_env_t *env, ham_offset_t minimum_page_count);

/*
 * initialize the ham_db_t structure for accessing local files
 */
extern ham_status_t
db_initialize_local(ham_db_t *db);

/*
 * initialize the ham_db_t structure for accessing a remote server
 */
extern ham_status_t
db_initialize_remote(ham_db_t *db);


#ifdef __cplusplus
} // extern "C" {
#endif

#endif /* HAM_DB_H__ */
