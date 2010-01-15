/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 * 
 *
 * internal macros and headers
 *
 */

#ifndef HAM_DB_H__
#define HAM_DB_H__

#include "endian.h"
#include "backend.h"
#include "cache.h"
#include "page.h"
#include "os.h"
#include "freelist.h"
#include "extkeys.h"
#include "error.h"
#include "txn.h"
#include "log.h"
#include "mem.h"
#include "device.h"
#include "env.h"
#include "keys.h"
#include "statistics.h"


#ifdef __cplusplus
extern "C" {
#endif


#define OFFSETOF(type, member) ((size_t) &((type *)0)->member)

/**
 * This is the minimum chunk size; all chunks (pages and blobs) are aligned
 * to this size. 
 */
#define DB_CHUNKSIZE        32

/**
 * the maximum number of indices (if this file is an environment with 
 * multiple indices)
 */
#define DB_MAX_INDICES      16  /* 16*32 = 512 byte wasted */

/*
 * the size of an index data
 */
#define DB_INDEX_SIZE       sizeof(db_indexdata_t) /* 32 */

#include "packstart.h"

/*
 * the persistent database header
 */
typedef HAM_PACK_0 struct HAM_PACK_1 
{
    /* magic cookie - always "ham\0" */
    ham_u8_t  _magic[4];

    /* version information - major, minor, rev, reserved */
    ham_u8_t  _version[4];

    /* serial number */
    ham_u32_t _serialno;

    /* size of the page */
    ham_u32_t _pagesize;

    /*
     * NOTE: formerly, the _max_databases was 32 bits, but since
     * nobody would use more than 64K tables/indexes, we have the
     * MSW free for repurposing; as we store data in Little Endian
     * order, that would be the second WORD.
     *
     * That is now repurposed to recall the probable access mode
     * as once set up during ham_create_ex/ham_env_create_db.
     *
     * For reasons of backwards compatibility, the default value
     * there would be zero (0).
     */

    /* maximum number of databases for this environment */
    ham_u16_t _max_databases;

    /* the data access mode; 0: classic */
    ham_u16_t _data_access_mode;

    /* 
     * following here: 
     *
     * 1. the private data of the index backend(s) 
     *      -> see db_get_indexdata()
     *
     * 2. the freelist data
     *      -> see db_get_freelist()
     */

} HAM_PACK_2 db_header_t;

#include "packstop.h"

/*
 * get byte #i of the 'version'-header
 */
#define dbheader_get_version(hdr, i)      ((hdr)->_version[i])

/*
 * get the key size
 */
#define db_get_keysize(db)         be_get_keysize(db_get_backend(db))

/**
 * get the size of the persistent header of a page
 *
 * equals the size of struct ham_perm_page_union_t, without the payload byte
 *
 * !!
 * this is not equal to sizeof(struct ham_perm_page_union_t)-1, because of
 * padding (i.e. on gcc 4.1/64bit the size would be 15 bytes)
 */
#define db_get_persistent_header_size()   (OFFSETOF(ham_perm_page_union_t, _s._payload) /*(sizeof(ham_u32_t)*3)*/ /* 12 */ )

/*
 * get the maximum number of databases for this file
 */
#define db_get_max_databases(db)   ham_db2h16(db_get_header(db)->_max_databases)


#include "packstart.h"

/*
 * the persistent database index header
 */
typedef HAM_PACK_0 union HAM_PACK_1 
{
    HAM_PACK_0 struct HAM_PACK_1 {
        /* name of the DB: 1..HAM_DEFAULT_DATABASE_NAME-1 */
        ham_u16_t _dbname;

        /* maximum keys in an internal page */
        ham_u16_t _maxkeys;

        /* key size in this page */
        ham_u16_t _keysize;

        /* reserved */
        ham_u16_t  _reserved1;
    
        /* address of this page */
        ham_offset_t _self;

        /* flags for this database */
        ham_u32_t _flags;

        /* last used record number value */
        ham_offset_t _recno;

        /* reserved in 1.0.x up to 1.0.9 */
        ham_u32_t _reserved2;
    } HAM_PACK_2 b;

    ham_u8_t _space[32];
} HAM_PACK_2 db_indexdata_t;

#include "packstop.h"


/*
 * get the private data of the backend; interpretation of the
 * data is up to the backend
 */
#define db_get_indexdata_arrptr(db)                         \
    ((db_indexdata_t *)((ham_u8_t *)page_get_payload(       \
        db_get_header_page(db)) + sizeof(db_header_t)))

/*
 * get the private data of the backend; interpretation of the
 * data is up to the backend
 */
#define db_get_indexdata_ptr(db, i)       (db_get_indexdata_arrptr(db) + (i))

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

/*
 * the database structure
 */
struct ham_db_t
{
    /* the current transaction ID */
    ham_u64_t _txn_id;

    /* the last recno-value */
    ham_u64_t _recno;

    /* the last error code */
    ham_status_t _error;

    /* non-zero after this item has been opened/created.
     * Indicates whether this db is 'active', i.e. between 
     * a create/open and matching close API call. */
    ham_bool_t _is_active;

    /* a custom error handler */
    ham_errhandler_fun _errh;

    /* the user-provided context data */
    void *_context;

    /* the backend pointer - btree, hashtable etc */
    ham_backend_t *_backend;

    /* the memory allocator */
    mem_allocator_t *_allocator;

    /* linked list of all cursors */
    ham_cursor_t *_cursors;

    /* the size of the last allocated data pointer for records */
    ham_size_t _rec_allocsize;

    /* the last allocated data pointer for records */
    void *_rec_allocdata;

    /* the size of the last allocated data pointer for keys */
    ham_size_t _key_allocsize;

    /* the last allocated data pointer for keys */
    void *_key_allocdata;

    /* the prefix-comparison function */
    ham_prefix_compare_func_t _prefix_func;

    /* the comparison function */
    ham_compare_func_t _cmp_func;

    /* the duplicate comparison function */
    ham_duplicate_compare_func_t _dupe_func;

    /* the file header page */
    ham_page_t *_hdrpage;

    /* the active txn */
    ham_txn_t *_txn;

    /* the log object */
    ham_log_t *_log;

    /* the cache for extended keys */
    extkey_cache_t *_extkey_cache;

    /* the database flags - a combination of the persistent flags
     * and runtime flags */
    ham_u32_t _rt_flags;

    /* the offset of this database in the environment _indexdata */
    ham_u16_t _indexdata_offset;

    /* the environment of this database - can be NULL */
    ham_env_t *_env;

    /* the next database in a linked list of databases */
    ham_db_t *_next;

    /* the freelist cache */
    freelist_cache_t *_freelist_cache;

    /* linked list of all record-level filters */
    ham_record_filter_t *_record_filters;

    /* current data access mode (DAM) */
    ham_u16_t  _data_access_mode;

    /** some freelist algorithm specific run-time data */
    ham_runtime_statistics_globdata_t _global_perf_data;

    /** some database specific run-time data */
    ham_runtime_statistics_dbdata_t _db_perf_data;
};

/*
 * get the currently active transaction
 */
#define db_get_txn(db)                    (env_get_txn(db_get_env(db)))

/*
 * get the logging object
 */
#define db_get_log(db)                    (env_get_log(db_get_env(db)))

/*
 * get the cache for extended keys
 */
#define db_get_extkey_cache(db)           (env_get_extkey_cache(db_get_env(db)))

/*
 * get the header page
 */
#define db_get_header_page(db)            (env_get_header_page(db_get_env(db)))

/*
 * get the page size
 */
#define db_get_pagesize(db)               (env_get_pagesize(db_get_env(db)))

/**
 * get the size of the usable persistent payload of a page
 */
#define db_get_usable_pagesize(db)        (db_get_pagesize(db)                 \
                                            - db_get_persistent_header_size())

/*
 * get the current transaction ID
 */
#define db_get_txn_id(db)                 (env_get_txn_id(db_get_env(db)))

/*
 * get the last recno value
 */
#define db_get_recno(db)                  (db)->_recno

/*
 * set the last recno value
 */
#define db_set_recno(db, r)               (db)->_recno=(r)

/*
 * get the last error code
 */
#define db_get_error(db)                  (db)->_error

/*
 * set the last error code
 */
#define db_set_error(db, e)               (db)->_error=(e)

/*
 * get the user-provided context pointer
 */
#define db_get_context_data(db)           (db)->_context

/*
 * set the user-provided context pointer
 */
#define db_set_context_data(db, ctxt)     (db)->_context=(ctxt)

/*
 * get the backend pointer
 */
#define db_get_backend(db)                (db)->_backend

/*
 * set the backend pointer
 */
#define db_set_backend(db, be)            (db)->_backend=(be)

/*
 * get the memory allocator
 */
#define db_get_allocator(db)              (db)->_allocator

/*
 * set the memory allocator
 */
#define db_set_allocator(db, a)           (db)->_allocator=(a);

/*
 * get the device
 */
#define db_get_device(db)                 (env_get_device(db_get_env(db)))

/*
 * get the cache pointer
 */
#define db_get_cache(db)                  (env_get_cache(db_get_env(db)))

/*
 * get the prefix comparison function
 */
#define db_get_prefix_compare_func(db) (db)->_prefix_func

/*
 * set the prefix comparison function
 */
#define db_set_prefix_compare_func(db, f) (db)->_prefix_func=(f)

/*
 * get the key comparison function
 */
#define db_get_compare_func(db)        (db)->_cmp_func

/*
 * set the key comparison function
 */
#define db_set_compare_func(db, f)     (db)->_cmp_func=(f)

/*
 * set the duplicate comparison function
 */
#define db_set_duplicate_compare_func(db, f) (db)->_dupe_func=(f)

/*
 * get the duplicate comparison function
 */
#define db_get_duplicate_compare_func(db)    (db)->_dupe_func

/*
 * get the runtime-flags - the flags are "mixed" with the flags from the Env
 */
#define db_get_rt_flags(db)            (env_get_rt_flags(db_get_env(db))      \
                                            | (db)->_rt_flags)

/*
 * set the runtime-flags - NOT setting environment flags!
 */
#define db_set_rt_flags(db, f)         (db)->_rt_flags=(f)

/*
 * get the index of this database in the indexdata array
 */
#define db_get_indexdata_offset(db)    (db)->_indexdata_offset

/*
 * set the index of this database in the indexdata array
 */
#define db_set_indexdata_offset(db, o) (db)->_indexdata_offset=(o)

/*
 * get the environment pointer
 */
#define db_get_env(db)                 (db)->_env

/*
 * set the environment pointer
 */
#define db_set_env(db, env)            (db)->_env=(env)

/*
 * get the next database in a linked list of databases
 */
#define db_get_next(db)                (db)->_next

/*
 * set the pointer to the next database
 */
#define db_set_next(db, next)          (db)->_next=(next)

/*
 * get the freelist cache pointer
 */
#define db_get_freelist_cache(db)      (env_get_freelist_cache(db_get_env(db)))

/*
 * get the linked list of all record-level filters
 */
#define db_get_record_filter(db)       (db)->_record_filters

/*
 * set the linked list of all record-level filters
 */
#define db_set_record_filter(db, f)    (db)->_record_filters=(f)

/*
 * get the linked list of all cursors
 */
#define db_get_cursors(db)             (db)->_cursors

/*
 * set the linked list of all cursors
 */
#define db_set_cursors(db, c)          (db)->_cursors=(c)

/*
 * get the size of the last allocated data blob
 */
#define db_get_record_allocsize(db)    (db)->_rec_allocsize

/*
 * set the size of the last allocated data blob
 */
#define db_set_record_allocsize(db, s) (db)->_rec_allocsize=(s)

/*
 * get the pointer to the last allocated data blob
 */
#define db_get_record_allocdata(db)    (db)->_rec_allocdata

/*
 * set the pointer to the last allocated data blob
 */
#define db_set_record_allocdata(db, p) (db)->_rec_allocdata=(p)

/*
 * get the size of the last allocated key blob
 */
#define db_get_key_allocsize(db)       (db)->_key_allocsize

/*
 * set the size of the last allocated key blob
 */
#define db_set_key_allocsize(db, s)    (db)->_key_allocsize=(s)

/*
 * get the pointer to the last allocated key blob
 */
#define db_get_key_allocdata(db)       (db)->_key_allocdata

/*
 * set the pointer to the last allocated key blob
 */
#define db_set_key_allocdata(db, p)    (db)->_key_allocdata=(p)

/*
 * get the expected data access mode for this file
 */
#define db_get_data_access_mode(db)   (db)->_data_access_mode

/*
 * set the expected data access mode for this file
 */
#define db_set_data_access_mode(db,s)  (db)->_data_access_mode=(s)

#define db_set_data_access_mode_masked(db, or_mask, and_mask) (db)->_data_access_mode=(((db)->_data_access_mode & (and_mask)) | (or_mask))

/*
 * check if a given data access mode / mode-set has been set
 */
#define db_is_mgt_mode_set(mode_collective, mask)                \
    (((mode_collective) & (mask)) == (mask))

/**
 * check whether this database has been opened/created.
 */
#define db_is_active(db)             (db)->_is_active

/**
 * set the 'active' flag of the database: a non-zero value 
 * for @a s sets the @a db to 'active', zero(0) sets the @a db 
 * to 'inactive' (closed)
 */
#define db_set_active(db,s)          (db)->_is_active=!!(s)

/*
 * get a reference to the DB FILE (global) statistics
 */
#define db_get_global_perf_data(db)  (env_get_global_perf_data(db_get_env(db)))

/*
 * get a reference to the per-database statistics
 */
#define db_get_db_perf_data(db)      &(db)->_db_perf_data

/*
 * get a pointer to the header data
 */
#define db_get_header(db)              ((db_header_t *)(page_get_payload(\
                                          db_get_header_page(db))))

/*
 * get the size of the database header
 *
 * implemented as a function - a macro would break gcc aliasing rules
 */
extern ham_size_t
db_get_header_size(ham_db_t *db);

/*
 * get the freelist object of the database
 *
 * implemented as a function - a macro would break gcc aliasing rules
 */
extern freelist_payload_t *
db_get_freelist(ham_db_t *db);

/*
 * get the dirty-flag
 */
#define db_is_dirty(db)             page_is_dirty(db_get_header_page(db))

/*
 * set the dirty-flag
 */
#define db_set_dirty(db)            page_set_dirty(db_get_header_page(db))

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
 * on error, the database error code (db_get_error()) is set; the caller
 * HAS to check for this error!
 *
 * the default key compare function - uses memcmp
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
 * on error, the database error code (db_get_error()) is set; the caller
 * HAS to check for this error!
 */
extern int HAM_CALLCONV 
db_default_recno_compare(ham_db_t *db,
        const ham_u8_t *lhs, ham_size_t lhs_length,
        const ham_u8_t *rhs, ham_size_t rhs_length);

/**
 * the default prefix compare function - uses memcmp
 */
extern int HAM_CALLCONV 
db_default_prefix_compare(ham_db_t *db,
        const ham_u8_t *lhs, ham_size_t lhs_length,
        ham_size_t lhs_real_length,
        const ham_u8_t *rhs, ham_size_t rhs_length,
        ham_size_t rhs_real_length);

/**
 * load an extended key
 * returns the full data of the extended key in ext_key
 * 'ext_key' must have been initialized before calling this function.
 *
 * @note
 * This routine can cope with HAM_KEY_USER_ALLOC-ated 'dest'-inations.
 */
extern ham_status_t
db_get_extended_key(ham_db_t *db, ham_u8_t *key_data,
                    ham_size_t key_length, ham_u32_t key_flags,
                    ham_key_t *ext_key);

/**
 * function which compares two keys
 *
 * calls the comparison function
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
extern void
db_release_ham_key_after_compare(ham_db_t *db, ham_key_t *key);


/**
 * create a backend object according to the database flags
 */
extern ham_backend_t *
db_create_backend(ham_db_t *db, ham_u32_t flags);

/**
 * fetch a page
 */
extern ham_page_t *
db_fetch_page(ham_db_t *db, ham_offset_t address, ham_u32_t flags);

#define DB_ONLY_FROM_CACHE                0x0002

/**
 * Register new pages in the cache, but give them an 'old' age upon first creation, so they
 * are flushed before anything else.
 *
 * This is a hacky way to ensure code simplicity while blob I/O does not thrash the cache but
 * meanwhile still gets added to the activity log in a proper fashion.
 */
#define DB_NEW_PAGE_DOESNT_THRASH_CACHE    0x0004

/**
 * flush a page
 */
extern ham_status_t
db_flush_page(ham_db_t *db, ham_page_t *page, ham_u32_t flags);

/**
 * flush all pages, and clear the cache
 *
 * @param flags: set to DB_FLUSH_NODELETE if you do NOT want the cache to
 * be cleared
 */
extern ham_status_t
db_flush_all(ham_cache_t *cache, ham_u32_t flags);

#define DB_FLUSH_NODELETE       1

/**
 * allocate a new page
 *
 * !!! the page will be aligned at the current page size. any wasted
 * space (due to the alignment) is added to the freelist.
 *
 * @remark flags can be of the following value:
 *  PAGE_IGNORE_FREELIST        ignores all freelist-operations
    PAGE_CLEAR_WITH_ZERO        memset the persistent page with 0
 */
extern ham_page_t *
db_alloc_page(ham_db_t *db, ham_u32_t type, ham_u32_t flags);

#define PAGE_IGNORE_FREELIST          2
#define PAGE_CLEAR_WITH_ZERO          4

/**
 * free a page
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
 * write a page, then delete the page from memory
 *
 * @remark this function is used by the cache; it shouldn't be used
 * anywhere else.
 */
extern ham_status_t
db_write_page_and_delete(ham_page_t *page, ham_u32_t flags);

/**
 * resize the record data
 *
 * set the size to 0, and the data is freed
 */
extern ham_status_t
db_resize_allocdata(ham_db_t *db, ham_size_t size);

/**
 * an internal database flag - use mmap instead of read(2)
 */
#define DB_USE_MMAP                  0x00000100

/**
 * an internal database flag - env handle is private
 */
#define DB_ENV_IS_PRIVATE            0x00080000


#ifdef __cplusplus
} // extern "C" {
#endif

#endif /* HAM_DB_H__ */
