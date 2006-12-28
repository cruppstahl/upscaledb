/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 * internal macros and headers
 *
 */

#ifndef HAM_DB_H__
#define HAM_DB_H__

#ifdef __cplusplus
extern "C" {
#endif

#include "endian.h"
#include "backend.h"
#include "cache.h"
#include "page.h"
#include "os.h"
#include "freelist.h"
#include "extkeys.h"

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
    ham_u16_t _pagesize;

    /* size of the key */
    ham_u16_t _keysize;

    /* global database flags which were specified when the database
     * was created */
    ham_u32_t _flags;

    /* dirty-flag */
    ham_u8_t _dirty;

    /* private data of the index backend */
    ham_u8_t _indexdata[64];

    /* the active txn */
    ham_txn_t *_txn;

    /* the cache for extended keys */
    extkey_cache_t *_extkey_cache;

    /*
     * start of the freelist - with a variable size!! don't add members
     * after this field.
     */
    freel_payload_t _freelist;

} db_header_t;

#include "packstop.h"

/*
 * set the 'magic' field of a file header
 */
#define db_set_magic(db, a,b,c,d)  { db_get_header(db)._magic[0]=a; \
                                     db_get_header(db)._magic[1]=b; \
                                     db_get_header(db)._magic[2]=c; \
                                     db_get_header(db)._magic[3]=d; }

/*
 * get byte #i of the 'magic'-header
 */
#define db_get_magic(db, i)        (db_get_header(db)._magic[i])

/*
 * set the version of a file header
 */
#define db_set_version(db,a,b,c,d) { db_get_header(db)._version[0]=a; \
                                     db_get_header(db)._version[1]=b; \
                                     db_get_header(db)._version[2]=c; \
                                     db_get_header(db)._version[3]=d; }

/*
 * get byte #i of the 'version'-header
 */
#define db_get_version(db, i)      (db_get_header(db)._version[i])

/*
 * get the serial number
 */
#define db_get_serialno(db)        (ham_db2h32(db_get_header(db)._serialno))

/*
 * set the serial number
 */
#define db_set_serialno(db, n)     db_get_header(db)._serialno=ham_h2db32(n)

/*
 * get the key size
 */
#define db_get_keysize(db)         (ham_db2h16(db_get_header(db)._keysize))

/*
 * set the key size
 */
#define db_set_keysize(db, ks)     db_get_header(db)._keysize=ham_db2h16(ks)

/*
 * get the page size
 */
#define db_get_pagesize(db)        (ham_db2h16(db_get_header(db)._pagesize))

/*
 * set the page size
 */
#define db_set_pagesize(db, ps)    db_get_header(db)._pagesize=ham_h2db16(ps)

/**
 * get the size of the usable persistent payload of a page
 */
#define db_get_usable_pagesize(db) (db_get_pagesize(db)-(sizeof(ham_u32_t)*3))

/*
 * get the flags
 */
#define db_get_flags(db)           ham_db2h32(db_get_header(db)._flags)

/*
 * set the flags
 */
#define db_set_flags(db, f)        db_get_header(db)._flags=ham_h2db32(f)

/*
 * get the private data of the backend; interpretation of the
 * data is up to the backend
 */
#define db_get_indexdata(db)       db_get_header(db)._indexdata

/*
 * get the currently active transaction
 */
#define db_get_txn(db)             db_get_header(db)._txn

/*
 * set the currently active transaction
 */
#define db_set_txn(db, txn)        db_get_header(db)._txn=txn

/*
 * get the cache for extended keys
 */
#define db_get_extkey_cache(db)    db_get_header(db)._extkey_cache

/*
 * set the cache for extended keys
 */
#define db_set_extkey_cache(db, c) db_get_header(db)._extkey_cache=c

/*
 * the database structure
 */
struct ham_db_t
{
    /* the file handle */
    ham_fd_t _fd;

    /* the last error code */
    ham_status_t _error;

    /* a custom error handler */
    ham_errhandler_fun _errh;

    /* the backend pointer - btree, hashtable etc */
    ham_backend_t *_backend;

    /* the cache */
    ham_cache_t *_cache;

    /* the freelist's private cache */
    ham_page_t *_flcache;

    /* the size of the last allocated data pointer for records */
    ham_size_t _rec_allocsize;

    /* the last allocated data pointer for records */
    void *_rec_allocdata;

    /* the size of the last allocated data pointer for keys */
    ham_size_t _key_allocsize;

    /* the last allocated data pointer for keys */
    void *_key_allocdata;

    /* the prefix-comparison function */
    ham_prefix_compare_func_t _prefixcompfoo;

    /* the comparison function */
    ham_compare_func_t _compfoo;

    /* the file header page */
    ham_page_t *_hdrpage;

    /* the database header - this is basically a mirror of the header-page
     *
     * it's needed because when a file is opened (or created), we need a
     * valid header, even when the _hdr-page is not yet available */
    db_header_t _hdr;
};

/*
 * get the header page
 */
#define db_get_header_page(db)         (db)->_hdrpage

/*
 * set the header page
 */
#define db_set_header_page(db, h)      (db)->_hdrpage=(h)

/*
 * get the file handle
 */
#define db_get_fd(db)                  (db->_fd)

/*
 * set the file handle
 */
#define db_set_fd(db, fd)              (db)->_fd=fd

/*
 * check if the file handle is valid
 */
#define db_is_open(db)              (db_get_fd(db)!=HAM_INVALID_FD)

/*
 * get the last error code
 */
#define db_get_error(db)               (db)->_error

/*
 * set the last error code
 */
#define db_set_error(db, e)            (db)->_error=e

/*
 * get the backend pointer
 */
#define db_get_backend(db)             (db)->_backend

/*
 * set the backend pointer
 */
#define db_set_backend(db, be)         (db)->_backend=be

/*
 * get the cache pointer
 */
#define db_get_cache(db)               (db)->_cache

/*
 * set the cache pointer
 */
#define db_set_cache(db, c)            (db)->_cache=c

/*
 * get the freelist's private cache
 */
#define db_get_freelist_cache(db)      (db)->_flcache

/*
 * set the freelist's private cache
 */
#define db_set_freelist_cache(db, c)   (db)->_flcache=c

/*
 * get the prefix comparison function
 */
#define db_get_prefix_compare_func(db) (db)->_prefixcompfoo

/*
 * set the prefix comparison function
 */
#define db_set_prefix_compare_func(db, f) (db)->_prefixcompfoo=f

/*
 * get the default comparison function
 */
#define db_get_compare_func(db)        (db)->_compfoo

/*
 * set the default comparison function
 */
#define db_set_compare_func(db, f)     (db)->_compfoo=f

/*
 * get the dirty-flag
 */
#define db_is_dirty(db)                (db)->_hdr._dirty

/*
 * set the dirty-flag
 */
#define db_set_dirty(db, d)            (db)->_hdr._dirty=d

/*
 * get the size of the last allocated data blob
 */
#define db_get_record_allocsize(db)    (db)->_rec_allocsize

/*
 * set the size of the last allocated data blob
 */
#define db_set_record_allocsize(db, s) (db)->_rec_allocsize=s

/*
 * get the pointer to the last allocated data blob
 */
#define db_get_record_allocdata(db)    (db)->_rec_allocdata

/*
 * set the pointer to the last allocated data blob
 */
#define db_set_record_allocdata(db, p) (db)->_rec_allocdata=p

/*
 * get the size of the last allocated key blob
 */
#define db_get_key_allocsize(db)       (db)->_key_allocsize

/*
 * set the size of the last allocated key blob
 */
#define db_set_key_allocsize(db, s)    (db)->_key_allocsize=s

/*
 * get the pointer to the last allocated key blob
 */
#define db_get_key_allocdata(db)       (db)->_key_allocdata

/*
 * set the pointer to the last allocated key blob
 */
#define db_set_key_allocdata(db, p)    (db)->_key_allocdata=p
/*
 * get a pointer to the header data
 */
#define db_get_header(db)              (db)->_hdr

/**
 * uncouple all cursors from a page
 *
 * @remark this is called whenever the page is deleted or becoming invalid
 */
extern ham_status_t
db_uncouple_all_cursors(ham_db_t *db, ham_page_t *page);

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
extern int
db_default_compare(const ham_u8_t *lhs, ham_size_t lhs_length,
                   const ham_u8_t *rhs, ham_size_t rhs_length);

/**
 * the default prefix compare function - uses memcmp
 */
extern int
db_default_prefix_compare(const ham_u8_t *lhs, ham_size_t lhs_length,
                   ham_size_t lhs_real_length,
                   const ham_u8_t *rhs, ham_size_t rhs_length,
                   ham_size_t rhs_real_length);

/**
 * load an extended key
 * returns the full data of the extended key in ext_key
 */
extern ham_status_t
db_get_extended_key(ham_db_t *db, ham_txn_t *txn, ham_u8_t *key_data,
                    ham_size_t key_length, ham_u32_t key_flags,
                    ham_u8_t **ext_key);

/**
 * function which compares two keys
 *
 * calls the comparison function
 */
extern int
db_compare_keys(ham_db_t *db, ham_txn_t *txn, ham_page_t *page,
                long lhs_idx, ham_u32_t lhs_flags,
                const ham_u8_t *lhs, ham_size_t lhs_length,
                long rhs_idx, ham_u32_t rhs_flags,
                const ham_u8_t *rhs, ham_size_t rhs_length);

/**
 * create a backend object according to the database flags
 */
extern ham_backend_t *
db_create_backend(ham_db_t *db, ham_u32_t flags);

/**
 * fetch a page
 */
extern ham_page_t *
db_fetch_page(ham_db_t *db, ham_txn_t *txn, ham_offset_t address,
        ham_u32_t flags);
#define DB_READ_ONLY            1
#define DB_ONLY_FROM_CACHE      2

/**
 * flush a page
 */
extern ham_status_t
db_flush_page(ham_db_t *db, ham_txn_t *txn, ham_page_t *page,
        ham_u32_t flags);
#define DB_REVERT_CHANGES       1

/**
 * flush all pages, and clear the cache
 *
 * @param flags: set to DB_FLUSH_NODELETE if you do NOT want the cache to
 * be cleared
 */
extern ham_status_t
db_flush_all(ham_db_t *db, ham_txn_t *txn, ham_u32_t flags);
#define DB_FLUSH_NODELETE       1

/**
 * allocate memory for a ham_db_t-structure
 */
extern ham_page_t *
db_alloc_page_struct(ham_db_t *db);

/**
 * free memory of a page
 *
 * !!!
 * will NOT write the page to the device!
 */
extern void
db_free_page_struct(ham_page_t *page);

/**
 * write a page to the device
 */
extern ham_status_t
db_write_page_to_device(ham_page_t *page);

/**
 * read a page from the device
 */
extern ham_status_t
db_fetch_page_from_device(ham_page_t *page, ham_offset_t address);

/**
 * allocate a new page on the device
 *
 * @remark flags can be of the following value:
 *  PAGE_IGNORE_FREELIST        ignores all freelist-operations
    PAGE_CLEAR_WITH_ZERO        memset the persistent page with 0
 */
extern ham_page_t *
db_alloc_page_device(ham_db_t *db, ham_txn_t *txn, ham_u32_t flags);

/**
 * allocate a new page
 *
 * !!! the page will be aligned at the current page size. any wasted
 * space (due to the alignment) is added to the freelist.
 * TODO nur wenn NO_ALIGN nicht gesetzt ist! (sollte das nicht eher der
 * default sein??)
 *
 * @remark flags can be of the following value:
 *  PAGE_IGNORE_FREELIST        ignores all freelist-operations
    PAGE_CLEAR_WITH_ZERO        memset the persistent page with 0
 */
extern ham_page_t *
db_alloc_page(ham_db_t *db, ham_u32_t type, ham_txn_t *txn, ham_u32_t flags);
#define PAGE_IGNORE_FREELIST          2
#define PAGE_CLEAR_WITH_ZERO          4

/**
 * free a page
 *
 * @remark will mark the page as deleted; the page will be deleted
 * when the transaction is committed (or not deleted if the transaction
 * is aborted).
 */
extern ham_status_t
db_free_page(ham_db_t *db, ham_txn_t *txn, ham_page_t *page,
        ham_u32_t flags);

/**
 * write a page, then delete the page from memory
 *
 * @remark this function is used by the cache; it shouldn't be used
 * anywhere else.
 */
extern ham_status_t
db_write_page_and_delete(ham_db_t *db, ham_page_t *page, ham_u32_t flags);

/**
 * an internal database flag - use mmap instead of read(2)
 */
#define DB_USE_MMAP                  0x00000100

#ifdef __cplusplus
} // extern "C" {
#endif

#endif /* HAM_DB_H__ */
