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
#include "os.h"

/*
 * the size of the database header page is always constant
 */
#define SIZEOF_PERS_HEADER  (1024*4)

#include "packstart.h"

/*
 * the database structure
 */
struct ham_db_t
{
    /*
     * the database has a persistent header and non-persistent runtime
     * information (i.e. file handle etc). 
     *
     * the non-persistent part of the database header
     */
    struct {
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

        /* the dirty-flag - set to HAM_TRUE if the persistent header
         * was changed */
        ham_bool_t _dirty;

        /* the size of the last allocated data pointer */
        ham_size_t _allocsize;

        /* the last allocated data pointer */
        void *_allocdata;

        /* the prefix-comparison function */
        ham_prefix_compare_func_t _prefixcompfoo;

        /* the comparison function */
        ham_compare_func_t _compfoo;

    } _npers;

    /*
     * the persistent part of the database header
     */
    union {
        /*
         * the database header is always SIZEOF_PERS_HEADER bytes long
         */
        ham_u8_t _persistent[SIZEOF_PERS_HEADER];

        HAM_PACK_0 struct HAM_PACK_1 {
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

            /* address of the freelist-page */
            ham_offset_t _freelist;

            /* private data of the index backend */
            ham_u8_t _indexdata[64];

        } HAM_PACK_2 _pers;

    } _u;

};

#include "packstop.h"

/* 
 * set the 'magic' field of a file header
 */
#define db_set_magic(db, a,b,c,d)  { db->_u._pers._magic[0]=a; \
                                     db->_u._pers._magic[1]=b; \
                                     db->_u._pers._magic[2]=c; \
                                     db->_u._pers._magic[3]=d; }

/* 
 * get byte #i of the 'magic'-header
 */
#define db_get_magic(db, i)        (db->_u._pers._magic[i])

/*
 * set the version of a file header
 */
#define db_set_version(db,a,b,c,d) { db->_u._pers._version[0]=a; \
                                     db->_u._pers._version[1]=b; \
                                     db->_u._pers._version[2]=c; \
                                     db->_u._pers._version[3]=d; }

/*
 * get byte #i of the 'version'-header
 */
#define db_get_version(db, i)      (db->_u._pers._version[i])

/*
 * get the serial number
 */
#define db_get_serialno(db)        (ham_db2h32(db->_u._pers._serialno))

/*
 * set the serial number
 */
#define db_set_serialno(db, n)     (db)->_u._pers._serialno=ham_h2db32(n)

/*
 * get the key size
 */
#define db_get_keysize(db)          (ham_db2h16(db->_u._pers._keysize))

/*
 * set the key size
 */
#define db_set_keysize(db, ks)      (db)->_u._pers._keysize=ham_db2h16(ks)

/*
 * get the page size
 */
#define db_get_pagesize(db)         (ham_db2h16(db->_u._pers._pagesize))

/*
 * set the page size
 */
#define db_set_pagesize(db, ps)     (db)->_u._pers._pagesize=ham_h2db16(ps)

/* 
 * get the file handle
 */
#define db_get_fd(db)              (db->_npers._fd)

/* 
 * set the file handle
 */
#define db_set_fd(db, fd)          (db)->_npers._fd=fd

/*
 * check if the file handle is valid
 */
#define db_is_open(db)             (db_get_fd(db)!=-1)

/* 
 * get the flags
 */
#define db_get_flags(db)           ham_db2h32((db)->_u._pers._flags)

/* 
 * set the flags
 */
#define db_set_flags(db, f)        (db)->_u._pers._flags=ham_h2db32(f)

/* 
 * get the freelist start address
 */
#define db_get_freelist(db)        ham_db2h_offset((db)->_u._pers._freelist)

/* 
 * set the freelist start address
 */
#define db_set_freelist(db, a)     (db)->_u._pers._freelist=ham_h2db_offset(a)

/*
 * get the private data of the backend; interpretation of the 
 * data is up to the backend
 */
#define db_get_indexdata(db)       (db)->_u._pers._indexdata

/* 
 * get the last error code
 */
#define db_get_error(db)           (db)->_npers._error

/* 
 * set the last error code
 */
#define db_set_error(db, e)        (db)->_npers._error=e

/* 
 * get the backend pointer
 */
#define db_get_backend(db)         (db)->_npers._backend

/* 
 * set the backend pointer
 */
#define db_set_backend(db, be)     (db)->_npers._backend=be

/*
 * get the cache pointer
 */
#define db_get_cache(db)           (db)->_npers._cache

/*
 * set the cache pointer
 */
#define db_set_cache(db, c)        (db)->_npers._cache=c

/*
 * get the prefix comparison function
 */
#define db_get_prefix_compare_func(db)    (db)->_npers._prefixcompfoo

/*
 * get the default comparison function
 */
#define db_get_compare_func(db)    (db)->_npers._compfoo

/* 
 * get the dirty-flag
 */
#define db_is_dirty(db)            (db)->_npers._dirty==HAM_TRUE

/* 
 * set the dirty-flag 
 */
#define db_set_dirty(db, d)        (db)->_npers._dirty=d

/*
 * get the size of the last allocated data blob
 */
#define db_get_record_allocsize(db) (db)->_npers._allocsize

/*
 * set the size of the last allocated data blob
 */
#define db_set_record_allocsize(db, s) (db)->_npers._allocsize=s

/*
 * get the pointer to the last allocated data blob
 */
#define db_get_record_allocdata(db) (db)->_npers._allocdata

/*
 * set the pointer to the last allocated data blob
 */
#define db_set_record_allocdata(db, p) (db)->_npers._allocdata=p

/**
 * compare two keys
 *
 * this function will call the prefix-compare function and the 
 * default compare function whenever it's necessary. 
 *
 * on error, the database error code (db_get_error()) is set; the caller
 * HAS to check for this error!
 *
 */

/**
 * the default key compare function - uses memcmp
 */
extern int 
db_default_compare(const ham_u8_t *lhs, ham_size_t lhs_length, 
                   const ham_u8_t *rhs, ham_size_t rhs_length);

/**
 * function which compares two keys
 *
 * calls the comparison function
 */
extern int
db_compare_keys(ham_db_t *db, ham_page_t *page,
                long lhs_idx, ham_u32_t lhs_flags, const ham_u8_t *lhs, 
                ham_size_t lhs_length, ham_size_t lhs_real_length, 
                long rhs_idx, ham_u32_t rhs_flags, const ham_u8_t *rhs, 
                ham_size_t rhs_length, ham_size_t rhs_real_length);

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

/**
 * flush a page
 */
extern ham_status_t
db_flush_page(ham_db_t *db, ham_txn_t *txn, ham_page_t *page,
        ham_u32_t flags);
#define DB_REVERT_CHANGES       1

/**
 * flush all page
 */
extern ham_status_t
db_flush_all(ham_db_t *db, ham_txn_t *txn);

/**
 * allocate a new page 
 *
 * !!! the page will be aligned at the current page size. any wasted 
 * space (due to the alignment) is added to the freelist.
 * TODO nur wenn NO_ALIGN nicht gesetzt ist! (sollte das nicht eher der 
 * default sein??)
 *
 * @remark flags can be of the following value:
 *  HAM_NO_PAGE_ALIGN           (see ham/hamsterdb.h)
 *  PAGE_IGNORE_FREELIST        ignores all freelist-operations
 */
extern ham_page_t *
db_alloc_page(ham_db_t *db, ham_txn_t *txn, ham_u32_t flags);
#define PAGE_IGNORE_FREELIST        0x2

/**
 * free a page
 *
 * @remark will mark the page as deleted; the page will be deleted
 * when the transaction is committed (or not deleted if the transaction
 * is aborted).
 *
 * @remark the page will automatically be unlocked, if it's locked.
 */
extern ham_status_t
db_free_page(ham_db_t *db, ham_txn_t *txn, ham_page_t *page, 
        ham_u32_t flags);

/**
 * lock a page for writing
 *
 * @remark this call might create and return a shadowpage
 *
 * @remark do not forget to unlock the page! (also note the comments for 
 * @a db_free_page)
 *
 * flags: DB_READ_ONLY
 * flags: DB_READ_WRITE 
 */
extern ham_page_t *
db_lock_page(ham_db_t *db, ham_txn_t *txn, ham_page_t *page, 
        ham_u32_t flags);
#define DB_READ_ONLY    1
#define DB_READ_WRITE   2

/**
 * unlock a page
 * 
 * flags: DB_REVERT_CHANGES
 */
extern ham_status_t
db_unlock_page(ham_db_t *db, ham_txn_t *txn, ham_page_t *page, 
        ham_u32_t flags);

/**
 * write a page, then delete the page from memory
 *
 * @remark this function is used by the cache; it shouldn't be used 
 * anywhere else.
 */
extern ham_status_t 
db_write_page_and_delete(ham_db_t *db, ham_page_t *page);

/**
 * an internal database flag - use mmap instead of read(2)
 */
#define DB_USE_MMAP                  0x00000100

#ifdef __cplusplus
} // extern "C" {
#endif 

#endif /* HAM_DB_H__ */
