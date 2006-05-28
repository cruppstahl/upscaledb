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
#include "cachemgr.h"
#include "backend.h"
#include "freelist.h"

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

        /* the cache manager */
        ham_cachemgr_t *_cm;

        /* the backend */
        ham_backend_t *_backend;

        /* the prefix-comparison function */
        ham_prefix_compare_func_t _prefixcompfoo;

        /* the comparison function */
        ham_compare_func_t _compfoo;

        /* the dirty-flag - set to HAM_TRUE if the persistent header
         * was changed */
        ham_bool_t _dirty;

        /* the size of the last allocated data pointer */
        ham_size_t _allocsize;

        /* the last allocated data pointer */
        void *_allocdata;

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

            /* size of the key */
            ham_u16_t _keysize;

            /* size of the page */
            ham_u16_t _pagesize;

            /* maximum number of keys in a page */
            ham_u16_t _maxkeys;

            /* database flags which were specified when the database
             * was created */
            ham_u32_t _flags;

            /* start of the freelist */
            freel_payload_t _freelist;

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
 * get the page size
 */
#define db_get_pagesize(db)         (ham_db2h16(db->_u._pers._pagesize))

/* 
 * get maximum number of keys per page
 */
#define db_get_maxkeys(db)         (ham_db2h16(db->_u._pers._maxkeys))

/* 
 * set maximum number of keys per page
 */
#define db_set_maxkeys(db, s)      db->_u._pers._maxkeys=ham_h2db16(s)

/* 
 * get the file handle
 */
#define db_get_fd(db)              (db->_npers._fd)

/* 
 * set the file handle
 */
#define db_set_fd(db, fd)          (db)->_npers._fd=fd

/* 
 * set the flags
 */
#define db_set_flags(db, f)        (db)->_u._pers._flags=ham_h2db32(f)

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
 * get the prefix comparison function
 */
#define db_get_prefix_compare_func(db)    (db)->_npers._prefixcompfoo

/*
 * get the default comparison function
 */
#define db_get_compare_func(db)    (db)->_npers._compfoo

/*
 * get the cache manager
 */
#define db_get_cm(db)              (db)->_npers._cm

/*
 * set the cache manager
 */
#define db_set_cm(db, cm)          (db)->_npers._cm=(cm)

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
 * store a variable sized key
 *
 * @remark this function creates a blob in the database and stores
 * the key in the blob; it returns the blobid of the new key
 */
extern ham_offset_t
db_ext_key_insert(ham_db_t *db, ham_txn_t *txn, ham_page_t *page, 
        ham_key_t *key);

/**
 * delete a variable sized key
 *
 */
extern ham_status_t
db_ext_key_erase(ham_db_t *db, ham_txn_t *txn, ham_offset_t blobid);

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
extern int
db_compare_keys(ham_db_t *db, ham_page_t *page,
                long lhs_idx, ham_u32_t lhs_flags, const ham_u8_t *lhs, 
                ham_size_t lhs_length, ham_size_t lhs_real_length, 
                long rhs_idx, ham_u32_t rhs_flags, const ham_u8_t *rhs, 
                ham_size_t rhs_length, ham_size_t rhs_real_length);

/*
 * flush all pages
 *
 * @remark this function is forwarded to the cache manager
 */ 
extern ham_status_t 
db_flush_all(ham_db_t *db, ham_u32_t flags);


#ifdef __cplusplus
} // extern "C" {
#endif 

#endif /* HAM_DB_H__ */
