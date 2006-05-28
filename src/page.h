/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 * an object which handles a database page 
 *
 */

#ifndef HAM_PAGE_H__
#define HAM_PAGE_H__

#ifdef __cplusplus
extern "C" {
#endif 

#include <ham/hamsterdb.h>
#include "endian.h"

/*
 * indices for page lists
 *
 * each page is a node in several linked lists - via _npers._prev and 
 * _npers._next. both members are arrays of pointers and can be used
 * with _npers._prev[PAGE_LIST_BUCKET] etc. (or with the macros 
 * defined below).
 */

/* a bucket in the hash table of the cache manager */
#define PAGE_LIST_BUCKET           0
/* a node in the linked list of unreferenced pages */
#define PAGE_LIST_UNREF            1
/* a node in the linked list of a transaction */
#define PAGE_LIST_TXN              2
/* garbage collected pages */
#define PAGE_LIST_GARBAGE          3
/* array limit */
#define MAX_PAGE_LISTS             4

/**
 * an "extended key" - a structure for caching variable sized keys
 * in memory
 */
typedef struct ham_ext_key_t {
    /** the key size */
    ham_u32_t size;

    /** the key data */
    ham_u8_t *data; 

} ham_ext_key_t;

/**
 * the page structure
 */
struct ham_page_t;
typedef struct ham_page_t ham_page_t;
struct ham_page_t {
    /**
     * the header is non-persistent and NOT written to disk. 
     * it's caching some run-time values which
     * we don't want to recalculate whenever we need them.
     */
    struct {
        /** address of this page */
        ham_offset_t _self;

        /** reference to the database object */
        ham_db_t *_owner;

        /** a reference counter */
        ham_size_t _refcount;

        /** "dirty"-flag */
        ham_bool_t _dirty;

        /** linked lists of pages - see comments above */
        ham_page_t *_prev[MAX_PAGE_LISTS], *_next[MAX_PAGE_LISTS];

        /** pointer to a shadow-page of this page */
        ham_page_t *_shadowpage;

        /** pointer to the original page, if this page is a shadowpage */
        ham_page_t *_orig_page;

        /** cached array of variable sized keys */
        ham_ext_key_t *_extkeys;

    } _npers; 

    /**
     * from here on everything will be written to disk 
     */
    struct {
        /** 
         * this is just a blob - the backend (hashdb, btree etc) 
         * will use it appropriately
         */
        ham_u8_t _payload[1];

    } _pers;

};

/**
 * get the address of this page
 */
#define page_get_self(page)         (ham_db2h_offset((page)->_npers._self))

/**
 * set the address of this page
 */
#define page_set_self(page, a)      (page)->_npers._self=ham_h2db_offset(a)

/** 
 * get the database object which 0wnz this page 
 */
#define page_get_owner(page)        ((page)->_npers._owner)

/** 
 * set the database object which 0wnz this page 
 */
#define page_set_owner(page, db)    (page)->_npers._owner=db

/** 
 * get the dirty-flag
 */
#define page_is_dirty(page)         ((page)->_npers._dirty)

/** 
 * set the dirty-flag
 */
#define page_set_dirty(page, d)     (page)->_npers._dirty=d

/** 
 * get the shadowpage of this page
 */
#define page_get_shadowpage(page)   ((page)->_npers._shadowpage)

/** 
 * set the shadowpage of this page
 */
#define page_set_shadowpage(page, s) (page)->_npers._shadowpage=s

/** 
 * get the original page of this shadowpage 
 */
#define page_get_orig_page(page)    ((page)->_npers._orig_page)

/** 
 * set the original page of this shadowpage 
 */
#define page_set_orig_page(page, s) (page)->_npers._orig_page=s

/** 
 * get the extended key array of this page
 */
#define page_get_extkeys(page)      ((page)->_npers._extkeys)

/** 
 * set the extended key array of this page
 */
#define page_set_extkeys(page, x)   (page)->_npers._extkeys=x

/** 
 * get the previous page of a linked list
#define page_get_previous(page, which)    ((page)->_npers._prev[(which)])
 */
extern ham_page_t *
page_get_previous(ham_page_t *page, int which);

/** 
 * set the previous page of a linked list
#define page_set_previous(page, which, p) (page)->_npers._prev[(which)]=(p)
 */
extern void
page_set_previous(ham_page_t *page, int which, ham_page_t *other);

/** 
 * get the next page of a linked list
#define page_get_next(page, which)        ((page)->_npers._next[(which)])
 */
extern ham_page_t *
page_get_next(ham_page_t *page, int which);

/** 
 * set the next page of a linked list
#define page_set_next(page, which, p)     (page)->_npers._next[(which)]=(p)
 */
extern void
page_set_next(ham_page_t *page, int which, ham_page_t *other);

/**
 * get pointer to persistent payload
 */
#define page_get_payload(page)            (page)->_pers._payload

/**
 * allocate a new page in RAM
 */
extern ham_page_t *
page_new(ham_db_t *db);

/**
 * release allocated memory for extended keys
 */
extern void
page_delete_ext_keys(ham_page_t *page);

/**
 * release the allocated memory
 *
 * @remark this function calls page_delete_ext_keys().
 */
extern void
page_delete(ham_page_t *page);

/**
 * release the allocated memory
 */
extern void
page_delete(ham_page_t *page);

/**
 * get the reference counter
 */
#define page_ref_get(p)            ((p)->_npers._refcount)

#define page_ref_inc(p, f) page_ref_inc_impl(p, __FILE__, __LINE__)
#define page_ref_dec(p, f) page_ref_dec_impl(p, __FILE__, __LINE__)
/**
 * increase the reference counter
 */
extern void
page_ref_inc_impl(ham_page_t *page, const char *file, int line);

/**
 * decrease the reference counter
 *
 * @remark returns the new reference counter
 */
extern ham_size_t
page_ref_dec_impl(ham_page_t *page, const char *file, int line);

/**
 * read a page from the database file
 */
extern ham_status_t
page_io_read(ham_page_t *page, ham_offset_t address);

/**
 * write a page to the database file
 */
extern ham_status_t
page_io_write(ham_page_t *page);

/**
 * allocate a new page in the file; the page will be aligned at
 * the current page size. any wasted space (due to the alignment) is added 
 * to the freelist.
 *
 * @remark flags can be of the following value:
 *  HAM_NO_PAGE_ALIGN           (see ham/hamsterdb.h)
 *  PAGE_IGNORE_FREELIST        ignores all freelist-operations
 */
extern ham_status_t
page_io_alloc(ham_page_t *page, ham_txn_t *txn, ham_u32_t flags);

#define PAGE_IGNORE_FREELIST        0x2

/**
 * free a page of the file; this page is added to the freelist. the 
 * allocated RAM of the page is NOT released! 
 */
extern ham_status_t
page_io_free(ham_txn_t *txn, ham_page_t *page);

/**
 * linked list functions: insert the page at the beginning of a list
 *
 * @remark returns the new head of the list
 */
extern ham_page_t *
page_list_insert(ham_page_t *head, int which, ham_page_t *page);

/**
 * linked list functions: insert the page at the beginning of a ring list
 *
 * @remark returns the new head of the list
 */
extern ham_page_t *
page_list_insert_ring(ham_page_t *head, int which, ham_page_t *page);

/**
 * linked list functions: remove the page from a list
 *
 * @remark returns the new head of the list
 */
extern ham_page_t *
page_list_remove(ham_page_t *head, int which, ham_page_t *page);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_PAGE_H__ */
