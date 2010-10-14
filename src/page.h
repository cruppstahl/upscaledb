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
 * @brief an object which handles a database page 
 *
 */

#ifndef HAM_PAGE_H__
#define HAM_PAGE_H__

#include "internal_fwd_decl.h"

#include "endianswap.h"
#include "error.h"


#ifdef __cplusplus
extern "C" {
#endif

/*
 * indices for page lists
 *
 * Each ham_page_t structure is a node in several linked lists.
 * In order to avoid multiple memory allocations, the previous/next pointers 
 * are part of the ham_page_t structure (_npers._prev and _npers._next).
 * Both fields are arrays of pointers and can be used i.e.
 * with _npers._prev[PAGE_LIST_BUCKET] etc. (or with the macros 
 * defined below).
 */

/* a bucket in the hash table of the cache manager */
#define PAGE_LIST_BUCKET           0
/* garbage collected pages */
#define PAGE_LIST_GARBAGE          1
/* list of all cached pages */
#define PAGE_LIST_CACHED           2
/* array limit */
#define MAX_PAGE_LISTS             3

#include "packstart.h"

/**
 * The page header which is persisted on disc
 *
 * This structure definition is present outside of @ref ham_page_t scope 
 * to allow compile-time OFFSETOF macros to correctly judge the size, depending 
 * on platform and compiler settings.
 */
typedef HAM_PACK_0 union HAM_PACK_1 ham_perm_page_union_t
{
    /*
     * this header is only available if the (non-persistent) flag
     * NPERS_NO_HEADER is not set! 
     *
     * all blob-areas in the file do not have such a header, if they
     * span page-boundaries
     *
     * !!
     * if this structure is changed, db_get_usable_pagesize has 
     * to be changed as well!
     */
    HAM_PACK_0 struct HAM_PACK_1 page_union_header_t {
        /**
         * flags of this page - currently only used for the page type
         * @sa page_type_codes
         */
        ham_u32_t _flags;

        /** some reserved bytes */
        ham_u32_t _reserved1;
        ham_u32_t _reserved2;

        /**
         * this is just a blob - the backend (hashdb, btree etc) 
         * will use it appropriately
         */
        ham_u8_t _payload[1];
    } HAM_PACK_2 _s;

    /* a char pointer to the allocated storage on disk */
    ham_u8_t _p[1];

} HAM_PACK_2 ham_perm_page_union_t;

#include "packstop.h"

/**
 * get the size of the persistent header of a page
 *
 * equals the size of struct ham_perm_page_union_t, without the payload byte
 *
 * @note
 * this is not equal to sizeof(struct ham_perm_page_union_t)-1, because of
 * padding (i.e. on gcc 4.1/64bit the size would be 15 bytes)
 */
#define page_get_persistent_header_size()   (OFFSETOF(ham_perm_page_union_t, _s._payload) /*(sizeof(ham_u32_t)*3)*/ /* 12 */ )


/**
 * the page structure
 */
struct ham_page_t {
    /**
     * the header is non-persistent and NOT written to disk. 
     * it's caching some run-time values which
     * we don't want to recalculate whenever we need them.
     */
    struct {
        /** address of this page */
        ham_offset_t _self;

        /** the allocator */
        struct mem_allocator_t *_alloc;

        /** reference to the database object */
        ham_db_t *_owner;

        /** the device of this page */
        struct ham_device_t *_device;

        /** non-persistent flags */
        ham_u32_t _flags;

        /** cache counter - used by the cache module
         *
         * The higher the counter, the more 'important' this page is
         * believed to be; when searching for pages to re-use, the empty
         * page with the lowest counter value is re-purposed. Each valid
         * page use bumps up the page counter by a certain amount, up to
         * a page-type specific upper bound.
         *
         * See also @ref cache_put_page()
         * and its invocations in the code. @ref page_new() initialized
         * the counter for each new page.
         */
        ham_u32_t _cache_cntr;

        /** locks the page; if a page is locked, the cache will not purge it */
        ham_u32_t _lock;

        /** is this page dirty and needs to be flushed to disk? */
        ham_u32_t _dirty;

#if defined(HAM_OS_WIN32) || defined(HAM_OS_WIN64)
        /** handle for win32 mmap */
        HANDLE _win32mmap;
#endif

        /** linked lists of pages - see comments above */
        ham_page_t *_prev[MAX_PAGE_LISTS], *_next[MAX_PAGE_LISTS];

        /** linked list of all cursors which point to that page */
        ham_cursor_t *_cursors;

        /** the lsn of the last BEFORE-image, that was written to the log */
        ham_u64_t _before_img_lsn;

    } _npers; 

    /**
     * from here on everything will be written to disk 
     */
    ham_perm_page_union_t *_pers;
};

/** get the address of this page */
#define page_get_self(page)          ((page)->_npers._self)

/** set the address of this page */
#define page_set_self(page, a)       (page)->_npers._self=(a)

/** * the database object which 0wnz this page */
#define page_get_owner(page)         ((page)->_npers._owner)

/** set the database object which 0wnz this page */
#define page_set_owner(page, db)     (page)->_npers._owner=(db)

/** get the previous page of a linked list */
#ifdef HAM_DEBUG
extern ham_page_t *
page_get_previous(ham_page_t *page, int which);
#else
#   define page_get_previous(page, which)    ((page)->_npers._prev[(which)])
#endif /* HAM_DEBUG */

/** set the previous page of a linked list */
#ifdef HAM_DEBUG
extern void
page_set_previous(ham_page_t *page, int which, ham_page_t *other);
#else
#   define page_set_previous(page, which, p) (page)->_npers._prev[(which)]=(p)
#endif /* HAM_DEBUG */

/** get the next page of a linked list */
#ifdef HAM_DEBUG
extern ham_page_t *
page_get_next(ham_page_t *page, int which);
#else
#   define page_get_next(page, which)        ((page)->_npers._next[(which)])
#endif /* HAM_DEBUG */

/** set the next page of a linked list */
#ifdef HAM_DEBUG
extern void
page_set_next(ham_page_t *page, int which, ham_page_t *other);
#else
#   define page_set_next(page, which, p)     (page)->_npers._next[(which)]=(p)
#endif /* HAM_DEBUG */

/** get memory allocator */
#define page_get_allocator(page)             (page)->_npers._alloc

/** set memory allocator */
#define page_set_allocator(page, a)          (page)->_npers._alloc=a

/** get the device of this page */
#define page_get_device(page)                (page)->_npers._device

/** set the device of this page */
#define page_set_device(page, d)             (page)->_npers._device=d

/** get linked list of cursors */
#define page_get_cursors(page)           (page)->_npers._cursors

/** set linked list of cursors */
#define page_set_cursors(page, c)        (page)->_npers._cursors=(c)

/** get the lsn of the last BEFORE-image that was written to the log */
#define page_get_before_img_lsn(page)    (page)->_npers._before_img_lsn

/** set the lsn of the last BEFORE-image that was written to the log */
#define page_set_before_img_lsn(page, l) (page)->_npers._before_img_lsn=(l)

/** get persistent page flags */
#define page_get_pers_flags(page)        (ham_db2h32((page)->_pers->_s._flags))

/** set persistent page flags */
#define page_set_pers_flags(page, f)     (page)->_pers->_s._flags=ham_h2db32(f)

/** get non-persistent page flags */
#define page_get_npers_flags(page)       (page)->_npers._flags

/** set non-persistent page flags */
#define page_set_npers_flags(page, f)    (page)->_npers._flags=(f)

/** get the cache counter */
#define page_get_cache_cntr(page)        (page)->_npers._cache_cntr

/** set the cache counter */
#define page_set_cache_cntr(page, c)     (page)->_npers._cache_cntr=(c)

/** is the page locked? */
#define page_is_locked(page)             (page)->_npers._lock

/** lock this page */
#define page_lock(page)                  (++(page)->_npers._lock)

/** unlock this page */
#define page_unlock(page)       do {                                          \
                                    ham_assert(page_is_locked(page)>0, ("")); \
                                    --(page)->_npers._lock;                   \
                                } while (0)

/** page->_pers was allocated with malloc, not mmap */
#define PAGE_NPERS_MALLOC               1

/** page will be deleted when committed */
#define PAGE_NPERS_DELETE_PENDING       2

/** page has no header */
#define PAGE_NPERS_NO_HEADER            4

/** is this page dirty? */
#define page_is_dirty(page)             ((page)->_npers._dirty)

/** mark this page dirty */
#define page_set_dirty(page)            (page)->_npers._dirty=1

/** page is no longer dirty */
#define page_set_undirty(page)          (page)->_npers._dirty=0

#if defined(HAM_OS_WIN32) || defined(HAM_OS_WIN64)
/** win32: get a pointer to the mmap handle */
#   define page_get_mmap_handle_ptr(p)  &((p)->_npers._win32mmap)
#else
#   define page_get_mmap_handle_ptr(p)  0
#endif

/** set the page-type */
#define page_set_type(page, t)          page_set_pers_flags(page, t)

/** get the page-type */
#define page_get_type(page)             (page_get_pers_flags(page))

/**
 * @defgroup page_type_codes valid page types
 * @{
 *
 * Each database page is tagged with a type code; these are all 
 * known/supported page type codes.
 * 
 * @note When ELBLOBs (Extremely Large BLOBs) are stored in the database, 
 * that is BLOBs which span multiple pages apiece, only their initial page 
 * will have a valid type code; subsequent pages of the ELBLOB will store 
 * the data as-is, so as to provide one continuous storage space per ELBLOB.
 * 
 * @sa ham_perm_page_union_t::page_union_header_t::_flags
 */

/** unidentified db page type */
#define PAGE_TYPE_UNKNOWN               0x00000000     

/** the db header page: this is the first page in the database/environment */
#define PAGE_TYPE_HEADER                0x10000000     

/** the db B+tree root page */
#define PAGE_TYPE_B_ROOT                0x20000000     

/** a B+tree node page, i.e. a page which is part of the database index */
#define PAGE_TYPE_B_INDEX               0x30000000     

/** a freelist management page */
#define PAGE_TYPE_FREELIST              0x40000000     

/** a page which stores (the front part of) a BLOB. */
#define PAGE_TYPE_BLOB                  0x50000000     

/**
 * @}
 */

/** get pointer to persistent payload (after the header!) */
#define page_get_payload(page)          (page)->_pers->_s._payload

/** get pointer to persistent payload (including the header!) */
#define page_get_raw_payload(page)      (page)->_pers->_p

/** set pointer to persistent data */
#define page_set_pers(page, p)          (page)->_pers=(p)

/** get pointer to persistent data */
#define page_get_pers(page)             (page)->_pers

/**
 * check if a page is in a linked list
 */
extern ham_bool_t 
page_is_in_list(ham_page_t *head, ham_page_t *page, int which);

/**
 * linked list functions: insert the page at the beginning of a list
 *
 * @remark returns the new head of the list
 */
extern ham_page_t *
page_list_insert(ham_page_t *head, int which, ham_page_t *page);

/**
 * linked list functions: remove the page from a list
 *
 * @remark returns the new head of the list
 */
extern ham_page_t *
page_list_remove(ham_page_t *head, int which, ham_page_t *page);

/**
 * add a cursor to this page
 */
extern void
page_add_cursor(ham_page_t *page, ham_cursor_t *cursor);

/**
 * remove a cursor from this page
 */
extern void
page_remove_cursor(ham_page_t *page, ham_cursor_t *cursor);

/**
 * create a new page structure
 *
 * @return a pointer to a new @ref ham_page_t instance.
 *
 * @return NULL if out of memory
 */
extern ham_page_t *
page_new(ham_env_t *env);

/**
 * delete a page structure
 */
extern void
page_delete(ham_page_t *page);

/**
 * allocate a new page from the device
 */
extern ham_status_t
page_alloc(ham_page_t *page);

/**
 * fetch a page from the device
 */
extern ham_status_t
page_fetch(ham_page_t *page);

/**
 * write a page to the device
 */
extern ham_status_t
page_flush(ham_page_t *page);

/**
 * free a page
 */
extern ham_status_t
page_free(ham_page_t *page);


#ifdef __cplusplus
} // extern "C" {
#endif

#endif /* HAM_PAGE_H__ */
