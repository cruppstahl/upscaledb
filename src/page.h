/*
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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

#include <string.h>

#include "internal_fwd_decl.h"

#include "endianswap.h"
#include "error.h"
#include "mem.h"

#include "packstart.h"

/*
 * This header is only available if the (non-persistent) flag
 * NPERS_NO_HEADER is not set!
 *
 * all blob-areas in the file do not have such a header, if they
 * span page-boundaries
 *
 * !!
 * if this structure is changed, db_get_usable_pagesize has
 * to be changed as well!
 */
HAM_PACK_0 struct HAM_PACK_1 page_header_t {
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

} HAM_PACK_2;

/**
 * The page header which is persisted on disc
 *
 * This structure definition is present outside of @ref Page scope
 * to allow compile-time OFFSETOF macros to correctly judge the size, depending
 * on platform and compiler settings.
 */
typedef HAM_PACK_0 union HAM_PACK_1 page_data_t
{
    /** the persistent header */
    struct page_header_t _s;

    /** a char pointer to the allocated storage on disk */
    ham_u8_t _p[1];

} HAM_PACK_2 page_data_t;

#include "packstop.h"


/**
 * The Page class
 *
 * Each Page instance is a node in several linked lists.
 * In order to avoid multiple memory allocations, the previous/next pointers
 * are part of the Page class (m_prev and m_next).
 * Both fields are arrays of pointers and can be used i.e.
 * with m_prev[Page::LIST_BUCKET] etc. (or with the methods
 * defined below).
 */
class Page {
  public:
    /**
     * get the size of the persistent header of a page
     *
     * equals the size of struct page_data_t, without the payload byte
     *
     * @note
     * this is not equal to sizeof(struct page_data_t)-1, because of
     * padding (i.e. on gcc 4.1/64bit the size would be 15 bytes)
     */
    static int sizeof_persistent_header;

    enum {
        /** a bucket in the hash table of the cache manager */
        LIST_BUCKET     = 0,
        /** list of all cached pages */
        LIST_CACHED     = 1,
        /** list of all pages in a changeset */
        LIST_CHANGESET  = 2,
        /** array limit */
        MAX_LISTS       = 3
    };

    enum {
        /** page->m_pers was allocated with malloc, not mmap */
        NPERS_MALLOC            = 1,
        /** page will be deleted when committed */
        NPERS_DELETE_PENDING    = 2,
        /** page has no header */
        NPERS_NO_HEADER         = 4
    };

    /**
     * Page types
     *
     * @note When large BLOBs span multiple pages, only their initial page
     * will have a valid type code; subsequent pages of this blog will store
     * the data as-is, so as to provide one continuous storage space
     */
    enum {
        /** unidentified db page type */
        TYPE_UNKNOWN            =  0x00000000,
        /** the db header page: this is the first page in the environment */
        TYPE_HEADER             =  0x10000000,
        /** the db B+tree root page */
        TYPE_B_ROOT             =  0x20000000,
        /** a B+tree node page */
        TYPE_B_INDEX            =  0x30000000,
        /** a freelist management page */
        TYPE_FREELIST           =  0x40000000,
        /** a page which stores (the front part of) a BLOB. */
        TYPE_BLOB               =  0x50000000
    };


    /** default constructor */
    Page(Environment *env=0, Database *db=0);

    /** destructor - asserts that m_pers is NULL! */
    ~Page();

    /** is this the header page? */
    bool is_header() {
        return (m_self==0);
    }

    /** get the address of this page */
    ham_offset_t get_self() {
        return (m_self);
    }

    /** set the address of this page */
    void set_self(ham_offset_t address) {
        m_self=address;
    }

    /** the database object which 0wnz this page */
    Database *get_db() {
        return (m_db);
    }

    /** set the database object which 0wnz this page */
    void set_db(Database *db) {
        m_db=db;
    }

    /** get the device of this page */
    Device *get_device() {
        return (m_device);
    }

    /** set the device of this page */
    void set_device(Device *device) {
        m_device=device;
    }

    /** get non-persistent page flags */
    ham_u32_t get_flags() {
        return (m_flags);
    }

    /** set non-persistent page flags */
    void set_flags(ham_u32_t flags) {
        m_flags=flags;
    }

    /** is this page dirty? */
    bool is_dirty() {
        return (m_dirty);
    }

    /** mark this page dirty/not dirty */
    void set_dirty(bool dirty) {
        m_dirty=dirty;
    }

    /** win32: get a pointer to the mmap handle */
#if defined(HAM_OS_WIN32) || defined(HAM_OS_WIN64)
    ham_fd_t *get_mmap_handle_ptr() {
        return (&m_win32mmap);
    }
#else
    ham_fd_t *get_mmap_handle_ptr() {
        return (0);
    }
#endif

    /** get linked list of cursors */
    Cursor *get_cursors() {
        return (m_cursors);
    }

    /** set linked list of cursors */
    void set_cursors(Cursor *cursor) {
        m_cursors=cursor;
    }

    /** get the previous page of a linked list */
    Page *get_previous(int which) {
        return (m_prev[which]);
    }

    /** set the previous page of a linked list */
    void set_previous(int which, Page *other) {
        m_prev[which]=other;
    }

    /** get the next page of a linked list */
    Page *get_next(int which) {
        return (m_next[which]);
    }

    /** set the next page of a linked list */
    void set_next(int which, Page *other) {
        m_next[which]=other;
    }

    /** set the page-type */
    void set_type(ham_u32_t type) {
        m_pers->_s._flags=ham_h2db32(type);
    }

    /** get the page-type */
    ham_u32_t get_type() {
        return (ham_db2h32(m_pers->_s._flags));
    }

    /** get pointer to persistent payload (after the header!) */
    ham_u8_t *get_payload() {
        return (m_pers->_s._payload);
    }

    /** get pointer to persistent payload (including the header!) */
    ham_u8_t *get_raw_payload() {
        return (m_pers->_p);
    }

    /** set pointer to persistent data */
    void set_pers(page_data_t *data) {
        m_pers=data;
    }

    /** get pointer to persistent data */
    page_data_t *get_pers() {
        return (m_pers);
    }

    /** allocate a new page from the device */
    ham_status_t allocate();

    /** read a page from the device */
    ham_status_t fetch(ham_offset_t address);

    /** write a page to the device */
    ham_status_t flush();

    /** frees a page - deletes the persistent part and moves the page to
     * the freelist (if a freelist is available) */
    ham_status_t free();

    /** returns true if this page is in a linked list */
    bool is_in_list(Page *list_head, int which) {
        if (get_next(which))
            return (true);
        if (get_previous(which))
            return (true);
        if (list_head==this)
            return (true);
        return (false);
    }

    /** inserts this page at the beginning of a list and returns the
     * new head of the list
     */
    Page *list_insert(Page *list_head, int which) {
        set_next(which, 0);
        set_previous(which, 0);

        if (!list_head)
            return (this);

        set_next(which, list_head);
        list_head->set_previous(which, this);
        return (this);
    }

    /** removes this page from a list and returns the new head of the list */
    Page *list_remove(Page *list_head, int which) {
        Page *n, *p;

        if (this==list_head) {
            n=get_next(which);
            if (n)
                n->set_previous(which, 0);
            set_next(which, 0);
            set_previous(which, 0);
            return (n);
        }

        n=get_next(which);
        p=get_previous(which);
        if (p)
            p->set_next(which, n);
        if (n)
            n->set_previous(which, p);
        set_next(which, 0);
        set_previous(which, 0);
        return (list_head);
    }

    /** adds a cursor to this page */
    void add_cursor(Cursor *cursor);

    /** removes a cursor from this page */
    void remove_cursor(Cursor *cursor);

    /** uncouples all cursors from this page if the cursor is coupled
     * to a key >= start */
    ham_status_t uncouple_all_cursors(ham_size_t start=0);

  private:
    /** address of this page */
    ham_offset_t m_self;

    /** reference to the database object; can be NULL */
    Database *m_db;

    /** the device of this page */
    Device *m_device;

    /** non-persistent flags */
    ham_u32_t m_flags;

    /** is this page dirty and needs to be flushed to disk? */
    bool m_dirty;

#if defined(HAM_OS_WIN32) || defined(HAM_OS_WIN64)
    /** handle for win32 mmap */
    HANDLE m_win32mmap;
#endif

    /** linked list of all cursors which point to that page */
    Cursor *m_cursors;

    /** linked lists of pages - see comments above */
    Page *m_prev[Page::MAX_LISTS];
    Page *m_next[Page::MAX_LISTS];

    /** from here on everything will be written to disk */
    page_data_t *m_pers;
};


#endif /* HAM_PAGE_H__ */
