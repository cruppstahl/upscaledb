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
 * @brief a base class for a generic database backend
 *
 */

#ifndef HAM_BACKEND_H__
#define HAM_BACKEND_H__

#include "internal_fwd_decl.h"

/**
* @defgroup ham_cb_status hamsterdb Backend Node/Page Enumerator Status Codes
* @{
*/

/** continue with the traversal */
#define CB_CONTINUE            0 
/** do not not descend another level (or descend from page to key traversal) */
#define CB_DO_NOT_DESCEND    1 
/** stop the traversal entirely */
#define CB_STOP                2 

/**
 * @}
 */


/**
 * a callback function for enumerating the index nodes/pages using the 
 * @ref Backend::enumerate callback/method.
 *
 * @param event one of the @ref ham_cb_event state codes
 *
 * @return one of the @ref ham_cb_status values or a @ref ham_status_codes 
 *         error code when an error occurred.
 */
typedef ham_status_t (*ham_enumerate_cb_t)(int event, void *param1, 
                    void *param2, void *context);

/**
* @defgroup ham_cb_event hamsterdb Backend Node/Page Enumerator State Codes
* @{
*/

/** descend one level; param1 is an integer value with the new level */
#define ENUM_EVENT_DESCEND      1

/** start of a new page; param1 points to the page */
#define ENUM_EVENT_PAGE_START   2

/** end of a new page; param1 points to the page */
#define ENUM_EVENT_PAGE_STOP    3

/** an item in the page; param1 points to the key; param2 is the index 
 * of the key in the page */
#define ENUM_EVENT_ITEM         4

struct btree_cursor_t;

/**
* @}
*/

/**
 * the backend structure - these functions and members are inherited
 * by every other backend (i.e. btree, hashdb etc). 
 */
class Backend
{
  public:
    Backend(Database *db, ham_u32_t flags)
      : m_db(db), m_keysize(0), m_recno(0), m_is_active(false), m_flags(flags) {
    }

    /**
     * destructor; can be overwritten
     */
    virtual ~Backend() { }

    /**
     * create and initialize a backend
     *
     * @remark this function is called after the ham_db_t structure
     * was allocated and the file was opened
     */
    virtual ham_status_t create(ham_u16_t keysize, ham_u32_t flags) = 0;

    /**
     * open and initialize a backend
     *
     * @remark this function is called after the ham_db_t structure
     * was allocated and the file was opened
     */
    virtual ham_status_t open(ham_u32_t flags) = 0;

    /**
     * close the backend
     *
     * @remark this function is called before the file is closed
     */
    virtual void close() = 0;

    /**
     * flushes the backend's meta information to the index data;
     * this does not flush the whole index!
     */
    virtual ham_status_t flush_indexdata() = 0;

    /**
     * find a key in the index
     */
    virtual ham_status_t find(Transaction *txn, ham_key_t *key, 
                    ham_record_t *record, ham_u32_t flags) = 0;

    /**
     * insert (or update) a key in the index
     *
     * the backend is responsible for inserting or updating the
     * record. (see blob.h for blob management functions)
     */
    virtual ham_status_t insert(Transaction *txn, ham_key_t *key, 
                    ham_record_t *record, ham_u32_t flags) = 0;

    /**
     * erase a key in the index
     */
    virtual ham_status_t erase(Transaction *txn, ham_key_t *key, 
                    ham_u32_t flags) = 0;

    /**
     * iterate the whole tree and enumerate every item
     */
    virtual ham_status_t enumerate(ham_enumerate_cb_t cb, void *context) = 0;

    /**
     * verify the whole tree
     */
    virtual ham_status_t check_integrity() = 0;

    /**
     * estimate the number of keys per page, given the keysize
     */
    virtual ham_status_t calc_keycount_per_page(ham_size_t *keycount, 
                    ham_u16_t keysize) = 0;

    /**
     * Close (and free) all cursors related to this database table.
     */
    virtual ham_status_t close_cursors(ham_u32_t flags) = 0;

    /**
     * uncouple all cursors from a page
     *
     * @remark this is called whenever the page is deleted or
     * becoming invalid
     */
    virtual ham_status_t uncouple_all_cursors(Page *page, ham_size_t start) = 0;

    /**
     * looks up a key, points cursor to this key
     */
    virtual ham_status_t find_cursor(Transaction *txn, btree_cursor_t *cursor, 
           ham_key_t *key, ham_record_t *record, ham_u32_t flags) = 0;

    /**
     * inserts a key, points cursor to the new key
     */
    virtual ham_status_t insert_cursor(Transaction *txn, ham_key_t *key, 
                        ham_record_t *record, btree_cursor_t *cursor,
                        ham_u32_t flags) = 0;

    /**
     * Remove all extended keys for the given @a page from the
     * extended key cache.
     */
    virtual ham_status_t free_page_extkeys(Page *page, ham_u32_t flags) = 0;

    /** get the database pointer */
    Database *get_db() {
        return m_db;
    }

    /** get the key size */
    ham_u16_t get_keysize() {
        return m_keysize;
    }

    /** set the key size */
    void set_keysize(ham_u16_t keysize) {
        m_keysize=keysize;
    }

    /** get the flags */
    ham_u32_t get_flags() {
        return m_flags;
    }

    /** set the flags */
    void set_flags(ham_u32_t flags) {
        m_flags=flags;
    }

    /** get the last used record number */
    ham_u64_t get_recno() {
        return m_recno;
    }

    /** set the last used record number */
    void set_recno(ham_u64_t recno) {
        m_recno=recno;
    }

    /** check whether this backend is active */
    bool is_active() {
        return m_is_active;
    }

    /** set the is_active-flag */
    void set_active(bool b) {
        m_is_active=b;
    }

  private:
    /** pointer to the database object */
    Database *m_db;

    /** the keysize of this backend index */
    ham_u16_t m_keysize;

    /** the last used record number */
    ham_offset_t m_recno;

    /** flag if this backend has been fully initialized */
    bool m_is_active;

    /** the persistent flags of this backend index */
    ham_u32_t m_flags;
};

#endif /* HAM_BACKEND_H__ */
