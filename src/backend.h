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

namespace ham {

/**
* @defgroup ham_cb_status hamsterdb Backend Node/Page Enumerator Status Codes
* @{
*/
enum {
  /** continue with the traversal */
  HAM_ENUM_CONTINUE                 = 0,

  /** do not not descend another level (or from page to key traversal) */
  HAM_ENUM_DO_NOT_DESCEND           = 1,

  /** stop the traversal entirely */
  HAM_ENUM_STOP                     = 2
};

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
 *     error code when an error occurred.
 */
typedef ham_status_t (*ham_enumerate_cb_t)(int event, void *param1,
          void *param2, void *context);

/**
* @defgroup ham_cb_event hamsterdb Backend Node/Page Enumerator State Codes
* @{
*/

enum {
  /** descend one level; param1 is an integer value with the new level */
  HAM_ENUM_EVENT_DESCEND            = 1,

  /** start of a new page; param1 points to the page */
  HAM_ENUM_EVENT_PAGE_START         = 2,

  /** end of a new page; param1 points to the page */
  HAM_ENUM_EVENT_PAGE_STOP          = 3,

  /** an item in the page; param1 points to the key; param2 is the index
   * of the key in the page */
  HAM_ENUM_EVENT_ITEM               = 4
};

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
      : m_db(db), m_keysize(0), m_is_active(false), m_flags(flags) {
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
    ham_status_t create(ham_u16_t keysize, ham_u32_t flags) {
      return (do_create(keysize, flags));
    }

    /**
     * open and initialize a backend
     *
     * @remark this function is called after the ham_db_t structure
     * was allocated and the file was opened
     */
    ham_status_t open(ham_u32_t flags) {
      return (do_open(flags));
    }

    /**
     * close the backend
     *
     * @remark this function is called before the file is closed
     */
    void close(ham_u32_t flags = 0) {
      return (do_close(flags));
    }

    /**
     * flushes the backend's meta information to the index data;
     * this does not flush the whole index!
     */
    ham_status_t flush_indexdata() {
      return (do_flush_indexdata());
    }

    /**
     * find a key in the index
     */
    ham_status_t find(Transaction *txn, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags) {
      return (do_find(txn, 0, key, record, flags));
    }

    /**
     * insert (or update) a key in the index
     *
     * the backend is responsible for inserting or updating the
     * record. (see blob.h for blob management functions)
     */
    ham_status_t insert(Transaction *txn, ham_key_t *key,
                    ham_record_t *record, ham_u32_t flags) {
      return (do_insert_cursor(txn, key, record, 0, flags));
    }

    /**
     * erase a key in the index
     */
    ham_status_t erase(Transaction *txn, ham_key_t *key,
                    ham_u32_t flags) {
      return (do_erase(txn, key, flags));
    }

    /**
     * iterate the whole tree and enumerate every item
     */
    ham_status_t enumerate(ham_enumerate_cb_t cb, void *context) {
      return (do_enumerate(cb, context));
    }

    /**
     * verify the whole tree
     */
    ham_status_t check_integrity() {
      return (do_check_integrity());
    }

    /**
     * estimate the number of keys per page, given the keysize
     */
    ham_status_t calc_keycount_per_page(ham_size_t *keycount,
                    ham_u16_t keysize) {
      return (do_calc_keycount_per_page(keycount, keysize));
    }

    /**
     * uncouple all cursors from a page
     *
     * @remark this is called whenever the page is deleted or
     * becoming invalid
     */
    ham_status_t uncouple_all_cursors(Page *page, ham_size_t start) {
      return (do_uncouple_all_cursors(page, start));
    }

    /**
     * looks up a key, points cursor to this key
     */
    ham_status_t find_cursor(Transaction *txn, Cursor *cursor,
                    ham_key_t *key, ham_record_t *record, ham_u32_t flags) {
      return (do_find(txn, cursor, key, record, flags));
    }

    /**
     * inserts a key, points cursor to the new key
     */
    ham_status_t insert_cursor(Transaction *txn, ham_key_t *key,
                    ham_record_t *record, Cursor *cursor, ham_u32_t flags) {
      return (do_insert_cursor(txn, key, record, cursor, flags));
    }

    /**
     * erases the key that the cursor points to
     */
    ham_status_t erase_cursor(Transaction *txn, ham_key_t *key,
                    Cursor *cursor, ham_u32_t flags) {
      return (do_erase_cursor(txn, key, cursor, flags));
    }

    /** implementation for flush_indexdata() */
    // TODO make this protected
    virtual ham_status_t do_flush_indexdata() = 0;

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
      m_flags = flags;
    }

    /** check whether this backend is active */
    bool is_active() {
      return m_is_active;
    }

    /** set the is_active-flag */
    void set_active(bool b) {
      m_is_active = b;
    }

    /** implementation for find() */
    // TODO make this protected
    virtual ham_status_t do_find(Transaction *txn, Cursor *cursor,
                    ham_key_t *key, ham_record_t *record, ham_u32_t flags) = 0;

    /**
     * read a key
     *
     * @a dest must have been initialized before calling this function; the
     * dest->data space will be reused when the specified size is large enough;
     * otherwise the old dest->data will be ham_mem_free()d and a new
     * space allocated.
     *
     * This can save superfluous heap free+allocation actions in there.
     *
     * @note
     * This routine can cope with HAM_KEY_USER_ALLOC-ated 'dest'-inations.
         // TODO make this private
         // TODO use arena; get rid of txn parameter
     */
    virtual ham_status_t read_key(Transaction *txn, BtreeKey *source,
                    ham_key_t *dest) = 0;

    /**
     * read a record
     *
     * @param rid same as record->_rid, if key is not TINY/SMALL. Otherwise,
     * and if HAM_DIRECT_ACCESS is set, we use the rid-pointer to the
     * original record ID
     *
     * flags: either 0 or HAM_DIRECT_ACCESS
         // TODO make this private
         // TODO use arena; get rid of txn parameter
     */
    virtual ham_status_t read_record(Transaction *txn, ham_record_t *record,
                    ham_u64_t *ridptr, ham_u32_t flags) = 0;

  protected:
    /** implementation for create() */
    virtual ham_status_t do_create(ham_u16_t keysize, ham_u32_t flags) = 0;

    /** implementation for open() */
    virtual ham_status_t do_open(ham_u32_t flags) = 0;

    /** implementation for close() */
    virtual void do_close(ham_u32_t flags) = 0;

    /** implementation for erase() */
    virtual ham_status_t do_erase(Transaction *txn, ham_key_t *key,
                            ham_u32_t flags) = 0;

    /** implementation for enumerate() */
    virtual ham_status_t do_enumerate(ham_enumerate_cb_t cb, void *context) = 0;

    /** implementation for check_integrity() */
    virtual ham_status_t do_check_integrity() = 0;

    /** implementation for calc_keycount_per_page */
    // TODO this is btree-private??
    virtual ham_status_t do_calc_keycount_per_page(ham_size_t *keycount,
                            ham_u16_t keysize) = 0;

    /** implementation for uncouple_all_cursors() */
    virtual ham_status_t do_uncouple_all_cursors(Page *page,
                            ham_size_t start) = 0;

    /** implementation for insert_cursor() */
    virtual ham_status_t do_insert_cursor(Transaction *txn, ham_key_t *key,
                            ham_record_t *record, Cursor *cursor,
                            ham_u32_t flags) = 0;

    /** implementation for erase_cursor() */
    virtual ham_status_t do_erase_cursor(Transaction *txn, ham_key_t *key,
                            Cursor *cursor, ham_u32_t flags) = 0;

    /** pointer to the database object */
    Database *m_db;

  private:
    /** the keysize of this backend index */
    ham_u16_t m_keysize;

    /** flag if this backend has been fully initialized */
    bool m_is_active;

    /** the persistent flags of this backend index */
    ham_u32_t m_flags;
};

} // namespace ham

#endif /* HAM_BACKEND_H__ */

