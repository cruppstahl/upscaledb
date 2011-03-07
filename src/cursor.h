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
 * @brief a base-"class" for cursors
 *
 */

#ifndef HAM_CURSORS_H__
#define HAM_CURSORS_H__

#include "internal_fwd_decl.h"

#include "error.h"
#include "txn_cursor.h"


#ifdef __cplusplus
extern "C" {
#endif 

/**
 * The Cursor structure - these functions and members are "inherited"
 * by every other cursor (i.e. btree, hashdb etc).
 */
#define CURSOR_DECLARATIONS(clss)                                       \
    /**                                                                 \
     * Clone an existing cursor                                         \
     */                                                                 \
    ham_status_t (*_fun_clone)(clss *cu, clss **newit);                 \
                                                                        \
    /**                                                                 \
     * Close an existing cursor                                         \
     */                                                                 \
    void (*_fun_close)(clss *cu);                                       \
                                                                        \
    /**                                                                 \
     * Overwrite the record of this cursor                              \
     */                                                                 \
    ham_status_t (*_fun_overwrite)(clss *cu, ham_record_t *record,      \
            ham_u32_t flags);                                           \
                                                                        \
    /**                                                                 \
     * Move the cursor                                                  \
     */                                                                 \
    ham_status_t (*_fun_move)(clss *cu, ham_key_t *key,                 \
            ham_record_t *record, ham_u32_t flags);                     \
                                                                        \
    /**                                                                 \
     * Find a key in the index and positions the cursor                 \
     * on this key                                                      \
     */                                                                 \
    ham_status_t (*_fun_find)(clss *cu, ham_key_t *key,                 \
                    ham_record_t *record, ham_u32_t flags);             \
                                                                        \
    /**                                                                 \
     * Insert (or update) a key in the index                            \
     */                                                                 \
    ham_status_t (*_fun_insert)(clss *cu, ham_key_t *key,               \
                    ham_record_t *record, ham_u32_t flags);             \
                                                                        \
    /**                                                                 \
     * Erases the key from the index and positions the cursor to the    \
     * next key                                                         \
     */                                                                 \
    ham_status_t (*_fun_erase)(clss *cu, ham_u32_t flags);              \
                                                                        \
    /**                                                                 \
     * Count the number of records stored with the referenced key.      \
     */                                                                 \
    ham_status_t (*_fun_get_duplicate_count)(ham_cursor_t *cursor,      \
            ham_size_t *count, ham_u32_t flags);                        \
                                                                        \
    /**                                                                 \
     * Returns true if cursor is nil, otherwise false                   \
     */                                                                 \
    ham_bool_t (*_fun_is_nil)(ham_cursor_t *cursor);                    \
                                                                        \
    /** Pointer to the Database object */                               \
    ham_db_t *_db;                                                      \
                                                                        \
    /** Pointer to the Transaction */                                   \
    ham_txn_t *_txn;                                                    \
                                                                        \
    /** A Cursor which can walk over Transaction trees */               \
    txn_cursor_t _txn_cursor;                                           \
                                                                        \
    /** The remote database handle */                                   \
    ham_u64_t _remote_handle;                                           \
                                                                        \
    /** Linked list of all Cursors in this Database */                  \
    clss *_next, *_previous;                                            \
                                                                        \
    /** Linked list of Cursors which point to the same page */          \
    clss *_next_in_page, *_previous_in_page;                            \
                                                                        \
    /** Stores the last operation (insert/find or move); needed for     \
     * ham_cursor_move. Values can be HAM_CURSOR_NEXT,                  \
     * HAM_CURSOR_PREVIOUS or CURSOR_LOOKUP_INSERT */                   \
    ham_u32_t _lastop;                                                  \
                                                                        \
    /** Cursor flags */                                                 \
    ham_u32_t _flags


/**
 * a generic Cursor structure, which has the same memory layout as
 * all other backends
 */
struct ham_cursor_t
{
    CURSOR_DECLARATIONS(ham_cursor_t);
};

/** Get the Cursor flags */
#define cursor_get_flags(c)               (c)->_flags

/** Set the Cursor flags */
#define cursor_set_flags(c, f)            (c)->_flags=(f)

/** Cursor flag: cursor is coupled to the Transaction cursor (_txn_cursor) */
#define CURSOR_COUPLED_TO_TXN               0x100000

/** Get the 'next' pointer of the linked list */
#define cursor_get_next(c)                (c)->_next

/** Set the 'next' pointer of the linked list */
#define cursor_set_next(c, n)             (c)->_next=(n)

/** Get the 'previous' pointer of the linked list */
#define cursor_get_previous(c)            (c)->_previous

/** Set the 'previous' pointer of the linked list */
#define cursor_set_previous(c, p)         (c)->_previous=(p)

/** Get the 'next' pointer of the linked list */
#define cursor_get_next_in_page(c)       (c)->_next_in_page

/** Set the 'next' pointer of the linked list */
#define cursor_set_next_in_page(c, n)                                        \
    {                                                                        \
        if (n)                                                                \
            ham_assert((c)->_previous_in_page!=(n), (0));                    \
        (c)->_next_in_page=(n);                                                \
    }

/** Get the 'previous' pointer of the linked list */
#define cursor_get_previous_in_page(c)   (c)->_previous_in_page

/** Set the 'previous' pointer of the linked list */
#define cursor_set_previous_in_page(c, p)                                    \
    {                                                                        \
        if (p)                                                                \
            ham_assert((c)->_next_in_page!=(p), (0));                        \
        (c)->_previous_in_page=(p);                                            \
    }

/** Set the Database pointer */
#define cursor_set_db(c, db)            (c)->_db=db

/** Get the Database pointer */
#define cursor_get_db(c)                (c)->_db

/** Get the Transaction handle */
#define cursor_get_txn(c)               (c)->_txn

/** Set the Transaction handle */
#define cursor_set_txn(c, txn)          (c)->_txn=(txn)

/** Get a pointer to the Transaction cursor */
#define cursor_get_txn_cursor(c)        (&(c)->_txn_cursor)

/** Get the remote Database handle */
#define cursor_get_remote_handle(c)     (c)->_remote_handle

/** Set the remote Database handle */
#define cursor_set_remote_handle(c, h)  (c)->_remote_handle=(h)

/** Get the previous operation */
#define cursor_get_lastop(c)            (c)->_lastop

/** Store the current operation; needed for ham_cursor_move */
#define cursor_set_lastop(c, o)         (c)->_lastop=(o)

/** flag for cursor_set_lastop */
#define CURSOR_LOOKUP_INSERT            0x10000


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_CURSORS_H__ */
