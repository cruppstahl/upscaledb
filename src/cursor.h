/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 *
 * a base-"class" for cursors
 *
 */

#ifndef HAM_CURSORS_H__
#define HAM_CURSORS_H__

#include <ham/hamsterdb.h>
#include <ham/hamsterdb_int.h>
#include "txn.h"

/**
 * the cursor structure - these functions and members are "inherited"
 * by every other cursor (i.e. btree, hashdb etc).
 */
#define CURSOR_DECLARATIONS(clss)                                       \
    /**                                                                 \
     * clone an existing cursor                                         \
     */                                                                 \
    ham_status_t (*_fun_clone)(clss *cu, clss **newit);                 \
                                                                        \
    /**                                                                 \
     * close an existing cursor                                         \
     */                                                                 \
    ham_status_t (*_fun_close)(clss *cu);                               \
                                                                        \
    /**                                                                 \
     * replace the record of this cursor                                \
     */                                                                 \
    ham_status_t (*_fun_replace)(clss *cu, ham_record_t *record,        \
            ham_u32_t flags);                                           \
                                                                        \
    /**                                                                 \
     * move the cursor                                                  \
     */                                                                 \
    ham_status_t (*_fun_move)(clss *cu, ham_key_t *key,                 \
            ham_record_t *record, ham_u32_t flags);                     \
                                                                        \
    /**                                                                 \
     * find a key in the index and positions the cursor                 \
     * on this key                                                      \
     */                                                                 \
    ham_status_t (*_fun_find)(clss *cu, ham_key_t *key, ham_u32_t flags);\
                                                                        \
    /**                                                                 \
     * insert (or update) a key in the index                            \
     */                                                                 \
    ham_status_t (*_fun_insert)(clss *cu, ham_key_t *key,               \
            ham_record_t *record, ham_u32_t flags);                     \
                                                                        \
    /**                                                                 \
     * erases the key from the index and positions the cursor to the    \
     * next key                                                         \
     */                                                                 \
    ham_status_t (*_fun_erase)(clss *cu, ham_offset_t *rid,             \
            ham_u32_t *intflags, ham_u32_t flags);                      \
                                                                        \
    /**                                                                 \
     * pointer to the database object                                   \
     */                                                                 \
    ham_db_t *_db;                                                      \
                                                                        \
    /**                                                                 \
     * pointer to the transaction object (not yet used)                 \
     */                                                                 \
    ham_txn_t *_txn;                                                    \
                                                                        \
    /**                                                                 \
     * linked list of cursors which point to the same page              \
     */                                                                 \
    clss *_next, *_previous;


/**
 * a generic cursor structure, which has the same memory layout as
 * all other backends
 */
struct ham_cursor_t
{
    CURSOR_DECLARATIONS(ham_cursor_t)
};

/**
 * get the 'next' pointer of the linked list
 */
#define cursor_get_next(c)              (c)->_next

/**
 * set the 'next' pointer of the linked list
 */
#define cursor_set_next(c, n) { if (n) ham_assert(c->_previous!=n, ("")); (c)->_next=n; }

/**
 * get the 'previous' pointer of the linked list
 */
#define cursor_get_previous(c)          (c)->_previous

/**
 * set the 'previous' pointer of the linked list
 */
#define cursor_set_previous(c, p) {if (p) ham_assert(c->_next!=p, ("")); (c)->_previous=p; }

/**
 * get the database pointer
 */
#define cursor_get_db(c)                (c)->_db


#endif /* HAM_CURSORS_H__ */
