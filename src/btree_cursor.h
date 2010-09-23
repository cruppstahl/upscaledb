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
 * @brief btree cursors
 *
 */

#ifndef HAM_BT_CURSORS_H__
#define HAM_BT_CURSORS_H__

#include "internal_fwd_decl.h"

#include "blob.h"
#include "cursor.h"


#ifdef __cplusplus
extern "C" {
#endif 

/**
 * the cursor structure for a b+tree
 */
struct ham_bt_cursor_t;
typedef struct ham_bt_cursor_t ham_bt_cursor_t;
struct ham_bt_cursor_t
{
    /**
     * the common declarations of all cursors
     */
    CURSOR_DECLARATIONS(ham_bt_cursor_t);

    /**
     * internal cursor flags
     */
    ham_u32_t _flags;

    /*
     * the id of the duplicate key
     */
    ham_size_t _dupe_id;

    /*
     * cached flags and record ID of the current duplicate
     */
    dupe_entry_t _dupe_cache;

    /**
     * "coupled" or "uncoupled" states; coupled means that the
     * cursor points into a ham_page_t object, which is in
     * memory. "uncoupled" means that the cursor has a copy
     * of the key on which it points
     */
    union ham_bt_cursor_union_t {
        struct ham_bt_cursor_coupled_t {
            /*
             * the page we're pointing to
             */
            ham_page_t *_page;

            /*
             * the offset of the key in the page
             */
            ham_size_t _index;

        } _coupled;

        struct ham_bt_cursor_uncoupled_t {
            /*
             * a copy of the key at which we're pointing
             */
            ham_key_t *_key;

        } _uncoupled;

    } _u;

};

/**
 * cursor flag: the cursor is coupled
 */
#define BT_CURSOR_FLAG_COUPLED              1

/**
 * cursor flag: the cursor is uncoupled
 */
#define BT_CURSOR_FLAG_UNCOUPLED            2

/**
 * get the database pointer
 */
#define bt_cursor_get_db(cu)                (cu)->_db

/**
 * set the database pointer
 */
#define bt_cursor_set_db(cu, db)            (cu)->_db=(db)

/**
 * get the txn pointer
 */
#define bt_cursor_get_txn(cu)               (cu)->_txn

/**
 * set the txn pointer
 */
#define bt_cursor_set_txn(cu, txn)          (cu)->_txn=(txn)

/**
* get the allocator
*/
#define bt_cursor_get_allocator(cu)         (cu)->_allocator

/**
* set the allocator
*/
#define bt_cursor_set_allocator(cu, a)      (cu)->_allocator=(a)

/**
 * get the flags
 */
#define bt_cursor_get_flags(cu)             (cu)->_flags

/**
 * set the flags
 */
#define bt_cursor_set_flags(cu, f)          (cu)->_flags=(f)

/**
 * Is the cursor pointing to "NULL"? This is the case when the
 * cursor is neither coupled nor uncoupled.
 */
#define bt_cursor_is_nil(cu)                               \
                (!((cu)->_flags&BT_CURSOR_FLAG_COUPLED) && \
                 !((cu)->_flags&BT_CURSOR_FLAG_UNCOUPLED))

/**
 * get the page we're pointing to - if the cursor is coupled
 */
#define bt_cursor_get_coupled_page(cu)      (cu)->_u._coupled._page

/**
 * set the page we're pointing to - if the cursor is coupled
 */
#define bt_cursor_set_coupled_page(cu, p)   (cu)->_u._coupled._page=p

/**
 * get the key index we're pointing to - if the cursor is coupled
 */
#define bt_cursor_get_coupled_index(cu)     (cu)->_u._coupled._index

/**
 * set the key index we're pointing to - if the cursor is coupled
 */
#define bt_cursor_set_coupled_index(cu, i)  (cu)->_u._coupled._index=i

/**
 * get the duplicate key we're pointing to - if the cursor is coupled
 */
#define bt_cursor_get_dupe_id(cu)           (cu)->_dupe_id

/**
 * set the duplicate key we're pointing to - if the cursor is coupled
 */
#define bt_cursor_set_dupe_id(cu, d)        (cu)->_dupe_id=d

/**
 * get the duplicate key's cache
 */
#define bt_cursor_get_dupe_cache(cu)        (&(cu)->_dupe_cache)

/**
 * get the key we're pointing to - if the cursor is uncoupled
 */
#define bt_cursor_get_uncoupled_key(cu)     (cu)->_u._uncoupled._key

/**
 * set the key we're pointing to - if the cursor is uncoupled
 */
#define bt_cursor_set_uncoupled_key(cu, k)  (cu)->_u._uncoupled._key=k

/*
 * set a cursor to NIL
 */
ham_status_t
bt_cursor_set_to_nil(ham_bt_cursor_t *c);

/**
 * couple the cursor
 *
 * @remark to couple a page, it has to be uncoupled!
 */
ham_status_t
bt_cursor_couple(ham_bt_cursor_t *cu);

/**
 * uncouple the cursor
 *
 * @remark to uncouple a page, it has to be coupled!
 */
ham_status_t
bt_cursor_uncouple(ham_bt_cursor_t *c, ham_u32_t flags);

/**
 * flag for @ref bt_cursor_uncouple: uncouple from the page, but do not
 * call @ref page_remove_cursor()
 */
#define BT_CURSOR_UNCOUPLE_NO_REMOVE        1

/**
 * create a new cursor
 */
ham_status_t
bt_cursor_create(ham_db_t *db, ham_txn_t *txn, ham_u32_t flags,
            ham_bt_cursor_t **cu);

/**
 * returns true if a cursor points to this key, otherwise false
 */
ham_bool_t 
bt_cursor_points_to(ham_bt_cursor_t *cursor, btree_key_t *key);

/**
 * uncouple all cursors from a page
 *
 * @remark this is called whenever the page is deleted or becoming invalid
 */
ham_status_t
bt_uncouple_all_cursors(ham_page_t *page, ham_size_t start);

#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* HAM_BT_CURSORS_H__ */
