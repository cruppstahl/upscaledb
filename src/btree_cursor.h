/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for licence information
 *
 * btree cursors
 *
 */

#ifndef HAM_BT_CURSORS_H__
#define HAM_BT_CURSORS_H__

#include "page.h"
#include "txn.h"
#include "cursor.h"
#include "keys.h"

/**
 * the cursor structure for a b+tree
 */
struct ham_bt_cursor_t;
typedef struct ham_bt_cursor_t ham_bt_cursor_t;
struct ham_bt_cursor_t
{
    /**
     * the common declaratons of all cursors
     */
    CURSOR_DECLARATIONS(ham_bt_cursor_t)

    /**
     * internal cursor flags
     */
    ham_u32_t _flags;

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
#define bt_cursor_set_db(cu, db)            (cu)->_db=db

/**
 * get the txn pointer
 */
#define bt_cursor_get_txn(cu)               (cu)->_txn

/**
 * set the txn pointer
 */
#define bt_cursor_set_txn(cu, txn)          (cu)->_txn=txn

/**
 * get the flags
 */
#define bt_cursor_get_flags(cu)             (cu)->_flags

/**
 * set the flags
 */
#define bt_cursor_set_flags(cu, f)          (cu)->_flags=f

/**
 * is the cursor pointing to "NULL"? this is the case when the
 * cursor is neither coupled nor uncoupled
 */
#define bt_cursor_is_null(cu)                              \
                ~(((cu)->_flags&BT_CURSOR_FLAG_COUPLED) || \
                  ((cu)->_flags&BT_CURSOR_FLAG_UNCOUPLED))

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
 * get the key we're pointing to - if the cursor is uncoupled
 */
#define bt_cursor_get_uncoupled_key(cu)     (cu)->_u._uncoupled._key

/**
 * set the key we're pointing to - if the cursor is uncoupled
 */
#define bt_cursor_set_uncoupled_key(cu, k)  (cu)->_u._uncoupled._key=k

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
bt_cursor_uncouple(ham_bt_cursor_t *c, ham_txn_t *txn, ham_u32_t flags);

/**
 * flag for bt_cursor_uncouple: uncouple from the page, but do not
 * call page_remove_cursor()
 */
#define BT_CURSOR_UNCOUPLE_NO_REMOVE        1

/**
 * create a new cursor
 */
ham_status_t
bt_cursor_create(ham_db_t *db, ham_txn_t *txn, ham_u32_t flags,
            ham_bt_cursor_t **cu);

/**
 * clone an existing cursor
 */
ham_status_t
bt_cursor_clone(ham_bt_cursor_t *cu, ham_bt_cursor_t **newit);

/**
 * close an existing cursor
 */
ham_status_t
bt_cursor_close(ham_bt_cursor_t *cu);

/**
 * set the cursor to the first item in the database
 */
ham_status_t
bt_cursor_move(ham_bt_cursor_t *c, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags);

/**
 * replace the record of this cursor
 */
ham_status_t
bt_cursor_replace(ham_bt_cursor_t *cu, ham_record_t *record,
            ham_u32_t flags);

/**
 * find a key in the index and positions the cursor
 * on this key
 */
ham_status_t
bt_cursor_find(ham_bt_cursor_t *cu, ham_key_t *key, ham_u32_t flags);

/**
 * insert (or update) a key in the index
 */
ham_status_t
bt_cursor_insert(ham_bt_cursor_t *cu, ham_key_t *key,
            ham_record_t *record, ham_u32_t flags);

/**
 * erases the key from the index and positions the cursor to the
 * next key
 */
ham_status_t
bt_cursor_erase(ham_bt_cursor_t *cu, ham_offset_t *rid,
            ham_u32_t *intflags, ham_u32_t flags);


#endif /* HAM_BT_CURSORS_H__ */
