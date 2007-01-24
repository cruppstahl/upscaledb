/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 * btree searching
 *
 */

#include <string.h>
#include "db.h"
#include "error.h"
#include "btree.h"
#include "keys.h"
#include "btree_cursor.h"

ham_status_t 
btree_find_cursor(ham_btree_t *be, ham_bt_cursor_t *cursor, 
           ham_key_t *key, ham_record_t *record, ham_u32_t flags)
{
    ham_page_t *page;
    btree_node_t *node;
    key_t *entry;
    ham_s32_t idx;
    ham_db_t *db=btree_get_db(be);

    /* get the address of the root page */
    db_set_error(db, 0);
    if (!btree_get_rootpage(be))
        return (db_set_error(db, HAM_KEY_NOT_FOUND));

    /* load the root page */
    page=db_fetch_page(db, btree_get_rootpage(be), flags);
    if (!page)
        return (db_get_error(db));

    /* now traverse the root to the leaf nodes, till we find a leaf */
    while (1) {
        if (!page) {
            if (!db_get_error(db))
                db_set_error(db, HAM_KEY_NOT_FOUND);
            return (db_get_error(db));
        }

        node=ham_page_get_btree_node(page);
        if (btree_node_is_leaf(node))
            break;

        page=btree_traverse_tree(db, page, key, 0);
    }

    /* check the leaf page for the key */
    idx=btree_node_search_by_key(db, page, key);
    if (db_get_error(db))
        return (db_get_error(db));
    if (idx<0) {
        db_set_error(db, HAM_KEY_NOT_FOUND);
        return (db_get_error(db));
    }

    /* load the entry, and store record ID and key flags */
    entry=btree_node_get_key(db, node, idx);
    if (record) {
        record->_rid=key_get_ptr(entry);
        record->_intflags=key_get_flags(entry);
    }

    /* set the cursor-position to this key */
    if (cursor) {
        ham_assert(!(bt_cursor_get_flags(cursor)&BT_CURSOR_FLAG_UNCOUPLED), 
                ("coupling an uncoupled cursor, but need a nil-cursor"));
        ham_assert(!(bt_cursor_get_flags(cursor)&BT_CURSOR_FLAG_COUPLED), 
                ("coupling a coupled cursor, but need a nil-cursor"));
        page_add_cursor(page, (ham_cursor_t *)cursor);
        bt_cursor_set_flags(cursor, 
                bt_cursor_get_flags(cursor)|BT_CURSOR_FLAG_COUPLED);
        bt_cursor_set_coupled_page(cursor, page);
        bt_cursor_set_coupled_index(cursor, idx);
    }

    return (0);
}

ham_status_t 
btree_find(ham_btree_t *be, ham_key_t *key,
           ham_record_t *record, ham_u32_t flags)
{
    return (btree_find_cursor(be, 0, key, record, flags));
}

