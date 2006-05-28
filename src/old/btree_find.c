/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file COPYING for licence information
 *
 * btree searching
 *
 */

#include <string.h>
#include <ham/config.h>
#include "db.h"
#include "error.h"
#include "btree.h"

ham_status_t 
btree_find(ham_btree_t *be, ham_txn_t *txn, ham_key_t *key,
           ham_offset_t *rid, ham_u32_t flags)
{
    ham_size_t idx;
    btree_entry_t *entry;
    btree_node_t *node;
    ham_page_t *page, *oldpage;
    ham_db_t *db=btree_get_db(be);
    ham_offset_t rootaddr;

    db_set_error(db, 0);
    rootaddr=btree_get_rootpage(be);

    if (!rootaddr)
        return (db_set_error(db, HAM_KEY_NOT_FOUND));

    /* get the root page of the tree */
    page=txn_fetch_page(txn, rootaddr, flags);
    ham_assert(page!=0, "error 0x%x while fetching root page", 
            db_get_error(db));

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

        oldpage=page;
        page=btree_find_child(db, txn, page, key);
    }

    /* 
     * if the key is in the database, then it must be in this leaf.
     * btree_node_search_by_key() returns 0 on error, otherwise a 
     * 1-based index.
     */
    idx=btree_node_search_by_key(db, page, key);
    if (db_get_error(db))
        return (db_get_error(db));
    if (!idx) {
        db_set_error(db, HAM_KEY_NOT_FOUND);
        return (db_get_error(db));
    }

    idx--;
    entry=btree_node_get_entry(db, node, idx);
    *rid=btree_entry_get_ptr(entry);
    return (0);
}

long
btree_get_slot(ham_db_t *db, ham_page_t *page, ham_key_t *key)
{
    int cmp;
    ham_size_t i, keysize;
    btree_entry_t *lhs, *rhs;
    btree_node_t *node=ham_page_get_btree_node(page);
    keysize=db_get_keysize(db); 

    /*
     * if the value we are searching for is < the smallest value in this 
     * node: get the "down left"-child node
     */
    lhs=btree_node_get_entry(db, node, 0);
    cmp=db_compare_keys(db, page, 
            -1, key->_flags, key->data, key->size, key->size,
            0, btree_entry_get_flags(lhs), btree_entry_get_key(lhs), 
            btree_entry_get_real_size(db, lhs), btree_entry_get_size(lhs));
    if (db_get_error(db))
        return (db_get_error(db));
    if (cmp<0)
        return (cmp);

    /* 
     * otherwise search all but the last element
     */
    for (i=0; i<btree_node_get_count(node)-1; i++) {
        lhs=btree_node_get_entry(db, node, i);
        rhs=btree_node_get_entry(db, node, i+1);

        cmp=db_compare_keys(db, page,
                -1, key->_flags, key->data, key->size, key->size,
                i, btree_entry_get_flags(lhs), btree_entry_get_key(lhs), 
                btree_entry_get_real_size(db, lhs), btree_entry_get_size(lhs));
        if (db_get_error(db))
            return (db_get_error(db));
        if (cmp<0)
            continue;

        cmp=db_compare_keys(db, page,
                -1, key->_flags, key->data, key->size, key->size,
                i+1, btree_entry_get_flags(rhs), btree_entry_get_key(rhs), 
                btree_entry_get_real_size(db, rhs), btree_entry_get_size(rhs));
        if (db_get_error(db))
            return (db_get_error(db));
        if (cmp<0)
            return (i);
    }

    return (btree_node_get_count(node)-1);
}

ham_page_t *
btree_find_child(ham_db_t *db, ham_txn_t *txn, ham_page_t *page, ham_key_t *key)
{
    return (btree_find_child2(db, txn, page, key, 0));
}

ham_page_t *
btree_find_child2(ham_db_t *db, ham_txn_t *txn, ham_page_t *page, 
        ham_key_t *key, long *index)
{
    long slot;
    btree_entry_t *bte;
    btree_node_t *node=ham_page_get_btree_node(page);

    ham_assert(btree_node_get_count(node)>0, 0, 0);
    ham_assert(btree_node_get_ptr_left(node)>0, 0, 0);

    slot=btree_get_slot(db, page, key);

    if (index)
        *index=slot;

    if (slot==-1)
        return (txn_fetch_page(txn, btree_node_get_ptr_left(node), 0));
    else {
        bte=btree_node_get_entry(db, node, slot);
        return (txn_fetch_page(txn, btree_entry_get_ptr(bte), 0));
    }
}
