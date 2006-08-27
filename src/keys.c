/**
 * Copyright 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 *
 */

#include <ham/hamsterdb.h>
#include "db.h"
#include "keys.h"
#include "btree.h"

int
key_compare_int_to_pub(ham_page_t *page, ham_u16_t lhs, ham_key_t *rhs)
{
    key_t *l;
    btree_node_t *node=ham_page_get_btree_node(page);

    l=btree_node_get_key(page_get_owner(page), node, lhs);

    return (db_compare_keys(page_get_owner(page), page, 
                lhs, key_get_flags(l), key_get_key(l), 
                db_get_keysize(page_get_owner(page)), key_get_size(l), 
                0, 0, rhs->data, db_get_keysize(page_get_owner(page)), 
                rhs->size));
}

int
key_compare_pub_to_int(ham_page_t *page, ham_key_t *lhs, ham_u16_t rhs)
{
    key_t *r;
    btree_node_t *node=ham_page_get_btree_node(page);

    r=btree_node_get_key(page_get_owner(page), node, rhs);

    return (db_compare_keys(page_get_owner(page), page, 
                0, 0, lhs->data, db_get_keysize(page_get_owner(page)), 
                lhs->size, rhs, key_get_flags(r), key_get_key(r), 
                db_get_keysize(page_get_owner(page)), key_get_size(r)));
}

int
key_compare_int_to_int(ham_page_t *page, ham_u16_t lhs, ham_u16_t rhs)
{
    key_t *l, *r;
    btree_node_t *node=ham_page_get_btree_node(page);

    l=btree_node_get_key(page_get_owner(page), node, lhs);
    r=btree_node_get_key(page_get_owner(page), node, rhs);

    return (db_compare_keys(page_get_owner(page), page, 
                lhs, key_get_flags(l), key_get_key(l), 
                db_get_keysize(page_get_owner(page)), key_get_size(l), 
                rhs, key_get_flags(r), key_get_key(r), 
                db_get_keysize(page_get_owner(page)), key_get_size(r)));
}

