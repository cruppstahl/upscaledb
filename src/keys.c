/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 */

#include <ham/hamsterdb.h>
#include "db.h"
#include "keys.h"
#include "btree.h"
#include "error.h"
#include "blob.h"

int
key_compare_int_to_pub(ham_page_t *page, ham_u16_t lhs, ham_key_t *rhs)
{
    key_t *l;
    btree_node_t *node=ham_page_get_btree_node(page);

    l=btree_node_get_key(page_get_owner(page), node, lhs);

    return (db_compare_keys(page_get_owner(page), page, 
                lhs, key_get_flags(l), key_get_key(l), 
                key_get_size(l), 0, rhs->_flags, rhs->data, rhs->size));
}

int
key_compare_pub_to_int(ham_page_t *page, ham_key_t *lhs, ham_u16_t rhs)
{
    key_t *r;
    btree_node_t *node=ham_page_get_btree_node(page);

    r=btree_node_get_key(page_get_owner(page), node, rhs);

    return (db_compare_keys(page_get_owner(page), page, 
                0, lhs->_flags, lhs->data, lhs->size, 
                rhs, key_get_flags(r), key_get_key(r), key_get_size(r)));
}

int
key_compare_int_to_int(ham_page_t *page, 
        ham_u16_t lhs, ham_u16_t rhs)
{
    key_t *l, *r;
    btree_node_t *node=ham_page_get_btree_node(page);

    l=btree_node_get_key(page_get_owner(page), node, lhs);
    r=btree_node_get_key(page_get_owner(page), node, rhs);

    return (db_compare_keys(page_get_owner(page), page, 
                lhs, key_get_flags(l), key_get_key(l), 
                key_get_size(l), rhs, key_get_flags(r), key_get_key(r), 
                key_get_size(r)));
}

ham_offset_t
key_insert_extended(ham_db_t *db, ham_page_t *page, 
        ham_key_t *key)
{
    ham_offset_t blobid;
    ham_u8_t *data_ptr=(ham_u8_t *)key->data;
    ham_status_t st;

    ham_assert(key->size>db_get_keysize(db), ("invalid keysize"));
    
    if ((st=blob_allocate(db, 
                data_ptr +(db_get_keysize(db)-sizeof(ham_offset_t)), 
                key->size-(db_get_keysize(db)-sizeof(ham_offset_t)), 
                0, &blobid))) {
        db_set_error(db, st);
        return (0);
    }

    return (blobid);
}

