/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file COPYING for licence information
 *
 * btree dump
 *
 */

#include <string.h>
#include <stdio.h>
#include <ham/config.h>
#include "db.h"
#include "error.h"
#include "btree.h"
#include "txn.h"

/*
 * dump a whole level in the tree - start with "page" and traverse
 * the linked list of all the siblings
 */
static ham_status_t 
my_dump_level(ham_txn_t *txn, ham_page_t *page, ham_u32_t level, 
        ham_dump_cb_t cb);

/*
 * dump a single page
 * TODO wird static, wenn der b-baum laeuft
 */
ham_status_t
my_dump_page(ham_page_t *page, ham_u32_t level, ham_u32_t count, 
        ham_dump_cb_t cb);

ham_status_t 
btree_dump(ham_btree_t *be, ham_txn_t *txn, ham_dump_cb_t cb)
{
    ham_page_t *page;
    ham_u32_t level=0;
    ham_offset_t ptr_left;
    btree_node_t *node;
    ham_db_t *db=btree_get_db(be);
    ham_assert(btree_get_rootpage(be)!=0, 0, 0);
    ham_assert(cb!=0, 0, 0);

    /* get the root page of the tree */
    page=txn_fetch_page(txn, btree_get_rootpage(be), 0);
    if (!page) {
        ham_trace("error 0x%x while fetching root page", db_get_error(db));
        return (db_get_error(db));
    }

    /* while we found a page... */
    while (page) {
        node=ham_page_get_btree_node(page);
        ptr_left=btree_node_get_ptr_left(node);

        /*
         * dump the page and all its siblings
         */
        (void)my_dump_level(txn, page, level, cb);

        /*
         * follow the pointer to the smallest child
         */
        if (ptr_left)
            page=txn_fetch_page(txn, ptr_left, 0);
        else
            page=0;

        ++level;
    }

    return (0);
}

static ham_status_t 
my_dump_level(ham_txn_t *txn, ham_page_t *page, ham_u32_t level, 
        ham_dump_cb_t cb)
{
    ham_page_t *sib;
    ham_u32_t count=0;
    btree_node_t *node;

    while (page) {
        /*
         * dump the page
         */
        (void)my_dump_page(page, level, count, cb);

        /* 
         * get the right sibling
         */
        node=ham_page_get_btree_node(page);
        if (btree_node_get_right(node))
            sib=txn_fetch_page(txn, btree_node_get_right(node), 0);
        else
            sib=0;

        page=sib;

        ++count;
    }

    return (0);
}

ham_status_t
my_dump_page(ham_page_t *page, ham_u32_t level, ham_u32_t sibcount, 
        ham_dump_cb_t cb)
{
    ham_size_t i, count;
    ham_db_t *db=page_get_owner(page);
    btree_entry_t *bte;
    btree_node_t *node=ham_page_get_btree_node(page);

    count=btree_node_get_count(node);
    printf("\n------ page 0x%lx at level #%d, sibling #%d--------------\n", 
            page_get_self(page), level, sibcount);
    printf("left: 0x%lx, right: 0x%lx, ptr_left: 0x%lx\n", 
            btree_node_get_left(node), btree_node_get_right(node), 
            btree_node_get_ptr_left(node));
    printf("found %d items:\n", count);
    
    for (i=0; i<count; i++) {
        bte=btree_node_get_entry(db, node, i);
        printf(" %02d: ", i);
        printf(" key (%2d byte): ", btree_entry_get_size(bte));
        cb(btree_entry_get_key(bte), btree_entry_get_size(bte));
        printf("      ptr: 0x%lx\n", btree_entry_get_ptr(bte));
    }

    return (0);
}

