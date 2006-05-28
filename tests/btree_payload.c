/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 * page-io unit tests
 *
 */

#include <CUnit/Basic.h>
#include <stdio.h>
#include <unistd.h>
#include <ham/hamsterdb.h>
#include "../src/page.h"
#include "../src/db.h"
#include "../src/btree.h"

static const char *filename="/tmp/hamster-test.db";

void 
test_btree_payload(void)
{
    ham_status_t st;
    ham_txn_t txn;
    unsigned i, max, keysize;
    ham_db_t *db;
    ham_page_t *page;
    char buffer[128];
    btree_node_t *node;

    /* create the database handle */
    st=ham_new(&db);
    CU_ASSERT(st==0);

    /* delete the file */
    (void)unlink(filename);

    /* create a database */
    st=ham_create(db, filename, 0, 0664);
    CU_ASSERT(st==0);

    ham_txn_begin(&txn, db, 0);

    /* create a page */
    page=page_new(db);
    CU_ASSERT(page!=0);
    /* get HD memory */
    st=page_io_alloc(page, &txn, 0);
    CU_ASSERT(st==0);

    /* get the btree payload */
    node=ham_page_get_btree_node(page);
    /* get the key length */
    keysize=ham_get_keysize(db);
    printf("keysize is %d\n", keysize);

    /* get the maximum number of entries */
    max=db_get_maxkeys(db);
    printf("maxkeys is %d\n", max);

    /* fill all keys */
    for (i=0; i<max; i++) {
        btree_entry_t *bte=btree_node_get_entry(db, node, i);
        CU_ASSERT(bte!=0);
        memset(btree_entry_get_key(bte), i, keysize);
        btree_entry_set_ptr(bte, i);
        btree_entry_set_size(bte, keysize);
        btree_node_set_count(node, btree_node_get_count(node)+1);
    }

    /* check all keys with btree_node_get_entry() */
    for (i=0; i<max; i++) {
        btree_entry_t *bte=btree_node_get_entry(db, node, i);
        CU_ASSERT(bte!=0);
        memset(buffer, i, 128);
        CU_ASSERT(0==memcmp(buffer, btree_entry_get_key(bte), keysize));
        CU_ASSERT(btree_entry_get_ptr(bte)==i);
    }

    /* check all keys with btree_node_search_by_key() */
    for (i=0; i<max; i++) {
        ham_key_t key;
        unsigned idx;
        btree_entry_t *bte;

        key.data=buffer;
        key.size=keysize;
        memset(buffer, i, 128);

        idx=btree_node_search_by_key(db, page, &key);
        /* index is based on 1 - check it, then normalize it */
        CU_ASSERT(idx==i+1);
        idx--;

        bte=btree_node_get_entry(db, node, idx);
        CU_ASSERT(bte!=0);
        CU_ASSERT(0==memcmp(buffer, btree_entry_get_key(bte), keysize));
        CU_ASSERT(btree_entry_get_ptr(bte)==i);
    }

    /* check all keys with btree_node_search_by_ptr() */
    for (i=0; i<max; i++) {
        unsigned idx;
        btree_entry_t *bte;

        memset(buffer, i, 128);
        idx=btree_node_search_by_ptr(db, node, i);
        /* index is based on 1 - check it, then normalize it */
        CU_ASSERT(idx==i+1);
        idx--;

        bte=btree_node_get_entry(db, node, idx);
        CU_ASSERT(bte!=0);
        CU_ASSERT(0==memcmp(buffer, btree_entry_get_key(bte), keysize));
    }

    /* release the memory */
    page_delete(page);

    /* close the database */
    st=ham_close(db);
    CU_ASSERT(st==0);

    /* cleanup */
    ham_delete(db);
}
