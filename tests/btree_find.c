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
test_btree_find(void)
{
    char *dummydata="garbage data; at least 20 bytes long.";
    ham_status_t st;
    ham_db_t *db;
    ham_key_t key;
    ham_record_t record;

    key.size=20;
    key.data=dummydata;

    /* create the database handle */
    st=ham_new(&db);
    CU_ASSERT(st==0);

    /* delete the file */
    (void)unlink(filename);

    /* create a database */
    st=ham_create(db, filename, 0, 0664);
    CU_ASSERT(st==0);

    /* search for an item; make sure that the search fails, cause we 
     * have no root page yet */
    st=ham_find(db, &key, &record, 0);
    CU_ASSERT(st!=0);

    /* close the database */
    st=ham_close(db);
    CU_ASSERT(st==0);

    /* cleanup */
    ham_delete(db);
}
