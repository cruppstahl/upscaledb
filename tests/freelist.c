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
#include "../src/db.h"

static const char *filename="/tmp/hamster-test.db";

void 
test_freelist(void)
{
#define MAXRANGES 500
    int i;
    ham_status_t st;
    ham_offset_t off;
    ham_db_t *db;
    ham_txn_t txn;

    /* create the database handle */
    st=ham_new(&db);
    CU_ASSERT(st==0);

    /* delete the file */
    (void)unlink(filename);

    /* create a database */
    st=ham_create(db, filename, 0, 0664);
    CU_ASSERT(st==0);

    ham_txn_begin(&txn, db, 0);

    /* insert a lot of free ranges */
    for (i=1; i<=MAXRANGES; i++) {
        /*printf("adding range %d/%d\n", i, i);*/
        st=freel_add_area(db, &txn, i, i);
        CU_ASSERT(st==0);
    }

    /* fetch all ranges, then check if they are still available */ 
    for (i=MAXRANGES; i>=1; i--) {
        /*printf("fetching range %d/%d\n", i, i);*/
        off=freel_alloc_area(db, &txn, i, 0);
        CU_ASSERT(off==i);
        if (off!=i)
            printf("offset: needed %d, got %d\n", i, (int)off);
        off=freel_alloc_area(db, &txn, i, 0);
        CU_ASSERT(off==0);
    }

#if 0
    /* insert a range of 1000 bytes, and request 2 x 500 bytes */
    st=freel_add_area(db, 1000, 1000);
    CU_ASSERT(st==0);
    off=freel_alloc_area(db, 500, 0);
    CU_ASSERT(off==1000);
    off=freel_alloc_area(db, 500, 0);
    CU_ASSERT(off==1000);
    off=freel_alloc_area(db, 500, 0);
    CU_ASSERT(off==0);
#endif

    /* close the database */
    st=ham_close(db);
    CU_ASSERT(st==0);

    /* cleanup */
    ham_delete(db);
}
