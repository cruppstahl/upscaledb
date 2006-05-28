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

static const char *filename="/tmp/hamster-test.db";

void 
test_cache(void)
{
#define MAXPAGES  5
    ham_status_t st;
    ham_txn_t txn;
    unsigned i;
    ham_db_t *db;
    ham_page_t *pages[MAXPAGES];

    /* create the database handle */
    st=ham_new(&db);
    CU_ASSERT(st==0);

    /* delete the file */
    (void)unlink(filename);

    /* create a database */
    st=ham_create(db, filename, 0, 0664);
    CU_ASSERT(st==0);

    ham_txn_begin(&txn, db, 0);

    /* create a couple of page */
    for (i=0; i<MAXPAGES; i++) {
        /* get RAM */
        pages[i]=page_new(db);
        CU_ASSERT(pages[i]!=0);
        /* get HD memory */
        st=page_io_alloc(pages[i], &txn, 0);
        CU_ASSERT(st==0);
        /* fill the page */
        memset(&pages[i]->_pers, i, ham_get_pagesize(db));
        /* write page to disk */
        st=page_io_write(pages[i]);
        CU_ASSERT(st==0);
    }

    /* release the memory */
    for (i=0; i<MAXPAGES; i++) {
        page_delete(pages[i]);
    }

    /* 
     * read the pages from disk 
     * we've created MAXPAGES, with addresses from 
     * 1024 to 1024+MAXPAGES*pagesize
     */
    for (i=0; i<MAXPAGES; i++) {
        unsigned pagesize=ham_get_pagesize(db);
        ham_page_t *p=txn_fetch_page(&txn, 1024+i*pagesize, 0);
        CU_ASSERT(p!=0);
    }

    /* close the database */
    st=ham_close(db);
    CU_ASSERT(st==0);

    /* cleanup */
    ham_delete(db);
}
