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

static void 
my_test_pageio(ham_u16_t pagesize)
{
#define MAXPAGES  5
    ham_status_t st;
    unsigned i, j;
    ham_db_t *db;
    ham_txn_t txn;
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

    /*
     * TODO fix this bug: pagesize-setzen schlägt immer fehl
     */
#if 0
    if (pagesize) {
        st=ham_set_pagesize(db, pagesize);
        CU_ASSERT(st==0);
    }
#endif

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

    /* read the pages from disk */
    for (i=0; i<MAXPAGES; i++) {
        st=page_io_read(pages[i], page_get_self(pages[i]));
        CU_ASSERT(st==0);
        /* compare the memory */
        for (j=0; j<ham_get_pagesize(db); j++)
            if (pages[i]->_pers._payload[j]!=i)
                CU_ASSERT(pages[i]->_pers._payload[j]==i);
    }

    /* release the memory */
    for (i=0; i<MAXPAGES; i++) {
        page_delete(pages[i]);
    }

    /* close the database */
    st=ham_close(db);
    CU_ASSERT(st==0);

    /* cleanup */
    ham_delete(db);
}

void 
test_pageio(void)
{
    my_test_pageio(0);
    my_test_pageio(1024*1);
    my_test_pageio(1024*2);
    my_test_pageio(1024*4);
    my_test_pageio(333);
    my_test_pageio(666);
}
