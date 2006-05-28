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
#include "../src/error.h"

static const char *filename="/tmp/hamster-test.db";
static int handler_used=0;

void handler(const char *message)
{
    handler_used=1;
}

void 
test_errhand(void)
{
    ham_status_t st;
    ham_db_t *db;

    /* create the database handle */
    st=ham_new(&db);
    CU_ASSERT(st==0);

    /* set an error handler */
    ham_set_errhandler(handler);

    /* provoke an error - delete the file, then try to open it */
    (void)unlink(filename);

    /* open the database */
    st=ham_open(db, filename, 0);
    CU_ASSERT(st!=0);

    /* make sure the handler was used */
    CU_ASSERT(handler_used!=0);

    /* remove the error handler */
    ham_set_errhandler(0);

    /* close the database */
    st=ham_close(db);
    CU_ASSERT(st==0);

    /* cleanup */
    ham_delete(db);
}
