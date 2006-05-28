/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 * blob unit tests
 *
 * these tests insert a lot of blobs
 */

#include <CUnit/Basic.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <ham/hamsterdb.h>
#include "../src/blob.h"

static const char *filename="/tmp/hamster-test.db";

#define TESTRUNS    100

typedef struct 
{
    ham_bool_t active;
    ham_size_t size;
    ham_u8_t *data;
} testitem;

static void
my_create_test(testitem *test)
{
    static ham_u32_t r=0;
    ham_u32_t j;
    test->size=rand()%(10*1024)+1;
    if (test->data)
        free(test->data);
    test->data=malloc(test->size);
    test->active=HAM_TRUE;
    for (j=0; j<test->size; j++)
        test->data[j]=(ham_u8_t)(j+r);
    r++;
}

void 
test_blob(void)
{
    int i;
    ham_status_t st;
    ham_db_t *db;
    ham_offset_t blobid;
    ham_record_t record;
    ham_txn_t txn;
    testitem tests[TESTRUNS];

    memset(tests, 0, sizeof(tests));
    memset(&record, 0, sizeof(record));
    srand(time(0));

    /* create the database handle */
    st=ham_new(&db);
    CU_ASSERT(st==0);

    /* delete the file */
    (void)unlink(filename);

    /* create a database */
    st=ham_create(db, filename, 0, 0664);
    CU_ASSERT(st==0);

    ham_txn_begin(&txn, db, 0);

    /* create the tests */
    for (i=0; i<TESTRUNS; i++) 
        my_create_test(&tests[i]);

    for (i=0; i<TESTRUNS; i++) {
        /* insert a blob */
        st=blob_allocate(db, &txn, tests[i].data, tests[i].size, 0, &blobid);
        CU_ASSERT(st==0);

        /*
        printf("%02d: inserted blob of size %d -> 0x%lx\n", i, 
                tests[i].size, blobid); 
                */

        st=ham_txn_commit(&txn, 0);
        CU_ASSERT(st==0);

        /* fetch the blob */
        st=blob_read(db, &txn, blobid, &record, 0);
        CU_ASSERT(st==0);
        CU_ASSERT(record.size==tests[i].size);
        CU_ASSERT(0==memcmp(record.data, tests[i].data, tests[i].size));

        st=ham_txn_commit(&txn, 0);
        CU_ASSERT(st==0);

        /* replace the test */
        my_create_test(&tests[i]);
        st=blob_replace(db, &txn, blobid, tests[i].data, tests[i].size, 
                0, &blobid);
        CU_ASSERT(st==0);

        /*
        printf("%02d: replaced blob, new blobid is 0x%lx\n", i, blobid); 
        */

        st=ham_txn_commit(&txn, 0);
        CU_ASSERT(st==0);

        /* fetch the blob */
        st=blob_read(db, &txn, blobid, &record, 0);
        CU_ASSERT(st==0);
        CU_ASSERT(record.size==tests[i].size);
        CU_ASSERT(0==memcmp(record.data, tests[i].data, tests[i].size));

        st=ham_txn_commit(&txn, 0);
        CU_ASSERT(st==0);

        /* delete the blob */
        st=blob_free(db, &txn, blobid, 0);
        CU_ASSERT(st==0);

        st=ham_txn_commit(&txn, 0);
        CU_ASSERT(st==0);

        /* fetch the blob - must fail */
        st=blob_read(db, &txn, blobid, &record, 0);
        CU_ASSERT(st==HAM_BLOB_NOT_FOUND);

        st=ham_txn_commit(&txn, 0);
        CU_ASSERT(st==0);
    }

    for (i=0; i<TESTRUNS; i++) 
        free(tests[i].data);

    st=ham_txn_abort(&txn, 0);
    CU_ASSERT(st==0);

    /* close the database */
    st=ham_close(db);
    CU_ASSERT(st==0);

    /* cleanup */
    ham_delete(db);
}
