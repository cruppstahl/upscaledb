/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 * insert1 unit tests
 *
 * these tests inserts random data in a page, till the page is full.
 * the page is not split.
 *
 */

#include <CUnit/Basic.h>
#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <unistd.h>
#include <ham/hamsterdb.h>
#include "../src/page.h"
#include "../src/db.h"
#include "../src/btree.h"

static const char *filename="/tmp/hamster-test.db";

void 
test_btree_extkeys(void)
{
    ham_status_t st;
    unsigned long i;
    unsigned int maxkeys, keysize;
    ham_db_t *db;
    char keybuffer[20];
    char databuffer[128];

    /* create the database handle */
    st=ham_new(&db);
    CU_ASSERT(st==0);

    /* delete the file */
    (void)unlink(filename);

    /* create a database */
    st=ham_create_ex(db, filename, 0, 0664, 0, 10, HAM_DEFAULT_CACHESIZE);
    CU_ASSERT(st==0);

    /* get the keysize */
    keysize=ham_get_keysize(db);
    CU_ASSERT(keysize==10);

    /* get the maximum number of keys in one page */
    maxkeys=db_get_maxkeys(db);

    /* insert keys */
    for (i=5; i<maxkeys; i++) {
        ham_key_t key;
        ham_record_t record;

        memset(&key, 0, sizeof(key));
        memset(&record, 0, sizeof(record));

        memset(keybuffer, i, sizeof(keybuffer));
        memset(databuffer, i, sizeof(databuffer));
        key.size=sizeof(keybuffer);
        key.data=keybuffer;
        record.size=sizeof(databuffer);
        record.data=databuffer;

        st=ham_insert(db, &key, &record, 0);
        CU_ASSERT(st==0);
    }
    for (i=0; i<5; i++) {
        ham_key_t key;
        ham_record_t record;

        memset(&key, 0, sizeof(key));
        memset(&record, 0, sizeof(record));

        memset(keybuffer, i, sizeof(keybuffer));
        memset(databuffer, i, sizeof(databuffer));
        key.size=sizeof(keybuffer);
        key.data=keybuffer;
        record.size=sizeof(databuffer);
        record.data=databuffer;

        st=ham_insert(db, &key, &record, 0);
        CU_ASSERT(st==0);
    }

    /* check all keys with find() */
    for (i=0; i<maxkeys; i++) {
        ham_key_t key;
        ham_record_t record;

        memset(&key, 0, sizeof(key));
        memset(&record, 0, sizeof(record));

        memset(keybuffer, i, sizeof(keybuffer));
        memset(databuffer, i, sizeof(databuffer));
        key.size=sizeof(keybuffer);
        key.data=keybuffer;

        st=ham_find(db, &key, &record, 0);
        CU_ASSERT(st==0);
        CU_ASSERT(record.size==sizeof(databuffer));
        CU_ASSERT(0==memcmp(record.data, databuffer, record.size));
    }

    /* dump to stdout, if integrity check fails */
    if (ham_check_integrity(db))
        ham_dump(db, 0);

    /* close the database */
    st=ham_close(db);
    CU_ASSERT(st==0);

    /* cleanup */
    ham_delete(db);
}
