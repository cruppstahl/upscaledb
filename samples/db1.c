/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for license and copyright 
 * information.
 *
 * a simple example, which creates a database, inserts some values, 
 * looks them up and erases them
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h> /* for exit() */
#include <ham/hamsterdb.h>

#define LOOP 10

void 
error(const char *foo, ham_status_t st)
{
    printf("%s() returned error %d: %s\n", foo, st, ham_strerror(st));
    exit(-1);
}

int 
main(int argc, char **argv)
{
    int i;
    ham_status_t st;    /* status variable */
    ham_db_t *db;       /* hamsterdb database object */

    /*
     * first step: create a new hamsterdb object 
     */
    st=ham_new(&db);
    if (st!=HAM_SUCCESS)
        error("ham_new", st);

    /*
     * second step: create a new hamsterdb database
     *
     * we could also use ham_create_ex() if we wanted to specify the 
     * page size, key size or cache size limits
     */
    st=ham_create(db, "test.db", 0, 0664);
    if (st!=HAM_SUCCESS)
        error("ham_create", st);

    /*
     * now we can insert, delete or lookup values in the database
     *
     * for our test program, we just insert a few values, then look them 
     * up, then delete them and try to look them up again (which will fail).
     */
    for (i=0; i<LOOP; i++) {
        ham_key_t key;
        ham_record_t record;

        memset(&key, 0, sizeof(key));
        key.size=sizeof(i);
        key.data=&i;

        memset(&record, 0, sizeof(record));
        record.size=sizeof(i);
        record.data=&i;

        /* note: the second parameter of ham_insert() is reserved; set it to 
         * NULL */
        st=ham_insert(db, 0, &key, &record, 0);
        if (st!=HAM_SUCCESS)
            error("ham_insert", st);
    }

    /*
     * now lookup all values
     *
     * for ham_find(), we could use the flag HAM_RECORD_USER_ALLOC, if WE
     * allocate record.data (otherwise the memory is automatically allocated
     * by hamsterdb)
     */
    for (i=0; i<LOOP; i++) {
        ham_key_t key;
        ham_record_t record;

        memset(&key, 0, sizeof(key));
        key.size=sizeof(i);
        key.data=&i;

        memset(&record, 0, sizeof(record));

        /* note: the second parameter of ham_find() is reserved; set it to 
         * NULL */
        st=ham_find(db, 0, &key, &record, 0);
        if (st!=HAM_SUCCESS)
            error("ham_find", st);

        /*
         * check if the value is ok
         */
        if (*(int *)record.data!=i) {
            printf("ham_find() ok, but returned bad value\n");
            return (-1);
        }
    }

    /*
     * now erase all values
     */
    for (i=0; i<LOOP; i++) {
        ham_key_t key;

        memset(&key, 0, sizeof(key));
        key.size=sizeof(i);
        key.data=&i;

        /* note: the second parameter of ham_erase() is reserved; set it to 
         * NULL */
        st=ham_erase(db, 0, &key, 0);
        if (st!=HAM_SUCCESS)
            error("ham_erase", st);
    }

    /*
     * once more we try to find all values... every ham_find() call must
     * now fail with HAM_KEY_NOT_FOUND
     */
    for (i=0; i<LOOP; i++) {
        ham_key_t key;
        ham_record_t record;

        memset(&key, 0, sizeof(key));
        key.size=sizeof(i);
        key.data=&i;

        memset(&record, 0, sizeof(record));

        st=ham_find(db, 0, &key, &record, 0);
        if (st!=HAM_KEY_NOT_FOUND)
            error("ham_find", st);
    }

    /*
     * we're done! close the database handle
     */
    st=ham_close(db);
    if (st!=HAM_SUCCESS)
        error("ham_close", st);

    /*
     * delete the database object to avoid memory leaks
     */
    ham_delete(db);

    printf("success!\n");
    return (0);
}

