/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 *
 * A simple example which connects to a hamsterdb server (see server1.c),
 * creates a database, inserts some values, looks them up and erases them.
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
    ham_status_t st;       /* status variable */
    ham_env_t *env;        /* hamsterdb Environment object */
    ham_db_t *db;          /* hamsterdb Database object */
    ham_key_t key;         /* the structure for a key */
    ham_record_t record;   /* the structure for a record */

    memset(&key, 0, sizeof(key));
    memset(&record, 0, sizeof(record));

    st=ham_new(&db);
    if (st!=HAM_SUCCESS)
        error("ham_new", st);
    st=ham_env_new(&env);
    if (st!=HAM_SUCCESS)
        error("ham_env_new", st);

    /*
     * now connect to the server which should listen at 8080
     *
     * ham_env_create_ex() will not really create a new Environment but rather
     * connect to an already existing one
     */
    st=ham_env_create_ex(env, "http://localhost:8080/env1.db", 0, 0, 0);
    if (st!=HAM_SUCCESS)
        error("ham_env_create_ex", st);

    /*
     * now open a Database in this Environment
     */
    st=ham_env_open_db(env, db, 13, 0, 0);
    if (st!=HAM_SUCCESS)
        error("ham_env_open_db", st);

    /*
     * now we can insert, delete or lookup values in the database
     */
    for (i=0; i<LOOP; i++) {
        key.data=&i;
        key.size=sizeof(i);

        record.size=key.size;
        record.data=key.data;

        st=ham_insert(db, 0, &key, &record, 0);
        if (st!=HAM_SUCCESS)
            error("ham_insert", st);
    }

    /*
     * now lookup all values
     */
    for (i=0; i<LOOP; i++) {
        key.data=&i;
        key.size=sizeof(i);

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
     * erase everything
     */
    for (i=0; i<LOOP; i++) {
        key.data=&i;
        key.size=sizeof(i);

        st=ham_erase(db, 0, &key, 0);
        if (st!=HAM_SUCCESS)
            error("ham_erase", st);
    }

    /*
     * and make sure that the database is empty
     */
    for (i=0; i<LOOP; i++) {
        key.data=&i;
        key.size=sizeof(i);

        st=ham_find(db, 0, &key, &record, 0);
        if (st!=HAM_KEY_NOT_FOUND)
            error("ham_find", st);
    }

    /*
     * close the database handle
     */
    st=ham_close(db, 0);
    if (st!=HAM_SUCCESS)
        error("ham_close", st);

    /*
     * delete the database object to avoid memory leaks
     */
    ham_delete(db);

    printf("success!\n");
    return (0);
}

