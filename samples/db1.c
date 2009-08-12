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
 * A simple example, which creates a database, inserts some values, 
 * looks them up and erases them.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h> /* for exit() */
#if UNDER_CE
#	include <windows.h>
#endif
#include <ham/hamsterdb.h>

#define LOOP 10

void 
error(const char *foo, ham_status_t st)
{
#if UNDER_CE
	wchar_t title[1024];
	wchar_t text[1024];

	MultiByteToWideChar(CP_ACP, 0, foo, -1, title, 
            sizeof(title)/sizeof(wchar_t));
	MultiByteToWideChar(CP_ACP, 0, ham_strerror(st), -1, text, 
            sizeof(text)/sizeof(wchar_t));

	MessageBox(0, title, text, 0);
#endif
    printf("%s() returned error %d: %s\n", foo, st, ham_strerror(st));
    exit(-1);
}

int 
main(int argc, char **argv)
{
    int i;
    ham_status_t st;       /* status variable */
    ham_db_t *db;          /* hamsterdb database object */
    ham_key_t key;         /* the structure for a key */
    ham_record_t record;   /* the structure for a record */

    memset(&key, 0, sizeof(key));
    memset(&record, 0, sizeof(record));

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
        key.data=&i;
        key.size=sizeof(i);

        record.size=key.size;
        record.data=key.data;

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
        key.data=&i;
        key.size=sizeof(i);

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
     * close the database handle, then re-open it (to demonstrate the 
     * call ham_open)
     */
    st=ham_close(db, 0);
    if (st!=HAM_SUCCESS)
        error("ham_close", st);
    st=ham_open(db, "test.db", 0);
    if (st!=HAM_SUCCESS)
        error("ham_open", st);

    /*
     * now erase all values
     */
    for (i=0; i<LOOP; i++) {
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
        key.size=sizeof(i);
        key.data=&i;

        st=ham_find(db, 0, &key, &record, 0);
        if (st!=HAM_KEY_NOT_FOUND)
            error("ham_find", st);
    }

    /*
     * we're done! close the database handle
     */
    st=ham_close(db, 0);
    if (st!=HAM_SUCCESS)
        error("ham_close", st);

    /*
     * delete the database object to avoid memory leaks
     */
    ham_delete(db);

#if UNDER_CE
    error("success", 0);
#endif
    printf("success!\n");
	return (0);
}

#if UNDER_CE
int 
_tmain(int argc, _TCHAR* argv[])
{
	return (main(0, 0));
}
#endif
