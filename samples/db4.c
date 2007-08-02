/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for license and copyright 
 * information.
 *
 * This sample uses hamsterdb to read data from stdin; 
 * every word is inserted into the database in the order of its processing.
 * Then a cursor is used to print all words in the original order.
 *
 * This is basically the same as in db3.c, but we use a "record number"
 * database to assign keys.
 */

#include <stdio.h>
#include <string.h>
#include <ham/hamsterdb.h>

int 
main(int argc, char **argv)
{
    ham_status_t st;      /* status variable */
    ham_db_t *db;         /* hamsterdb database object */
    ham_cursor_t *cursor; /* a database cursor */
    char line[1024*4];    /* a buffer for reading lines */

    /*
     * first step: create a new hamsterdb object 
     */
    st=ham_new(&db);
    if (st!=HAM_SUCCESS) {
        printf("ham_new() failed with error %d\n", st);
        return (-1);
    }

    /*
     * second step: create a new hamsterdb "record number" database
     *
     * we could create an in-memory-database to speed up the sorting.
     */
    st=ham_create(db, "test.db", HAM_RECORD_NUMBER, 0664);
    if (st!=HAM_SUCCESS) {
        printf("ham_create() failed with error %d\n", st);
        return (-1);
    }

    /*
     * now we read each line from stdin and split it in words; then each 
     * word is inserted into the database
     */
    while (fgets(line, sizeof(line), stdin)) {
        char *start=line, *p;

        /*
         * strtok is not the best function because it's not threadsafe
         * and not flexible, but it's good enough for this example.
         */
        while ((p=strtok(start, " \t\r\n"))) {
            ham_key_t key;
            ham_record_t record;
            ham_u64_t recno;

            memset(&key, 0, sizeof(key));
            memset(&record, 0, sizeof(record));

            key.flags=HAM_KEY_USER_ALLOC;
            key.data=&recno;
            key.size=sizeof(recno);

            record.data=p;
            record.size=(ham_size_t)strlen(p)+1; /* also store the terminating 0-byte */

            /* note: the second parameter of ham_insert() is reserved; set it 
             * to NULL */
            st=ham_insert(db, 0, &key, &record, 0);
            if (st!=HAM_SUCCESS && st!=HAM_DUPLICATE_KEY) {
                printf("ham_insert() failed with error %d\n", st);
                return (-1);
            }
            printf(".");

            start=0;
        }
    }

    /* 
     * create a cursor 
     */
    st=ham_cursor_create(db, 0, 0, &cursor);
    if (st!=HAM_SUCCESS) {
        printf("ham_cursor_create() failed with error %d\n", st);
        return (-1);
    }

    while (1) {
        ham_key_t key;
        ham_record_t record;

        memset(&key, 0, sizeof(key));
        memset(&record, 0, sizeof(record));

        st=ham_cursor_move(cursor, &key, &record, HAM_CURSOR_NEXT);
        if (st!=HAM_SUCCESS) {
            /* reached end of the database? */
            if (st==HAM_KEY_NOT_FOUND)
                break;
            else {
                printf("ham_cursor_next() failed with error %d\n", st);
                return (-1);
            }
        }

        /* 
         * print the record number and the word 
         */
#if WIN32
        printf("%I64u: %s\n", *(ham_u64_t *)key.data, (const char *)record.data);
#else
        printf("%llu: %s\n", *(ham_u64_t *)key.data, (const char *)record.data);
#endif
    }

    /*
     * we're done! close the cursor 
     */
    st=ham_cursor_close(cursor);
    if (st!=HAM_SUCCESS) {
        printf("ham_cursor_close() failed with error %d\n", st);
        return (-1);
    }

    /*
     * then close the database handle
     */
    st=ham_close(db);
    if (st!=HAM_SUCCESS) {
        printf("ham_close() failed with error %d\n", st);
        return (-1);
    }

    /*
     * delete the database object to avoid memory leaks
     */
    ham_delete(db);

    /* 
     * success!
     */
    return (0);
}

