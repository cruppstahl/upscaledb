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
 * This sample uses hamsterdb to sort data from stdin; 
 * every word is inserted into the database (duplicate words are ignored).
 * Then a cursor is used to print all words in sorted order.
 */

#include <stdio.h>
#include <string.h>
#include <ham/hamsterdb.h>

static int 
my_string_compare(ham_db_t *db, const ham_u8_t *lhs, ham_size_t lhs_length, 
                  const ham_u8_t *rhs, ham_size_t rhs_length)
{
    (void)db;

    return strncmp((const char *)lhs, (const char *)rhs, 
            lhs_length<rhs_length ? lhs_length : rhs_length);
}

int 
main(int argc, char **argv)
{
    ham_status_t st;      /* status variable */
    ham_db_t *db;         /* hamsterdb database object */
    ham_cursor_t *cursor; /* a database cursor */
    char line[1024*4];    /* a buffer for reading lines */
    ham_key_t key;
    ham_record_t record;

    memset(&key, 0, sizeof(key));
    memset(&record, 0, sizeof(record));

    printf("This sample uses hamsterdb to sort data.\n");
    printf("Reading from stdin...\n");

    /*
     * first step: create a new hamsterdb object 
     */
    st=ham_new(&db);
    if (st!=HAM_SUCCESS) {
        printf("ham_new() failed with error %d\n", st);
        return (-1);
    }

    /*
     * second step: since we use strings as our database-keys, we use 
     * our own comparison function based on strcmp instead of the default
     * memcmp function.
     */
    st=ham_set_compare_func(db, my_string_compare);
    if (st) {
        printf("ham_set_compare_func() failed with error %d\n", st);
        return (-1);
    }

    /*
     * third step: create a new hamsterdb database
     *
     * we could create an in-memory-database to speed up the sorting.
     */
    st=ham_create(db, "test.db", 0, 0664);
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
            key.data=p;
            key.size=(ham_size_t)strlen(p)+1; /* also store the terminating 
                                                0-byte */

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

    /*
     * iterate over all items with HAM_CURSOR_NEXT, and print the words
     */
    while (1) {
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

        /* print the word */
        printf("%s\n", (const char *)key.data);
    }

    /*
     * then close the database handle; the flag
     * HAM_AUTO_CLEANUP will automatically close all cursors, and we
     * do not need to call ham_cursor_close
     */
    st=ham_close(db, HAM_AUTO_CLEANUP);
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

