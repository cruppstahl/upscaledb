/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for license and copyright 
 * information.
 *
 * this example copies one database into another. this works also for 
 * copying in-memory-databases to on-disk-databases and vice versa.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h> /* for exit() */
#include <ham/hamsterdb.h>

void 
error(const char *foo, ham_status_t st)
{
    printf("%s() returned error %d: %s\n", foo, st, ham_strerror(st));
    exit(-1);
}

void
usage(void)
{
    printf("usage: ./db2 <source> <destination>\n");
    exit(-1);
}

void
copy_db(ham_db_t *source, ham_db_t *dest)
{
    ham_cursor_t *c;    /* hamsterdb cursor object */
    ham_status_t st;
    ham_key_t key;
    ham_record_t rec;

    memset(&key, 0, sizeof(key));
    memset(&rec, 0, sizeof(rec));

    /* create a new cursor */
    st=ham_cursor_create(source, 0, 0, &c); 
    if (st)
        error("ham_cursor_create", st);

    /* get a cursor to the source database */
    st=ham_cursor_move(c, &key, &rec, HAM_CURSOR_FIRST);
    if (st==HAM_KEY_NOT_FOUND) {
        printf("database is empty!\n");
        exit(-1);
    }
    else if (st)
        error("ham_cursor_move", st);

    do {
        /* insert this element into the new database */
        st=ham_insert(dest, 0, &key, &rec, 0);
        if (st)
            error("ham_insert", st);

        /* give some feedback to the user */
        printf(".");

        /* fetch the next item, and repeat till we've reached the end
         * of the database */
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        st=ham_cursor_move(c, &key, &rec, HAM_CURSOR_NEXT);
        if (st && st!=HAM_CURSOR_IS_NIL)
            error("ham_cursor_move", st);

    } while (st==0);

    /* clean up and return */
    ham_cursor_close(c);
}

int 
main(int argc, char **argv)
{
    ham_status_t st;
    ham_db_t *src, *dest;
    const char *src_path=0, *dest_path=0;

    /*
     * check and parse the command line parameters
     */
    if (argc!=3)
        usage();
    src_path =argv[1];
    dest_path=argv[2];

    /*
     * open the source database
     */
    st=ham_new(&src);
    if (st)
        error("ham_new", st);

    st=ham_open(src, src_path, 0);
    if (st)
        error("ham_open", st);

    /*
     * create the destination database
     */
    st=ham_new(&dest);
    if (st)
        error("ham_new", st);

    st=ham_create(dest, dest_path, 0, 0664);
    if (st)
        error("ham_create", st);

    /*
     * copy the data
     */
    copy_db(src, dest);

    /*
     * clean up and return
     */
    st=ham_close(src);
    if (st)
        error("ham_close", st);
    st=ham_close(dest);
    if (st)
        error("ham_close", st);
    ham_delete(src);
    ham_delete(dest);

    printf("\nsuccess!\n");
    return (0);
}

