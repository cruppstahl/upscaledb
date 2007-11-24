/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 *
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/backend.h"
#include "../src/btree.h"

#include "getopts.h"

#define ARG_HELP            1
#define ARG_DBNAME          2
#define ARG_FULL            3

/*
 * command line parameters
 */
static option_t opts[]={
    {
        ARG_HELP,               // symbolic name of this option
        "h",                    // short option 
        "help",                 // long option 
        "this help screen",     // help string
        0 },                    // no flags
    {
        ARG_DBNAME,
        "db",
        "dbname",
        "only print info about this database",
        GETOPTS_NEED_ARGUMENT },
    {
        ARG_FULL,
        "f",
        "full",
        "print full information",
        0 },
    { 0, 0, 0, 0, 0 } /* terminating element */
};

static void 
error(const char *foo, ham_status_t st)
{
    printf("%s() returned error %d: %s\n", foo, st, ham_strerror(st));
    exit(-1);
}

static void
print_environment(ham_env_t *env)
{
    /*
     * we need a temp. database
     */
    ham_db_t *db;
    ham_status_t st;

    st=ham_new(&db);
    if (st)
        error("ham_new", st);

    st=ham_env_open_db(env, db, 0xf001, 0, 0);
    if (st)
        error("ham_env_open_db", st);

    printf("environment\n");
    printf("    pagesize:                   %u\n", db_get_pagesize(db));
    printf("    version:                    %u.%u.%u.%u\n", 
            db_get_version(db, 0),
            db_get_version(db, 1),
            db_get_version(db, 2),
            db_get_version(db, 3));
    printf("    serialno:                   %u\n", db_get_serialno(db));
    printf("    max databases:              %u\n", db_get_max_databases(db));

    st=ham_close(db, 0);
    if (st)
        error("ham_close", st);
    ham_delete(db);
}

static void
print_database(ham_db_t *db, ham_u16_t dbname, int full)
{
    ham_btree_t *be;
    ham_cursor_t *cursor;
    ham_status_t st;
    ham_key_t key;
    ham_record_t rec;
    unsigned num_items=0, ext_keys=0, min_key_size=0xffffffff, 
             max_key_size=0, min_rec_size=0xffffffff, max_rec_size=0,
            total_key_size=0, total_rec_size=0;

    be=(ham_btree_t *)db_get_backend(db);

    memset(&key, 0, sizeof(key));
    memset(&rec, 0, sizeof(rec));

    printf("\n");
    printf("    database %d (0x%x)\n", (int)dbname, (int)dbname);
    printf("        max key size:           %u\n", 
            be_get_keysize(be));
    printf("        max keys per page:      %u\n", 
            btree_get_maxkeys(be));
    printf("        address of root page:   %llu\n", 
            (long long unsigned int)btree_get_rootpage(be));
    printf("        flags:                  0x%04x\n", 
            db_get_rt_flags(db));

    if (!full)
        return;

    st=ham_cursor_create(db, 0, 0, &cursor);
    if (st!=HAM_SUCCESS)
        error("ham_cursor_create", st);

    while (1) {
        st=ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT);
        if (st!=HAM_SUCCESS) {
            /* reached end of the database? */
            if (st==HAM_KEY_NOT_FOUND)
                break;
            else 
                error("ham_cursor_next", st);
        }

        num_items++;

        if (key.size<min_key_size)
            min_key_size=key.size;
        if (key.size>max_key_size)
            max_key_size=key.size;

        if (rec.size<min_rec_size)
            min_rec_size=rec.size;
        if (rec.size>max_rec_size)
            max_rec_size=rec.size;

        if (key.size>db_get_keysize(db))
            ext_keys++;

        total_key_size+=key.size;
        total_rec_size+=rec.size;
    }

    ham_cursor_close(cursor);

    printf("        number of items:        %u\n", num_items);
    if (num_items==0)
        return;
    printf("        average key size:       %u\n", total_key_size/num_items);
    printf("        minimum key size:       %u\n", min_key_size);
    printf("        maximum key size:       %u\n", max_key_size);
    printf("        number of extended keys:%u\n", ext_keys);
    printf("        total keys (bytes):     %u\n", total_key_size);
    printf("        average record size:    %u\n", total_rec_size/num_items);
    printf("        minimum record size:    %u\n", min_rec_size);
    printf("        maximum record size:    %u\n", min_rec_size);
    printf("        total records (bytes):  %u\n", total_rec_size);
}

int
main(int argc, char **argv)
{
    unsigned opt;
    char *param, *filename=0, *endptr=0;
    unsigned short dbname=0xffff;
    int full=0;

    ham_u16_t names[1024];
    ham_size_t i, names_count=1024;
    ham_status_t st;
    ham_env_t *env;
    ham_db_t *db;

    getopts_init(argc, argv, "ham_info");

    while ((opt=getopts(&opts[0], &param))) {
        switch (opt) {
            case ARG_DBNAME:
                if (!param) {
                    printf("Parameter `dbname' is missing.\n");
                    return (-1);
                }
                dbname=(short)strtoul(param, &endptr, 0);
                if (endptr && *endptr) {
                    printf("Invalid parameter `dbname'; numerical value "
                           "expected.\n");
                    return (-1);
                }
                break;
            case ARG_FULL:
                full=1;
                break;
            case GETOPTS_PARAMETER:
                if (filename) {
                    printf("Multiple files specified. Please specify "
                           "only one filename.\n");
                    return (-1);
                }
                filename=param;
                break;
            case ARG_HELP:
                printf("Copyright (C) 2005-2007 Christoph Rupp "
                       "(chris@crupp.de).\n\n"
                       "This program is free software; you can redistribute "
                       "it and/or modify it\nunder the terms of the GNU "
                       "General Public License as published by the Free\n"
                       "Software Foundation; either version 2 of the License,\n"
                       "or (at your option) any later version.\n\n"
                       "See file COPYING.GPL2 and COPYING.GPL3 for License "
                       "information.\n\n");
                getopts_usage(&opts[0]);
                return (0);
            default:
                printf("Invalid or unknown parameter `%s'. "
                       "Enter `ham_info --help' for usage.", param);
                return (-1);
        }
    }

    if (!filename) {
        printf("Filename is missing. Enter `ham_info --help' for usage.\n");
        return (-1);
    }

    /*
     * open the environment
     */
    st=ham_env_new(&env);
    if (st!=HAM_SUCCESS)
        error("ham_env_new", st);
    st=ham_env_open(env, filename, HAM_READ_ONLY);
    if (st==HAM_FILE_NOT_FOUND) {
        printf("File `%s' not found or unable to open it\n", filename);
        return (-1);
    }
    else if (st!=HAM_SUCCESS)
        error("ham_env_open", st);

    /*
     * print information about the environment
     */
    print_environment(env);

    /*
     * get a list of all databases
     */
    st=ham_env_get_database_names(env, names, &names_count);
    if (st!=HAM_SUCCESS)
        error("ham_env_get_database_names", st);

    /*
     * did the user specify a database name? if yes, show only this database
     */
    if (dbname!=0xffff) {
        st=ham_new(&db);
        if (st)
            error("ham_new", st);
    
        st=ham_env_open_db(env, db, dbname, 0, 0);
        if (st==HAM_DATABASE_NOT_FOUND) {
            printf("Database %u (0x%x) not found\n", dbname, dbname);
            return (-1);
        }
        else if (st)
            error("ham_env_open_db", st);
    
        print_database(db, names[i], full);
    
        st=ham_close(db, 0);
        if (st)
            error("ham_close", st);
        ham_delete(db);
    }
    else {
        /*
         * otherwise: for each database: print information about the database
         */
        for (i=0; i<names_count; i++) {
            st=ham_new(&db);
            if (st)
                error("ham_new", st);
    
            st=ham_env_open_db(env, db, names[i], 0, 0);
            if (st)
                error("ham_env_open_db", st);
    
            print_database(db, names[i], full);
    
            st=ham_close(db, 0);
            if (st)
                error("ham_close", st);
            ham_delete(db);
        }
    } 
    /*
     * clean up
     */
    st=ham_env_close(env, 0);
    if (st!=HAM_SUCCESS)
        error("ham_env_close", st);

    ham_env_delete(env);

    return (0);
}
