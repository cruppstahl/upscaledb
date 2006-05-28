/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file COPYING for licence information
 *
 * main source file for ham_dump
 *
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <ham/hamsterdb.h>
#include "page.h"
#include "freelist.h"
#include "db.h"

#define DUMP_HEADER   1
#define DUMP_FREELIST 2
#define DUMP_DATA     3
#define TYPE_STRING   1
#define TYPE_CHAR     2
#define TYPE_INT8     3
#define TYPE_INT16    4
#define TYPE_INT32    5
#define TYPE_INT64    6
#define TYPE_FLOAT    7
#define TYPE_DOUBLE   8
#define TYPE_BINARY   9

static int
my_usage(const char *prgname)
{
    printf("usage: %s [options] <database>\n"
           "  where [options] is one of the following: \n"
           "  -?, --help: this page\n"
           "  -hdr, --header: only dump header information\n"
           "  -fl, --freelist: only dump the freelist\n"
           "  --data (default): dump the whole index and data\n"
           "  -kt, --keytype=string|char|int8|int16|int32|int64|float|double|binary: \n"
           "          type of key (default: binary)\n"
           "  -dt, --datatype=string|char|int8|int16|int32|int64|float|double|binary: \n"
           "          type of data (default: binary)\n"
           "  -dl, --datalength=<num>: dump at most <num> bytes of data\n"
           "  and <database> is the filename of the database\n", prgname);
    return (0);
}

static int
my_parse_type(const char *p)
{
    p=strchr(p, '=');
    if (!p) 
        return (0);
    p++;
    if (!strcmp(p, "string"))
        return TYPE_STRING;
    if (!strcmp(p, "char"))
        return TYPE_CHAR;
    if (!strcmp(p, "int8"))
        return TYPE_INT8;
    if (!strcmp(p, "int16"))
        return TYPE_INT16;
    if (!strcmp(p, "int32"))
        return TYPE_INT32;
    if (!strcmp(p, "int64"))
        return TYPE_INT64;
    if (!strcmp(p, "float"))
        return TYPE_FLOAT;
    if (!strcmp(p, "double"))
        return TYPE_DOUBLE;
    if (!strcmp(p, "binary"))
        return TYPE_BINARY;
    return (0);
}

static int
my_parse_size(const char *p)
{
    p=strchr(p, '=');
    if (!p) 
        return (0);
    return ((int)strtoul(++p, 0, 0));
}

static void
my_handle_error(ham_status_t st)
{
    if (st) {
        printf("error %d: %s\n", st, ham_strerror(st));
        exit(-1);
    }
}

static void
my_dump_freelist_page(ham_db_t *db, ham_page_t *page, 
        freel_entry_t *list, ham_size_t elements)
{
    ham_size_t i, used=0;
    printf("freelist page 0x%lx\n", page ? page_get_self(page) : 0);

    for (i=0; i<elements; i++) {
        if (freel_get_address(&list[i])) {
            used++;
            printf("\t0x%08lx: %8d bytes\n", 
                freel_get_address(&list[i]), freel_get_size(&list[i]));
        }
    }

    printf("--- %d entries used (of %d)\n", used, elements);
}

static int
my_dump_freelist(ham_db_t *db)
{
    ham_offset_t overflow;
    ham_size_t max=freel_get_max_header_elements(db);

    /* dump the header page */
    my_dump_freelist_page(db, 0,
            freel_page_get_entries(&db->_u._pers._freelist), max);

    /* continue with overflow pages */
    overflow=freel_page_get_overflow(&db->_u._pers._freelist);
    max=freel_get_max_overflow_elements(db);
    while (overflow) {
        ham_page_t *p;
        freel_payload_t *fp;

        /* allocate the overflow page */
        p=db_page_fetch(db, overflow, 0);
        if (!p) {
            printf("fatal error: overflow pointer is broken\n");
            return (-1);
        }

        /* get a pointer to a freelist-page */
        fp=page_get_freel_payload(p);

        /* first member is the overflow pointer */
        overflow=freel_page_get_overflow(fp);

        /* dump the page */
        my_dump_freelist_page(db, p, freel_page_get_entries(fp), max);
        db_page_flush(p, 0);
    }

    return (0);
}

int 
main(int argc, char **argv)
{
    int i;
    int action  =DUMP_DATA;
    int keytype =TYPE_BINARY;
    int datatype=TYPE_BINARY;
    int datasize=12;
    const char *filename=0;
    ham_db_t *db;
    ham_status_t st;

    /*
     * parse the parameters
     */
    for (i=1; i<argc; i++) {
        if (strstr(argv[i], "--help") || strstr(argv[i], "-?"))
            return (my_usage(argv[0]));
        if (strstr(argv[i], "--header") || strstr(argv[i], "-hdr"))
            action=DUMP_HEADER;
        else if (strstr(argv[i], "--freelist") || strstr(argv[i], "-fl"))
            action=DUMP_FREELIST;
        else if (strstr(argv[i], "--data"))
            action=DUMP_DATA;
        else if (strstr(argv[i], "--keytype") || strstr(argv[i], "-kt")) {
            keytype=my_parse_type(argv[i]);
            if (!keytype) {
                printf("invalid keytype in parameter %d\n", i);
                return (my_usage(argv[0]));
            }
        }
        else if (strstr(argv[i], "--datatype") || strstr(argv[i], "-dt")) {
            datatype=my_parse_type(argv[i]);
            if (!datatype) {
                printf("invalid datatype in parameter %d\n", i);
                return (my_usage(argv[0]));
            }
        }
        else if (strstr(argv[i], "--datasize") || strstr(argv[i], "-ds")) {
            datasize=my_parse_size(argv[i]);
            if (!datasize) {
                printf("invalid datasize in parameter %d\n", i);
                return (my_usage(argv[0]));
            }
        }
        else if (strstr(argv[i], "--")==argv[i]) {
            printf("invalid parameter %d: %s\n", i, argv[i]);
            return (my_usage(argv[0]));
        }
        else {
            filename=argv[i];
        }
    }

    if (!filename) {
        printf("no filename given\n");
        return (my_usage(argv[0]));
    }

    /*
     * TODO hier gehts weiter - alles dumpen, was verlangt wurde
    printf("action: %d\n", action);
    printf("keytype: %d\n", keytype);
    printf("datatype: %d\n", datatype);
    printf("datasize: %d\n", datasize);
     */

    /*
     * open the database
     */
    st=ham_new(&db);
    if (st) 
        my_handle_error(st);
    st=ham_open(db, filename, 0);
        my_handle_error(st);

    if (action==DUMP_FREELIST)
        return (my_dump_freelist(db));

    return (0);
}
