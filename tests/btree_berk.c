/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 * insert unit tests
 *
 * this test works as btree_row, but uses berkeley-db (for benchmarking)
 *
 */

#include <CUnit/Basic.h>
#include <stdio.h>
#include <unistd.h>
#include <malloc.h>
#include <ctype.h>
#include <db.h> /* berkeley db.h */
#include "main.h"

static const char *filename="./berkeley-test.db";

#define INPUT_MAX    3000000
static long          g_input[INPUT_MAX];
static unsigned int  g_input_count=0;
static int           g_quiet=0;

#if PROFILE
#include <sys/time.h>
struct timeval g_tv1, g_tv2;
#endif /* PROFILE */

static void
my_mark_deleted(int value)
{
    int i;
    for (i=0; i<g_input_count; i++) {
        if (g_input[i]==value) {
            g_input[i]=0;
            return;
        }
    }
}

static void
my_fill_key(char *buffer, unsigned value, unsigned keysize)
{
    memset(buffer, 0, keysize);
    *(unsigned *)buffer=value;
}

#if 0
static int 
my_compare_keys(DB *db, const DBT *key1, const DBT *key2)
{
    unsigned ulhs=*(unsigned *)key1->data;
    unsigned urhs=*(unsigned *)key2->data;

    if (ulhs<urhs)
        return -1;
    if (ulhs==urhs)
        return 0;
    return 1;
}
#endif

static void
my_read_input(void)
{
    int i;
    FILE *f;
    char line[1024*1024];
    
    if (argc==1) {
        printf("need argument '-' for stdin or filename\n");
        return;
    }

    for (i=2; i<argc; i++) {
        if (!strcmp(argv[i], "--quiet")) {
            g_quiet=1;
            continue;
        }

        /* we need an argument for the text file, or "-" for stdin */
        if (!strcmp(argv[i], "-")) 
            f=stdin;
        else 
            f=fopen(argv[i], "rt");

        if (!f) {
            printf("failed to open %s\n", argv[i]);
            return;
        }
    }

    /* 
     * parse the input file and insert the items in g_input; resize 
     * g_input, if necessary
     */
    while (!feof(f)) {
        int sign=1;
        int item;
        /* read from the file */
        char *p=fgets(line, sizeof(line), f);
        if (!p)
            break;

        /* parse the tokens */
        while (*p) {
            sign=1;
            item=0;

            if (!*p)
                break;
            while (!isdigit(*p) && *p) {
                if (*p=='-')
                    sign=-1;
                p++;
            }

            item=0;

            while (isdigit(*p) && *p) {
                item*=10;
                item+=*p-'0';
                p++;
            }

            if (!item)
                continue;

            /* 'item' holds the item */
            if (g_input_count==INPUT_MAX) {
                printf("item overflow!\n");
                g_input_count=0;
                return;
            }
            g_input[g_input_count++]=item*sign;
        }
    }
}

void 
test_btree_berk(void)
{
    unsigned i, ret, keysize, error=0;
    DB *dbp;
    DBT key, data;
    char buffer[128];

    keysize=4;

    /*
     * read the input data from stdin or from a file
     * the data is read to g_input and g_input_count;
     */
    my_read_input();
    if (!g_input_count)
        return;

    /* delete the file */
    (void)unlink(filename);

#if PROFILE
    gettimeofday(&g_tv1, 0);
#endif /* PROFILE */

    /* create the database handle */
    ret=db_create(&dbp, 0, 0);
    CU_ASSERT(ret==0);
    ret=dbp->open(dbp, 0, filename, 0, DB_BTREE, DB_CREATE, 0);
    CU_ASSERT(ret==0);

    /* insert or erase keys */
    for (i=0; i<g_input_count; i++) {
        long value=g_input[i];
        if (value<0)
            value*=-1;

        my_fill_key(buffer, value, keysize);
        memset(&key, 0,  sizeof(DBT));
        memset(&data, 0, sizeof(DBT));
        key.data=buffer;
        key.size=keysize;
        data.data=&value;
        data.size=sizeof(value);

        if (g_input[i]>0) {
            ret=dbp->put(dbp, 0, &key, &data, DB_NOOVERWRITE);
            CU_ASSERT(ret==0);
            if (ret) {
                printf("db->put(%ld) failed with status 0x%x (%d)\n", 
                        value, ret, ret);
                error=1;
            }
        }
        else {
            ret=dbp->del(dbp, 0, &key, 0);
            CU_ASSERT(ret==0);
            if (ret) {
                printf("ham_erase(%ld) failed with status 0x%x (%d)\n", 
                        value, ret, ret);
                error=1;
            }
            my_mark_deleted(value);
        }
    }

    /* check all keys with find() */
    for (i=0; i<g_input_count; i++) {
        long value=g_input[i];
        if (value<0)
            value*=-1;
        if (!value)
            continue;

        my_fill_key(buffer, value, keysize);
        memset(&key, 0,  sizeof(DBT));
        memset(&data, 0, sizeof(DBT));
        key.data=buffer;
        key.size=keysize;
        data.data=&value;
        data.size=sizeof(value);

        ret=dbp->get(dbp, 0, &key, &data, 0);
        if (g_input[i]<0) {
            CU_ASSERT(ret!=0);
            if (ret==0) {
                printf("dbp->get() found 0x%lx (dez. %ld), although it "
                        "was deleted\n", value, value);
                error=1;
            }
        }
        else {
            CU_ASSERT(ret==0);
            if (ret) {
                printf("dbp->get() didn't find 0x%lx (dez. %ld)\n", 
                        g_input[i], g_input[i]);
                error=1;
            }
            CU_ASSERT(*(unsigned *)key.data==g_input[i]);
            if (ret==0 && *(unsigned *)key.data!=g_input[i]) {
                printf("dbp->get() data comparison failed - data 0x%x, "
                    "input 0x%lx dez %ld)\n", *(unsigned *)key.data, 
                    g_input[i], g_input[i]);
                error=1;
            }
        }
    }

    /* close the database */
    dbp->close(dbp, 0);

#if PROFILE
    gettimeofday(&g_tv2, 0);
    printf("time elapsed: %lld.%lld sec\n", g_tv2.tv_sec-g_tv1.tv_sec, 
            g_tv2.tv_usec-g_tv1.tv_usec);
#endif /* PROFILE */
}

