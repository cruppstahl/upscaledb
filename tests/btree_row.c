/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 * insert2 unit tests
 *
 * this test inserts sequential keys till a page split occurs
 *
 */

#include <CUnit/Basic.h>
#include <stdio.h>
#include <unistd.h>
#include <malloc.h>
#include <ctype.h>
#include <ham/hamsterdb.h>
#include "../src/page.h"
#include "../src/db.h"
#include "../src/btree.h"
#include "main.h"

static const char *filename="./hamster-test.db";

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
my_dump_func(const ham_u8_t *key, ham_size_t keysize)
{
    printf("%d\n", *(unsigned *)key);
}

static int 
my_compare_keys(const ham_u8_t *lhs, ham_size_t lhs_length, 
                const ham_u8_t *rhs, ham_size_t rhs_length)
{
    unsigned ulhs=*(unsigned *)lhs;
    unsigned urhs=*(unsigned *)rhs;

    if (ulhs<urhs)
        return -1;
    if (ulhs==urhs)
        return 0;
    return 1;
}

static void
my_fill_key(char *buffer, unsigned value, ham_size_t keysize)
{
    memset(buffer, 0, keysize);
    *(unsigned *)buffer=value;
}

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
    fclose(f);
}

void 
test_btree_row(void)
{
    ham_status_t st;
    unsigned i, keysize, pagesize, error=0;
    ham_db_t *db;
    char buffer[128];

    /*
     * read the input data from stdin or from a file
     * the data is read to g_input and g_input_count;
     */
    my_read_input();
    if (!g_input_count)
        return;

    /* create the database handle */
    st=ham_new(&db);
    CU_ASSERT(st==0);

    /* delete the file */
    (void)unlink(filename);

#if PROFILE
    gettimeofday(&g_tv1, 0);
#endif /* PROFILE */

    pagesize=1024*4;
    keysize=8;

    /* create a database */
    st=ham_create_ex(db, filename, 0, 0664, 
            (short)pagesize, (short)keysize, 1024*1024);
    CU_ASSERT(st==0);
    ham_set_compare_func(db, my_compare_keys);

    CU_ASSERT(keysize==ham_get_keysize(db));
    CU_ASSERT(pagesize==ham_get_pagesize(db));

    /* insert or erase keys */
    for (i=0; i<g_input_count; i++) {
        ham_key_t key;
        ham_record_t record;
        long value=g_input[i];
        if (value<0)
            value*=-1;

        my_fill_key(buffer, value, keysize);
        key.size=keysize;
        key.data=buffer;
        memset(&record, 0, sizeof(record));
        record.data=&value;
        record.size=sizeof(value);

        if (g_input[i]>0) {
            /*printf("insert %d\n", value);*/
            st=ham_insert(db, &key, &record, 0);
            CU_ASSERT(st==0);
            if (st) {
                printf("ham_insert(%ld) failed with status 0x%x (%d)\n", 
                        value, st, st);
                error=1;
            }
        }
        else {
            /*printf("erase %d\n", value);*/
            st=ham_erase(db, &key, 0);
            CU_ASSERT(st==0);
            if (st) {
                printf("ham_erase(%ld) failed with status 0x%x (%d)\n", 
                        value, st, st);
                error=1;
            }
            my_mark_deleted(value);
        }
        if (!g_quiet) {
            st=ham_check_integrity(db);
            CU_ASSERT(st==0);
            if (st) {
                ham_dump(db, my_dump_func);
                printf("verify failed - last value: %ld\n", value);
                return;
            }
        }
    }

    /* check all keys with find() */
    for (i=0; i<g_input_count; i++) {
        ham_key_t key;
        ham_record_t record;
        long value=g_input[i];
        if (value<0)
            value*=-1;
        if (!value)
            continue;

        my_fill_key(buffer, value, keysize);
        key.size=keysize;
        key.data=buffer;
        memset(&record, 0, sizeof(record));

        st=ham_find(db, &key, &record, 0);
        if (g_input[i]<0) {
            CU_ASSERT(st==HAM_KEY_NOT_FOUND);
            if (st!=HAM_KEY_NOT_FOUND) {
                printf("XXXXX found 0x%lx (dez. %ld), although it "
                        "was deleted\n", value, value);
                error=1;
            }
        }
        else {
            CU_ASSERT(st==0);
            if (st) {
                printf("XXXXX didn't find 0x%lx (dez. %ld)\n", 
                        g_input[i], g_input[i]);
                error=1;
            }
            CU_ASSERT(*(unsigned *)record.data==g_input[i]);
            if (st==0 && *(unsigned *)record.data!=g_input[i]) {
                printf("XXXXX data comparison failed - data 0x%lx, input 0x%lx "
                    "(dez %ld)\n", *(unsigned long*)record.data, 
                    g_input[i], g_input[i]);
                error=1;
            }
        }
    }

    /* 
     * check integrity of the tree and dump it to stdout 
     */
    if (!g_quiet || error || ham_check_integrity(db))
        ham_dump(db, my_dump_func);

    /* close the database */
    st=ham_close(db);
    CU_ASSERT(st==0);

    /* cleanup */
    ham_delete(db);

#if PROFILE
    gettimeofday(&g_tv2, 0);
    printf("time elapsed: %lld.%lld sec\n", g_tv2.tv_sec-g_tv1.tv_sec, 
            g_tv2.tv_usec-g_tv1.tv_usec);
#endif /* PROFILE */
}

/*
 * the following code is a helper function to dump a single page;
 * it can be called from gdb and helps debugging:
 *
 * (gdb) call pp(page)
 *
 */
extern ham_status_t
my_dump_page(ham_page_t *page, ham_u32_t level, ham_u32_t count, 
        ham_dump_cb_t cb);

void
pp(ham_page_t *p)
{
    (void)my_dump_page(p, 0, 0, my_dump_func);
}
