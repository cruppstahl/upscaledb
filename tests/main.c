/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 * entry point for a test suite based on CUnit (http://cunit.sf.net)
 *
 */

#include <stdio.h>
#include <string.h>
#include <CUnit/Basic.h>
#include <ham/hamsterdb.h>
#include "main.h"

/* 
 * array of all tests, indexed by name
 */
typedef struct {
    /* name of the test */
    const char *name;
    /* test function */
    void (*foo)(void);
} test_entry; 

/* 
 * global variables, declared in main.h
 */
int argc; 
char **argv;

test_entry tests[]={
    { "pageio", test_pageio },
    { "db", test_db },
    { "errhand", test_errhand },
    { "cache", test_cache },
    { "freelist", test_freelist },
    { "blob", test_blob },
    { "btree_payload", test_btree_payload },
    { "btree_find", test_btree_find },
    { "btree_row", test_btree_row },
    { "btree_extkeys", test_btree_extkeys },
    /*{ "btree_berk", test_btree_berk },*/
    { 0, 0 }
};

/* 
 * suite initialization function.
 * returns zero on success, non-zero otherwise.
 */
static int 
init_suite(void) {
    return 0;
}

/* 
 * suite cleanup function.
 * returns zero on success, non-zero otherwise.
 */
static int 
clean_suite(void)
{
    return 0;
}

/* 
 * the main() function for setting up and running the tests.
 * returns a CUE_SUCCESS on successful running, another
 * CUnit error code on failure.
 */
int 
main(int _argc, char **_argv)
{
    int i=0, found=0;
    CU_pSuite suite;

    argc=_argc;
    argv=_argv;

    /* initialize the CUnit test registry */
    if (CUE_SUCCESS!=CU_initialize_registry())
        return CU_get_error();

    /* add a suite to the registry */
    suite=CU_add_suite("Suite_1", init_suite, clean_suite);
    if (!suite) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    /* 
     * if argc>=2: only run the test with the name in argv[1];
     * otherwise run all tests
     */
    if (argc>=2) {
        while (tests[i].name) {
            if (tests[i].name && !strcmp(tests[i].name, argv[1])) {
                CU_add_test(suite, tests[i].name, tests[i].foo);
                found=1;
                break;
            }
            i++;
        }
    }
    else {
        while (tests[i].name) {
            CU_add_test(suite, tests[i].name, tests[i].foo);
            found=1;
            i++;
        }
    }

    if (!found) {
        printf("no valid test found\n");
        return -1;
    }

    /* Run all tests using the CUnit Basic interface */
    CU_basic_set_mode(CU_BRM_VERBOSE);
    CU_basic_run_tests();
    CU_cleanup_registry();
    return CU_get_error();
}

