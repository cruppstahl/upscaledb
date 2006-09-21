/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for licence information
 *
 * unit test; reads and executes test files; compares hamsterdb
 * to berkeleydb
 */

#include <assert.h>
#include <unistd.h>
#include <stdio.h>
#include "getopts.h"

#include <ham/hamsterdb.h>

#define ARG_HELP            1
#define ARG_CREATE          2
#define ARG_OPEN            3
#define ARG_IN_MEMORY_DB    4
#define ARG_DB              5

ham_u32_t g_flags=0;

option_t opts[]={
  { 
      ARG_HELP,               // symbolic name of this option
      "h",                    // short option 
      "help",                 // long option 
      "this help screen",     // help string
      0 },                    // no flags
  { 
      ARG_CREATE,
      "c",
      "create",
      "<filename>   create an empty database file",
      GETOPTS_NEED_ARGUMENT },
  { 
      ARG_OPEN,
      "o",
      "open",
      "<filename>   open a database file",
      GETOPTS_NEED_ARGUMENT },
  { 
      ARG_IN_MEMORY_DB,
      "mem",
      "in-memory",
      "create an in-memory-db",
      0 },
  { 
      ARG_DB,
      "db",
      "db",
      "<testscript>   run the big database test",
      GETOPTS_NEED_ARGUMENT },
  { 0, 0, 0, 0, 0 }           // the terminating entry
};

int 
test_db(const char *filename);
static int
my_test_db(const char *filename)
{
    return (test_db(filename));
}

static int
my_test_create(const char *filename)
{
    ham_status_t st;
    ham_db_t *db;

    (void)unlink(filename);

    st=ham_new(&db);
    assert(st==0);
    st=ham_create(db, filename, g_flags, 0644);
    assert(st==0);
    st=ham_close(db);
    assert(st==0);
    st=ham_delete(db);
    assert(st==0);

    return (0);
}

static int
my_test_open(const char *filename)
{
    ham_status_t st;
    ham_db_t *db;

    st=ham_new(&db);
    assert(st==0);
    st=ham_open(db, filename, g_flags);
    assert(st==0);
    st=ham_close(db);
    assert(st==0);
    st=ham_delete(db);
    assert(st==0);

    return (0);
}

int 
main(int argc, char **argv)
{
    unsigned int opt;
    char *param;
    getopts_init(argc, argv, "test");

    while ((opt=getopts(&opts[0], &param))) {
        if (opt==ARG_HELP) {
            break;
        }
        else if (opt==ARG_DB) {
            printf("getopt: db test, file is %s\n", param);
            return (my_test_db(param));
        }
        else if (opt==ARG_CREATE) {
            printf("getopt: create file is %s\n", param);
            return (my_test_create(param));
        }
        else if (opt==ARG_OPEN) {
            printf("getopt: open file is %s\n", param);
            return (my_test_open(param));
        }
        else if (opt==ARG_IN_MEMORY_DB) {
            printf("getopt: in-memory-db\n");
            g_flags|=HAM_IN_MEMORY_DB;
        }
        else if (opt==GETOPTS_UNKNOWN) {
            printf("getopt: unknown parameter %s\n", param);
            break;
        }
        else if (opt==GETOPTS_MISSING_PARAM) {
            printf("getopt: parameter of %s is missing\n", param);
            break;
        }
    }

    getopts_usage(&opts[0]);
    return (0);
}
