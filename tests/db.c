/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <ctype.h>
#include <unistd.h>
#include <ham/hamsterdb.h>
#include <ham/hamsterdb_int.h>
#include <db.h>
#include <sys/time.h>
#include "getopts.h"
#include "../src/error.h"
#include "../src/config.h"

static ham_u64_t g_total_insert=0;
static ham_u64_t g_tv1, g_tv2;

static unsigned long g_filesize, g_filepos;

#define FILENAME_BERK "test-berk.db"
#define FILENAME_HAM  "test-ham.db"

#define PROFILE_START(w,i)  while (config.profile&w) {                      \
                                struct timeval tv;                          \
                                gettimeofday(&tv, 0);                       \
                                g_tv1=(ham_u64_t)(tv.tv_sec)*1000+          \
                                    (ham_u64_t)(tv.tv_usec)/1000;           \
                                break;                                      \
                            }       
#define PROFILE_STOP(w,i)     while (config.profile&w) {                    \
                                struct timeval tv;                          \
                                gettimeofday(&tv, 0);                       \
                                g_tv2=(ham_u64_t)(tv.tv_sec)*1000+          \
                                    (ham_u64_t)(tv.tv_usec)/1000;           \
                                if (w==PROF_INSERT)                         \
                                    config.prof_insert[i]+=g_tv2-g_tv1;     \
                                else if (w==PROF_ERASE)                     \
                                    config.prof_erase[i]+=g_tv2-g_tv1;      \
                                else if (w==PROF_FIND)                      \
                                    config.prof_find[i]+=g_tv2-g_tv1;       \
                                else if (w==PROF_OTHER)                     \
                                    config.prof_other[i]+=g_tv2-g_tv1;      \
                                else if (w==PROF_CURSOR)                    \
                                    config.prof_cursor[i]+=g_tv2-g_tv1;     \
                                break;                                      \
                            }

#define ARG_HELP        1
#define ARG_VERBOSE     2
#define ARG_PROFILE     3
#define ARG_QUIET       4
#define ARG_CHECK       5
#define ARG_BACKEND1    6
#define ARG_BACKEND2    7
#define ARG_DUMP        9
#define ARG_INMEMORY   10
#define ARG_OVERWRITE  11
#define ARG_PROGRESS   12
#define ARG_MMAP       13
#define ARG_PAGESIZE   14
#define ARG_KEYSIZE    15
#define ARG_CACHESIZE  16
#define ARG_CACHEPOLICY 17
#define ARG_REOPEN     18
#define ARG_USERALLOC  19
#define ARG_OPT_SIZE   20
#define ARG_FILE       21

#define PROF_INSERT     1
#define PROF_ERASE      2
#define PROF_FIND       4
#define PROF_OTHER      8
#define PROF_CURSOR    16
#define PROF_ALL      (PROF_INSERT|PROF_ERASE|PROF_FIND|PROF_CURSOR|PROF_OTHER)
#define PROF_NONE     (~PROF_ALL)

#define BACKEND_NONE    0
#define BACKEND_HAMSTER 1
#define BACKEND_BERK    2

#define NUMERIC_KEY     1

#define VERBOSE2(x)     if (config.verbose>=2) ham_log(x)
#define FAIL            ham_trace

/*
 * configuration
 */
static struct {
    /* verbose level */
    unsigned verbose;

    /* check level */
    unsigned check;

    /* dump the database? */
    unsigned dump;

    /* in-memory-database?  */
    unsigned inmemory;

    /* open/fullcheck/close after close? */
    unsigned reopen;

    /* ham_find: use flag HAM_RECORD_USER_ALLOC */
    unsigned useralloc;

    /* overwrite keys?  */
    unsigned overwrite;

    /* show progress?  */
    unsigned progress;

    /* optimize for size?  */
    unsigned opt_size;

    /* use mmap? */
    unsigned mmap;

    /* the page size */
    unsigned pagesize;

    /* the key size */
    unsigned keysize;

    /* the cache size */
    unsigned cachesize;

    /* use strict cache policy? */
    unsigned strict_cache;

    /* do profile?  */
    unsigned profile;

    /* be quiet?  */
    unsigned quiet;

    /* flags  */
    unsigned flags;

    /* the backends */
    unsigned backend[2];

    /* input filename */
    const char *filename;

    /* current line in the parser */
    unsigned cur_line;

    /* hamster-db handle */
    ham_db_t *hamdb;

    /* berkeley-db handle */
    DB *dbp;

    /* return values of the insert- or erase-call of the 
     * two backends*/
    unsigned long long retval[2];

    /* performance counter */
    ham_u64_t prof_insert[2];
    ham_u64_t prof_erase[2];
    ham_u64_t prof_find[2];
    ham_u64_t prof_other[2];
    ham_u64_t prof_cursor[2];

} config;

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
        ARG_VERBOSE,
        "v",
        "verbose",
        "be verbose",
        0 },
    {
        ARG_FILE,
        "f",
        "file",
        "the test script file",
        GETOPTS_NEED_ARGUMENT },
    {
        ARG_PROFILE,
        "p",
        "profile",
        "enable profiling",
        GETOPTS_NEED_ARGUMENT },
    {
        ARG_QUIET,
        "q",
        "quiet",
        "suppress output",
        GETOPTS_NEED_ARGUMENT },
    {
        ARG_CHECK,
        "c",
        "check",
        "do more consistency checks (-c twice will check even more)",
        GETOPTS_NEED_ARGUMENT },
    {
        ARG_BACKEND1,
        "b1",
        "backend1",
        "<hamster|berk|none> - the first backend",
        GETOPTS_NEED_ARGUMENT },
    {
        ARG_BACKEND2,
        "b2",
        "backend2",
        "<hamster|berk|none> - the second backend",
        GETOPTS_NEED_ARGUMENT },
    {
        ARG_DUMP,
        "d",
        "dump",
        "dump the hamster-database",
        0 },
    {
        ARG_INMEMORY,
        "inmem",
        "inmemorydb",
        "create in-memory-databases (if available)",
        GETOPTS_NEED_ARGUMENT },
    {
        ARG_OVERWRITE,
        "over",
        "overwrite",
        "overwrite existing keys",
        GETOPTS_NEED_ARGUMENT },
    {
        ARG_PROGRESS,
        "prog",
        "progress",
        "show progress",
        GETOPTS_NEED_ARGUMENT },
    {
        ARG_MMAP,
        "mmap",
        "mmap",
        "enable/disable mmap",
        GETOPTS_NEED_ARGUMENT },
    {
        ARG_REOPEN,
        "reopen",
        "reopen",
        "call OPEN/FULLCHECK/CLOSE after each close",
        GETOPTS_NEED_ARGUMENT },
    {
        ARG_USERALLOC,
        "usr",
        "useralloc",
        "ham_find: use flag HAM_RECORD_USER_ALLOC",
        GETOPTS_NEED_ARGUMENT },
    {
        ARG_OPT_SIZE,
        "size",
        "optimizesize",
        "creates database with HAM_OPTIMIZE_SIZE flag",
        GETOPTS_NEED_ARGUMENT },
    {
        ARG_PAGESIZE,
        "ps",
        "pagesize",
        "set the pagesize (use 0 for default)",
        GETOPTS_NEED_ARGUMENT },
    {
        ARG_KEYSIZE,
        "ks",
        "keysize",
        "set the keysize (use 0 for default)",
        GETOPTS_NEED_ARGUMENT },
    {
        ARG_CACHESIZE,
        "cs",
        "cachesize",
        "set the cachesize (use 0 for default)",
        GETOPTS_NEED_ARGUMENT },
    {
        ARG_CACHEPOLICY,
        "cp",
        "cachepolicy",
        "set the cachepolicy (allowed value: 'strict')",
        GETOPTS_NEED_ARGUMENT },
    { 0, 0, 0, 0, 0 }
};

static void
my_increment_progressbar(void)
{
#define PROGRESSBAR_COLUMNS  60
    int i;
    float pos=(float)g_filesize/(float)PROGRESSBAR_COLUMNS;
    pos=(float)g_filepos/pos;

    if (config.progress) {
        printf("progress: ");
        for (i=0; i<(int)pos; i++)
            printf("*");
        for (; i<PROGRESSBAR_COLUMNS; i++)
            printf(".");
        printf("\r");
    }
}

static const char *
my_get_profile_name(int i)
{
    switch (config.backend[i]) {
        case BACKEND_BERK:
            return "berkeley";
        case BACKEND_HAMSTER:
            return "hamster ";
        case BACKEND_NONE:
            return "none    ";
        default:
            return "?? unknown ??";
    }
}

static void
my_print_profile(void)
{
    float f, total[2]={0, 0};

    if (config.profile&PROF_INSERT) {
        f=config.prof_insert[0];
        total[0]+=f;
        f/=1000.f;
        printf("insert: profile of backend %s:\t%f sec\n", 
                my_get_profile_name(0), f);
        f=config.prof_insert[1];
        total[1]+=f;
        f/=1000.f;
        printf("insert: profile of backend %s:\t%f sec\n", 
                my_get_profile_name(1), f);
    }
    if (config.profile&PROF_ERASE) {
        f=config.prof_erase[0];
        total[0]+=f;
        f/=1000.f;
        printf("erase:  profile of backend %s:\t%f sec\n", 
                my_get_profile_name(0), f);
        f=config.prof_erase[1];
        total[1]+=f;
        f/=1000.f;
        printf("erase:  profile of backend %s:\t%f sec\n", 
                my_get_profile_name(1), f);
    }
    if (config.profile&PROF_FIND) {
        f=config.prof_find[0];
        total[0]+=f;
        f/=1000.f;
        printf("find:   profile of backend %s:\t%f sec\n", 
                my_get_profile_name(0), f);
        f=config.prof_find[1];
        total[1]+=f;
        f/=1000.f;
        printf("find:   profile of backend %s:\t%f sec\n", 
                my_get_profile_name(1), f);
    }
    if (config.profile&PROF_CURSOR) {
        f=config.prof_cursor[0];
        total[0]+=f;
        f/=1000.f;
        printf("cursor: profile of backend %s:\t%f sec\n", 
                my_get_profile_name(0), f);
        f=config.prof_cursor[1];
        total[1]+=f;
        f/=1000.f;
        printf("cursor: profile of backend %s:\t%f sec\n", 
                my_get_profile_name(1), f);
    }
    if (config.profile&PROF_OTHER) {
        f=config.prof_other[0];
        total[0]+=f;
        f/=1000.f;
        printf("other:  profile of backend %s:\t%f sec\n", 
                my_get_profile_name(0), f);
        f=config.prof_other[1];
        total[1]+=f;
        f/=1000.f;
        printf("other:  profile of backend %s:\t%f sec\n", 
                my_get_profile_name(1), f);
    }
    if (config.profile==PROF_ALL) {
        total[0]/=1000.f;
        printf("total:  profile of backend %s:\t%f sec\n", 
                my_get_profile_name(0), total[0]);
        total[1]/=1000.f;
        printf("total:  profile of backend %s:\t%f sec\n", 
                my_get_profile_name(1), total[1]);
    }
}

static void
my_dump_func(const ham_u8_t *key, ham_size_t keysize)
{
    ham_size_t i;

    if (config.flags & NUMERIC_KEY) {
        printf("%d\n", *(unsigned *)key);
    }
    else {
        for (i=0; i<keysize; i++)
            printf("%c", (char)key[i]);
        printf("\n");
    }
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

static int
my_compare_return(void)
{
    int ret;
    ham_status_t st;

    /* 
     * only one backend? don't compare
     */
    if (config.backend[1]==BACKEND_NONE) 
        return (HAM_TRUE);

    /*
     * otherwise check the return values
     */
    if (config.backend[0]==BACKEND_BERK) {
        ret=(int)config.retval[0];
        st =(ham_status_t)config.retval[1];
    }
    else {
        ret=(int)config.retval[1];
        st =(ham_status_t)config.retval[0];
    }

    switch (st) {
        case HAM_SUCCESS: 
            ham_assert(ret==0, ("hamster return: %d, berk: %d", st, ret));
            break;
        case HAM_KEY_NOT_FOUND:
            ham_assert(ret==DB_NOTFOUND, ("hamster return: %d, berk: %d", 
                    st, ret));
            break;
        case HAM_DUPLICATE_KEY:
            ham_assert(ret==DB_KEYEXIST, ("hamster return: %d, berk: %d", 
                    st, ret));
            break;
        case HAM_CACHE_FULL:
            ham_assert(1!=0, ("hamster return: %d, berk: %d", st, ret));
            return (HAM_FALSE);
        default:
            ham_assert(1!=0, ("hamster return: %d, berk: %d", st, ret));
            return (HAM_FALSE);
    }

    return (HAM_TRUE);
}

static char *
my_get_token(char *p, unsigned *pos)
{
    char *ret=&p[*pos];
    while (*ret && isspace(*ret))
        ret++;
    p=ret;
    while (*p && !isspace(*p))
        p++;
    *p=0;
    *pos=p-ret+1;
    return ret;
}

static char *
my_strtok(char *s, char *t)
{
    char *e, *p=strtok(s, t);
    if (!p) {
        ham_trace(("line %d: expected token '%s'", config.cur_line, t));
        exit(-1);
    }
    while (*p==' ' || *p=='\t' || *p=='\n' || *p=='\r' || *p=='(' || *p=='"')
        p++;
    e=p+strlen(p)-1;
    while (*e==' ' || *e=='\t' || *e=='\n' || *e=='\r' || *e==')' || *e=='"')
        e--;
    *(e+1)=0;
    return p;
}

static int
my_compare_databases(void)
{
    int ret, berk, ham;
    DBC *cursor;
    DBT key, rec;
    ham_key_t hkey;
    ham_record_t hrec;
    ham_status_t st;
    ham_cursor_t *hamc;

    memset(&key, 0, sizeof(key));
    memset(&rec, 0, sizeof(rec));

    if (!config.dbp || !config.hamdb)
        return 1;
    if (config.backend[0]==BACKEND_NONE || 
        config.backend[1]==BACKEND_NONE)
        return 1;
    if (config.backend[0]==BACKEND_BERK) {
        berk=0;
        ham=1;
    }
    else {
        ham=0;
        berk=1;
    }

    /*
     * get a cursor on the berkeley database and for hamsterdb; traverse the 
     * database, and compare each record
     */
    PROFILE_START(PROF_CURSOR, berk);
    ret=config.dbp->cursor(config.dbp, NULL, &cursor, 0);
    ham_assert(ret==0, ("berkeley-db error"));
    PROFILE_STOP(PROF_CURSOR, berk);

    PROFILE_START(PROF_CURSOR, ham);
    st=ham_cursor_create(config.hamdb, 0, 0, &hamc);
    ham_assert(st==0, ("hamster-db error"));
    memset(&hkey, 0, sizeof(hkey));
    memset(&hrec, 0, sizeof(hrec));
    st=ham_cursor_move(hamc, &hkey, &hrec, HAM_CURSOR_FIRST);
    PROFILE_STOP(PROF_CURSOR, ham);
    ham_assert(st==0, ("hamster-db error"));

    VERBOSE2(("comparing databases...", 0));
    PROFILE_START(PROF_CURSOR, berk);
    while ((ret=cursor->c_get(cursor, &key, &rec, DB_NEXT))==0) {
        PROFILE_STOP(PROF_CURSOR, berk);
    
        if (config.useralloc) {
            hrec.data=malloc(1024*1024*64); /* 64 mb? */
            if (!hrec.data) {
                FAIL(("useralloc: out of memory"));
                return 0;
            }
            hrec.flags=HAM_RECORD_USER_ALLOC;
        }

        ham_assert(hrec.size==rec.size, ("%u != %u", hrec.size, rec.size));
        if (hrec.data)
            ham_assert(!memcmp(hrec.data, rec.data, rec.size), (0));
        if (config.useralloc) 
            free(hrec.data);

        memset(&hkey, 0, sizeof(hkey));
        memset(&hrec, 0, sizeof(hrec));
        PROFILE_START(PROF_CURSOR, ham);
        st=ham_cursor_move(hamc, &hkey, &hrec, HAM_CURSOR_NEXT);
        PROFILE_STOP(PROF_CURSOR, ham);
        ham_assert(st==0, ("hamster-db error"));
    }
    ham_assert(ret==DB_NOTFOUND, (0));
    cursor->c_close(cursor);
    ham_cursor_close(hamc);

    return 1;
}

static ham_bool_t
my_execute_open(char *line);
static ham_bool_t
my_execute_fullcheck(char *line);
static ham_bool_t
my_execute_close(void);

static ham_bool_t 
my_execute_reopen(void)
{
    ham_bool_t b, old=config.reopen, reopen=0;

    /* avoid recursion */
    config.reopen=0; 

    if (config.hamdb) {
        reopen=1;
        b=my_execute_close();
        ham_assert(b==1, ("reopen failed with status %u", b));
    }

    b=my_execute_open("");
    ham_assert(b==1, ("reopen failed with status %u", b));
    b=my_execute_fullcheck("");
    ham_assert(b==1, ("reopen failed with status %u", b));
    b=my_execute_close();
    ham_assert(b==1, ("reopen failed with status %u", b));

    if (reopen) {
        b=my_execute_open("");
        ham_assert(b==1, ("reopen failed with status %u", b));
    }

    config.reopen=old; 

    return (1);
}

static ham_bool_t
my_execute_create(char *line)
{
    int i, ret;
    ham_status_t st;
    char *flags;
    ham_u32_t f=0;

    flags=line;

    /*
     * check flag NUMERIC_KEY
     */
    if (flags && strstr(flags, "NUMERIC_KEY")) {
        config.flags |= NUMERIC_KEY;
        VERBOSE2(("using numeric keys"));
    }

    for (i=0; i<2; i++) {
        switch (config.backend[i]) {
            case BACKEND_NONE: 
                break;
            case BACKEND_BERK: 
                VERBOSE2(("opening backend %d (berkeley)", i));
                if (config.dbp) {
                    FAIL(("berkeley handle already exists"));
                    return 0;
                }
                (void)unlink(FILENAME_BERK);
                PROFILE_START(PROF_OTHER, i);
                ret=db_create(&config.dbp, 0, 0);
                ham_assert(ret==0, (0));
                ret=config.dbp->open(config.dbp, 0, FILENAME_BERK, 0, 
                        DB_BTREE, DB_CREATE, 0);
                ham_assert(ret==0, (0));
                PROFILE_STOP(PROF_OTHER, i);
                break;
            case BACKEND_HAMSTER: 
                VERBOSE2(("opening backend %d (hamster)", i));
                if (config.hamdb) {
                    FAIL(("hamster handle already exists"));
                    return 0;
                }
                (void)unlink(FILENAME_HAM);
                PROFILE_START(PROF_OTHER, i);
                st=ham_new(&config.hamdb);
                ham_assert(st==0, (0));
                if (config.inmemory) {
                    f|=HAM_IN_MEMORY_DB;
                    config.cachesize=0;
                }
                f|=config.mmap?0:HAM_DISABLE_MMAP; 
                f|=config.strict_cache?HAM_CACHE_STRICT:0;
                f|=config.opt_size?HAM_OPTIMIZE_SIZE:0;
                st=ham_create_ex(config.hamdb, FILENAME_HAM, f, 0664, 
                        config.pagesize, config.keysize, config.cachesize);
                ham_assert(st==0, (0));
                ham_assert(config.hamdb->_backend!=0, (0));
                if (config.flags & NUMERIC_KEY)
                    ham_set_compare_func(config.hamdb, my_compare_keys);
                PROFILE_STOP(PROF_OTHER, i);
                break;
        }
    }

    return 1;
}

static ham_bool_t
my_execute_open(char *line)
{
    int i, ret;
    ham_status_t st;
    char *flags;

    flags=line;

    /*
     * check flag NUMERIC_KEY
     */
    if (flags && strstr(flags, "NUMERIC_KEY")) {
        config.flags |= NUMERIC_KEY;
        VERBOSE2(("using numeric keys"));
    }

    for (i=0; i<2; i++) {
        switch (config.backend[i]) {
            case BACKEND_NONE: 
                break;
            case BACKEND_BERK: 
                VERBOSE2(("opening backend %d (berkeley)", i));
                if (config.dbp) {
                    FAIL(("berkeley handle already exists"));
                    return 0;
                }
                PROFILE_START(PROF_OTHER, i);
                ret=db_create(&config.dbp, 0, 0);
                ham_assert(ret==0, (0));
                ret=config.dbp->open(config.dbp, 0, FILENAME_BERK, 0, 
                        DB_BTREE, 0, 0);
                ham_assert(ret==0, (0));
                PROFILE_STOP(PROF_OTHER, i);
                break;
            case BACKEND_HAMSTER: 
                VERBOSE2(("opening backend %d (hamster)", i));
                if (config.hamdb) {
                    FAIL(("hamster handle already exists"));
                    return 0;
                }
                PROFILE_START(PROF_OTHER, i);
                st=ham_new(&config.hamdb);
                ham_assert(st==0, (0));
                st=ham_open(config.hamdb, FILENAME_HAM, 0);
                ham_assert(st==0, (0));
                if (config.flags & NUMERIC_KEY)
                    ham_set_compare_func(config.hamdb, my_compare_keys);
                PROFILE_STOP(PROF_OTHER, i);
                break;
        }
    }

    return 1;
}

static ham_bool_t
my_execute_flush(void)
{
    int i;
    ham_status_t st;

    for (i=0; i<2; i++) {
        switch (config.backend[i]) {
            case BACKEND_NONE: 
                break;
            case BACKEND_BERK: 
                /*PROFILE_START(i);*/
                /* nothing to do here TODO */
                /*PROFILE_STOP(i);*/
                break;
            case BACKEND_HAMSTER: 
                VERBOSE2(("flushing backend %d (hamster)", i));
                if (!config.hamdb) {
                    FAIL(("hamster handle is invalid"));
                    return 0;
                }
                /*PROFILE_START(i);*/
                st=ham_flush(config.hamdb);
                ham_assert(st==0, (0));
                /*PROFILE_STOP(i);*/
                break;
        }
    }

    return 1;
}

static ham_bool_t
my_execute_insert(char *line)
{
    int i, use_numeric_key=0, data_size=0;
    unsigned long numeric_key;
    char *flags, *keytok, *data;

    /*
     * syntax: INSERT flags, key, datasize
     */
    flags =my_strtok(line, ",");
    keytok=my_strtok(0, ",");
    data  =my_strtok(0, ",");

    VERBOSE2(("insert: flags=%s, key=%s, data=%s", flags, keytok, data));

    /*
     * check flag NUMERIC_KEY
     */
    if ((flags && strstr(flags, "NUMERIC_KEY")) ||
        (config.flags & NUMERIC_KEY)) {
        use_numeric_key=1;
        numeric_key=strtoul(keytok, 0, 0);
        if (!numeric_key) {
            FAIL(("line %d: key is invalid", config.cur_line));
            return 0;
        }
    }

    /*
     * allocate and initialize data 
     */
    data_size=strtoul(data, 0, 0);
    if (data_size) {
        data=(char *)malloc(data_size);
        if (!data) {
            FAIL(("line %d: out of memory", config.cur_line));
            return 0;
        }
        for (i=0; i<data_size; i++)
            data[i]=i&0xff;
    }

    /*
     * now insert the value
     */
    for (i=0; i<2; i++) {
        switch (config.backend[i]) {
            case BACKEND_NONE: 
                break;
            case BACKEND_BERK: {
                DBT key, record;
                if (!config.dbp) {
                    FAIL(("berkeley handle is invalid"));
                    if (data_size)
                        free(data);
                    return 0;
                }
                PROFILE_START(PROF_INSERT, i);

                memset(&key, 0, sizeof(key));
                memset(&record, 0, sizeof(record));

                if (use_numeric_key) {
                    key.size=sizeof(unsigned long);
                    key.data=&numeric_key;
                }
                else {
                    key.size=strlen(keytok);
                    key.data=keytok;
                }
                record.size=data_size;
                record.data=data_size ? data : 0;

                config.retval[i]=config.dbp->put(config.dbp, 0, &key, &record, 
                        config.overwrite?0:DB_NOOVERWRITE);
                PROFILE_STOP(PROF_INSERT, i);
                VERBOSE2(("inserting into backend %d (berkeley): status %d",
                        i, (int)config.retval[i]));
                break;
            }
            case BACKEND_HAMSTER: {
                ham_key_t key;
                ham_record_t record;
                if (!config.hamdb) {
                    FAIL(("hamster handle is invalid"));
                    if (data_size)
                        free(data);
                    return 0;
                }
                PROFILE_START(PROF_INSERT, i);

                memset(&key, 0, sizeof(key));
                memset(&record, 0, sizeof(record));

                if (use_numeric_key) {
                    key.size=4; /*sizeof(unsigned long); */
                    key.data=&numeric_key;
                }
                else {
                    key.size=strlen(keytok);
                    key.data=keytok;
                }
                record.size=data_size;
                record.data=data_size ? data : 0;

                config.retval[i]=ham_insert(config.hamdb, 0,
                        &key, &record, 
                        config.overwrite?HAM_OVERWRITE:0);
                g_total_insert+=record.size;
                PROFILE_STOP(PROF_INSERT, i);
                VERBOSE2(("inserting into backend %d (hamster): status %d", 
                        i, (ham_status_t)config.retval[i]));
                break;
            }
        }
    }
    
    if (data_size)
        free(data);

    /*
     * compare the two return values
     */
    return my_compare_return();
}

static ham_bool_t
my_execute_erase(char *line)
{
    int i, use_numeric_key=0;
    unsigned long numeric_key;
    char *flags, *keytok;

    /*
     * syntax: INSERT flags, key, datasize
     */
    flags =my_strtok(line, ",");
    keytok=my_strtok(0, ",");

    VERBOSE2(("erase: flags=%s, key=%s", flags, keytok));

    /*
     * check flag NUMERIC_KEY
     */
    if ((flags && strstr(flags, "NUMERIC_KEY")) ||
        (config.flags & NUMERIC_KEY)) {
        use_numeric_key=1;
        numeric_key=strtoul(keytok, 0, 0);
        if (!numeric_key) {
            FAIL(("line %d: key is invalid", config.cur_line));
            return 0;
        }
    }

    /*
     * now erase the key
     */
    for (i=0; i<2; i++) {
        switch (config.backend[i]) {
            case BACKEND_NONE: 
                break;
            case BACKEND_BERK: {
                DBT key;
                if (!config.dbp) {
                    FAIL(("berkeley handle is invalid"));
                    return 0;
                }
                PROFILE_START(PROF_ERASE, i);

                memset(&key, 0, sizeof(key));

                if (use_numeric_key) {
                    key.size=sizeof(unsigned long);
                    key.data=&numeric_key;
                }
                else {
                    key.size=strlen(keytok);
                    key.data=keytok;
                }

                config.retval[i]=config.dbp->del(config.dbp, 0, &key, 0);
                PROFILE_STOP(PROF_ERASE, i);
                VERBOSE2(("erasing from backend %d (berkeley): status %d",
                        i, (int)config.retval[i]));
                break;
            }
            case BACKEND_HAMSTER: {
                ham_key_t key;
                if (!config.hamdb) {
                    FAIL(("hamster handle is invalid"));
                    return 0;
                }
                PROFILE_START(PROF_ERASE, i);

                memset(&key, 0, sizeof(key));

                if (use_numeric_key) {
                    key.size=sizeof(unsigned long);
                    key.data=&numeric_key;
                }
                else {
                    key.size=strlen(keytok);
                    key.data=keytok;
                }

                config.retval[i]=ham_erase(config.hamdb, 0, &key, 0);
                PROFILE_STOP(PROF_ERASE, i);
                VERBOSE2(("erasing from backend %d (hamster): status %d", 
                        i, (ham_status_t)config.retval[i]));
                break;
            }
        }
    }

    /*
     * compare the two return values
     */
    return my_compare_return();
}

static ham_bool_t
my_execute_find(char *line)
{
    int i, use_numeric_key=0;
    unsigned long numeric_key;
    char *flags, *keytok;

    /*
     * syntax: FIND flags, key
     */
    flags =my_strtok(line, ",");
    keytok=my_strtok(0, ",");

    VERBOSE2(("find: flags=%s, key=%s", flags, keytok));

    /*
     * check flag NUMERIC_KEY
     */
    if ((flags && strstr(flags, "NUMERIC_KEY")) ||
        (config.flags & NUMERIC_KEY)) {
        use_numeric_key=1;
        numeric_key=strtoul(keytok, 0, 0);
        if (!numeric_key) {
            FAIL(("line %d: key is invalid", config.cur_line));
            return 0;
        }
    }

    /*
     * now find the key
     */
    for (i=0; i<2; i++) {
        switch (config.backend[i]) {
            case BACKEND_NONE: 
                break;
            case BACKEND_BERK: {
                DBT key, rec;
                if (!config.dbp) {
                    FAIL(("berkeley handle is invalid"));
                    return 0;
                }
                PROFILE_START(PROF_FIND, i);

                memset(&key, 0, sizeof(key));
                memset(&rec, 0, sizeof(rec));

                if (use_numeric_key) {
                    key.size=sizeof(unsigned long);
                    key.data=&numeric_key;
                }
                else {
                    key.size=strlen(keytok);
                    key.data=keytok;
                }

                config.retval[i]=config.dbp->get(config.dbp, 0, &key, &rec, 0);
                PROFILE_STOP(PROF_FIND, i);
                VERBOSE2(("finding from backend %d (berkeley): status %d",
                        i, (int)config.retval[i]));
                break;
            }
            case BACKEND_HAMSTER: {
                ham_key_t key;
                ham_record_t record;
                if (!config.hamdb) {
                    FAIL(("hamster handle is invalid"));
                    return 0;
                }
                PROFILE_START(PROF_FIND, i);

                memset(&key, 0, sizeof(key));
                memset(&record, 0, sizeof(record));

                if (use_numeric_key) {
                    key.size=sizeof(unsigned long);
                    key.data=&numeric_key;
                }
                else {
                    key.size=strlen(keytok);
                    key.data=keytok;
                }

                config.retval[i]=ham_find(config.hamdb, 0, &key, &record, 0);
                PROFILE_STOP(PROF_FIND, i);
                VERBOSE2(("find from backend %d (hamster): status %d", 
                        i, (ham_status_t)config.retval[i]));
                break;
            }
        }
    }

    /*
     * compare the two return values
     */
    return my_compare_return();
}

static ham_bool_t
my_execute_fullcheck(char *line)
{
    if (config.reopen>=2) {
        ham_bool_t b=my_execute_reopen();
        ham_assert(b!=0, ("my_execute_reopen failed"));
    }

    /* 
     * check integrity
     */
    if (config.check==1) {
        if (config.backend[0]==BACKEND_HAMSTER ||
            config.backend[1]==BACKEND_HAMSTER) {
            ham_status_t st=0;
            st=ham_check_integrity(config.hamdb, 0);
            if (config.dump>=1) 
                (void)ham_dump(config.hamdb, 0, my_dump_func);
            ham_assert(st==0, ("check integrity failed"));
        }
    }

    /*
     * check database contents
     */
    if (!my_compare_databases()) 
        ham_assert(0, 
                ("failed to compare the databases, or databases not equal"));
    return HAM_TRUE;
}

static ham_bool_t
my_execute_close(void)
{
    int i;
    ham_status_t st;

    /* 
     * dump
     */
    if (config.dump>=1) {
        ham_status_t st=0;
        if (config.backend[0]==BACKEND_HAMSTER ||
            config.backend[1]==BACKEND_HAMSTER)
            ham_assert((st=ham_dump(config.hamdb, 0, my_dump_func))==0, (0));
        if (st)
            ham_trace(("hamster dump failed with status %d", st));
    }

    for (i=0; i<2; i++) {
        switch (config.backend[i]) {
            case BACKEND_NONE: 
                break;
            case BACKEND_BERK: 
                VERBOSE2(("closing backend %d (berkeley)", i));
                if (!config.dbp) {
                    FAIL(("berkeley handle is invalid"));
                    return 0;
                }
                PROFILE_START(PROF_OTHER, i);
                config.dbp->close(config.dbp, 0);
                config.dbp=0;
                PROFILE_STOP(PROF_OTHER, i);
                break;
            case BACKEND_HAMSTER: 
                VERBOSE2(("closing backend %d (hamster)", i));
                if (!config.hamdb) {
                    FAIL(("hamster handle is invalid"));
                    return 0;
                }
                PROFILE_START(PROF_OTHER, i);
                st=ham_close(config.hamdb);
                ham_assert(st==0, (0));
                ham_delete(config.hamdb);
                config.hamdb=0;
                PROFILE_STOP(PROF_OTHER, i);
                break;
        }
    }

    if (config.reopen) {
        ham_bool_t b=my_execute_reopen();
        ham_assert(b!=0, ("my_execute_reopen failed"));
    }

    return 1;
}

static ham_bool_t
my_execute(char *line)
{
    unsigned pos=0;
    char *tok=my_get_token(line, &pos);
    if (!tok || tok[0]==0)
        return 1;
    VERBOSE2(("reading token '%s'", tok));
    if (strstr(tok, "--")) {
        my_increment_progressbar();
        return 1;
    }
    if (!strcasecmp(tok, "CREATE")) {
        my_increment_progressbar();
        return my_execute_create(&line[pos]);
    }
    if (!strcasecmp(tok, "OPEN")) {
        my_increment_progressbar();
        return my_execute_open(&line[pos]);
    }
    if (!strcasecmp(tok, "INSERT")) {
        my_increment_progressbar();
        return my_execute_insert(&line[pos]);
    }
    if (!strcasecmp(tok, "ERASE")) {
        my_increment_progressbar();
        return my_execute_erase(&line[pos]);
    }
    if (!strcasecmp(tok, "FIND")) {
        my_increment_progressbar();
        return my_execute_find(&line[pos]);
    }
    if (!strcasecmp(tok, "FULLCHECK")) {
        my_increment_progressbar();
        return my_execute_fullcheck(&line[pos]);
    }
    if (!strcasecmp(tok, "CLOSE")) {
        my_increment_progressbar();
        return my_execute_close();
    }
    if (!strcasecmp(tok, "FLUSH")) {
        my_increment_progressbar();
        return my_execute_flush();
    }
    ham_trace(("line %d: invalid token '%s'", config.cur_line, tok));
    return HAM_FALSE;
}

int 
main(int argc, char **argv)
{
    FILE *f;
    unsigned opt;
    char *param;
    char line[1024*1024];

    getopts_init(argc, argv, "test");

    /*
     * initialize configuration with sane default values
     */
    memset(&config, 0, sizeof(config));
    config.verbose=1;
    config.check=1;
    config.backend[0]=BACKEND_HAMSTER;
    config.backend[1]=BACKEND_BERK;
    config.mmap=1; /* mmap is enabled by default */
    config.cachesize=HAM_DEFAULT_CACHESIZE;

    /*
     * parse command line parameters
     */
    while ((opt=getopts(&opts[0], &param))) {
        if (opt==ARG_HELP) {
            getopts_usage(&opts[0]);
            break;
        }
        else if (opt==ARG_PROFILE) {
            if (!param) {
                printf("missing profile parameter (none, all, "
                        "insert, erase, find, other)\n");
                return (-1);
            }
            if (!strcmp(param, "all")) {
                config.profile=PROF_ALL;
                continue;
            }
            if (!strcmp(param, "none")) {
                config.profile=PROF_NONE;
                continue;
            }
            if (!strcmp(param, "insert")) {
                config.profile=PROF_INSERT;
                continue;
            }
            if (!strcmp(param, "erase")) {
                config.profile=PROF_ERASE;
                continue;
            }
            if (!strcmp(param, "find")) {
                config.profile=PROF_FIND;
                continue;
            }
            if (!strcmp(param, "other")) {
                config.profile=PROF_OTHER;
                continue;
            }
            printf("bad profile parameter (none, all, "
                        "insert, erase, find, other)\n");
            return (-1);
        }
        else if (opt==ARG_CHECK) {
            if (!param || param[0]=='1' || param[0]=='y' || param[0]=='Y')
                config.check++;
            else
                config.check=0;
        }
        else if (opt==ARG_QUIET) {
            if (!param || param[0]=='1' || param[0]=='y' || param[0]=='Y')
                config.quiet++;
            else
                config.quiet=0;
        }
        else if (opt==ARG_VERBOSE) {
            config.verbose++;
        }
        else if (opt==ARG_FILE) {
            config.filename=param;
        }
        else if (opt==ARG_BACKEND1) {
            if (!strcmp(param, "berk")) 
                config.backend[0]=BACKEND_BERK;
            else if (!strcmp(param, "hamster")) 
                config.backend[0]=BACKEND_HAMSTER;
            else if (!strcmp(param, "none")) 
                config.backend[0]=BACKEND_NONE;
            else 
                ham_trace(("backend 1: unknown backend %s", param));
        }
        else if (opt==ARG_BACKEND2) {
            if (!strcmp(param, "berk")) 
                config.backend[1]=BACKEND_BERK;
            else if (!strcmp(param, "hamster")) 
                config.backend[1]=BACKEND_HAMSTER;
            else if (!strcmp(param, "none")) 
                config.backend[1]=BACKEND_NONE;
            else 
                ham_trace(("backend 2: unknown backend %s", param));
        }
        else if (opt==ARG_DUMP) {
            config.dump++;
        }
        else if (opt==ARG_INMEMORY) {
            if (!param || param[0]=='1' || param[0]=='y' || param[0]=='Y')
                config.inmemory=1;
            else
                config.inmemory=0;
        }
        else if (opt==ARG_OVERWRITE) {
            if (!param || param[0]=='1' || param[0]=='y' || param[0]=='Y')
                config.overwrite=1;
            else
                config.overwrite=0;
        }
        else if (opt==ARG_PROGRESS) {
            if (!param || param[0]=='1' || param[0]=='y' || param[0]=='Y')
                config.progress=1;
            else
                config.progress=0;
        }
        else if (opt==ARG_OPT_SIZE) {
            if (!param || param[0]=='1' || param[0]=='y' || param[0]=='Y')
                config.opt_size=1;
            else
                config.opt_size=0;
        }
        else if (opt==ARG_MMAP) {
            if (!param || param[0]=='1' || param[0]=='y' || param[0]=='Y')
                config.mmap=1;
            else
                config.mmap=0;
        }
        else if (opt==ARG_PAGESIZE) {
            config.pagesize=strtoul(param, 0, 0);
        }
        else if (opt==ARG_KEYSIZE) {
            config.keysize=strtoul(param, 0, 0);
        }
        else if (opt==ARG_CACHESIZE) {
            config.cachesize=strtoul(param, 0, 0);
        }
        else if (opt==ARG_CACHEPOLICY) {
            config.strict_cache=!strcmp(param, "strict");
        }
        else if (opt==ARG_REOPEN) {
            config.reopen++;
        }
        else if (opt==ARG_USERALLOC) {
            config.useralloc=1;
        }
        else if (opt==GETOPTS_UNKNOWN) {
            ham_trace(("unknown parameter %s", param));
            return (-1);
        }
        else {
            ham_trace(("unknown parameter %d", opt));
            return (-1);
        }
    }

    /*
     * open the file
     */
    if (!config.filename) {
        f=stdin;
        if (config.progress)  /* no progress bar if reading from stdin */
            config.progress=0;
    }
    else {
        f=fopen(config.filename, "rt");
        if (!f) {
            ham_trace(("cannot open %s: %s", config.filename, 
                    strerror(errno)));
            return (-1);
        }
        /* get the file size, if we display a progress bar */
        if (config.progress) {
            fseek(f, 0, SEEK_END);
            g_filesize=ftell(f);
            fseek(f, 0, SEEK_SET);
            g_filepos =0;
            VERBOSE2(("file size is %u bytes", g_filesize));
        }
    }

    /*
     * ... and run the test
     */
    while (!feof(f)) {
        config.cur_line++;

        /* read from the file */
        char *p=fgets(line, sizeof(line), f);
        if (!p)
            break;

        if (config.progress)
            g_filepos=ftell(f);

        if (!my_execute(line))
            break;

        if (config.check>=2) {
            if ((config.backend[0]==BACKEND_HAMSTER ||
                config.backend[1]==BACKEND_HAMSTER) && config.hamdb)
                ham_assert(ham_check_integrity(config.hamdb, 0)==0, (0));
        }

        VERBOSE2(("---- line %04d ----", config.cur_line));
    }

    if (config.filename)
        fclose(f);

    if (config.profile) 
        my_print_profile();

    printf("totally inserted: %llu\n", (long long unsigned int)g_total_insert);
    return (0);
}
