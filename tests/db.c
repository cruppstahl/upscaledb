/**
 * Copyright (C) 2005, 2006 Christoph Rupp (chris@crupp.de)
 * see file LICENSE for license and copyright information
 *
 */

#include <CUnit/Basic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <unistd.h>
#include <ham/hamsterdb.h>
#include <db.h>
#include <sys/time.h>
#include "../src/error.h"
#include "getopts.h"
#include "main.h"

static ham_u64_t g_tv1, g_tv2;

#define FILENAME_BERK "/tmp/test-berk.db"
#define FILENAME_HAM  "/tmp/test-ham.db"

#define PROFILE_START(i)    while (config.profile) {                        \
                                struct timeval tv;                          \
                                gettimeofday(&tv, 0);                       \
                                g_tv1=(ham_u64_t)(tv.tv_sec)*1000+          \
                                    (ham_u64_t)(tv.tv_usec)/1000;           \
                                break;                                      \
                            }       
#define PROFILE_STOP(i)     while (config.profile) {                        \
                                struct timeval tv;                          \
                                gettimeofday(&tv, 0);                       \
                                g_tv2=(ham_u64_t)(tv.tv_sec)*1000+          \
                                    (ham_u64_t)(tv.tv_usec)/1000;           \
                                config.prof[i]+=g_tv2-g_tv1;                \
                                break;                                      \
                            }

#define ARG_HELP        1
#define ARG_VERBOSE     2
#define ARG_PROFILE     3
#define ARG_QUIET       4
#define ARG_CHECK       5
#define ARG_BACKEND1    6
#define ARG_BACKEND2    7
#define ARG_INPUT       8
#define ARG_DUMP        9

#define BACKEND_NONE    0
#define BACKEND_HAMSTER 1
#define BACKEND_BERK    2

#define NUMERIC_KEY     1

#define VERBOSE2        if (config.verbose>=2) ham_trace
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
    ham_u64_t prof[2];

} config;

/*
 * command line parameters
 */
static option_t opts[]={
    {
        ARG_HELP,
        "h",
        "help",
        "this help screen",
        0 },
    {
        ARG_VERBOSE,
        "v",
        "verbose",
        "be verbose",
        0 },
    {
        ARG_PROFILE,
        "p",
        "profile",
        "enable profiling",
        0 },
    {
        ARG_QUIET,
        "q",
        "quiet",
        "suppress output",
        0 },
    {
        ARG_CHECK,
        "c",
        "check",
        "do more consistency checks (-c twice will check even more)",
        0 },
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
        ARG_INPUT,
        "in",
        "input",
        "<filename> - input filename",
        GETOPTS_NEED_ARGUMENT },
    {
        ARG_DUMP,
        "d",
        "dump",
        "dump the hamster-database",
        0 },
    { 0, 0, 0, 0, 0 }
};

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
    if (config.backend[1]==BACKEND_NONE) {
        return (HAM_TRUE);
    }

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
            ham_assert(ret==0, "hamster return: %d, berk: %d", st, ret);
            break;
        case HAM_KEY_NOT_FOUND:
            ham_assert(ret==DB_NOTFOUND, "hamster return: %d, berk: %d", 
                    st, ret);
            break;
        case HAM_DUPLICATE_KEY:
            ham_assert(ret==DB_KEYEXIST, "hamster return: %d, berk: %d", 
                    st, ret);
            break;
        default:
            ham_assert(1!=0, "hamster return: %d, berk: %d", st, ret);
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
        ham_trace("line %d: expected token '%s'", config.cur_line, t);
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

    memset(&key, 0, sizeof(key));
    memset(&rec, 0, sizeof(rec));
    memset(&hkey, 0, sizeof(hkey));
    memset(&hrec, 0, sizeof(hrec));

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
     * get a cursor on the berkeley database; traverse the database, and
     * query hamster for each item
     */
    ret=config.dbp->cursor(config.dbp, NULL, &cursor, 0);
    ham_assert(ret==0, "berkeley-db error", 0);
    VERBOSE2("comparing databases...", 0);
    /*PROFILE_START(berk);*/
    while ((ret=cursor->c_get(cursor, &key, &rec, DB_NEXT))==0) {
        /*PROFILE_STOP(berk);*/
        hkey.size=key.size;
        hkey.data=key.data;
        /*PROFILE_START(ham);*/
        st=ham_find(config.hamdb, &hkey, &hrec, 0);
        /*PROFILE_STOP(ham);*/
        if (st==0) {
            ham_assert(hrec.size==rec.size, 0, 0);
            ham_assert(hrec.data!=0, 0, 0);
            if (hrec.data)
                ham_assert(!memcmp(hrec.data, rec.data, rec.size), 0, 0);
        }
        /*PROFILE_START(berk);*/
    }
    /*PROFILE_STOP(berk);*/
    ham_assert(ret==DB_NOTFOUND, 0, 0);
    cursor->c_close(cursor);

    return 1;
}

static ham_bool_t
my_execute_create(char *line)
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
        VERBOSE2("using numeric keys", 0);
    }

    for (i=0; i<2; i++) {
        switch (config.backend[i]) {
            case BACKEND_NONE: 
                break;
            case BACKEND_BERK: 
                VERBOSE2("opening backend %d (berkeley)", i);
                if (config.dbp) {
                    FAIL("berkeley handle already exists", 0);
                    return 0;
                }
                (void)unlink(FILENAME_BERK);
                PROFILE_START(i);
                ret=db_create(&config.dbp, 0, 0);
                ham_assert(ret==0, 0, 0);
                ret=config.dbp->open(config.dbp, 0, FILENAME_BERK, 0, 
                        DB_BTREE, DB_CREATE, 0);
                ham_assert(ret==0, 0, 0);
                PROFILE_STOP(i);
                break;
            case BACKEND_HAMSTER: 
                VERBOSE2("opening backend %d (hamster)", i);
                if (config.hamdb) {
                    FAIL("hamster handle already exists", 0);
                    return 0;
                }
                PROFILE_START(i);
                (void)unlink(FILENAME_HAM);
                st=ham_new(&config.hamdb);
                ham_assert(st==0, 0, 0);
                st=ham_create(config.hamdb, FILENAME_HAM, 0, 0664);
                ham_assert(st==0, 0, 0);
                if (config.flags & NUMERIC_KEY)
                    ham_set_compare_func(config.hamdb, my_compare_keys);
                PROFILE_STOP(i);
                break;
        }
    }

    return 1;
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
            ham_assert((st=ham_dump(config.hamdb, my_dump_func))==0, 0, 0);
        if (st)
            ham_trace("hamster dump failed with status %d", st);
    }

    for (i=0; i<2; i++) {
        switch (config.backend[i]) {
            case BACKEND_NONE: 
                break;
            case BACKEND_BERK: 
                VERBOSE2("closing backend %d (berkeley)", i);
                if (!config.dbp) {
                    FAIL("berkeley handle is invalid", 0);
                    return 0;
                }
                PROFILE_START(i);
                config.dbp->close(config.dbp, 0);
                config.dbp=0;
                PROFILE_STOP(i);
                break;
            case BACKEND_HAMSTER: 
                VERBOSE2("closing backend %d (hamster)", i);
                if (!config.hamdb) {
                    FAIL("hamster handle is invalid", 0);
                    return 0;
                }
                PROFILE_START(i);
                st=ham_close(config.hamdb);
                ham_assert(st==0, 0, 0);
                ham_delete(config.hamdb);
                config.hamdb=0;
                PROFILE_STOP(i);
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

    VERBOSE2("insert: flags=%s, key=%s, data=%s", flags, keytok, data);

    /*
     * check flag NUMERIC_KEY
     */
    if ((flags && strstr(flags, "NUMERIC_KEY")) ||
        (config.flags & NUMERIC_KEY)) {
        use_numeric_key=1;
        numeric_key=strtoul(keytok, 0, 0);
        if (!numeric_key) {
            FAIL("line %d: key is invalid", config.cur_line);
            return 0;
        }
    }

    /*
     * allocate and initialize data 
     */
    data_size=strtoul(data, 0, 0);
    if (!data_size) {
        FAIL("line %d: data size is invalid", config.cur_line);
        return 0;
    }
    data=(char *)malloc(data_size);
    if (!data) {
        FAIL("line %d: out of memory", config.cur_line);
        return 0;
    }
    for (i=0; i<data_size; i++)
        data[i]=i&0xff;

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
                    FAIL("berkeley handle is invalid", 0);
                    free(data);
                    return 0;
                }
                PROFILE_START(i);

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
                record.data=data;

                config.retval[i]=config.dbp->put(config.dbp, 0, &key, &record, 
                        DB_NOOVERWRITE);
                PROFILE_STOP(i);
                VERBOSE2("inserting into backend %d (berkeley): status %d",
                        i, (int)config.retval[i]);
                break;
            }
            case BACKEND_HAMSTER: {
                ham_key_t key;
                ham_record_t record;
                if (!config.hamdb) {
                    FAIL("hamster handle is invalid", 0);
                    free(data);
                    return 0;
                }
                PROFILE_START(i);

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
                record.data=data;

                config.retval[i]=ham_insert(config.hamdb, &key, &record, 0);
                PROFILE_STOP(i);
                VERBOSE2("inserting into backend %d (hamster): status %d", 
                        i, (ham_status_t)config.retval[i]);
                break;
            }
        }
    }

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

    VERBOSE2("erase: flags=%s, key=%s", flags, keytok);

    /*
     * check flag NUMERIC_KEY
     */
    if ((flags && strstr(flags, "NUMERIC_KEY")) ||
        (config.flags & NUMERIC_KEY)) {
        use_numeric_key=1;
        numeric_key=strtoul(keytok, 0, 0);
        if (!numeric_key) {
            FAIL("line %d: key is invalid", config.cur_line);
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
                    FAIL("berkeley handle is invalid", 0);
                    return 0;
                }
                PROFILE_START(i);

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
                PROFILE_STOP(i);
                VERBOSE2("erasing from backend %d (berkeley): status %d",
                        i, (int)config.retval[i]);
                break;
            }
            case BACKEND_HAMSTER: {
                ham_key_t key;
                if (!config.hamdb) {
                    FAIL("hamster handle is invalid", 0);
                    return 0;
                }
                PROFILE_START(i);

                memset(&key, 0, sizeof(key));

                if (use_numeric_key) {
                    key.size=sizeof(unsigned long);
                    key.data=&numeric_key;
                }
                else {
                    key.size=strlen(keytok);
                    key.data=keytok;
                }

                config.retval[i]=ham_erase(config.hamdb, &key, 0);
                PROFILE_STOP(i);
                VERBOSE2("erasing from backend %d (hamster): status %d", 
                        i, (ham_status_t)config.retval[i]);
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
    /* 
     * check integrity
     */
    if (config.check==1) {
        if (config.backend[0]==BACKEND_HAMSTER ||
            config.backend[1]==BACKEND_HAMSTER) {
            ham_status_t st=0;
            st=ham_check_integrity(config.hamdb);
            if (config.dump>=1) 
                (void)ham_dump(config.hamdb, my_dump_func);
            ham_assert(st==0, "check integrity failed", 0);
        }
    }

    /*
     * check database contents
     */
    if (!my_compare_databases()) 
        ham_assert("failed to compare the databases, or databases not equal", 
                0, 0);
    return HAM_TRUE;
}

static ham_bool_t
my_execute(char *line)
{
    unsigned pos=0;
    char *tok=my_get_token(line, &pos);
    if (!tok || tok[0]==0)
        return 1;
    VERBOSE2("reading token '%s'", tok);
    if (strstr(tok, "--"))
        return 1;
    if (!strcasecmp(tok, "CREATE"))
        return my_execute_create(&line[pos]);
    if (!strcasecmp(tok, "INSERT"))
        return my_execute_insert(&line[pos]);
    if (!strcasecmp(tok, "ERASE"))
        return my_execute_erase(&line[pos]);
    if (!strcasecmp(tok, "FULLCHECK"))
        return my_execute_fullcheck(&line[pos]);
    if (!strcasecmp(tok, "CLOSE"))
        return my_execute_close();
    ham_trace("line %d: invalid token '%s'", config.cur_line, tok);
    return HAM_FALSE;
}

void 
test_db(void)
{
    FILE *f;
    unsigned opt;
    char *param;
    char line[1024*1024];

    /*
     * initialize configuration with sane default values
     */
    memset(&config, 0, sizeof(config));
    config.verbose=1;
    config.check=1;
    config.backend[0]=BACKEND_HAMSTER;
    config.backend[1]=BACKEND_BERK;

    /*
     * parse command line parameters
     */
    getopts_init(argc-1, argv+1, "test db");

    while ((opt=getopts(&opts[0], &param))) {
        if (opt==ARG_HELP) {
            getopts_usage(&opts[0]);
            return;
        }
        else if (opt==ARG_PROFILE) {
            config.profile++;
        }
        else if (opt==ARG_CHECK) {
            config.check++;
        }
        else if (opt==ARG_QUIET) {
            config.quiet++;
        }
        else if (opt==ARG_VERBOSE) {
            config.verbose++;
        }
        else if (opt==ARG_BACKEND1) {
            if (!strcmp(param, "berk")) 
                config.backend[0]=BACKEND_BERK;
            else if (!strcmp(param, "hamster")) 
                config.backend[0]=BACKEND_HAMSTER;
            else if (!strcmp(param, "none")) 
                config.backend[0]=BACKEND_NONE;
            else 
                ham_trace("backend 1: unknown backend %s", param);
        }
        else if (opt==ARG_BACKEND2) {
            if (!strcmp(param, "berk")) 
                config.backend[1]=BACKEND_BERK;
            else if (!strcmp(param, "hamster")) 
                config.backend[1]=BACKEND_HAMSTER;
            else if (!strcmp(param, "none")) 
                config.backend[1]=BACKEND_NONE;
            else 
                ham_trace("backend 2: unknown backend %s", param);
        }
        else if (opt==ARG_INPUT) 
            config.filename=param;
        else if (opt==ARG_DUMP) 
            config.dump++;
        else if (opt==GETOPTS_UNKNOWN) {
            ham_trace("unknown parameter %s", param);
            return;
        }
        else if (opt==GETOPTS_PARAMETER) 
            config.filename=param;
        else {
            ham_trace("unknown parameter %d", opt);
            return;
        }
    }

    /*
     * open the file
     */
    if (!config.filename)
        f=stdin;
    else {
        f=fopen(config.filename, "rt");
        if (!f) {
            ham_trace("cannot open %s: %s", config.filename, 
                    strerror(errno));
            return;
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

        if (!my_execute(line))
            break;

        if (config.check>=2) {
            if ((config.backend[0]==BACKEND_HAMSTER ||
                config.backend[1]==BACKEND_HAMSTER) && config.hamdb)
                ham_assert(ham_check_integrity(config.hamdb)==0, 0, 0);
        }

        VERBOSE2("---- line %04d ----", config.cur_line);
    }

    if (config.filename)
        fclose(f);

    if (config.profile) {
        float f;
        f=config.prof[0];
        f/=1000.f;
        printf("\nprofile of backend %s:\t%f sec\n", 
                my_get_profile_name(0), f);
        f=config.prof[1];
        f/=1000.f;
        printf("\nprofile of backend %s:\t%f sec\n", 
                my_get_profile_name(1), f);
    }
}
