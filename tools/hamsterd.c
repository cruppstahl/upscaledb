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
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <signal.h>
#include <sys/signal.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <errno.h>

#include <ham/hamsterdb.h>
#include <ham/hamserver.h>

#include "getopts.h"
#include "config.h"

#define ARG_HELP            1
#define ARG_FOREGROUND      2
#define ARG_CONFIG          3
#define ARG_PIDFILE         4

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
        ARG_FOREGROUND,
        "f",
        "foreground",
        "run in foreground",
        0 },
    {
        ARG_CONFIG,
        "c",
        "config",
        "specify config file",
        GETOPTS_NEED_ARGUMENT },
    {
        ARG_PIDFILE,
        "p",
        "pid",
        "store pid in file",
        GETOPTS_NEED_ARGUMENT },
    { 0, 0, 0, 0, 0 } /* terminating element */
};

static int running = 1;

static void 
signal_handler(int sig)
{
    (void)sig;
    running = 0;
}

static void
daemonize(void)
{
    int fd;

    switch(fork()) {
    case 0:  /* i'm the child */
        break;
    case -1:
        printf("fork failed: %s\n", strerror(errno));
        break;
    default: /* i'm the parent */
        exit(0);
    }

    /* go to root directory */
    chdir("/");

    /* reset umask */
    umask(0);

    /* disassociate from process group */
    setpgrp();

    /* disassociate from control terminal */
    if ((fd=open("dev/tty", O_RDWR)) >= 0) {
        ioctl(fd, TIOCNOTTY, NULL);
        close(fd);
    }
}

void
read_config(const char *configfile, config_table_t **params)
{
    ham_status_t st;
    char *buf;
    FILE *fp;
    long len;

    /* read the whole file into 'buf' */
    fp=fopen(configfile, "rt");
    if (!fp) {
        printf("failed to open config file: %s\n", strerror(errno));
        exit(-1);
    }
    fseek(fp, 0, SEEK_END);
    len=ftell(fp);
    fseek(fp, 0, SEEK_SET);
    buf=(char *)malloc(len+1); /* for zero-terminating byte */
    fread(buf, len, 1, fp);
    fclose(fp);
    buf[len]='\0';

    /* parse the file */
    st=config_parse_string(buf, params);
    if (st) {
        printf("failed to read configuration file: %s\n", ham_strerror(st));
        exit(-1);
    }

    /* clean up */
    free(buf);
}

void
write_pidfile(const char *pidfile)
{
    FILE *fp=fopen(pidfile, "wt");
    if (!fp) {
        printf("failed to write pidfile: %s\n", strerror(errno));
        exit(-1);
    }
    fprintf(fp, "%u", (unsigned)getpid());
    fclose(fp);
}

#define COMPARE_FLAG(n)             if (!strcmp(#n, p)) f|=n


ham_u32_t
format_flags(char *flagstr)
{
    ham_u32_t f=0;
    char *saveptr=0;
    char *p;

    if (!flagstr || flagstr[0]=='\0')
        return (0);

    p=strtok_r(flagstr, "|", &saveptr);
    while (p) {
        COMPARE_FLAG(HAM_WRITE_THROUGH);
        else COMPARE_FLAG(HAM_IN_MEMORY_DB);
        else COMPARE_FLAG(HAM_DISABLE_MMAP);
        else COMPARE_FLAG(HAM_CACHE_STRICT);
        else COMPARE_FLAG(HAM_CACHE_UNLIMITED);
        else COMPARE_FLAG(HAM_DISABLE_FREELIST_FLUSH);
        else COMPARE_FLAG(HAM_LOCK_EXCLUSIVE);
        else COMPARE_FLAG(HAM_ENABLE_RECOVERY);
        else COMPARE_FLAG(HAM_ENABLE_TRANSACTIONS);
        else COMPARE_FLAG(HAM_READ_ONLY);
        else COMPARE_FLAG(HAM_USE_BTREE);
        else COMPARE_FLAG(HAM_DISABLE_VAR_KEYLEN);
        else COMPARE_FLAG(HAM_ENABLE_DUPLICATES);
        else COMPARE_FLAG(HAM_SORT_DUPLICATES);
        else COMPARE_FLAG(HAM_RECORD_NUMBER);
        else {
            printf("ignoring unknown flag %s\n", p);
        }
        p=strtok_r(0, "|", &saveptr);
    }

    return (f);
}

void
initialize_server(hamserver_t *srv, config_table_t *params)
{
    unsigned e, d;
    ham_env_t *env;
    ham_status_t st;

    for (e=0; e<params->env_count; e++) {
        ham_u32_t flags=format_flags(params->envs[e].flags);
        ham_bool_t created_env=HAM_FALSE;

        ham_env_new(&env);

        /* First try to open the Environment */
        st=ham_env_open(env, params->envs[e].path, flags);
        if (st) {
            /* Not found? if open_exclusive is false then we create the
             * Environment */
            if (st==HAM_FILE_NOT_FOUND && !params->envs[e].open_exclusive) {
                st=ham_env_create(env, params->envs[e].path, flags, 0644);
                if (st) {
                    printf("ham_env_create failed: %s\n", ham_strerror(st));
                    exit(-1);
                }
                created_env=1;
            }
            else {
                printf("ham_env_open failed: %s\n", ham_strerror(st));
                exit(-1);
            }
        }

        /* Now create each of the Databases if the Environment was
         * created */
        if (created_env) {
            ham_db_t *db;
    
            for (d=0; d<params->envs[e].db_count; d++) {
                ham_u32_t flags=format_flags(params->envs[e].dbs[d].flags);

                ham_new(&db);

                st=ham_env_create_db(env, db, params->envs[e].dbs[d].name, 
                                    flags, 0);
                if (st) {
                    printf("ham_env_create_db: %d\n", st);
                    exit(-1);
                }

                ham_close(db, 0);
                ham_delete(db);
            }
        }

        /* Add the Environment to the server */
        st=hamserver_add_env(srv, env, params->envs[e].url);
        if (st) {
            printf("hamserver_add_env failed: %s\n", ham_strerror(st));
            exit(-1);
        }

        /* Store env in configuration object */
        params->envs[e].env=env;
    }
}

int
main(int argc, char **argv)
{
    unsigned opt;
    char *param, *configfile=0, *pidfile=0;
    unsigned e, foreground=0;
    hamserver_t *srv;
    hamserver_config_t cfg;
    config_table_t *params=0;

    ham_u32_t maj, min, rev;
    const char *licensee, *product;
    ham_get_license(&licensee, &product);
    ham_get_version(&maj, &min, &rev);

    getopts_init(argc, argv, "hamsterd");

    while ((opt=getopts(&opts[0], &param))) {
        switch (opt) {
            case ARG_FOREGROUND:
                foreground=1;
                break;
            case ARG_CONFIG:
                configfile=param;
                break;
            case ARG_PIDFILE:
                pidfile=param;
                break;
            case ARG_HELP:
                printf("hamsterdb server %d.%d.%d - Copyright (C) 2005-2010 "
                       "Christoph Rupp (chris@crupp.de).\n\n",
                       maj, min, rev);

                if (licensee[0]=='\0')
                    printf(
                       "This program is free software; you can redistribute "
                       "it and/or modify it\nunder the terms of the GNU "
                       "General Public License as published by the Free\n"
                       "Software Foundation; either version 2 of the License,\n"
                       "or (at your option) any later version.\n\n"
                       "See file COPYING.GPL2 and COPYING.GPL3 for License "
                       "information.\n\n");
                else
                    printf("Commercial version; licensed for %s (%s)\n\n",
                            licensee, product);

                printf("usage: hamsterd [-f] -c configfile\n");
                printf("usage: hamsterd -h\n");
                printf("       -h:         this help screen (alias: --help)\n");
                printf("       -f:         run in foreground\n");
                printf("       configfile: path of configuration file\n");
                return (0);
            default:
                printf("Invalid or unknown parameter `%s'. "
                       "Enter `hamsterd --help' for usage.", param);
                return (-1);
        }
    }

    /* read and parse the configuration file */
    if (configfile)
        read_config(configfile, &params);
    else {
        printf("configuration file missing - please specify path with -c\n");
        printf("run ./hamsterd --help for more information.\n");
        exit(-1);
    }

    printf("hamsterd is starting...\n");

    /* register signals; these are the signals that will terminate the daemon */
    signal(SIGHUP, signal_handler);
    signal(SIGINT, signal_handler);
    signal(SIGQUIT, signal_handler);
    signal(SIGABRT, signal_handler);
    signal(SIGKILL, signal_handler);
    signal(SIGTERM, signal_handler);

    memset(&cfg, 0, sizeof(cfg));
    cfg.port=params->globals.port;
    cfg.access_log_path=params->globals.access_log;
    cfg.error_log_path=params->globals.error_log;
    if ((0!=hamserver_init(&cfg, &srv)))
        exit(-1);

    initialize_server(srv, params);

    if (!foreground)
        daemonize();

    if (pidfile)
        write_pidfile(pidfile);

    while (running)
        sleep(1);

    printf("hamsterd is stopping...\n");

    /* clean up */
    hamserver_close(srv);
    for (e=0; e<params->env_count; e++) {
        (void)ham_env_close(params->envs[e].env, HAM_AUTO_CLEANUP);
    }
    config_clear_table(params);

    return (0);
}
