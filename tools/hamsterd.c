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
#include "json.h"

#define ARG_HELP            1
#define ARG_FOREGROUND      2
#define ARG_CONFIG          3

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
read_config(const char *configfile)
{
    ham_status_t st;
    char *buf;
    param_table_t *params=0;
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
    st=json_parse_string(buf, &params);
    if (st) {
        printf("failed to read configuration file: %s\n", ham_strerror(st));
        exit(-1);
    }

    /* clean up */
    json_clear_table(params);
    free(buf);
}

int
main(int argc, char **argv)
{
    unsigned opt;
    char *param, *configfile=0;
    unsigned foreground=0;

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

    printf("hamsterd is starting...\n");

    /* read and parse the configuration file */
    if (configfile)
        read_config(configfile);

    signal(SIGTERM, signal_handler);

    if (!foreground)
        daemonize();

    while (running)
        sleep(1);

    printf("hamsterd is stopping...\n");

    return (0);
}
