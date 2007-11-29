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
#include <errno.h>

#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/backend.h"
#include "../src/btree.h"

#include "getopts.h"

#define ARG_HELP            1

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
    { 0, 0, 0, 0, 0 } /* terminating element */
};

static void 
error(const char *foo, ham_status_t st)
{
    printf("%s() returned error %d: %s\n", foo, st, ham_strerror(st));
    exit(-1);
}

static void
recover_env(const char *source, const char *destination)
{
    ham_env_t *denv;
    ham_status_t st;
    FILE *f;
    ham_u8_t hdrbuf[512];
    int r;
    db_header_t *hdr;
    ham_u32_t pagesize, max_dbs;
    ham_parameter_t params[3];

    /*
     * open the source file
     */
    f=fopen(source, "rb");
    if (!f) {
        printf("failed to open source file %s: %s\n", source, strerror(errno));
        return;
    }

    /*
     * read the header page
     */
    r=fread(hdrbuf, 1, sizeof(hdrbuf), f);
    if (r!=sizeof(hdrbuf)) {
        printf("failed to read source header: %s\n", strerror(errno));
        return;
    }

    /*
     * now get a pointer to a headerpage structure 
     */
    hdr=(db_header_t *)hdrbuf;
    pagesize=ham_db2h32(hdr->_pagesize);
    max_dbs =ham_db2h32(hdr->_max_databases);

    params[0].name =HAM_PARAM_PAGESIZE;
    params[1].value=pagesize;
    params[1].name =HAM_PARAM_MAX_ENV_DATABASES;
    params[1].value=max_dbs;
    params[2].name =0;
    params[2].value=0;

    /*
     * create a new environment with the same pagesize as the original
     */
    st=ham_env_new(&denv);
    if (st)
        error("ham_env_new", st);
    st=ham_env_create_ex(denv, destination, 0, 644, params);
    if (st)
        error("ham_env_create", st);

#define db_get_indexdata(db)     (&((ham_u8_t *)page_get_payload(             \
                                        db_get_header_page(db))+              \
                                          sizeof(db_header_t))[0])

    /*
     * clean up
     */
    st=ham_env_close(denv, 0);
    if (st)
        error("ham_env_close", st);
    ham_env_delete(denv);
    fclose(f);
}

static void
recover_database(ham_db_t *db, ham_u16_t dbname, int full)
{
}

int
main(int argc, char **argv)
{
    unsigned opt;
    char *param, *source=0, *destination=0;

    getopts_init(argc, argv, "ham_recover");

    while ((opt=getopts(&opts[0], &param))) {
        switch (opt) {
            case GETOPTS_PARAMETER:
                if (!source)
                    source=param;
                else if (!destination)
                    destination=param;
                else {
                    printf("Multiple files specified. Please specify "
                           "only two filenames.\n");
                    return (-1);
                }
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
                       "Enter `ham_recover --help' for usage.", param);
                return (-1);
        }
    }

    if (!source || !destination) {
        printf("Filename is missing. Enter `ham_recover --help' for usage.\n");
        return (-1);
    }

    /*
     * start the recovery process
     */
    recover_env(source, destination);

    return (0);
}
