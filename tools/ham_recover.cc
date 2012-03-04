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

#include <ham/hamsterdb.h>

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

int
main(int argc, char **argv)
{
    unsigned opt;
    char *param, *filename=0;

    ham_status_t st;
    ham_env_t *env;

    ham_u32_t maj, min, rev;
    const char *licensee, *product;
    ham_get_license(&licensee, &product);
    ham_get_version(&maj, &min, &rev);

    getopts_init(argc, argv, "ham_recover");

    while ((opt=getopts(&opts[0], &param))) {
        switch (opt) {
            case GETOPTS_PARAMETER:
                if (filename) {
                    printf("Multiple files specified. Please specify "
                           "only one filename.\n");
                    return (-1);
                }
                filename=param;
                break;
            case ARG_HELP:
                printf("hamsterdb %d.%d.%d - Copyright (C) 2005-20011"
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

                printf("usage: ham_recover file\n");
                printf("usage: ham_recover -h\n");
                printf("       -h:         this help screen (alias: --help)\n");
                return (0);
            default:
                printf("Invalid or unknown parameter `%s'. "
                       "Enter `ham_dump --help' for usage.", param);
                return (-1);
        }
    }

    if (!filename) {
        printf("Filename is missing. Enter `ham_recover --help' for usage.\n");
        return (-1);
    }

    /*
     * open the environment and check if recovery is required
     */
    st=ham_env_new(&env);
    if (st!=HAM_SUCCESS)
        error("ham_env_new", st);
    st=ham_env_open_ex(env, filename,
                HAM_ENABLE_RECOVERY|HAM_ENABLE_TRANSACTIONS, 0);
    if (st==HAM_FILE_NOT_FOUND) {
        printf("File `%s' not found or unable to open it\n", filename);
        return (-1);
    }
    if (st==0) {
        printf("File `%s' does not need to be recovered\n", filename);
        return (0);
    }
    else if (st!=HAM_NEED_RECOVERY)
        error("ham_env_open_ex", st);

    /* now start the recovery */
    st=ham_env_open_ex(env, filename, HAM_AUTO_RECOVERY, 0);
    if (st)
        error("ham_env_open_ex", st);

    /*
     * we're already done
     */
    st=ham_env_close(env, 0);
    if (st!=HAM_SUCCESS)
        error("ham_env_close", st);

    ham_env_delete(env);

    return (0);
}
