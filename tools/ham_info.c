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
#include "getopts.h"

#define ARG_HELP            1
#define ARG_DBNAME          2
#define ARG_FULL            3

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
        ARG_DBNAME,
        "db",
        "dbname",
        "only print info about this database",
        GETOPTS_NEED_ARGUMENT },
    {
        ARG_FULL,
        "f",
        "full",
        "print full information",
        0 },
    { 0, 0, 0, 0, 0 } /* terminating element */
};

int
main(int argc, char **argv)
{
    unsigned opt;
    char *param, *filename=0, *endptr=0;
    short dbname=0xffff;
    int full=0;

    getopts_init(argc, argv, "ham_info");

    while ((opt=getopts(&opts[0], &param))) {
        switch (opt) {
            case ARG_DBNAME:
                if (!param) {
                    printf("Parameter `dbname' is missing.\n");
                    return (-1);
                }
                dbname=(short)strtoul(param, &endptr, 0);
                if (endptr && *endptr) {
                    printf("Invalid parameter `dbname'; numerical value "
                           "expected.\n");
                    return (-1);
                }
                break;
            case ARG_FULL:
                full=1;
                break;
            case GETOPTS_PARAMETER:
                filename=param;
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
                       "Enter `ham_info --help' for usage.", param);
                return (-1);
        }
    }

    if (!filename) {
        printf("Filename is missing. Enter `ham_info --help' for usage.\n");
        return (-1);
    }

    return (0);
}
