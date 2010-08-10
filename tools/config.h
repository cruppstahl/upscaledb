/*
 * Copyright (C) 2005-2009 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#ifndef CONFIG_H__
#define CONFIG_H__

#ifdef __cplusplus
extern "C" {
#endif 

#include <ham/hamsterdb.h>

typedef struct config_table_t
{
    unsigned int state;
    unsigned int cur_env;
    unsigned int cur_db;
    char *key;

    struct config_global_t {
        unsigned int port;
        unsigned int enable_error_log;
        char *error_log;
        unsigned int enable_access_log;
        char *access_log;
    } globals;

    struct config_env_t {
        char *url;
        char *path;
        char *flags;
        unsigned int open_exclusive;
        ham_env_t *env;

        struct config_db_t {
            unsigned int name;
            char *flags;
        } *dbs;
        unsigned int db_count;
    } *envs;
    unsigned int env_count;
} config_table_t;


/*
 * read a json string and return config_table_t structure
 *
 * returns HAM_INV_PARAMETER if the string is badly formatted
 */
extern ham_status_t
config_parse_string(const char *string, config_table_t **params);

/*
 * releases the memory allocated by the parameter table
 */
extern void
config_clear_table(config_table_t *params);


#ifdef __cplusplus
} // extern "C"
#endif 

#endif /* CONFIG_H__ */

