/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

