/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
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

