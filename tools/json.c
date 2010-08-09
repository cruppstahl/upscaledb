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

#include <JSON_parser.h>
#include <stdio.h>
#include <string.h>
#include <malloc.h>
#include "json.h"
#include "error.h"
#include "util.h"

#define STATE_NONE            0
#define STATE_GLOBAL          1
#define STATE_ENVIRONMENTS    2
#define STATE_DATABASES       3

static int
__parser_cb(void *ctx, int type, const struct JSON_value_struct *value)
{
    param_table_t *p=(param_table_t *)ctx;

    printf("state: %d  ", p->state);

    switch (p->state) {
        case STATE_NONE: {
            if (type==JSON_T_OBJECT_BEGIN)
                break;
            if (type==JSON_T_KEY) {
                printf("key %s\n", value->vu.str.value);
                if (!strcmp("global", value->vu.str.value)) {
                    p->state=STATE_GLOBAL;
                    break;
                }
                if (!strcmp("environments", value->vu.str.value)) {
                    p->state=STATE_ENVIRONMENTS;
                    break;
                }
            }
            /* everything else: fail */
            return (0);
        }

        case STATE_GLOBAL: {
            if (type==JSON_T_OBJECT_BEGIN)
                break;
            if (type==JSON_T_OBJECT_END) {
                p->state=STATE_NONE;
                break;
            }
            if (type==JSON_T_KEY) {
                if (p->key)
                    free(p->key);
                p->key=strdup(value->vu.str.value);
                break;
            }
            if (type==JSON_T_INTEGER) {
                printf("%s: %u\n", p->key, (unsigned)value->vu.integer_value);
                break;
            }
            if (type==JSON_T_STRING) {
                printf("%s: %s\n", p->key, value->vu.str.value);
                break;
            }
            if (type==JSON_T_TRUE) {
                printf("%s: true\n", p->key);
                break;
            }
            if (type==JSON_T_FALSE) {
                printf("%s: false\n", p->key);
                break;
            }
            /* everything else: fail */
            return (0);
        }

        case STATE_ENVIRONMENTS: {
            if (type==JSON_T_OBJECT_BEGIN) {
                p->env_count++;
                p->envs=(struct param_env_t *)realloc(p->envs, 
                        p->env_count*sizeof(struct param_env_t));
                memset(&p->envs[p->env_count-1], 0, sizeof(struct param_env_t));
                p->cur_env=p->env_count;
                break;
            }
            if (type==JSON_T_KEY) {
                if (p->key)
                    free(p->key);
                p->key=strdup(value->vu.str.value);
                break;
            }
            if (type==JSON_T_STRING) {
                printf("%s: %s\n", p->key, value->vu.str.value);
                break;
            }
            if (type==JSON_T_TRUE) {
                printf("%s: true\n", p->key);
                break;
            }
            if (type==JSON_T_FALSE) {
                printf("%s: false\n", p->key);
                break;
            }
            if (type==JSON_T_ARRAY_BEGIN) {
                printf("array begin\n");
                if (!strcmp("databases", p->key)) {
                    p->state=STATE_DATABASES;
                }
                break;
            }
            /* everything else: fail */
            break;
        }
        case STATE_DATABASES: {
            if (type==JSON_T_OBJECT_BEGIN) {
                struct param_env_t *e=&p->envs[p->cur_env-1];
                e->db_count++;
                e->dbs=(struct param_db_t *)realloc(e->dbs, 
                        e->db_count*sizeof(struct param_db_t));
                memset(&e->dbs[e->db_count-1], 0, sizeof(struct param_db_t));
                p->cur_db=e->db_count;
                break;
            }
            if (type==JSON_T_KEY) {
                if (p->key)
                    free(p->key);
                p->key=strdup(value->vu.str.value);
                break;
            }
            if (type==JSON_T_STRING) {
                printf("%s: %s\n", p->key, value->vu.str.value);
                break;
            }
            if (type==JSON_T_INTEGER) {
                printf("%s: %u\n", p->key, (unsigned)value->vu.integer_value);
                break;
            }
            /* everything else: fail */
            break;
        }
    }

    return (1);
}

ham_status_t
json_parse_string(const char *string, param_table_t **params)
{
    unsigned count=0;
    JSON_config config;
    struct JSON_parser_struct *jc=0;
    param_table_t *p=(param_table_t *)calloc(sizeof(param_table_t), 1);
    if (!p)
        return (HAM_OUT_OF_MEMORY);

    *params=0;

    init_JSON_config(&config);
    config.depth=20;
    config.callback=__parser_cb;
    config.callback_ctx=(void *)p;
    config.allow_comments=1;
    config.handle_floats_manually=1;

    jc=new_JSON_parser(&config);
    while (*string) {
        if (!JSON_parser_char(jc, *string)) {
            delete_JSON_parser(jc);
            ham_log(("JSON syntax error in byte %u: %s", count, string));
            json_clear_table(p);
            return (HAM_INV_PARAMETER);
        }
        count++;
        string++;
    }

    if (!JSON_parser_done(jc)) {
        delete_JSON_parser(jc);
        ham_log(("JSON syntax error"));
        json_clear_table(p);
        return (HAM_INV_PARAMETER);
    }

    *params=p;
    delete_JSON_parser(jc);

    return (0);
}

void
json_clear_table(param_table_t *params)
{
    unsigned int e, d;

    if (!params)
        return;
    if (params->key)
        free(params->key);
    if (params->globals.error_log)
        free(params->globals.error_log);
    if (params->globals.access_log)
        free(params->globals.access_log);

    for (e=0; e<params->env_count; e++) {
        if (params->envs[e].url)
            free(params->envs[e].url);
        if (params->envs[e].path)
            free(params->envs[e].path);
        if (params->envs[e].flags)
            free(params->envs[e].flags);

        for (d=0; d<params->envs[e].db_count; d++) {
            if (params->envs[e].dbs[d].flags)
                free(params->envs[e].dbs[d].flags);
        }
        if (params->envs[e].dbs)
            free(params->envs[e].dbs);
    }
    if (params->envs)
        free(params->envs);

    free(params);
}
