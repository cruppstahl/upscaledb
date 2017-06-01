/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

#include <JSON_parser.h>
#include <stdio.h>
#include <string.h>
#ifdef HAVE_MALLOC_H
#  include <malloc.h>
#else
#  include <stdlib.h>
#endif

#include "config.h"

#define STATE_NONE          0
#define STATE_GLOBAL        1
#define STATE_ENVIRONMENTS  2
#define STATE_DATABASES     3

extern void hlog(int level, const char *format, ...);

static int
__parser_cb(void *ctx, int type, const struct JSON_value_struct *value) {
  config_table_t *p = (config_table_t *)ctx;

  switch (p->state) {
    case STATE_NONE: {
      if (type == JSON_T_OBJECT_BEGIN)
        break;
      if (type == JSON_T_KEY) {
        if (!strcmp("global", value->vu.str.value)) {
          p->state = STATE_GLOBAL;
          break;
        }
        if (!strcmp("environments", value->vu.str.value)) {
          p->state = STATE_ENVIRONMENTS;
          break;
        }
      }
      /* everything else: fail */
      return (0);
    }

    case STATE_GLOBAL: {
      if (type == JSON_T_OBJECT_BEGIN)
        break;
      if (type == JSON_T_OBJECT_END) {
        p->state = STATE_NONE;
        break;
      }
      if (type == JSON_T_KEY) {
        if (p->key)
          free(p->key);
        p->key = strdup(value->vu.str.value);
        break;
      }
      if (type == JSON_T_INTEGER) {
        if (!strcmp("port", p->key)) {
          p->globals.port = value->vu.integer_value;
          break;
        }
      }
      if (type == JSON_T_STRING) {
        if (!strcmp("error-log", p->key)) {
          p->globals.error_log = strdup(value->vu.str.value);
          break;
        }
        if (!strcmp("access-log", p->key)) {
          p->globals.access_log = strdup(value->vu.str.value);
          break;
        }
      }
      if (type == JSON_T_TRUE) {
        if (!strcmp("enable-error-log", p->key)) {
          p->globals.enable_error_log = 1;
          break;
        }
        if (!strcmp("enable-access-log", p->key)) {
          p->globals.enable_access_log = 1;
          break;
        }
      }
      if (type == JSON_T_FALSE) {
        if (!strcmp("enable-error-log", p->key)) {
          p->globals.enable_error_log = 0;
          break;
        }
        if (!strcmp("enable-access-log", p->key)) {
          p->globals.enable_access_log = 0;
          break;
        }
      }
      /* everything else: fail */
      return (0);
    }

    case STATE_ENVIRONMENTS: {
      if (type == JSON_T_OBJECT_BEGIN) {
        p->env_count++;
        p->envs = (struct config_env_t *)realloc(p->envs,
            p->env_count * sizeof(struct config_env_t));
        memset(&p->envs[p->env_count - 1], 0, sizeof(struct config_env_t));
        p->cur_env = p->env_count;
        break;
      }
      if (type == JSON_T_KEY) {
        if (p->key)
          free(p->key);
        p->key = strdup(value->vu.str.value);
        break;
      }
      if (type == JSON_T_STRING) {
        if (!strcmp("url", p->key)) {
          p->envs[p->cur_env - 1].url = strdup(value->vu.str.value);
          break;
        }
        if (!strcmp("path", p->key)) {
          p->envs[p->cur_env - 1].path = strdup(value->vu.str.value);
          break;
        }
        if (!strcmp("flags", p->key)) {
          p->envs[p->cur_env - 1].flags = strdup(value->vu.str.value);
          break;
        }
      }
      if (type == JSON_T_TRUE) {
        if (!strcmp("open-exclusive", p->key)) {
          p->envs[p->cur_env - 1].open_exclusive = 1;
          break;
        }
      }
      if (type == JSON_T_FALSE) {
        if (!strcmp("open-exclusive", p->key)) {
          p->envs[p->cur_env - 1].open_exclusive = 0;
          break;
        }
      }
      if (type == JSON_T_ARRAY_BEGIN) {
        if (!strcmp("databases", p->key)) {
          p->state = STATE_DATABASES;
          break;
        }
      }
      /* everything else: fail */
      break;
    }
    case STATE_DATABASES: {
      if (type == JSON_T_OBJECT_BEGIN) {
        struct config_env_t *e = &p->envs[p->cur_env - 1];
        e->db_count++;
        e->dbs = (struct config_db_t *)realloc(e->dbs,
            e->db_count * sizeof(struct config_db_t));
        memset(&e->dbs[e->db_count - 1], 0, sizeof(struct config_db_t));
        p->cur_db = e->db_count;
        break;
      }
      if (type == JSON_T_KEY) {
        if (p->key)
          free(p->key);
        p->key = strdup(value->vu.str.value);
        break;
      }
      if (type == JSON_T_STRING) {
        if (!strcmp("flags", p->key)) {
          struct config_env_t *e = &p->envs[p->cur_env - 1];
          e->dbs[p->cur_db - 1].flags = strdup(value->vu.str.value);
          break;
        }
      }
      if (type == JSON_T_INTEGER) {
        if (!strcmp("name", p->key)) {
          struct config_env_t *e = &p->envs[p->cur_env - 1];
          e->dbs[p->cur_db - 1].name = value->vu.integer_value;
          break;
        }
      }
      /* everything else: fail */
      break;
    }
  }

  return (1);
}

ups_status_t
config_parse_string(const char *string, config_table_t **params) {
  unsigned count = 0;
  JSON_config config;
  struct JSON_parser_struct *jc = 0;
  config_table_t *p = (config_table_t *)calloc(sizeof(config_table_t), 1);
  if (!p)
    return (UPS_OUT_OF_MEMORY);

  *params = 0;

  init_JSON_config(&config);
  config.depth = 20;
  config.callback=__parser_cb;
  config.callback_ctx = (void *)p;
  config.allow_comments = 1;
  config.handle_floats_manually = 1;

  jc = new_JSON_parser(&config);
  while (*string) {
    if (!JSON_parser_char(jc, *string)) {
      delete_JSON_parser(jc);
      hlog(3, "JSON syntax error in byte %u\n", count);
      config_clear_table(p);
      return (UPS_INV_PARAMETER);
    }
    count++;
    string++;
  }

  if (!JSON_parser_done(jc)) {
    delete_JSON_parser(jc);
    config_clear_table(p);
    return (UPS_INV_PARAMETER);
  }

  *params = p;
  delete_JSON_parser(jc);

  return (0);
}

void
config_clear_table(config_table_t *params) {
  unsigned int e, d;

  if (!params)
    return;
  if (params->key)
    free(params->key);
  if (params->globals.error_log)
    free(params->globals.error_log);
  if (params->globals.access_log)
    free(params->globals.access_log);

  for (e = 0; e < params->env_count; e++) {
    if (params->envs[e].url)
      free(params->envs[e].url);
    if (params->envs[e].path)
      free(params->envs[e].path);
    if (params->envs[e].flags)
      free(params->envs[e].flags);

    for (d = 0; d < params->envs[e].db_count; d++) {
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
