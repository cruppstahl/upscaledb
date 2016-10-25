/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
 * All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * See the file COPYING for License information.
 */

#ifndef UPS_TOOLS_CONFIG_H
#define UPS_TOOLS_CONFIG_H

#ifdef __cplusplus
extern "C" {
#endif

#include <ups/upscaledb.h>

typedef struct config_table_t {
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
    ups_env_t *env;

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
 * returns UPS_INV_PARAMETER if the string is badly formatted
 */
extern ups_status_t
config_parse_string(const char *string, config_table_t **params);

/*
 * releases the memory allocated by the parameter table
 */
extern void
config_clear_table(config_table_t *params);

#ifdef __cplusplus
} // extern "C"
#endif

#endif /* UPS_TOOLS_CONFIG_H */

