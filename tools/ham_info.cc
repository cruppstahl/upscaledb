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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <ham/hamsterdb_int.h>

#include "getopts.h"
#include "common.h"

#define ARG_HELP        1
#define ARG_DBNAME      2
#define ARG_FULL        3
#define ARG_QUIET       4

static bool quiet = false;

/*
 * command line parameters
 */
static option_t opts[] = {
  {
    ARG_HELP,         // symbolic name of this option
    "h",          // short option
    "help",         // long option
    "this help screen",   // help string
    0 },          // no flags
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
  {
    ARG_QUIET,
    "q",
    "quiet",
    "do not print information",
    0 },
  { 0, 0, 0, 0, 0 } /* terminating element */
};

static void
error(const char *foo, ham_status_t st) {
  printf("%s() returned error %d: %s\n", foo, st, ham_strerror(st));
  exit(-1);
}

static const char *
get_compressor_name(int library) {
  switch (library) {
    case HAM_COMPRESSOR_ZLIB:
      return ("zlib");
    case HAM_COMPRESSOR_SNAPPY:
      return ("snappy");
    case HAM_COMPRESSOR_LZF:
      return ("lzf");
    case HAM_COMPRESSOR_LZO:
      return ("lzo");
    case HAM_COMPRESSOR_BITMAP:
      return ("bitmap");
    default:
      return ("???");
  }
}

static void
print_environment(ham_env_t *env) {
  ham_parameter_t params[] = {
    {HAM_PARAM_PAGE_SIZE, 0},
    {HAM_PARAM_MAX_DATABASES, 0},
    {HAM_PARAM_JOURNAL_COMPRESSION, 0},
    {0, 0}
  };

  ham_status_t st = ham_env_get_parameters(env, &params[0]);
  if (st != 0)
    error("ham_env_get_parameters", st);

  if (!quiet) {
    ham_u32_t v1, v2, v3;
    ham_get_version(&v1, &v2, &v3);

    printf("environment\n");
    printf("  page_size:            %u\n", (unsigned)params[0].value);
    printf("  version:              %u.%u.%u %s\n",
            v1, v2, v3, ham_is_pro() ? "pro!" : "");
    printf("  max databases:        %u\n", (unsigned)params[1].value);
    if (params[2].value)
      printf("  journal compression:  %s\n",
                      get_compressor_name((int)params[2].value));
  }
}

static void
print_database(ham_db_t *db, ham_u16_t dbname, int full) {
  ham_cursor_t *cursor;
  ham_key_t key = {0};
  ham_record_t rec = {0};

  // get the database information
  ham_parameter_t params[] = {
    {HAM_PARAM_KEY_TYPE, 0},
    {HAM_PARAM_KEY_SIZE, 0},
    {HAM_PARAM_RECORD_SIZE, 0},
    {HAM_PARAM_MAX_KEYS_PER_PAGE, 0},
    {HAM_PARAM_FLAGS, 0},
    {HAM_PARAM_RECORD_COMPRESSION, 0},
    {HAM_PARAM_KEY_COMPRESSION, 0},
    {0, 0}
  };

  ham_status_t st = ham_db_get_parameters(db, &params[0]);
  if (st != 0)
    error("ham_db_get_parameters", st);

  unsigned num_items = 0, min_key_size = 0xffffffff,
      max_key_size = 0, min_rec_size = 0xffffffff, max_rec_size = 0,
      total_key_size = 0, total_rec_size = 0, extended_keys = 0;

  if (!quiet) {
    const char *key_type = 0;
    switch (params[0].value) {
      case HAM_TYPE_UINT8:
        key_type = "HAM_TYPE_UINT8";
        break;
      case HAM_TYPE_UINT16:
        key_type = "HAM_TYPE_UINT16";
        break;
      case HAM_TYPE_UINT32:
        key_type = "HAM_TYPE_UINT32";
        break;
      case HAM_TYPE_UINT64:
        key_type = "HAM_TYPE_UINT64";
        break;
      case HAM_TYPE_REAL32:
        key_type = "HAM_TYPE_REAL32";
        break;
      case HAM_TYPE_REAL64:
        key_type = "HAM_TYPE_REAL64";
        break;
      case HAM_TYPE_CUSTOM:
        key_type = "HAM_TYPE_CUSTOM";
        break;
      default:
        key_type = "HAM_TYPE_BINARY";
        break;
    }
    printf("\n");
    printf("  database %d (0x%x)\n", (int)dbname, (int)dbname);
    printf("    key type:             %s\n", key_type);
    printf("    max key size:         %u\n", (unsigned)params[1].value);
    printf("    max keys per page:    %u\n", (unsigned)params[3].value);
    printf("    flags:                0x%04x\n", (unsigned)params[4].value);
    if (params[5].value)
      printf("    record compression:   %s\n",
                      get_compressor_name((int)params[5].value));
    if (params[6].value)
      printf("    key compression:      %s\n",
                      get_compressor_name((int)params[6].value));
    if (params[2].value == HAM_RECORD_SIZE_UNLIMITED)
      printf("    record size:          unlimited\n");
    else
      printf("    record size:          %d (inline: %s)\n",
                      (unsigned)params[2].value,
                      params[4].value & HAM_FORCE_RECORDS_INLINE
                            ? "yes"
                            : "no");
  }

  if (!full)
    return;

  st = ham_cursor_create(&cursor, db, 0, 0);
  if (st != HAM_SUCCESS)
    error("ham_cursor_create", st);

  while (1) {
    st = ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT);
    if (st != HAM_SUCCESS) {
      /* reached end of the database? */
      if (st == HAM_KEY_NOT_FOUND)
        break;
      else
        error("ham_cursor_next", st);
    }

    num_items++;

    if (key.size < min_key_size)
      min_key_size = key.size;
    if (key.size > max_key_size)
      max_key_size = key.size;
    if (key.size > 256)
      extended_keys++;

    if (rec.size < min_rec_size)
      min_rec_size = rec.size;
    if (rec.size > max_rec_size)
      max_rec_size = rec.size;

    total_key_size += key.size;
    total_rec_size += rec.size;
  }

  ham_cursor_close(cursor);

  if (!quiet) {
    printf("    number of items:    %u\n", num_items);
    if (num_items == 0)
      return;
    printf("    average key size:     %u\n", total_key_size / num_items);
    printf("    minimum key size:     %u\n", min_key_size);
    printf("    maximum key size:     %u\n", max_key_size);
    printf("    total keys (bytes):   %u\n", total_key_size);
    if (extended_keys)
      printf("    extended keys   :   %u\n", extended_keys);
    printf("    average record size:  %u\n", total_rec_size / num_items);
    printf("    minimum record size:  %u\n", min_rec_size);
    printf("    maximum record size:  %u\n", min_rec_size);
    printf("    total records (bytes):  %u\n", total_rec_size);
  }
}

int
main(int argc, char **argv) {
  unsigned opt;
  char *param, *filename = 0, *endptr = 0;
  unsigned short dbname = 0xffff;
  int full = 0;

  ham_u16_t names[1024];
  ham_u32_t i, names_count = 1024;
  ham_status_t st;
  ham_env_t *env;
  ham_db_t *db;

  getopts_init(argc, argv, "ham_info");

  while ((opt = getopts(&opts[0], &param))) {
    switch (opt) {
      case ARG_DBNAME:
        if (!param) {
          printf("Parameter `dbname' is missing.\n");
          return (-1);
        }
        dbname = (short)strtoul(param, &endptr, 0);
        if (endptr && *endptr) {
          printf("Invalid parameter `dbname'; numerical value "
               "expected.\n");
          return (-1);
        }
        break;
      case ARG_FULL:
        full = 1;
        break;
      case ARG_QUIET:
        quiet = true;
        break;
      case GETOPTS_PARAMETER:
        if (filename) {
          printf("Multiple files specified. Please specify "
               "only one filename.\n");
          return (-1);
        }
        filename = param;
        break;
      case ARG_HELP:
        print_banner("ham_info");

        printf("usage: ham_info [-db DBNAME] [-f] file\n");
        printf("usage: ham_info -h\n");
        printf("     -h:     this help screen (alias: --help)\n");
        printf("     -db DBNAME: only print info about "
            "this database (alias: --dbname=<arg>)\n");
        printf("     -f:     print full information "
            "(alias: --full)\n");
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

  /* open the environment */
  st = ham_env_open(&env, filename, HAM_READ_ONLY, 0);
  if (st == HAM_FILE_NOT_FOUND) {
    printf("File `%s' not found or unable to open it\n", filename);
    return (-1);
  }
  else if (st != HAM_SUCCESS)
    error("ham_env_open", st);

  /* print information about the environment */
  print_environment(env);

  /* get a list of all databases */
  st = ham_env_get_database_names(env, names, &names_count);
  if (st != HAM_SUCCESS)
    error("ham_env_get_database_names", st);

  /* did the user specify a database name? if yes, show only this database */
  if (dbname != 0xffff) {
    st = ham_env_open_db(env, &db, dbname, 0, 0);
    if (st == HAM_DATABASE_NOT_FOUND) {
      printf("Database %u (0x%x) not found\n", dbname, dbname);
      return (-1);
    }
    else if (st)
      error("ham_env_open_db", st);

    print_database(db, dbname, full);

    st = ham_db_close(db, 0);
    if (st)
      error("ham_db_close", st);
  }
  else {
    /* otherwise: for each database: print information about the database */
    for (i = 0; i < names_count; i++) {
      st = ham_env_open_db(env, &db, names[i], 0, 0);
      if (st)
        error("ham_env_open_db", st);

      print_database(db, names[i], full);

      st = ham_db_close(db, 0);
      if (st)
        error("ham_db_close", st);
    }
  }
  /* clean up */
  st = ham_env_close(env, 0);
  if (st != HAM_SUCCESS)
    error("ham_env_close", st);

  return (0);
}
