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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <ups/upscaledb_int.h>

#include "getopts.h"
#include "common.h"

#define ARG_HELP        1
#define ARG_DBNAME      2
#define ARG_FULL        3
#define ARG_BTREE       4
#define ARG_QUIET       5

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
    ARG_BTREE,
    "b",
    "btree",
    "print btree information (for developers)",
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
error(const char *foo, ups_status_t st) {
  printf("%s() returned error %d: %s\n", foo, st, ups_strerror(st));
  exit(-1);
}

static const char *
get_compressor_name(int library) {
  switch (library) {
    case UPS_COMPRESSOR_ZLIB:
      return ("zlib");
    case UPS_COMPRESSOR_SNAPPY:
      return ("snappy");
    case UPS_COMPRESSOR_LZF:
      return ("lzf");
    case UPS_COMPRESSOR_UINT32_VARBYTE:
      return ("varbyte");
    case UPS_COMPRESSOR_UINT32_SIMDCOMP:
      return ("simdcomp");
    case UPS_COMPRESSOR_UINT32_GROUPVARINT:
      return ("groupvarint");
    case UPS_COMPRESSOR_UINT32_STREAMVBYTE:
      return ("streamvbyte");
    case UPS_COMPRESSOR_UINT32_FOR:
      return ("for");
    default:
      return ("???");
  }
}

static const char *
get_type_name(uint64_t type) {
  switch (type) {
    case UPS_TYPE_UINT8:
      return ("UPS_TYPE_UINT8");
    case UPS_TYPE_UINT16:
      return ("UPS_TYPE_UINT16");
    case UPS_TYPE_UINT32:
      return ("UPS_TYPE_UINT32");
    case UPS_TYPE_UINT64:
      return ("UPS_TYPE_UINT64");
    case UPS_TYPE_REAL32:
      return ("UPS_TYPE_REAL32");
    case UPS_TYPE_REAL64:
      return ("UPS_TYPE_REAL64");
    case UPS_TYPE_CUSTOM:
      return ("UPS_TYPE_CUSTOM");
    default:
      return ("UPS_TYPE_BINARY");
  }
}

static void
print_environment(ups_env_t *env) {
  ups_parameter_t params[] = {
    {UPS_PARAM_PAGE_SIZE, 0},
    {UPS_PARAM_MAX_DATABASES, 0},
    {UPS_PARAM_JOURNAL_COMPRESSION, 0},
    {0, 0}
  };

  ups_status_t st = ups_env_get_parameters(env, &params[0]);
  if (st != 0)
    error("ups_env_get_parameters", st);

  if (!quiet) {
    uint32_t v1, v2, v3;
    ups_get_version(&v1, &v2, &v3);

    printf("environment\n");
    printf("  page_size:            %u\n", (unsigned)params[0].value);
    printf("  version:              %u.%u.%u\n",
            v1, v2, v3);
    printf("  max databases:        %u\n", (unsigned)params[1].value);
    if (params[2].value)
      printf("  journal compression:  %s\n",
                      get_compressor_name((int)params[2].value));
  }
}

static void
print_full_information(ups_db_t *db) {
  ups_cursor_t *cursor;
  ups_status_t st;
  ups_key_t key = {0};
  ups_record_t rec = {0};

  unsigned num_items = 0, min_key_size = 0xffffffff,
      max_key_size = 0, min_rec_size = 0xffffffff, max_rec_size = 0,
      total_key_size = 0, total_rec_size = 0, extended_keys = 0;

  st = ups_cursor_create(&cursor, db, 0, 0);
  if (st != UPS_SUCCESS)
    error("ups_cursor_create", st);

  while (1) {
    st = ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT);
    if (st != UPS_SUCCESS) {
      /* reached end of the database? */
      if (st == UPS_KEY_NOT_FOUND)
        break;
      else
        error("ups_cursor_next", st);
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

  ups_cursor_close(cursor);

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
    printf("    maximum record size:  %u\n", max_rec_size);
    printf("    total records (bytes):  %u\n", total_rec_size);
  }
}

static void
print_btree_metrics(btree_metrics_t *metrics, const char *prefix) {
  printf("    %s: number of pages:    %u\n", prefix,
                  (uint32_t)metrics->number_of_pages);
  printf("    %s: number of keys:     %u\n", prefix,
                  (uint32_t)metrics->number_of_keys);
  printf("    %s: keys per page (min, avg, max):      %u, %u, %u\n", prefix,
                  metrics->keys_per_page.min,
                  metrics->keys_per_page.avg,
                  metrics->keys_per_page.max);
  printf("    %s: keylist ranges (min, avg, max):     %u, %u, %u\n", prefix,
                  metrics->keylist_ranges.min,
                  metrics->keylist_ranges.avg,
                  metrics->keylist_ranges.max);
  printf("    %s: recordlist ranges (min, avg, max):  %u, %u, %u\n", prefix,
                  metrics->recordlist_ranges.min,
                  metrics->recordlist_ranges.avg,
                  metrics->recordlist_ranges.max);
  printf("    %s: keylist index (min, avg, max):      %u, %u, %u\n", prefix,
                  metrics->keylist_index.min,
                  metrics->keylist_index.avg,
                  metrics->keylist_index.max);
  printf("    %s: recordlist index (min, avg, max):   %u, %u, %u\n", prefix,
                  metrics->recordlist_index.min,
                  metrics->recordlist_index.avg,
                  metrics->recordlist_index.max);
  printf("    %s: keylist unused (min, avg, max):     %u, %u, %u\n", prefix,
                  metrics->keylist_unused.min,
                  metrics->keylist_unused.avg,
                  metrics->keylist_unused.max);
  printf("    %s: recordlist unused (min, avg, max):  %u, %u, %u\n", prefix,
                  metrics->recordlist_unused.min,
                  metrics->recordlist_unused.avg,
                  metrics->recordlist_unused.max);
  printf("    %s: keylist blocks (min, avg, max):     %u, %u, %u\n", prefix,
                  metrics->keylist_blocks_per_page.min,
                  metrics->keylist_blocks_per_page.avg,
                  metrics->keylist_blocks_per_page.max);
  printf("    %s: keylist block size (min, avg, max): %u, %u, %u\n", prefix,
                  metrics->keylist_block_sizes.min,
                  metrics->keylist_block_sizes.avg,
                  metrics->keylist_block_sizes.max);
}

static void
print_btree_information(ups_env_t *env, ups_db_t *db) {
  ups_env_metrics_t metrics;

  ups_status_t st = ups_env_get_metrics(env, &metrics);
  if (st != UPS_SUCCESS)
    error("ups_env_get_metrics", st);

  print_btree_metrics(&metrics.btree_internal_metrics, "btree node");
  print_btree_metrics(&metrics.btree_leaf_metrics, "btree leaf");
}

static void
print_database(ups_env_t *env, ups_db_t *db, uint16_t dbname,
                int full, int btree) {
  // get the database information
  ups_parameter_t params[] = {
    {UPS_PARAM_KEY_TYPE, 0},
    {UPS_PARAM_KEY_SIZE, 0},
    {UPS_PARAM_RECORD_SIZE, 0},
    {UPS_PARAM_MAX_KEYS_PER_PAGE, 0},
    {UPS_PARAM_FLAGS, 0},
    {UPS_PARAM_RECORD_COMPRESSION, 0},
    {UPS_PARAM_KEY_COMPRESSION, 0},
    {UPS_PARAM_RECORD_TYPE, 0},
    {0, 0}
  };

  ups_status_t st = ups_db_get_parameters(db, &params[0]);
  if (st != 0)
    error("ups_db_get_parameters", st);

  if (!quiet) {
    const char *key_type = get_type_name(params[0].value);
    const char *record_type = get_type_name(params[7].value);
    printf("\n");
    printf("  database %d (0x%x)\n", (int)dbname, (int)dbname);
    printf("    key type:             %s\n", key_type);
    printf("    max key size:         %u\n", (unsigned)params[1].value);
    printf("    max keys per page:    %u\n", (unsigned)params[3].value);
    printf("    record type:          %s\n", record_type);
    printf("    flags:                0x%04x\n", (unsigned)params[4].value);
    if (params[5].value)
      printf("    record compression:   %s\n",
                      get_compressor_name((int)params[5].value));
    if (params[6].value)
      printf("    key compression:      %s\n",
                      get_compressor_name((int)params[6].value));
    if (params[2].value == UPS_RECORD_SIZE_UNLIMITED)
      printf("    record size:          unlimited\n");
    else
      printf("    record size:          %d (inline: %s)\n",
                      (unsigned)params[2].value,
                      params[4].value & UPS_FORCE_RECORDS_INLINE
                            ? "yes"
                            : "no");
  }

  if (full)
    print_full_information(db);
  if (btree)
    print_btree_information(env, db);
}

int
main(int argc, char **argv) {
  unsigned opt;
  const char *param;
  const char *filename = 0;
  char *endptr = 0;
  unsigned short dbname = 0xffff;
  int full = 0;
  int btree = 0;

  uint16_t names[1024];
  uint32_t i, names_count = 1024;
  ups_status_t st;
  ups_env_t *env;
  ups_db_t *db;

  getopts_init(argc, argv, "ups_info");

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
      case ARG_BTREE:
        btree = 1;
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
        print_banner("ups_info");

        printf("usage: ups_info [-db DBNAME] [-f] file\n");
        printf("usage: ups_info -h\n");
        printf("     -h:     this help screen (alias: --help)\n");
        printf("     -db DBNAME: only print info about "
            "this database (alias: --dbname=<arg>)\n");
        printf("     -b:     print btree information (for developers)"
            "(alias: --btree)\n");
        printf("     -f:     print full information "
            "(alias: --full)\n");
        return (0);
      default:
        printf("Invalid or unknown parameter `%s'. "
             "Enter `ups_info --help' for usage.", param);
        return (-1);
    }
  }

  if (!filename) {
    printf("Filename is missing. Enter `ups_info --help' for usage.\n");
    return (-1);
  }

  /* open the environment */
  st = ups_env_open(&env, filename,
                  UPS_READ_ONLY | UPS_IGNORE_MISSING_CALLBACK, 0);
  if (st == UPS_FILE_NOT_FOUND) {
    printf("File `%s' not found or unable to open it\n", filename);
    return (-1);
  }
  else if (st != UPS_SUCCESS)
    error("ups_env_open", st);

  /* print information about the environment */
  print_environment(env);

  /* get a list of all databases */
  st = ups_env_get_database_names(env, names, &names_count);
  if (st != UPS_SUCCESS)
    error("ups_env_get_database_names", st);

  /* did the user specify a database name? if yes, show only this database */
  if (dbname != 0xffff) {
    st = ups_env_open_db(env, &db, dbname, 0, 0);
    if (st == UPS_DATABASE_NOT_FOUND) {
      printf("Database %u (0x%x) not found\n", dbname, dbname);
      return (-1);
    }
    else if (st)
      error("ups_env_open_db", st);

    print_database(env, db, dbname, full, btree);

    st = ups_db_close(db, 0);
    if (st)
      error("ups_db_close", st);
  }
  else {
    /* otherwise: for each database: print information about the database */
    for (i = 0; i < names_count; i++) {
      st = ups_env_open_db(env, &db, names[i], 0, 0);
      if (st)
        error("ups_env_open_db", st);

      print_database(env, db, names[i], full, btree);

      st = ups_db_close(db, 0);
      if (st)
        error("ups_db_close", st);
    }
  }
  /* clean up */
  st = ups_env_close(env, 0);
  if (st != UPS_SUCCESS)
    error("ups_env_close", st);

  return (0);
}
