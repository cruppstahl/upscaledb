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
#include <string.h>
#include <stdlib.h>

#include <ups/upscaledb.h>

#include "getopts.h"
#include "common.h"

#define ARG_HELP            1
#define ARG_DBNAME          2
#define ARG_REC_FORMAT      3
#define ARG_KEY_MAX_SIZE    4
#define ARG_REC_MAX_SIZE    5

#define FMT_STRING          1
#define FMT_NUMERIC         2
#define FMT_BINARY          3

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
    "only dump this database",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_KEY_MAX_SIZE,
    "maxkey",
    "max-key-size",
    "maximum of bytes to dump",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_REC_FORMAT,
    "rec",
    "record-format",
    "format of the record\n\t\t(numeric, string, binary)",
    GETOPTS_NEED_ARGUMENT },
  {
    ARG_REC_MAX_SIZE,
    "maxrec",
    "max-rec-size",
    "maximum of bytes to dump",
    GETOPTS_NEED_ARGUMENT },
  { 0, 0, 0, 0, 0 } /* terminating element */
};

static void
error(const char *foo, ups_status_t st)
{
  printf("%s() returned error %d: %s\n", foo, st, ups_strerror(st));
  exit(-1);
}

static void
warn(const char *foo, ups_status_t st)
{
  printf("%s() returned error %d: %s\n", foo, st, ups_strerror(st));
}

static void
dump_item(ups_key_t *key, ups_record_t *rec, int key_fmt, int max_key_size,
                int rec_fmt, int max_rec_size)
{
  int i, ok = 0;

  printf("key: ");

  if (!max_key_size)
    max_key_size = key->size;

  if (!key->data || !key->size)
    printf("(null)");
  else {
    switch (key_fmt) {
    case UPS_TYPE_UINT8:
      printf("%c", *(unsigned char *)key->data);
      break;
    case UPS_TYPE_UINT16:
      printf("%u", (unsigned) *(unsigned short *)key->data);
      break;
    case UPS_TYPE_UINT32:
      printf("%u", *(unsigned int *)key->data);
      break;
    case UPS_TYPE_UINT64:
      printf("%llu", *(unsigned long long *)key->data);
      break;
    case UPS_TYPE_REAL32:
      printf("%f", *(float *)key->data);
      break;
    case UPS_TYPE_REAL64:
      printf("%f", (float)*(double *)key->data);
      break;
    default:
      if (key->size < max_key_size)
        max_key_size = key->size;
      for (i = 0; i < max_key_size; i++)
        printf("%02x ", ((unsigned char *)key->data)[i]);
      break;
    }
  }

  printf(" => ");

  ok = 0;

  if (!max_rec_size)
    max_rec_size = rec->size;

  char *zterm = 0;

  if (!rec->data || !rec->size)
    printf("(null)");
  else {
    switch (rec_fmt) {
    case FMT_STRING:
      if (!(zterm = (char *)realloc(zterm, rec->size + 1))) {
        printf("out of memory\n");
        exit(-1);
      }
      memcpy(zterm, rec->data, rec->size);
      zterm[rec->size] = 0;
      if (rec->size > (unsigned)max_rec_size)
        zterm[max_rec_size] = 0;
      printf("%s", zterm);
      break;
    case FMT_NUMERIC:
      switch (rec->size) {
      case 1:
        printf("%c", *(unsigned char *)rec->data);
        ok = 1;
        break;
      case 2:
        printf("%u", (unsigned) *(unsigned short *)rec->data);
        ok = 1;
        break;
      case 4:
        printf("%u", *(unsigned int *)rec->data);
        ok = 1;
        break;
      case 8:
        printf("%llu", *(unsigned long long *)rec->data);
        ok = 1;
        break;
      default:
        /* fall through */
        break;
      }
      if (ok)
        break;
      break;
    case FMT_BINARY:
      if (rec->size < (unsigned)max_rec_size)
        max_rec_size = rec->size;
      for (i = 0; i < max_rec_size; i++)
        printf("%02x ", ((unsigned char *)rec->data)[i]);
      break;
    }
  }

  printf("\n");

  if (zterm) {
    free(zterm);
    zterm = 0;
  }
}

static void
dump_database(ups_db_t *db, uint16_t dbname, int max_key_size,
        int rec_fmt, int max_rec_size)
{
  ups_cursor_t *cursor;
  ups_status_t st;
  ups_key_t key;
  ups_record_t rec;

  memset(&key, 0, sizeof(key));
  memset(&rec, 0, sizeof(rec));

  printf("database %d (0x%x)\n", (int)dbname, (int)dbname);

  st = ups_cursor_create(&cursor, db, 0, 0);
  if (st != UPS_SUCCESS)
    error("ups_cursor_create", st);

  int key_fmt;
  ups_parameter_t params[] = {
     {UPS_PARAM_KEY_TYPE, 0},
     {0, }
  };
  st = ups_db_get_parameters(db, &params[0]);
  if (st)
    error("ups_db_get_parameter", st);
  key_fmt = (int)params[0].value;

  while (1) {
    st = ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT);
    if (st != UPS_SUCCESS) {
      /* reached end of the database? */
      if (st == UPS_KEY_NOT_FOUND)
        break;
      else
        warn("ups_cursor_next", st);
    }

    dump_item(&key, &rec, key_fmt, max_key_size, rec_fmt, max_rec_size);
  }

  ups_cursor_close(cursor);
  printf("\n");
}

int
main(int argc, char **argv) {
  unsigned opt;
  const char *param;
  const char *filename = 0;
  char *endptr = 0;
  int rec = FMT_BINARY, key_size = 16, rec_size = 16;
  unsigned short dbname = 0xffff;

  uint16_t names[1024];
  uint32_t i, names_count = 1024;
  ups_status_t st;
  ups_env_t *env;
  ups_db_t *db;

  getopts_init(argc, argv, "ups_dump");

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
      case ARG_REC_FORMAT:
        if (param) {
          if (!strcmp(param, "numeric"))
            rec = FMT_NUMERIC;
          else if (!strcmp(param, "string"))
            rec = FMT_STRING;
          else if (!strcmp(param, "binary"))
            rec = FMT_BINARY;
          else {
            printf("Invalid parameter `record-format'.\n");
            return (-1);
          }
        }
        break;
      case ARG_KEY_MAX_SIZE:
        key_size = (short)strtoul(param, &endptr, 0);
        if (endptr && *endptr) {
          printf("Invalid parameter `max-key-size'; numerical value "
             "expected.\n");
          return (-1);
        }
        break;
      case ARG_REC_MAX_SIZE:
        rec_size = (short)strtoul(param, &endptr, 0);
        if (endptr && *endptr) {
          printf("Invalid parameter `max-rec-size'; numerical value "
             "expected.\n");
          return (-1);
        }
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
        print_banner("ups_dump");

        printf("usage: ups_dump [-db DBNAME] [-key FMT] [-maxkey N] "
           "[-rec FMT] [-maxrec N] file\n");
        printf("usage: ups_dump -h\n");
        printf("     -h:     this help screen (alias: --help)\n");
        printf("     -db DBNAME: only dump "
           "this database (alias: --dbname=<arg>)\n");
        printf("     -maxkey N:  limit key length to N bytes "
           "(alias: --max-key-size=<arg>)\n");
        printf("     -rec FMT:   specify format of the record "
           "('string', 'binary' (default),\t    \t\t   'numeric')"
           " (alias: --record-format=<arg>)\n");
        printf("     -maxrec N:  limit record length to N bytes "
           "(alias: --max-rec-size=<arg>)\n");

        return (0);
      default:
        printf("Invalid or unknown parameter `%s'. "
           "Enter `ups_dump --help' for usage.", param);
        return (-1);
    }
  }

  if (!filename) {
    printf("Filename is missing. Enter `ups_dump --help' for usage.\n");
    return (-1);
  }

  /*
   * open the environment
   */
  st = ups_env_open(&env, filename,
                  UPS_READ_ONLY | UPS_IGNORE_MISSING_CALLBACK, 0);
  if (st == UPS_FILE_NOT_FOUND) {
    printf("File `%s' not found or unable to open it\n", filename);
    return (-1);
  }
  else if (st != UPS_SUCCESS)
    error("ups_env_open", st);

  /*
   * get a list of all databases
   */
  st = ups_env_get_database_names(env, names, &names_count);
  if (st != UPS_SUCCESS)
    error("ups_env_get_database_names", st);

  /*
   * did the user specify a database name? if yes, show only this database
   */
  if (dbname != 0xffff) {
    st = ups_env_open_db(env, &db, dbname, 0, 0);
    if (st == UPS_DATABASE_NOT_FOUND) {
      printf("Database %u (0x%x) not found\n", dbname, dbname);
      return (-1);
    }
    else if (st)
      error("ups_env_open_db", st);

    dump_database(db, dbname, key_size, rec, rec_size);

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

      dump_database(db, names[i], key_size, rec, rec_size);

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
