/**
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <ham/hamsterdb.h>

#include "../src/db.h"
#include "../src/env.h"
#include "../src/backend.h"
#include "../src/btree.h"

#include "getopts.h"

#define ARG_HELP      1
#define ARG_DBNAME      2
#define ARG_FULL      3
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

static void
print_environment(ham_env_t *env) {
  /* we need a temp. database */
  ham_db_t *db;
  ham_status_t st;

  st=ham_env_open_db(env, &db, 0xf001, 0, 0);
  if (st)
    error("ham_env_open_db", st);

  if (!quiet) {
    printf("environment\n");
    printf("  pagesize:           %u\n",
            ((ham::Environment *)env)->get_pagesize());
    printf("  version:          %u.%u.%u.%u\n",
            ((ham::Environment *)env)->get_version(0),
            ((ham::Environment *)env)->get_version(1),
            ((ham::Environment *)env)->get_version(2),
            ((ham::Environment *)env)->get_version(3));
    printf("  serialno:           %u\n",
            ((ham::Environment *)env)->get_serialno());
    printf("  max databases:        %u\n",
            ((ham::Environment *)env)->get_max_databases());
  }

  st = ham_db_close(db, 0);
  if (st)
    error("ham_db_close", st);
}

static void
print_database(ham_db_t *db, ham_u16_t dbname, int full) {
  ham::BtreeBackend *be;
  ham_cursor_t *cursor;
  ham_status_t st;
  ham_key_t key;
  ham_record_t rec;
  unsigned num_items = 0, ext_keys = 0, min_key_size = 0xffffffff,
      max_key_size = 0, min_rec_size = 0xffffffff, max_rec_size = 0,
      total_key_size = 0, total_rec_size = 0;

  be = (ham::BtreeBackend *)((ham::Database *)db)->get_backend();

  memset(&key, 0, sizeof(key));
  memset(&rec, 0, sizeof(rec));

  if (!quiet) {
    printf("\n");
    printf("  database %d (0x%x)\n", (int)dbname, (int)dbname);
    printf("    max key size:       %u\n", be->get_keysize());
    printf("    max keys per page:    %u\n", be->get_maxkeys());
    printf("    address of root page:   %llu\n",
        (long long unsigned int)be->get_rootpage());
    printf("    flags:          0x%04x\n",
        ((ham::Database *)db)->get_rt_flags());
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

    if (rec.size < min_rec_size)
      min_rec_size = rec.size;
    if (rec.size > max_rec_size)
      max_rec_size = rec.size;

    if (key.size > ((ham::Database *)db)->get_keysize())
      ext_keys++;

    total_key_size += key.size;
    total_rec_size += rec.size;
  }

  ham_cursor_close(cursor);

  if (!quiet) {
    printf("    number of items:    %u\n", num_items);
    if (num_items == 0)
      return;
    printf("    average key size:     %u\n", total_key_size/num_items);
    printf("    minimum key size:     %u\n", min_key_size);
    printf("    maximum key size:     %u\n", max_key_size);
    printf("    number of extended keys:%u\n", ext_keys);
    printf("    total keys (bytes):   %u\n", total_key_size);
    printf("    average record size:  %u\n", total_rec_size/num_items);
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
  ham_size_t i, names_count = 1024;
  ham_status_t st;
  ham_env_t *env;
  ham_db_t *db;

  ham_u32_t maj, min, rev;
  const char *licensee, *product;
  ham_get_license(&licensee, &product);
  ham_get_version(&maj, &min, &rev);

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
        printf("hamsterdb %d.%d.%d - Copyright (C) 2005-2012 "
             "Christoph Rupp (chris@crupp.de).\n\n",
             maj, min, rev);

        if (licensee[0] == '\0')
          printf(
             "This program is free software; you can redistribute "
             "it and/or modify it\nunder the terms of the GNU "
             "General Public License as published by the Free\n"
             "Software Foundation; either version 2 of the License,\n"
             "or (at your option) any later version.\n\n"
             "See file COPYING.GPL2 and COPYING.GPL3 for License "
             "information.\n\n");
        else
          printf("Commercial version; licensed for %s (%s)\n\n",
              licensee, product);

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
