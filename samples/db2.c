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

/**
 * This example opens an Environment and copies one Database into another.
 * With small modifications this sample would also be able to copy
 * In Memory-Environments to On Disk-Environments and vice versa.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h> /* for exit() */
#include <ups/upscaledb.h>

void
error(const char *foo, ups_status_t st) {
  printf("%s() returned error %d: %s\n", foo, st, ups_strerror(st));
  exit(-1);
}

void
usage() {
  printf("usage: ./db2 <environment> <source-db> <destination-db>\n");
  exit(-1);
}

void
copy_db(ups_db_t *source, ups_db_t *dest) {
  ups_cursor_t *cursor;  /* upscaledb cursor object */
  ups_status_t st;
  ups_key_t key;
  ups_record_t rec;

  memset(&key, 0, sizeof(key));
  memset(&rec, 0, sizeof(rec));

  /* create a new cursor */
  st = ups_cursor_create(&cursor, source, 0, 0);
  if (st)
    error("ups_cursor_create", st);

  /* get a cursor to the source database */
  st = ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_FIRST);
  if (st == UPS_KEY_NOT_FOUND) {
    printf("database is empty!\n");
    return;
  }
  else if (st)
    error("ups_cursor_move", st);

  do {
    /* insert this element into the new database */
    st = ups_db_insert(dest, 0, &key, &rec, UPS_DUPLICATE);
    if (st)
      error("ups_db_insert", st);

    /* give some feedback to the user */
    printf(".");

    /* fetch the next item, and repeat till we've reached the end
     * of the database */
    st = ups_cursor_move(cursor, &key, &rec, UPS_CURSOR_NEXT);
    if (st && st != UPS_KEY_NOT_FOUND)
      error("ups_cursor_move", st);

  } while (st == 0);

  /* clean up and return */
  ups_cursor_close(cursor);
}

int
main(int argc, char **argv) {
  ups_status_t st;
  ups_env_t *env = 0;
  ups_db_t *src_db = 0;
  ups_db_t *dest_db = 0;
  uint16_t src_name;
  uint16_t dest_name;
  const char *env_path = 0;

  /* check and parse the command line parameters */
  if (argc != 4)
    usage();
  env_path = argv[1];
  src_name = atoi(argv[2]);
  dest_name = atoi(argv[3]);
  if (src_name == 0 || dest_name == 0)
    usage();

  /* open the Environment */
  st = ups_env_open(&env, env_path, 0, 0);
  if (st)
    error("ups_env_open", st);

  /* open the source database */
  st = ups_env_open_db(env, &src_db, src_name, 0, 0);
  if (st)
    error("ups_env_open_db", st);

  /* create the destination database */
  st = ups_env_create_db(env, &dest_db, dest_name,
                  UPS_ENABLE_DUPLICATE_KEYS, 0);
  if (st)
    error("ups_env_create_db", st);

  /* copy the data */
  copy_db(src_db, dest_db);

  /* clean up and return */
  st = ups_env_close(env, UPS_AUTO_CLEANUP);
  if (st)
    error("ups_env_close", st);

  printf("\nsuccess!\n");
  return (0);
}

