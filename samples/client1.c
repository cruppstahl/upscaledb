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
 * A simple example which connects to a upscaledb server (see server1.c),
 * creates a database, inserts some values, looks them up and erases them.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h> /* for exit() */
#include <ups/upscaledb.h>

#define LOOP 1000

void
error(const char *foo, ups_status_t st) {
  printf("%s() returned error %d: %s\n", foo, st, ups_strerror(st));
  exit(-1);
}

int
main(int argc, char **argv) {
  int i;
  ups_status_t st;      /* status variable */
  ups_env_t *env;       /* upscaledb Environment object */
  ups_db_t *db;         /* upscaledb Database object */
  ups_key_t key = {0};     /* the structure for a key */
  ups_record_t record = {0};   /* the structure for a record */

  /*
   * Connect to the server which should listen at 8080. The server is
   * implemented in server1.c.
   */
  st = ups_env_create(&env, "ups://localhost:8080/env1.db", 0, 0, 0);
  if (st != UPS_SUCCESS)
    error("ups_env_create", st);

  /* now open a Database in this Environment */
  st = ups_env_open_db(env, &db, 13, 0, 0);
  if (st != UPS_SUCCESS)
    error("ups_env_open_db", st);

  /* now we can insert, delete or lookup values in the database */
  for (i = 0; i < LOOP; i++) {
    key.data = &i;
    key.size = sizeof(i);

    record.size = key.size;
    record.data = key.data;

    st = ups_db_insert(db, 0, &key, &record, 0);
    if (st != UPS_SUCCESS)
      error("ups_db_insert", st);
  }

  /* now lookup all values */
  for (i = 0; i < LOOP; i++) {
    key.data = &i;
    key.size = sizeof(i);

    st = ups_db_find(db, 0, &key, &record, 0);
    if (st != UPS_SUCCESS)
      error("ups_db_find", st);

    /* check if the value is ok */
    if (*(int *)record.data != i) {
      printf("ups_db_find() ok, but returned bad value\n");
      return (-1);
    }
  }

  /* erase everything */
  for (i = 0; i < LOOP; i++) {
    key.data = &i;
    key.size = sizeof(i);

    st = ups_db_erase(db, 0, &key, 0);
    if (st != UPS_SUCCESS)
      error("ups_db_erase", st);
  }

  /* and make sure that the database is empty */
  for (i = 0; i < LOOP; i++) {
    key.data = &i;
    key.size = sizeof(i);

    st = ups_db_find(db, 0, &key, &record, 0);
    if (st != UPS_KEY_NOT_FOUND)
      error("ups_db_find", st);
  }

  /* close the database handle */
  st = ups_db_close(db, 0);
  if (st != UPS_SUCCESS)
    error("ups_db_close", st);

  /* close the environment handle */
  st = ups_env_close(env, 0);
  if (st != UPS_SUCCESS)
    error("ups_env_close", st);

  printf("success!\n");
  return (0);
}

