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
 * A simple example which creates a database, inserts some values,
 * looks them up and erases them.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h> /* for exit() */
#include <ups/upscaledb.h>

#define LOOP      10
#define DATABASE_NAME  1

void
error(const char *foo, ups_status_t st) {
  printf("%s() returned error %d: %s\n", foo, st, ups_strerror(st));
  exit(-1);
}

int
main(int argc, char **argv) {
  uint32_t i;
  ups_status_t st;             /* status variable */
  ups_env_t *env;              /* upscaledb environment object */
  ups_db_t *db;                /* upscaledb database object */
  ups_key_t key = {0};         /* the structure for a key */
  ups_record_t record = {0};   /* the structure for a record */
  ups_parameter_t params[] = { /* parameters for ups_env_create_db */
    {UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT32},
    {UPS_PARAM_RECORD_SIZE, sizeof(uint32_t)},
    {0, }
  };

  /* First create a new upscaledb Environment */
  st = ups_env_create(&env, "test.db", 0, 0664, 0);
  if (st != UPS_SUCCESS)
    error("ups_env_create", st);

  /* And in this Environment we create a new Database for uint32-keys
   * and uint32-records. */
  st = ups_env_create_db(env, &db, DATABASE_NAME, 0, &params[0]);
  if (st != UPS_SUCCESS)
    error("ups_env_create_db", st);

  /*
   * now we can insert, delete or lookup values in the database
   *
   * for our test program, we just insert a few values, then look them
   * up, then delete them and try to look them up again (which will fail).
   */
  for (i = 0; i < LOOP; i++) {
    key.data = &i;
    key.size = sizeof(i);

    record.size = key.size;
    record.data = key.data;

    st = ups_db_insert(db, 0, &key, &record, 0);
    if (st != UPS_SUCCESS)
      error("ups_db_insert", st);
  }

  /*
   * now lookup all values
   *
   * for ups_db_find(), we could use the flag UPS_RECORD_USER_ALLOC, if WE
   * allocate record.data (otherwise the memory is automatically allocated
   * by upscaledb)
   */
  for (i = 0; i < LOOP; i++) {
    key.data = &i;
    key.size = sizeof(i);

    st = ups_db_find(db, 0, &key, &record, 0);
    if (st != UPS_SUCCESS)
      error("ups_db_find", st);

    /*
     * check if the value is ok
     */
    if (*(int *)record.data != i) {
      printf("ups_db_find() ok, but returned bad value\n");
      return (-1);
    }
  }

  /*
   * close the database handle, then re-open it (to demonstrate how to open
   * an Environment and a Database)
   */
  st = ups_db_close(db, 0);
  if (st != UPS_SUCCESS)
    error("ups_db_close", st);
  st = ups_env_close(env, 0);
  if (st != UPS_SUCCESS)
    error("ups_env_close", st);

  st = ups_env_open(&env, "test.db", 0, 0);
  if (st != UPS_SUCCESS)
    error("ups_env_open", st);
  st = ups_env_open_db(env, &db, DATABASE_NAME, 0, 0);
  if (st != UPS_SUCCESS)
    error("ups_env_open_db", st);

  /* now erase all values */
  for (i = 0; i < LOOP; i++) {
    key.size = sizeof(i);
    key.data = &i;

    st = ups_db_erase(db, 0, &key, 0);
    if (st != UPS_SUCCESS)
      error("ups_db_erase", st);
  }

  /*
   * once more we try to find all values... every ups_db_find() call must
   * now fail with UPS_KEY_NOT_FOUND
   */
  for (i = 0; i < LOOP; i++) {
    key.size = sizeof(i);
    key.data = &i;

    st = ups_db_find(db, 0, &key, &record, 0);
    if (st != UPS_KEY_NOT_FOUND)
      error("ups_db_find", st);
  }

  /* we're done! close the handles. UPS_AUTO_CLEANUP will also close the
   * 'db' handle */
  st = ups_env_close(env, UPS_AUTO_CLEANUP);
  if (st != UPS_SUCCESS)
    error("ups_env_close", st);

  printf("success!\n");
  return (0);
}

