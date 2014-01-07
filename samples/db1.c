/**
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 * A simple example which creates a database, inserts some values,
 * looks them up and erases them.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h> /* for exit() */
#include <ham/hamsterdb.h>

#define LOOP      10
#define DATABASE_NAME  1

void
error(const char *foo, ham_status_t st) {
  printf("%s() returned error %d: %s\n", foo, st, ham_strerror(st));
  exit(-1);
}

int
main(int argc, char **argv) {
  ham_u32_t i;
  ham_status_t st;             /* status variable */
  ham_env_t *env;              /* hamsterdb environment object */
  ham_db_t *db;                /* hamsterdb database object */
  ham_key_t key = {0};         /* the structure for a key */
  ham_record_t record = {0};   /* the structure for a record */
  ham_parameter_t params[] = { /* parameters for ham_env_create_db */
    {HAM_PARAM_KEY_TYPE, HAM_TYPE_UINT32},
    {HAM_PARAM_RECORD_SIZE, sizeof(ham_u32_t)},
    {0, }
  };

  /* First create a new hamsterdb Environment */
  st = ham_env_create(&env, "test.db", 0, 0664, 0);
  if (st != HAM_SUCCESS)
    error("ham_create", st);

  /* And in this Environment we create a new Database for uint32-keys
   * and uint32-records. */
  st = ham_env_create_db(env, &db, DATABASE_NAME, 0, &params[0]);
  if (st != HAM_SUCCESS)
    error("ham_create", st);

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

    st = ham_db_insert(db, 0, &key, &record, 0);
    if (st != HAM_SUCCESS)
      error("ham_db_insert", st);
  }

  /*
   * now lookup all values
   *
   * for ham_db_find(), we could use the flag HAM_RECORD_USER_ALLOC, if WE
   * allocate record.data (otherwise the memory is automatically allocated
   * by hamsterdb)
   */
  for (i = 0; i < LOOP; i++) {
    key.data = &i;
    key.size = sizeof(i);

    st = ham_db_find(db, 0, &key, &record, 0);
    if (st != HAM_SUCCESS)
      error("ham_db_find", st);

    /*
     * check if the value is ok
     */
    if (*(int *)record.data != i) {
      printf("ham_db_find() ok, but returned bad value\n");
      return (-1);
    }
  }

  /*
   * close the database handle, then re-open it (to demonstrate how to open
   * an Environment and a Database)
   */
  st = ham_db_close(db, 0);
  if (st != HAM_SUCCESS)
    error("ham_db_close", st);
  st = ham_env_close(env, 0);
  if (st != HAM_SUCCESS)
    error("ham_env_close", st);

  st = ham_env_open(&env, "test.db", 0, 0);
  if (st != HAM_SUCCESS)
    error("ham_env_open", st);
  st = ham_env_open_db(env, &db, DATABASE_NAME, 0, 0);
  if (st != HAM_SUCCESS)
    error("ham_env_open_db", st);

  /* now erase all values */
  for (i = 0; i < LOOP; i++) {
    key.size = sizeof(i);
    key.data = &i;

    st = ham_db_erase(db, 0, &key, 0);
    if (st != HAM_SUCCESS)
      error("ham_db_erase", st);
  }

  /*
   * once more we try to find all values... every ham_db_find() call must
   * now fail with HAM_KEY_NOT_FOUND
   */
  for (i = 0; i < LOOP; i++) {
    key.size = sizeof(i);
    key.data = &i;

    st = ham_db_find(db, 0, &key, &record, 0);
    if (st != HAM_KEY_NOT_FOUND)
      error("ham_db_find", st);
  }

  /* we're done! close the handles. HAM_AUTO_CLEANUP will also close the
   * 'db' handle */
  st = ham_env_close(env, HAM_AUTO_CLEANUP);
  if (st != HAM_SUCCESS)
    error("ham_env_close", st);

  printf("success!\n");
  return (0);
}

