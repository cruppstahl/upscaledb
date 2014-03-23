/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

/**
 * A simple example which connects to a hamsterdb server (see server1.c),
 * creates a database, inserts some values, looks them up and erases them.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h> /* for exit() */
#include <ham/hamsterdb.h>

#define LOOP 1000

void
error(const char *foo, ham_status_t st) {
  printf("%s() returned error %d: %s\n", foo, st, ham_strerror(st));
  exit(-1);
}

int
main(int argc, char **argv) {
  int i;
  ham_status_t st;      /* status variable */
  ham_env_t *env;       /* hamsterdb Environment object */
  ham_db_t *db;         /* hamsterdb Database object */
  ham_key_t key = {0};     /* the structure for a key */
  ham_record_t record = {0};   /* the structure for a record */

  /*
   * Connect to the server which should listen at 8080. The server is
   * implemented in server1.c.
   */
  st = ham_env_create(&env, "ham://localhost:8080/env1.db", 0, 0, 0);
  if (st != HAM_SUCCESS)
    error("ham_env_create", st);

  /* now open a Database in this Environment */
  st = ham_env_open_db(env, &db, 13, 0, 0);
  if (st != HAM_SUCCESS)
    error("ham_env_open_db", st);

  /* now we can insert, delete or lookup values in the database */
  for (i = 0; i < LOOP; i++) {
    key.data = &i;
    key.size = sizeof(i);

    record.size = key.size;
    record.data = key.data;

    st = ham_db_insert(db, 0, &key, &record, 0);
    if (st != HAM_SUCCESS)
      error("ham_db_insert", st);
  }

  /* now lookup all values */
  for (i = 0; i < LOOP; i++) {
    key.data = &i;
    key.size = sizeof(i);

    st = ham_db_find(db, 0, &key, &record, 0);
    if (st != HAM_SUCCESS)
      error("ham_db_find", st);

    /* check if the value is ok */
    if (*(int *)record.data != i) {
      printf("ham_db_find() ok, but returned bad value\n");
      return (-1);
    }
  }

  /* erase everything */
  for (i = 0; i < LOOP; i++) {
    key.data = &i;
    key.size = sizeof(i);

    st = ham_db_erase(db, 0, &key, 0);
    if (st != HAM_SUCCESS)
      error("ham_db_erase", st);
  }

  /* and make sure that the database is empty */
  for (i = 0; i < LOOP; i++) {
    key.data = &i;
    key.size = sizeof(i);

    st = ham_db_find(db, 0, &key, &record, 0);
    if (st != HAM_KEY_NOT_FOUND)
      error("ham_db_find", st);
  }

  /* close the database handle */
  st = ham_db_close(db, 0);
  if (st != HAM_SUCCESS)
    error("ham_db_close", st);

  /* close the environment handle */
  st = ham_env_close(env, 0);
  if (st != HAM_SUCCESS)
    error("ham_env_close", st);

  printf("success!\n");
  return (0);
}

