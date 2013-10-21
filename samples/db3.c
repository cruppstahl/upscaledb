/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 *
 *
 * This sample uses hamsterdb to sort data from stdin.
 * Every word is inserted into the database (duplicate words are ignored).
 * Then a cursor is used to print all words in sorted order.
 */

#include <stdio.h>
#include <string.h>
#include <ham/hamsterdb.h>

#define DATABASE_NAME       1

static int
my_string_compare(ham_db_t *db, const ham_u8_t *lhs, ham_size_t lhs_length,
          const ham_u8_t *rhs, ham_size_t rhs_length) {
  int s = strncmp((const char *)lhs, (const char *)rhs,
      lhs_length < rhs_length ? lhs_length : rhs_length);
  if (s < 0)
    return -1;
  if (s > 0)
    return +1;
  return 0;
}

int
main(int argc, char **argv) {
  ham_status_t st;             /* status variable */
  ham_env_t *env;              /* hamsterdb environment object */
  ham_db_t *db;                /* hamsterdb database object */
  ham_cursor_t *cursor;        /* a database cursor */
  char line[1024 * 4];         /* a buffer for reading lines */
  ham_key_t key = {0};
  ham_record_t record = {0};
  ham_parameter_t params[] = { /* parameters for ham_env_create_db */
    {HAM_PARAM_KEY_TYPE, HAM_TYPE_CUSTOM},
    {HAM_PARAM_RECORD_SIZE, 0}, /* we do not store records, only keys */
    {0, }
  };

  printf("This sample uses hamsterdb to sort data.\n");
  printf("Reading from stdin...\n");

  /*
   * Create a new hamsterdb Environment.
   */
  st = ham_env_create(&env, "test.db", 0, 0664, 0);
  if (st != HAM_SUCCESS) {
    printf("ham_env_create() failed with error %d\n", st);
    return (-1);
  }

  /*
   * Create a new Database in the new Environment. The HAM_TYPE_CUSTOM
   * parameter allows us to set a custom compare function.
   */
  st = ham_env_create_db(env, &db, DATABASE_NAME,
          HAM_ENABLE_EXTENDED_KEYS | HAM_ENABLE_DUPLICATE_KEYS, &params[0]);
  if (st != HAM_SUCCESS) {
    printf("ham_env_create_db() failed with error %d\n", st);
    return (-1);
  }

  /*
   * Since we use strings as our database keys we use our own comparison
   * function based on strcmp instead of the default memcmp function.
   */
  st = ham_db_set_compare_func(db, my_string_compare);
  if (st) {
    printf("ham_set_compare_func() failed with error %d\n", st);
    return (-1);
  }

  /*
   * Now read each line from stdin and split it in words; then each
   * word is inserted into the database
   */
  while (fgets(line, sizeof(line), stdin)) {
    char *start = line, *p;

    /*
     * strtok is not the best function because it's not threadsafe
     * and not flexible, but it's good enough for this example.
     */
    while ((p = strtok(start, " \t\r\n"))) {
      key.data = p;
      key.size = (ham_size_t)strlen(p) + 1; /* also store the terminating
                           * 0-byte */

      st = ham_db_insert(db, 0, &key, &record, 0);
      if (st != HAM_SUCCESS && st!=HAM_DUPLICATE_KEY) {
        printf("ham_db_insert() failed with error %d\n", st);
        return (-1);
      }
      printf(".");

      start = 0;
    }
  }

  /* create a cursor */
  st = ham_cursor_create(&cursor, db, 0, 0);
  if (st != HAM_SUCCESS) {
    printf("ham_cursor_create() failed with error %d\n", st);
    return (-1);
  }

  /* iterate over all items with HAM_CURSOR_NEXT, and print the words */
  while (1) {
    st = ham_cursor_move(cursor, &key, &record, HAM_CURSOR_NEXT);
    if (st != HAM_SUCCESS) {
      /* reached end of the database? */
      if (st == HAM_KEY_NOT_FOUND)
        break;
      else {
        printf("ham_cursor_next() failed with error %d\n", st);
        return (-1);
      }
    }

    /* print the word */
    printf("%s\n", (const char *)key.data);
  }

  /*
   * Then close the handles; the flag HAM_AUTO_CLEANUP will automatically
   * close all database and cursors, and we do not need to call
   * ham_cursor_close and ham_db_close
   */
  st = ham_env_close(env, HAM_AUTO_CLEANUP);
  if (st != HAM_SUCCESS) {
    printf("ham_env_close() failed with error %d\n", st);
    return (-1);
  }

  /* success! */
  return (0);
}

