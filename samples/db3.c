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

/** This sample uses upscaledb to sort data from stdin.
 * Every word is inserted into the database (duplicate words are ignored).
 * Then a cursor is used to print all words in sorted order.
 */

#include <stdio.h>
#include <string.h>
#include <ups/upscaledb.h>

#define DATABASE_NAME       1

static int
my_string_compare(ups_db_t *db, const uint8_t *lhs, uint32_t lhs_length,
          const uint8_t *rhs, uint32_t rhs_length) {
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
  ups_status_t st;             /* status variable */
  ups_env_t *env;              /* upscaledb environment object */
  ups_db_t *db;                /* upscaledb database object */
  ups_cursor_t *cursor;        /* a database cursor */
  char line[1024 * 4];         /* a buffer for reading lines */
  ups_key_t key = {0};
  ups_record_t record = {0};
  ups_parameter_t params[] = { /* parameters for ups_env_create_db */
    {UPS_PARAM_KEY_TYPE, UPS_TYPE_CUSTOM},
    {UPS_PARAM_RECORD_SIZE, 0}, /* we do not store records, only keys */
    {0, }
  };

  printf("This sample uses upscaledb to sort data.\n");
  printf("Reading from stdin...\n");

  /*
   * Create a new upscaledb Environment.
   */
  st = ups_env_create(&env, "test.db", 0, 0664, 0);
  if (st != UPS_SUCCESS) {
    printf("ups_env_create() failed with error %d\n", st);
    return (-1);
  }

  /*
   * Create a new Database in the new Environment. The UPS_TYPE_CUSTOM
   * parameter allows us to set a custom compare function.
   */
  st = ups_env_create_db(env, &db, DATABASE_NAME,
                  UPS_ENABLE_DUPLICATE_KEYS, &params[0]);
  if (st != UPS_SUCCESS) {
    printf("ups_env_create_db() failed with error %d\n", st);
    return (-1);
  }

  /*
   * Since we use strings as our database keys we use our own comparison
   * function based on strcmp instead of the default memcmp function.
   */
  st = ups_db_set_compare_func(db, my_string_compare);
  if (st) {
    printf("ups_set_compare_func() failed with error %d\n", st);
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
      key.size = (uint32_t)strlen(p) + 1; /* also store the terminating
                           * 0-byte */

      st = ups_db_insert(db, 0, &key, &record, 0);
      if (st != UPS_SUCCESS && st!=UPS_DUPLICATE_KEY) {
        printf("ups_db_insert() failed with error %d\n", st);
        return (-1);
      }
      printf(".");

      start = 0;
    }
  }

  /* create a cursor */
  st = ups_cursor_create(&cursor, db, 0, 0);
  if (st != UPS_SUCCESS) {
    printf("ups_cursor_create() failed with error %d\n", st);
    return (-1);
  }

  /* iterate over all items with UPS_CURSOR_NEXT, and print the words */
  while (1) {
    st = ups_cursor_move(cursor, &key, &record, UPS_CURSOR_NEXT);
    if (st != UPS_SUCCESS) {
      /* reached end of the database? */
      if (st == UPS_KEY_NOT_FOUND)
        break;
      else {
        printf("ups_cursor_next() failed with error %d\n", st);
        return (-1);
      }
    }

    /* print the word */
    printf("%s\n", (const char *)key.data);
  }

  /*
   * Then close the handles; the flag UPS_AUTO_CLEANUP will automatically
   * close all database and cursors, and we do not need to call
   * ups_cursor_close and ups_db_close
   */
  st = ups_env_close(env, UPS_AUTO_CLEANUP);
  if (st != UPS_SUCCESS) {
    printf("ups_env_close() failed with error %d\n", st);
    return (-1);
  }

  /* success! */
  return (0);
}

