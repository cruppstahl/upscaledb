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
 * This sample demonstrates the use of duplicate items. Every line is
 * split into words, and each word is inserted with its line number.
 * Then a cursor is used to print all words in a sorted order, with the
 * lines in which the word occurred.
 */

#include <stdio.h>
#include <string.h>
#include <ups/upscaledb.h>

#define DATABASE_NAME     1

int
main(int argc, char **argv) {
  ups_status_t st;          /* status variable */
  ups_env_t *env;           /* upscaledb environment object */
  ups_db_t *db;             /* upscaledb database object */
  ups_cursor_t *cursor;     /* a database cursor */
  char line[1024 * 4];      /* a buffer for reading lines */
  uint32_t lineno = 0;     /* the current line number */
  ups_key_t key;
  ups_record_t record;
  ups_parameter_t params[] = {  /* we insert 4 byte records only */
    {UPS_PARAM_RECORD_SIZE, sizeof(uint32_t)},
    {0, 0}
  };

  memset(&key, 0, sizeof(key));
  memset(&record, 0, sizeof(record));

  printf("This sample uses upscaledb and duplicate keys to list all words "
      "in the\noriginal order, together with their line number.\n");
  printf("Reading from stdin...\n");

  /* Create a new Database with support for duplicate keys */
  st = ups_env_create(&env, 0, UPS_IN_MEMORY, 0664, 0);
  if (st != UPS_SUCCESS) {
    printf("ups_env_create() failed with error %d\n", st);
    return (-1);
  }
  st = ups_env_create_db(env, &db, DATABASE_NAME,
                  UPS_ENABLE_DUPLICATE_KEYS, &params[0]);
  if (st != UPS_SUCCESS) {
    printf("ups_env_create_db() failed with error %d\n", st);
    return (-1);
  }

  /*
   * Now read each line from stdin and split it in words; then each
   * word is inserted into the database
   */
  while (fgets(line, sizeof(line), stdin)) {
    char *start = line, *p;
    lineno++;

    /*
     * strtok is not the best function because it's not threadsafe
     * and not flexible, but it's good enough for this example.
     */
    while ((p = strtok(start, " \t\r\n"))) {
      key.data = p;
      key.size = (uint32_t)strlen(p) + 1; /* also store the terminating
                                             * 0-byte */
      record.data = &lineno;
      record.size = sizeof(lineno);

      st = ups_db_insert(db, 0, &key, &record, UPS_DUPLICATE);
      if (st != UPS_SUCCESS) {
        printf("ups_db_insert() failed with error %d\n", st);
        return (-1);
      }
      printf(".");

      start = 0;
    }
  }

  /* Create a cursor */
  st = ups_cursor_create(&cursor, db, 0, 0);
  if (st != UPS_SUCCESS) {
    printf("ups_cursor_create() failed with error %d\n", st);
    return (-1);
  }

  /* Iterate over all items and print them */
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

    /* print the word and the line number */
    printf("%s: appeared in line %u\n", (const char *)key.data,
        *(unsigned *)record.data);
  }

  /*
   * Then close the handles; the flag UPS_AUTO_CLEANUP will automatically
   * close all cursors and we do not need to call ups_cursor_close and
   * ups_db_close
   */
  st = ups_env_close(env, UPS_AUTO_CLEANUP);
  if (st != UPS_SUCCESS) {
    printf("ups_env_close() failed with error %d\n", st);
    return (-1);
  }

  /* success! */
  return (0);
}

