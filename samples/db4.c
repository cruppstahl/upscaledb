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
 * This sample uses hamsterdb to read data from stdin into a "record number"
 * database; every word is inserted into the database in the order of
 * its processing. Then a cursor is used to print all words in the
 * original order.
 */

#include <stdio.h>
#include <string.h>
#include <ham/hamsterdb.h>

#define DATABASE_NAME       1

int
main(int argc, char **argv) {
  ham_status_t st;    /* status variable */
  ham_env_t *env;     /* hamsterdb environment object */
  ham_db_t *db;       /* hamsterdb database object */
  ham_cursor_t *cursor;   /* a database cursor */
  char line[1024 * 4];  /* a buffer for reading lines */
  ham_key_t key;
  ham_record_t record;

  memset(&key, 0, sizeof(key));
  memset(&record, 0, sizeof(record));

  printf("This sample uses hamsterdb to list all words in the "
      "original order.\n");
  printf("Reading from stdin...\n");

  /*
   * Create a new hamsterdb "record number" Database.
   * We could create an in-memory-Environment to speed up the sorting.
   */
  st = ham_env_create(&env, "test.db", 0, 0664, 0);
  if (st != HAM_SUCCESS) {
    printf("ham_env_create() failed with error %d\n", st);
    return (-1);
  }

  st = ham_env_create_db(env, &db, DATABASE_NAME, HAM_RECORD_NUMBER, 0);
  if (st != HAM_SUCCESS) {
    printf("ham_env_create_db() failed with error %d\n", st);
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
      ham_u64_t recno;

      key.flags = HAM_KEY_USER_ALLOC;
      key.data = &recno;
      key.size = sizeof(recno);

      record.data = p;
      record.size = (ham_u32_t)strlen(p) + 1; /* also store
                            * terminating 0 */

      st = ham_db_insert(db, 0, &key, &record, 0);
      if (st != HAM_SUCCESS && st != HAM_DUPLICATE_KEY) {
        printf("ham_db_insert() failed with error %d\n", st);
        return (-1);
      }
      printf(".");

      start = 0;
    }
  }

  /* Create a cursor */
  st = ham_cursor_create(&cursor, db, 0, 0);
  if (st != HAM_SUCCESS) {
    printf("ham_cursor_create() failed with error %d\n", st);
    return (-1);
  }

  memset(&key, 0, sizeof(key));

  /* Iterate over all items and print the records */
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

    /* print the record number and the word */
#ifdef WIN32
    printf("%I64u: %s\n", *(ham_u64_t *)key.data,
        (const char *)record.data);
#else
    printf("%llu: %s\n", *(unsigned long long *)key.data,
        (const char *)record.data);
#endif
  }

  /*
   * Then close the handles; the flag HAM_AUTO_CLEANUP will automatically 
   * close all databases and cursors and we do not need to
   * call ham_cursor_close and ham_db_close
   */
  st = ham_env_close(env, HAM_AUTO_CLEANUP);
  if (st != HAM_SUCCESS) {
    printf("ham_env_close() failed with error %d\n", st);
    return (-1);
  }

  /* success! */
  return (0);
}

