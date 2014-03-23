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
 * This example opens an Environment and copies one Database into another.
 * With small modifications this sample would also be able to copy
 * In Memory-Environments to On Disk-Environments and vice versa.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h> /* for exit() */
#include <ham/hamsterdb.h>

void
error(const char *foo, ham_status_t st) {
  printf("%s() returned error %d: %s\n", foo, st, ham_strerror(st));
  exit(-1);
}

void
usage() {
  printf("usage: ./db2 <environment> <source-db> <destination-db>\n");
  exit(-1);
}

void
copy_db(ham_db_t *source, ham_db_t *dest) {
  ham_cursor_t *cursor;  /* hamsterdb cursor object */
  ham_status_t st;
  ham_key_t key;
  ham_record_t rec;

  memset(&key, 0, sizeof(key));
  memset(&rec, 0, sizeof(rec));

  /* create a new cursor */
  st = ham_cursor_create(&cursor, source, 0, 0);
  if (st)
    error("ham_cursor_create", st);

  /* get a cursor to the source database */
  st = ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_FIRST);
  if (st == HAM_KEY_NOT_FOUND) {
    printf("database is empty!\n");
    return;
  }
  else if (st)
    error("ham_cursor_move", st);

  do {
    /* insert this element into the new database */
    st = ham_db_insert(dest, 0, &key, &rec, HAM_DUPLICATE);
    if (st)
      error("ham_db_insert", st);

    /* give some feedback to the user */
    printf(".");

    /* fetch the next item, and repeat till we've reached the end
     * of the database */
    st = ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT);
    if (st && st != HAM_KEY_NOT_FOUND)
      error("ham_cursor_move", st);

  } while (st == 0);

  /* clean up and return */
  ham_cursor_close(cursor);
}

int
main(int argc, char **argv) {
  ham_status_t st;
  ham_env_t *env = 0;
  ham_db_t *src_db = 0;
  ham_db_t *dest_db = 0;
  ham_u16_t src_name;
  ham_u16_t dest_name;
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
  st = ham_env_open(&env, env_path, 0, 0);
  if (st)
    error("ham_env_open", st);

  /* open the source database */
  st = ham_env_open_db(env, &src_db, src_name, 0, 0);
  if (st)
    error("ham_env_open_db", st);

  /* create the destination database */
  st = ham_env_create_db(env, &dest_db, dest_name,
                  HAM_ENABLE_DUPLICATE_KEYS, 0);
  if (st)
    error("ham_env_create_db", st);

  /* copy the data */
  copy_db(src_db, dest_db);

  /* clean up and return */
  st = ham_env_close(env, HAM_AUTO_CLEANUP);
  if (st)
    error("ham_env_close", st);

  printf("\nsuccess!\n");
  return (0);
}

