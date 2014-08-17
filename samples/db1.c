/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h> /* for exit() */
#include <time.h> /* for time() */
#include <ham/hamsterdb.h>

// this is the "name" of our database
#define DATABASE_NAME        1

// maximum length of a node name
#define MAX_NODE_NAME      100

// temporary buffer to create the keys. because of this buffer, the functions
// below are not threadsafe!
static char buffer[MAX_NODE_NAME + 1 + sizeof(time_t)];

// error handler; for demonstration purposes. Simply terminates in case
// of an error
static void
error(const char *foo, ham_status_t st) {
  printf("%s() returned error %d: %s\n", foo, st, ham_strerror(st));
  exit(-1);
}

// Function which creates a key structure. It first adds the zero-terminated
// node_name, then appends the time.
//
// As already discussed it would be VERY good if the node name would have fixed
// length, i.e. a crc32 or an integer identifier.
static void
make_key(ham_key_t *key, time_t time, const char *node_name) {
  size_t node_name_length = strlen(node_name);
  strcpy(buffer, node_name);
  *(time_t *)&buffer[node_name_length] = time;
  key->data = (void *)&buffer[0];
  key->size = node_name_length + 4;
}

// Stores a network event in the database; has some TODOs for you!
static void
store_event(ham_cursor_t *cursor, time_t time, const char *node_name,
        const char *event_data, size_t event_data_size) {
  // extract year, month, day, hour, minute from the timestamp
  struct tm *t = gmtime(&time);
  t->tm_sec = 0;
  time = mktime(t);

  ham_key_t key = {0};
  make_key(&key, time, node_name);
  ham_record_t record = {0};
  record.data = (void *)event_data;
  record.size = (ham_u32_t)event_data_size;

  // now perform the insert; this will fail with HAM_DUPLICATE_KEY if the
  // key already exists
  ham_status_t st = ham_cursor_insert(cursor, &key, &record, 0);

  if (st == HAM_DUPLICATE_KEY) {
    // key already exists - place a cursor on the key
    ham_record_t existing_record = {0};
    st = ham_cursor_find(cursor, &key, &existing_record, 0);
    if (st != HAM_SUCCESS)
      error("ham_cursor_find", st);
    // TODO #1: unpack the protobuf encoded data in 'existing_record'
    // TODO #2: accumulate/merge the data in 'existing_record' with the
    //              data in 'record'
    ham_record_t new_record = {0};
    // TODO #3: pack the accumulated data with protobuf and store it in
    //              'new_record'

    // Now overwrite the old data with the accumulated one
    st = ham_cursor_overwrite(cursor, &new_record, 0);
    if (st != HAM_SUCCESS)
      error("ham_cursor_overwrite", st);
  }
}

int
main(int argc, char **argv) {
  ham_status_t st;             /* status variable */
  ham_env_t *env;              /* hamsterdb environment object */
  ham_db_t *db;                /* hamsterdb database object */

  /* First create a new hamsterdb Environment */
  st = ham_env_create(&env, "test.db", 0, 0664, 0);
  if (st != HAM_SUCCESS)
    error("ham_env_create", st);

  /* And in this Environment we create a new Database */
  st = ham_env_create_db(env, &db, DATABASE_NAME, 0, 0);
  if (st != HAM_SUCCESS)
    error("ham_env_create_db", st);

  /* if a new event arrives we first try to insert it; if this fails (because
   * the event already exists) then place a cursor on the event, accumulate
   * the data and then overwrite the key
   */
  ham_cursor_t *cursor;
  st = ham_cursor_create(&cursor, db, 0, 0);
  if (st != HAM_SUCCESS)
    error("ham_cursor_create", st);

  // Assuming that we now receive a network event from a certain node
  // then we just store it in the database
  const char *event_data = "12345"; // this is the data from the network
  size_t event_data_size = 6; // the size of the received data
  time_t now = time(0);
  store_event(cursor, now, "testnode1", event_data, event_data_size);

  // Assume that a second event is retrieved, then the data is accumulated
  store_event(cursor, now, "testnode1", event_data, event_data_size);

  // And now another event from a different node is returned
  store_event(cursor, now, "testnode2", event_data, event_data_size);

  // Two nodes have sent data; you can use the 'ham_dump' tool from the
  // hamsterdb distribution to print all key/record pairs. You will see that
  // it only contains two key/record pairs: one for testnode1, another one
  // for testnode2.

  /* we're done! close the handles. */
  st = ham_cursor_close(cursor);
  if (st != HAM_SUCCESS)
    error("ham_cursor_close", st);
  st = ham_db_close(db, 0);
  if (st != HAM_SUCCESS)
    error("ham_db_close", st);
  st = ham_env_close(env, 0);
  if (st != HAM_SUCCESS)
    error("ham_env_close", st);

  printf("success!\n");
  return (0);
}

