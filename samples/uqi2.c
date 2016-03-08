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
 * This example demonstrates a more complex UQI queries which applies a
 * user-supplied predicate function to filter data
 */

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h> /* for exit() */

#include <ups/upscaledb.h>
#include <ups/upscaledb_uqi.h>

#define DATABASE_NAME  1

void
error(const char *foo, ups_status_t st)
{
  printf("%s() returned error %d: %s\n", foo, st, ups_strerror(st));
  exit(-1);
}

/*
 * Aggregation plugin: initialization
 * Allocates storage for the aggregated counter.
 */
static void *
mycount_init(int flags, int key_type, uint32_t key_size,
                int record_type, uint32_t record_size, const char *reserved)
{
  uint64_t *count = (uint64_t *)malloc(sizeof(uint64_t));
  *count = 0;
  return count;
}

/*
 * Aggregation plugin: de-initialization.
 * Releases the storage which was allocated in mycount_init
 */
static void
mycount_cleanup(void *state)
{
  free(state);
}

/*
 * Aggregation plugin: aggregates a single value
 * Here we simply increment the counter
 */
static void
mycount_single(void *state, const void *key_data, uint32_t key_size,
                const void *record_data, uint32_t record_size)
{
  *(uint64_t *)state += 1;
}

/*
 * Aggregation plugin: aggregates a list of values
 * Here we simply increase the counter by the number of elements in the list.
 * This function is called for fixed-length data, but never for
 * variable-length data.
 */
static void
mycount_many(void *state, const void *key_data_list,
                const void *record_data_list, size_t list_length)
{
  uint64_t *pcount = (uint64_t *)state;
  *pcount += list_length;
}

/*
 * Aggregation plugin: returns the results
 */
static void
mycount_results(void *state, uqi_result_t *result)
{
  uint64_t *pcount = (uint64_t *)state;
  uqi_result_add_row(result, 0, 0, pcount, sizeof(*pcount));
}

/*
 * Predicate plugin: checks if the record is 10
 */
static int
equals10_predicate(void *state, const void *key_data, uint32_t key_size,
                const void *record_data, uint32_t record_size)
{
  assert(record_size == sizeof(uint32_t));
  return *(uint32_t *)record_data == 10;
}

int
main(int argc, char **argv)
{
  ups_status_t st;             /* status variable */
  ups_env_t *env;              /* upscaledb environment object */
  ups_db_t *db;                /* upscaledb database object */
  ups_parameter_t params[] = { /* parameters for ups_env_create_db */
    {UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT32},
    {UPS_PARAM_RECORD_TYPE, UPS_TYPE_UINT32},
    {0, 0}
  };

  /* First create a new upscaledb Environment */
  st = ups_env_create(&env, "test.db", 0, 0664, 0);
  if (st != UPS_SUCCESS)
    error("ups_env_create", st);

  /* And in this Environment we create a new Database for uint32-keys
   * and uint32-records. */
  st = ups_env_create_db(env, &db, DATABASE_NAME, UPS_RECORD_NUMBER32,
                  &params[0]);
  if (st != UPS_SUCCESS)
    error("ups_env_create_db", st);

  /*
   * now insert a bunch of key/value pairs. The IDs are automatically
   * assigned, the record is a pseudo-random value
   */
  for (int i = 0; i < 10000; i++) {
    uint32_t value = 1 + i % 30;
    ups_key_t key = {0};
    ups_record_t record = ups_make_record(&value, sizeof(value));

    st = ups_db_insert(db, 0, &key, &record, 0);
    if (st != UPS_SUCCESS)
      error("ups_db_insert", st);
  }

  uqi_result_t *result;
  ups_record_t record = {0};

  /* Our query will count all database entries with a record value of 10.
   * For demonstration purposes we will use our own COUNT function (instead
   * of the built-in "COUNT". */

  /* The first plugin will perform the aggregation. */
  uqi_plugin_t agg;
  memset(&agg, 0, sizeof(agg));
  agg.name = "mycount";
  agg.type = UQI_PLUGIN_AGGREGATE;
  agg.init = mycount_init;
  agg.cleanup = mycount_cleanup;
  agg.agg_single = mycount_single;
  agg.agg_many = mycount_many;
  agg.results = mycount_results;
  st = uqi_register_plugin(&agg);
  if (st != UPS_SUCCESS)
    error("uqi_register_plugin", st);

  /* Our second plugin will filter the data: all values != 10 are discarded.
   * This plugin does not keep any state and therefore does not require an
   * init or cleanup function. */
  uqi_plugin_t pred;
  memset(&pred, 0, sizeof(pred));
  pred.type = UQI_PLUGIN_PREDICATE;
  pred.name = "equals10";
  pred.pred = equals10_predicate;
  st = uqi_register_plugin(&pred);
  if (st != UPS_SUCCESS)
    error("uqi_register_plugin", st);


  /* Now run the query */
  st = uqi_select(env, "mycount($record) FROM DATABASE 1 "
                  "WHERE equals10($record)", &result);
  if (st != UPS_SUCCESS)
    error("uqi_select", st);

  /* Our aggregation plugin stores the result as a 64bit variable in the
   * result's record (see mycount_results()) */
  uqi_result_get_record(result, 0, &record);
  printf("mycount($record): %lu\n", *(uint64_t *)record.data);
  uqi_result_close(result);

  /* we're done! close the handles. UPS_AUTO_CLEANUP will also close the
   * 'db' handle */
  st = ups_env_close(env, UPS_AUTO_CLEANUP);
  if (st != UPS_SUCCESS)
    error("ups_env_close", st);

  printf("success!\n");
  return 0;
}

