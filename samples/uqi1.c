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
 * This example demonstrates a few simple UQI queries.
 */

#include <stdio.h>
#include <string.h>
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
    uint32_t value = 50 + i % 30;
    ups_key_t key = {0};
    ups_record_t record = ups_make_record(&value, sizeof(value));

    st = ups_db_insert(db, 0, &key, &record, 0);
    if (st != UPS_SUCCESS)
      error("ups_db_insert", st);
  }

  uqi_result_t *result;
  ups_key_t key = {0};
  ups_record_t record = {0};

  /* now calculate the maximum record value */
  st = uqi_select(env, "MAX($record) FROM DATABASE 1", &result);
  if (st != UPS_SUCCESS)
    error("uqi_select", st);
  uqi_result_get_key(result, 0, &key);
  uqi_result_get_record(result, 0, &record);
  printf("MAX($record):     key %2u, record %u\n",
                  *(uint32_t *)key.data, *(uint32_t *)record.data);
  uqi_result_close(result);

  /* ... and the minimum record value */
  st = uqi_select(env, "MIN($record) FROM DATABASE 1", &result);
  if (st != UPS_SUCCESS)
    error("uqi_select", st);
  uqi_result_get_key(result, 0, &key);
  uqi_result_get_record(result, 0, &record);
  printf("MIN($record):     key %2u, record %u\n",
                  *(uint32_t *)key.data, *(uint32_t *)record.data);
  uqi_result_close(result);

  /* ... and the average record value */
  st = uqi_select(env, "AVERAGE($record) FROM DATABASE 1", &result);
  if (st != UPS_SUCCESS)
    error("uqi_select", st);
  uqi_result_get_record(result, 0, &record);
  printf("AVERAGE($record): %f\n", *(double *)record.data);
  uqi_result_close(result);

  /* we're done! close the handles. UPS_AUTO_CLEANUP will also close the
   * 'db' handle */
  st = ups_env_close(env, UPS_AUTO_CLEANUP);
  if (st != UPS_SUCCESS)
    error("ups_env_close", st);

  printf("success!\n");
  return 0;
}

