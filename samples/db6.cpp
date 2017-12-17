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
 * A simple example, which creates a database, inserts some values,
 * looks them up and erases them. Uses the C++ api.
 */

#include <iostream>
#include <ups/upscaledb.hpp>

#define LOOP 10

int
run_demo() {
  uint32_t i;
  upscaledb::env env;          /* upscaledb environment object */
  upscaledb::db db;            /* upscaledb database object */
  upscaledb::key key;          /* a key */
  upscaledb::record record;    /* a record */
  ups_parameter_t params[] = { /* parameters for ups_env_create_db */
    {UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT32},
    {UPS_PARAM_RECORD_SIZE, sizeof(uint32_t)},
    {0, }
  };

  /* Create a new environment file and a database in this environment */
  env.create("test.db");
  db = env.create_db(1, 0, &params[0]);

  /*
   * Now we can insert, delete or lookup values in the database
   *
   * for our test program, we just insert a few values, then look them
   * up, then delete them and try to look them up again (which will fail).
   */
  for (i = 0; i < LOOP; i++) {
    key.set_size(sizeof(i));
    key.set_data(&i);

    record.set_size(sizeof(i));
    record.set_data(&i);

    db.insert(&key, &record);
  }

  /*
   * Now lookup all values
   *
   * for db::find(), we could use the flag UPS_RECORD_USER_ALLOC, if WE
   * allocate record.data (otherwise the memory is automatically allocated
   * by upscaledb)
   */
  for (i = 0; i < LOOP; i++) {
    key.set_size(sizeof(i));
    key.set_data(&i);

    record = db.find(&key);

    /* Check if the value is ok */
    if (*(uint32_t *)record.get_data() != i) {
      std::cerr << "db::find() ok, but returned bad value" << std::endl;
      return (-1);
    }
  }

  /*
   * close the database handle, then re-open it (just to demonstrate how
   * to open a database file)
   */
  db.close();
  env.close();
  env.open("test.db");
  db = env.open_db(1);

  /* now erase all values */
  for (i = 0; i < LOOP; i++) {
    key.set_size(sizeof(i));
    key.set_data(&i);

    db.erase(&key);
  }

  /*
   * Once more we try to find all values. Every db::find() call must
   * now fail with UPS_KEY_NOT_FOUND
   */
  for (i = 0; i < LOOP; i++) {
    key.set_size(sizeof(i));
    key.set_data(&i);

    try {
      record = db.find(&key);
    }
    catch (upscaledb::error &e) {
      if (e.get_errno() != UPS_KEY_NOT_FOUND) {
        std::cerr << "db::find() returned error " << e.get_string()
              << std::endl;
        return (-1);
      }
    }
  }

  /*
   * Done! No need to close the database handles, they are closed in their
   * destructor.
   */

  std::cout << "success!" << std::endl;
  return (0);
}

int
main(int argc, char **argv)
{
    ups_env_t* env;
    ups_env_create(&env, "test.db", UPS_ENABLE_TRANSACTIONS, 0664, 0);

    ups_parameter_t params[] = {
    {UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT32},
    {0, }
    };

    ups_txn_t* txn;
    ups_txn_begin(&txn, env, 0, 0, 0);

    ups_db_t* db;
    ups_env_create_db(env, &db, 1, 0, &params[0]);

    for (int i = 0; i < 4; i++)
    {
        ups_key_t key = ups_make_key(&i, sizeof(i));
        ups_record_t record = {0};

        ups_db_insert(db, txn, &key, &record, 0);
    }

    uint64_t size;
    ups_db_count(db,0,0,&size);
    std::cout << size << std::endl;

    ups_txn_commit(txn, 0);

    return 0;
}
