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
 * A simple example, which creates a database, inserts some values,
 * looks them up and erases them. Uses the C++ api.
 */

#include <iostream>
#include <ham/hamsterdb.hpp>

#define LOOP 10

int
run_demo() {
  ham_u32_t i;
  hamsterdb::env env;          /* hamsterdb environment object */
  hamsterdb::db db;            /* hamsterdb database object */
  hamsterdb::key key;          /* a key */
  hamsterdb::record record;    /* a record */
  ham_parameter_t params[] = { /* parameters for ham_env_create_db */
    {HAM_PARAM_KEY_TYPE, HAM_TYPE_UINT32},
    {HAM_PARAM_RECORD_SIZE, sizeof(ham_u32_t)},
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
   * for db::find(), we could use the flag HAM_RECORD_USER_ALLOC, if WE
   * allocate record.data (otherwise the memory is automatically allocated
   * by hamsterdb)
   */
  for (i = 0; i < LOOP; i++) {
    key.set_size(sizeof(i));
    key.set_data(&i);

    record = db.find(&key);

    /* Check if the value is ok */
    if (*(ham_u32_t *)record.get_data() != i) {
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
   * now fail with HAM_KEY_NOT_FOUND
   */
  for (i = 0; i < LOOP; i++) {
    key.set_size(sizeof(i));
    key.set_data(&i);

    try {
      record = db.find(&key);
    }
    catch (hamsterdb::error &e) {
      if (e.get_errno() != HAM_KEY_NOT_FOUND) {
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
  try {
    return (run_demo());
  }
  catch (hamsterdb::error &e) {
    std::cerr << "run_demo() failed with unexpected error "
          << e.get_errno() << " ('"
          << e.get_string() << "')" << std::endl;
    return (-1);
  }
}
