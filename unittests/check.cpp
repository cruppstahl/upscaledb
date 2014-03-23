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

#include "3rdparty/catch/catch.hpp"

#include "globals.h"
#include "os.hpp"

struct CheckIntegrityFixture {
  CheckIntegrityFixture(bool inmemory = false, ham_parameter_t *env_params = 0,
                  ham_parameter_t *db_params = 0)
    : m_inmemory(inmemory) {
    setup(env_params, db_params);
  }

  ~CheckIntegrityFixture() {
    teardown();
  }

  void setup(ham_parameter_t *env_params, ham_parameter_t *db_params) {
    os::unlink(Globals::opath(".test"));
    REQUIRE(0 ==
      ham_env_create(&m_env, Globals::opath(".test"),
          m_inmemory ? HAM_IN_MEMORY : 0, 0644, env_params));
    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 33, 0, db_params));
  } 

  void teardown() {
    REQUIRE(0 == ham_db_close(m_db, 0));
    REQUIRE(0 == ham_env_close(m_env, 0));
  }

  bool m_inmemory;
  ham_db_t *m_db;
  ham_env_t *m_env;

  void emptyDatabaseTest() {
    REQUIRE(HAM_INV_PARAMETER == ham_db_check_integrity(0, 0));
    REQUIRE(0 == ham_db_check_integrity(m_db, 0));
  }

  void smallDatabaseTest() {
    ham_key_t key = {};
    ham_record_t rec = {};

    for (int i = 0; i < 5; i++) {
      key.size = sizeof(i);
      key.data = &i;
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    }

    REQUIRE(0 == ham_db_check_integrity(m_db, 0));
  }

  void levelledDatabaseTest() {
    ham_key_t key = {};
    ham_record_t rec = {};

    ham_parameter_t env_params[] = {
      { HAM_PARAM_PAGESIZE, 1024 },
      { 0, 0 }
    };
    ham_parameter_t db_params[] = {
      { HAM_PARAM_KEYSIZE, 80 },
      { 0, 0 }
    };

    teardown();
    setup(env_params, db_params);

    char buffer[80] = {0};
    for (int i = 0; i < 100; i++) {
      *(int *)&buffer[0] = i;
      key.size = sizeof(buffer);
      key.data = &buffer[0];

      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
      REQUIRE(0 == ham_db_check_integrity(m_db, 0));
    }
  }
};

TEST_CASE("CheckIntegrity-disk/emptyDatabaseTest",
          "Runs integrity check on an empty database")
{
  CheckIntegrityFixture f;
  f.emptyDatabaseTest();
}

TEST_CASE("CheckIntegrity-disk/smallDatabaseTest",
          "Runs integrity check on a small database")
{
  CheckIntegrityFixture f;
  f.smallDatabaseTest();
}

TEST_CASE("CheckIntegrity-disk/levelledDatabaseTest",
          "Runs integrity check on a database with multiple btree levels")
{
  CheckIntegrityFixture f;
  f.levelledDatabaseTest();
}

TEST_CASE("CheckIntegrity-inmem/emptyDatabaseTest",
          "Runs integrity check on an empty database")
{
  CheckIntegrityFixture f(true);
  f.emptyDatabaseTest();
}

TEST_CASE("CheckIntegrity-inmem/smallDatabaseTest",
          "Runs integrity check on a small database")
{
  CheckIntegrityFixture f(true);
  f.smallDatabaseTest();
}

TEST_CASE("CheckIntegrity-inmem/levelledDatabaseTest",
          "Runs integrity check on a database with multiple btree levels")
{
  CheckIntegrityFixture f(true);
  f.levelledDatabaseTest();
}

