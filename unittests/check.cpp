/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
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

    for (int i = 0; i < 100; i++) {
      key.size = sizeof(i);
      key.data = &i;
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    }

    REQUIRE(0 == ham_db_check_integrity(m_db, 0));
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

