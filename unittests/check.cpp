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

#include "3rdparty/catch/catch.hpp"

#include "utils.h"
#include "os.hpp"

struct CheckIntegrityFixture {
  CheckIntegrityFixture(bool inmemory = false, ups_parameter_t *env_params = 0,
                  ups_parameter_t *db_params = 0)
    : m_inmemory(inmemory) {
    setup(env_params, db_params);
  }

  ~CheckIntegrityFixture() {
    teardown();
  }

  void setup(ups_parameter_t *env_params, ups_parameter_t *db_params) {
    os::unlink(Utils::opath(".test"));
    REQUIRE(0 ==
      ups_env_create(&m_env, Utils::opath(".test"),
          m_inmemory ? UPS_IN_MEMORY : 0, 0644, env_params));
    REQUIRE(0 == ups_env_create_db(m_env, &m_db, 33, 0, db_params));
  } 

  void teardown() {
    REQUIRE(0 == ups_db_close(m_db, 0));
    REQUIRE(0 == ups_env_close(m_env, 0));
  }

  bool m_inmemory;
  ups_db_t *m_db;
  ups_env_t *m_env;

  void emptyDatabaseTest() {
    REQUIRE(UPS_INV_PARAMETER == ups_db_check_integrity(0, 0));
    REQUIRE(0 == ups_db_check_integrity(m_db, 0));
  }

  void smallDatabaseTest() {
    ups_key_t key = {};
    ups_record_t rec = {};

    for (int i = 0; i < 5; i++) {
      key.size = sizeof(i);
      key.data = &i;
      REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, 0));
    }

    REQUIRE(0 == ups_db_check_integrity(m_db, 0));
  }

  void levelledDatabaseTest() {
    ups_key_t key = {};
    ups_record_t rec = {};

    ups_parameter_t env_params[] = {
      { UPS_PARAM_PAGESIZE, 1024 },
      { 0, 0 }
    };
    ups_parameter_t db_params[] = {
      { UPS_PARAM_KEYSIZE, 80 },
      { 0, 0 }
    };

    teardown();
    setup(env_params, db_params);

    char buffer[80] = {0};
    for (int i = 0; i < 100; i++) {
      *(int *)&buffer[0] = i;
      key.size = sizeof(buffer);
      key.data = &buffer[0];

      REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, 0));
      REQUIRE(0 == ups_db_check_integrity(m_db, 0));
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

