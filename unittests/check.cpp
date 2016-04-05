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
#include "fixture.hpp"

struct CheckIntegrityFixture : BaseFixture {
  CheckIntegrityFixture(bool inmemory = false, ups_parameter_t *env_params = 0,
                  ups_parameter_t *db_params = 0) {
    require_create(inmemory ? UPS_IN_MEMORY : 0, env_params,
                    0, db_params);
  }

  ~CheckIntegrityFixture() {
    close();
  }

  void emptyDatabaseTest() {
    REQUIRE(UPS_INV_PARAMETER == ups_db_check_integrity(0, 0));
    REQUIRE(0 == ups_db_check_integrity(db, 0));
  }

  void smallDatabaseTest() {
    DbProxy dbp(db);

    for (uint32_t i = 0; i < 5; i++)
      dbp.require_insert(i, 0);

    dbp.require_check_integrity();
  }

  void levelledDatabaseTest() {
    ups_parameter_t env_params[] = {
        { UPS_PARAM_PAGESIZE, 1024 },
        { 0, 0 }
    };
    ups_parameter_t db_params[] = {
        { UPS_PARAM_KEYSIZE, 80 },
        { 0, 0 }
    };

    close();
    require_create(0, env_params, 0, db_params);
    DbProxy dbp(db);

    std::vector<uint8_t> kvec(80);
    std::vector<uint8_t> rvec;
    for (int i = 0; i < 100; i++) {
      *(int *)&kvec[0] = i;
      dbp.require_insert(kvec, rvec)
         .require_check_integrity();
    }
  }
};

TEST_CASE("CheckIntegrity/disk/emptyDatabaseTest", "")
{
  CheckIntegrityFixture f;
  f.emptyDatabaseTest();
}

TEST_CASE("CheckIntegrity/disk/smallDatabaseTest", "")
{
  CheckIntegrityFixture f;
  f.smallDatabaseTest();
}

TEST_CASE("CheckIntegrity/disk/levelledDatabaseTest", "")
{
  CheckIntegrityFixture f;
  f.levelledDatabaseTest();
}

TEST_CASE("CheckIntegrity/inmem/emptyDatabaseTest", "")
{
  CheckIntegrityFixture f(true);
  f.emptyDatabaseTest();
}

TEST_CASE("CheckIntegrity/inmem/smallDatabaseTest", "")
{
  CheckIntegrityFixture f(true);
  f.smallDatabaseTest();
}

TEST_CASE("CheckIntegrity/inmem/levelledDatabaseTest", "")
{
  CheckIntegrityFixture f(true);
  f.levelledDatabaseTest();
}

