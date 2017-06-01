/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

#include "3rdparty/catch/catch.hpp"

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

