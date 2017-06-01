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

#include "4db/db.h"

#include "os.hpp"
#include "fixture.hpp"

struct BtreeEraseFixture : BaseFixture {
  uint32_t m_flags;

  BtreeEraseFixture(uint32_t flags = 0)
    : m_flags(flags) {
    require_create(flags);
  }

  void prepare(int num_inserts) {
    ups_key_t key = {};
    ups_record_t rec = {};

    ups_parameter_t p1[] = {
      { UPS_PARAM_PAGESIZE, 1024 },
      { 0, 0 }
    };
    ups_parameter_t p2[] = {
      { UPS_PARAM_KEYSIZE, 80 },
      { 0, 0 }
    };

    close();
    require_create(m_flags, p1, 0, p2);

    char buffer[80] = {0};
    for (int i = 0; i < num_inserts * 10; i += 10) {
      *(int *)&buffer[0] = i;
      key.data = &buffer[0];
      rec.data = &buffer[0];
      key.size = sizeof(buffer);
      rec.size = sizeof(buffer);

      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    }
  }

  void collapseRootTest() {
    ups_key_t key = {};

    prepare(8);

    REQUIRE(UPS_INV_KEY_SIZE == ups_db_erase(db, 0, &key, 0));

    char buffer[80] = {0};
    for (int i = 0; i < 80; i += 10) {
      *(int *)&buffer[0] = i;
      key.data = &buffer[0];
      key.size = sizeof(buffer);

      REQUIRE(0 == ups_db_erase(db, 0, &key, 0));
    }
  }

  void shiftFromRightTest() {
    ups_key_t key = {};
    int i = 0;

    prepare(8);

    char buffer[80] = {0};
    *(int *)&buffer[0] = i;
    key.data = &buffer[0];
    key.size = sizeof(buffer);

    REQUIRE(0 == ups_db_erase(db, 0, &key, 0));
  }

  void shiftFromLeftTest() {
    ups_key_t key = {};
    ups_record_t rec = {};

    prepare(8);

    char buffer[80] = {0};
    *(int *)&buffer[0] = 21;
    key.data = &buffer[0];
    key.size = sizeof(buffer);
    rec.data = &buffer[0];
    rec.size = sizeof(buffer);
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));

    *(int *)&buffer[0] = 22;
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));

    *(int *)&buffer[0] = 23;
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));

    *(int *)&buffer[0] = 70;
    REQUIRE(0 == ups_db_erase(db, 0, &key, 0));

    *(int *)&buffer[0] = 60;
    REQUIRE(0 == ups_db_erase(db, 0, &key, 0));

    *(int *)&buffer[0] = 50;
    REQUIRE(0 == ups_db_erase(db, 0, &key, 0));
  }

  void mergeWithLeftTest() {
    ups_key_t key = {};

    prepare(8);
    char buffer[80] = {0};

    for (int i = 70; i >= 50; i -= 10) {
      *(int *)&buffer[0] = i;
      key.data = &buffer[0];
      key.size = sizeof(buffer);

      REQUIRE(0 == ups_db_erase(db, 0, &key, 0));
    }
  }
};

TEST_CASE("BtreeErase/collapseRootTest", "")
{
  BtreeEraseFixture f;
  f.collapseRootTest();
}

TEST_CASE("BtreeErase/shiftFromRightTest", "")
{
  BtreeEraseFixture f;
  f.shiftFromRightTest();
}

TEST_CASE("BtreeErase/shiftFromLeftTest", "")
{
  BtreeEraseFixture f;
  f.shiftFromLeftTest();
}

TEST_CASE("BtreeErase/mergeWithLeftTest", "")
{
  BtreeEraseFixture f;
  f.mergeWithLeftTest();
}


TEST_CASE("BtreeErase/inmem/collapseRootTest", "")
{
  BtreeEraseFixture f(UPS_IN_MEMORY);
  f.collapseRootTest();
}

TEST_CASE("BtreeErase/inmem/shiftFromRightTest", "")
{
  BtreeEraseFixture f(UPS_IN_MEMORY);
  f.shiftFromRightTest();
}

TEST_CASE("BtreeErase/inmem/shiftFromLeftTest", "")
{
  BtreeEraseFixture f(UPS_IN_MEMORY);
  f.shiftFromLeftTest();
}

TEST_CASE("BtreeErase/inmem/mergeWithLeftTest", "")
{
  BtreeEraseFixture f(UPS_IN_MEMORY);
  f.mergeWithLeftTest();
}

