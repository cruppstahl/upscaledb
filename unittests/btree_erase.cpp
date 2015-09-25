/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

#include "4db/db.h"

struct BtreeEraseFixture {
  ups_db_t *m_db;
  ups_env_t *m_env;
  uint32_t m_flags;

  BtreeEraseFixture(uint32_t flags = 0)
    : m_db(0), m_env(0), m_flags(flags) {
    os::unlink(Utils::opath(".test"));
    REQUIRE(0 ==
        ups_env_create(&m_env, Utils::opath(".test"), m_flags, 0644, 0));
    REQUIRE(0 ==
        ups_env_create_db(m_env, &m_db, 1, 0, 0));
  }

  ~BtreeEraseFixture() {
    teardown();
  }

  void teardown() {
    if (m_env)
	  REQUIRE(0 == ups_env_close(m_env, UPS_AUTO_CLEANUP));
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

    teardown();
    REQUIRE(0 ==
        ups_env_create(&m_env, Utils::opath(".test"), m_flags, 0644,
            &p1[0]));
    REQUIRE(0 ==
        ups_env_create_db(m_env, &m_db, 1, 0, &p2[0]));

    char buffer[80] = {0};
    for (int i = 0; i < num_inserts * 10; i += 10) {
      *(int *)&buffer[0] = i;
      key.data = &buffer[0];
      rec.data = &buffer[0];
      key.size = sizeof(buffer);
      rec.size = sizeof(buffer);

      REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, 0));
    }
  }

  void collapseRootTest() {
    ups_key_t key = {};

    prepare(8);

    REQUIRE(UPS_INV_KEY_SIZE == ups_db_erase(m_db, 0, &key, 0));

    char buffer[80] = {0};
    for (int i = 0; i < 80; i += 10) {
      *(int *)&buffer[0] = i;
      key.data = &buffer[0];
      key.size = sizeof(buffer);

      REQUIRE(0 == ups_db_erase(m_db, 0, &key, 0));
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

    REQUIRE(0 == ups_db_erase(m_db, 0, &key, 0));
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
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, 0));

    *(int *)&buffer[0] = 22;
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, 0));

    *(int *)&buffer[0] = 23;
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, 0));

    *(int *)&buffer[0] = 70;
    REQUIRE(0 == ups_db_erase(m_db, 0, &key, 0));

    *(int *)&buffer[0] = 60;
    REQUIRE(0 == ups_db_erase(m_db, 0, &key, 0));

    *(int *)&buffer[0] = 50;
    REQUIRE(0 == ups_db_erase(m_db, 0, &key, 0));
  }

  void mergeWithLeftTest() {
    ups_key_t key = {};

    prepare(8);
    char buffer[80] = {0};

    for (int i = 70; i >= 50; i -= 10) {
      *(int *)&buffer[0] = i;
      key.data = &buffer[0];
      key.size = sizeof(buffer);

      REQUIRE(0 == ups_db_erase(m_db, 0, &key, 0));
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


TEST_CASE("BtreeErase-inmem/collapseRootTest", "")
{
  BtreeEraseFixture f(UPS_IN_MEMORY);
  f.collapseRootTest();
}

TEST_CASE("BtreeErase-inmem/shiftFromRightTest", "")
{
  BtreeEraseFixture f(UPS_IN_MEMORY);
  f.shiftFromRightTest();
}

TEST_CASE("BtreeErase-inmem/shiftFromLeftTest", "")
{
  BtreeEraseFixture f(UPS_IN_MEMORY);
  f.shiftFromLeftTest();
}

TEST_CASE("BtreeErase-inmem/mergeWithLeftTest", "")
{
  BtreeEraseFixture f(UPS_IN_MEMORY);
  f.mergeWithLeftTest();
}

