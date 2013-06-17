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

#include "../src/config.h"

#include "3rdparty/catch/catch.hpp"

#include "globals.h"
#include "os.hpp"

#include "../src/db.h"
#include "../src/version.h"

struct BtreeEraseFixture {
  ham_db_t *m_db;
  ham_env_t *m_env;
  ham_u32_t m_flags;

  BtreeEraseFixture(ham_u32_t flags = 0)
    : m_db(0), m_env(0), m_flags(flags) {
    os::unlink(Globals::opath(".test"));
    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"), m_flags, 0644, 0));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
  }

  ~BtreeEraseFixture() {
    teardown();
  }

  void teardown() {
    if (m_env)
	  REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void prepare(int num_inserts) {
    ham_key_t key = {};
    ham_record_t rec = {};

    ham_parameter_t p1[] = {
      { HAM_PARAM_PAGESIZE, 1024 },
      { 0, 0 }
    };
    ham_parameter_t p2[] = {
      { HAM_PARAM_KEYSIZE, 80 },
      { 0, 0 }
    };

    teardown();
    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"), m_flags, 0644,
            &p1[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, 0, &p2[0]));

    for (int i = 0; i < num_inserts * 10; i += 10) {
      key.data = &i;
      rec.data = &i;
      key.size = sizeof(i);
      rec.size = sizeof(i);

      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    }
  }

  void collapseRootTest() {
    ham_key_t key = {};

    prepare(8);

    REQUIRE(HAM_KEY_NOT_FOUND == ham_db_erase(m_db, 0, &key, 0));

    for (int i = 0; i < 80; i += 10) {
      key.data = &i;
      key.size = sizeof(i);

      REQUIRE(0 == ham_db_erase(m_db, 0, &key, 0));
    }
  }

  void shiftFromRightTest() {
    ham_key_t key = {};
    int i = 0;

    prepare(8);

    key.data = &i;
    key.size = sizeof(i);

    REQUIRE(0 == ham_db_erase(m_db, 0, &key, 0));
  }

  void shiftFromLeftTest() {
    ham_key_t key = {};
    ham_record_t rec = {};
    int i;

    prepare(8);

    i = 21;
    key.data = &i;
    key.size = sizeof(i);
    rec.data = &i;
    rec.size = sizeof(i);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    i = 22;
    key.data = &i;
    key.size = sizeof(i);
    rec.data = &i;
    rec.size = sizeof(i);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    i = 23;
    key.data = &i;
    key.size = sizeof(i);
    rec.data = &i;
    rec.size = sizeof(i);
    REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));

    i = 70;
    key.data = &i;
    key.size = sizeof(i);
    REQUIRE(0 == ham_db_erase(m_db, 0, &key, 0));

    i = 60;
    key.data = &i;
    key.size = sizeof(i);
    REQUIRE(0 == ham_db_erase(m_db, 0, &key, 0));

    i = 50;
    key.data = &i;
    key.size = sizeof(i);
    REQUIRE(0 == ham_db_erase(m_db, 0, &key, 0));
  }

  void mergeWithLeftTest() {
    ham_key_t key = {};

    prepare(8);

    for (int i = 70; i >= 50; i -= 10) {
      key.data = &i;
      key.size = sizeof(i);

      REQUIRE(0 == ham_db_erase(m_db, 0, &key, 0));
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
  BtreeEraseFixture f(HAM_IN_MEMORY);
  f.collapseRootTest();
}

TEST_CASE("BtreeErase-inmem/shiftFromRightTest", "")
{
  BtreeEraseFixture f(HAM_IN_MEMORY);
  f.shiftFromRightTest();
}

TEST_CASE("BtreeErase-inmem/shiftFromLeftTest", "")
{
  BtreeEraseFixture f(HAM_IN_MEMORY);
  f.shiftFromLeftTest();
}

TEST_CASE("BtreeErase-inmem/mergeWithLeftTest", "")
{
  BtreeEraseFixture f(HAM_IN_MEMORY);
  f.mergeWithLeftTest();
}

