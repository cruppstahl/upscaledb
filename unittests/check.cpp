/**
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include "../src/config.h"

#include <stdexcept>
#include <string.h>
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "os.hpp"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;

class CheckIntegrityTest : public hamsterDB_fixture
{
  define_super(hamsterDB_fixture);

public:
  CheckIntegrityTest(bool inmemorydb = false,
      const char *name = "CheckIntegrityTest")
    : hamsterDB_fixture(name), m_inmemory(inmemorydb) {
    testrunner::get_instance()->register_fixture(this);
    BFC_REGISTER_TEST(CheckIntegrityTest, emptyDatabaseTest);
    BFC_REGISTER_TEST(CheckIntegrityTest, smallDatabaseTest);
    BFC_REGISTER_TEST(CheckIntegrityTest, levelledDatabaseTest);
  }

protected:
  ham_db_t *m_db;
  ham_env_t *m_env;
  bool m_inmemory;

public:
  virtual void setup() {
    __super::setup();

    os::unlink(BFC_OPATH(".test"));

    BFC_ASSERT_EQUAL(0,
      ham_env_create(&m_env, BFC_OPATH(".test"),
          m_inmemory ? HAM_IN_MEMORY : 0, 0644, 0));
    BFC_ASSERT_EQUAL(0, ham_env_create_db(m_env, &m_db, 33, 0, 0));
  }

  virtual void teardown() {
    __super::teardown();

    BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    BFC_ASSERT_EQUAL(0, ham_env_close(m_env, 0));
  }

  void emptyDatabaseTest() {
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_check_integrity(0, 0));
    BFC_ASSERT_EQUAL(0,
        ham_check_integrity(m_db, 0));
  }

  void smallDatabaseTest() {
    ham_key_t key = {};
    ham_record_t rec = {};

    for (int i = 0; i < 5; i++) {
      key.size = sizeof(i);
      key.data = &i;
      BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));
    }

    BFC_ASSERT_EQUAL(0, ham_check_integrity(m_db, 0));
  }

  void levelledDatabaseTest() {
    ham_key_t key = {};
    ham_record_t rec = {};

    ham_parameter_t env_params[] = {
      { HAM_PARAM_PAGESIZE, 1024 },
      { 0, 0 }
    };
    ham_parameter_t db_params[] = {
      { HAM_PARAM_KEYSIZE, 128 },
      { 0, 0 }
    };

    teardown();
    BFC_ASSERT_EQUAL(0,
      ham_env_create(&m_env, BFC_OPATH(".test"),
          m_inmemory ? HAM_IN_MEMORY : 0, 0644, env_params));
    BFC_ASSERT_EQUAL(0, ham_env_create_db(m_env, &m_db, 33, 0, db_params));

    for (int i = 0; i < 100; i++) {
      key.size = sizeof(i);
      key.data = &i;
      BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));
    }

    BFC_ASSERT_EQUAL(0, ham_check_integrity(m_db, 0));
  }
};

class InMemoryCheckIntegrityTest : public CheckIntegrityTest
{
public:
  InMemoryCheckIntegrityTest()
    : CheckIntegrityTest(true, "InMemoryCheckIntegrityTest") {
  }
};

BFC_REGISTER_FIXTURE(CheckIntegrityTest);
BFC_REGISTER_FIXTURE(InMemoryCheckIntegrityTest);

