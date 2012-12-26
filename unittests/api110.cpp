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
#include <cstring>
#include <time.h>

#include <ham/hamsterdb.h>

#include "../src/db.h"
#include "../src/env.h"
#include "../src/version.h"
#include "../src/serial.h"
#include "../src/btree.h"
#include "../src/btree_stats.h"
#include "../src/os.h"
#include "os.hpp"
#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;
using namespace hamsterdb;


class APIv110Test : public hamsterDB_fixture {
  define_super(hamsterDB_fixture);

public:
  APIv110Test()
    : hamsterDB_fixture("APIv110Test"), m_db(NULL) {
    testrunner::get_instance()->register_fixture(this);
    BFC_REGISTER_TEST(APIv110Test, transactionTest);
    BFC_REGISTER_TEST(APIv110Test, v10xDBformatDetectTest);
    BFC_REGISTER_TEST(APIv110Test, getInitializedEnvParamsTest);
    BFC_REGISTER_TEST(APIv110Test, getInitializedReadonlyEnvParamsTest);
    BFC_REGISTER_TEST(APIv110Test, getInitializedDbParamsTest);
    BFC_REGISTER_TEST(APIv110Test, getInitializedReadonlyDbParamsTest);
    BFC_REGISTER_TEST(APIv110Test, negativeApproxMatchingTest);
    BFC_REGISTER_TEST(APIv110Test, issue7Test);
  }

protected:
  ham_db_t *m_db;
  ham_env_t *m_env;

public:
  virtual void setup() {
    __super::setup();

    os::unlink(BFC_OPATH(".test"));
    BFC_ASSERT_EQUAL(0, ham_env_create(&m_env, 0, HAM_IN_MEMORY, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_env_create_db(m_env, &m_db, 1, 0, 0));
  }

  virtual void teardown() {
    __super::teardown();

    if (m_env)
      BFC_ASSERT_EQUAL(0, ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void transactionTest() {
    ham_txn_t *txn;
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, ham_txn_begin(&txn, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, ham_txn_abort(txn, 0));

    // reopen the database, check the transaction flag vs. actual
    // use of transactions
    teardown();

    BFC_ASSERT_EQUAL(0, ham_env_create(&m_env, BFC_OPATH(".test"),
          HAM_ENABLE_TRANSACTIONS, 0644, 0));
    BFC_ASSERT_EQUAL(0, ham_env_create_db(m_env, &m_db, 1, 0, 0));

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
  };

  void v10xDBformatDetectTest() {
    teardown();
    os::unlink(BFC_OPATH(".test"));

    BFC_ASSERT_EQUAL(true,
      os::copy(BFC_IPATH("data/dupe-endian-test-open-database-be.hdb"),
        BFC_OPATH(".test")));
    BFC_ASSERT_EQUAL(HAM_INV_FILE_VERSION,
        ham_env_open(&m_env, BFC_OPATH(".test"), 0, 0));

    teardown();
    os::unlink(BFC_OPATH(".test"));

    BFC_ASSERT_EQUAL(true,
      os::copy(BFC_IPATH("data/dupe-endian-test-open-database-le.hdb"),
        BFC_OPATH(".test")));
    BFC_ASSERT_EQUAL(HAM_INV_FILE_VERSION,
        ham_env_open(&m_env, BFC_OPATH(".test"), 0, 0));

    teardown();
    os::unlink(BFC_OPATH(".test"));

    /* now the same, environment-based */
    BFC_ASSERT_EQUAL(true,
      os::copy(BFC_IPATH("data/dupe-endian-test-open-database-be.hdb"),
        BFC_OPATH(".test")));
    BFC_ASSERT_EQUAL(HAM_INV_FILE_VERSION,
        ham_env_open(&m_env, BFC_OPATH(".test"), 0, 0));

    teardown();
    os::unlink(BFC_OPATH(".test"));

    BFC_ASSERT_EQUAL(true,
      os::copy(BFC_IPATH("data/dupe-endian-test-open-database-le.hdb"),
        BFC_OPATH(".test")));

    BFC_ASSERT_EQUAL(HAM_INV_FILE_VERSION,
        ham_env_open(&m_env, BFC_OPATH(".test"), 0, 0));
  }

  ham_u64_t get_param_value(ham_parameter_t *param, ham_u16_t name) {
    for (; param->name; param++) {
      if (param->name == name)
        return (param->value);
    }
    return ((ham_u64_t)-1);
  }

  void getInitializedEnvParamsTest() {
    ham_parameter_t params[] = {
      { HAM_PARAM_CACHESIZE, 0 },
      { HAM_PARAM_PAGESIZE, 0 },
      { HAM_PARAM_MAX_DATABASES, 0 },
      { HAM_PARAM_FLAGS, 0 },
      { HAM_PARAM_FILEMODE, 0 },
      { HAM_PARAM_FILENAME, 0 },
      { 0,0 }
    };
    ham_parameter_t set_params[] = {
      { HAM_PARAM_CACHESIZE, 1024*32 },
      { HAM_PARAM_PAGESIZE, 1024*64 },
      { HAM_PARAM_MAX_DATABASES, 32 },
      { 0,0 }
    };

    teardown();
    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"), HAM_DISABLE_MMAP,
                0664, &set_params[0]));

    BFC_ASSERT_EQUAL(0, ham_env_get_parameters(m_env, params));

    BFC_ASSERT_EQUAL(1024 * 32u,
        get_param_value(params, HAM_PARAM_CACHESIZE));
    BFC_ASSERT_EQUAL(1024 * 64u,
        get_param_value(params, HAM_PARAM_PAGESIZE));
    BFC_ASSERT_EQUAL((ham_u64_t)32,
        get_param_value(params, HAM_PARAM_MAX_DATABASES));
    BFC_ASSERT_EQUAL((ham_u64_t)HAM_DISABLE_MMAP,
        get_param_value(params, HAM_PARAM_FLAGS));
    BFC_ASSERT_EQUAL((ham_u64_t)0664,
        get_param_value(params, HAM_PARAM_FILEMODE));
    BFC_ASSERT_EQUAL(0, strcmp(BFC_OPATH(".test"),
        (char *)get_param_value(params, HAM_PARAM_FILENAME)));
  }

  void getInitializedReadonlyEnvParamsTest() {
    ham_parameter_t params[] = {
      { HAM_PARAM_CACHESIZE, 0 },
      { HAM_PARAM_PAGESIZE, 0 },
      { HAM_PARAM_MAX_DATABASES, 0 },
      { HAM_PARAM_FLAGS, 0 },
      { HAM_PARAM_FILEMODE, 0 },
      { HAM_PARAM_FILENAME, 0 },
      { 0,0 }
    };
    ham_parameter_t set_params[] = {
      { HAM_PARAM_CACHESIZE, 1024*32 },
      { HAM_PARAM_PAGESIZE, 1024*64 },
      { HAM_PARAM_MAX_DATABASES, 32 },
      { 0,0 }
    };

    teardown();
    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"), HAM_DISABLE_MMAP,
                0664, &set_params[0]));
    teardown();
    BFC_ASSERT_EQUAL(0,
        ham_env_open(&m_env, BFC_OPATH(".test"), HAM_READ_ONLY, 0));

    BFC_ASSERT_EQUAL(0, ham_env_get_parameters(m_env, params));

    BFC_ASSERT_EQUAL((ham_u64_t)HAM_DEFAULT_CACHESIZE,
        get_param_value(params, HAM_PARAM_CACHESIZE));
    BFC_ASSERT_EQUAL(1024 * 64u,
        get_param_value(params, HAM_PARAM_PAGESIZE));
    BFC_ASSERT_EQUAL((ham_u64_t)32,
        get_param_value(params, HAM_PARAM_MAX_DATABASES));
    BFC_ASSERT_EQUAL((ham_u64_t)HAM_READ_ONLY,
        get_param_value(params, HAM_PARAM_FLAGS));
    BFC_ASSERT_EQUAL((ham_u64_t)0644,
        get_param_value(params, HAM_PARAM_FILEMODE));
    BFC_ASSERT_EQUAL(0, strcmp(BFC_OPATH(".test"),
        (char *)get_param_value(params, HAM_PARAM_FILENAME)));
  }

  void getInitializedDbParamsTest() {
    ham_parameter_t params[] = {
      { HAM_PARAM_KEYSIZE, 0 },
      { HAM_PARAM_DATABASE_NAME, 0 },
      { HAM_PARAM_FLAGS, 0 },
      { HAM_PARAM_MAX_KEYS_PER_PAGE, 0 },
      { 0,0 }
    };

    ham_parameter_t env_params[] = {
      { HAM_PARAM_CACHESIZE, 1024 * 32 },
      { HAM_PARAM_PAGESIZE, 1024 },
      { 0,0 }
    };

    ham_parameter_t db_params[] = {
      { HAM_PARAM_KEYSIZE, 16 },
      { 0,0 }
    };

    teardown();
    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test.db"),
            HAM_CACHE_STRICT, 0644, &env_params[0]));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, &db_params[0]));

    BFC_ASSERT_EQUAL(0, ham_db_get_parameters(m_db, params));
    BFC_ASSERT_EQUAL(16u,
        get_param_value(params, HAM_PARAM_KEYSIZE));
    BFC_ASSERT_EQUAL((ham_u64_t)36,
        get_param_value(params, HAM_PARAM_MAX_KEYS_PER_PAGE));
    BFC_ASSERT_EQUAL((ham_u64_t)1,
        get_param_value(params, HAM_PARAM_DATABASE_NAME));
    BFC_ASSERT_EQUAL((unsigned)HAM_CACHE_STRICT,
        get_param_value(params, HAM_PARAM_FLAGS));
  }

  void getInitializedReadonlyDbParamsTest() {
    ham_parameter_t params[] = {
      { HAM_PARAM_KEYSIZE, 0 },
      { HAM_PARAM_DATABASE_NAME, 0 },
      { HAM_PARAM_FLAGS, 0 },
      { HAM_PARAM_MAX_KEYS_PER_PAGE, 0 },
      { 0,0 }
    };

    ham_parameter_t env_params[] = {
      { HAM_PARAM_CACHESIZE, 1024 * 32 },
      { HAM_PARAM_PAGESIZE, 1024 },
      { 0,0 }
    };

    ham_parameter_t db_params[] = {
      { HAM_PARAM_KEYSIZE, 16 },
      { 0,0 }
    };

    teardown();
    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test.db"),
            HAM_CACHE_STRICT, 0644, &env_params[0]));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, &db_params[0]));

    BFC_ASSERT_EQUAL(0, ham_db_close(m_db, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_open_db(m_env, &m_db, 1, 0, 0));

    BFC_ASSERT_EQUAL(0, ham_db_get_parameters(m_db, params));
    BFC_ASSERT_EQUAL(16u,
        get_param_value(params, HAM_PARAM_KEYSIZE));
    BFC_ASSERT_EQUAL((ham_u64_t)36,
        get_param_value(params, HAM_PARAM_MAX_KEYS_PER_PAGE));
    BFC_ASSERT_EQUAL((ham_u64_t)1,
        get_param_value(params, HAM_PARAM_DATABASE_NAME));
    BFC_ASSERT_EQUAL((unsigned)HAM_CACHE_STRICT,
        get_param_value(params, HAM_PARAM_FLAGS));
  }

  void negativeApproxMatchingTest() {
    ham_key_t key = {};
    ham_cursor_t *cursor;

    teardown();
    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test.db"),
            HAM_ENABLE_TRANSACTIONS, 0644, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_cursor_create(&cursor, m_db, 0, 0));

    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
          ham_cursor_find(cursor, &key, 0, HAM_FIND_GEQ_MATCH));

    BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
  }

  void issue7Test() {
    ham_key_t key1 = {};
    ham_key_t key2 = {};
    ham_record_t rec1 = {};
    ham_record_t rec2 = {};
    ham_txn_t *txn;

    key1.data = (void *)"FooBar";
    key1.size = strlen("FooBar")+1;
    key2.data = (void *)"Foo";
    key2.size = strlen("Foo")+1;

    teardown();
    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test.db"),
            HAM_ENABLE_TRANSACTIONS, 0644, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, 0));

    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, txn, &key1, &rec1, 0));
    BFC_ASSERT_EQUAL(0, ham_db_find(m_db, txn, &key2, &rec2, HAM_FIND_GT_MATCH));
    BFC_ASSERT_EQUAL(0, strcmp((const char *)key2.data, "FooBar"));

    BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
  }
};


BFC_REGISTER_FIXTURE(APIv110Test);

