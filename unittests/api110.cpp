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

#include "../src/config.h"

#include <time.h>

#include "3rdparty/catch/catch.hpp"

#include "globals.h"
#include "os.hpp"

#include "../src/db.h"
#include "../src/env.h"
#include "../src/version.h"
#include "../src/serial.h"
#include "../src/btree_index.h"
#include "../src/btree_stats.h"
#include "../src/os.h"

using namespace hamsterdb;

struct APIv110Fixture {
  ham_db_t *m_db;
  ham_env_t *m_env;

  APIv110Fixture()
    : m_db(0) {
    os::unlink(Globals::opath(".test"));
    REQUIRE(0 == ham_env_create(&m_env, 0, HAM_IN_MEMORY, 0, 0));
    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 1, 0, 0));
  }

  ~APIv110Fixture() {
    teardown();
  }

  void teardown() {
    if (m_env)
      REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void transactionTest() {
    ham_txn_t *txn;
    REQUIRE(HAM_INV_PARAMETER == ham_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(HAM_INV_PARAMETER == ham_txn_abort(txn, 0));

    // reopen the database, check the transaction flag vs. actual
    // use of transactions
    teardown();

    REQUIRE(0 == ham_env_create(&m_env, Globals::opath(".test"),
          HAM_ENABLE_TRANSACTIONS, 0644, 0));
    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 1, 0, 0));

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(0 == ham_txn_abort(txn, 0));
  };

  void v10xDBformatDetectTest() {
#ifndef HAM_OS_WIN32
	teardown();
    os::unlink(Globals::opath(".test"));

    REQUIRE(true ==
      os::copy(Globals::ipath("data/dupe-endian-test-open-database-be.hdb"),
        Globals::opath(".test")));
    REQUIRE(HAM_INV_FILE_VERSION ==
        ham_env_open(&m_env, Globals::opath(".test"), 0, 0));

    teardown();
    os::unlink(Globals::opath(".test"));

    REQUIRE(true ==
      os::copy(Globals::ipath("data/dupe-endian-test-open-database-le.hdb"),
        Globals::opath(".test")));
    REQUIRE(HAM_INV_FILE_VERSION ==
        ham_env_open(&m_env, Globals::opath(".test"), 0, 0));

    teardown();
    os::unlink(Globals::opath(".test"));

    /* now the same, environment-based */
    REQUIRE(true ==
      os::copy(Globals::ipath("data/dupe-endian-test-open-database-be.hdb"),
        Globals::opath(".test")));
    REQUIRE(HAM_INV_FILE_VERSION ==
        ham_env_open(&m_env, Globals::opath(".test"), 0, 0));

    teardown();
    os::unlink(Globals::opath(".test"));

    REQUIRE(true ==
      os::copy(Globals::ipath("data/dupe-endian-test-open-database-le.hdb"),
        Globals::opath(".test")));

    REQUIRE(HAM_INV_FILE_VERSION ==
        ham_env_open(&m_env, Globals::opath(".test"), 0, 0));
#endif
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
      { 0,0 }
    };

    teardown();
    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"), HAM_DISABLE_MMAP,
                0664, &set_params[0]));

    REQUIRE(0 == ham_env_get_parameters(m_env, params));

    REQUIRE(get_param_value(params, HAM_PARAM_CACHESIZE)
                    == (ham_u64_t)(1024 * 32));
    REQUIRE(get_param_value(params, HAM_PARAM_PAGESIZE)
                    == (ham_u64_t)(1024 * 64));
    REQUIRE((ham_u64_t)HAM_DISABLE_MMAP ==
        get_param_value(params, HAM_PARAM_FLAGS));
    REQUIRE((ham_u64_t)0664 ==
        get_param_value(params, HAM_PARAM_FILEMODE));
    REQUIRE(0 == strcmp(Globals::opath(".test"),
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
      { 0,0 }
    };

    teardown();
    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"), HAM_DISABLE_MMAP,
                0664, &set_params[0]));
    teardown();
    REQUIRE(0 ==
        ham_env_open(&m_env, Globals::opath(".test"), HAM_READ_ONLY, 0));

    REQUIRE(0 == ham_env_get_parameters(m_env, params));

    REQUIRE((ham_u64_t)HAM_DEFAULT_CACHESIZE ==
        get_param_value(params, HAM_PARAM_CACHESIZE));
    REQUIRE(get_param_value(params, HAM_PARAM_PAGESIZE)
                    == (ham_u64_t)(1024 * 64));
    REQUIRE((ham_u64_t)HAM_READ_ONLY ==
        get_param_value(params, HAM_PARAM_FLAGS));
    REQUIRE((ham_u64_t)0644 ==
        get_param_value(params, HAM_PARAM_FILEMODE));
    REQUIRE(0 == strcmp(Globals::opath(".test"),
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
    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test.db"),
            0, 0644, &env_params[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, 0, &db_params[0]));

    REQUIRE(0 == ham_db_get_parameters(m_db, params));
    REQUIRE(16u ==
        get_param_value(params, HAM_PARAM_KEYSIZE));
    REQUIRE((ham_u64_t)1 ==
        get_param_value(params, HAM_PARAM_DATABASE_NAME));
    REQUIRE(0u ==
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
    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test.db"),
            0, 0644, &env_params[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, 0, &db_params[0]));

    REQUIRE(0 == ham_db_close(m_db, 0));
    REQUIRE(0 ==
        ham_env_open_db(m_env, &m_db, 1, 0, 0));

    REQUIRE(0 == ham_db_get_parameters(m_db, params));
    REQUIRE(16u ==
        get_param_value(params, HAM_PARAM_KEYSIZE));
    REQUIRE((ham_u64_t)1 ==
        get_param_value(params, HAM_PARAM_DATABASE_NAME));
    REQUIRE((unsigned)0 ==
        get_param_value(params, HAM_PARAM_FLAGS));
  }

  void negativeApproxMatchingTest() {
    ham_cursor_t *cursor;

    teardown();
    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test.db"),
            HAM_ENABLE_TRANSACTIONS, 0644, 0));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
    REQUIRE(0 == ham_cursor_create(&cursor, m_db, 0, 0));

    REQUIRE(0 == ham_cursor_close(cursor));
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
    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test.db"),
            HAM_ENABLE_TRANSACTIONS, 0644, 0));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, 0, 0));

    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(0 == ham_db_insert(m_db, txn, &key1, &rec1, 0));
    REQUIRE(0 == ham_db_find(m_db, txn, &key2, &rec2, HAM_FIND_GT_MATCH));
    REQUIRE(0 == strcmp((const char *)key2.data, "FooBar"));

    REQUIRE(0 == ham_txn_abort(txn, 0));
  }
};


TEST_CASE("APIv110/transactionTest", "")
{
  APIv110Fixture f;
  f.transactionTest();
}

TEST_CASE("APIv110/v10xDBformatDetectTest", "")
{
  APIv110Fixture f;
  f.v10xDBformatDetectTest();
}

TEST_CASE("APIv110/getInitializedEnvParamsTest", "")
{
  APIv110Fixture f;
  f.getInitializedEnvParamsTest();
}

TEST_CASE("APIv110/getInitializedReadonlyEnvParamsTest", "")
{
  APIv110Fixture f;
  f.getInitializedReadonlyEnvParamsTest();
}

TEST_CASE("APIv110/getInitializedDbParamsTest", "")
{
  APIv110Fixture f;
  f.getInitializedDbParamsTest();
}

TEST_CASE("APIv110/getInitializedReadonlyDbParamsTest", "")
{
  APIv110Fixture f;
  f.getInitializedReadonlyDbParamsTest();
}

TEST_CASE("APIv110/negativeApproxMatchingTest", "")
{
  APIv110Fixture f;
  f.negativeApproxMatchingTest();
}

TEST_CASE("APIv110/issue7Test", "")
{
  APIv110Fixture f;
  f.issue7Test();
}

