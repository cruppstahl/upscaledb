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

#include <time.h>

#include "3rdparty/catch/catch.hpp"

#include "utils.h"
#include "os.hpp"

#include "1os/os.h"
#include "3btree/btree_index.h"
#include "3btree/btree_stats.h"
#include "4db/db.h"
#include "4env/env.h"

using namespace upscaledb;

struct APIv110Fixture {
  ups_db_t *m_db;
  ups_env_t *m_env;

  APIv110Fixture()
    : m_db(0) {
    os::unlink(Utils::opath(".test"));
    REQUIRE(0 == ups_env_create(&m_env, 0, UPS_IN_MEMORY, 0, 0));
    REQUIRE(0 == ups_env_create_db(m_env, &m_db, 1, 0, 0));
  }

  ~APIv110Fixture() {
    teardown();
  }

  void teardown() {
    if (m_env)
      REQUIRE(0 == ups_env_close(m_env, UPS_AUTO_CLEANUP));
  }

  void transactionTest() {
    ups_txn_t *txn;
    REQUIRE(UPS_INV_PARAMETER == ups_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(UPS_INV_PARAMETER == ups_txn_abort(txn, 0));

    // reopen the database, check the transaction flag vs. actual
    // use of transactions
    teardown();

    REQUIRE(0 == ups_env_create(&m_env, Utils::opath(".test"),
          UPS_ENABLE_TRANSACTIONS, 0644, 0));
    REQUIRE(0 == ups_env_create_db(m_env, &m_db, 1, 0, 0));

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(0 == ups_txn_abort(txn, 0));
  };

  uint64_t get_param_value(ups_parameter_t *param, uint16_t name) {
    for (; param->name; param++) {
      if (param->name == name)
        return (param->value);
    }
    return ((uint64_t)-1);
  }

  void getInitializedEnvParamsTest() {
    ups_parameter_t params[] = {
      { UPS_PARAM_CACHESIZE, 0 },
      { UPS_PARAM_PAGESIZE, 0 },
      { UPS_PARAM_MAX_DATABASES, 0 },
      { UPS_PARAM_FLAGS, 0 },
      { UPS_PARAM_FILEMODE, 0 },
      { UPS_PARAM_FILENAME, 0 },
      { 0,0 }
    };
    ups_parameter_t set_params[] = {
      { UPS_PARAM_CACHESIZE, 1024*32 },
      { UPS_PARAM_PAGESIZE, 1024*64 },
      { 0,0 }
    };

    teardown();
    REQUIRE(0 ==
        ups_env_create(&m_env, Utils::opath(".test"), UPS_DISABLE_MMAP,
                0664, &set_params[0]));

    REQUIRE(0 == ups_env_get_parameters(m_env, params));

    REQUIRE(get_param_value(params, UPS_PARAM_CACHESIZE)
                    == (uint64_t)(1024 * 32));
    REQUIRE(get_param_value(params, UPS_PARAM_PAGESIZE)
                    == (uint64_t)(1024 * 64));
    REQUIRE((uint64_t)UPS_DISABLE_MMAP ==
        get_param_value(params, UPS_PARAM_FLAGS));
    REQUIRE((uint64_t)0664 ==
        get_param_value(params, UPS_PARAM_FILEMODE));
    REQUIRE(0 == strcmp(Utils::opath(".test"),
        (char *)get_param_value(params, UPS_PARAM_FILENAME)));
  }

  void getInitializedReadonlyEnvParamsTest() {
    ups_parameter_t params[] = {
      { UPS_PARAM_CACHESIZE, 0 },
      { UPS_PARAM_PAGESIZE, 0 },
      { UPS_PARAM_MAX_DATABASES, 0 },
      { UPS_PARAM_FLAGS, 0 },
      { UPS_PARAM_FILEMODE, 0 },
      { UPS_PARAM_FILENAME, 0 },
      { 0,0 }
    };
    ups_parameter_t set_params[] = {
      { UPS_PARAM_CACHESIZE, 1024*32 },
      { UPS_PARAM_PAGESIZE, 1024*64 },
      { 0,0 }
    };

    teardown();
    REQUIRE(0 ==
        ups_env_create(&m_env, Utils::opath(".test"), UPS_DISABLE_MMAP,
                0664, &set_params[0]));
    teardown();
    REQUIRE(0 ==
        ups_env_open(&m_env, Utils::opath(".test"), UPS_READ_ONLY, 0));

    REQUIRE(0 == ups_env_get_parameters(m_env, params));

    REQUIRE((uint64_t)UPS_DEFAULT_CACHE_SIZE ==
        get_param_value(params, UPS_PARAM_CACHE_SIZE));
    REQUIRE(get_param_value(params, UPS_PARAM_PAGE_SIZE)
                    == (uint64_t)(1024 * 64));
    REQUIRE((uint64_t)UPS_READ_ONLY ==
        get_param_value(params, UPS_PARAM_FLAGS));
    REQUIRE((uint64_t)0644 ==
        get_param_value(params, UPS_PARAM_FILEMODE));
    REQUIRE(0 == strcmp(Utils::opath(".test"),
        (char *)get_param_value(params, UPS_PARAM_FILENAME)));
  }

  void getInitializedDbParamsTest() {
    ups_parameter_t params[] = {
      { UPS_PARAM_KEYSIZE, 0 },
      { UPS_PARAM_DATABASE_NAME, 0 },
      { UPS_PARAM_FLAGS, 0 },
      { UPS_PARAM_MAX_KEYS_PER_PAGE, 0 },
      { 0,0 }
    };

    ups_parameter_t env_params[] = {
      { UPS_PARAM_CACHESIZE, 1024 * 32 },
      { UPS_PARAM_PAGESIZE, 1024 },
      { 0,0 }
    };

    ups_parameter_t db_params[] = {
      { UPS_PARAM_KEYSIZE, 16 },
      { 0,0 }
    };

    teardown();
    REQUIRE(0 == ups_env_create(&m_env, Utils::opath(".test.db"),
            0, 0644, &env_params[0]));
    REQUIRE(0 == ups_env_create_db(m_env, &m_db, 1, 0, &db_params[0]));

    REQUIRE(0 == ups_db_get_parameters(m_db, params));
    REQUIRE(16u == get_param_value(params, UPS_PARAM_KEYSIZE));
    REQUIRE((uint64_t)1 == get_param_value(params, UPS_PARAM_DATABASE_NAME));
    REQUIRE(0u == get_param_value(params, UPS_PARAM_FLAGS));
  }

  void getInitializedReadonlyDbParamsTest() {
    ups_parameter_t params[] = {
      { UPS_PARAM_KEYSIZE, 0 },
      { UPS_PARAM_DATABASE_NAME, 0 },
      { UPS_PARAM_FLAGS, 0 },
      { UPS_PARAM_MAX_KEYS_PER_PAGE, 0 },
      { 0,0 }
    };

    ups_parameter_t env_params[] = {
      { UPS_PARAM_CACHESIZE, 1024 * 32 },
      { UPS_PARAM_PAGESIZE, 1024 },
      { 0,0 }
    };

    ups_parameter_t db_params[] = {
      { UPS_PARAM_KEYSIZE, 16 },
      { 0,0 }
    };

    teardown();
    REQUIRE(0 ==
        ups_env_create(&m_env, Utils::opath(".test.db"),
            0, 0644, &env_params[0]));
    REQUIRE(0 ==
        ups_env_create_db(m_env, &m_db, 1, 0, &db_params[0]));

    REQUIRE(0 == ups_db_close(m_db, 0));
    REQUIRE(0 ==
        ups_env_open_db(m_env, &m_db, 1, 0, 0));

    REQUIRE(0 == ups_db_get_parameters(m_db, params));
    REQUIRE(16u == get_param_value(params, UPS_PARAM_KEYSIZE));
    REQUIRE((uint64_t)1 == get_param_value(params, UPS_PARAM_DATABASE_NAME));
    REQUIRE((unsigned)0 == get_param_value(params, UPS_PARAM_FLAGS));
  }

  void negativeApproxMatchingTest() {
    ups_cursor_t *cursor;

    teardown();
    REQUIRE(0 ==
        ups_env_create(&m_env, Utils::opath(".test.db"),
            UPS_ENABLE_TRANSACTIONS, 0644, 0));
    REQUIRE(0 ==
        ups_env_create_db(m_env, &m_db, 1, 0, 0));
    REQUIRE(0 == ups_cursor_create(&cursor, m_db, 0, 0));

    REQUIRE(0 == ups_cursor_close(cursor));
  }

  void issue7Test() {
    ups_key_t key1 = {};
    ups_key_t key2 = {};
    ups_record_t rec1 = {};
    ups_record_t rec2 = {};
    ups_txn_t *txn;

    key1.data = (void *)"FooBar";
    key1.size = strlen("FooBar")+1;
    key2.data = (void *)"Foo";
    key2.size = strlen("Foo")+1;

    teardown();
    REQUIRE(0 ==
        ups_env_create(&m_env, Utils::opath(".test.db"),
            UPS_ENABLE_TRANSACTIONS, 0644, 0));
    REQUIRE(0 ==
        ups_env_create_db(m_env, &m_db, 1, 0, 0));

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(m_db, txn, &key1, &rec1, 0));
    REQUIRE(0 == ups_db_find(m_db, txn, &key2, &rec2, UPS_FIND_GT_MATCH));
    REQUIRE(0 == strcmp((const char *)key2.data, "FooBar"));

    REQUIRE(0 == ups_txn_abort(txn, 0));
  }
};


TEST_CASE("APIv110/transactionTest", "")
{
  APIv110Fixture f;
  f.transactionTest();
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

#include "1base/spinlock.h"

TEST_CASE("APIv110/spinlockTest", "")
{
  upscaledb::Spinlock lock;
  lock.lock();
  REQUIRE(false == lock.try_lock());
  lock.unlock();
  REQUIRE(true == lock.try_lock());
  lock.unlock();
}

