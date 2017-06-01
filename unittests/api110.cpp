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

#include <time.h>

#include "3rdparty/catch/catch.hpp"

#include "os.hpp"
#include "fixture.hpp"

#include "1os/os.h"

using namespace upscaledb;

struct APIv110Fixture : BaseFixture {
  APIv110Fixture() {
    require_create(UPS_IN_MEMORY);
  }

  ~APIv110Fixture() {
    close();
  }

  void transactionTest() {
    ups_txn_t *txn = nullptr;
    REQUIRE(UPS_INV_PARAMETER == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(UPS_INV_PARAMETER == ups_txn_abort(txn, 0));

    // reopen the database, check the transaction flag vs. actual
    // use of transactions
    close();
    require_create(UPS_ENABLE_TRANSACTIONS);

    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_txn_abort(txn, 0));
  };

  uint64_t get_param_value(ups_parameter_t *param, uint16_t name) {
    for (; param->name; param++) {
      if (param->name == name)
        return param->value;
    }
    return (uint64_t)-1;
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
      { UPS_PARAM_CACHESIZE, 1024 * 32 },
      { UPS_PARAM_PAGESIZE, 1024 * 64 },
      { 0,0 }
    };

    close();
    require_create(UPS_DISABLE_MMAP, set_params);
    require_parameter(UPS_PARAM_CACHESIZE, 1024 * 32);
    require_parameter(UPS_PARAM_FLAGS, UPS_DISABLE_MMAP);
    require_parameter(UPS_PARAM_FILEMODE, 0644);
    require_filename("test.db");
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
      { UPS_PARAM_CACHESIZE, 1024 * 32 },
      { UPS_PARAM_PAGESIZE, 1024 * 64 },
      { 0,0 }
    };


    close();
    require_create(UPS_DISABLE_MMAP, set_params);
    close();
    require_open(UPS_READ_ONLY);
    require_parameter(UPS_PARAM_CACHE_SIZE, UPS_DEFAULT_CACHE_SIZE);
    require_parameter(UPS_PARAM_PAGE_SIZE, 1024 * 64);
    require_parameter(UPS_PARAM_FLAGS, UPS_READ_ONLY);
    require_parameter(UPS_PARAM_FILEMODE, 0644);
    require_filename("test.db");
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

    close();
    require_create(0, env_params, 0, db_params);
    DbProxy dbp(db);
    dbp.require_parameter(UPS_PARAM_KEYSIZE, 16)
       .require_parameter(UPS_PARAM_DATABASE_NAME, 1)
       .require_parameter(UPS_PARAM_FLAGS, 0);
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

    close();
    require_create(0, env_params, 0, db_params);
    close();
    require_open();
    DbProxy dbp(db);
    dbp.require_parameter(UPS_PARAM_KEYSIZE, 16)
       .require_parameter(UPS_PARAM_DATABASE_NAME, 1)
       .require_parameter(UPS_PARAM_FLAGS, 0);
  }

  void issue7Test() {
    ups_record_t rec1 = {0};
    ups_record_t rec2 = {0};
    ups_txn_t *txn;

    ups_key_t key1 = ups_make_key((void *)"FooBar", 7);
    ups_key_t key2 = ups_make_key((void *)"Foo", 4);

    close();
    require_create(UPS_ENABLE_TRANSACTIONS);

    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == ups_db_insert(db, txn, &key1, &rec1, 0));
    REQUIRE(0 == ups_db_find(db, txn, &key2, &rec2, UPS_FIND_GT_MATCH));
    REQUIRE(0 == ::strcmp((const char *)key2.data, "FooBar"));

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

