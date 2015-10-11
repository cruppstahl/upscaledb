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

#include "ups/upscaledb_uqi.h"

#include "3btree/btree_index.h"
#include "4context/context.h"
#include "4db/db_local.h"
#include "4env/env_local.h"

#include "utils.h"
#include "os.hpp"

namespace upscaledb {

// only select even numbers
static ups_bool_t
sum_if_predicate(const void *key_data, uint16_t key_size, void *context)
{
  uint32_t *p = (uint32_t *)key_data;
  return ((*p & 1) == 0);
}

// only select numbers < 10
static ups_bool_t
average_if_predicate(const void *key_data, uint16_t key_size, void *context)
{
  float *p = (float *)key_data;
  return (*p < 10.f);
}

static ups_bool_t
count_if_predicate(const void *key_data, uint16_t key_size, void *context)
{
  uint8_t *p = (uint8_t *)key_data;
  return (*p & 1);
}

struct HolaFixture {
  ups_db_t *m_db;
  ups_env_t *m_env;
  bool m_use_transactions;

  HolaFixture(bool use_transactions, int type, bool use_duplicates = false)
    : m_use_transactions(use_transactions) {
    os::unlink(Utils::opath(".test"));
    ups_parameter_t params[] = {
        {UPS_PARAM_KEY_TYPE, (uint64_t)type},
        {0, 0}
    };
    REQUIRE(0 == ups_env_create(&m_env, ".test",
                            use_transactions
                                ? UPS_ENABLE_TRANSACTIONS
                                : 0,
                            0, 0));
    REQUIRE(0 == ups_env_create_db(m_env, &m_db, 1,
                            use_duplicates
                                ? UPS_ENABLE_DUPLICATES
                                : 0, &params[0]));
  }

  ~HolaFixture() {
    teardown();
  }

  void teardown() {
    if (m_env)
      REQUIRE(0 == ups_env_close(m_env, UPS_AUTO_CLEANUP));
    m_env = 0;
    m_db = 0;
  }

  void sumTest(int count) {
    ups_key_t key = {0};
    ups_record_t record = {0};
    uint32_t sum = 0;

    ups_txn_t *txn = 0;
    if (m_use_transactions)
      REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    // insert a few keys
    for (int i = 0; i < count; i++) {
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ups_db_insert(m_db, txn, &key, &record, 0));
      sum += i;
    }

    uqi_result_t result;
    REQUIRE(0 == uqi_sum(m_db, txn, &result));
    REQUIRE(result.type == UPS_TYPE_UINT64);
    REQUIRE(result.u.result_u64 == sum);

    if (m_use_transactions)
      ups_txn_abort(txn, 0);
  }

  ups_status_t insertBtree(uint32_t key) {
    ups_key_t k = {0};
    k.data = &key;
    k.size = sizeof(key);
    ups_record_t r = {0};

    Context context((LocalEnvironment *)m_env, 0, 0);

    BtreeIndex *be = ((LocalDatabase *)m_db)->btree_index();
    return (be->insert(&context, 0, &k, &r, 0));
  }

  ups_status_t insertTxn(ups_txn_t *txn, uint32_t key) {
    ups_key_t k = {0};
    k.data = &key;
    k.size = sizeof(key);
    ups_record_t r = {0};

    return (ups_db_insert(m_db, txn, &k, &r, 0));
  }

  // tests the following sequences:
  // btree
  // btree, txn
  // btree, txn, btree
  // btree, txn, btree, txn
  // btree, txn, btree, txn, btree
  void sumMixedTest() {
    uint32_t sum = 0;
    ups_txn_t *txn = 0;
    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    // insert a few keys
    REQUIRE(0 == insertBtree(1));  sum += 1;
    REQUIRE(0 == insertBtree(2));  sum += 2;
    REQUIRE(0 == insertBtree(3));  sum += 3;

    // check the sum
    uqi_result_t result;
    REQUIRE(0 == uqi_sum(m_db, txn, &result));
    REQUIRE(result.type == UPS_TYPE_UINT64);
    REQUIRE(result.u.result_u64 == sum);

    // continue with more keys
    REQUIRE(0 == insertTxn(txn, 4));  sum += 4;
    REQUIRE(0 == insertTxn(txn, 5));  sum += 5;
    REQUIRE(0 == insertTxn(txn, 6));  sum += 6;

    // check the sum
    REQUIRE(0 == uqi_sum(m_db, txn, &result));
    REQUIRE(result.u.result_u64 == sum);

    // continue inserting keys
    REQUIRE(0 == insertBtree(7));  sum += 7;
    REQUIRE(0 == insertBtree(8));  sum += 8;
    REQUIRE(0 == insertBtree(9));  sum += 9;

    // check once more
    REQUIRE(0 == uqi_sum(m_db, txn, &result));
    REQUIRE(result.u.result_u64 == sum);

    // repeat two more times
    REQUIRE(0 == insertTxn(txn, 10));  sum += 10;
    REQUIRE(0 == insertTxn(txn, 11));  sum += 11;
    REQUIRE(0 == insertTxn(txn, 12));  sum += 12;

    REQUIRE(0 == uqi_sum(m_db, txn, &result));
    REQUIRE(result.u.result_u64 == sum);

    REQUIRE(0 == insertBtree(13));  sum += 13;
    REQUIRE(0 == insertBtree(14));  sum += 14;
    REQUIRE(0 == insertBtree(15));  sum += 15;

    REQUIRE(0 == uqi_sum(m_db, txn, &result));
    REQUIRE(result.u.result_u64 == sum);

    ups_txn_abort(txn, 0);
  }

  // tests the following sequences:
  // txn
  // txn, btree
  // txn, btree, txn
  // txn, btree, txn, btree
  // txn, btree, txn, btree, txn
  void sumMixedReverseTest() {
    uint32_t sum = 0;
    ups_txn_t *txn = 0;
    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));

    // insert a few keys
    REQUIRE(0 == insertTxn(txn, 1));  sum += 1;
    REQUIRE(0 == insertTxn(txn, 2));  sum += 2;
    REQUIRE(0 == insertTxn(txn, 3));  sum += 3;

    // check the sum
    uqi_result_t result;
    REQUIRE(0 == uqi_sum(m_db, txn, &result));
    REQUIRE(result.type == UPS_TYPE_UINT64);
    REQUIRE(result.u.result_u64 == sum);

    // continue with more keys
    REQUIRE(0 == insertBtree(4));  sum += 4;
    REQUIRE(0 == insertBtree(5));  sum += 5;
    REQUIRE(0 == insertBtree(6));  sum += 6;

    // check the sum
    REQUIRE(0 == uqi_sum(m_db, txn, &result));
    REQUIRE(result.u.result_u64 == sum);

    // continue inserting keys
    REQUIRE(0 == insertTxn(txn, 7));  sum += 7;
    REQUIRE(0 == insertTxn(txn, 8));  sum += 8;
    REQUIRE(0 == insertTxn(txn, 9));  sum += 9;

    // check once more
    REQUIRE(0 == uqi_sum(m_db, txn, &result));
    REQUIRE(result.u.result_u64 == sum);

    // repeat two more times
    REQUIRE(0 == insertBtree(10));  sum += 10;
    REQUIRE(0 == insertBtree(11));  sum += 11;
    REQUIRE(0 == insertBtree(12));  sum += 12;

    REQUIRE(0 == uqi_sum(m_db, txn, &result));
    REQUIRE(result.u.result_u64 == sum);

    REQUIRE(0 == insertTxn(txn, 13));  sum += 13;
    REQUIRE(0 == insertTxn(txn, 14));  sum += 14;
    REQUIRE(0 == insertTxn(txn, 15));  sum += 15;

    REQUIRE(0 == uqi_sum(m_db, txn, &result));
    REQUIRE(result.u.result_u64 == sum);

    ups_txn_abort(txn, 0);
  }

  void sumIfTest(int count) {
    ups_key_t key = {0};
    ups_record_t record = {0};
    uint32_t sum = 0;

    // insert a few keys
    for (int i = 0; i < count; i++) {
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ups_db_insert(m_db, 0, &key, &record, 0));
      if ((i & 1) == 0)
        sum += i;
    }

    uqi_bool_predicate_t predicate;
    predicate.context = 0;
    predicate.predicate_func = sum_if_predicate;

    uqi_result_t result;
    REQUIRE(0 == uqi_sum_if(m_db, 0, &predicate, &result));
    REQUIRE(result.type == UPS_TYPE_UINT64);
    REQUIRE(result.u.result_u64 == sum);
  }

  void averageTest(int count) {
    ups_key_t key = {0};
    ups_record_t record = {0};
    float sum = 0;

    // insert a few keys
    for (int i = 0; i < count; i++) {
      float f = (float)i;
      key.data = &f;
      key.size = sizeof(f);
      REQUIRE(0 == ups_db_insert(m_db, 0, &key, &record, 0));
      sum += f;
    }

    uqi_result_t result;
    REQUIRE(0 == uqi_average(m_db, 0, &result));
    REQUIRE(result.type == UPS_TYPE_REAL64);
    REQUIRE(result.u.result_double == sum / count);
  }

  void averageIfTest(int count) {
    ups_key_t key = {0};
    ups_record_t record = {0};
    float sum = 0;
    int c = 0;

    // insert a few keys
    for (int i = 0; i < count; i++) {
      float f = (float)i;
      key.data = &f;
      key.size = sizeof(f);
      REQUIRE(0 == ups_db_insert(m_db, 0, &key, &record, 0));
      if (f < 10.f) {
        sum += f;
        c++;
      }
    }

    uqi_bool_predicate_t predicate;
    predicate.context = 0;
    predicate.predicate_func = average_if_predicate;

    uqi_result_t result;
    REQUIRE(0 == uqi_average_if(m_db, 0, &predicate, &result));
    REQUIRE(result.type == UPS_TYPE_REAL64);
    REQUIRE(result.u.result_double == sum / c);
  }

  void countIfTest(int count) {
    ups_key_t key = {0};
    ups_record_t record = {0};
    char buffer[200] = {0};
    uint64_t c = 0;

    // insert a few keys
    for (int i = 0; i < count; i++) {
      buffer[0] = (char)i;
      key.size = i + 1;
      key.data = &buffer[0];
      REQUIRE(0 == ups_db_insert(m_db, 0, &key, &record, 0));
      if ((i & 1) == 0)
        c++;
    }

    uqi_bool_predicate_t predicate;
    predicate.context = 0;
    predicate.predicate_func = count_if_predicate;

    uqi_result_t result;
    REQUIRE(0 == uqi_count_if(m_db, 0, &predicate, &result));
    REQUIRE(result.type == UPS_TYPE_UINT64);
    REQUIRE(result.u.result_u64 == c);
  }

  void countDistinctIfTest(int count) {
    ups_key_t key = {0};
    ups_record_t record = {0};
    char buffer[200] = {0};
    uint64_t c = 0;

    // insert a few keys
    for (int i = 0; i < count; i++) {
      buffer[0] = (char)i;
      key.size = i + 1;
      key.data = &buffer[0];
      REQUIRE(0 == ups_db_insert(m_db, 0, &key, &record, 0));
      if ((i & 1) == 0)
        c++;
    }

    // and once more as duplicates
    for (int i = 0; i < count; i++) {
      buffer[0] = (char)i;
      key.size = i + 1;
      key.data = &buffer[0];
      REQUIRE(0 == ups_db_insert(m_db, 0, &key, &record, UPS_DUPLICATE));
      if ((i & 1) == 0)
        c++;
    }

    uqi_bool_predicate_t predicate;
    predicate.context = 0;
    predicate.predicate_func = count_if_predicate;

    uqi_result_t result;
    REQUIRE(0 == uqi_count_distinct_if(m_db, 0, &predicate, &result));
    REQUIRE(result.type == UPS_TYPE_UINT64);
    REQUIRE(result.u.result_u64 == c);
  }
};

TEST_CASE("Hola/sumTest", "")
{
  HolaFixture f(false, UPS_TYPE_UINT32);
  f.sumTest(10);
}

TEST_CASE("Hola/sumLargeTest", "")
{
  HolaFixture f(false, UPS_TYPE_UINT32);
  f.sumTest(10000);
}

TEST_CASE("Hola/sumTxnTest", "")
{
  HolaFixture f(true, UPS_TYPE_UINT32);
  f.sumTest(10);
}

TEST_CASE("Hola/sumTxnLargeTest", "")
{
  HolaFixture f(true, UPS_TYPE_UINT32);
  f.sumTest(10000);
}

TEST_CASE("Hola/sumMixedTest", "")
{
  HolaFixture f(true, UPS_TYPE_UINT32);
  f.sumMixedTest();
}

TEST_CASE("Hola/sumMixedReverseTest", "")
{
  HolaFixture f(true, UPS_TYPE_UINT32);
  f.sumMixedReverseTest();
}

TEST_CASE("Hola/sumIfTest", "")
{
  HolaFixture f(false, UPS_TYPE_UINT32);
  f.sumIfTest(10);
}

TEST_CASE("Hola/averageTest", "")
{
  HolaFixture f(false, UPS_TYPE_REAL32);
  f.averageTest(20);
}

TEST_CASE("Hola/averageIfTest", "")
{
  HolaFixture f(false, UPS_TYPE_REAL32);
  f.averageIfTest(20);
}

TEST_CASE("Hola/countIfTest", "")
{
  HolaFixture f(false, UPS_TYPE_BINARY);
  f.countIfTest(20);
}

TEST_CASE("Hola/countDistinctIfTest", "")
{
  HolaFixture f(false, UPS_TYPE_BINARY, true);
  f.countIfTest(20);
}

} // namespace upscaledb
