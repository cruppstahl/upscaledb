/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "../src/config.h"

#include "3rdparty/catch/catch.hpp"
#include "ham/hamsterdb_ola.h"

#include "utils.h"
#include "os.hpp"
#include "../src/btree_index.h"

namespace hamsterdb {

// only select even numbers
static ham_bool_t
sum_if_predicate(const void *key_data, ham_u16_t key_size, void *context)
{
  ham_u32_t *p = (ham_u32_t *)key_data;
  return ((*p & 1) == 0);
}

// only select numbers < 10
static ham_bool_t
average_if_predicate(const void *key_data, ham_u16_t key_size, void *context)
{
  float *p = (float *)key_data;
  return (*p < 10.f);
}

static ham_bool_t
count_if_predicate(const void *key_data, ham_u16_t key_size, void *context)
{
  ham_u8_t *p = (ham_u8_t *)key_data;
  return (*p & 1);
}

struct HolaFixture {
  ham_db_t *m_db;
  ham_env_t *m_env;
  bool m_use_transactions;

  HolaFixture(bool use_transactions, int type, bool use_duplicates = false)
    : m_use_transactions(use_transactions) {
    os::unlink(Utils::opath(".test"));
    ham_parameter_t params[] = {
        {HAM_PARAM_KEY_TYPE, type},
        {0, 0}
    };
    REQUIRE(0 == ham_env_create(&m_env, ".test",
                            use_transactions
                                ? HAM_ENABLE_TRANSACTIONS
                                : 0,
                            0, 0));
    REQUIRE(0 == ham_env_create_db(m_env, &m_db, 1,
                            use_duplicates
                                ? HAM_ENABLE_DUPLICATES
                                : 0, &params[0]));
  }

  ~HolaFixture() {
    teardown();
  }

  void teardown() {
    if (m_env)
      REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
    m_env = 0;
    m_db = 0;
  }

  void sumTest(int count) {
    ham_key_t key = {0};
    ham_record_t record = {0};
    ham_u32_t sum = 0;

    ham_txn_t *txn = 0;
    if (m_use_transactions)
      REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    // insert a few keys
    for (int i = 0; i < count; i++) {
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ham_db_insert(m_db, txn, &key, &record, 0));
      sum += i;
    }

    hola_result_t result;
    REQUIRE(0 == hola_sum(m_db, txn, &result));
    REQUIRE(result.type == HAM_TYPE_UINT64);
    REQUIRE(result.u.result_u64 == sum);

    if (m_use_transactions)
      ham_txn_abort(txn, 0);
  }

  ham_status_t insertBtree(ham_u32_t key) {
    ham_key_t k = {0};
    k.data = &key;
    k.size = sizeof(key);
    ham_record_t r = {0};

    BtreeIndex *be = ((LocalDatabase *)m_db)->get_btree_index();
    return (be->insert(0, 0, &k, &r, 0));
  }

  ham_status_t insertTxn(ham_txn_t *txn, ham_u32_t key) {
    ham_key_t k = {0};
    k.data = &key;
    k.size = sizeof(key);
    ham_record_t r = {0};

    return (ham_db_insert(m_db, txn, &k, &r, 0));
  }

  // tests the following sequences:
  // btree
  // btree, txn
  // btree, txn, btree
  // btree, txn, btree, txn
  // btree, txn, btree, txn, btree
  void sumMixedTest() {
    ham_u32_t sum = 0;
    ham_txn_t *txn = 0;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    // insert a few keys
    REQUIRE(0 == insertBtree(1));  sum += 1;
    REQUIRE(0 == insertBtree(2));  sum += 2;
    REQUIRE(0 == insertBtree(3));  sum += 3;

    // check the sum
    hola_result_t result;
    REQUIRE(0 == hola_sum(m_db, txn, &result));
    REQUIRE(result.type == HAM_TYPE_UINT64);
    REQUIRE(result.u.result_u64 == sum);

    // continue with more keys
    REQUIRE(0 == insertTxn(txn, 4));  sum += 4;
    REQUIRE(0 == insertTxn(txn, 5));  sum += 5;
    REQUIRE(0 == insertTxn(txn, 6));  sum += 6;

    // check the sum
    REQUIRE(0 == hola_sum(m_db, txn, &result));
    REQUIRE(result.u.result_u64 == sum);

    // continue inserting keys
    REQUIRE(0 == insertBtree(7));  sum += 7;
    REQUIRE(0 == insertBtree(8));  sum += 8;
    REQUIRE(0 == insertBtree(9));  sum += 9;

    // check once more
    REQUIRE(0 == hola_sum(m_db, txn, &result));
    REQUIRE(result.u.result_u64 == sum);

    // repeat two more times
    REQUIRE(0 == insertTxn(txn, 10));  sum += 10;
    REQUIRE(0 == insertTxn(txn, 11));  sum += 11;
    REQUIRE(0 == insertTxn(txn, 12));  sum += 12;

    REQUIRE(0 == hola_sum(m_db, txn, &result));
    REQUIRE(result.u.result_u64 == sum);

    REQUIRE(0 == insertBtree(13));  sum += 13;
    REQUIRE(0 == insertBtree(14));  sum += 14;
    REQUIRE(0 == insertBtree(15));  sum += 15;

    REQUIRE(0 == hola_sum(m_db, txn, &result));
    REQUIRE(result.u.result_u64 == sum);

    ham_txn_abort(txn, 0);
  }

  // tests the following sequences:
  // txn
  // txn, btree
  // txn, btree, txn
  // txn, btree, txn, btree
  // txn, btree, txn, btree, txn
  void sumMixedReverseTest() {
    ham_u32_t sum = 0;
    ham_txn_t *txn = 0;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    // insert a few keys
    REQUIRE(0 == insertTxn(txn, 1));  sum += 1;
    REQUIRE(0 == insertTxn(txn, 2));  sum += 2;
    REQUIRE(0 == insertTxn(txn, 3));  sum += 3;

    // check the sum
    hola_result_t result;
    REQUIRE(0 == hola_sum(m_db, txn, &result));
    REQUIRE(result.type == HAM_TYPE_UINT64);
    REQUIRE(result.u.result_u64 == sum);

    // continue with more keys
    REQUIRE(0 == insertBtree(4));  sum += 4;
    REQUIRE(0 == insertBtree(5));  sum += 5;
    REQUIRE(0 == insertBtree(6));  sum += 6;

    // check the sum
    REQUIRE(0 == hola_sum(m_db, txn, &result));
    REQUIRE(result.u.result_u64 == sum);

    // continue inserting keys
    REQUIRE(0 == insertTxn(txn, 7));  sum += 7;
    REQUIRE(0 == insertTxn(txn, 8));  sum += 8;
    REQUIRE(0 == insertTxn(txn, 9));  sum += 9;

    // check once more
    REQUIRE(0 == hola_sum(m_db, txn, &result));
    REQUIRE(result.u.result_u64 == sum);

    // repeat two more times
    REQUIRE(0 == insertBtree(10));  sum += 10;
    REQUIRE(0 == insertBtree(11));  sum += 11;
    REQUIRE(0 == insertBtree(12));  sum += 12;

    REQUIRE(0 == hola_sum(m_db, txn, &result));
    REQUIRE(result.u.result_u64 == sum);

    REQUIRE(0 == insertTxn(txn, 13));  sum += 13;
    REQUIRE(0 == insertTxn(txn, 14));  sum += 14;
    REQUIRE(0 == insertTxn(txn, 15));  sum += 15;

    REQUIRE(0 == hola_sum(m_db, txn, &result));
    REQUIRE(result.u.result_u64 == sum);

    ham_txn_abort(txn, 0);
  }

  void sumIfTest(int count) {
    ham_key_t key = {0};
    ham_record_t record = {0};
    ham_u32_t sum = 0;

    // insert a few keys
    for (int i = 0; i < count; i++) {
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &record, 0));
      if ((i & 1) == 0)
        sum += i;
    }

    hola_bool_predicate_t predicate;
    predicate.context = 0;
    predicate.predicate_func = sum_if_predicate;

    hola_result_t result;
    REQUIRE(0 == hola_sum_if(m_db, 0, &predicate, &result));
    REQUIRE(result.type == HAM_TYPE_UINT64);
    REQUIRE(result.u.result_u64 == sum);
  }

  void averageTest(int count) {
    ham_key_t key = {0};
    ham_record_t record = {0};
    float sum = 0;

    // insert a few keys
    for (int i = 0; i < count; i++) {
      float f = i;
      key.data = &f;
      key.size = sizeof(f);
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &record, 0));
      sum += f;
    }

    hola_result_t result;
    REQUIRE(0 == hola_average(m_db, 0, &result));
    REQUIRE(result.type == HAM_TYPE_REAL64);
    REQUIRE(result.u.result_double == sum / count);
  }

  void averageIfTest(int count) {
    ham_key_t key = {0};
    ham_record_t record = {0};
    float sum = 0;
    int c = 0;

    // insert a few keys
    for (int i = 0; i < count; i++) {
      float f = i;
      key.data = &f;
      key.size = sizeof(f);
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &record, 0));
      if (f < 10.f) {
        sum += f;
        c++;
      }
    }

    hola_bool_predicate_t predicate;
    predicate.context = 0;
    predicate.predicate_func = average_if_predicate;

    hola_result_t result;
    REQUIRE(0 == hola_average_if(m_db, 0, &predicate, &result));
    REQUIRE(result.type == HAM_TYPE_REAL64);
    REQUIRE(result.u.result_double == sum / c);
  }

  void countIfTest(int count) {
    ham_key_t key = {0};
    ham_record_t record = {0};
    char buffer[200] = {0};
    ham_u64_t c = 0;

    // insert a few keys
    for (int i = 0; i < count; i++) {
      buffer[0] = (char)i;
      key.size = i + 1;
      key.data = &buffer[0];
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &record, 0));
      if ((i & 1) == 0)
        c++;
    }

    hola_bool_predicate_t predicate;
    predicate.context = 0;
    predicate.predicate_func = count_if_predicate;

    hola_result_t result;
    REQUIRE(0 == hola_count_if(m_db, 0, &predicate, &result));
    REQUIRE(result.type == HAM_TYPE_UINT64);
    REQUIRE(result.u.result_u64 == c);
  }

  void countDistinctIfTest(int count) {
    ham_key_t key = {0};
    ham_record_t record = {0};
    char buffer[200] = {0};
    ham_u64_t c = 0;

    // insert a few keys
    for (int i = 0; i < count; i++) {
      buffer[0] = (char)i;
      key.size = i + 1;
      key.data = &buffer[0];
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &record, 0));
      if ((i & 1) == 0)
        c++;
    }

    // and once more as duplicates
    for (int i = 0; i < count; i++) {
      buffer[0] = (char)i;
      key.size = i + 1;
      key.data = &buffer[0];
      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &record, HAM_DUPLICATE));
      if ((i & 1) == 0)
        c++;
    }

    hola_bool_predicate_t predicate;
    predicate.context = 0;
    predicate.predicate_func = count_if_predicate;

    hola_result_t result;
    REQUIRE(0 == hola_count_distinct_if(m_db, 0, &predicate, &result));
    REQUIRE(result.type == HAM_TYPE_UINT64);
    REQUIRE(result.u.result_u64 == c);
  }
};

TEST_CASE("Hola/sumTest", "")
{
  HolaFixture f(false, HAM_TYPE_UINT32);
  f.sumTest(10);
}

TEST_CASE("Hola/sumLargeTest", "")
{
  HolaFixture f(false, HAM_TYPE_UINT32);
  f.sumTest(10000);
}

TEST_CASE("Hola/sumTxnTest", "")
{
  HolaFixture f(true, HAM_TYPE_UINT32);
  f.sumTest(10);
}

TEST_CASE("Hola/sumTxnLargeTest", "")
{
  HolaFixture f(true, HAM_TYPE_UINT32);
  f.sumTest(10000);
}

TEST_CASE("Hola/sumMixedTest", "")
{
  HolaFixture f(true, HAM_TYPE_UINT32);
  f.sumMixedTest();
}

TEST_CASE("Hola/sumMixedReverseTest", "")
{
  HolaFixture f(true, HAM_TYPE_UINT32);
  f.sumMixedReverseTest();
}

TEST_CASE("Hola/sumIfTest", "")
{
  HolaFixture f(false, HAM_TYPE_UINT32);
  f.sumIfTest(10);
}

TEST_CASE("Hola/averageTest", "")
{
  HolaFixture f(false, HAM_TYPE_REAL32);
  f.averageTest(20);
}

TEST_CASE("Hola/averageIfTest", "")
{
  HolaFixture f(false, HAM_TYPE_REAL32);
  f.averageIfTest(20);
}

TEST_CASE("Hola/countIfTest", "")
{
  HolaFixture f(false, HAM_TYPE_BINARY);
  f.countIfTest(20);
}

TEST_CASE("Hola/countDistinctIfTest", "")
{
  HolaFixture f(false, HAM_TYPE_BINARY, true);
  f.countIfTest(20);
}

} // namespace hamsterdb
