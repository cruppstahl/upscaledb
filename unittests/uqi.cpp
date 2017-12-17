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

#include "ups/upscaledb_uqi.h"

#include "4context/context.h"
#include "4uqi/plugins.h"
#include "4uqi/parser.h"
#include "4uqi/result.h"

#include "os.hpp"
#include "fixture.hpp"

namespace upscaledb {

struct ResultProxy {
  ResultProxy(uqi_result_t *result_ = nullptr)
    : result(result_) {
  }

  ~ResultProxy() {
    if (result)
      close();
  }

  ResultProxy &close() {
    uqi_result_close(result);
    result = nullptr;
    return *this;
  }

  template<typename T>
  ResultProxy &require(const char *key, uint32_t result_type, T record) {
    REQUIRE(uqi_result_get_row_count(result) == 1);
    REQUIRE(uqi_result_get_key_type(result) == UPS_TYPE_BINARY);
    ups_key_t k;
    uqi_result_get_key(result, 0, &k);
    REQUIRE(::strcmp(key, (const char *)k.data) == 0);
    REQUIRE(::strlen(key) == (size_t)k.size - 1);
    REQUIRE(uqi_result_get_record_type(result) == result_type);
    uint32_t size;
    REQUIRE(*(T *)uqi_result_get_record_data(result, &size) == record);
    return *this;
  }

  ResultProxy &require_row_count(uint32_t count) {
    REQUIRE(count == uqi_result_get_row_count(result));
    return *this;
  }

  ResultProxy &require_key_type(uint32_t type) {
    REQUIRE(type == uqi_result_get_key_type(result));
    return *this;
  }

  ResultProxy &require_record_type(uint32_t type) {
    REQUIRE(type == uqi_result_get_record_type(result));
    return *this;
  }

  ResultProxy &require_key(int row, void *data, uint32_t size) {
    ups_key_t key = {0};
    uqi_result_get_key(result, row, &key);
    REQUIRE(key.size == size);
    REQUIRE(0 == ::memcmp(key.data, data, size));
    return *this;
  }

  ResultProxy &require_record(int row, void *data, uint32_t size) {
    ups_record_t record = {0};
    uqi_result_get_record(result, row, &record);
    REQUIRE(record.size == size);
    REQUIRE(0 == ::memcmp(record.data, data, size));
    return *this;
  }

  ResultProxy &require_key_data(void *data, uint32_t size) {
    uint32_t s;
    void *p = uqi_result_get_key_data(result, &s);
    REQUIRE(s == size);
    REQUIRE(0 == ::memcmp(p, data, size));
    return *this;
  }

  ResultProxy &require_record_data(void *data, uint32_t size) {
    uint32_t s;
    void *p = uqi_result_get_record_data(result, &s);
    REQUIRE(s == size);
    REQUIRE(0 == ::memcmp(p, data, size));
    return *this;
  }

  uqi_result_t *result;
};

template<typename T>
static void
expect_result(uqi_result_t *result, const char *key,
                  uint32_t result_type, T record)
{
  REQUIRE(uqi_result_get_row_count(result) == 1);
  REQUIRE(uqi_result_get_key_type(result) == UPS_TYPE_BINARY);

  ups_key_t k;
  uqi_result_get_key(result, 0, &k);
  REQUIRE(::strcmp(key, (const char *)k.data) == 0);
  REQUIRE(::strlen(key) == (size_t)k.size - 1);

  REQUIRE(uqi_result_get_record_type(result) == result_type);
  uint32_t size;
  REQUIRE(*(T *)uqi_result_get_record_data(result, &size) == record);
}

static void *
agg_init(int flags, int key_type, uint32_t key_size, int record_type,
                    uint32_t record_size, const char *reserved)
{
  return new uint64_t(0);
}

static void
agg_single(void *state, const void *key_data, uint32_t key_size,
                    const void *record_data, uint32_t record_size)
{
  uint64_t *cooked_state = (uint64_t *)state;
  if (key_data) {
    REQUIRE(key_size == 4);
    *cooked_state += *(uint32_t *)key_data;
  }
  else {
    REQUIRE(record_size == 8);
    *cooked_state += *(uint64_t *)record_data;
  }
}

static void
agg_many(void *state, const void *key_data, const void *record_data,
                    size_t list_length)
{
  uint64_t *cooked_state = (uint64_t *)state;

  if (key_data) {
    uint32_t *stream = (uint32_t *)key_data;
    for (size_t i = 0; i < list_length; i++, stream++)
      *cooked_state += *stream;
  }
  else {
    uint64_t *stream = (uint64_t *)record_data;
    for (size_t i = 0; i < list_length; i++, stream++)
      *cooked_state += *stream;
  }
}

static void
agg_results(void *state, uqi_result_t *result)
{
  uint64_t *cooked_state = (uint64_t *)state;

  Result *r = (Result *)result;
  r->row_count = 1;
  r->key_type = UPS_TYPE_BINARY;
  r->add_key("AGG");
  r->record_type = UPS_TYPE_UINT64;
  r->add_record(*cooked_state);

  delete cooked_state;
}

static int
even_predicate(void *state, const void *key_data, uint32_t key_size,
                const void *record_data, uint32_t record_size)
{
  const uint32_t *i = (const uint32_t *)key_data;
  return (*i & 1) == 0;
}

static int
key_predicate(void *state, const void *key_data, uint32_t key_size,
                const void *record_data, uint32_t record_size)
{
  const uint32_t *i = (const uint32_t *)key_data;
  return *i < 2500;
}

static int
record_predicate(void *state, const void *key_data, uint32_t key_size,
                const void *record_data, uint32_t record_size)
{
  const uint32_t *i = (const uint32_t *)record_data;
  return *i < 5000;
}

static int
test1_predicate(void *state, const void *key_data, uint32_t key_size,
                const void *record_data, uint32_t record_size)
{
  const uint8_t *p = (const uint8_t *)key_data;
  return (p[0] & 1) == 0;
}

static void *
lt10_init(int flags, int key_type, uint32_t key_size, int record_type,
                uint32_t record_size, const char *reserved)
{
  REQUIRE(flags == UQI_STREAM_KEY);
  return 0;
}

static int
lt10_predicate(void *state, const void *key_data, uint32_t key_size,
                const void *record_data, uint32_t record_size)
{
  const float *f = (const float *)key_data;
  return *f < 10.0f;
}

struct UqiFixture : BaseFixture {
  bool use_transactions;

  UqiFixture(bool use_transactions_, int key_type, bool use_duplicates = false,
                uint32_t page_size = 1024 * 16)
    : use_transactions(use_transactions_) {
    ups_parameter_t env_params[] = {
        {UPS_PARAM_PAGE_SIZE, (uint64_t)page_size},
        {0, 0}
    };
    ups_parameter_t db_params[] = {
        {UPS_PARAM_KEY_TYPE, (uint64_t)key_type},
        {0, 0}
    };
    require_create(use_transactions ? UPS_ENABLE_TRANSACTIONS : 0, env_params,
                    use_duplicates ? UPS_ENABLE_DUPLICATES : 0, db_params);
  }

  void countTest(uint64_t count) {
    ups_key_t key = {0};
    ups_record_t record = {0};

    // insert a few keys
    for (uint32_t i = 0; i < count; i++) {
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
    }

    ResultProxy rp;
    REQUIRE(0 == uqi_select(env, "coUNT ($key) from database 1", &rp.result));
    rp.require("COUNT", UPS_TYPE_UINT64, count);
  }

  void cursorTest() {
    int i;
    ups_key_t key = ups_make_key(&i, sizeof(i));
    ups_record_t record = {0};
    uint64_t sum = 0;

    // insert a few keys
    for (i = 0; i < 10; i++) {
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
      sum += i;
    }

    ups_cursor_t *cursor;
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    ResultProxy rp;

    REQUIRE(0 == ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
    REQUIRE(0 == uqi_select_range(env, "SUM($key) from database 1",
                            cursor, 0, &rp.result));
    rp.require("SUM", UPS_TYPE_UINT64, sum)
      .close();

    i = 5;
    REQUIRE(0 == ups_cursor_find(cursor, &key, 0, 0));
    REQUIRE(0 == uqi_select_range(env, "SUM($key) from database 1",
                            cursor, 0, &rp.result));
    rp.require("SUM", UPS_TYPE_UINT64, (uint64_t)5 + 6 + 7 + 8 + 9)
      .close();

    // |cursor| is now located at the end of the database
    REQUIRE(UPS_KEY_NOT_FOUND ==
                    ups_cursor_move(cursor, 0, 0, UPS_CURSOR_NEXT));

    REQUIRE(0 == ups_cursor_close(cursor));
  }

  void endCursorTest() {
    int i = 0;
    uint64_t sum = 0;

    ups_key_t key = ups_make_key(&i, sizeof(i));
    ups_record_t record = {0};

    // insert a few keys
    for (; i < 100; i++) {
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
      sum += i;
    }

    // then more without summing up
    for (; i < 200; i++) {
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
    }

    ups_cursor_t *cursor;
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    i = 100;
    REQUIRE(0 == ups_cursor_find(cursor, &key, 0, 0));

    ResultProxy rp;
    REQUIRE(0 == uqi_select_range(env, "COUNT($key) from database 1",
                            0, cursor, &rp.result));
    rp.require("COUNT", UPS_TYPE_UINT64, (uint64_t)100)
      .close();

    REQUIRE(0 == uqi_select_range(env, "SUM($key) from database 1",
                            0, cursor, &rp.result));
    rp.require("SUM", UPS_TYPE_UINT64, sum);

    REQUIRE(0 == ups_cursor_close(cursor));
  }

  void endTxnCursorTest() {
    int i = 0;
    uint64_t sum = 0;

    ups_key_t key = ups_make_key(&i, sizeof(i));

    // insert a few keys
    for (; i < 100; i++) {
      REQUIRE(0 == insertBtree((uint32_t)i));
      sum += i;
    }

    // then more without summing up
    ups_txn_t *txn;
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    for (; i < 120; i++) {
      REQUIRE(0 == insertTxn(txn, (uint32_t)i));
    }
    REQUIRE(0 == ups_txn_commit(txn, 0));

    // and a few more btree keys
    for (; i < 300; i++) {
      REQUIRE(0 == insertBtree((uint32_t)i));
    }

    ups_cursor_t *cursor;
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));
    i = 100;
    REQUIRE(0 == ups_cursor_find(cursor, &key, 0, 0));

    ResultProxy rp;
    REQUIRE(0 == uqi_select_range(env, "COUNT($key) from database 1",
                            0, cursor, &rp.result));
    rp.require("COUNT", UPS_TYPE_UINT64, (uint64_t)100)
      .close();

    REQUIRE(0 == uqi_select_range(env, "SUM($key) from database 1",
                            0, cursor, &rp.result));
    rp.require("SUM", UPS_TYPE_UINT64, sum)
      .close();

    i = 110;
    REQUIRE(0 == ups_cursor_find(cursor, &key, 0, 0));
    REQUIRE(0 == uqi_select_range(env, "COUNT($key) from database 1",
                            0, cursor, &rp.result));
    rp.require("COUNT", UPS_TYPE_UINT64, (uint64_t)110);

    REQUIRE(0 == ups_cursor_close(cursor));
  }

  void invalidCursorTest() {
    int i;
    ups_key_t key = ups_make_key(&i, sizeof(i));
    ups_record_t record = {0};
    uint32_t sum = 0;

    // create an empty second database
    ups_db_t *db2;
    REQUIRE(0 == ups_env_create_db(env, &db2, 2, 0, 0));

    // insert a few keys into the first(!) database
    for (i = 0; i < 10; i++) {
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
      sum += i;
    }

    ups_cursor_t *cursor1;
    REQUIRE(0 == ups_cursor_create(&cursor1, db, 0, 0));
    ups_cursor_t *cursor2;
    REQUIRE(0 == ups_cursor_create(&cursor2, db, 0, 0));

    ResultProxy rp;
    REQUIRE(UPS_CURSOR_IS_NIL
                    == uqi_select_range(env, "SUM($key) from database 1",
                            cursor1, 0, &rp.result));

    REQUIRE(0 == ups_cursor_move(cursor1, 0, 0, UPS_CURSOR_FIRST));

    // use cursor of db1 on database 2 -> must fail
    REQUIRE(UPS_INV_PARAMETER
                    == uqi_select_range(env, "SUM($key) from database 2",
                            cursor1, 0, &rp.result));

    REQUIRE(0 == ups_cursor_close(cursor1));
    REQUIRE(0 == ups_cursor_close(cursor2));
  }

  void sumTest(int count) {
    ups_record_t record = {0};
    uint64_t sum = 0;

    // insert a few keys
    for (int i = 0; i < count; i++) {
      ups_key_t key = ups_make_key(&i, sizeof(i));
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
      sum += i;
    }

    ResultProxy rp;
    REQUIRE(0 == uqi_select(env, "SUM($key) from database 1", &rp.result));
    rp.require("SUM", UPS_TYPE_UINT64, sum);
  }

  void negativeSumTest() {
    ups_key_t key1 = ups_make_key((void *)"hello again", 11);
    ups_key_t key2 = ups_make_key((void *)"ich sag einfach", 16);
    ups_key_t key3 = ups_make_key((void *)"hello again...", 14);
    ups_record_t record = {0};

    // insert a few keys
    REQUIRE(0 == ups_db_insert(db, 0, &key1, &record, 0));
    REQUIRE(0 == ups_db_insert(db, 0, &key2, &record, 0));
    REQUIRE(0 == ups_db_insert(db, 0, &key3, &record, 0));

    uqi_result_t *result;
    REQUIRE(UPS_PARSER_ERROR == uqi_select(env,
                "SUM($key) from database 1", &result));
    REQUIRE(UPS_PARSER_ERROR == uqi_select(env,
                "average($key) from database 1", &result));
  }

  void closedDatabaseTest() {
    ups_key_t key = {0};
    ups_record_t record = {0};
    uint64_t sum = 0;

    // insert a few keys
    for (int i = 0; i < 10; i++) {
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
      sum += i;
    }

    // now close it - will be opened automatically
    REQUIRE(0 == ups_db_close(db, 0));
    db = 0;

    ResultProxy rp;
    REQUIRE(0 == uqi_select(env, "SUM($key) from database 1", &rp.result));
    rp.require("SUM", UPS_TYPE_UINT64, sum);
  }

  void unknownDatabaseTest() {
    uqi_result_t *result;
    REQUIRE(UPS_DATABASE_NOT_FOUND
                == uqi_select(env, "SUM($key) from database 100", &result));
  }

  ups_status_t insertBtree(uint32_t key) {
    ups_key_t k = ups_make_key(&key, sizeof(key));
    ups_record_t r = {0};

    Context context(lenv(), 0, 0);
    return btree_index()->insert(&context, 0, &k, &r, 0);
  }

  ups_status_t insertBtree(const std::string &key) {
    ups_key_t k = ups_make_key((void *)key.c_str(), (uint16_t)key.size());
    ups_record_t r = {0};

    Context context(lenv(), 0, 0);
    return btree_index()->insert(&context, 0, &k, &r, 0);
  }

  ups_status_t insertTxn(ups_txn_t *txn, uint32_t key) {
    ups_key_t k = ups_make_key(&key, sizeof(key));
    ups_record_t r = {0};
    return ups_db_insert(db, txn, &k, &r, 0);
  }

  ups_status_t insertTxn(ups_txn_t *txn, const std::string &key) {
    ups_key_t k = ups_make_key((void *)key.c_str(), (uint16_t)key.size());
    ups_record_t r = {0};
    return ups_db_insert(db, txn, &k, &r, 0);
  }

  // tests the following sequences:
  // btree
  // btree, txn
  // btree, txn, btree
  // btree, txn, btree, txn
  // btree, txn, btree, txn, btree
  void sumMixedTest() {
    uint64_t sum = 0;
    ups_txn_t *txn = 0;

    // insert a few keys
    REQUIRE(0 == insertBtree(1)); sum += 1;
    REQUIRE(0 == insertBtree(2)); sum += 2;
    REQUIRE(0 == insertBtree(3)); sum += 3;

    // check the sum
    ResultProxy rp;
    REQUIRE(0 == uqi_select(env, "SUM($key) from database 1", &rp.result));
    rp.require("SUM", UPS_TYPE_UINT64, sum)
      .close();

    // continue with more keys
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == insertTxn(txn, 4)); sum += 4;
    REQUIRE(0 == insertTxn(txn, 5)); sum += 5;
    REQUIRE(0 == insertTxn(txn, 6)); sum += 6;
    REQUIRE(0 == ups_txn_commit(txn, 0));

    // check the sum
    REQUIRE(0 == uqi_select(env, "SUM($key) from database 1", &rp.result));
    rp.require("SUM", UPS_TYPE_UINT64, sum)
      .close();

    // continue inserting keys
    REQUIRE(0 == insertBtree(7)); sum += 7;
    REQUIRE(0 == insertBtree(8)); sum += 8;
    REQUIRE(0 == insertBtree(9)); sum += 9;

    // check once more
    REQUIRE(0 == uqi_select(env, "SUM($key) from database 1", &rp.result));
    rp.require("SUM", UPS_TYPE_UINT64, sum)
      .close();

    // repeat two more times
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == insertTxn(txn, 10)); sum += 10;
    REQUIRE(0 == insertTxn(txn, 11)); sum += 11;
    REQUIRE(0 == insertTxn(txn, 12)); sum += 12;
    REQUIRE(0 == ups_txn_commit(txn, 0));

    REQUIRE(0 == uqi_select(env, "SUM($key) from database 1", &rp.result));
    rp.require("SUM", UPS_TYPE_UINT64, sum)
      .close();

    REQUIRE(0 == insertBtree(13)); sum += 13;
    REQUIRE(0 == insertBtree(14)); sum += 14;
    REQUIRE(0 == insertBtree(15)); sum += 15;

    REQUIRE(0 == uqi_select(env, "SUM($key) from database 1", &rp.result));
    rp.require("SUM", UPS_TYPE_UINT64, sum);
  }

  void largeMixedTest() {
    char buffer[32] = {0};

    // insert a few long keys
    for (int i = 0; i < 24; i++) {
      ::memset(buffer, 'a' + i, 31);
      REQUIRE(0 == insertBtree(buffer));
    }

    // insert short transactional keys "between" the btree keys
    ups_txn_t *txn = 0;
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    for (int i = 0; i < 24; i++) {
      buffer[0] = 'a' + i;
      buffer[1] = '\0';
      REQUIRE(0 == insertTxn(txn, buffer));
    }
    REQUIRE(0 == ups_txn_commit(txn, 0));

    ResultProxy rp;
    REQUIRE(0 == uqi_select(env, "COUNT($key) from database 1", &rp.result));
    rp.require("COUNT", UPS_TYPE_UINT64, 24 * 2);
  }

  // tests the following sequences:
  // txn
  // txn, btree
  // txn, btree, txn
  // txn, btree, txn, btree
  // txn, btree, txn, btree, txn
  void sumMixedReverseTest() {
    uint64_t sum = 0;
    ups_txn_t *txn = 0;

    // insert a few keys
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == insertTxn(txn, 1));  sum += 1;
    REQUIRE(0 == insertTxn(txn, 2));  sum += 2;
    REQUIRE(0 == insertTxn(txn, 3));  sum += 3;
    REQUIRE(0 == ups_txn_commit(txn, 0));

    // check the sum
    ResultProxy rp;
    REQUIRE(0 == uqi_select(env, "SUM($key) from database 1", &rp.result));
    rp.require("SUM", UPS_TYPE_UINT64, sum)
      .close();

    // continue with more keys
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == insertBtree(4));  sum += 4;
    REQUIRE(0 == insertBtree(5));  sum += 5;
    REQUIRE(0 == insertBtree(6));  sum += 6;
    REQUIRE(0 == ups_txn_commit(txn, 0));

    // check the sum
    REQUIRE(0 == uqi_select(env, "SUM($key) from database 1", &rp.result));
    rp.require("SUM", UPS_TYPE_UINT64, sum)
      .close();

    // continue inserting keys
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == insertTxn(txn, 7));  sum += 7;
    REQUIRE(0 == insertTxn(txn, 8));  sum += 8;
    REQUIRE(0 == insertTxn(txn, 9));  sum += 9;
    REQUIRE(0 == ups_txn_commit(txn, 0));

    // check once more
    REQUIRE(0 == uqi_select(env, "SUM($key) from database 1", &rp.result));
    rp.require("SUM", UPS_TYPE_UINT64, sum)
      .close();

    // repeat two more times
    REQUIRE(0 == insertBtree(10));  sum += 10;
    REQUIRE(0 == insertBtree(11));  sum += 11;
    REQUIRE(0 == insertBtree(12));  sum += 12;

    REQUIRE(0 == uqi_select(env, "SUM($key) from database 1", &rp.result));
    rp.require("SUM", UPS_TYPE_UINT64, sum)
      .close();

    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));
    REQUIRE(0 == insertTxn(txn, 13));  sum += 13;
    REQUIRE(0 == insertTxn(txn, 14));  sum += 14;
    REQUIRE(0 == insertTxn(txn, 15));  sum += 15;
    REQUIRE(0 == ups_txn_commit(txn, 0));

    REQUIRE(0 == uqi_select(env, "SUM($key) from database 1", &rp.result));
    rp.require("SUM", UPS_TYPE_UINT64, sum);
  }

  void sumIfTest(int count) {
    ups_key_t key = {0};
    ups_record_t record = {0};
    uint64_t sum = 0;

    // insert a few keys
    for (int i = 0; i < count; i++) {
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
      if ((i & 1) == 0)
        sum += i;
    }

    uqi_plugin_t even_plugin = {0};
    even_plugin.name = "even";
    even_plugin.type = UQI_PLUGIN_PREDICATE;
    even_plugin.pred = even_predicate;
    REQUIRE(0 == uqi_register_plugin(&even_plugin));

    ResultProxy rp;
    REQUIRE(0 == uqi_select(env, "SUM($key) from database 1 WHERE even($key)",
                            &rp.result));
    rp.require("SUM", UPS_TYPE_UINT64, sum);
  }

  void averageTest(int count) {
    ups_key_t key = {0};
    ups_record_t record = {0};
    double sum = 0;

    // insert a few keys
    for (int i = 0; i < count; i++) {
      float f = (float)i;
      key.data = &f;
      key.size = sizeof(f);
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
      sum += f;
    }

    ResultProxy rp;
    REQUIRE(0 == uqi_select(env, "AVERAGE($key) from database 1",
                            &rp.result));
    rp.require("AVERAGE", UPS_TYPE_REAL64, sum / (double)count);
  }

  void averageIfTest(int count) {
    ups_key_t key = {0};
    ups_record_t record = {0};
    double sum = 0;
    int c = 0;

    // insert a few keys
    for (int i = 0; i < count; i++) {
      float f = (float)i;
      key.data = &f;
      key.size = sizeof(f);
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
      if (f < 10.f) {
        sum += f;
        c++;
      }
    }

    uqi_plugin_t plugin = {0};
    plugin.name = "if_lt_10";
    plugin.type = UQI_PLUGIN_PREDICATE;
    plugin.pred = lt10_predicate;
    plugin.init = lt10_init;
    REQUIRE(0 == uqi_register_plugin(&plugin));

    ResultProxy rp;
    REQUIRE(0 == uqi_select(env, "average($key) from database 1 WHERE "
                            "IF_Lt_10($key)", &rp.result));
    rp.require("AVERAGE", UPS_TYPE_REAL64, sum / (double)c);
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
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
      if ((i & 1) == 0)
        c++;
    }

    uqi_plugin_t plugin = {0};
    plugin.name = "test1";
    plugin.type = UQI_PLUGIN_PREDICATE;
    plugin.pred = test1_predicate;
    REQUIRE(0 == uqi_register_plugin(&plugin));

    ResultProxy rp;
    REQUIRE(0 == uqi_select(env, "COUNT($key) from database 1 WHERE test1($key)",
                            &rp.result));
    rp.require("COUNT", UPS_TYPE_UINT64, c);
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
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
      if ((i & 1) == 0)
        c++;
    }

    // and once more as duplicates
    for (int i = 0; i < count; i++) {
      buffer[0] = (char)i;
      key.size = i + 1;
      key.data = &buffer[0];
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, UPS_DUPLICATE));
      if ((i & 1) == 0)
        c++;
    }

    uqi_plugin_t plugin = {0};
    plugin.name = "test1";
    plugin.type = UQI_PLUGIN_PREDICATE;
    plugin.pred = test1_predicate;
    REQUIRE(0 == uqi_register_plugin(&plugin));

    ResultProxy rp;
    REQUIRE(0 == uqi_select(env, "dinstinct COUNT($key) from database 1 "
                            "WHERE test1($key)", &rp.result));
    rp.require("COUNT", UPS_TYPE_UINT64, c / 2);
  }
};

TEST_CASE("Uqi/sumMixedTest", "")
{
  UqiFixture f(true, UPS_TYPE_UINT32);
  f.sumMixedTest();
}

TEST_CASE("Uqi/largeMixedTest", "")
{
  UqiFixture f(true, UPS_TYPE_BINARY, false, 1024);
  f.largeMixedTest();
}

TEST_CASE("Uqi/sumMixedReverseTest", "")
{
  UqiFixture f(true, UPS_TYPE_UINT32);
  f.sumMixedReverseTest();
}

TEST_CASE("Uqi/sumIfTest", "")
{
  UqiFixture f(false, UPS_TYPE_UINT32);
  f.sumIfTest(10);
}

TEST_CASE("Uqi/averageTest", "")
{
  UqiFixture f(false, UPS_TYPE_REAL32);
  f.averageTest(20);
}

TEST_CASE("Uqi/averageIfTest", "")
{
  UqiFixture f(false, UPS_TYPE_REAL32);
  f.averageIfTest(20);
}

TEST_CASE("Uqi/countIfTest", "")
{
  UqiFixture f(false, UPS_TYPE_BINARY);
  f.countIfTest(20);
}

TEST_CASE("Uqi/countDistinctIfTest", "")
{
  UqiFixture f(false, UPS_TYPE_BINARY, true);
  f.countIfTest(20);
}

TEST_CASE("Uqi/pluginTest", "")
{
  REQUIRE(upscaledb::PluginManager::get("foo") == 0);
  REQUIRE(upscaledb::PluginManager::is_registered("foo") == false);
  REQUIRE(upscaledb::PluginManager::import("noexist", "foo")
              == UPS_PLUGIN_NOT_FOUND);
  REQUIRE(upscaledb::PluginManager::import("/usr/lib/libsnappy.so", "foo")
              == UPS_PLUGIN_NOT_FOUND);
  // Win32: copy ../win32/msvc2013/out/unittests_XXX/unittests-plugin.dll .
  // Then reenable the next lines
#if 0
  REQUIRE(upscaledb::PluginManager::import("unittests-plugin.dll", "foo")
    == UPS_PLUGIN_NOT_FOUND);
  REQUIRE(upscaledb::PluginManager::import("unittests-plugin.dll", "test1")
    == UPS_PLUGIN_NOT_FOUND);
  REQUIRE(upscaledb::PluginManager::import("unittests-plugin.dll", "test2")
    == UPS_PLUGIN_NOT_FOUND);
  REQUIRE(upscaledb::PluginManager::import("unittests-plugin.dll", "test3")
    == UPS_PLUGIN_NOT_FOUND);
  REQUIRE(upscaledb::PluginManager::import("unittests-plugin.dll", "test4")
    == 0);
#else
  REQUIRE(upscaledb::PluginManager::import("./plugin.so", "foo")
              == UPS_PLUGIN_NOT_FOUND);
  REQUIRE(upscaledb::PluginManager::import("./plugin.so", "test1")
              == UPS_PLUGIN_NOT_FOUND);
  REQUIRE(upscaledb::PluginManager::import("./plugin.so", "test2")
              == UPS_PLUGIN_NOT_FOUND);
  REQUIRE(upscaledb::PluginManager::import("./plugin.so", "test3")
              == UPS_PLUGIN_NOT_FOUND);
  REQUIRE(upscaledb::PluginManager::import("./plugin.so", "test4")
              == 0);
#endif
  REQUIRE(upscaledb::PluginManager::get("test4") != 0);
  REQUIRE(upscaledb::PluginManager::is_registered("test4") == true);
}

static void
check(const char *query, bool distinct, const char *function,
                uint16_t db, const char *predicate = 0)
{
  SelectStatement stmt;
  REQUIRE(upscaledb::Parser::parse_select(query, stmt) == 0);
  REQUIRE(stmt.distinct == distinct);
  REQUIRE(stmt.dbid == db);
  REQUIRE(stmt.function.name == function);
  if (predicate)
    REQUIRE(stmt.predicate.name == predicate);
}

TEST_CASE("Uqi/parserTest", "")
{
  SelectStatement stmt;
  REQUIRE(upscaledb::Parser::parse_select("", stmt)
                == UPS_PARSER_ERROR);
  REQUIRE(upscaledb::Parser::parse_select("foo bar", stmt)
                == UPS_PARSER_ERROR);

  // test hex. and octal numbers
  REQUIRE(upscaledb::Parser::parse_select("bar($key) from database 010", stmt)
                == 0);
  REQUIRE(stmt.dbid == 8);
  REQUIRE(upscaledb::Parser::parse_select("bar($key) from database 0x10", stmt)
                == 0);
  REQUIRE(stmt.dbid == 16);
  REQUIRE(upscaledb::Parser::parse_select("bar($key) from database 0X10", stmt)
                == 0);
  REQUIRE(stmt.dbid == 16);

  REQUIRE(upscaledb::Parser::parse_select("bar($key) from database 1", stmt)
                == 0);

  // Win32: copy ../win32/msvc2013/out/unittests_debug_x64/unittests-plugin.dll .
  // Then reenable the next lines
#if 0
  REQUIRE(upscaledb::PluginManager::import("unittests-plugin.dll", "test4")
    == 0);
  REQUIRE(upscaledb::Parser::parse_select("\"test4@unittests-plugin.dll\"($key) from database 1", stmt)
    == 0);
#else
  REQUIRE(upscaledb::PluginManager::import("./plugin.so", "test4")
                == 0);
  REQUIRE(upscaledb::Parser::parse_select("\"test4@./plugin.so\"($key) from database 1", stmt)
                == 0);
#endif
  REQUIRE(upscaledb::Parser::parse_select("test4($key) from database 1", stmt)
    == 0);
  REQUIRE(upscaledb::Parser::parse_select("\"test4@no.so\"($key) from database 1", stmt)
                == UPS_PLUGIN_NOT_FOUND);
  REQUIRE(upscaledb::Parser::parse_select("test4($key) from database 1 "
                          "where test4($key) limit 12", stmt)
                == UPS_PARSER_ERROR);
  REQUIRE(upscaledb::Parser::parse_select("test4($key) from database 1 "
                          "limit 12", stmt)
                == UPS_PARSER_ERROR);

  check("test4($key) from database 10",
                false, "test4", 10);
  check("DISTINCT test4($key) from database 10",
                true, "test4", 10);
  check("test4($key) from database 1 where test4($key)",
                false, "test4", 1, "test4");
  check("t($key) from database 1 where test4($key)",
                false, "t", 1, "test4");
  check("DISTINCT test4($key) from database 10",
                true, "test4", 10, 0);
  check("DISTINCT test4($key) from database 10",
                true, "test4", 10, 0);

  stmt = SelectStatement();
  REQUIRE(upscaledb::Parser::parse_select("SUM($record) FROM database 1",
                stmt) == 0);
  REQUIRE(stmt.function.flags == UQI_STREAM_RECORD);

  REQUIRE(upscaledb::Parser::parse_select("SUM($key, $record) FROM database 1",
                stmt) == 0);
  REQUIRE(stmt.function.flags == (UQI_STREAM_KEY | UQI_STREAM_RECORD));
}

TEST_CASE("Uqi/closedDatabaseTest", "")
{
  UqiFixture f(false, UPS_TYPE_UINT32);
  f.closedDatabaseTest();
}

TEST_CASE("Uqi/unknownDatabaseTest", "")
{
  UqiFixture f(false, UPS_TYPE_UINT32);
  f.unknownDatabaseTest();
}

TEST_CASE("Uqi/cursorTest", "")
{
  UqiFixture f(false, UPS_TYPE_UINT32);
  f.cursorTest();
}

TEST_CASE("Uqi/endCursorTest", "")
{
  UqiFixture f(false, UPS_TYPE_UINT32);
  f.endCursorTest();
}

TEST_CASE("Uqi/endTxnCursorTest", "")
{
  UqiFixture f(true, UPS_TYPE_UINT32);
  f.endTxnCursorTest();
}

TEST_CASE("Uqi/invalidCursorTest", "")
{
  UqiFixture f(false, UPS_TYPE_UINT32);
  f.invalidCursorTest();
}

TEST_CASE("Uqi/sumTest", "")
{
  UqiFixture f(false, UPS_TYPE_UINT32);
  f.sumTest(10);
}

TEST_CASE("Uqi/negativeSumTest", "")
{
  UqiFixture f(false, UPS_TYPE_BINARY);
  f.negativeSumTest();
}

TEST_CASE("Uqi/sumLargeTest", "")
{
  UqiFixture f(false, UPS_TYPE_UINT32);
  f.sumTest(10000);
}

TEST_CASE("Uqi/countTest", "")
{
  UqiFixture f(false, UPS_TYPE_UINT32);
  f.countTest(10);
}

TEST_CASE("Uqi/countLargeTest", "")
{
  UqiFixture f(false, UPS_TYPE_UINT32);
  f.countTest(10000);
}

struct QueryFixture : BaseFixture {
  QueryFixture(uint32_t flags, uint32_t key_type, uint32_t record_type) {
    ups_parameter_t db_params[] = {
        {UPS_PARAM_KEY_TYPE, (uint64_t)key_type},
        {UPS_PARAM_RECORD_TYPE, (uint64_t)record_type},
        {0, 0}
    };
    require_create(0, nullptr, flags, db_params);
  }

  ~QueryFixture() {
    close();
  }

  void run(std::string fname) {
    uint64_t key_sum = 0;
    uint64_t key_filtered = 0;
    uint64_t record_sum = 0;
    uint64_t record_filtered = 0;

    for (uint32_t i = 0; i < 5000u; i++) {
      uint64_t j = i * 2;
      ups_key_t key = ups_make_key(&i, sizeof(i));
      ups_record_t record = ups_make_record(&j, sizeof(j));
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));

      // precompute the results of the various queries
      key_sum += i;
      record_sum += j;
      if (i < 2500)
        record_filtered += j;
      if (j < 5000)
        key_filtered += i;
    }

    std::string query;
    ResultProxy rp;

    // query keys only
    query = fname + "($key) from database 1";
    REQUIRE(0 == uqi_select(env, query.c_str(), &rp.result));
    rp.require_record_data(&key_sum, sizeof(key_sum))
      .close();

    // query records only
    query = fname + "($record) from database 1";
    REQUIRE(0 == uqi_select(env, query.c_str(), &rp.result));
    rp.require_record_data(&record_sum, sizeof(record_sum))
      .close();

    uqi_plugin_t key_plugin = {0};
    key_plugin.name = "key_pred";
    key_plugin.type = UQI_PLUGIN_PREDICATE;
    key_plugin.pred = key_predicate;
    REQUIRE(0 == uqi_register_plugin(&key_plugin));

    uqi_plugin_t record_plugin = {0};
    record_plugin.name = "record_pred";
    record_plugin.type = UQI_PLUGIN_PREDICATE;
    record_plugin.pred = record_predicate;
    REQUIRE(0 == uqi_register_plugin(&record_plugin));

    // query both (keys and records)
    query = fname + "($key) from database 1 where record_pred($record)";
    REQUIRE(0 == uqi_select(env, query.c_str(), &rp.result));
    rp.require_record_data(&key_filtered, sizeof(key_filtered))
      .close();

    // query both (keys and records) vice versa
    query = fname + "($record) from database 1 where key_pred($key)";
    REQUIRE(0 == uqi_select(env, query.c_str(), &rp.result));
    rp.require_record_data(&record_filtered, sizeof(record_filtered));
  }

  void resultTest() {
    Result result;
    uqi_result_t *res = reinterpret_cast<uqi_result_t *>(&result);

    const char *keys[] = {"one", "two", "three", "four", "five", "six",
                          "seven", "eight", "nine", "ten"};
    uqi_result_initialize(res, UPS_TYPE_BINARY, UPS_TYPE_UINT32);

    for (int i = 0; i < 10; i++)
      uqi_result_add_row(res, keys[i], ::strlen(keys[i]), &i, sizeof(i));
    
    ResultProxy rp(res);
    rp.require_row_count(10)
      .require_key_type(UPS_TYPE_BINARY)
      .require_record_type(UPS_TYPE_UINT32);

    ups_key_t key = {0};
    ups_record_t record = {0};
    for (int i = 0; i < 10; i++) {
      rp.require_key(i, (void *)keys[i], ::strlen(keys[i]))
        .require_record(i, &i, sizeof(i));
    }

    rp.result = nullptr; // avoid call to uqi_result_close
  }

  void sumOnRecordsTest() {
    uint64_t sum = 0;
    uint64_t pred = 0;
    int i = 0;

    // insert a few keys
    for (double d = 0; d < 10000; d++, i++) {
      ups_key_t key = ups_make_key(&d, sizeof(d));
      ups_record_t record = ups_make_record(&i, sizeof(i));
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
      sum += i;
      if (i < 5000)
        pred += i;
    }

    ResultProxy rp;
    REQUIRE(0 == uqi_select(env, "SUM($record) from database 1", &rp.result));
    rp.require("SUM", UPS_TYPE_UINT64, sum)
      .close();

    uqi_plugin_t record_plugin = {0};
    record_plugin.name = "record_pred";
    record_plugin.type = UQI_PLUGIN_PREDICATE;
    record_plugin.pred = record_predicate;
    REQUIRE(0 == uqi_register_plugin(&record_plugin));
    REQUIRE(0 == uqi_select(env, "SUM($record) from database 1 where "
                            "record_pred($record)", &rp.result));
    rp.require("SUM", UPS_TYPE_UINT64, pred);
  }

  void averageOnRecordsTest() {
    double sum = 0;
    double pred = 0;
    int i = 0;

    // insert a few keys
    for (double d = 0; d < 10000; d++, i++) {
      ups_key_t key = ups_make_key(&d, sizeof(d));
      ups_record_t record = ups_make_record(&i, sizeof(i));
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
      sum += i;
      if (i < 5000)
        pred += i;
    }

    ResultProxy rp;
    REQUIRE(0 == uqi_select(env, "AVERAGE($record) from database 1",
                            &rp.result));
    rp.require("AVERAGE", UPS_TYPE_REAL64, sum / 10000.0)
      .close();

    uqi_plugin_t record_plugin = {0};
    record_plugin.name = "record_pred";
    record_plugin.type = UQI_PLUGIN_PREDICATE;
    record_plugin.pred = record_predicate;
    REQUIRE(0 == uqi_register_plugin(&record_plugin));
    REQUIRE(0 == uqi_select(env, "AVERAGE($record) from database 1 where "
                            "record_pred($record)", &rp.result));
    rp.require("AVERAGE", UPS_TYPE_REAL64, pred / 5000.0);
  }

  void pluginOnRecordsTest() {
    uint64_t sum = 0;
    uint64_t pred = 0;
    uint64_t i = 0;

    // insert a few keys
    for (double d = 0; d < 10000; d++, i++) {
      ups_key_t key = ups_make_key(&d, sizeof(d));
      ups_record_t record = ups_make_record(&i, sizeof(i));
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
      sum += i;
      if (i < 5000)
        pred += i;
    }

    uqi_plugin_t plugin = {0};
    plugin.name = "agg";
    plugin.type = UQI_PLUGIN_AGGREGATE;
    plugin.init = agg_init;
    plugin.agg_single = agg_single;
    plugin.agg_many = agg_many;
    plugin.results = agg_results;
    REQUIRE(0 == uqi_register_plugin(&plugin));

    ResultProxy rp;
    REQUIRE(0 == uqi_select(env, "agg($record) from database 1", &rp.result));
    rp.require("AGG", UPS_TYPE_UINT64, sum)
      .close();

    uqi_plugin_t record_plugin = {0};
    record_plugin.name = "record_pred";
    record_plugin.type = UQI_PLUGIN_PREDICATE;
    record_plugin.pred = record_predicate;
    REQUIRE(0 == uqi_register_plugin(&record_plugin));
    REQUIRE(0 == uqi_select(env, "agg($record) from database 1 where "
                            "record_pred($record)", &rp.result));
    rp.require("AGG", UPS_TYPE_UINT64, pred);
  }

  void valueTest() {
    ups_record_t record = {0};
    int count = 1000;

    // insert a few keys
    for (int i = 0; i < count; i++) {
      ups_key_t key = ups_make_key(&i, sizeof(i));
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
    }

    ResultProxy rp;
    REQUIRE(0 == uqi_select(env, "value($key) from database 1", &rp.result));
    rp.require_row_count(count)
      .require_key_type(UPS_TYPE_UINT32);

    for (int i = 0; i < count; i++)
      rp.require_key(i, &i, sizeof(i));
    rp.close();

    uqi_plugin_t even_plugin = {0};
    even_plugin.name = "even";
    even_plugin.type = UQI_PLUGIN_PREDICATE;
    even_plugin.pred = even_predicate;
    REQUIRE(0 == uqi_register_plugin(&even_plugin));

    REQUIRE(0 == uqi_select(env, "value($key) from database 1 "
                            "WHERE even($key)", &rp.result));
    rp.require_row_count(count / 2)
      .require_key_type(UPS_TYPE_UINT32);
    for (int i = 0; i < count; i++) {
      if (i & 1)
        continue;
      rp.require_key(i / 2, &i, sizeof(i));
    }
  }

  void valueOnRecordsTest() {
    int count = 1000;

    for (int i = 0; i < count; i++) {
      uint64_t r = i;
      ups_key_t key = ups_make_key(&i, sizeof(i));
      ups_record_t record = ups_make_record(&r, sizeof(r));
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
    }

    ResultProxy rp;
    REQUIRE(0 == uqi_select(env, "value($record) from database 1",
                            &rp.result));
    rp.require_row_count(count)
      .require_record_type(UPS_TYPE_UINT64);
    for (int i = 0; i < count; i++) {
      uint64_t r = i;
      rp.require_record(i, &r, sizeof(r));
    }
  }

  void binaryValueTest() {
    ups_record_t record = {0};
    int count = 200;
    char buffer[16] = {0};

    for (int i = 0; i < count; i++) {
      uint16_t size = sizeof(buffer) - (i % 5);
      ups_key_t key = ups_make_key(&buffer[0], size);
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
      buffer[0]++;
    }

    ResultProxy rp;
    REQUIRE(0 == uqi_select(env, "value($key) from database 1", &rp.result));
    rp.require_row_count(count)
      .require_key_type(UPS_TYPE_BINARY);

    buffer[0] = 0;
    for (int i = 0; i < count; i++) {
      ups_key_t key = {0};
      uint16_t size = sizeof(buffer) - (i % 5);
      rp.require_key(i, buffer, size);
      buffer[0]++;
    }
  }

  void binaryValueOnRecordsTest() {
    int count = 200;
    char buffer[300] = {0};

    for (int i = 0; i < count; i++) {
      ups_key_t key = ups_make_key(&i, sizeof(i));
      uint16_t size = sizeof(buffer) - (i % 5);
      ups_record_t record = ups_make_record(&buffer[0], size);
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
      buffer[0]++;
    }

    ResultProxy rp;
    REQUIRE(0 == uqi_select(env, "value($record) from database 1", &rp.result));
    rp.require_row_count(count)
      .require_record_type(UPS_TYPE_BINARY);

    buffer[0] = 0;
    for (int i = 0; i < count; i++) {
      uint16_t size = sizeof(buffer) - (i % 5);
      rp.require_record(i, buffer, size);
      buffer[0]++;
    }

    // reopen, try again. file is now mapped
    close();
    require_open();

    REQUIRE(0 == uqi_select(env, "value($record) from database 1", &rp.result));
    rp.require_row_count(count)
      .require_record_type(UPS_TYPE_BINARY);
  }

  void minMaxTest() {
    int count = 200;
    double min_record = std::numeric_limits<double>::max();
    double max_record = std::numeric_limits<double>::min();
    int min_key = 0;
    int max_key = 0;

    for (int i = 0; i < count; i++) {
      ups_key_t key = ups_make_key(&i, sizeof(i));
      double d = (double)::rand();
      ups_record_t record = ups_make_record(&d, sizeof(d));
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
      if (d < min_record) {
        min_record = d;
        min_key = i;
      }
      if (d > max_record) {
        max_record = d;
        max_key = i;
      }
    }

    ResultProxy rp;
    REQUIRE(0 == uqi_select(env, "min($record) from database 1", &rp.result));
    rp.require_record(0, &min_record, sizeof(min_record))
      .require_key(0, &min_key, sizeof(min_key))
      .close();

    REQUIRE(0 == uqi_select(env, "max($record) from database 1", &rp.result));
    rp.require_record(0, &max_record, sizeof(max_record))
      .require_key(0, &max_key, sizeof(max_key))
      .close();

    REQUIRE(UPS_PARSER_ERROR == uqi_select(env, "min($key, $record) "
                            "from database 1", &rp.result));
    REQUIRE(UPS_PARSER_ERROR == uqi_select(env, "max($key, $record) "
                            "from database 1", &rp.result));
  }

  void minMaxBinaryTest() {
    int count = 200;
    double min_record = std::numeric_limits<double>::max();
    double max_record = std::numeric_limits<double>::min();
    std::vector<char> min_key;
    std::vector<char> max_key;

    char buffer[300] = {0};
    for (int i = 0; i < count; i++) {
      ::sprintf(buffer, "%04d", i);

      ups_key_t key = ups_make_key(buffer, sizeof(buffer));
      double d = (double)::rand();
      ups_record_t record = ups_make_record(&d, sizeof(d));
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
      if (d < min_record) {
        min_key = std::vector<char>(buffer, &buffer[sizeof(buffer)]);
        min_record = d;
      }
      if (d > max_record) {
        max_key = std::vector<char>(buffer, &buffer[sizeof(buffer)]);
        max_record = d;
      }
    }

    ResultProxy rp;
    REQUIRE(0 == uqi_select(env, "min($record) from database 1", &rp.result));
    rp.require_record(0, &min_record, sizeof(min_record))
      .require_key(0, min_key.data(), min_key.size())
      .close();

    REQUIRE(0 == uqi_select(env, "max($record) from database 1", &rp.result));
    rp.require_record(0, &max_record, sizeof(max_record))
      .require_key(0, max_key.data(), max_key.size())
      .close();

    REQUIRE(UPS_PARSER_ERROR == uqi_select(env, "min($key, $record) "
                            "from database 1", &rp.result));
    REQUIRE(UPS_PARSER_ERROR == uqi_select(env, "max($key, $record) "
                            "from database 1", &rp.result));

    // reopen, try again. file is now mapped
    close();
    require_open();

    REQUIRE(0 == uqi_select(env, "min($record) from database 1", &rp.result));
    rp.require_record(0, &min_record, sizeof(min_record))
      .require_key(0, min_key.data(), min_key.size())
      .close();

    REQUIRE(0 == uqi_select(env, "max($record) from database 1", &rp.result));
    rp.require_record(0, &max_record, sizeof(max_record))
      .require_key(0, max_key.data(), max_key.size())
      .close();
  }

  void topBottomTest() {
    size_t count = 200;
    std::vector<uint32_t> inserted;
    std::vector<uint32_t> inserted_even;
    std::set<uint32_t> input;

    // insert 200 unique! integers
    while (input.size() < count)
      input.insert(::rand());

    int i = 0;
    for (std::set<uint32_t>::iterator it = input.begin();
          it != input.end(); it++, i++) {
      ups_key_t key = ups_make_key(&i, sizeof(i));
      uint32_t u = *it;
      ups_record_t record = ups_make_record(&u, sizeof(u));
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
      inserted.push_back(u);
      if ((i & 1) == 0)
        inserted_even.push_back(u);
    }

    std::sort(inserted.begin(), inserted.end());
    std::sort(inserted_even.begin(), inserted_even.end());

    ResultProxy rp;

    // top($record) limit 10
    REQUIRE(0 == uqi_select(env, "top($record) from database 1 limit 10",
                            &rp.result));
    rp.require_row_count(10);
    for (int i = 0; i < 10; i++)
      rp.require_record(i, &inserted[inserted.size() - 10 + i],
                      sizeof(uint32_t));
    rp.close();

    // top($record) limit 1
    REQUIRE(0 == uqi_select(env, "top($record) from database 1",
                            &rp.result));
    rp.require_row_count(1);
    for (int i = 0; i < 1; i++)
      rp.require_record(i, &inserted[inserted.size() - 1 + i],
                      sizeof(uint32_t));
    rp.close();

    // top($record) limit 50
    REQUIRE(0 == uqi_select(env, "top($record) from database 1 limit 50",
                            &rp.result));
    rp.require_row_count(50);
    for (int i = 0; i < 50; i++)
      rp.require_record(i, &inserted[inserted.size() - 50 + i],
                      sizeof(uint32_t));
    rp.close();

    // top($record) limit 10 where even($record)
    uqi_plugin_t even_plugin = {0};
    even_plugin.name = "even";
    even_plugin.type = UQI_PLUGIN_PREDICATE;
    even_plugin.pred = even_predicate;
    REQUIRE(0 == uqi_register_plugin(&even_plugin));

    REQUIRE(0 == uqi_select(env, "top($record) from database 1 "
                            "WHERE even($record) limit 10", &rp.result));
    rp.require_row_count(10);
    for (int i = 0; i < 10; i++)
      rp.require_record(i, &inserted_even[inserted_even.size() - 10 + i],
                      sizeof(uint32_t));
    rp.close();

    // bottom($record) limit 10
    REQUIRE(0 == uqi_select(env, "bottom($record) from database 1 limit 10",
                            &rp.result));
    rp.require_row_count(10);
    for (int i = 0; i < 10; i++)
      rp.require_record(i, &inserted[i], sizeof(uint32_t));
    rp.close();

    // bottom($record) limit 1
    REQUIRE(0 == uqi_select(env, "bottom($record) from database 1",
                            &rp.result));
    rp.require_row_count(1);
    for (int i = 0; i < 1; i++)
      rp.require_record(i, &inserted[i], sizeof(uint32_t));
    rp.close();

    // bottom($record) limit 50
    REQUIRE(0 == uqi_select(env, "bottom($record) from database 1 limit 50",
                            &rp.result));
    rp.require_row_count(50);
    for (int i = 0; i < 50; i++)
      rp.require_record(i, &inserted[i], sizeof(uint32_t));
    rp.close();

    // bottom($record) limit 10 where even($record)
    REQUIRE(0 == uqi_select(env, "bottom($record) from database 1 "
                            "WHERE even($record) limit 10", &rp.result));
    rp.require_row_count(10);
    for (int i = 0; i < 10; i++)
      rp.require_record(i, &inserted_even[i], sizeof(uint32_t));
  }

  typedef std::map<uint32_t, std::vector<char> > Map;

  void compare_results_reverse(uqi_result_t *result, Map &inserted) {
    uint32_t row_count = uqi_result_get_row_count(result);
    Map::iterator it = inserted.end();
    for (uint32_t i = 0; i < row_count; i++)
      it--;
    for (uint32_t i = 0; i < row_count; i++, it++) {
      ups_record_t rec = {0};
      uqi_result_get_record(result, i, &rec);
      REQUIRE(sizeof(uint32_t) == rec.size);
      REQUIRE(it->first == *(uint32_t *)rec.data);
      ups_key_t key = {0};
      uqi_result_get_key(result, i, &key);
      REQUIRE(16 == key.size);
      REQUIRE(0 == ::memcmp(it->second.data(), key.data, key.size));
    }
  }

  void compare_results(uqi_result_t *result, Map &inserted) {
    uint32_t row_count = uqi_result_get_row_count(result);
    Map::iterator it = inserted.begin();
    for (uint32_t i = 0; i < row_count; i++, it++) {
      ups_record_t rec = {0};
      uqi_result_get_record(result, i, &rec);
      REQUIRE(sizeof(uint32_t) == rec.size);
      REQUIRE(it->first == *(uint32_t *)rec.data);
      ups_key_t key = {0};
      uqi_result_get_key(result, i, &key);
      REQUIRE(16 == key.size);
      REQUIRE(0 == ::memcmp(it->second.data(), key.data, key.size));
    }
  }

  void topBottomBinaryTest() {
    int count = 200;
    Map inserted;
    Map inserted_even;

    char buffer[16] = {0};
    for (int i = 0; i < count; i++) {
      ::sprintf(buffer, "%04d", i);

      uint32_t u = ::rand();
      ups_key_t key = ups_make_key(buffer, sizeof(buffer));
      ups_record_t record = ups_make_record(&u, sizeof(u));
      REQUIRE(0 == ups_db_insert(db, 0, &key, &record, 0));
      inserted[u] = std::vector<char>(buffer, buffer + sizeof(buffer));

      int *p = (int *)&buffer[0]; // copy behaviour from "even" plugin
      if ((*p & 1) == 0)
        inserted_even[u] = std::vector<char>(buffer, buffer + sizeof(buffer));
    }

    uqi_result_t *result;

    // top($record) limit 10
    REQUIRE(0 == uqi_select(env, "top($record) from database 1 limit 10",
                            &result));
    REQUIRE(uqi_result_get_row_count(result) == 10);
    compare_results_reverse(result, inserted);
    uqi_result_close(result);

    // top($record) limit 1
    REQUIRE(0 == uqi_select(env, "top($record) from database 1",
                            &result));
    REQUIRE(uqi_result_get_row_count(result) == 1);
    compare_results_reverse(result, inserted);
    uqi_result_close(result);

    // top($record) limit 50
    REQUIRE(0 == uqi_select(env, "top($record) from database 1 limit 50",
                            &result));
    REQUIRE(uqi_result_get_row_count(result) == 50);
    compare_results_reverse(result, inserted);
    uqi_result_close(result);

    // top($record) limit 10 where even($record)
    uqi_plugin_t even_plugin = {0};
    even_plugin.name = "even";
    even_plugin.type = UQI_PLUGIN_PREDICATE;
    even_plugin.pred = even_predicate;
    REQUIRE(0 == uqi_register_plugin(&even_plugin));

    REQUIRE(0 == uqi_select(env, "top($record) from database 1 "
                            "WHERE even($record) limit 10", &result));
    REQUIRE(uqi_result_get_row_count(result) == 10);
    compare_results_reverse(result, inserted_even);
    uqi_result_close(result);

    // bottom($record) limit 10
    REQUIRE(0 == uqi_select(env, "bottom($record) from database 1 limit 10",
                            &result));
    REQUIRE(uqi_result_get_row_count(result) == 10);
    compare_results(result, inserted);
    uqi_result_close(result);

    // bottom($record) limit 1
    REQUIRE(0 == uqi_select(env, "bottom($record) from database 1",
                            &result));
    REQUIRE(uqi_result_get_row_count(result) == 1);
    compare_results(result, inserted);
    uqi_result_close(result);

    // bottom($record) limit 50
    REQUIRE(0 == uqi_select(env, "bottom($record) from database 1 limit 50",
                            &result));
    REQUIRE(uqi_result_get_row_count(result) == 50);
    compare_results(result, inserted);
    uqi_result_close(result);

    // bottom($record) limit 10 where even($record)
    REQUIRE(0 == uqi_select(env, "bottom($record) from database 1 "
                            "WHERE even($record) limit 10", &result));
    REQUIRE(uqi_result_get_row_count(result) == 10);
    compare_results(result, inserted_even);
    uqi_result_close(result);
  }

  void issue102Test() {
    close();
    ups_parameter_t params[] = {
        {UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT32},
        {0, }
    };

    require_create(UPS_ENABLE_TRANSACTIONS, nullptr, 0, params);

    ups_txn_t *txn;
    REQUIRE(0 == ups_txn_begin(&txn, env, 0, 0, 0));

    ups_db_t *db;
    REQUIRE(0 == ups_env_create_db(env, &db, 3, 0, &params[0]));

    for (int i = 0; i < 4; i++) {
      ups_key_t key = ups_make_key(&i, sizeof(i));
      ups_record_t record = {0};

      REQUIRE(0 == ups_db_insert(db, txn, &key, &record, 0));
    }

    uint64_t size;
    REQUIRE(0 == ups_db_count(db, 0, 0, &size));
	REQUIRE(size == 0);

    REQUIRE(0 == ups_txn_commit(txn, 0));
    REQUIRE(0 == ups_db_close(db, 0));
  }
};

// fixed length keys, fixed length records
TEST_CASE("Uqi/queryTest1", "")
{
  QueryFixture f(0, UPS_TYPE_UINT32, UPS_TYPE_UINT64);
  f.run("sum");
}

// fixed length keys, fixed length records
TEST_CASE("Uqi/queryTest2", "")
{
  QueryFixture f(0, UPS_TYPE_UINT32, UPS_TYPE_REAL64);
  f.run("sum");
}

// variable length keys, fixed length records
// SUM does not work on binary keys, therefore use a custom aggregation function
TEST_CASE("Uqi/queryTest3", "")
{
  uqi_plugin_t plugin = {0};
  plugin.name = "agg";
  plugin.type = UQI_PLUGIN_AGGREGATE;
  plugin.init = agg_init;
  plugin.agg_single = agg_single;
  plugin.agg_many = agg_many;
  plugin.results = agg_results;
  REQUIRE(0 == uqi_register_plugin(&plugin));

  QueryFixture f(0, UPS_TYPE_BINARY, UPS_TYPE_UINT64);
  f.run("agg");
}

// variable length keys, variable length records
// SUM does not work on binary keys, therefore use a custom aggregation function
TEST_CASE("Uqi/queryTest4", "")
{
  uqi_plugin_t plugin = {0};
  plugin.name = "agg";
  plugin.type = UQI_PLUGIN_AGGREGATE;
  plugin.init = agg_init;
  plugin.agg_single = agg_single;
  plugin.agg_many = agg_many;
  plugin.results = agg_results;
  REQUIRE(0 == uqi_register_plugin(&plugin));

  QueryFixture f(0, UPS_TYPE_BINARY, UPS_TYPE_BINARY);
  f.run("agg");
}

TEST_CASE("Uqi/resultTest", "")
{
  QueryFixture f(0, UPS_TYPE_BINARY, UPS_TYPE_BINARY);
  f.resultTest();
}

TEST_CASE("Uqi/sumOnRecordsTest", "")
{
  QueryFixture f(0, UPS_TYPE_REAL64, UPS_TYPE_UINT32);
  f.sumOnRecordsTest();
}

TEST_CASE("Uqi/averageOnRecordsTest", "")
{
  QueryFixture f(0, UPS_TYPE_REAL64, UPS_TYPE_UINT32);
  f.averageOnRecordsTest();
}

TEST_CASE("Uqi/pluginOnRecordsTest", "")
{
  QueryFixture f(0, UPS_TYPE_REAL64, UPS_TYPE_UINT64);
  f.pluginOnRecordsTest();
}

TEST_CASE("Uqi/valueTest", "")
{
  QueryFixture f(0, UPS_TYPE_UINT32, UPS_TYPE_BINARY);
  f.valueTest();
}

TEST_CASE("Uqi/valueOnRecordsTest", "")
{
  QueryFixture f(0, UPS_TYPE_UINT32, UPS_TYPE_UINT64);
  f.valueOnRecordsTest();
}

TEST_CASE("Uqi/binaryValueTest", "")
{
  QueryFixture f(0, UPS_TYPE_BINARY, UPS_TYPE_BINARY);
  f.binaryValueTest();
}

TEST_CASE("Uqi/binaryValueOnRecordsTest", "")
{
  QueryFixture f(0, UPS_TYPE_UINT32, UPS_TYPE_BINARY);
  f.binaryValueOnRecordsTest();
}

TEST_CASE("Uqi/minMaxTest", "")
{
  QueryFixture f(0, UPS_TYPE_UINT32, UPS_TYPE_REAL64);
  f.minMaxTest();
}

TEST_CASE("Uqi/minMaxBinaryTest", "")
{
  QueryFixture f(0, UPS_TYPE_BINARY, UPS_TYPE_REAL64);
  f.minMaxBinaryTest();
}

TEST_CASE("Uqi/topBottomTest", "")
{
  QueryFixture f(0, UPS_TYPE_UINT32, UPS_TYPE_UINT32);
  f.topBottomTest();
}

TEST_CASE("Uqi/topBottomBinaryTest", "")
{
  QueryFixture f(0, UPS_TYPE_BINARY, UPS_TYPE_UINT32);
  f.topBottomBinaryTest();
}

TEST_CASE("Uqi/issue102Test", "")
{
  QueryFixture f(0, UPS_TYPE_UINT32, UPS_TYPE_BINARY);
  f.issue102Test();
}

} // namespace upscaledb
