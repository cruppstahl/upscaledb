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

#include "3rdparty/catch/catch.hpp"

#include "ups/upscaledb_uqi.h"

#include "3btree/btree_index.h"
#include "4context/context.h"
#include "4db/db_local.h"
#include "4uqi/plugins.h"
#include "4uqi/parser.h"

#include "utils.h"
#include "os.hpp"

namespace upscaledb {

static int
even_predicate(void *state, const void *data, uint16_t size)
{
  const uint32_t *i = (const uint32_t *)data;
  return ((*i & 1) == 0);
}

static int
test1_predicate(void *state, const void *data, uint16_t size)
{
  const uint8_t *p = (const uint8_t *)data;
  return ((p[0] & 1) == 0);
}

static int
lt10_predicate(void *state, const void *data, uint16_t size)
{
  const float *f = (const float *)data;
  return (*f < 10.0f);
}

struct UqiFixture {
  ups_db_t *m_db;
  ups_env_t *m_env;
  bool m_use_transactions;

  UqiFixture(bool use_transactions, int key_type, bool use_duplicates = false,
                uint32_t page_size = 1024 * 16)
    : m_use_transactions(use_transactions) {
    os::unlink(Utils::opath(".test"));
    ups_parameter_t env_params[] = {
        {UPS_PARAM_PAGE_SIZE, (uint64_t)page_size},
        {0, 0}
    };
    ups_parameter_t db_params[] = {
        {UPS_PARAM_KEY_TYPE, (uint64_t)key_type},
        {0, 0}
    };
    REQUIRE(0 == ups_env_create(&m_env, ".test",
                            use_transactions
                                ? UPS_ENABLE_TRANSACTIONS
                                : 0,
                            0, &env_params[0]));
    REQUIRE(0 == ups_env_create_db(m_env, &m_db, 1,
                            use_duplicates
                                ? UPS_ENABLE_DUPLICATES
                                : 0, &db_params[0]));
  }

  ~UqiFixture() {
    teardown();
  }

  void teardown() {
    if (m_env)
      REQUIRE(0 == ups_env_close(m_env, UPS_AUTO_CLEANUP));
    m_env = 0;
    m_db = 0;
  }

  void countTest(int count) {
    ups_key_t key = {0};
    ups_record_t record = {0};

    // insert a few keys
    for (int i = 0; i < count; i++) {
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ups_db_insert(m_db, 0, &key, &record, 0));
    }

    uqi_result_t result;
    REQUIRE(0 == uqi_select(m_env, "coUNT ($key) from database 1", &result));
    REQUIRE(result.type == UPS_TYPE_UINT64);
    REQUIRE(result.u.result_u64 == (uint64_t)count);
  }

  void cursorTest() {
    int i;
    ups_key_t key = ups_make_key(&i, sizeof(i));
    ups_record_t record = {0};
    uint32_t sum = 0;

    // insert a few keys
    for (i = 0; i < 10; i++) {
      REQUIRE(0 == ups_db_insert(m_db, 0, &key, &record, 0));
      sum += i;
    }

    ups_cursor_t *cursor;
    REQUIRE(0 == ups_cursor_create(&cursor, m_db, 0, 0));

    uqi_result_t result;

    REQUIRE(0 == ups_cursor_move(cursor, 0, 0, UPS_CURSOR_FIRST));
    REQUIRE(0 == uqi_select_range(m_env, "SUM($key) from database 1",
                            &cursor, 0, &result));
    REQUIRE(result.type == UPS_TYPE_UINT64);
    REQUIRE(result.u.result_u64 == sum);

    i = 5;
    REQUIRE(0 == ups_cursor_find(cursor, &key, 0, 0));
    REQUIRE(0 == uqi_select_range(m_env, "SUM($key) from database 1",
                            &cursor, 0, &result));
    REQUIRE(result.type == UPS_TYPE_UINT64);
    REQUIRE(result.u.result_u64 == 5 + 6 + 7 + 8 + 9);

    // |cursor| is now located at the end of the database
    REQUIRE(UPS_KEY_NOT_FOUND == ups_cursor_move(cursor, 0, 0,
                                          UPS_CURSOR_NEXT));

    REQUIRE(0 == ups_cursor_close(cursor));
  }

  void invalidCursorTest() {
    int i;
    ups_key_t key = ups_make_key(&i, sizeof(i));
    ups_record_t record = {0};
    uint32_t sum = 0;

    // create an empty second database
    ups_db_t *db2;
    REQUIRE(0 == ups_env_create_db(m_env, &db2, 2, 0, 0));

    // insert a few keys into the first(!) database
    for (i = 0; i < 10; i++) {
      REQUIRE(0 == ups_db_insert(m_db, 0, &key, &record, 0));
      sum += i;
    }

    ups_cursor_t *cursor1;
    REQUIRE(0 == ups_cursor_create(&cursor1, m_db, 0, 0));
    ups_cursor_t *cursor2;
    REQUIRE(0 == ups_cursor_create(&cursor2, m_db, 0, 0));

    uqi_result_t result;
    REQUIRE(UPS_CURSOR_IS_NIL
                    == uqi_select_range(m_env, "SUM($key) from database 1",
                            &cursor1, 0, &result));

    REQUIRE(0 == ups_cursor_move(cursor1, 0, 0, UPS_CURSOR_FIRST));

    // use cursor of db1 on database 2 -> must fail
    REQUIRE(UPS_INV_PARAMETER
                    == uqi_select_range(m_env, "SUM($key) from database 2",
                            &cursor1, 0, &result));

    REQUIRE(0 == ups_cursor_close(cursor1));
    REQUIRE(0 == ups_cursor_close(cursor2));
  }

  void sumTest(int count) {
    ups_key_t key = {0};
    ups_record_t record = {0};
    uint32_t sum = 0;

    // insert a few keys
    for (int i = 0; i < count; i++) {
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ups_db_insert(m_db, 0, &key, &record, 0));
      sum += i;
    }

    uqi_result_t result;
    REQUIRE(0 == uqi_select(m_env, "SUM($key) from database 1", &result));
    REQUIRE(result.type == UPS_TYPE_UINT64);
    REQUIRE(result.u.result_u64 == sum);
  }

  void closedDatabaseTest() {
    ups_key_t key = {0};
    ups_record_t record = {0};
    uint32_t sum = 0;

    // insert a few keys
    for (int i = 0; i < 10; i++) {
      key.data = &i;
      key.size = sizeof(i);
      REQUIRE(0 == ups_db_insert(m_db, 0, &key, &record, 0));
      sum += i;
    }

    // now close it - will be opened automatically
    REQUIRE(0 == ups_db_close(m_db, 0));
    m_db = 0;

    uqi_result_t result;
    REQUIRE(0 == uqi_select(m_env, "SUM($key) from database 1", &result));
    REQUIRE(result.type == UPS_TYPE_UINT64);
    REQUIRE(result.u.result_u64 == sum);
  }

  void unknownDatabaseTest() {
    uqi_result_t result;
    REQUIRE(UPS_DATABASE_NOT_FOUND
                == uqi_select(m_env, "SUM($key) from database 100", &result));
  }

  ups_status_t insertBtree(uint32_t key) {
    ups_key_t k = ups_make_key(&key, sizeof(key));
    ups_record_t r = {0};

    Context context((LocalEnvironment *)m_env, 0, 0);

    BtreeIndex *be = ((LocalDatabase *)m_db)->btree_index();
    return (be->insert(&context, 0, &k, &r, 0));
  }

  ups_status_t insertBtree(const std::string &key) {
    ups_key_t k = ups_make_key((void *)key.c_str(), (uint16_t)key.size());
    ups_record_t r = {0};

    Context context((LocalEnvironment *)m_env, 0, 0);

    BtreeIndex *be = ((LocalDatabase *)m_db)->btree_index();
    return (be->insert(&context, 0, &k, &r, 0));
  }

  ups_status_t insertTxn(ups_txn_t *txn, uint32_t key) {
    ups_key_t k = ups_make_key(&key, sizeof(key));
    ups_record_t r = {0};

    return (ups_db_insert(m_db, txn, &k, &r, 0));
  }

  ups_status_t insertTxn(ups_txn_t *txn, const std::string &key) {
    ups_key_t k = ups_make_key((void *)key.c_str(), (uint16_t)key.size());
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

    // insert a few keys
    REQUIRE(0 == insertBtree(1)); sum += 1;
    REQUIRE(0 == insertBtree(2)); sum += 2;
    REQUIRE(0 == insertBtree(3)); sum += 3;

    // check the sum
    uqi_result_t result;
    REQUIRE(0 == uqi_select(m_env, "SUM($key) from database 1", &result));
    REQUIRE(result.type == UPS_TYPE_UINT64);
    REQUIRE(result.u.result_u64 == sum);

    // continue with more keys
    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(0 == insertTxn(txn, 4)); sum += 4;
    REQUIRE(0 == insertTxn(txn, 5)); sum += 5;
    REQUIRE(0 == insertTxn(txn, 6)); sum += 6;
    REQUIRE(0 == ups_txn_commit(txn, 0));

    // check the sum
    REQUIRE(0 == uqi_select(m_env, "SUM($key) from database 1", &result));
    REQUIRE(result.u.result_u64 == sum);

    // continue inserting keys
    REQUIRE(0 == insertBtree(7)); sum += 7;
    REQUIRE(0 == insertBtree(8)); sum += 8;
    REQUIRE(0 == insertBtree(9)); sum += 9;

    // check once more
    REQUIRE(0 == uqi_select(m_env, "SUM($key) from database 1", &result));
    REQUIRE(result.u.result_u64 == sum);

    // repeat two more times
    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(0 == insertTxn(txn, 10)); sum += 10;
    REQUIRE(0 == insertTxn(txn, 11)); sum += 11;
    REQUIRE(0 == insertTxn(txn, 12)); sum += 12;
    REQUIRE(0 == ups_txn_commit(txn, 0));

    REQUIRE(0 == uqi_select(m_env, "SUM($key) from database 1", &result));
    REQUIRE(result.u.result_u64 == sum);

    REQUIRE(0 == insertBtree(13)); sum += 13;
    REQUIRE(0 == insertBtree(14)); sum += 14;
    REQUIRE(0 == insertBtree(15)); sum += 15;

    REQUIRE(0 == uqi_select(m_env, "SUM($key) from database 1", &result));
    REQUIRE(result.u.result_u64 == sum);
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
    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));
    for (int i = 0; i < 24; i++) {
      buffer[0] = 'a' + i;
      buffer[1] = '\0';
      REQUIRE(0 == insertTxn(txn, buffer));
    }
    REQUIRE(0 == ups_txn_commit(txn, 0));

    uqi_result_t result;
    REQUIRE(0 == uqi_select(m_env, "COUNT($key) from database 1", &result));
    REQUIRE(result.type == UPS_TYPE_UINT64);
    REQUIRE(result.u.result_u64 == 24 * 2);
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

    // insert a few keys
    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(0 == insertTxn(txn, 1));  sum += 1;
    REQUIRE(0 == insertTxn(txn, 2));  sum += 2;
    REQUIRE(0 == insertTxn(txn, 3));  sum += 3;
    REQUIRE(0 == ups_txn_commit(txn, 0));

    // check the sum
    uqi_result_t result;
    REQUIRE(0 == uqi_select(m_env, "SUM($key) from database 1", &result));
    REQUIRE(result.type == UPS_TYPE_UINT64);
    REQUIRE(result.u.result_u64 == sum);

    // continue with more keys
    REQUIRE(0 == insertBtree(4));  sum += 4;
    REQUIRE(0 == insertBtree(5));  sum += 5;
    REQUIRE(0 == insertBtree(6));  sum += 6;
    REQUIRE(0 == ups_txn_commit(txn, 0));

    // check the sum
    REQUIRE(0 == uqi_select(m_env, "SUM($key) from database 1", &result));
    REQUIRE(result.u.result_u64 == sum);

    // continue inserting keys
    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(0 == insertTxn(txn, 7));  sum += 7;
    REQUIRE(0 == insertTxn(txn, 8));  sum += 8;
    REQUIRE(0 == insertTxn(txn, 9));  sum += 9;
    REQUIRE(0 == ups_txn_commit(txn, 0));

    // check once more
    REQUIRE(0 == uqi_select(m_env, "SUM($key) from database 1", &result));
    REQUIRE(result.u.result_u64 == sum);

    // repeat two more times
    REQUIRE(0 == insertBtree(10));  sum += 10;
    REQUIRE(0 == insertBtree(11));  sum += 11;
    REQUIRE(0 == insertBtree(12));  sum += 12;

    REQUIRE(0 == uqi_select(m_env, "SUM($key) from database 1", &result));
    REQUIRE(result.u.result_u64 == sum);

    REQUIRE(0 == ups_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(0 == insertTxn(txn, 13));  sum += 13;
    REQUIRE(0 == insertTxn(txn, 14));  sum += 14;
    REQUIRE(0 == insertTxn(txn, 15));  sum += 15;
    REQUIRE(0 == ups_txn_commit(txn, 0));

    REQUIRE(0 == uqi_select(m_env, "SUM($key) from database 1", &result));
    REQUIRE(result.u.result_u64 == sum);
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

    uqi_plugin_t even_plugin = {0};
    even_plugin.name = "even";
    even_plugin.type = UQI_PLUGIN_PREDICATE;
    even_plugin.pred = even_predicate;
    REQUIRE(0 == uqi_register_plugin(&even_plugin));

    uqi_result_t result;
    REQUIRE(0 == uqi_select(m_env, "SUM($key) from database 1 WHERE even($key)",
                            &result));
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
    REQUIRE(0 == uqi_select(m_env, "AVERAGE($key) from database 1", &result));
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

    uqi_plugin_t plugin = {0};
    plugin.name = "if_lt_10";
    plugin.type = UQI_PLUGIN_PREDICATE;
    plugin.pred = lt10_predicate;
    REQUIRE(0 == uqi_register_plugin(&plugin));

    uqi_result_t result;
    REQUIRE(0 == uqi_select(m_env, "average($key) from database 1 WHERE "
                            "IF_Lt_10($key)", &result));
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

    uqi_plugin_t plugin = {0};
    plugin.name = "test1";
    plugin.type = UQI_PLUGIN_PREDICATE;
    plugin.pred = test1_predicate;
    REQUIRE(0 == uqi_register_plugin(&plugin));

    uqi_result_t result;
    REQUIRE(0 == uqi_select(m_env, "COUNT($key) from database 1 WHERE test1($key)",
                            &result));
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

    uqi_plugin_t plugin = {0};
    plugin.name = "test1";
    plugin.type = UQI_PLUGIN_PREDICATE;
    plugin.pred = test1_predicate;
    REQUIRE(0 == uqi_register_plugin(&plugin));

    uqi_result_t result;
    REQUIRE(0 == uqi_select(m_env, "dinstinct COUNT($key) from database 1 "
                            "WHERE test1($key)", &result));
    REQUIRE(result.type == UPS_TYPE_UINT64);
    REQUIRE(result.u.result_u64 == c / 2);

    REQUIRE(0 == uqi_select(m_env, "count($key) from database 1 "
                            "WHERE test1($key)", &result));
    REQUIRE(result.type == UPS_TYPE_UINT64);
    REQUIRE(result.u.result_u64 == c / 2);
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
  REQUIRE(upscaledb::PluginManager::get("test4") != 0);
  REQUIRE(upscaledb::PluginManager::is_registered("test4") == true);
}

static void
check(const char *query, bool distinct, const char *function,
                uint16_t db, const char *predicate = 0, int limit = 0)
{
  SelectStatement stmt;
  REQUIRE(upscaledb::Parser::parse_select(query, stmt) == 0);
  REQUIRE(stmt.distinct == distinct);
  REQUIRE(stmt.dbid == db);
  REQUIRE(stmt.function.first == function);
  REQUIRE(stmt.limit == limit);
  if (predicate)
    REQUIRE(stmt.predicate.first == predicate);
}

TEST_CASE("Uqi/parserTest", "")
{
  SelectStatement stmt;
  REQUIRE(upscaledb::Parser::parse_select("", stmt)
                == UPS_PARSER_ERROR);
  REQUIRE(upscaledb::Parser::parse_select("foo bar", stmt)
                == UPS_PARSER_ERROR);

  REQUIRE(upscaledb::Parser::parse_select("bar($key) from database 1", stmt)
                == 0);
  REQUIRE(upscaledb::PluginManager::import("./plugin.so", "test4")
                == 0);
  REQUIRE(upscaledb::Parser::parse_select("test4($key) from database 1", stmt)
                == 0);
  REQUIRE(upscaledb::Parser::parse_select("\"test4@./plugin.so\"($key) from database 1", stmt)
                == 0);
  REQUIRE(upscaledb::Parser::parse_select("\"test4@no.so\"($key) from database 1", stmt)
                == UPS_PLUGIN_NOT_FOUND);

  check("test4($key) from database 10",
                false, "test4", 10);
  check("DISTINCT test4($key) from database 10",
                true, "test4", 10);
  check("test4($key) from database 1 where test4($key)",
                false, "test4", 1, "test4");
  check("test4($key) from database 1 where test4($key) limit 12",
                false, "test4", 1, "test4", 12);
  check("DISTINCT test4($key) from database 10 limit 999",
                true, "test4", 10, 0, 999);
  check("DISTINCT test4($key) from database 10 limit 0",
                true, "test4", 10, 0, 0);
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

} // namespace upscaledb
