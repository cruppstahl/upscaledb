/**
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include <string.h>

#include "3rdparty/catch/catch.hpp"
#include "3rdparty/sparsemap/sparsemap.h"

#include "utils.h"

#include "../src/db_local.h"
#include "../src/btree_node_proxy.h"
#include "../src/btree_index.h"
#include "../src/page_manager.h"

using namespace hamsterdb;
using namespace sparsemap;

TEST_CASE("Bitmap/persistentFlag", "")
{
  ham_parameter_t params[] = {
      {HAM_PARAM_KEY_TYPE, HAM_TYPE_UINT64},
      {HAM_PARAM_KEY_COMPRESSION, HAM_COMPRESSOR_BITMAP},
      {0, 0}
  };

  ham_env_t *env;
  ham_db_t *db;

  REQUIRE(0 == ham_env_create(&env, Utils::opath("test.db"), 0, 0644, 0));
  REQUIRE(0 == ham_env_create_db(env, &db, 1, HAM_RECORD_NUMBER, &params[0]));
  LocalDatabase *ldb = (LocalDatabase *)db;
  REQUIRE(ldb->get_key_compression_algorithm() == HAM_COMPRESSOR_BITMAP);
  REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));

  REQUIRE(0 == ham_env_open(&env, Utils::opath("test.db"), 0, 0));
  REQUIRE(0 == ham_env_open_db(env, &db, 1, 0, 0));
  ldb = (LocalDatabase *)db;
  REQUIRE(ldb->get_key_compression_algorithm() == HAM_COMPRESSOR_BITMAP);
  REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
}

template<typename IndexedType, typename BitVector>
void
sparsemap_tests()
{
  uint8_t buffer[1024];

  SparseMap<IndexedType, BitVector> sm;
  sm.create(buffer, sizeof(buffer));
  REQUIRE(sm.get_size() == 4);
  sm.set(0, true);
  REQUIRE(sm.get_size() == 4 + sizeof(IndexedType) + sizeof(BitVector) * 2);
  REQUIRE(sm.is_set(0) == true);
  REQUIRE(sm.get_size() == 4 + sizeof(IndexedType) + sizeof(BitVector) * 2);
  REQUIRE(sm.is_set(1) == false);
  sm.set(0, false);
  REQUIRE(sm.get_size() == 4);

  sm.clear();
  sm.set(64, true);
  REQUIRE(sm.is_set(64) == true);
  REQUIRE(sm.get_size() == 4 + sizeof(IndexedType) + sizeof(BitVector) * 2);

  sm.clear();

  // set [0..10000]
  for (int i = 0; i < 10000; i++) {
    REQUIRE(sm.is_set(i) == false);
    sm.set(i, true);
    REQUIRE(sm.is_set(i) == true);
  }

  for (int i = 0; i < 10000; i++)
    REQUIRE(sm.is_set(i) == true);

  // unset [0..10000]
  for (int i = 0; i < 10000; i++) {
    REQUIRE(sm.is_set(i) == true);
    sm.set(i, false);
    REQUIRE(sm.is_set(i) == false);
  }

  for (int i = 0; i < 10000; i++)
    REQUIRE(sm.is_set(i) == false);

  sm.clear();

  // set [10000..0]
  for (int i = 10000; i >= 0; i--) {
    REQUIRE(sm.is_set(i) == false);
    sm.set(i, true);
    REQUIRE(sm.is_set(i) == true);
  }

  for (int i = 10000; i >= 0; i--)
    REQUIRE(sm.is_set(i) == true);

  // open and compare
  SparseMap<IndexedType, BitVector> sm2;
  sm2.open(buffer, sizeof(buffer));
  for (int i = 0; i < 10000; i++)
    REQUIRE(sm2.is_set(i) == sm.is_set(i));

  // unset [10000..0]
  for (int i = 10000; i >= 0; i--) {
    REQUIRE(sm.is_set(i) == true);
    sm.set(i, false);
    REQUIRE(sm.is_set(i) == false);
  }

  for (int i = 10000; i >= 0; i--)
    REQUIRE(sm.is_set(i) == false);

  sm.clear();

  int capacity;
  if (sizeof(BitVector) == 4)
    capacity = 512;
  else
    capacity = 2048;

  sm.set(0, true);
  sm.set(capacity * 2 + 1, true);
  REQUIRE(sm.is_set(0) == true);
  REQUIRE(sm.is_set(capacity * 2 + 0) == false);
  REQUIRE(sm.is_set(capacity * 2 + 1) == true);
  REQUIRE(sm.is_set(capacity * 2 + 2) == false);
  sm.set(capacity, true);
  REQUIRE(sm.is_set(0) == true);
  REQUIRE(sm.is_set(capacity - 1) == false);
  REQUIRE(sm.is_set(capacity) == true);
  REQUIRE(sm.is_set(capacity + 1) == false);
  REQUIRE(sm.is_set(capacity * 2 + 2) == false);
  REQUIRE(sm.is_set(capacity * 2 + 0) == false);
  REQUIRE(sm.is_set(capacity * 2 + 1) == true);
  REQUIRE(sm.is_set(capacity * 2 + 2) == false);

  sm.clear();

  for (int i = 0; i < 10000; i++)
    sm.set(i, true);
  for (int i = 0; i < 10000; i++)
    REQUIRE(sm.select(i) == (unsigned)i);
  for (int i = 0; i < 10000; i++)
    REQUIRE(sm.calc_popcount(i) == (unsigned)i);
  for (int i = 0; i < 10000; i++) {
    sm.set(i, false);
    REQUIRE(sm.calc_popcount(i) == 0);
    if (i < 9999)
      REQUIRE(sm.select(0) == (size_t)i + 1);
  }

  sm.clear();

  for (int i = 0; i < 8; i++)
    sm.set(i * 10, true);
  for (int i = 0; i < 8; i++)
    REQUIRE(sm.select(i) == (unsigned)i * 10);

  uint8_t buffer2[1024];
  sm2.create(buffer2, sizeof(buffer2));
  sm.clear();
  for (int i = 0; i < capacity * 2; i++)
    sm.set(i, true);
  sm.split(capacity, &sm2);
  for (int i = 0; i < capacity; i++) {
    REQUIRE(sm.is_set(i) == true);
    REQUIRE(sm2.is_set(i) == false);
  }
  for (int i = capacity; i < capacity * 2; i++) {
    REQUIRE(sm.is_set(i) == false);
    REQUIRE(sm2.is_set(i) == true);
  }
}

TEST_CASE("Bitmap/sparseMapTests1", "")
{
  sparsemap_tests<ham_u32_t, ham_u64_t>();
}
  
TEST_CASE("Bitmap/sparseMapTests2", "")
{
  sparsemap_tests<ham_u64_t, ham_u64_t>();
}

TEST_CASE("Bitmap/sparseMapTests3", "")
{
  sparsemap_tests<ham_u64_t, ham_u32_t>();
}

TEST_CASE("Bitmap/sparseMapTests4", "")
{
  sparsemap_tests<ham_u32_t, ham_u32_t>();
}

TEST_CASE("Bitmap/sparseMapTests5", "")
{
  sparsemap_tests<ham_u16_t, ham_u32_t>();
}

TEST_CASE("Bitmap/sparseMapTests6", "")
{
  sparsemap_tests<ham_u16_t, ham_u64_t>();
}

TEST_CASE("Bitmap/nodeCapacityTest1", "")
{
  ham_parameter_t params[] = {
      {HAM_PARAM_KEY_TYPE, HAM_TYPE_UINT64},
      {HAM_PARAM_KEY_COMPRESSION, HAM_COMPRESSOR_BITMAP},
      {0, 0}
  };

  ham_env_t *env;
  ham_db_t *db;
  Page *page;

  REQUIRE(0 == ham_env_create(&env, Utils::opath("test.db"), 0, 0644, 0));
  REQUIRE(0 == ham_env_create_db(env, &db, 1, HAM_RECORD_NUMBER, &params[0]));

  BtreeIndex *bt = ((LocalDatabase *)db)->get_btree_index();
  REQUIRE((page = ((LocalEnvironment *)env)->get_page_manager()->fetch_page(
                          (LocalDatabase *)db, bt->get_root_address())));
  BtreeNodeProxy *node = bt->get_node_from_page(page);
  REQUIRE(node->get_capacity() == 1760);
  REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
}

TEST_CASE("Bitmap/nodeCapacityTest2", "")
{
  ham_parameter_t params[] = {
      {HAM_PARAM_KEY_TYPE, HAM_TYPE_UINT64},
      {HAM_PARAM_KEY_COMPRESSION, HAM_COMPRESSOR_BITMAP},
      {HAM_PARAM_RECORD_SIZE, 0},
      {0, 0}
  };

  ham_env_t *env;
  ham_db_t *db;
  Page *page;

  REQUIRE(0 == ham_env_create(&env, Utils::opath("test.db"), 0, 0644, 0));
  REQUIRE(0 == ham_env_create_db(env, &db, 1, HAM_RECORD_NUMBER, &params[0]));

  BtreeIndex *bt = ((LocalDatabase *)db)->get_btree_index();
  REQUIRE((page = ((LocalEnvironment *)env)->get_page_manager()->fetch_page(
                          (LocalDatabase *)db, bt->get_root_address())));
  BtreeNodeProxy *node = bt->get_node_from_page(page);
  REQUIRE(130624 == node->get_capacity());
  REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
}

static void
insert_find_erase(int record_size)
{
  ham_parameter_t params[] = {
      {HAM_PARAM_KEY_COMPRESSION, HAM_COMPRESSOR_BITMAP},
      {HAM_PARAM_RECORD_SIZE, (ham_u64_t)record_size},
      {0, 0}
  };

  ham_env_t *env;
  ham_db_t *db;

  REQUIRE(0 == ham_env_create(&env, Utils::opath("test.db"), 0, 0644, 0));
  REQUIRE(0 == ham_env_create_db(env, &db, 1, HAM_RECORD_NUMBER, &params[0]));

  char buffer1[64] = {0};

  for (ham_u64_t i = 0; i < 10000; i++) {
    ham_key_t key = {0};
    ham_record_t rec = {0};
    sprintf(buffer1, "%04lu", i);
    rec.data = &buffer1[0];
    rec.size = record_size;

    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));
  }

  for (ham_u64_t i = 0; i < 10000; i++) {
    ham_u64_t k = i + 1;
    ham_key_t key = {0};
    ham_record_t rec = {0};
    key.data = &k;
    key.size = sizeof(k);

    REQUIRE(0 == ham_db_find(db, 0, &key, &rec, 0));
    REQUIRE(rec.size == (ham_u32_t)record_size);
    if (record_size) {
      sprintf(buffer1, "%04lu", i);
      REQUIRE(0 == memcmp(buffer1, rec.data, rec.size));
    }
  }

  ham_cursor_t *cursor;
  REQUIRE(0 == ham_cursor_create(&cursor, db, 0, 0));
  for (ham_u64_t i = 0; i < 10000; i++) {
    ham_u64_t k = i + 1;
    ham_key_t key = {0};
    ham_record_t rec = {0};
    key.data = &k;
    key.size = sizeof(k);

    REQUIRE(0 == ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));
    REQUIRE(rec.size == (ham_u32_t)record_size);
    if (record_size) {
      sprintf(buffer1, "%04lu", i);
      REQUIRE(0 == memcmp(buffer1, rec.data, rec.size));
    }
  }
  REQUIRE(HAM_KEY_NOT_FOUND == ham_cursor_move(cursor, 0, 0, HAM_CURSOR_NEXT));
  ham_cursor_close(cursor);

  ham_u64_t count = 0;
  REQUIRE(0 == ham_db_get_key_count(db, 0, 0, &count));
  REQUIRE(count == 10000);

  for (ham_u64_t i = 0; i < 10000; i++) {
    ham_u64_t k = i + 1;
    ham_key_t key = {0};
    key.data = &k;
    key.size = sizeof(k);

    REQUIRE(0 == ham_db_erase(db, 0, &key, 0));
  }

  REQUIRE(0 == ham_db_get_key_count(db, 0, 0, &count));
  REQUIRE(count == 0);

  for (ham_u64_t i = 0; i < 10000; i++) {
    ham_u64_t k = i + 1;
    ham_key_t key = {0};
    ham_record_t rec = {0};
    key.data = &k;
    key.size = sizeof(k);

    REQUIRE(HAM_KEY_NOT_FOUND == ham_db_find(db, 0, &key, &rec, 0));
  }

  REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
}

static void
hola_test(int record_size)
{
  ham_parameter_t params[] = {
      {HAM_PARAM_KEY_COMPRESSION, HAM_COMPRESSOR_BITMAP},
      {HAM_PARAM_RECORD_SIZE, (ham_u64_t)record_size},
      {0, 0}
  };

  ham_env_t *env;
  ham_db_t *db;

  REQUIRE(0 == ham_env_create(&env, Utils::opath("test.db"), 0, 0644, 0));
  REQUIRE(0 == ham_env_create_db(env, &db, 1, HAM_RECORD_NUMBER, &params[0]));

  char buffer1[64] = {0};

  for (ham_u64_t i = 0; i < 10000; i++) {
    ham_key_t key = {0};
    ham_record_t rec = {0};
    sprintf(buffer1, "%04lu", i);
    rec.data = &buffer1[0];
    rec.size = record_size;

    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));
  }

  hola_result_t result;
  REQUIRE(0 == hola_sum(db, 0, &result));
  REQUIRE(result.type == HAM_TYPE_UINT64);
  REQUIRE(result.u.result_u64 == 50005000ul);

  /*
  ham_cursor_t *cursor;
  REQUIRE(0 == ham_cursor_create(&cursor, db, 0, 0));
  REQUIRE(0 == ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));
  ham_cursor_close(cursor);
  */

  REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
}

static void
hola_mixed_test()
{
  ham_parameter_t params[] = {
      {HAM_PARAM_KEY_COMPRESSION, HAM_COMPRESSOR_BITMAP},
      {HAM_PARAM_RECORD_SIZE, 0},
      {0, 0}
  };

  ham_env_t *env;
  ham_db_t *db;
  ham_txn_t *txn;

  REQUIRE(0 == ham_env_create(&env, Utils::opath("test.db"),
                          HAM_ENABLE_TRANSACTIONS, 0644, 0));
  REQUIRE(0 == ham_env_create_db(env, &db, 1,
                          HAM_RECORD_NUMBER, &params[0]));

  ham_record_t rec = {0};
  for (ham_u64_t i = 0; i < 100; i++) {
    ham_key_t key = {0};
    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));
  }

  REQUIRE(0 == ham_txn_begin(&txn, env, 0, 0, 0));
  for (ham_u64_t i = 100; i < 200; i++) {
    ham_key_t key = {0};
    REQUIRE(0 == ham_db_insert(db, txn, &key, &rec, 0));
  }

  hola_result_t result;
  REQUIRE(0 == hola_sum(db, txn, &result));
  REQUIRE(result.type == HAM_TYPE_UINT64);
  REQUIRE(result.u.result_u64 == 20100ul);

  REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
}

TEST_CASE("Bitmap/insertFindEraseTest", "")
{
  insert_find_erase(0);
}

TEST_CASE("Bitmap/insertFindEraseSplitMergeTest", "")
{
  insert_find_erase(9);
}

TEST_CASE("Bitmap/holaTest", "")
{
  hola_test(0);
}

TEST_CASE("Bitmap/holaSplitTest", "")
{
  hola_test(9);
}

TEST_CASE("Bitmap/holaMixedTest", "")
{
  hola_mixed_test();
}
