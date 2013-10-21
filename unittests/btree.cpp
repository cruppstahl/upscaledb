/**
 * Copyright (C) 2005-2013 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * See files COPYING.* for License information.
 */

#include "../src/config.h"

#include "3rdparty/catch/catch.hpp"

#include "globals.h"
#include "os.hpp"

#include "../src/env_local.h"
#include "../src/page_manager.h"
#include "../src/btree_index.h"
#include "../src/btree_node_proxy.h"
#include "../src/btree_node_legacy.h"

namespace hamsterdb {

bool g_split = false;
extern void (*g_BTREE_INSERT_SPLIT_HOOK)(void);

static void
split_hook()
{
  g_split = true;
}

struct BtreeFixture {

  BtreeFixture() {
    os::unlink(Globals::opath("test.db"));
  }

  ~BtreeFixture() {
  }

  void binaryTypeTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_parameter_t ps[] = {
        { HAM_PARAM_KEY_TYPE, HAM_TYPE_BINARY },
        { 0, 0 }
    };

    // create the database with flags and parameters
    REQUIRE(0 == ham_env_create(&env, Globals::opath("test.db"), 0, 0, 0));
    REQUIRE(0 == ham_env_create_db(env, &db, 1, 0, &ps[0]));

    ham_parameter_t query[] = {
        {HAM_PARAM_KEY_TYPE, 0},
        {HAM_PARAM_KEY_SIZE, 0},
        {HAM_PARAM_MAX_KEYS_PER_PAGE, 0},
        {HAM_PARAM_RECORD_SIZE, 0},
        {0, 0}
    };
    REQUIRE(0 == ham_db_get_parameters(db, query));
    REQUIRE(HAM_TYPE_BINARY == query[0].value);
    REQUIRE(21 == query[1].value);
    REQUIRE(510 == query[2].value);
    REQUIRE(HAM_RECORD_SIZE_UNLIMITED == query[3].value);

#ifdef HAVE_GCC_ABI_DEMANGLE
    std::string s;
    s = ((LocalDatabase *)db)->get_btree_index()->test_get_classname();
    REQUIRE(s == "hamsterdb::BtreeIndexTraitsImpl<hamsterdb::LegacyNodeLayout, hamsterdb::VariableSizeCompare>");
#endif

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void fixedTypeTest(int type, int size, int maxkeys, const char *abiname) {
    ham_db_t *db;
    ham_env_t *env;
    ham_parameter_t ps[] = {
        { HAM_PARAM_KEY_TYPE, (ham_u64_t)type },
        { 0, 0 },
        { 0, 0 }
    };

    if (type == HAM_TYPE_BINARY) {
      ps[1].name = HAM_PARAM_KEY_SIZE;
      ps[1].value = size;
    }

    // create the database with flags and parameters
    REQUIRE(0 == ham_env_create(&env, Globals::opath("test.db"), 0, 0, 0));
    if (type != HAM_TYPE_BINARY)
      REQUIRE(HAM_INV_PARAMETER
           == ham_env_create_db(env, &db, 1, HAM_ENABLE_EXTENDED_KEYS, &ps[0]));

    int flags = 0;
    if (type == HAM_TYPE_BINARY)
      flags = HAM_DISABLE_VARIABLE_KEYS;
    REQUIRE(0 == ham_env_create_db(env, &db, 1, flags, &ps[0]));

    ham_parameter_t query[] = {
        {HAM_PARAM_KEY_TYPE, 0},
        {HAM_PARAM_KEY_SIZE, 0},
        {HAM_PARAM_MAX_KEYS_PER_PAGE, 0},
        {0, 0}
    };
    REQUIRE(0 == ham_db_get_parameters(db, query));
    REQUIRE(type == (int)query[0].value);
    REQUIRE(size == (int)query[1].value);
    REQUIRE(maxkeys == (int)query[2].value);

#ifdef HAVE_GCC_ABI_DEMANGLE
    std::string abi;
    abi = ((LocalDatabase *)db)->get_btree_index()->test_get_classname();
    REQUIRE(abi == abiname);
#endif

    // only keys with that specific length are allowed
    ham_cursor_t *cursor;
    REQUIRE(0 == ham_cursor_create(&cursor, db, 0, 0));

    ham_record_t rec = {0};
    ham_key_t key = {0};
    char buffer[100] = {0};
    key.data = (void *)buffer;
    key.size = size + 1;
    REQUIRE(HAM_INV_KEY_SIZE == ham_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(HAM_INV_KEY_SIZE == ham_cursor_insert(cursor, &key, &rec, 0));
    key.size = size - 1;
    REQUIRE(HAM_INV_KEY_SIZE == ham_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(HAM_INV_KEY_SIZE == ham_cursor_insert(cursor, &key, &rec, 0));
    key.size = size;
    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ham_cursor_insert(cursor, &key, &rec, HAM_OVERWRITE));

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));

    // reopen and check the demangled API string once more
    REQUIRE(0 == ham_env_open(&env, Globals::opath("test.db"), 0, 0));
    REQUIRE(0 == ham_env_open_db(env, &db, 1, 0, 0));

    REQUIRE(0 == ham_db_get_parameters(db, query));
    REQUIRE(type == (int)query[0].value);
    REQUIRE(size == (int)query[1].value);
    REQUIRE(maxkeys == (int)query[2].value);

#ifdef HAVE_GCC_ABI_DEMANGLE
    std::string abi2;
    abi2 = ((LocalDatabase *)db)->get_btree_index()->test_get_classname();
    REQUIRE(abi2 == abi);
#endif

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void autoDefaultRecords() {
    ham_db_t *db;
    ham_env_t *env;
    ham_parameter_t p1[] = {
        { HAM_PARAM_PAGE_SIZE, 1024 * 64 },
        { 0, 0 }
    };
    ham_parameter_t p2[] = {
        { HAM_PARAM_KEY_TYPE, HAM_TYPE_UINT32 },
        { HAM_PARAM_RECORD_SIZE, 10 },
        { 0, 0 }
    };

    // create the database with flags and parameters
    REQUIRE(0 == ham_env_create(&env, Globals::opath("test.db"), 0, 0, &p1[0]));
    REQUIRE(0 == ham_env_create_db(env, &db, 1, 0, &p2[0]));

    ham_parameter_t query[] = {
        {HAM_PARAM_KEY_TYPE, 0},
        {HAM_PARAM_KEY_SIZE, 0},
        {HAM_PARAM_RECORD_SIZE, 0},
        {HAM_PARAM_MAX_KEYS_PER_PAGE, 0},
        {HAM_PARAM_FLAGS, 0},
        {0, 0}
    };
    REQUIRE(0 == ham_db_get_parameters(db, query));
    REQUIRE(HAM_TYPE_UINT32 == (int)query[0].value);
    REQUIRE(4 == (int)query[1].value);
    REQUIRE(10 == (int)query[2].value);
    REQUIRE(4366 == (int)query[3].value);
    REQUIRE((HAM_DISABLE_VARIABLE_KEYS | HAM_FORCE_RECORDS_INLINE)
                   == (int)query[4].value);

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));

    // reopen and make sure the flag was persisted
    REQUIRE(0 == ham_env_open(&env, Globals::opath("test.db"), 0, 0));
    REQUIRE(0 == ham_env_open_db(env, &db, 1, 0, 0));
    REQUIRE(0 == ham_db_get_parameters(db, query));
    REQUIRE(HAM_TYPE_UINT32 == (int)query[0].value);
    REQUIRE(4 == (int)query[1].value);
    REQUIRE(10 == (int)query[2].value);
    REQUIRE(4366 == (int)query[3].value);
    REQUIRE((HAM_DISABLE_VARIABLE_KEYS | HAM_FORCE_RECORDS_INLINE)
                   == (int)query[4].value);

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void persistentNodeFlags() {
    ham_db_t *db;
    ham_env_t *env;
    ham_parameter_t p[] = {
        { HAM_PARAM_KEY_TYPE, HAM_TYPE_UINT32 },
        { HAM_PARAM_RECORD_SIZE, 10 },
        { 0, 0 }
    };

    // create the database with flags and parameters
    REQUIRE(0 == ham_env_create(&env, Globals::opath("test.db"), 0, 0, 0));
    REQUIRE(0 == ham_env_create_db(env, &db, 1, 0, &p[0]));

    ham_parameter_t query[] = {
        {HAM_PARAM_KEY_TYPE, 0},
        {HAM_PARAM_KEY_SIZE, 0},
        {HAM_PARAM_RECORD_SIZE, 0},
        {HAM_PARAM_MAX_KEYS_PER_PAGE, 0},
        {HAM_PARAM_FLAGS, 0},
        {0, 0}
    };
    REQUIRE(0 == ham_db_get_parameters(db, query));
    REQUIRE(HAM_TYPE_UINT32 == (int)query[0].value);
    REQUIRE(4 == (int)query[1].value);
    REQUIRE(10 == (int)query[2].value);
    REQUIRE(1088 == (int)query[3].value);
    REQUIRE((HAM_DISABLE_VARIABLE_KEYS | HAM_FORCE_RECORDS_INLINE)
                   == (int)query[4].value);

    // now insert a key
    ham_u32_t k = 33;
    char buffer[10] = {0};
    ham_key_t key = {0};
    key.data = &k;
    key.size = sizeof(k);
    ham_record_t rec = {0};
    rec.size = sizeof(buffer);
    rec.data = &buffer[0];
    REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));

    // reopen and fetch the root page of the database
    REQUIRE(0 == ham_env_open(&env, Globals::opath("test.db"), 0, 0));
    REQUIRE(0 == ham_env_open_db(env, &db, 1, 0, 0));
    LocalEnvironment *lenv = (LocalEnvironment *)env;

    Page *page;
    REQUIRE(0 == lenv->get_page_manager()->fetch_page(&page,
                            (LocalDatabase *)db, 1024 * 16));
    PBtreeNode *node = PBtreeNode::from_page(page);
    REQUIRE((node->get_flags() & PBtreeNode::kLeafNode)
                   == PBtreeNode::kLeafNode);

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void internalNodeTest() {
    ham_db_t *db;
    ham_env_t *env;
    Page *page;
    BtreeNodeProxy *node;
    ham_parameter_t p[] = {
        { HAM_PARAM_KEY_TYPE, HAM_TYPE_UINT32 },
        { HAM_PARAM_RECORD_SIZE, 10 },
        { 0, 0 }
    };

    // create the database with flags and parameters
    REQUIRE(0 == ham_env_create(&env, Globals::opath("test.db"), 0, 0, 0));
    REQUIRE(0 == ham_env_create_db(env, &db, 1, 0, &p[0]));

    LocalEnvironment *lenv = (LocalEnvironment *)env;
    LocalDatabase *ldb = (LocalDatabase *)db;

    g_BTREE_INSERT_SPLIT_HOOK = split_hook;

    std::string expected_leafname = "hamsterdb::BtreeNodeProxyImpl<hamsterdb::PaxNodeLayout<hamsterdb::PodKeyList<unsigned int>, hamsterdb::InlineRecordList>, hamsterdb::NumericCompare<unsigned int> >";
    std::string expected_internalname = "hamsterdb::BtreeNodeProxyImpl<hamsterdb::PaxNodeLayout<hamsterdb::PodKeyList<unsigned int>, hamsterdb::InternalRecordList>, hamsterdb::NumericCompare<unsigned int> >";

    // check if the root page proxy was created correctly (it's a leaf)
    REQUIRE(0 == lenv->get_page_manager()->fetch_page(&page,
                            (LocalDatabase *)db, 1024 * 16));
    node = ldb->get_btree_index()->get_node_from_page(page);
    REQUIRE((node->get_flags() & PBtreeNode::kLeafNode)
                   == PBtreeNode::kLeafNode);
    REQUIRE(node->test_get_classname() == expected_leafname);

    char buffer[10] = {0};
    ham_key_t key = {0};
    ham_record_t rec = {0};
    rec.size = sizeof(buffer);
    rec.data = &buffer[0];

    // now insert keys till the page is split and a new root is created
    g_split = false;
    ham_u32_t k = 1;
    while (!g_split) {
      key.data = &k;
      key.size = sizeof(k);
      REQUIRE(0 == ham_db_insert(db, 0, &key, &rec, 0));
      k++;
    }

    // now check the leaf page (same as the previous root page)
    REQUIRE(0 == lenv->get_page_manager()->fetch_page(&page,
                            (LocalDatabase *)db, 1024 * 16));
    node = ldb->get_btree_index()->get_node_from_page(page);
    REQUIRE((node->get_flags() & PBtreeNode::kLeafNode)
                   == PBtreeNode::kLeafNode);
    REQUIRE(node->test_get_classname() == expected_leafname);

    // check the other leaf
    REQUIRE(0 == lenv->get_page_manager()->fetch_page(&page,
                            (LocalDatabase *)db, 2 * 1024 * 16));
    node = ldb->get_btree_index()->get_node_from_page(page);
    REQUIRE((node->get_flags() & PBtreeNode::kLeafNode)
                   == PBtreeNode::kLeafNode);
    REQUIRE(node->test_get_classname() == expected_leafname);

    // and the new root page (must be an internal page)
    REQUIRE(0 == lenv->get_page_manager()->fetch_page(&page,
                            (LocalDatabase *)db, 3 * 1024 * 16));
    node = ldb->get_btree_index()->get_node_from_page(page);
    REQUIRE((node->get_flags() & PBtreeNode::kLeafNode) == 0);
    REQUIRE(node->test_get_classname() == expected_internalname);

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void forceInternalNodeTest() {
    ham_db_t *db;
    ham_env_t *env;
    ham_parameter_t p[] = {
        { HAM_PARAM_KEY_TYPE, HAM_TYPE_UINT32 },
        { HAM_PARAM_RECORD_SIZE, 512 },
        { 0, 0 }
    };

    // create the database with flags and parameters
    REQUIRE(0 == ham_env_create(&env, Globals::opath("test.db"), 0, 0, 0));
    REQUIRE(0 == ham_env_create_db(env, &db, 1,
                            HAM_FORCE_RECORDS_INLINE, &p[0]));

    ham_parameter_t query[] = {
        {HAM_PARAM_KEY_TYPE, 0},
        {HAM_PARAM_KEY_SIZE, 0},
        {HAM_PARAM_RECORD_SIZE, 0},
        {HAM_PARAM_MAX_KEYS_PER_PAGE, 0},
        {HAM_PARAM_FLAGS, 0},
        {0, 0}
    };
    REQUIRE(0 == ham_db_get_parameters(db, query));
    REQUIRE(HAM_TYPE_UINT32 == (int)query[0].value);
    REQUIRE(4 == (int)query[1].value);
    REQUIRE(512 == (int)query[2].value);
    REQUIRE(30 == (int)query[3].value);
    REQUIRE((HAM_DISABLE_VARIABLE_KEYS | HAM_FORCE_RECORDS_INLINE)
                   == (int)query[4].value);

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));

    // reopen and make sure the flag was persisted
    REQUIRE(0 == ham_env_open(&env, Globals::opath("test.db"), 0, 0));
    REQUIRE(0 == ham_env_open_db(env, &db, 1, 0, 0));
    REQUIRE(0 == ham_db_get_parameters(db, query));
    REQUIRE(HAM_TYPE_UINT32 == (int)query[0].value);
    REQUIRE(4 == (int)query[1].value);
    REQUIRE(512 == (int)query[2].value);
    REQUIRE(30 == (int)query[3].value);
    REQUIRE((HAM_DISABLE_VARIABLE_KEYS | HAM_FORCE_RECORDS_INLINE)
                   == (int)query[4].value);

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }
};

TEST_CASE("Btree/binaryTypeTest", "")
{
  BtreeFixture f;
  f.binaryTypeTest();
}

TEST_CASE("Btree/uint8Type", "")
{
  BtreeFixture f;
  f.fixedTypeTest(HAM_TYPE_UINT8, 1, 1634,
      "hamsterdb::BtreeIndexTraitsImpl<hamsterdb::PaxNodeLayout<hamsterdb::PodKeyList<unsigned char>, hamsterdb::DefaultRecordList>, hamsterdb::NumericCompare<unsigned char> >");
}

TEST_CASE("Btree/uint16Type", "")
{
  BtreeFixture f;
  f.fixedTypeTest(HAM_TYPE_UINT16, 2, 1484,
      "hamsterdb::BtreeIndexTraitsImpl<hamsterdb::PaxNodeLayout<hamsterdb::PodKeyList<unsigned short>, hamsterdb::DefaultRecordList>, hamsterdb::NumericCompare<unsigned short> >");
}

TEST_CASE("Btree/uint32Type", "")
{
  BtreeFixture f;
  f.fixedTypeTest(HAM_TYPE_UINT32, 4, 1256,
      "hamsterdb::BtreeIndexTraitsImpl<hamsterdb::PaxNodeLayout<hamsterdb::PodKeyList<unsigned int>, hamsterdb::DefaultRecordList>, hamsterdb::NumericCompare<unsigned int> >");
}

TEST_CASE("Btree/uint64Type", "")
{
  BtreeFixture f;
  f.fixedTypeTest(HAM_TYPE_UINT64, 8, 960,
      "hamsterdb::BtreeIndexTraitsImpl<hamsterdb::PaxNodeLayout<hamsterdb::PodKeyList<unsigned long>, hamsterdb::DefaultRecordList>, hamsterdb::NumericCompare<unsigned long> >");
}

TEST_CASE("Btree/real32Type", "")
{
  BtreeFixture f;
  f.fixedTypeTest(HAM_TYPE_REAL32, 4, 1256,
      "hamsterdb::BtreeIndexTraitsImpl<hamsterdb::PaxNodeLayout<hamsterdb::PodKeyList<float>, hamsterdb::DefaultRecordList>, hamsterdb::NumericCompare<float> >");
}

TEST_CASE("Btree/real64Type", "")
{
  BtreeFixture f;
  f.fixedTypeTest(HAM_TYPE_REAL64, 8, 960,
      "hamsterdb::BtreeIndexTraitsImpl<hamsterdb::PaxNodeLayout<hamsterdb::PodKeyList<double>, hamsterdb::DefaultRecordList>, hamsterdb::NumericCompare<double> >");
}

TEST_CASE("Btree/fixedBinaryType", "")
{
  BtreeFixture f;
  f.fixedTypeTest(HAM_TYPE_BINARY, 8, 960,
      "hamsterdb::BtreeIndexTraitsImpl<hamsterdb::PaxNodeLayout<hamsterdb::BinaryKeyList, hamsterdb::DefaultRecordList>, hamsterdb::FixedSizeCompare>");
}

TEST_CASE("Btree/autoDefaultRecords", "")
{
  BtreeFixture f;
  f.autoDefaultRecords();
}

TEST_CASE("Btree/persistentNodeFlags", "")
{
  BtreeFixture f;
  f.persistentNodeFlags();
}

TEST_CASE("Btree/internalNodeTest", "")
{
  BtreeFixture f;
  f.internalNodeTest();
}

TEST_CASE("Btree/forceInternalNodeTest", "")
{
  BtreeFixture f;
  f.forceInternalNodeTest();
}


} // namespace hamsterdb
