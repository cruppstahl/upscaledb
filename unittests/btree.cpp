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

#include "utils.h"
#include "os.hpp"

#include "3page_manager/page_manager.h"
#include "3btree/btree_index.h"
#include "3btree/btree_node_proxy.h"
#include "3btree/btree_impl_default.h"
#include "4env/env_local.h"
#include "4context/context.h"

namespace upscaledb {

bool g_split = false;
extern void (*g_BTREE_INSERT_SPLIT_HOOK)(void);

static void
split_hook()
{
  g_split = true;
}

struct BtreeFixture {

  BtreeFixture() {
    os::unlink(Utils::opath("test.db"));
  }

  ~BtreeFixture() {
  }

  void binaryTypeTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_parameter_t ps[] = {
        { UPS_PARAM_KEY_TYPE, UPS_TYPE_BINARY },
        { 0, 0 }
    };

    // create the database with flags and parameters
    REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"), 0, 0, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, &ps[0]));

    ups_parameter_t query[] = {
        {UPS_PARAM_KEY_TYPE, 0},
        {UPS_PARAM_KEY_SIZE, 0},
        {UPS_PARAM_MAX_KEYS_PER_PAGE, 0},
        {UPS_PARAM_RECORD_SIZE, 0},
        {0, 0}
    };
    REQUIRE(0 == ups_db_get_parameters(db, query));
    REQUIRE((uint64_t)UPS_TYPE_BINARY == query[0].value);
    REQUIRE(UPS_KEY_SIZE_UNLIMITED == query[1].value);
    REQUIRE(441u == (unsigned)query[2].value);
    REQUIRE(UPS_RECORD_SIZE_UNLIMITED == query[3].value);

#ifdef HAVE_GCC_ABI_DEMANGLE
    std::string s;
    s = ((LocalDatabase *)db)->btree_index()->test_get_classname();
    REQUIRE(s == "upscaledb::BtreeIndexTraitsImpl<upscaledb::DefaultNodeImpl<upscaledb::DefLayout::VariableLengthKeyList, upscaledb::PaxLayout::DefaultRecordList>, upscaledb::VariableSizeCompare>");
#endif

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void fixedTypeTest(int type, int size, int maxkeys, const char *abiname) {
    std::string abi;
    ups_db_t *db;
    ups_env_t *env;
    ups_parameter_t ps[] = {
        { UPS_PARAM_KEY_TYPE, (uint64_t)type },
        { 0, 0 },
        { 0, 0 }
    };

    if (type == UPS_TYPE_BINARY) {
      ps[1].name = UPS_PARAM_KEY_SIZE;
      ps[1].value = size;
    }

    // create the database with flags and parameters
    REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"), 0, 0, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, &ps[0]));

    ups_parameter_t query[] = {
        {UPS_PARAM_KEY_TYPE, 0},
        {UPS_PARAM_KEY_SIZE, 0},
        {UPS_PARAM_MAX_KEYS_PER_PAGE, 0},
        {0, 0}
    };
    REQUIRE(0 == ups_db_get_parameters(db, query));
    REQUIRE(type == (int)query[0].value);
    REQUIRE(size == (int)query[1].value);
    REQUIRE(maxkeys == (int)query[2].value);

#ifdef HAVE_GCC_ABI_DEMANGLE
    abi = ((LocalDatabase *)db)->btree_index()->test_get_classname();
    REQUIRE(abi == abiname);
#endif

    // only keys with that specific length are allowed
    ups_cursor_t *cursor;
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    ups_record_t rec = {0};
    ups_key_t key = {0};
    char buffer[100] = {0};
    key.data = (void *)buffer;
    key.size = size + 1;
    REQUIRE(UPS_INV_KEY_SIZE == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(UPS_INV_KEY_SIZE == ups_cursor_insert(cursor, &key, &rec, 0));
    key.size = size - 1;
    REQUIRE(UPS_INV_KEY_SIZE == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(UPS_INV_KEY_SIZE == ups_cursor_insert(cursor, &key, &rec, 0));
    key.size = size;
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, UPS_OVERWRITE));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

    // reopen and check the demangled API string once more
    REQUIRE(0 == ups_env_open(&env, Utils::opath("test.db"), 0, 0));
    REQUIRE(0 == ups_env_open_db(env, &db, 1, 0, 0));

    REQUIRE(0 == ups_db_get_parameters(db, query));
    REQUIRE(type == (int)query[0].value);
    REQUIRE(size == (int)query[1].value);
    REQUIRE(maxkeys == (int)query[2].value);

#ifdef HAVE_GCC_ABI_DEMANGLE
    std::string abi2;
    abi2 = ((LocalDatabase *)db)->btree_index()->test_get_classname();
    REQUIRE(abi2 == abi);
#endif

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void autoDefaultRecords() {
    ups_db_t *db;
    ups_env_t *env;
    ups_parameter_t p1[] = {
        { UPS_PARAM_PAGE_SIZE, 1024 * 64 },
        { 0, 0 }
    };
    ups_parameter_t p2[] = {
        { UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT32 },
        { UPS_PARAM_RECORD_SIZE, 10 },
        { 0, 0 }
    };

    // create the database with flags and parameters
    REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"), 0, 0, &p1[0]));
    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, &p2[0]));

    ups_parameter_t query[] = {
        {UPS_PARAM_KEY_TYPE, 0},
        {UPS_PARAM_KEY_SIZE, 0},
        {UPS_PARAM_RECORD_SIZE, 0},
        {UPS_PARAM_MAX_KEYS_PER_PAGE, 0},
        {UPS_PARAM_FLAGS, 0},
        {0, 0}
    };
    REQUIRE(0 == ups_db_get_parameters(db, query));
    REQUIRE(UPS_TYPE_UINT32 == (int)query[0].value);
    REQUIRE(4 == (int)query[1].value);
    REQUIRE(10 == (int)query[2].value);
    REQUIRE(4677 == (int)query[3].value);
    REQUIRE(UPS_FORCE_RECORDS_INLINE == (int)query[4].value);

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

    // reopen and make sure the flag was persisted
    REQUIRE(0 == ups_env_open(&env, Utils::opath("test.db"), 0, 0));
    REQUIRE(0 == ups_env_open_db(env, &db, 1, 0, 0));
    REQUIRE(0 == ups_db_get_parameters(db, query));
    REQUIRE(UPS_TYPE_UINT32 == (int)query[0].value);
    REQUIRE(4 == (int)query[1].value);
    REQUIRE(10 == (int)query[2].value);
    REQUIRE(4677 == (int)query[3].value);
    REQUIRE(UPS_FORCE_RECORDS_INLINE == (int)query[4].value);

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void persistentNodeFlags() {
    ups_db_t *db;
    ups_env_t *env;
    ups_parameter_t p[] = {
        { UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT32 },
        { UPS_PARAM_RECORD_SIZE, 10 },
        { 0, 0 }
    };

    // create the database with flags and parameters
    REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"), 0, 0, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, &p[0]));

    ups_parameter_t query[] = {
        {UPS_PARAM_KEY_TYPE, 0},
        {UPS_PARAM_KEY_SIZE, 0},
        {UPS_PARAM_RECORD_SIZE, 0},
        {UPS_PARAM_MAX_KEYS_PER_PAGE, 0},
        {UPS_PARAM_FLAGS, 0},
        {0, 0}
    };
    REQUIRE(0 == ups_db_get_parameters(db, query));
    REQUIRE(UPS_TYPE_UINT32 == (int)query[0].value);
    REQUIRE(4 == (int)query[1].value);
    REQUIRE(10 == (int)query[2].value);
    REQUIRE(1166 == (int)query[3].value);
    REQUIRE(UPS_FORCE_RECORDS_INLINE == (int)query[4].value);

    // now insert a key
    uint32_t k = 33;
    char buffer[10] = {0};
    ups_key_t key = {0};
    key.data = &k;
    key.size = sizeof(k);
    ups_record_t rec = {0};
    rec.size = sizeof(buffer);
    rec.data = &buffer[0];
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

    // reopen and fetch the root page of the database
    REQUIRE(0 == ups_env_open(&env, Utils::opath("test.db"), 0, 0));
    REQUIRE(0 == ups_env_open_db(env, &db, 1, 0, 0));
    LocalEnvironment *lenv = (LocalEnvironment *)env;
    Context context(lenv, 0, 0);

    Page *page;
    REQUIRE((page = lenv->page_manager()->fetch(&context, 1024 * 16)));
    context.changeset.clear(); // unlock pages
    PBtreeNode *node = PBtreeNode::from_page(page);
    REQUIRE((node->flags() & PBtreeNode::kLeafNode)
                   == PBtreeNode::kLeafNode);

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void internalNodeTest() {
    ups_db_t *db;
    ups_env_t *env;
    Page *page;
    BtreeNodeProxy *node;
    ups_parameter_t p[] = {
        { UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT32 },
        { UPS_PARAM_RECORD_SIZE, 10 },
        { 0, 0 }
    };

    // create the database with flags and parameters
    REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"), 0, 0, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 1, 0, &p[0]));

    LocalEnvironment *lenv = (LocalEnvironment *)env;
    LocalDatabase *ldb = (LocalDatabase *)db;
    Context context(lenv, 0, 0);

    g_BTREE_INSERT_SPLIT_HOOK = split_hook;

    // check if the root page proxy was created correctly (it's a leaf)
    REQUIRE((page = lenv->page_manager()->fetch(&context, 1024 * 16)));
    context.changeset.clear(); // unlock pages
    node = ldb->btree_index()->get_node_from_page(page);
    REQUIRE((node->flags() & PBtreeNode::kLeafNode)
                   == PBtreeNode::kLeafNode);
#ifdef HAVE_GCC_ABI_DEMANGLE
    std::string expected_internalname = "upscaledb::BtreeNodeProxyImpl<upscaledb::PaxNodeImpl<upscaledb::PaxLayout::PodKeyList<unsigned int>, upscaledb::PaxLayout::InternalRecordList>, upscaledb::NumericCompare<unsigned int> >";
    std::string expected_leafname = "upscaledb::BtreeNodeProxyImpl<upscaledb::PaxNodeImpl<upscaledb::PaxLayout::PodKeyList<unsigned int>, upscaledb::PaxLayout::InlineRecordList>, upscaledb::NumericCompare<unsigned int> >";
    REQUIRE(node->test_get_classname() == expected_leafname);
#endif

    char buffer[10] = {0};
    ups_key_t key = {0};
    ups_record_t rec = {0};
    rec.size = sizeof(buffer);
    rec.data = &buffer[0];

    // now insert keys till the page is split and a new root is created
    g_split = false;
    uint32_t k = 1;
    while (!g_split) {
      key.data = &k;
      key.size = sizeof(k);
      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
      k++;
    }

    // now check the leaf page (same as the previous root page)
    REQUIRE((page = lenv->page_manager()->fetch(&context, 1024 * 16)));
    context.changeset.clear(); // unlock pages
    node = ldb->btree_index()->get_node_from_page(page);
    REQUIRE((node->flags() & PBtreeNode::kLeafNode)
                   == PBtreeNode::kLeafNode);
#ifdef HAVE_GCC_ABI_DEMANGLE
    REQUIRE(node->test_get_classname() == expected_leafname);
#endif

    // check the other leaf
    REQUIRE((page = lenv->page_manager()->fetch(&context, 2 * 1024 * 16)));
    context.changeset.clear(); // unlock pages
    node = ldb->btree_index()->get_node_from_page(page);
    REQUIRE((node->flags() & PBtreeNode::kLeafNode)
                   == PBtreeNode::kLeafNode);
#ifdef HAVE_GCC_ABI_DEMANGLE
    REQUIRE(node->test_get_classname() == expected_leafname);
#endif

    // and the new root page (must be an internal page)
    REQUIRE((page = lenv->page_manager()->fetch(&context, 3 * 1024 * 16)));
    context.changeset.clear(); // unlock pages
    node = ldb->btree_index()->get_node_from_page(page);
    REQUIRE((node->flags() & PBtreeNode::kLeafNode) == 0);
#ifdef HAVE_GCC_ABI_DEMANGLE
    REQUIRE(node->test_get_classname() == expected_internalname);
#endif

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void forceInternalNodeTest() {
    ups_db_t *db;
    ups_env_t *env;
    ups_parameter_t p[] = {
        { UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT32 },
        { UPS_PARAM_RECORD_SIZE, 512 },
        { 0, 0 }
    };

    // create the database with flags and parameters
    REQUIRE(0 == ups_env_create(&env, Utils::opath("test.db"), 0, 0, 0));
    REQUIRE(0 == ups_env_create_db(env, &db, 1,
                            UPS_FORCE_RECORDS_INLINE, &p[0]));

    ups_parameter_t query[] = {
        {UPS_PARAM_KEY_TYPE, 0},
        {UPS_PARAM_KEY_SIZE, 0},
        {UPS_PARAM_RECORD_SIZE, 0},
        {UPS_PARAM_MAX_KEYS_PER_PAGE, 0},
        {UPS_PARAM_FLAGS, 0},
        {0, 0}
    };
    REQUIRE(0 == ups_db_get_parameters(db, query));
    REQUIRE(UPS_TYPE_UINT32 == (int)query[0].value);
    REQUIRE(4 == (int)query[1].value);
    REQUIRE(512 == (int)query[2].value);
    REQUIRE(31 == (int)query[3].value);
    REQUIRE(UPS_FORCE_RECORDS_INLINE == (int)query[4].value);

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));

    // reopen and make sure the flag was persisted
    REQUIRE(0 == ups_env_open(&env, Utils::opath("test.db"), 0, 0));
    REQUIRE(0 == ups_env_open_db(env, &db, 1, 0, 0));
    REQUIRE(0 == ups_db_get_parameters(db, query));
    REQUIRE(UPS_TYPE_UINT32 == (int)query[0].value);
    REQUIRE(4 == (int)query[1].value);
    REQUIRE(512 == (int)query[2].value);
    REQUIRE(31 == (int)query[3].value);
    REQUIRE(UPS_FORCE_RECORDS_INLINE == (int)query[4].value);

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
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
  f.fixedTypeTest(UPS_TYPE_UINT8, 1, 1633,
      "upscaledb::BtreeIndexTraitsImpl<upscaledb::PaxNodeImpl<upscaledb::PaxLayout::PodKeyList<unsigned char>, upscaledb::PaxLayout::DefaultRecordList>, upscaledb::NumericCompare<unsigned char> >");
}

TEST_CASE("Btree/uint16Type", "")
{
  BtreeFixture f;
  f.fixedTypeTest(UPS_TYPE_UINT16, 2, 1485,
      "upscaledb::BtreeIndexTraitsImpl<upscaledb::PaxNodeImpl<upscaledb::PaxLayout::PodKeyList<unsigned short>, upscaledb::PaxLayout::DefaultRecordList>, upscaledb::NumericCompare<unsigned short> >");
}

TEST_CASE("Btree/uint32Type", "")
{
  BtreeFixture f;
  f.fixedTypeTest(UPS_TYPE_UINT32, 4, 1256,
      "upscaledb::BtreeIndexTraitsImpl<upscaledb::PaxNodeImpl<upscaledb::PaxLayout::PodKeyList<unsigned int>, upscaledb::PaxLayout::DefaultRecordList>, upscaledb::NumericCompare<unsigned int> >");
}

TEST_CASE("Btree/uint64Type", "")
{
  BtreeFixture f;
  const char *abiname;
  if (sizeof(unsigned long) == 4)
    abiname = "upscaledb::BtreeIndexTraitsImpl<upscaledb::PaxNodeImpl<upscaledb::PaxLayout::PodKeyList<unsigned long long>, upscaledb::PaxLayout::DefaultRecordList>, upscaledb::NumericCompare<unsigned long long> >";
  else
    abiname = "upscaledb::BtreeIndexTraitsImpl<upscaledb::PaxNodeImpl<upscaledb::PaxLayout::PodKeyList<unsigned long>, upscaledb::PaxLayout::DefaultRecordList>, upscaledb::NumericCompare<unsigned long> >";

  f.fixedTypeTest(UPS_TYPE_UINT64, 8, 960, abiname);
}

TEST_CASE("Btree/real32Type", "")
{
  BtreeFixture f;
  f.fixedTypeTest(UPS_TYPE_REAL32, 4, 1256,
      "upscaledb::BtreeIndexTraitsImpl<upscaledb::PaxNodeImpl<upscaledb::PaxLayout::PodKeyList<float>, upscaledb::PaxLayout::DefaultRecordList>, upscaledb::NumericCompare<float> >");
}

TEST_CASE("Btree/real64Type", "")
{
  BtreeFixture f;
  f.fixedTypeTest(UPS_TYPE_REAL64, 8, 960,
      "upscaledb::BtreeIndexTraitsImpl<upscaledb::PaxNodeImpl<upscaledb::PaxLayout::PodKeyList<double>, upscaledb::PaxLayout::DefaultRecordList>, upscaledb::NumericCompare<double> >");
}

TEST_CASE("Btree/fixedBinaryType", "")
{
  BtreeFixture f;
  f.fixedTypeTest(UPS_TYPE_BINARY, 8, 960,
      "upscaledb::BtreeIndexTraitsImpl<upscaledb::PaxNodeImpl<upscaledb::PaxLayout::BinaryKeyList, upscaledb::PaxLayout::DefaultRecordList>, upscaledb::FixedSizeCompare>");
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


} // namespace upscaledb
