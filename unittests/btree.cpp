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

#include "3page_manager/page_manager.h"
#include "4env/env_local.h"
#include "4context/context.h"

#include "os.hpp"
#include "fixture.hpp"

namespace upscaledb {

bool g_split = false;
extern void (*g_BTREE_INSERT_SPLIT_HOOK)(void);

static void
split_hook() {
  g_split = true;
}

struct BtreeFixture : BaseFixture {

  void binaryTypeTest() {
    ups_parameter_t ps[] = {
        { UPS_PARAM_KEY_TYPE, UPS_TYPE_BINARY },
        { 0, 0 }
    };

    // create the database with flags and parameters
    require_create(0, nullptr, 0, ps);

    ups_parameter_t query[] = {
        {UPS_PARAM_KEY_TYPE, 0},
        {UPS_PARAM_KEY_SIZE, 0},
        {UPS_PARAM_MAX_KEYS_PER_PAGE, 0},
        {UPS_PARAM_RECORD_SIZE, 0},
        {0, 0}
    };

    DbProxy dbp(db);
    dbp.require_parameters(query);
    REQUIRE((uint64_t)UPS_TYPE_BINARY == query[0].value);
    REQUIRE(UPS_KEY_SIZE_UNLIMITED == query[1].value);
    REQUIRE(441u == (unsigned)query[2].value);
    REQUIRE(UPS_RECORD_SIZE_UNLIMITED == query[3].value);

#ifdef HAVE_GCC_ABI_DEMANGLE
    std::string s = btree_index()->test_get_classname();
    REQUIRE(s == "upscaledb::BtreeIndexTraitsImpl<upscaledb::DefaultNodeImpl<upscaledb::VariableLengthKeyList, upscaledb::DefaultRecordList>, upscaledb::VariableSizeCompare>");
#endif
  }

  void fixedTypeTest(int type, int size, int maxkeys, const char *abiname) {
    std::string abi;
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
    require_create(0, nullptr, 0, ps);

    ups_parameter_t query[] = {
        {UPS_PARAM_KEY_TYPE, 0},
        {UPS_PARAM_KEY_SIZE, 0},
        {UPS_PARAM_MAX_KEYS_PER_PAGE, 0},
        {0, 0}
    };
    DbProxy dbp(db);
    dbp.require_parameters(query);
    REQUIRE(type == (int)query[0].value);
    REQUIRE(size == (int)query[1].value);
    REQUIRE(maxkeys == (int)query[2].value);

#ifdef HAVE_GCC_ABI_DEMANGLE
    abi = ((LocalDb *)db)->btree_index->test_get_classname();
    REQUIRE(abi == abiname);
#endif

    // only keys with that specific length are allowed
    ups_cursor_t *cursor;
    REQUIRE(0 == ups_cursor_create(&cursor, db, 0, 0));

    char buffer[100] = {0};
    ups_key_t key = ups_make_key(buffer, (uint16_t)(size + 1));
    ups_record_t rec = {0};
    REQUIRE(UPS_INV_KEY_SIZE == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(UPS_INV_KEY_SIZE == ups_cursor_insert(cursor, &key, &rec, 0));
    key.size = size - 1;
    REQUIRE(UPS_INV_KEY_SIZE == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(UPS_INV_KEY_SIZE == ups_cursor_insert(cursor, &key, &rec, 0));
    key.size = size;
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    REQUIRE(0 == ups_cursor_insert(cursor, &key, &rec, UPS_OVERWRITE));

    // reopen and check the demangled API string once more
    close();
    require_open();

    REQUIRE(0 == ups_db_get_parameters(db, query));
    REQUIRE(type == (int)query[0].value);
    REQUIRE(size == (int)query[1].value);
    REQUIRE(maxkeys == (int)query[2].value);

#ifdef HAVE_GCC_ABI_DEMANGLE
    std::string abi2;
    abi2 = btree_index()->test_get_classname();
    REQUIRE(abi2 == abi);
#endif
  }

  void autoDefaultRecords() {
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
    require_create(0, p1, 0, p2);

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

    // reopen and make sure the flag was persisted
    close();
    require_open();
    REQUIRE(0 == ups_db_get_parameters(db, query));
    REQUIRE(UPS_TYPE_UINT32 == (int)query[0].value);
    REQUIRE(4 == (int)query[1].value);
    REQUIRE(10 == (int)query[2].value);
    REQUIRE(4677 == (int)query[3].value);
    REQUIRE(UPS_FORCE_RECORDS_INLINE == (int)query[4].value);
  }

  void persistentNodeFlags() {
    ups_parameter_t p[] = {
        { UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT32 },
        { UPS_PARAM_RECORD_SIZE, 10 },
        { 0, 0 }
    };

    // create the database with flags and parameters
    require_create(0, nullptr, 0, p);

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
    ups_key_t key = ups_make_key(&k, sizeof(k));
    ups_record_t rec = ups_make_record(buffer, sizeof(buffer));
    REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));

    // reopen and fetch the root page of the database
    close();
    require_open();
    Context context(lenv(), 0, 0);

    Page *page = 0;
    REQUIRE((page = lenv()->page_manager->fetch(&context, 1024 * 16)));
    context.changeset.clear(); // unlock pages
    PBtreeNode *node = PBtreeNode::from_page(page);
    REQUIRE(ISSET(node->flags(), PBtreeNode::kLeafNode));
  }

  void internalNodeTest() {
    Page *page;
    BtreeNodeProxy *node;
    ups_parameter_t p[] = {
        { UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT32 },
        { UPS_PARAM_RECORD_SIZE, 10 },
        { 0, 0 }
    };

    // create the database with flags and parameters
    require_create(0, nullptr, 0, p);

    Context context(lenv(), 0, 0);

    g_BTREE_INSERT_SPLIT_HOOK = split_hook;

    // check if the root page proxy was created correctly (it's a leaf)
    REQUIRE((page = lenv()->page_manager->fetch(&context, 1024 * 16)));
    context.changeset.clear(); // unlock pages
    node = btree_index()->get_node_from_page(page);
    REQUIRE(ISSET(node->flags(), PBtreeNode::kLeafNode));
#ifdef HAVE_GCC_ABI_DEMANGLE
    std::string expected_internalname = "upscaledb::BtreeNodeProxyImpl<upscaledb::PaxNodeImpl<upscaledb::PodKeyList<unsigned int>, upscaledb::InternalRecordList>, upscaledb::NumericCompare<unsigned int> >";
    std::string expected_leafname = "upscaledb::BtreeNodeProxyImpl<upscaledb::PaxNodeImpl<upscaledb::PodKeyList<unsigned int>, upscaledb::InlineRecordList>, upscaledb::NumericCompare<unsigned int> >";
    REQUIRE(node->test_get_classname() == expected_leafname);
#endif

    char buffer[10] = {0};
    uint32_t k = 1;
    ups_key_t key = ups_make_key(&k, sizeof(k));
    ups_record_t rec = ups_make_record(buffer, sizeof(buffer));

    // now insert keys till the page is split and a new root is created
    g_split = false;
    while (!g_split) {
      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
      k++;
    }

    // now check the leaf page (same as the previous root page)
    REQUIRE((page = lenv()->page_manager->fetch(&context, 1024 * 16)));
    context.changeset.clear(); // unlock pages
    node = btree_index()->get_node_from_page(page);
    REQUIRE(ISSET(node->flags(), PBtreeNode::kLeafNode));
#ifdef HAVE_GCC_ABI_DEMANGLE
    REQUIRE(node->test_get_classname() == expected_leafname);
#endif

    // check the other leaf
    REQUIRE((page = lenv()->page_manager->fetch(&context, 2 * 1024 * 16)));
    context.changeset.clear(); // unlock pages
    node = btree_index()->get_node_from_page(page);
    REQUIRE(ISSET(node->flags(), PBtreeNode::kLeafNode));
#ifdef HAVE_GCC_ABI_DEMANGLE
    REQUIRE(node->test_get_classname() == expected_leafname);
#endif

    // and the new root page (must be an internal page)
    REQUIRE((page = lenv()->page_manager->fetch(&context, 3 * 1024 * 16)));
    context.changeset.clear(); // unlock pages
    node = btree_index()->get_node_from_page(page);
    REQUIRE(NOTSET(node->flags(), PBtreeNode::kLeafNode));
#ifdef HAVE_GCC_ABI_DEMANGLE
    REQUIRE(node->test_get_classname() == expected_internalname);
#endif
  }

  void forceInternalNodeTest() {
    ups_parameter_t p[] = {
        { UPS_PARAM_KEY_TYPE, UPS_TYPE_UINT32 },
        { UPS_PARAM_RECORD_SIZE, 512 },
        { 0, 0 }
    };

    // create the database with flags and parameters
    require_create(0, nullptr, UPS_FORCE_RECORDS_INLINE, p);

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

    close();
    require_open();

    // reopen and make sure the flag was persisted
    REQUIRE(0 == ups_db_get_parameters(db, query));
    REQUIRE(UPS_TYPE_UINT32 == (int)query[0].value);
    REQUIRE(4 == (int)query[1].value);
    REQUIRE(512 == (int)query[2].value);
    REQUIRE(31 == (int)query[3].value);
    REQUIRE(UPS_FORCE_RECORDS_INLINE == (int)query[4].value);
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
      "upscaledb::BtreeIndexTraitsImpl<upscaledb::PaxNodeImpl<upscaledb::PodKeyList<unsigned char>, upscaledb::DefaultRecordList>, upscaledb::NumericCompare<unsigned char> >");
}

TEST_CASE("Btree/uint16Type", "")
{
  BtreeFixture f;
  f.fixedTypeTest(UPS_TYPE_UINT16, 2, 1485,
      "upscaledb::BtreeIndexTraitsImpl<upscaledb::PaxNodeImpl<upscaledb::PodKeyList<unsigned short>, upscaledb::DefaultRecordList>, upscaledb::NumericCompare<unsigned short> >");
}

TEST_CASE("Btree/uint32Type", "")
{
  BtreeFixture f;
  f.fixedTypeTest(UPS_TYPE_UINT32, 4, 1256,
      "upscaledb::BtreeIndexTraitsImpl<upscaledb::PaxNodeImpl<upscaledb::PodKeyList<unsigned int>, upscaledb::DefaultRecordList>, upscaledb::NumericCompare<unsigned int> >");
}

TEST_CASE("Btree/uint64Type", "")
{
  BtreeFixture f;
  const char *abiname;
  if (sizeof(unsigned long) == 4)
    abiname = "upscaledb::BtreeIndexTraitsImpl<upscaledb::PaxNodeImpl<upscaledb::PodKeyList<unsigned long long>, upscaledb::DefaultRecordList>, upscaledb::NumericCompare<unsigned long long> >";
  else
    abiname = "upscaledb::BtreeIndexTraitsImpl<upscaledb::PaxNodeImpl<upscaledb::PodKeyList<unsigned long>, upscaledb::DefaultRecordList>, upscaledb::NumericCompare<unsigned long> >";

  f.fixedTypeTest(UPS_TYPE_UINT64, 8, 960, abiname);
}

TEST_CASE("Btree/real32Type", "")
{
  BtreeFixture f;
  f.fixedTypeTest(UPS_TYPE_REAL32, 4, 1256,
      "upscaledb::BtreeIndexTraitsImpl<upscaledb::PaxNodeImpl<upscaledb::PodKeyList<float>, upscaledb::DefaultRecordList>, upscaledb::NumericCompare<float> >");
}

TEST_CASE("Btree/real64Type", "")
{
  BtreeFixture f;
  f.fixedTypeTest(UPS_TYPE_REAL64, 8, 960,
      "upscaledb::BtreeIndexTraitsImpl<upscaledb::PaxNodeImpl<upscaledb::PodKeyList<double>, upscaledb::DefaultRecordList>, upscaledb::NumericCompare<double> >");
}

TEST_CASE("Btree/fixedBinaryType", "")
{
  BtreeFixture f;
  f.fixedTypeTest(UPS_TYPE_BINARY, 8, 960,
      "upscaledb::BtreeIndexTraitsImpl<upscaledb::PaxNodeImpl<upscaledb::BinaryKeyList, upscaledb::DefaultRecordList>, upscaledb::FixedSizeCompare>");
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
