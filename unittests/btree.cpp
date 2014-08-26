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

#include "3rdparty/catch/catch.hpp"

#include "utils.h"
#include "os.hpp"

#include "3page_manager/page_manager.h"
#include "3btree/btree_index.h"
#include "3btree/btree_node_proxy.h"
#include "3btree/btree_impl_default.h"
#include "4env/env_local.h"

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
    os::unlink(Utils::opath("test.db"));
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
    REQUIRE(0 == ham_env_create(&env, Utils::opath("test.db"), 0, 0, 0));
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
    REQUIRE(HAM_KEY_SIZE_UNLIMITED == query[1].value);
    REQUIRE(441 == query[2].value);
    REQUIRE(HAM_RECORD_SIZE_UNLIMITED == query[3].value);

#ifdef HAVE_GCC_ABI_DEMANGLE
    // do not run the next test if this is an evaluation version, because
    // eval-versions have obfuscated symbol names
    if (ham_is_pro_evaluation() == 0) {
      std::string s;
      s = ((LocalDatabase *)db)->get_btree_index()->test_get_classname();
      REQUIRE(s == "hamsterdb::BtreeIndexTraitsImpl<hamsterdb::DefaultNodeImpl<hamsterdb::DefLayout::VariableLengthKeyList, hamsterdb::PaxLayout::DefaultRecordList>, hamsterdb::VariableSizeCompare>");
    }
#endif

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void fixedTypeTest(int type, int size, int maxkeys, const char *abiname) {
    std::string abi;
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
    REQUIRE(0 == ham_env_create(&env, Utils::opath("test.db"), 0, 0, 0));
    REQUIRE(0 == ham_env_create_db(env, &db, 1, 0, &ps[0]));

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
    // do not run the next test if this is an evaluation version, because
    // eval-versions have obfuscated symbol names
    if (ham_is_pro_evaluation() == 0) {
      abi = ((LocalDatabase *)db)->get_btree_index()->test_get_classname();
      REQUIRE(abi == abiname);
    }
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
    REQUIRE(0 == ham_env_open(&env, Utils::opath("test.db"), 0, 0));
    REQUIRE(0 == ham_env_open_db(env, &db, 1, 0, 0));

    REQUIRE(0 == ham_db_get_parameters(db, query));
    REQUIRE(type == (int)query[0].value);
    REQUIRE(size == (int)query[1].value);
    REQUIRE(maxkeys == (int)query[2].value);

#ifdef HAVE_GCC_ABI_DEMANGLE
    // do not run the next test if this is an evaluation version, because
    // eval-versions have obfuscated symbol names
    if (ham_is_pro_evaluation() == 0) {
      std::string abi2;
      abi2 = ((LocalDatabase *)db)->get_btree_index()->test_get_classname();
      REQUIRE(abi2 == abi);
    }
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
    REQUIRE(0 == ham_env_create(&env, Utils::opath("test.db"), 0, 0, &p1[0]));
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
    REQUIRE(4677 == (int)query[3].value);
    REQUIRE(HAM_FORCE_RECORDS_INLINE == (int)query[4].value);

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));

    // reopen and make sure the flag was persisted
    REQUIRE(0 == ham_env_open(&env, Utils::opath("test.db"), 0, 0));
    REQUIRE(0 == ham_env_open_db(env, &db, 1, 0, 0));
    REQUIRE(0 == ham_db_get_parameters(db, query));
    REQUIRE(HAM_TYPE_UINT32 == (int)query[0].value);
    REQUIRE(4 == (int)query[1].value);
    REQUIRE(10 == (int)query[2].value);
    REQUIRE(4677 == (int)query[3].value);
    REQUIRE(HAM_FORCE_RECORDS_INLINE == (int)query[4].value);

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
    REQUIRE(0 == ham_env_create(&env, Utils::opath("test.db"), 0, 0, 0));
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
    REQUIRE(1166 == (int)query[3].value);
    REQUIRE(HAM_FORCE_RECORDS_INLINE == (int)query[4].value);

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
    REQUIRE(0 == ham_env_open(&env, Utils::opath("test.db"), 0, 0));
    REQUIRE(0 == ham_env_open_db(env, &db, 1, 0, 0));
    LocalEnvironment *lenv = (LocalEnvironment *)env;

    Page *page;
    REQUIRE((page = lenv->get_page_manager()->fetch_page((LocalDatabase *)db,
                        1024 * 16)));
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
    REQUIRE(0 == ham_env_create(&env, Utils::opath("test.db"), 0, 0, 0));
    REQUIRE(0 == ham_env_create_db(env, &db, 1, 0, &p[0]));

    LocalEnvironment *lenv = (LocalEnvironment *)env;
    LocalDatabase *ldb = (LocalDatabase *)db;

    g_BTREE_INSERT_SPLIT_HOOK = split_hook;

    // check if the root page proxy was created correctly (it's a leaf)
    REQUIRE((page = lenv->get_page_manager()->fetch_page((LocalDatabase *)db,
                    1024 * 16)));
    node = ldb->get_btree_index()->get_node_from_page(page);
    REQUIRE((node->get_flags() & PBtreeNode::kLeafNode)
                   == PBtreeNode::kLeafNode);
#ifdef HAVE_GCC_ABI_DEMANGLE
    std::string expected_internalname = "hamsterdb::BtreeNodeProxyImpl<hamsterdb::PaxNodeImpl<hamsterdb::PaxLayout::PodKeyList<unsigned int>, hamsterdb::PaxLayout::InternalRecordList>, hamsterdb::NumericCompare<unsigned int> >";
    std::string expected_leafname = "hamsterdb::BtreeNodeProxyImpl<hamsterdb::PaxNodeImpl<hamsterdb::PaxLayout::PodKeyList<unsigned int>, hamsterdb::PaxLayout::InlineRecordList>, hamsterdb::NumericCompare<unsigned int> >";
    // do not run the next test if this is an evaluation version, because
    // eval-versions have obfuscated symbol names
    if (ham_is_pro_evaluation() == 0) {
      REQUIRE(node->test_get_classname() == expected_leafname);
    }
#endif

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
    REQUIRE((page = lenv->get_page_manager()->fetch_page((LocalDatabase *)db,
                        1024 * 16)));
    node = ldb->get_btree_index()->get_node_from_page(page);
    REQUIRE((node->get_flags() & PBtreeNode::kLeafNode)
                   == PBtreeNode::kLeafNode);
#ifdef HAVE_GCC_ABI_DEMANGLE
    // do not run the next test if this is an evaluation version, because
    // eval-versions have obfuscated symbol names
    if (ham_is_pro_evaluation() == 0) {
      REQUIRE(node->test_get_classname() == expected_leafname);
    }
#endif

    // check the other leaf
    REQUIRE((page = lenv->get_page_manager()->fetch_page((LocalDatabase *)db,
                        2 * 1024 * 16)));
    node = ldb->get_btree_index()->get_node_from_page(page);
    REQUIRE((node->get_flags() & PBtreeNode::kLeafNode)
                   == PBtreeNode::kLeafNode);
#ifdef HAVE_GCC_ABI_DEMANGLE
    // do not run the next test if this is an evaluation version, because
    // eval-versions have obfuscated symbol names
    if (ham_is_pro_evaluation() == 0) {
      REQUIRE(node->test_get_classname() == expected_leafname);
    }
#endif

    // and the new root page (must be an internal page)
    REQUIRE((page = lenv->get_page_manager()->fetch_page((LocalDatabase *)db,
                        3 * 1024 * 16)));
    node = ldb->get_btree_index()->get_node_from_page(page);
    REQUIRE((node->get_flags() & PBtreeNode::kLeafNode) == 0);
#ifdef HAVE_GCC_ABI_DEMANGLE
    // do not run the next test if this is an evaluation version, because
    // eval-versions have obfuscated symbol names
    if (ham_is_pro_evaluation() == 0) {
      REQUIRE(node->test_get_classname() == expected_internalname);
    }
#endif

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
    REQUIRE(0 == ham_env_create(&env, Utils::opath("test.db"), 0, 0, 0));
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
    REQUIRE(31 == (int)query[3].value);
    REQUIRE(HAM_FORCE_RECORDS_INLINE == (int)query[4].value);

    REQUIRE(0 == ham_env_close(env, HAM_AUTO_CLEANUP));

    // reopen and make sure the flag was persisted
    REQUIRE(0 == ham_env_open(&env, Utils::opath("test.db"), 0, 0));
    REQUIRE(0 == ham_env_open_db(env, &db, 1, 0, 0));
    REQUIRE(0 == ham_db_get_parameters(db, query));
    REQUIRE(HAM_TYPE_UINT32 == (int)query[0].value);
    REQUIRE(4 == (int)query[1].value);
    REQUIRE(512 == (int)query[2].value);
    REQUIRE(31 == (int)query[3].value);
    REQUIRE(HAM_FORCE_RECORDS_INLINE == (int)query[4].value);

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
  f.fixedTypeTest(HAM_TYPE_UINT8, 1, 1633,
      "hamsterdb::BtreeIndexTraitsImpl<hamsterdb::PaxNodeImpl<hamsterdb::PaxLayout::PodKeyList<unsigned char>, hamsterdb::PaxLayout::DefaultRecordList>, hamsterdb::NumericCompare<unsigned char> >");
}

TEST_CASE("Btree/uint16Type", "")
{
  BtreeFixture f;
  f.fixedTypeTest(HAM_TYPE_UINT16, 2, 1485,
      "hamsterdb::BtreeIndexTraitsImpl<hamsterdb::PaxNodeImpl<hamsterdb::PaxLayout::PodKeyList<unsigned short>, hamsterdb::PaxLayout::DefaultRecordList>, hamsterdb::NumericCompare<unsigned short> >");
}

TEST_CASE("Btree/uint32Type", "")
{
  BtreeFixture f;
  f.fixedTypeTest(HAM_TYPE_UINT32, 4, 1256,
      "hamsterdb::BtreeIndexTraitsImpl<hamsterdb::PaxNodeImpl<hamsterdb::PaxLayout::PodKeyList<unsigned int>, hamsterdb::PaxLayout::DefaultRecordList>, hamsterdb::NumericCompare<unsigned int> >");
}

TEST_CASE("Btree/uint64Type", "")
{
  BtreeFixture f;
  f.fixedTypeTest(HAM_TYPE_UINT64, 8, 960,
      "hamsterdb::BtreeIndexTraitsImpl<hamsterdb::PaxNodeImpl<hamsterdb::PaxLayout::PodKeyList<unsigned long>, hamsterdb::PaxLayout::DefaultRecordList>, hamsterdb::NumericCompare<unsigned long> >");
}

TEST_CASE("Btree/real32Type", "")
{
  BtreeFixture f;
  f.fixedTypeTest(HAM_TYPE_REAL32, 4, 1256,
      "hamsterdb::BtreeIndexTraitsImpl<hamsterdb::PaxNodeImpl<hamsterdb::PaxLayout::PodKeyList<float>, hamsterdb::PaxLayout::DefaultRecordList>, hamsterdb::NumericCompare<float> >");
}

TEST_CASE("Btree/real64Type", "")
{
  BtreeFixture f;
  f.fixedTypeTest(HAM_TYPE_REAL64, 8, 960,
      "hamsterdb::BtreeIndexTraitsImpl<hamsterdb::PaxNodeImpl<hamsterdb::PaxLayout::PodKeyList<double>, hamsterdb::PaxLayout::DefaultRecordList>, hamsterdb::NumericCompare<double> >");
}

TEST_CASE("Btree/fixedBinaryType", "")
{
  BtreeFixture f;
  f.fixedTypeTest(HAM_TYPE_BINARY, 8, 960,
      "hamsterdb::BtreeIndexTraitsImpl<hamsterdb::PaxNodeImpl<hamsterdb::PaxLayout::BinaryKeyList, hamsterdb::PaxLayout::DefaultRecordList>, hamsterdb::FixedSizeCompare>");
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
