/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

#include <cstring>

#include "3rdparty/catch/catch.hpp"

#include "utils.h"

#include "2page/page.h"
#include "3btree/btree_index.h"
#include "3btree/btree_node_proxy.h"
#include "4context/context.h"
#include "4db/db_local.h"
#include "4env/env.h"

namespace hamsterdb {

struct MiscFixture {
  ham_db_t *m_db;
  ham_env_t *m_env;
  LocalDatabase *m_dbp;
  BtreeIndex *m_btree;
  ScopedPtr<Context> m_context;

  MiscFixture() {
    ham_parameter_t p[] = { { HAM_PARAM_PAGESIZE, 4096 }, { 0, 0 } };

    REQUIRE(0 ==
          ham_env_create(&m_env, 0, HAM_IN_MEMORY, 0644, &p[0]));
    REQUIRE(0 ==
          ham_env_create_db(m_env, &m_db, 1, 0, 0));

    m_dbp = (LocalDatabase *)m_db;
    m_btree = m_dbp->btree_index();
    m_context.reset(new Context((LocalEnvironment *)m_env, 0, m_dbp));
  }

  ~MiscFixture() {
    m_context->changeset.clear();
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void copyKeyInt2PubEmptyTest() {
    Page *page;
    page = new Page(((LocalEnvironment *)m_env)->device());
    page->set_db(m_dbp);
    page->alloc(0, Page::kInitializeWithZeroes);
    BtreeNodeProxy *node = m_btree->get_node_from_page(page);

    ham_key_t key = {0};

    node->insert(m_context.get(), &key, PBtreeNode::kInsertPrepend);

    ByteArray arena;
    memset(&key, 0, sizeof(key));
    node->get_key(m_context.get(), 0, &arena, &key);
    REQUIRE(key.size == 0);
    REQUIRE(key.data == 0);

    delete page;
  }

  void copyKeyInt2PubTinyTest() {
    Page *page;
    page = new Page(((LocalEnvironment *)m_env)->device());
    page->set_db(m_dbp);
    page->alloc(0, Page::kInitializeWithZeroes);
    BtreeNodeProxy *node = m_btree->get_node_from_page(page);

    ham_key_t key = {0};
    key.data = (void *)"a";
    key.size = 1;

    node->insert(m_context.get(), &key, PBtreeNode::kInsertPrepend);

    ByteArray arena;
    memset(&key, 0, sizeof(key));
    node->get_key(m_context.get(), 0, &arena, &key);
    REQUIRE(1 == key.size);
    REQUIRE('a' == ((char *)key.data)[0]);

    delete page;
  }

  void copyKeyInt2PubSmallTest() {
    Page *page;
    page = new Page(((LocalEnvironment *)m_env)->device());
    page->set_db(m_dbp);
    page->alloc(0, Page::kInitializeWithZeroes);
    BtreeNodeProxy *node = m_btree->get_node_from_page(page);

    ham_key_t key = {0};
    key.data = (void *)"1234567\0";
    key.size = 8;

    node->insert(m_context.get(), &key, PBtreeNode::kInsertPrepend);

    ByteArray arena;
    memset(&key, 0, sizeof(key));
    node->get_key(m_context.get(), 0, &arena, &key);
    REQUIRE(key.size == 8);
    REQUIRE(0 == ::strcmp((char *)key.data, "1234567\0"));

    delete page;
  }

  void copyKeyInt2PubFullTest() {
    Page *page;
    page = new Page(((LocalEnvironment *)m_env)->device());
    page->set_db(m_dbp);
    page->alloc(0, Page::kInitializeWithZeroes);
    BtreeNodeProxy *node = m_btree->get_node_from_page(page);

    ham_key_t key = {0};
    key.data = (void *)"123456781234567\0";
    key.size = 16;

    node->insert(m_context.get(), &key, PBtreeNode::kInsertPrepend);

    ByteArray arena;
    memset(&key, 0, sizeof(key));
    node->get_key(m_context.get(), 0, &arena, &key);
    REQUIRE(key.size == 16);
    REQUIRE(0 == ::strcmp((char *)key.data, "123456781234567\0"));

    delete page;
  }
};

TEST_CASE("MiscFixture/copyKeyInt2PubEmptyTest",
           "Tests miscellaneous functions")
{
  MiscFixture mt;
  mt.copyKeyInt2PubEmptyTest();
}

TEST_CASE("MiscFixture/copyKeyInt2PubTinyTest",
           "Tests miscellaneous functions")
{
  MiscFixture mt;
  mt.copyKeyInt2PubTinyTest();
}

TEST_CASE("MiscFixture/copyKeyInt2PubSmallTest",
           "Tests miscellaneous functions")
{
  MiscFixture mt;
  mt.copyKeyInt2PubSmallTest();
}

TEST_CASE("MiscFixture/copyKeyInt2PubFullTest",
           "Tests miscellaneous functions")
{
  MiscFixture mt;
  mt.copyKeyInt2PubFullTest();
}

} // namespace hamsterdb
