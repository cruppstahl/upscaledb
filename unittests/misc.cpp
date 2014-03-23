/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

#include "../src/config.h"

#include <cstring>

#include "3rdparty/catch/catch.hpp"

#include "globals.h"

#include "../src/db_local.h"
#include "../src/page.h"
#include "../src/util.h"
#include "../src/env.h"
#include "../src/btree_index.h"
#include "../src/btree_node_proxy.h"

namespace hamsterdb {

struct MiscFixture {
  ham_db_t *m_db;
  ham_env_t *m_env;
  LocalDatabase *m_dbp;
  BtreeIndex *m_btree;

  MiscFixture() {
    ham_parameter_t p[] = { { HAM_PARAM_PAGESIZE, 4096 }, { 0, 0 } };

    REQUIRE(0 ==
          ham_env_create(&m_env, 0, HAM_IN_MEMORY, 0644, &p[0]));
    REQUIRE(0 ==
          ham_env_create_db(m_env, &m_db, 1, 0, 0));

    m_dbp = (LocalDatabase *)m_db;
    m_btree = m_dbp->get_btree_index();
  }

  ~MiscFixture() {
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void copyKeyInt2PubEmptyTest() {
    Page *page;
    page = new Page((LocalEnvironment *)m_env);
    page->set_db(m_dbp);
    page->allocate(0, Page::kInitializeWithZeroes);
    BtreeNodeProxy *node = m_btree->get_node_from_page(page);

    ham_key_t key = {0};
    ByteArray arena;

    node->test_set_key(0, "", 0, 0, 0x12345);

    node->get_key(0, &arena, &key);
    REQUIRE(key.size == 0);
    REQUIRE(key.data == 0);

    delete page;
  }

  void copyKeyInt2PubTinyTest() {
    Page *page;
    page = new Page((LocalEnvironment *)m_env);
    page->set_db(m_dbp);
    page->allocate(0, Page::kInitializeWithZeroes);
    BtreeNodeProxy *node = m_btree->get_node_from_page(page);

    ham_key_t key = {0};
    ByteArray arena;

    node->test_set_key(0, "a", 1, 0, 0x12345);

    node->get_key(0, &arena, &key);
    REQUIRE(1 == key.size);
    REQUIRE('a' == ((char *)key.data)[0]);

    delete page;
  }

  void copyKeyInt2PubSmallTest() {
    Page *page;
    page = new Page((LocalEnvironment *)m_env);
    page->set_db(m_dbp);
    page->allocate(0, Page::kInitializeWithZeroes);
    BtreeNodeProxy *node = m_btree->get_node_from_page(page);

    ham_key_t key = {0};
    ByteArray arena;

    node->test_set_key(0, "1234567\0", 8, 0, 0x12345);

    node->get_key(0, &arena, &key);
    REQUIRE(key.size == 8);
    REQUIRE(0 == ::strcmp((char *)key.data, "1234567\0"));

    delete page;
  }

  void copyKeyInt2PubFullTest() {
    Page *page;
    page = new Page((LocalEnvironment *)m_env);
    page->set_db(m_dbp);
    page->allocate(0, Page::kInitializeWithZeroes);
    BtreeNodeProxy *node = m_btree->get_node_from_page(page);

    ham_key_t key = {0};
    ByteArray arena;

    node->test_set_key(0, "123456781234567\0", 16, 0, 0x12345);

    node->get_key(0, &arena, &key);
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
