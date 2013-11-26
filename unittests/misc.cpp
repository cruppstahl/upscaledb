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

#include <cstring>

#include "3rdparty/catch/catch.hpp"

#include "globals.h"

#include "../src/db_local.h"
#include "../src/page.h"
#include "../src/util.h"
#include "../src/env.h"
#include "../src/btree_index.h"
#include "../src/btree_key.h"
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
    page->allocate();
    memset(page->get_raw_payload(), 0, 4096);
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
    page->allocate();
    memset(page->get_raw_payload(), 0, 4096);
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
    page->allocate();
    memset(page->get_raw_payload(), 0, 4096);
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
    page->allocate();
    memset(page->get_raw_payload(), 0, 4096);
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
