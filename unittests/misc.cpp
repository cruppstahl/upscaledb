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

#include <cstring>

#include "3rdparty/catch/catch.hpp"

#include "utils.h"

#include "2page/page.h"
#include "3btree/btree_index.h"
#include "3btree/btree_node_proxy.h"
#include "4context/context.h"
#include "4db/db_local.h"
#include "4env/env.h"

namespace upscaledb {

struct MiscFixture {
  ups_db_t *m_db;
  ups_env_t *m_env;
  LocalDb *m_dbp;
  BtreeIndex *m_btree;
  ScopedPtr<Context> m_context;

  MiscFixture() {
    ups_parameter_t p[] = { { UPS_PARAM_PAGESIZE, 4096 }, { 0, 0 } };

    REQUIRE(0 ==
          ups_env_create(&m_env, 0, UPS_IN_MEMORY, 0644, &p[0]));
    REQUIRE(0 ==
          ups_env_create_db(m_env, &m_db, 1, 0, 0));

    m_dbp = (LocalDb *)m_db;
    m_btree = m_dbp->btree_index.get();
    m_context.reset(new Context((LocalEnv *)m_env, 0, m_dbp));
  }

  ~MiscFixture() {
    m_context->changeset.clear();
    REQUIRE(0 == ups_env_close(m_env, UPS_AUTO_CLEANUP));
  }

  void copyKeyInt2PubEmptyTest() {
    Page *page;
    page = new Page(((LocalEnv *)m_env)->device.get());
    page->set_db(m_dbp);
    page->alloc(0, Page::kInitializeWithZeroes);
    BtreeNodeProxy *node = m_btree->get_node_from_page(page);

    ups_key_t key = {0};

    node->insert(m_context.get(), &key, PBtreeNode::kInsertPrepend);

    ByteArray arena;
    memset(&key, 0, sizeof(key));
    node->key(m_context.get(), 0, &arena, &key);
    REQUIRE(key.size == 0);
    REQUIRE(key.data == 0);

    delete page;
  }

  void copyKeyInt2PubTinyTest() {
    Page *page;
    page = new Page(((LocalEnv *)m_env)->device.get());
    page->set_db(m_dbp);
    page->alloc(0, Page::kInitializeWithZeroes);
    BtreeNodeProxy *node = m_btree->get_node_from_page(page);

    ups_key_t key = {0};
    key.data = (void *)"a";
    key.size = 1;

    node->insert(m_context.get(), &key, PBtreeNode::kInsertPrepend);

    ByteArray arena;
    memset(&key, 0, sizeof(key));
    node->key(m_context.get(), 0, &arena, &key);
    REQUIRE(1 == key.size);
    REQUIRE('a' == ((char *)key.data)[0]);

    delete page;
  }

  void copyKeyInt2PubSmallTest() {
    Page *page;
    page = new Page(((LocalEnv *)m_env)->device.get());
    page->set_db(m_dbp);
    page->alloc(0, Page::kInitializeWithZeroes);
    BtreeNodeProxy *node = m_btree->get_node_from_page(page);

    ups_key_t key = {0};
    key.data = (void *)"1234567\0";
    key.size = 8;

    node->insert(m_context.get(), &key, PBtreeNode::kInsertPrepend);

    ByteArray arena;
    memset(&key, 0, sizeof(key));
    node->key(m_context.get(), 0, &arena, &key);
    REQUIRE(key.size == 8);
    REQUIRE(0 == ::strcmp((char *)key.data, "1234567\0"));

    delete page;
  }

  void copyKeyInt2PubFullTest() {
    Page *page;
    page = new Page(((LocalEnv *)m_env)->device.get());
    page->set_db(m_dbp);
    page->alloc(0, Page::kInitializeWithZeroes);
    BtreeNodeProxy *node = m_btree->get_node_from_page(page);

    ups_key_t key = {0};
    key.data = (void *)"123456781234567\0";
    key.size = 16;

    node->insert(m_context.get(), &key, PBtreeNode::kInsertPrepend);

    ByteArray arena;
    memset(&key, 0, sizeof(key));
    node->key(m_context.get(), 0, &arena, &key);
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

} // namespace upscaledb
