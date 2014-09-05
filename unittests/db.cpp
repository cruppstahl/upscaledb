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

#include "ham/hamsterdb.h"

#include "2page/page.h"
#include "3btree/btree_index.h"
#include "3btree/btree_index_factory.h"
#include "3blob_manager/blob_manager.h"
#include "3page_manager/page_manager.h"
#include "4txn/txn.h"
#include "4db/db.h"
#include "4env/env.h"
#include "4env/env_header.h"
#include "3btree/btree_node.h"

namespace hamsterdb {

struct DbFixture {
  ham_db_t *m_db;
  LocalDatabase *m_dbp;
  ham_env_t *m_env;
  bool m_inmemory;

  DbFixture(bool inmemory = false)
    : m_db(0), m_dbp(0), m_env(0), m_inmemory(inmemory) {
    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"),
            (m_inmemory ? HAM_IN_MEMORY : 0), 0644, 0));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 13,
            HAM_ENABLE_DUPLICATE_KEYS, 0));
    m_dbp = (LocalDatabase *)m_db;
  }

  ~DbFixture() {
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void headerTest() {
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;
    lenv->get_header()->set_magic('1', '2', '3', '4');
    REQUIRE(true ==
        lenv->get_header()->verify_magic('1', '2', '3', '4'));

    lenv->get_header()->set_version(1, 2, 3, 4);
    REQUIRE((ham_u8_t)1 == lenv->get_header()->get_version(0));
    REQUIRE((ham_u8_t)2 == lenv->get_header()->get_version(1));
    REQUIRE((ham_u8_t)3 == lenv->get_header()->get_version(2));
    REQUIRE((ham_u8_t)4 == lenv->get_header()->get_version(3));
  }

  void structureTest() {
    REQUIRE(((LocalEnvironment *)m_env)->get_header()->get_header_page() != 0);

    REQUIRE(0 == m_dbp->get_error());
    m_dbp->set_error(HAM_IO_ERROR);
    REQUIRE(HAM_IO_ERROR == m_dbp->get_error());

    REQUIRE(m_dbp->get_btree_index()); // already initialized

    ((LocalEnvironment *)m_env)->get_header()->get_header_page()->set_dirty(false);
    REQUIRE(!((LocalEnvironment *)m_env)->get_header()->get_header_page()->is_dirty());
    ((LocalEnvironment *)m_env)->mark_header_page_dirty();
    REQUIRE(((LocalEnvironment *)m_env)->get_header()->get_header_page()->is_dirty());

    REQUIRE(0 != m_dbp->get_rt_flags());

    REQUIRE(m_dbp->get_env() != 0);
  }

  void defaultCompareTest() {
    ham_key_t key1 = {0}, key2 = {0};
    key1.data = (void *)"abc";
    key1.size = 3;
    key2.data = (void *)"abc";
    key2.size = 3;
    REQUIRE( 0 == m_dbp->get_btree_index()->compare_keys(&key1, &key2));
    key1.data = (void *)"ab";
    key1.size = 2;
    key2.data = (void *)"abc";
    key2.size = 3;
    REQUIRE(-1 == m_dbp->get_btree_index()->compare_keys(&key1, &key2));
    key1.data = (void *)"abc";
    key1.size = 3;
    key2.data = (void *)"bcd";
    key2.size = 3;
    REQUIRE(-1 == m_dbp->get_btree_index()->compare_keys(&key1, &key2));
    key1.data = (void *)"abc";
    key1.size = 3;
    key2.data = (void *)0;
    key2.size = 0;
    REQUIRE(+1 == m_dbp->get_btree_index()->compare_keys(&key1, &key2));
    key1.data = (void *)0;
    key1.size = 0;
    key2.data = (void *)"abc";
    key2.size = 3;
    REQUIRE(-1 == m_dbp->get_btree_index()->compare_keys(&key1, &key2));
  }

  void flushPageTest() {
    Page *page;
    ham_u64_t address;
    ham_u8_t *p;

    PageManager *pm = ((LocalEnvironment *)m_env)->get_page_manager();

    REQUIRE((page = pm->alloc_page(m_dbp, 0)));

    REQUIRE(m_dbp == page->get_db());
    p = page->get_payload();
    for (int i = 0; i < 16; i++)
      p[i] = (ham_u8_t)i;
    page->set_dirty(true);
    address = page->get_address();
    page->flush();
    pm->test_remove_page(page);
    delete page;

    REQUIRE((page = pm->fetch_page(m_dbp, address)));
    REQUIRE(page != 0);
    REQUIRE(address == page->get_address());
    pm->test_remove_page(page);
    delete page;
  }

  void checkStructurePackingTest() {
    // checks to make sure structure packing by the compiler is still okay
    // HAM_PACK_0 HAM_PACK_1 HAM_PACK_2 OFFSETOF
    REQUIRE(sizeof(PBlobHeader) == 28);
    REQUIRE(sizeof(PBtreeNode) == 33);
    REQUIRE(sizeof(PEnvironmentHeader) == 32);
    REQUIRE(sizeof(PBtreeHeader) == 24);
    REQUIRE(sizeof(PPageData) == 17);
    PPageData p;
    REQUIRE(sizeof(p._s) == 17);
    REQUIRE(Page::kSizeofPersistentHeader == 16);

    REQUIRE(PBtreeNode::get_entry_offset() == 32);
    Page page(0);
    LocalDatabase db((LocalEnvironment *)m_env, 1, 0);
    BtreeIndex be(&db, 0, 0, 0, HAM_KEY_SIZE_UNLIMITED);

    page.set_address(1000);
    page.set_db(&db);
    db.m_btree_index = &be;
    be.m_key_size = 666;
    REQUIRE(Page::kSizeofPersistentHeader == 16);
    // make sure the 'header page' is at least as large as your usual
    // header page, then hack it...
    struct {
      PPageData drit;
      PEnvironmentHeader drat;
    } hdrpage_pers = {{{0}}};
    Page hdrpage(0);
    hdrpage.set_data((PPageData *)&hdrpage_pers);
    Page *hp = &hdrpage;
    ham_u8_t *pl1 = hp->get_payload();
    REQUIRE(pl1);
    REQUIRE((pl1 - (ham_u8_t *)hdrpage.get_data()) == 16);
    PEnvironmentHeader *hdrptr = (PEnvironmentHeader *)(hdrpage.get_payload());
    REQUIRE(((ham_u8_t *)hdrptr - (ham_u8_t *)hdrpage.get_data()) == 16);
    hdrpage.set_data(0);
  }

};

TEST_CASE("Db/checkStructurePackingTest", "")
{
  DbFixture f;
  f.checkStructurePackingTest();
}

TEST_CASE("Db/headerTest", "")
{
  DbFixture f;
  f.headerTest();
}

TEST_CASE("Db/structureTest", "")
{
  DbFixture f;
  f.structureTest();
}

TEST_CASE("Db/defaultCompareTest", "")
{
  DbFixture f;
  f.defaultCompareTest();
}

TEST_CASE("Db/flushPageTest", "")
{
  DbFixture f;
  f.flushPageTest();
}


TEST_CASE("Db-inmem/checkStructurePackingTest", "")
{
  DbFixture f(true);
  f.checkStructurePackingTest();
}

TEST_CASE("Db-inmem/headerTest", "")
{
  DbFixture f(true);
  f.headerTest();
}

TEST_CASE("Db-inmem/structureTest", "")
{
  DbFixture f(true);
  f.structureTest();
}

TEST_CASE("Db-inmem/defaultCompareTest", "")
{
  DbFixture f(true);
  f.defaultCompareTest();
}

} // namespace hamsterdb
