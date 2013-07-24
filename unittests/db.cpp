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

#include <ham/hamsterdb.h>

#include "../src/db.h"
#include "../src/cache.h"
#include "../src/page.h"
#include "../src/env.h"
#include "../src/env_header.h"
#include "../src/btree_index.h"
#include "../src/blob_manager.h"
#include "../src/page_manager.h"
#include "../src/txn.h"
#include "../src/log.h"
#include "../src/btree_node.h"

namespace hamsterdb {

struct DbFixture {
  ham_db_t *m_db;
  LocalDatabase *m_dbp;
  ham_env_t *m_env;
  bool m_inmemory;

  DbFixture(bool inmemory = false)
    : m_db(0), m_dbp(0), m_env(0), m_inmemory(inmemory) {
    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"),
            (m_inmemory ? HAM_IN_MEMORY : 0), 0644, 0));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 13,
            HAM_ENABLE_DUPLICATES, 0));
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

    lenv->get_header()->set_serialno(0x1234);
    REQUIRE(0x1234u == lenv->get_header()->get_serialno());
  }

  void structureTest() {
    REQUIRE(((LocalEnvironment *)m_env)->get_header()->get_header_page() != 0);

    REQUIRE(0 == m_dbp->get_error());
    m_dbp->set_error(HAM_IO_ERROR);
    REQUIRE(HAM_IO_ERROR == m_dbp->get_error());

    REQUIRE(m_dbp->get_btree_index()); // already initialized

    REQUIRE(((LocalEnvironment *)m_env)->get_page_manager()->test_get_cache());

    REQUIRE(0 != m_dbp->m_prefix_func);
    ham_prefix_compare_func_t oldfoo = m_dbp->m_prefix_func;
    m_dbp->set_prefix_compare_func((ham_prefix_compare_func_t)18);
    REQUIRE((ham_prefix_compare_func_t)18 == m_dbp->m_prefix_func);
    m_dbp->set_prefix_compare_func(oldfoo);

    ham_compare_func_t oldfoo2 = m_dbp->get_compare_func();
    REQUIRE(0 != m_dbp->get_compare_func());
    m_dbp->set_compare_func((ham_compare_func_t)19);
    REQUIRE((ham_compare_func_t)19 == m_dbp->get_compare_func());
    m_dbp->set_compare_func(oldfoo2);

    ((LocalEnvironment *)m_env)->get_header()->get_header_page()->set_dirty(false);
    REQUIRE(!((LocalEnvironment *)m_env)->get_header()->get_header_page()->is_dirty());
    ((LocalEnvironment *)m_env)->mark_header_page_dirty();
    REQUIRE(((LocalEnvironment *)m_env)->get_header()->get_header_page()->is_dirty());

    REQUIRE(0!=m_dbp->get_rt_flags());

    REQUIRE(m_dbp->get_env() != 0);
  }

  void defaultCompareTest() {
    REQUIRE( 0 == LocalDatabase::default_compare(0,
            (ham_u8_t *)"abc", 3, (ham_u8_t *)"abc", 3));
    REQUIRE(-1 == LocalDatabase::default_compare(0,
            (ham_u8_t *)"ab",  2, (ham_u8_t *)"abc", 3));
    REQUIRE(-1 == LocalDatabase::default_compare(0,
            (ham_u8_t *)"abc", 3, (ham_u8_t *)"bcd", 3));
    REQUIRE(+1 == LocalDatabase::default_compare(0,
            (ham_u8_t *)"abc", 3, (ham_u8_t *)0,   0));
    REQUIRE(-1 == LocalDatabase::default_compare(0,
            (ham_u8_t *)0,   0, (ham_u8_t *)"abc", 3));
  }

  void defaultPrefixCompareTest() {
    REQUIRE(HAM_PREFIX_REQUEST_FULLKEY ==
        LocalDatabase::default_prefix_compare(0,
            (ham_u8_t *)"abc", 3, 3,
            (ham_u8_t *)"abc", 3, 3));
    // comparison code has become 'smarter' so can resolve this one 
    // without the need for further help
    REQUIRE(-1 ==
        LocalDatabase::default_prefix_compare(0,
            (ham_u8_t *)"ab",  2, 2,
            (ham_u8_t *)"abc", 3, 3));
    REQUIRE(HAM_PREFIX_REQUEST_FULLKEY ==
        LocalDatabase::default_prefix_compare(0,
            (ham_u8_t *)"ab",  2, 3,
            (ham_u8_t *)"abc", 3, 3));
    REQUIRE(-1 ==
        LocalDatabase::default_prefix_compare(0,
            (ham_u8_t *)"abc", 3, 3,
            (ham_u8_t *)"bcd", 3, 3));
    // comparison code has become 'smarter' so can resolve this 
    // one without the need for further help
    REQUIRE(+1 ==
        LocalDatabase::default_prefix_compare(0,
            (ham_u8_t *)"abc", 3, 3,
            (ham_u8_t *)0,   0, 0));
    REQUIRE(-1 ==
        LocalDatabase::default_prefix_compare(0,
            (ham_u8_t *)0,   0, 0,
            (ham_u8_t *)"abc", 3, 3));
    REQUIRE(HAM_PREFIX_REQUEST_FULLKEY ==
        LocalDatabase::default_prefix_compare(0,
            (ham_u8_t *)"abc", 3, 3,
            (ham_u8_t *)0,   0, 3));
    REQUIRE(HAM_PREFIX_REQUEST_FULLKEY ==
        LocalDatabase::default_prefix_compare(0,
            (ham_u8_t *)0,   0, 3,
            (ham_u8_t *)"abc", 3, 3));
    REQUIRE(HAM_PREFIX_REQUEST_FULLKEY ==
        LocalDatabase::default_prefix_compare(0,
            (ham_u8_t *)"abc", 3, 80239,
            (ham_u8_t *)"abc", 3, 2));
  }

  void flushPageTest() {
    Page *page;
    ham_u64_t address;
    ham_u8_t *p;

    PageManager *pm = ((LocalEnvironment *)m_env)->get_page_manager();

    REQUIRE(0 == pm->alloc_page(&page, m_dbp, 0, PageManager::kIgnoreFreelist));

    REQUIRE(m_dbp == page->get_db());
    p = page->get_raw_payload();
    for (int i = 0; i < 16; i++)
      p[i] = (ham_u8_t)i;
    page->set_dirty(true);
    address = page->get_address();
    REQUIRE(0 == page->flush());
    pm->test_get_cache()->remove_page(page);
    delete page;

    REQUIRE(0 == pm->fetch_page(&page, m_dbp, address));
    REQUIRE(page != 0);
    REQUIRE(address == page->get_address());
    p = page->get_raw_payload();
    pm->test_get_cache()->remove_page(page);
    delete page;
  }

  // using a function to compare the constants is easier for debugging
  bool compare_sizes(size_t a, size_t b) {
    return a == b;
  }

  void checkStructurePackingTest() {
    // checks to make sure structure packing by the compiler is still okay
    // HAM_PACK_0 HAM_PACK_1 HAM_PACK_2 OFFSETOF
    REQUIRE(compare_sizes(sizeof(PBlobHeader), 28));
    REQUIRE(compare_sizes(sizeof(PDupeEntry), 16));
    REQUIRE(compare_sizes(sizeof(PDupeTable),
        8 + sizeof(PDupeEntry)));
    REQUIRE(compare_sizes(sizeof(PBtreeNode), 28+sizeof(PBtreeKey)));
    REQUIRE(compare_sizes(sizeof(PBtreeKey), 12));
    REQUIRE(compare_sizes(sizeof(PEnvironmentHeader), 20));
    REQUIRE(compare_sizes(sizeof(PBtreeHeader), 32));
    REQUIRE(compare_sizes(sizeof(PFreelistPayload),
        16 + 13 + sizeof(PFreelistPageStatistics)));
    REQUIRE(compare_sizes(sizeof(PFreelistPageStatistics),
        4 * 8 + sizeof(PFreelistSlotsizeStats)
            * HAM_FREELIST_SLOT_SPREAD));
    REQUIRE(compare_sizes(sizeof(PFreelistSlotsizeStats), 8 * 4));
    REQUIRE(compare_sizes(HAM_FREELIST_SLOT_SPREAD, 16 - 5 + 1));
    REQUIRE(compare_sizes(PFreelistPayload::get_bitmap_offset(),
        16 + 12 + sizeof(PFreelistPageStatistics)));
    REQUIRE(compare_sizes(PBtreeKey::kSizeofOverhead, 11));
    REQUIRE(compare_sizes(sizeof(Log::PEnvironmentHeader), 16));
    REQUIRE(compare_sizes(sizeof(Log::PEntry), 32));
    REQUIRE(compare_sizes(sizeof(PPageData), 13));
    PPageData p;
    REQUIRE(compare_sizes(sizeof(p._s), 13));
    REQUIRE(compare_sizes(Page::sizeof_persistent_header, 12));

    REQUIRE(compare_sizes(PBtreeNode::get_entry_offset(), 28));
    Page page;
    LocalDatabase db((LocalEnvironment *)m_env, 1, 0);
    BtreeIndex be(&db, 0);

    page.set_address(1000);
    page.set_db(&db);
    db.m_btree_index = &be;
    be.m_keysize = 666;
    REQUIRE(compare_sizes(Page::sizeof_persistent_header, 12));
    // make sure the 'header page' is at least as large as your usual
    // header page, then hack it...
    struct {
      PPageData drit;
      PEnvironmentHeader drat;
    } hdrpage_pers = {{{0}}};
    Page hdrpage;
    hdrpage.set_data((PPageData *)&hdrpage_pers);
    Page *hp = &hdrpage;
    ham_u8_t *pl1 = hp->get_payload();
    REQUIRE(pl1);
    REQUIRE(compare_sizes(pl1 - (ham_u8_t *)hdrpage.get_data(), 12));
    PEnvironmentHeader *hdrptr = (PEnvironmentHeader *)(hdrpage.get_payload());
    REQUIRE(compare_sizes(((ham_u8_t *)hdrptr)
        - (ham_u8_t *)hdrpage.get_data(), 12));
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

TEST_CASE("Db/defaultPrefixCompareTest", "")
{
  DbFixture f;
  f.defaultPrefixCompareTest();
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

TEST_CASE("Db-inmem/defaultPrefixCompareTest", "")
{
  DbFixture f(true);
  f.defaultPrefixCompareTest();
}

} // namespace hamsterdb
