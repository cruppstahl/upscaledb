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

#include <stdexcept>

#include <ham/hamsterdb.h>

#include "../src/db.h"
#include "../src/cache.h"
#include "../src/page.h"
#include "../src/env.h"
#include "../src/btree.h"
#include "../src/blob_manager.h"
#include "../src/page_manager.h"
#include "../src/txn.h"
#include "../src/log.h"
#include "../src/btree_node.h"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;
using namespace hamsterdb;

class DbTest : public hamsterDB_fixture {
  define_super(hamsterDB_fixture);

public:
  DbTest(bool inmemory = false, const char *name = "DbTest")
    : hamsterDB_fixture(name), m_db(0), m_dbp(0), m_env(0),
      m_inmemory(inmemory) {
    testrunner::get_instance()->register_fixture(this);
    BFC_REGISTER_TEST(DbTest, checkStructurePackingTest);
    BFC_REGISTER_TEST(DbTest, headerTest);
    BFC_REGISTER_TEST(DbTest, structureTest);
    BFC_REGISTER_TEST(DbTest, envStructureTest);
    BFC_REGISTER_TEST(DbTest, defaultCompareTest);
    BFC_REGISTER_TEST(DbTest, defaultPrefixCompareTest);
    BFC_REGISTER_TEST(DbTest, allocPageTest);
    BFC_REGISTER_TEST(DbTest, fetchPageTest);
    BFC_REGISTER_TEST(DbTest, flushPageTest);
  }

protected:
  ham_db_t *m_db;
  Database *m_dbp;
  ham_env_t *m_env;
  bool m_inmemory;

public:
  virtual void setup() {
    __super::setup();

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
            (m_inmemory ? HAM_IN_MEMORY : 0), 0644, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 13,
            HAM_ENABLE_DUPLICATES, 0));
    m_dbp=(Database *)m_db;
  }

  virtual void teardown() {
    __super::teardown();

    ham_env_close(m_env, HAM_AUTO_CLEANUP);
  }

  void headerTest() {
    ((Environment *)m_env)->set_magic('1', '2', '3', '4');
    BFC_ASSERT_EQUAL(true,
        ((Environment *)m_env)->verify_magic('1', '2', '3', '4'));

    ((Environment *)m_env)->set_version(1, 2, 3, 4);
    BFC_ASSERT_EQUAL((ham_u8_t)1, ((Environment *)m_env)->get_version(0));
    BFC_ASSERT_EQUAL((ham_u8_t)2, ((Environment *)m_env)->get_version(1));
    BFC_ASSERT_EQUAL((ham_u8_t)3, ((Environment *)m_env)->get_version(2));
    BFC_ASSERT_EQUAL((ham_u8_t)4, ((Environment *)m_env)->get_version(3));

    ((Environment *)m_env)->set_serialno(0x1234);
    BFC_ASSERT_EQUAL(0x1234u, ((Environment *)m_env)->get_serialno());
  }

  void structureTest() {
    BFC_ASSERT(((Environment *)m_env)->get_header_page()!=0);

    BFC_ASSERT_EQUAL(0, m_dbp->get_error());
    m_dbp->set_error(HAM_IO_ERROR);
    BFC_ASSERT_EQUAL(HAM_IO_ERROR, m_dbp->get_error());

    BFC_ASSERT_NOTNULL(m_dbp->get_btree());// already initialized
    BtreeIndex *oldbe = m_dbp->get_btree();
    m_dbp->set_btree((BtreeIndex *)15);
    BFC_ASSERT_EQUAL((BtreeIndex *)15, m_dbp->get_btree());
    m_dbp->set_btree(oldbe);

    BFC_ASSERT_NOTNULL(((Environment *)m_env)->get_page_manager()->test_get_cache());

    BFC_ASSERT(0 != m_dbp->get_prefix_compare_func());
    ham_prefix_compare_func_t oldfoo = m_dbp->get_prefix_compare_func();
    m_dbp->set_prefix_compare_func((ham_prefix_compare_func_t)18);
    BFC_ASSERT_EQUAL((ham_prefix_compare_func_t)18,
        m_dbp->get_prefix_compare_func());
    m_dbp->set_prefix_compare_func(oldfoo);

    ham_compare_func_t oldfoo2 = m_dbp->get_compare_func();
    BFC_ASSERT(0 != m_dbp->get_compare_func());
    m_dbp->set_compare_func((ham_compare_func_t)19);
    BFC_ASSERT_EQUAL((ham_compare_func_t)19, m_dbp->get_compare_func());
    m_dbp->set_compare_func(oldfoo2);

    ((Environment *)m_env)->get_header_page()->set_dirty(false);
    BFC_ASSERT(!((Environment *)m_env)->is_dirty());
    ((Environment *)m_env)->set_dirty(true);
    BFC_ASSERT(((Environment *)m_env)->is_dirty());

    BFC_ASSERT(0!=m_dbp->get_rt_flags());

    BFC_ASSERT(m_dbp->get_env() != 0);
  }

  void envStructureTest() {
    Environment *env = new LocalEnvironment;

    env->set_txn_id(0x12345ull);
    env->set_file_mode(0666);
    env->set_flags(0x18);

    /* TODO test other stuff! */
    env->set_flags(0);
    env->set_header_page(0);

    delete env;
  }

  void defaultCompareTest() {
    BFC_ASSERT_EQUAL( 0, Database::default_compare(0,
            (ham_u8_t *)"abc", 3, (ham_u8_t *)"abc", 3));
    BFC_ASSERT_EQUAL(-1, Database::default_compare(0,
            (ham_u8_t *)"ab",  2, (ham_u8_t *)"abc", 3));
    BFC_ASSERT_EQUAL(-1, Database::default_compare(0,
            (ham_u8_t *)"abc", 3, (ham_u8_t *)"bcd", 3));
    BFC_ASSERT_EQUAL(+1, Database::default_compare(0,
            (ham_u8_t *)"abc", 3, (ham_u8_t *)0,   0));
    BFC_ASSERT_EQUAL(-1, Database::default_compare(0,
            (ham_u8_t *)0,   0, (ham_u8_t *)"abc", 3));
  }

  void defaultPrefixCompareTest() {
    BFC_ASSERT_EQUAL(HAM_PREFIX_REQUEST_FULLKEY,
        Database::default_prefix_compare(0,
            (ham_u8_t *)"abc", 3, 3,
            (ham_u8_t *)"abc", 3, 3));
    // comparison code has become 'smarter' so can resolve this one 
    // without the need for further help
    BFC_ASSERT_EQUAL(-1,
        Database::default_prefix_compare(0,
            (ham_u8_t *)"ab",  2, 2,
            (ham_u8_t *)"abc", 3, 3));
    BFC_ASSERT_EQUAL(HAM_PREFIX_REQUEST_FULLKEY,
        Database::default_prefix_compare(0,
            (ham_u8_t *)"ab",  2, 3,
            (ham_u8_t *)"abc", 3, 3));
    BFC_ASSERT_EQUAL(-1,
        Database::default_prefix_compare(0,
            (ham_u8_t *)"abc", 3, 3,
            (ham_u8_t *)"bcd", 3, 3));
    // comparison code has become 'smarter' so can resolve this 
    // one without the need for further help
    BFC_ASSERT_EQUAL(+1,
        Database::default_prefix_compare(0,
            (ham_u8_t *)"abc", 3, 3,
            (ham_u8_t *)0,   0, 0));
    BFC_ASSERT_EQUAL(-1,
        Database::default_prefix_compare(0,
            (ham_u8_t *)0,   0, 0,
            (ham_u8_t *)"abc", 3, 3));
    BFC_ASSERT_EQUAL(HAM_PREFIX_REQUEST_FULLKEY,
        Database::default_prefix_compare(0,
            (ham_u8_t *)"abc", 3, 3,
            (ham_u8_t *)0,   0, 3));
    BFC_ASSERT_EQUAL(HAM_PREFIX_REQUEST_FULLKEY,
        Database::default_prefix_compare(0,
            (ham_u8_t *)0,   0, 3,
            (ham_u8_t *)"abc", 3, 3));
    BFC_ASSERT_EQUAL(HAM_PREFIX_REQUEST_FULLKEY,
        Database::default_prefix_compare(0,
            (ham_u8_t *)"abc", 3, 80239,
            (ham_u8_t *)"abc", 3, 2));
  }

  void allocPageTest() {
    Page *page;
    BFC_ASSERT_EQUAL(0,
        m_dbp->alloc_page(&page, 0, PageManager::kIgnoreFreelist));
    BFC_ASSERT_EQUAL(m_dbp, page->get_db());
    page->free();
    ((Environment *)m_env)->get_page_manager()->test_get_cache()->remove_page(page);
    delete page;
  }

  void fetchPageTest() {
    Page *p1, *p2;
    BFC_ASSERT_EQUAL(0,
        m_dbp->alloc_page(&p1, 0, PageManager::kIgnoreFreelist));
    BFC_ASSERT_EQUAL(m_dbp, p1->get_db());
    BFC_ASSERT_EQUAL(0, m_dbp->fetch_page(&p2, p1->get_self()));
    BFC_ASSERT_EQUAL(p2->get_self(), p1->get_self());
    p1->free();
    ((Environment *)m_env)->get_page_manager()->test_get_cache()->remove_page(p1);
    delete p1;
  }

  void flushPageTest() {
    Page *page;
    ham_u64_t address;
    ham_u8_t *p;

    BFC_ASSERT_EQUAL(0,
            m_dbp->alloc_page(&page, 0, PageManager::kIgnoreFreelist));

    BFC_ASSERT_EQUAL(m_dbp, page->get_db());
    p = page->get_raw_payload();
    for (int i = 0; i < 16; i++)
      p[i] = (ham_u8_t)i;
    page->set_dirty(true);
    address = page->get_self();
    BFC_ASSERT_EQUAL(0, page->flush());
    page->free();
    ((Environment *)m_env)->get_page_manager()->test_get_cache()->remove_page(page);
    delete page;

    BFC_ASSERT_EQUAL(0, m_dbp->fetch_page(&page, address));
    BFC_ASSERT(page != 0);
    BFC_ASSERT_EQUAL(address, page->get_self());
    p = page->get_raw_payload();
    page->free();
    ((Environment *)m_env)->get_page_manager()->test_get_cache()->remove_page(page);
    delete page;
  }

  // using a function to compare the constants is easier for debugging
  bool compare_sizes(size_t a, size_t b) {
    return a == b;
  }

  void checkStructurePackingTest() {
    // checks to make sure structure packing by the compiler is still okay
    // HAM_PACK_0 HAM_PACK_1 HAM_PACK_2 OFFSETOF
    BFC_ASSERT(compare_sizes(sizeof(PBlobHeader), 28));
    BFC_ASSERT(compare_sizes(sizeof(PDupeEntry), 16));
    BFC_ASSERT(compare_sizes(sizeof(PDupeTable),
        8 + sizeof(PDupeEntry)));
    BFC_ASSERT(compare_sizes(sizeof(PBtreeNode), 28+sizeof(PBtreeKey)));
    BFC_ASSERT(compare_sizes(sizeof(PBtreeKey), 12));
    BFC_ASSERT(compare_sizes(sizeof(PEnvHeader), 20));
    BFC_ASSERT(compare_sizes(sizeof(PBtreeDescriptor), 32));
    BFC_ASSERT(compare_sizes(sizeof(PFullFreelistPayload),
        16 + 13 + sizeof(PFreelistPageStatistics)));
    BFC_ASSERT(compare_sizes(sizeof(PFreelistPageStatistics),
        4 * 8 + sizeof(PFreelistSlotsizeStats)
            * HAM_FREELIST_SLOT_SPREAD));
    BFC_ASSERT(compare_sizes(sizeof(PFreelistSlotsizeStats), 8 * 4));
    BFC_ASSERT(compare_sizes(HAM_FREELIST_SLOT_SPREAD, 16 - 5 + 1));
    BFC_ASSERT(compare_sizes(freel_get_bitmap_offset(),
        16 + 12 + sizeof(PFreelistPageStatistics)));
    BFC_ASSERT(compare_sizes(PBtreeKey::ms_sizeof_overhead, 11));
    BFC_ASSERT(compare_sizes(sizeof(Log::PHeader), 16));
    BFC_ASSERT(compare_sizes(sizeof(Log::PEntry), 32));
    BFC_ASSERT(compare_sizes(sizeof(PageData), 13));
    PageData p;
    BFC_ASSERT(compare_sizes(sizeof(p._s), 13));
    BFC_ASSERT(compare_sizes(Page::sizeof_persistent_header, 12));

    BFC_ASSERT(compare_sizes(OFFSETOF(PBtreeNode, _entries), 28));
    Page page;
    LocalDatabase db((Environment *)m_env, 1, 0);
    BtreeIndex be(&db, 0);

    page.set_self(1000);
    page.set_db(&db);
    db.set_btree(&be);
    be.set_keysize(666);
    BFC_ASSERT(compare_sizes(Page::sizeof_persistent_header, 12));
    // make sure the 'header page' is at least as large as your usual
    // header page, then hack it...
    struct {
      PageData drit;
      PEnvHeader drat;
    } hdrpage_pers = {{{0}}};
    Page hdrpage;
    hdrpage.set_pers((PageData *)&hdrpage_pers);
    Page *hp = &hdrpage;
    ham_u8_t *pl1 = hp->get_payload();
    BFC_ASSERT(pl1);
    BFC_ASSERT(compare_sizes(pl1 - (ham_u8_t *)hdrpage.get_pers(), 12));
    PEnvHeader *hdrptr = (PEnvHeader *)(hdrpage.get_payload());
    BFC_ASSERT(compare_sizes(((ham_u8_t *)hdrptr)
        - (ham_u8_t *)hdrpage.get_pers(), 12));
    hdrpage.set_pers(0);
  }

};

class DbInMemoryTest : public DbTest {
public:
  DbInMemoryTest()
    : DbTest(true, "DbInMemoryTest") {
    clear_tests(); // don't inherit tests
    testrunner::get_instance()->register_fixture(this);
    BFC_REGISTER_TEST(DbInMemoryTest, checkStructurePackingTest);
    BFC_REGISTER_TEST(DbInMemoryTest, headerTest);
    BFC_REGISTER_TEST(DbInMemoryTest, structureTest);
    BFC_REGISTER_TEST(DbInMemoryTest, envStructureTest);
    BFC_REGISTER_TEST(DbInMemoryTest, defaultCompareTest);
    BFC_REGISTER_TEST(DbInMemoryTest, defaultPrefixCompareTest);
    BFC_REGISTER_TEST(DbInMemoryTest, allocPageTest);
  }
};

BFC_REGISTER_FIXTURE(DbTest);
BFC_REGISTER_FIXTURE(DbInMemoryTest);
