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

#include "../src/db.h"
#include "../src/page.h"
#include "../src/freelist.h"
#include "../src/env.h"
#include "../src/page_manager.h"
#include "../src/blob_manager_disk.h"
#include "../src/device.h"

#define CHUNKSIZE Freelist::kBlobAlignment

namespace hamsterdb {

struct FreelistFixture {
  ham_env_t *m_env;
  ham_db_t *m_db;
  Freelist *m_freelist;
  LocalEnvironment *m_lenv;

  FreelistFixture()
    : m_env(0), m_db(0), m_freelist(0) {
    setup();
  }

  ~FreelistFixture() {
    teardown();
  }

  ham_status_t open(ham_u32_t flags = 0) {
    ham_status_t st;
    st = ham_env_close(m_env, HAM_AUTO_CLEANUP);
    if (st)
      return (st);
    st = ham_env_open(&m_env, Globals::opath(".test"), flags, 0);
    if (st)
      return (st);
    st = ham_env_open_db(m_env, &m_db, 1, 0, 0);
    if (st)
      return (st);
    m_lenv = (LocalEnvironment *)m_env;
    m_freelist = m_lenv->get_page_manager()->test_get_freelist();
    return (0);
  }

  void setup() {
    ham_parameter_t p[] = {
      { HAM_PARAM_PAGESIZE, 4096 },
      { 0, 0 }
    };

    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"),
                HAM_DISABLE_MMAP | HAM_ENABLE_TRANSACTIONS, 0644, &p[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, 0, 0));

    m_lenv = (LocalEnvironment *)m_env;
    m_freelist = m_lenv->get_page_manager()->test_get_freelist();
  }

  void teardown() {
    /* need to clear the changeset, otherwise ham_db_close() will complain */
    if (m_env) {
      m_lenv->get_changeset().clear();
      REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
    }
  }

  void structureTest() {
    open(HAM_DISABLE_RECLAIM_INTERNAL);

    PFreelistPayload *f = (((LocalEnvironment *)m_env)->get_freelist_payload());

    REQUIRE(0ull == f->get_overflow());
    f->set_overflow(0x12345678ull);

    f->set_start_address(0x7878787878787878ull);
    REQUIRE(0x7878787878787878ull == f->get_start_address());
    REQUIRE(0x12345678ull == f->get_overflow());

    // reopen the database, check if the values were stored correctly
    m_lenv->mark_header_page_dirty();

    REQUIRE(0 == open(0));
    f = (((LocalEnvironment *)m_env)->get_freelist_payload());

    REQUIRE(0x7878787878787878ull == f->get_start_address());
    REQUIRE(0x12345678ull == f->get_overflow());
  }

  void markAllocPageTest() {
    ham_u32_t ps = m_lenv->get_page_size();
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    for (int i = 0; i < 10; i++) {
      m_freelist->free_area(ps + i * CHUNKSIZE, CHUNKSIZE);
    }

    for (int i = 0; i < 10; i++) {
      ham_u64_t o = m_freelist->alloc_area(CHUNKSIZE);
      REQUIRE((ham_u64_t)(ps + i * CHUNKSIZE) == o);
    }

    ham_u64_t o = m_freelist->alloc_area(CHUNKSIZE);
    REQUIRE((ham_u64_t)0 == o);
    REQUIRE(m_lenv->get_header()->get_header_page()->is_dirty());
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void freePageTest() {
    ham_u32_t ps = m_lenv->get_page_size();
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    for (int i = 0; i < 10; i++) {
      m_freelist->free_area(ps + i * ps, ps);
    }

    for (int i = 0; i < 10; i++) {
      REQUIRE(true == m_freelist->is_page_free(ps + i * ps));
    }

    for (int i = 0; i < 10; i++) {
      ham_u64_t o = m_freelist->alloc_area(ps);
      REQUIRE((ham_u64_t)(ps + i * ps) == o);
      REQUIRE(false == m_freelist->is_page_free(ps + i * ps));
    }

    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void isFreeTest() {
    ham_u32_t ps = m_lenv->get_page_size();
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    for (int i = 0; i < 3; i++) {
      m_freelist->free_area(ps + i * ps, ps);
    }

    for (int i = 0; i < 3; i++) {
      REQUIRE(true == m_freelist->is_page_free(ps + i * ps));
    }

    ham_u64_t o = m_freelist->alloc_area(CHUNKSIZE);
    REQUIRE((ham_u64_t)(ps) == o);

    REQUIRE(false == m_freelist->is_page_free(ps + 0 * ps));
    REQUIRE(true  == m_freelist->is_page_free(ps + 1 * ps));
    REQUIRE(true  == m_freelist->is_page_free(ps + 2 * ps));

    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void simpleReclaimTest() {
    PageManager *pm = m_lenv->get_page_manager();
    ham_u32_t page_size = m_lenv->get_page_size();
    Page *page;

    REQUIRE((page = pm->alloc_page(0, Page::kTypeFreelist,
                  PageManager::kClearWithZero)));

    // allocate a blob from the freelist - must fail
    ham_u64_t o = m_freelist->alloc_area(CHUNKSIZE);
    REQUIRE(0ull == o);

    pm->add_to_freelist(page);
    REQUIRE(true  == m_freelist->is_page_free(page->get_address()));

    // verify file size
    REQUIRE((ham_u64_t)(page_size * 3) == m_lenv->get_device()->get_filesize());

    // reopen the file
    m_lenv->get_changeset().clear();
    open();

    // should have 1 page less
    REQUIRE((ham_u64_t)(page_size * 2) == m_lenv->get_device()->get_filesize());

    // allocate a blob from the freelist - must fail
    o = m_freelist->alloc_area(CHUNKSIZE);
    REQUIRE(0ull == o);
  }

  void reclaimTest() {
    PageManager *pm = m_lenv->get_page_manager();
    ham_u32_t page_size = m_lenv->get_page_size();
    Page *page[5] = {0};

    // allocate 5 pages
    for (int i = 0; i < 5; i++) {
      REQUIRE((page[i] = pm->alloc_page(0, Page::kTypeFreelist,
                  PageManager::kClearWithZero)));
      REQUIRE(page[i]->get_address() == (2 + i) * page_size);
    }

    // free the last 3 of them and move them to the freelist (and verify with
    // is_page_free)
    for (int i = 2; i < 5; i++) {
      pm->add_to_freelist(page[i]);
      REQUIRE(true  == m_freelist->is_page_free(page[i]->get_address()));
    }
    for (int i = 0; i < 2; i++) {
      REQUIRE(false == m_freelist->is_page_free(page[i]->get_address()));
    }

    // reopen the file
    m_lenv->get_changeset().clear();
    open();

    for (int i = 0; i < 2; i++)
      REQUIRE(false == m_freelist->is_page_free((2 + i) * page_size));

    // verify file size
    REQUIRE((ham_u64_t)(page_size * 4) == m_lenv->get_device()->get_filesize());

    // allocate a new page from the freelist - must fail
    ham_u64_t o = m_freelist->alloc_area(CHUNKSIZE);
    REQUIRE(0ull == o);

    // allocate a new page from disk - must succeed
    Page *p;
    pm = m_lenv->get_page_manager();
    REQUIRE((p = pm->alloc_page(0, Page::kTypeFreelist,
                        PageManager::kClearWithZero)));
    REQUIRE(p->get_address() == 4 * page_size);

    // check file size once more
    m_lenv->get_changeset().clear();
    open();

    // and check file size - must have grown by 1 page
    REQUIRE((ham_u64_t)(5 * page_size) == m_lenv->get_device()->get_filesize());
  }

  void truncateTest() {
    PageManager *pm = m_lenv->get_page_manager();
    ham_u32_t page_size = m_lenv->get_page_size();
    Page *page[5] = {0};

    // allocate 5 pages
    for (int i = 0; i < 5; i++) {
      REQUIRE((page[i] = pm->alloc_page(0, Page::kTypeFreelist,
                  PageManager::kClearWithZero)));
      REQUIRE(page[i]->get_address() == (2 + i) * page_size);
    }

    // free the last 3 of them and move them to the freelist (and verify with
    // is_page_free)
    for (int i = 2; i < 5; i++) {
      pm->add_to_freelist(page[i]);
      REQUIRE(true  == m_freelist->is_page_free(page[i]->get_address()));
    }
    for (int i = 0; i < 2; i++) {
      REQUIRE(false == m_freelist->is_page_free(page[i]->get_address()));
    }

    // truncate the free pages and verify that they are *not longer free*
    for (int i = 2; i < 5; i++) {
      REQUIRE(true  == m_freelist->is_page_free(page[i]->get_address()));
      m_freelist->truncate_page(page[i]->get_address());
      REQUIRE(false == m_freelist->is_page_free((2 + i) * page_size));
    }

    // reopen and check again
    m_lenv->get_changeset().clear();
    open();
    pm = m_lenv->get_page_manager();

    for (int i = 2; i < 5; i++) {
      REQUIRE(false == m_freelist->is_page_free((2 + i) * page_size));
    }

    // freelist is empty
    ham_u64_t o = m_freelist->alloc_area(CHUNKSIZE);
    REQUIRE(0ull == o);

    // allocate the page; this must increase the file size
    ham_u64_t filesize = m_lenv->get_device()->get_filesize();
    Page *p;
    REQUIRE((p = pm->alloc_page(0, Page::kTypeFreelist,
                        PageManager::kClearWithZero)));
    REQUIRE(filesize == p->get_address());

    // and this page is NOT free
    REQUIRE(false == m_freelist->is_page_free(p->get_address()));

    // not even after a part of it was added to the freelist
    pm->add_to_freelist((LocalDatabase *)m_db, p->get_address() + 960, 64);
    REQUIRE(false == m_freelist->is_page_free(p->get_address()));

    m_lenv->get_changeset().clear();
  }

  void markAllocAlignedTest() {
    ham_u32_t ps = m_lenv->get_page_size();
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    m_freelist->free_area(ps, ps);
    ham_u64_t o = m_freelist->alloc_page();
    REQUIRE((ham_u64_t)ps == o);
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void markAllocHighOffsetTest() {
    ham_u32_t ps = m_lenv->get_page_size();
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    for (int i = 60; i < 70; i++) {
      m_freelist->free_area(ps + i * CHUNKSIZE, CHUNKSIZE);
    }

    for (int i = 60; i < 70; i++) {
      ham_u64_t o = m_freelist->alloc_area(CHUNKSIZE);
      REQUIRE(o == (ham_u64_t)ps + i * CHUNKSIZE);
    }

    ham_u64_t o = m_freelist->alloc_area(CHUNKSIZE);
    REQUIRE((ham_u64_t)0 == o);
    REQUIRE(true == m_lenv->get_header()->get_header_page()->is_dirty());
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void markAllocRangeTest() {
    ham_u32_t ps = m_lenv->get_page_size();
    ham_u64_t offset = ps;
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    for (int i = 60; i < 70; i++) {
      m_freelist->free_area(offset, (i + 1) * CHUNKSIZE);
      offset += (i + 1) * CHUNKSIZE;
    }

    offset = ps;
    for (int i = 60; i < 70; i++) {
      ham_u64_t o = m_freelist->alloc_area((i + 1) * CHUNKSIZE);
      REQUIRE((ham_u64_t)offset == o);
      offset += (i + 1) * CHUNKSIZE;
    }

    ham_u64_t o = m_freelist->alloc_area(CHUNKSIZE);
    REQUIRE(0 == o);
    REQUIRE(true == m_lenv->get_header()->get_header_page()->is_dirty());
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void markAllocOverflowTest() {
    ham_u64_t o = m_lenv->get_usable_page_size() * 8 * CHUNKSIZE;
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    m_freelist->free_area(o, CHUNKSIZE);
    REQUIRE(0 == ham_txn_commit(txn, 0));

    /* need to clear the changeset, otherwise ham_db_close() will complain */
    m_lenv->get_changeset().clear();
    REQUIRE(0 == open(HAM_ENABLE_TRANSACTIONS));
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    ham_u64_t addr = m_freelist->alloc_area(CHUNKSIZE);
    REQUIRE(o == addr);
    addr = m_freelist->alloc_area(CHUNKSIZE);
    REQUIRE((ham_u64_t)0 == addr);

    m_freelist->free_area(o * 2, CHUNKSIZE);

    REQUIRE(0 == ham_txn_commit(txn, 0));
    /* need to clear the changeset, otherwise ham_db_close() will complain */
    m_lenv->get_changeset().clear();
    REQUIRE(0 == open(HAM_ENABLE_TRANSACTIONS));
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    addr = m_freelist->alloc_area(CHUNKSIZE);
    REQUIRE(addr == o * 2);
    addr = m_freelist->alloc_area(CHUNKSIZE);
    REQUIRE((ham_u64_t)0 == addr);
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void markAllocOverflow2Test() {
    ham_u64_t o = m_lenv->get_usable_page_size() * 8 * CHUNKSIZE;
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    m_freelist->free_area(o * 3, CHUNKSIZE);
    ham_u64_t addr;
    addr = m_freelist->alloc_area(CHUNKSIZE);
    REQUIRE(addr == 3 * o);
    REQUIRE(true == m_lenv->get_header()->get_header_page()->is_dirty());

    REQUIRE(0 == ham_txn_commit(txn, 0));
    /* need to clear the changeset, otherwise ham_db_close() will complain */
    m_lenv->get_changeset().clear();
    REQUIRE(0 == open(HAM_ENABLE_TRANSACTIONS));
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    addr = m_freelist->alloc_area(CHUNKSIZE);
    REQUIRE((ham_u64_t)0 == addr);

    m_freelist->free_area(o * 10, CHUNKSIZE);
    addr = m_freelist->alloc_area(CHUNKSIZE);
    REQUIRE(addr == 10 * o);

    REQUIRE(0 == ham_txn_commit(txn, 0));
    /* need to clear the changeset, otherwise ham_db_close() will complain */
    m_lenv->get_changeset().clear();
    REQUIRE(0 == open(HAM_ENABLE_TRANSACTIONS));
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    addr = m_freelist->alloc_area(CHUNKSIZE);
    REQUIRE((ham_u64_t)0 == addr);
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void markAllocOverflow3Test() {
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    // this code snippet crashed in an acceptance test
    m_freelist->free_area(2036736, m_lenv->get_page_size() - 1024);
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void markAllocAlignTest() {
    ham_u64_t addr;
    ham_u32_t ps = m_lenv->get_page_size();
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    m_freelist->free_area(ps, ps);
    addr = m_freelist->alloc_page();
    REQUIRE(ps == addr);
    addr = m_freelist->alloc_area(CHUNKSIZE);
    REQUIRE(0ull == addr);
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void markAllocAlignMultipleTest() {
    ham_u64_t addr;
    ham_u32_t ps = m_lenv->get_page_size();
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    m_freelist->free_area(ps, ps * 2);
    addr = m_freelist->alloc_page();
    REQUIRE(addr == (ham_u64_t)ps * 1);
    addr = m_freelist->alloc_page();
    REQUIRE(addr == (ham_u64_t)ps * 2);
    addr = m_freelist->alloc_area(CHUNKSIZE);
    REQUIRE(addr == (ham_u64_t)0);
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void checkStructurePackingTest() {
    // checks to make sure structure packing by the compiler is still okay
    // most relevant structures are already checked in
    // db.cpp checkStructurePackingTest()
    REQUIRE(PFreelistPayload::get_bitmap_offset() == 28);
    REQUIRE(HAM_FREELIST_SLOT_SPREAD == 16 - 5 + 1);
  }

  void markAllocTwiceTest() {
    ham_u32_t ps = m_lenv->get_page_size();
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    m_freelist->free_area(ps, ps);
    m_freelist->free_area(ps, ps);
    m_freelist->free_area(ps, ps);
    ham_u64_t o = m_freelist->alloc_page();
    REQUIRE((ham_u64_t)ps == o);
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void simpleReopenTest() {
    REQUIRE(0 == open(HAM_ENABLE_TRANSACTIONS | HAM_DISABLE_RECLAIM_INTERNAL));
    ham_u32_t ps = m_lenv->get_page_size();
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    m_freelist->free_area(ps, ps);
    REQUIRE(0 == ham_txn_commit(txn, 0));
    REQUIRE(0 == ham_db_close(m_db, 0));
    REQUIRE(0 == open(HAM_ENABLE_TRANSACTIONS | HAM_DISABLE_RECLAIM_INTERNAL));
    ham_u64_t o = m_freelist->alloc_page();
    REQUIRE((ham_u64_t)ps == o);
  }
};


TEST_CASE("Freelist/checkStructurePackingTest", "")
{
  FreelistFixture f;
  f.checkStructurePackingTest();
}

TEST_CASE("Freelist/structureTest", "")
{
  FreelistFixture f;
  f.structureTest();
}

TEST_CASE("Freelist/markAllocAlignedTest", "")
{
  FreelistFixture f;
  f.markAllocAlignedTest();
}

TEST_CASE("Freelist/markAllocPageTest", "")
{
  FreelistFixture f;
  f.markAllocPageTest();
}

TEST_CASE("Freelist/freePageTest", "")
{
  FreelistFixture f;
  f.freePageTest();
}

TEST_CASE("Freelist/isFreeTest", "")
{
  FreelistFixture f;
  f.isFreeTest();
}

TEST_CASE("Freelist/markAllocHighOffsetTest", "")
{
  FreelistFixture f;
  f.markAllocHighOffsetTest();
}

TEST_CASE("Freelist/markAllocRangeTest", "")
{
  FreelistFixture f;
  f.markAllocRangeTest();
}

TEST_CASE("Freelist/markAllocOverflowTest", "")
{
  FreelistFixture f;
  f.markAllocOverflowTest();
}

TEST_CASE("Freelist/markAllocOverflow2Test", "")
{
  FreelistFixture f;
  f.markAllocOverflow2Test();
}

TEST_CASE("Freelist/markAllocOverflow3Test", "")
{
  FreelistFixture f;
  f.markAllocOverflow3Test();
}

TEST_CASE("Freelist/markAllocAlignTest", "")
{
  FreelistFixture f;
  f.markAllocAlignTest();
}

TEST_CASE("Freelist/markAllocAlignMultipleTest", "")
{
  FreelistFixture f;
  f.markAllocAlignMultipleTest();
}

TEST_CASE("Freelist/markAllocTwiceTest", "")
{
  FreelistFixture f;
  f.markAllocTwiceTest();
}

TEST_CASE("Freelist/simpleReopenTest", "")
{
  FreelistFixture f;
  f.simpleReopenTest();
}

TEST_CASE("Freelist/simpleReclaimTest", "")
{
  FreelistFixture f;
  f.simpleReclaimTest();
}

TEST_CASE("Freelist/reclaimTest", "")
{
  FreelistFixture f;
  f.reclaimTest();
}

TEST_CASE("Freelist/truncateTest", "")
{
  FreelistFixture f;
  f.truncateTest();
}

} // namespace hamsterdb
