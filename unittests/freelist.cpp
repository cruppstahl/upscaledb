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

using namespace hamsterdb;

#define CHUNKSIZE Freelist::kBlobAlignment

struct FreelistFixture {
  ham_env_t *m_env;
  ham_db_t *m_db;
  Freelist *m_freelist;

  FreelistFixture()
    : m_env(0), m_db(0), m_freelist(0) {
    setup();
  }

  ~FreelistFixture() {
    teardown();
  }

  ham_status_t open(ham_u32_t flags) {
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
    m_freelist = ((Environment *)m_env)->get_page_manager()->get_freelist(0);
    return (0);
  }

  void setup() {
    ham_parameter_t p[] = {
      { HAM_PARAM_PAGESIZE, 4096 },
      { 0, 0 }
    };

    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"),
            HAM_ENABLE_TRANSACTIONS, 0644, &p[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
    m_freelist = ((Environment *)m_env)->get_page_manager()->get_freelist(0);
  }

  void teardown() {
    /* need to clear the changeset, otherwise ham_db_close() will complain */
    if (m_env) {
      ((Environment *)m_env)->get_changeset().clear();
      REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
    }
  }

  void structureTest() {
    PFreelistPayload *f;

    f = (((Environment *)m_env)->get_freelist_payload());

    REQUIRE(0ull == freel_get_overflow(f));
    freel_set_overflow(f, 0x12345678ull);

    freel_set_start_address(f, 0x7878787878787878ull);
    REQUIRE(0x7878787878787878ull == freel_get_start_address(f));
    REQUIRE(0x12345678ull == freel_get_overflow(f));

    // reopen the database, check if the values were stored correctly
    ((Environment *)m_env)->set_dirty(true);

    REQUIRE(0 == open(0));
    f = (((Environment *)m_env)->get_freelist_payload());

    REQUIRE(0x7878787878787878ull == freel_get_start_address(f));
    REQUIRE(0x12345678ull == freel_get_overflow(f));
  }

  void markAllocPageTest() {
    ham_size_t ps = ((Environment *)m_env)->get_pagesize();
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    for (int i = 0; i < 10; i++) {
      REQUIRE(0 ==
          m_freelist->free_area(ps + i * CHUNKSIZE, CHUNKSIZE));
    }

    for (int i = 0; i < 10; i++) {
      ham_u64_t o;
      REQUIRE(0 ==
          m_freelist->alloc_area(CHUNKSIZE, &o));
      REQUIRE((ham_u64_t)(ps + i * CHUNKSIZE) == o);
    }

    ham_u64_t o;
    REQUIRE(0 ==
        m_freelist->alloc_area(CHUNKSIZE, &o));
    REQUIRE((ham_u64_t)0 == o);
    REQUIRE(((Environment *)m_env)->is_dirty());
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void markAllocAlignedTest() {
    ham_size_t ps = ((Environment *)m_env)->get_pagesize();
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    REQUIRE(0 ==
          m_freelist->free_area(ps, ps));
    ham_u64_t o;
    REQUIRE(0 == m_freelist->alloc_page(&o));
    REQUIRE((ham_u64_t)ps == o);
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void markAllocHighOffsetTest() {
    ham_size_t ps = ((Environment *)m_env)->get_pagesize();
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    for (int i = 60; i < 70; i++) {
      REQUIRE(0 ==
          m_freelist->free_area(ps + i * CHUNKSIZE, CHUNKSIZE));
    }

    for (int i = 60; i < 70; i++) {
      ham_u64_t o;
      REQUIRE(0 == m_freelist->alloc_area(CHUNKSIZE, &o));
      REQUIRE(o == (ham_u64_t)ps + i * CHUNKSIZE);
    }

    ham_u64_t o;
    REQUIRE(0 ==
          m_freelist->alloc_area(CHUNKSIZE, &o));
    REQUIRE((ham_u64_t)0 == o);
    REQUIRE(true == ((Environment *)m_env)->is_dirty());
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void markAllocRangeTest() {
    ham_size_t ps = ((Environment *)m_env)->get_pagesize();
    ham_u64_t offset = ps;
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    for (int i = 60; i < 70; i++) {
      REQUIRE(0 == m_freelist->free_area(offset, (i + 1) * CHUNKSIZE));
      offset += (i + 1) * CHUNKSIZE;
    }

    offset = ps;
    for (int i = 60; i < 70; i++) {
      ham_u64_t o;
      REQUIRE(0 ==
          m_freelist->alloc_area((i + 1) * CHUNKSIZE, &o));
      REQUIRE((ham_u64_t)offset == o);
      offset += (i + 1) * CHUNKSIZE;
    }

    ham_u64_t o;
    REQUIRE(0 == m_freelist->alloc_area(CHUNKSIZE, &o));
    REQUIRE(true == ((Environment *)m_env)->is_dirty());
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void markAllocOverflowTest() {
    ham_u64_t o = ((Environment *)m_env)->get_usable_pagesize()
        * 8 * CHUNKSIZE;
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    REQUIRE(0 ==
         m_freelist->free_area(o, CHUNKSIZE));
    REQUIRE(0 == ham_txn_commit(txn, 0));

    /* need to clear the changeset, otherwise ham_db_close() will complain */
    ((Environment *)m_env)->get_changeset().clear();
    REQUIRE(0 == open(HAM_ENABLE_TRANSACTIONS));
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    ham_u64_t addr;
    REQUIRE(0 ==
        m_freelist->alloc_area(CHUNKSIZE, &addr));
    REQUIRE(o == addr);
    REQUIRE(0 ==
        m_freelist->alloc_area(CHUNKSIZE, &addr));
    REQUIRE((ham_u64_t)0 == addr);

    REQUIRE(0 ==
         m_freelist->free_area(o * 2, CHUNKSIZE));

    REQUIRE(0 == ham_txn_commit(txn, 0));
    /* need to clear the changeset, otherwise ham_db_close() will complain */
    ((Environment *)m_env)->get_changeset().clear();
    REQUIRE(0 == open(HAM_ENABLE_TRANSACTIONS));
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    REQUIRE(0 ==
        m_freelist->alloc_area(CHUNKSIZE, &addr));
    REQUIRE(addr == o * 2);
    REQUIRE(0 ==
        m_freelist->alloc_area(CHUNKSIZE, &addr));
    REQUIRE((ham_u64_t)0 == addr);
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void markAllocOverflow2Test() {
    ham_u64_t o = ((Environment *)m_env)->get_usable_pagesize()
        * 8 * CHUNKSIZE;
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    REQUIRE(0 == m_freelist->free_area(o * 3, CHUNKSIZE));
    ham_u64_t addr;
    REQUIRE(0 == m_freelist->alloc_area(CHUNKSIZE, &addr));
    REQUIRE(addr == 3 * o);
    REQUIRE(true == ((Environment *)m_env)->is_dirty());

    REQUIRE(0 == ham_txn_commit(txn, 0));
    /* need to clear the changeset, otherwise ham_db_close() will complain */
    ((Environment *)m_env)->get_changeset().clear();
    REQUIRE(0 == open(HAM_ENABLE_TRANSACTIONS));
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    REQUIRE(0 == m_freelist->alloc_area(CHUNKSIZE, &addr));
    REQUIRE((ham_u64_t)0 == addr);

    REQUIRE(0 == m_freelist->free_area(o * 10, CHUNKSIZE));
    REQUIRE(0 == m_freelist->alloc_area(CHUNKSIZE, &addr));
    REQUIRE(addr == 10 * o);

    REQUIRE(0 == ham_txn_commit(txn, 0));
    /* need to clear the changeset, otherwise ham_db_close() will complain */
    ((Environment *)m_env)->get_changeset().clear();
    REQUIRE(0 == open(HAM_ENABLE_TRANSACTIONS));
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    REQUIRE(0 == m_freelist->alloc_area(CHUNKSIZE, &addr));
    REQUIRE((ham_u64_t)0 == addr);
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void markAllocOverflow3Test() {
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    // this code snippet crashed in an acceptance test
    REQUIRE(0 == m_freelist->free_area(2036736,
          ((Environment *)m_env)->get_pagesize() - 1024));
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void markAllocAlignTest() {
    ham_u64_t addr;
    ham_size_t ps=((Environment *)m_env)->get_pagesize();
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    REQUIRE(0 == m_freelist->free_area(ps, ps));
    REQUIRE(0 == m_freelist->alloc_page(&addr));
    REQUIRE(ps == addr);
    REQUIRE(0 ==
        m_freelist->alloc_area(CHUNKSIZE, &addr));
    REQUIRE(0ull == addr);
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void markAllocAlignMultipleTest() {
    ham_u64_t addr;
    ham_size_t ps = ((Environment *)m_env)->get_pagesize();
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    REQUIRE(0 == m_freelist->free_area(ps, ps * 2));
    REQUIRE(0 == m_freelist->alloc_page(&addr));
    REQUIRE(addr == (ham_u64_t)ps * 1);
    REQUIRE(0 == m_freelist->alloc_page(&addr));
    REQUIRE(addr == (ham_u64_t)ps * 2);
    REQUIRE(0 == m_freelist->alloc_area(CHUNKSIZE, &addr));
    REQUIRE(addr == (ham_u64_t)0);
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void checkStructurePackingTest() {
    // checks to make sure structure packing by the compiler is still okay
    REQUIRE(sizeof(PFreelistPayload) ==
        (size_t)(16 + 13 + sizeof(PFreelistPageStatistics)));
    REQUIRE(freel_get_bitmap_offset() ==
        16 + 12 + sizeof(PFreelistPageStatistics));
    REQUIRE(sizeof(PFreelistPageStatistics) ==
        8 * 4 + sizeof(PFreelistSlotsizeStats) * HAM_FREELIST_SLOT_SPREAD);
    REQUIRE(sizeof(PFreelistSlotsizeStats) == 8 * 4);
    REQUIRE(HAM_FREELIST_SLOT_SPREAD == 16 - 5 + 1);
  }

  void markAllocTwiceTest() {
    ham_size_t ps = ((Environment *)m_env)->get_pagesize();
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    REQUIRE(0 == m_freelist->free_area(ps, ps));
    REQUIRE(0 == m_freelist->free_area(ps, ps));
    REQUIRE(0 == m_freelist->free_area(ps, ps));
    ham_u64_t o;
    REQUIRE(0 == m_freelist->alloc_page(&o));
    REQUIRE((ham_u64_t)ps == o);
    REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void simpleReopenTest() {
    ham_size_t ps = ((Environment *)m_env)->get_pagesize();
    ham_txn_t *txn;
    REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));
    REQUIRE(0 == m_freelist->free_area(ps, ps));
    REQUIRE(0 == ham_txn_commit(txn, 0));
    REQUIRE(0 == ham_db_close(m_db, 0));
    REQUIRE(0 == open(HAM_ENABLE_TRANSACTIONS));
    ham_u64_t o;
    REQUIRE(0 == m_freelist->alloc_page(&o));
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

