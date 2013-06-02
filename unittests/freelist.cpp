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

#include <stdlib.h>

#include <ham/hamsterdb.h>

#include "../src/db.h"
#include "../src/page.h"
#include "../src/full_freelist.h"
#include "../src/env.h"
#include "../src/page_manager.h"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;
using namespace hamsterdb;

class FullFreelistTest : public hamsterDB_fixture {
  define_super(hamsterDB_fixture);

public:
  FullFreelistTest()
    : hamsterDB_fixture("FullFreelistTest"), m_env(0), m_db(0),
      m_freelist(0) {
    testrunner::get_instance()->register_fixture(this);
    BFC_REGISTER_TEST(FullFreelistTest, checkStructurePackingTest);
    BFC_REGISTER_TEST(FullFreelistTest, structureTest);
    BFC_REGISTER_TEST(FullFreelistTest, markAllocAlignedTest);
    BFC_REGISTER_TEST(FullFreelistTest, markAllocPageTest);
    BFC_REGISTER_TEST(FullFreelistTest, markAllocHighOffsetTest);
    BFC_REGISTER_TEST(FullFreelistTest, markAllocRangeTest);
    BFC_REGISTER_TEST(FullFreelistTest, markAllocOverflowTest);
    BFC_REGISTER_TEST(FullFreelistTest, markAllocOverflow2Test);
    BFC_REGISTER_TEST(FullFreelistTest, markAllocOverflow3Test);
    BFC_REGISTER_TEST(FullFreelistTest, markAllocAlignTest);
    BFC_REGISTER_TEST(FullFreelistTest, markAllocAlignMultipleTest);
    BFC_REGISTER_TEST(FullFreelistTest, markAllocTwiceTest);
    BFC_REGISTER_TEST(FullFreelistTest, simpleReopenTest);
  }

protected:
  ham_env_t *m_env;
  ham_db_t *m_db;
  FullFreelist *m_freelist;

public:
  virtual ham_status_t open(ham_u32_t flags) {
    ham_status_t st;
    st = ham_env_close(m_env, HAM_AUTO_CLEANUP);
    if (st)
      return (st);
    st = ham_env_open(&m_env, BFC_OPATH(".test"), flags, 0);
    if (st)
      return (st);
    st = ham_env_open_db(m_env, &m_db, 1, 0, 0);
    if (st)
      return (st);
    m_freelist = ((Environment *)m_env)->get_page_manager()->get_freelist();
    return (0);
  }

  virtual void setup() {
    __super::setup();

    ham_parameter_t p[] = {
      { HAM_PARAM_PAGESIZE, 4096 },
      { 0, 0 }
    };

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
            HAM_ENABLE_TRANSACTIONS, 0644, &p[0]));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
    m_freelist = ((Environment *)m_env)->get_page_manager()->get_freelist();
  }

  virtual void teardown() {
    __super::teardown();

    /* need to clear the changeset, otherwise ham_db_close() will complain */
    if (m_env) {
      ((Environment *)m_env)->get_changeset().clear();
      BFC_ASSERT_EQUAL(0, ham_env_close(m_env, HAM_AUTO_CLEANUP));
    }
  }

  void structureTest() {
    PFullFreelistPayload *f;

    f = (((Environment *)m_env)->get_freelist_payload());

    BFC_ASSERT_EQUAL(0ull, freel_get_overflow(f));
    freel_set_overflow(f, 0x12345678ull);

    freel_set_start_address(f, 0x7878787878787878ull);
    BFC_ASSERT_EQUAL(0x7878787878787878ull, freel_get_start_address(f));
    BFC_ASSERT_EQUAL(0x12345678ull, freel_get_overflow(f));

    // reopen the database, check if the values were stored correctly
    ((Environment *)m_env)->set_dirty(true);

    BFC_ASSERT_EQUAL(0, open(0));
    f = (((Environment *)m_env)->get_freelist_payload());

    BFC_ASSERT_EQUAL(0x7878787878787878ull, freel_get_start_address(f));
    BFC_ASSERT_EQUAL(0x12345678ull, freel_get_overflow(f));
  }

  void markAllocPageTest() {
    ham_size_t ps = ((Environment *)m_env)->get_pagesize();
    ham_txn_t *txn;
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    for (int i = 0; i < 10; i++) {
      BFC_ASSERT_EQUAL(0,
          m_freelist->free_area(ps + i * DB_CHUNKSIZE, DB_CHUNKSIZE));
    }

    for (int i = 0; i < 10; i++) {
      ham_u64_t o;
      BFC_ASSERT_EQUAL(0,
          m_freelist->alloc_area(&o, DB_CHUNKSIZE));
      BFC_ASSERT_EQUAL((ham_u64_t)(ps + i * DB_CHUNKSIZE), o);
    }

    ham_u64_t o;
    BFC_ASSERT_EQUAL(0,
        m_freelist->alloc_area(&o, DB_CHUNKSIZE));
    BFC_ASSERT_EQUAL((ham_u64_t)0, o);
    BFC_ASSERT(((Environment *)m_env)->is_dirty());
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void markAllocAlignedTest() {
    ham_size_t ps = ((Environment *)m_env)->get_pagesize();
    ham_txn_t *txn;
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    BFC_ASSERT_EQUAL(0,
          m_freelist->free_area(ps, ps));
    ham_u64_t o;
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_page(&o));
    BFC_ASSERT_EQUAL((ham_u64_t)ps, o);
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void markAllocHighOffsetTest() {
    ham_size_t ps = ((Environment *)m_env)->get_pagesize();
    ham_txn_t *txn;
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    for (int i = 60; i < 70; i++) {
      BFC_ASSERT_EQUAL(0,
          m_freelist->free_area(ps + i * DB_CHUNKSIZE, DB_CHUNKSIZE));
    }

    for (int i = 60; i < 70; i++) {
      ham_u64_t o;
      BFC_ASSERT_EQUAL(0,
          m_freelist->alloc_area(&o, DB_CHUNKSIZE));
      BFC_ASSERT_EQUAL((ham_u64_t)ps + i * DB_CHUNKSIZE, o);
    }

    ham_u64_t o;
    BFC_ASSERT_EQUAL(0,
          m_freelist->alloc_area(&o, DB_CHUNKSIZE));
    BFC_ASSERT_EQUAL((ham_u64_t)0, o);
    BFC_ASSERT(((Environment *)m_env)->is_dirty());
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void markAllocRangeTest() {
    ham_size_t ps = ((Environment *)m_env)->get_pagesize();
    ham_u64_t offset = ps;
    ham_txn_t *txn;
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    for (int i = 60; i < 70; i++) {
      BFC_ASSERT_EQUAL_I(0,
          m_freelist->free_area(offset, (i + 1) * DB_CHUNKSIZE), i);
      offset += (i + 1) * DB_CHUNKSIZE;
    }

    offset = ps;
    for (int i = 60; i < 70; i++) {
      ham_u64_t o;
      BFC_ASSERT_EQUAL(0,
          m_freelist->alloc_area(&o, (i+1)*DB_CHUNKSIZE));
      BFC_ASSERT_EQUAL((ham_u64_t)offset, o);
      offset += (i + 1) * DB_CHUNKSIZE;
    }

    ham_u64_t o;
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_area(&o, DB_CHUNKSIZE));
    BFC_ASSERT(((Environment *)m_env)->is_dirty());
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void markAllocOverflowTest() {
    ham_u64_t o = ((Environment *)m_env)->get_usable_pagesize()
        * 8 * DB_CHUNKSIZE;
    ham_txn_t *txn;
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    BFC_ASSERT_EQUAL(0,
         m_freelist->free_area(o, DB_CHUNKSIZE));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));

    /* need to clear the changeset, otherwise ham_db_close() will complain */
    ((Environment *)m_env)->get_changeset().clear();
    BFC_ASSERT_EQUAL(0, open(HAM_ENABLE_TRANSACTIONS));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    ham_u64_t addr;
    BFC_ASSERT_EQUAL(0,
        m_freelist->alloc_area(&addr, DB_CHUNKSIZE));
    BFC_ASSERT_EQUAL(o, addr);
    BFC_ASSERT_EQUAL(0,
        m_freelist->alloc_area(&addr, DB_CHUNKSIZE));
    BFC_ASSERT_EQUAL((ham_u64_t)0, addr);

    BFC_ASSERT_EQUAL(0,
         m_freelist->free_area(o * 2, DB_CHUNKSIZE));

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    /* need to clear the changeset, otherwise ham_db_close() will complain */
    ((Environment *)m_env)->get_changeset().clear();
    BFC_ASSERT_EQUAL(0, open(HAM_ENABLE_TRANSACTIONS));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    BFC_ASSERT_EQUAL(0,
        m_freelist->alloc_area(&addr, DB_CHUNKSIZE));
    BFC_ASSERT_EQUAL(o*2, addr);
    BFC_ASSERT_EQUAL(0,
        m_freelist->alloc_area(&addr, DB_CHUNKSIZE));
    BFC_ASSERT_EQUAL((ham_u64_t)0, addr);
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void markAllocOverflow2Test() {
    ham_u64_t o = ((Environment *)m_env)->get_usable_pagesize()
        * 8 * DB_CHUNKSIZE;
    ham_txn_t *txn;
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    BFC_ASSERT_EQUAL(0,
         m_freelist->free_area(o * 3, DB_CHUNKSIZE));
    ham_u64_t addr;
    BFC_ASSERT_EQUAL(0,
        m_freelist->alloc_area(&addr, DB_CHUNKSIZE));
    BFC_ASSERT_EQUAL(3 * o, addr);
    BFC_ASSERT(((Environment *)m_env)->is_dirty());

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    /* need to clear the changeset, otherwise ham_db_close() will complain */
    ((Environment *)m_env)->get_changeset().clear();
    BFC_ASSERT_EQUAL(0, open(HAM_ENABLE_TRANSACTIONS));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    BFC_ASSERT_EQUAL(0,
        m_freelist->alloc_area(&addr, DB_CHUNKSIZE));
    BFC_ASSERT_EQUAL((ham_u64_t)0, addr);

    BFC_ASSERT_EQUAL(0,
         m_freelist->free_area(o * 10, DB_CHUNKSIZE));
    BFC_ASSERT_EQUAL(0,
        m_freelist->alloc_area(&addr, DB_CHUNKSIZE));
    BFC_ASSERT_EQUAL(10 * o, addr);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    /* need to clear the changeset, otherwise ham_db_close() will complain */
    ((Environment *)m_env)->get_changeset().clear();
    BFC_ASSERT_EQUAL(0, open(HAM_ENABLE_TRANSACTIONS));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    BFC_ASSERT_EQUAL(0,
        m_freelist->alloc_area(&addr, DB_CHUNKSIZE));
    BFC_ASSERT_EQUAL((ham_u64_t)0, addr);
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void markAllocOverflow3Test() {
    ham_txn_t *txn;
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    // this code snippet crashed in an acceptance test
    BFC_ASSERT_EQUAL(0, m_freelist->free_area(2036736,
          ((Environment *)m_env)->get_pagesize() - 1024));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void markAllocAlignTest() {
    ham_u64_t addr;
    ham_size_t ps=((Environment *)m_env)->get_pagesize();
    ham_txn_t *txn;
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    BFC_ASSERT_EQUAL(0, m_freelist->free_area(ps, ps));
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_page(&addr));
    BFC_ASSERT_EQUAL(ps, addr);
    BFC_ASSERT_EQUAL(0,
        m_freelist->alloc_area(&addr, DB_CHUNKSIZE));
    BFC_ASSERT_EQUAL(0ull, addr);
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void markAllocAlignMultipleTest() {
    ham_u64_t addr;
    ham_size_t ps = ((Environment *)m_env)->get_pagesize();
    ham_txn_t *txn;
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    BFC_ASSERT_EQUAL(0, m_freelist->free_area(ps, ps * 2));
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_page(&addr));
    BFC_ASSERT_EQUAL((ham_u64_t)ps * 1, addr);
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_page(&addr));
    BFC_ASSERT_EQUAL((ham_u64_t)ps * 2, addr);
    BFC_ASSERT_EQUAL(0,
        m_freelist->alloc_area(&addr, DB_CHUNKSIZE));
    BFC_ASSERT_EQUAL((ham_u64_t)0, addr);
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  // using a function to compare the constants is easier for debugging
  bool compare_sizes(size_t a, size_t b) {
    return a == b;
  }

  void checkStructurePackingTest() {
    // checks to make sure structure packing by the compiler is still okay
    BFC_ASSERT(compare_sizes(sizeof(PFullFreelistPayload),
        16 + 13 + sizeof(PFreelistPageStatistics)));
    BFC_ASSERT(compare_sizes(freel_get_bitmap_offset(),
        16 + 12 + sizeof(PFreelistPageStatistics)));
    BFC_ASSERT(compare_sizes(sizeof(PFreelistPageStatistics),
        8 * 4 + sizeof(PFreelistSlotsizeStats)
            * HAM_FREELIST_SLOT_SPREAD));
    BFC_ASSERT(compare_sizes(sizeof(PFreelistSlotsizeStats), 8 * 4));
    BFC_ASSERT(compare_sizes(HAM_FREELIST_SLOT_SPREAD, 16 - 5 + 1));
  }

  void markAllocTwiceTest() {
    ham_size_t ps = ((Environment *)m_env)->get_pagesize();
    ham_txn_t *txn;
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    BFC_ASSERT_EQUAL(0, m_freelist->free_area(ps, ps));
    BFC_ASSERT_EQUAL(0, m_freelist->free_area(ps, ps));
    BFC_ASSERT_EQUAL(0, m_freelist->free_area(ps, ps));
    ham_u64_t o;
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_page(&o));
    BFC_ASSERT_EQUAL((ham_u64_t)ps, o);
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void simpleReopenTest() {
    ham_size_t ps = ((Environment *)m_env)->get_pagesize();
    ham_txn_t *txn;
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
    BFC_ASSERT_EQUAL(0, m_freelist->free_area(ps, ps));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    BFC_ASSERT_EQUAL(0, ham_db_close(m_db, 0));
    BFC_ASSERT_EQUAL(0, open(HAM_ENABLE_TRANSACTIONS));
    ham_u64_t o;
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_page(&o));
    BFC_ASSERT_EQUAL((ham_u64_t)ps, o);
  }
};

BFC_REGISTER_FIXTURE(FullFreelistTest);

