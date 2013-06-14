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
#include "../src/blob_manager_disk.h"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;
using namespace hamsterdb;

#define CHUNKSIZE 32

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
  Freelist *m_freelist;

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
    m_freelist = ((Environment *)m_env)->get_page_manager()->get_freelist(0);
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
    m_freelist = ((Environment *)m_env)->get_page_manager()->get_freelist(0);
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
          m_freelist->free_area(ps + i * CHUNKSIZE, CHUNKSIZE));
    }

    for (int i = 0; i < 10; i++) {
      ham_u64_t o;
      BFC_ASSERT_EQUAL(0,
          m_freelist->alloc_area(CHUNKSIZE, &o));
      BFC_ASSERT_EQUAL((ham_u64_t)(ps + i * CHUNKSIZE), o);
    }

    ham_u64_t o;
    BFC_ASSERT_EQUAL(0,
        m_freelist->alloc_area(CHUNKSIZE, &o));
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
          m_freelist->free_area(ps + i * CHUNKSIZE, CHUNKSIZE));
    }

    for (int i = 60; i < 70; i++) {
      ham_u64_t o;
      BFC_ASSERT_EQUAL(0,
          m_freelist->alloc_area(CHUNKSIZE, &o));
      BFC_ASSERT_EQUAL((ham_u64_t)ps + i * CHUNKSIZE, o);
    }

    ham_u64_t o;
    BFC_ASSERT_EQUAL(0,
          m_freelist->alloc_area(CHUNKSIZE, &o));
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
          m_freelist->free_area(offset, (i + 1) * CHUNKSIZE), i);
      offset += (i + 1) * CHUNKSIZE;
    }

    offset = ps;
    for (int i = 60; i < 70; i++) {
      ham_u64_t o;
      BFC_ASSERT_EQUAL(0,
          m_freelist->alloc_area((i + 1) * CHUNKSIZE, &o));
      BFC_ASSERT_EQUAL((ham_u64_t)offset, o);
      offset += (i + 1) * CHUNKSIZE;
    }

    ham_u64_t o;
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_area(CHUNKSIZE, &o));
    BFC_ASSERT(((Environment *)m_env)->is_dirty());
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void markAllocOverflowTest() {
    ham_u64_t o = ((Environment *)m_env)->get_usable_pagesize()
        * 8 * CHUNKSIZE;
    ham_txn_t *txn;
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    BFC_ASSERT_EQUAL(0,
         m_freelist->free_area(o, CHUNKSIZE));
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));

    /* need to clear the changeset, otherwise ham_db_close() will complain */
    ((Environment *)m_env)->get_changeset().clear();
    BFC_ASSERT_EQUAL(0, open(HAM_ENABLE_TRANSACTIONS));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    ham_u64_t addr;
    BFC_ASSERT_EQUAL(0,
        m_freelist->alloc_area(CHUNKSIZE, &addr));
    BFC_ASSERT_EQUAL(o, addr);
    BFC_ASSERT_EQUAL(0,
        m_freelist->alloc_area(CHUNKSIZE, &addr));
    BFC_ASSERT_EQUAL((ham_u64_t)0, addr);

    BFC_ASSERT_EQUAL(0,
         m_freelist->free_area(o * 2, CHUNKSIZE));

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    /* need to clear the changeset, otherwise ham_db_close() will complain */
    ((Environment *)m_env)->get_changeset().clear();
    BFC_ASSERT_EQUAL(0, open(HAM_ENABLE_TRANSACTIONS));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    BFC_ASSERT_EQUAL(0,
        m_freelist->alloc_area(CHUNKSIZE, &addr));
    BFC_ASSERT_EQUAL(o * 2, addr);
    BFC_ASSERT_EQUAL(0,
        m_freelist->alloc_area(CHUNKSIZE, &addr));
    BFC_ASSERT_EQUAL((ham_u64_t)0, addr);
    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
  }

  void markAllocOverflow2Test() {
    ham_u64_t o = ((Environment *)m_env)->get_usable_pagesize()
        * 8 * CHUNKSIZE;
    ham_txn_t *txn;
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    BFC_ASSERT_EQUAL(0,
         m_freelist->free_area(o * 3, CHUNKSIZE));
    ham_u64_t addr;
    BFC_ASSERT_EQUAL(0,
        m_freelist->alloc_area(CHUNKSIZE, &addr));
    BFC_ASSERT_EQUAL(3 * o, addr);
    BFC_ASSERT(((Environment *)m_env)->is_dirty());

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    /* need to clear the changeset, otherwise ham_db_close() will complain */
    ((Environment *)m_env)->get_changeset().clear();
    BFC_ASSERT_EQUAL(0, open(HAM_ENABLE_TRANSACTIONS));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    BFC_ASSERT_EQUAL(0,
        m_freelist->alloc_area(CHUNKSIZE, &addr));
    BFC_ASSERT_EQUAL((ham_u64_t)0, addr);

    BFC_ASSERT_EQUAL(0,
         m_freelist->free_area(o * 10, CHUNKSIZE));
    BFC_ASSERT_EQUAL(0,
        m_freelist->alloc_area(CHUNKSIZE, &addr));
    BFC_ASSERT_EQUAL(10 * o, addr);

    BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    /* need to clear the changeset, otherwise ham_db_close() will complain */
    ((Environment *)m_env)->get_changeset().clear();
    BFC_ASSERT_EQUAL(0, open(HAM_ENABLE_TRANSACTIONS));
    BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    BFC_ASSERT_EQUAL(0,
        m_freelist->alloc_area(CHUNKSIZE, &addr));
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
        m_freelist->alloc_area(CHUNKSIZE, &addr));
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
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_area(CHUNKSIZE, &addr));
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

class ReducedFreelistTest : public hamsterDB_fixture
{
  define_super(hamsterDB_fixture);

public:
  ReducedFreelistTest()
    : hamsterDB_fixture("ReducedFreelistTest"), m_env(0), m_db(0),
      m_pagesize(4096), m_freelist(0) {
    testrunner::get_instance()->register_fixture(this);
    BFC_REGISTER_TEST(ReducedFreelistTest, alignmentTest);
    BFC_REGISTER_TEST(ReducedFreelistTest, simplePageTest);
    BFC_REGISTER_TEST(ReducedFreelistTest, simpleBlobTest);
    BFC_REGISTER_TEST(ReducedFreelistTest, collapsePageTest);
    BFC_REGISTER_TEST(ReducedFreelistTest, collapseMultipageBlobTest);
    BFC_REGISTER_TEST(ReducedFreelistTest, simplePersistentTest);
    BFC_REGISTER_TEST(ReducedFreelistTest, bigBlobTest);
    BFC_REGISTER_TEST(ReducedFreelistTest, blobFreeTest);
    BFC_REGISTER_TEST(ReducedFreelistTest, pageFreeTest);
  }

protected:
  ham_env_t *m_env;
  ham_db_t *m_db;
  ham_u32_t m_pagesize;
  ReducedFreelist *m_freelist;

public:
  virtual void setup() {
    ham_parameter_t ps[] = {
      { HAM_PARAM_PAGESIZE, m_pagesize },
      { 0, 0 }
    };

    ham_parameter_t fp[] = {
      { HAM_PARAM_FREELIST_POLICY, HAM_PARAM_FREELIST_POLICY_REDUCED },
      { 0, 0 }
    };

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
            HAM_ENABLE_TRANSACTIONS, 0644, &ps[0]));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, &fp[0]));
    m_freelist = (ReducedFreelist *)
        ((Environment *)m_env)->get_page_manager()->get_freelist((Database *)m_db);
  }

  virtual void teardown() {
    BFC_ASSERT_EQUAL(0, ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void reopen() {
    BFC_ASSERT_EQUAL(0, ham_env_close(m_env, HAM_AUTO_CLEANUP));
    BFC_ASSERT_EQUAL(0,
        ham_env_open(&m_env, BFC_OPATH(".test"), HAM_ENABLE_TRANSACTIONS, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_open_db(m_env, &m_db, 1, 0, 0));
    m_freelist = (ReducedFreelist *)
        ((Environment *)m_env)->get_page_manager()->get_freelist((Database *)m_db);
  }

  void alignmentTest() {
    PageManager *pm = ((Environment *)m_env)->get_page_manager();
   
    BFC_ASSERT_EQUAL(1, pm->get_blob_alignment((Database *)m_db));
    BFC_ASSERT_EQUAL(32, pm->get_blob_alignment(0));
  }

  void simplePageTest() {
    Page *page = new Page((Environment *)m_env);
    BFC_ASSERT_EQUAL(0, page->allocate());

    ham_u64_t o, pid;
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_page(&o));
    BFC_ASSERT_EQUAL(0ull, o);

    pid = page->get_self();
    m_freelist->free_page(page);
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_page(&o));
    BFC_ASSERT_EQUAL(pid, o);

    BFC_ASSERT_EQUAL(0, m_freelist->alloc_page(&o));
    BFC_ASSERT_EQUAL(0ull, o);

    page->free();
    delete page;

    ((Environment *)m_env)->get_changeset().clear();
  }

  void simpleBlobTest() {
    ham_u64_t o;
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_area(10, &o));
    BFC_ASSERT_EQUAL(0ull, o);

    BFC_ASSERT_EQUAL(0, m_freelist->free_area(10000, 500));
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_area(1000, &o));
    BFC_ASSERT_EQUAL(0ull, o);
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_area(501, &o));
    BFC_ASSERT_EQUAL(0ull, o);
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_area(500, &o));
    BFC_ASSERT_EQUAL(10000ull, o);
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_area(500, &o));
    BFC_ASSERT_EQUAL(0ull, o);

    BFC_ASSERT_EQUAL(0, m_freelist->free_area(10000, 500));
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_area(250, &o));
    BFC_ASSERT_EQUAL(10000ull, o);
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_area(200, &o));
    BFC_ASSERT_EQUAL(10250ull, o);
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_area(50, &o));
    BFC_ASSERT_EQUAL(10450ull, o);
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_area(50, &o));
    BFC_ASSERT_EQUAL(0ull, o);
  }

  void collapsePageTest() {
    Page *page = new Page((Environment *)m_env);
    BFC_ASSERT_EQUAL(0, page->allocate());
    page->set_type(Page::TYPE_BLOB);

    ham_u64_t o, pid = page->get_self();
    m_freelist->free_area(pid, 50);
    m_freelist->free_area(pid + 50, 50);
    m_freelist->free_area(pid + 100, 50);
    m_freelist->free_page(page);

    BFC_ASSERT_EQUAL(0, m_freelist->alloc_page(&o));
    BFC_ASSERT_EQUAL(pid, o);
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_area(50, &o));
    BFC_ASSERT_EQUAL(0ull, o);

    page->free();
    delete page;
  }

  void collapseMultipageBlobTest() {
    Page *page = new Page((Environment *)m_env);
    BFC_ASSERT_EQUAL(0, page->allocate());
    page->set_type(Page::TYPE_BLOB);

    ham_u64_t o, pid = page->get_self();
    BFC_ASSERT_EQUAL(0, m_freelist->free_area(pid + 1024 * 2, 1024 * 3));
    BFC_ASSERT_EQUAL(0, m_freelist->free_page(page));

    BFC_ASSERT_EQUAL(0, m_freelist->alloc_page(&o));
    BFC_ASSERT_EQUAL(12288ull, o);
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_area(1024, &o));
    BFC_ASSERT_EQUAL(8192ull, o);
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_area(1, &o));
    BFC_ASSERT_EQUAL(9216ull, o);

    page->free();
    delete page;
  }

  void simplePersistentTest() {
    m_freelist->free_area(m_pagesize * 1, m_pagesize / 2);
    m_freelist->free_area(m_pagesize * 2, m_pagesize / 2);
    m_freelist->free_area(m_pagesize * 3, m_pagesize / 2);
    m_freelist->free_area(m_pagesize * 4, 10);

    reopen();

    ham_u64_t o;
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_area(1, &o));
    BFC_ASSERT_EQUAL(0ull, o);
  }

  void bigBlobTest() {
    Environment *env = (Environment *)m_env;

    // allocate a big blob
    DiskBlobManager *dbm = (DiskBlobManager *)env->get_blob_manager();

    ham_u64_t blob_id = 0;
    ham_record_t record = {0};
    record.data = ::calloc(m_pagesize * 3, 1);
    record.size = m_pagesize * 3;
    BFC_ASSERT_EQUAL(0, dbm->allocate((Database *)m_db, &record, 0, &blob_id));
    BFC_ASSERT(blob_id != 0);
    ::free(record.data);

    // free it
    BFC_ASSERT_EQUAL(0, dbm->free((Database *)m_db, blob_id));

    // check the freelist
    const ReducedFreelist::EntryVec &vec = m_freelist->get_entries();
    ReducedFreelist::EntryVec::const_iterator it = vec.begin();
    BFC_ASSERT_EQUAL(2u, vec.size());

    BFC_ASSERT_EQUAL(blob_id, it->first); it++;
    BFC_ASSERT_EQUAL(blob_id + m_pagesize * 3 + sizeof(PBlobHeader), it->first);

    env->get_changeset().clear();
  }

  void blobFreeTest() {
    ham_u64_t blob[4] = {0};
    ham_u8_t buffer[32] = {0};
    Database *db = (Database *)m_db;
    Environment *env = (Environment *)m_env;

    // allocate four blobs, free two of them
    DiskBlobManager *dbm = (DiskBlobManager *)env->get_blob_manager();
    for (int i = 0; i < 4; i++) {
      ham_record_t record = {0};
      record.data = (void *)&buffer[0];
      record.size = 20 + i;

      BFC_ASSERT_EQUAL(0, dbm->allocate(db, &record, 0, &blob[i]));
    }
    BFC_ASSERT_EQUAL(0, dbm->free(db, blob[0]));
    BFC_ASSERT_EQUAL(0, dbm->free(db, blob[2]));

    env->get_changeset().clear();

    // now check the freelist
    ham_u64_t o;
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_area(20, &o));
    BFC_ASSERT_EQUAL(8390ull, o);
    BFC_ASSERT_EQUAL(0, m_freelist->alloc_area(52, &o));
    BFC_ASSERT_EQUAL(8410ull, o);
  }

  void pageFreeTest() {
    Page *page;
    Environment *env = (Environment *)m_env;

    // allocate a page, store it in the freelist
    BFC_ASSERT_EQUAL(0, env->get_page_manager()->alloc_page(&page, 0, 0, 0));
    ham_u64_t pid = page->get_self();
    BFC_ASSERT_EQUAL(0, env->get_page_manager()->add_to_freelist(page));
    // make sure that the modified page is written to disk (free_page changes
    // the page type)
    BFC_ASSERT_EQUAL(0, env->get_page_manager()->flush_all_pages(true));

    env->get_changeset().clear();

    // allocate a new page
    env = (Environment *)m_env;
    BFC_ASSERT_EQUAL(0, env->get_page_manager()->alloc_page(&page, 0, 0, 0));
    BFC_ASSERT_EQUAL(pid, page->get_self());

    env->get_changeset().clear();
  }
};

BFC_REGISTER_FIXTURE(FullFreelistTest);
BFC_REGISTER_FIXTURE(ReducedFreelistTest);

