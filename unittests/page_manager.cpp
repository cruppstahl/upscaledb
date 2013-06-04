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
#include <string.h>

#include <ham/hamsterdb.h>

#include "../src/db.h"
#include "../src/page.h"
#include "../src/device.h"
#include "../src/env.h"
#include "../src/txn.h"
#include "../src/page_manager.h"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;
using namespace hamsterdb;

class PageManagerTest : public hamsterDB_fixture {
  define_super(hamsterDB_fixture);

public:
  PageManagerTest(bool inmemorydb = false, const char *name = "PageManagerTest")
    : hamsterDB_fixture(name), m_db(0), m_inmemory(inmemorydb), m_device(0) {
    testrunner::get_instance()->register_fixture(this);
    BFC_REGISTER_TEST(PageManagerTest, newDeleteTest);
    BFC_REGISTER_TEST(PageManagerTest, fetchPageTest);
    BFC_REGISTER_TEST(PageManagerTest, allocPageTest);
    BFC_REGISTER_TEST(PageManagerTest, fetchInvalidPageTest);
  }

protected:
  ham_db_t *m_db;
  ham_env_t *m_env;
  bool m_inmemory;
  Device *m_device;

public:
  virtual void setup() {
    ham_u32_t flags = 0;

    __super::setup();

    if (m_inmemory)
      flags |= HAM_IN_MEMORY;

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"), flags, 0644, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
  }

  virtual void teardown() {
    __super::teardown();

    BFC_ASSERT_EQUAL(0, ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void newDeleteTest() {
    PageManager *pm = ((Environment *)m_env)->get_page_manager();

    BFC_ASSERT(pm->get_cache() != 0);
    if (m_inmemory)
      BFC_ASSERT(pm->get_freelist(0) == 0);
  }

  void fetchPageTest() {
    PageManager *pm = ((Environment *)m_env)->get_page_manager();
    Page *page;

    page = 0;
    BFC_ASSERT_EQUAL(0, pm->fetch_page(&page, 0, 16 * 1024ull, false));
    BFC_ASSERT_EQUAL(page->get_self(), 16 * 1024ull);

    page = 0;
    BFC_ASSERT_EQUAL(0, pm->fetch_page(&page, 0, 16 * 1024ull, true));
    BFC_ASSERT_EQUAL(page->get_self(), 16 * 1024ull);
    BFC_ASSERT(page != 0);
  }

  void allocPageTest() {
    PageManager *pm = ((Environment *)m_env)->get_page_manager();
    Page *page;

    page = 0;
    BFC_ASSERT_EQUAL(0,
            pm->alloc_page(&page, 0, Page::TYPE_FREELIST,
                PageManager::CLEAR_WITH_ZERO));
    if (m_inmemory == false)
      BFC_ASSERT_EQUAL(page->get_self(), 2 * 16 * 1024ull);
    BFC_ASSERT(page != 0);
    BFC_ASSERT(page->get_db() == 0);
  }

  void fetchInvalidPageTest() {
    PageManager *pm = ((Environment *)m_env)->get_page_manager();
    Page *page;

    page = 0;
    BFC_ASSERT_EQUAL(HAM_IO_ERROR,
            pm->fetch_page(&page, 0, 1024 * 1024 * 200, false));

    BFC_ASSERT_EQUAL(page, (Page *)0);
  }
};

class InMemoryPageManagerTest : public PageManagerTest {
public:
  InMemoryPageManagerTest()
    : PageManagerTest(true, "InMemoryPageManagerTest") {
    clear_tests(); // don't inherit tests
    testrunner::get_instance()->register_fixture(this);
    BFC_REGISTER_TEST(InMemoryPageManagerTest, newDeleteTest);
    BFC_REGISTER_TEST(InMemoryPageManagerTest, allocPageTest);
  }
};

BFC_REGISTER_FIXTURE(PageManagerTest);
BFC_REGISTER_FIXTURE(InMemoryPageManagerTest);

