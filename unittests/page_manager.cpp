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

#include "../src/page.h"
#include "../src/device.h"
#include "../src/env.h"
#include "../src/txn.h"
#include "../src/page_manager.h"

namespace hamsterdb {

struct PageManagerFixture {
  ham_db_t *m_db;
  ham_env_t *m_env;
  bool m_inmemory;
  Device *m_device;

  PageManagerFixture(bool inmemorydb = false)
      : m_db(0), m_inmemory(inmemorydb), m_device(0) {
    ham_u32_t flags = 0;

    if (m_inmemory)
      flags |= HAM_IN_MEMORY;

    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"), flags, 0644, 0));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
  }

  ~PageManagerFixture() {
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void newDeleteTest() {
    PageManager *pm = ((Environment *)m_env)->get_page_manager();

    REQUIRE(pm->test_get_cache());
    if (m_inmemory)
      REQUIRE(!pm->test_get_freelist());
  }

  void fetchPageTest() {
    PageManager *pm = ((Environment *)m_env)->get_page_manager();
    Page *page;

    page = 0;
    REQUIRE(0 == pm->fetch_page(&page, 0, 16 * 1024ull, false));
    REQUIRE(page->get_address() == 16 * 1024ull);

    page = 0;
    REQUIRE(0 == pm->fetch_page(&page, 0, 16 * 1024ull, true));
    REQUIRE(page->get_address() == 16 * 1024ull);
    REQUIRE(page);
  }

  void allocPageTest() {
    PageManager *pm = ((Environment *)m_env)->get_page_manager();
    Page *page;

    page = 0;
    REQUIRE(0 ==
            pm->alloc_page(&page, 0, Page::kTypeFreelist,
                PageManager::kClearWithZero));
    if (m_inmemory == false)
      REQUIRE(page->get_address() == 2 * 16 * 1024ull);
    REQUIRE(page != 0);
    REQUIRE(!page->get_db());
  }

  void fetchInvalidPageTest() {
    PageManager *pm = ((Environment *)m_env)->get_page_manager();
    Page *page;

    page = 0;
    REQUIRE(HAM_IO_ERROR ==
            pm->fetch_page(&page, 0, 1024 * 1024 * 200, false));

    REQUIRE(page == (Page *)0);
  }
};

TEST_CASE("PageManager/newDelete", "")
{
  PageManagerFixture f;
  f.newDeleteTest();
}

TEST_CASE("PageManager/fetchPage", "")
{
  PageManagerFixture f;
  f.fetchPageTest();
}

TEST_CASE("PageManager/allocPage", "")
{
  PageManagerFixture f;
  f.allocPageTest();
}

TEST_CASE("PageManager/fetchInvalidPage", "")
{
  PageManagerFixture f;
  f.fetchInvalidPageTest();
}


TEST_CASE("PageManager-inmem/newDelete", "")
{
  PageManagerFixture f(true);
  f.newDeleteTest();
}

TEST_CASE("PageManager-inmem/allocPage", "")
{
  PageManagerFixture f(true);
  f.allocPageTest();
}

} // namespace hamsterdb
