/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
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

#include "3rdparty/catch/catch.hpp"

#include "utils.h"
#include "os.hpp"

#include "2page/page.h"
#include "2device/device.h"
#include "4db/db.h"
#include "4env/env_local.h"
#include "4txn/txn.h"

using namespace hamsterdb;

struct PageFixture {
  ham_db_t *m_db;
  ham_env_t *m_env;
  bool m_inmemory;
  bool m_usemmap;

  PageFixture(bool inmemorydb = false, bool mmap = true)
    : m_db(0), m_inmemory(inmemorydb), m_usemmap(mmap) {
    uint32_t flags = 0;

    if (m_inmemory)
      flags |= HAM_IN_MEMORY;
    if (!m_usemmap)
      flags |= HAM_DISABLE_MMAP;

    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"), flags, 0644, 0));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
  }

  ~PageFixture() {
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void newDeleteTest() {
    Page *page;
    page = new Page(((LocalEnvironment *)m_env)->device());
    REQUIRE(page);
    delete page;
  }

  void allocFreeTest() {
    Page *page;
    page = new Page(((LocalEnvironment *)m_env)->device());
    page->alloc(0, 1024);
    delete page;
  }

  void multipleAllocFreeTest() {
    int i;
    Page *page;
    uint32_t ps = ((LocalEnvironment *)m_env)->config().page_size_bytes;

    for (i = 0; i < 10; i++) {
      page = new Page(((LocalEnvironment *)m_env)->device());
      page->alloc(0, ps);
      /* i+2 since we need 1 page for the header page and one page
       * for the root page */
      if (!m_inmemory)
        REQUIRE(page->get_address() == (i + 2) * ps);
      delete page;
    }
  }

  void fetchFlushTest() {
    Page *page, *temp;
    uint32_t ps = ((LocalEnvironment *)m_env)->config().page_size_bytes;

    Device *device = ((LocalEnvironment *)m_env)->device(); 
    page = new Page(device);
    temp = new Page(device);
    page->alloc(0, ps);
    REQUIRE(page->get_address() == ps * 2);

    page->fetch(page->get_address());

    // patch the size, otherwise we run into asserts

    memset(page->get_payload(), 0x13, ps - Page::kSizeofPersistentHeader);
    page->set_dirty(true);
    page->flush(device, page->get_persisted_data());

    REQUIRE(false == page->is_dirty());
    temp->fetch(ps * 2);
    REQUIRE(0 == memcmp(page->get_data(), temp->get_data(), ps));

    delete temp;
    delete page;
  }
};

TEST_CASE("Page/newDelete", "")
{
  PageFixture f;
  f.newDeleteTest();
}

TEST_CASE("Page/allocFree", "")
{
  PageFixture f;
  f.allocFreeTest();
}

TEST_CASE("Page/multipleAllocFree", "")
{
  PageFixture f;
  f.multipleAllocFreeTest();
}

TEST_CASE("Page/fetchFlush", "")
{
  PageFixture f;
  f.fetchFlushTest();
}

TEST_CASE("Page-nommap/newDelete", "")
{
  PageFixture f(false, false);
  f.newDeleteTest();
}

TEST_CASE("Page-nommap/allocFree", "")
{
  PageFixture f(false, false);
  f.allocFreeTest();
}

TEST_CASE("Page-nommap/multipleAllocFree", "")
{
  PageFixture f(false, false);
  f.multipleAllocFreeTest();
}

TEST_CASE("Page-nommap/fetchFlush", "")
{
  PageFixture f(false, false);
  f.fetchFlushTest();
}


TEST_CASE("Page-inmem/newDelete", "")
{
  PageFixture f(true);
  f.newDeleteTest();
}

TEST_CASE("Page-inmem/allocFree", "")
{
  PageFixture f(true);
  f.allocFreeTest();
}

TEST_CASE("Page-inmem/multipleAllocFree", "")
{
  PageFixture f(true);
  f.multipleAllocFreeTest();
}

