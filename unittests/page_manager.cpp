/**
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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
#include "../src/config.h"
#include "../src/page_manager.h"

namespace hamsterdb {

struct PageManagerFixture {
  ham_db_t *m_db;
  ham_env_t *m_env;
  bool m_inmemory;
  Device *m_device;

  PageManagerFixture(bool inmemorydb = false, ham_u32_t cachesize = 0)
      : m_db(0), m_inmemory(inmemorydb), m_device(0) {
    ham_u32_t flags = 0;

    if (m_inmemory)
      flags |= HAM_IN_MEMORY;

    ham_parameter_t params[2] = {{0, 0}, {0, 0}};
    if (cachesize) {
      params[0].name = HAM_PARAM_CACHE_SIZE;
      params[0].value = cachesize;
    }

    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"), flags,
                0644, &params[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
  }

  ~PageManagerFixture() {
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void fetchPageTest() {
    PageManager *pm = ((LocalEnvironment *)m_env)->get_page_manager();
    Page *page;

    page = 0;
    REQUIRE((page = pm->fetch_page(0, 16 * 1024ull, false)));
    REQUIRE(page->get_address() == 16 * 1024ull);

    page = 0;
    REQUIRE((page = pm->fetch_page(0, 16 * 1024ull, true)));
    REQUIRE(page->get_address() == 16 * 1024ull);
    REQUIRE(page);
  }

  void allocPageTest() {
    PageManager *pm = ((LocalEnvironment *)m_env)->get_page_manager();
    Page *page;

    page = 0;
    REQUIRE((page = pm->alloc_page(0, Page::kTypePageManager,
                            PageManager::kClearWithZero)));
    if (m_inmemory == false)
      REQUIRE(page->get_address() == 2 * 16 * 1024ull);
    REQUIRE(page != 0);
    REQUIRE(!page->get_db());
  }

  void setCacheSizeEnvCreate() {
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));

    ham_db_t *db = 0;
    ham_parameter_t param[] = {
      { HAM_PARAM_CACHE_SIZE, 100 * 1024 },
      { HAM_PARAM_PAGE_SIZE,  1024 },
      { 0, 0 }
    };

    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"),  
            0, 0644, &param[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &db, 13, 0, 0));

    LocalEnvironment *lenv = (LocalEnvironment *)m_env;

    REQUIRE(102400ull == lenv->get_page_manager()->get_cache_capacity());
  }

  void setCacheSizeEnvOpen(ham_u64_t size) {
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));

    ham_parameter_t param[] = {
      { HAM_PARAM_CACHE_SIZE, size },
      { 0, 0 }
    };

    REQUIRE(0 ==
        ham_env_open(&m_env, Globals::opath(".test"),  
            0, &param[0]));

    LocalEnvironment *lenv = (LocalEnvironment *)m_env;

    REQUIRE(size == lenv->get_page_manager()->get_cache_capacity());
  }

  void cachePutGet() {
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;

    PPageData pers;
    memset(&pers, 0, sizeof(pers));

    Page *page = new Page(lenv);
    page->set_address(0x123ull);
    page->set_data(&pers);
    page->set_flags(Page::kNpersNoHeader);

    lenv->get_page_manager()->store_page(page);
    REQUIRE(page == lenv->get_page_manager()->fetch_page(0x123ull));
    lenv->get_page_manager()->test_remove_page(page);

    page->set_data(0);
    delete page;
  }

  void cachePutGetRemove() {
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;

    PPageData pers;
    memset(&pers, 0, sizeof(pers));

    Page *page = new Page(lenv);
    page->set_address(0x123ull);
    page->set_data(&pers);
    page->set_flags(Page::kNpersNoHeader);

    lenv->get_page_manager()->store_page(page);
    REQUIRE(page == lenv->get_page_manager()->fetch_page(0x123ull));
    lenv->get_page_manager()->test_remove_page(page);
    REQUIRE((Page *)0 == lenv->get_page_manager()->fetch_page(0x123ull));

    page->set_data(0);
    delete page;
  }

  void cacheManyPuts() {
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;
    Page *page[20];
    PPageData pers[20];

    for (int i = 0; i < 20; i++) {
      page[i] = new Page(lenv);
      memset(&pers[i], 0, sizeof(pers[i]));
      page[i]->set_flags(Page::kNpersNoHeader);
      page[i]->set_address(i + 1);
      page[i]->set_data(&pers[i]);
      lenv->get_page_manager()->store_page(page[i]);
    }
    for (int i = 0; i < 20; i++)
      REQUIRE(page[i] == lenv->get_page_manager()->fetch_page(i + 1));
    for (int i = 0; i < 20; i++)
      lenv->get_page_manager()->test_remove_page(page[i]);
    for (int i = 0; i < 20; i++) {
      REQUIRE((Page *)0 == lenv->get_page_manager()->fetch_page(i + 1));
      page[i]->set_data(0);
      delete page[i];
    }
  }

  void cacheNegativeGets() {
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;

    for (int i = 0; i < 20; i++)
      REQUIRE((Page *)0 == lenv->get_page_manager()->fetch_page(i + 1));
  }

  void cacheFullTest() {
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;

    PPageData pers;
    memset(&pers, 0, sizeof(pers));
    std::vector<Page *> v;

    for (unsigned int i = 0; i < 15; i++) {
      Page *p = new Page(lenv);
      p->set_flags(Page::kNpersNoHeader | Page::kNpersMalloc);
      p->set_address(i + 1);
      p->set_data(&pers);
      v.push_back(p);
      lenv->get_page_manager()->store_page(p);
      REQUIRE(false == lenv->get_page_manager()->cache_is_full());
    }

    for (unsigned int i = 0; i < 5; i++) {
      Page *p = new Page(lenv);
      p->set_flags(Page::kNpersNoHeader | Page::kNpersMalloc);
      p->set_address(i + 15 + 1);
      p->set_data(&pers);
      v.push_back(p);
      lenv->get_page_manager()->store_page(p);
      REQUIRE(true == lenv->get_page_manager()->cache_is_full());
    }

    for (unsigned int i = 0; i < 5; i++) {
      REQUIRE(true == lenv->get_page_manager()->cache_is_full());
      Page *p = v.back();
      v.pop_back();
      lenv->get_page_manager()->test_remove_page(p);
      p->set_data(0);
      delete p;
    }

    for (unsigned int i = 0; i < 15; i++) {
      Page *p = v.back();
      v.pop_back();
      lenv->get_page_manager()->test_remove_page(p);
      REQUIRE(false == lenv->get_page_manager()->cache_is_full());
      p->set_data(0);
      delete p;
    }

    REQUIRE(false == lenv->get_page_manager()->cache_is_full());
  }

  void storeStateTest() {
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;
    PageManager *pm = lenv->get_page_manager();
    ham_u32_t page_size = lenv->get_page_size();

    // fill with freelist pages and blob pages
    for (int i = 0; i < 10; i++)
      pm->m_free_pages[page_size * (i + 100)] = 1;

    pm->m_needs_flush = true;
    REQUIRE(pm->store_state() == page_size * 2);

    // reopen the database
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
    REQUIRE(0 == ham_env_open(&m_env, Globals::opath(".test"),  0, 0));

    lenv = (LocalEnvironment *)m_env;
    pm = lenv->get_page_manager();

    // and check again - the entries must be collapsed
    PageManager::FreeMap::iterator it = pm->m_free_pages.begin();
    REQUIRE(it->first == page_size * 100);
    REQUIRE(it->second == 10);
  }

  void reclaimTest() {
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;
    PageManager *pm = lenv->get_page_manager();
    ham_u32_t page_size = lenv->get_page_size();
    Page *page[5] = {0};

    // force-flush the state of the PageManager; otherwise it will be
    // written AFTER the allocated pages, and disable the reclaim
    pm->m_needs_flush = true;
    pm->store_state();

    // allocate 5 pages
    for (int i = 0; i < 5; i++) {
      REQUIRE((page[i] = pm->alloc_page(0, 0)));
      REQUIRE(page[i]->get_address() == (3 + i) * page_size);
    }

    // free the last 3 of them and move them to the freelist (and verify with
    // is_page_free)
    for (int i = 2; i < 5; i++) {
      pm->add_to_freelist(page[i]);
      REQUIRE(true == pm->test_is_page_free(page[i]->get_address()));
    }
    for (int i = 0; i < 2; i++) {
      REQUIRE(false == pm->test_is_page_free(page[i]->get_address()));
    }

    // verify file size
    REQUIRE((ham_u64_t)(page_size * 8) == lenv->get_device()->get_file_size());

    // reopen the file
    lenv->get_changeset().clear();
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
    REQUIRE(0 == ham_env_open(&m_env, Globals::opath(".test"),  0, 0));

    lenv = (LocalEnvironment *)m_env;
    pm = lenv->get_page_manager();

    for (int i = 0; i < 2; i++)
      REQUIRE(false == pm->test_is_page_free((3 + i) * page_size));

    // verify file size
#ifndef WIN32
    REQUIRE((ham_u64_t)(page_size * 6) == lenv->get_device()->get_file_size());
#endif
  }

  void collapseFreelistTest() {
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;
    PageManager *pm = lenv->get_page_manager();
    ham_u32_t page_size = lenv->get_page_size();

    for (int i = 1; i <= 150; i++)
      pm->m_free_pages[page_size * i] = 1;

    // store the state on disk
    pm->m_needs_flush = true;
    ham_u64_t page_id = pm->store_state();

    pm->flush_all_pages();
    pm->m_free_pages.clear();

    pm->load_state(page_id);

    REQUIRE(10 == pm->m_free_pages.size());
    for (int i = 1; i < 10; i++)
      REQUIRE(pm->m_free_pages[page_size * (1 + i * 15)] == 15);
  }

  void encodeDecodeTest() {
    ham_u8_t buffer[32] = {0};
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;
    PageManager *pm = lenv->get_page_manager();

    for (int i = 1; i < 10000; i++) {
      int num_bytes = pm->encode(&buffer[0], i * 13);
      REQUIRE(pm->decode(num_bytes, &buffer[0]) == (ham_u64_t)i * 13);
    }
  }

  void storeBigStateTest() {
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;
    PageManager *pm = lenv->get_page_manager();
    ham_u32_t page_size = lenv->get_page_size();

    for (int i = 1; i <= 30000; i++) {
      if (i & 1) // only store every 2nd page to avoid collapsing
        pm->m_free_pages[page_size * i] = 1;
    }

    // store the state on disk
    pm->m_needs_flush = true;
    ham_u64_t page_id = pm->store_state();

    pm->flush_all_pages();
    pm->m_free_pages.clear();

    pm->load_state(page_id);

    REQUIRE(15000 == pm->m_free_pages.size());
    for (int i = 1; i <= 30000; i++) {
      if (i & 1)
        REQUIRE(pm->m_free_pages[page_size * i] == 1);
    }

    REQUIRE(pm->m_page_count_page_manager == 4);
  }

  void allocMultiBlobs() {
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;
    PageManager *pm = lenv->get_page_manager();
    ham_u32_t page_size = lenv->get_page_size();

    Page *head = pm->alloc_multiple_blob_pages(0, 10);
    REQUIRE(head != 0);
    pm->add_to_freelist(head, 10);

    Page *page1 = pm->alloc_multiple_blob_pages(0, 2);
    REQUIRE(page1 != 0);
    REQUIRE(page1->get_address() == head->get_address());

    Page *page2 = pm->alloc_multiple_blob_pages(0, 8);
    REQUIRE(page2 != 0);
    REQUIRE(page2->get_address() == page1->get_address() + page_size * 2);
  }
};

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

TEST_CASE("PageManager/setCacheSizeEnvCreate", "")
{
  PageManagerFixture f;
  f.setCacheSizeEnvCreate();
}

TEST_CASE("PageManager/setCacheSizeEnvOpen", "")
{
  PageManagerFixture f;
  f.setCacheSizeEnvOpen(100 * 1024);
}

TEST_CASE("PageManager/setBigCacheSizeEnvOpen", "")
{
  PageManagerFixture f;
  f.setCacheSizeEnvOpen(1024ull * 1024ull * 1024ull * 16ull);
}

TEST_CASE("PageManager/cachePutGet", "")
{
  PageManagerFixture f;
  f.cachePutGet();
}

TEST_CASE("PageManager/cachePutGetRemove", "")
{
  PageManagerFixture f;
  f.cachePutGetRemove();
}

TEST_CASE("PageManager/cacheManyPuts", "")
{
  PageManagerFixture f;
  f.cacheManyPuts();
}

TEST_CASE("PageManager/cacheNegativeGets", "")
{
  PageManagerFixture f;
  f.cacheNegativeGets();
}

TEST_CASE("PageManager/cacheFullTest", "")
{
  PageManagerFixture f(false, 16 * HAM_DEFAULT_PAGESIZE);
  f.cacheFullTest();
}

TEST_CASE("PageManager/storeStateTest", "")
{
  PageManagerFixture f(false, 16 * HAM_DEFAULT_PAGESIZE);
  f.storeStateTest();
}

TEST_CASE("PageManager/reclaimTest", "")
{
  PageManagerFixture f(false, 16 * HAM_DEFAULT_PAGESIZE);
  f.reclaimTest();
}

TEST_CASE("PageManager/collapseFreelistTest", "")
{
  PageManagerFixture f(false);
  f.collapseFreelistTest();
}

TEST_CASE("PageManager/encodeDecodeTest", "")
{
  PageManagerFixture f(false);
  f.encodeDecodeTest();
}

TEST_CASE("PageManager/storeBigStateTest", "")
{
  PageManagerFixture f(false);
  f.storeBigStateTest();
}

TEST_CASE("PageManager/allocMultiBlobs", "")
{
  PageManagerFixture f(false);
  f.allocMultiBlobs();
}

TEST_CASE("PageManager-inmem/allocPage", "")
{
  PageManagerFixture f(true);
  f.allocPageTest();
}

} // namespace hamsterdb
