/*
 * Copyright (C) 2005-2015 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "3rdparty/catch/catch.hpp"

#include "utils.h"

#include "1base/pickle.h"
#include "2page/page.h"
#include "2device/device.h"
#include "3page_manager/freelist.h"
#include "3page_manager/page_manager.h"
#include "3page_manager/page_manager_test.h"
#include "4context/context.h"
#include "4env/env_local.h"
#include "4txn/txn.h"

namespace hamsterdb {


struct PageManagerFixture {
  ham_db_t *m_db;
  ham_env_t *m_env;
  bool m_inmemory;
  Device *m_device;
  ScopedPtr<Context> m_context;

  PageManagerFixture(bool inmemorydb = false, uint32_t cachesize = 0)
      : m_db(0), m_inmemory(inmemorydb), m_device(0) {
    uint32_t flags = 0;

    if (m_inmemory)
      flags |= HAM_IN_MEMORY;

    ham_parameter_t params[2] = {{0, 0}, {0, 0}};
    if (cachesize) {
      params[0].name = HAM_PARAM_CACHE_SIZE;
      params[0].value = cachesize;
    }

    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"), flags,
                0644, &params[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, 0, 0));

    m_context.reset(new Context((LocalEnvironment *)m_env, 0,
                            (LocalDatabase *)m_db));
  }

  ~PageManagerFixture() {
    m_context->changeset.clear();
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void fetchPageTest() {
    PageManager *pm = ((LocalEnvironment *)m_env)->page_manager();
    Page *page;

    page = 0;
    REQUIRE((page = pm->fetch(m_context.get(), 16 * 1024ull)) != 0);
    REQUIRE(page->get_address() == 16 * 1024ull);

    page = 0;
    REQUIRE((page = pm->fetch(m_context.get(), 16 * 1024ull,
                    PageManager::kOnlyFromCache)) != 0);
    REQUIRE(page->get_address() == 16 * 1024ull);
    REQUIRE(page != 0);
  }

  void allocPageTest() {
    PageManager *pm = ((LocalEnvironment *)m_env)->page_manager();
    Page *page;

    page = 0;
    REQUIRE((page = pm->alloc(m_context.get(), Page::kTypePageManager,
                            PageManager::kClearWithZero)) != 0);
    if (m_inmemory == false)
      REQUIRE(page->get_address() == 2 * 16 * 1024ull);
    REQUIRE(page != 0);
    REQUIRE(page->get_db() == ((LocalDatabase *)m_db));
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
        ham_env_create(&m_env, Utils::opath(".test"),  
            0, 0644, &param[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &db, 13, 0, 0));

    LocalEnvironment *lenv = (LocalEnvironment *)m_env;

    REQUIRE(102400ull == lenv->config().cache_size_bytes);
  }

  void setCacheSizeEnvOpen(uint64_t size) {
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));

    ham_parameter_t param[] = {
      { HAM_PARAM_CACHE_SIZE, size },
      { 0, 0 }
    };

    REQUIRE(0 ==
        ham_env_open(&m_env, Utils::opath(".test"),  
            0, &param[0]));

    LocalEnvironment *lenv = (LocalEnvironment *)m_env;

    REQUIRE(size == lenv->config().cache_size_bytes);
  }

  void cachePutGet() {
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;

    PPageData pers;
    memset(&pers, 0, sizeof(pers));

    Page *page = new Page(lenv->device());
    page->set_address(0x123ull);
    page->set_data(&pers);
    page->set_without_header(true);

    PageManagerTest test = lenv->page_manager()->test();
    test.store_page(page);
    REQUIRE(page == test.fetch_page(0x123ull));
    test.remove_page(page);

    page->set_data(0);
    delete page;
  }

  void cachePutGetRemove() {
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;

    PPageData pers;
    memset(&pers, 0, sizeof(pers));

    Page *page = new Page(lenv->device());
    page->set_address(0x123ull);
    page->set_data(&pers);
    page->set_without_header(true);

    PageManagerTest test = lenv->page_manager()->test();
    test.store_page(page);
    REQUIRE(page == test.fetch_page(0x123ull));
    test.remove_page(page);
    REQUIRE((Page *)0 == test.fetch_page(0x123ull));

    page->set_data(0);
    delete page;
  }

  void cacheManyPuts() {
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;
    Page *page[20];
    PPageData pers[20];
    PageManagerTest test = lenv->page_manager()->test();

    for (int i = 0; i < 20; i++) {
      page[i] = new Page(lenv->device());
      memset(&pers[i], 0, sizeof(pers[i]));
      page[i]->set_without_header(true);
      page[i]->set_address(i + 1);
      page[i]->set_data(&pers[i]);
      test.store_page(page[i]);
    }
    for (int i = 0; i < 20; i++)
      REQUIRE(page[i] == test.fetch_page(i + 1));
    for (int i = 0; i < 20; i++)
      test.remove_page(page[i]);
    for (int i = 0; i < 20; i++) {
      REQUIRE((Page *)0 == test.fetch_page(i + 1));
      page[i]->set_data(0);
      delete page[i];
    }
  }

  void cacheNegativeGets() {
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;
    PageManagerTest test = lenv->page_manager()->test();

    for (int i = 0; i < 20; i++)
      REQUIRE((Page *)0 == test.fetch_page(i + 1));
  }

  void cacheFullTest() {
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;
    PageManagerTest test = lenv->page_manager()->test();

    PPageData pers;
    memset(&pers, 0, sizeof(pers));
    std::vector<Page *> v;

    for (unsigned int i = 0; i < 15; i++) {
      Page *p = new Page(lenv->device());
      p->set_without_header(true);
      p->assign_allocated_buffer(&pers, i + 1);
      v.push_back(p);
      test.store_page(p);
      REQUIRE(false == test.is_cache_full());
    }

    for (unsigned int i = 0; i < 5; i++) {
      Page *p = new Page(lenv->device());
      p->set_without_header(true);
      p->assign_allocated_buffer(&pers, i + 15 + 1);
      v.push_back(p);
      test.store_page(p);
      REQUIRE(true == test.is_cache_full());
    }

    for (unsigned int i = 0; i < 5; i++) {
      REQUIRE(true == test.is_cache_full());
      Page *p = v.back();
      v.pop_back();
      test.remove_page(p);
      p->set_data(0);
      delete p;
    }

    for (unsigned int i = 0; i < 15; i++) {
      Page *p = v.back();
      v.pop_back();
      test.remove_page(p);
      REQUIRE(false == test.is_cache_full());
      p->set_data(0);
      delete p;
    }

    REQUIRE(false == test.is_cache_full());
  }

  void storeStateTest() {
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;
    PageManagerTest test = lenv->page_manager()->test();
    uint32_t page_size = lenv->config().page_size_bytes;

    // fill with freelist pages and blob pages
    for (int i = 0; i < 10; i++)
      test.state()->freelist.free_pages[page_size * (i + 100)] = 1;

    test.state()->needs_flush = true;
    REQUIRE(test.store_state() == page_size * 2);

    // reopen the database
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
    REQUIRE(0 == ham_env_open(&m_env, Utils::opath(".test"),  0, 0));

    lenv = (LocalEnvironment *)m_env;
    test = lenv->page_manager()->test();

    // and check again - the entries must be collapsed
    Freelist::FreeMap::iterator it = test.state()->freelist.free_pages.begin();
    REQUIRE(it->first == page_size * 100);
    REQUIRE(it->second == 10);
  }

  void reclaimTest() {
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;
    PageManager *pm = lenv->page_manager();
    PageManagerTest test = lenv->page_manager()->test();
    uint32_t page_size = lenv->config().page_size_bytes;
    Page *page[5] = {0};

    // force-flush the state of the PageManager; otherwise it will be
    // written AFTER the allocated pages, and disable the reclaim
    test.state()->needs_flush = true;
    // pretend there is data to write, otherwise store_state() is a nop
    test.state()->freelist.free_pages[page_size] = 0;
    test.store_state();
    test.state()->freelist.free_pages.clear(); // clean up again

    // allocate 5 pages
    for (int i = 0; i < 5; i++) {
      REQUIRE((page[i] = pm->alloc(m_context.get(), 0)) != 0);
      REQUIRE(page[i]->get_address() == (3 + i) * page_size);
    }

    // free the last 3 of them and move them to the freelist (and verify with
    // is_page_free())
    for (int i = 2; i < 5; i++) {
      pm->del(m_context.get(), page[i]);
      REQUIRE(true == test.is_page_free(page[i]->get_address()));
    }
    for (int i = 0; i < 2; i++) {
      REQUIRE(false == test.is_page_free(page[i]->get_address()));
    }

    // verify file size
    REQUIRE((uint64_t)(page_size * 8) == lenv->device()->file_size());

    // reopen the file
    m_context->changeset.clear();
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
    REQUIRE(0 == ham_env_open(&m_env, Utils::opath(".test"),  0, 0));
    m_context.reset(new Context((LocalEnvironment *)m_env, 0,
                            (LocalDatabase *)m_db));

    lenv = (LocalEnvironment *)m_env;
    pm = lenv->page_manager();

    PageManagerTest test2 = pm->test();
    for (int i = 0; i < 2; i++)
      REQUIRE(false == test2.is_page_free((3 + i) * page_size));

    // verify file size
#ifndef WIN32
    REQUIRE((uint64_t)(page_size * 5) == lenv->device()->file_size());
#endif
  }

  void collapseFreelistTest() {
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;
    PageManager *pm = lenv->page_manager();
    PageManagerTest test = pm->test();
    uint32_t page_size = lenv->config().page_size_bytes;

    for (int i = 1; i <= 150; i++)
      test.state()->freelist.free_pages[page_size * i] = 1;

    // store the state on disk
    test.state()->needs_flush = true;
    uint64_t page_id = test.store_state();

    pm->flush_all_pages();
    test.state()->freelist.free_pages.clear();

    pm->initialize(page_id);

    REQUIRE(10 == test.state()->freelist.free_pages.size());
    for (int i = 1; i < 10; i++)
      REQUIRE(test.state()->freelist.free_pages[page_size * (1 + i * 15)] == 15);
  }

  void encodeDecodeTest() {
    uint8_t buffer[32] = {0};

    for (int i = 1; i < 10000; i++) {
      int num_bytes = Pickle::encode_u64(&buffer[0], i * 13);
      REQUIRE(Pickle::decode_u64(num_bytes, &buffer[0]) == (uint64_t)i * 13);
    }
  }

  void storeBigStateTest() {
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;
    PageManager *pm = lenv->page_manager();
    PageManagerTest test = pm->test();
    uint32_t page_size = lenv->config().page_size_bytes;

    test.state()->last_blob_page_id = page_size * 100;

    for (int i = 1; i <= 30000; i++) {
      if (i & 1) // only store every 2nd page to avoid collapsing
        test.state()->freelist.free_pages[page_size * i] = 1;
    }

    // store the state on disk
    test.state()->needs_flush = true;
    uint64_t page_id = test.store_state();

    pm->flush_all_pages();
    test.state()->freelist.free_pages.clear();
    test.state()->last_blob_page_id = 0;

    pm->initialize(page_id);

    REQUIRE(test.state()->last_blob_page_id == page_size * 100);

    REQUIRE(15000 == test.state()->freelist.free_pages.size());
    for (int i = 1; i <= 30000; i++) {
      if (i & 1)
        REQUIRE(test.state()->freelist.free_pages[page_size * i] == 1);
    }

    REQUIRE(test.state()->page_count_page_manager == 4u);
  }

  void allocMultiBlobs() {
    LocalEnvironment *lenv = (LocalEnvironment *)m_env;
    PageManager *pm = lenv->page_manager();
    uint32_t page_size = lenv->config().page_size_bytes;

    Context context(lenv, 0, 0);

    Page *head = pm->alloc_multiple_blob_pages(&context, 10);
    REQUIRE(head != 0);
    pm->del(&context, head, 10);

    Page *page1 = pm->alloc_multiple_blob_pages(&context, 2);
    REQUIRE(page1 != 0);
    REQUIRE(page1->get_address() == head->get_address());

    Page *page2 = pm->alloc_multiple_blob_pages(&context, 8);
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
  PageManagerFixture f(false, 16 * HAM_DEFAULT_PAGE_SIZE);
  f.cacheFullTest();
}

TEST_CASE("PageManager/storeStateTest", "")
{
  PageManagerFixture f(false, 16 * HAM_DEFAULT_PAGE_SIZE);
  f.storeStateTest();
}

TEST_CASE("PageManager/reclaimTest", "")
{
  PageManagerFixture f(false, 16 * HAM_DEFAULT_PAGE_SIZE);
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
