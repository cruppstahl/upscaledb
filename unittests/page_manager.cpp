/*
 * Copyright (C) 2005-2017 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * See the file COPYING for License information.
 */

#include "3rdparty/catch/catch.hpp"

#include "1base/pickle.h"
#include "3page_manager/freelist.h"
#include "3page_manager/page_manager.h"
#include "4context/context.h"

#include "fixture.hpp"

namespace upscaledb {

struct PageManagerProxy {
  PageManagerProxy(LocalEnv *lenv)
    : page_manager(lenv->page_manager.get()) {
  }

  Page *fetch(Context *context, uint64_t address, uint32_t flags = 0) {
    return page_manager->fetch(context, address, flags);
  }

  Page *alloc(Context *context, uint32_t type, uint32_t flags = 0) {
    return page_manager->alloc(context, type, flags);
  }

  PageManager *page_manager;
};

struct PageManagerFixture : BaseFixture {
  ScopedPtr<Context> context;

  PageManagerFixture(bool inmemorydb = false, uint32_t cachesize = 0) {
    uint32_t flags = 0;

    if (inmemorydb)
      flags |= UPS_IN_MEMORY;

    ups_parameter_t params[2] = {{0, 0}, {0, 0}};
    if (cachesize) {
      params[0].name = UPS_PARAM_CACHE_SIZE;
      params[0].value = cachesize;
    }

    require_create(flags, params);
    context.reset(new Context(lenv(), 0, ldb()));
  }

  ~PageManagerFixture() {
    context->changeset.clear();
  }

  void fetchPageTest() {
    PageManagerProxy pmp(lenv());

    Page *page = pmp.fetch(context.get(), 16 * 1024);
    REQUIRE(page->address() == 16 * 1024ull);

    page = pmp.fetch(context.get(), 16 * 1024, PageManager::kOnlyFromCache);
    REQUIRE(page->address() == 16 * 1024ull);
  }

  void allocPageTest() {
    PageManagerProxy pmp(lenv());

    Page *page = pmp.alloc(context.get(), Page::kTypePageManager,
                    PageManager::kClearWithZero);
    if (!is_in_memory())
      REQUIRE(page->address() == 2 * 16 * 1024ull);
    REQUIRE(page->db() == ldb());
  }

  void setCacheSizeEnvCreate() {
    ups_parameter_t param[] = {
        { UPS_PARAM_CACHE_SIZE, 100 * 1024 },
        { UPS_PARAM_PAGE_SIZE,  1024 },
        { 0, 0 }
    };

    close();
    require_create(0, param);

    REQUIRE(102400ull == lenv()->config.cache_size_bytes);
  }

  void setCacheSizeEnvOpen(uint64_t size) {
    ups_parameter_t param[] = {
        { UPS_PARAM_CACHE_SIZE, size },
        { 0, 0 }
    };

    close();
    require_open(0, param);

    REQUIRE(size == lenv()->config.cache_size_bytes);
  }

  void cachePutGet() {
    PPageData pers;
    ::memset(&pers, 0, sizeof(pers));

    Page *page = new Page(lenv()->device.get());
    page->set_address(0x123ull);
    page->set_data(&pers);
    page->set_without_header(true);

    PageManager *page_manager = lenv()->page_manager.get();

    page_manager->state->cache.put(page);
    REQUIRE(page == page_manager->state->cache.get(0x123ull));
    page_manager->state->cache.del(page);

    page->set_data(0);
    delete page;
  }

  void cachePutGetRemove() {
    PPageData pers;
    ::memset(&pers, 0, sizeof(pers));

    Page *page = new Page(lenv()->device.get());
    page->set_address(0x123ull);
    page->set_data(&pers);
    page->set_without_header(true);

    PageManager *page_manager = lenv()->page_manager.get();
    page_manager->state->cache.put(page);
    REQUIRE(page == page_manager->state->cache.get(0x123ull));
    page_manager->state->cache.del(page);
    REQUIRE((Page *)0 == page_manager->state->cache.get(0x123ull));

    page->set_data(0);
    delete page;
  }

  void cacheManyPuts() {
    Page *page[20];
    PPageData pers[20];
    PageManager *page_manager = lenv()->page_manager.get();

    for (int i = 0; i < 20; i++) {
      page[i] = new Page(lenv()->device.get());
      ::memset(&pers[i], 0, sizeof(pers[i]));
      page[i]->set_without_header(true);
      page[i]->set_address(i + 1);
      page[i]->set_data(&pers[i]);
      page_manager->state->cache.put(page[i]);
    }
    for (int i = 0; i < 20; i++)
      REQUIRE(page[i] == page_manager->state->cache.get(i + 1));
    for (int i = 0; i < 20; i++)
      page_manager->state->cache.del(page[i]);
    for (int i = 0; i < 20; i++) {
      REQUIRE((Page *)0 == page_manager->state->cache.get(i + 1));
      page[i]->set_data(0);
      delete page[i];
    }
  }

  void cacheNegativeGets() {
    PageManager *page_manager = lenv()->page_manager.get();

    for (int i = 0; i < 20; i++)
      REQUIRE((Page *)0 == page_manager->state->cache.get(i + 1));
  }

  void cacheFullTest() {
    PageManager *page_manager = lenv()->page_manager.get();

    PPageData pers;
    ::memset(&pers, 0, sizeof(pers));
    std::vector<Page *> v;

    for (unsigned int i = 0; i < 15; i++) {
      Page *p = new Page(lenv()->device.get());
      p->set_without_header(true);
      p->assign_allocated_buffer(&pers, i + 1);
      v.push_back(p);
      page_manager->state->cache.put(p);
      REQUIRE(false == page_manager->state->cache.is_cache_full());
    }

    for (unsigned int i = 0; i < 5; i++) {
      Page *p = new Page(lenv()->device.get());
      p->set_without_header(true);
      p->assign_allocated_buffer(&pers, i + 15 + 1);
      v.push_back(p);
      page_manager->state->cache.put(p);
      REQUIRE(true == page_manager->state->cache.is_cache_full());
    }

    for (unsigned int i = 0; i < 5; i++) {
      REQUIRE(true == page_manager->state->cache.is_cache_full());
      Page *p = v.back();
      v.pop_back();
      page_manager->state->cache.del(p);
      p->set_data(0);
      delete p;
    }

    for (unsigned int i = 0; i < 15; i++) {
      Page *p = v.back();
      v.pop_back();
      page_manager->state->cache.del(p);
      REQUIRE(false == page_manager->state->cache.is_cache_full());
      p->set_data(0);
      delete p;
    }

    REQUIRE(false == page_manager->state->cache.is_cache_full());
  }

  void storeStateTest() {
    PageManagerState *state = lenv()->page_manager->state.get();
    uint32_t page_size = lenv()->config.page_size_bytes;

    // fill with freelist pages and blob pages
    for (int i = 0; i < 10; i++)
      state->freelist.free_pages[page_size * (i + 100)] = 1;

    state->needs_flush = true;
    REQUIRE(lenv()->page_manager->test_store_state() == page_size * 2);

    // reopen the database
    close();
    require_open();

    state = lenv()->page_manager->state.get();

    // and check again - the entries must be collapsed
    Freelist::FreeMap::iterator it = state->freelist.free_pages.begin();
    REQUIRE(it->first == page_size * 100);
    REQUIRE(it->second == 10);
  }

  void reclaimTest() {
    PageManager *page_manager = lenv()->page_manager.get();
    uint32_t page_size = lenv()->config.page_size_bytes;
    Page *page[5] = {0};

    // force-flush the state of the PageManager; otherwise it will be
    // written AFTER the allocated pages, and disable the reclaim
    page_manager->state->needs_flush = true;
    // pretend there is data to write, otherwise test_store_state() is a nop
    page_manager->state->freelist.free_pages[page_size] = 0;
    page_manager->test_store_state();
    page_manager->state->freelist.free_pages.clear(); // clean up again

    // allocate 5 pages
    for (int i = 0; i < 5; i++) {
      REQUIRE((page[i] = page_manager->alloc(context.get(), 0)) != 0);
      REQUIRE(page[i]->address() == (3 + i) * page_size);
    }

    // free the last 3 of them and move them to the freelist (and verify with
    // has())
    for (int i = 2; i < 5; i++) {
      page_manager->del(context.get(), page[i]);
      REQUIRE(true == page_manager->state->freelist.has(page[i]->address()));
    }
    for (int i = 0; i < 2; i++) {
      REQUIRE(false == page_manager->state->freelist.has(page[i]->address()));
    }

    // verify file size
    REQUIRE((uint64_t)(page_size * 8) == lenv()->device->file_size());

    // reopen the file
    context->changeset.clear();
    close();
    require_open();
    context.reset(new Context(lenv(), 0, ldb()));

    page_manager = lenv()->page_manager.get();

    for (int i = 0; i < 2; i++)
      REQUIRE(false == page_manager->state->freelist.has((3 + i) * page_size));

    // verify file size
#ifndef WIN32
    REQUIRE((uint64_t)(page_size * 5) == lenv()->device->file_size());
#endif
  }

  void issue60Test() {
#ifndef WIN32
    const char *foo = "123456789012345567890123456789012345678901234567890";

    close();
    require_create(0);

    DbProxy dbp(db);

    for (uint32_t i = 0; i < 50000; i++)
      dbp.require_insert(i, foo);

    for (uint32_t i = 0; i < 50000; i++)
      dbp.require_erase(i);

    REQUIRE(0 == ups_db_close(db, 0));
    REQUIRE(0 == ups_env_erase_db(env, 1, 0));
    close();

    File f;
    f.open("test.db", false);
    REQUIRE(f.file_size() == 1024 * 16);
    f.close();
#endif
  }

  void collapseFreelistTest() {
    PageManager *page_manager = lenv()->page_manager.get();
    uint32_t page_size = lenv()->config.page_size_bytes;

    for (int i = 1; i <= 150; i++)
      page_manager->state->freelist.free_pages[page_size * i] = 1;

    // store the state on disk
    page_manager->state->needs_flush = true;
    uint64_t page_id = page_manager->test_store_state();

    page_manager->flush_all_pages();
    page_manager->state->freelist.free_pages.clear();

    page_manager->initialize(page_id);

    REQUIRE(10 == page_manager->state->freelist.free_pages.size());
    for (int i = 1; i < 10; i++)
      REQUIRE(page_manager->state->freelist.free_pages[page_size * (1 + i * 15)] == 15);
  }

  void encodeDecodeTest() {
    uint8_t buffer[32] = {0};

    for (int i = 1; i < 10000; i++) {
      int num_bytes = Pickle::encode_u64(&buffer[0], i * 13);
      REQUIRE(Pickle::decode_u64(num_bytes, &buffer[0]) == (uint64_t)i * 13);
    }
  }

  void storeBigStateTest() {
    PageManager *page_manager = lenv()->page_manager.get();
    uint32_t page_size = lenv()->config.page_size_bytes;

    page_manager->state->last_blob_page_id = page_size * 100;

    for (int i = 1; i <= 30000; i++) {
      if (i & 1) // only store every 2nd page to avoid collapsing
        page_manager->state->freelist.free_pages[page_size * i] = 1;
    }

    // store the state on disk
    page_manager->state->needs_flush = true;
    uint64_t page_id = page_manager->test_store_state();

    page_manager->flush_all_pages();
    page_manager->state->freelist.free_pages.clear();
    page_manager->state->last_blob_page_id = 0;

    page_manager->initialize(page_id);

    REQUIRE(page_manager->state->last_blob_page_id == page_size * 100);

    REQUIRE(15000 == page_manager->state->freelist.free_pages.size());
    for (int i = 1; i <= 30000; i++) {
      if (i & 1)
        REQUIRE(page_manager->state->freelist.free_pages[page_size * i] == 1);
    }

    REQUIRE(page_manager->state->page_count_page_manager == 4u);
  }

  void allocMultiBlobs() {
    PageManager *page_manager = lenv()->page_manager.get();
    uint32_t page_size = lenv()->config.page_size_bytes;

    Context c(lenv(), 0, 0);

    Page *head = page_manager->alloc_multiple_blob_pages(&c, 10);
    REQUIRE(head != 0);
    page_manager->del(&c, head, 10);

    Page *page1 = page_manager->alloc_multiple_blob_pages(&c, 2);
    REQUIRE(page1 != 0);
    REQUIRE(page1->address() == head->address());

    Page *page2 = page_manager->alloc_multiple_blob_pages(&c, 8);
    REQUIRE(page2 != 0);
    REQUIRE(page2->address() == page1->address() + page_size * 2);
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
  PageManagerFixture f(false, 16 * UPS_DEFAULT_PAGE_SIZE);
  f.cacheFullTest();
}

TEST_CASE("PageManager/storeStateTest", "")
{
  PageManagerFixture f(false, 16 * UPS_DEFAULT_PAGE_SIZE);
  f.storeStateTest();
}

TEST_CASE("PageManager/reclaimTest", "")
{
  PageManagerFixture f(false, 16 * UPS_DEFAULT_PAGE_SIZE);
  f.reclaimTest();
}

TEST_CASE("PageManager/issue60Test", "")
{
  PageManagerFixture f(false, 16 * UPS_DEFAULT_PAGE_SIZE);
  f.issue60Test();
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

} // namespace upscaledb
