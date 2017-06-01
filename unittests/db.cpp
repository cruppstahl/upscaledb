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

#include "ups/upscaledb.h"

#include "4context/context.h"

#include "fixture.hpp"

namespace upscaledb {

struct DbFixture : BaseFixture {
  bool m_inmemory;
  ScopedPtr<Context> context;

  DbFixture(bool inmemory = false)
    : m_inmemory(inmemory) {
    require_create(inmemory ? UPS_IN_MEMORY : 0, nullptr,
                    UPS_ENABLE_DUPLICATE_KEYS, nullptr);
    context.reset(new Context(lenv(), 0, ldb()));
  }

  ~DbFixture() {
    context->changeset.clear();
    close();
  }

  void headerTest() {
    lenv()->header->set_magic('1', '2', '3', '4');
    REQUIRE(true == lenv()->header->verify_magic('1', '2', '3', '4'));

    lenv()->header->set_version(1, 2, 3, 4);
    REQUIRE((uint8_t)1 == lenv()->header->version(0));
    REQUIRE((uint8_t)2 == lenv()->header->version(1));
    REQUIRE((uint8_t)3 == lenv()->header->version(2));
    REQUIRE((uint8_t)4 == lenv()->header->version(3));
  }

  void defaultCompareTest() {
    ups_key_t key1 = {0}, key2 = {0};
    key1.data = (void *)"abc";
    key1.size = 3;
    key2.data = (void *)"abc";
    key2.size = 3;
    REQUIRE( 0 == btree_index()->compare_keys(&key1, &key2));
    key1.data = (void *)"ab";
    key1.size = 2;
    key2.data = (void *)"abc";
    key2.size = 3;
    REQUIRE(-1 == btree_index()->compare_keys(&key1, &key2));
    key1.data = (void *)"abc";
    key1.size = 3;
    key2.data = (void *)"bcd";
    key2.size = 3;
    REQUIRE(-1 == btree_index()->compare_keys(&key1, &key2));
    key1.data = (void *)"abc";
    key1.size = 3;
    key2.data = (void *)0;
    key2.size = 0;
    REQUIRE(+1 == btree_index()->compare_keys(&key1, &key2));
    key1.data = (void *)0;
    key1.size = 0;
    key2.data = (void *)"abc";
    key2.size = 3;
    REQUIRE(-1 == btree_index()->compare_keys(&key1, &key2));
  }

  void flushPageTest() {
    Page *page = nullptr;
    uint64_t address;
    uint8_t *p;

    PageManager *pm = lenv()->page_manager.get();

    REQUIRE((page = pm->alloc(context.get(), 0)));
    context->changeset.clear(); // unlock pages

    REQUIRE(ldb() == page->db());
    p = page->payload();
    for (int i = 0; i < 16; i++)
      p[i] = (uint8_t)i;
    page->set_dirty(true);
    address = page->address();
    page->flush();
    pm->state->cache.del(page);
    delete page;

    REQUIRE((page = pm->fetch(context.get(), address)));
    context->changeset.clear(); // unlock pages
    REQUIRE(page != 0);
    REQUIRE(address == page->address());
    pm->state->cache.del(page);
    delete page;
  }
};

TEST_CASE("Db/headerTest", "")
{
  DbFixture f;
  f.headerTest();
}

TEST_CASE("Db/defaultCompareTest", "")
{
  DbFixture f;
  f.defaultCompareTest();
}

TEST_CASE("Db/flushPageTest", "")
{
  DbFixture f;
  f.flushPageTest();
}


TEST_CASE("Db/inmem/headerTest", "")
{
  DbFixture f(true);
  f.headerTest();
}

TEST_CASE("Db/inmem/defaultCompareTest", "")
{
  DbFixture f(true);
  f.defaultCompareTest();
}

} // namespace upscaledb
