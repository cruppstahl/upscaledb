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

#include "4context/context.h"

#include "os.hpp"
#include "fixture.hpp"

using namespace upscaledb;

struct BtreeInsertFixture : BaseFixture {
  ScopedPtr<Context> context;

  BtreeInsertFixture() {
    ups_parameter_t p1[] = {
      { UPS_PARAM_PAGESIZE, 1024 },
      { 0, 0 }
    };
    ups_parameter_t p2[] = {
      { UPS_PARAM_KEYSIZE, 80 },
      { 0, 0 }
    };

    require_create(0, p1, 0, p2);
    context.reset(new Context(lenv(), 0, 0));
  }

  ~BtreeInsertFixture() {
    context->changeset.clear();
    close();
  }

  Page *fetch_page(uint64_t address) {
    return (lenv()->page_manager->fetch(context.get(), address));
  }

  void defaultPivotTest() {
    ups_key_t key = {};
    ups_record_t rec = {};

    char buffer[80] = {0};
    for (int i = 11; i >= 0; i--) {
      *(int *)&buffer[0] = i;
      key.data = &buffer[0];
      key.size = sizeof(buffer);

      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    }

    /* now verify that the index has 3 pages - root and two pages in
     * level 1 - both are 50% full
     *
     * the first page is the old root page, which became an index
     * page after the split
     */
    Page *page;
    PBtreeNode *node;
    REQUIRE((page = fetch_page(lenv()->config.page_size_bytes * 1)));
    REQUIRE((Page::kTypeBindex & page->type()));
    node = PBtreeNode::from_page(page);
    REQUIRE(7 == node->length());

    REQUIRE((page = fetch_page(lenv()->config.page_size_bytes * 2)));
    REQUIRE((Page::kTypeBindex & page->type()));
    node = PBtreeNode::from_page(page);
    REQUIRE(5 == node->length());

    REQUIRE((page = fetch_page(lenv()->config.page_size_bytes * 3)));
    REQUIRE((Page::kTypeBindex & page->type()));
    node = PBtreeNode::from_page(page);
    REQUIRE(1 == node->length());
  }

  void defaultLatePivotTest() {
    ups_key_t key = {};
    ups_record_t rec = {};

    char buffer[80] = {0};
    for (int i = 0; i < 12; i++) {
      *(int *)&buffer[0] = i;
      key.data = &buffer[0];
      key.size = sizeof(buffer);

      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    }

    /* now verify that the index has 3 pages - root and two pages in
     * level 1 - both are 50% full
     *
     * the first page is the old root page, which became an index
     * page after the split
     */
    Page *page = nullptr;
    PBtreeNode *node = nullptr;
    REQUIRE((page = fetch_page(lenv()->config.page_size_bytes * 1)));
    REQUIRE((unsigned)Page::kTypeBindex == page->type());
    node = PBtreeNode::from_page(page);
    REQUIRE(10 == node->length());

    REQUIRE((page = fetch_page(lenv()->config.page_size_bytes * 2)));
    REQUIRE((unsigned)Page::kTypeBindex == page->type());
    node = PBtreeNode::from_page(page);
    REQUIRE(2 == node->length());

    REQUIRE((page = fetch_page(lenv()->config.page_size_bytes * 3)));
    REQUIRE((unsigned)Page::kTypeBroot == page->type());
    node = PBtreeNode::from_page(page);
    REQUIRE(1 == node->length());
  }

  void sequentialInsertPivotTest() {
    ups_key_t key = {};
    ups_record_t rec = {};

    char buffer[80] = {0};
    for (int i = 0; i < 12; i++) {
      *(int *)&buffer[0] = i;
      key.data = &buffer[0];
      key.size = sizeof(buffer);

      REQUIRE(0 == ups_db_insert(db, 0, &key, &rec, 0));
    }

    /* now verify that the index has 3 pages - root and two pages in
     * level 1 - both are 50% full
     *
     * the first page is the old root page, which became an index
     * page after the split
     */
    Page *page = nullptr;
    PBtreeNode *node = nullptr;
    REQUIRE((page = fetch_page(lenv()->config.page_size_bytes * 1)));
    REQUIRE((unsigned)Page::kTypeBindex == page->type());
    node = PBtreeNode::from_page(page);
    REQUIRE(10 == node->length());

    REQUIRE((page = fetch_page(lenv()->config.page_size_bytes * 2)));
    REQUIRE((unsigned)Page::kTypeBindex == page->type());
    node = PBtreeNode::from_page(page);
    REQUIRE(2 == node->length());

    REQUIRE((page = fetch_page(lenv()->config.page_size_bytes * 3)));
    REQUIRE((unsigned)Page::kTypeBroot == page->type());
    node = PBtreeNode::from_page(page);
    REQUIRE(1 == node->length());
  }
};

TEST_CASE("BtreeInsert/defaultPivotTest", "")
{
  BtreeInsertFixture f;
  f.defaultPivotTest();
}

TEST_CASE("BtreeInsert/defaultLatePivotTest", "")
{
  BtreeInsertFixture f;
  f.defaultLatePivotTest();
}

TEST_CASE("BtreeInsert/sequentialInsertPivotTest", "")
{
  BtreeInsertFixture f;
  f.sequentialInsertPivotTest();
}

