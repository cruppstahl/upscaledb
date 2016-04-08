/*
 * Copyright (C) 2005-2016 Christoph Rupp (chris@crupp.de).
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
    Page *page;
    PBtreeNode *node;
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
    Page *page;
    PBtreeNode *node;
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

