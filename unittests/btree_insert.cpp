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
#include "os.hpp"

#include "../src/db.h"
#include "../src/version.h"
#include "../src/page.h"
#include "../src/env.h"
#include "../src/btree.h"
#include "../src/btree_node.h"
#include "../src/page_manager.h"

using namespace hamsterdb;

struct BtreeInsertFixture {
  ham_db_t *m_db;
  ham_env_t *m_env;
  Environment *m_environ;

  BtreeInsertFixture()
    : m_db(0), m_env(0), m_environ(0) {
    ham_parameter_t p1[] = {
      { HAM_PARAM_PAGESIZE, 1024 },
      { 0, 0 }
    };
    ham_parameter_t p2[] = {
      { HAM_PARAM_KEYSIZE, 80 },
      { 0, 0 }
    };

    os::unlink(Globals::opath(".test"));
    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"), 0, 0644, &p1[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, 0, &p2[0]));
    m_environ = (Environment *)m_env;
  }

  ~BtreeInsertFixture() {
    if (m_env)
	  REQUIRE(0 == ham_env_close(m_env, 0));
  }

  ham_status_t fetch_page(Page **page, ham_u64_t address) {
    LocalDatabase *db = (LocalDatabase *)m_db;
    PageManager *pm = db->get_env()->get_page_manager();
    return (pm->fetch_page(page, db, address));
  }

  void defaultPivotTest() {
    ham_key_t key = {};
    ham_record_t rec = {};

    for (int i = 11; i >= 0; i--) {
      key.data = &i;
      key.size = sizeof(i);

      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    }

    /* now verify that the index has 3 pages - root and two pages in
     * level 1 - both are 50% full
     *
     * the first page is the old root page, which became an index
     * page after the split
     */
    Page *page;
    PBtreeNode *node;
    REQUIRE(0 == fetch_page(&page, m_environ->get_pagesize() * 1));
    REQUIRE((Page::kTypeBindex & page->get_type()));
    node = PBtreeNode::from_page(page);
    REQUIRE(7 == node->get_count());

    REQUIRE(0 == fetch_page(&page, m_environ->get_pagesize() * 2));
    REQUIRE((Page::kTypeBindex & page->get_type()));
    node = PBtreeNode::from_page(page);
    REQUIRE(5 == node->get_count());

    REQUIRE(0 == fetch_page(&page, m_environ->get_pagesize() * 3));
    REQUIRE((Page::kTypeBindex & page->get_type()));
    node = PBtreeNode::from_page(page);
    REQUIRE(1 == node->get_count());
  }

  void defaultLatePivotTest() {
    ham_key_t key = {};
    ham_record_t rec = {};

    for (int i = 0; i < 11; i++) {
      key.data = &i;
      key.size = sizeof(i);

      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    }

    /* now verify that the index has 3 pages - root and two pages in
     * level 1 - both are 50% full
     *
     * the first page is the old root page, which became an index
     * page after the split
     */
    Page *page;
    PBtreeNode *node;
    REQUIRE(0 == fetch_page(&page, m_environ->get_pagesize() * 1));
    REQUIRE((unsigned)Page::kTypeBindex == page->get_type());
    node = PBtreeNode::from_page(page);
    REQUIRE(8 == node->get_count());

    REQUIRE(0 == fetch_page(&page, m_environ->get_pagesize() * 2));
    REQUIRE((unsigned)Page::kTypeBindex == page->get_type());
    node = PBtreeNode::from_page(page);
    REQUIRE(3 == node->get_count());

    REQUIRE(0 == fetch_page(&page, m_environ->get_pagesize() * 3));
    REQUIRE((unsigned)Page::kTypeBroot == page->get_type());
    node = PBtreeNode::from_page(page);
    REQUIRE(1 == node->get_count());
  }

  void sequentialInsertPivotTest() {
    ham_key_t key = {};
    ham_record_t rec = {};

    for (int i = 0; i < 11; i++) {
      key.data = &i;
      key.size = sizeof(i);

      REQUIRE(0 == ham_db_insert(m_db, 0, &key, &rec, 0));
    }

    /* now verify that the index has 3 pages - root and two pages in
     * level 1 - both are 50% full
     *
     * the first page is the old root page, which became an index
     * page after the split
     */
    Page *page;
    PBtreeNode *node;
    REQUIRE(0 == fetch_page(&page, m_environ->get_pagesize() * 1));
    REQUIRE((unsigned)Page::kTypeBindex == page->get_type());
    node = PBtreeNode::from_page(page);
    REQUIRE(8 == node->get_count());

    REQUIRE(0 == fetch_page(&page, m_environ->get_pagesize() * 2));
    REQUIRE((unsigned)Page::kTypeBindex == page->get_type());
    node = PBtreeNode::from_page(page);
    REQUIRE(3 == node->get_count());

    REQUIRE(0 == fetch_page(&page, m_environ->get_pagesize() * 3));
    REQUIRE((unsigned)Page::kTypeBroot == page->get_type());
    node = PBtreeNode::from_page(page);
    REQUIRE(1 == node->get_count());
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

