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
#include <cstring>
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/version.h"
#include "../src/page.h"
#include "../src/env.h"
#include "../src/btree.h"
#include "../src/btree_node.h"
#include "os.hpp"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;
using namespace hamsterdb;

class BtreeInsertTest : public hamsterDB_fixture {
  define_super(hamsterDB_fixture);

public:
  BtreeInsertTest()
    : hamsterDB_fixture("BtreeInsertTest"), m_db(0)  {
    testrunner::get_instance()->register_fixture(this);
    BFC_REGISTER_TEST(BtreeInsertTest, defaultPivotTest);
    BFC_REGISTER_TEST(BtreeInsertTest, defaultLatePivotTest);
    BFC_REGISTER_TEST(BtreeInsertTest, sequentialInsertPivotTest);
  }

protected:
  ham_db_t *m_db;
  ham_env_t *m_env;
  Environment *m_environ;

public:
  virtual void setup() {
    __super::setup();

    ham_parameter_t p1[] = {
      { HAM_PARAM_PAGESIZE, 1024 },
      { 0, 0 }
    };
    ham_parameter_t p2[] = {
      { HAM_PARAM_KEYSIZE, 80 },
      { 0, 0 }
    };

    os::unlink(BFC_OPATH(".test"));
    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"), 0, 0644, &p1[0]));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, &p2[0]));
    m_environ = (Environment *)m_env;
  }

  virtual void teardown() {
    __super::teardown();

    if (m_env)
	  BFC_ASSERT_EQUAL(0, ham_env_close(m_env, 0));
  }

  void defaultPivotTest() {
    ham_key_t key = {};
    ham_record_t rec = {};

    for (int i = 11; i >= 0; i--) {
      key.data = &i;
      key.size = sizeof(i);

      BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, 0, &key, &rec, 0));
    }

    /* now verify that the index has 3 pages - root and two pages in
     * level 1 - both are 50% full
     *
     * the first page is the old root page, which became an index
     * page after the split
     */
    Page *page;
    PBtreeNode *node;
    BFC_ASSERT_EQUAL(0,
        ((Database *)m_db)->fetch_page(&page, m_environ->get_pagesize() * 1));
    BFC_ASSERT(Page::TYPE_B_INDEX & page->get_type());
    node = PBtreeNode::from_page(page);
    BFC_ASSERT_EQUAL(7, node->get_count());

    BFC_ASSERT_EQUAL(0,
        ((Database *)m_db)->fetch_page(&page, m_environ->get_pagesize() * 2));
    BFC_ASSERT(Page::TYPE_B_INDEX & page->get_type());
    node = PBtreeNode::from_page(page);
    BFC_ASSERT_EQUAL(5, node->get_count());

    BFC_ASSERT_EQUAL(0,
        ((Database *)m_db)->fetch_page(&page, m_environ->get_pagesize() * 3));
    BFC_ASSERT(Page::TYPE_B_INDEX & page->get_type());
    node = PBtreeNode::from_page(page);
    BFC_ASSERT_EQUAL(1, node->get_count());
  }

  void defaultLatePivotTest() {
    ham_key_t key = {};
    ham_record_t rec = {};

    for (int i = 0; i < 11; i++) {
      key.data = &i;
      key.size = sizeof(i);

      BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, 0, &key, &rec, 0));
    }

    /* now verify that the index has 3 pages - root and two pages in
     * level 1 - both are 50% full
     *
     * the first page is the old root page, which became an index
     * page after the split
     */
    Page *page;
    PBtreeNode *node;
    BFC_ASSERT_EQUAL(0,
        ((Database *)m_db)->fetch_page(&page, m_environ->get_pagesize() * 1));
    BFC_ASSERT_EQUAL((unsigned)Page::TYPE_B_INDEX, page->get_type());
    node = PBtreeNode::from_page(page);
    BFC_ASSERT_EQUAL(8, node->get_count());

    BFC_ASSERT_EQUAL(0,
        ((Database *)m_db)->fetch_page(&page, m_environ->get_pagesize() * 2));
    BFC_ASSERT_EQUAL((unsigned)Page::TYPE_B_INDEX, page->get_type());
    node = PBtreeNode::from_page(page);
    BFC_ASSERT_EQUAL(3, node->get_count());

    BFC_ASSERT_EQUAL(0,
        ((Database *)m_db)->fetch_page(&page, m_environ->get_pagesize() * 3));
    BFC_ASSERT_EQUAL((unsigned)Page::TYPE_B_ROOT, page->get_type());
    node = PBtreeNode::from_page(page);
    BFC_ASSERT_EQUAL(1, node->get_count());
  }

  void sequentialInsertPivotTest() {
    ham_key_t key = {};
    ham_record_t rec = {};

    for (int i = 0; i < 11; i++) {
      key.data = &i;
      key.size = sizeof(i);

      BFC_ASSERT_EQUAL(0, ham_db_insert(m_db, 0, &key, &rec, 0));
    }

    /* now verify that the index has 3 pages - root and two pages in
     * level 1 - both are 50% full
     *
     * the first page is the old root page, which became an index
     * page after the split
     */
    Page *page;
    PBtreeNode *node;
    BFC_ASSERT_EQUAL(0,
        ((Database *)m_db)->fetch_page(&page, m_environ->get_pagesize() * 1));
    BFC_ASSERT_EQUAL((unsigned)Page::TYPE_B_INDEX, page->get_type());
    node = PBtreeNode::from_page(page);
    BFC_ASSERT_EQUAL(8, node->get_count());

    BFC_ASSERT_EQUAL(0,
        ((Database *)m_db)->fetch_page(&page, m_environ->get_pagesize() * 2));
    BFC_ASSERT_EQUAL((unsigned)Page::TYPE_B_INDEX, page->get_type());
    node = PBtreeNode::from_page(page);
    BFC_ASSERT_EQUAL(3, node->get_count());

    BFC_ASSERT_EQUAL(0,
        ((Database *)m_db)->fetch_page(&page, m_environ->get_pagesize() * 3));
    BFC_ASSERT_EQUAL((unsigned)Page::TYPE_B_ROOT, page->get_type());
    node = PBtreeNode::from_page(page);
    BFC_ASSERT_EQUAL(1, node->get_count());
  }
};

BFC_REGISTER_FIXTURE(BtreeInsertTest);

