/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
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

#include "../src/config.h"

#include "3rdparty/catch/catch.hpp"

#include "utils.h"
#include "os.hpp"

#include "../src/db.h"
#include "../src/version.h"
#include "../src/page.h"
#include "../src/env.h"
#include "../src/btree_index.h"
#include "../src/btree_node.h"
#include "../src/page_manager.h"

using namespace hamsterdb;

struct BtreeInsertFixture {
  ham_db_t *m_db;
  ham_env_t *m_env;
  LocalEnvironment *m_environ;

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

    os::unlink(Utils::opath(".test"));
    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"), 0, 0644, &p1[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, 0, &p2[0]));
    m_environ = (LocalEnvironment *)m_env;
  }

  ~BtreeInsertFixture() {
    if (m_env)
	  REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  Page *fetch_page(ham_u64_t address) {
    LocalDatabase *db = (LocalDatabase *)m_db;
    PageManager *pm = db->get_local_env()->get_page_manager();
    return (pm->fetch_page(db, address));
  }

  void defaultPivotTest() {
    ham_key_t key = {};
    ham_record_t rec = {};

    char buffer[80] = {0};
    for (int i = 11; i >= 0; i--) {
      *(int *)&buffer[0] = i;
      key.data = &buffer[0];
      key.size = sizeof(buffer);

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
    REQUIRE((page = fetch_page(m_environ->get_page_size() * 1)));
    REQUIRE((Page::kTypeBindex & page->get_type()));
    node = PBtreeNode::from_page(page);
    REQUIRE(7 == node->get_count());

    REQUIRE((page = fetch_page(m_environ->get_page_size() * 2)));
    REQUIRE((Page::kTypeBindex & page->get_type()));
    node = PBtreeNode::from_page(page);
    REQUIRE(5 == node->get_count());

    REQUIRE((page = fetch_page(m_environ->get_page_size() * 3)));
    REQUIRE((Page::kTypeBindex & page->get_type()));
    node = PBtreeNode::from_page(page);
    REQUIRE(1 == node->get_count());
  }

  void defaultLatePivotTest() {
    ham_key_t key = {};
    ham_record_t rec = {};

    char buffer[80] = {0};
    for (int i = 0; i < 11; i++) {
      *(int *)&buffer[0] = i;
      key.data = &buffer[0];
      key.size = sizeof(buffer);

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
    REQUIRE((page = fetch_page(m_environ->get_page_size() * 1)));
    REQUIRE((unsigned)Page::kTypeBindex == page->get_type());
    node = PBtreeNode::from_page(page);
    REQUIRE(8 == node->get_count());

    REQUIRE((page = fetch_page(m_environ->get_page_size() * 2)));
    REQUIRE((unsigned)Page::kTypeBindex == page->get_type());
    node = PBtreeNode::from_page(page);
    REQUIRE(3 == node->get_count());

    REQUIRE((page = fetch_page(m_environ->get_page_size() * 3)));
    REQUIRE((unsigned)Page::kTypeBroot == page->get_type());
    node = PBtreeNode::from_page(page);
    REQUIRE(1 == node->get_count());
  }

  void sequentialInsertPivotTest() {
    ham_key_t key = {};
    ham_record_t rec = {};

    char buffer[80] = {0};
    for (int i = 0; i < 11; i++) {
      *(int *)&buffer[0] = i;
      key.data = &buffer[0];
      key.size = sizeof(buffer);

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
    REQUIRE((page = fetch_page(m_environ->get_page_size() * 1)));
    REQUIRE((unsigned)Page::kTypeBindex == page->get_type());
    node = PBtreeNode::from_page(page);
    REQUIRE(8 == node->get_count());

    REQUIRE((page = fetch_page(m_environ->get_page_size() * 2)));
    REQUIRE((unsigned)Page::kTypeBindex == page->get_type());
    node = PBtreeNode::from_page(page);
    REQUIRE(3 == node->get_count());

    REQUIRE((page = fetch_page(m_environ->get_page_size() * 3)));
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

