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

#include "2page/page.h"
#include "3changeset/changeset.h"
#include "4db/db.h"

using namespace hamsterdb;

struct ChangesetFixture {
  ChangesetFixture() {
    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"),
                HAM_ENABLE_RECOVERY, 0644, 0));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
  }

  ~ChangesetFixture() {
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  ham_db_t *m_db;
  ham_env_t *m_env;
};

TEST_CASE("Changeset/addPages",
          "Basic test of the Changeset internals")
{
  ChangesetFixture f;
  Changeset ch((LocalEnvironment *)f.m_env);
  Page *page[3];
  for (int i = 0; i < 3; i++) {
    page[i] = new Page((LocalEnvironment *)f.m_env);
    page[i]->set_address(1024 * i);
  }
  for (int i = 0; i < 3; i++)
    ch.add_page(page[i]);
  REQUIRE(page[1] ==
      page[2]->get_next(Page::kListChangeset));
  REQUIRE(page[0] ==
        page[1]->get_next(Page::kListChangeset));
  REQUIRE((Page *)NULL ==
        page[0]->get_next(Page::kListChangeset));
  REQUIRE(page[1] ==
        page[0]->get_previous(Page::kListChangeset));
  REQUIRE(page[2] ==
        page[1]->get_previous(Page::kListChangeset));
  REQUIRE((Page *)NULL ==
        page[2]->get_previous(Page::kListChangeset));
  for (int i = 0; i < 3; i++)
    delete page[i];
}

TEST_CASE("Changeset/getPages",
          "Basic test of the Changeset internals")
{
  ChangesetFixture f;
  Changeset ch((LocalEnvironment *)f.m_env);
  Page *page[3];
  for (int i = 0; i < 3; i++) {
    page[i] = new Page((LocalEnvironment *)f.m_env);
    page[i]->set_address(1024 * i);
  }
  for (int i = 0; i < 3; i++)
    ch.add_page(page[i]);

  for (int i = 0; i < 3; i++)
    REQUIRE(page[i] == ch.get_page(page[i]->get_address()));
  REQUIRE((Page *)NULL == ch.get_page(999));

  for (int i = 0; i < 3; i++)
    delete page[i];
}

TEST_CASE("Changeset/clear",
          "Basic test of the Changeset internals")
{
  ChangesetFixture f;
  Changeset ch((LocalEnvironment *)f.m_env);
  Page *page[3];
  for (int i = 0; i < 3; i++) {
    page[i] = new Page((LocalEnvironment *)f.m_env);
    page[i]->set_address(1024 * i);
  }
  for (int i = 0; i < 3; i++)
    ch.add_page(page[i]);

  REQUIRE(false == ch.is_empty());
  ch.clear();
  REQUIRE(true == ch.is_empty());

  for (int i = 0; i < 3; i++)
    REQUIRE((Page *)NULL == ch.get_page(page[i]->get_address()));

  for (int i = 0; i < 3; i++)
    delete page[i];
}

