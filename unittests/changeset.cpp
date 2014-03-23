/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

#include "../src/config.h"

#include "3rdparty/catch/catch.hpp"

#include "globals.h"

#include "../src/changeset.h"
#include "../src/page.h"
#include "../src/db.h"

using namespace hamsterdb;

struct ChangesetFixture {
  ChangesetFixture() {
    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"),
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

