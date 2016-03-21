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

#include "utils.h"

#include "2page/page.h"
#include "3changeset/changeset.h"
#include "4db/db.h"
#include "4env/env_local.h"

namespace upscaledb {

struct ChangesetFixture {
  ChangesetFixture() {
    REQUIRE(0 == ups_env_create(&m_env, Utils::opath(".test"),
                UPS_ENABLE_TRANSACTIONS, 0644, 0));
    REQUIRE(0 == ups_env_create_db(m_env, &m_db, 1, 0, 0));
  }

  ~ChangesetFixture() {
    REQUIRE(0 == ups_env_close(m_env, UPS_AUTO_CLEANUP));
  }

  ups_db_t *m_db;
  ups_env_t *m_env;

  void addPages() {
    Changeset ch((LocalEnv *)m_env);
    Page *page[3];
    for (int i = 0; i < 3; i++) {
      page[i] = new Page(((LocalEnv *)m_env)->device.get());
      page[i]->set_address(1024 * (i + 1));
    }
    for (int i = 0; i < 3; i++)
      ch.put(page[i]);

    REQUIRE(page[1] == page[2]->next(Page::kListChangeset));
    REQUIRE(page[0] == page[1]->next(Page::kListChangeset));
    REQUIRE((Page *)NULL == page[0]->next(Page::kListChangeset));
    REQUIRE(page[1] == page[0]->previous(Page::kListChangeset));
    REQUIRE(page[2] == page[1]->previous(Page::kListChangeset));
    REQUIRE((Page *)NULL == page[2]->previous(Page::kListChangeset));

	ch.clear();

    for (int i = 0; i < 3; i++)
      delete page[i];
  }

  void getPages() {
    Changeset ch((LocalEnv *)m_env);
    Page *page[3];
    for (int i = 0; i < 3; i++) {
      page[i] = new Page(((LocalEnv *)m_env)->device.get());
      page[i]->set_address(1024 * (i + 1));
    }
    for (int i = 0; i < 3; i++)
      ch.put(page[i]);
  
    for (int i = 0; i < 3; i++)
      REQUIRE(page[i] == ch.get(page[i]->address()));
    REQUIRE((Page *)NULL == ch.get(999));

	ch.clear();

    for (int i = 0; i < 3; i++)
      delete page[i];
  }
};

TEST_CASE("Changeset/addPages",
          "Basic test of the Changeset internals")
{
  ChangesetFixture f;
  f.addPages();
}

TEST_CASE("Changeset/getPages",
          "Basic test of the Changeset internals")
{
  ChangesetFixture f;
  f.getPages();
}

TEST_CASE("Changeset/clear",
          "Basic test of the Changeset internals")
{
  ChangesetFixture f;
  Changeset ch((LocalEnv *)f.m_env);
  Page *page[3];
  for (int i = 0; i < 3; i++) {
    page[i] = new Page(((LocalEnv *)f.m_env)->device.get());
    page[i]->set_address(1024 * i);
  }
  for (int i = 0; i < 3; i++)
    ch.put(page[i]);

  REQUIRE(false == ch.is_empty());
  ch.clear();
  REQUIRE(true == ch.is_empty());

  for (int i = 0; i < 3; i++)
    REQUIRE((Page *)NULL == ch.get(page[i]->address()));

  ch.clear();

  for (int i = 0; i < 3; i++)
    delete page[i];
}

} // namespace upscaledb

