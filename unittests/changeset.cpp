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

#include "fixture.hpp"

#include "3changeset/changeset.h"

namespace upscaledb {

struct ChangesetProxy {
  ChangesetProxy(LocalEnv *env)
    : changeset(env) {
  }

  ~ChangesetProxy() {
    changeset.clear();
  }

  ChangesetProxy &put(PageProxy &pp) {
    changeset.put(pp.page);
    return *this;
  }

  ChangesetProxy &require_get(uint64_t address, PageProxy &pp) {
    return require_get(address, pp.page);
  }

  ChangesetProxy &require_get(uint64_t address, Page *page) {
    REQUIRE(changeset.get(address) == page);
    return *this;
  }

  ChangesetProxy &require_empty(bool empty = true) {
    REQUIRE(changeset.is_empty() == empty);
    return *this;
  }

  ChangesetProxy &clear() {
    changeset.clear();
    return *this;
  }

  Changeset changeset;
};

struct ChangesetFixture : BaseFixture {
  ChangesetFixture() {
    require_create(UPS_ENABLE_TRANSACTIONS);
  }

  void addPages() {
    PageProxy pages[3]; // allocate this first, otherwise ~ChangesetProxy fails
    ChangesetProxy cp(lenv());

    for (int i = 0; i < 3; i++) {
      pages[i].allocate(lenv())
              .set_address(1024 * (i + 1));
      cp.put(pages[i]);
    }

    REQUIRE(pages[1].page == pages[2].page->next(Page::kListChangeset));
    REQUIRE(pages[0].page == pages[1].page->next(Page::kListChangeset));
    REQUIRE(nullptr == pages[0].page->next(Page::kListChangeset));
    REQUIRE(pages[1].page == pages[0].page->previous(Page::kListChangeset));
    REQUIRE(pages[2].page == pages[1].page->previous(Page::kListChangeset));
    REQUIRE(nullptr == pages[2].page->previous(Page::kListChangeset));
  }

  void getPages() {
    PageProxy pages[3]; // allocate this first, otherwise ~ChangesetProxy fails
    ChangesetProxy ch(lenv());

    for (int i = 0; i < 3; i++) {
      pages[i].allocate(lenv())
              .set_address(1024 * (i + 1));
      ch.put(pages[i]);
    }
  
    ch.require_get(pages[0].page->address(), pages[0])
      .require_get(pages[1].page->address(), pages[1])
      .require_get(pages[2].page->address(), pages[2])
      .require_get(999, nullptr);
  }

  void clear() {
    PageProxy pages[3]; // allocate this first, otherwise ~ChangesetProxy fails
    ChangesetProxy ch(lenv());

    for (int i = 0; i < 3; i++) {
      pages[i].allocate(lenv())
              .set_address(1024 * i);
      ch.put(pages[i]);
    }

    ch.require_empty(false)
      .clear()
      .require_empty(true)
      .require_get(pages[0].page->address(), nullptr)
      .require_get(pages[1].page->address(), nullptr)
      .require_get(pages[2].page->address(), nullptr);
  }
};

TEST_CASE("Changeset/addPages")
{
  ChangesetFixture f;
  f.addPages();
}

TEST_CASE("Changeset/getPages")
{
  ChangesetFixture f;
  f.getPages();
}

TEST_CASE("Changeset/clear")
{
  ChangesetFixture f;
  f.clear();
}

} // namespace upscaledb

