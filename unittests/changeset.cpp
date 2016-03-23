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

