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

#include "2page/page.h"
#include "2device/device.h"
#include "4db/db.h"
#include "4env/env_local.h"
#include "4txn/txn.h"

#include "utils.h"
#include "os.hpp"
#include "fixture.hpp"

using namespace upscaledb;

struct PageFixture : public BaseFixture {
  PageFixture(uint32_t env_flags = 0) {
    require_create(env_flags);
  }

  void multipleAllocFreeTest() {
    uint32_t page_size = lenv()->config.page_size_bytes;

    for (uint32_t i = 0; i < 10; i++) {
      PageProxy pp(lenv());
      pp.require_alloc(0, page_size);
      /* i+2 since we need 1 page for the header page and one page
       * for the root page */
      if (!is_in_memory())
        pp.require_address((i + 2) * page_size);
    }
  }

  void fetchFlushTest() {
    uint32_t page_size = lenv()->config.page_size_bytes;

    PageProxy pp(lenv());
    pp.require_alloc(0, page_size)
      .require_address(page_size * 2);

    // patch the size, otherwise we run into asserts

    ::memset(pp.page->payload(), 0x13,
                    page_size - Page::kSizeofPersistentHeader);
    pp.set_dirty()
      .require_flush()
      .require_dirty(false);

    PageProxy tmp(lenv());
    tmp.require_fetch(page_size * 2)
       .require_data(pp.page->data(), page_size);
  }
};

TEST_CASE("Page/newDelete", "")
{
  PageFixture f;
  PageProxy pp(f.lenv()->device.get());
  REQUIRE(pp.page != 0);
}

TEST_CASE("Page/allocFree", "")
{
  PageFixture f;
  PageProxy pp(f.lenv()->device.get());
  pp.require_alloc(0, 1024);
}

TEST_CASE("Page/multipleAllocFree", "")
{
  PageFixture f;
  f.multipleAllocFreeTest();
}

TEST_CASE("Page/fetchFlush", "")
{
  PageFixture f;
  f.fetchFlushTest();
}

TEST_CASE("Page/nommap/newDelete", "")
{
  PageFixture f(UPS_DISABLE_MMAP);
  PageProxy pp(f.lenv()->device.get());
  REQUIRE(pp.page != 0);
}

TEST_CASE("Page/nommap/allocFree", "")
{
  PageFixture f(UPS_DISABLE_MMAP);
  PageProxy pp(f.lenv()->device.get());
  pp.require_alloc(0, 1024);
}

TEST_CASE("Page/nommap/multipleAllocFree", "")
{
  PageFixture f(UPS_DISABLE_MMAP);
  f.multipleAllocFreeTest();
}

TEST_CASE("Page/nommap/fetchFlush", "")
{
  PageFixture f(UPS_DISABLE_MMAP);
  f.fetchFlushTest();
}


TEST_CASE("Page/inmem/newDelete", "")
{
  PageFixture f(UPS_IN_MEMORY);
  PageProxy pp(f.lenv()->device.get());
  REQUIRE(pp.page != 0);
}

TEST_CASE("Page/inmem/allocFree", "")
{
  PageFixture f(UPS_IN_MEMORY);
  PageProxy pp(f.lenv()->device.get());
  pp.require_alloc(0, 1024);
}

TEST_CASE("Page/inmem/multipleAllocFree", "")
{
  PageFixture f(UPS_IN_MEMORY);
  f.multipleAllocFreeTest();
}

