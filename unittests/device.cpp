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

#include "2device/device.h"

#include "os.hpp"
#include "fixture.hpp"

using namespace upscaledb;

struct DeviceFixture : BaseFixture {
  DeviceFixture(bool inmemory) {
    require_create(inmemory ? UPS_IN_MEMORY : 0);
  }

  void createCloseTest() {
    Device *dev = device();
    REQUIRE(true == dev->is_open());
    dev->close();
    REQUIRE(false == dev->is_open());
    dev->open();
    REQUIRE(true == dev->is_open());
  }

  void openCloseTest() {
    DeviceProxy dp(lenv());
    dp.require_open(true)
      .close()
      .require_open(false)
      .open()
      .require_open(true)
      .close()
      .require_open(false)
      .open()
      .require_open(true);
  }

  void allocTest() {
    int i;
    uint64_t address;
    Device *dev = device();

    REQUIRE(true == dev->is_open());
    for (i = 0; i < 10; i++) {
      address = dev->alloc(1024);
      REQUIRE(address == (lenv()->config.page_size_bytes * 2) + 1024 * i);
    }
  }

  void allocFreeTest() {
    PageProxy pp(lenv(), ldb());
    DeviceProxy dp(lenv());

    dp.require_open()
      .alloc_page(pp);
    REQUIRE(pp.page->data());
    dp.free_page(pp);
  }

  void flushTest() {
    DeviceProxy dp(lenv());

    dp.require_open()
      .require_flush()
      .require_open();
  }

  void mmapUnmapTest() {
    std::vector<PageProxy *> pages;
    for (int i = 0; i < 10; i++)
      pages.push_back(new PageProxy(lenv(), ldb()));
    uint32_t page_size = UPS_DEFAULT_PAGE_SIZE;
    std::vector<uint8_t> temp(page_size);

    DeviceProxy dp(lenv());
    dp.require_open()
      .require_truncate(page_size * 10);

    for (int i = 0; i < 10; i++) {
      pages[i]->set_address(i * page_size);
      dp.require_read_page(*pages[i], i * page_size);
    }
    for (int i = 0; i < 10; i++) {
      ::memset(pages[i]->page->raw_payload(), i, page_size);
      pages[i]->set_dirty();
    }
    for (int i = 0; i < 10; i++)
      pages[i]->require_flush();
    for (int i = 0; i < 10; i++) {
      std::fill(temp.begin(), temp.end(), (uint8_t)i);
      dp.free_page(*pages[i])
        .require_read_page(*pages[i], i * page_size);
      pages[i]->require_payload(temp.data(),
                              page_size - Page::kSizeofPersistentHeader);
    }
    for (int i = 0; i < 10; i++) {
      dp.free_page(*pages[i]);
      delete pages[i];
    }
  }

  void readWriteTest() {
    int i;
    uint32_t page_size = UPS_DEFAULT_PAGE_SIZE;
    std::vector<std::vector<uint8_t>> buffers(10);
    std::vector<uint8_t> temp(page_size);

    EnvConfig &cfg = const_cast<EnvConfig &>(lenv()->config);
    cfg.flags |= UPS_DISABLE_MMAP;

    DeviceProxy dp(lenv());
    dp.require_open()
      .require_truncate(page_size * 10);

    for (uint8_t i = 0; i < 10; i++) {
      buffers[i].resize(page_size);
      dp.require_read(i * page_size, buffers[i].data(), page_size);
    }
    for (uint8_t i = 0; i < 10; i++)
      std::fill(buffers[i].begin(), buffers[i].end(), i);
    for (uint8_t i = 0; i < 10; i++)
      dp.require_write(i * page_size, buffers[i].data(), page_size);
    for (uint8_t i = 0; i < 10; i++) {
      dp.require_read(i * page_size, buffers[i].data(), page_size);
      std::fill(temp.begin(), temp.end(), i);
      REQUIRE(0 == ::memcmp(buffers[i].data(), temp.data(), page_size));
    }
  }

  void readWritePageTest() {
    PageProxy pages[2] = {{lenv(), ldb()}, {lenv(), ldb()}};
    uint32_t page_size = UPS_DEFAULT_PAGE_SIZE;

    EnvConfig &cfg = const_cast<EnvConfig &>(lenv()->config);
    cfg.flags |= UPS_DISABLE_MMAP;

    DeviceProxy dp(lenv());
    dp.require_open()
      .require_truncate(page_size * 2);

    for (uint8_t i = 0; i < 2; i++) {
      pages[i].set_address(page_size * i);
      pages[i].set_dirty(true);
      dp.require_read_page(pages[i], page_size * i);
    }

    for (uint8_t i = 0; i < 2; i++) {
      pages[i].require_allocated();
      ::memset(pages[i].page->payload(), i + 1,
                      page_size - Page::kSizeofPersistentHeader);
      pages[i].require_flush();
      pages[i].close();
    }

    for (uint8_t i = 0; i < 2; i++) {
      char temp[UPS_DEFAULT_PAGE_SIZE];
      ::memset(temp, i + 1, sizeof(temp));
      PageProxy pp(lenv());
      pp.set_address(page_size * i);
      dp.require_read_page(pp, page_size * i);
      pp.require_payload(temp, page_size - Page::kSizeofPersistentHeader);
    }
  }
};

TEST_CASE("Device/newDelete", "")
{
  DeviceFixture f(false);
  // go out of scope
}

TEST_CASE("Device/createClose", "")
{
  DeviceFixture f(false);
  f.createCloseTest();
}

TEST_CASE("Device/openClose", "")
{
  DeviceFixture f(false);
  f.openCloseTest();
}

TEST_CASE("Device/alloc", "")
{
  DeviceFixture f(false);
  f.allocTest();
}

TEST_CASE("Device/allocFree", "")
{
  DeviceFixture f(false);
  f.allocFreeTest();
}

TEST_CASE("Device/flush", "")
{
  DeviceFixture f(false);
  f.flushTest();
}

TEST_CASE("Device/mmapUnmap", "")
{
  DeviceFixture f(false);
  f.mmapUnmapTest();
}

TEST_CASE("Device/readWrite", "")
{
  DeviceFixture f(false);
  f.readWriteTest();
}

TEST_CASE("Device/readWritePage", "")
{
  DeviceFixture f(false);
  f.readWritePageTest();
}


TEST_CASE("Device/inmem/newDelete", "")
{
  DeviceFixture f(true);
  // go out of scope
}

TEST_CASE("Device/inmem/allocFree", "")
{
  DeviceFixture f(true);
  f.allocFreeTest();
}

TEST_CASE("Device/inmem/flush", "")
{
  DeviceFixture f(true);
  f.flushTest();
}

