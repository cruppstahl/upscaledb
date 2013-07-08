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

#include "../src/device.h"
#include "../src/env.h"

using namespace hamsterdb;

struct DeviceFixture
{
  ham_db_t *m_db;
  ham_env_t *m_env;
  Device *m_dev;

  DeviceFixture(bool inmemory) {
    (void)os::unlink(Globals::opath(".test"));

    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"),
            inmemory ? HAM_IN_MEMORY : 0, 0644, 0));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
    m_dev = ((Environment *)m_env)->get_device();
  }

  ~DeviceFixture() {
    REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void newDeleteTest() {
  }

  void createCloseTest() {
    REQUIRE(true == m_dev->is_open());
    REQUIRE(0 == m_dev->close());
    REQUIRE(false == m_dev->is_open());
    REQUIRE(0 == m_dev->open(Globals::opath(".test"), 0));
    REQUIRE(true == m_dev->is_open());
  }

  void openCloseTest() {
    REQUIRE(true == m_dev->is_open());
    REQUIRE(0 == m_dev->close());
    REQUIRE(false == m_dev->is_open());
    REQUIRE(0 == m_dev->open(Globals::opath(".test"), 0));
    REQUIRE(true == m_dev->is_open());
    REQUIRE(0 == m_dev->close());
    REQUIRE(false == m_dev->is_open());
    REQUIRE(0 == m_dev->open(Globals::opath(".test"), 0));
    REQUIRE(true == m_dev->is_open());
  }

  void allocTest() {
    int i;
    ham_u64_t address;

    REQUIRE(true == m_dev->is_open());
    for (i = 0; i < 10; i++) {
      REQUIRE(0 == m_dev->alloc(1024, &address));
      REQUIRE(address ==
                  (((Environment *)m_env)->get_pagesize() * 2) + 1024 * i);
    }
  }

  void allocFreeTest() {
    Page page((Environment *)m_env);
    page.set_db((LocalDatabase *)m_db);

    REQUIRE(true == m_dev->is_open());
    REQUIRE(0 == m_dev->alloc_page(&page));
    REQUIRE(page.get_data());
    m_dev->free_page(&page);
  }

  void flushTest() {
    REQUIRE(true == m_dev->is_open());
    REQUIRE(0 == m_dev->flush());
    REQUIRE(true == m_dev->is_open());
  }

  void mmapUnmapTest() {
    int i;
    Page pages[10];
    ham_size_t ps = HAM_DEFAULT_PAGESIZE;
    ham_u8_t *temp = (ham_u8_t *)malloc(ps);

    REQUIRE(true == m_dev->is_open());
    REQUIRE(0 == m_dev->truncate(ps * 10));
    for (i = 0; i < 10; i++) {
      memset(&pages[i], 0, sizeof(Page));
      pages[i].set_db((LocalDatabase *)m_db);
      pages[i].set_address(i * ps);
      REQUIRE(0 == m_dev->read_page(&pages[i]));
    }
    for (i = 0; i < 10; i++)
      memset(pages[i].get_data(), i, ps);
    for (i = 0; i < 10; i++)
      REQUIRE(0 == m_dev->write_page(&pages[i]));
    for (i = 0; i < 10; i++) {
      ham_u8_t *buffer;
      memset(temp, i, ps);
      m_dev->free_page(&pages[i]);

      REQUIRE(0 == m_dev->read_page(&pages[i]));
      buffer = (ham_u8_t *)pages[i].get_data();
      REQUIRE(0 == memcmp(buffer, temp, ps));
    }
    for (i = 0; i < 10; i++)
      m_dev->free_page(&pages[i]);
    free(temp);
  }

  void readWriteTest() {
    int i;
    ham_u8_t *buffer[10] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    ham_size_t ps = HAM_DEFAULT_PAGESIZE;
    ham_u8_t *temp = (ham_u8_t *)malloc(ps);

    m_dev->test_disable_mmap();

    REQUIRE(true == m_dev->is_open());
    REQUIRE(0 == m_dev->truncate(ps * 10));
    for (i = 0; i < 10; i++) {
      buffer[i] = (ham_u8_t *)malloc(ps);
      REQUIRE(0 == m_dev->read(i * ps, buffer[i], ps));
    }
    for (i = 0; i < 10; i++)
      memset(buffer[i], i, ps);
    for (i = 0; i < 10; i++)
      REQUIRE(0 == m_dev->write(i * ps, buffer[i], ps));
    for (i = 0; i < 10; i++) {
      REQUIRE(0 == m_dev->read(i * ps, buffer[i], ps));
      memset(temp, i, ps);
      REQUIRE(0 == memcmp(buffer[i], temp, ps));
      free(buffer[i]);
    }
    free(temp);
  }

  void readWritePageTest() {
    int i;
    Page *pages[2];
    ham_size_t ps = HAM_DEFAULT_PAGESIZE;

    m_dev->test_disable_mmap();

    REQUIRE(1 == m_dev->is_open());
    REQUIRE(0 == m_dev->truncate(ps * 2));
    for (i = 0; i < 2; i++) {
      REQUIRE((pages[i] = new Page((Environment *)m_env)));
      pages[i]->set_address(ps * i);
      REQUIRE(0 == m_dev->read_page(pages[i]));
    }
    for (i = 0; i < 2; i++) {
      REQUIRE((pages[i]->get_flags() & Page::kNpersMalloc) != 0);
      memset(pages[i]->get_data(), i + 1, ps);
      REQUIRE(0 == m_dev->write_page(pages[i]));
      delete pages[i];
    }

    for (i = 0; i < 2; i++) {
      char temp[1024];
      memset(temp, i + 1, sizeof(temp));
      REQUIRE((pages[i] = new Page((Environment *)m_env)));
      pages[i]->set_address(ps * i);
      REQUIRE(0 == m_dev->read_page(pages[i]));
      REQUIRE(0 == memcmp(pages[i]->get_data(), temp, sizeof(temp)));
      delete pages[i];
    }
  }
};

TEST_CASE("Device/newDelete", "")
{
  DeviceFixture f(false);
  f. newDeleteTest();
}

TEST_CASE("Device/createClose", "")
{
  DeviceFixture f(false);
  f. createCloseTest();
}

TEST_CASE("Device/openClose", "")
{
  DeviceFixture f(false);
  f. openCloseTest();
}

TEST_CASE("Device/alloc", "")
{
  DeviceFixture f(false);
  f. allocTest();
}

TEST_CASE("Device/allocFree", "")
{
  DeviceFixture f(false);
  f. allocFreeTest();
}

TEST_CASE("Device/flush", "")
{
  DeviceFixture f(false);
  f. flushTest();
}

TEST_CASE("Device/mmapUnmap", "")
{
  DeviceFixture f(false);
  f. mmapUnmapTest();
}

TEST_CASE("Device/readWrite", "")
{
  DeviceFixture f(false);
  f. readWriteTest();
}

TEST_CASE("Device/readWritePage", "")
{
  DeviceFixture f(false);
  f. readWritePageTest();
}


TEST_CASE("Device-inmem/newDelete", "")
{
  DeviceFixture f(true);
  f. newDeleteTest();
}

TEST_CASE("Device-inmem/allocFree", "")
{
  DeviceFixture f(true);
  f. allocFreeTest();
}

TEST_CASE("Device-inmem/flush", "")
{
  DeviceFixture f(true);
  f. flushTest();
}

