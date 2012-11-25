/**
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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
#include "../src/device.h"
#include "../src/env.h"
#include "os.hpp"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;
using namespace ham;

class DeviceTest : public hamsterDB_fixture
{
  define_super(hamsterDB_fixture);

public:
  DeviceTest(bool inmemory = false, const char *name = "DeviceTest")
    : hamsterDB_fixture(name), m_db(0), m_inmemory(inmemory), m_dev(0) {
    testrunner::get_instance()->register_fixture(this);
    BFC_REGISTER_TEST(DeviceTest, newDeleteTest);
    BFC_REGISTER_TEST(DeviceTest, createCloseTest);
    BFC_REGISTER_TEST(DeviceTest, openCloseTest);
    BFC_REGISTER_TEST(DeviceTest, pagesizeTest);
    BFC_REGISTER_TEST(DeviceTest, allocTest);
    BFC_REGISTER_TEST(DeviceTest, allocFreeTest);
    BFC_REGISTER_TEST(DeviceTest, flushTest);
    BFC_REGISTER_TEST(DeviceTest, mmapUnmapTest);
    BFC_REGISTER_TEST(DeviceTest, readWriteTest);
    BFC_REGISTER_TEST(DeviceTest, readWritePageTest);
  }

protected:
  ham_db_t *m_db;
  ham_env_t *m_env;
  bool m_inmemory;
  Device *m_dev;

public:
  virtual void setup() {
    __super::setup();

    (void)os::unlink(BFC_OPATH(".test"));

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
            m_inmemory ? HAM_IN_MEMORY : 0, 0644, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
    m_dev = ((Environment *)m_env)->get_device();
  }

  virtual void teardown() {
    __super::teardown();

    BFC_ASSERT_EQUAL(0, ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void newDeleteTest() {
  }

  void createCloseTest() {
    BFC_ASSERT_EQUAL(1, m_dev->is_open());
    if (!m_inmemory) {
      BFC_ASSERT_EQUAL(0, m_dev->close());
      BFC_ASSERT_EQUAL(0, m_dev->is_open());
      BFC_ASSERT_EQUAL(0, m_dev->open(BFC_OPATH(".test"), 0));
      BFC_ASSERT_EQUAL(1, m_dev->is_open());
    }
  }

  void openCloseTest() {
    if (!m_inmemory) {
      BFC_ASSERT_EQUAL(1, m_dev->is_open());
      BFC_ASSERT_EQUAL(0, m_dev->close());
      BFC_ASSERT_EQUAL(0, m_dev->is_open());
      BFC_ASSERT_EQUAL(0, m_dev->open(BFC_OPATH(".test"), 0));
      BFC_ASSERT_EQUAL(1, m_dev->is_open());
      BFC_ASSERT_EQUAL(0, m_dev->close());
      BFC_ASSERT_EQUAL(0, m_dev->is_open());
      BFC_ASSERT_EQUAL(0, m_dev->open(BFC_OPATH(".test"), 0));
      BFC_ASSERT_EQUAL(1, m_dev->is_open());
    }
  }

  void pagesizeTest() {
    ham_size_t cps;
    ham_size_t ps = m_dev->get_pagesize();
    BFC_ASSERT(ps!=0);
    BFC_ASSERT(ps % 1024 == 0);
    cps = m_dev->get_pagesize();
    BFC_ASSERT(cps != 0);
    BFC_ASSERT(cps % DB_CHUNKSIZE == 0);
    if (!m_inmemory)
      BFC_ASSERT_EQUAL(cps, ps);
  }

  void allocTest() {
    int i;
    ham_offset_t address;

    BFC_ASSERT_EQUAL(1, m_dev->is_open());
    for (i = 0; i < 10; i++) {
      BFC_ASSERT_EQUAL(0, m_dev->alloc(1024, &address));
      BFC_ASSERT_EQUAL((((Environment *)m_env)->get_pagesize() * 2) + 1024 * i,
                address);
    }
  }

  void allocFreeTest() {
    Page page((Environment *)m_env);
    page.set_db((Database *)m_db);

    BFC_ASSERT_EQUAL(1, m_dev->is_open());
    BFC_ASSERT_EQUAL(0, m_dev->alloc_page(&page));
    BFC_ASSERT(page.get_pers() != 0);
    BFC_ASSERT_EQUAL(0, m_dev->free_page(&page));
  }

  void flushTest() {
    BFC_ASSERT_EQUAL(1, m_dev->is_open());
    BFC_ASSERT_EQUAL(0, m_dev->flush());
    BFC_ASSERT_EQUAL(1, m_dev->is_open());
  }

  void mmapUnmapTest() {
    int i;
    Page pages[10];
    ham_size_t ps = m_dev->get_pagesize();
    ham_u8_t *temp = (ham_u8_t *)malloc(ps);

    BFC_ASSERT_EQUAL(1, m_dev->is_open());
    BFC_ASSERT_EQUAL(0, m_dev->truncate(ps * 10));
    for (i = 0; i < 10; i++) {
      memset(&pages[i], 0, sizeof(Page));
      pages[i].set_db((Database *)m_db);
      pages[i].set_self(i * ps);
      BFC_ASSERT_EQUAL(0, m_dev->read_page(&pages[i]));
    }
    for (i = 0; i < 10; i++)
      memset(pages[i].get_pers(), i, ps);
    for (i = 0; i < 10; i++)
      BFC_ASSERT_EQUAL(0, m_dev->write_page(&pages[i]));
    for (i = 0; i < 10; i++) {
      ham_u8_t *buffer;
      memset(temp, i, ps);
      BFC_ASSERT_EQUAL(0, m_dev->free_page(&pages[i]));

      BFC_ASSERT_EQUAL(0, m_dev->read_page(&pages[i]));
      buffer = (ham_u8_t *)pages[i].get_pers();
      BFC_ASSERT_EQUAL(0, memcmp(buffer, temp, ps));
    }
    for (i = 0; i < 10; i++)
      BFC_ASSERT_EQUAL(0, m_dev->free_page(&pages[i]));
    free(temp);
  }

  void readWriteTest() {
    int i;
    ham_u8_t *buffer[10] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    ham_size_t ps = m_dev->get_pagesize();
    ham_u8_t *temp = (ham_u8_t *)malloc(ps);

    m_dev->set_flags(HAM_DISABLE_MMAP);

    BFC_ASSERT_EQUAL(1, m_dev->is_open());
    BFC_ASSERT_EQUAL(0, m_dev->truncate(ps*10));
    for (i = 0; i < 10; i++) {
      buffer[i] = (ham_u8_t *)malloc(ps);
      BFC_ASSERT_EQUAL(0, m_dev->read(i * ps, buffer[i], ps));
    }
    for (i = 0; i < 10; i++)
      memset(buffer[i], i, ps);
    for (i = 0; i < 10; i++)
      BFC_ASSERT_EQUAL(0, m_dev->write(i * ps, buffer[i], ps));
    for (i = 0; i < 10; i++) {
      BFC_ASSERT_EQUAL(0, m_dev->read(i * ps, buffer[i], ps));
      memset(temp, i, ps);
      BFC_ASSERT_EQUAL(0, memcmp(buffer[i], temp, ps));
      free(buffer[i]);
    }
    free(temp);
  }

  void readWritePageTest() {
    int i;
    Page *pages[2];
    ham_size_t ps = m_dev->get_pagesize();

    m_dev->set_flags(HAM_DISABLE_MMAP);

    BFC_ASSERT_EQUAL(1, m_dev->is_open());
    BFC_ASSERT_EQUAL(0, m_dev->truncate(ps*2));
    for (i = 0; i < 2; i++) {
      BFC_ASSERT((pages[i] = new Page((Environment *)m_env)));
      pages[i]->set_self(ps * i);
      BFC_ASSERT_EQUAL(0, m_dev->read_page(pages[i]));
    }
    for (i = 0; i < 2; i++) {
      BFC_ASSERT(pages[i]->get_flags() & Page::NPERS_MALLOC);
      memset(pages[i]->get_pers(), i + 1, ps);
      BFC_ASSERT_EQUAL(0, m_dev->write_page(pages[i]));
      BFC_ASSERT_EQUAL(0, pages[i]->free());
      delete pages[i];
    }

    for (i = 0; i < 2; i++) {
      char temp[1024];
      memset(temp, i + 1, sizeof(temp));
      BFC_ASSERT((pages[i] = new Page((Environment *)m_env)));
      pages[i]->set_self(ps * i);
      BFC_ASSERT_EQUAL(0, m_dev->read_page(pages[i]));
      BFC_ASSERT_EQUAL(0, memcmp(pages[i]->get_pers(), temp, sizeof(temp)));
      BFC_ASSERT_EQUAL(0, pages[i]->free());
      delete pages[i];
    }
  }

};

class InMemoryDeviceTest : public DeviceTest {
public:
  InMemoryDeviceTest()
    : DeviceTest(true, "InMemoryDeviceTest") {
    clear_tests(); // don't inherit tests
    testrunner::get_instance()->register_fixture(this);
    BFC_REGISTER_TEST(InMemoryDeviceTest, newDeleteTest);
    BFC_REGISTER_TEST(InMemoryDeviceTest, createCloseTest);
    BFC_REGISTER_TEST(InMemoryDeviceTest, openCloseTest);
    BFC_REGISTER_TEST(InMemoryDeviceTest, pagesizeTest);
    BFC_REGISTER_TEST(InMemoryDeviceTest, allocFreeTest);
    BFC_REGISTER_TEST(InMemoryDeviceTest, flushTest);
  }
};

BFC_REGISTER_FIXTURE(DeviceTest);
BFC_REGISTER_FIXTURE(InMemoryDeviceTest);

