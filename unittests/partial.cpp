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
#include "../src/blob.h"
#include "../src/env.h"
#include "../src/page.h"
#include "../src/btree_key.h"
#include "../src/freelist.h"
#include "os.hpp"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;
using namespace ham;

class BasePartialWriteTest : public hamsterDB_fixture {
  define_super(hamsterDB_fixture);

public:
  BasePartialWriteTest(const char *name, ham_size_t pagesize = 0,
      bool inmemory = false)
    : hamsterDB_fixture(name), m_pagesize(pagesize), m_inmemory(inmemory) {
  }

protected:
  ham_size_t m_pagesize;
  bool m_inmemory;
  ham_db_t *m_db;
  ham_env_t *m_env;

public:
  virtual void setup() {
    __super::setup();

    ham_parameter_t params[] = {
      { 0, 0 },
      { 0, 0 }
    };

    if (m_pagesize) {
      params[0].name = HAM_PARAM_PAGESIZE;
      params[0].value = m_pagesize;
    }

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
            m_inmemory ? HAM_IN_MEMORY : 0, 0644, &params[0]));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
  }

  virtual void teardown() {
    __super::teardown();

    BFC_ASSERT_EQUAL(0, ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void fillBuffer(ham_u8_t *ptr, ham_size_t offset, ham_size_t size) {
    for (ham_size_t i = 0; i < size; i++)
      ptr[offset + i] = (ham_u8_t)i;
  }

  void simpleInsertTest() {
    ham_key_t key = {};
    ham_record_t rec = {};
    ham_u8_t buffer[50];

    /* fill the buffer with a pattern */
    fillBuffer(&buffer[0], 0, sizeof(buffer));

    /* write at offset 0, partial size 50, record size 50 (no gaps) */
    rec.partial_offset = 0;
    rec.partial_size = 50;
    rec.size = 50;
    rec.data = buffer;
    BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_PARTIAL));

    /* verify the key */
    memset(&rec, 0, sizeof(rec));
    BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));

    BFC_ASSERT_EQUAL(50u, rec.size);
    BFC_ASSERT_EQUAL(0, memcmp(buffer, rec.data, rec.size));
  }

  virtual void insertGaps(unsigned partial_offset,
          unsigned partial_size, unsigned record_size) {
    ham_key_t key = {};
    ham_record_t rec = {};
    ham_u8_t *buffer = (ham_u8_t *)malloc(record_size);

    BFC_ASSERT(partial_offset + partial_size <= record_size);

    /* fill the buffer with a pattern */
    fillBuffer(&buffer[0], 0, record_size);

    rec.partial_offset = partial_offset;
    rec.partial_size = partial_size;
    rec.size = record_size;
    rec.data = buffer;
    BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_PARTIAL));

    /* verify the key */
    memset(&rec, 0, sizeof(rec));
    BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));

    memset(&buffer[0], 0, record_size);
    fillBuffer(&buffer[partial_offset], 0, partial_size);
    BFC_ASSERT_EQUAL(record_size, rec.size);
    BFC_ASSERT_EQUAL(0, memcmp(buffer, rec.data, rec.size));
    free(buffer);
  }

  void insertGapsAtEndTestSmall() {
    insertGaps(0, 50, 100);
  }

  void insertGapsAtEndTestBig() {
    insertGaps(0, 500, 1000);
  }

  void insertGapsAtEndTestBigger() {
    insertGaps(0, 5000, 10000);
  }

  void insertGapsAtEndTestBiggerPlus1() {
    insertGaps(0, 5001, 10001);
  }

  void insertGapsAtEndTestBiggest() {
    insertGaps(0, 50000, 100000);
  }

  void insertGapsAtEndTestBiggestPlus1() {
    insertGaps(0, 50001, 100001);
  }

  void insertGapsAtEndTestSuperbig() {
    insertGaps(0, 500000, 1000000);
  }

  void insertGapsAtEndTestSuperbigPlus1() {
    insertGaps(0, 500001, 1000001);
  }

  void insertGapsAtBeginningSmall() {
    insertGaps(50, 50, 100);
  }

  void insertGapsAtBeginningBig() {
    insertGaps(500, 500, 1000);
  }

  void insertGapsAtBeginningBigger() {
    insertGaps(5000, 5000, 10000);
  }

  void insertGapsAtBeginningBiggerPlus1() {
    insertGaps(5001, 5001, 10002);
  }

  void insertGapsAtBeginningBiggest() {
    insertGaps(50000, 50000, 100000);
  }

  void insertGapsAtBeginningBiggestPlus1() {
    insertGaps(50001, 50001, 100002);
  }

  void insertGapsAtBeginningSuperbig() {
    insertGaps(500000, 500000, 1000000);
  }

  void insertGapsAtBeginningSuperbigPlus1() {
    insertGaps(500001, 500001, 1000002);
  }

  void insertGapsTestSmall() {
    insertGaps(50, 50, 200);
  }

  void insertGapsTestBig() {
    insertGaps(500, 500, 2000);
  }

  void insertGapsTestBigger() {
    insertGaps(5000, 5000, 20000);
  }

  void insertGapsTestBiggerPlus1() {
    insertGaps(5001, 5001, 20001);
  }

  void insertGapsTestBiggest() {
    insertGaps(50000, 50000, 200000);
  }

  void insertGapsTestBiggestPlus1() {
    insertGaps(50001, 50001, 200001);
  }

  void insertGapsTestSuperbig() {
    insertGaps(500000, 500000, 2000000);
  }

  void insertGapsTestSuperbigPlus1() {
    insertGaps(500001, 500001, 2000001);
  }

  void insertGapsTestPagesize() {
    ham_size_t ps = ((Environment *)m_env)->get_pagesize();
    insertGaps(ps, ps, ps * 2);
  }

  void insertGapsTestPagesize2() {
    ham_size_t ps = ((Environment *)m_env)->get_pagesize();
    insertGaps(ps * 2, ps * 2, ps * 4);
  }

  void insertGapsTestPagesize4() {
    ham_size_t ps = ((Environment *)m_env)->get_pagesize();
    insertGaps(ps * 4, ps * 4, ps * 8);
  }
};

class PartialWriteTest : public BasePartialWriteTest {
  define_super(BasePartialWriteTest);

public:
  PartialWriteTest(ham_size_t pagesize = 0,
      const char *name = "PartialWriteTest", bool inmemory = false)
    : BasePartialWriteTest(name, pagesize, inmemory) {
    testrunner::get_instance()->register_fixture(this);

    /* write at offset 0, partial size 50, record size 50 (no gaps) */
    BFC_REGISTER_TEST(PartialWriteTest, simpleInsertTest);

    /* write at offset 0, partial size 50, record size 100 (gap at end) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsAtEndTestSmall);

    /* write at offset 0, partial size 500, record size 1000 (gap at end) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsAtEndTestBig);

    /* write at offset 0, partial size 5000, record size 10000 (gap at end) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsAtEndTestBigger);

    /* write at offset 0, partial size 5001, record size 10001 (gap at end) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsAtEndTestBiggerPlus1);

    /* write at offset 0, partial size 50000, record size 100000 (gap at end) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsAtEndTestBiggest);

    /* write at offset 0, partial size 50001, record size 100001 (gap at end) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsAtEndTestBiggestPlus1);

    /* write at offset 0, partial size 500000, record size 1000000 (gap at end) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsAtEndTestSuperbig);

    /* write at offset 0, partial size 500001, record size 1000001 (gap at end) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsAtEndTestSuperbigPlus1);

    /* write at offset 50, partial size 50, record size 100 (gap at beginning) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsAtBeginningSmall);

    /* write at offset 500, partial size 500, record size 1000 (gap at beginning) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsAtBeginningBig);

    /* write at offset 5000, partial size 5000, record size 10000 (gap at beginning) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsAtBeginningBigger);

    /* write at offset 5001, partial size 5001, record size 10001 (gap at beginning) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsAtBeginningBiggerPlus1);

    /* write at offset 50000, partial size 50000, record size 100000 (gap at beginning) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsAtBeginningBiggest);

    /* write at offset 50001, partial size 50001, record size 100001 (gap at beginning) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsAtBeginningBiggestPlus1);

    /* write at offset 500000, partial size 500000, record size 1000000 (gap at beginning) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsAtBeginningSuperbig);

    /* write at offset 500001, partial size 500001, record size 1000001 (gap at beginning) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsAtBeginningSuperbigPlus1);

    /* write at offset 50, partial size 50, record size 200 (gap at
     * beginning AND end) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsTestSmall);

    /* write at offset 500, partial size 500, record size 2000 (gap at
     * beginning AND end) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsTestBig);

    /* write at offset 5000, partial size 5000, record size 20000 (gap at
     * beginning AND end) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsTestBigger);

    /* write at offset 5001, partial size 5001, record size 20001 (gap at
     * beginning AND end) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsTestBiggerPlus1);

    /* write at offset 50000, partial size 50000, record size 200000 (gap at
     * beginning AND end) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsTestBiggest);

    /* write at offset 50001, partial size 50001, record size 200001 (gap at
     * beginning AND end) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsTestBiggestPlus1);

    /* write at offset 500000, partial size 500000, record size 2000000
     * (gap at beginning AND end) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsTestSuperbig);

    /* write at offset 500001, partial size 500001, record size 2000001
     * (gap at beginning AND end) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsTestSuperbigPlus1);

    /* write at offset PS, partial size PS, record size 2*PS
     * (gap at beginning AND end) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsTestPagesize);

    /* write at offset PS*2, partial size PS*2, record size 4*PS
     * (gap at beginning AND end) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsTestPagesize2);

    /* write at offset PS*4, partial size PS*4, record size 8*PS
     * (gap at beginning AND end) */
    BFC_REGISTER_TEST(PartialWriteTest, insertGapsTestPagesize4);
  }

public:
};

class OverwritePartialWriteTest : public BasePartialWriteTest {
  define_super(BasePartialWriteTest);

public:
  OverwritePartialWriteTest(const char *name, ham_size_t pagesize,
      bool inmemory = false)
    : BasePartialWriteTest(name, pagesize, inmemory) {
    testrunner::get_instance()->register_fixture(this);
    /* write at offset 0, partial size 50, record size 50 (no gaps) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, simpleInsertTest);

    /* write at offset 0, partial size 50, record size 100 (gap at end) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsAtEndTestSmall);

    /* write at offset 0, partial size 500, record size 1000 (gap at end) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsAtEndTestBig);

    /* write at offset 0, partial size 5000, record size 10000 (gap at end) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsAtEndTestBigger);

    /* write at offset 0, partial size 5001, record size 10001 (gap at end) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsAtEndTestBiggerPlus1);

    /* write at offset 0, partial size 50000, record size 100000 (gap at end) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsAtEndTestBiggest);

    /* write at offset 0, partial size 50001, record size 100001 (gap at end) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsAtEndTestBiggestPlus1);

    /* write at offset 0, partial size 500000, record size 1000000 (gap at end) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsAtEndTestSuperbig);

    /* write at offset 0, partial size 500001, record size 1000001 (gap at end) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsAtEndTestSuperbigPlus1);

    /* write at offset 50, partial size 50, record size 100 (gap at beginning) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsAtBeginningSmall);

    /* write at offset 500, partial size 500, record size 1000 (gap at beginning) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsAtBeginningBig);

    /* write at offset 5000, partial size 5000, record size 10000 (gap at beginning) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsAtBeginningBigger);

    /* write at offset 5001, partial size 5001, record size 10001 (gap at beginning) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsAtBeginningBiggerPlus1);

    /* write at offset 50000, partial size 50000, record size 100000 (gap at beginning) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsAtBeginningBiggest);

    /* write at offset 50001, partial size 50001, record size 100001 (gap at beginning) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsAtBeginningBiggestPlus1);

    /* write at offset 500000, partial size 500000, record size 1000000 (gap at beginning) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsAtBeginningSuperbig);

    /* write at offset 500001, partial size 500001, record size 1000001 (gap at beginning) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsAtBeginningSuperbigPlus1);

    /* write at offset 50, partial size 50, record size 200 (gap at
     * beginning AND end) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsTestSmall);

    /* write at offset 500, partial size 500, record size 2000 (gap at
     * beginning AND end) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsTestBig);

    /* write at offset 5000, partial size 5000, record size 20000 (gap at
     * beginning AND end) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsTestBigger);

    /* write at offset 5001, partial size 5001, record size 20001 (gap at
     * beginning AND end) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsTestBiggerPlus1);

    /* write at offset 50000, partial size 50000, record size 200000 (gap at
     * beginning AND end) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsTestBiggest);

    /* write at offset 50001, partial size 50001, record size 200001 (gap at
     * beginning AND end) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsTestBiggestPlus1);

    /* write at offset 500000, partial size 500000, record size 2000000
     * (gap at beginning AND end) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsTestSuperbig);

    /* write at offset 500001, partial size 500001, record size 2000001
     * (gap at beginning AND end) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsTestSuperbigPlus1);

    /* write at offset PS, partial size PS, record size 2*PS
     * (gap at beginning AND end) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsTestPagesize);

    /* write at offset PS*2, partial size PS*2, record size 4*PS
     * (gap at beginning AND end) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsTestPagesize2);

    /* write at offset PS*4, partial size PS*4, record size 8*PS
     * (gap at beginning AND end) */
    BFC_REGISTER_TEST(OverwritePartialWriteTest, insertGapsTestPagesize4);
  }

public:
  void fillBufferReverse(ham_u8_t *ptr, ham_size_t size) {
    for (ham_size_t i = 0; i < size; i++)
      ptr[i] = (ham_u8_t)(0xff - i);
  }

  virtual void insertGaps(unsigned partial_offset,
          unsigned partial_size, unsigned record_size) {
    ham_key_t key = {};
    ham_record_t rec = {};
    ham_u8_t *buffer = (ham_u8_t *)malloc(record_size);

    BFC_ASSERT(partial_offset + partial_size <= record_size);

    /* first: insert a record with a unique pattern and insert this record*/
    fillBufferReverse(&buffer[0], record_size);
    rec.size = record_size;
    rec.data = buffer;
    BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

    /* then fill the buffer with another pattern and insert the partial
     * record */
    fillBuffer(&buffer[0], 0, record_size);

    rec.partial_offset = partial_offset;
    rec.partial_size = partial_size;
    rec.size = record_size;
    rec.data = buffer;
    BFC_ASSERT_EQUAL(0,
        ham_insert(m_db, 0, &key, &rec, HAM_PARTIAL | HAM_OVERWRITE));

    /* verify the key */
    memset(&rec, 0, sizeof(rec));
    BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));

    fillBufferReverse(&buffer[0], record_size);
    fillBuffer(&buffer[partial_offset], 0, partial_size);
    BFC_ASSERT_EQUAL(record_size, rec.size);
    BFC_ASSERT_EQUAL(0, memcmp(buffer, rec.data, rec.size));
    free(buffer);
  }
};

class ShrinkPartialWriteTest : public BasePartialWriteTest {
  define_super(BasePartialWriteTest);

public:
  ShrinkPartialWriteTest()
    : BasePartialWriteTest("ShrinkPartialWriteTest") {
    testrunner::get_instance()->register_fixture(this);
    /* write at offset 0, partial size 50, record size 50 (no gaps) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, simpleInsertTest);

    /* write at offset 0, partial size 50, record size 100 (gap at end) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsAtEndTestSmall);

    /* write at offset 0, partial size 500, record size 1000 (gap at end) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsAtEndTestBig);

    /* write at offset 0, partial size 5000, record size 10000 (gap at end) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsAtEndTestBigger);

    /* write at offset 0, partial size 5001, record size 10001 (gap at end) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsAtEndTestBiggerPlus1);

    /* write at offset 0, partial size 50000, record size 100000 (gap at end) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsAtEndTestBiggest);

    /* write at offset 0, partial size 50001, record size 100001 (gap at end) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsAtEndTestBiggestPlus1);

    /* write at offset 0, partial size 500000, record size 1000000 (gap at end) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsAtEndTestSuperbig);

    /* write at offset 0, partial size 500001, record size 1000001 (gap at end) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsAtEndTestSuperbigPlus1);

    /* write at offset 50, partial size 50, record size 100 (gap at beginning) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsAtBeginningSmall);

    /* write at offset 500, partial size 500, record size 1000 (gap at beginning) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsAtBeginningBig);

    /* write at offset 5000, partial size 5000, record size 10000 (gap at beginning) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsAtBeginningBigger);

    /* write at offset 5001, partial size 5001, record size 10001 (gap at beginning) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsAtBeginningBiggerPlus1);

    /* write at offset 50000, partial size 50000, record size 100000 (gap at beginning) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsAtBeginningBiggest);

    /* write at offset 50001, partial size 50001, record size 100001 (gap at beginning) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsAtBeginningBiggestPlus1);

    /* write at offset 500000, partial size 500000, record size 1000000 (gap at beginning) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsAtBeginningSuperbig);

    /* write at offset 500001, partial size 500001, record size 1000001 (gap at beginning) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsAtBeginningSuperbigPlus1);

    /* write at offset 50, partial size 50, record size 200 (gap at
     * beginning AND end) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsTestSmall);

    /* write at offset 500, partial size 500, record size 2000 (gap at
     * beginning AND end) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsTestBig);

    /* write at offset 5000, partial size 5000, record size 20000 (gap at
     * beginning AND end) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsTestBigger);

    /* write at offset 5001, partial size 5001, record size 20001 (gap at
     * beginning AND end) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsTestBiggerPlus1);

    /* write at offset 50000, partial size 50000, record size 200000 (gap at
     * beginning AND end) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsTestBiggest);

    /* write at offset 50001, partial size 50001, record size 200001 (gap at
     * beginning AND end) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsTestBiggestPlus1);

    /* write at offset 500000, partial size 500000, record size 2000000
     * (gap at beginning AND end) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsTestSuperbig);

    /* write at offset 500001, partial size 500001, record size 2000001
     * (gap at beginning AND end) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsTestSuperbigPlus1);

    /* write at offset PS, partial size PS, record size 2*PS
     * (gap at beginning AND end) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsTestPagesize);

    /* write at offset PS*2, partial size PS*2, record size 4*PS
     * (gap at beginning AND end) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsTestPagesize2);

    /* write at offset PS*4, partial size PS*4, record size 8*PS
     * (gap at beginning AND end) */
    BFC_REGISTER_TEST(ShrinkPartialWriteTest, insertGapsTestPagesize4);
  }

public:
  void fillBufferReverse(ham_u8_t *ptr, ham_size_t size) {
    for (ham_size_t i = 0; i < size; i++)
      ptr[i] = (ham_u8_t)(0xff - i);
  }

  virtual void insertGaps(unsigned partial_offset,
          unsigned partial_size, unsigned record_size) {
    ham_key_t key = {};
    ham_record_t rec = {};
    ham_u8_t *buffer = (ham_u8_t *)malloc(record_size * 2);

    BFC_ASSERT(partial_offset + partial_size <= record_size);

    /* first: insert a record with a unique pattern and insert this record
     * this record will be TWICE as big as the one to overwrite*/
    fillBufferReverse(&buffer[0], record_size*2);
    rec.size = record_size * 2;
    rec.data = buffer;
    BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

    /* then fill the buffer with another pattern and insert the partial
     * record */
    fillBuffer(&buffer[0], 0, record_size);

    rec.partial_offset = partial_offset;
    rec.partial_size = partial_size;
    rec.size = record_size;
    rec.data = buffer;
    BFC_ASSERT_EQUAL(0,
        ham_insert(m_db, 0, &key, &rec, HAM_PARTIAL | HAM_OVERWRITE));

    /* verify the key */
    memset(&rec, 0, sizeof(rec));
    BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));

    fillBufferReverse(&buffer[0], record_size);
    fillBuffer(&buffer[partial_offset], 0, partial_size);
    BFC_ASSERT_EQUAL(record_size, rec.size);
    BFC_ASSERT_EQUAL(0, memcmp(buffer, rec.data, rec.size));
    free(buffer);
  }
};

class GrowPartialWriteTest : public BasePartialWriteTest {
  define_super(BasePartialWriteTest);

public:
  GrowPartialWriteTest()
    : BasePartialWriteTest("GrowPartialWriteTest") {
    testrunner::get_instance()->register_fixture(this);
    /* write at offset 0, partial size 50, record size 50 (no gaps) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, simpleInsertTest);

    /* write at offset 0, partial size 50, record size 100 (gap at end) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsAtEndTestSmall);

    /* write at offset 0, partial size 500, record size 1000 (gap at end) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsAtEndTestBig);

    /* write at offset 0, partial size 5000, record size 10000 (gap at end) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsAtEndTestBigger);

    /* write at offset 0, partial size 5001, record size 10001 (gap at end) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsAtEndTestBiggerPlus1);

    /* write at offset 0, partial size 50000, record size 100000 (gap at end) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsAtEndTestBiggest);

    /* write at offset 0, partial size 50001, record size 100001 (gap at end) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsAtEndTestBiggestPlus1);

    /* write at offset 0, partial size 500000, record size 1000000 (gap at end) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsAtEndTestSuperbig);

    /* write at offset 0, partial size 500001, record size 1000001 (gap at end) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsAtEndTestSuperbigPlus1);

    /* write at offset 50, partial size 50, record size 100 (gap at beginning) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsAtBeginningSmall);

    /* write at offset 500, partial size 500, record size 1000 (gap at beginning) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsAtBeginningBig);

    /* write at offset 5000, partial size 5000, record size 10000 (gap at beginning) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsAtBeginningBigger);

    /* write at offset 5001, partial size 5001, record size 10001 (gap at beginning) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsAtBeginningBiggerPlus1);

    /* write at offset 50000, partial size 50000, record size 100000 (gap at beginning) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsAtBeginningBiggest);

    /* write at offset 50001, partial size 50001, record size 100001 (gap at beginning) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsAtBeginningBiggestPlus1);

    /* write at offset 500000, partial size 500000, record size 1000000 (gap at beginning) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsAtBeginningSuperbig);

    /* write at offset 500001, partial size 500001, record size 1000001 (gap at beginning) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsAtBeginningSuperbigPlus1);

    /* write at offset 50, partial size 50, record size 200 (gap at
     * beginning AND end) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsTestSmall);

    /* write at offset 500, partial size 500, record size 2000 (gap at
     * beginning AND end) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsTestBig);

    /* write at offset 5000, partial size 5000, record size 20000 (gap at
     * beginning AND end) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsTestBigger);

    /* write at offset 5001, partial size 5001, record size 20001 (gap at
     * beginning AND end) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsTestBiggerPlus1);

    /* write at offset 50000, partial size 50000, record size 200000 (gap at
     * beginning AND end) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsTestBiggest);

    /* write at offset 50001, partial size 50001, record size 200001 (gap at
     * beginning AND end) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsTestBiggestPlus1);

    /* write at offset 500000, partial size 500000, record size 2000000
     * (gap at beginning AND end) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsTestSuperbig);

    /* write at offset 500001, partial size 500001, record size 2000001
     * (gap at beginning AND end) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsTestSuperbigPlus1);

    /* write at offset PS, partial size PS, record size 2*PS
     * (gap at beginning AND end) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsTestPagesize);

    /* write at offset PS*2, partial size PS*2, record size 4*PS
     * (gap at beginning AND end) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsTestPagesize2);

    /* write at offset PS*4, partial size PS*4, record size 8*PS
     * (gap at beginning AND end) */
    BFC_REGISTER_TEST(GrowPartialWriteTest, insertGapsTestPagesize4);
  }

public:
  void fillBufferReverse(ham_u8_t *ptr, ham_size_t size) {
    for (ham_size_t i = 0; i < size; i++)
      ptr[i] = (ham_u8_t)(0xff - i);
  }

  virtual void insertGaps(unsigned partial_offset,
          unsigned partial_size, unsigned record_size) {
    ham_key_t key = {};
    ham_record_t rec = {};
    ham_u8_t *buffer = (ham_u8_t *)malloc(record_size);

    BFC_ASSERT(partial_offset + partial_size <= record_size);

    /* first: insert a record with a unique pattern and insert this record.
     * this record will be SMALLER then the one which overwrites */
    fillBufferReverse(&buffer[0], record_size);
    rec.size = record_size / 2;
    rec.data = buffer;
    BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

    /* then fill the buffer with another pattern and insert the partial
     * record */
    fillBuffer(&buffer[0], 0, record_size);

    rec.partial_offset = partial_offset;
    rec.partial_size = partial_size;
    rec.size = record_size;
    rec.data = buffer;
    BFC_ASSERT_EQUAL(0,
        ham_insert(m_db, 0, &key, &rec, HAM_PARTIAL|HAM_OVERWRITE));

    /* verify the key */
    memset(&rec, 0, sizeof(rec));
    BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));

    memset(&buffer[0], 0, record_size);
    fillBuffer(&buffer[partial_offset], 0, partial_size);
    BFC_ASSERT_EQUAL(record_size, rec.size);
    BFC_ASSERT_EQUAL(0, memcmp(buffer, rec.data, rec.size));
    free(buffer);
  }
};

class PartialWriteTest1024k : public PartialWriteTest {
  define_super(PartialWriteTest);

public:
  PartialWriteTest1024k()
    : PartialWriteTest(1024, "PartialWriteTest1024k") {
  }
};

class PartialWriteTest2048k : public PartialWriteTest {
  define_super(PartialWriteTest);

public:
  PartialWriteTest2048k()
    : PartialWriteTest(2048, "PartialWriteTest2048k") {
  }
};

class PartialWriteTest4096k : public PartialWriteTest {
  define_super(PartialWriteTest);

public:
  PartialWriteTest4096k()
    : PartialWriteTest(4096, "PartialWriteTest4096k") {
  }
};

class PartialWriteTest16384k : public PartialWriteTest {
  define_super(PartialWriteTest);

public:
  PartialWriteTest16384k()
    : PartialWriteTest(16384, "PartialWriteTest16384k") {
  }
};

class PartialWriteTest65536k : public PartialWriteTest {
  define_super(PartialWriteTest);

public:
  PartialWriteTest65536k()
    : PartialWriteTest(65536, "PartialWriteTest65536k") {
  }
};

class InMemoryPartialWriteTest1024k : public PartialWriteTest {
  define_super(PartialWriteTest);

public:
  InMemoryPartialWriteTest1024k()
    : PartialWriteTest(1024, "InMemoryPartialWriteTest1024k", true) {
  }
};

class InMemoryPartialWriteTest2048k : public PartialWriteTest {
  define_super(PartialWriteTest);

public:
  InMemoryPartialWriteTest2048k()
    : PartialWriteTest(2048, "InMemoryPartialWriteTest2048k", true) {
  }
};

class InMemoryPartialWriteTest4096k : public PartialWriteTest {
  define_super(PartialWriteTest);

public:
  InMemoryPartialWriteTest4096k()
    : PartialWriteTest(4096, "InMemoryPartialWriteTest4096k", true) {
  }
};

class InMemoryPartialWriteTest16384k : public PartialWriteTest {
  define_super(PartialWriteTest);

public:
  InMemoryPartialWriteTest16384k()
    : PartialWriteTest(16384, "InMemoryPartialWriteTest16384k", true) {
  }
};

class InMemoryPartialWriteTest65536k : public PartialWriteTest {
  define_super(PartialWriteTest);

public:
  InMemoryPartialWriteTest65536k()
    : PartialWriteTest(65536, "InMemoryPartialWriteTest65536k", true) {
  }
};

class OverwritePartialWriteTest1024k : public OverwritePartialWriteTest {
  define_super(OverwritePartialWriteTest);

public:
  OverwritePartialWriteTest1024k()
    : OverwritePartialWriteTest("OverwritePartialWriteTest1024k", 1024) {
  }
};

class OverwritePartialWriteTest2048k : public OverwritePartialWriteTest {
  define_super(OverwritePartialWriteTest);

public:
  OverwritePartialWriteTest2048k()
    : OverwritePartialWriteTest("OverwritePartialWriteTest2048k", 2048) {
  }
};

class OverwritePartialWriteTest4096k : public OverwritePartialWriteTest {
  define_super(OverwritePartialWriteTest);

public:
  OverwritePartialWriteTest4096k()
    : OverwritePartialWriteTest("OverwritePartialWriteTest4096k", 4096) {
  }
};

class OverwritePartialWriteTest16384k : public OverwritePartialWriteTest {
  define_super(OverwritePartialWriteTest);

public:
  OverwritePartialWriteTest16384k()
    : OverwritePartialWriteTest("OverwritePartialWriteTest16384k", 16384) {
  }
};

class OverwritePartialWriteTest65536k : public OverwritePartialWriteTest {
  define_super(OverwritePartialWriteTest);

public:
  OverwritePartialWriteTest65536k()
    : OverwritePartialWriteTest("OverwritePartialWriteTest65536k", 65536) {
  }
};

class InMemoryOverwritePartialWriteTest1024k :
      public OverwritePartialWriteTest {
  define_super(OverwritePartialWriteTest);

public:
  InMemoryOverwritePartialWriteTest1024k()
    : OverwritePartialWriteTest("InMemoryOverwritePartialWriteTest1024k",
      1024, true) {
  }
};

class InMemoryOverwritePartialWriteTest2048k :
      public OverwritePartialWriteTest {
  define_super(OverwritePartialWriteTest);

public:
  InMemoryOverwritePartialWriteTest2048k()
    : OverwritePartialWriteTest("InMemoryOverwritePartialWriteTest2048k",
      2048, true) {
  }
};

class InMemoryOverwritePartialWriteTest4096k :
      public OverwritePartialWriteTest {
  define_super(OverwritePartialWriteTest);

public:
  InMemoryOverwritePartialWriteTest4096k()
    : OverwritePartialWriteTest("InMemoryOverwritePartialWriteTest4096k",
      4096, true) {
  }
};

class InMemoryOverwritePartialWriteTest16384k :
      public OverwritePartialWriteTest {
  define_super(OverwritePartialWriteTest);

public:
  InMemoryOverwritePartialWriteTest16384k()
    : OverwritePartialWriteTest("InMemoryOverwritePartialWriteTest16384k",
      16384, true) {
  }
};

class InMemoryOverwritePartialWriteTest65536k :
      public OverwritePartialWriteTest {
  define_super(OverwritePartialWriteTest);

public:
  InMemoryOverwritePartialWriteTest65536k()
    : OverwritePartialWriteTest("InMemoryOverwritePartialWriteTest65536k",
      65536, true) {
  }
};

class PartialReadTest : public hamsterDB_fixture {
  define_super(hamsterDB_fixture);

public:
  PartialReadTest(const char *name, ham_size_t pagesize = 0,
      bool inmemory = false, ham_u32_t find_flags = 0)
    : hamsterDB_fixture(name), m_pagesize(pagesize),
    m_inmemory(inmemory), m_find_flags(find_flags) {
    testrunner::get_instance()->register_fixture(this);

    /* read at offset 0, partial size 50, record size 50 (no gaps) */
    BFC_REGISTER_TEST(PartialReadTest, simpleFindTest);

    /* read at offset 0, partial size 50, record size 100 (gap at end) */
    BFC_REGISTER_TEST(PartialReadTest, findGapsAtEndTestSmall);

    /* read at offset 0, partial size 500, record size 1000 (gap at end) */
    BFC_REGISTER_TEST(PartialReadTest, findGapsAtEndTestBig);

    /* read at offset 0, partial size 5000, record size 10000 (gap at end) */
    BFC_REGISTER_TEST(PartialReadTest, findGapsAtEndTestBigger);

    /* read at offset 0, partial size 50000, record size 100000 (gap at end) */
    BFC_REGISTER_TEST(PartialReadTest, findGapsAtEndTestBiggest);

    /* read at offset 0, partial size 500000, record size 1000000 (gap at end) */
    BFC_REGISTER_TEST(PartialReadTest, findGapsAtEndTestSuperbig);

    /* read at offset 50, partial size 50, record size 100 (gap at end) */
    BFC_REGISTER_TEST(PartialReadTest, findGapsAtBeginningTestSmall);

    /* read at offset 500, partial size 500, record size 1000 (gap at end) */
    BFC_REGISTER_TEST(PartialReadTest, findGapsAtBeginningTestBig);

    /* read at offset 5000, partial size 5000, record size 10000 (gap at end) */
    BFC_REGISTER_TEST(PartialReadTest, findGapsAtBeginningTestBigger);

    /* read at offset 50000, partial size 50000, record size 100000 (gap at end) */
    BFC_REGISTER_TEST(PartialReadTest, findGapsAtBeginningTestBiggest);

    /* read at offset 500000, partial size 500000, record size 1000000 (gap at end) */
    BFC_REGISTER_TEST(PartialReadTest, findGapsAtBeginningTestSuperbig);

    /* read at offset 50, partial size 50, record size 200 (gap
     * at beginning and end) */
    BFC_REGISTER_TEST(PartialReadTest, findGapsTestSmall);

    /* read at offset 500, partial size 500, record size 2000 (gap
     * at beginning and end) */
    BFC_REGISTER_TEST(PartialReadTest, findGapsTestBig);

    /* read at offset 5000, partial size 5000, record size 20000 (gap
     * at beginning and end) */
    BFC_REGISTER_TEST(PartialReadTest, findGapsTestBigger);

    /* read at offset 50000, partial size 50000, record size 200000 (gap
     * at beginning and end) */
    BFC_REGISTER_TEST(PartialReadTest, findGapsTestBiggest);

    /* read at offset 500000, partial size 500000, record size 2000000 (gap
     * at beginning and end) */
    BFC_REGISTER_TEST(PartialReadTest, findGapsTestSuperbig);
  }

protected:
  ham_size_t m_pagesize;
  bool m_inmemory;
  ham_u32_t m_find_flags;
  ham_db_t *m_db;
  ham_env_t *m_env;

public:
  virtual void setup() {
    __super::setup();

    ham_parameter_t params[] = {
      { 0, 0 },
      { 0, 0 }
    };

    if (m_pagesize) {
      params[0].name = HAM_PARAM_PAGESIZE;
      params[0].value = m_pagesize;
    }

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
            m_inmemory ? HAM_IN_MEMORY : 0, 0644, &params[0]));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
  }

  virtual void teardown() {
    __super::teardown();

    BFC_ASSERT_EQUAL(0, ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void fillBuffer(ham_u8_t *ptr, ham_size_t offset, ham_size_t size) {
    for (ham_size_t i = 0; i < size; i++)
      ptr[i] = (ham_u8_t)(offset + i);
  }

  void simpleFindTest() {
    ham_key_t key = {};
    ham_record_t rec = {};
    ham_u8_t buffer[50];

    /* fill the buffer with a pattern */
    fillBuffer(&buffer[0], 0, sizeof(buffer));

    /* write a record of 50 bytes */
    rec.size = 50;
    rec.data = buffer;
    BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

    /* read at 0, 50 (no gap) */
    memset(&rec, 0, sizeof(rec));
    rec.partial_offset = 0;
    rec.partial_size = 50;
    BFC_ASSERT_EQUAL(0,
        ham_find(m_db, 0, &key, &rec, HAM_PARTIAL|m_find_flags));

    BFC_ASSERT_EQUAL(50u, rec.size);
    BFC_ASSERT_EQUAL(0, memcmp(buffer, rec.data, rec.size));
  }

  void findTest(unsigned partial_offset, unsigned partial_size,
          unsigned record_size) {
    ham_key_t key = {};
    ham_record_t rec = {};
    ham_u8_t *buffer = (ham_u8_t *)malloc(record_size);

    /* fill the buffer with a pattern */
    fillBuffer(&buffer[0], 0, record_size);

    /* write the record */
    rec.size = record_size;
    rec.data = buffer;
    BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

    /* now do the partial read */
    memset(&rec, 0, sizeof(rec));
    rec.partial_offset = partial_offset;
    rec.partial_size = partial_size;
    BFC_ASSERT_EQUAL(0,
        ham_find(m_db, 0, &key, &rec, HAM_PARTIAL|m_find_flags));

    memset(&buffer[0], 0, record_size);
    fillBuffer(&buffer[0], partial_offset, partial_size);
    BFC_ASSERT_EQUAL(partial_size, rec.size);
    BFC_ASSERT_EQUAL(0, memcmp(buffer, rec.data, rec.size));

    free(buffer);
  }

  void findGapsAtEndTestSmall() {
    findTest(0, 50, 100);
  }

  void findGapsAtEndTestBig() {
    findTest(0, 500, 1000);
  }

  void findGapsAtEndTestBigger() {
    findTest(0, 5000, 10000);
  }

  void findGapsAtEndTestBiggest() {
    findTest(0, 50000, 100000);
  }

  void findGapsAtEndTestSuperbig() {
    findTest(0, 500000, 1000000);
  }

  void findGapsAtBeginningTestSmall() {
    findTest(50, 50, 100);
  }

  void findGapsAtBeginningTestBig() {
    findTest(500, 500, 1000);
  }

  void findGapsAtBeginningTestBigger() {
    findTest(5000, 5000, 10000);
  }

  void findGapsAtBeginningTestBiggest() {
    findTest(50000, 50000, 100000);
  }

  void findGapsAtBeginningTestSuperbig() {
    findTest(500000, 500000, 1000000);
  }

  void findGapsTestSmall() {
    findTest(50, 50, 200);
  }

  void findGapsTestBig() {
    findTest(500, 500, 2000);
  }

  void findGapsTestBigger() {
    findTest(5000, 5000, 20000);
  }

  void findGapsTestBiggest() {
    findTest(50000, 50000, 200000);
  }

  void findGapsTestSuperbig() {
    findTest(500000, 500000, 2000000);
  }
};

class PartialReadTest1024k : public PartialReadTest {
  define_super(PartialReadTest);

public:
  PartialReadTest1024k()
    : PartialReadTest("PartialReadTest1024k", 1024) {
  }
};

class PartialReadTest2048k : public PartialReadTest {
  define_super(PartialReadTest);

public:
  PartialReadTest2048k()
    : PartialReadTest("PartialReadTest2048k", 2048) {
  }
};

class PartialReadTest4096k : public PartialReadTest {
  define_super(PartialReadTest);

public:
  PartialReadTest4096k()
    : PartialReadTest("PartialReadTest4096k", 4096) {
  }
};

class PartialReadTest16384k : public PartialReadTest {
  define_super(PartialReadTest);

public:
  PartialReadTest16384k()
    : PartialReadTest("PartialReadTest16384k", 16384) {
  }
};

class PartialReadTest65536k : public PartialReadTest {
  define_super(PartialReadTest);

public:
  PartialReadTest65536k()
    : PartialReadTest("PartialReadTest65536k", 65536) {
  }
};

class InMemoryPartialReadTest1024k : public PartialReadTest {
  define_super(PartialReadTest);

public:
  InMemoryPartialReadTest1024k()
    : PartialReadTest("InMemoryPartialReadTest1024k", 1024, true) {
  }
};

class InMemoryPartialReadTest2048k : public PartialReadTest {
  define_super(PartialReadTest);

public:
  InMemoryPartialReadTest2048k()
    : PartialReadTest("InMemoryPartialReadTest2048k", 2048, true) {
  }
};

class InMemoryPartialReadTest4096k : public PartialReadTest {
  define_super(PartialReadTest);

public:
  InMemoryPartialReadTest4096k()
    : PartialReadTest("InMemoryPartialReadTest4096k", 4096, true) {
  }
};

class InMemoryPartialReadTest16384k : public PartialReadTest {
  define_super(PartialReadTest);

public:
  InMemoryPartialReadTest16384k()
    : PartialReadTest("InMemoryPartialReadTest16384k", 16384, true) {
  }
};

class InMemoryPartialReadTest65536k : public PartialReadTest {
  define_super(PartialReadTest);

public:
  InMemoryPartialReadTest65536k()
    : PartialReadTest("InMemoryPartialReadTest65536k", 65536, true) {
  }
};

class DirectAccessPartialReadTest1024k : public PartialReadTest {
  define_super(PartialReadTest);

public:
  DirectAccessPartialReadTest1024k()
    : PartialReadTest("DirectAccessPartialReadTest1024k", 1024,
        true, HAM_DIRECT_ACCESS) {
  }
};

class DirectAccessPartialReadTest2048k : public PartialReadTest {
  define_super(PartialReadTest);

public:
  DirectAccessPartialReadTest2048k()
    : PartialReadTest("DirectAccessPartialReadTest2048k", 2048,
        true, HAM_DIRECT_ACCESS) {
  }
};

class DirectAccessPartialReadTest4096k : public PartialReadTest {
  define_super(PartialReadTest);

public:
  DirectAccessPartialReadTest4096k()
    : PartialReadTest("DirectAccessPartialReadTest4096k", 4096,
        true, HAM_DIRECT_ACCESS) {
  }
};

class DirectAccessPartialReadTest16384k : public PartialReadTest {
  define_super(PartialReadTest);

public:
  DirectAccessPartialReadTest16384k()
    : PartialReadTest("DirectAccessPartialReadTest16384k", 16384,
        true, HAM_DIRECT_ACCESS) {
  }
};

class DirectAccessPartialReadTest65536k : public PartialReadTest {
  define_super(PartialReadTest);

public:
  DirectAccessPartialReadTest65536k()
    : PartialReadTest("DirectAccessPartialReadTest65536k", 65536,
        true, HAM_DIRECT_ACCESS) {
  }
};

class MiscPartialTests : public hamsterDB_fixture {
  define_super(hamsterDB_fixture);

public:
  MiscPartialTests(const char *name = "MiscPartialTests",
      bool inmemory = false, ham_u32_t find_flags = 0)
  :   hamsterDB_fixture(name), m_inmemory(inmemory),
      m_find_flags(find_flags) {
    testrunner::get_instance()->register_fixture(this);
    BFC_REGISTER_TEST(MiscPartialTests, negativeInsertTest);
    BFC_REGISTER_TEST(MiscPartialTests, negativeCursorInsertTest);
    BFC_REGISTER_TEST(MiscPartialTests, invalidInsertParametersTest);
    BFC_REGISTER_TEST(MiscPartialTests, invalidFindParametersTest);
    BFC_REGISTER_TEST(MiscPartialTests, reduceSizeTest);
    BFC_REGISTER_TEST(MiscPartialTests, disabledSmallRecordsTest);
    BFC_REGISTER_TEST(MiscPartialTests, disabledTransactionsTest);
  }

  ham_db_t *m_db;
  ham_env_t *m_env;
  bool m_inmemory;
  ham_u32_t m_find_flags;

  virtual void setup() {
    __super::setup();

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
            m_inmemory ? HAM_IN_MEMORY : 0, 0644, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
  }

  virtual void teardown() {
    __super::teardown();

    BFC_ASSERT_EQUAL(0, ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void negativeInsertTest() {
    ham_key_t key = {};
    ham_record_t rec = {};

    ham_db_t *db;
    ham_env_t *env;
    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test.db"),
            (m_inmemory ? HAM_IN_MEMORY : 0), 0644, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db, 1, HAM_ENABLE_DUPLICATES, 0));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_insert(db, 0, &key, &rec, HAM_PARTIAL));
    BFC_ASSERT_EQUAL(0,
        ham_insert(db, 0, &key, &rec, 0));

    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void negativeCursorInsertTest() {
    ham_key_t key = {};
    ham_record_t rec = {};

    ham_db_t *db;
    ham_env_t *env;
    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test.db"),
            (m_inmemory ? HAM_IN_MEMORY : 0), 0644, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db, 1, HAM_ENABLE_DUPLICATES, 0));

    ham_cursor_t *c;
    BFC_ASSERT_EQUAL(0, ham_cursor_create(db, 0, 0, &c));

    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_cursor_insert(c, &key, &rec, HAM_PARTIAL));
    BFC_ASSERT_EQUAL(0,
        ham_cursor_insert(c, &key, &rec, 0));
    BFC_ASSERT_EQUAL(0, ham_cursor_close(c));

    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }

  void invalidInsertParametersTest() {
    ham_key_t key = {};
    ham_record_t rec = {};
    ham_u8_t buffer[500];

    ham_cursor_t *c;
    BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));

    rec.data = (void *)&buffer[0];
    rec.size = sizeof(buffer);

    /* partial_offset > size */
    rec.partial_offset = 600;
    rec.partial_size = 50;
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_insert(m_db, 0, &key, &rec, HAM_PARTIAL));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_cursor_insert(c, &key, &rec, HAM_PARTIAL));

    /* partial_offset + partial_size > size */
    rec.partial_offset = 100;
    rec.partial_size = 450;
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_insert(m_db, 0, &key, &rec, HAM_PARTIAL));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_cursor_insert(c, &key, &rec, HAM_PARTIAL));

    /* partial_size > size */
    rec.partial_offset = 0;
    rec.partial_size = 600;
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_insert(m_db, 0, &key, &rec, HAM_PARTIAL));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_cursor_insert(c, &key, &rec, HAM_PARTIAL));

    BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
  }

  void invalidFindParametersTest() {
    ham_key_t key = {};
    ham_record_t rec = {};
    ham_u8_t buffer[500];

    ham_cursor_t *c;
    BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));

    rec.data = (void *)&buffer[0];
    rec.size = sizeof(buffer);
    BFC_ASSERT_EQUAL(0,
        ham_insert(m_db, 0, &key, &rec, 0));

    /* partial_offset > size */
    rec.partial_offset = 600;
    rec.partial_size = 50;
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_find(m_db, 0, &key, &rec, HAM_PARTIAL|m_find_flags));
    BFC_ASSERT_EQUAL(0,
        ham_cursor_find(c, &key, 0, 0));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_cursor_move(c, &key, &rec, HAM_PARTIAL|m_find_flags));

    BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
  }

  void reduceSizeTest() {
    ham_key_t key = {};
    ham_record_t rec = {};
    ham_u8_t buffer[500];

    ham_cursor_t *c;
    BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));

    rec.data = (void *)&buffer[0];
    rec.size = sizeof(buffer);
    BFC_ASSERT_EQUAL(0,
        ham_insert(m_db, 0, &key, &rec, 0));

    /* partial_offset + partial_size > size */
    rec.partial_offset = 100;
    rec.partial_size = 450;
    BFC_ASSERT_EQUAL(0,
        ham_find(m_db, 0, &key, &rec, HAM_PARTIAL|m_find_flags));
    BFC_ASSERT_EQUAL(400u, rec.size);
    BFC_ASSERT_EQUAL(0,
        ham_cursor_find(c, &key, 0, 0));
    BFC_ASSERT_EQUAL(0,
        ham_cursor_move(c, &key, &rec, HAM_PARTIAL|m_find_flags));
    BFC_ASSERT_EQUAL(400u, rec.size);

    /* partial_size > size */
    rec.partial_offset = 0;
    rec.partial_size = 600;
    BFC_ASSERT_EQUAL(0,
        ham_find(m_db, 0, &key, &rec, HAM_PARTIAL|m_find_flags));
    BFC_ASSERT_EQUAL(500u, rec.size);
    BFC_ASSERT_EQUAL(0,
        ham_cursor_find(c, &key, 0, 0));
    BFC_ASSERT_EQUAL(0,
        ham_cursor_move(c, &key, &rec, HAM_PARTIAL|m_find_flags));
    BFC_ASSERT_EQUAL(500u, rec.size);

    BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
  }

  void disabledSmallRecordsTest() {
    ham_key_t key = {};
    ham_record_t rec = {};
    ham_u8_t buffer[8];

    rec.data = (void *)&buffer[0];
    rec.size = 8;
    BFC_ASSERT_EQUAL(0,
        ham_insert(m_db, 0, &key, &rec, 0));

    rec.data = (void *)&buffer[0];
    rec.size = 1;
    rec.partial_offset = 0;
    rec.partial_size = 1;
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_insert(m_db, 0, &key, &rec, HAM_PARTIAL));

    rec.size = 5;
    rec.partial_offset = 0;
    rec.partial_size = 1;
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_insert(m_db, 0, &key, &rec, HAM_PARTIAL));

    rec.size = 8;
    rec.partial_offset = 0;
    rec.partial_size = 1;
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_insert(m_db, 0, &key, &rec, HAM_PARTIAL));

    rec.size = 1;
    rec.partial_offset = 0;
    rec.partial_size = 1;
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_find(m_db, 0, &key, &rec, HAM_PARTIAL));

    rec.size = 5;
    rec.partial_offset = 0;
    rec.partial_size = 1;
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_find(m_db, 0, &key, &rec, HAM_PARTIAL));

    rec.size = 8;
    rec.partial_offset = 0;
    rec.partial_size = 1;
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_find(m_db, 0, &key, &rec, HAM_PARTIAL));
  }

  void disabledTransactionsTest() {
    ham_db_t *db;
    ham_env_t *env;
    BFC_ASSERT_EQUAL(0,
        ham_env_create(&env, BFC_OPATH(".test2"),
            HAM_ENABLE_TRANSACTIONS, 0644, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(env, &db, 1, 0, 0));

    ham_cursor_t *c;
    BFC_ASSERT_EQUAL(0, ham_cursor_create(db, 0, 0, &c));

    ham_key_t key = {};
    ham_record_t rec = {};
    ham_u8_t buffer[16];

    rec.data = (void *)&buffer[0];
    rec.size = 16;
    BFC_ASSERT_EQUAL(0,
        ham_insert(db, 0, &key, &rec, 0));

    rec.data = (void *)&buffer[0];
    rec.size = 1;
    rec.partial_offset = 0;
    rec.partial_size = 1;
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_insert(db, 0, &key, &rec, HAM_PARTIAL));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_cursor_insert(c, &key, &rec, HAM_PARTIAL));

    rec.partial_offset = 0;
    rec.partial_size = 1;
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_find(db, 0, &key, &rec, HAM_PARTIAL));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_cursor_find(c, &key, &rec, HAM_PARTIAL));
    BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
        ham_cursor_move(c, &key, &rec, HAM_PARTIAL));

    BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
    BFC_ASSERT_EQUAL(0, ham_env_close(env, HAM_AUTO_CLEANUP));
  }
};

class InMemoryMiscPartialTests : public MiscPartialTests {
  define_super(MiscPartialTests);

public:
  InMemoryMiscPartialTests()
    : MiscPartialTests("InMemoryMiscPartialTests", true) {
  }
};

class DirectAccessMiscPartialTests : public MiscPartialTests {
  define_super(MiscPartialTests);

public:
  DirectAccessMiscPartialTests()
    : MiscPartialTests("DirectAccessMiscPartialTests", true,
        HAM_DIRECT_ACCESS) {
  }
};

BFC_REGISTER_FIXTURE(PartialWriteTest1024k);
BFC_REGISTER_FIXTURE(PartialWriteTest2048k);
BFC_REGISTER_FIXTURE(PartialWriteTest4096k);
BFC_REGISTER_FIXTURE(PartialWriteTest16384k);
BFC_REGISTER_FIXTURE(PartialWriteTest65536k);
BFC_REGISTER_FIXTURE(InMemoryPartialWriteTest1024k);
BFC_REGISTER_FIXTURE(InMemoryPartialWriteTest2048k);
BFC_REGISTER_FIXTURE(InMemoryPartialWriteTest4096k);
BFC_REGISTER_FIXTURE(InMemoryPartialWriteTest16384k);
BFC_REGISTER_FIXTURE(InMemoryPartialWriteTest65536k);
BFC_REGISTER_FIXTURE(OverwritePartialWriteTest1024k);
BFC_REGISTER_FIXTURE(OverwritePartialWriteTest2048k);
BFC_REGISTER_FIXTURE(OverwritePartialWriteTest4096k);
BFC_REGISTER_FIXTURE(OverwritePartialWriteTest16384k);
BFC_REGISTER_FIXTURE(OverwritePartialWriteTest65536k);
BFC_REGISTER_FIXTURE(InMemoryOverwritePartialWriteTest1024k);
BFC_REGISTER_FIXTURE(InMemoryOverwritePartialWriteTest2048k);
BFC_REGISTER_FIXTURE(InMemoryOverwritePartialWriteTest4096k);
BFC_REGISTER_FIXTURE(InMemoryOverwritePartialWriteTest16384k);
BFC_REGISTER_FIXTURE(InMemoryOverwritePartialWriteTest65536k);
BFC_REGISTER_FIXTURE(ShrinkPartialWriteTest);
BFC_REGISTER_FIXTURE(GrowPartialWriteTest);

BFC_REGISTER_FIXTURE(PartialReadTest1024k);
BFC_REGISTER_FIXTURE(PartialReadTest2048k);
BFC_REGISTER_FIXTURE(PartialReadTest4096k);
BFC_REGISTER_FIXTURE(PartialReadTest16384k);
BFC_REGISTER_FIXTURE(PartialReadTest65536k);
BFC_REGISTER_FIXTURE(InMemoryPartialReadTest1024k);
BFC_REGISTER_FIXTURE(InMemoryPartialReadTest2048k);
BFC_REGISTER_FIXTURE(InMemoryPartialReadTest4096k);
BFC_REGISTER_FIXTURE(InMemoryPartialReadTest16384k);
BFC_REGISTER_FIXTURE(InMemoryPartialReadTest65536k);
BFC_REGISTER_FIXTURE(DirectAccessPartialReadTest1024k);
BFC_REGISTER_FIXTURE(DirectAccessPartialReadTest2048k);
BFC_REGISTER_FIXTURE(DirectAccessPartialReadTest4096k);
BFC_REGISTER_FIXTURE(DirectAccessPartialReadTest16384k);
BFC_REGISTER_FIXTURE(DirectAccessPartialReadTest65536k);

BFC_REGISTER_FIXTURE(MiscPartialTests);
BFC_REGISTER_FIXTURE(InMemoryMiscPartialTests);
BFC_REGISTER_FIXTURE(DirectAccessMiscPartialTests);

