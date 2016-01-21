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
#include "3blob_manager/blob_manager.h"
#include "4db/db.h"
#include "4env/env_local.h"

#include "utils.h"
#include "os.hpp"

using namespace upscaledb;

struct PartialWriteFixture {
  uint32_t m_page_size;
  bool m_inmemory;
  ups_db_t *m_db;
  ups_env_t *m_env;

  PartialWriteFixture(uint32_t page_size = 0, bool inmemory = false)
    : m_page_size(page_size), m_inmemory(inmemory) {
    setup();
  }

  ~PartialWriteFixture() {
    teardown();
  }

  void setup() {
    ups_parameter_t params[] = {
      { 0, 0 },
      { 0, 0 }
    };

    if (m_page_size) {
      params[0].name = UPS_PARAM_PAGESIZE;
      params[0].value = m_page_size;
    }

    REQUIRE(0 ==
        ups_env_create(&m_env, Utils::opath(".test"),
            m_inmemory ? UPS_IN_MEMORY : 0, 0644, &params[0]));
    REQUIRE(0 ==
        ups_env_create_db(m_env, &m_db, 1, 0, 0));
  }

  void teardown() {
    REQUIRE(0 == ups_env_close(m_env, UPS_AUTO_CLEANUP));
  }

  void fillBuffer(uint8_t *ptr, uint32_t offset, uint32_t size) {
    for (uint32_t i = 0; i < size; i++)
      ptr[offset + i] = (uint8_t)i;
  }

  void simpleInsertTest() {
    ups_key_t key = {};
    ups_record_t rec = {};
    uint8_t buffer[50];

    /* fill the buffer with a pattern */
    fillBuffer(&buffer[0], 0, sizeof(buffer));

    /* write at offset 0, partial size 50, record size 50 (no gaps) */
    rec.partial_offset = 0;
    rec.partial_size = 50;
    rec.size = 50;
    rec.data = buffer;
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, UPS_PARTIAL));

    /* verify the key */
    memset(&rec, 0, sizeof(rec));
    REQUIRE(0 == ups_db_find(m_db, 0, &key, &rec, 0));

    REQUIRE(50u == rec.size);
    REQUIRE(0 == memcmp(buffer, rec.data, rec.size));
  }

  virtual void insertGaps(unsigned partial_offset,
          unsigned partial_size, unsigned record_size) {
    ups_key_t key = {};
    ups_record_t rec = {};
    uint8_t *buffer = (uint8_t *)malloc(record_size);

    REQUIRE((unsigned)(partial_offset + partial_size) <= record_size);

    /* fill the buffer with a pattern */
    fillBuffer(&buffer[0], 0, record_size);

    rec.partial_offset = partial_offset;
    rec.partial_size = partial_size;
    rec.size = record_size;
    rec.data = buffer;
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, UPS_PARTIAL));

    /* verify the key */
    memset(&rec, 0, sizeof(rec));
    REQUIRE(0 == ups_db_find(m_db, 0, &key, &rec, 0));

    memset(&buffer[0], 0, record_size);
    fillBuffer(&buffer[partial_offset], 0, partial_size);
    REQUIRE(record_size == rec.size);
    REQUIRE(0 == memcmp(buffer, rec.data, rec.size));
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
    uint32_t ps = ((LocalEnvironment *)m_env)->config().page_size_bytes;
    insertGaps(ps, ps, ps * 2);
  }

  void insertGapsTestPagesize2() {
    uint32_t ps = ((LocalEnvironment *)m_env)->config().page_size_bytes;
    insertGaps(ps * 2, ps * 2, ps * 4);
  }

  void insertGapsTestPagesize4() {
    uint32_t ps = ((LocalEnvironment *)m_env)->config().page_size_bytes;
    insertGaps(ps * 4, ps * 4, ps * 8);
  }
};

#include "partial-write-ps1.h"
#include "partial-write-ps2.h"
#include "partial-write-ps4.h"
#include "partial-write-ps16.h"
#include "partial-write-ps64.h"

#include "partial-write-inmem-ps1.h"
#include "partial-write-inmem-ps2.h"
#include "partial-write-inmem-ps4.h"
#include "partial-write-inmem-ps16.h"
#include "partial-write-inmem-ps64.h"


struct OverwritePartialWriteFixture : public PartialWriteFixture {
  OverwritePartialWriteFixture(uint32_t page_size, bool inmemory = false)
    : PartialWriteFixture(page_size, inmemory) {
  }

  void fillBufferReverse(uint8_t *ptr, uint32_t size) {
    for (uint32_t i = 0; i < size; i++)
      ptr[i] = (uint8_t)(0xff - i);
  }

  virtual void insertGaps(unsigned partial_offset,
          unsigned partial_size, unsigned record_size) {
    ups_key_t key = {};
    ups_record_t rec = {};
    uint8_t *buffer = (uint8_t *)malloc(record_size);

    REQUIRE((unsigned)(partial_offset + partial_size) <= record_size);

    /* first: insert a record with a unique pattern and insert this record*/
    fillBufferReverse(&buffer[0], record_size);
    rec.size = record_size;
    rec.data = buffer;
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, 0));

    /* then fill the buffer with another pattern and insert the partial
     * record */
    fillBuffer(&buffer[0], 0, record_size);

    rec.partial_offset = partial_offset;
    rec.partial_size = partial_size;
    rec.size = record_size;
    rec.data = buffer;
    REQUIRE(0 ==
        ups_db_insert(m_db, 0, &key, &rec, UPS_PARTIAL | UPS_OVERWRITE));

    /* verify the key */
    memset(&rec, 0, sizeof(rec));
    REQUIRE(0 == ups_db_find(m_db, 0, &key, &rec, 0));

    fillBufferReverse(&buffer[0], record_size);
    fillBuffer(&buffer[partial_offset], 0, partial_size);
    REQUIRE(record_size == rec.size);
    REQUIRE(0 == memcmp(buffer, rec.data, rec.size));
    free(buffer);
  }
};

#include "partial-overwrite-ps1.h"
#include "partial-overwrite-ps2.h"
#include "partial-overwrite-ps4.h"
#include "partial-overwrite-ps16.h"
#include "partial-overwrite-ps64.h"

#include "partial-overwrite-inmem-ps1.h"
#include "partial-overwrite-inmem-ps2.h"
#include "partial-overwrite-inmem-ps4.h"
#include "partial-overwrite-inmem-ps16.h"
#include "partial-overwrite-inmem-ps64.h"

struct ShrinkPartialWriteFixture : public PartialWriteFixture {
  ShrinkPartialWriteFixture() {
  }

  void fillBufferReverse(uint8_t *ptr, uint32_t size) {
    for (uint32_t i = 0; i < size; i++)
      ptr[i] = (uint8_t)(0xff - i);
  }

  virtual void insertGaps(unsigned partial_offset,
          unsigned partial_size, unsigned record_size) {
    ups_key_t key = {};
    ups_record_t rec = {};
    uint8_t *buffer = (uint8_t *)malloc(record_size * 2);

    REQUIRE((unsigned)(partial_offset + partial_size) <= record_size);

    /* first: insert a record with a unique pattern and insert this record
     * this record will be TWICE as big as the one to overwrite */
    fillBufferReverse(&buffer[0], record_size * 2);
    rec.size = record_size * 2;
    rec.data = buffer;
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, 0));

    /* then fill the buffer with another pattern and insert the partial
     * record */
    fillBuffer(&buffer[0], 0, record_size);

    rec.partial_offset = partial_offset;
    rec.partial_size = partial_size;
    rec.size = record_size;
    rec.data = buffer;
    REQUIRE(0 ==
        ups_db_insert(m_db, 0, &key, &rec, UPS_PARTIAL | UPS_OVERWRITE));

    /* verify the key */
    memset(&rec, 0, sizeof(rec));
    REQUIRE(0 == ups_db_find(m_db, 0, &key, &rec, 0));

    fillBufferReverse(&buffer[0], record_size);
    fillBuffer(&buffer[partial_offset], 0, partial_size);
    REQUIRE(record_size == rec.size);
    REQUIRE(0 == memcmp(buffer, rec.data, rec.size));
    free(buffer);
  }
};

/* write at offset 0, partial size 50, record size 50 (no gaps) */
TEST_CASE("Partial-shrink/simpleInsertTest", "")
{
  ShrinkPartialWriteFixture f;
  f.simpleInsertTest();
}

/* write at offset 0, partial size 50, record size 100 (gap at end) */
TEST_CASE("Partial-shrink/insertGapsAtEndTestSmall", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsAtEndTestSmall();
}

/* write at offset 0, partial size 500, record size 1000 (gap at end) */
TEST_CASE("Partial-shrink/insertGapsAtEndTestBig", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsAtEndTestBig();
}

/* write at offset 0, partial size 5000, record size 10000 (gap at end) */
TEST_CASE("Partial-shrink/insertGapsAtEndTestBigger", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsAtEndTestBigger();
}

/* write at offset 0, partial size 5001, record size 10001 (gap at end) */
TEST_CASE("Partial-shrink/insertGapsAtEndTestBiggerPlus1", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsAtEndTestBiggerPlus1();
}

/* write at offset 0, partial size 50000, record size 100000 (gap at end) */
TEST_CASE("Partial-shrink/insertGapsAtEndTestBiggest", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsAtEndTestBiggest();
}

/* write at offset 0, partial size 50001, record size 100001 (gap at end) */
TEST_CASE("Partial-shrink/insertGapsAtEndTestBiggestPlus1", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsAtEndTestBiggestPlus1();
}

/* write at offset 0, partial size 500000, record size 1000000 (gap at end) */
TEST_CASE("Partial-shrink/insertGapsAtEndTestSuperbig", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsAtEndTestSuperbig();
}

/* write at offset 0, partial size 500001, record size 1000001 (gap at end) */
TEST_CASE("Partial-shrink/insertGapsAtEndTestSuperbigPlus1", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsAtEndTestSuperbigPlus1();
}

/* write at offset 50, partial size 50, record size 100 (gap at beginning) */
TEST_CASE("Partial-shrink/insertGapsAtBeginningSmall", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsAtBeginningSmall();
}

/* write at offset 500, partial size 500, record size 1000 (gap at beginning) */
TEST_CASE("Partial-shrink/insertGapsAtBeginningBig", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsAtBeginningBig();
}

/* write at offset 5000, partial size 5000, record size 10000 (gap at beginning) */
TEST_CASE("Partial-shrink/insertGapsAtBeginningBigger", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsAtBeginningBigger();
}

/* write at offset 5001, partial size 5001, record size 10001 (gap at beginning) */
TEST_CASE("Partial-shrink/insertGapsAtBeginningBiggerPlus1", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsAtBeginningBiggerPlus1();
}

/* write at offset 50000, partial size 50000, record size 100000 (gap at beginning) */
TEST_CASE("Partial-shrink/insertGapsAtBeginningBiggest", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsAtBeginningBiggest();
}

/* write at offset 50001, partial size 50001, record size 100001 (gap at beginning) */
TEST_CASE("Partial-shrink/insertGapsAtBeginningBiggestPlus1", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsAtBeginningBiggestPlus1();
}

/* write at offset 500000, partial size 500000, record size 1000000 (gap at beginning) */
TEST_CASE("Partial-shrink/insertGapsAtBeginningSuperbig", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsAtBeginningSuperbig();
}

/* write at offset 500001, partial size 500001, record size 1000001 (gap at beginning) */
TEST_CASE("Partial-shrink/insertGapsAtBeginningSuperbigPlus1", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsAtBeginningSuperbigPlus1();
}

/* write at offset 50, partial size 50, record size 200 (gap at
* beginning AND end) */
TEST_CASE("Partial-shrink/insertGapsTestSmall", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsTestSmall();
}

/* write at offset 500, partial size 500, record size 2000 (gap at
* beginning AND end) */
TEST_CASE("Partial-shrink/insertGapsTestBig", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsTestBig();
}

/* write at offset 5000, partial size 5000, record size 20000 (gap at
* beginning AND end) */
TEST_CASE("Partial-shrink/insertGapsTestBigger", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsTestBigger();
}

/* write at offset 5001, partial size 5001, record size 20001 (gap at
* beginning AND end) */
TEST_CASE("Partial-shrink/insertGapsTestBiggerPlus1", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsTestBiggerPlus1();
}

/* write at offset 50000, partial size 50000, record size 200000 (gap at
* beginning AND end) */
TEST_CASE("Partial-shrink/insertGapsTestBiggest", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsTestBiggest();
}

/* write at offset 50001, partial size 50001, record size 200001 (gap at
* beginning AND end) */
TEST_CASE("Partial-shrink/insertGapsTestBiggestPlus1", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsTestBiggestPlus1();
}

/* write at offset 500000, partial size 500000, record size 2000000
* (gap at beginning AND end) */
TEST_CASE("Partial-shrink/insertGapsTestSuperbig", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsTestSuperbig();
}

/* write at offset 500001, partial size 500001, record size 2000001
* (gap at beginning AND end) */
TEST_CASE("Partial-shrink/insertGapsTestSuperbigPlus1", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsTestSuperbigPlus1();
}

/* write at offset PS, partial size PS, record size 2*PS
* (gap at beginning AND end) */
TEST_CASE("Partial-shrink/insertGapsTestPagesize", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsTestPagesize();
}

/* write at offset PS*2, partial size PS*2, record size 4*PS
* (gap at beginning AND end) */
TEST_CASE("Partial-shrink/insertGapsTestPagesize2", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsTestPagesize2();
}

/* write at offset PS*4, partial size PS*4, record size 8*PS
* (gap at beginning AND end) */
TEST_CASE("Partial-shrink/insertGapsTestPagesize4", "")
{
  ShrinkPartialWriteFixture f;
  f.insertGapsTestPagesize4();
}


struct GrowPartialWriteFixture : public PartialWriteFixture {
  GrowPartialWriteFixture() {
  }

  void fillBufferReverse(uint8_t *ptr, uint32_t size) {
    for (uint32_t i = 0; i < size; i++)
      ptr[i] = (uint8_t)(0xff - i);
  }

  virtual void insertGaps(unsigned partial_offset,
          unsigned partial_size, unsigned record_size) {
    ups_key_t key = {};
    ups_record_t rec = {};
    uint8_t *buffer = (uint8_t *)malloc(record_size);

    REQUIRE((unsigned)(partial_offset + partial_size) <= record_size);

    /* first: insert a record with a unique pattern and insert this record.
     * this record will be SMALLER then the one which overwrites */
    fillBufferReverse(&buffer[0], record_size);
    rec.size = record_size / 2;
    rec.data = buffer;
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, 0));

    /* then fill the buffer with another pattern and insert the partial
     * record */
    fillBuffer(&buffer[0], 0, record_size);

    rec.partial_offset = partial_offset;
    rec.partial_size = partial_size;
    rec.size = record_size;
    rec.data = buffer;
    REQUIRE(0 ==
        ups_db_insert(m_db, 0, &key, &rec, UPS_PARTIAL|UPS_OVERWRITE));

    /* verify the key */
    memset(&rec, 0, sizeof(rec));
    REQUIRE(0 == ups_db_find(m_db, 0, &key, &rec, 0));

    memset(&buffer[0], 0, record_size);
    fillBuffer(&buffer[partial_offset], 0, partial_size);
    REQUIRE(record_size == rec.size);
    REQUIRE(0 == memcmp(buffer, rec.data, rec.size));
    free(buffer);
  }
};

/* write at offset 0, partial size 50, record size 50 (no gaps) */
TEST_CASE("Partial-grow/simpleInsertTest", "")
{
  GrowPartialWriteFixture f;
  f.simpleInsertTest();
}

/* write at offset 0, partial size 50, record size 100 (gap at end) */
TEST_CASE("Partial-grow/insertGapsAtEndTestSmall", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsAtEndTestSmall();
}

/* write at offset 0, partial size 500, record size 1000 (gap at end) */
TEST_CASE("Partial-grow/insertGapsAtEndTestBig", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsAtEndTestBig();
}

/* write at offset 0, partial size 5000, record size 10000 (gap at end) */
TEST_CASE("Partial-grow/insertGapsAtEndTestBigger", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsAtEndTestBigger();
}

/* write at offset 0, partial size 5001, record size 10001 (gap at end) */
TEST_CASE("Partial-grow/insertGapsAtEndTestBiggerPlus1", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsAtEndTestBiggerPlus1();
}

/* write at offset 0, partial size 50000, record size 100000 (gap at end) */
TEST_CASE("Partial-grow/insertGapsAtEndTestBiggest", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsAtEndTestBiggest();
}

/* write at offset 0, partial size 50001, record size 100001 (gap at end) */
TEST_CASE("Partial-grow/insertGapsAtEndTestBiggestPlus1", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsAtEndTestBiggestPlus1();
}

/* write at offset 0, partial size 500000, record size 1000000 (gap at end) */
TEST_CASE("Partial-grow/insertGapsAtEndTestSuperbig", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsAtEndTestSuperbig();
}

/* write at offset 0, partial size 500001, record size 1000001 (gap at end) */
TEST_CASE("Partial-grow/insertGapsAtEndTestSuperbigPlus1", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsAtEndTestSuperbigPlus1();
}

/* write at offset 50, partial size 50, record size 100 (gap at beginning) */
TEST_CASE("Partial-grow/insertGapsAtBeginningSmall", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsAtBeginningSmall();
}

/* write at offset 500, partial size 500, record size 1000 (gap at beginning) */
TEST_CASE("Partial-grow/insertGapsAtBeginningBig", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsAtBeginningBig();
}

/* write at offset 5000, partial size 5000, record size 10000 (gap at beginning) */
TEST_CASE("Partial-grow/insertGapsAtBeginningBigger", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsAtBeginningBigger();
}

/* write at offset 5001, partial size 5001, record size 10001 (gap at beginning) */
TEST_CASE("Partial-grow/insertGapsAtBeginningBiggerPlus1", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsAtBeginningBiggerPlus1();
}

/* write at offset 50000, partial size 50000, record size 100000 (gap at beginning) */
TEST_CASE("Partial-grow/insertGapsAtBeginningBiggest", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsAtBeginningBiggest();
}

/* write at offset 50001, partial size 50001, record size 100001 (gap at beginning) */
TEST_CASE("Partial-grow/insertGapsAtBeginningBiggestPlus1", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsAtBeginningBiggestPlus1();
}

/* write at offset 500000, partial size 500000, record size 1000000 (gap at beginning) */
TEST_CASE("Partial-grow/insertGapsAtBeginningSuperbig", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsAtBeginningSuperbig();
}

/* write at offset 500001, partial size 500001, record size 1000001 (gap at beginning) */
TEST_CASE("Partial-grow/insertGapsAtBeginningSuperbigPlus1", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsAtBeginningSuperbigPlus1();
}

/* write at offset 50, partial size 50, record size 200 (gap at
* beginning AND end) */
TEST_CASE("Partial-grow/insertGapsTestSmall", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsTestSmall();
}

/* write at offset 500, partial size 500, record size 2000 (gap at
* beginning AND end) */
TEST_CASE("Partial-grow/insertGapsTestBig", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsTestBig();
}

/* write at offset 5000, partial size 5000, record size 20000 (gap at
* beginning AND end) */
TEST_CASE("Partial-grow/insertGapsTestBigger", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsTestBigger();
}

/* write at offset 5001, partial size 5001, record size 20001 (gap at
* beginning AND end) */
TEST_CASE("Partial-grow/insertGapsTestBiggerPlus1", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsTestBiggerPlus1();
}

/* write at offset 50000, partial size 50000, record size 200000 (gap at
* beginning AND end) */
TEST_CASE("Partial-grow/insertGapsTestBiggest", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsTestBiggest();
}

/* write at offset 50001, partial size 50001, record size 200001 (gap at
* beginning AND end) */
TEST_CASE("Partial-grow/insertGapsTestBiggestPlus1", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsTestBiggestPlus1();
}

/* write at offset 500000, partial size 500000, record size 2000000
* (gap at beginning AND end) */
TEST_CASE("Partial-grow/insertGapsTestSuperbig", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsTestSuperbig();
}

/* write at offset 500001, partial size 500001, record size 2000001
* (gap at beginning AND end) */
TEST_CASE("Partial-grow/insertGapsTestSuperbigPlus1", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsTestSuperbigPlus1();
}

/* write at offset PS, partial size PS, record size 2*PS
* (gap at beginning AND end) */
TEST_CASE("Partial-grow/insertGapsTestPagesize", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsTestPagesize();
}

/* write at offset PS*2, partial size PS*2, record size 4*PS
* (gap at beginning AND end) */
TEST_CASE("Partial-grow/insertGapsTestPagesize2", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsTestPagesize2();
}

/* write at offset PS*4, partial size PS*4, record size 8*PS
* (gap at beginning AND end) */
TEST_CASE("Partial-grow/insertGapsTestPagesize4", "")
{
  GrowPartialWriteFixture f;
  f.insertGapsTestPagesize4();
}

struct PartialReadFixture {
  uint32_t m_page_size;
  bool m_inmemory;
  uint32_t m_find_flags;
  ups_db_t *m_db;
  ups_env_t *m_env;

  PartialReadFixture(uint32_t page_size = 0, bool inmemory = false,
                  uint32_t find_flags = 0)
    : m_page_size(page_size), m_inmemory(inmemory), m_find_flags(find_flags) {
    ups_parameter_t params[] = {
      { 0, 0 },
      { 0, 0 }
    };

    if (m_page_size) {
      params[0].name = UPS_PARAM_PAGESIZE;
      params[0].value = m_page_size;
    }

    REQUIRE(0 ==
        ups_env_create(&m_env, Utils::opath(".test"),
            m_inmemory ? UPS_IN_MEMORY : 0, 0644, &params[0]));
    REQUIRE(0 ==
        ups_env_create_db(m_env, &m_db, 1, 0, 0));
  }

  ~PartialReadFixture() {
    REQUIRE(0 == ups_env_close(m_env, UPS_AUTO_CLEANUP));
  }

  void fillBuffer(uint8_t *ptr, uint32_t offset, uint32_t size) {
    for (uint32_t i = 0; i < size; i++)
      ptr[i] = (uint8_t)(offset + i);
  }

  void simpleFindTest() {
    ups_key_t key = {};
    ups_record_t rec = {};
    uint8_t buffer[50];

    /* fill the buffer with a pattern */
    fillBuffer(&buffer[0], 0, sizeof(buffer));

    /* write a record of 50 bytes */
    rec.size = 50;
    rec.data = buffer;
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, 0));

    /* read at 0, 50 (no gap) */
    memset(&rec, 0, sizeof(rec));
    rec.partial_offset = 0;
    rec.partial_size = 50;
    REQUIRE(0 ==
        ups_db_find(m_db, 0, &key, &rec, UPS_PARTIAL|m_find_flags));

    REQUIRE(50u == rec.size);
    REQUIRE(0 == memcmp(buffer, rec.data, rec.size));
  }

  void findTest(unsigned partial_offset, unsigned partial_size,
          unsigned record_size) {
    ups_key_t key = {};
    ups_record_t rec = {};
    uint8_t *buffer = (uint8_t *)malloc(record_size);

    /* fill the buffer with a pattern */
    fillBuffer(&buffer[0], 0, record_size);

    /* write the record */
    rec.size = record_size;
    rec.data = buffer;
    REQUIRE(0 == ups_db_insert(m_db, 0, &key, &rec, 0));

    /* now do the partial read */
    memset(&rec, 0, sizeof(rec));
    rec.partial_offset = partial_offset;
    rec.partial_size = partial_size;
    REQUIRE(0 ==
        ups_db_find(m_db, 0, &key, &rec, UPS_PARTIAL|m_find_flags));

    memset(&buffer[0], 0, record_size);
    fillBuffer(&buffer[0], partial_offset, partial_size);
    REQUIRE(partial_size == rec.partial_size);
    REQUIRE(record_size == rec.size);
    REQUIRE(0 == memcmp(buffer, rec.data, rec.partial_size));

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

#include "partial-read-ps1.h"
#include "partial-read-ps2.h"
#include "partial-read-ps4.h"
#include "partial-read-ps16.h"
#include "partial-read-ps64.h"

#include "partial-read-inmem-ps1.h"
#include "partial-read-inmem-ps2.h"
#include "partial-read-inmem-ps4.h"
#include "partial-read-inmem-ps16.h"
#include "partial-read-inmem-ps64.h"

#include "partial-read-direct-ps1.h"
#include "partial-read-direct-ps2.h"
#include "partial-read-direct-ps4.h"
#include "partial-read-direct-ps16.h"
#include "partial-read-direct-ps64.h"

struct MiscPartialFixture {
  ups_db_t *m_db;
  ups_env_t *m_env;
  bool m_inmemory;
  uint32_t m_find_flags;

  MiscPartialFixture(bool inmemory = false, uint32_t find_flags = 0)
    : m_inmemory(inmemory), m_find_flags(find_flags) {
    REQUIRE(0 ==
        ups_env_create(&m_env, Utils::opath(".test"),
            m_inmemory ? UPS_IN_MEMORY : 0, 0644, 0));
    REQUIRE(0 ==
        ups_env_create_db(m_env, &m_db, 1, 0, 0));
  }

  ~MiscPartialFixture() {
    REQUIRE(0 == ups_env_close(m_env, UPS_AUTO_CLEANUP));
  }

  void negativeInsertTest() {
    ups_key_t key = {};
    ups_record_t rec = {};

    ups_db_t *db;
    ups_env_t *env;
    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test.db"),
            (m_inmemory ? UPS_IN_MEMORY : 0), 0644, 0));
    REQUIRE(0 ==
        ups_env_create_db(env, &db, 1, UPS_ENABLE_DUPLICATE_KEYS, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(db, 0, &key, &rec, UPS_PARTIAL));
    REQUIRE(0 ==
        ups_db_insert(db, 0, &key, &rec, 0));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void negativeCursorInsertTest() {
    ups_key_t key = {};
    ups_record_t rec = {};

    ups_db_t *db;
    ups_env_t *env;
    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test.db"),
            (m_inmemory ? UPS_IN_MEMORY : 0), 0644, 0));
    REQUIRE(0 ==
        ups_env_create_db(env, &db, 1, UPS_ENABLE_DUPLICATE_KEYS, 0));

    ups_cursor_t *c;
    REQUIRE(0 == ups_cursor_create(&c, m_db, 0, 0));

    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_insert(c, &key, &rec, UPS_PARTIAL));
    REQUIRE(0 ==
        ups_cursor_insert(c, &key, &rec, 0));
    REQUIRE(0 == ups_cursor_close(c));

    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void invalidInsertParametersTest() {
    ups_key_t key = {};
    ups_record_t rec = {};
    uint8_t buffer[500];

    ups_cursor_t *c;
    REQUIRE(0 == ups_cursor_create(&c, m_db, 0, 0));

    rec.data = (void *)&buffer[0];
    rec.size = sizeof(buffer);

    /* partial_offset > size */
    rec.partial_offset = 600;
    rec.partial_size = 50;
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(m_db, 0, &key, &rec, UPS_PARTIAL));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_insert(c, &key, &rec, UPS_PARTIAL));

    /* partial_offset + partial_size > size */
    rec.partial_offset = 100;
    rec.partial_size = 450;
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(m_db, 0, &key, &rec, UPS_PARTIAL));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_insert(c, &key, &rec, UPS_PARTIAL));

    /* partial_size > size */
    rec.partial_offset = 0;
    rec.partial_size = 600;
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(m_db, 0, &key, &rec, UPS_PARTIAL));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_insert(c, &key, &rec, UPS_PARTIAL));

    REQUIRE(0 == ups_cursor_close(c));
  }

  void invalidFindParametersTest() {
    ups_key_t key = {};
    ups_record_t rec = {};
    uint8_t buffer[500];

    ups_cursor_t *c;
    REQUIRE(0 == ups_cursor_create(&c, m_db, 0, 0));

    rec.data = (void *)&buffer[0];
    rec.size = sizeof(buffer);
    REQUIRE(0 ==
        ups_db_insert(m_db, 0, &key, &rec, 0));

    /* partial_offset > size */
    rec.partial_offset = 600;
    rec.partial_size = 50;
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_find(m_db, 0, &key, &rec, UPS_PARTIAL|m_find_flags));
    REQUIRE(0 ==
        ups_cursor_find(c, &key, 0, 0));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_move(c, &key, &rec, UPS_PARTIAL|m_find_flags));

    REQUIRE(0 == ups_cursor_close(c));
  }

  void reduceSizeTest() {
    ups_key_t key = {};
    ups_record_t rec = {};
    uint8_t buffer[500];

    ups_cursor_t *c;
    REQUIRE(0 == ups_cursor_create(&c, m_db, 0, 0));

    rec.data = (void *)&buffer[0];
    rec.size = sizeof(buffer);
    REQUIRE(0 ==
        ups_db_insert(m_db, 0, &key, &rec, 0));

    /* partial_offset + partial_size > size */
    rec.partial_offset = 100;
    rec.partial_size = 450;
    REQUIRE(0 ==
        ups_db_find(m_db, 0, &key, &rec, UPS_PARTIAL | m_find_flags));
    REQUIRE(400u == rec.partial_size);
    REQUIRE(500u == rec.size);
    REQUIRE(0 ==
        ups_cursor_find(c, &key, 0, 0));
    REQUIRE(0 ==
        ups_cursor_move(c, &key, &rec, UPS_PARTIAL | m_find_flags));
    REQUIRE(400u == rec.partial_size);
    REQUIRE(500u == rec.size);

    /* partial_size > size */
    rec.partial_offset = 0;
    rec.partial_size = 600;
    REQUIRE(0 ==
        ups_db_find(m_db, 0, &key, &rec, UPS_PARTIAL|m_find_flags));
    REQUIRE(500u == rec.size);
    REQUIRE(0 ==
        ups_cursor_find(c, &key, 0, 0));
    REQUIRE(0 ==
        ups_cursor_move(c, &key, &rec, UPS_PARTIAL|m_find_flags));
    REQUIRE(500u == rec.size);

    REQUIRE(0 == ups_cursor_close(c));
  }

  void disabledSmallRecordsTest() {
    ups_key_t key = {0};
    ups_record_t rec = {0};
    uint8_t buffer[8];

    rec.data = (void *)&buffer[0];
    rec.size = 8;
    REQUIRE(0 ==
        ups_db_insert(m_db, 0, &key, &rec, 0));

    rec.data = (void *)&buffer[0];
    rec.size = 1;
    rec.partial_offset = 0;
    rec.partial_size = 1;
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(m_db, 0, &key, &rec, UPS_PARTIAL));

    rec.size = 5;
    rec.partial_offset = 0;
    rec.partial_size = 1;
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(m_db, 0, &key, &rec, UPS_PARTIAL));

    rec.size = 8;
    rec.partial_offset = 0;
    rec.partial_size = 1;
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(m_db, 0, &key, &rec, UPS_PARTIAL));

    rec.size = 1;
    rec.partial_offset = 0;
    rec.partial_size = 1;
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_find(m_db, 0, &key, &rec, UPS_PARTIAL));

    rec.size = 5;
    rec.partial_offset = 0;
    rec.partial_size = 1;
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_find(m_db, 0, &key, &rec, UPS_PARTIAL));

    rec.size = 8;
    rec.partial_offset = 0;
    rec.partial_size = 1;
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_find(m_db, 0, &key, &rec, UPS_PARTIAL));
  }

  void disabledTransactionsTest() {
    ups_db_t *db;
    ups_env_t *env;
    REQUIRE(0 ==
        ups_env_create(&env, Utils::opath(".test2"),
            UPS_ENABLE_TRANSACTIONS, 0644, 0));
    REQUIRE(0 ==
        ups_env_create_db(env, &db, 1, 0, 0));

    ups_cursor_t *c;
    REQUIRE(0 == ups_cursor_create(&c, db, 0, 0));

    ups_key_t key = {};
    ups_record_t rec = {};
    uint8_t buffer[16] = {0};

    rec.data = (void *)&buffer[0];
    rec.size = 16;
    REQUIRE(0 ==
        ups_db_insert(db, 0, &key, &rec, 0));

    rec.data = (void *)&buffer[0];
    rec.size = 1;
    rec.partial_offset = 0;
    rec.partial_size = 1;
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_insert(db, 0, &key, &rec, UPS_PARTIAL));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_insert(c, &key, &rec, UPS_PARTIAL));

    rec.partial_offset = 0;
    rec.partial_size = 1;
    REQUIRE(UPS_INV_PARAMETER ==
        ups_db_find(db, 0, &key, &rec, UPS_PARTIAL));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_find(c, &key, &rec, UPS_PARTIAL));
    REQUIRE(UPS_INV_PARAMETER ==
        ups_cursor_move(c, &key, &rec, UPS_PARTIAL));

    REQUIRE(0 == ups_cursor_close(c));
    REQUIRE(0 == ups_env_close(env, UPS_AUTO_CLEANUP));
  }

  void partialSizeTest() {
    ups_key_t key = {};
    ups_record_t rec = {};
    uint8_t buffer[500];

    rec.data = (void *)&buffer[0];
    rec.size = sizeof(buffer);
    REQUIRE(0 ==
        ups_db_insert(m_db, 0, &key, &rec, 0));

    rec.partial_offset = 50;
    rec.partial_size = 400;
    REQUIRE(0 ==
        ups_db_find(m_db, 0, &key, &rec, UPS_PARTIAL | m_find_flags));
    REQUIRE(500u == rec.size);
    REQUIRE(400u == rec.partial_size);
    REQUIRE(50u == rec.partial_offset);
  }
};

TEST_CASE("PartialMisc/negativeInsertTest", "")
{
  MiscPartialFixture f;
  f.negativeInsertTest();
}

TEST_CASE("PartialMisc/negativeCursorInsertTest", "")
{
  MiscPartialFixture f;
  f.negativeCursorInsertTest();
}

TEST_CASE("PartialMisc/invalidInsertParametersTest", "")
{
  MiscPartialFixture f;
  f.invalidInsertParametersTest();
}

TEST_CASE("PartialMisc/invalidFindParametersTest", "")
{
  MiscPartialFixture f;
  f.invalidFindParametersTest();
}

TEST_CASE("PartialMisc/reduceSizeTest", "")
{
  MiscPartialFixture f;
  f.reduceSizeTest();
}

TEST_CASE("PartialMisc/disabledSmallRecordsTest", "")
{
  MiscPartialFixture f;
  f.disabledSmallRecordsTest();
}

TEST_CASE("PartialMisc/disabledTransactionsTest", "")
{
  MiscPartialFixture f;
  f.disabledTransactionsTest();
}

TEST_CASE("PartialMisc/partialSizeTest", "")
{
  MiscPartialFixture f;
  f.partialSizeTest();
}

TEST_CASE("PartialMisc-inmem/negativeInsertTest", "")
{
  MiscPartialFixture f(true);
  f.negativeInsertTest();
}

TEST_CASE("PartialMisc-inmem/negativeCursorInsertTest", "")
{
  MiscPartialFixture f(true);
  f.negativeCursorInsertTest();
}

TEST_CASE("PartialMisc-inmem/invalidInsertParametersTest", "")
{
  MiscPartialFixture f(true);
  f.invalidInsertParametersTest();
}

TEST_CASE("PartialMisc-inmem/invalidFindParametersTest", "")
{
  MiscPartialFixture f(true);
  f.invalidFindParametersTest();
}

TEST_CASE("PartialMisc-inmem/reduceSizeTest", "")
{
  MiscPartialFixture f(true);
  f.reduceSizeTest();
}

TEST_CASE("PartialMisc-inmem/disabledSmallRecordsTest", "")
{
  MiscPartialFixture f(true);
  f.disabledSmallRecordsTest();
}

TEST_CASE("PartialMisc-inmem/disabledTransactionsTest", "")
{
  MiscPartialFixture f(true);
  f.disabledTransactionsTest();
}

TEST_CASE("PartialMisc-inmem/partialSizeTest", "")
{
  MiscPartialFixture f(true);
  f.partialSizeTest();
}

TEST_CASE("PartialMisc-direct/negativeInsertTest", "")
{
  MiscPartialFixture f(true, UPS_DIRECT_ACCESS);
  f.negativeInsertTest();
}

TEST_CASE("PartialMisc-direct/negativeCursorInsertTest", "")
{
  MiscPartialFixture f(true, UPS_DIRECT_ACCESS);
  f.negativeCursorInsertTest();
}

TEST_CASE("PartialMisc-direct/invalidInsertParametersTest", "")
{
  MiscPartialFixture f(true, UPS_DIRECT_ACCESS);
  f.invalidInsertParametersTest();
}

TEST_CASE("PartialMisc-direct/invalidFindParametersTest", "")
{
  MiscPartialFixture f(true, UPS_DIRECT_ACCESS);
  f.invalidFindParametersTest();
}

TEST_CASE("PartialMisc-direct/reduceSizeTest", "")
{
  MiscPartialFixture f(true, UPS_DIRECT_ACCESS);
  f.reduceSizeTest();
}

TEST_CASE("PartialMisc-direct/disabledSmallRecordsTest", "")
{
  MiscPartialFixture f(true, UPS_DIRECT_ACCESS);
  f.disabledSmallRecordsTest();
}

TEST_CASE("PartialMisc-direct/disabledTransactionsTest", "")
{
  MiscPartialFixture f(true, UPS_DIRECT_ACCESS);
  f.disabledTransactionsTest();
}

TEST_CASE("PartialMisc-direct/partialSizeTest", "")
{
  MiscPartialFixture f(true, UPS_DIRECT_ACCESS);
  f.partialSizeTest();
}
