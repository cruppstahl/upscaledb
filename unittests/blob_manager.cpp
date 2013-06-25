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

#include "../src/db.h"
#include "../src/blob_manager.h"
#include "../src/env.h"
#include "../src/page.h"
#include "../src/btree_key.h"
#include "../src/page_manager.h"

using namespace hamsterdb;

struct BlobManagerFixture {
  ham_db_t *m_db;
  ham_env_t *m_env;
  bool m_inmemory;
  bool m_use_txn;
  ham_size_t m_cachesize;
  ham_size_t m_pagesize;
  BlobManager *m_blob_manager;

  BlobManagerFixture(bool inmemory = false, bool use_txn = false,
        ham_size_t cachesize = 0, ham_size_t pagesize = 0)
    : m_db(0), m_inmemory(inmemory), m_use_txn(use_txn),
      m_cachesize(cachesize), m_pagesize(pagesize) {
    ham_parameter_t params[3] = {
      { HAM_PARAM_CACHESIZE, m_cachesize },
      // set pagesize, otherwise 16-bit limit bugs in freelist
      // will fire on Win32
      { HAM_PARAM_PAGESIZE, (m_pagesize ? m_pagesize : 4096) },
      { 0, 0 }
    };

    os::unlink(Globals::opath(".test"));

    REQUIRE(0 ==
        ham_env_create(&m_env, Globals::opath(".test"),
          (m_inmemory
            ? HAM_IN_MEMORY
            : (m_use_txn
              ? HAM_ENABLE_TRANSACTIONS
              : 0)),
            0644, &params[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
    m_blob_manager = ((Environment *)m_env)->get_blob_manager();
  }

  ~BlobManagerFixture() {
    /* clear the changeset, otherwise ham_db_close will complain */
    if (!m_inmemory && m_env)
      ((Environment *)m_env)->get_changeset().clear();
    if (m_env)
        REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void structureTest() {
    PBlobHeader b;

    b.set_self((ham_u64_t)0x12345ull);
    REQUIRE((ham_u64_t)0x12345ull == b.get_self());

    b.set_alloc_size(0x789ull);
    REQUIRE((ham_u64_t)0x789ull == b.get_alloc_size());

    b.set_size(0x123ull);
    REQUIRE((ham_u64_t)0x123ull == b.get_size());
  }

  void dupeStructureTest() {
    PDupeTable t;
    ::memset(&t, 0, sizeof(t));

    dupe_table_set_count(&t, 0x789ull);
    REQUIRE((ham_u32_t)0x789ull == dupe_table_get_count(&t));

    dupe_table_set_capacity(&t, 0x123ull);
    REQUIRE((ham_u32_t)0x123ull == dupe_table_get_capacity(&t));

    PDupeEntry *e = dupe_table_get_entry(&t, 0);
    dupe_entry_set_flags(e, 0x13);
    REQUIRE((ham_u8_t)0x13 == dupe_entry_get_flags(e));

    dupe_entry_set_rid(e, (ham_u64_t)0x12345ull);
    REQUIRE((ham_u64_t)0x12345ull ==
            dupe_entry_get_rid(e));
  }

  void allocReadFreeTest() {
    ham_u8_t buffer[64];
    ham_u64_t blobid;
    ham_record_t record;
    ::memset(&record, 0, sizeof(record));
    ::memset(&buffer, 0x12, sizeof(buffer));

    record.size = sizeof(buffer);
    record.data = buffer;
    REQUIRE(0 ==
        m_blob_manager->allocate((LocalDatabase *)m_db, &record, 0, &blobid));
    REQUIRE(blobid != 0ull);

    ByteArray *arena = &((LocalDatabase *)m_db)->get_record_arena();

    REQUIRE(0 ==
        m_blob_manager->read((LocalDatabase *)m_db,
                blobid, &record, 0, arena));
    REQUIRE(record.size == (ham_size_t)sizeof(buffer));
    REQUIRE(0 == ::memcmp(buffer, record.data, record.size));

    REQUIRE(0 == m_blob_manager->free((LocalDatabase *)m_db, blobid, 0));
  }

  void replaceTest() {
    ham_u8_t buffer[64], buffer2[64];
    ham_u64_t blobid, blobid2;
    ham_record_t record;
    ::memset(&record,  0, sizeof(record));
    ::memset(&buffer,  0x12, sizeof(buffer));
    ::memset(&buffer2, 0x15, sizeof(buffer2));

    record.size = sizeof(buffer);
    record.data = buffer;
    REQUIRE(0 ==
        m_blob_manager->allocate((LocalDatabase *)m_db, &record, 0, &blobid));
    REQUIRE(blobid != 0ull);

    ByteArray *arena = &((LocalDatabase *)m_db)->get_record_arena();
    REQUIRE(0 ==
        m_blob_manager->read((LocalDatabase *)m_db, blobid, &record, 0, arena));
    REQUIRE(record.size == (ham_size_t)sizeof(buffer));
    REQUIRE(0 == ::memcmp(buffer, record.data, record.size));

    record.size = sizeof(buffer2);
    record.data = buffer2;
    REQUIRE(0 == m_blob_manager->overwrite((LocalDatabase *)m_db,
                blobid, &record, 0, &blobid2));
    REQUIRE(blobid2 != 0ull);

    REQUIRE(0 ==
        m_blob_manager->read((LocalDatabase *)m_db, blobid2, &record, 0, arena));
    REQUIRE(record.size == (ham_size_t)sizeof(buffer2));
    REQUIRE(0 == ::memcmp(buffer2, record.data, record.size));

    REQUIRE(0 == m_blob_manager->free((LocalDatabase *)m_db, blobid2, 0));
  }

  void replaceWithBigTest() {
    ham_u8_t buffer[64], buffer2[128];
    ham_u64_t blobid, blobid2;
    ham_record_t record;
    ::memset(&record,  0, sizeof(record));
    ::memset(&buffer,  0x12, sizeof(buffer));
    ::memset(&buffer2, 0x15, sizeof(buffer2));

    record.data = buffer;
    record.size = sizeof(buffer);
    REQUIRE(0 ==
        m_blob_manager->allocate((LocalDatabase *)m_db, &record, 0, &blobid));
    REQUIRE(blobid != 0ull);

    ByteArray *arena = &((LocalDatabase *)m_db)->get_record_arena();
    REQUIRE(0 ==
        m_blob_manager->read((LocalDatabase *)m_db, blobid, &record, 0, arena));
    REQUIRE(record.size == (ham_size_t)sizeof(buffer));
    REQUIRE(0 == ::memcmp(buffer, record.data, record.size));

    record.size = sizeof(buffer2);
    record.data = buffer2;
    REQUIRE(0 == m_blob_manager->overwrite((LocalDatabase *)m_db, blobid,
            &record, 0, &blobid2));
    REQUIRE(blobid2 != 0ull);

    REQUIRE(0 ==
        m_blob_manager->read((LocalDatabase *)m_db, blobid2, &record, 0, arena));
    REQUIRE(record.size == (ham_size_t)sizeof(buffer2));
    REQUIRE(0 == ::memcmp(buffer2, record.data, record.size));

    REQUIRE(0 == m_blob_manager->free((LocalDatabase *)m_db, blobid2, 0));
  }

  void replaceWithSmallTest() {
    ham_u8_t buffer[128], buffer2[64];
    ham_u64_t blobid, blobid2;
    ham_record_t record;
    ::memset(&record,  0, sizeof(record));
    ::memset(&buffer,  0x12, sizeof(buffer));
    ::memset(&buffer2, 0x15, sizeof(buffer2));

    record.data = buffer;
    record.size = sizeof(buffer);
    REQUIRE(0 ==
        m_blob_manager->allocate((LocalDatabase *)m_db, &record, 0, &blobid));
    REQUIRE(blobid != 0ull);

    ByteArray *arena = &((LocalDatabase *)m_db)->get_record_arena();
    REQUIRE(0 ==
        m_blob_manager->read((LocalDatabase *)m_db, blobid, &record, 0, arena));
    REQUIRE(record.size == (ham_size_t)sizeof(buffer));
    REQUIRE(0 == ::memcmp(buffer, record.data, record.size));

    record.size = sizeof(buffer2);
    record.data = buffer2;
    REQUIRE(0 == m_blob_manager->overwrite((LocalDatabase *)m_db,
                blobid, &record, 0, &blobid2));
    REQUIRE(blobid2 != 0ull);

    REQUIRE(0 ==
        m_blob_manager->read((LocalDatabase *)m_db, blobid2, &record, 0, arena));
    REQUIRE(record.size == (ham_size_t)sizeof(buffer2));
    REQUIRE(0 == ::memcmp(buffer2, record.data, record.size));

    /* make sure that at least 64bit are in the freelist */
    if (!m_inmemory) {
      ham_u64_t addr;
      Freelist *f = ((Environment *)m_env)->get_page_manager()->get_freelist(0);
      REQUIRE(0 == f->alloc_area(64, &addr));
      REQUIRE(addr != 0ull);
    }

    REQUIRE(0 == m_blob_manager->free((LocalDatabase *)m_db, blobid2, 0));

    /* and now another 64bit should be in the freelist */
    if (!m_inmemory) {
      ham_u64_t addr;
      Freelist *f = ((Environment *)m_env)->get_page_manager()->get_freelist(0);
      REQUIRE(0 == f->alloc_area(64, &addr));
      REQUIRE(addr != 0ull);
    }
  }

  void replaceBiggerAndBiggerTest() {
    const int BLOCKS = 32;
    unsigned ps = ((Environment *)m_env)->get_pagesize();
    ham_u8_t *buffer = (ham_u8_t *)malloc(ps * BLOCKS * 2);
    ham_u64_t blobid, blobid2;
    ham_record_t record;
    ::memset(&record, 0, sizeof(record));
    ::memset(buffer,  0, ps*BLOCKS*2);

    /* first: create a big blob and erase it - we want to use the
     * space from the freelist */
    record.data = buffer;
    record.size = ps * BLOCKS * 2;
    REQUIRE(0 ==
        m_blob_manager->allocate((LocalDatabase *)m_db, &record, 0, &blobid));
    REQUIRE(blobid != 0ull);

    /* verify it */
    ByteArray *arena = &((LocalDatabase *)m_db)->get_record_arena();
    REQUIRE(0 ==
        m_blob_manager->read((LocalDatabase *)m_db, blobid, &record, 0, arena));
    REQUIRE(record.size == (ham_size_t)ps * BLOCKS * 2);

    /* and erase it */
    REQUIRE(0 == m_blob_manager->free((LocalDatabase *)m_db, blobid, 0));

    /* now use a loop to allocate the buffer, and make it bigger and
     * bigger */
    for (int i = 1; i < 32; i++) {
      record.size = i * ps;
      record.data = (void *)buffer;
      ::memset(buffer, i, record.size);
      if (i == 1) {
        REQUIRE(0 == m_blob_manager->allocate((LocalDatabase *)m_db, &record,
                0, &blobid2));
      }
      else {
        REQUIRE(0 == m_blob_manager->overwrite((LocalDatabase *)m_db, blobid,
                  &record, 0, &blobid2));
      }
      blobid = blobid2;
      REQUIRE(blobid != 0ull);
    }
    REQUIRE(0 == m_blob_manager->free((LocalDatabase *)m_db, blobid, 0));
    ::free(buffer);
  }

  void loopInsert(int loops, int factor) {
    ham_u8_t *buffer;
    ham_u64_t *blobid;
    ham_record_t record;
    ham_txn_t *txn = 0; /* need a txn object for the blob routines */
    ::memset(&record, 0, sizeof(record));
    ::memset(&buffer, 0x12, sizeof(buffer));

    blobid = (ham_u64_t *)::malloc(sizeof(ham_u64_t)*loops);
    REQUIRE(blobid != 0);
    if (!m_inmemory && m_use_txn)
      REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    for (int i = 0; i < loops; i++) {
      buffer = (ham_u8_t *)::malloc((i + 1) * factor);
      REQUIRE(buffer != 0);
      ::memset(buffer, (char)i, (i + 1) * factor);

      ham_record_t rec = {0};
      rec.data = buffer;
      rec.size = (ham_size_t)((i + 1) * factor);
      REQUIRE(0 == m_blob_manager->allocate((LocalDatabase *)m_db, &rec,
                  0, &blobid[i]));
      REQUIRE(blobid[i] != 0ull);

      ::free(buffer);
    }

    ByteArray *arena = &((LocalDatabase *)m_db)->get_record_arena();

    for (int i = 0; i < loops; i++) {
      buffer = (ham_u8_t *)::malloc((i + 1) * factor);
      REQUIRE(buffer != 0);
      ::memset(buffer, (char)i, (i + 1) * factor);

      REQUIRE(0 == m_blob_manager->read((LocalDatabase *)m_db, blobid[i], &record,
                    0, arena));
      REQUIRE(record.size == (ham_size_t)(i+1)*factor);
      REQUIRE(0 == ::memcmp(buffer, record.data, record.size));

      ::free(buffer);
    }

    for (int i = 0; i < loops; i++) {
      REQUIRE(0 == m_blob_manager->free((LocalDatabase *)m_db, blobid[i], 0));
    }

    ::free(blobid);
    if (!m_inmemory && m_use_txn)
      REQUIRE(0 == ham_txn_commit(txn, 0));
  }

  void multipleAllocReadFreeTest() {
    loopInsert(20, 2048);
  }

  void hugeBlobTest() {
    loopInsert(10, 1024 * 1024 * 4);
  }

  void smallBlobTest() {
    loopInsert(20, 64);
  }
};


TEST_CASE("BlobManager/structureTest", "")
{
  BlobManagerFixture f(false, true, 1024);
  f.structureTest();
}

TEST_CASE("BlobManager/dupeStructureTest", "")
{
  BlobManagerFixture f(false, true, 1024);
  f.dupeStructureTest();
}

TEST_CASE("BlobManager/allocReadFreeTest", "")
{
  BlobManagerFixture f(false, true, 1024);
  f.allocReadFreeTest();
}

TEST_CASE("BlobManager/replaceTest", "")
{
  BlobManagerFixture f(false, true, 1024);
  f.replaceTest();
}

TEST_CASE("BlobManager/replaceWithBigTest", "")
{
  BlobManagerFixture f(false, true, 1024);
  f.replaceWithBigTest();
}

TEST_CASE("BlobManager/replaceWithSmallTest", "")
{
  BlobManagerFixture f(false, true, 1024);
  f.replaceWithSmallTest();
}

TEST_CASE("BlobManager/replaceBiggerAndBiggerTest", "")
{
  BlobManagerFixture f(false, true, 1024);
  f.replaceBiggerAndBiggerTest();
}

TEST_CASE("BlobManager/multipleAllocReadFreeTest", "")
{
  BlobManagerFixture f(false, true, 1024);
  f.multipleAllocReadFreeTest();
}

TEST_CASE("BlobManager/hugeBlobTest", "")
{
  BlobManagerFixture f(false, true, 1024);
  f.hugeBlobTest();
}

TEST_CASE("BlobManager/smallBlobTest", "")
{
  BlobManagerFixture f(false, true, 1024);
  f.smallBlobTest();
}


TEST_CASE("BlobManager-notxn/structureTest", "")
{
  BlobManagerFixture f(false, false, 1024);
  f.structureTest();
}

TEST_CASE("BlobManager-notxn/dupeStructureTest", "")
{
  BlobManagerFixture f(false, false, 1024);
  f.dupeStructureTest();
}

TEST_CASE("BlobManager-notxn/allocReadFreeTest", "")
{
  BlobManagerFixture f(false, false, 1024);
  f.allocReadFreeTest();
}

TEST_CASE("BlobManager-notxn/replaceTest", "")
{
  BlobManagerFixture f(false, false, 1024);
  f.replaceTest();
}

TEST_CASE("BlobManager-notxn/replaceWithBigTest", "")
{
  BlobManagerFixture f(false, false, 1024);
  f.replaceWithBigTest();
}

TEST_CASE("BlobManager-notxn/replaceWithSmallTest", "")
{
  BlobManagerFixture f(false, false, 1024);
  f.replaceWithSmallTest();
}

TEST_CASE("BlobManager-notxn/replaceBiggerAndBiggerTest", "")
{
  BlobManagerFixture f(false, false, 1024);
  f.replaceBiggerAndBiggerTest();
}

TEST_CASE("BlobManager-notxn/multipleAllocReadFreeTest", "")
{
  BlobManagerFixture f(false, false, 1024);
  f.multipleAllocReadFreeTest();
}

TEST_CASE("BlobManager-notxn/hugeBlobTest", "")
{
  BlobManagerFixture f(false, false, 1024);
  f.hugeBlobTest();
}

TEST_CASE("BlobManager-notxn/smallBlobTest", "")
{
  BlobManagerFixture f(false, false, 1024);
  f.smallBlobTest();
}


TEST_CASE("BlobManager-64k/structureTest", "")
{
  BlobManagerFixture f(false, true, 1024 * 64, 1024 * 64);
  f.structureTest();
}

TEST_CASE("BlobManager-64k/dupeStructureTest", "")
{
  BlobManagerFixture f(false, true, 1024 * 64, 1024 * 64);
  f.dupeStructureTest();
}

TEST_CASE("BlobManager-64k/allocReadFreeTest", "")
{
  BlobManagerFixture f(false, true, 1024 * 64, 1024 * 64);
  f.allocReadFreeTest();
}

TEST_CASE("BlobManager-64k/replaceTest", "")
{
  BlobManagerFixture f(false, true, 1024 * 64, 1024 * 64);
  f.replaceTest();
}

TEST_CASE("BlobManager-64k/replaceWithBigTest", "")
{
  BlobManagerFixture f(false, true, 1024 * 64, 1024 * 64);
  f.replaceWithBigTest();
}

TEST_CASE("BlobManager-64k/replaceWithSmallTest", "")
{
  BlobManagerFixture f(false, true, 1024 * 64, 1024 * 64);
  f.replaceWithSmallTest();
}

TEST_CASE("BlobManager-64k/replaceBiggerAndBiggerTest", "")
{
  BlobManagerFixture f(false, true, 1024 * 64, 1024 * 64);
  f.replaceBiggerAndBiggerTest();
}

TEST_CASE("BlobManager-64k/multipleAllocReadFreeTest", "")
{
  BlobManagerFixture f(false, true, 1024 * 64, 1024 * 64);
  f.multipleAllocReadFreeTest();
}

TEST_CASE("BlobManager-64k/hugeBlobTest", "")
{
  BlobManagerFixture f(false, true, 1024 * 64, 1024 * 64);
  f.hugeBlobTest();
}

TEST_CASE("BlobManager-64k/smallBlobTest", "")
{
  BlobManagerFixture f(false, true, 1024 * 64, 1024 * 64);
  f.smallBlobTest();
}


TEST_CASE("BlobManager-nocache/structureTest", "")
{
  BlobManagerFixture f(false, true, 0);
  f.structureTest();
}

TEST_CASE("BlobManager-nocache/dupeStructureTest", "")
{
  BlobManagerFixture f(false, true, 0);
  f.dupeStructureTest();
}

TEST_CASE("BlobManager-nocache/allocReadFreeTest", "")
{
  BlobManagerFixture f(false, true, 0);
  f.allocReadFreeTest();
}

TEST_CASE("BlobManager-nocache/replaceTest", "")
{
  BlobManagerFixture f(false, true, 0);
  f.replaceTest();
}

TEST_CASE("BlobManager-nocache/replaceWithBigTest", "")
{
  BlobManagerFixture f(false, true, 0);
  f.replaceWithBigTest();
}

TEST_CASE("BlobManager-nocache/replaceWithSmallTest", "")
{
  BlobManagerFixture f(false, true, 0);
  f.replaceWithSmallTest();
}

TEST_CASE("BlobManager-nocache/replaceBiggerAndBiggerTest", "")
{
  BlobManagerFixture f(false, true, 0);
  f.replaceBiggerAndBiggerTest();
}

TEST_CASE("BlobManager-nocache/multipleAllocReadFreeTest", "")
{
  BlobManagerFixture f(false, true, 0);
  f.multipleAllocReadFreeTest();
}

TEST_CASE("BlobManager-nocache/hugeBlobTest", "")
{
  BlobManagerFixture f(false, true, 0);
  f.hugeBlobTest();
}

TEST_CASE("BlobManager-nocache/smallBlobTest", "")
{
  BlobManagerFixture f(false, true, 0);
  f.smallBlobTest();
}


TEST_CASE("BlobManager-nocache-notxn/structureTest", "")
{
  BlobManagerFixture f(false, false, 0);
  f.structureTest();
}

TEST_CASE("BlobManager-nocache-notxn/dupeStructureTest", "")
{
  BlobManagerFixture f(false, false, 0);
  f.dupeStructureTest();
}

TEST_CASE("BlobManager-nocache-notxn/allocReadFreeTest", "")
{
  BlobManagerFixture f(false, false, 0);
  f.allocReadFreeTest();
}

TEST_CASE("BlobManager-nocache-notxn/replaceTest", "")
{
  BlobManagerFixture f(false, false, 0);
  f.replaceTest();
}

TEST_CASE("BlobManager-nocache-notxn/replaceWithBigTest", "")
{
  BlobManagerFixture f(false, false, 0);
  f.replaceWithBigTest();
}

TEST_CASE("BlobManager-nocache-notxn/replaceWithSmallTest", "")
{
  BlobManagerFixture f(false, false, 0);
  f.replaceWithSmallTest();
}

TEST_CASE("BlobManager-nocache-notxn/replaceBiggerAndBiggerTest", "")
{
  BlobManagerFixture f(false, false, 0);
  f.replaceBiggerAndBiggerTest();
}

TEST_CASE("BlobManager-nocache-notxn/multipleAllocReadFreeTest", "")
{
  BlobManagerFixture f(false, false, 0);
  f.multipleAllocReadFreeTest();
}

TEST_CASE("BlobManager-nocache-notxn/hugeBlobTest", "")
{
  BlobManagerFixture f(false, false, 0);
  f.hugeBlobTest();
}

TEST_CASE("BlobManager-nocache-notxn/smallBlobTest", "")
{
  BlobManagerFixture f(false, false, 0);
  f.smallBlobTest();
}


TEST_CASE("BlobManager-nocache-64k/structureTest", "")
{
  BlobManagerFixture f(false, true, 0, 1024 * 64);
  f.structureTest();
}

TEST_CASE("BlobManager-nocache-64k/dupeStructureTest", "")
{
  BlobManagerFixture f(false, true, 0, 1024 * 64);
  f.dupeStructureTest();
}

TEST_CASE("BlobManager-nocache-64k/allocReadFreeTest", "")
{
  BlobManagerFixture f(false, true, 0, 1024 * 64);
  f.allocReadFreeTest();
}

TEST_CASE("BlobManager-nocache-64k/replaceTest", "")
{
  BlobManagerFixture f(false, true, 0, 1024 * 64);
  f.replaceTest();
}

TEST_CASE("BlobManager-nocache-64k/replaceWithBigTest", "")
{
  BlobManagerFixture f(false, true, 0, 1024 * 64);
  f.replaceWithBigTest();
}

TEST_CASE("BlobManager-nocache-64k/replaceWithSmallTest", "")
{
  BlobManagerFixture f(false, true, 0, 1024 * 64);
  f.replaceWithSmallTest();
}

TEST_CASE("BlobManager-nocache-64k/replaceBiggerAndBiggerTest", "")
{
  BlobManagerFixture f(false, true, 0, 1024 * 64);
  f.replaceBiggerAndBiggerTest();
}

TEST_CASE("BlobManager-nocache-64k/multipleAllocReadFreeTest", "")
{
  BlobManagerFixture f(false, true, 0, 1024 * 64);
  f.multipleAllocReadFreeTest();
}

TEST_CASE("BlobManager-nocache-64k/hugeBlobTest", "")
{
  BlobManagerFixture f(false, true, 0, 1024 * 64);
  f.hugeBlobTest();
}

TEST_CASE("BlobManager-nocache-64k/smallBlobTest", "")
{
  BlobManagerFixture f(false, true, 0, 1024 * 64);
  f.smallBlobTest();
}


TEST_CASE("BlobManager-inmem/structureTest", "")
{
  BlobManagerFixture f(true, false);
  f.structureTest();
}

TEST_CASE("BlobManager-inmem/dupeStructureTest", "")
{
  BlobManagerFixture f(true, false);
  f.dupeStructureTest();
}

TEST_CASE("BlobManager-inmem/allocReadFreeTest", "")
{
  BlobManagerFixture f(true, false);
  f.allocReadFreeTest();
}

TEST_CASE("BlobManager-inmem/replaceTest", "")
{
  BlobManagerFixture f(true, false);
  f.replaceTest();
}

TEST_CASE("BlobManager-inmem/replaceWithBigTest", "")
{
  BlobManagerFixture f(true, false);
  f.replaceWithBigTest();
}

TEST_CASE("BlobManager-inmem/replaceWithSmallTest", "")
{
  BlobManagerFixture f(true, false);
  f.replaceWithSmallTest();
}

TEST_CASE("BlobManager-inmem/replaceBiggerAndBiggerTest", "")
{
  BlobManagerFixture f(true, false);
  f.replaceBiggerAndBiggerTest();
}

TEST_CASE("BlobManager-inmem/multipleAllocReadFreeTest", "")
{
  BlobManagerFixture f(true, false);
  f.multipleAllocReadFreeTest();
}

TEST_CASE("BlobManager-inmem/hugeBlobTest", "")
{
  BlobManagerFixture f(true, false);
  f.hugeBlobTest();
}

TEST_CASE("BlobManager-inmem/smallBlobTest", "")
{
  BlobManagerFixture f(true, false);
  f.smallBlobTest();
}


TEST_CASE("BlobManager-inmem-64k/structureTest", "")
{
  BlobManagerFixture f(true, false, 0, 1024 * 64);
  f.structureTest();
}

TEST_CASE("BlobManager-inmem-64k/dupeStructureTest", "")
{
  BlobManagerFixture f(true, false, 0, 1024 * 64);
  f.dupeStructureTest();
}

TEST_CASE("BlobManager-inmem-64k/allocReadFreeTest", "")
{
  BlobManagerFixture f(true, false, 0, 1024 * 64);
  f.allocReadFreeTest();
}

TEST_CASE("BlobManager-inmem-64k/replaceTest", "")
{
  BlobManagerFixture f(true, false, 0, 1024 * 64);
  f.replaceTest();
}

TEST_CASE("BlobManager-inmem-64k/replaceWithBigTest", "")
{
  BlobManagerFixture f(true, false, 0, 1024 * 64);
  f.replaceWithBigTest();
}

TEST_CASE("BlobManager-inmem-64k/replaceWithSmallTest", "")
{
  BlobManagerFixture f(true, false, 0, 1024 * 64);
  f.replaceWithSmallTest();
}

TEST_CASE("BlobManager-inmem-64k/replaceBiggerAndBiggerTest", "")
{
  BlobManagerFixture f(true, false, 0, 1024 * 64);
  f.replaceBiggerAndBiggerTest();
}

TEST_CASE("BlobManager-inmem-64k/multipleAllocReadFreeTest", "")
{
  BlobManagerFixture f(true, false, 0, 1024 * 64);
  f.multipleAllocReadFreeTest();
}

TEST_CASE("BlobManager-inmem-64k/hugeBlobTest", "")
{
  BlobManagerFixture f(true, false, 0, 1024 * 64);
  f.hugeBlobTest();
}

TEST_CASE("BlobManager-inmem-64k/smallBlobTest", "")
{
  BlobManagerFixture f(true, false, 0, 1024 * 64);
  f.smallBlobTest();
}

