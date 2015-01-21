/*
 * Copyright (C) 2005-2014 Christoph Rupp (chris@crupp.de).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "3rdparty/catch/catch.hpp"

#include "utils.h"
#include "os.hpp"

#include "2page/page.h"
#include "3page_manager/page_manager.h"
#include "3page_manager/page_manager_test.h"
#include "3btree/btree_flags.h"
#include "3blob_manager/blob_manager_disk.h"
#include "4env/env.h"
#include "4db/db_local.h"

namespace hamsterdb {

struct BlobManagerFixture {
  ham_db_t *m_db;
  ham_env_t *m_env;
  bool m_inmemory;
  bool m_use_txn;
  uint32_t m_cache_size;
  uint32_t m_page_size;
  BlobManager *m_blob_manager;

  BlobManagerFixture(bool inmemory = false, bool use_txn = false,
        uint32_t cache_size = 0, uint32_t page_size = 0)
    : m_db(0), m_inmemory(inmemory), m_use_txn(use_txn),
      m_cache_size(cache_size), m_page_size(page_size) {
    ham_parameter_t params[3] = {
      { HAM_PARAM_CACHESIZE, m_cache_size },
      // set page_size, otherwise 16-bit limit bugs in freelist
      // will fire on Win32
      { HAM_PARAM_PAGESIZE, (m_page_size ? m_page_size : 4096) },
      { 0, 0 }
    };

    os::unlink(Utils::opath(".test"));

    REQUIRE(0 ==
        ham_env_create(&m_env, Utils::opath(".test"),
          (m_inmemory
            ? HAM_IN_MEMORY
            : (m_use_txn
              ? HAM_ENABLE_TRANSACTIONS
              : 0)),
            0644, &params[0]));
    REQUIRE(0 ==
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
    m_blob_manager = ((LocalEnvironment *)m_env)->get_blob_manager();
  }

  ~BlobManagerFixture() {
    /* clear the changeset, otherwise ham_db_close will complain */
    if (!m_inmemory && m_env)
      ((LocalEnvironment *)m_env)->get_changeset()->clear();
    if (m_env)
        REQUIRE(0 == ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void structureTest() {
    PBlobHeader b;

    b.set_self((uint64_t)0x12345ull);
    REQUIRE((uint64_t)0x12345ull == b.get_self());

    b.set_alloc_size(0x789ull);
    REQUIRE((uint64_t)0x789ull == b.get_alloc_size());

    b.set_size(0x123ull);
    REQUIRE((uint64_t)0x123ull == b.get_size());
  }

  void allocReadFreeTest() {
    uint8_t buffer[64];
    uint64_t blobid;
    ham_record_t record;
    ::memset(&record, 0, sizeof(record));
    ::memset(&buffer, 0x12, sizeof(buffer));

    record.size = sizeof(buffer);
    record.data = buffer;
    blobid = m_blob_manager->allocate((LocalDatabase *)m_db, &record, 0);
    REQUIRE(blobid != 0ull);

    ByteArray *arena = &((LocalDatabase *)m_db)->get_record_arena();

    m_blob_manager->read((LocalDatabase *)m_db, blobid, &record, 0, arena);
    REQUIRE(record.size == (uint32_t)sizeof(buffer));
    REQUIRE(0 == ::memcmp(buffer, record.data, record.size));

    m_blob_manager->erase((LocalDatabase *)m_db, blobid, 0);
  }

  void freeBlobTest() {
    uint8_t buffer[64];
    uint64_t blobid;
    ham_record_t record;
    ::memset(&record, 0, sizeof(record));
    ::memset(&buffer, 0x12, sizeof(buffer));

    LocalEnvironment *lenv = (LocalEnvironment *)m_env;

    record.size = sizeof(buffer);
    record.data = buffer;
    blobid = m_blob_manager->allocate((LocalDatabase *)m_db, &record, 0);
    REQUIRE(blobid != 0ull);

    uint64_t page_id = (blobid / lenv->get_page_size())
                            * lenv->get_page_size();

    PageManagerTestGateway test(lenv->get_page_manager());
    REQUIRE(test.is_page_free(page_id) == false);

    m_blob_manager->erase((LocalDatabase *)m_db, blobid, 0);

    REQUIRE(test.is_page_free(page_id) == true);
  }

  void replaceTest() {
    uint8_t buffer[64], buffer2[64];
    uint64_t blobid, blobid2;
    ham_record_t record;
    ::memset(&record,  0, sizeof(record));
    ::memset(&buffer,  0x12, sizeof(buffer));
    ::memset(&buffer2, 0x15, sizeof(buffer2));

    record.size = sizeof(buffer);
    record.data = buffer;
    blobid = m_blob_manager->allocate((LocalDatabase *)m_db, &record, 0);
    REQUIRE(blobid != 0ull);

    ByteArray *arena = &((LocalDatabase *)m_db)->get_record_arena();
    m_blob_manager->read((LocalDatabase *)m_db, blobid, &record, 0, arena);
    REQUIRE(record.size == (uint32_t)sizeof(buffer));
    REQUIRE(0 == ::memcmp(buffer, record.data, record.size));

    record.size = sizeof(buffer2);
    record.data = buffer2;
    blobid2 = m_blob_manager->overwrite((LocalDatabase *)m_db, blobid,
                    &record, 0);
    REQUIRE(blobid2 != 0ull);

    m_blob_manager->read((LocalDatabase *)m_db, blobid2, &record, 0, arena);
    REQUIRE(record.size == (uint32_t)sizeof(buffer2));
    REQUIRE(0 == ::memcmp(buffer2, record.data, record.size));

    m_blob_manager->erase((LocalDatabase *)m_db, blobid2, 0);
  }

  void replaceWithBigTest() {
    uint8_t buffer[64], buffer2[128];
    uint64_t blobid, blobid2;
    ham_record_t record;
    ::memset(&record,  0, sizeof(record));
    ::memset(&buffer,  0x12, sizeof(buffer));
    ::memset(&buffer2, 0x15, sizeof(buffer2));

    record.data = buffer;
    record.size = sizeof(buffer);
    blobid = m_blob_manager->allocate((LocalDatabase *)m_db, &record, 0);
    REQUIRE(blobid != 0ull);

    ByteArray *arena = &((LocalDatabase *)m_db)->get_record_arena();
    m_blob_manager->read((LocalDatabase *)m_db, blobid, &record, 0, arena);
    REQUIRE(record.size == (uint32_t)sizeof(buffer));
    REQUIRE(0 == ::memcmp(buffer, record.data, record.size));

    record.size = sizeof(buffer2);
    record.data = buffer2;
    blobid2 = m_blob_manager->overwrite((LocalDatabase *)m_db, blobid,
                    &record, 0);
    REQUIRE(blobid2 != 0ull);

    m_blob_manager->read((LocalDatabase *)m_db, blobid2, &record, 0, arena);
    REQUIRE(record.size == (uint32_t)sizeof(buffer2));
    REQUIRE(0 == ::memcmp(buffer2, record.data, record.size));

    m_blob_manager->erase((LocalDatabase *)m_db, blobid2, 0);
  }

  void replaceWithSmallTest() {
    uint8_t buffer[128], buffer2[64];
    uint64_t blobid, blobid2;
    ham_record_t record;
    ::memset(&record,  0, sizeof(record));
    ::memset(&buffer,  0x12, sizeof(buffer));
    ::memset(&buffer2, 0x15, sizeof(buffer2));

    LocalEnvironment *lenv = (LocalEnvironment *)m_env;
    LocalDatabase *ldb = (LocalDatabase *)m_db;

    record.data = buffer;
    record.size = sizeof(buffer);
    blobid = m_blob_manager->allocate(ldb, &record, 0);
    REQUIRE(blobid != 0ull);

    /* verify the freelist information */
    if (!m_inmemory) {
      Page *page = lenv->get_page_manager()->fetch(ldb,
                      (blobid / lenv->get_page_size()) * lenv->get_page_size());
      PBlobPageHeader *header = PBlobPageHeader::from_page(page);
      if (lenv->get_page_size() == 1024 * 16) {
        REQUIRE(header->get_free_bytes() == 3666);
        REQUIRE(header->get_freelist_size(0) == 3666);
      }
      REQUIRE(header->get_freelist_offset(0) == 436);
    }

    ByteArray *arena = &ldb->get_record_arena();
    m_blob_manager->read(ldb, blobid, &record, 0, arena);
    REQUIRE(record.size == (uint32_t)sizeof(buffer));
    REQUIRE(0 == ::memcmp(buffer, record.data, record.size));

    record.size = sizeof(buffer2);
    record.data = buffer2;
    blobid2 = m_blob_manager->overwrite((LocalDatabase *)m_db, blobid,
                    &record, 0);

    /* verify the freelist information - free area must have increased
     * by 64 bytes (the size difference between both records) */
    if (!m_inmemory) {
      REQUIRE(blobid2 == blobid);

      Page *page = lenv->get_page_manager()->fetch(ldb,
                      (blobid / lenv->get_page_size()) * lenv->get_page_size());
      PBlobPageHeader *header = PBlobPageHeader::from_page(page);
      if (lenv->get_page_size() == 1024 * 16) {
        REQUIRE(header->get_free_bytes() == 3666 - 64);
        REQUIRE(header->get_freelist_size(0) == 3666);
      }
    }

    m_blob_manager->read(ldb, blobid2, &record, 0, arena);
    REQUIRE(record.size == (uint32_t)sizeof(buffer2));
    REQUIRE(0 == ::memcmp(buffer2, record.data, record.size));

    m_blob_manager->erase(ldb, blobid2, 0);

    /* once more check the freelist */
    if (!m_inmemory) {
      Page *page = lenv->get_page_manager()->fetch(ldb,
                      (blobid / lenv->get_page_size()) * lenv->get_page_size());
      PBlobPageHeader *header = PBlobPageHeader::from_page(page);
      if (lenv->get_page_size() == 1024 * 16) {
        REQUIRE(header->get_free_bytes() == 3758);
        REQUIRE(header->get_freelist_size(0) == 3666);
      }
    }
  }

  void replaceBiggerAndBiggerTest() {
    const int BLOCKS = 32;
    unsigned ps = ((LocalEnvironment *)m_env)->get_page_size();
    uint8_t *buffer = (uint8_t *)malloc(ps * BLOCKS * 2);
    uint64_t blobid, blobid2;
    ham_record_t record;
    ::memset(&record, 0, sizeof(record));
    ::memset(buffer,  0, ps * BLOCKS * 2);

    /* first: create a big blob and erase it - we want to use the
     * space from the freelist */
    record.data = buffer;
    record.size = ps * BLOCKS * 2;
    blobid = m_blob_manager->allocate((LocalDatabase *)m_db, &record, 0);
    REQUIRE(blobid != 0ull);

    /* verify it */
    ByteArray *arena = &((LocalDatabase *)m_db)->get_record_arena();
    m_blob_manager->read((LocalDatabase *)m_db, blobid, &record, 0, arena);
    REQUIRE(record.size == (uint32_t)ps * BLOCKS * 2);

    /* and erase it */
    m_blob_manager->erase((LocalDatabase *)m_db, blobid, 0);

    /* now use a loop to allocate the buffer, and make it bigger and
     * bigger */
    for (int i = 1; i < 32; i++) {
      record.size = i * ps;
      record.data = (void *)buffer;
      ::memset(buffer, i, record.size);
      if (i == 1) {
        blobid2 = m_blob_manager->allocate((LocalDatabase *)m_db, &record, 0);
      }
      else {
        blobid2 = m_blob_manager->overwrite((LocalDatabase *)m_db, blobid,
                    &record, 0);
      }
      blobid = blobid2;
      REQUIRE(blobid != 0ull);
    }
    m_blob_manager->erase((LocalDatabase *)m_db, blobid, 0);
    ::free(buffer);
  }

  void loopInsert(int loops, int factor) {
    uint8_t *buffer;
    uint64_t *blobid;
    ham_record_t record;
    ham_txn_t *txn = 0; /* need a txn object for the blob routines */
    ::memset(&record, 0, sizeof(record));
    ::memset(&buffer, 0x12, sizeof(buffer));

    blobid = (uint64_t *)::malloc(sizeof(uint64_t) * loops);
    if (!m_inmemory && m_use_txn)
      REQUIRE(0 == ham_txn_begin(&txn, m_env, 0, 0, 0));

    for (int i = 0; i < loops; i++) {
      buffer = (uint8_t *)::malloc((i + 1) * factor);
      ::memset(buffer, (char)i, (i + 1) * factor);

      ham_record_t rec = {0};
      rec.data = buffer;
      rec.size = (uint32_t)((i + 1) * factor);
      blobid[i] = m_blob_manager->allocate((LocalDatabase *)m_db, &rec, 0);
      REQUIRE(blobid[i] != 0ull);

      ::free(buffer);
    }

    ByteArray *arena = &((LocalDatabase *)m_db)->get_record_arena();

    for (int i = 0; i < loops; i++) {
      buffer = (uint8_t *)::malloc((i + 1) * factor);
      ::memset(buffer, (char)i, (i + 1) * factor);

      m_blob_manager->read((LocalDatabase *)m_db, blobid[i], &record, 0, arena);
      REQUIRE(record.size == (uint32_t)(i + 1) * factor);
      REQUIRE(0 == ::memcmp(buffer, record.data, record.size));

      ::free(buffer);
    }

    for (int i = 0; i < loops; i++) {
      m_blob_manager->erase((LocalDatabase *)m_db, blobid[i], 0);
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

TEST_CASE("BlobManager/allocReadFreeTest", "")
{
  BlobManagerFixture f(false, true, 1024);
  f.allocReadFreeTest();
}

TEST_CASE("BlobManager/freeBlobTest", "")
{
  BlobManagerFixture f(false, true, 1024);
  f.freeBlobTest();
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

} // namespace hamsterdb
