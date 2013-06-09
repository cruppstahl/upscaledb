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

#include <stdexcept>
#include <cstring>

#include <ham/hamsterdb.h>

#include "../src/db.h"
#include "../src/blob.h"
#include "../src/env.h"
#include "../src/page.h"
#include "../src/btree_key.h"
#include "../src/page_manager.h"
#include "os.hpp"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;
using namespace hamsterdb;

class BlobTest : public hamsterDB_fixture
{
  define_super(hamsterDB_fixture);

public:
  BlobTest(bool inmemory = false, bool use_txn = false,
        ham_size_t cachesize = 0, ham_size_t pagesize = 0,
        const char *name = "BlobTest")
    : hamsterDB_fixture(name), m_db(0), m_inmemory(inmemory),
    m_use_txn(use_txn), m_cachesize(cachesize), m_pagesize(pagesize) {
    testrunner::get_instance()->register_fixture(this);
    BFC_REGISTER_TEST(BlobTest, structureTest);
    BFC_REGISTER_TEST(BlobTest, dupeStructureTest);
    BFC_REGISTER_TEST(BlobTest, allocReadFreeTest);
    BFC_REGISTER_TEST(BlobTest, replaceTest);
    BFC_REGISTER_TEST(BlobTest, replaceWithBigTest);
    BFC_REGISTER_TEST(BlobTest, replaceWithSmallTest);
    BFC_REGISTER_TEST(BlobTest, replaceBiggerAndBiggerTest);
    /* negative tests are not necessary, because hamsterdb asserts that
     * blob-IDs actually exist */
    BFC_REGISTER_TEST(BlobTest, multipleAllocReadFreeTest);
    BFC_REGISTER_TEST(BlobTest, hugeBlobTest);
    BFC_REGISTER_TEST(BlobTest, smallBlobTest);
  }

protected:
  ham_db_t *m_db;
  ham_env_t *m_env;
  bool m_inmemory;
  bool m_use_txn;
  ham_size_t m_cachesize;
  ham_size_t m_pagesize;
  BlobManager *m_blob_manager;

public:
  virtual void setup() {
    __super::setup();

    ham_parameter_t params[3] = {
      { HAM_PARAM_CACHESIZE, m_cachesize },
      // set pagesize, otherwise 16-bit limit bugs in freelist
      // will fire on Win32
      { HAM_PARAM_PAGESIZE, (m_pagesize ? m_pagesize : 4096) },
      { 0, 0 }
    };

    os::unlink(BFC_OPATH(".test"));

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),
          (m_inmemory
            ? HAM_IN_MEMORY
            : (m_use_txn
              ? HAM_ENABLE_TRANSACTIONS
              : 0)),
            0644, &params[0]));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 1, 0, 0));
    m_blob_manager = ((Environment *)m_env)->get_blob_manager();
  }

  virtual void teardown() {
    __super::teardown();

    /* clear the changeset, otherwise ham_db_close will complain */
    if (!m_inmemory && m_env)
      ((Environment *)m_env)->get_changeset().clear();
    if (m_env)
        BFC_ASSERT_EQUAL(0, ham_env_close(m_env, HAM_AUTO_CLEANUP));
  }

  void structureTest() {
    PBlobHeader b;

    b.set_self((ham_u64_t)0x12345ull);
    BFC_ASSERT_EQUAL((ham_u64_t)0x12345ull, b.get_self());

    b.set_alloc_size(0x789ull);
    BFC_ASSERT_EQUAL((ham_u64_t)0x789ull, b.get_alloc_size());

    b.set_size(0x123ull);
    BFC_ASSERT_EQUAL((ham_u64_t)0x123ull, b.get_size());
  }

  void dupeStructureTest() {
    PDupeTable t;
    ::memset(&t, 0, sizeof(t));

    dupe_table_set_count(&t, 0x789ull);
    BFC_ASSERT_EQUAL((ham_u32_t)0x789ull, dupe_table_get_count(&t));

    dupe_table_set_capacity(&t, 0x123ull);
    BFC_ASSERT_EQUAL((ham_u32_t)0x123ull, dupe_table_get_capacity(&t));

    PDupeEntry *e = dupe_table_get_entry(&t, 0);
    dupe_entry_set_flags(e, 0x13);
    BFC_ASSERT_EQUAL((ham_u8_t)0x13, dupe_entry_get_flags(e));

    dupe_entry_set_rid(e, (ham_u64_t)0x12345ull);
    BFC_ASSERT_EQUAL((ham_u64_t)0x12345ull,
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
    BFC_ASSERT_EQUAL(0,
        m_blob_manager->allocate((Database *)m_db, &record, 0, &blobid));
    BFC_ASSERT(blobid != 0);

    ByteArray *arena = &((Database *)m_db)->get_record_arena();

    BFC_ASSERT_EQUAL(0,
        m_blob_manager->read((Database *)m_db,
                blobid, &record, 0, arena));
    BFC_ASSERT_EQUAL(record.size, (ham_size_t)sizeof(buffer));
    BFC_ASSERT_EQUAL(0, ::memcmp(buffer, record.data, record.size));

    BFC_ASSERT_EQUAL(0, m_blob_manager->free((Database *)m_db, blobid, 0));
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
    BFC_ASSERT_EQUAL(0,
        m_blob_manager->allocate((Database *)m_db, &record, 0, &blobid));
    BFC_ASSERT(blobid != 0);

    ByteArray *arena = &((Database *)m_db)->get_record_arena();
    BFC_ASSERT_EQUAL(0,
        m_blob_manager->read((Database *)m_db, blobid, &record, 0, arena));
    BFC_ASSERT_EQUAL(record.size, (ham_size_t)sizeof(buffer));
    BFC_ASSERT_EQUAL(0, ::memcmp(buffer, record.data, record.size));

    record.size = sizeof(buffer2);
    record.data = buffer2;
    BFC_ASSERT_EQUAL(0,
        m_blob_manager->overwrite((Database *)m_db,
                blobid, &record, 0, &blobid2));
    BFC_ASSERT(blobid2 != 0);

    BFC_ASSERT_EQUAL(0,
        m_blob_manager->read((Database *)m_db, blobid2, &record, 0, arena));
    BFC_ASSERT_EQUAL(record.size, (ham_size_t)sizeof(buffer2));
    BFC_ASSERT_EQUAL(0, ::memcmp(buffer2, record.data, record.size));

    BFC_ASSERT_EQUAL(0, m_blob_manager->free((Database *)m_db, blobid2, 0));
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
    BFC_ASSERT_EQUAL(0,
        m_blob_manager->allocate((Database *)m_db, &record, 0, &blobid));
    BFC_ASSERT(blobid != 0);

    ByteArray *arena = &((Database *)m_db)->get_record_arena();
    BFC_ASSERT_EQUAL(0,
        m_blob_manager->read((Database *)m_db, blobid, &record, 0, arena));
    BFC_ASSERT_EQUAL(record.size, (ham_size_t)sizeof(buffer));
    BFC_ASSERT_EQUAL(0, ::memcmp(buffer, record.data, record.size));

    record.size = sizeof(buffer2);
    record.data = buffer2;
    BFC_ASSERT_EQUAL(0,
        m_blob_manager->overwrite((Database *)m_db, blobid,
            &record, 0, &blobid2));
    BFC_ASSERT(blobid2 != 0);

    BFC_ASSERT_EQUAL(0,
        m_blob_manager->read((Database *)m_db, blobid2, &record, 0, arena));
    BFC_ASSERT_EQUAL(record.size, (ham_size_t)sizeof(buffer2));
    BFC_ASSERT_EQUAL(0, ::memcmp(buffer2, record.data, record.size));

    BFC_ASSERT_EQUAL(0, m_blob_manager->free((Database *)m_db, blobid2, 0));
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
    BFC_ASSERT_EQUAL(0,
        m_blob_manager->allocate((Database *)m_db, &record, 0, &blobid));
    BFC_ASSERT(blobid != 0);

    ByteArray *arena = &((Database *)m_db)->get_record_arena();
    BFC_ASSERT_EQUAL(0,
        m_blob_manager->read((Database *)m_db, blobid, &record, 0, arena));
    BFC_ASSERT_EQUAL(record.size, (ham_size_t)sizeof(buffer));
    BFC_ASSERT_EQUAL(0, ::memcmp(buffer, record.data, record.size));

    record.size = sizeof(buffer2);
    record.data = buffer2;
    BFC_ASSERT_EQUAL(0,
        m_blob_manager->overwrite((Database *)m_db,
                blobid, &record, 0, &blobid2));
    BFC_ASSERT(blobid2 != 0);

    BFC_ASSERT_EQUAL(0,
        m_blob_manager->read((Database *)m_db, blobid2, &record, 0, arena));
    BFC_ASSERT_EQUAL(record.size, (ham_size_t)sizeof(buffer2));
    BFC_ASSERT_EQUAL(0, ::memcmp(buffer2, record.data, record.size));

    /* make sure that at least 64bit are in the freelist */
    if (!m_inmemory) {
      ham_u64_t addr;
      Freelist *f = ((Environment *)m_env)->get_page_manager()->get_freelist(0);
      BFC_ASSERT_EQUAL(0, f->alloc_area(64, &addr));
      BFC_ASSERT(addr != 0);
    }

    BFC_ASSERT_EQUAL(0, m_blob_manager->free((Database *)m_db, blobid2, 0));

    /* and now another 64bit should be in the freelist */
    if (!m_inmemory) {
      ham_u64_t addr;
      Freelist *f = ((Environment *)m_env)->get_page_manager()->get_freelist(0);
      BFC_ASSERT_EQUAL(0, f->alloc_area(64, &addr));
      BFC_ASSERT(addr != 0);
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
    BFC_ASSERT_EQUAL(0,
        m_blob_manager->allocate((Database *)m_db, &record, 0, &blobid));
    BFC_ASSERT(blobid != 0);

    /* verify it */
    ByteArray *arena = &((Database *)m_db)->get_record_arena();
    BFC_ASSERT_EQUAL(0,
        m_blob_manager->read((Database *)m_db, blobid, &record, 0, arena));
    BFC_ASSERT_EQUAL(record.size, (ham_size_t)ps * BLOCKS * 2);

    /* and erase it */
    BFC_ASSERT_EQUAL(0, m_blob_manager->free((Database *)m_db, blobid, 0));

    /* now use a loop to allocate the buffer, and make it bigger and
     * bigger */
    for (int i = 1; i < 32; i++) {
      record.size = i * ps;
      record.data = (void *)buffer;
      ::memset(buffer, i, record.size);
      if (i == 1) {
        BFC_ASSERT_EQUAL(0,
            m_blob_manager->allocate((Database *)m_db, &record,
                0, &blobid2));
      }
      else {
        BFC_ASSERT_EQUAL(0,
        m_blob_manager->overwrite((Database *)m_db, blobid,
                  &record, 0, &blobid2));
      }
      blobid = blobid2;
      BFC_ASSERT(blobid != 0);
    }
    BFC_ASSERT_EQUAL(0, m_blob_manager->free((Database *)m_db, blobid, 0));
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
    BFC_ASSERT(blobid != 0);
    if (!m_inmemory && m_use_txn)
      BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

    for (int i = 0; i < loops; i++) {
      buffer = (ham_u8_t *)::malloc((i + 1) * factor);
      BFC_ASSERT_I(buffer != 0, i);
      ::memset(buffer, (char)i, (i + 1) * factor);

      ham_record_t rec = {0};
      rec.data = buffer;
      rec.size = (ham_size_t)((i + 1) * factor);
      BFC_ASSERT_EQUAL(0,
            m_blob_manager->allocate((Database *)m_db, &rec,
                  0, &blobid[i]));
      BFC_ASSERT_I(blobid[i] != 0, i);

      ::free(buffer);
    }

    ByteArray *arena = &((Database *)m_db)->get_record_arena();

    for (int i = 0; i < loops; i++) {
      buffer = (ham_u8_t *)::malloc((i + 1) * factor);
      BFC_ASSERT_I(buffer != 0, i);
      ::memset(buffer, (char)i, (i + 1) * factor);

      BFC_ASSERT_EQUAL_I(0,
          m_blob_manager->read((Database *)m_db, blobid[i], &record,
                    0, arena), i);
      BFC_ASSERT_EQUAL_I(record.size, (ham_size_t)(i+1)*factor, i);
      BFC_ASSERT_EQUAL_I(0, ::memcmp(buffer, record.data, record.size), i);

      ::free(buffer);
    }

    for (int i = 0; i < loops; i++) {
      BFC_ASSERT_EQUAL_I(0,
        m_blob_manager->free((Database *)m_db, blobid[i], 0), i);
    }

    ::free(blobid);
    if (!m_inmemory && m_use_txn)
      BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
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

class FileBlobTest : public BlobTest {
public:
  FileBlobTest(ham_size_t cachesize = 1024, ham_size_t pagesize = 0,
        const char *name = "FileBlobTest")
    : BlobTest(false, true, cachesize, pagesize, name) {
  }
};

class FileBlobNoTxnTest : public BlobTest {
public:
  FileBlobNoTxnTest(ham_size_t cachesize = 1024, ham_size_t pagesize = 0,
        const char *name = "FileBlobNoTxnTest")
    : BlobTest(false, false, cachesize, pagesize, name) {
  }
};

class FileBlobTest64Kpage : public FileBlobTest {
public:
  FileBlobTest64Kpage(ham_size_t cachesize = 64 * 1024,
        ham_size_t pagesize = 64 * 1024,
        const char *name = "FileBlobTest64Kpage")
    : FileBlobTest(cachesize, pagesize, name) {
  }
};

class NoCacheBlobTest : public BlobTest {
public:
  NoCacheBlobTest(ham_size_t cachesize = 0, ham_size_t pagesize = 0,
        const char *name = "NoCacheBlobTest")
    : BlobTest(false, true, cachesize, pagesize, name) {
  }
};

class NoCacheBlobNoTxnTest : public BlobTest {
public:
  NoCacheBlobNoTxnTest(ham_size_t cachesize = 0, ham_size_t pagesize = 0,
        const char *name = "NoCacheBlobNoTxnTest")
    : BlobTest(false, false, cachesize, pagesize, name) {
  }
};

class NoCacheBlobTest64Kpage : public NoCacheBlobTest {
public:
  NoCacheBlobTest64Kpage(ham_size_t cachesize = 0,
        ham_size_t pagesize = 64 * 1024,
        const char *name = "NoCacheBlobTest64Kpage")
    : NoCacheBlobTest(cachesize, pagesize, name) {
  }
};

class InMemoryBlobTest : public BlobTest {
public:
  InMemoryBlobTest(ham_size_t cachesize = 0, ham_size_t pagesize = 0,
        const char *name = "InMemoryBlobTest")
    : BlobTest(true, false, cachesize, pagesize, name) {
  }
};

class InMemoryBlobTest64Kpage : public InMemoryBlobTest {
public:
  InMemoryBlobTest64Kpage(ham_size_t cachesize = 0,
        ham_size_t pagesize = 64 * 1024,
        const char *name = "InMemoryBlobTest64Kpage")
    : InMemoryBlobTest(cachesize, pagesize, name) {
  }
};


BFC_REGISTER_FIXTURE(FileBlobTest);
BFC_REGISTER_FIXTURE(FileBlobNoTxnTest);
BFC_REGISTER_FIXTURE(NoCacheBlobTest);
BFC_REGISTER_FIXTURE(NoCacheBlobNoTxnTest);
BFC_REGISTER_FIXTURE(InMemoryBlobTest);
/* re-run these tests with the Win32/Win64 pagesize setting as well! */
BFC_REGISTER_FIXTURE(FileBlobTest64Kpage);
BFC_REGISTER_FIXTURE(NoCacheBlobTest64Kpage);
BFC_REGISTER_FIXTURE(InMemoryBlobTest64Kpage);
