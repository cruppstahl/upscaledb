/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
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
#include "memtracker.h"
#include "os.hpp"

#include "bfc-testsuite.hpp"

using namespace bfc;

class BlobTest : public fixture
{
public:
    BlobTest(ham_bool_t inmemory=HAM_FALSE, ham_size_t cachesize=0, 
                const char *name=0)
    :   fixture(name ? name : "BlobTest"),
        m_db(0), m_alloc(0), m_inmemory(inmemory), m_cachesize(cachesize)
    {
        if (name)
            return;
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(BlobTest, structureTest);
        BFC_REGISTER_TEST(BlobTest, dupeStructureTest);
        BFC_REGISTER_TEST(BlobTest, allocReadFreeTest);
        BFC_REGISTER_TEST(BlobTest, replaceTest);
        BFC_REGISTER_TEST(BlobTest, replaceWithBigTest);
        BFC_REGISTER_TEST(BlobTest, replaceWithSmallTest);
        /* negative tests are not necessary, because hamsterdb asserts that
         * blob-IDs actually exist */
        BFC_REGISTER_TEST(BlobTest, multipleAllocReadFreeTest);
        BFC_REGISTER_TEST(BlobTest, hugeBlobTest);
        BFC_REGISTER_TEST(BlobTest, smallBlobTest);
    }

protected:
    ham_db_t *m_db;
    memtracker_t *m_alloc;
    ham_bool_t m_inmemory;
    ham_size_t m_cachesize;

public:
    void setup()
    { 
        ham_parameter_t params[2]=
        {
            { HAM_PARAM_CACHESIZE, m_cachesize },
            { 0, 0 }
        };

        os::unlink(BFC_OPATH(".test"));

        BFC_ASSERT((m_alloc=memtracker_new())!=0);
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        BFC_ASSERT_EQUAL(0, 
                ham_create_ex(m_db, BFC_OPATH(".test"), 
                    m_inmemory ? HAM_IN_MEMORY_DB : 0, 0644, &params[0]));
    }
    
    void teardown() 
    { 
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void structureTest(void)
    {
        blob_t b;
        ::memset(&b, 0, sizeof(b));

        blob_set_self(&b, (ham_offset_t)0x12345ull);
        BFC_ASSERT_EQUAL((ham_offset_t)0x12345ull, 
                        blob_get_self(&b));

        blob_set_alloc_size(&b, 0x789ull);
        BFC_ASSERT_EQUAL((ham_u64_t)0x789ull, blob_get_alloc_size(&b));

        blob_set_size(&b, 0x123ull);
        BFC_ASSERT_EQUAL((ham_u64_t)0x123ull, blob_get_size(&b));

        blob_set_flags(&b, 0x13);
        BFC_ASSERT_EQUAL((ham_u32_t)0x13, blob_get_flags(&b));
    }

    void dupeStructureTest(void)
    {
        dupe_table_t t;
        ::memset(&t, 0, sizeof(t));

        dupe_table_set_count(&t, 0x789ull);
        BFC_ASSERT_EQUAL((ham_u32_t)0x789ull, dupe_table_get_count(&t));

        dupe_table_set_capacity(&t, 0x123ull);
        BFC_ASSERT_EQUAL((ham_u32_t)0x123ull, dupe_table_get_capacity(&t));

        dupe_entry_t *e=dupe_table_get_entry(&t, 0);
        dupe_entry_set_flags(e, 0x13);
        BFC_ASSERT_EQUAL((ham_u8_t)0x13, dupe_entry_get_flags(e));

        dupe_entry_set_rid(e, (ham_offset_t)0x12345ull);
        BFC_ASSERT_EQUAL((ham_offset_t)0x12345ull, 
                        dupe_entry_get_rid(e));
    }

    void allocReadFreeTest(void)
    {
        ham_u8_t buffer[64];
        ham_offset_t blobid;
        ham_record_t record;
        ::memset(&record, 0, sizeof(record));
        ::memset(&buffer, 0x12, sizeof(buffer));

        BFC_ASSERT_EQUAL(0, blob_allocate(m_db, buffer, 
                                (ham_size_t)sizeof(buffer), 0, &blobid));
        BFC_ASSERT(blobid!=0);

        BFC_ASSERT_EQUAL(0, blob_read(m_db, blobid, 
                                &record, 0));
        BFC_ASSERT_EQUAL(record.size, (ham_size_t)sizeof(buffer));
        BFC_ASSERT(0==::memcmp(buffer, record.data, record.size));

        BFC_ASSERT_EQUAL(0, blob_free(m_db, blobid, 0));
    }

    void replaceTest(void)
    {
        ham_u8_t buffer[64], buffer2[64];
        ham_offset_t blobid, blobid2;
        ham_record_t record;
        ::memset(&record,  0, sizeof(record));
        ::memset(&buffer,  0x12, sizeof(buffer));
        ::memset(&buffer2, 0x15, sizeof(buffer2));

        BFC_ASSERT_EQUAL(0, blob_allocate(m_db, buffer, 
                                (ham_size_t)sizeof(buffer), 0, &blobid));
        BFC_ASSERT(blobid!=0);

        BFC_ASSERT_EQUAL(0, blob_read(m_db, blobid, 
                                &record, 0));
        BFC_ASSERT_EQUAL(record.size, (ham_size_t)sizeof(buffer));
        BFC_ASSERT(0==::memcmp(buffer, record.data, record.size));

        BFC_ASSERT_EQUAL(0, blob_overwrite(m_db, blobid, buffer2, 
                    sizeof(buffer2), 0, &blobid2));
        BFC_ASSERT(blobid2!=0);

        BFC_ASSERT_EQUAL(0, blob_read(m_db, blobid2, 
                                &record, 0));
        BFC_ASSERT_EQUAL(record.size, (ham_size_t)sizeof(buffer2));
        BFC_ASSERT(0==::memcmp(buffer2, record.data, record.size));

        BFC_ASSERT_EQUAL(0, blob_free(m_db, blobid2, 0));
    }

    void replaceWithBigTest(void)
    {
        ham_u8_t buffer[64], buffer2[128];
        ham_offset_t blobid, blobid2;
        ham_record_t record;
        ::memset(&record,  0, sizeof(record));
        ::memset(&buffer,  0x12, sizeof(buffer));
        ::memset(&buffer2, 0x15, sizeof(buffer2));

        BFC_ASSERT_EQUAL(0, blob_allocate(m_db, buffer, 
                                (ham_size_t)sizeof(buffer), 0, &blobid));
        BFC_ASSERT(blobid!=0);

        BFC_ASSERT_EQUAL(0, blob_read(m_db, blobid, 
                                &record, 0));
        BFC_ASSERT_EQUAL(record.size, (ham_size_t)sizeof(buffer));
        BFC_ASSERT(0==::memcmp(buffer, record.data, record.size));

        BFC_ASSERT_EQUAL(0, blob_overwrite(m_db, blobid, buffer2, 
                    sizeof(buffer2), 0, &blobid2));
        BFC_ASSERT(blobid2!=0);

        BFC_ASSERT_EQUAL(0, blob_read(m_db, blobid2, 
                                &record, 0));
        BFC_ASSERT_EQUAL(record.size, (ham_size_t)sizeof(buffer2));
        BFC_ASSERT(0==::memcmp(buffer2, record.data, record.size));

        BFC_ASSERT_EQUAL(0, blob_free(m_db, blobid2, 0));
    }

    void replaceWithSmallTest(void)
    {
        ham_u8_t buffer[128], buffer2[64];
        ham_offset_t blobid, blobid2;
        ham_record_t record;
        ::memset(&record,  0, sizeof(record));
        ::memset(&buffer,  0x12, sizeof(buffer));
        ::memset(&buffer2, 0x15, sizeof(buffer2));

        BFC_ASSERT_EQUAL(0, blob_allocate(m_db, buffer, 
                                (ham_size_t)sizeof(buffer), 0, &blobid));
        BFC_ASSERT(blobid!=0);

        BFC_ASSERT_EQUAL(0, blob_read(m_db, blobid, 
                                &record, 0));
        BFC_ASSERT_EQUAL(record.size, (ham_size_t)sizeof(buffer));
        BFC_ASSERT(0==::memcmp(buffer, record.data, record.size));

        BFC_ASSERT_EQUAL(0, blob_overwrite(m_db, blobid, buffer2, 
                    sizeof(buffer2), 0, &blobid2));
        BFC_ASSERT(blobid2!=0);

        BFC_ASSERT_EQUAL(0, blob_read(m_db, blobid2, 
                                &record, 0));
        BFC_ASSERT_EQUAL(record.size, (ham_size_t)sizeof(buffer2));
        BFC_ASSERT(0==::memcmp(buffer2, record.data, record.size));

        /* make sure that at least 64bit are in the freelist */
        if (!m_inmemory)
            BFC_ASSERT(freel_alloc_area(m_db, 64)!=0);

        BFC_ASSERT_EQUAL(0, blob_free(m_db, blobid2, 0));

        /* and now another 64bit should be in the freelist */
        if (!m_inmemory)
            BFC_ASSERT(freel_alloc_area(m_db, 64)!=0);
    }

    void loopInsert(int loops, int factor)
    {
        ham_u8_t *buffer;
        ham_offset_t *blobid;
        ham_record_t record;
        ham_txn_t txn; /* need a txn object for the blob routines */
        ::memset(&record, 0, sizeof(record));
        ::memset(&buffer, 0x12, sizeof(buffer));

		blobid=(ham_offset_t *)::malloc(sizeof(ham_offset_t)*loops);
		BFC_ASSERT(blobid!=0);
        BFC_ASSERT(txn_begin(&txn, m_db, 0)==HAM_SUCCESS);

        for (int i=0; i<loops; i++) {
            buffer=(ham_u8_t *)::malloc((i+1)*factor);
            BFC_ASSERT(buffer!=0);
            ::memset(buffer, (char)i, (i+1)*factor);

            BFC_ASSERT_EQUAL(0, blob_allocate(m_db, buffer, 
                                (ham_size_t)((i+1)*factor), 0, &blobid[i]));
            BFC_ASSERT(blobid[i]!=0);

            ::free(buffer);
        }

        for (int i=0; i<loops; i++) {
            buffer=(ham_u8_t *)::malloc((i+1)*factor);
            BFC_ASSERT(buffer!=0);
            ::memset(buffer, (char)i, (i+1)*factor);

            BFC_ASSERT_EQUAL(0, blob_read(m_db, blobid[i], 
                                &record, 0));
            BFC_ASSERT_EQUAL(record.size, (ham_size_t)(i+1)*factor);
            BFC_ASSERT(0==::memcmp(buffer, record.data, record.size));

            ::free(buffer);
        }

        for (int i=0; i<loops; i++) {
            BFC_ASSERT_EQUAL(0, blob_free(m_db, blobid[i], 0));
        }

		::free(blobid);
        BFC_ASSERT(txn_commit(&txn, 0)==HAM_SUCCESS);
    }

    void multipleAllocReadFreeTest(void)
    {
        loopInsert(20, 2048);
    }

    void hugeBlobTest(void)
    {
        loopInsert(10, 1024*1024*4);
    }

    void smallBlobTest(void)
    {
        loopInsert(20, 64);
    }

};

class FileBlobTest : public BlobTest
{
public:
    FileBlobTest()
    : BlobTest(HAM_FALSE, 1024, "FileBlobTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(FileBlobTest, structureTest);
        BFC_REGISTER_TEST(FileBlobTest, allocReadFreeTest);
        BFC_REGISTER_TEST(FileBlobTest, replaceTest);
        BFC_REGISTER_TEST(FileBlobTest, replaceWithBigTest);
        BFC_REGISTER_TEST(FileBlobTest, replaceWithSmallTest);
        /* negative tests are not necessary, because hamsterdb asserts that
         * blob-IDs actually exist */
        BFC_REGISTER_TEST(FileBlobTest, multipleAllocReadFreeTest);
        BFC_REGISTER_TEST(FileBlobTest, hugeBlobTest);
        BFC_REGISTER_TEST(FileBlobTest, smallBlobTest);
    }

};

class NoCacheBlobTest : public BlobTest
{
public:
    NoCacheBlobTest()
    : BlobTest(HAM_FALSE, 0, "NoCacheBlobTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(NoCacheBlobTest, structureTest);
        BFC_REGISTER_TEST(NoCacheBlobTest, allocReadFreeTest);
        BFC_REGISTER_TEST(NoCacheBlobTest, replaceTest);
        BFC_REGISTER_TEST(NoCacheBlobTest, replaceWithBigTest);
        BFC_REGISTER_TEST(NoCacheBlobTest, replaceWithSmallTest);
        /* negative tests are not necessary, because hamsterdb asserts that
         * blob-IDs actually exist */
        BFC_REGISTER_TEST(NoCacheBlobTest, multipleAllocReadFreeTest);
        BFC_REGISTER_TEST(NoCacheBlobTest, hugeBlobTest);
        BFC_REGISTER_TEST(NoCacheBlobTest, smallBlobTest);
    }
};

class InMemoryBlobTest : public BlobTest
{
public:
    InMemoryBlobTest()
    : BlobTest(HAM_TRUE, 0, "InMemoryBlobTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(InMemoryBlobTest, structureTest);
        BFC_REGISTER_TEST(InMemoryBlobTest, allocReadFreeTest);
        BFC_REGISTER_TEST(InMemoryBlobTest, replaceTest);
        BFC_REGISTER_TEST(InMemoryBlobTest, replaceWithBigTest);
        BFC_REGISTER_TEST(InMemoryBlobTest, replaceWithSmallTest);
        /* negative tests are not necessary, because hamsterdb asserts that
         * blob-IDs actually exist */
        BFC_REGISTER_TEST(InMemoryBlobTest, multipleAllocReadFreeTest);
        BFC_REGISTER_TEST(InMemoryBlobTest, hugeBlobTest);
        BFC_REGISTER_TEST(InMemoryBlobTest, smallBlobTest);
    }
};

BFC_REGISTER_FIXTURE(FileBlobTest);
BFC_REGISTER_FIXTURE(NoCacheBlobTest);
BFC_REGISTER_FIXTURE(InMemoryBlobTest);

