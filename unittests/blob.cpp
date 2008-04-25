/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 */

#include <stdexcept>
#include <cppunit/extensions/HelperMacros.h>
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/blob.h"
#include "memtracker.h"
#include "os.hpp"

class BlobTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(BlobTest);
    CPPUNIT_TEST      (structureTest);
    CPPUNIT_TEST      (dupeStructureTest);
    CPPUNIT_TEST      (allocReadFreeTest);
    CPPUNIT_TEST      (replaceTest);
    CPPUNIT_TEST      (replaceWithBigTest);
    CPPUNIT_TEST      (replaceWithSmallTest);
    /* negative tests are not necessary, because hamsterdb asserts that
     * blob-IDs actually exist */
    CPPUNIT_TEST      (multipleAllocReadFreeTest);
    CPPUNIT_TEST      (hugeBlobTest);
    CPPUNIT_TEST      (smallBlobTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_db_t *m_db;
    memtracker_t *m_alloc;
    ham_bool_t m_inmemory;
    ham_size_t m_cachesize;

public:
    BlobTest(ham_bool_t inmemory=HAM_FALSE, ham_size_t cachesize=0)
    :   m_db(0), m_alloc(0), m_inmemory(inmemory), m_cachesize(cachesize)
    {
    }

    void setUp()
    { 
        ham_parameter_t params[2]=
        {
            { HAM_PARAM_CACHESIZE, m_cachesize },
            { 0, 0 }
        };

        os::unlink(".test");

        CPPUNIT_ASSERT((m_alloc=memtracker_new())!=0);
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create_ex(m_db, ".test", 
                    m_inmemory ? HAM_IN_MEMORY_DB : 0, 0644, &params[0]));
    }
    
    void tearDown() 
    { 
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        CPPUNIT_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void structureTest(void)
    {
        blob_t b;
        ::memset(&b, 0, sizeof(b));

        blob_set_self(&b, (ham_offset_t)0x12345ull);
        CPPUNIT_ASSERT_EQUAL((ham_offset_t)0x12345ull, 
                        blob_get_self(&b));

        blob_set_alloc_size(&b, 0x789ull);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)0x789ull, blob_get_alloc_size(&b));

        blob_set_size(&b, 0x123ull);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)0x123ull, blob_get_size(&b));

        blob_set_flags(&b, 0x13);
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)0x13, blob_get_flags(&b));
    }

    void dupeStructureTest(void)
    {
        dupe_table_t t;
        ::memset(&t, 0, sizeof(t));

        dupe_table_set_count(&t, 0x789ull);
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)0x789ull, dupe_table_get_count(&t));

        dupe_table_set_capacity(&t, 0x123ull);
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)0x123ull, dupe_table_get_capacity(&t));

        dupe_entry_t *e=dupe_table_get_entry(&t, 0);
        dupe_entry_set_flags(e, 0x13);
        CPPUNIT_ASSERT_EQUAL((ham_u8_t)0x13, dupe_entry_get_flags(e));

        dupe_entry_set_rid(e, (ham_offset_t)0x12345ull);
        CPPUNIT_ASSERT_EQUAL((ham_offset_t)0x12345ull, 
                        dupe_entry_get_rid(e));
    }

    void allocReadFreeTest(void)
    {
        ham_u8_t buffer[64];
        ham_offset_t blobid;
        ham_record_t record;
        ::memset(&record, 0, sizeof(record));
        ::memset(&buffer, 0x12, sizeof(buffer));

        CPPUNIT_ASSERT_EQUAL(0, blob_allocate(m_db, buffer, 
                                (ham_size_t)sizeof(buffer), 0, &blobid));
        CPPUNIT_ASSERT(blobid!=0);

        CPPUNIT_ASSERT_EQUAL(0, blob_read(m_db, blobid, 
                                &record, 0));
        CPPUNIT_ASSERT_EQUAL(record.size, (ham_size_t)sizeof(buffer));
        CPPUNIT_ASSERT(0==::memcmp(buffer, record.data, record.size));

        CPPUNIT_ASSERT_EQUAL(0, blob_free(m_db, blobid, 0));
    }

    void replaceTest(void)
    {
        ham_u8_t buffer[64], buffer2[64];
        ham_offset_t blobid, blobid2;
        ham_record_t record;
        ::memset(&record,  0, sizeof(record));
        ::memset(&buffer,  0x12, sizeof(buffer));
        ::memset(&buffer2, 0x15, sizeof(buffer2));

        CPPUNIT_ASSERT_EQUAL(0, blob_allocate(m_db, buffer, 
                                (ham_size_t)sizeof(buffer), 0, &blobid));
        CPPUNIT_ASSERT(blobid!=0);

        CPPUNIT_ASSERT_EQUAL(0, blob_read(m_db, blobid, 
                                &record, 0));
        CPPUNIT_ASSERT_EQUAL(record.size, (ham_size_t)sizeof(buffer));
        CPPUNIT_ASSERT(0==::memcmp(buffer, record.data, record.size));

        CPPUNIT_ASSERT_EQUAL(0, blob_overwrite(m_db, blobid, buffer2, 
                    sizeof(buffer2), 0, &blobid2));
        CPPUNIT_ASSERT(blobid2!=0);

        CPPUNIT_ASSERT_EQUAL(0, blob_read(m_db, blobid2, 
                                &record, 0));
        CPPUNIT_ASSERT_EQUAL(record.size, (ham_size_t)sizeof(buffer2));
        CPPUNIT_ASSERT(0==::memcmp(buffer2, record.data, record.size));

        CPPUNIT_ASSERT_EQUAL(0, blob_free(m_db, blobid2, 0));
    }

    void replaceWithBigTest(void)
    {
        ham_u8_t buffer[64], buffer2[128];
        ham_offset_t blobid, blobid2;
        ham_record_t record;
        ::memset(&record,  0, sizeof(record));
        ::memset(&buffer,  0x12, sizeof(buffer));
        ::memset(&buffer2, 0x15, sizeof(buffer2));

        CPPUNIT_ASSERT_EQUAL(0, blob_allocate(m_db, buffer, 
                                (ham_size_t)sizeof(buffer), 0, &blobid));
        CPPUNIT_ASSERT(blobid!=0);

        CPPUNIT_ASSERT_EQUAL(0, blob_read(m_db, blobid, 
                                &record, 0));
        CPPUNIT_ASSERT_EQUAL(record.size, (ham_size_t)sizeof(buffer));
        CPPUNIT_ASSERT(0==::memcmp(buffer, record.data, record.size));

        CPPUNIT_ASSERT_EQUAL(0, blob_overwrite(m_db, blobid, buffer2, 
                    sizeof(buffer2), 0, &blobid2));
        CPPUNIT_ASSERT(blobid2!=0);

        CPPUNIT_ASSERT_EQUAL(0, blob_read(m_db, blobid2, 
                                &record, 0));
        CPPUNIT_ASSERT_EQUAL(record.size, (ham_size_t)sizeof(buffer2));
        CPPUNIT_ASSERT(0==::memcmp(buffer2, record.data, record.size));

        CPPUNIT_ASSERT_EQUAL(0, blob_free(m_db, blobid2, 0));
    }

    void replaceWithSmallTest(void)
    {
        ham_u8_t buffer[128], buffer2[64];
        ham_offset_t blobid, blobid2;
        ham_record_t record;
        ::memset(&record,  0, sizeof(record));
        ::memset(&buffer,  0x12, sizeof(buffer));
        ::memset(&buffer2, 0x15, sizeof(buffer2));

        CPPUNIT_ASSERT_EQUAL(0, blob_allocate(m_db, buffer, 
                                (ham_size_t)sizeof(buffer), 0, &blobid));
        CPPUNIT_ASSERT(blobid!=0);

        CPPUNIT_ASSERT_EQUAL(0, blob_read(m_db, blobid, 
                                &record, 0));
        CPPUNIT_ASSERT_EQUAL(record.size, (ham_size_t)sizeof(buffer));
        CPPUNIT_ASSERT(0==::memcmp(buffer, record.data, record.size));

        CPPUNIT_ASSERT_EQUAL(0, blob_overwrite(m_db, blobid, buffer2, 
                    sizeof(buffer2), 0, &blobid2));
        CPPUNIT_ASSERT(blobid2!=0);

        CPPUNIT_ASSERT_EQUAL(0, blob_read(m_db, blobid2, 
                                &record, 0));
        CPPUNIT_ASSERT_EQUAL(record.size, (ham_size_t)sizeof(buffer2));
        CPPUNIT_ASSERT(0==::memcmp(buffer2, record.data, record.size));

        /* make sure that at least 64bit are in the freelist */
        if (!m_inmemory)
            CPPUNIT_ASSERT(freel_alloc_area(m_db, 64)!=0);

        CPPUNIT_ASSERT_EQUAL(0, blob_free(m_db, blobid2, 0));

        /* and now another 64bit should be in the freelist */
        if (!m_inmemory)
            CPPUNIT_ASSERT(freel_alloc_area(m_db, 64)!=0);
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
		CPPUNIT_ASSERT(blobid!=0);
        CPPUNIT_ASSERT(ham_txn_begin(&txn, m_db, 0)==HAM_SUCCESS);

        for (int i=0; i<loops; i++) {
            buffer=(ham_u8_t *)::malloc((i+1)*factor);
            CPPUNIT_ASSERT(buffer!=0);
            ::memset(buffer, (char)i, (i+1)*factor);

            CPPUNIT_ASSERT_EQUAL(0, blob_allocate(m_db, buffer, 
                                (ham_size_t)((i+1)*factor), 0, &blobid[i]));
            CPPUNIT_ASSERT(blobid[i]!=0);

            ::free(buffer);
        }

        for (int i=0; i<loops; i++) {
            buffer=(ham_u8_t *)::malloc((i+1)*factor);
            CPPUNIT_ASSERT(buffer!=0);
            ::memset(buffer, (char)i, (i+1)*factor);

            CPPUNIT_ASSERT_EQUAL(0, blob_read(m_db, blobid[i], 
                                &record, 0));
            CPPUNIT_ASSERT_EQUAL(record.size, (ham_size_t)(i+1)*factor);
            CPPUNIT_ASSERT(0==::memcmp(buffer, record.data, record.size));

            ::free(buffer);
        }

        for (int i=0; i<loops; i++) {
            CPPUNIT_ASSERT_EQUAL(0, blob_free(m_db, blobid[i], 0));
        }

		::free(blobid);
        CPPUNIT_ASSERT(ham_txn_commit(&txn, 0)==HAM_SUCCESS);
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
    CPPUNIT_TEST_SUITE(FileBlobTest);
    CPPUNIT_TEST      (structureTest);
    CPPUNIT_TEST      (allocReadFreeTest);
    CPPUNIT_TEST      (replaceTest);
    CPPUNIT_TEST      (replaceWithBigTest);
    CPPUNIT_TEST      (replaceWithSmallTest);
    /* negative tests are not necessary, because hamsterdb asserts that
     * blob-IDs actually exist */
    CPPUNIT_TEST      (multipleAllocReadFreeTest);
    CPPUNIT_TEST      (hugeBlobTest);
    CPPUNIT_TEST      (smallBlobTest);
    CPPUNIT_TEST_SUITE_END();

public:
    FileBlobTest()
    : BlobTest(HAM_FALSE)
    {
    }
};

class NoCacheBlobTest : public BlobTest
{
    CPPUNIT_TEST_SUITE(NoCacheBlobTest);
    CPPUNIT_TEST      (structureTest);
    CPPUNIT_TEST      (allocReadFreeTest);
    CPPUNIT_TEST      (replaceTest);
    CPPUNIT_TEST      (replaceWithBigTest);
    CPPUNIT_TEST      (replaceWithSmallTest);
    /* negative tests are not necessary, because hamsterdb asserts that
     * blob-IDs actually exist */
    CPPUNIT_TEST      (multipleAllocReadFreeTest);
    CPPUNIT_TEST      (hugeBlobTest);
    CPPUNIT_TEST      (smallBlobTest);
    CPPUNIT_TEST_SUITE_END();

public:
    NoCacheBlobTest()
    : BlobTest(HAM_FALSE)
    {
    }
};

class InMemoryBlobTest : public BlobTest
{
    CPPUNIT_TEST_SUITE(InMemoryBlobTest);
    CPPUNIT_TEST      (structureTest);
    CPPUNIT_TEST      (allocReadFreeTest);
    CPPUNIT_TEST      (replaceTest);
    CPPUNIT_TEST      (replaceWithBigTest);
    CPPUNIT_TEST      (replaceWithSmallTest);
    CPPUNIT_TEST      (multipleAllocReadFreeTest);
    CPPUNIT_TEST      (hugeBlobTest);
    CPPUNIT_TEST      (smallBlobTest);
    CPPUNIT_TEST_SUITE_END();

public:
    InMemoryBlobTest()
    : BlobTest(HAM_TRUE)
    {
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(FileBlobTest);
CPPUNIT_TEST_SUITE_REGISTRATION(NoCacheBlobTest);
CPPUNIT_TEST_SUITE_REGISTRATION(InMemoryBlobTest);

