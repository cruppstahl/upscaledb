/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
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

class BlobTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(BlobTest);
    CPPUNIT_TEST      (structureTest);
    CPPUNIT_TEST      (allocReadFreeTest);
    CPPUNIT_TEST      (replaceTest);
    CPPUNIT_TEST      (replaceWithBigTest);
    CPPUNIT_TEST      (replaceWithSmallTest);
    /* negative tests are not necessary, because hamsterdb asserts that
     * blob-IDs actually exist */
    //CPPUNIT_TEST      (negativeReadTest);
    //CPPUNIT_TEST      (negativeFreeTest);
    CPPUNIT_TEST      (multipleAllocReadFreeTest);
    CPPUNIT_TEST      (hugeBlobTest);
    CPPUNIT_TEST      (smallBlobTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_db_t *m_db;
    memtracker_t *m_alloc;
    ham_bool_t m_inmemory;

public:
    BlobTest(ham_bool_t inmemory=HAM_FALSE)
    :   m_db(0), m_alloc(0), m_inmemory(inmemory)
    {
    }

    void setUp()
    { 
#if WIN32
        (void)DeleteFileA((LPCSTR)".test");
#else
        (void)unlink(".test");
#endif
        CPPUNIT_ASSERT((m_alloc=memtracker_new())!=0);
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, ".test", 
                    m_inmemory ? HAM_IN_MEMORY_DB : 0, 0));
    }
    
    void tearDown() 
    { 
        ham_close(m_db);
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

        blob_set_real_size(&b, 0xabcull);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)0xabcull, blob_get_real_size(&b));

        blob_set_user_size(&b, 0x123ull);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)0x123ull, blob_get_user_size(&b));

        blob_set_flags(&b, 0x13);
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)0x13, blob_get_flags(&b));
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

        CPPUNIT_ASSERT_EQUAL(0, blob_replace(m_db, blobid, buffer2, 
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

        CPPUNIT_ASSERT_EQUAL(0, blob_replace(m_db, blobid, buffer2, 
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

        CPPUNIT_ASSERT_EQUAL(0, blob_replace(m_db, blobid, buffer2, 
                    sizeof(buffer2), 0, &blobid2));
        CPPUNIT_ASSERT(blobid2!=0);

        CPPUNIT_ASSERT_EQUAL(0, blob_read(m_db, blobid2, 
                                &record, 0));
        CPPUNIT_ASSERT_EQUAL(record.size, (ham_size_t)sizeof(buffer2));
        CPPUNIT_ASSERT(0==::memcmp(buffer2, record.data, record.size));

        /* make sure that at least 64bit are in the freelist */
        CPPUNIT_ASSERT(freel_alloc_area(m_db, 64)!=0);

        CPPUNIT_ASSERT_EQUAL(0, blob_free(m_db, blobid2, 0));

        /* and now another 64bit should be in the freelist */
        CPPUNIT_ASSERT(freel_alloc_area(m_db, 64)!=0);
    }

    void negativeReadTest(void)
    {
        ham_offset_t blobid=64ull*12;
        ham_record_t record;
        ::memset(&record, 0, sizeof(record));

        CPPUNIT_ASSERT_EQUAL(HAM_BLOB_NOT_FOUND, blob_read(m_db, blobid, 
                                &record, 0));
    }

    void negativeFreeTest(void)
    {
        ham_offset_t blobid=64ull*12;

        CPPUNIT_ASSERT_EQUAL(HAM_BLOB_NOT_FOUND, blob_free(m_db, blobid, 0));
    }

    void loopInsert(int loops, int factor)
    {
        ham_u8_t *buffer;
        ham_offset_t *blobid;
        ham_record_t record;
        ::memset(&record, 0, sizeof(record));
        ::memset(&buffer, 0x12, sizeof(buffer));

		blobid=(ham_offset_t *)::malloc(sizeof(ham_offset_t)*loops);
		CPPUNIT_ASSERT(blobid!=0);

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
    //CPPUNIT_TEST      (negativeReadTest);
    //CPPUNIT_TEST      (negativeFreeTest);
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
    : BlobTest(HAM_FALSE)
    {
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(FileBlobTest);
CPPUNIT_TEST_SUITE_REGISTRATION(InMemoryBlobTest);

