/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 * unit tests for blob.h/blob.c
 *
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
    CPPUNIT_TEST      (negativeReadTest);
    CPPUNIT_TEST      (negativeFreeTest);
    CPPUNIT_TEST      (multipleAllocReadFreeTest);
    CPPUNIT_TEST      (hugeBlobTest);
    // TODO see TODO file for missing tests
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_db_t *m_db;
    memtracker_t *m_alloc;

public:
    void setUp()
    { 
        CPPUNIT_ASSERT((m_alloc=memtracker_new())!=0);
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, 0, 0, 0));
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
        CPPUNIT_ASSERT_EQUAL(0x789ull, blob_get_alloc_size(&b));

        blob_set_real_size(&b, 0xabcull);
        CPPUNIT_ASSERT_EQUAL(0xabcull, blob_get_real_size(&b));

        blob_set_user_size(&b, 0x123ull);
        CPPUNIT_ASSERT_EQUAL(0x123ull, blob_get_user_size(&b));

        blob_set_flags(&b, 0x13);
        CPPUNIT_ASSERT_EQUAL(0x13, blob_get_flags(&b));
    }

    void allocReadFreeTest(void)
    {
        ham_u8_t buffer[16];
        ham_offset_t blobid;
        ham_record_t record;
        ::memset(&record, 0, sizeof(record));
        ::memset(&buffer, 0x12, sizeof(buffer));

        CPPUNIT_ASSERT_EQUAL(0, blob_allocate(m_db, buffer, 
                                (ham_size_t)sizeof(buffer), 0, &blobid));
        CPPUNIT_ASSERT(blobid!=0);

        CPPUNIT_ASSERT_EQUAL(0, blob_read(m_db, blobid, 
                                &record, 0));
        CPPUNIT_ASSERT_EQUAL(record.size, sizeof(buffer));
        CPPUNIT_ASSERT(::memcmp(buffer, record.data, record.size));

        CPPUNIT_ASSERT_EQUAL(0, blob_free(m_db, blobid, 0));
    }

    void negativeReadTest(void)
    {
        ham_offset_t blobid=0x12345ull;
        ham_record_t record;
        ::memset(&record, 0, sizeof(record));

        CPPUNIT_ASSERT_EQUAL(HAM_BLOB_NOT_FOUND, blob_read(m_db, blobid, 
                                &record, 0));
    }

    void negativeFreeTest(void)
    {
        ham_offset_t blobid=0x12345ull;

        CPPUNIT_ASSERT_EQUAL(HAM_BLOB_NOT_FOUND, blob_free(m_db, blobid, 0));
    }

    void loopInsert(int loops, int factor)
    {
        ham_u8_t *buffer;
        ham_offset_t blobid[loops];
        ham_record_t record;
        ::memset(&record, 0, sizeof(record));
        ::memset(&buffer, 0x12, sizeof(buffer));

        for (int i=0; i<loops; i++) {
            buffer=::malloc((i+1)*factor);
            CPPUNIT_ASSERT(buffer!=0);
            ::memset(buffer, (char)i, (i+1)*factor);

            CPPUNIT_ASSERT_EQUAL(0, blob_allocate(m_db, buffer, 
                                (ham_size_t)sizeof(buffer), 0, &blobid[i]));
            CPPUNIT_ASSERT(blobid[i]!=0);

            ::free(buffer);
        }

        for (int i=0; i<loops; i++) {
            buffer=::malloc((i+1)*factor);
            CPPUNIT_ASSERT(buffer!=0);
            ::memset(buffer, (char)i, (i+1)*factor);

            CPPUNIT_ASSERT_EQUAL(0, blob_read(m_db, blobid[i], 
                                &record, 0));
            CPPUNIT_ASSERT_EQUAL(record.size, (i+1)*factor);
            CPPUNIT_ASSERT(::memcmp(buffer, record.data, record.size));

            ::free(buffer);
        }

        for (int i=0; i<loops; i++) {
            CPPUNIT_ASSERT_EQUAL(0, blob_free(m_db, blobid[i], 0));
        }
    }

    void multipleAllocReadFreeTest(void)
    {
        loopInsert(20, 2048);
    }

    void hugeBlobTest(void)
    {
        loopInsert(10, 1024*1024*4);
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(BlobTest);

