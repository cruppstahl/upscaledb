/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 * unit tests for cache.h/cache.c
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
        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, 0, HAM_IN_MEMORY_DB, 0));
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

};

CPPUNIT_TEST_SUITE_REGISTRATION(BlobTest);

