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
#include "../src/extkeys.h"
#include "memtracker.h"

class ExtendedKeyTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(ExtendedKeyTest);
    CPPUNIT_TEST      (keyStructureTest);
    CPPUNIT_TEST      (cacheStructureTest);
    CPPUNIT_TEST      (insertFetchRemoveTest);
    CPPUNIT_TEST      (negativeFetchTest);
    CPPUNIT_TEST      (negativeRemoveTest);
    CPPUNIT_TEST      (bigCacheTest);
    CPPUNIT_TEST      (purgeTest);
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

        if (!db_get_extkey_cache(m_db))
            db_set_extkey_cache(m_db, extkey_cache_new(m_db));
        CPPUNIT_ASSERT(db_get_extkey_cache(m_db));
    }
    
    void tearDown() 
    { 
        ham_close(m_db);
        ham_delete(m_db);
        CPPUNIT_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void keyStructureTest(void)
    {
        extkey_t e;

        CPPUNIT_ASSERT_EQUAL(SIZEOF_EXTKEY_T, sizeof(extkey_t)-1);

        extkey_set_blobid(&e, (ham_offset_t)0x12345);
        CPPUNIT_ASSERT_EQUAL((ham_offset_t)0x12345, 
                extkey_get_blobid(&e));

        extkey_set_txn_id(&e, (ham_u64_t)0x12345678);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)0x12345678, 
                extkey_get_txn_id(&e));

        extkey_set_next(&e, (extkey_t *)0x13);
        CPPUNIT_ASSERT_EQUAL((extkey_t *)0x13, extkey_get_next(&e));

        extkey_set_size(&e, 200);
        CPPUNIT_ASSERT_EQUAL((ham_size_t)200, extkey_get_size(&e));
    }

    void cacheStructureTest(void)
    {
        ham_size_t tmp;
        extkey_cache_t *c=db_get_extkey_cache(m_db);

        extkey_cache_set_db(c, m_db);
        CPPUNIT_ASSERT_EQUAL(m_db, extkey_cache_get_db(c));

        tmp=extkey_cache_get_usedsize(c);
        extkey_cache_set_usedsize(c, 1000);
        CPPUNIT_ASSERT_EQUAL((ham_size_t)1000, extkey_cache_get_usedsize(c));
        extkey_cache_set_usedsize(c, tmp);

        tmp=extkey_cache_get_bucketsize(c);
        extkey_cache_set_bucketsize(c, 500);
        CPPUNIT_ASSERT_EQUAL((ham_size_t)500, extkey_cache_get_bucketsize(c));
        extkey_cache_set_bucketsize(c, tmp);

        for (ham_size_t i=0; i<extkey_cache_get_bucketsize(c); i++) {
            extkey_t *e;

            e=extkey_cache_get_bucket(c, i);
            CPPUNIT_ASSERT_EQUAL((extkey_t *)0, e);

            extkey_cache_set_bucket(c, i, (extkey_t *)(i+1));
            e=extkey_cache_get_bucket(c, i);
            CPPUNIT_ASSERT_EQUAL((extkey_t *)(i+1), e);

            extkey_cache_set_bucket(c, i, 0);
        }
    }

    void insertFetchRemoveTest(void)
    {
        extkey_cache_t *c=db_get_extkey_cache(m_db);
        ham_u8_t *pbuffer, buffer[12]={0};
        ham_size_t size;

        CPPUNIT_ASSERT_EQUAL(0, 
                extkey_cache_insert(c, 0x123, sizeof(buffer), buffer));

        CPPUNIT_ASSERT_EQUAL(0, 
                extkey_cache_fetch(c, 0x123, &size, &pbuffer));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)12, size);
        CPPUNIT_ASSERT(::memcmp(pbuffer, buffer, size)==0);

        CPPUNIT_ASSERT_EQUAL(0, 
                extkey_cache_remove(c, 0x123));
    }

    void negativeFetchTest(void)
    {
        extkey_cache_t *c=db_get_extkey_cache(m_db);
        ham_u8_t *pbuffer, buffer[12]={0};
        ham_size_t size;

        CPPUNIT_ASSERT_EQUAL(0, 
                extkey_cache_insert(c, 0x123, sizeof(buffer), buffer));

        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                extkey_cache_fetch(c, 0x1234, &size, &pbuffer));
        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                extkey_cache_fetch(c, 0x12345, &size, &pbuffer));

        CPPUNIT_ASSERT_EQUAL(0, 
                extkey_cache_remove(c, 0x123));
        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                extkey_cache_fetch(c, 0x123, &size, &pbuffer));
    }

    void negativeRemoveTest(void)
    {
        extkey_cache_t *c=db_get_extkey_cache(m_db);

        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                extkey_cache_remove(c, 0x12345));
    }

    void bigCacheTest(void)
    {
        extkey_cache_t *c=db_get_extkey_cache(m_db);
        ham_u8_t *pbuffer, buffer[12]={0};
        ham_size_t size;

        for (ham_size_t i=0; i<extkey_cache_get_bucketsize(c)*4; i++) {
            CPPUNIT_ASSERT_EQUAL(0, 
                extkey_cache_insert(c, (ham_offset_t)i, 
                    sizeof(buffer), buffer));
        }

        for (ham_size_t i=0; i<extkey_cache_get_bucketsize(c)*4; i++) {
            CPPUNIT_ASSERT_EQUAL(0, 
                extkey_cache_fetch(c, (ham_offset_t)i, 
                    &size, &pbuffer));
            CPPUNIT_ASSERT_EQUAL((ham_size_t)12, size);
        }

        for (ham_size_t i=0; i<extkey_cache_get_bucketsize(c)*4; i++) {
            CPPUNIT_ASSERT_EQUAL(0, 
                extkey_cache_remove(c, (ham_offset_t)i));
        }
    }

    void purgeTest(void)
    {
        extkey_cache_t *c=db_get_extkey_cache(m_db);
        ham_u8_t *pbuffer, buffer[12]={0};
        ham_size_t size;

        for (int i=0; i<20; i++) {
            CPPUNIT_ASSERT_EQUAL(0, 
                extkey_cache_insert(c, (ham_offset_t)i, 
                    sizeof(buffer), buffer));
        }

        db_set_txn_id(m_db, db_get_txn_id(m_db)+2000);

        CPPUNIT_ASSERT_EQUAL(0, extkey_cache_purge(c));

        for (int i=0; i<20; i++) {
            CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                extkey_cache_fetch(c, (ham_offset_t)i, 
                    &size, &pbuffer));
        }
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(ExtendedKeyTest);

