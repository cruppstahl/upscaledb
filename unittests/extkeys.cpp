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

#include <stdexcept>
#include <cstring>
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/extkeys.h"
#include "memtracker.h"

#include "bfc-testsuite.hpp"

using namespace bfc;

class ExtendedKeyTest : public fixture
{
public:
    ExtendedKeyTest()
    :   fixture("ExtendedKeyTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(ExtendedKeyTest, keyStructureTest);
        BFC_REGISTER_TEST(ExtendedKeyTest, cacheStructureTest);
        BFC_REGISTER_TEST(ExtendedKeyTest, insertFetchRemoveTest);
        BFC_REGISTER_TEST(ExtendedKeyTest, negativeFetchTest);
        BFC_REGISTER_TEST(ExtendedKeyTest, negativeRemoveTest);
        BFC_REGISTER_TEST(ExtendedKeyTest, bigCacheTest);
        BFC_REGISTER_TEST(ExtendedKeyTest, purgeTest);
    }

protected:
    ham_db_t *m_db;
    memtracker_t *m_alloc;

public:
    void setup()
    { 
        BFC_ASSERT((m_alloc=memtracker_new())!=0);
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        BFC_ASSERT_EQUAL(0, ham_create(m_db, 0, HAM_IN_MEMORY_DB, 0));

        if (!db_get_extkey_cache(m_db))
            db_set_extkey_cache(m_db, extkey_cache_new(m_db));
        BFC_ASSERT(db_get_extkey_cache(m_db));
    }
    
    void teardown() 
    { 
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void keyStructureTest(void)
    {
        extkey_t e;

        BFC_ASSERT_EQUAL(SIZEOF_EXTKEY_T, sizeof(extkey_t)-1);

        extkey_set_blobid(&e, (ham_offset_t)0x12345);
        BFC_ASSERT_EQUAL((ham_offset_t)0x12345, 
                extkey_get_blobid(&e));

        extkey_set_txn_id(&e, (ham_u64_t)0x12345678);
        BFC_ASSERT_EQUAL((ham_u64_t)0x12345678, 
                extkey_get_txn_id(&e));

        extkey_set_next(&e, (extkey_t *)0x13);
        BFC_ASSERT_EQUAL((extkey_t *)0x13, extkey_get_next(&e));

        extkey_set_size(&e, 200);
        BFC_ASSERT_EQUAL((ham_size_t)200, extkey_get_size(&e));
    }

    void cacheStructureTest(void)
    {
        ham_size_t tmp;
        extkey_cache_t *c=db_get_extkey_cache(m_db);

        extkey_cache_set_db(c, m_db);
        BFC_ASSERT_EQUAL(m_db, extkey_cache_get_db(c));

        tmp=extkey_cache_get_usedsize(c);
        extkey_cache_set_usedsize(c, 1000);
        BFC_ASSERT_EQUAL((ham_size_t)1000, extkey_cache_get_usedsize(c));
        extkey_cache_set_usedsize(c, tmp);

        tmp=extkey_cache_get_bucketsize(c);
        extkey_cache_set_bucketsize(c, 500);
        BFC_ASSERT_EQUAL((ham_size_t)500, extkey_cache_get_bucketsize(c));
        extkey_cache_set_bucketsize(c, tmp);

        for (ham_size_t i=0; i<extkey_cache_get_bucketsize(c); i++) {
            extkey_t *e;

            e=extkey_cache_get_bucket(c, i);
            BFC_ASSERT_EQUAL((extkey_t *)0, e);

            extkey_cache_set_bucket(c, i, (extkey_t *)(i+1));
            e=extkey_cache_get_bucket(c, i);
            BFC_ASSERT_EQUAL((extkey_t *)(i+1), e);

            extkey_cache_set_bucket(c, i, 0);
        }
    }

    void insertFetchRemoveTest(void)
    {
        extkey_cache_t *c=db_get_extkey_cache(m_db);
        ham_u8_t *pbuffer, buffer[12]={0};
        ham_size_t size;

        BFC_ASSERT_EQUAL(0, 
                extkey_cache_insert(c, 0x123, sizeof(buffer), buffer));

        BFC_ASSERT_EQUAL(0, 
                extkey_cache_fetch(c, 0x123, &size, &pbuffer));
        BFC_ASSERT_EQUAL((ham_size_t)12, size);
        BFC_ASSERT(::memcmp(pbuffer, buffer, size)==0);

        BFC_ASSERT_EQUAL(0, 
                extkey_cache_remove(c, 0x123));
    }

    void negativeFetchTest(void)
    {
        extkey_cache_t *c=db_get_extkey_cache(m_db);
        ham_u8_t *pbuffer, buffer[12]={0};
        ham_size_t size;

        BFC_ASSERT_EQUAL(0, 
                extkey_cache_insert(c, 0x123, sizeof(buffer), buffer));

        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                extkey_cache_fetch(c, 0x1234, &size, &pbuffer));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                extkey_cache_fetch(c, 0x12345, &size, &pbuffer));

        BFC_ASSERT_EQUAL(0, 
                extkey_cache_remove(c, 0x123));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                extkey_cache_fetch(c, 0x123, &size, &pbuffer));
    }

    void negativeRemoveTest(void)
    {
        extkey_cache_t *c=db_get_extkey_cache(m_db);

        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                extkey_cache_remove(c, 0x12345));
    }

    void bigCacheTest(void)
    {
        extkey_cache_t *c=db_get_extkey_cache(m_db);
        ham_u8_t *pbuffer, buffer[12]={0};
        ham_size_t size;

        for (ham_size_t i=0; i<extkey_cache_get_bucketsize(c)*4; i++) {
            BFC_ASSERT_EQUAL(0, 
                extkey_cache_insert(c, (ham_offset_t)i, 
                    sizeof(buffer), buffer));
        }

        for (ham_size_t i=0; i<extkey_cache_get_bucketsize(c)*4; i++) {
            BFC_ASSERT_EQUAL(0, 
                extkey_cache_fetch(c, (ham_offset_t)i, 
                    &size, &pbuffer));
            BFC_ASSERT_EQUAL((ham_size_t)12, size);
        }

        for (ham_size_t i=0; i<extkey_cache_get_bucketsize(c)*4; i++) {
            BFC_ASSERT_EQUAL(0, 
                extkey_cache_remove(c, (ham_offset_t)i));
        }
    }

    void purgeTest(void)
    {
        extkey_cache_t *c=db_get_extkey_cache(m_db);
        ham_u8_t *pbuffer, buffer[12]={0};
        ham_size_t size;

        for (int i=0; i<20; i++) {
            BFC_ASSERT_EQUAL(0, 
                extkey_cache_insert(c, (ham_offset_t)i, 
                    sizeof(buffer), buffer));
        }

        db_set_txn_id(m_db, db_get_txn_id(m_db)+2000);

        BFC_ASSERT_EQUAL(0, extkey_cache_purge(c));

        for (int i=0; i<20; i++) {
            BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                extkey_cache_fetch(c, (ham_offset_t)i, 
                    &size, &pbuffer));
        }
    }
};

BFC_REGISTER_FIXTURE(ExtendedKeyTest);

