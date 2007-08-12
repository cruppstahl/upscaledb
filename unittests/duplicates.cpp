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
#include <errno.h>
#include <string.h>
#include <cppunit/extensions/HelperMacros.h>
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "memtracker.h"

class DupeTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(DupeTest);
    CPPUNIT_TEST      (insertDuplicatesTest);
    CPPUNIT_TEST      (insertEraseTest);
    CPPUNIT_TEST      (reopenTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_u32_t m_flags;
    ham_db_t *m_db;

public:
    DupeTest(ham_u32_t flags=0)
    :   m_flags(flags)
    {
    }

    void setUp()
    { 
#if WIN32
        (void)DeleteFileA((LPCSTR)".test");
#else
        if (unlink(".test")) {
            if (errno!=2)
                printf("failed to unlink .test: %s\n", strerror(errno));
        }
#endif
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&m_db));
        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, ".test", 
                    m_flags|HAM_ENABLE_DUPLICATES, 0664));
    }

    void tearDown()
    {
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(m_db));
    }
    
    void insertDuplicatesTest(void)
    {
        ham_key_t key;
        ham_record_t rec, rec2;
        char data[16];
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        ::memset(&data, 0x13, sizeof(data));
        rec.data=data;
        rec.size=sizeof(data);

        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));
        ::memset(&rec2, 0, sizeof(rec2));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec2, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec2.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec2.data, sizeof(data)));

        ::memset(&data, 0x14, sizeof(data));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        ::memset(&rec2, 0, sizeof(rec2));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec2, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec2.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec2.data, sizeof(data)));

        ::memset(&data, 0x15, sizeof(data));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        ::memset(&rec2, 0, sizeof(rec2));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec2, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec2.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec2.data, sizeof(data)));

        ::memset(&data, 0x16, sizeof(data));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        ::memset(&rec2, 0, sizeof(rec2));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec2, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec2.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec2.data, sizeof(data)));
    }

    void insertEraseTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        char data[16];
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        ::memset(&data, 0x13, sizeof(data));
        rec.data=data;
        rec.size=sizeof(data);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        ::memset(&data, 0x14, sizeof(data));
        rec.data=data;
        rec.size=sizeof(data);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        ::memset(&data, 0x15, sizeof(data));
        rec.data=data;
        rec.size=sizeof(data);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec.data, sizeof(data)));

        ::memset(&data, 0x14, sizeof(data));
        CPPUNIT_ASSERT_EQUAL(0, ham_erase(m_db, 0, &key, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec.data, sizeof(data)));

        ::memset(&data, 0x13, sizeof(data));
        CPPUNIT_ASSERT_EQUAL(0, ham_erase(m_db, 0, &key, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec.data, sizeof(data)));

        CPPUNIT_ASSERT_EQUAL(0, ham_erase(m_db, 0, &key, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_find(m_db, 0, &key, &rec, 0));
    }

    void reopenTest(void)
    {
        ham_key_t key;
        ham_record_t rec, rec2;
        char data[16];
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        ::memset(&data, 0x13, sizeof(data));
        rec.data=data;
        rec.size=sizeof(data);

        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));
        ::memset(&rec2, 0, sizeof(rec2));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec2, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec2.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec2.data, sizeof(data)));

        ::memset(&data, 0x14, sizeof(data));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        ::memset(&rec2, 0, sizeof(rec2));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec2, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec2.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec2.data, sizeof(data)));

        /* reopen the database */
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
        CPPUNIT_ASSERT_EQUAL(0, ham_open(m_db, ".test", m_flags));
        CPPUNIT_ASSERT(db_get_rt_flags(m_db)&HAM_ENABLE_DUPLICATES);

        ::memset(&data, 0x15, sizeof(data));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        ::memset(&rec2, 0, sizeof(rec2));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec2, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec2.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec2.data, sizeof(data)));

        ::memset(&data, 0x16, sizeof(data));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        ::memset(&rec2, 0, sizeof(rec2));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec2, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec2.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec2.data, sizeof(data)));
    }


};

class InMemoryDupeTest : public DupeTest
{
    CPPUNIT_TEST_SUITE(InMemoryDupeTest);
    CPPUNIT_TEST      (insertDuplicatesTest);
    CPPUNIT_TEST      (insertEraseTest);
    CPPUNIT_TEST_SUITE_END();

public:
    InMemoryDupeTest()
    :   DupeTest(HAM_IN_MEMORY_DB)
    {
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(DupeTest);
CPPUNIT_TEST_SUITE_REGISTRATION(InMemoryDupeTest);
