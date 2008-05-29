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
#include <cppunit/extensions/HelperMacros.h>
#include <ham/hamsterdb.h>
#include "memtracker.h"
#include "../src/db.h"
#include "../src/version.h"
#include "os.hpp"

class EraseTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(EraseTest);
    CPPUNIT_TEST      (collapseRootTest);
    CPPUNIT_TEST      (shiftFromRightTest);
    CPPUNIT_TEST      (shiftFromLeftTest);
    CPPUNIT_TEST      (mergeWithLeftTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_db_t *m_db;
    ham_u32_t m_flags;
    memtracker_t *m_alloc;

public:
    EraseTest(ham_u32_t flags=0)
        : m_db(0), m_flags(flags), m_alloc(0)
    {
    }

    void setUp()
    { 
        os::unlink(".test");
        CPPUNIT_ASSERT((m_alloc=memtracker_new())!=0);
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, ".test", m_flags, 0644));
    }
    
    void tearDown() 
    { 
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        CPPUNIT_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void prepare(int num_inserts)
    {
        ham_key_t key;
        ham_record_t rec;

        ham_parameter_t ps[]={
            {HAM_PARAM_PAGESIZE,   1024}, 
            {HAM_PARAM_KEYSIZE,   128}, 
            {0, 0}
        };

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create_ex(m_db, ".test", m_flags, 0644, &ps[0]));

        for (int i=0; i<num_inserts*10; i+=10) {
            key.data=&i;
            rec.data=&i;
            key.size=sizeof(i);
            rec.size=sizeof(i);

            CPPUNIT_ASSERT_EQUAL(0,
                    ham_insert(m_db, 0, &key, &rec, 0));
        }
    }

    void collapseRootTest() {
        ham_key_t key;
        memset(&key, 0, sizeof(key));

        prepare(8);

        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                ham_erase(m_db, 0, &key, 0));

        for (int i=0; i<80; i+=10) {
            key.data=&i;
            key.size=sizeof(i);

            CPPUNIT_ASSERT_EQUAL(0,
                    ham_erase(m_db, 0, &key, 0));
        }
    }

    void shiftFromRightTest() {
        ham_key_t key;
        memset(&key, 0, sizeof(key));
        int i=0;

        prepare(8);

        key.data=&i;
        key.size=sizeof(i);

        CPPUNIT_ASSERT_EQUAL(0, ham_erase(m_db, 0, &key, 0));
    }

    void shiftFromLeftTest() {
        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        int i;

        prepare(8);

        i=21;
        key.data=&i;
        key.size=sizeof(i);
        rec.data=&i;
        rec.size=sizeof(i);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));
        i=22;
        key.data=&i;
        key.size=sizeof(i);
        rec.data=&i;
        rec.size=sizeof(i);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));
        i=23;
        key.data=&i;
        key.size=sizeof(i);
        rec.data=&i;
        rec.size=sizeof(i);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

        i=70;
        key.data=&i;
        key.size=sizeof(i);
        CPPUNIT_ASSERT_EQUAL(0, ham_erase(m_db, 0, &key, 0));

        i=60;
        key.data=&i;
        key.size=sizeof(i);
        CPPUNIT_ASSERT_EQUAL(0, ham_erase(m_db, 0, &key, 0));

        i=50;
        key.data=&i;
        key.size=sizeof(i);
        CPPUNIT_ASSERT_EQUAL(0, ham_erase(m_db, 0, &key, 0));
    }

    void mergeWithLeftTest() {
        ham_key_t key;
        memset(&key, 0, sizeof(key));

        prepare(8);

        for (int i=70; i>=50; i-=10) {
            key.data=&i;
            key.size=sizeof(i);

            CPPUNIT_ASSERT_EQUAL(0, ham_erase(m_db, 0, &key, 0));
        }
    }
};

class InMemoryEraseTest : public EraseTest
{
    CPPUNIT_TEST_SUITE(EraseTest);
    CPPUNIT_TEST      (collapseRootTest);
    CPPUNIT_TEST      (shiftFromRightTest);
    CPPUNIT_TEST      (shiftFromLeftTest);
    CPPUNIT_TEST      (mergeWithLeftTest);
    CPPUNIT_TEST_SUITE_END();

public:
    InMemoryEraseTest() 
        : EraseTest(HAM_IN_MEMORY_DB) 
    {
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(EraseTest);
CPPUNIT_TEST_SUITE_REGISTRATION(InMemoryEraseTest);

