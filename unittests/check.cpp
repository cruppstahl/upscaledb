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
#include <ham/hamsterdb.h>
#include "memtracker.h"
#include "../src/db.h"
#include "os.hpp"

#include "bfc-testsuite.hpp"

using namespace bfc;

class CheckIntegrityTest : public fixture
{
public:
    CheckIntegrityTest(ham_bool_t inmemorydb=HAM_FALSE, const char *name=0)
        : fixture(name ? name : "CheckIntegrityTest"),
            m_inmemory(inmemorydb)
    {
        if (name)
            return;
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(CheckIntegrityTest, emptyDatabaseTest);
        BFC_REGISTER_TEST(CheckIntegrityTest, smallDatabaseTest);
        BFC_REGISTER_TEST(CheckIntegrityTest, levelledDatabaseTest);
    }

protected:
    ham_db_t *m_db;
    ham_bool_t m_inmemory;
    memtracker_t *m_alloc;

public:
    void setup()
    { 
        os::unlink(".test");

        BFC_ASSERT((m_alloc=memtracker_new())!=0);
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        BFC_ASSERT_EQUAL(0, ham_create(m_db, ".test", 
                    m_inmemory ? HAM_IN_MEMORY_DB : 0,
                    0644));
    }
    
    void teardown() 
    { 
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void emptyDatabaseTest()
    {
#ifdef HAM_ENABLE_INTERNAL
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_check_integrity(0, 0));
        BFC_ASSERT_EQUAL(0,
                ham_check_integrity(m_db, 0));
#endif
    }

    void smallDatabaseTest()
    {
#ifdef HAM_ENABLE_INTERNAL
        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        for (int i=0; i<5; i++) {
            key.size=sizeof(i);
            key.data=&i;
            BFC_ASSERT_EQUAL(0, 
                    ham_insert(m_db, 0, &key, &rec, 0));
        }

        BFC_ASSERT_EQUAL(0,
                ham_check_integrity(m_db, 0));
#endif
    }

    void levelledDatabaseTest()
    {
#ifdef HAM_ENABLE_INTERNAL
        ham_key_t key;
        ham_record_t rec;
        ham_parameter_t params[]={
            { HAM_PARAM_PAGESIZE, 1024 },
            { HAM_PARAM_KEYSIZE, 128 },
            { 0, 0 }
        };
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_create_ex(m_db, ".test", 
                    m_inmemory ? HAM_IN_MEMORY_DB : 0,
                    0644, &params[0]));

        for (int i=0; i<100; i++) {
            key.size=sizeof(i);
            key.data=&i;
            BFC_ASSERT_EQUAL(0, 
                    ham_insert(m_db, 0, &key, &rec, 0));
        }

        BFC_ASSERT_EQUAL(0,
                ham_check_integrity(m_db, 0));
#endif
    }
};

class InMemoryCheckIntegrityTest : public CheckIntegrityTest
{
public:
    InMemoryCheckIntegrityTest()
    :   CheckIntegrityTest(HAM_TRUE, "InMemoryCheckIntegrityTest")
    {
        clear_tests(); // don't inherit tests
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(InMemoryCheckIntegrityTest, emptyDatabaseTest);
        BFC_REGISTER_TEST(InMemoryCheckIntegrityTest, smallDatabaseTest);
        BFC_REGISTER_TEST(InMemoryCheckIntegrityTest, levelledDatabaseTest);
    }
};

#ifdef HAM_ENABLE_INTERNAL
BFC_REGISTER_FIXTURE(CheckIntegrityTest);
BFC_REGISTER_FIXTURE(InMemoryCheckIntegrityTest);
#endif

