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
#include <string.h>
#include <ham/hamsterdb.h>
#include "memtracker.h"
#include "../src/db.h"
#include "os.hpp"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;

class CheckIntegrityTest : public hamsterDB_fixture
{
	define_super(hamsterDB_fixture);

public:
    CheckIntegrityTest(ham_bool_t inmemorydb=HAM_FALSE, const char *name="CheckIntegrityTest")
        : hamsterDB_fixture(name),
            m_inmemory(inmemorydb)
    {
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
    virtual void setup() 
	{ 
		__super::setup();

        os::unlink(BFC_OPATH(".test"));

        BFC_ASSERT((m_alloc=memtracker_new())!=0);
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        BFC_ASSERT_EQUAL(0, ham_create(m_db, BFC_OPATH(".test"), 
                    m_inmemory ? HAM_IN_MEMORY_DB : 0,
                    0644));
    }
    
    virtual void teardown() 
	{ 
		__super::teardown();

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void emptyDatabaseTest()
    {
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_check_integrity(0, 0));
        BFC_ASSERT_EQUAL(0,
                ham_check_integrity(m_db, 0));
    }

    void smallDatabaseTest()
    {
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        for (int i=0; i<5; i++) {
            key.size=sizeof(i);
            key.data=&i;
            BFC_ASSERT_EQUAL(0, 
                    ham_insert(m_db, 0, &key, &rec, 0));
        }

        BFC_ASSERT_EQUAL(0,
                ham_check_integrity(m_db, 0));
    }

    void levelledDatabaseTest()
    {
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
        BFC_ASSERT_EQUAL(0, ham_create_ex(m_db, BFC_OPATH(".test"), 
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
    }
};

class InMemoryCheckIntegrityTest : public CheckIntegrityTest
{
public:
    InMemoryCheckIntegrityTest()
    :   CheckIntegrityTest(HAM_TRUE, "InMemoryCheckIntegrityTest")
    {
    }
};

BFC_REGISTER_FIXTURE(CheckIntegrityTest);
BFC_REGISTER_FIXTURE(InMemoryCheckIntegrityTest);

