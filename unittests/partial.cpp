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
#include <cstring>
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/blob.h"
#include "../src/env.h"
#include "../src/page.h"
#include "../src/keys.h"
#include "../src/freelist.h"
#include "memtracker.h"
#include "os.hpp"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;

class PartialRecordTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    PartialRecordTest()
    :   hamsterDB_fixture("PartialRecordTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(PartialRecordTest, negativeInsertTest);
        BFC_REGISTER_TEST(PartialRecordTest, negativeCursorInsertTest);
    }

protected:
    ham_db_t *m_db;
    ham_env_t *m_env;
    memtracker_t *m_alloc;

public:
    virtual void setup() 
    { 
        __super::setup();

        BFC_ASSERT((m_alloc=memtracker_new())!=0);
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        BFC_ASSERT_EQUAL(0, 
                ham_create_ex(m_db, BFC_OPATH(".test"), 
                        0, 0644, 0));
        m_env=db_get_env(m_db);
    }
    
    virtual void teardown() 
    { 
        __super::teardown();

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void negativeInsertTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        ham_db_t *db;
        BFC_ASSERT_EQUAL(0, ham_new(&db));
        BFC_ASSERT_EQUAL(0, 
                ham_create_ex(db, BFC_OPATH(".test.db"), 
                        HAM_SORT_DUPLICATES|HAM_ENABLE_DUPLICATES, 0644, 0));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(db, 0, &key, &rec, HAM_PARTIAL));
        BFC_ASSERT_EQUAL(0, 
                ham_insert(db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        ham_delete(db);
    }

    void negativeCursorInsertTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        ham_db_t *db;
        BFC_ASSERT_EQUAL(0, ham_new(&db));
        BFC_ASSERT_EQUAL(0, 
                ham_create_ex(db, BFC_OPATH(".test.db"), 
                        HAM_SORT_DUPLICATES|HAM_ENABLE_DUPLICATES, 0644, 0));

        ham_cursor_t *c;
        BFC_ASSERT_EQUAL(0, ham_cursor_create(db, 0, 0, &c));

        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_insert(c, &key, &rec, HAM_PARTIAL));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_insert(c, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        ham_delete(db);
    }

};


BFC_REGISTER_FIXTURE(PartialRecordTest);
