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

#include <cstring>
#include <ham/hamsterdb.h>
#include "../src/btree_cursor.h"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;

class BaseCursorTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    BaseCursorTest(const char *classname, ham_u32_t flags=0)
    :   hamsterDB_fixture(classname), m_flags(flags)
    {
    }

public:
    ham_db_t *m_db;
    ham_u32_t m_flags;

    virtual void setup() 
	{ 
		__super::setup();

        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        BFC_ASSERT_EQUAL(0, ham_create(m_db, BFC_OPATH(".test"), 
                    m_flags|HAM_ENABLE_DUPLICATES, 0664));
    }

    virtual void teardown() 
    { 
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_AUTO_CLEANUP));
        BFC_ASSERT_EQUAL(0, ham_delete(m_db));

        __super::teardown();
    }

    void getDuplicateRecordSizeTest()
    {
        const int MAX=20;
        ham_key_t key={0};
        ham_record_t rec={0};
        ham_cursor_t *c;
        char data[16];

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));

        for (int i=0; i<MAX; i++) {
            rec.data=data;
            rec.size=i;
            ::memset(&data, i+0x15, sizeof(data));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(c, &key, &rec, HAM_DUPLICATE));
        }

        for (int i=0; i<MAX; i++) {
            ham_offset_t size=0;

            ::memset(&key, 0, sizeof(key));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c, &key, &rec, 
                        i==0 ? HAM_CURSOR_FIRST : HAM_CURSOR_NEXT));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_get_record_size(c, &size));
            BFC_ASSERT_EQUAL(size, rec.size);
        }

        BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
    }

    void getRecordSizeTest()
    {
        const int MAX=20;
        ham_key_t key={0};
        ham_record_t rec={0};
        ham_cursor_t *c;
        char data[16];

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));

        for (int i=0; i<MAX; i++) {
            key.data=data;
            key.size=sizeof(data);
            rec.data=data;
            rec.size=i;
            ::memset(&data, i+0x15, sizeof(data));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(c, &key, &rec, HAM_DUPLICATE));
        }

        for (int i=0; i<MAX; i++) {
            ham_offset_t size=0;

            key.data=data;
            key.size=sizeof(data);
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c, &key, &rec, 
                        i==0 ? HAM_CURSOR_FIRST : HAM_CURSOR_NEXT));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_get_record_size(c, &size));
            BFC_ASSERT_EQUAL(size, rec.size);
        }

        BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
    }
};

class CursorTest : public BaseCursorTest
{
public:
    CursorTest()
    :   BaseCursorTest("CursorTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(CursorTest, getDuplicateRecordSizeTest);
        BFC_REGISTER_TEST(CursorTest, getRecordSizeTest);
    }
};

class InMemoryCursorTest : public BaseCursorTest
{
public:
    InMemoryCursorTest()
    :   BaseCursorTest("InMemoryCursorTest", HAM_IN_MEMORY_DB)
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(InMemoryCursorTest, getDuplicateRecordSizeTest);
        BFC_REGISTER_TEST(InMemoryCursorTest, getRecordSizeTest);
    }
};


BFC_REGISTER_FIXTURE(CursorTest);
BFC_REGISTER_FIXTURE(InMemoryCursorTest);
