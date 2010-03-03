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

        /* write at offset 0, partial size 50, record size 50 (no gaps) */
        BFC_REGISTER_TEST(PartialRecordTest, simpleInsertTest);

        /* write at offset 0, partial size 50, record size 100 (gap at end) */
        BFC_REGISTER_TEST(PartialRecordTest, insertGapsAtEndTestSmall);

        /* write at offset 0, partial size 500, record size 1000 (gap at end) */
        BFC_REGISTER_TEST(PartialRecordTest, insertGapsAtEndTestBig);

        /* write at offset 0, partial size 5000, record size 10000 (gap at end) */
        BFC_REGISTER_TEST(PartialRecordTest, insertGapsAtEndTestBigger);

        /* write at offset 0, partial size 5001, record size 10001 (gap at end) */
        BFC_REGISTER_TEST(PartialRecordTest, insertGapsAtEndTestBiggerPlus1);

        /* write at offset 0, partial size 50000, record size 100000 (gap at end) */
        BFC_REGISTER_TEST(PartialRecordTest, insertGapsAtEndTestBiggest);

        /* write at offset 0, partial size 50001, record size 100001 (gap at end) */
        BFC_REGISTER_TEST(PartialRecordTest, insertGapsAtEndTestBiggestPlus1);

        /* write at offset 0, partial size 500000, record size 1000000 (gap at end) */
        BFC_REGISTER_TEST(PartialRecordTest, insertGapsAtEndTestSuperbig);

        /* write at offset 0, partial size 500001, record size 1000001 (gap at end) */
        BFC_REGISTER_TEST(PartialRecordTest, insertGapsAtEndTestSuperbigPlus1);
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

    void fillBuffer(ham_u8_t *ptr, ham_size_t offset, ham_size_t size)
    {
        for (ham_size_t i=0; i<size; i++)
            ptr[offset+i]=(ham_u8_t)i;
    }

    void simpleInsertTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_u8_t buffer[50];

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        /* fill the buffer with a pattern */
        fillBuffer(&buffer[0], 0, sizeof(buffer));

        /* write at offset 0, partial size 50, record size 50 (no gaps) */
        rec.partial_offset=0;
        rec.partial_size=50;
        rec.size=50;
        rec.data=buffer;
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_PARTIAL));

        /* verify the key */
        memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));

        BFC_ASSERT_EQUAL(50u, rec.size);
        BFC_ASSERT_EQUAL(0, memcmp(buffer, rec.data, rec.size));
    }

    void insertGapsAtEndTest(unsigned partial_size, unsigned record_size)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_u8_t *buffer=(ham_u8_t *)malloc(record_size);

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        /* fill the buffer with a pattern */
        fillBuffer(&buffer[0], 0, record_size);

        rec.partial_offset=0;
        rec.partial_size=partial_size;
        rec.size=record_size;
        rec.data=buffer;
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_PARTIAL));

        /* verify the key */
        memset(&buffer[partial_size], 0, record_size-partial_size);
        memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, HAM_PARTIAL));

        BFC_ASSERT_EQUAL(record_size, rec.size);
        BFC_ASSERT_EQUAL(0, memcmp(buffer, rec.data, rec.size));
        free(buffer);
    }

    void insertGapsAtEndTestSmall(void)
    {
        insertGapsAtEndTest(50, 100);
    }

    void insertGapsAtEndTestBig(void)
    {
        insertGapsAtEndTest(500, 1000);
    }

    void insertGapsAtEndTestBigger(void)
    {
        insertGapsAtEndTest(5000, 10000);
    }

    void insertGapsAtEndTestBiggerPlus1(void)
    {
        insertGapsAtEndTest(5001, 10001);
    }

    void insertGapsAtEndTestBiggest(void)
    {
        insertGapsAtEndTest(50000, 100000);
    }

    void insertGapsAtEndTestBiggestPlus1(void)
    {
        insertGapsAtEndTest(50001, 100001);
    }

    void insertGapsAtEndTestSuperbig(void)
    {
        insertGapsAtEndTest(500000, 1000000);
    }

    void insertGapsAtEndTestSuperbigPlus1(void)
    {
        insertGapsAtEndTest(500001, 1000001);
    }

};


BFC_REGISTER_FIXTURE(PartialRecordTest);
