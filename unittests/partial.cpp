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

class BasePartialRecordTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    BasePartialRecordTest(const char *name)
    :   hamsterDB_fixture(name)
    {
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

    virtual void insertGaps(unsigned partial_offset, 
                    unsigned partial_size, unsigned record_size)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_u8_t *buffer=(ham_u8_t *)malloc(record_size);

        BFC_ASSERT(partial_offset+partial_size<=record_size);

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        /* fill the buffer with a pattern */
        fillBuffer(&buffer[0], 0, record_size);

        rec.partial_offset=partial_offset;
        rec.partial_size=partial_size;
        rec.size=record_size;
        rec.data=buffer;
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_PARTIAL));

        /* verify the key */
        memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));

        memset(&buffer[0], 0, record_size);
        fillBuffer(&buffer[partial_offset], 0, partial_size);
        BFC_ASSERT_EQUAL(record_size, rec.size);
        BFC_ASSERT_EQUAL(0, memcmp(buffer, rec.data, rec.size));
        free(buffer);
    }

    void insertGapsAtEndTestSmall(void)
    {
        insertGaps(0, 50, 100);
    }

    void insertGapsAtEndTestBig(void)
    {
        insertGaps(0, 500, 1000);
    }

    void insertGapsAtEndTestBigger(void)
    {
        insertGaps(0, 5000, 10000);
    }

    void insertGapsAtEndTestBiggerPlus1(void)
    {
        insertGaps(0, 5001, 10001);
    }

    void insertGapsAtEndTestBiggest(void)
    {
        insertGaps(0, 50000, 100000);
    }

    void insertGapsAtEndTestBiggestPlus1(void)
    {
        insertGaps(0, 50001, 100001);
    }

    void insertGapsAtEndTestSuperbig(void)
    {
        insertGaps(0, 500000, 1000000);
    }

    void insertGapsAtEndTestSuperbigPlus1(void)
    {
        insertGaps(0, 500001, 1000001);
    }

    void insertGapsAtBeginningSmall(void)
    {
        insertGaps(50, 50, 100);
    }

    void insertGapsAtBeginningBig(void)
    {
        insertGaps(500, 500, 1000);
    }

    void insertGapsAtBeginningBigger(void)
    {
        insertGaps(5000, 5000, 10000);
    }

    void insertGapsAtBeginningBiggerPlus1(void)
    {
        insertGaps(5001, 5001, 10002);
    }

    void insertGapsAtBeginningBiggest(void)
    {
        insertGaps(50000, 50000, 100000);
    }

    void insertGapsAtBeginningBiggestPlus1(void)
    {
        insertGaps(50001, 50001, 100002);
    }

    void insertGapsAtBeginningSuperbig(void)
    {
        insertGaps(500000, 500000, 1000000);
    }

    void insertGapsAtBeginningSuperbigPlus1(void)
    {
        insertGaps(500001, 500001, 1000002);
    }

    void insertGapsTestSmall(void)
    {
        insertGaps(50, 50, 200);
    }

    void insertGapsTestBig(void)
    {
        insertGaps(500, 500, 2000);
    }

    void insertGapsTestBigger(void)
    {
        insertGaps(5000, 5000, 20000);
    }

    void insertGapsTestBiggerPlus1(void)
    {
        insertGaps(5001, 5001, 20001);
    }

    void insertGapsTestBiggest(void)
    {
        insertGaps(50000, 50000, 200000);
    }

    void insertGapsTestBiggestPlus1(void)
    {
        insertGaps(50001, 50001, 200001);
    }

    void insertGapsTestSuperbig(void)
    {
        insertGaps(500000, 500000, 2000000);
    }

    void insertGapsTestSuperbigPlus1(void)
    {
        insertGaps(500001, 500001, 2000001);
    }
};

class PartialRecordTest : public BasePartialRecordTest
{
    define_super(BasePartialRecordTest);

public:
    PartialRecordTest()
    :   BasePartialRecordTest("PartialRecordTest")
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

        /* write at offset 50, partial size 50, record size 100 (gap at beginning) */
        BFC_REGISTER_TEST(PartialRecordTest, insertGapsAtBeginningSmall);

        /* write at offset 500, partial size 500, record size 1000 (gap at beginning) */
        BFC_REGISTER_TEST(PartialRecordTest, insertGapsAtBeginningBig);

        /* write at offset 5000, partial size 5000, record size 10000 (gap at beginning) */
        BFC_REGISTER_TEST(PartialRecordTest, insertGapsAtBeginningBigger);

        /* write at offset 5001, partial size 5001, record size 10001 (gap at beginning) */
        BFC_REGISTER_TEST(PartialRecordTest, insertGapsAtBeginningBiggerPlus1);

        /* write at offset 50000, partial size 50000, record size 100000 (gap at beginning) */
        BFC_REGISTER_TEST(PartialRecordTest, insertGapsAtBeginningBiggest);

        /* write at offset 50001, partial size 50001, record size 100001 (gap at beginning) */
        BFC_REGISTER_TEST(PartialRecordTest, insertGapsAtBeginningBiggestPlus1);

        /* write at offset 500000, partial size 500000, record size 1000000 (gap at beginning) */
        BFC_REGISTER_TEST(PartialRecordTest, insertGapsAtBeginningSuperbig);

        /* write at offset 500001, partial size 500001, record size 1000001 (gap at beginning) */
        BFC_REGISTER_TEST(PartialRecordTest, insertGapsAtBeginningSuperbigPlus1);

        /* write at offset 50, partial size 50, record size 200 (gap at
         * beginning AND end) */
        BFC_REGISTER_TEST(PartialRecordTest, insertGapsTestSmall);

        /* write at offset 500, partial size 500, record size 2000 (gap at
         * beginning AND end) */
        BFC_REGISTER_TEST(PartialRecordTest, insertGapsTestBig);

        /* write at offset 5000, partial size 5000, record size 20000 (gap at
         * beginning AND end) */
        BFC_REGISTER_TEST(PartialRecordTest, insertGapsTestBigger);

        /* write at offset 5001, partial size 5001, record size 20001 (gap at
         * beginning AND end) */
        BFC_REGISTER_TEST(PartialRecordTest, insertGapsTestBiggerPlus1);

        /* write at offset 50000, partial size 50000, record size 200000 (gap at
         * beginning AND end) */
        BFC_REGISTER_TEST(PartialRecordTest, insertGapsTestBiggest);

        /* write at offset 50001, partial size 50001, record size 200001 (gap at
         * beginning AND end) */
        BFC_REGISTER_TEST(PartialRecordTest, insertGapsTestBiggestPlus1);

        /* write at offset 500000, partial size 500000, record size 2000000 
         * (gap at beginning AND end) */
        BFC_REGISTER_TEST(PartialRecordTest, insertGapsTestSuperbig);

        /* write at offset 500001, partial size 500001, record size 2000001 
         * (gap at beginning AND end) */
        BFC_REGISTER_TEST(PartialRecordTest, insertGapsTestSuperbigPlus1);
    }

public:
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

class OverwritePartialRecordTest : public BasePartialRecordTest
{
    define_super(BasePartialRecordTest);

public:
    OverwritePartialRecordTest()
    :   BasePartialRecordTest("OverwritePartialRecordTest")
    {
        testrunner::get_instance()->register_fixture(this);
        /* write at offset 0, partial size 50, record size 50 (no gaps) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, simpleInsertTest);

        /* write at offset 0, partial size 50, record size 100 (gap at end) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, insertGapsAtEndTestSmall);

        /* write at offset 0, partial size 500, record size 1000 (gap at end) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, insertGapsAtEndTestBig);

        /* write at offset 0, partial size 5000, record size 10000 (gap at end) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, insertGapsAtEndTestBigger);

        /* write at offset 0, partial size 5001, record size 10001 (gap at end) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, insertGapsAtEndTestBiggerPlus1);

        /* write at offset 0, partial size 50000, record size 100000 (gap at end) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, insertGapsAtEndTestBiggest);

        /* write at offset 0, partial size 50001, record size 100001 (gap at end) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, insertGapsAtEndTestBiggestPlus1);

        /* write at offset 0, partial size 500000, record size 1000000 (gap at end) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, insertGapsAtEndTestSuperbig);

        /* write at offset 0, partial size 500001, record size 1000001 (gap at end) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, insertGapsAtEndTestSuperbigPlus1);

        /* write at offset 50, partial size 50, record size 100 (gap at beginning) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, insertGapsAtBeginningSmall);

        /* write at offset 500, partial size 500, record size 1000 (gap at beginning) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, insertGapsAtBeginningBig);

        /* write at offset 5000, partial size 5000, record size 10000 (gap at beginning) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, insertGapsAtBeginningBigger);

        /* write at offset 5001, partial size 5001, record size 10001 (gap at beginning) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, insertGapsAtBeginningBiggerPlus1);

        /* write at offset 50000, partial size 50000, record size 100000 (gap at beginning) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, insertGapsAtBeginningBiggest);

        /* write at offset 50001, partial size 50001, record size 100001 (gap at beginning) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, insertGapsAtBeginningBiggestPlus1);

        /* write at offset 500000, partial size 500000, record size 1000000 (gap at beginning) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, insertGapsAtBeginningSuperbig);

        /* write at offset 500001, partial size 500001, record size 1000001 (gap at beginning) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, insertGapsAtBeginningSuperbigPlus1);

        /* write at offset 50, partial size 50, record size 200 (gap at
         * beginning AND end) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, insertGapsTestSmall);

        /* write at offset 500, partial size 500, record size 2000 (gap at
         * beginning AND end) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, insertGapsTestBig);

        /* write at offset 5000, partial size 5000, record size 20000 (gap at
         * beginning AND end) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, insertGapsTestBigger);

        /* write at offset 5001, partial size 5001, record size 20001 (gap at
         * beginning AND end) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, insertGapsTestBiggerPlus1);

        /* write at offset 50000, partial size 50000, record size 200000 (gap at
         * beginning AND end) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, insertGapsTestBiggest);

        /* write at offset 50001, partial size 50001, record size 200001 (gap at
         * beginning AND end) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, insertGapsTestBiggestPlus1);

        /* write at offset 500000, partial size 500000, record size 2000000 
         * (gap at beginning AND end) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, insertGapsTestSuperbig);

        /* write at offset 500001, partial size 500001, record size 2000001 
         * (gap at beginning AND end) */
        BFC_REGISTER_TEST(OverwritePartialRecordTest, insertGapsTestSuperbigPlus1);
    }

protected:

public:
    void fillBufferReverse(ham_u8_t *ptr, ham_size_t size)
    {
        for (ham_size_t i=0; i<size; i++)
            ptr[i]=(ham_u8_t)(0xff-i);
    }

    virtual void insertGaps(unsigned partial_offset, 
                    unsigned partial_size, unsigned record_size)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_u8_t *buffer=(ham_u8_t *)malloc(record_size);

        BFC_ASSERT(partial_offset+partial_size<=record_size);

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        /* first: insert a record with a unique pattern and insert this record*/
        fillBufferReverse(&buffer[0], record_size);
        rec.size=record_size;
        rec.data=buffer;
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

        /* then fill the buffer with another pattern and insert the partial
         * record */
        fillBuffer(&buffer[0], 0, record_size);

        rec.partial_offset=partial_offset;
        rec.partial_size=partial_size;
        rec.size=record_size;
        rec.data=buffer;
        BFC_ASSERT_EQUAL(0, 
                ham_insert(m_db, 0, &key, &rec, HAM_PARTIAL|HAM_OVERWRITE));

        /* verify the key */
        memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));

        fillBufferReverse(&buffer[0], record_size);
        fillBuffer(&buffer[partial_offset], 0, partial_size);
        BFC_ASSERT_EQUAL(record_size, rec.size);
        BFC_ASSERT_EQUAL(0, memcmp(buffer, rec.data, rec.size));
        free(buffer);
    }

};

BFC_REGISTER_FIXTURE(PartialRecordTest);
BFC_REGISTER_FIXTURE(OverwritePartialRecordTest);
