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
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/page.h"
#include "../src/freelist.h"
#include "memtracker.h"

#include "bfc-testsuite.hpp"

using namespace bfc;

class FreelistTest : public fixture
{
public:
    FreelistTest()
    :   fixture("FreelistTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(FreelistTest, structureTest);
        BFC_REGISTER_TEST(FreelistTest, markAllocAlignedTest);
        BFC_REGISTER_TEST(FreelistTest, markAllocPageTest);
        BFC_REGISTER_TEST(FreelistTest, markAllocHighOffsetTest);
        BFC_REGISTER_TEST(FreelistTest, markAllocRangeTest);
        BFC_REGISTER_TEST(FreelistTest, markAllocOverflowTest);
        BFC_REGISTER_TEST(FreelistTest, markAllocOverflow2Test);
        BFC_REGISTER_TEST(FreelistTest, markAllocOverflow3Test);
        BFC_REGISTER_TEST(FreelistTest, markAllocOverflow4Test);
        BFC_REGISTER_TEST(FreelistTest, markAllocAlignTest);
        BFC_REGISTER_TEST(FreelistTest, markAllocAlignMultipleTest);
    }

protected:
    ham_db_t *m_db;
    memtracker_t *m_alloc;

public:
    void setup()
    { 
        ham_parameter_t p[]={{HAM_PARAM_PAGESIZE, 4096}, {0, 0}};

        BFC_ASSERT((m_alloc=memtracker_new())!=0);
        BFC_ASSERT(ham_new(&m_db)==HAM_SUCCESS);
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        BFC_ASSERT(ham_create_ex(m_db, BFC_OPATH(".test"), 0, 0644, 
                        &p[0])==HAM_SUCCESS);
    }
    
    void teardown() 
    { 
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        m_db=0;
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void structureTest(void)
    {
        freelist_payload_t *f;

        f=db_get_freelist(m_db);

        BFC_ASSERT(freel_get_allocated_bits(f)==0);
        freel_set_allocated_bits(f, 13);

        BFC_ASSERT(freel_get_max_bits(f)==0);
        freel_set_max_bits(f, 0x1234);

        BFC_ASSERT(freel_get_overflow(f)==0ull);
        freel_set_overflow(f, 0x12345678ull);

        freel_set_start_address(f, 0x7878787878787878ull);
        BFC_ASSERT(freel_get_start_address(f)==0x7878787878787878ull);
        BFC_ASSERT(freel_get_allocated_bits(f)==13);
        BFC_ASSERT(freel_get_max_bits(f)==0x1234);
        BFC_ASSERT(freel_get_overflow(f)==0x12345678ull);

        db_set_dirty(m_db, 1);

        // reopen the database, check if the values were stored correctly
        teardown();
        BFC_ASSERT(ham_new(&m_db)==HAM_SUCCESS);
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        BFC_ASSERT(ham_open(m_db, BFC_OPATH(".test"), 0)==HAM_SUCCESS);
        f=db_get_freelist(m_db);

        BFC_ASSERT(freel_get_start_address(f)==0x7878787878787878ull);
        BFC_ASSERT(freel_get_allocated_bits(f)==13);
        BFC_ASSERT(freel_get_max_bits(f)==0x1234);
        BFC_ASSERT(freel_get_overflow(f)==0x12345678ull);
    }

    void markAllocPageTest(void)
    {
        ham_size_t ps=db_get_pagesize(m_db);
        ham_txn_t txn;
        BFC_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));

        for (int i=0; i<10; i++) {
            BFC_ASSERT_EQUAL(0, 
                    freel_mark_free(m_db, ps+i*DB_CHUNKSIZE, DB_CHUNKSIZE, 
                        HAM_FALSE));
        }

        for (int i=0; i<10; i++) {
            BFC_ASSERT_EQUAL((ham_offset_t)(ps+i*DB_CHUNKSIZE), 
                    freel_alloc_area(m_db, DB_CHUNKSIZE));
        }

        BFC_ASSERT_EQUAL((ham_offset_t)0, 
                freel_alloc_area(m_db, DB_CHUNKSIZE));
        BFC_ASSERT(db_is_dirty(m_db));
        BFC_ASSERT_EQUAL(0, txn_commit(&txn, 0));
    }

    void markAllocAlignedTest(void)
    {
        ham_size_t ps=db_get_pagesize(m_db);
        ham_txn_t txn;
        BFC_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL(0, 
                    freel_mark_free(m_db, ps, ps, HAM_FALSE));
        BFC_ASSERT_EQUAL((ham_offset_t)ps, freel_alloc_page(m_db));
        BFC_ASSERT_EQUAL(0, txn_commit(&txn, 0));
    }

    void markAllocHighOffsetTest(void)
    {
        ham_size_t ps=db_get_pagesize(m_db);
        ham_txn_t txn;
        BFC_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));

        for (int i=60; i<70; i++) {
            BFC_ASSERT_EQUAL(0, freel_mark_free(m_db,
                        ps+i*DB_CHUNKSIZE, DB_CHUNKSIZE, HAM_FALSE));
        }

        for (int i=60; i<70; i++) {
            BFC_ASSERT(ps+i*DB_CHUNKSIZE==freel_alloc_area(m_db, 
                        DB_CHUNKSIZE));
        }

        BFC_ASSERT(0==freel_alloc_area(m_db, DB_CHUNKSIZE));
        BFC_ASSERT(db_is_dirty(m_db));
        BFC_ASSERT_EQUAL(0, txn_commit(&txn, 0));
    }

    void markAllocRangeTest(void)
    {
        ham_size_t ps=db_get_pagesize(m_db);
        ham_offset_t offset=ps;
        ham_txn_t txn;
        BFC_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));

        for (int i=60; i<70; i++) {
            BFC_ASSERT_EQUAL(0, freel_mark_free(m_db, offset, 
                        (i+1)*DB_CHUNKSIZE, HAM_FALSE));
            offset+=(i+1)*DB_CHUNKSIZE;
        }

        offset=ps;
        for (int i=60; i<70; i++) {
            BFC_ASSERT(offset==freel_alloc_area(m_db, (i+1)*DB_CHUNKSIZE));
            offset+=(i+1)*DB_CHUNKSIZE;
        }

        BFC_ASSERT(0==freel_alloc_area(m_db, DB_CHUNKSIZE));
        BFC_ASSERT(db_is_dirty(m_db));
        BFC_ASSERT_EQUAL(0, txn_commit(&txn, 0));
    }

    void markAllocOverflowTest(void)
    {
        ham_offset_t o=db_get_usable_pagesize(m_db)*8*DB_CHUNKSIZE;
        ham_txn_t txn;
        BFC_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL(0, 
                freel_mark_free(m_db, o, DB_CHUNKSIZE, HAM_FALSE));
        BFC_ASSERT_EQUAL(0, txn_commit(&txn, 0));

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), 0));
        BFC_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL(o, 
                freel_alloc_area(m_db, DB_CHUNKSIZE)); 
        BFC_ASSERT_EQUAL((ham_offset_t)0, 
                freel_alloc_area(m_db, DB_CHUNKSIZE));

        BFC_ASSERT_EQUAL(0, 
                freel_mark_free(m_db, o*2, DB_CHUNKSIZE, HAM_FALSE));

        BFC_ASSERT_EQUAL(0, txn_commit(&txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), 0));
        BFC_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL(o*2,
                freel_alloc_area(m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL((ham_offset_t)0,
                freel_alloc_area(m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL(0, txn_commit(&txn, 0));
    }

    void markAllocOverflow2Test(void)
    {
        ham_offset_t o=db_get_usable_pagesize(m_db)*8*DB_CHUNKSIZE;
        ham_txn_t txn;
        BFC_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL(0, 
                freel_mark_free(m_db, 3*o, DB_CHUNKSIZE, HAM_FALSE));
        BFC_ASSERT_EQUAL(3*o, 
                freel_alloc_area(m_db, DB_CHUNKSIZE));
        BFC_ASSERT(db_is_dirty(m_db));

        BFC_ASSERT_EQUAL(0, txn_commit(&txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), 0));
        BFC_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL((ham_offset_t)0, 
                freel_alloc_area(m_db, DB_CHUNKSIZE));

        BFC_ASSERT_EQUAL(0, 
                freel_mark_free(m_db, 10*o, DB_CHUNKSIZE, HAM_FALSE));
        BFC_ASSERT_EQUAL(10*o, 
                freel_alloc_area(m_db, DB_CHUNKSIZE));

        BFC_ASSERT_EQUAL(0, txn_commit(&txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), 0));
        BFC_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL((ham_offset_t)0, 
                freel_alloc_area(m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL(0, txn_commit(&txn, 0));
    }

    void markAllocOverflow4Test(void)
    {
        ham_offset_t o=(ham_offset_t)1024*1024*1024*4;
        ham_txn_t txn;
        BFC_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL(0, 
                freel_mark_free(m_db, o, DB_CHUNKSIZE*3, HAM_FALSE));
        BFC_ASSERT_EQUAL(o, 
                freel_alloc_area(m_db, DB_CHUNKSIZE));
        BFC_ASSERT(db_is_dirty(m_db));
        BFC_ASSERT_EQUAL(0, txn_commit(&txn, 0));

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), 0));
        BFC_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL((ham_offset_t)o+DB_CHUNKSIZE, 
                freel_alloc_area(m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL((ham_offset_t)o+DB_CHUNKSIZE*2,
                freel_alloc_area(m_db, DB_CHUNKSIZE));

        BFC_ASSERT_EQUAL(0, 
                freel_mark_free(m_db, o, DB_CHUNKSIZE*2, HAM_FALSE));
        BFC_ASSERT_EQUAL(o, 
                freel_alloc_area(m_db, DB_CHUNKSIZE));

        BFC_ASSERT_EQUAL(0, txn_commit(&txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), 0));
        BFC_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL(o+DB_CHUNKSIZE, 
                freel_alloc_area(m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL((ham_offset_t)0, 
                freel_alloc_area(m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL(0, txn_commit(&txn, 0));
    }

    void markAllocOverflow3Test(void)
    {
        ham_txn_t txn;
        BFC_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));
        // this code snipped crashed in an acceptance test
        BFC_ASSERT_EQUAL(0, freel_mark_free(m_db, 2036736, 
                    db_get_pagesize(m_db)-1024, HAM_FALSE));
        BFC_ASSERT_EQUAL(0, txn_commit(&txn, 0));
    }

    void markAllocAlignTest(void)
    {
        ham_size_t ps=db_get_pagesize(m_db);
        ham_txn_t txn;
        BFC_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));

        BFC_ASSERT(freel_mark_free(m_db, ps, ps, HAM_FALSE)==HAM_SUCCESS);
        BFC_ASSERT(freel_alloc_page(m_db)==ps);
        BFC_ASSERT(freel_alloc_area(m_db, DB_CHUNKSIZE)==0);
        BFC_ASSERT_EQUAL(0, txn_commit(&txn, 0));
    }

    void markAllocAlignMultipleTest(void)
    {
        ham_size_t ps=db_get_pagesize(m_db);
        ham_txn_t txn;
        BFC_ASSERT_EQUAL(0, txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL(0, freel_mark_free(m_db, ps, ps*2, HAM_FALSE));
        BFC_ASSERT_EQUAL((ham_u64_t)ps*1, freel_alloc_page(m_db));
        BFC_ASSERT_EQUAL((ham_u64_t)ps*2, freel_alloc_page(m_db));
        BFC_ASSERT_EQUAL((ham_u64_t)0, 
                freel_alloc_area(m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL(0, txn_commit(&txn, 0));
    }

};

BFC_REGISTER_FIXTURE(FreelistTest);

