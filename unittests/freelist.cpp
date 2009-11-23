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
#include "hamster_fixture.hpp"

using namespace bfc;

class FreelistBaseTest : public hamsterDB_fixture
{
	define_super(hamsterDB_fixture);

public:
    FreelistBaseTest(const char *name, unsigned pagesize=4096)
    :   hamsterDB_fixture(name)
    {
        m_pagesize=pagesize;
    }

protected:
    ham_db_t *m_db;
    ham_u32_t m_pagesize;
    memtracker_t *m_alloc;

public:

    virtual ham_status_t open(ham_u32_t flags)
    {
        return (ham_open_ex(m_db, BFC_OPATH(".test"), flags, 0));
    }

    virtual void setup() 
	{ 
		__super::setup();
        ham_parameter_t p[]={
            {HAM_PARAM_PAGESIZE, m_pagesize}, 
            {0, 0}};

        BFC_ASSERT((m_alloc=memtracker_new())!=0);
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        BFC_ASSERT_EQUAL(0, 
                ham_create_ex(m_db, BFC_OPATH(".test"), 
                    HAM_ENABLE_TRANSACTIONS, 0644, &p[0]));
    }
    
    virtual void teardown() 
	{ 
		__super::teardown();

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        m_db=0;
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void structureTest(void)
    {
        freelist_payload_t *f;

        f=db_get_freelist(m_db);

        BFC_ASSERT(freel_get_allocated_bits16(f)==0);
        freel_set_allocated_bits16(f, 13);

        BFC_ASSERT(freel_get_max_bits16(f)==0);
        freel_set_max_bits16(f, 0x1234);

        BFC_ASSERT(freel_get_overflow(f)==0ull);
        freel_set_overflow(f, 0x12345678ull);

        freel_set_start_address(f, 0x7878787878787878ull);
        BFC_ASSERT(freel_get_start_address(f)==0x7878787878787878ull);
        BFC_ASSERT(freel_get_allocated_bits16(f)==13);
        BFC_ASSERT(freel_get_max_bits16(f)==0x1234);
        BFC_ASSERT(freel_get_overflow(f)==0x12345678ull);

        db_set_dirty(m_db);

        // reopen the database, check if the values were stored correctly
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        m_db=0;
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));

        BFC_ASSERT(ham_new(&m_db)==HAM_SUCCESS);
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        BFC_ASSERT_EQUAL(0, open(0));
        f=db_get_freelist(m_db);

        BFC_ASSERT(freel_get_start_address(f)==0x7878787878787878ull);
        BFC_ASSERT(freel_get_allocated_bits16(f)==13);
        BFC_ASSERT(freel_get_max_bits16(f)==0x1234);
        BFC_ASSERT(freel_get_overflow(f)==0x12345678ull);
    }

    void markAllocPageTest(void)
    {
        ham_size_t ps=db_get_pagesize(m_db);
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

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
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void markAllocAlignedTest(void)
    {
        ham_size_t ps=db_get_pagesize(m_db);
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL(0, 
                    freel_mark_free(m_db, ps, ps, HAM_FALSE));
        BFC_ASSERT_EQUAL((ham_offset_t)ps, freel_alloc_page(m_db));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void markAllocHighOffsetTest(void)
    {
        ham_size_t ps=db_get_pagesize(m_db);
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

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
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void markAllocRangeTest(void)
    {
        ham_size_t ps=db_get_pagesize(m_db);
        ham_offset_t offset=ps;
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        for (int i=60; i<70; i++) {
            BFC_ASSERT_EQUAL_I(0, freel_mark_free(m_db, offset, 
                        (i+1)*DB_CHUNKSIZE, HAM_FALSE), i);
            offset+=(i+1)*DB_CHUNKSIZE;
        }

        offset=ps;
        for (int i=60; i<70; i++) {
            BFC_ASSERT_I(offset==freel_alloc_area(m_db, (i+1)*DB_CHUNKSIZE), i);
            offset+=(i+1)*DB_CHUNKSIZE;
        }

        BFC_ASSERT(0==freel_alloc_area(m_db, DB_CHUNKSIZE));
        BFC_ASSERT(db_is_dirty(m_db));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void markAllocOverflowTest(void)
    {
        ham_offset_t o=db_get_usable_pagesize(m_db)*8*DB_CHUNKSIZE;
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL(0, 
                freel_mark_free(m_db, o, DB_CHUNKSIZE, HAM_FALSE));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, open(HAM_ENABLE_TRANSACTIONS));
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL(o, 
                freel_alloc_area(m_db, DB_CHUNKSIZE)); 
        BFC_ASSERT_EQUAL((ham_offset_t)0, 
                freel_alloc_area(m_db, DB_CHUNKSIZE));

        BFC_ASSERT_EQUAL(0, 
                freel_mark_free(m_db, o*2, DB_CHUNKSIZE, HAM_FALSE));

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, open(HAM_ENABLE_TRANSACTIONS));
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL(o*2,
                freel_alloc_area(m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL((ham_offset_t)0,
                freel_alloc_area(m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void markAllocOverflow2Test(void)
    {
        ham_offset_t o=db_get_usable_pagesize(m_db)*8*DB_CHUNKSIZE;
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL(0, 
                freel_mark_free(m_db, 3*o, DB_CHUNKSIZE, HAM_FALSE));
		/*
		 * The hinters must be disabled for this test to succeed; at least
		 * they need to be instructed to kick in late.
		 */
		db_set_data_access_mode(m_db, 
				db_get_data_access_mode(m_db) & 
						~(HAM_DAM_SEQUENTIAL_INSERT
						 | HAM_DAM_RANDOM_WRITE
						 | HAM_DAM_FAST_INSERT));

        BFC_ASSERT_EQUAL(3*o, 
                freel_alloc_area(m_db, DB_CHUNKSIZE));
        BFC_ASSERT(db_is_dirty(m_db));

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, open(HAM_ENABLE_TRANSACTIONS));
		db_set_data_access_mode(m_db, 
				db_get_data_access_mode(m_db) & 
						~(HAM_DAM_SEQUENTIAL_INSERT
						 | HAM_DAM_RANDOM_WRITE
						 | HAM_DAM_FAST_INSERT));
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL((ham_offset_t)0, 
                freel_alloc_area(m_db, DB_CHUNKSIZE));

        BFC_ASSERT_EQUAL(0, 
                freel_mark_free(m_db, 10*o, DB_CHUNKSIZE, HAM_FALSE));
        BFC_ASSERT_EQUAL(10*o, 
                freel_alloc_area(m_db, DB_CHUNKSIZE));

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, open(HAM_ENABLE_TRANSACTIONS));
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL((ham_offset_t)0, 
                freel_alloc_area(m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void markAllocOverflow4Test(void)
    {
        ham_offset_t o=(ham_offset_t)1024*1024*1024*4;
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL(0, 
                freel_mark_free(m_db, o, DB_CHUNKSIZE*3, HAM_FALSE));
		/*
		 * The hinters must be disabled for this test to succeed; at least
		 * they need to be instructed to kick in late.
		 */
		db_set_data_access_mode(m_db, 
				db_get_data_access_mode(m_db) & 
						~(HAM_DAM_SEQUENTIAL_INSERT
						 | HAM_DAM_RANDOM_WRITE
						 | HAM_DAM_FAST_INSERT));
		/*
		 * and since we'll be having about 33027 freelist entries in the list, 
         * the hinters will make a ruckus anyhow; the only way to get a hit 
         * on the alloc is either through luck (which would take multiple 
         * rounds as the hinters will drive the free space search using 
         * SRNG technology, but it _is_ deterministic, so we could test for 
         * that; however, I'm lazy so I'll just set a special 'impossible mode'
		 * to disable the hinters entirely.
		 */
		db_set_data_access_mode(m_db, 
				db_get_data_access_mode(m_db) 
						| HAM_DAM_RANDOM_WRITE 
                        | HAM_DAM_SEQUENTIAL_INSERT);

        BFC_ASSERT_EQUAL(o, 
                freel_alloc_area(m_db, DB_CHUNKSIZE));
        BFC_ASSERT(db_is_dirty(m_db));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, open(HAM_ENABLE_TRANSACTIONS));

        /* set DAM - see above */
		db_set_data_access_mode(m_db, 
				db_get_data_access_mode(m_db) & 
						~(HAM_DAM_SEQUENTIAL_INSERT
						 | HAM_DAM_RANDOM_WRITE
						 | HAM_DAM_FAST_INSERT));
		db_set_data_access_mode(m_db, 
				db_get_data_access_mode(m_db) 
						| HAM_DAM_RANDOM_WRITE 
                        | HAM_DAM_SEQUENTIAL_INSERT);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL((ham_offset_t)o+DB_CHUNKSIZE, 
                freel_alloc_area(m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL((ham_offset_t)o+DB_CHUNKSIZE*2,
                freel_alloc_area(m_db, DB_CHUNKSIZE));

        BFC_ASSERT_EQUAL(0, 
                freel_mark_free(m_db, o, DB_CHUNKSIZE*2, HAM_FALSE));
        BFC_ASSERT_EQUAL(o, 
                freel_alloc_area(m_db, DB_CHUNKSIZE));

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, open(HAM_ENABLE_TRANSACTIONS));
        /* set DAM - see above */
		db_set_data_access_mode(m_db, 
				db_get_data_access_mode(m_db) & 
						~(HAM_DAM_SEQUENTIAL_INSERT
						 | HAM_DAM_RANDOM_WRITE
						 | HAM_DAM_FAST_INSERT));
		db_set_data_access_mode(m_db, 
				db_get_data_access_mode(m_db) 
						| HAM_DAM_RANDOM_WRITE 
                        | HAM_DAM_SEQUENTIAL_INSERT);
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL(o+DB_CHUNKSIZE, 
                freel_alloc_area(m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL((ham_offset_t)0, 
                freel_alloc_area(m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void markAllocOverflow3Test(void)
    {
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        // this code snippet crashed in an acceptance test
        BFC_ASSERT_EQUAL(0, freel_mark_free(m_db, 2036736, 
                    db_get_pagesize(m_db)-1024, HAM_FALSE));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void markAllocAlignTest(void)
    {
        ham_size_t ps=db_get_pagesize(m_db);
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        BFC_ASSERT(freel_mark_free(m_db, ps, ps, HAM_FALSE)==HAM_SUCCESS);
        BFC_ASSERT(freel_alloc_page(m_db)==ps);
        BFC_ASSERT(freel_alloc_area(m_db, DB_CHUNKSIZE)==0);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void markAllocAlignMultipleTest(void)
    {
        ham_size_t ps=db_get_pagesize(m_db);
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL(0, freel_mark_free(m_db, ps, ps*2, HAM_FALSE));
        BFC_ASSERT_EQUAL((ham_u64_t)ps*1, freel_alloc_page(m_db));
        BFC_ASSERT_EQUAL((ham_u64_t)ps*2, freel_alloc_page(m_db));
        BFC_ASSERT_EQUAL((ham_u64_t)0, 
                freel_alloc_area(m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

	// using a function to compare the constants is easier for debugging
	bool compare_sizes(size_t a, size_t b)
	{
		return a == b;
	}

	void checkStructurePackingTest(void)
    {
		// checks to make sure structure packing by the compiler is still okay
		BFC_ASSERT(compare_sizes(sizeof(freelist_payload_t), 
                16 + 13 + sizeof(freelist_page_statistics_t)));
		freelist_payload_t f;
		BFC_ASSERT(compare_sizes(sizeof(f._s._s16), 5));
		BFC_ASSERT(compare_sizes(OFFSETOF(freelist_payload_t, _s._s16), 16));
		BFC_ASSERT(compare_sizes(OFFSETOF(freelist_payload_t, _s._s16._bitmap),
                16 + 4));
		BFC_ASSERT(compare_sizes(db_get_freelist_header_size16(), 16 + 4));
		BFC_ASSERT(compare_sizes(db_get_freelist_header_size32(), 
                16 + 12 + sizeof(freelist_page_statistics_t)));
		BFC_ASSERT(compare_sizes(sizeof(freelist_page_statistics_t), 
                8*4 + sizeof(freelist_slotsize_stats_t)
                        *HAM_FREELIST_SLOT_SPREAD));
		BFC_ASSERT(compare_sizes(sizeof(freelist_slotsize_stats_t), 8*4));
		BFC_ASSERT(compare_sizes(HAM_FREELIST_SLOT_SPREAD, 16-5+1));
    }

};

class FreelistV1Test : public FreelistBaseTest
{
	define_super(FreelistBaseTest);

public:
    FreelistV1Test()
    :   FreelistBaseTest("FreelistV1Test")
    {
        testrunner::get_instance()->register_fixture(this);
		BFC_REGISTER_TEST(FreelistV1Test, checkStructurePackingTest);
        BFC_REGISTER_TEST(FreelistV1Test, structureTest);
        BFC_REGISTER_TEST(FreelistV1Test, markAllocAlignedTest);
        BFC_REGISTER_TEST(FreelistV1Test, markAllocPageTest);
        BFC_REGISTER_TEST(FreelistV1Test, markAllocHighOffsetTest);
        BFC_REGISTER_TEST(FreelistV1Test, markAllocRangeTest);
        BFC_REGISTER_TEST(FreelistV1Test, markAllocOverflowTest);
        BFC_REGISTER_TEST(FreelistV1Test, markAllocOverflow2Test);
        BFC_REGISTER_TEST(FreelistV1Test, markAllocOverflow3Test);
        BFC_REGISTER_TEST(FreelistV1Test, markAllocOverflow4Test);
        BFC_REGISTER_TEST(FreelistV1Test, markAllocAlignTest);
        BFC_REGISTER_TEST(FreelistV1Test, markAllocAlignMultipleTest);
    }

    virtual void setup() 
	{ 
		__super::setup();

		db_set_data_access_mode(m_db, 
				db_get_data_access_mode(m_db)|HAM_DAM_ENFORCE_PRE110_FORMAT);
    }

    virtual ham_status_t open(ham_u32_t flags)
    {
        ham_status_t st=ham_open_ex(m_db, BFC_OPATH(".test"), flags, 0);
        if (st==0)
		    db_set_data_access_mode(m_db, 
				db_get_data_access_mode(m_db)|HAM_DAM_ENFORCE_PRE110_FORMAT);
        return (st);
    }
};

class FreelistV2Test : public FreelistBaseTest
{
	define_super(FreelistBaseTest);

public:
    FreelistV2Test()
    :   FreelistBaseTest("FreelistV2Test")
    {
        testrunner::get_instance()->register_fixture(this);
		BFC_REGISTER_TEST(FreelistV2Test, checkStructurePackingTest);
        BFC_REGISTER_TEST(FreelistV2Test, structureTest);
        BFC_REGISTER_TEST(FreelistV2Test, markAllocAlignedTest);
        BFC_REGISTER_TEST(FreelistV2Test, markAllocPageTest);
        BFC_REGISTER_TEST(FreelistV2Test, markAllocHighOffsetTest);
        BFC_REGISTER_TEST(FreelistV2Test, markAllocRangeTest);
        BFC_REGISTER_TEST(FreelistV2Test, markAllocOverflowTest);
        BFC_REGISTER_TEST(FreelistV2Test, markAllocOverflow2Test);
        BFC_REGISTER_TEST(FreelistV2Test, markAllocOverflow3Test);
        BFC_REGISTER_TEST(FreelistV2Test, markAllocOverflow4Test);
        BFC_REGISTER_TEST(FreelistV2Test, markAllocAlignTest);
        BFC_REGISTER_TEST(FreelistV2Test, markAllocAlignMultipleTest);
    }
};

class FreelistV2Pagesize3072Test : public FreelistBaseTest
{
	define_super(FreelistBaseTest);

public:
    FreelistV2Pagesize3072Test()
    :   FreelistBaseTest("FreelistV2Pagesize3072Test", 3072)
    {
        testrunner::get_instance()->register_fixture(this);
		BFC_REGISTER_TEST(FreelistV2Pagesize3072Test, checkStructurePackingTest);
        BFC_REGISTER_TEST(FreelistV2Pagesize3072Test, structureTest);
        BFC_REGISTER_TEST(FreelistV2Pagesize3072Test, markAllocAlignedTest);
        BFC_REGISTER_TEST(FreelistV2Pagesize3072Test, markAllocPageTest);
        BFC_REGISTER_TEST(FreelistV2Pagesize3072Test, markAllocHighOffsetTest);
        BFC_REGISTER_TEST(FreelistV2Pagesize3072Test, markAllocRangeTest);
        BFC_REGISTER_TEST(FreelistV2Pagesize3072Test, markAllocOverflowTest);
        BFC_REGISTER_TEST(FreelistV2Pagesize3072Test, markAllocOverflow2Test);
        BFC_REGISTER_TEST(FreelistV2Pagesize3072Test, markAllocOverflow3Test);
        BFC_REGISTER_TEST(FreelistV2Pagesize3072Test, markAllocOverflow4Test);
        BFC_REGISTER_TEST(FreelistV2Pagesize3072Test, markAllocAlignTest);
        BFC_REGISTER_TEST(FreelistV2Pagesize3072Test, markAllocAlignMultipleTest);
    }
};

BFC_REGISTER_FIXTURE(FreelistV1Test);
BFC_REGISTER_FIXTURE(FreelistV2Test);
BFC_REGISTER_FIXTURE(FreelistV2Pagesize3072Test);

