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
#include "../src/env.h"
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
    ham_env_t *m_env;
    ham_u32_t m_pagesize;
    memtracker_t *m_alloc;

public:

    virtual ham_status_t open(ham_u32_t flags)
    {
        ham_status_t st=ham_open_ex(m_db, BFC_OPATH(".test"), flags, 0);
        if (st)
            return (st);
        m_env=ham_get_env(m_db);
        return (0);
    }

    virtual void setup() 
    { 
        __super::setup();
        ham_parameter_t p[]={
            {HAM_PARAM_PAGESIZE, m_pagesize}, 
            {0, 0}};

        BFC_ASSERT((m_alloc=memtracker_new())!=0);
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        BFC_ASSERT_EQUAL(0, 
                ham_create_ex(m_db, BFC_OPATH(".test"), 
                    HAM_ENABLE_TRANSACTIONS, 0644, &p[0]));
        m_env=ham_get_env(m_db);
    }
    
    virtual void teardown() 
    { 
        __super::teardown();

        /* need to clear the changeset, otherwise ham_close() will complain */
        if (m_env)
            ((Environment *)m_env)->get_changeset().clear();

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        m_db=0;
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void structureTest(void)
    {
        freelist_payload_t *f;

        f=(((Environment *)m_env)->get_freelist());

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

        // reopen the database, check if the values were stored correctly
        ((Environment *)m_env)->set_dirty();
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        m_db=0;
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));

        BFC_ASSERT(ham_new(&m_db)==HAM_SUCCESS);
        BFC_ASSERT_EQUAL(0, open(0));
        f=(((Environment *)m_env)->get_freelist());

        BFC_ASSERT(freel_get_start_address(f)==0x7878787878787878ull);
        BFC_ASSERT(freel_get_allocated_bits16(f)==13);
        BFC_ASSERT(freel_get_max_bits16(f)==0x1234);
        BFC_ASSERT(freel_get_overflow(f)==0x12345678ull);
    }

    void markAllocPageTest(void)
    {
        ham_size_t ps=((Environment *)m_env)->get_pagesize();
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

        for (int i=0; i<10; i++) {
            BFC_ASSERT_EQUAL(0, 
                    freel_mark_free((Environment *)m_env, (Database *)m_db, 
                            ps+i*DB_CHUNKSIZE, DB_CHUNKSIZE, HAM_FALSE));
        }

        for (int i=0; i<10; i++) {
            ham_offset_t o;
            BFC_ASSERT_EQUAL(0, 
                    freel_alloc_area(&o, (Environment *)m_env, 
                            (Database *)m_db, DB_CHUNKSIZE));
            BFC_ASSERT_EQUAL((ham_offset_t)(ps+i*DB_CHUNKSIZE), o);
        }

        ham_offset_t o;
        BFC_ASSERT_EQUAL(0,
                freel_alloc_area(&o, (Environment *)m_env, 
                            (Database *)m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL((ham_offset_t)0, o);
        BFC_ASSERT(((Environment *)m_env)->is_dirty());
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void markAllocAlignedTest(void)
    {
        ham_size_t ps=((Environment *)m_env)->get_pagesize();
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

        BFC_ASSERT_EQUAL(0, 
                freel_mark_free((Environment *)m_env, (Database *)m_db, 
                            ps, ps, HAM_FALSE));
        ham_offset_t o;
        BFC_ASSERT_EQUAL(0, freel_alloc_page(&o, (Environment *)m_env, 
                            (Database *)m_db));
        BFC_ASSERT_EQUAL((ham_offset_t)ps, o);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void markAllocHighOffsetTest(void)
    {
        ham_size_t ps=((Environment *)m_env)->get_pagesize();
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

        for (int i=60; i<70; i++) {
            BFC_ASSERT_EQUAL(0, 
                    freel_mark_free((Environment *)m_env, (Database *)m_db,
                            ps+i*DB_CHUNKSIZE, DB_CHUNKSIZE, HAM_FALSE));
        }

        for (int i=60; i<70; i++) {
            ham_offset_t o;
            BFC_ASSERT_EQUAL(0, 
                    freel_alloc_area(&o, (Environment *)m_env, 
                            (Database *)m_db, DB_CHUNKSIZE));
            BFC_ASSERT_EQUAL((ham_offset_t)ps+i*DB_CHUNKSIZE, o);
        }

        ham_offset_t o;
        BFC_ASSERT_EQUAL(0, 
                    freel_alloc_area(&o, (Environment *)m_env, 
                            (Database *)m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL((ham_offset_t)0, o);
        BFC_ASSERT(((Environment *)m_env)->is_dirty());
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void markAllocRangeTest(void)
    {
        ham_size_t ps=((Environment *)m_env)->get_pagesize();
        ham_offset_t offset=ps;
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

        for (int i=60; i<70; i++) {
            BFC_ASSERT_EQUAL_I(0, freel_mark_free((Environment *)m_env, (Database *)m_db, offset, 
                        (i+1)*DB_CHUNKSIZE, HAM_FALSE), i);
            offset+=(i+1)*DB_CHUNKSIZE;
        }

        offset=ps;
        for (int i=60; i<70; i++) {
            ham_offset_t o;
            BFC_ASSERT_EQUAL(0, 
                    freel_alloc_area(&o, (Environment *)m_env, (Database *)m_db, (i+1)*DB_CHUNKSIZE));
            BFC_ASSERT_EQUAL((ham_offset_t)offset, o);
            offset+=(i+1)*DB_CHUNKSIZE;
        }

        ham_offset_t o;
        BFC_ASSERT_EQUAL(0, freel_alloc_area(&o, (Environment *)m_env, (Database *)m_db, DB_CHUNKSIZE));
        BFC_ASSERT(((Environment *)m_env)->is_dirty());
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void markAllocOverflowTest(void)
    {
        ham_offset_t o=((Environment *)m_env)->get_usable_pagesize()*8*DB_CHUNKSIZE;
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

        BFC_ASSERT_EQUAL(0, 
                freel_mark_free((Environment *)m_env, (Database *)m_db, o, DB_CHUNKSIZE, HAM_FALSE));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));

        /* need to clear the changeset, otherwise ham_close() will complain */
        ((Environment *)m_env)->get_changeset().clear();
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, open(HAM_ENABLE_TRANSACTIONS));
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

        ham_offset_t addr;
        BFC_ASSERT_EQUAL(0, 
                freel_alloc_area(&addr, (Environment *)m_env, (Database *)m_db, DB_CHUNKSIZE)); 
        BFC_ASSERT_EQUAL(o, addr);
        BFC_ASSERT_EQUAL(0, 
                freel_alloc_area(&addr, (Environment *)m_env, (Database *)m_db, DB_CHUNKSIZE)); 
        BFC_ASSERT_EQUAL((ham_offset_t)0, addr);

        BFC_ASSERT_EQUAL(0, 
                freel_mark_free((Environment *)m_env, (Database *)m_db, o*2, DB_CHUNKSIZE, HAM_FALSE));

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
        /* need to clear the changeset, otherwise ham_close() will complain */
        ((Environment *)m_env)->get_changeset().clear();
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, open(HAM_ENABLE_TRANSACTIONS));
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

        BFC_ASSERT_EQUAL(0,
                freel_alloc_area(&addr, (Environment *)m_env, (Database *)m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL(o*2, addr);
        BFC_ASSERT_EQUAL(0,
                freel_alloc_area(&addr, (Environment *)m_env, (Database *)m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL((ham_offset_t)0, addr);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void markAllocOverflow2Test(void)
    {
        ham_offset_t o=((Environment *)m_env)->get_usable_pagesize()*8*DB_CHUNKSIZE;
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

        BFC_ASSERT_EQUAL(0, 
                freel_mark_free((Environment *)m_env, (Database *)m_db, 3*o, DB_CHUNKSIZE, HAM_FALSE));
        /*
         * The hinters must be disabled for this test to succeed; at least
         * they need to be instructed to kick in late.
         */
        ((Database *)m_db)->set_data_access_mode( 
                ((Database *)m_db)->get_data_access_mode() &
                        ~(HAM_DAM_SEQUENTIAL_INSERT|HAM_DAM_RANDOM_WRITE));

        ham_offset_t addr;
        BFC_ASSERT_EQUAL(0,
                freel_alloc_area(&addr, (Environment *)m_env, (Database *)m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL(3*o, addr);
        BFC_ASSERT(((Environment *)m_env)->is_dirty());

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
        /* need to clear the changeset, otherwise ham_close() will complain */
        ((Environment *)m_env)->get_changeset().clear();
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, open(HAM_ENABLE_TRANSACTIONS));
        ((Database *)m_db)->set_data_access_mode( 
                ((Database *)m_db)->get_data_access_mode() &
                        ~(HAM_DAM_SEQUENTIAL_INSERT|HAM_DAM_RANDOM_WRITE));
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

        BFC_ASSERT_EQUAL(0,
                freel_alloc_area(&addr, (Environment *)m_env, (Database *)m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL((ham_offset_t)0, addr);

        BFC_ASSERT_EQUAL(0, 
                freel_mark_free((Environment *)m_env, (Database *)m_db, 10*o, DB_CHUNKSIZE, HAM_FALSE));
        BFC_ASSERT_EQUAL(0,
                freel_alloc_area(&addr, (Environment *)m_env, (Database *)m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL(10*o, addr);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
        /* need to clear the changeset, otherwise ham_close() will complain */
        ((Environment *)m_env)->get_changeset().clear();
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, open(HAM_ENABLE_TRANSACTIONS));
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

        BFC_ASSERT_EQUAL(0,
                freel_alloc_area(&addr, (Environment *)m_env, (Database *)m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL((ham_offset_t)0, addr); 
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void markAllocOverflow4Test(void)
    {
        ham_offset_t o=(ham_offset_t)1024*1024*1024*4;
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

        BFC_ASSERT_EQUAL(0, 
                freel_mark_free((Environment *)m_env, (Database *)m_db, o, DB_CHUNKSIZE*3, HAM_FALSE));
        /*
         * The hinters must be disabled for this test to succeed; at least
         * they need to be instructed to kick in late.
         */
        ((Database *)m_db)->set_data_access_mode( 
                ((Database *)m_db)->get_data_access_mode() &
                        ~(HAM_DAM_SEQUENTIAL_INSERT|HAM_DAM_RANDOM_WRITE));
        /*
         * and since we'll be having about 33027 freelist entries in the list, 
         * the hinters will make a ruckus anyhow; the only way to get a hit 
         * on the alloc is either through luck (which would take multiple 
         * rounds as the hinters will drive the free space search using 
         * SRNG technology, but it _is_ deterministic, so we could test for 
         * that; however, I'm lazy so I'll just set a special 'impossible mode'
         * to disable the hinters entirely.
         */
        ((Database *)m_db)->set_data_access_mode( 
                ((Database *)m_db)->get_data_access_mode() 
                        | HAM_DAM_RANDOM_WRITE | HAM_DAM_SEQUENTIAL_INSERT);

        ham_offset_t addr;
        BFC_ASSERT_EQUAL(0, 
                freel_alloc_area(&addr, (Environment *)m_env, (Database *)m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL(o, addr);
        BFC_ASSERT(((Environment *)m_env)->is_dirty());
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));

        /* need to clear the changeset, otherwise ham_close() will complain */
        ((Environment *)m_env)->get_changeset().clear();
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, open(HAM_ENABLE_TRANSACTIONS));

        /* set DAM - see above */
        ((Database *)m_db)->set_data_access_mode(
                ((Database *)m_db)->get_data_access_mode() & 
                        ~(HAM_DAM_SEQUENTIAL_INSERT|HAM_DAM_RANDOM_WRITE));
        ((Database *)m_db)->set_data_access_mode( 
                ((Database *)m_db)->get_data_access_mode()
                        | HAM_DAM_RANDOM_WRITE | HAM_DAM_SEQUENTIAL_INSERT);

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

        BFC_ASSERT_EQUAL(0,
                freel_alloc_area(&addr, (Environment *)m_env, (Database *)m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL((ham_offset_t)o+DB_CHUNKSIZE, addr);
        BFC_ASSERT_EQUAL(0,
                freel_alloc_area(&addr, (Environment *)m_env, (Database *)m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL((ham_offset_t)o+DB_CHUNKSIZE*2, addr);

        BFC_ASSERT_EQUAL(0, 
                freel_mark_free((Environment *)m_env, (Database *)m_db, o, DB_CHUNKSIZE*2, HAM_FALSE));
        BFC_ASSERT_EQUAL(0, 
                freel_alloc_area(&addr, (Environment *)m_env, (Database *)m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL(o, addr);

        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
        /* need to clear the changeset, otherwise ham_close() will complain */
        ((Environment *)m_env)->get_changeset().clear();
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, open(HAM_ENABLE_TRANSACTIONS));
        /* set DAM - see above */
        ((Database *)m_db)->set_data_access_mode(
                ((Database *)m_db)->get_data_access_mode() & 
                        ~(HAM_DAM_SEQUENTIAL_INSERT|HAM_DAM_RANDOM_WRITE));
        ((Database *)m_db)->set_data_access_mode( 
                ((Database *)m_db)->get_data_access_mode()
                        | HAM_DAM_RANDOM_WRITE | HAM_DAM_SEQUENTIAL_INSERT);
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

        BFC_ASSERT_EQUAL(0,
                freel_alloc_area(&addr, (Environment *)m_env, (Database *)m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL(o+DB_CHUNKSIZE, addr);
        BFC_ASSERT_EQUAL(0, 
                freel_alloc_area(&addr, (Environment *)m_env, (Database *)m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL((ham_offset_t)0, addr);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void markAllocOverflow3Test(void)
    {
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));
        // this code snippet crashed in an acceptance test
        BFC_ASSERT_EQUAL(0, freel_mark_free((Environment *)m_env, (Database *)m_db, 2036736, 
                    ((Environment *)m_env)->get_pagesize()-1024, HAM_FALSE));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void markAllocAlignTest(void)
    {
        ham_offset_t addr;
        ham_size_t ps=((Environment *)m_env)->get_pagesize();
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

        BFC_ASSERT_EQUAL(0, 
                freel_mark_free((Environment *)m_env, (Database *)m_db, ps, ps, HAM_FALSE));
        BFC_ASSERT_EQUAL(0,
                freel_alloc_page(&addr, (Environment *)m_env, (Database *)m_db));
        BFC_ASSERT_EQUAL(ps, addr);
        BFC_ASSERT_EQUAL(0,
                freel_alloc_area(&addr, (Environment *)m_env, (Database *)m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL(0ull, addr);
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
    }

    void markAllocAlignMultipleTest(void)
    {
        ham_offset_t addr;
        ham_size_t ps=((Environment *)m_env)->get_pagesize();
        ham_txn_t *txn;
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_env, 0, 0, 0));

        BFC_ASSERT_EQUAL(0, 
                freel_mark_free((Environment *)m_env, (Database *)m_db, ps, ps*2, HAM_FALSE));
        BFC_ASSERT_EQUAL(0, 
                freel_alloc_page(&addr, (Environment *)m_env, (Database *)m_db));
        BFC_ASSERT_EQUAL((ham_u64_t)ps*1, addr);
        BFC_ASSERT_EQUAL(0, 
                freel_alloc_page(&addr, (Environment *)m_env, (Database *)m_db));
        BFC_ASSERT_EQUAL((ham_u64_t)ps*2, addr);
        BFC_ASSERT_EQUAL(0,
                freel_alloc_area(&addr, (Environment *)m_env, (Database *)m_db, DB_CHUNKSIZE));
        BFC_ASSERT_EQUAL((ham_u64_t)0, addr);
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

        ((Database *)m_db)->set_data_access_mode(
                ((Database *)m_db)->get_data_access_mode() |
                    HAM_DAM_ENFORCE_PRE110_FORMAT);
    }

    virtual ham_status_t open(ham_u32_t flags)
    {
        ham_status_t st=ham_open_ex(m_db, BFC_OPATH(".test"), flags, 0);
        if (st==0)
            ((Database *)m_db)->set_data_access_mode(
                ((Database *)m_db)->get_data_access_mode()
                    |HAM_DAM_ENFORCE_PRE110_FORMAT);
        m_env=ham_get_env(m_db);
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

BFC_REGISTER_FIXTURE(FreelistV1Test);
BFC_REGISTER_FIXTURE(FreelistV2Test);

