/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 * unit tests for mem.h/mem.c
 *
 */

#include <stdexcept>
#include <cppunit/extensions/HelperMacros.h>
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/page.h"
#include "../src/freelist.h"
#include "memtracker.h"

class FreelistTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(FreelistTest);
    CPPUNIT_TEST      (structureTest);
    CPPUNIT_TEST      (markAllocTest);
    CPPUNIT_TEST      (markAllocHighOffsetTest);
    CPPUNIT_TEST      (markAllocRangeTest);
    CPPUNIT_TEST      (markAllocOverflowTest);
    CPPUNIT_TEST      (markAllocOverflow2Test);
    CPPUNIT_TEST      (markAllocOverflow3Test);
    CPPUNIT_TEST      (markAllocAlignTest);
    CPPUNIT_TEST      (markAllocAlignMultipleTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_db_t *m_db;
    memtracker_t *m_alloc;

public:
    void setUp()
    { 
        ham_parameter_t p[]={{HAM_PARAM_PAGESIZE, 4096}, {0, 0}};

        CPPUNIT_ASSERT((m_alloc=memtracker_new())!=0);
        CPPUNIT_ASSERT(ham_new(&m_db)==HAM_SUCCESS);
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        CPPUNIT_ASSERT(ham_create_ex(m_db, ".test", 0, 0644, 
                        &p[0])==HAM_SUCCESS);
    }
    
    void tearDown() 
    { 
        CPPUNIT_ASSERT(ham_close(m_db)==HAM_SUCCESS);
        ham_delete(m_db);
        m_db=0;
        CPPUNIT_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void structureTest(void)
    {
        freelist_t *f;

        f=db_get_freelist(m_db);

        CPPUNIT_ASSERT(freel_get_start_address(f)==db_get_pagesize(m_db));
        freel_set_start_address(f, 0x7878787878787878ull);

        CPPUNIT_ASSERT(freel_get_used_bits(f)==0);
        freel_set_used_bits(f, 13);

        CPPUNIT_ASSERT(freel_get_max_bits(f)!=0);
        CPPUNIT_ASSERT(freel_get_max_bits(f)>>3 <= 
                db_get_usable_pagesize(m_db)-(OFFSET_OF(db_header_t, 
                    _freelist_start)+sizeof(freelist_t)));
        freel_set_max_bits(f, 0x1234);

        CPPUNIT_ASSERT(freel_get_overflow(f)==0ull);
        freel_set_overflow(f, 0x12345678ull);

        CPPUNIT_ASSERT(freel_get_start_address(f)==0x7878787878787878ull);
        CPPUNIT_ASSERT(freel_get_used_bits(f)==13);
        CPPUNIT_ASSERT(freel_get_max_bits(f)==0x1234);
        CPPUNIT_ASSERT(freel_get_overflow(f)==0x12345678ull);

        db_set_dirty(m_db, 1);

        // reopen the database, check if the values were stored correctly
        tearDown();
        CPPUNIT_ASSERT(ham_new(&m_db)==HAM_SUCCESS);
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        CPPUNIT_ASSERT(ham_open(m_db, ".test", 0)==HAM_SUCCESS);
        f=db_get_freelist(m_db);

        CPPUNIT_ASSERT(freel_get_start_address(f)==0x7878787878787878ull);
        CPPUNIT_ASSERT(freel_get_used_bits(f)==13);
        CPPUNIT_ASSERT(freel_get_max_bits(f)==0x1234);
        CPPUNIT_ASSERT(freel_get_overflow(f)==0x12345678ull);
    }

    void markAllocTest(void)
    {
        ham_size_t ps=db_get_pagesize(m_db);

        for (int i=0; i<10; i++) {
            CPPUNIT_ASSERT(freel_mark_free(m_db, ps+i*64, 64)==HAM_SUCCESS);
        }

        for (int i=0; i<10; i++) {
            CPPUNIT_ASSERT(ps+i*64==freel_alloc_area(m_db, 64));
        }

        CPPUNIT_ASSERT(0==freel_alloc_area(m_db, 64));
    }

    void markAllocHighOffsetTest(void)
    {
        ham_size_t ps=db_get_pagesize(m_db);

        for (int i=60; i<70; i++) {
            CPPUNIT_ASSERT(freel_mark_free(m_db, ps+i*64, 64)==HAM_SUCCESS);
        }

        for (int i=60; i<70; i++) {
            CPPUNIT_ASSERT(ps+i*64==freel_alloc_area(m_db, 64));
        }

        CPPUNIT_ASSERT(0==freel_alloc_area(m_db, 64));
    }

    void markAllocRangeTest(void)
    {
        ham_size_t ps=db_get_pagesize(m_db);
        ham_offset_t offset=ps;

        for (int i=60; i<70; i++) {
            CPPUNIT_ASSERT(freel_mark_free(m_db, offset, (i+1)*64)
                    ==HAM_SUCCESS);
            offset+=(i+1)*64;
        }

        offset=ps;
        for (int i=60; i<70; i++) {
            CPPUNIT_ASSERT(offset==freel_alloc_area(m_db, (i+1)*64));
            offset+=(i+1)*64;
        }

        CPPUNIT_ASSERT(0==freel_alloc_area(m_db, 64));
    }

        /* TODO füllen, ham_close; ham_open, alloc */

    void markAllocOverflowTest(void)
    {
        ham_offset_t o;

        o=db_get_usable_pagesize(m_db)*8*DB_CHUNKSIZE;
        CPPUNIT_ASSERT(freel_mark_free(m_db, o, DB_CHUNKSIZE)==HAM_SUCCESS);
        CPPUNIT_ASSERT(freel_alloc_area(m_db, DB_CHUNKSIZE)==o); 
        CPPUNIT_ASSERT(freel_alloc_area(m_db, 64)==0);

        CPPUNIT_ASSERT(freel_mark_free(m_db, o*2, DB_CHUNKSIZE)==HAM_SUCCESS);
        CPPUNIT_ASSERT(freel_alloc_area(m_db, DB_CHUNKSIZE)==o*2); 
        CPPUNIT_ASSERT(freel_alloc_area(m_db, 64)==0);
    }

    void markAllocOverflow2Test(void)
    {
        ham_offset_t o;

        o=db_get_usable_pagesize(m_db)*8*DB_CHUNKSIZE;
        CPPUNIT_ASSERT(freel_mark_free(m_db, 3*o, DB_CHUNKSIZE)==HAM_SUCCESS);
        CPPUNIT_ASSERT(freel_alloc_area(m_db, DB_CHUNKSIZE)==3*o); 
        CPPUNIT_ASSERT(freel_alloc_area(m_db, 64)==0);

        CPPUNIT_ASSERT(freel_mark_free(m_db, 10*o, DB_CHUNKSIZE)==HAM_SUCCESS);
        CPPUNIT_ASSERT(freel_alloc_area(m_db, DB_CHUNKSIZE)==10*o); 
        CPPUNIT_ASSERT(freel_alloc_area(m_db, 64)==0);
    }

    void markAllocOverflow3Test(void)
    {
        // this code snipped crashed in an acceptance test
        CPPUNIT_ASSERT(freel_mark_free(m_db, 2036736, 
                    db_get_pagesize(m_db)-1024)==HAM_SUCCESS);
    }

    void markAllocAlignTest(void)
    {
        ham_size_t ps=db_get_pagesize(m_db);

        CPPUNIT_ASSERT(freel_mark_free(m_db, ps, ps)==HAM_SUCCESS);
        CPPUNIT_ASSERT(freel_alloc_page(m_db)==ps);
        CPPUNIT_ASSERT(freel_alloc_area(m_db, 64)==0);
    }

    void markAllocAlignMultipleTest(void)
    {
        ham_size_t ps=db_get_pagesize(m_db);

        CPPUNIT_ASSERT(freel_mark_free(m_db, ps, ps*3)==HAM_SUCCESS);
        CPPUNIT_ASSERT(freel_alloc_page(m_db)==ps*1);
        CPPUNIT_ASSERT(freel_alloc_page(m_db)==ps*2);
        CPPUNIT_ASSERT(freel_alloc_page(m_db)==ps*3);
        CPPUNIT_ASSERT(freel_alloc_area(m_db, 64)==0);
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(FreelistTest);

