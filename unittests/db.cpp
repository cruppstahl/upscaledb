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
#include "memtracker.h"

class DbTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(DbTest);
    CPPUNIT_TEST      (headerTest);
    CPPUNIT_TEST      (structureTest);
    CPPUNIT_TEST      (defaultCompareTest);
    CPPUNIT_TEST      (defaultPrefixCompareTest);
    CPPUNIT_TEST      (allocPageTest);

/*
    CPPUNIT_TEST      (freePageTest);
    CPPUNIT_TEST      (fetchPageTest);
    CPPUNIT_TEST      (flushPageTest);
    CPPUNIT_TEST      (allocPageStructTest);
    CPPUNIT_TEST      (freePageStructTest);
*/
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_db_t *m_db;
    ham_bool_t m_inmemory;
    ham_device_t *m_dev;
    memtracker_t *m_alloc;

public:
    void setUp()
    { 
        CPPUNIT_ASSERT(0==ham_new(&m_db));
        m_alloc=memtracker_new();
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        db_set_device(m_db, (m_dev=ham_device_new(m_db, m_inmemory)));
        db_set_pagesize(m_db, m_dev->get_pagesize(m_dev));
    }
    
    void tearDown() 
    { 
        ham_delete(m_db);
        CPPUNIT_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void headerTest()
    {
        db_set_magic(m_db, '1', '2', '3', '4');
        CPPUNIT_ASSERT(db_get_magic(m_db, 0)=='1');
        CPPUNIT_ASSERT(db_get_magic(m_db, 1)=='2');
        CPPUNIT_ASSERT(db_get_magic(m_db, 2)=='3');
        CPPUNIT_ASSERT(db_get_magic(m_db, 3)=='4');

        db_set_version(m_db, 1, 2, 3, 4);
        CPPUNIT_ASSERT(db_get_version(m_db, 0)==1);
        CPPUNIT_ASSERT(db_get_version(m_db, 1)==2);
        CPPUNIT_ASSERT(db_get_version(m_db, 2)==3);
        CPPUNIT_ASSERT(db_get_version(m_db, 3)==4);

        db_set_serialno(m_db, 0x1234);
        CPPUNIT_ASSERT(db_get_serialno(m_db)==0x1234);

        db_set_keysize(m_db, 12);
        CPPUNIT_ASSERT(db_get_keysize(m_db)==12);

        db_set_pagesize(m_db, 1024*64);
        CPPUNIT_ASSERT(db_get_pagesize(m_db)==1024*64);

        db_set_pers_flags(m_db, 12);
        CPPUNIT_ASSERT(db_get_pers_flags(m_db)==12);

        db_set_txn(m_db, (ham_txn_t *)13);
        CPPUNIT_ASSERT(db_get_txn(m_db)==(ham_txn_t *)13);

        db_set_extkey_cache(m_db, (extkey_cache_t *)14);
        CPPUNIT_ASSERT(db_get_extkey_cache(m_db)==(extkey_cache_t *)14);
    }

    void structureTest()
    {
        CPPUNIT_ASSERT(db_get_header_page(m_db)==0);
        db_set_header_page(m_db, (ham_page_t *)13);
        CPPUNIT_ASSERT(db_get_header_page(m_db)==(ham_page_t *)13);
        db_set_header_page(m_db, 0); /* avoid crash in ham_delete */
        CPPUNIT_ASSERT(db_get_header_page(m_db)==0);

        CPPUNIT_ASSERT(db_get_error(m_db)==HAM_SUCCESS);
        db_set_error(m_db, HAM_IO_ERROR);
        CPPUNIT_ASSERT(db_get_error(m_db)==HAM_IO_ERROR);

        CPPUNIT_ASSERT(db_get_backend(m_db)==0);
        db_set_backend(m_db, (ham_backend_t *)15);
        CPPUNIT_ASSERT(db_get_backend(m_db)==(ham_backend_t *)15);
        db_set_backend(m_db, 0);

        CPPUNIT_ASSERT(db_get_cache(m_db)==0);
        db_set_cache(m_db, (ham_cache_t *)16);
        CPPUNIT_ASSERT(db_get_cache(m_db)==(ham_cache_t *)16);
        db_set_cache(m_db, 0);

        CPPUNIT_ASSERT(db_get_freelist_cache(m_db)==0);
        db_set_freelist_cache(m_db, (ham_page_t *)17);
        CPPUNIT_ASSERT(db_get_freelist_cache(m_db)==(ham_page_t *)17);
        db_set_freelist_cache(m_db, 0);

        CPPUNIT_ASSERT(db_get_prefix_compare_func(m_db)==0);
        db_set_prefix_compare_func(m_db, (ham_prefix_compare_func_t)18);
        CPPUNIT_ASSERT(db_get_prefix_compare_func(m_db)==
                    (ham_prefix_compare_func_t)18);

        CPPUNIT_ASSERT(db_get_compare_func(m_db)==0);
        db_set_compare_func(m_db, (ham_compare_func_t)19);
        CPPUNIT_ASSERT(db_get_compare_func(m_db)==(ham_compare_func_t)19);

        CPPUNIT_ASSERT(!db_is_dirty(m_db));
        db_set_dirty(m_db, 1);
        CPPUNIT_ASSERT(db_is_dirty(m_db));

        CPPUNIT_ASSERT(db_get_rt_flags(m_db)==0);
        db_set_rt_flags(m_db, 20);
        CPPUNIT_ASSERT(db_get_rt_flags(m_db)==20);

        CPPUNIT_ASSERT(db_get_record_allocsize(m_db)==0);
        db_set_record_allocsize(m_db, 21);
        CPPUNIT_ASSERT(db_get_record_allocsize(m_db)==21);
        db_set_record_allocsize(m_db, 0);

        CPPUNIT_ASSERT(db_get_record_allocdata(m_db)==0);
        db_set_record_allocdata(m_db, (void *)22);
        CPPUNIT_ASSERT(db_get_record_allocdata(m_db)==(void *)22);
        db_set_record_allocdata(m_db, 0);
    }

    void defaultCompareTest()
    {
        CPPUNIT_ASSERT( 0==db_default_compare(
                        (ham_u8_t *)"abc", 3, (ham_u8_t *)"abc", 3));
        CPPUNIT_ASSERT(-1==db_default_compare(
                        (ham_u8_t *)"ab",  2, (ham_u8_t *)"abc", 3));
        CPPUNIT_ASSERT(-1==db_default_compare(
                        (ham_u8_t *)"abc", 3, (ham_u8_t *)"bcd", 3));
        CPPUNIT_ASSERT(+1==db_default_compare(
                        (ham_u8_t *)"abc", 3, (ham_u8_t *)0,     0));
        CPPUNIT_ASSERT(-1==db_default_compare(
                        (ham_u8_t *)0,     0, (ham_u8_t *)"abc", 3));
    }

    void defaultPrefixCompareTest()
    {
        CPPUNIT_ASSERT(db_default_prefix_compare(
                        (ham_u8_t *)"abc", 3, 3, 
                        (ham_u8_t *)"abc", 3, 3)==HAM_PREFIX_REQUEST_FULLKEY);
        CPPUNIT_ASSERT(db_default_prefix_compare(
                        (ham_u8_t *)"ab",  2, 2, 
                        (ham_u8_t *)"abc", 3, 3)==HAM_PREFIX_REQUEST_FULLKEY);
        CPPUNIT_ASSERT(-1==db_default_prefix_compare(
                        (ham_u8_t *)"abc", 3, 3,
                        (ham_u8_t *)"bcd", 3, 3));
        CPPUNIT_ASSERT(db_default_prefix_compare(
                        (ham_u8_t *)"abc", 3, 3,
                        (ham_u8_t *)0,     0, 0)==HAM_PREFIX_REQUEST_FULLKEY);
        CPPUNIT_ASSERT(db_default_prefix_compare(
                        (ham_u8_t *)0,     0, 0,
                        (ham_u8_t *)"abc", 3, 3)==HAM_PREFIX_REQUEST_FULLKEY);
        CPPUNIT_ASSERT(db_default_prefix_compare(
                        (ham_u8_t *)"abc", 3, 80239, 
                        (ham_u8_t *)"abc", 3, 2)==HAM_PREFIX_REQUEST_FULLKEY);
    }

    void allocPageTest()
    {
        ham_page_t *page;

        CPPUNIT_ASSERT((page=db_alloc_page(m_db, 0, PAGE_IGNORE_FREELIST))!=0);
#if 0
        CPPUNIT_ASSERT(page_get_owner(page)==m_db);
        CPPUNIT_ASSERT(db_free_page(page)==HAM_SUCCESS);
#endif
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(DbTest);
