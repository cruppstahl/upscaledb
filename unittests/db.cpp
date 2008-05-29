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

#include <stdexcept>
#include <cppunit/extensions/HelperMacros.h>
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/cache.h"
#include "../src/page.h"
#include "../src/env.h"
#include "memtracker.h"

class DbTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(DbTest);
    CPPUNIT_TEST      (headerTest);
    CPPUNIT_TEST      (structureTest);
    CPPUNIT_TEST      (envStructureTest);
    CPPUNIT_TEST      (defaultCompareTest);
    CPPUNIT_TEST      (defaultPrefixCompareTest);
    CPPUNIT_TEST      (allocPageTest);
    CPPUNIT_TEST      (fetchPageTest);
    CPPUNIT_TEST      (flushPageTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_db_t *m_db;
    ham_bool_t m_inmemory;
    ham_device_t *m_dev;
    memtracker_t *m_alloc;

public:
    DbTest(bool inmem=false)
    :   m_inmemory(inmem)
    {
    }

    void setUp()
    { 
        ham_page_t *p;
        CPPUNIT_ASSERT(0==ham_new(&m_db));
        m_alloc=memtracker_new(); //ham_default_allocator_new();
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        CPPUNIT_ASSERT((m_dev=ham_device_new((mem_allocator_t *)m_alloc, 
                        m_inmemory))!=0);
        db_set_device(m_db, m_dev);
        CPPUNIT_ASSERT(m_dev->create(m_dev, ".test", 0, 0644)==HAM_SUCCESS);
        p=page_new(m_db);
        CPPUNIT_ASSERT(0==page_alloc(p, m_dev->get_pagesize(m_dev)));
        db_set_header_page(m_db, p);
        db_set_pagesize(m_db, m_dev->get_pagesize(m_dev));
    }
    
    void tearDown() 
    { 
        if (db_get_header_page(m_db)) {
            page_free(db_get_header_page(m_db));
            page_delete(db_get_header_page(m_db));
            db_set_header_page(m_db, 0);
        }
        if (db_get_cache(m_db)) {
            cache_delete(m_db, db_get_cache(m_db));
            db_set_cache(m_db, 0);
        }
        if (db_get_device(m_db)) {
            if (db_get_device(m_db)->is_open(db_get_device(m_db)))
                db_get_device(m_db)->close(db_get_device(m_db));
            db_get_device(m_db)->destroy(db_get_device(m_db));
            db_set_device(m_db, 0);
        }
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

        ham_size_t ps=db_get_pagesize(m_db);
        db_set_pagesize(m_db, 1024*64);
        CPPUNIT_ASSERT(db_get_pagesize(m_db)==1024*64);
        db_set_pagesize(m_db, ps);

        db_set_txn(m_db, (ham_txn_t *)13);
        CPPUNIT_ASSERT(db_get_txn(m_db)==(ham_txn_t *)13);

        db_set_extkey_cache(m_db, (extkey_cache_t *)14);
        CPPUNIT_ASSERT(db_get_extkey_cache(m_db)==(extkey_cache_t *)14);
    }

    void structureTest()
    {
        CPPUNIT_ASSERT(db_get_header_page(m_db)!=0);

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

        CPPUNIT_ASSERT(db_get_env(m_db)==0);
        db_set_env(m_db, (ham_env_t *)30);
        CPPUNIT_ASSERT(db_get_env(m_db)==(ham_env_t *)30);
        db_set_env(m_db, 0);

        CPPUNIT_ASSERT(db_get_next(m_db)==0);
        db_set_next(m_db, (ham_db_t *)40);
        CPPUNIT_ASSERT(db_get_next(m_db)==(ham_db_t *)40);

        CPPUNIT_ASSERT(db_get_record_allocsize(m_db)==0);
        db_set_record_allocsize(m_db, 21);
        CPPUNIT_ASSERT(db_get_record_allocsize(m_db)==21);
        db_set_record_allocsize(m_db, 0);

        CPPUNIT_ASSERT(db_get_record_allocdata(m_db)==0);
        db_set_record_allocdata(m_db, (void *)22);
        CPPUNIT_ASSERT(db_get_record_allocdata(m_db)==(void *)22);
        db_set_record_allocdata(m_db, 0);
    }

    void envStructureTest()
    {
        ham_env_t *env;

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        env_set_txn_id(env, 0x12345ull);
        env_set_device(env, (ham_device_t *)0x13);
        env_set_cache(env, (ham_cache_t *)0x14);
        env_set_txn(env, (ham_txn_t *)0x16);
        env_set_extkey_cache(env, (extkey_cache_t *)0x17);
        env_set_rt_flags(env, 0x18);
        env_set_header_page(env, (ham_page_t *)0x19);
        env_set_list(env, m_db);

        db_set_env(m_db, env);

        CPPUNIT_ASSERT_EQUAL((ham_page_t *)0x19, 
                db_get_header_page(m_db));

        CPPUNIT_ASSERT_EQUAL((ham_cache_t *)0x14, 
                db_get_cache(m_db));

        CPPUNIT_ASSERT_EQUAL((ham_u32_t)0x18, db_get_rt_flags(m_db));

        CPPUNIT_ASSERT_EQUAL(env, db_get_env(m_db));

        env_set_device(env, 0);
        env_set_cache(env, 0);
        env_set_txn(env, 0);
        env_set_extkey_cache(env, 0);
        env_set_rt_flags(env, 0x18);
        env_set_header_page(env, 0);
        env_set_list(env, 0);
        db_set_env(m_db, 0);
        ham_env_delete(env);
    }

    void defaultCompareTest()
    {
        CPPUNIT_ASSERT( 0==db_default_compare(0,
                        (ham_u8_t *)"abc", 3, (ham_u8_t *)"abc", 3));
        CPPUNIT_ASSERT(-1==db_default_compare(0,
                        (ham_u8_t *)"ab",  2, (ham_u8_t *)"abc", 3));
        CPPUNIT_ASSERT(-1==db_default_compare(0,
                        (ham_u8_t *)"abc", 3, (ham_u8_t *)"bcd", 3));
        CPPUNIT_ASSERT(+1==db_default_compare(0,
                        (ham_u8_t *)"abc", 3, (ham_u8_t *)0,     0));
        CPPUNIT_ASSERT(-1==db_default_compare(0,
                        (ham_u8_t *)0,     0, (ham_u8_t *)"abc", 3));
    }

    void defaultPrefixCompareTest()
    {
        CPPUNIT_ASSERT(db_default_prefix_compare(0,
                        (ham_u8_t *)"abc", 3, 3, 
                        (ham_u8_t *)"abc", 3, 3)==HAM_PREFIX_REQUEST_FULLKEY);
        CPPUNIT_ASSERT(db_default_prefix_compare(0,
                        (ham_u8_t *)"ab",  2, 2, 
                        (ham_u8_t *)"abc", 3, 3)==HAM_PREFIX_REQUEST_FULLKEY);
        CPPUNIT_ASSERT(-1==db_default_prefix_compare(0,
                        (ham_u8_t *)"abc", 3, 3,
                        (ham_u8_t *)"bcd", 3, 3));
        CPPUNIT_ASSERT(db_default_prefix_compare(0,
                        (ham_u8_t *)"abc", 3, 3,
                        (ham_u8_t *)0,     0, 0)==HAM_PREFIX_REQUEST_FULLKEY);
        CPPUNIT_ASSERT(db_default_prefix_compare(0,
                        (ham_u8_t *)0,     0, 0,
                        (ham_u8_t *)"abc", 3, 3)==HAM_PREFIX_REQUEST_FULLKEY);
        CPPUNIT_ASSERT(db_default_prefix_compare(0,
                        (ham_u8_t *)"abc", 3, 80239, 
                        (ham_u8_t *)"abc", 3, 2)==HAM_PREFIX_REQUEST_FULLKEY);
    }

    void allocPageTest(void)
    {
        ham_page_t *page;
        CPPUNIT_ASSERT((page=db_alloc_page(m_db, 0, PAGE_IGNORE_FREELIST))!=0);
        CPPUNIT_ASSERT(page_get_owner(page)==m_db);
        CPPUNIT_ASSERT(db_free_page(page, 0)==HAM_SUCCESS);
        CPPUNIT_ASSERT(m_dev->close(m_dev)==HAM_SUCCESS);
    }

    void fetchPageTest(void)
    {
        ham_page_t *p1, *p2;
        CPPUNIT_ASSERT((p1=db_alloc_page(m_db, 0, PAGE_IGNORE_FREELIST))!=0);
        CPPUNIT_ASSERT(page_get_owner(p1)==m_db);
        CPPUNIT_ASSERT((p2=db_fetch_page(m_db, page_get_self(p1), 0))!=0);
        CPPUNIT_ASSERT(page_get_self(p2)==page_get_self(p1));
        CPPUNIT_ASSERT(db_free_page(p1, 0)==HAM_SUCCESS);
        CPPUNIT_ASSERT(db_free_page(p2, 0)==HAM_SUCCESS);
    }

    void flushPageTest(void)
    {
        ham_page_t *page;
        ham_offset_t address;
        ham_u8_t *p;

        ham_cache_t *cache=cache_new(m_db, 15);
        CPPUNIT_ASSERT(cache!=0);
        db_set_cache(m_db, cache);

        CPPUNIT_ASSERT((page=db_alloc_page(m_db, 0, PAGE_IGNORE_FREELIST))!=0);
        CPPUNIT_ASSERT(page_get_owner(page)==m_db);
        p=page_get_raw_payload(page);
        for (int i=0; i<16; i++)
            p[i]=(ham_u8_t)i;
        page_set_dirty(page);
        address=page_get_self(page);
        CPPUNIT_ASSERT(db_flush_page(m_db, page, 0)==HAM_SUCCESS);
        CPPUNIT_ASSERT(db_free_page(page, 0)==HAM_SUCCESS);

        CPPUNIT_ASSERT((page=db_fetch_page(m_db, address, 0))!=0);
        CPPUNIT_ASSERT(page_get_self(page)==address);
        p=page_get_raw_payload(page);
        /* TODO see comment in db.c - db_free_page()
        for (int i=0; i<16; i++)
            CPPUNIT_ASSERT(p[i]==(ham_u8_t)i);
        */
        CPPUNIT_ASSERT(db_free_page(page, 0)==HAM_SUCCESS);
    }
};

class DbInMemoryTest : public DbTest
{
    CPPUNIT_TEST_SUITE(DbTest);
    CPPUNIT_TEST      (headerTest);
    CPPUNIT_TEST      (structureTest);
    CPPUNIT_TEST      (envStructureTest);
    CPPUNIT_TEST      (defaultCompareTest);
    CPPUNIT_TEST      (defaultPrefixCompareTest);
    CPPUNIT_TEST      (allocPageTest);
    CPPUNIT_TEST_SUITE_END();

public:
    DbInMemoryTest()
    :   DbTest(true)
    { };
};

CPPUNIT_TEST_SUITE_REGISTRATION(DbTest);
CPPUNIT_TEST_SUITE_REGISTRATION(DbInMemoryTest);
