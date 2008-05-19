/**
 * Copyright (C) 2005-2008 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 */

#include <stdexcept>
#include <cppunit/extensions/HelperMacros.h>
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/page.h"
#include "../src/device.h"
#include "memtracker.h"

class PageTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(PageTest);
    CPPUNIT_TEST      (newDeleteTest);
    CPPUNIT_TEST      (allocFreeTest);
    CPPUNIT_TEST      (multipleAllocFreeTest);
    CPPUNIT_TEST      (fetchFlushTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_db_t *m_db;
    ham_bool_t m_inmemory;
    ham_bool_t m_usemmap;
    ham_device_t *m_dev;
    memtracker_t *m_alloc;

public:
    PageTest(ham_bool_t inmemorydb=HAM_FALSE, ham_bool_t mmap=HAM_TRUE)
    :   m_inmemory(inmemorydb), m_usemmap(mmap)
    {
    } 

    void setUp()
    { 
        ham_page_t *p;
        CPPUNIT_ASSERT((m_alloc=memtracker_new())!=0);
        CPPUNIT_ASSERT(0==ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        CPPUNIT_ASSERT((m_dev=ham_device_new((mem_allocator_t *)m_alloc, 
                        m_inmemory))!=0);
        if (!m_usemmap)
            m_dev->set_flags(m_dev, DEVICE_NO_MMAP);
        CPPUNIT_ASSERT(m_dev->create(m_dev, ".test", 0, 0644)==HAM_SUCCESS);
        db_set_device(m_db, m_dev);
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
        if (db_get_device(m_db)) {
            if (db_get_device(m_db)->is_open(db_get_device(m_db)))
                db_get_device(m_db)->close(db_get_device(m_db));
            db_get_device(m_db)->destroy(db_get_device(m_db));
            db_set_device(m_db, 0);
        }
        ham_delete(m_db);
        CPPUNIT_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void newDeleteTest()
    {
        ham_page_t *page;
        page=page_new(m_db);
        CPPUNIT_ASSERT(page!=0);
        page_delete(page);
    }

    void allocFreeTest()
    {
        ham_page_t *page;
        page=page_new(m_db);
        CPPUNIT_ASSERT(page_alloc(page, db_get_pagesize(m_db))==HAM_SUCCESS);
        CPPUNIT_ASSERT(page_free(page)==HAM_SUCCESS);

        CPPUNIT_ASSERT_EQUAL((ham_offset_t)0, page_get_before_img_lsn(page));
        page_set_before_img_lsn(page, 0x13ull);
        CPPUNIT_ASSERT_EQUAL((ham_offset_t)0x13, page_get_before_img_lsn(page));

        page_delete(page);
    }

    void multipleAllocFreeTest()
    {
        int i;
        ham_page_t *page;
        ham_size_t ps=os_get_pagesize();

        for (i=0; i<10; i++) {
            page=page_new(m_db);
            CPPUNIT_ASSERT(page_alloc(page, db_get_pagesize(m_db))==0);
            if (!m_inmemory)
                CPPUNIT_ASSERT(page_get_self(page)==(i+1)*ps);
            CPPUNIT_ASSERT(page_free(page)==HAM_SUCCESS);
            page_delete(page);
        }
    }

    void fetchFlushTest()
    {
        ham_page_t *page, *temp;
        ham_size_t ps=os_get_pagesize();

        page=page_new(m_db);
        temp=page_new(m_db);
        CPPUNIT_ASSERT(page_alloc(page, db_get_pagesize(m_db))==HAM_SUCCESS);
        CPPUNIT_ASSERT(page_get_self(page)==ps);
        CPPUNIT_ASSERT(page_free(page)==HAM_SUCCESS);
        
        CPPUNIT_ASSERT(page_fetch(page, db_get_pagesize(m_db))==HAM_SUCCESS);
        memset(page_get_pers(page), 0x13, ps);
        page_set_dirty(page);
        CPPUNIT_ASSERT(page_flush(page)==HAM_SUCCESS);

        CPPUNIT_ASSERT(page_is_dirty(page)==0);
        page_set_self(temp, ps);
        CPPUNIT_ASSERT(page_fetch(temp, db_get_pagesize(m_db))==HAM_SUCCESS);
        CPPUNIT_ASSERT(0==memcmp(page_get_pers(page), page_get_pers(temp), ps));

        CPPUNIT_ASSERT(page_free(page)==HAM_SUCCESS);
        CPPUNIT_ASSERT(page_free(temp)==HAM_SUCCESS);

        page_delete(temp);
        page_delete(page);
    }

};

class RwPageTest : public PageTest
{
    CPPUNIT_TEST_SUITE(RwPageTest);
    CPPUNIT_TEST      (newDeleteTest);
    CPPUNIT_TEST      (allocFreeTest);
    CPPUNIT_TEST      (multipleAllocFreeTest);
    CPPUNIT_TEST      (fetchFlushTest);
    CPPUNIT_TEST_SUITE_END();

public:
    RwPageTest()
    :   PageTest(HAM_FALSE, HAM_FALSE)
    {
    }
};

class InMemoryPageTest : public PageTest
{
    CPPUNIT_TEST_SUITE(InMemoryPageTest);
    CPPUNIT_TEST      (newDeleteTest);
    CPPUNIT_TEST      (allocFreeTest);
    CPPUNIT_TEST      (multipleAllocFreeTest);
    CPPUNIT_TEST_SUITE_END();

public:
    InMemoryPageTest()
    :   PageTest(HAM_TRUE, HAM_FALSE)
    {
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(PageTest);
CPPUNIT_TEST_SUITE_REGISTRATION(RwPageTest);
CPPUNIT_TEST_SUITE_REGISTRATION(InMemoryPageTest);

