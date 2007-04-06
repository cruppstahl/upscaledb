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
        CPPUNIT_ASSERT((m_alloc=memtracker_new())!=0);
        CPPUNIT_ASSERT(0==ham_new(&m_db));
        CPPUNIT_ASSERT((m_dev=ham_device_new(m_db, m_inmemory))!=0);
        if (!m_usemmap)
            m_dev->set_flags(m_dev, DEVICE_NO_MMAP);
        CPPUNIT_ASSERT(m_dev->create(m_dev, ".test", 0, 0644)==HAM_SUCCESS);
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        db_set_device(m_db, m_dev);
        db_set_pagesize(m_db, m_dev->get_pagesize(m_dev));
    }
    
    void tearDown() 
    { 
        CPPUNIT_ASSERT(m_dev->close(m_dev)==HAM_SUCCESS);
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
        CPPUNIT_ASSERT(page_alloc(page)==HAM_SUCCESS);
        CPPUNIT_ASSERT(page_free(page)==HAM_SUCCESS);
        page_delete(page);
    }

    void multipleAllocFreeTest()
    {
        int i;
        ham_page_t *page;
        ham_size_t ps=os_get_pagesize();

        for (i=0; i<10; i++) {
            page=page_new(m_db);
            CPPUNIT_ASSERT(page_alloc(page)==HAM_SUCCESS);
            if (!m_inmemory)
                CPPUNIT_ASSERT(page_get_self(page)==i*ps);
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
        CPPUNIT_ASSERT(page_alloc(page)==HAM_SUCCESS);
        CPPUNIT_ASSERT(page_get_self(page)==0);
        CPPUNIT_ASSERT(page_free(page)==HAM_SUCCESS);
        
        CPPUNIT_ASSERT(page_fetch(page)==HAM_SUCCESS);
        memset(page_get_pers(page), 0x13, ps);
        page_set_dirty(page, 1);
        CPPUNIT_ASSERT(page_flush(page)==HAM_SUCCESS);

        page_set_self(temp, 0);
        CPPUNIT_ASSERT(page_fetch(temp)==HAM_SUCCESS);
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

