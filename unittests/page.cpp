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
    CPPUNIT_TEST      (negativeFreeTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_db_t *m_db;
    ham_bool_t m_inmemory;
    ham_device_t *m_dev;
    memtracker_t *m_alloc;

public:
    PageTest(ham_bool_t inmemorydb=HAM_FALSE)
    :   m_inmemory(inmemorydb)
    {
    } 

    void setUp()
    { 
        m_alloc=memtracker_new();
        CPPUNIT_ASSERT(0==ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        db_set_device(m_db, (m_dev=ham_device_new(m_db, m_inmemory)));
        db_set_pagesize(m_db, m_dev->get_pagesize(m_dev));
    }
    
    void tearDown() 
    { 
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
            CPPUNIT_ASSERT(page_get_self(page)==i*ps);
            CPPUNIT_ASSERT(page_free(page)==HAM_SUCCESS);
            page_delete(page);
        }
    }

    void fetchFlushTest()
    {
        /* TODO */
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(PageTest);

