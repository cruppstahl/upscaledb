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
#include "../src/device.h"
#include "memtracker.h"

class DeviceTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(DeviceTest);
    CPPUNIT_TEST      (newDeleteTest);
    CPPUNIT_TEST      (createCloseTest);
    CPPUNIT_TEST      (openCloseTest);
    CPPUNIT_TEST      (pagesizeTest);
    CPPUNIT_TEST      (allocFreeTest);
    CPPUNIT_TEST      (flushTest);
    CPPUNIT_TEST      (mmapUnmapTest);
    CPPUNIT_TEST      (readWriteTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_db_t *m_db;
    ham_bool_t m_inmemory;
    ham_device_t *m_dev;
    memtracker_t *m_alloc;

public:
    DeviceTest(ham_bool_t inmemorydb=HAM_FALSE)
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
        ham_device_t *dev=ham_device_new(m_db, m_inmemory);
        CPPUNIT_ASSERT(dev!=0);
    }

    void createCloseTest()
    {
        CPPUNIT_ASSERT(!m_dev->is_open(m_dev));
        CPPUNIT_ASSERT(m_dev->create(m_dev, ".test", 0, 0644)==HAM_SUCCESS);
        CPPUNIT_ASSERT(m_dev->is_open(m_dev));
        CPPUNIT_ASSERT(m_dev->close(m_dev)==HAM_SUCCESS);
        CPPUNIT_ASSERT(!m_dev->is_open(m_dev));
    }

    void openCloseTest()
    {
        if (!m_inmemory) {
            CPPUNIT_ASSERT(!m_dev->is_open(m_dev));
            CPPUNIT_ASSERT(m_dev->create(m_dev, ".test", 0, 0644)==HAM_SUCCESS);
            CPPUNIT_ASSERT(m_dev->is_open(m_dev));
            CPPUNIT_ASSERT(m_dev->close(m_dev)==HAM_SUCCESS);
            CPPUNIT_ASSERT(!m_dev->is_open(m_dev));
            CPPUNIT_ASSERT(m_dev->open(m_dev, ".test", 0)==HAM_SUCCESS);
            CPPUNIT_ASSERT(m_dev->is_open(m_dev));
            CPPUNIT_ASSERT(m_dev->close(m_dev)==HAM_SUCCESS);
            CPPUNIT_ASSERT(!m_dev->is_open(m_dev));
        }
    }

    void pagesizeTest()
    {
        ham_size_t ps=m_dev->get_pagesize(m_dev);
        CPPUNIT_ASSERT(ps!=0);
        CPPUNIT_ASSERT(ps%1024==0);
    }

    void allocFreeTest()
    {
        void *buffer=0;
        ham_page_t page;
        memset(&page, 0, sizeof(page));

        CPPUNIT_ASSERT(m_dev->create(m_dev, ".test", 0, 0644)==HAM_SUCCESS);
        CPPUNIT_ASSERT(m_dev->is_open(m_dev));
        CPPUNIT_ASSERT(m_dev->alloc_page(m_dev, &page)==HAM_SUCCESS);
        CPPUNIT_ASSERT(page_get_pers(&page)!=0);
        CPPUNIT_ASSERT(m_dev->free_page(m_dev, &page)==HAM_SUCCESS);
        CPPUNIT_ASSERT(m_dev->close(m_dev)==HAM_SUCCESS);
    }

    void flushTest()
    {
        CPPUNIT_ASSERT(m_dev->create(m_dev, ".test", 0, 0644)==HAM_SUCCESS);
        CPPUNIT_ASSERT(m_dev->is_open(m_dev));
        CPPUNIT_ASSERT(m_dev->flush(m_dev)==HAM_SUCCESS);
        CPPUNIT_ASSERT(m_dev->is_open(m_dev));
        CPPUNIT_ASSERT(m_dev->close(m_dev)==HAM_SUCCESS);
        CPPUNIT_ASSERT(!m_dev->is_open(m_dev));
    }

    void mmapUnmapTest()
    {
        int i;
        ham_page_t pages[10];
        ham_size_t ps=m_dev->get_pagesize(m_dev);
        ham_u8_t *temp=(ham_u8_t *)malloc(ps);

        CPPUNIT_ASSERT(m_dev->create(m_dev, ".test", 0, 0644)==HAM_SUCCESS);
        CPPUNIT_ASSERT(m_dev->is_open(m_dev));
        CPPUNIT_ASSERT(m_dev->truncate(m_dev, ps*10)==HAM_SUCCESS);
        for (i=0; i<10; i++) {
            memset(&pages[i], 0, sizeof(ham_page_t));
            page_set_self(&pages[i], i*ps);
            CPPUNIT_ASSERT(m_dev->read_page(m_dev, &pages[i])==HAM_SUCCESS);
        }
        for (i=0; i<10; i++)
            memset(page_get_pers(&pages[i]), i, ps);
        for (i=0; i<10; i++) {
            CPPUNIT_ASSERT(m_dev->write_page(m_dev, &pages[i])==HAM_SUCCESS);
        }
        for (i=0; i<10; i++) {
            int j;
            ham_u8_t *buffer;
            memset(temp, i, ps);
            CPPUNIT_ASSERT(m_dev->free_page(m_dev, &pages[i])==HAM_SUCCESS);

            CPPUNIT_ASSERT(m_dev->read_page(m_dev, &pages[i])==HAM_SUCCESS);
            buffer=(ham_u8_t *)page_get_pers(&pages[i]);
            CPPUNIT_ASSERT(memcmp(buffer, temp, ps)==0);
        }
        for (i=0; i<10; i++) {
            CPPUNIT_ASSERT(m_dev->free_page(m_dev, &pages[i])==HAM_SUCCESS);
        }
        m_dev->close(m_dev);
        free(temp);
    }

    void readWriteTest()
    {
        int i;
        ham_u8_t *buffer[10]={ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        ham_size_t ps=m_dev->get_pagesize(m_dev);
        ham_u8_t *temp=(ham_u8_t *)malloc(ps);

        m_dev->set_flags(m_dev, DEVICE_NO_MMAP);

        CPPUNIT_ASSERT(m_dev->create(m_dev, ".test", 0, 0644)==HAM_SUCCESS);
        CPPUNIT_ASSERT(m_dev->is_open(m_dev));
        CPPUNIT_ASSERT(m_dev->truncate(m_dev, ps*10)==HAM_SUCCESS);
        for (i=0; i<10; i++) {
            buffer[i]=(ham_u8_t *)malloc(ps);
            CPPUNIT_ASSERT(m_dev->read(m_dev, i*ps, 
                        buffer[i], ps)==HAM_SUCCESS);
        }
        for (i=0; i<10; i++)
            memset(buffer[i], i, ps);
        for (i=0; i<10; i++) {
            CPPUNIT_ASSERT(m_dev->write(m_dev, i*ps, 
                        buffer[i], ps)==HAM_SUCCESS);
        }
        for (i=0; i<10; i++) {
            int j;
            CPPUNIT_ASSERT(m_dev->read(m_dev, i*ps, 
                        buffer[i], ps)==HAM_SUCCESS);
            memset(temp, i, ps);
            CPPUNIT_ASSERT(memcmp(buffer[i], temp, ps)==0);
        }
        m_dev->close(m_dev);
        free(temp);
    }

};

class InMemoryDeviceTest : public DeviceTest
{
    CPPUNIT_TEST_SUITE(InMemoryDeviceTest);
    CPPUNIT_TEST      (newDeleteTest);
    CPPUNIT_TEST      (createCloseTest);
    CPPUNIT_TEST      (openCloseTest);
    CPPUNIT_TEST      (pagesizeTest);
    CPPUNIT_TEST      (allocFreeTest);
    CPPUNIT_TEST      (flushTest);
    CPPUNIT_TEST      (mmapUnmapTest);
    CPPUNIT_TEST      (readWriteTest);
    CPPUNIT_TEST_SUITE_END();

public:
    InMemoryDeviceTest() 
    :   DeviceTest(HAM_TRUE) 
    { 
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(DeviceTest);
CPPUNIT_TEST_SUITE_REGISTRATION(InMemoryDeviceTest);
