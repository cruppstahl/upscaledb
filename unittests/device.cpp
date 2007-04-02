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
    ham_dev_t *m_dev;
    memtracker_t *m_alloc;

public:
    void setUp()
    { 
        m_alloc=memtracker_new();
        CPPUNIT_ASSERT(0==ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        db_set_device((m_dev=ham_dev_new(m_db, m_inmemory)));
    }
    
    void tearDown() 
    { 
        ham_delete(m_db);
        CPPUNIT_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void newDeleteTest()
    {
        ham_dev_t *dev=ham_dev_new(m_db, m_inmemory);
        CPPUNIT_ASSERT(dev!=0);
        dev->del(dev);
    }

    void createCloseTest()
    {
        CPPUNIT_ASSERT(!m_dev->is_open(m_dev));
        CPPUNIT_ASSERT(m_dev->create(m_dev, ".test", 0644, 0)==HAM_SUCCESS);
        CPPUNIT_ASSERT(m_dev->is_open(m_dev));
        CPPUNIT_ASSERT(m_dev->close(m_dev)==HAM_SUCCESS);
        CPPUNIT_ASSERT(!m_dev->is_open(m_dev));
    }

    void openCloseTest()
    {
        CPPUNIT_ASSERT(!m_dev->is_open(m_dev));
        CPPUNIT_ASSERT(m_dev->create(m_dev, ".test", 0644, 0)==HAM_SUCCESS);
        CPPUNIT_ASSERT(m_dev->is_open(m_dev));
        CPPUNIT_ASSERT(m_dev->close(m_dev)==HAM_SUCCESS);
        CPPUNIT_ASSERT(!m_dev->is_open(m_dev));
        CPPUNIT_ASSERT(m_dev->open(m_dev, ".test", 0)==HAM_SUCCESS);
        CPPUNIT_ASSERT(m_dev->is_open(m_dev));
        CPPUNIT_ASSERT(m_dev->close(m_dev)==HAM_SUCCESS);
        CPPUNIT_ASSERT(!m_dev->is_open(m_dev));
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

        CPPUNIT_ASSERT(m_dev->create(m_dev, ".test", 0644, 0)==HAM_SUCCESS);
        CPPUNIT_ASSERT(m_dev->is_open(m_dev));
        CPPUNIT_ASSERT(m_dev->alloc(m_dev, &buffer, 4096)==HAM_SUCCESS);
        CPPUNIT_ASSERT(buffer!=0);
        CPPUNIT_ASSERT(m_dev->free(m_dev, &buffer, 4096)==HAM_SUCCESS);
        CPPUNIT_ASSERT(m_dev->close(m_dev)==HAM_SUCCESS);
    }

    void flushTest()
    {
        CPPUNIT_ASSERT(m_dev->create(m_dev, ".test", 0644, 0)==HAM_ASSERT);
        CPPUNIT_ASSERT(m_dev->is_open(m_dev));
        CPPUNIT_ASSERT(m_dev->flush(m_dev)==HAM_ASSERT);
        CPPUNIT_ASSERT(m_dev->is_open(m_dev));
        CPPUNIT_ASSERT(m_dev->close(m_dev)==HAM_ASSERT);
        CPPUNIT_ASSERT(!m_dev->is_open(m_dev));
    }

    void mmapUnmapTest()
    {
        int i;
        void *buffer[10]={ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        ham_size_t ps=m_dev->get_pagesize(m_dev);

        CPPUNIT_ASSERT(m_dev->create(m_dev, ".test", 0644, 0)==HAM_ASSERT);
        CPPUNIT_ASSERT(m_dev->is_open(m_dev));
        CPPUNIT_ASSERT(m_dev->truncate(m_dev, ps*10)==HAM_ASSERT);
        for (i=0; i<10; i++) {
            CPPUNIT_ASSERT(m_dev->read(m_dev, i*ps, ps, 
                        &buffer[i], 0)==HAM_SUCCESS);
        }
        for (i=0; i<10; i++)
            memset(buffer[i], i, ps);
        for (i=0; i<10; i++) {
            CPPUNIT_ASSERT(m_dev->write(m_dev, i*ps, ps, 
                        buffer[i], 0)==HAM_SUCCESS);
        }
        for (i=0; i<10; i++) {
            int j;
            CPPUNIT_ASSERT(m_dev->read(m_dev, i*ps, ps, 
                        &buffer[i], DEVICE_NO_MALLOC)==HAM_SUCCESS);
            for (j=0; j<ps; j++)
                CPPUNIT_ASSERT(buffer[j]==i);
        }
        for (i=0; i<10; i++) {
            CPPUNIT_ASSERT(m_dev->free(m_dev, i*ps, ps, 
                        buffer[i], 0)==HAM_SUCCESS);
        }
        m_dev->close(m_dev);
    }

    void readWriteTest()
    {
        int i;
        void *buffer[10]={ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        ham_size_t ps=m_dev->get_pagesize(m_dev);

        m_dev->set_flags(m_dev, DEVICE_NO_MMAP);

        CPPUNIT_ASSERT(m_dev->create(m_dev, ".test", 0644, 0)==HAM_ASSERT);
        CPPUNIT_ASSERT(m_dev->is_open(m_dev));
        CPPUNIT_ASSERT(m_dev->truncate(m_dev, ps*10)==HAM_ASSERT);
        for (i=0; i<10; i++) {
            CPPUNIT_ASSERT(m_dev->read(m_dev, i*ps, ps, 
                        &buffer[i], 0)==HAM_SUCCESS);
        }
        for (i=0; i<10; i++)
            memset(buffer[i], i, ps);
        for (i=0; i<10; i++) {
            CPPUNIT_ASSERT(m_dev->write(m_dev, i*ps, ps, 
                        buffer[i], 0)==HAM_SUCCESS);
        }
        for (i=0; i<10; i++) {
            int j;
            CPPUNIT_ASSERT(m_dev->read(m_dev, i*ps, ps, 
                        &buffer[i], DEVICE_NO_MALLOC)==HAM_SUCCESS);
            for (j=0; j<ps; j++)
                CPPUNIT_ASSERT(buffer[j]==i);
        }
        for (i=0; i<10; i++) {
            CPPUNIT_ASSERT(m_dev->free(m_dev, i*ps, ps, 
                        buffer[i], 0)==HAM_SUCCESS);
        }
        m_dev->close(m_dev);
    }

};

class InMemoryDeviceTest : public CppUnit::TestFixture
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
    InMemoryDeviceTest() : m_inmemory(HAM_TRUE) { }
}

CPPUNIT_TEST_SUITE_REGISTRATION(DeviceTest);
CPPUNIT_TEST_SUITE_REGISTRATION(InMemoryDeviceTest);
