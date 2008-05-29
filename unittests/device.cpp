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
#include "../src/device.h"
#include "memtracker.h"
#include "os.hpp"

class DeviceTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(DeviceTest);
    CPPUNIT_TEST      (newDeleteTest);
    CPPUNIT_TEST      (createCloseTest);
    CPPUNIT_TEST      (openCloseTest);
    CPPUNIT_TEST      (pagesizeTest);
    CPPUNIT_TEST      (allocTest);
    CPPUNIT_TEST      (allocFreeTest);
    CPPUNIT_TEST      (flushTest);
    CPPUNIT_TEST      (mmapUnmapTest);
    CPPUNIT_TEST      (readWriteTest);
    CPPUNIT_TEST      (readWritePageTest);
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
        (void)os::unlink(".test");

        ham_page_t *p;
        m_alloc=memtracker_new();
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        CPPUNIT_ASSERT((m_dev=ham_device_new((mem_allocator_t *)m_alloc, 
                        m_inmemory))!=0);
        db_set_device(m_db, m_dev);
        CPPUNIT_ASSERT_EQUAL(0, m_dev->create(m_dev, ".test", 0, 0644));
        p=page_new(m_db);
        CPPUNIT_ASSERT_EQUAL(0, page_alloc(p, m_dev->get_pagesize(m_dev)));
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
    }

    void createCloseTest()
    {
        CPPUNIT_ASSERT(m_dev->is_open(m_dev));
        CPPUNIT_ASSERT(m_dev->close(m_dev)==HAM_SUCCESS);
        CPPUNIT_ASSERT(!m_dev->is_open(m_dev));
    }

    void openCloseTest()
    {
        if (!m_inmemory) {
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

    void allocTest()
    {
        int i;
        ham_offset_t address;

        CPPUNIT_ASSERT(m_dev->is_open(m_dev));
        for (i=0; i<10; i++) {
            CPPUNIT_ASSERT(m_dev->alloc(m_dev, 1024, &address)==HAM_SUCCESS);
            CPPUNIT_ASSERT(address==db_get_pagesize(m_db)+1024*i);
        }
        CPPUNIT_ASSERT(m_dev->close(m_dev)==HAM_SUCCESS);
    }

    void allocFreeTest()
    {
        ham_page_t page;
        memset(&page, 0, sizeof(page));
        page_set_owner(&page, m_db);

        CPPUNIT_ASSERT(m_dev->is_open(m_dev));
        CPPUNIT_ASSERT(m_dev->alloc_page(m_dev, &page, 
                    db_get_pagesize(m_db))==HAM_SUCCESS);
        CPPUNIT_ASSERT(page_get_pers(&page)!=0);
        CPPUNIT_ASSERT(m_dev->free_page(m_dev, &page)==HAM_SUCCESS);
        CPPUNIT_ASSERT(m_dev->close(m_dev)==HAM_SUCCESS);
    }

    void flushTest()
    {
        CPPUNIT_ASSERT(m_dev->is_open(m_dev));
        CPPUNIT_ASSERT(m_dev->flush(m_dev)==HAM_SUCCESS);
        CPPUNIT_ASSERT(m_dev->is_open(m_dev));
    }

    void mmapUnmapTest()
    {
        int i;
        ham_page_t pages[10];
        ham_size_t ps=m_dev->get_pagesize(m_dev);
        ham_u8_t *temp=(ham_u8_t *)malloc(ps);

        CPPUNIT_ASSERT(m_dev->is_open(m_dev));
        CPPUNIT_ASSERT(m_dev->truncate(m_dev, ps*10)==HAM_SUCCESS);
        for (i=0; i<10; i++) {
            memset(&pages[i], 0, sizeof(ham_page_t));
            page_set_owner(&pages[i], m_db);
            page_set_self(&pages[i], i*ps);
            CPPUNIT_ASSERT(m_dev->read_page(m_dev, &pages[i], 
                        db_get_pagesize(m_db))==HAM_SUCCESS);
        }
        for (i=0; i<10; i++)
            memset(page_get_pers(&pages[i]), i, ps);
        for (i=0; i<10; i++) {
            CPPUNIT_ASSERT(m_dev->write_page(m_dev, &pages[i])==HAM_SUCCESS);
        }
        for (i=0; i<10; i++) {
            ham_u8_t *buffer;
            memset(temp, i, ps);
            CPPUNIT_ASSERT(m_dev->free_page(m_dev, &pages[i])==HAM_SUCCESS);

            CPPUNIT_ASSERT(m_dev->read_page(m_dev, &pages[i], 
                        db_get_pagesize(m_db))==HAM_SUCCESS);
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

        CPPUNIT_ASSERT(m_dev->is_open(m_dev));
        CPPUNIT_ASSERT(m_dev->truncate(m_dev, ps*10)==HAM_SUCCESS);
        for (i=0; i<10; i++) {
            buffer[i]=(ham_u8_t *)malloc(ps);
            CPPUNIT_ASSERT_EQUAL(0,
                    m_dev->read(m_db, m_dev, i*ps, buffer[i], ps));
        }
        for (i=0; i<10; i++)
            memset(buffer[i], i, ps);
        for (i=0; i<10; i++) {
            CPPUNIT_ASSERT_EQUAL(0, 
                    m_dev->write(m_db, m_dev, i*ps, buffer[i], ps));
        }
        for (i=0; i<10; i++) {
            CPPUNIT_ASSERT_EQUAL(0, 
                    m_dev->read(m_db, m_dev, i*ps, buffer[i], ps));
            memset(temp, i, ps);
            CPPUNIT_ASSERT(memcmp(buffer[i], temp, ps)==0);
            free(buffer[i]);
        }
        m_dev->close(m_dev);
        free(temp);
    }

    void readWritePageTest()
    {
        int i;
        ham_page_t *pages[2];
        ham_size_t ps=m_dev->get_pagesize(m_dev);

        m_dev->set_flags(m_dev, HAM_DISABLE_MMAP);

        CPPUNIT_ASSERT(m_dev->is_open(m_dev));
        CPPUNIT_ASSERT(m_dev->truncate(m_dev, ps*2)==HAM_SUCCESS);
        for (i=0; i<2; i++) {
            CPPUNIT_ASSERT((pages[i]=page_new(m_db)));
            page_set_self(pages[i], ps*i);
            CPPUNIT_ASSERT_EQUAL(0,
                    m_dev->read_page(m_dev, pages[i], 0));
        }
        for (i=0; i<2; i++) {
            CPPUNIT_ASSERT(page_get_npers_flags(pages[i])&PAGE_NPERS_MALLOC);
            memset(page_get_pers(pages[i]), i+1, ps);
            CPPUNIT_ASSERT_EQUAL(0, 
                    m_dev->write_page(m_dev, pages[i]));
            CPPUNIT_ASSERT_EQUAL(0, page_free(pages[i]));
            page_delete(pages[i]);
        }

        for (i=0; i<2; i++) {
            char temp[1024];
            memset(temp, i+1, sizeof(temp));
            CPPUNIT_ASSERT((pages[i]=page_new(m_db)));
            page_set_self(pages[i], ps*i);
            CPPUNIT_ASSERT_EQUAL(0, 
                    m_dev->read_page(m_dev, pages[i], ps));
            CPPUNIT_ASSERT_EQUAL(0, 
                    memcmp(page_get_pers(pages[i]), temp, sizeof(temp)));
            CPPUNIT_ASSERT_EQUAL(0, page_free(pages[i]));
            page_delete(pages[i]);
        }
        m_dev->close(m_dev);
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
    CPPUNIT_TEST_SUITE_END();

public:
    InMemoryDeviceTest() 
    :   DeviceTest(HAM_TRUE) 
    { 
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(DeviceTest);
CPPUNIT_TEST_SUITE_REGISTRATION(InMemoryDeviceTest);

