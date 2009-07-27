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
#include <string.h> // [i_a]
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/page.h"
#include "../src/device.h"
#include "memtracker.h"

#include "bfc-testsuite.hpp"

using namespace bfc;

class PageTest : public fixture
{
public:
    PageTest(ham_bool_t inmemorydb=HAM_FALSE, ham_bool_t mmap=HAM_TRUE, 
            const char *name=0)
    :   fixture(name ? name : "PageTest"), 
        m_db(0), m_inmemory(inmemorydb), m_usemmap(mmap), m_dev(0), m_alloc(0)
    {
        if (name)
            return;
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(PageTest, newDeleteTest);
        BFC_REGISTER_TEST(PageTest, allocFreeTest);
        BFC_REGISTER_TEST(PageTest, multipleAllocFreeTest);
        BFC_REGISTER_TEST(PageTest, fetchFlushTest);
    }

protected:
    ham_db_t *m_db;
    ham_bool_t m_inmemory;
    ham_bool_t m_usemmap;
    ham_device_t *m_dev;
    memtracker_t *m_alloc;

public:
    void setup()
    { 
        ham_page_t *p;
        BFC_ASSERT((m_alloc=memtracker_new())!=0);
        BFC_ASSERT(0==ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        BFC_ASSERT((m_dev=ham_device_new((mem_allocator_t *)m_alloc, 
                        m_inmemory))!=0);
        if (!m_usemmap)
            m_dev->set_flags(m_dev, DEVICE_NO_MMAP);
        BFC_ASSERT(m_dev->create(m_dev, BFC_OPATH(".test"), 0, 0644)==HAM_SUCCESS);
        db_set_device(m_db, m_dev);
        p=page_new(m_db);
        BFC_ASSERT(0==page_alloc(p, m_dev->get_pagesize(m_dev)));
        db_set_header_page(m_db, p);
        db_set_pagesize(m_db, m_dev->get_pagesize(m_dev));
    }
    
    void teardown() 
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
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void newDeleteTest()
    {
        ham_page_t *page;
        page=page_new(m_db);
        BFC_ASSERT(page!=0);
        page_delete(page);
    }

    void allocFreeTest()
    {
        ham_page_t *page;
        page=page_new(m_db);
        BFC_ASSERT(page_alloc(page, db_get_pagesize(m_db))==HAM_SUCCESS);
        BFC_ASSERT(page_free(page)==HAM_SUCCESS);

        BFC_ASSERT_EQUAL((ham_offset_t)0, page_get_before_img_lsn(page));
        page_set_before_img_lsn(page, 0x13ull);
        BFC_ASSERT_EQUAL((ham_offset_t)0x13, page_get_before_img_lsn(page));

        page_delete(page);
    }

    void multipleAllocFreeTest()
    {
        int i;
        ham_page_t *page;
        ham_size_t ps=os_get_pagesize();

        for (i=0; i<10; i++) {
            page=page_new(m_db);
            BFC_ASSERT(page_alloc(page, db_get_pagesize(m_db))==0);
            if (!m_inmemory)
                BFC_ASSERT(page_get_self(page)==(i+1)*ps);
            BFC_ASSERT(page_free(page)==HAM_SUCCESS);
            page_delete(page);
        }
    }

    void fetchFlushTest()
    {
        ham_page_t *page, *temp;
        ham_size_t ps=os_get_pagesize();

        page=page_new(m_db);
        temp=page_new(m_db);
        BFC_ASSERT(page_alloc(page, db_get_pagesize(m_db))==HAM_SUCCESS);
        BFC_ASSERT(page_get_self(page)==ps);
        BFC_ASSERT(page_free(page)==HAM_SUCCESS);
        
        BFC_ASSERT(page_fetch(page, db_get_pagesize(m_db))==HAM_SUCCESS);
        memset(page_get_pers(page), 0x13, ps);
        page_set_dirty(page);
        BFC_ASSERT(page_flush(page)==HAM_SUCCESS);

        BFC_ASSERT(page_is_dirty(page)==0);
        page_set_self(temp, ps);
        BFC_ASSERT(page_fetch(temp, db_get_pagesize(m_db))==HAM_SUCCESS);
        BFC_ASSERT(0==memcmp(page_get_pers(page), page_get_pers(temp), ps));

        BFC_ASSERT(page_free(page)==HAM_SUCCESS);
        BFC_ASSERT(page_free(temp)==HAM_SUCCESS);

        page_delete(temp);
        page_delete(page);
    }

};

class RwPageTest : public PageTest
{
public:
    RwPageTest()
    : PageTest(HAM_FALSE, HAM_FALSE, "RwPageTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(RwPageTest, newDeleteTest);
        BFC_REGISTER_TEST(RwPageTest, allocFreeTest);
        BFC_REGISTER_TEST(RwPageTest, multipleAllocFreeTest);
        BFC_REGISTER_TEST(RwPageTest, fetchFlushTest);
    }
};

class InMemoryPageTest : public PageTest
{
public:
    InMemoryPageTest()
    : PageTest(HAM_TRUE, HAM_FALSE, "InMemoryPageTest")
    {
        BFC_REGISTER_TEST(InMemoryPageTest, newDeleteTest);
        BFC_REGISTER_TEST(InMemoryPageTest, allocFreeTest);
        BFC_REGISTER_TEST(InMemoryPageTest, multipleAllocFreeTest);
    }
};

BFC_REGISTER_FIXTURE(PageTest);
BFC_REGISTER_FIXTURE(RwPageTest);
BFC_REGISTER_FIXTURE(InMemoryPageTest);

