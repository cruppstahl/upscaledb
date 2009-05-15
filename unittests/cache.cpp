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
#include <vector>
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/page.h"
#include "../src/cache.h"
#include "../src/error.h"
#include "memtracker.h"

#include "bfc-testsuite.hpp"

using namespace bfc;

class CacheTest : public fixture
{
public:
    CacheTest()
    : fixture("CacheTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(CacheTest, newDeleteTest);
        BFC_REGISTER_TEST(CacheTest, structureTest);
        BFC_REGISTER_TEST(CacheTest, putGetTest);
        BFC_REGISTER_TEST(CacheTest, putGetRemoveGetTest);
        BFC_REGISTER_TEST(CacheTest, putGetReplaceTest);
        BFC_REGISTER_TEST(CacheTest, multiplePutTest);
        BFC_REGISTER_TEST(CacheTest, negativeGetTest);
        BFC_REGISTER_TEST(CacheTest, garbageTest);
        BFC_REGISTER_TEST(CacheTest, unusedTest);
        BFC_REGISTER_TEST(CacheTest, overflowTest);
    }

protected:
    ham_db_t *m_db;
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
                        HAM_TRUE))!=0);
        BFC_ASSERT(m_dev->create(m_dev, ".test", 0, 0644)==HAM_SUCCESS);
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

    void newDeleteTest(void)
    {
        ham_cache_t *cache=cache_new(m_db, 15);
        BFC_ASSERT(cache!=0);
        cache_delete(m_db, cache);
    }

    void structureTest(void)
    {
        ham_cache_t *cache=cache_new(m_db, 15);
        BFC_ASSERT(cache!=0);
        BFC_ASSERT(cache_get_max_elements(cache)==15);
        cache_set_cur_elements(cache, 12);
        BFC_ASSERT(cache_get_cur_elements(cache)==12);
        cache_set_bucketsize(cache, 11);
        BFC_ASSERT(cache_get_bucketsize(cache)==11);
        BFC_ASSERT(cache_get_totallist(cache)==0);
        BFC_ASSERT(cache_get_unused_page(cache)==0);
        BFC_ASSERT(cache_get_page(cache, 0x123ull, 0)==0);
        BFC_ASSERT(cache_too_big(cache)==0);
        cache_delete(m_db, cache);
    }
    
    void putGetTest(void)
    {
        ham_page_t *page;
        ham_cache_t *cache=cache_new(m_db, 15);
        BFC_ASSERT(cache!=0);
        page=page_new(m_db);
        page_set_self(page, 0x123ull);
        page_set_npers_flags(page, PAGE_NPERS_NO_HEADER);
        BFC_ASSERT(cache_put_page(cache, page)==HAM_SUCCESS);
        BFC_ASSERT(cache_get_page(cache, 0x123ull, 0)==page);
        cache_delete(m_db, cache);
        page_delete(page);
    }

    void putGetRemoveGetTest(void)
    {
        ham_page_t *page;
        ham_cache_t *cache=cache_new(m_db, 15);
        BFC_ASSERT(cache!=0);
        page=page_new(m_db);
        page_set_npers_flags(page, PAGE_NPERS_NO_HEADER);
        page_set_self(page, 0x123ull);
        BFC_ASSERT(cache_put_page(cache, page)==HAM_SUCCESS);
        BFC_ASSERT(cache_get_page(cache, 0x123ull, 0)==page);
        BFC_ASSERT(cache_remove_page(cache, page)==HAM_SUCCESS);
        BFC_ASSERT(cache_get_page(cache, 0x123ull, 0)==0);
        cache_delete(m_db, cache);
        page_delete(page);
    }
    
    void putGetReplaceTest(void)
    {
        ham_page_t *page1, *page2;
        ham_cache_t *cache=cache_new(m_db, 15);
        BFC_ASSERT(cache!=0);
        page1=page_new(m_db);
        page_set_npers_flags(page1, PAGE_NPERS_NO_HEADER);
        page_set_self(page1, 0x123ull);
        page2=page_new(m_db);
        page_set_npers_flags(page2, PAGE_NPERS_NO_HEADER);
        page_set_self(page2, 0x456ull);
        BFC_ASSERT(cache_put_page(cache, page1)==HAM_SUCCESS);
        BFC_ASSERT(cache_remove_page(cache, page1)==HAM_SUCCESS);
        BFC_ASSERT(cache_put_page(cache, page2)==HAM_SUCCESS);
        BFC_ASSERT(cache_get_page(cache, 0x123ull, 0)==0);
        BFC_ASSERT(cache_get_page(cache, 0x456ull, 0)==page2);
        cache_delete(m_db, cache);
        page_delete(page1);
        page_delete(page2);
    }
    
    void multiplePutTest(void)
    {
        ham_page_t *page[20];
        ham_cache_t *cache=cache_new(m_db, 15);

        for (int i=0; i<20; i++) {
            page[i]=page_new(m_db);
            page_set_npers_flags(page[i], PAGE_NPERS_NO_HEADER);
            page_set_self(page[i], i*1024);
            BFC_ASSERT(cache_put_page(cache, page[i])==HAM_SUCCESS);
        }
        for (int i=0; i<20; i++) {
            BFC_ASSERT(cache_get_page(cache, i*1024, 0)==page[i]);
        }
        for (int i=0; i<20; i++) {
            BFC_ASSERT(cache_remove_page(cache, page[i])==0);
        }
        for (int i=0; i<20; i++) {
            BFC_ASSERT(cache_get_page(cache, i*1024, 0)==0);
            page_delete(page[i]);
        }
        cache_delete(m_db, cache);
    }
    
    void negativeGetTest(void)
    {
        ham_cache_t *cache=cache_new(m_db, 15);
        for (int i=0; i<20; i++) {
            BFC_ASSERT(cache_get_page(cache, i*1024*13, 0)==0);
        }
        cache_delete(m_db, cache);
    }
    
    void garbageTest(void)
    {
        ham_page_t *page;
        ham_cache_t *cache=cache_new(m_db, 15);
        BFC_ASSERT(cache!=0);
        page=page_new(m_db);
        page_set_npers_flags(page, PAGE_NPERS_NO_HEADER);
        page_set_self(page, 0x123ull);
        BFC_ASSERT(cache_put_page(cache, page)==HAM_SUCCESS);
        BFC_ASSERT(cache_get_page(cache, 0x123ull, 0)==page);
        BFC_ASSERT(cache_move_to_garbage(cache, page)==HAM_SUCCESS);
        BFC_ASSERT(cache_get_page(cache, 0x123ull, 0)==0);
        BFC_ASSERT(cache_get_unused_page(cache)==page);
        BFC_ASSERT(cache_get_unused_page(cache)==0);
        cache_delete(m_db, cache);
        page_delete(page);
    }
    
    void unusedTest(void)
    {
        ham_page_t *page1, *page2;
        ham_cache_t *cache=cache_new(m_db, 15);
        BFC_ASSERT(cache!=0);
        page1=page_new(m_db);
        page_set_npers_flags(page1, PAGE_NPERS_NO_HEADER);
        page_set_self(page1, 0x123ull);
        page_add_ref(page1);
        page2=page_new(m_db);
        page_set_npers_flags(page2, PAGE_NPERS_NO_HEADER);
        page_set_self(page2, 0x456ull);
        BFC_ASSERT(cache_put_page(cache, page1)==HAM_SUCCESS);
        BFC_ASSERT(cache_put_page(cache, page2)==HAM_SUCCESS);
        BFC_ASSERT(cache_get_unused_page(cache)==page2);
        BFC_ASSERT(cache_get_unused_page(cache)==0);
        BFC_ASSERT(cache_get_unused_page(cache)==0);
        BFC_ASSERT(cache_get_page(cache, 0x123ull, 0)==page1);
        BFC_ASSERT(cache_get_page(cache, 0x456ull, 0)==0);
        cache_delete(m_db, cache);
        page_release_ref(page1);
        page_delete(page1);
        page_delete(page2);
    }
    
    void overflowTest(void)
    {
        ham_cache_t *cache=cache_new(m_db, 15);
        std::vector<ham_page_t *> v;

        for (unsigned int i=0; i<cache_get_max_elements(cache)+10; i++) {
            ham_page_t *p=page_new(m_db);
            page_set_npers_flags(p, PAGE_NPERS_NO_HEADER);
            page_set_self(p, i*1024);
            v.push_back(p);
            BFC_ASSERT(cache_put_page(cache, p)==0);
        }

        for (unsigned int i=0; i<=10; i++) {
            ham_page_t *p;
            BFC_ASSERT(cache_too_big(cache));
            p=v.back();
            v.pop_back();
            BFC_ASSERT(cache_remove_page(cache, p)==HAM_SUCCESS);
            page_delete(p);
        }

        for (unsigned int i=0; i<cache_get_max_elements(cache)-1; i++) {
            ham_page_t *p;
            p=v.back();
            v.pop_back();
            BFC_ASSERT(!cache_too_big(cache));
            BFC_ASSERT(cache_remove_page(cache, p)==HAM_SUCCESS);
            page_delete(p);
        }

        BFC_ASSERT(!cache_too_big(cache));
        cache_delete(m_db, cache);
    }
};

BFC_REGISTER_FIXTURE(CacheTest);

