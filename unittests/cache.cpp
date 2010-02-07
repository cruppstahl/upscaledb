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
#include <vector>
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/page.h"
#include "../src/cache.h"
#include "../src/error.h"
#include "../src/env.h"
#include "memtracker.h"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;

class CacheTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    CacheTest()
    : hamsterDB_fixture("CacheTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(CacheTest, newDeleteTest);
        BFC_REGISTER_TEST(CacheTest, structureTest);
        BFC_REGISTER_TEST(CacheTest, putGetTest);
        BFC_REGISTER_TEST(CacheTest, putGetRemoveGetTest);
        BFC_REGISTER_TEST(CacheTest, putGetReplaceTest);
        BFC_REGISTER_TEST(CacheTest, multiplePutTest);
        BFC_REGISTER_TEST(CacheTest, negativeGetTest);
        BFC_REGISTER_TEST(CacheTest, unusedTest);
        BFC_REGISTER_TEST(CacheTest, overflowTest);
    }

protected:
    ham_db_t *m_db;
    ham_env_t *m_env;
    memtracker_t *m_alloc;

public:
    virtual void setup() 
    { 
        __super::setup();

        m_alloc=memtracker_new();
        BFC_ASSERT_EQUAL(0, ham_env_new(&m_env));
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        //db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        //env_set_allocator(m_env, (mem_allocator_t *)m_alloc);
        BFC_ASSERT_EQUAL(0, 
                ham_env_create(m_env, BFC_OPATH(".test"), 
                        HAM_ENABLE_TRANSACTIONS
                        | HAM_ENABLE_RECOVERY, 0644));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create_db(m_env, m_db, 13, 
                        HAM_ENABLE_DUPLICATES, 0));
    }
    
    virtual void teardown() { 
        __super::teardown();

        ham_env_close(m_env, 0);
        ham_close(m_db, 0);
        ham_delete(m_db);
        ham_env_delete(m_env);
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void newDeleteTest(void)
    {
        ham_cache_t *cache=cache_new(m_env, 15);
        BFC_ASSERT(cache!=0);
        cache_delete(m_env, cache);
    }

    void structureTest(void)
    {
        ham_cache_t *cache=cache_new(m_env, 15);
        BFC_ASSERT(cache!=0);
        BFC_ASSERT(cache_get_max_elements(cache)==15);
        cache_set_cur_elements(cache, 12);
        BFC_ASSERT(cache_get_cur_elements(cache)==12);
        cache_set_bucketsize(cache, 11);
        BFC_ASSERT(cache_get_bucketsize(cache)==11);
        BFC_ASSERT(cache_get_totallist(cache)==0);
        BFC_ASSERT(cache_get_unused_page(cache)==0);
        BFC_ASSERT(cache_get_page(cache, 0x123ull, 0)==0);
        BFC_ASSERT(cache_too_big(cache, 0)==0);
        cache_delete(m_env, cache);
    }
    
    void putGetTest(void)
    {
        ham_page_t *page;
        ham_perm_page_union_t pers;
        memset(&pers, 0, sizeof(pers));
        ham_cache_t *cache=cache_new(m_env, 15);
        BFC_ASSERT(cache!=0);
        page=page_new(m_env);
        page_set_self(page, 0x123ull);
        page_set_pers(page, &pers);
        page_set_npers_flags(page, PAGE_NPERS_NO_HEADER);
        BFC_ASSERT(cache_put_page(cache, page)==HAM_SUCCESS);
        BFC_ASSERT(cache_get_page(cache, 0x123ull, 0)==page);
        cache_delete(m_env, cache);
        page_set_pers(page, 0);
        page_delete(page);
    }

    void putGetRemoveGetTest(void)
    {
        ham_page_t *page;
        ham_perm_page_union_t pers;
        memset(&pers, 0, sizeof(pers));
        ham_cache_t *cache=cache_new(m_env, 15);
        BFC_ASSERT(cache!=0);
        page=page_new(m_env);
        page_set_npers_flags(page, PAGE_NPERS_NO_HEADER);
        page_set_self(page, 0x123ull);
        page_set_pers(page, &pers);
        BFC_ASSERT(cache_put_page(cache, page)==HAM_SUCCESS);
        BFC_ASSERT(cache_get_cur_elements(cache)==1);
        BFC_ASSERT(cache_get_page(cache, 0x123ull, 0)==page);
        BFC_ASSERT(cache_get_cur_elements(cache)==0);
        BFC_ASSERT(cache_remove_page(cache, page)==HAM_SUCCESS);
        BFC_ASSERT(cache_get_cur_elements(cache)==0);
        BFC_ASSERT(cache_get_page(cache, 0x123ull, 0)==0);
        cache_delete(m_env, cache);
        page_set_pers(page, 0);
        page_delete(page);
    }
    
    void putGetReplaceTest(void)
    {
        ham_page_t *page1, *page2;
        ham_perm_page_union_t pers1, pers2;
        memset(&pers1, 0, sizeof(pers1));
        memset(&pers2, 0, sizeof(pers2));
        ham_cache_t *cache=cache_new(m_env, 15);
        BFC_ASSERT(cache!=0);
        page1=page_new(m_env);
        page_set_npers_flags(page1, PAGE_NPERS_NO_HEADER);
        page_set_self(page1, 0x123ull);
        page_set_pers(page1, &pers1);
        page2=page_new(m_env);
        page_set_npers_flags(page2, PAGE_NPERS_NO_HEADER);
        page_set_self(page2, 0x456ull);
        page_set_pers(page2, &pers2);
        BFC_ASSERT(cache_put_page(cache, page1)==HAM_SUCCESS);
        BFC_ASSERT(cache_get_cur_elements(cache)==1);
        BFC_ASSERT(cache_remove_page(cache, page1)==HAM_SUCCESS);
        BFC_ASSERT(cache_get_cur_elements(cache)==0);
        BFC_ASSERT(cache_put_page(cache, page2)==HAM_SUCCESS);
        BFC_ASSERT(cache_get_cur_elements(cache)==1);
        BFC_ASSERT(cache_get_page(cache, 0x123ull, 0)==0);
        BFC_ASSERT(cache_get_cur_elements(cache)==1);
        BFC_ASSERT(cache_get_page(cache, 0x456ull, 0)==page2);
        BFC_ASSERT(cache_get_cur_elements(cache)==0);
        cache_delete(m_env, cache);
        page_set_pers(page1, 0);
        page_delete(page1);
        page_set_pers(page2, 0);
        page_delete(page2);
    }
    
    void multiplePutTest(void)
    {
        ham_page_t *page[20];
        ham_perm_page_union_t pers[20];
        ham_cache_t *cache=cache_new(m_env, 15);

        for (int i=0; i<20; i++) {
            page[i]=page_new(m_env);
            memset(&pers[i], 0, sizeof(pers[i]));
            page_set_npers_flags(page[i], PAGE_NPERS_NO_HEADER);
            page_set_self(page[i], i*1024);
            page_set_pers(page[i], &pers[i]);
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
            page_set_pers(page[i], 0);
            page_delete(page[i]);
        }
        cache_delete(m_env, cache);
    }
    
    void negativeGetTest(void)
    {
        ham_cache_t *cache=cache_new(m_env, 15);
        for (int i=0; i<20; i++) {
            BFC_ASSERT(cache_get_page(cache, i*1024*13, 0)==0);
        }
        cache_delete(m_env, cache);
    }
    
    void unusedTest(void)
    {
        ham_page_t *page1, *page2;
        ham_perm_page_union_t pers1, pers2;
        memset(&pers1, 0, sizeof(pers1));
        memset(&pers2, 0, sizeof(pers2));
        ham_cache_t *cache=cache_new(m_env, 15);
        BFC_ASSERT(cache!=0);
        page1=page_new(m_env);
        page_set_npers_flags(page1, PAGE_NPERS_NO_HEADER);
        page_set_self(page1, 0x123ull);
        page_set_pers(page1, &pers1);
        page_add_ref(page1);
        page2=page_new(m_env);
        page_set_npers_flags(page2, PAGE_NPERS_NO_HEADER);
        page_set_self(page2, 0x456ull);
        page_set_pers(page2, &pers2);
        BFC_ASSERT(cache_put_page(cache, page1)==HAM_SUCCESS);
        BFC_ASSERT(cache_put_page(cache, page2)==HAM_SUCCESS);
        BFC_ASSERT(cache_get_unused_page(cache)==page2);
        BFC_ASSERT(cache_get_unused_page(cache)==0);
        BFC_ASSERT(cache_get_unused_page(cache)==0);
        BFC_ASSERT(cache_get_page(cache, 0x123ull, 0)==page1);
        BFC_ASSERT(cache_get_page(cache, 0x456ull, 0)==0);
        cache_delete(m_env, cache);
        page_release_ref(page1);
        page_set_pers(page1, 0);
        page_delete(page1);
        page_set_pers(page2, 0);
        page_delete(page2);
    }
    
    void overflowTest(void)
    {
        ham_cache_t *cache=cache_new(m_env, 15);
        ham_perm_page_union_t pers;
        memset(&pers, 0, sizeof(pers));
        std::vector<ham_page_t *> v;

        for (unsigned int i=0; i<cache_get_max_elements(cache)+10; i++) {
            ham_page_t *p=page_new(m_env);
            page_set_npers_flags(p, PAGE_NPERS_NO_HEADER);
            page_set_self(p, i*1024);
            page_set_pers(p, &pers);
            v.push_back(p);
            BFC_ASSERT(cache_put_page(cache, p)==0);
        }

        for (unsigned int i=0; i<=10; i++) {
            ham_page_t *p;
            BFC_ASSERT(cache_too_big(cache, 0));
            p=v.back();
            v.pop_back();
            BFC_ASSERT(cache_remove_page(cache, p)==HAM_SUCCESS);
            page_set_pers(p, 0);
            page_delete(p);
        }

        for (unsigned int i=0; i<cache_get_max_elements(cache)-1; i++) {
            ham_page_t *p;
            p=v.back();
            v.pop_back();
            BFC_ASSERT(!cache_too_big(cache, 0));
            BFC_ASSERT(cache_remove_page(cache, p)==HAM_SUCCESS);
            page_set_pers(p, 0);
            page_delete(p);
        }

        BFC_ASSERT(!cache_too_big(cache, 0));
        cache_delete(m_env, cache);
    }
};

BFC_REGISTER_FIXTURE(CacheTest);

