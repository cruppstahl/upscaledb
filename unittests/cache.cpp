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
#include "../src/os.h"
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
        BFC_REGISTER_TEST(CacheTest, strictTest);
        BFC_REGISTER_TEST(CacheTest, setSizeEnvCreateTest);
        BFC_REGISTER_TEST(CacheTest, setSizeEnvOpenTest);
        BFC_REGISTER_TEST(CacheTest, setSizeDbCreateTest);
        BFC_REGISTER_TEST(CacheTest, setSizeDbOpenTest);
        BFC_REGISTER_TEST(CacheTest, bigSizeTest);
        BFC_REGISTER_TEST(CacheTest, bigSizePublicApiTest);
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
        env_set_allocator(m_env, (mem_allocator_t *)m_alloc);
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

        if (m_db) {
            ham_close(m_db, 0);
            ham_delete(m_db);
        }
        if (m_env) {
            ham_env_close(m_env, 0);
            ham_env_delete(m_env);
        }
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));
        m_db=0;
        m_env=0;
    }

    void newDeleteTest(void)
    {
        ham_cache_t *cache=cache_new(m_env, 15);
        BFC_ASSERT(cache!=0);
        cache_delete(cache);
    }

    void structureTest(void)
    {
        ham_cache_t *cache=cache_new(m_env, 15);
        BFC_ASSERT(cache!=0);
        BFC_ASSERT(cache_get_capacity(cache)==15);
        BFC_ASSERT(cache_get_env(cache)==m_env);
        cache_set_cur_elements(cache, 12);
        BFC_ASSERT(cache_get_cur_elements(cache)==12);
        cache_set_bucketsize(cache, 11);
        BFC_ASSERT(cache_get_bucketsize(cache)==11);
        BFC_ASSERT(cache_get_totallist(cache)==0);
        BFC_ASSERT(cache_get_unused_page(cache)==0);
        BFC_ASSERT(cache_get_page(cache, 0x123ull, 0)==0);
        BFC_ASSERT(cache_too_big(cache)==1);
        cache_delete(cache);
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
        cache_put_page(cache, page);
        BFC_ASSERT_EQUAL(page, cache_get_page(cache, 0x123ull, 0));
        cache_delete(cache);
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
        cache_put_page(cache, page);
        BFC_ASSERT_EQUAL(1u, cache_get_cur_elements(cache));
        BFC_ASSERT(cache_get_page(cache, 0x123ull, 0)==page);
        BFC_ASSERT(cache_get_cur_elements(cache)==0);
        cache_remove_page(cache, page);
        BFC_ASSERT(cache_get_cur_elements(cache)==0);
        BFC_ASSERT(cache_get_page(cache, 0x123ull, 0)==0);
        cache_delete(cache);
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
        cache_put_page(cache, page1);
        BFC_ASSERT(cache_get_cur_elements(cache)==1);
        cache_remove_page(cache, page1);
        BFC_ASSERT(cache_get_cur_elements(cache)==0);
        cache_put_page(cache, page2);
        BFC_ASSERT(cache_get_cur_elements(cache)==1);
        BFC_ASSERT(cache_get_page(cache, 0x123ull, 0)==0);
        BFC_ASSERT(cache_get_cur_elements(cache)==1);
        BFC_ASSERT(cache_get_page(cache, 0x456ull, 0)==page2);
        BFC_ASSERT(cache_get_cur_elements(cache)==0);
        cache_delete(cache);
        page_set_pers(page1, 0);
        page_delete(page1);
        page_set_pers(page2, 0);
        page_delete(page2);
    }
    
    void multiplePutTest(void)
    {
        const int MAX=20;
        ham_page_t *page[MAX];
        ham_perm_page_union_t pers[MAX];
        ham_cache_t *cache=cache_new(m_env, 15);

        for (int i=0; i<MAX; i++) {
            page[i]=page_new(m_env);
            memset(&pers[i], 0, sizeof(pers[i]));
            page_set_npers_flags(page[i], PAGE_NPERS_NO_HEADER);
            page_set_self(page[i], (i+1)*1024);
            page_set_pers(page[i], &pers[i]);
            cache_put_page(cache, page[i]);
        }
        for (int i=0; i<MAX; i++) {
            BFC_ASSERT_EQUAL(page[i], 
                    cache_get_page(cache, (i+1)*1024, 0));
        }
        for (int i=0; i<MAX; i++) {
            cache_remove_page(cache, page[i]);
        }
        for (int i=0; i<MAX; i++) {
            BFC_ASSERT_EQUAL((ham_page_t *)0, 
                    cache_get_page(cache, (i+1)*1024, 0));
            page_set_pers(page[i], 0);
            page_delete(page[i]);
        }
        cache_delete(cache);
    }
    
    void negativeGetTest(void)
    {
        ham_cache_t *cache=cache_new(m_env, 15);
        for (int i=0; i<20; i++) {
            BFC_ASSERT(cache_get_page(cache, i*1024*13, 0)==0);
        }
        cache_delete(cache);
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

        cache_put_page(cache, page1);
        cache_put_page(cache, page2);
        BFC_ASSERT_EQUAL(page2, cache_get_unused_page(cache));
        BFC_ASSERT_EQUAL((ham_page_t *)0, cache_get_unused_page(cache));
        page_release_ref(page1);
        BFC_ASSERT_EQUAL(page1, cache_get_page(cache, 0x123ull, CACHE_NOREMOVE));
        BFC_ASSERT_EQUAL((ham_page_t *)0, cache_get_page(cache, 0x456ull, 0));
        BFC_ASSERT_EQUAL(page1, cache_get_unused_page(cache));
        BFC_ASSERT_EQUAL((ham_page_t *)0, cache_get_page(cache, 0x123ull, 0));
        cache_delete(cache);
        page_set_pers(page1, 0);
        page_delete(page1);
        page_set_pers(page2, 0);
        page_delete(page2);
    }
    
    void overflowTest(void)
    {
        ham_cache_t *cache=cache_new(m_env, 15*os_get_pagesize());
        ham_perm_page_union_t pers;
        memset(&pers, 0, sizeof(pers));
        std::vector<ham_page_t *> v;

        for (unsigned int i=0; i<15; i++) {
            ham_page_t *p=page_new(m_env);
            page_set_npers_flags(p, PAGE_NPERS_NO_HEADER);
            page_set_self(p, i*1024);
            page_set_pers(p, &pers);
            v.push_back(p);
            cache_put_page(cache, p);
            BFC_ASSERT(!cache_too_big(cache));
        }

        for (unsigned int i=0; i<5; i++) {
            ham_page_t *p=page_new(m_env);
            page_set_npers_flags(p, PAGE_NPERS_NO_HEADER);
            page_set_self(p, i*1024);
            page_set_pers(p, &pers);
            v.push_back(p);
            cache_put_page(cache, p);
            BFC_ASSERT(cache_too_big(cache)); // now it's too big
        }

        for (unsigned int i=0; i<5; i++) {
            ham_page_t *p;
            BFC_ASSERT(cache_too_big(cache));
            p=v.back();
            v.pop_back();
            cache_remove_page(cache, p);
            page_set_pers(p, 0);
            page_delete(p);
        }

        for (unsigned int i=0; i<15; i++) {
            ham_page_t *p;
            p=v.back();
            v.pop_back();
            cache_remove_page(cache, p);
            BFC_ASSERT(!cache_too_big(cache));
            page_set_pers(p, 0);
            page_delete(p);
        }

        BFC_ASSERT(!cache_too_big(cache));
        cache_delete(cache);
    }

    void strictTest(void)
    {
        ham_env_close(m_env, 0);
        ham_close(m_db, 0);

        ham_parameter_t param[]={
            {HAM_PARAM_PAGESIZE,  1024*128}, 
            {0, 0}};

        ham_page_t *p[1024];
        ham_db_t *db;
        BFC_ASSERT_EQUAL(0, ham_new(&db));
        BFC_ASSERT_EQUAL(0, 
                ham_create_ex(db, ".test", HAM_CACHE_STRICT, 0644, &param[0]));
        ham_env_t *env=db_get_env(db);
        ham_cache_t *cache=env_get_cache(env);

        BFC_ASSERT_EQUAL(cache_get_capacity(cache), 1024*1024*2u);

        unsigned int max_pages=HAM_DEFAULT_CACHESIZE/(1024*128);
        unsigned int i;
        for (i=0; i<max_pages+1; i++) {
            BFC_ASSERT_EQUAL(0, db_alloc_page(&p[i], db, 0, 0));
            page_add_ref(p[i]);
        }

        BFC_ASSERT_EQUAL(HAM_CACHE_FULL, db_alloc_page(&p[i], db, 0, 0));

        for (i=0; i<max_pages+1; i++) {
            page_release_ref(p[i]);
        }

        BFC_ASSERT_EQUAL(0, db_alloc_page(&p[i], db, 0, 0));

        ham_close(db, 0);
        ham_delete(db);
    }

    void setSizeEnvCreateTest(void)
    {
        ham_env_t *env;
        ham_parameter_t param[]={
            {HAM_PARAM_CACHESIZE, 100*1024}, 
            {HAM_PARAM_PAGESIZE,  1024}, 
            {0, 0}};

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create_ex(env, ".test.db", 0, 0644, &param[0]));
        ham_cache_t *cache=env_get_cache(env);

        BFC_ASSERT_EQUAL(100*1024u, cache_get_capacity(cache));

        ham_env_close(env, 0);
        ham_env_delete(env);
    }

    void setSizeEnvOpenTest(void)
    {
        ham_env_t *env;
        ham_parameter_t param[]={
            {HAM_PARAM_CACHESIZE, 100*1024}, 
            {0, 0}};

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create_ex(env, ".test.db", 0, 0644, &param[0]));
        ham_env_close(env, 0);
        BFC_ASSERT_EQUAL(0, 
                ham_env_open_ex(env, ".test.db", 0, &param[0]));
        ham_cache_t *cache=env_get_cache(env);

        BFC_ASSERT_EQUAL(100*1024u, cache_get_capacity(cache));

        ham_env_close(env, 0);
        ham_env_delete(env);
    }

    void setSizeDbCreateTest(void)
    {
        ham_db_t *db;
        ham_parameter_t param[]={
            {HAM_PARAM_CACHESIZE, 100*1024}, 
            {HAM_PARAM_PAGESIZE,  1024}, 
            {0, 0}};

        BFC_ASSERT_EQUAL(0, ham_new(&db));
        BFC_ASSERT_EQUAL(0, ham_create_ex(db, ".test.db", 0, 0644, &param[0]));
        ham_env_t *env=db_get_env(db);
        ham_cache_t *cache=env_get_cache(env);

        BFC_ASSERT_EQUAL(100*1024u, cache_get_capacity(cache));

        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        ham_delete(db);
    }

    void setSizeDbOpenTest(void)
    {
        ham_db_t *db;
        ham_parameter_t param[]={
            {HAM_PARAM_CACHESIZE, 100*1024}, 
            {0, 0}};

        BFC_ASSERT_EQUAL(0, ham_new(&db));
        BFC_ASSERT_EQUAL(0, ham_create_ex(db, ".test.db", 0, 0644, &param[0]));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        BFC_ASSERT_EQUAL(0, ham_open_ex(db, ".test.db", 0, &param[0]));
        ham_env_t *env=db_get_env(db);
        ham_cache_t *cache=env_get_cache(env);

        BFC_ASSERT_EQUAL(100*1024u, cache_get_capacity(cache));

        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        ham_delete(db);
    }

    void bigSizeTest(void)
    {
        ham_u64_t size=1024ull*1024ull*1024ull*16ull;
        ham_cache_t *cache=cache_new(m_env, size);
        BFC_ASSERT(cache!=0);
        BFC_ASSERT_EQUAL(size, cache_get_capacity(cache));
        BFC_ASSERT_EQUAL(m_env, cache_get_env(cache));
        cache_set_cur_elements(cache, 12);
        BFC_ASSERT_EQUAL(12ull, cache_get_cur_elements(cache));
        cache_set_bucketsize(cache, 11);
        BFC_ASSERT_EQUAL(11ull, cache_get_bucketsize(cache));
        BFC_ASSERT(cache_get_totallist(cache)==0);
        BFC_ASSERT(cache_get_unused_page(cache)==0);
        BFC_ASSERT(cache_get_page(cache, 0x123ull, 0)==0);
        cache_delete(cache);
    }

    void bigSizePublicApiTest(void)
    {
        teardown();

        ham_u64_t size=1024ull*1024ull*1024ull*16ull;
        ham_env_t *env;
        ham_env_new(&env);
        ham_parameter_t params[]={
            {HAM_PARAM_CACHESIZE, size},
            {0, 0}
        };
        BFC_ASSERT_EQUAL(0, 
                ham_env_create_ex(env, BFC_OPATH(".test"), 0, 
                    0644, &params[0]));

        ham_cache_t *cache=env_get_cache(env);
        BFC_ASSERT(cache!=0);
        BFC_ASSERT_EQUAL(size, cache_get_capacity(cache));

        params[0].value=0;
        BFC_ASSERT_EQUAL(0, ham_env_get_parameters(env, params));
        BFC_ASSERT_EQUAL(size, params[0].value);
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        ham_env_delete(env);
    }
};

BFC_REGISTER_FIXTURE(CacheTest);

