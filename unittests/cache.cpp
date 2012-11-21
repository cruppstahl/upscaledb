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

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;
using namespace ham;

class CacheTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    CacheTest()
    : hamsterDB_fixture("CacheTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(CacheTest, newDeleteTest);
        BFC_REGISTER_TEST(CacheTest, putGetTest);
        BFC_REGISTER_TEST(CacheTest, putGetRemoveGetTest);
        BFC_REGISTER_TEST(CacheTest, putGetReplaceTest);
        BFC_REGISTER_TEST(CacheTest, multiplePutTest);
        BFC_REGISTER_TEST(CacheTest, negativeGetTest);
        BFC_REGISTER_TEST(CacheTest, overflowTest);
        BFC_REGISTER_TEST(CacheTest, strictTest);
        BFC_REGISTER_TEST(CacheTest, setSizeEnvCreateTest);
        BFC_REGISTER_TEST(CacheTest, setSizeEnvOpenTest);
        BFC_REGISTER_TEST(CacheTest, setSizeDbCreateTest);
        BFC_REGISTER_TEST(CacheTest, setSizeDbOpenTest);
        BFC_REGISTER_TEST(CacheTest, bigSizeTest);
    }

protected:
    ham_db_t *m_db;
    ham_env_t *m_env;

public:
    virtual void setup()
    {
        __super::setup();

        BFC_ASSERT_EQUAL(0, ham_env_new(&m_env));
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        BFC_ASSERT_EQUAL(0,
                ham_env_create(m_env, BFC_OPATH(".test"),
                        HAM_ENABLE_TRANSACTIONS
                        | HAM_ENABLE_RECOVERY, 0644, 0));
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
    }

    void newDeleteTest(void)
    {
        Cache *cache=new Cache((Environment *)m_env, 15);
        BFC_ASSERT(cache!=0);
        delete cache;
    }

    void putGetTest(void)
    {
        Page *page;
        PageData pers;
        memset(&pers, 0, sizeof(pers));
        Cache *cache=new Cache((Environment *)m_env, 15);
        BFC_ASSERT(cache!=0);
        page=new Page((Environment *)m_env);
        page->set_self(0x123ull);
        page->set_pers(&pers);
        page->set_flags(Page::NPERS_NO_HEADER);
        cache->put_page(page);
        cache->get_page(0x123ull, 0);
        delete cache;
        page->set_pers(0);
        delete page;
    }

    void putGetRemoveGetTest(void)
    {
        Page *page;
        PageData pers;
        memset(&pers, 0, sizeof(pers));
        Cache *cache=new Cache((Environment *)m_env, 15);
        BFC_ASSERT(cache!=0);
        page=new Page((Environment *)m_env);
        page->set_flags(Page::NPERS_NO_HEADER);
        page->set_self(0x123ull);
        page->set_pers(&pers);
        cache->put_page(page);
        BFC_ASSERT(cache->get_cur_elements()==1);
        BFC_ASSERT(cache->get_page(0x123ull, 0)==page);
        BFC_ASSERT(cache->get_cur_elements()==0);
        cache->remove_page(page);
        BFC_ASSERT(cache->get_cur_elements()==0);
        BFC_ASSERT(cache->get_page(0x123ull, 0)==0);
        delete cache;
        page->set_pers(0);
        delete page;
    }

    void putGetReplaceTest(void)
    {
        Page *page1, *page2;
        PageData pers1, pers2;
        memset(&pers1, 0, sizeof(pers1));
        memset(&pers2, 0, sizeof(pers2));
        Cache *cache=new Cache((Environment *)m_env, 15);
        BFC_ASSERT(cache!=0);
        page1=new Page((Environment *)m_env);
        page1->set_flags(Page::NPERS_NO_HEADER);
        page1->set_self(0x123ull);
        page1->set_pers(&pers1);
        page2=new Page((Environment *)m_env);
        page2->set_flags(Page::NPERS_NO_HEADER);
        page2->set_self(0x456ull);
        page2->set_pers(&pers2);
        cache->put_page(page1);
        BFC_ASSERT(cache->get_cur_elements()==1);
        cache->remove_page(page1);
        BFC_ASSERT(cache->get_cur_elements()==0);
        cache->put_page(page2);
        BFC_ASSERT(cache->get_cur_elements()==1);
        BFC_ASSERT(cache->get_page(0x123ull, 0)==0);
        BFC_ASSERT(cache->get_cur_elements()==1);
        BFC_ASSERT(cache->get_page(0x456ull, 0)==page2);
        BFC_ASSERT(cache->get_cur_elements()==0);
        delete cache;
        page1->set_pers(0);
        delete page1;
        page2->set_pers(0);
        delete page2;
    }

    void multiplePutTest(void)
    {
        Page *page[20];
        PageData pers[20];
        Cache *cache=new Cache((Environment *)m_env, 15);

        for (int i=0; i<20; i++) {
            page[i]=new Page((Environment *)m_env);
            memset(&pers[i], 0, sizeof(pers[i]));
            page[i]->set_flags(Page::NPERS_NO_HEADER);
            page[i]->set_self((i+1)*1024);
            page[i]->set_pers(&pers[i]);
            cache->put_page(page[i]);
        }
        for (int i=0; i<20; i++) {
            BFC_ASSERT(cache->get_page((i+1)*1024, 0)==page[i]);
        }
        for (int i=0; i<20; i++) {
            cache->remove_page(page[i]);
        }
        for (int i=0; i<20; i++) {
            BFC_ASSERT(cache->get_page((i+1)*1024, 0)==0);
            page[i]->set_pers(0);
            delete page[i];
        }
        delete cache;
    }

    void negativeGetTest(void)
    {
        Cache *cache=new Cache((Environment *)m_env, 15);
        for (int i=0; i<20; i++) {
            BFC_ASSERT(cache->get_page(i*1024*13, 0)==0);
        }
        delete cache;
    }

    void overflowTest(void)
    {
        Cache *cache=new Cache((Environment *)m_env, 15*os_get_pagesize());
        PageData pers;
        memset(&pers, 0, sizeof(pers));
        std::vector<Page *> v;

        for (unsigned int i=0; i<15; i++) {
            Page *p=new Page((Environment *)m_env);
            p->set_flags(Page::NPERS_NO_HEADER);
            p->set_self((i+1)*1024);
            p->set_pers(&pers);
            v.push_back(p);
            cache->put_page(p);
            BFC_ASSERT(!cache->is_too_big());
        }

        for (unsigned int i=0; i<5; i++) {
            Page *p=new Page((Environment *)m_env);
            p->set_flags(Page::NPERS_NO_HEADER);
            p->set_self((i+1)*1024);
            p->set_pers(&pers);
            v.push_back(p);
            cache->put_page(p);
            BFC_ASSERT(cache->is_too_big());
        }

        for (unsigned int i=0; i<5; i++) {
            Page *p;
            BFC_ASSERT(cache->is_too_big());
            p=v.back();
            v.pop_back();
            cache->remove_page(p);
            p->set_pers(0);
            delete p;
        }

        for (unsigned int i=0; i<15; i++) {
            Page *p;
            p=v.back();
            v.pop_back();
            cache->remove_page(p);
            BFC_ASSERT(!cache->is_too_big());
            p->set_pers(0);
            delete p;
        }

        BFC_ASSERT(!cache->is_too_big());
        delete cache;
    }

    void strictTest(void)
    {
        ham_env_close(m_env, 0);
        ham_close(m_db, 0);

        ham_parameter_t param[]={
            {HAM_PARAM_PAGESIZE,  1024*128},
            {0, 0}};

        Page *p[1024];
        ham_db_t *db;
        BFC_ASSERT_EQUAL(0, ham_new(&db));
        BFC_ASSERT_EQUAL(0,
                ham_create_ex(db, ".test", HAM_CACHE_STRICT, 0644, &param[0]));
        ham_env_t *env=ham_get_env(db);
        Cache *cache=((Environment *)env)->get_cache();

        BFC_ASSERT_EQUAL(cache->get_capacity(), 1024*1024*2u);

        unsigned int max_pages=HAM_DEFAULT_CACHESIZE/(1024*128);
        unsigned int i;
        for (i=0; i<max_pages; i++)
            BFC_ASSERT_EQUAL(0, db_alloc_page(&p[i], (Database *)db, 0, 0));

        BFC_ASSERT_EQUAL(HAM_CACHE_FULL, db_alloc_page(&p[i], (Database *)db, 0, 0));
        BFC_ASSERT_EQUAL(0, env_purge_cache((Environment *)ham_get_env(db)));
        BFC_ASSERT_EQUAL(0, db_alloc_page(&p[i], (Database *)db, 0, 0));

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
                ham_env_create(env, ".test.db", 0, 0644, &param[0]));
        Cache *cache=((Environment *)env)->get_cache();

        BFC_ASSERT_EQUAL(100*1024u, cache->get_capacity());

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
                ham_env_create(env, ".test.db", 0, 0644, &param[0]));
        ham_env_close(env, 0);
        BFC_ASSERT_EQUAL(0,
                ham_env_open(env, ".test.db", 0, &param[0]));
        Cache *cache=((Environment *)env)->get_cache();

        BFC_ASSERT_EQUAL(100*1024u, cache->get_capacity());

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
        ham_env_t *env=ham_get_env(db);
        Cache *cache=((Environment *)env)->get_cache();

        BFC_ASSERT_EQUAL(100*1024u, cache->get_capacity());

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
        ham_env_t *env=ham_get_env(db);
        Cache *cache=((Environment *)env)->get_cache();

        BFC_ASSERT_EQUAL(100*1024u, cache->get_capacity());

        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        ham_delete(db);
    }

    void bigSizeTest(void)
    {
        ham_u64_t size=1024ull*1024ull*1024ull*16ull;
        Cache *cache=new Cache((Environment *)m_env, size);
        BFC_ASSERT(cache!=0);
        BFC_ASSERT_EQUAL(size, cache->get_capacity());
        delete cache;
    }
};

BFC_REGISTER_FIXTURE(CacheTest);

