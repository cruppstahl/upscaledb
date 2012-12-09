/**
 * Copyright (C) 2005-2012 Christoph Rupp (chris@crupp.de).
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

class CacheTest : public hamsterDB_fixture {
  define_super(hamsterDB_fixture);

public:
  CacheTest()
    : hamsterDB_fixture("CacheTest") {
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
  virtual void setup() {
    __super::setup();

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"), 0, 0644, 0));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 13, HAM_ENABLE_DUPLICATES, 0));
  }

  virtual void teardown() {
    __super::teardown();

    ham_env_close(m_env, HAM_AUTO_CLEANUP);
  }

  void newDeleteTest() {
    Cache *cache = new Cache((Environment *)m_env, 15);
    BFC_ASSERT(cache != 0);
    delete cache;
  }

  void putGetTest() {
    Page *page;
    PageData pers;
    memset(&pers, 0, sizeof(pers));
    Cache *cache = new Cache((Environment *)m_env, 15);
    page = new Page((Environment *)m_env);
    page->set_self(0x123ull);
    page->set_pers(&pers);
    page->set_flags(Page::NPERS_NO_HEADER);
    cache->put_page(page);
    cache->get_page(0x123ull, 0);
    delete cache;
    page->set_pers(0);
    delete page;
  }

  void putGetRemoveGetTest() {
    Page *page;
    PageData pers;
    memset(&pers, 0, sizeof(pers));
    Cache *cache = new Cache((Environment *)m_env, 15);
    page = new Page((Environment *)m_env);
    page->set_flags(Page::NPERS_NO_HEADER);
    page->set_self(0x123ull);
    page->set_pers(&pers);
    cache->put_page(page);
    BFC_ASSERT_EQUAL(1u, cache->get_cur_elements());
    BFC_ASSERT_EQUAL(page, cache->get_page(0x123ull, 0));
    BFC_ASSERT_EQUAL(0u, cache->get_cur_elements());
    cache->remove_page(page);
    BFC_ASSERT_EQUAL(0u, cache->get_cur_elements());
    BFC_ASSERT_EQUAL((Page *)0, cache->get_page(0x123ull));
    delete cache;
    page->set_pers(0);
    delete page;
  }

  void putGetReplaceTest() {
    Page *page1, *page2;
    PageData pers1, pers2;
    memset(&pers1, 0, sizeof(pers1));
    memset(&pers2, 0, sizeof(pers2));
    Cache *cache = new Cache((Environment *)m_env, 15);
    page1 = new Page((Environment *)m_env);
    page1->set_flags(Page::NPERS_NO_HEADER);
    page1->set_self(0x123ull);
    page1->set_pers(&pers1);
    page2 = new Page((Environment *)m_env);
    page2->set_flags(Page::NPERS_NO_HEADER);
    page2->set_self(0x456ull);
    page2->set_pers(&pers2);
    cache->put_page(page1);
    BFC_ASSERT_EQUAL(1u, cache->get_cur_elements());
    cache->remove_page(page1);
    BFC_ASSERT_EQUAL(0u, cache->get_cur_elements());
    cache->put_page(page2);
    BFC_ASSERT_EQUAL(1u, cache->get_cur_elements());
    BFC_ASSERT_EQUAL((Page *)0, cache->get_page(0x123ull));
    BFC_ASSERT_EQUAL(1u, cache->get_cur_elements());
    BFC_ASSERT_EQUAL(page2, cache->get_page(0x456ull));
    BFC_ASSERT_EQUAL(0u, cache->get_cur_elements());
    delete cache;
    page1->set_pers(0);
    delete page1;
    page2->set_pers(0);
    delete page2;
  }

  void multiplePutTest() {
    Page *page[20];
    PageData pers[20];
    Cache *cache = new Cache((Environment *)m_env, 15);

    for (int i = 0; i < 20; i++) {
      page[i] = new Page((Environment *)m_env);
      memset(&pers[i], 0, sizeof(pers[i]));
      page[i]->set_flags(Page::NPERS_NO_HEADER);
      page[i]->set_self((i + 1) * 1024);
      page[i]->set_pers(&pers[i]);
      cache->put_page(page[i]);
    }
    for (int i = 0; i < 20; i++)
      BFC_ASSERT_EQUAL(page[i], cache->get_page((i + 1) * 1024, 0));
    for (int i = 0; i < 20; i++)
      cache->remove_page(page[i]);
    for (int i = 0; i < 20; i++) {
      BFC_ASSERT_EQUAL((Page *)0, cache->get_page((i + 1) * 1024));
      page[i]->set_pers(0);
      delete page[i];
    }
    delete cache;
  }

  void negativeGetTest() {
    Cache *cache = new Cache((Environment *)m_env, 15);
    for (int i = 0; i < 20; i++)
      BFC_ASSERT_EQUAL((Page *)0, cache->get_page(i * 1024 * 13));
    delete cache;
  }

  void overflowTest() {
    Cache *cache = new Cache((Environment *)m_env, 15 * os_get_pagesize());
    PageData pers;
    memset(&pers, 0, sizeof(pers));
    std::vector<Page *> v;

    for (unsigned int i = 0; i < 15; i++) {
      Page *p = new Page((Environment *)m_env);
      p->set_flags(Page::NPERS_NO_HEADER);
      p->set_self((i + 1) * 1024);
      p->set_pers(&pers);
      v.push_back(p);
      cache->put_page(p);
      BFC_ASSERT_EQUAL(false, cache->is_too_big());
    }

    for (unsigned int i = 0; i < 5; i++) {
      Page *p = new Page((Environment *)m_env);
      p->set_flags(Page::NPERS_NO_HEADER);
      p->set_self((i + 1) * 1024);
      p->set_pers(&pers);
      v.push_back(p);
      cache->put_page(p);
      BFC_ASSERT(cache->is_too_big());
    }

    for (unsigned int i = 0; i < 5; i++) {
      Page *p;
      BFC_ASSERT(cache->is_too_big());
      p = v.back();
      v.pop_back();
      cache->remove_page(p);
      p->set_pers(0);
      delete p;
    }

    for (unsigned int i = 0; i < 15; i++) {
      Page *p;
      p = v.back();
      v.pop_back();
      cache->remove_page(p);
      BFC_ASSERT(!cache->is_too_big());
      p->set_pers(0);
      delete p;
    }

    BFC_ASSERT(!cache->is_too_big());
    delete cache;
  }

  void strictTest() {
    teardown();

    ham_parameter_t param[] = {
      { HAM_PARAM_PAGESIZE, 1024 * 128 },
      { 0, 0 }
    };

    Page *p[1024];

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),  
            HAM_CACHE_STRICT, 0644, &param[0]));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 13, 0, 0));

    Cache *cache = ((Environment *)m_env)->get_cache();

    BFC_ASSERT_EQUAL(cache->get_capacity(), 1024 * 1024 * 2u);

    unsigned int max_pages = HAM_DEFAULT_CACHESIZE / (1024 * 128);
    unsigned int i;
    for (i = 0; i < max_pages; i++)
      BFC_ASSERT_EQUAL(0, ((Database *)m_db)->alloc_page(&p[i], 0, 0));

    BFC_ASSERT_EQUAL(HAM_CACHE_FULL,
        ((Database *)m_db)->alloc_page(&p[i], 0, 0));
    BFC_ASSERT_EQUAL(0, env_purge_cache((Environment *)m_env));
    BFC_ASSERT_EQUAL(0, ((Database *)m_db)->alloc_page(&p[i], 0, 0));
  }

  void setSizeEnvCreateTest() {
    ham_parameter_t param[] = {
      { HAM_PARAM_CACHESIZE, 100 * 1024 },
      { HAM_PARAM_PAGESIZE,  1024 },
      { 0, 0 }
    };

    teardown();

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),  
            HAM_CACHE_STRICT, 0644, &param[0]));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 13, 0, 0));

    Cache *cache = ((Environment *)m_env)->get_cache();

    BFC_ASSERT_EQUAL(100 * 1024u, cache->get_capacity());
  }

  void setSizeEnvOpenTest() {
    ham_parameter_t param[] = {
      { HAM_PARAM_CACHESIZE, 100 * 1024 },
      { 0, 0 }
    };

    teardown();

    BFC_ASSERT_EQUAL(0,
        ham_env_open(&m_env, BFC_OPATH(".test.db"), 0, &param[0]));

    Cache *cache = ((Environment *)m_env)->get_cache();

    BFC_ASSERT_EQUAL(100 * 1024u, cache->get_capacity());
  }

  void setSizeDbCreateTest() {
    ham_parameter_t param[] = {
      { HAM_PARAM_CACHESIZE, 100 * 1024 },
      { HAM_PARAM_PAGESIZE, 1024 },
      { 0, 0 }
    };

    teardown();

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),  
            HAM_CACHE_STRICT, 0644, &param[0]));
    BFC_ASSERT_EQUAL(0,
        ham_env_create_db(m_env, &m_db, 13, 0, 0));

    Cache *cache = ((Environment *)m_env)->get_cache();

    BFC_ASSERT_EQUAL(100 * 1024u, cache->get_capacity());
  }

  void setSizeDbOpenTest() {
    ham_parameter_t param[] = {
      { HAM_PARAM_CACHESIZE, 100 * 1024 },
      { 0, 0 }
    };

    teardown();

    BFC_ASSERT_EQUAL(0,
        ham_env_create(&m_env, BFC_OPATH(".test"),  
            0, 0644, &param[0]));
    teardown();

    BFC_ASSERT_EQUAL(0,
        ham_env_open(&m_env, BFC_OPATH(".test.db"), 0, &param[0]));

    Cache *cache = ((Environment *)m_env)->get_cache();

    BFC_ASSERT_EQUAL(100 * 1024u, cache->get_capacity());
  }

  void bigSizeTest() {
    ham_u64_t size = 1024ull * 1024ull * 1024ull * 16ull;
    Cache *cache = new Cache((Environment *)m_env, size);
    BFC_ASSERT(cache != 0);
    BFC_ASSERT_EQUAL(size, cache->get_capacity());
    delete cache;
  }
};

BFC_REGISTER_FIXTURE(CacheTest);

