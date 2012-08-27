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
#include <string.h>
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/page.h"
#include "../src/device.h"
#include "../src/env.h"
#include "../src/txn.h"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;
using namespace ham;

class PageTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    PageTest(ham_bool_t inmemorydb=HAM_FALSE, ham_bool_t mmap=HAM_TRUE,
            const char *name="PageTest")
    :   hamsterDB_fixture(name),
        m_db(0), m_inmemory(inmemorydb), m_usemmap(mmap), m_dev(0)
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(PageTest, newDeleteTest);
        BFC_REGISTER_TEST(PageTest, allocFreeTest);
        BFC_REGISTER_TEST(PageTest, multipleAllocFreeTest);
        BFC_REGISTER_TEST(PageTest, fetchFlushTest);
    }

protected:
    ham_db_t *m_db;
    ham_env_t *m_env;
    ham_bool_t m_inmemory;
    ham_bool_t m_usemmap;
    ham_device_t *m_dev;

public:
    virtual void setup()
    {
        ham_u32_t flags=0;

        __super::setup();

        if (m_inmemory)
            flags|=HAM_IN_MEMORY;
        if (!m_usemmap)
            flags|=HAM_DISABLE_MMAP;

        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        BFC_ASSERT_EQUAL(0,
                ham_create_ex(m_db, BFC_OPATH(".test"),
                                flags, 0644, 0));
        m_env=ham_get_env(m_db);
    }

    virtual void teardown()
    {
        __super::teardown();

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
    }

    void newDeleteTest()
    {
        Page *page;
        page=new Page((Environment *)m_env);
        BFC_ASSERT(page!=0);
        delete page;
    }

    void allocFreeTest()
    {
        Page *page;
        page=new Page((Environment *)m_env);
        BFC_ASSERT_EQUAL(0, page->allocate());
        BFC_ASSERT_EQUAL(0, page->free());
        delete page;
    }

    void multipleAllocFreeTest()
    {
        int i;
        Page *page;
        ham_size_t ps=((Environment *)m_env)->get_pagesize();

        for (i=0; i<10; i++) {
            page=new Page((Environment *)m_env);
            BFC_ASSERT_EQUAL(0, page->allocate());
            /* i+2 since we need 1 page for the header page and one page
             * for the root page */
            if (!m_inmemory)
                BFC_ASSERT_EQUAL((i+2)*ps, page->get_self());
            BFC_ASSERT_EQUAL(0, page->free());
            delete page;
        }
    }

    void fetchFlushTest()
    {
        Page *page, *temp;
        ham_size_t ps=((Environment *)m_env)->get_pagesize();

        page=new Page((Environment *)m_env);
        temp=new Page((Environment *)m_env);
        BFC_ASSERT_EQUAL(0, page->allocate());
        BFC_ASSERT_EQUAL(ps*2, page->get_self());
        BFC_ASSERT_EQUAL(0, page->free());

        BFC_ASSERT_EQUAL(0, page->fetch(page->get_self()));
        memset(page->get_pers(), 0x13, ps);
        page->set_dirty(true);
        BFC_ASSERT_EQUAL(0, page->flush());

        BFC_ASSERT_EQUAL(false, page->is_dirty());
        BFC_ASSERT_EQUAL(0, temp->fetch(ps*2));
        BFC_ASSERT_EQUAL(0,
                memcmp(page->get_pers(), temp->get_pers(), ps));

        BFC_ASSERT_EQUAL(0, page->free());
        BFC_ASSERT_EQUAL(0, temp->free());

        delete temp;
        delete page;
    }

};

class RwPageTest : public PageTest
{
public:
    RwPageTest()
    : PageTest(HAM_FALSE, HAM_FALSE, "RwPageTest")
    {
        /* constructor will register all tests from parent page */
    }
};

class InMemoryPageTest : public PageTest
{
public:
    InMemoryPageTest()
    : PageTest(HAM_TRUE, HAM_FALSE, "InMemoryPageTest")
    {
        clear_tests(); // don't inherit tests
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(InMemoryPageTest, newDeleteTest);
        BFC_REGISTER_TEST(InMemoryPageTest, allocFreeTest);
        BFC_REGISTER_TEST(InMemoryPageTest, multipleAllocFreeTest);
    }
};

BFC_REGISTER_FIXTURE(PageTest);
BFC_REGISTER_FIXTURE(RwPageTest);
BFC_REGISTER_FIXTURE(InMemoryPageTest);

