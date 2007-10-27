/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or 
 * (at your option) any later version.
 *
 * See file COPYING.GPL2 and COPYING.GPL3 for License information.
 */

#include <stdexcept>
#include <cppunit/extensions/HelperMacros.h>
#include <ham/hamsterdb_int.h>
#include "memtracker.h"
#include "../src/db.h"
#include "os.hpp"

typedef struct simple_filter_t
{
    int written;
    int read;
    int closed;
} simple_filter_t;

static ham_status_t
my_pre_cb(ham_db_t *, ham_page_filter_t *filter, ham_u8_t *, ham_size_t)
{
    simple_filter_t *sf=(simple_filter_t *)filter->userdata;
    sf->written++;
    return (0);
}

static ham_status_t
my_post_cb(ham_db_t *, ham_page_filter_t *filter, ham_u8_t *, ham_size_t)
{
    simple_filter_t *sf=(simple_filter_t *)filter->userdata;
    sf->read++;
    return (0);
}

static void
my_close_cb(ham_db_t *, ham_page_filter_t *filter)
{
    simple_filter_t *sf=(simple_filter_t *)filter->userdata;
    sf->closed++;
}

class PageFilterTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(PageFilterTest);
    CPPUNIT_TEST      (addRemoveTest);
    CPPUNIT_TEST      (simpleFilterTest);
    CPPUNIT_TEST      (aesFilterTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_db_t *m_db;
    ham_u32_t m_flags;
    memtracker_t *m_alloc;

public:
    void setUp()
    { 
        m_flags=0;

        os::unlink(".test");
        CPPUNIT_ASSERT((m_alloc=memtracker_new())!=0);
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
    }
    
    void tearDown() 
    { 
        ham_delete(m_db);
        CPPUNIT_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void addRemoveTest()
    {
        ham_page_filter_t filter1, filter2, filter3;
        memset(&filter1, 0, sizeof(filter1));
        memset(&filter2, 0, sizeof(filter2));
        memset(&filter3, 0, sizeof(filter3));

        CPPUNIT_ASSERT_EQUAL(0, ham_add_page_filter(m_db, &filter1));
        CPPUNIT_ASSERT(filter1._next==0);
        CPPUNIT_ASSERT(filter1._prev==0);
        CPPUNIT_ASSERT_EQUAL(&filter1, db_get_page_filter(m_db));

        CPPUNIT_ASSERT_EQUAL(0, ham_add_page_filter(m_db, &filter2));
        CPPUNIT_ASSERT(filter1._next==&filter2);
        CPPUNIT_ASSERT(filter2._prev==&filter1);
        CPPUNIT_ASSERT(filter1._prev==0);
        CPPUNIT_ASSERT(filter2._next==0);
        CPPUNIT_ASSERT_EQUAL(&filter1, db_get_page_filter(m_db));

        CPPUNIT_ASSERT_EQUAL(0, ham_add_page_filter(m_db, &filter3));
        CPPUNIT_ASSERT(filter1._next==&filter2);
        CPPUNIT_ASSERT(filter2._prev==&filter1);
        CPPUNIT_ASSERT(filter2._next==&filter3);
        CPPUNIT_ASSERT(filter3._prev==&filter2);
        CPPUNIT_ASSERT(filter1._prev==0);
        CPPUNIT_ASSERT(filter3._next==0);
        CPPUNIT_ASSERT_EQUAL(&filter1, db_get_page_filter(m_db));

        CPPUNIT_ASSERT_EQUAL(0, ham_remove_page_filter(m_db, &filter2));
        CPPUNIT_ASSERT(filter1._next==&filter3);
        CPPUNIT_ASSERT(filter3._prev==&filter1);
        CPPUNIT_ASSERT(filter1._prev==0);
        CPPUNIT_ASSERT(filter3._next==0);
        CPPUNIT_ASSERT_EQUAL(&filter1, db_get_page_filter(m_db));

        CPPUNIT_ASSERT_EQUAL(0, ham_remove_page_filter(m_db, &filter3));
        CPPUNIT_ASSERT(filter1._prev==0);
        CPPUNIT_ASSERT(filter1._next==0);
        CPPUNIT_ASSERT_EQUAL(&filter1, db_get_page_filter(m_db));

        CPPUNIT_ASSERT_EQUAL(0, ham_remove_page_filter(m_db, &filter1));
        CPPUNIT_ASSERT(0==db_get_page_filter(m_db));

        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, ".test", m_flags, 0644));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void simpleFilterTest()
    {
        simple_filter_t sf;
        ham_page_filter_t filter;
        memset(&filter, 0, sizeof(filter));
        memset(&sf, 0, sizeof(sf));
        filter.userdata=(void *)&sf;
        filter.pre_cb=my_pre_cb;
        filter.post_cb=my_post_cb;
        filter.close_cb=my_close_cb;

        CPPUNIT_ASSERT_EQUAL(0, ham_add_page_filter(m_db, &filter));

        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, ".test", m_flags, 0644));

        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));

        CPPUNIT_ASSERT_EQUAL(1, sf.written);
        CPPUNIT_ASSERT_EQUAL(1, sf.read);
        CPPUNIT_ASSERT_EQUAL(1, sf.closed);

        memset(&sf, 0, sizeof(sf));
        CPPUNIT_ASSERT_EQUAL(0, ham_add_page_filter(m_db, &filter));
        CPPUNIT_ASSERT_EQUAL(0, ham_open(m_db, ".test", 0));
        CPPUNIT_ASSERT_EQUAL(0, sf.written);
        CPPUNIT_ASSERT_EQUAL(0, sf.read);
        CPPUNIT_ASSERT_EQUAL(0, sf.closed);

        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, sf.written);
        CPPUNIT_ASSERT_EQUAL(1, sf.read);
        CPPUNIT_ASSERT_EQUAL(0, sf.closed);

        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, sf.written);
        CPPUNIT_ASSERT_EQUAL(1, sf.read);
        CPPUNIT_ASSERT_EQUAL(1, sf.closed);
    }

    void aesFilterTest()
    {
        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        ham_u8_t aeskey[16]={0x13};

        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, ".test", m_flags, 0644));
        CPPUNIT_ASSERT_EQUAL(0, ham_enable_encryption(m_db, aeskey, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_open(m_db, ".test", 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_enable_encryption(m_db, aeskey, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(PageFilterTest);

