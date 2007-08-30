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
#include <vector>
#include <cppunit/extensions/HelperMacros.h>
#include <ham/hamsterdb.h>
#include "../src/btree_cursor.h"
#include "../src/db.h"
#include "../src/page.h"
#include "../src/error.h"
#include "memtracker.h"

class BtreeCursorTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(BtreeCursorTest);
    CPPUNIT_TEST      (createCloseTest);
    CPPUNIT_TEST      (cloneTest);
    CPPUNIT_TEST      (structureTest);
    CPPUNIT_TEST      (linkedListTest);
    CPPUNIT_TEST      (linkedListReverseCloseTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_db_t *m_db;
    ham_device_t *m_dev;
    bool m_inmemory;
    memtracker_t *m_alloc;

public:
    BtreeCursorTest(bool inmemory=false)
    :   m_inmemory(inmemory) 
    {
    }

    void setUp()
    { 
        ham_page_t *p;
        CPPUNIT_ASSERT((m_alloc=memtracker_new())!=0);
        CPPUNIT_ASSERT(0==ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        CPPUNIT_ASSERT((m_dev=ham_device_new((mem_allocator_t *)m_alloc, 
                        HAM_TRUE))!=0);
        CPPUNIT_ASSERT(m_dev->create(m_dev, ".test", 
                                m_inmemory?HAM_IN_MEMORY_DB:0, 
                                0644)==HAM_SUCCESS);
        db_set_device(m_db, m_dev);
        p=page_new(m_db);
        CPPUNIT_ASSERT(0==page_alloc(p, m_dev->get_pagesize(m_dev)));
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
        if (db_get_cache(m_db)) {
            cache_delete(m_db, db_get_cache(m_db));
            db_set_cache(m_db, 0);
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

    void createCloseTest(void)
    {
        ham_bt_cursor_t *cursor;

        CPPUNIT_ASSERT(bt_cursor_create(m_db, 0, 0, &cursor)==0);
        CPPUNIT_ASSERT(cursor!=0);
        CPPUNIT_ASSERT(bt_cursor_close(cursor)==0);
    }

    void cloneTest(void)
    {
        ham_bt_cursor_t *cursor, *clone;

        CPPUNIT_ASSERT(bt_cursor_create(m_db, 0, 0, &cursor)==0);
        CPPUNIT_ASSERT(cursor!=0);
        CPPUNIT_ASSERT(bt_cursor_clone(cursor, &clone)==0);
        CPPUNIT_ASSERT(clone!=0);
        CPPUNIT_ASSERT(bt_cursor_close(cursor)==0);
        CPPUNIT_ASSERT(bt_cursor_close(clone)==0);
    }

    void structureTest(void)
    {
        ham_bt_cursor_t *cursor;

        CPPUNIT_ASSERT(bt_cursor_create(m_db, 0, 0, &cursor)==0);
        CPPUNIT_ASSERT(cursor!=0);

        CPPUNIT_ASSERT(bt_cursor_get_db(cursor)==m_db);
        bt_cursor_set_db(cursor, (ham_db_t *)0x13);
        CPPUNIT_ASSERT(bt_cursor_get_db(cursor)==(ham_db_t *)0x13);
        bt_cursor_set_db(cursor, m_db);
        CPPUNIT_ASSERT(bt_cursor_get_db(cursor)==m_db);

        CPPUNIT_ASSERT(bt_cursor_get_txn(cursor)==0);
        bt_cursor_set_txn(cursor, (ham_txn_t *)0x13);
        CPPUNIT_ASSERT(bt_cursor_get_txn(cursor)==(ham_txn_t *)0x13);
        bt_cursor_set_txn(cursor, 0);
        CPPUNIT_ASSERT(bt_cursor_get_txn(cursor)==0);

        CPPUNIT_ASSERT(bt_cursor_get_flags(cursor)==0);
        bt_cursor_set_flags(cursor, 0x13);
        CPPUNIT_ASSERT(bt_cursor_get_flags(cursor)==0x13);
        bt_cursor_set_flags(cursor, 0);
        CPPUNIT_ASSERT(bt_cursor_get_flags(cursor)==0);

        CPPUNIT_ASSERT(bt_cursor_close(cursor)==0);
    }

    void linkedListTest(void)
    {
        ham_bt_cursor_t *cursor[5], *clone;

        CPPUNIT_ASSERT_EQUAL((ham_cursor_t *)0, db_get_cursors(m_db));

        for (int i=0; i<5; i++) {
            CPPUNIT_ASSERT_EQUAL(0, 
                            bt_cursor_create(m_db, 0, 0, &cursor[i]));
            CPPUNIT_ASSERT(cursor[i]!=0);
            CPPUNIT_ASSERT_EQUAL((ham_cursor_t *)cursor[i], 
                            db_get_cursors(m_db));
        }

        CPPUNIT_ASSERT_EQUAL(0, bt_cursor_clone(cursor[0], &clone));
        CPPUNIT_ASSERT(clone!=0);
        CPPUNIT_ASSERT_EQUAL((ham_cursor_t *)clone, db_get_cursors(m_db));

        for (int i=0; i<5; i++) {
            CPPUNIT_ASSERT_EQUAL(0, bt_cursor_close(cursor[i]));
        }
        CPPUNIT_ASSERT_EQUAL(0, bt_cursor_close(clone));

        CPPUNIT_ASSERT_EQUAL((ham_cursor_t *)0, db_get_cursors(m_db));
    }

    void linkedListReverseCloseTest(void)
    {
        ham_bt_cursor_t *cursor[5], *clone;

        CPPUNIT_ASSERT_EQUAL((ham_cursor_t *)0, db_get_cursors(m_db));

        for (int i=0; i<5; i++) {
            CPPUNIT_ASSERT_EQUAL(0, 
                            bt_cursor_create(m_db, 0, 0, &cursor[i]));
            CPPUNIT_ASSERT(cursor[i]!=0);
            CPPUNIT_ASSERT_EQUAL((ham_cursor_t *)cursor[i], 
                            db_get_cursors(m_db));
        }

        CPPUNIT_ASSERT_EQUAL(0, bt_cursor_clone(cursor[0], &clone));
        CPPUNIT_ASSERT(clone!=0);
        CPPUNIT_ASSERT_EQUAL((ham_cursor_t *)clone, db_get_cursors(m_db));

        for (int i=4; i>=0; i--) {
            CPPUNIT_ASSERT_EQUAL(0, bt_cursor_close(cursor[i]));
        }
        CPPUNIT_ASSERT_EQUAL(0, bt_cursor_close(clone));

        CPPUNIT_ASSERT_EQUAL((ham_cursor_t *)0, db_get_cursors(m_db));
    }

};

class InMemoryBtreeCursorTest : public BtreeCursorTest
{
    CPPUNIT_TEST_SUITE(InMemoryBtreeCursorTest);
    CPPUNIT_TEST      (createCloseTest);
    CPPUNIT_TEST      (cloneTest);
    CPPUNIT_TEST      (structureTest);
    CPPUNIT_TEST      (linkedListTest);
    CPPUNIT_TEST      (linkedListReverseCloseTest);
    CPPUNIT_TEST_SUITE_END();

public:
    InMemoryBtreeCursorTest()
    :   BtreeCursorTest(true)
    {
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(BtreeCursorTest);
CPPUNIT_TEST_SUITE_REGISTRATION(InMemoryBtreeCursorTest);

