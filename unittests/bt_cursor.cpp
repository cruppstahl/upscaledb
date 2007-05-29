/**
 * Copyright (C) 2005-2007 Christoph Rupp (chris@crupp.de).
 * All rights reserved. See file LICENSE for licence and copyright
 * information.
 *
 * unit tests for cursors
 *
 */

#include <stdexcept>
#include <vector>
#include <cppunit/extensions/HelperMacros.h>
#include <ham/hamsterdb.h>
#include "../src/btree_cursor.h"
#include "../src/db.h"
#include "../src/page.h"
#include "memtracker.h"

class BtreeCursorTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(BtreeCursorTest);
    CPPUNIT_TEST      (createCloseTest);
    CPPUNIT_TEST      (cloneTest);
    CPPUNIT_TEST      (structureTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_db_t *m_db;
    ham_device_t *m_dev;
    memtracker_t *m_alloc;

public:
    void setUp()
    { 
        ham_page_t *p;
        CPPUNIT_ASSERT((m_alloc=memtracker_new())!=0);
        CPPUNIT_ASSERT(0==ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        CPPUNIT_ASSERT((m_dev=ham_device_new(m_db, HAM_TRUE))!=0);
        CPPUNIT_ASSERT(m_dev->create(m_dev, ".test", 0, 0644)==HAM_SUCCESS);
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

};

CPPUNIT_TEST_SUITE_REGISTRATION(BtreeCursorTest);

