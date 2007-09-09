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
#include <errno.h>
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
    CPPUNIT_TEST      (cursorGetErasedItemTest);
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
#if WIN32
        (void)DeleteFileA((LPCSTR)".test");
#else
        if (unlink(".test")) {
            if (errno!=2)
                printf("failed to unlink .test: %s\n", strerror(errno));
        }
#endif
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&m_db));
        CPPUNIT_ASSERT((m_alloc=memtracker_new())!=0);
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, ".test", 
                    m_inmemory?HAM_IN_MEMORY_DB:0, 0664));
    }

    void tearDown()
    {
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(m_db));
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

    void cursorGetErasedItemTest(void)
    {
        ham_cursor_t *cursor, *cursor2;
        ham_key_t key;
        ham_record_t rec;
        int value=0;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        key.data=&value;
        key.size=sizeof(value);

        value=1;
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));
        value=2;
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor2));
        value=1;
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_find(cursor, &key, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_erase(m_db, 0, &key, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, 
                ham_cursor_move(cursor, &key, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(cursor, &key, 0, HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(cursor2, &key, 0, HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_erase(cursor, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, 
                ham_cursor_move(cursor2, &key, 0, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor2));
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
    CPPUNIT_TEST      (cursorGetErasedItemTest);
    CPPUNIT_TEST_SUITE_END();

public:
    InMemoryBtreeCursorTest()
    :   BtreeCursorTest(true)
    {
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(BtreeCursorTest);
CPPUNIT_TEST_SUITE_REGISTRATION(InMemoryBtreeCursorTest);

