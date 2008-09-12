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
#include <cstring>
#include <vector>
#include <errno.h>
#include <cppunit/extensions/HelperMacros.h>
#include <ham/hamsterdb.h>
#include "../src/btree_cursor.h"
#include "../src/db.h"
#include "../src/page.h"
#include "../src/error.h"
#include "../src/btree.h"
#include "memtracker.h"
#include "os.hpp"

class BtreeCursorTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(BtreeCursorTest);
    CPPUNIT_TEST      (createCloseTest);
    CPPUNIT_TEST      (cloneTest);
    CPPUNIT_TEST      (moveTest);
    CPPUNIT_TEST      (moveSplitTest);
    CPPUNIT_TEST      (overwriteTest);
    CPPUNIT_TEST      (structureTest);
    CPPUNIT_TEST      (linkedListTest);
    CPPUNIT_TEST      (linkedListReverseCloseTest);
    CPPUNIT_TEST      (cursorGetErasedItemTest);
    CPPUNIT_TEST      (couplingTest);
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
        os::unlink(".test");

        CPPUNIT_ASSERT_EQUAL(0, ham_new(&m_db));
        CPPUNIT_ASSERT((m_alloc=memtracker_new())!=0);
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, ".test", 
                    HAM_ENABLE_DUPLICATES|(m_inmemory?HAM_IN_MEMORY_DB:0),
                    0664));
    }

    void tearDown()
    {
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(m_db));
        CPPUNIT_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void createCloseTest(void)
    {
        ham_bt_cursor_t *cursor;

        CPPUNIT_ASSERT(bt_cursor_create(m_db, 0, 0, &cursor)==0);
        CPPUNIT_ASSERT(cursor!=0);
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close((ham_cursor_t *)cursor));
    }

    void cloneTest(void)
    {
        ham_bt_cursor_t *cursor, *clone;

        CPPUNIT_ASSERT(bt_cursor_create(m_db, 0, 0, &cursor)==0);
        CPPUNIT_ASSERT(cursor!=0);
        CPPUNIT_ASSERT(bt_cursor_clone(cursor, &clone)==0);
        CPPUNIT_ASSERT(clone!=0);
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close((ham_cursor_t *)clone));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close((ham_cursor_t *)cursor));
    }

    void overwriteTest(void)
    {
        ham_cursor_t *cursor;
        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        int x=5;
        key.size=sizeof(x);
        key.data=&x;
        rec.size=sizeof(x);
        rec.data=&x;

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_insert(cursor, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_overwrite(cursor, &rec, 0));

        ham_btree_t *be=(ham_btree_t *)db_get_backend(m_db);
        ham_page_t *page=db_fetch_page(m_db, btree_get_rootpage(be), 0);
        CPPUNIT_ASSERT(page!=0);
        CPPUNIT_ASSERT_EQUAL(0, db_uncouple_all_cursors(page, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_overwrite(cursor, &rec, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor));
    }

    void moveSplitTest(void)
    {
        ham_cursor_t *cursor, *cursor2, *cursor3;
        ham_key_t key;
        ham_record_t rec;
        ham_parameter_t params[]={
            { HAM_PARAM_PAGESIZE, 1024 },
            { HAM_PARAM_KEYSIZE, 128 },
            { 0, 0 }
        };
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_create_ex(m_db, ".test", 
                    HAM_ENABLE_DUPLICATES|(m_inmemory?HAM_IN_MEMORY_DB:0),
                    0664, &params[0]));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor2));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor3));

        for (int i=0; i<64; i++) {
            key.size=sizeof(i);
            key.data=&i;
            rec.size=sizeof(i);
            rec.data=&i;

            CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));
        }

        CPPUNIT_ASSERT_EQUAL(0,
                ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL(0, *(int *)key.data);
        CPPUNIT_ASSERT_EQUAL(0, *(int *)rec.data);
        CPPUNIT_ASSERT_EQUAL(0,
                ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(63, *(int *)key.data);
        CPPUNIT_ASSERT_EQUAL(63, *(int *)rec.data);

        for (int i=0; i<64; i++) {
            CPPUNIT_ASSERT_EQUAL(0,
                    ham_cursor_move(cursor2, &key, &rec, HAM_CURSOR_NEXT));
            CPPUNIT_ASSERT_EQUAL(i, *(int *)key.data);
            CPPUNIT_ASSERT_EQUAL(i, *(int *)rec.data);
        }
        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                ham_cursor_move(cursor2, 0, 0, HAM_CURSOR_NEXT));
        for (int i=63; i>=0; i--) {
            CPPUNIT_ASSERT_EQUAL(0,
                    ham_cursor_move(cursor3, &key, &rec, HAM_CURSOR_PREVIOUS));
            CPPUNIT_ASSERT_EQUAL(i, *(int *)key.data);
            CPPUNIT_ASSERT_EQUAL(i, *(int *)rec.data);
        }
        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                ham_cursor_move(cursor3, 0, 0, HAM_CURSOR_PREVIOUS));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor2));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor3));
    }

    void moveTest(void)
    {
        ham_cursor_t *cursor;

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));

        /* no move, and cursor is nil: returns 0 if key/rec is 0 */
        CPPUNIT_ASSERT_EQUAL(0,
                    ham_cursor_move(cursor, 0, 0, 0));

        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                    ham_cursor_move(cursor, 0, 0, HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                    ham_cursor_move(cursor, 0, 0, HAM_CURSOR_NEXT));
        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                    ham_cursor_move(cursor, 0, 0, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                    ham_cursor_move(cursor, 0, 0, HAM_CURSOR_PREVIOUS));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor));
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

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close((ham_cursor_t *)cursor));
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
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_cursor_close((ham_cursor_t *)cursor[i]));
        }
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close((ham_cursor_t *)clone));

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
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_cursor_close((ham_cursor_t *)cursor[i]));
        }
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close((ham_cursor_t *)clone));

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

    void couplingTest(void)
    {
        ham_cursor_t *c, *clone;
        ham_bt_cursor_t *btc;
        ham_key_t key1, key2, key3;
        ham_record_t rec;
        int v1=1, v2=2, v3=3;

        memset(&key1, 0, sizeof(key1));
        memset(&key2, 0, sizeof(key2));
        memset(&key3, 0, sizeof(key3));
        key1.size=sizeof(int);
        key1.data=(void *)&v1;
        key2.size=sizeof(int);
        key2.data=(void *)&v2;
        key3.size=sizeof(int);
        key3.data=(void *)&v3;
        memset(&rec, 0, sizeof(rec));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        btc=(ham_bt_cursor_t *)c;
        /* after create: cursor is NIL */
        CPPUNIT_ASSERT(!(bt_cursor_get_flags(btc)&BT_CURSOR_FLAG_COUPLED));
        CPPUNIT_ASSERT(!(bt_cursor_get_flags(btc)&BT_CURSOR_FLAG_UNCOUPLED));

        /* after insert: cursor is NIL */
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key2, &rec, 0));
        CPPUNIT_ASSERT(!(bt_cursor_get_flags(btc)&BT_CURSOR_FLAG_COUPLED));
        CPPUNIT_ASSERT(!(bt_cursor_get_flags(btc)&BT_CURSOR_FLAG_UNCOUPLED));

        /* move to item: cursor is coupled */
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_find(c, &key2, 0));
        CPPUNIT_ASSERT(bt_cursor_get_flags(btc)&BT_CURSOR_FLAG_COUPLED);
        CPPUNIT_ASSERT(!(bt_cursor_get_flags(btc)&BT_CURSOR_FLAG_UNCOUPLED));

        /* clone the coupled cursor */
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_clone(c, &clone));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(clone));

        /* insert item BEFORE the first item - cursor is uncoupled */
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key1, &rec, 0));
        CPPUNIT_ASSERT(!(bt_cursor_get_flags(btc)&BT_CURSOR_FLAG_COUPLED));
        CPPUNIT_ASSERT(bt_cursor_get_flags(btc)&BT_CURSOR_FLAG_UNCOUPLED);

        /* move to item: cursor is coupled */
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_find(c, &key2, 0));
        CPPUNIT_ASSERT(bt_cursor_get_flags(btc)&BT_CURSOR_FLAG_COUPLED);
        CPPUNIT_ASSERT(!(bt_cursor_get_flags(btc)&BT_CURSOR_FLAG_UNCOUPLED));

        /* insert duplicate - cursor stays coupled */
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_insert(m_db, 0, &key2, &rec, HAM_DUPLICATE));
        CPPUNIT_ASSERT(bt_cursor_get_flags(btc)&BT_CURSOR_FLAG_COUPLED);
        CPPUNIT_ASSERT(!(bt_cursor_get_flags(btc)&BT_CURSOR_FLAG_UNCOUPLED));

        /* insert item AFTER the middle item - cursor stays coupled */
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_insert(m_db, 0, &key3, &rec, 0));
        CPPUNIT_ASSERT(bt_cursor_get_flags(btc)&BT_CURSOR_FLAG_COUPLED);
        CPPUNIT_ASSERT(!(bt_cursor_get_flags(btc)&BT_CURSOR_FLAG_UNCOUPLED));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c));
    }

};

class InMemoryBtreeCursorTest : public BtreeCursorTest
{
    CPPUNIT_TEST_SUITE(InMemoryBtreeCursorTest);
    CPPUNIT_TEST      (createCloseTest);
    CPPUNIT_TEST      (cloneTest);
    CPPUNIT_TEST      (moveTest);
    CPPUNIT_TEST      (moveSplitTest);
    CPPUNIT_TEST      (overwriteTest);
    CPPUNIT_TEST      (structureTest);
    CPPUNIT_TEST      (linkedListTest);
    CPPUNIT_TEST      (linkedListReverseCloseTest);
    CPPUNIT_TEST      (cursorGetErasedItemTest);
    CPPUNIT_TEST      (couplingTest);
    CPPUNIT_TEST_SUITE_END();

public:
    InMemoryBtreeCursorTest()
    :   BtreeCursorTest(true)
    {
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(BtreeCursorTest);
CPPUNIT_TEST_SUITE_REGISTRATION(InMemoryBtreeCursorTest);

