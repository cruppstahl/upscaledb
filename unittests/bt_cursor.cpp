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

    /*
     * insert 2 dupes, create 2 cursors (both on the first dupe).
     * delete the first cursor, make sure that both cursors are 
     * NILled and the second dupe is still available 
     */
    //CPPUNIT_TEST      (eraseDuplicateTest);

    /*
     * same as above, but uncouples the cursor before the first cursor
     * is deleted
     */
    //CPPUNIT_TEST      (eraseDuplicateUncoupledTest);

    /*
     * insert 2 dupes, create 2 cursors (both on the second dupe).
     * delete the first cursor, make sure that both cursors are 
     * NILled and the first dupe is still available 
     */
    //CPPUNIT_TEST      (eraseSecondDuplicateTest);

    /*
     * same as above, but uncouples the cursor before the second cursor
     * is deleted
     */
    //CPPUNIT_TEST      (eraseSecondDuplicateUncoupledTest);

    /*
     * insert 2 dupes, create 2 cursors (one on the first, the other on the
     * second dupe). delete the first cursor, make sure that it's NILled
     * and the other cursor is still valid.
     */
    //CPPUNIT_TEST      (eraseOtherDuplicateTest);

    /*
     * same as above, but uncouples the cursor before the second cursor
     * is deleted
     */
    //CPPUNIT_TEST      (eraseOtherDuplicateUncoupledTest);

    /*
     * inserts 3 dupes, creates 2 cursors on the middle item; delete the
     * first cursor, make sure that the second is NILled and that the first
     * and last item still exists
     */
    //CPPUNIT_TEST      (eraseMiddleDuplicateTest);

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

    void eraseDuplicateTest(void)
    {
        ham_cursor_t *c1, *c2;
        ham_key_t key;
        ham_record_t rec;
        int value=0;
        ::memset(&key, 0, sizeof(key));

        /* recreate the database with duplicates */
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, ".test", 
                HAM_ENABLE_DUPLICATES|(m_inmemory?HAM_IN_MEMORY_DB:0), 0664));

        ::memset(&rec, 0, sizeof(rec));
        value=1;
        rec.data=&value;
        rec.size=sizeof(value);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

        ::memset(&rec, 0, sizeof(rec));
        value=2;
        rec.data=&value;
        rec.size=sizeof(value);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c1));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c2));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_find(c1, &key, 0));
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_find(c2, &key, 0));
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c1, 0));
        CPPUNIT_ASSERT(bt_cursor_is_nil((ham_bt_cursor_t *)c1));
        CPPUNIT_ASSERT(bt_cursor_is_nil((ham_bt_cursor_t *)c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c1));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c2));
    }

    void eraseDuplicateUncoupledTest(void)
    {
        ham_cursor_t *c1, *c2;
        ham_key_t key;
        ham_record_t rec;
        int value=0;
        ::memset(&key, 0, sizeof(key));

        /* recreate the database with duplicates */
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, ".test", 
                HAM_ENABLE_DUPLICATES|(m_inmemory?HAM_IN_MEMORY_DB:0), 0664));

        ::memset(&rec, 0, sizeof(rec));
        value=1;
        rec.data=&value;
        rec.size=sizeof(value);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

        ::memset(&rec, 0, sizeof(rec));
        value=2;
        rec.data=&value;
        rec.size=sizeof(value);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c1));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c2));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_find(c1, &key, 0));
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_find(c2, &key, 0));
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, bt_cursor_uncouple((ham_bt_cursor_t *)c1, 0));
        CPPUNIT_ASSERT_EQUAL(0, bt_cursor_uncouple((ham_bt_cursor_t *)c2, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c1, 0));
        CPPUNIT_ASSERT(bt_cursor_is_nil((ham_bt_cursor_t *)c1));
        CPPUNIT_ASSERT(bt_cursor_is_nil((ham_bt_cursor_t *)c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c1));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c2));
    }

    void eraseSecondDuplicateTest(void)
    {
        ham_cursor_t *c1, *c2;
        ham_key_t key;
        ham_record_t rec;
        int value=0;
        ::memset(&key, 0, sizeof(key));

        /* recreate the database with duplicates */
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, ".test", 
                HAM_ENABLE_DUPLICATES|(m_inmemory?HAM_IN_MEMORY_DB:0), 0664));

        ::memset(&rec, 0, sizeof(rec));
        value=1;
        rec.data=&value;
        rec.size=sizeof(value);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

        ::memset(&rec, 0, sizeof(rec));
        value=2;
        rec.data=&value;
        rec.size=sizeof(value);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c1));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_move(c1, &key, &rec, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_move(c2, &key, &rec, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c1, 0));
        CPPUNIT_ASSERT(bt_cursor_is_nil((ham_bt_cursor_t *)c1));
        CPPUNIT_ASSERT(bt_cursor_is_nil((ham_bt_cursor_t *)c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c1));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c2));
    }

    void eraseSecondDuplicateUncoupledTest(void)
    {
        ham_cursor_t *c1, *c2;
        ham_key_t key;
        ham_record_t rec;
        int value=0;
        ::memset(&key, 0, sizeof(key));

        /* recreate the database with duplicates */
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, ".test", 
                HAM_ENABLE_DUPLICATES|(m_inmemory?HAM_IN_MEMORY_DB:0), 0664));

        ::memset(&rec, 0, sizeof(rec));
        value=1;
        rec.data=&value;
        rec.size=sizeof(value);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

        ::memset(&rec, 0, sizeof(rec));
        value=2;
        rec.data=&value;
        rec.size=sizeof(value);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c1));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_move(c1, &key, &rec, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_move(c2, &key, &rec, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, bt_cursor_uncouple((ham_bt_cursor_t *)c1, 0));
        CPPUNIT_ASSERT_EQUAL(0, bt_cursor_uncouple((ham_bt_cursor_t *)c2, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c1, 0));
        CPPUNIT_ASSERT(bt_cursor_is_nil((ham_bt_cursor_t *)c1));
        CPPUNIT_ASSERT(bt_cursor_is_nil((ham_bt_cursor_t *)c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c1));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c2));
    }

    void eraseOtherDuplicateTest(void)
    {
        ham_cursor_t *c1, *c2;
        ham_key_t key;
        ham_record_t rec;
        int value=0;
        ::memset(&key, 0, sizeof(key));

        /* recreate the database with duplicates */
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, ".test", 
                HAM_ENABLE_DUPLICATES|(m_inmemory?HAM_IN_MEMORY_DB:0), 0664));

        ::memset(&rec, 0, sizeof(rec));
        value=1;
        rec.data=&value;
        rec.size=sizeof(value);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

        ::memset(&rec, 0, sizeof(rec));
        value=2;
        rec.data=&value;
        rec.size=sizeof(value);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c1));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_move(c1, &key, &rec, HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_move(c2, &key, &rec, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c1, 0));
        CPPUNIT_ASSERT(bt_cursor_is_nil((ham_bt_cursor_t *)c1));
        CPPUNIT_ASSERT(!bt_cursor_is_nil((ham_bt_cursor_t *)c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c1));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c2));
    }

    void eraseOtherDuplicateUncoupledTest(void)
    {
        ham_cursor_t *c1, *c2;
        ham_key_t key;
        ham_record_t rec;
        int value=0;
        ::memset(&key, 0, sizeof(key));

        /* recreate the database with duplicates */
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, ".test", 
                HAM_ENABLE_DUPLICATES|(m_inmemory?HAM_IN_MEMORY_DB:0), 0664));

        ::memset(&rec, 0, sizeof(rec));
        value=1;
        rec.data=&value;
        rec.size=sizeof(value);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

        ::memset(&rec, 0, sizeof(rec));
        value=2;
        rec.data=&value;
        rec.size=sizeof(value);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c1));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_move(c1, &key, &rec, HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_move(c2, &key, &rec, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, bt_cursor_uncouple((ham_bt_cursor_t *)c2, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c1, 0));
        CPPUNIT_ASSERT(bt_cursor_is_nil((ham_bt_cursor_t *)c1));
        CPPUNIT_ASSERT(!bt_cursor_is_nil((ham_bt_cursor_t *)c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c1));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c2));
    }

    void eraseMiddleDuplicateTest(void)
    {
        ham_cursor_t *c1, *c2;
        ham_key_t key;
        ham_record_t rec;
        int value=0;
        ::memset(&key, 0, sizeof(key));

        /* recreate the database with duplicates */
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, ".test", 
                HAM_ENABLE_DUPLICATES|(m_inmemory?HAM_IN_MEMORY_DB:0), 0664));

        ::memset(&rec, 0, sizeof(rec));
        value=1;
        rec.data=&value;
        rec.size=sizeof(value);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

        ::memset(&rec, 0, sizeof(rec));
        value=2;
        rec.data=&value;
        rec.size=sizeof(value);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        ::memset(&rec, 0, sizeof(rec));
        value=3;
        rec.data=&value;
        rec.size=sizeof(value);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c1));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_move(c1, &key, &rec, HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL(3, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_move(c1, &key, &rec, HAM_CURSOR_NEXT));
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_move(c2, &key, &rec, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c1, 0));
        CPPUNIT_ASSERT(bt_cursor_is_nil((ham_bt_cursor_t *)c1));
        CPPUNIT_ASSERT(!bt_cursor_is_nil((ham_bt_cursor_t *)c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL(3, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, HAM_CURSOR_NEXT));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, HAM_CURSOR_PREVIOUS));
        CPPUNIT_ASSERT_EQUAL(3, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c1));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c2));
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
    //CPPUNIT_TEST      (eraseDuplicateTest);
    //CPPUNIT_TEST      (eraseDuplicateUncoupledTest);
    //CPPUNIT_TEST      (eraseSecondDuplicateTest);
    //CPPUNIT_TEST      (eraseSecondDuplicateUncoupledTest);
    //CPPUNIT_TEST      (eraseOtherDuplicateTest);
    //CPPUNIT_TEST      (eraseOtherDuplicateUncoupledTest);
    //CPPUNIT_TEST      (eraseMiddleDuplicateTest);
    CPPUNIT_TEST_SUITE_END();

public:
    InMemoryBtreeCursorTest()
    :   BtreeCursorTest(true)
    {
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(BtreeCursorTest);
CPPUNIT_TEST_SUITE_REGISTRATION(InMemoryBtreeCursorTest);

