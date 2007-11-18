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
#include <errno.h>
#include <string.h>
#include <cppunit/extensions/HelperMacros.h>
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/blob.h"
#include "../src/backend.h"
#include "../src/btree.h"
#include "../src/endian.h"
#include "memtracker.h"
#include "os.hpp"

class DupeTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(DupeTest);
    CPPUNIT_TEST      (invalidFlagsTest);
    CPPUNIT_TEST      (insertDuplicatesTest);
    CPPUNIT_TEST      (overwriteDuplicatesTest);
    CPPUNIT_TEST      (overwriteVariousDuplicatesTest);
    CPPUNIT_TEST      (insertMoveForwardTest);
    CPPUNIT_TEST      (insertMoveBackwardTest);
    CPPUNIT_TEST      (insertEraseTest);
    CPPUNIT_TEST      (insertTest);
    CPPUNIT_TEST      (insertSkipDuplicatesTest);
    CPPUNIT_TEST      (insertOnlyDuplicatesTest);
    CPPUNIT_TEST      (coupleUncoupleTest);
    CPPUNIT_TEST      (moveToLastDuplicateTest);

    /*
     * insert 2 dupes, create 2 cursors (both on the first dupe).
     * delete the first cursor, make sure that both cursors are 
     * NILled and the second dupe is still available 
     */
    CPPUNIT_TEST      (eraseDuplicateTest);

    /*
     * same as above, but uncouples the cursor before the first cursor
     * is deleted
     */
    CPPUNIT_TEST      (eraseDuplicateUncoupledTest);

    /*
     * insert 2 dupes, create 2 cursors (both on the second dupe).
     * delete the first cursor, make sure that both cursors are 
     * NILled and the first dupe is still available 
     */
    CPPUNIT_TEST      (eraseSecondDuplicateTest);

    /*
     * same as above, but uncouples the cursor before the second cursor
     * is deleted
     */
    CPPUNIT_TEST      (eraseSecondDuplicateUncoupledTest);

    /*
     * insert 2 dupes, create 2 cursors (one on the first, the other on the
     * second dupe). delete the first cursor, make sure that it's NILled
     * and the other cursor is still valid.
     */
    CPPUNIT_TEST      (eraseOtherDuplicateTest);

    /*
     * same as above, but uncouples the cursor before the second cursor
     * is deleted
     */
    CPPUNIT_TEST      (eraseOtherDuplicateUncoupledTest);

    /*
     * inserts 3 dupes, creates 2 cursors on the middle item; delete the
     * first cursor, make sure that the second is NILled and that the first
     * and last item still exists
     */
    CPPUNIT_TEST      (eraseMiddleDuplicateTest);

    /*
     * inserts a few TINY dupes, then erases them all but the last element
     */
    CPPUNIT_TEST      (eraseTinyDuplicatesTest);

    /*
     * inserts a few dplicates, reopens the database; continues inserting
     */
    CPPUNIT_TEST      (reopenTest);

    /*
     * test ham_cursor_move(... HAM_CURSOR_PREVIOUS)
     */
    CPPUNIT_TEST      (moveToPreviousDuplicateTest);

    /*
     * overwrite duplicates using ham_insert(... HAM_OVERWRITE)
     */
    CPPUNIT_TEST      (overwriteTest);

    /*
     * overwrite duplicates using ham_cursor_insert(... HAM_OVERWRITE)
     */
    CPPUNIT_TEST      (overwriteCursorTest);

    /*
     * same as overwriteCursorTest, but uses multiple cursors and makes
     * sure that their positions are not modified
     */
    CPPUNIT_TEST      (overwriteMultipleCursorTest);

    /*
     * reads a big-endian database (if started on a little-endian system)
     * or vice versa
     */
    CPPUNIT_TEST      (endianTest);

    /*
     * insert a few duplicate items, then delete them all with a cursor
     */
    CPPUNIT_TEST      (eraseCursorTest);

    /*
     * tests HAM_DUPLICATE_INSERT_LAST and makes sure that the cursor
     * always points to the inserted duplicate
     */
    CPPUNIT_TEST      (insertLastTest);

    /*
     * tests HAM_DUPLICATE_INSERT_FIRST and makes sure that the cursor
     * always points to the inserted duplicate
     */
    CPPUNIT_TEST      (insertFirstTest);

    /*
     * tests HAM_DUPLICATE_INSERT_AFTER and makes sure that the cursor
     * always points to the inserted duplicate
     */
    CPPUNIT_TEST      (insertAfterTest);

    /*
     * tests HAM_DUPLICATE_INSERT_BEFORE and makes sure that the cursor
     * always points to the inserted duplicate
     */
    CPPUNIT_TEST      (insertBeforeTest);

    /*
     * overwrite NULL-, TINY- and SMALL-duplicates with other
     * NULL-, TINY- and SMALL-duplicates
     */
    CPPUNIT_TEST      (overwriteVariousSizesTest);

    /*
     * tests get_cuplicate_count
     */
    CPPUNIT_TEST      (getDuplicateCountTest);

    CPPUNIT_TEST_SUITE_END();

protected:
    ham_u32_t m_flags;
    ham_db_t *m_db;
    std::vector<std::string> m_data;

public:
    DupeTest(ham_u32_t flags=0)
    :   m_flags(flags)
    {
    }

    void setUp()
    { 
        (void)os::unlink(".test");

        CPPUNIT_ASSERT_EQUAL(0, ham_new(&m_db));
        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, ".test", 
                    m_flags|HAM_ENABLE_DUPLICATES, 0664));

        m_data.resize(0);
    }

    void tearDown()
    {
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, HAM_AUTO_CLEANUP));
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(m_db));
    }
    
    void insertDuplicatesTest(void)
    {
        ham_key_t key;
        ham_record_t rec, rec2;
        char data[16];

        ::memset(&rec2, 0, sizeof(rec2));

        for (int i=0; i<10; i++) {
            ::memset(&key, 0, sizeof(key));
            ::memset(&rec, 0, sizeof(rec));
            rec.data=data;
            rec.size=sizeof(data);
            ::memset(&data, i+0x15, sizeof(data));
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        }

        ::memset(&data, 0x15, sizeof(data));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec2, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec2.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec2.data, sizeof(data)));
    }

    void overwriteDuplicatesTest(void)
    {
        ham_key_t key;
        ham_record_t rec, rec2;
        ham_cursor_t *c;
        ham_size_t count;
        char data[16];
        ::memset(&rec2, 0, sizeof(rec2));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));

        for (int i=0; i<5; i++) {
            ::memset(&key, 0, sizeof(key));
            ::memset(&rec, 0, sizeof(rec));
            rec.data=data;
            rec.size=sizeof(data);
            ::memset(&data, i+0x15, sizeof(data));
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        }

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        rec.data=data;
        rec.size=sizeof(data);
        ::memset(&data, 0x99, sizeof(data));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_move(c, &key, &rec2, 
                    HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec2.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec2.data, sizeof(data)));

        for (int i=1; i<5; i++) {
            ::memset(&key, 0, sizeof(key));
            ::memset(&rec2, 0, sizeof(rec));
            ::memset(&data, i+0x15, sizeof(data));
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_cursor_move(c, &key, &rec2, HAM_CURSOR_NEXT));
            CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec2.size);
            CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec2.data, sizeof(data)));
        }

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c, 0, 0, HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_get_duplicate_count(c, &count, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)5, count);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c));
    }

    void overwriteVariousDuplicatesTest(void)
    {
        ham_key_t key;
        ham_record_t rec, rec2;
#define MAX 10
        unsigned sizes[MAX]={0, 1, 2, 3, 4, 5, 936, 5, 100, 50};
        char *data;
        ham_cursor_t *cursor;
        ham_size_t count;

        ::memset(&rec2, 0, sizeof(rec2));

        for (unsigned i=0; i<MAX; i++) {
            data=0;
            ::memset(&key, 0, sizeof(key));
            ::memset(&rec, 0, sizeof(rec));
            if (sizes[i]) {
                data=(char *)malloc(sizes[i]);
                ::memset(data, i+0x15, sizes[i]);
            }
            rec.data=sizes[i] ? data : 0;
            rec.size=sizes[i];
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
            CPPUNIT_ASSERT_EQUAL(sizes[i], rec.size);
            if (sizes[i]) {
                CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec.data, sizes[i]));
                free(data);
            }
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec2, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, rec2.size);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));

        for (unsigned i=0; i<MAX; i++) {
            ::memset(&key, 0, sizeof(key));
            ::memset(&rec, 0, sizeof(rec));
            if (sizes[i]) {
                data=(char *)malloc(sizes[i]);
                ::memset(data, i+0x15, sizes[i]);
            }
            rec.data=sizes[i] ? data : 0;
            rec.size=sizes[i];
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));
            CPPUNIT_ASSERT_EQUAL(sizes[i], rec.size);
            if (sizes[i]) {
                CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec.data, sizes[i]));
                free(data);
            }
        }

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_get_duplicate_count(cursor, &count, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)MAX, count);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));

        for (int i=MAX-1; i>=0; i--) {
            ::memset(&key, 0, sizeof(key));
            ::memset(&rec, 0, sizeof(rec));
            if (sizes[i]) {
                data=(char *)malloc(sizes[i]);
                ::memset(data, i+0x15, sizes[i]);
            }
            rec.data=sizes[i] ? data : 0;
            rec.size=sizes[i];
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_PREVIOUS));
            CPPUNIT_ASSERT_EQUAL(sizes[i], rec.size);
            if (sizes[i]) {
                CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec.data, sizes[i]));
                free(data);
            }
        }

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_get_duplicate_count(cursor, &count, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)MAX, count);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        data=(char *)malloc(16);
        ::memset(data, 0x99, 16);
        rec.data=data;
        rec.size=16;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));

        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec2, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)16, rec2.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec2.data, 16));
        free(data);
    }
    
    void insertMoveForwardTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_cursor_t *cursor;
        char data[16];

        for (int i=0; i<5; i++) {
            ::memset(&key, 0, sizeof(key));
            ::memset(&rec, 0, sizeof(rec));
            rec.data=data;
            rec.size=sizeof(data);
            ::memset(&data, i+0x15, sizeof(data));
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));

        for (int i=0; i<5; i++) {
            ::memset(&key, 0, sizeof(key));
            ::memset(&rec, 0, sizeof(rec));
            ::memset(&data, i+0x15, sizeof(data));
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));
            CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec.size);
            CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec.data, sizeof(data)));
        }

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor));
    }

    void insertMoveBackwardTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_cursor_t *cursor;
        char data[16];

        for (int i=0; i<5; i++) {
            ::memset(&key, 0, sizeof(key));
            ::memset(&rec, 0, sizeof(rec));
            rec.data=data;
            rec.size=sizeof(data);
            ::memset(&data, i+0x15, sizeof(data));
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));

        for (int i=4; i>=0; i--) {
            ::memset(&key, 0, sizeof(key));
            ::memset(&rec, 0, sizeof(rec));
            ::memset(&data, i+0x15, sizeof(data));
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_PREVIOUS));
            CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec.size);
            CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec.data, sizeof(data)));
        }

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_PREVIOUS));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor));
    }

    void insertEraseTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        char data[16];
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        ::memset(&data, 0x13, sizeof(data));
        rec.data=data;
        rec.size=sizeof(data);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        ::memset(&data, 0x14, sizeof(data));
        rec.data=data;
        rec.size=sizeof(data);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        ::memset(&data, 0x15, sizeof(data));
        rec.data=data;
        rec.size=sizeof(data);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        ::memset(&rec, 0, sizeof(rec));
        ::memset(&data, 0x13, sizeof(data));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec.data, sizeof(data)));

        CPPUNIT_ASSERT_EQUAL(0, ham_erase(m_db, 0, &key, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_find(m_db, 0, &key, &rec, 0));
    }

    void insert(ham_key_t *key, ham_record_t *rec)
    {
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, key, rec, HAM_DUPLICATE));
    }

    void find(ham_key_t *key, ham_record_t *rec)
    {
        ham_record_t record;
        ::memset(&record, 0, sizeof(record));

        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, key, &record, 0));
        CPPUNIT_ASSERT_EQUAL(rec->size, record.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(rec->data, record.data, rec->size));

        rec->_rid=record._rid;
    }

    void erase(ham_key_t *key)
    {
        CPPUNIT_ASSERT_EQUAL(0, ham_erase(m_db, 0, key, 0));
    }

    void insertData(const char *k, const char *data)
    {
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        rec.data=(void *)data;
        rec.size=(ham_size_t)::strlen(data)+1;
        key.data=(void *)k;
        key.size=(ham_u16_t)(k ? ::strlen(k)+1 : 0);

        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
    }

    void checkData(ham_cursor_t *cursor, ham_u32_t flags, 
            ham_status_t expected, const char *data)
    {
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        CPPUNIT_ASSERT_EQUAL(expected, 
                ham_cursor_move(cursor, &key, &rec, flags));

        if (expected==0) {
            if (data) {
                CPPUNIT_ASSERT_EQUAL(rec.size, (ham_size_t)::strlen(data)+1);
                CPPUNIT_ASSERT_EQUAL(0, ::memcmp(rec.data, data, rec.size));
            }
            else {
                CPPUNIT_ASSERT_EQUAL(rec.size, (ham_size_t)0);
                CPPUNIT_ASSERT_EQUAL((void *)0, rec.data);
            }
        }
    }

    void insertTest(void)
    {
        ham_cursor_t *c;
        ham_size_t count;

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
        insertData(0, "1111111111");
        insertData(0, "2222222222");
        insertData(0, "3333333333");
        insertData(0, "4444444444");
        insertData(0, "5555555555");
        insertData(0, "6666666666");
        insertData(0, "7777777777");
        insertData(0, "8888888888");
        insertData(0, "9999999999");
        insertData(0, "0000000000");

        checkData(c, HAM_CURSOR_NEXT,     0, "1111111111");
        checkData(c, HAM_CURSOR_NEXT,     0, "2222222222");
        checkData(c, HAM_CURSOR_NEXT,     0, "3333333333");
        checkData(c, HAM_CURSOR_PREVIOUS, 0, "2222222222");
        checkData(c, HAM_CURSOR_NEXT,     0, "3333333333");
        checkData(c, HAM_CURSOR_PREVIOUS, 0, "2222222222");
        checkData(c, HAM_CURSOR_NEXT,     0, "3333333333");
        checkData(c, HAM_CURSOR_NEXT,     0, "4444444444");
        checkData(c, HAM_CURSOR_NEXT,     0, "5555555555");
        checkData(c, HAM_CURSOR_NEXT,     0, "6666666666");
        checkData(c, HAM_CURSOR_NEXT,     0, "7777777777");
        checkData(c, HAM_CURSOR_NEXT,     0, "8888888888");
        checkData(c, HAM_CURSOR_NEXT,     0, "9999999999");
        checkData(c, HAM_CURSOR_NEXT,     0, "0000000000");
        checkData(c, HAM_CURSOR_NEXT,     HAM_KEY_NOT_FOUND, "0000000000");
        checkData(c, HAM_CURSOR_PREVIOUS, 0, "9999999999");
        checkData(c, HAM_CURSOR_PREVIOUS, 0, "8888888888");
        checkData(c, HAM_CURSOR_PREVIOUS, 0, "7777777777");
        checkData(c, HAM_CURSOR_PREVIOUS, 0, "6666666666");
        checkData(c, HAM_CURSOR_PREVIOUS, 0, "5555555555");
        checkData(c, HAM_CURSOR_PREVIOUS, 0, "4444444444");
        checkData(c, HAM_CURSOR_PREVIOUS, 0, "3333333333");
        checkData(c, HAM_CURSOR_PREVIOUS, 0, "2222222222");
        checkData(c, HAM_CURSOR_PREVIOUS, 0, "1111111111");
        checkData(c, HAM_CURSOR_PREVIOUS, HAM_KEY_NOT_FOUND, "0000000000");
        checkData(c, HAM_CURSOR_NEXT,     0, "2222222222");
        checkData(c, HAM_CURSOR_NEXT,     0, "3333333333");

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_get_duplicate_count(c, &count, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)10, count);

        ham_cursor_close(c);
    }

    void insertSkipDuplicatesTest(void)
    {
        ham_cursor_t *c;

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
        insertData("000", "aaaaaaaaaa");
        insertData("111", "1111111111");
        insertData("111", "2222222222");
        insertData("111", "3333333333");
        insertData("111", "4444444444");
        insertData("111", "5555555555");
        insertData("111", "6666666666");
        insertData("111", "7777777777");
        insertData("111", "8888888888");
        insertData("111", "9999999999");
        insertData("111", "0000000000");
        insertData("222", "bbbbbbbbbb");
        insertData("333", "cccccccccc");

        checkData(c, HAM_CURSOR_NEXT,     0, "aaaaaaaaaa");
        checkData(c, HAM_CURSOR_NEXT,     0, "1111111111");
        checkData(c, HAM_CURSOR_NEXT,     0, "2222222222");
        checkData(c, HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES, 0, "bbbbbbbbbb");
        checkData(c, HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES, 0, "cccccccccc");
        checkData(c, HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES, 0, "bbbbbbbbbb");
        checkData(c, HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES, 0, "1111111111");
        checkData(c, HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES, 0, "aaaaaaaaaa");

        ham_cursor_close(c);
    }

    void insertOnlyDuplicatesTest(void)
    {
        ham_cursor_t *c;

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
        insertData("000", "aaaaaaaaaa");
        insertData("111", "8888888888");
        insertData("111", "9999999999");
        insertData("111", "0000000000");
        insertData("222", "bbbbbbbbbb");

        checkData(c, HAM_CURSOR_FIRST,    0, "aaaaaaaaaa");
        checkData(c, HAM_CURSOR_NEXT,     0, "8888888888");
        checkData(c, HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES, 0, "9999999999");
        checkData(c, HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES, 0, "0000000000");
        checkData(c, HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES, HAM_KEY_NOT_FOUND, 0);

        checkData(c, HAM_CURSOR_FIRST,    0, "aaaaaaaaaa");
        checkData(c, HAM_CURSOR_NEXT,     0, "8888888888");
        checkData(c, HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES, 0, "9999999999");
        checkData(c, HAM_CURSOR_PREVIOUS|HAM_ONLY_DUPLICATES, 0, "8888888888");
        checkData(c, HAM_CURSOR_PREVIOUS|HAM_ONLY_DUPLICATES, 
                        HAM_KEY_NOT_FOUND, 0);

        checkData(c, HAM_CURSOR_FIRST,    0, "aaaaaaaaaa");
        checkData(c, HAM_CURSOR_PREVIOUS|HAM_ONLY_DUPLICATES, 
                        HAM_KEY_NOT_FOUND, 0);

        ham_cursor_close(c);
    }

    void coupleUncoupleTest(void)
    {
        ham_cursor_t *c;
        ham_page_t *page;

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));

        insertData("000", "aaaaaaaaaa");
        insertData("111", "1111111111");
        insertData("111", "2222222222");
        insertData("111", "3333333333");
        insertData("111", "4444444444");
        insertData("111", "5555555555");
        insertData("111", "6666666666");
        insertData("111", "7777777777");
        insertData("111", "8888888888");
        insertData("111", "9999999999");
        insertData("111", "0000000000");
        insertData("222", "bbbbbbbbbb");
        insertData("333", "cccccccccc");

        ham_btree_t *be=(ham_btree_t *)db_get_backend(m_db);
        page=db_fetch_page(m_db, btree_get_rootpage(be), 0);
        CPPUNIT_ASSERT(page!=0);

        CPPUNIT_ASSERT_EQUAL(0, db_uncouple_all_cursors(page, 0));
        checkData(c, HAM_CURSOR_NEXT,     0, "aaaaaaaaaa");
        CPPUNIT_ASSERT_EQUAL(0, db_uncouple_all_cursors(page, 0));
        checkData(c, HAM_CURSOR_NEXT,     0, "1111111111");
        CPPUNIT_ASSERT_EQUAL(0, db_uncouple_all_cursors(page, 0));
        checkData(c, HAM_CURSOR_NEXT,     0, "2222222222");
        CPPUNIT_ASSERT_EQUAL(0, db_uncouple_all_cursors(page, 0));
        checkData(c, HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES, 0, "bbbbbbbbbb");
        CPPUNIT_ASSERT_EQUAL(0, db_uncouple_all_cursors(page, 0));
        checkData(c, HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES, 0, "cccccccccc");
        CPPUNIT_ASSERT_EQUAL(0, db_uncouple_all_cursors(page, 0));
        checkData(c, HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES, 0, "bbbbbbbbbb");
        CPPUNIT_ASSERT_EQUAL(0, db_uncouple_all_cursors(page, 0));
        checkData(c, HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES, 0, "1111111111");
        CPPUNIT_ASSERT_EQUAL(0, db_uncouple_all_cursors(page, 0));
        checkData(c, HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES, 0, "aaaaaaaaaa");

        ham_cursor_close(c);
    }

    void eraseOtherDuplicateUncoupledTest(void)
    {
        ham_cursor_t *c1, *c2;
        ham_key_t key;
        ham_record_t rec;
        int value=0;
        ::memset(&key, 0, sizeof(key));

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
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_move(c2, &key, &rec, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, bt_cursor_uncouple((ham_bt_cursor_t *)c2, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c1, 0));
        CPPUNIT_ASSERT(bt_cursor_is_nil((ham_bt_cursor_t *)c1));
        CPPUNIT_ASSERT(!bt_cursor_is_nil((ham_bt_cursor_t *)c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

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
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_move(c1, &key, &rec, HAM_CURSOR_NEXT));
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_move(c2, &key, &rec, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(3, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c1, 0));
        CPPUNIT_ASSERT(bt_cursor_is_nil((ham_bt_cursor_t *)c1));
        CPPUNIT_ASSERT(!bt_cursor_is_nil((ham_bt_cursor_t *)c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, HAM_CURSOR_NEXT));
        CPPUNIT_ASSERT_EQUAL(3, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(3, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, HAM_CURSOR_PREVIOUS));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c1));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c2));
    }

    void eraseTinyDuplicatesTest(void)
    {
        ham_cursor_t *c;

        insertData("111", "111");
        insertData("111", "222");
        insertData("111", "333");
        insertData("111", "444");
        insertData("111", "555");

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));

        checkData(c, HAM_CURSOR_FIRST,    0, "111");
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c, 0));
        checkData(c, HAM_CURSOR_FIRST,    0, "222");
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c, 0));
        checkData(c, HAM_CURSOR_FIRST,    0, "333");
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c, 0));
        checkData(c, HAM_CURSOR_FIRST,    0, "444");
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c, 0));
        checkData(c, HAM_CURSOR_FIRST,    0, "555");
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c, 0));
        checkData(c, HAM_CURSOR_FIRST,    HAM_KEY_NOT_FOUND, "555");

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c));
    }

    void reopenTest(void)
    {
        ham_cursor_t *c;

        insertData("000", "aaaaaaaaaa");
        insertData("111", "1111111111");
        insertData("111", "2222222222");
        insertData("111", "3333333333");
        insertData("222", "bbbbbbbbbb");

        /* reopen the database */
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_open(m_db, ".test", m_flags));
        CPPUNIT_ASSERT(db_get_rt_flags(m_db)&HAM_ENABLE_DUPLICATES);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
        insertData("111", "4444444444");
        insertData("111", "5555555555");
        insertData("111", "6666666666");

        checkData(c, HAM_CURSOR_FIRST,    0, "aaaaaaaaaa");
        checkData(c, HAM_CURSOR_NEXT,     0, "1111111111");
        checkData(c, HAM_CURSOR_NEXT,     0, "2222222222");
        checkData(c, HAM_CURSOR_NEXT,     0, "3333333333");
        checkData(c, HAM_CURSOR_NEXT,     0, "4444444444");
        checkData(c, HAM_CURSOR_NEXT,     0, "5555555555");
        checkData(c, HAM_CURSOR_NEXT,     0, "6666666666");
        checkData(c, HAM_CURSOR_NEXT,     0, "bbbbbbbbbb");

        checkData(c, HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES, 
                        HAM_KEY_NOT_FOUND, 0);
        checkData(c, HAM_CURSOR_NEXT, 
                        HAM_KEY_NOT_FOUND, 0);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c));
    }

    void moveToLastDuplicateTest(void)
    {
        ham_cursor_t *c;

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
        insertData(0, "3333333333");
        insertData(0, "2222222222");
        insertData(0, "1111111111");

        checkData(c, HAM_CURSOR_LAST,     0, "1111111111");

        ham_cursor_close(c);
    }

    void eraseDuplicateTest(void)
    {
        ham_cursor_t *c1, *c2;
        ham_key_t key;
        ham_record_t rec;
        int value=0;
        ::memset(&key, 0, sizeof(key));

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
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_find(c2, &key, 0));
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c1, 0));
        CPPUNIT_ASSERT(bt_cursor_is_nil((ham_bt_cursor_t *)c1));
        CPPUNIT_ASSERT(bt_cursor_is_nil((ham_bt_cursor_t *)c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

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
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_find(c2, &key, 0));
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, bt_cursor_uncouple((ham_bt_cursor_t *)c1, 0));
        CPPUNIT_ASSERT_EQUAL(0, bt_cursor_uncouple((ham_bt_cursor_t *)c2, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c1, 0));
        CPPUNIT_ASSERT(bt_cursor_is_nil((ham_bt_cursor_t *)c1));
        CPPUNIT_ASSERT(bt_cursor_is_nil((ham_bt_cursor_t *)c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

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
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_move(c2, &key, &rec, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c1, 0));
        CPPUNIT_ASSERT(bt_cursor_is_nil((ham_bt_cursor_t *)c1));
        CPPUNIT_ASSERT(bt_cursor_is_nil((ham_bt_cursor_t *)c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

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
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_move(c2, &key, &rec, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, bt_cursor_uncouple((ham_bt_cursor_t *)c1, 0));
        CPPUNIT_ASSERT_EQUAL(0, bt_cursor_uncouple((ham_bt_cursor_t *)c2, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c1, 0));
        CPPUNIT_ASSERT(bt_cursor_is_nil((ham_bt_cursor_t *)c1));
        CPPUNIT_ASSERT(bt_cursor_is_nil((ham_bt_cursor_t *)c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, HAM_CURSOR_FIRST));
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

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
        CPPUNIT_ASSERT_EQUAL(1, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_move(c2, &key, &rec, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c1, 0));
        CPPUNIT_ASSERT(bt_cursor_is_nil((ham_bt_cursor_t *)c1));
        CPPUNIT_ASSERT(!bt_cursor_is_nil((ham_bt_cursor_t *)c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, HAM_CURSOR_LAST));
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(2, *(int *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c1));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c2));
    }

    void moveToPreviousDuplicateTest(void)
    {
        ham_cursor_t *c;

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
        insertData(0, "1111111111");
        insertData(0, "2222222222");
        insertData(0, "3333333333");
        insertData(0, "4444444444");
        insertData(0, "5555555555");
        insertData(0, "6666666666");
        insertData(0, "7777777777");
        insertData(0, "8888888888");
        insertData(0, "9999999999");
        insertData(0, "0000000000");
        insertData("1", "xxxxxxxx");

        checkData(c, HAM_CURSOR_LAST,     0, "xxxxxxxx");
        checkData(c, HAM_CURSOR_PREVIOUS, 0, "0000000000");

        checkData(c, HAM_CURSOR_LAST,     0, "xxxxxxxx");
        checkData(c, HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES, 0, "1111111111");

        checkData(c, HAM_CURSOR_LAST,     0, "xxxxxxxx");
        checkData(c, HAM_CURSOR_PREVIOUS|HAM_ONLY_DUPLICATES, 
                HAM_KEY_NOT_FOUND, 0);

        checkData(c, HAM_CURSOR_FIRST,    0, "1111111111");
        checkData(c, HAM_CURSOR_PREVIOUS|HAM_ONLY_DUPLICATES, 
                HAM_KEY_NOT_FOUND, 0);
        checkData(c, HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES,
                HAM_KEY_NOT_FOUND, 0);
        checkData(c, HAM_CURSOR_PREVIOUS,
                HAM_KEY_NOT_FOUND, 0);

        ham_cursor_close(c);
    }

    void overwriteCursorTest(void)
    {
        ham_cursor_t *c;
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
        insertData(0, "1111111111");
        insertData(0, "2222222222");
        insertData(0, "33");
        insertData(0, "4444444444");
        insertData(0, "5555555555");

        checkData(c, HAM_CURSOR_FIRST,    0, "1111111111");
        checkData(c, HAM_CURSOR_NEXT,     0, "2222222222");
        checkData(c, HAM_CURSOR_NEXT,     0, "33");

        ::memset(&rec, 0, sizeof(rec));
        rec.data=(void *)"3333333333333333333333333333333333333333333333333333";
        rec.size=(ham_size_t)strlen((char *)rec.data)+1;
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_overwrite(c, &rec, 0));
        checkData(c, HAM_CURSOR_FIRST,    0, "1111111111");
        checkData(c, HAM_CURSOR_NEXT,     0, "2222222222");
        checkData(c, HAM_CURSOR_NEXT,     0, 
                "3333333333333333333333333333333333333333333333333333");
        checkData(c, HAM_CURSOR_NEXT,     0, "4444444444");

        ::memset(&rec, 0, sizeof(rec));
        rec.data=(void *)"44";
        rec.size=(ham_size_t)strlen((char *)rec.data)+1;
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_overwrite(c, &rec, 0));

        checkData(c, HAM_CURSOR_LAST,     0, "5555555555");
        checkData(c, HAM_CURSOR_PREVIOUS, 0, "44");
        checkData(c, HAM_CURSOR_PREVIOUS, 0, 
                "3333333333333333333333333333333333333333333333333333");

        ham_cursor_close(c);
    }

    void overwriteMultipleCursorTest(void)
    {
        ham_cursor_t *c1, *c2, *c3;
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c1));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c2));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c3));
        
        insertData(0, "1111111111");
        insertData(0, "2222222222");
        insertData(0, "33");
        insertData(0, "4444444444");
        insertData(0, "5555555555");

        checkData(c1, HAM_CURSOR_FIRST,    0, "1111111111");
        checkData(c1, HAM_CURSOR_NEXT,     0, "2222222222");
        checkData(c1, HAM_CURSOR_NEXT,     0, "33");
        checkData(c2, HAM_CURSOR_FIRST,    0, "1111111111");
        checkData(c3, HAM_CURSOR_FIRST,    0, "1111111111");
        checkData(c3, HAM_CURSOR_NEXT,     0, "2222222222");
        checkData(c3, HAM_CURSOR_NEXT,     0, "33");

        ::memset(&rec, 0, sizeof(rec));
        rec.data=(void *)"3333333333333333333333333333333333333333333333333333";
        rec.size=(ham_size_t)strlen((char *)rec.data)+1;
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_overwrite(c1, &rec, 0));
        checkData(c1, 0,                   0, 
                "3333333333333333333333333333333333333333333333333333");
        checkData(c2, HAM_CURSOR_FIRST,    0, "1111111111");
        checkData(c1, HAM_CURSOR_FIRST,    0, "1111111111");
        checkData(c1, HAM_CURSOR_NEXT,     0, "2222222222");
        checkData(c1, HAM_CURSOR_NEXT,     0, 
                "3333333333333333333333333333333333333333333333333333");
        checkData(c3, 0,                   0, 
                "3333333333333333333333333333333333333333333333333333");
        checkData(c1, HAM_CURSOR_NEXT,     0, "4444444444");
        checkData(c3, HAM_CURSOR_NEXT,     0, "4444444444");

        ::memset(&rec, 0, sizeof(rec));
        rec.data=(void *)"44";
        rec.size=(ham_size_t)strlen((char *)rec.data)+1;
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_overwrite(c1, &rec, 0));
        checkData(c3, 0,                   0, "44");
        checkData(c3, HAM_CURSOR_PREVIOUS, 0, 
                "3333333333333333333333333333333333333333333333333333");
        checkData(c3, HAM_CURSOR_NEXT,     0, "44");
        checkData(c3, HAM_CURSOR_NEXT,     0, "5555555555");

        checkData(c1, HAM_CURSOR_LAST,     0, "5555555555");
        checkData(c1, HAM_CURSOR_PREVIOUS, 0, "44");
        checkData(c1, HAM_CURSOR_PREVIOUS, 0, 
                "3333333333333333333333333333333333333333333333333333");
        checkData(c1, HAM_CURSOR_FIRST,    0, "1111111111");
        checkData(c2, HAM_CURSOR_FIRST,    0, "1111111111");

        ham_cursor_close(c1);
        ham_cursor_close(c2);
        ham_cursor_close(c3);
    }

    void invalidFlagsTest(void)
    {
        ham_cursor_t *c;

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_move(c, 0, 0, 
                        HAM_SKIP_DUPLICATES|HAM_ONLY_DUPLICATES));
        
        ham_cursor_close(c);
    }

    void overwriteTest(void)
    {
        ham_cursor_t *c1, *c2;
        ham_record_t rec;
        ham_key_t key;

        insertData(0, "111");
        insertData(0, "2222222222");
        insertData(0, "333");
        insertData(0, "4444444444");

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c1));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c2));

        checkData(c1, HAM_CURSOR_FIRST,     0, "111");
        checkData(c1, HAM_CURSOR_NEXT,      0, "2222222222");
        checkData(c2, HAM_CURSOR_FIRST,     0, "111");

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        rec.data=(void *)"1111111111111111111111111111111111111111";
        rec.size=(ham_size_t)strlen((char *)rec.data)+1;
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
        checkData(c2, 0,                    0, 
                "1111111111111111111111111111111111111111");

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        rec.data=(void *)"00";
        rec.size=(ham_size_t)strlen((char *)rec.data)+1;
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
        checkData(c2, 0,                    0, "00");

        checkData(c1, HAM_CURSOR_PREVIOUS,  0, "00");
        checkData(c1, HAM_CURSOR_FIRST,     0, "00");

        ham_cursor_close(c1);
        ham_cursor_close(c2);
    }

    void endianTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_cursor_t *c;

        /* close the existing database handle */
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));

        /* generated with `cat ../COPYING.GPL2 | ./db5` */
#if HAM_LITTLE_ENDIAN
        CPPUNIT_ASSERT_EQUAL(true, 
            os::copy("data/dupe-endian-test-open-database-be.hdb", ".test"));
#else
        CPPUNIT_ASSERT_EQUAL(true, 
            os::copy("data/dupe-endian-test-open-database-le.hdb", ".test"));
#endif
        CPPUNIT_ASSERT_EQUAL(0, ham_open(m_db, ".test", 0));

        memset(&key, 0, sizeof(key));
        key.data=(void *)"written";
        key.size=8;

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_find(c, &key, 0));
        memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_move(c, 0, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(_ham_byteswap32(125), *(unsigned int *)rec.data);

        memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_move(c, 0, &rec, 
                                HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES));
        CPPUNIT_ASSERT_EQUAL(_ham_byteswap32(142), *(unsigned int *)rec.data);
        
        memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_move(c, 0, &rec, 
                                HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES));
        CPPUNIT_ASSERT_EQUAL(_ham_byteswap32(235), *(unsigned int *)rec.data);

        memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_move(c, 0, &rec, 
                                HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES));
        CPPUNIT_ASSERT_EQUAL(_ham_byteswap32(331), *(unsigned int *)rec.data);

        memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, ham_cursor_move(c, 0, &rec, 
                                HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c));
    }

    void eraseCursorTest(void)
    {
        ham_key_t key;
        ham_cursor_t *c;

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
        insertData(0, "1111111111");
        insertData(0, "2222222222");
        insertData(0, "3333333333");
        insertData(0, "4444444444");
        insertData(0, "5555555555");

        memset(&key, 0, sizeof(key));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_find(c, &key, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c, 0));

        memset(&key, 0, sizeof(key));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_find(c, &key, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c, 0));

        memset(&key, 0, sizeof(key));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_find(c, &key, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c, 0));

        memset(&key, 0, sizeof(key));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_find(c, &key, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c, 0));

        memset(&key, 0, sizeof(key));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_find(c, &key, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c, 0));

        memset(&key, 0, sizeof(key));
        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                        ham_cursor_find(c, &key, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c));
    }

    void insertLastTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_cursor_t *c;
        const char *values[]={"11111", "222222", "3333333", "44444444"};

        memset(&key, 0, sizeof(key));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
        for (int i=0; i<4; i++) {
            memset(&rec, 0, sizeof(rec));
            rec.data=(void *)values[i];
            rec.size=(ham_size_t)strlen((char *)rec.data)+1;
            CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_insert(c, &key, &rec, 
                                HAM_DUPLICATE_INSERT_LAST));
            memset(&rec, 0, sizeof(rec));
            CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_move(c, 0, &rec, 0));
            CPPUNIT_ASSERT_EQUAL(strlen((char *)rec.data)+1,
                            strlen(values[i])+1);
            CPPUNIT_ASSERT_EQUAL(0, strcmp(values[i], (char *)rec.data));
            CPPUNIT_ASSERT_EQUAL((ham_size_t)i, 
                            bt_cursor_get_dupe_id((ham_bt_cursor_t *)c));
        }

        checkData(c, HAM_CURSOR_FIRST,    0, values[0]);
        checkData(c, HAM_CURSOR_NEXT,     0, values[1]);
        checkData(c, HAM_CURSOR_NEXT,     0, values[2]);
        checkData(c, HAM_CURSOR_NEXT,     0, values[3]);
        checkData(c, HAM_CURSOR_NEXT,     HAM_KEY_NOT_FOUND, values[3]);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c));
    }

    void insertFirstTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_cursor_t *c;
        const char *values[]={"11111", "222222", "3333333", "44444444"};

        memset(&key, 0, sizeof(key));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
        for (int i=0; i<4; i++) {
            memset(&rec, 0, sizeof(rec));
            rec.data=(void *)values[i];
            rec.size=(ham_size_t)strlen((char *)rec.data)+1;
            CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_insert(c, &key, &rec, 
                                HAM_DUPLICATE_INSERT_FIRST));
            memset(&rec, 0, sizeof(rec));
            CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_move(c, 0, &rec, 0));
            CPPUNIT_ASSERT_EQUAL(strlen((char *)rec.data)+1,
                            strlen(values[i])+1);
            CPPUNIT_ASSERT_EQUAL(0, strcmp(values[i], (char *)rec.data));
            CPPUNIT_ASSERT_EQUAL((ham_size_t)0, 
                            bt_cursor_get_dupe_id((ham_bt_cursor_t *)c));
        }

        checkData(c, HAM_CURSOR_FIRST,    0, values[3]);
        checkData(c, HAM_CURSOR_NEXT,     0, values[2]);
        checkData(c, HAM_CURSOR_NEXT,     0, values[1]);
        checkData(c, HAM_CURSOR_NEXT,     0, values[0]);
        checkData(c, HAM_CURSOR_NEXT,     HAM_KEY_NOT_FOUND, values[0]);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c));
    }

    void insertAfterTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_cursor_t *c;
        const char *values[]={"11111", "222222", "3333333", "44444444"};

        memset(&key, 0, sizeof(key));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
        for (int i=0; i<4; i++) {
            memset(&rec, 0, sizeof(rec));
            rec.data=(void *)values[i];
            rec.size=(ham_size_t)strlen((char *)rec.data)+1;
            CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_insert(c, &key, &rec, 
                                HAM_DUPLICATE_INSERT_AFTER));
            memset(&rec, 0, sizeof(rec));
            CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_move(c, 0, &rec, 0));
            CPPUNIT_ASSERT_EQUAL(strlen((char *)rec.data)+1,
                            strlen(values[i])+1);
            CPPUNIT_ASSERT_EQUAL(0, strcmp(values[i], (char *)rec.data));
            CPPUNIT_ASSERT_EQUAL((ham_size_t)(i>=1 ? 1 : 0), 
                            bt_cursor_get_dupe_id((ham_bt_cursor_t *)c));
            CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_move(c, 0, 0, HAM_CURSOR_FIRST));
        }

        checkData(c, HAM_CURSOR_FIRST,    0, values[0]);
        checkData(c, HAM_CURSOR_NEXT,     0, values[3]);
        checkData(c, HAM_CURSOR_NEXT,     0, values[2]);
        checkData(c, HAM_CURSOR_NEXT,     0, values[1]);
        checkData(c, HAM_CURSOR_NEXT,     HAM_KEY_NOT_FOUND, values[0]);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c));
    }

    void insertBeforeTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_cursor_t *c;
        const char *values[]={"11111", "222222", "3333333", "44444444"};

        memset(&key, 0, sizeof(key));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
        for (int i=0; i<4; i++) {
            memset(&rec, 0, sizeof(rec));
            rec.data=(void *)values[i];
            rec.size=(ham_size_t)strlen((char *)rec.data)+1;
            CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_insert(c, &key, &rec, 
                                HAM_DUPLICATE_INSERT_BEFORE));
            memset(&rec, 0, sizeof(rec));
            CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_move(c, 0, &rec, 0));
            CPPUNIT_ASSERT_EQUAL(strlen((char *)rec.data)+1,
                            strlen(values[i])+1);
            CPPUNIT_ASSERT_EQUAL(0, strcmp(values[i], (char *)rec.data));
            CPPUNIT_ASSERT_EQUAL((ham_size_t)(i<=1 ? 0 : i-1),
                            bt_cursor_get_dupe_id((ham_bt_cursor_t *)c));
            CPPUNIT_ASSERT_EQUAL(0, 
                        ham_cursor_move(c, 0, 0, HAM_CURSOR_LAST));
        }

        checkData(c, HAM_CURSOR_FIRST,    0, values[1]);
        checkData(c, HAM_CURSOR_NEXT,     0, values[2]);
        checkData(c, HAM_CURSOR_NEXT,     0, values[3]);
        checkData(c, HAM_CURSOR_NEXT,     0, values[0]);
        checkData(c, HAM_CURSOR_NEXT,     HAM_KEY_NOT_FOUND, values[0]);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c));
    }

    void overwriteVariousSizesTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_cursor_t *c;
        ham_size_t sizes[] ={0, 6, 8, 10};
        const char *values[]={0, "55555", "8888888", "999999999"};
        const char *newvalues[4];

        memset(&key, 0, sizeof(key));

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
        for (int s=0; s<5; s++) {
            for (int i=s, j=0; i<s+4; i++, j++) {
                memset(&rec, 0, sizeof(rec));
                rec.size=sizes[i%4];
                if (sizes[i%4]) {
                    rec.data=(void *)values[i%4];
                    newvalues[j]=values[i%4];
                }
                else {
                    rec.data=0;
                    newvalues[j]=0;
                }

                if (s==0) {
                    /* first round: insert the duplicates */
                    CPPUNIT_ASSERT_EQUAL(0, 
                            ham_cursor_insert(c, &key, &rec, 
                                    HAM_DUPLICATE_INSERT_LAST));
                }
                else {
                    /* other rounds: just overwrite them */
                    CPPUNIT_ASSERT_EQUAL(0, 
                            ham_cursor_overwrite(c, &rec, 0));
                    if (i!=(s+4)-1)
                        CPPUNIT_ASSERT_EQUAL(0, 
                            ham_cursor_move(c, 0, 0, HAM_CURSOR_NEXT));
                }
            }

            checkData(c, HAM_CURSOR_FIRST,    0, newvalues[0]);
            checkData(c, HAM_CURSOR_NEXT,     0, newvalues[1]);
            checkData(c, HAM_CURSOR_NEXT,     0, newvalues[2]);
            checkData(c, HAM_CURSOR_NEXT,     0, newvalues[3]);
            checkData(c, HAM_CURSOR_NEXT,     HAM_KEY_NOT_FOUND, newvalues[1]);

            /* move to first element */
            checkData(c, HAM_CURSOR_FIRST,    0, newvalues[0]);
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c));
    }

    void getDuplicateCountTest(void)
    {
        ham_size_t count;
        ham_cursor_t *c;

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));

        CPPUNIT_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, 
                ham_cursor_get_duplicate_count(c, &count, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)0, count);

        insertData(0, "1111111111");
        checkData(c, HAM_CURSOR_NEXT,     0, "1111111111");
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_get_duplicate_count(c, &count, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)1, count);

        insertData(0, "2222222222");
        checkData(c, HAM_CURSOR_NEXT,     0, "2222222222");
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_get_duplicate_count(c, &count, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)2, count);

        insertData(0, "3333333333");
        checkData(c, HAM_CURSOR_NEXT,     0, "3333333333");
        bt_cursor_uncouple((ham_bt_cursor_t *)c, 0);
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_get_duplicate_count(c, &count, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)3, count);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_erase(c, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, 
                ham_cursor_get_duplicate_count(c, &count, 0));
        checkData(c, HAM_CURSOR_FIRST,    0, "1111111111");
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_get_duplicate_count(c, &count, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)2, count);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c));

        if (!(m_flags&HAM_IN_MEMORY_DB)) {
            /* reopen the database */
            CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
            CPPUNIT_ASSERT_EQUAL(0, ham_open(m_db, ".test", m_flags));
            CPPUNIT_ASSERT(db_get_rt_flags(m_db)&HAM_ENABLE_DUPLICATES);

            CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));

            checkData(c, HAM_CURSOR_NEXT,     0, "1111111111");
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_cursor_get_duplicate_count(c, &count, 0));
            CPPUNIT_ASSERT_EQUAL((ham_size_t)2, count);

            CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(c));
        }
    }

};

class InMemoryDupeTest : public DupeTest
{
    CPPUNIT_TEST_SUITE(InMemoryDupeTest);
    CPPUNIT_TEST      (invalidFlagsTest);
    CPPUNIT_TEST      (insertDuplicatesTest);
    //CPPUNIT_TEST      (overwriteDuplicatesTest);
    //CPPUNIT_TEST      (overwriteVariousDuplicatesTest);
    CPPUNIT_TEST      (insertMoveForwardTest);
    CPPUNIT_TEST      (insertMoveBackwardTest);
    CPPUNIT_TEST      (insertEraseTest);
    CPPUNIT_TEST      (insertTest);
    CPPUNIT_TEST      (insertSkipDuplicatesTest);
    CPPUNIT_TEST      (insertOnlyDuplicatesTest);
    CPPUNIT_TEST      (coupleUncoupleTest);
    CPPUNIT_TEST      (moveToLastDuplicateTest);
    CPPUNIT_TEST      (eraseDuplicateTest);
    CPPUNIT_TEST      (eraseDuplicateUncoupledTest);
    CPPUNIT_TEST      (eraseSecondDuplicateTest);
    CPPUNIT_TEST      (eraseSecondDuplicateUncoupledTest);
    CPPUNIT_TEST      (eraseOtherDuplicateTest);
    CPPUNIT_TEST      (eraseOtherDuplicateUncoupledTest);
    CPPUNIT_TEST      (eraseMiddleDuplicateTest);
    CPPUNIT_TEST      (eraseTinyDuplicatesTest);
    CPPUNIT_TEST      (moveToPreviousDuplicateTest);
    CPPUNIT_TEST      (overwriteTest);
    CPPUNIT_TEST      (overwriteCursorTest);
    CPPUNIT_TEST      (overwriteMultipleCursorTest);
    CPPUNIT_TEST      (eraseCursorTest);
    CPPUNIT_TEST      (insertLastTest);
    CPPUNIT_TEST      (insertFirstTest);
    CPPUNIT_TEST      (insertAfterTest);
    CPPUNIT_TEST      (insertBeforeTest);
    CPPUNIT_TEST      (overwriteVariousSizesTest);
    CPPUNIT_TEST      (getDuplicateCountTest);
    CPPUNIT_TEST_SUITE_END();

public:
    InMemoryDupeTest()
    :   DupeTest(HAM_IN_MEMORY_DB)
    {
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(DupeTest);
CPPUNIT_TEST_SUITE_REGISTRATION(InMemoryDupeTest);
