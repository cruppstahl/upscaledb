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
#include "memtracker.h"

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
#if WIN32
        (void)DeleteFileA((LPCSTR)".test");
#else
        if (unlink(".test")) {
            if (errno!=2)
                printf("failed to unlink .test: %s\n", strerror(errno));
        }
#endif
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&m_db));
        CPPUNIT_ASSERT_EQUAL(0, ham_create(m_db, ".test", 
                    m_flags|HAM_ENABLE_DUPLICATES, 0664));

        m_data.resize(0);
    }

    void tearDown()
    {
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
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
        rec.size=::strlen(data)+1;
        key.data=(void *)k;
        key.size=k ? ::strlen(k)+1 : 0;

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
            CPPUNIT_ASSERT_EQUAL(rec.size, (ham_size_t)::strlen(data)+1);
            CPPUNIT_ASSERT_EQUAL(0, ::memcmp(rec.data, data, rec.size));
        }
    }

    void insertTest(void)
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

        CPPUNIT_ASSERT_EQUAL(0, db_uncouple_all_cursors(page));
        checkData(c, HAM_CURSOR_NEXT,     0, "aaaaaaaaaa");
        CPPUNIT_ASSERT_EQUAL(0, db_uncouple_all_cursors(page));
        checkData(c, HAM_CURSOR_NEXT,     0, "1111111111");
        CPPUNIT_ASSERT_EQUAL(0, db_uncouple_all_cursors(page));
        checkData(c, HAM_CURSOR_NEXT,     0, "2222222222");
        CPPUNIT_ASSERT_EQUAL(0, db_uncouple_all_cursors(page));
        checkData(c, HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES, 0, "bbbbbbbbbb");
        CPPUNIT_ASSERT_EQUAL(0, db_uncouple_all_cursors(page));
        checkData(c, HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES, 0, "cccccccccc");
        CPPUNIT_ASSERT_EQUAL(0, db_uncouple_all_cursors(page));
        checkData(c, HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES, 0, "bbbbbbbbbb");
        CPPUNIT_ASSERT_EQUAL(0, db_uncouple_all_cursors(page));
        checkData(c, HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES, 0, "1111111111");
        CPPUNIT_ASSERT_EQUAL(0, db_uncouple_all_cursors(page));
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

    void reopenTest(void)
    {
        ham_cursor_t *c;

        insertData("000", "aaaaaaaaaa");
        insertData("111", "1111111111");
        insertData("111", "2222222222");
        insertData("111", "3333333333");
        insertData("222", "bbbbbbbbbb");

        /* reopen the database */
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
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
        rec.size=strlen((char *)rec.data)+1;
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_overwrite(c, &rec, 0));
        checkData(c, HAM_CURSOR_FIRST,    0, "1111111111");
        checkData(c, HAM_CURSOR_NEXT,     0, "2222222222");
        checkData(c, HAM_CURSOR_NEXT,     0, 
                "3333333333333333333333333333333333333333333333333333");
        checkData(c, HAM_CURSOR_NEXT,     0, "4444444444");

        ::memset(&rec, 0, sizeof(rec));
        rec.data=(void *)"44";
        rec.size=strlen((char *)rec.data)+1;
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
        rec.size=strlen((char *)rec.data)+1;
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
        rec.size=strlen((char *)rec.data)+1;
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
        rec.size=strlen((char *)rec.data)+1;
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
        checkData(c2, 0,                    0, 
                "1111111111111111111111111111111111111111");

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        rec.data=(void *)"00";
        rec.size=strlen((char *)rec.data)+1;
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
        checkData(c2, 0,                    0, "00");

        checkData(c1, HAM_CURSOR_PREVIOUS,  0, "00");
        checkData(c1, HAM_CURSOR_FIRST,     0, "00");

        ham_cursor_close(c1);
        ham_cursor_close(c2);
    }

};

class InMemoryDupeTest : public DupeTest
{
    CPPUNIT_TEST_SUITE(InMemoryDupeTest);
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
    CPPUNIT_TEST      (eraseDuplicateTest);
    CPPUNIT_TEST      (eraseDuplicateUncoupledTest);
    CPPUNIT_TEST      (eraseSecondDuplicateTest);
    CPPUNIT_TEST      (eraseSecondDuplicateUncoupledTest);
    CPPUNIT_TEST      (eraseOtherDuplicateTest);
    CPPUNIT_TEST      (eraseOtherDuplicateUncoupledTest);
    CPPUNIT_TEST      (eraseMiddleDuplicateTest);
    CPPUNIT_TEST      (moveToPreviousDuplicateTest);
    CPPUNIT_TEST      (overwriteTest);
    CPPUNIT_TEST      (overwriteCursorTest);
    CPPUNIT_TEST      (overwriteMultipleCursorTest);
    CPPUNIT_TEST_SUITE_END();

public:
    InMemoryDupeTest()
    :   DupeTest(HAM_IN_MEMORY_DB)
    {
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(DupeTest);
CPPUNIT_TEST_SUITE_REGISTRATION(InMemoryDupeTest);
