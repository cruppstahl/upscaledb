/**
 * Copyright (C) 2005-2011 Christoph Rupp (chris@crupp.de).
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
#include <vector>
#include <algorithm>
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/blob.h"
#include "../src/backend.h"
#include "../src/btree.h"
#include "../src/endianswap.h"
#include "../src/cursor.h"
#include "../src/env.h"
#include "../src/btree_cursor.h"
#include "memtracker.h"
#include "os.hpp"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;

static int
__compare_numbers(ham_db_t *db, 
                  const ham_u8_t *lhs, ham_size_t lhs_length,
                  const ham_u8_t *rhs, ham_size_t rhs_length)
{
    ham_u32_t ulhs=*(ham_u32_t *)lhs;
    ham_u32_t urhs=*(ham_u32_t *)rhs;

    (void)db;

    if (ulhs<urhs)
        return -1;
    if (ulhs==urhs)
        return 0;
    return 1;
}

class DupeTest : public hamsterDB_fixture
{
	define_super(hamsterDB_fixture);

public:
    DupeTest(ham_u32_t flags=0, const char *name="DupeTest")
    :   hamsterDB_fixture(name), m_flags(flags)
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(DupeTest, invalidFlagsTest);
        BFC_REGISTER_TEST(DupeTest, insertDuplicatesTest);
        BFC_REGISTER_TEST(DupeTest, overwriteDuplicatesTest);
        BFC_REGISTER_TEST(DupeTest, overwriteVariousDuplicatesTest);
        BFC_REGISTER_TEST(DupeTest, insertMoveForwardTest);
        BFC_REGISTER_TEST(DupeTest, insertMoveBackwardTest);
        BFC_REGISTER_TEST(DupeTest, insertEraseTest);
        BFC_REGISTER_TEST(DupeTest, insertTest);
        BFC_REGISTER_TEST(DupeTest, insertSkipDuplicatesTest);
        BFC_REGISTER_TEST(DupeTest, insertOnlyDuplicatesTest);
        BFC_REGISTER_TEST(DupeTest, coupleUncoupleTest);
        BFC_REGISTER_TEST(DupeTest, moveToLastDuplicateTest);

        /*
         * insert 2 dupes, create 2 cursors (both on the first dupe).
         * delete the first cursor, make sure that both cursors are 
         * NILled and the second dupe is still available 
         */
        BFC_REGISTER_TEST(DupeTest, eraseDuplicateTest);

        /*
         * same as above, but uncouples the cursor before the first cursor
         * is deleted
         */
        BFC_REGISTER_TEST(DupeTest, eraseDuplicateUncoupledTest);

        /*
         * insert 2 dupes, create 2 cursors (both on the second dupe).
         * delete the first cursor, make sure that both cursors are 
         * NILled and the first dupe is still available 
         */
        BFC_REGISTER_TEST(DupeTest, eraseSecondDuplicateTest);

        /*
         * same as above, but uncouples the cursor before the second cursor
         * is deleted
         */
        BFC_REGISTER_TEST(DupeTest, eraseSecondDuplicateUncoupledTest);

        /*
         * insert 2 dupes, create 2 cursors (one on the first, the other on the
         * second dupe). delete the first cursor, make sure that it's NILled
         * and the other cursor is still valid.
         */
        BFC_REGISTER_TEST(DupeTest, eraseOtherDuplicateTest);

        /*
         * same as above, but uncouples the cursor before the second cursor
         * is deleted
         */
        BFC_REGISTER_TEST(DupeTest, eraseOtherDuplicateUncoupledTest);

        /*
         * inserts 3 dupes, creates 2 cursors on the middle item; delete the
         * first cursor, make sure that the second is NILled and that the first
         * and last item still exists
         */
        BFC_REGISTER_TEST(DupeTest, eraseMiddleDuplicateTest);

        /*
         * inserts a few TINY dupes, then erases them all but the last element
         */
        BFC_REGISTER_TEST(DupeTest, eraseTinyDuplicatesTest);

        /*
         * inserts a few duplicates, reopens the database; continues inserting
         */
        BFC_REGISTER_TEST(DupeTest, reopenTest);

        /*
         * test ham_cursor_move(... HAM_CURSOR_PREVIOUS)
         */
        BFC_REGISTER_TEST(DupeTest, moveToPreviousDuplicateTest);

        /*
         * overwrite duplicates using ham_insert(... HAM_OVERWRITE)
         */
        BFC_REGISTER_TEST(DupeTest, overwriteTest);

        /*
         * overwrite duplicates using ham_cursor_insert(... HAM_OVERWRITE)
         */
        BFC_REGISTER_TEST(DupeTest, overwriteCursorTest);

        /*
         * same as overwriteCursorTest, but uses multiple cursors and makes
         * sure that their positions are not modified
         */
        BFC_REGISTER_TEST(DupeTest, overwriteMultipleCursorTest);

        /*
         * reads a big-endian database (if started on a little-endian system)
         * or vice versa
         */
        BFC_REGISTER_TEST(DupeTest, endianTest);

        /*
         * insert a few duplicate items, then delete them all with a cursor
         */
        BFC_REGISTER_TEST(DupeTest, eraseCursorTest);

        /*
         * tests HAM_DUPLICATE_INSERT_LAST and makes sure that the cursor
         * always points to the inserted duplicate
         */
        BFC_REGISTER_TEST(DupeTest, insertLastTest);

        /*
         * tests HAM_DUPLICATE_INSERT_FIRST and makes sure that the cursor
         * always points to the inserted duplicate
         */
        BFC_REGISTER_TEST(DupeTest, insertFirstTest);

        /*
         * tests HAM_DUPLICATE_INSERT_AFTER and makes sure that the cursor
         * always points to the inserted duplicate
         */
        BFC_REGISTER_TEST(DupeTest, insertAfterTest);

        /*
         * tests HAM_DUPLICATE_INSERT_BEFORE and makes sure that the cursor
         * always points to the inserted duplicate
         */
        BFC_REGISTER_TEST(DupeTest, insertBeforeTest);

        /*
         * overwrite NULL-, TINY- and SMALL-duplicates with other
         * NULL-, TINY- and SMALL-duplicates
         */
        BFC_REGISTER_TEST(DupeTest, overwriteVariousSizesTest);

        /*
         * tests get_cuplicate_count
         */
        BFC_REGISTER_TEST(DupeTest, getDuplicateCountTest);

        /*
         * insert a lot of duplicates to provoke a page-split in the duplicate
         * table
         */
        BFC_REGISTER_TEST(DupeTest, insertManyManyTest);

        /*
         * insert several duplicates; then set a cursor to the 2nd duplicate. 
         * clone the cursor, move it to the next element. then erase the 
         * first cursor.
         */
        BFC_REGISTER_TEST(DupeTest, cloneTest);
    }

protected:
    ham_u32_t m_flags;
    ham_db_t *m_db;
    std::vector<std::string> m_data;

public:
    virtual void setup() 
	{ 
		__super::setup();

        (void)os::unlink(BFC_OPATH(".test"));

        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        BFC_ASSERT_EQUAL(0, ham_create(m_db, BFC_OPATH(".test"), 
                    m_flags|HAM_ENABLE_DUPLICATES, 0664));

        m_data.resize(0);
    }

    virtual void teardown() 
	{ 
		__super::teardown();

        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_AUTO_CLEANUP));
        BFC_ASSERT_EQUAL(0, ham_delete(m_db));
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
            BFC_ASSERT_EQUAL(0, 
                    ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        }

        ::memset(&data, 0x15, sizeof(data));
        BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec2, 0));
        BFC_ASSERT_EQUAL((ham_size_t)sizeof(data), rec2.size);
        BFC_ASSERT_EQUAL(0, ::memcmp(data, rec2.data, sizeof(data)));
    }

    void overwriteDuplicatesTest(void)
    {
        ham_key_t key;
        ham_record_t rec, rec2;
        ham_cursor_t *c;
        ham_size_t count;
        char data[16];
        ::memset(&rec2, 0, sizeof(rec2));

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));

        for (int i=0; i<5; i++) {
            ::memset(&key, 0, sizeof(key));
            ::memset(&rec, 0, sizeof(rec));
            rec.data=data;
            rec.size=sizeof(data);
            ::memset(&data, i+0x15, sizeof(data));
            BFC_ASSERT_EQUAL(0, 
                    ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        }

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        rec.data=data;
        rec.size=sizeof(data);
        ::memset(&data, 0x99, sizeof(data));
        BFC_ASSERT_EQUAL(0, 
                ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));

        BFC_ASSERT_EQUAL(0, ham_cursor_move(c, &key, &rec2, 
                    HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL((ham_size_t)sizeof(data), rec2.size);
        BFC_ASSERT_EQUAL(0, ::memcmp(data, rec2.data, sizeof(data)));

        for (int i=1; i<5; i++) {
            ::memset(&key, 0, sizeof(key));
            ::memset(&rec2, 0, sizeof(rec));
            ::memset(&data, i+0x15, sizeof(data));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c, &key, &rec2, HAM_CURSOR_NEXT));
            BFC_ASSERT_EQUAL((ham_size_t)sizeof(data), rec2.size);
            BFC_ASSERT_EQUAL(0, ::memcmp(data, rec2.data, sizeof(data)));
        }

        BFC_ASSERT_EQUAL(0, 
                ham_cursor_move(c, 0, 0, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_get_duplicate_count(c, &count, 0));
        BFC_ASSERT_EQUAL((ham_size_t)5, count);

        BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
    }

    void overwriteVariousDuplicatesTest(void)
    {
        ham_key_t key;
        ham_record_t rec, rec2;
#define MAX 10
        unsigned sizes[MAX]={0, 1, 2, 3, 4, 5, 936, 5, 100, 50};
        char *data=0;
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
            BFC_ASSERT_EQUAL(0, 
                    ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
            BFC_ASSERT_EQUAL(sizes[i], rec.size);
            if (sizes[i]) {
                BFC_ASSERT_EQUAL(0, ::memcmp(data, rec.data, sizes[i]));
                free(data);
            }
        }

        BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec2, 0));
        BFC_ASSERT_EQUAL((ham_size_t)0, rec2.size);

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));

        for (unsigned i=0; i<MAX; i++) {
            ::memset(&key, 0, sizeof(key));
            ::memset(&rec, 0, sizeof(rec));
            if (sizes[i]) {
                data=(char *)malloc(sizes[i]);
                ::memset(data, i+0x15, sizes[i]);
            }
            rec.data=sizes[i] ? data : 0;
            rec.size=sizes[i];
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));
            BFC_ASSERT_EQUAL(sizes[i], rec.size);
            if (sizes[i]) {
                BFC_ASSERT_EQUAL(0, ::memcmp(data, rec.data, sizes[i]));
                free(data);
            }
        }

        BFC_ASSERT_EQUAL(0, 
                ham_cursor_get_duplicate_count(cursor, &count, 0));
        BFC_ASSERT_EQUAL((ham_size_t)MAX, count);

        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));

        for (int i=MAX-1; i>=0; i--) {
            ::memset(&key, 0, sizeof(key));
            ::memset(&rec, 0, sizeof(rec));
            if (sizes[i]) {
                data=(char *)malloc(sizes[i]);
                ::memset(data, i+0x15, sizes[i]);
            }
            rec.data=sizes[i] ? data : 0;
            rec.size=sizes[i];
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_PREVIOUS));
            BFC_ASSERT_EQUAL(sizes[i], rec.size);
            if (sizes[i]) {
                BFC_ASSERT_EQUAL(0, ::memcmp(data, rec.data, sizes[i]));
                free(data);
            }
        }

        BFC_ASSERT_EQUAL(0, 
                ham_cursor_get_duplicate_count(cursor, &count, 0));
        BFC_ASSERT_EQUAL((ham_size_t)MAX, count);

        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        data=(char *)malloc(16);
        ::memset(data, 0x99, 16);
        rec.data=data;
        rec.size=16;
        BFC_ASSERT_EQUAL(0, 
                ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));

        BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec2, 0));
        BFC_ASSERT_EQUAL((ham_size_t)16, rec2.size);
        BFC_ASSERT_EQUAL(0, ::memcmp(data, rec2.data, 16));
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
            BFC_ASSERT_EQUAL(0, 
                    ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        }

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));

        for (int i=0; i<5; i++) {
            ::memset(&key, 0, sizeof(key));
            ::memset(&rec, 0, sizeof(rec));
            ::memset(&data, i+0x15, sizeof(data));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));
            BFC_ASSERT_EQUAL((ham_size_t)sizeof(data), rec.size);
            BFC_ASSERT_EQUAL(0, ::memcmp(data, rec.data, sizeof(data)));
        }

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_NEXT));

        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
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
            BFC_ASSERT_EQUAL(0, 
                    ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        }

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));

        for (int i=4; i>=0; i--) {
            ::memset(&key, 0, sizeof(key));
            ::memset(&rec, 0, sizeof(rec));
            ::memset(&data, i+0x15, sizeof(data));
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_PREVIOUS));
            BFC_ASSERT_EQUAL((ham_size_t)sizeof(data), rec.size);
            BFC_ASSERT_EQUAL(0, ::memcmp(data, rec.data, sizeof(data)));
        }

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_cursor_move(cursor, &key, &rec, HAM_CURSOR_PREVIOUS));

        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
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
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        ::memset(&data, 0x14, sizeof(data));
        rec.data=data;
        rec.size=sizeof(data);
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        ::memset(&data, 0x15, sizeof(data));
        rec.data=data;
        rec.size=sizeof(data);
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        ::memset(&rec, 0, sizeof(rec));
        ::memset(&data, 0x13, sizeof(data));
        BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL((ham_size_t)sizeof(data), rec.size);
        BFC_ASSERT_EQUAL(0, ::memcmp(data, rec.data, sizeof(data)));

        BFC_ASSERT_EQUAL(0, ham_erase(m_db, 0, &key, 0));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_find(m_db, 0, &key, &rec, 0));
    }

    void insert(ham_key_t *key, ham_record_t *rec)
    {
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, key, rec, HAM_DUPLICATE));
    }

    void find(ham_key_t *key, ham_record_t *rec)
    {
        ham_record_t record;
        ::memset(&record, 0, sizeof(record));

        BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, key, &record, 0));
        BFC_ASSERT_EQUAL(rec->size, record.size);
        BFC_ASSERT_EQUAL(0, ::memcmp(rec->data, record.data, rec->size));

        rec->_rid=record._rid;
    }

    void erase(ham_key_t *key)
    {
        BFC_ASSERT_EQUAL(0, ham_erase(m_db, 0, key, 0));
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

        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
    }

    void checkData(ham_cursor_t *cursor, ham_u32_t flags, 
            ham_status_t expected, const char *data)
    {
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        BFC_ASSERT_EQUAL(expected, 
                ham_cursor_move(cursor, &key, &rec, flags));

        if (expected==0) {
            if (data) {
                BFC_ASSERT_EQUAL(rec.size, (ham_size_t)::strlen(data)+1);
                BFC_ASSERT_EQUAL(0, ::memcmp(rec.data, data, rec.size));
            }
            else {
                BFC_ASSERT_EQUAL(rec.size, (ham_size_t)0);
                BFC_ASSERT_EQUAL((void *)0, rec.data);
            }
        }
    }

    void insertTest(void)
    {
        ham_cursor_t *c;
        ham_size_t count;

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
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

        BFC_ASSERT_EQUAL(0, 
                ham_cursor_get_duplicate_count(c, &count, 0));
        BFC_ASSERT_EQUAL((ham_size_t)10, count);

        ham_cursor_close(c);
    }

    void insertSkipDuplicatesTest(void)
    {
        ham_cursor_t *c;

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
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

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
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

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));

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

        ham_btree_t *be=(ham_btree_t *)((Database *)m_db)->get_backend();
        BFC_ASSERT_EQUAL(0, db_fetch_page(&page, (Database *)m_db,
                btree_get_rootpage(be), 0));
        BFC_ASSERT(page!=0);

        BFC_ASSERT_EQUAL(0, db_uncouple_all_cursors(page, 0));
        checkData(c, HAM_CURSOR_NEXT,     0, "aaaaaaaaaa");
        BFC_ASSERT_EQUAL(0, db_uncouple_all_cursors(page, 0));
        BFC_ASSERT_EQUAL(0, db_uncouple_all_cursors(page, 0));
        checkData(c, HAM_CURSOR_NEXT,     0, "1111111111");
        BFC_ASSERT_EQUAL(0, db_uncouple_all_cursors(page, 0));
        checkData(c, HAM_CURSOR_NEXT,     0, "2222222222");
        BFC_ASSERT_EQUAL(0, db_uncouple_all_cursors(page, 0));
        checkData(c, HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES, 0, "bbbbbbbbbb");
        BFC_ASSERT_EQUAL(0, db_uncouple_all_cursors(page, 0));
        checkData(c, HAM_CURSOR_NEXT|HAM_SKIP_DUPLICATES, 0, "cccccccccc");
        BFC_ASSERT_EQUAL(0, db_uncouple_all_cursors(page, 0));
        checkData(c, HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES, 0, "bbbbbbbbbb");
        BFC_ASSERT_EQUAL(0, db_uncouple_all_cursors(page, 0));
        checkData(c, HAM_CURSOR_PREVIOUS|HAM_SKIP_DUPLICATES, 0, "1111111111");
        BFC_ASSERT_EQUAL(0, db_uncouple_all_cursors(page, 0));
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
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

        ::memset(&rec, 0, sizeof(rec));
        value=2;
        rec.data=&value;
        rec.size=sizeof(value);
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c1));
        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                        ham_cursor_move(c1, &key, &rec, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(1, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                        ham_cursor_move(c2, &key, &rec, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(2, *(int *)rec.data);

        BFC_ASSERT_EQUAL(0, 
                btree_cursor_uncouple(((Cursor *)c2)->get_btree_cursor(), 0));
        BFC_ASSERT_EQUAL(0, ham_cursor_erase(c1, 0));
        BFC_ASSERT(((Cursor *)c1)->is_nil(Cursor::CURSOR_BTREE));
        BFC_ASSERT(!((Cursor *)c2)->is_nil(Cursor::CURSOR_BTREE));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(2, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, 0));
        BFC_ASSERT_EQUAL(2, *(int *)rec.data);

        BFC_ASSERT_EQUAL(0, ham_cursor_close(c1));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(c2));
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
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

        ::memset(&rec, 0, sizeof(rec));
        value=2;
        rec.data=&value;
        rec.size=sizeof(value);
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        ::memset(&rec, 0, sizeof(rec));
        value=3;
        rec.data=&value;
        rec.size=sizeof(value);
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c1));
        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                        ham_cursor_move(c1, &key, &rec, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(1, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                        ham_cursor_move(c1, &key, &rec, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(2, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                        ham_cursor_move(c2, &key, &rec, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(3, *(int *)rec.data);

        BFC_ASSERT_EQUAL(0, ham_cursor_erase(c1, 0));
        BFC_ASSERT(((Cursor *)c1)->is_nil(Cursor::CURSOR_BTREE));
        BFC_ASSERT(!((Cursor *)c2)->is_nil(Cursor::CURSOR_BTREE));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(1, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(3, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(3, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, HAM_CURSOR_PREVIOUS));
        BFC_ASSERT_EQUAL(1, *(int *)rec.data);

        BFC_ASSERT_EQUAL(0, ham_cursor_close(c1));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(c2));
    }

    void eraseTinyDuplicatesTest(void)
    {
        ham_cursor_t *c;

        insertData("111", "111");
        insertData("111", "222");
        insertData("111", "333");
        insertData("111", "444");
        insertData("111", "555");

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));

        checkData(c, HAM_CURSOR_FIRST,    0, "111");
        BFC_ASSERT_EQUAL(0, ham_cursor_erase(c, 0));
        checkData(c, HAM_CURSOR_FIRST,    0, "222");
        BFC_ASSERT_EQUAL(0, ham_cursor_erase(c, 0));
        checkData(c, HAM_CURSOR_FIRST,    0, "333");
        BFC_ASSERT_EQUAL(0, ham_cursor_erase(c, 0));
        checkData(c, HAM_CURSOR_FIRST,    0, "444");
        BFC_ASSERT_EQUAL(0, ham_cursor_erase(c, 0));
        checkData(c, HAM_CURSOR_FIRST,    0, "555");
        BFC_ASSERT_EQUAL(0, ham_cursor_erase(c, 0));
        checkData(c, HAM_CURSOR_FIRST,    HAM_KEY_NOT_FOUND, "555");

        BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
    }

    void reopenTest(void)
    {
        ham_cursor_t *c;

        insertData("000", "aaaaaaaaaa");
        insertData("111", "1111111111");
        insertData("111", "2222222222");
        insertData("111", "3333333333");
        insertData("222", "bbbbbbbbbb");

        if (!(m_flags&HAM_IN_MEMORY_DB)) {
			/* reopen the database */
			BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
			BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), m_flags));
		}
        BFC_ASSERT(((Database *)m_db)->get_rt_flags()&HAM_ENABLE_DUPLICATES);

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
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

        BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
    }

    void moveToLastDuplicateTest(void)
    {
        ham_cursor_t *c;

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
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
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

        ::memset(&rec, 0, sizeof(rec));
        value=2;
        rec.data=&value;
        rec.size=sizeof(value);
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c1));
        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c2));

        BFC_ASSERT_EQUAL(0, ham_cursor_find(c1, &key, 0));
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, 0));
        BFC_ASSERT_EQUAL(1, *(int *)rec.data);

        BFC_ASSERT_EQUAL(0, ham_cursor_find(c2, &key, 0));
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, 0));
        BFC_ASSERT_EQUAL(1, *(int *)rec.data);

        BFC_ASSERT_EQUAL(0, ham_cursor_erase(c1, 0));
        BFC_ASSERT(((Cursor *)c1)->is_nil(Cursor::CURSOR_BTREE));
        BFC_ASSERT(((Cursor *)c2)->is_nil(Cursor::CURSOR_BTREE));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(2, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(2, *(int *)rec.data);

        BFC_ASSERT_EQUAL(0, ham_cursor_close(c1));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(c2));
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
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

        ::memset(&rec, 0, sizeof(rec));
        value=2;
        rec.data=&value;
        rec.size=sizeof(value);
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c1));
        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c2));

        BFC_ASSERT_EQUAL(0, ham_cursor_find(c1, &key, 0));
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, 0));
        BFC_ASSERT_EQUAL(1, *(int *)rec.data);

        BFC_ASSERT_EQUAL(0, ham_cursor_find(c2, &key, 0));
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, 0));
        BFC_ASSERT_EQUAL(1, *(int *)rec.data);

        BFC_ASSERT_EQUAL(0, 
                btree_cursor_uncouple(((Cursor *)c1)->get_btree_cursor(), 0));
        BFC_ASSERT_EQUAL(0, 
                btree_cursor_uncouple(((Cursor *)c2)->get_btree_cursor(), 0));
        BFC_ASSERT_EQUAL(0, ham_cursor_erase(c1, 0));
        BFC_ASSERT(((Cursor *)c1)->is_nil(Cursor::CURSOR_BTREE));
        BFC_ASSERT(((Cursor *)c2)->is_nil(Cursor::CURSOR_BTREE));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(2, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(2, *(int *)rec.data);

        BFC_ASSERT_EQUAL(0, ham_cursor_close(c1));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(c2));
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
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

        ::memset(&rec, 0, sizeof(rec));
        value=2;
        rec.data=&value;
        rec.size=sizeof(value);
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c1));
        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                        ham_cursor_move(c1, &key, &rec, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(2, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                        ham_cursor_move(c2, &key, &rec, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(2, *(int *)rec.data);

        BFC_ASSERT_EQUAL(0, ham_cursor_erase(c1, 0));
        BFC_ASSERT(((Cursor *)c1)->is_nil(Cursor::CURSOR_BTREE));
        BFC_ASSERT(((Cursor *)c2)->is_nil(Cursor::CURSOR_BTREE));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(1, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(1, *(int *)rec.data);

        BFC_ASSERT_EQUAL(0, ham_cursor_close(c1));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(c2));
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
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

        ::memset(&rec, 0, sizeof(rec));
        value=2;
        rec.data=&value;
        rec.size=sizeof(value);
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c1));
        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                        ham_cursor_move(c1, &key, &rec, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(2, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                        ham_cursor_move(c2, &key, &rec, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(2, *(int *)rec.data);

        BFC_ASSERT_EQUAL(0, 
                btree_cursor_uncouple(((Cursor *)c1)->get_btree_cursor(), 0));
        BFC_ASSERT_EQUAL(0, 
                btree_cursor_uncouple(((Cursor *)c2)->get_btree_cursor(), 0));
        BFC_ASSERT_EQUAL(0, ham_cursor_erase(c1, 0));
        BFC_ASSERT(((Cursor *)c1)->is_nil(Cursor::CURSOR_BTREE));
        BFC_ASSERT(((Cursor *)c2)->is_nil(Cursor::CURSOR_BTREE));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(1, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(1, *(int *)rec.data);

        BFC_ASSERT_EQUAL(0, ham_cursor_close(c1));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(c2));
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
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

        ::memset(&rec, 0, sizeof(rec));
        value=2;
        rec.data=&value;
        rec.size=sizeof(value);
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c1));
        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                        ham_cursor_move(c1, &key, &rec, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(1, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                        ham_cursor_move(c2, &key, &rec, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(2, *(int *)rec.data);

        BFC_ASSERT_EQUAL(0, ham_cursor_erase(c1, 0));
        BFC_ASSERT(((Cursor *)c1)->is_nil(Cursor::CURSOR_BTREE));
        BFC_ASSERT(!((Cursor *)c2)->is_nil(Cursor::CURSOR_BTREE));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_move(c1, &key, &rec, HAM_CURSOR_LAST));
        BFC_ASSERT_EQUAL(2, *(int *)rec.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, 0));
        BFC_ASSERT_EQUAL(2, *(int *)rec.data);

        BFC_ASSERT_EQUAL(0, ham_cursor_close(c1));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(c2));
    }

    void moveToPreviousDuplicateTest(void)
    {
        ham_cursor_t *c;

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
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

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
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
        BFC_ASSERT_EQUAL(0, ham_cursor_overwrite(c, &rec, 0));
        checkData(c, HAM_CURSOR_FIRST,    0, "1111111111");
        checkData(c, HAM_CURSOR_NEXT,     0, "2222222222");
        checkData(c, HAM_CURSOR_NEXT,     0, 
                "3333333333333333333333333333333333333333333333333333");
        checkData(c, HAM_CURSOR_NEXT,     0, "4444444444");

        ::memset(&rec, 0, sizeof(rec));
        rec.data=(void *)"44";
        rec.size=(ham_size_t)strlen((char *)rec.data)+1;
        BFC_ASSERT_EQUAL(0, ham_cursor_overwrite(c, &rec, 0));

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

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c1));
        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c2));
        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c3));
        
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
        BFC_ASSERT_EQUAL(0, ham_cursor_overwrite(c1, &rec, 0));
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
        BFC_ASSERT_EQUAL(0, ham_cursor_overwrite(c1, &rec, 0));
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

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
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

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c1));
        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c2));

        checkData(c1, HAM_CURSOR_FIRST,     0, "111");
        checkData(c1, HAM_CURSOR_NEXT,      0, "2222222222");
        checkData(c2, HAM_CURSOR_FIRST,     0, "111");

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        rec.data=(void *)"1111111111111111111111111111111111111111";
        rec.size=(ham_size_t)strlen((char *)rec.data)+1;
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
        checkData(c2, 0,                    0, 
                "1111111111111111111111111111111111111111");

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        rec.data=(void *)"00";
        rec.size=(ham_size_t)strlen((char *)rec.data)+1;
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
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
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        (void)os::unlink(BFC_OPATH(".test"));

        /* generated with `cat ../COPYING.GPL2 | ./db5` */
#if defined(HAM_LITTLE_ENDIAN)
        BFC_ASSERT_EQUAL(true, 
            os::copy(BFC_IPATH("data/dupe-endian-test-open-database-be.hdb"), BFC_OPATH(".test")));
#else
        BFC_ASSERT_EQUAL(true, 
            os::copy(BFC_IPATH("data/dupe-endian-test-open-database-le.hdb"), BFC_OPATH(".test")));
#endif
        BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), 0));

        memset(&key, 0, sizeof(key));
        key.data=(void *)"written";
        key.size=8;

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));

        BFC_ASSERT_EQUAL(0, ham_cursor_find(c, &key, 0));
        memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, ham_cursor_move(c, 0, &rec, 0));
        BFC_ASSERT_EQUAL(_ham_byteswap32(125), *(unsigned int *)rec.data);

        memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, ham_cursor_move(c, 0, &rec, 
                                HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES));
        BFC_ASSERT_EQUAL(_ham_byteswap32(142), *(unsigned int *)rec.data);
        
        memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, ham_cursor_move(c, 0, &rec, 
                                HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES));
        BFC_ASSERT_EQUAL(_ham_byteswap32(235), *(unsigned int *)rec.data);

        memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, ham_cursor_move(c, 0, &rec, 
                                HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES));
        BFC_ASSERT_EQUAL(_ham_byteswap32(331), *(unsigned int *)rec.data);

        memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, ham_cursor_move(c, 0, &rec, 
                                HAM_CURSOR_NEXT|HAM_ONLY_DUPLICATES));

        BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
    }

    void eraseCursorTest(void)
    {
        ham_key_t key;
        ham_cursor_t *c;

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
        insertData(0, "1111111111");
        insertData(0, "2222222222");
        insertData(0, "3333333333");
        insertData(0, "4444444444");
        insertData(0, "5555555555");

        memset(&key, 0, sizeof(key));
        BFC_ASSERT_EQUAL(0, ham_cursor_find(c, &key, 0));
        BFC_ASSERT_EQUAL(0, ham_cursor_erase(c, 0));

        memset(&key, 0, sizeof(key));
        BFC_ASSERT_EQUAL(0, ham_cursor_find(c, &key, 0));
        BFC_ASSERT_EQUAL(0, ham_cursor_erase(c, 0));

        memset(&key, 0, sizeof(key));
        BFC_ASSERT_EQUAL(0, ham_cursor_find(c, &key, 0));
        BFC_ASSERT_EQUAL(0, ham_cursor_erase(c, 0));

        memset(&key, 0, sizeof(key));
        BFC_ASSERT_EQUAL(0, ham_cursor_find(c, &key, 0));
        BFC_ASSERT_EQUAL(0, ham_cursor_erase(c, 0));

        memset(&key, 0, sizeof(key));
        BFC_ASSERT_EQUAL(0, ham_cursor_find(c, &key, 0));
        BFC_ASSERT_EQUAL(0, ham_cursor_erase(c, 0));

        memset(&key, 0, sizeof(key));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                        ham_cursor_find(c, &key, 0));

        BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
    }

    void insertLastTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_cursor_t *c;
        const char *values[]={"11111", "222222", "3333333", "44444444"};

        memset(&key, 0, sizeof(key));

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
        for (int i=0; i<4; i++) {
            memset(&rec, 0, sizeof(rec));
            rec.data=(void *)values[i];
            rec.size=(ham_size_t)strlen((char *)rec.data)+1;
            BFC_ASSERT_EQUAL(0, 
                        ham_cursor_insert(c, &key, &rec, 
                                HAM_DUPLICATE_INSERT_LAST));
            memset(&rec, 0, sizeof(rec));
            BFC_ASSERT_EQUAL(0, 
                        ham_cursor_move(c, 0, &rec, 0));
            BFC_ASSERT_EQUAL(strlen((char *)rec.data)+1,
                            strlen(values[i])+1);
            BFC_ASSERT_EQUAL(0, strcmp(values[i], (char *)rec.data));
            BFC_ASSERT_EQUAL((ham_size_t)i, 
                            btree_cursor_get_dupe_id(
                                ((Cursor *)c)->get_btree_cursor()));
        }

        checkData(c, HAM_CURSOR_FIRST,    0, values[0]);
        checkData(c, HAM_CURSOR_NEXT,     0, values[1]);
        checkData(c, HAM_CURSOR_NEXT,     0, values[2]);
        checkData(c, HAM_CURSOR_NEXT,     0, values[3]);
        checkData(c, HAM_CURSOR_NEXT,     HAM_KEY_NOT_FOUND, values[3]);

        BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
    }

    void insertFirstTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_cursor_t *c;
        const char *values[]={"11111", "222222", "3333333", "44444444"};

        memset(&key, 0, sizeof(key));

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
        for (int i=0; i<4; i++) {
            memset(&rec, 0, sizeof(rec));
            rec.data=(void *)values[i];
            rec.size=(ham_size_t)strlen((char *)rec.data)+1;
            BFC_ASSERT_EQUAL(0, 
                        ham_cursor_insert(c, &key, &rec, 
                                HAM_DUPLICATE_INSERT_FIRST));
            memset(&rec, 0, sizeof(rec));
            BFC_ASSERT_EQUAL(0, 
                        ham_cursor_move(c, 0, &rec, 0));
            BFC_ASSERT_EQUAL(strlen((char *)rec.data)+1,
                            strlen(values[i])+1);
            BFC_ASSERT_EQUAL(0, strcmp(values[i], (char *)rec.data));
            BFC_ASSERT_EQUAL((ham_size_t)0, 
                            btree_cursor_get_dupe_id(
                                ((Cursor *)c)->get_btree_cursor()));
        }

        checkData(c, HAM_CURSOR_FIRST,    0, values[3]);
        checkData(c, HAM_CURSOR_NEXT,     0, values[2]);
        checkData(c, HAM_CURSOR_NEXT,     0, values[1]);
        checkData(c, HAM_CURSOR_NEXT,     0, values[0]);
        checkData(c, HAM_CURSOR_NEXT,     HAM_KEY_NOT_FOUND, values[0]);

        BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
    }

    void insertAfterTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_cursor_t *c;
        const char *values[]={"11111", "222222", "3333333", "44444444"};

        memset(&key, 0, sizeof(key));

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
        for (int i=0; i<4; i++) {
            memset(&rec, 0, sizeof(rec));
            rec.data=(void *)values[i];
            rec.size=(ham_size_t)strlen((char *)rec.data)+1;
            BFC_ASSERT_EQUAL(0, 
                        ham_cursor_insert(c, &key, &rec, 
                                HAM_DUPLICATE_INSERT_AFTER));
            memset(&rec, 0, sizeof(rec));
            BFC_ASSERT_EQUAL(0, 
                        ham_cursor_move(c, 0, &rec, 0));
            BFC_ASSERT_EQUAL(strlen((char *)rec.data)+1,
                            strlen(values[i])+1);
            BFC_ASSERT_EQUAL(0, strcmp(values[i], (char *)rec.data));
            BFC_ASSERT_EQUAL((ham_size_t)(i>=1 ? 1 : 0), 
                            btree_cursor_get_dupe_id(
                                ((Cursor *)c)->get_btree_cursor()));
            BFC_ASSERT_EQUAL(0, 
                        ham_cursor_move(c, 0, 0, HAM_CURSOR_FIRST));
        }

        checkData(c, HAM_CURSOR_FIRST,    0, values[0]);
        checkData(c, HAM_CURSOR_NEXT,     0, values[3]);
        checkData(c, HAM_CURSOR_NEXT,     0, values[2]);
        checkData(c, HAM_CURSOR_NEXT,     0, values[1]);
        checkData(c, HAM_CURSOR_NEXT,     HAM_KEY_NOT_FOUND, values[0]);

        BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
    }

    void insertBeforeTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_cursor_t *c;
        const char *values[]={"11111", "222222", "3333333", "44444444"};

        memset(&key, 0, sizeof(key));

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
        for (int i=0; i<4; i++) {
            memset(&rec, 0, sizeof(rec));
            rec.data=(void *)values[i];
            rec.size=(ham_size_t)strlen((char *)rec.data)+1;
            BFC_ASSERT_EQUAL(0, 
                        ham_cursor_insert(c, &key, &rec, 
                                HAM_DUPLICATE_INSERT_BEFORE));
            memset(&rec, 0, sizeof(rec));
            BFC_ASSERT_EQUAL(0, 
                        ham_cursor_move(c, 0, &rec, 0));
            BFC_ASSERT_EQUAL(strlen((char *)rec.data)+1,
                            strlen(values[i])+1);
            BFC_ASSERT_EQUAL(0, strcmp(values[i], (char *)rec.data));
            BFC_ASSERT_EQUAL((ham_size_t)(i<=1 ? 0 : i-1),
                            btree_cursor_get_dupe_id(
                                ((Cursor *)c)->get_btree_cursor()));
            BFC_ASSERT_EQUAL(0, 
                        ham_cursor_move(c, 0, 0, HAM_CURSOR_LAST));
        }

        checkData(c, HAM_CURSOR_FIRST,    0, values[1]);
        checkData(c, HAM_CURSOR_NEXT,     0, values[2]);
        checkData(c, HAM_CURSOR_NEXT,     0, values[3]);
        checkData(c, HAM_CURSOR_NEXT,     0, values[0]);
        checkData(c, HAM_CURSOR_NEXT,     HAM_KEY_NOT_FOUND, values[0]);

        BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
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

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
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
                    BFC_ASSERT_EQUAL(0, 
                            ham_cursor_insert(c, &key, &rec, 
                                    HAM_DUPLICATE_INSERT_LAST));
                }
                else {
                    /* other rounds: just overwrite them */
                    BFC_ASSERT_EQUAL(0, 
                            ham_cursor_overwrite(c, &rec, 0));
                    if (i!=(s+4)-1)
                        BFC_ASSERT_EQUAL(0, 
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

        BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
    }

    void getDuplicateCountTest(void)
    {
        ham_size_t count;
        ham_cursor_t *c;

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));

        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_get_duplicate_count(0, &count, 0));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_get_duplicate_count(c, 0, 0));
        BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, 
                ham_cursor_get_duplicate_count(c, &count, 0));
        BFC_ASSERT_EQUAL((ham_size_t)0, count);

        insertData(0, "1111111111");
        checkData(c, HAM_CURSOR_NEXT,     0, "1111111111");
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_get_duplicate_count(c, &count, 0));
        BFC_ASSERT_EQUAL((ham_size_t)1, count);

        insertData(0, "2222222222");
        checkData(c, HAM_CURSOR_NEXT,     0, "2222222222");
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_get_duplicate_count(c, &count, 0));
        BFC_ASSERT_EQUAL((ham_size_t)2, count);

        insertData(0, "3333333333");
        checkData(c, HAM_CURSOR_NEXT,     0, "3333333333");
        BFC_ASSERT_EQUAL(0, 
                btree_cursor_uncouple(((Cursor *)c)->get_btree_cursor(), 0));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_get_duplicate_count(c, &count, 0));
        BFC_ASSERT_EQUAL((ham_size_t)3, count);

        BFC_ASSERT_EQUAL(0, ham_cursor_erase(c, 0));
        BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, 
                ham_cursor_get_duplicate_count(c, &count, 0));
        checkData(c, HAM_CURSOR_FIRST,    0, "1111111111");
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_get_duplicate_count(c, &count, 0));
        BFC_ASSERT_EQUAL((ham_size_t)2, count);

        BFC_ASSERT_EQUAL(0, ham_cursor_close(c));

        if (!(m_flags&HAM_IN_MEMORY_DB)) {
            /* reopen the database */
            BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
            BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), m_flags));
            BFC_ASSERT(((Database *)m_db)->get_rt_flags()&HAM_ENABLE_DUPLICATES);

            BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));

            checkData(c, HAM_CURSOR_NEXT,     0, "1111111111");
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_get_duplicate_count(c, &count, 0));
            BFC_ASSERT_EQUAL((ham_size_t)2, count);

            BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
        }
    }

    void insertManyManyTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_cursor_t *c;
        ham_parameter_t params[2]=
        {
            { HAM_PARAM_PAGESIZE, 1024 },
            { 0, 0 }
        };

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_create_ex(m_db, BFC_OPATH(".test"), 
                    m_flags|HAM_ENABLE_DUPLICATES, 0664, &params[0]));

        memset(&key, 0, sizeof(key));
        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
        for (int i=0; i<256; i++) {
            memset(&rec, 0, sizeof(rec));
            rec.size=sizeof(i);
            rec.data=&i;

            BFC_ASSERT_EQUAL(0, 
                    ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        }

        for (int i=0; i<256; i++) {
            memset(&rec, 0, sizeof(rec));

            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c, &key, &rec, HAM_CURSOR_NEXT));
            BFC_ASSERT_EQUAL((ham_size_t)4, rec.size);
            BFC_ASSERT_EQUAL(i, *(int *)rec.data);
        }

        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_cursor_move(c, 0, 0, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
    }

    void cloneTest(void)
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
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

        ::memset(&rec, 0, sizeof(rec));
        value=2;
        rec.data=&value;
        rec.size=sizeof(value);
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        ::memset(&rec, 0, sizeof(rec));
        value=3;
        rec.data=&value;
        rec.size=sizeof(value);
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c1));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                        ham_cursor_move(c1, &key, &rec, HAM_CURSOR_FIRST));
        BFC_ASSERT_EQUAL(0, 
                        ham_cursor_move(c1, &key, &rec, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(2, *(int *)rec.data);

        BFC_ASSERT_EQUAL(0, ham_cursor_clone(c1, &c2));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                        ham_cursor_move(c2, &key, &rec, HAM_CURSOR_NEXT));
        BFC_ASSERT_EQUAL(3, *(int *)rec.data);

        BFC_ASSERT_EQUAL(0, ham_cursor_erase(c1, 0));
        BFC_ASSERT(((Cursor *)c1)->is_nil(Cursor::CURSOR_BTREE));
        BFC_ASSERT(!((Cursor *)c2)->is_nil(Cursor::CURSOR_BTREE));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_move(c2, &key, &rec, 0));
        BFC_ASSERT_EQUAL(3, *(int *)rec.data);

        BFC_ASSERT_EQUAL(0, ham_cursor_close(c1));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(c2));
    }
};

class InMemoryDupeTest : public DupeTest
{
public:
    InMemoryDupeTest()
        : DupeTest(HAM_IN_MEMORY_DB, "InMemoryDupeTest")
    {
    }
};

class SortedDupeTest : public hamsterDB_fixture
{
	define_super(hamsterDB_fixture);

public:
    SortedDupeTest(ham_u32_t flags=0, const char *name="SortedDupeTest")
    :   hamsterDB_fixture(name), m_flags(flags)
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(SortedDupeTest, invalidSortFlagsTest);
        BFC_REGISTER_TEST(SortedDupeTest, simpleInsertTest);
        BFC_REGISTER_TEST(SortedDupeTest, anotherSimpleInsertTest);
        BFC_REGISTER_TEST(SortedDupeTest, andAnotherSimpleInsertTest);
        BFC_REGISTER_TEST(SortedDupeTest, identicalRecordsTest);
    }

protected:
    ham_u32_t m_flags;
    ham_db_t *m_db;
    std::vector<ham_u32_t> m_data;

public:
    virtual void setup() 
	{ 
		__super::setup();

        (void)os::unlink(BFC_OPATH(".test"));

        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        BFC_ASSERT_EQUAL(0, ham_create(m_db, BFC_OPATH(".test"), 
                    m_flags|HAM_ENABLE_DUPLICATES|HAM_SORT_DUPLICATES, 0664));

        m_data.resize(0);
    }

    virtual void teardown() 
	{ 
		__super::teardown();

        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_AUTO_CLEANUP));
        BFC_ASSERT_EQUAL(0, ham_delete(m_db));
    }
    
    void insertDuplicate(ham_u32_t value)
    {
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        rec.data=&value;
        rec.size=sizeof(value);

        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));

        m_data.push_back(value);
        std::sort(m_data.begin(), m_data.end());
    }

    void checkDuplicates(void)
    {
        ham_cursor_t *c;
        ham_key_t key;
        ham_record_t rec;

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));

        for (std::vector<ham_u32_t>::iterator it=m_data.begin();
                it!=m_data.end(); it++) {
            ::memset(&key, 0, sizeof(key));
            ::memset(&rec, 0, sizeof(rec));
            BFC_ASSERT_EQUAL(0, 
                        ham_cursor_move(c, &key, &rec, HAM_CURSOR_NEXT));
            BFC_ASSERT_EQUAL(*it, *(ham_u32_t *)rec.data);
        }

        BFC_ASSERT_EQUAL(0, ham_cursor_close(c));
    }

    void invalidSortFlagsTest(void)
    {
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        /* create w/ transactions and sorting -> fail */
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
            ham_create(m_db, BFC_OPATH(".test"), 
                    HAM_ENABLE_TRANSACTIONS|HAM_SORT_DUPLICATES, 0664));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
            ham_open(m_db, BFC_OPATH(".test"), 
                    HAM_ENABLE_TRANSACTIONS|HAM_SORT_DUPLICATES));

        /* create w/o dupes, open with sorting -> fail */
        BFC_ASSERT_EQUAL(0, 
            ham_create(m_db, BFC_OPATH(".test"), 
                    m_flags&~HAM_ENABLE_DUPLICATES, 0664));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
            ham_open(m_db, BFC_OPATH(".test"), 
                    m_flags|HAM_SORT_DUPLICATES));

        /* sort without enable_dupes -> fail */
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
            ham_create(m_db, BFC_OPATH(".test"), 
                    m_flags|HAM_SORT_DUPLICATES, 0664));
        BFC_ASSERT_EQUAL(0, 
            ham_create(m_db, BFC_OPATH(".test"), 
                    m_flags|HAM_SORT_DUPLICATES|HAM_ENABLE_DUPLICATES, 0664));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        /* open w/ sorting -> ok */
        BFC_ASSERT_EQUAL(0, 
            ham_open(m_db, BFC_OPATH(".test"), 
                    m_flags|HAM_SORT_DUPLICATES));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        ham_env_t *env;
        ham_db_t *db;

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_env_create(env, BFC_OPATH(".test"), 
                    m_flags|HAM_SORT_DUPLICATES, 0664));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create(env, BFC_OPATH(".test"), 
                    m_flags, 0664));

        BFC_ASSERT_EQUAL(0, ham_new(&db));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_env_create_db(env, db, 13, HAM_SORT_DUPLICATES, 0));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create_db(env, db, 13, 
                        HAM_ENABLE_DUPLICATES|HAM_SORT_DUPLICATES, 0));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        BFC_ASSERT_EQUAL(0, 
                ham_env_open_db(env, db, 13, 
                        HAM_SORT_DUPLICATES, 0));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));

        BFC_ASSERT_EQUAL(0, 
                ham_env_create_db(env, db, 14, 0, 0));
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_env_open_db(env, db, 14, 
                        HAM_SORT_DUPLICATES, 0));

        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_env_open(env, BFC_OPATH(".test"), 
                    m_flags|HAM_SORT_DUPLICATES));

        BFC_ASSERT_EQUAL(0, 
                ham_env_create(env, BFC_OPATH(".test"), 
                    m_flags, 0664));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create_db(env, db, 99, 
                        HAM_ENABLE_DUPLICATES|HAM_SORT_DUPLICATES, 0));

        /* make sure that HAM_DUPLICATE_INSERT_* is not allowed if
         * sorting is enabled */
        ham_cursor_t *c;
        ham_key_t key;
        ham_record_t rec;
        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        BFC_ASSERT_EQUAL(0, ham_cursor_create(db, 0, 0, &c));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_insert(c, &key, &rec, 
                        HAM_DUPLICATE_INSERT_FIRST));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_insert(c, &key, &rec, 
                        HAM_DUPLICATE_INSERT_LAST));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_insert(c, &key, &rec, 
                        HAM_DUPLICATE_INSERT_BEFORE));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_insert(c, &key, &rec, 
                        HAM_DUPLICATE_INSERT_AFTER));
        BFC_ASSERT_EQUAL(0, 
                ham_cursor_insert(c, &key, &rec, 0));
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_overwrite(c, &rec, 0));
        
        ham_cursor_close(c);
        BFC_ASSERT_EQUAL(0, ham_close(db, 0));
        BFC_ASSERT_EQUAL(0, ham_delete(db));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void simpleInsertTest(void)
    {
        insertDuplicate(1);
        checkDuplicates();
        insertDuplicate(2);
        checkDuplicates();
        insertDuplicate(3);
        checkDuplicates();
    }

    void anotherSimpleInsertTest(void)
    {
        BFC_ASSERT_EQUAL(0, 
            ham_set_duplicate_compare_func(m_db, __compare_numbers));

        insertDuplicate(10);
        insertDuplicate(20);
        insertDuplicate(30);
        checkDuplicates();
        insertDuplicate(3);
        checkDuplicates();
        insertDuplicate(2);
        checkDuplicates();
        insertDuplicate(1);
        checkDuplicates();
        insertDuplicate(35);
        checkDuplicates();
        insertDuplicate(34);
        checkDuplicates();
        insertDuplicate(33);
        checkDuplicates();
        insertDuplicate(22);
        insertDuplicate(23);
        insertDuplicate(24);
        checkDuplicates();
    }

    void andAnotherSimpleInsertTest(void)
    {
        BFC_ASSERT_EQUAL(0, 
            ham_set_duplicate_compare_func(m_db, __compare_numbers));

        insertDuplicate(266);
        checkDuplicates();
        insertDuplicate(1875);
        checkDuplicates();
        insertDuplicate(216);
        checkDuplicates();
        insertDuplicate(1079);
        checkDuplicates();
        insertDuplicate(1439);
        checkDuplicates();
        insertDuplicate(939);
        checkDuplicates();
        insertDuplicate(1578);
        checkDuplicates();
        insertDuplicate(1022);
        checkDuplicates();
        insertDuplicate(274);
        checkDuplicates();
        insertDuplicate(357);
        checkDuplicates();
        insertDuplicate(1112);
        checkDuplicates();
        insertDuplicate(565);
        checkDuplicates();
        insertDuplicate(1841);
        checkDuplicates();
        insertDuplicate(651);
        checkDuplicates();
        insertDuplicate(1183);
        checkDuplicates();
        insertDuplicate(700);
        checkDuplicates();
    }

    /*
     * berkeleydb does not support duplicate keys with identical records,
     * therefore this item is not covered in the acceptance tests. 
     *
     * that's why it's tested here.
     */
    void identicalRecordsTest(void)
    {
        int i;
        for (i=0; i<10; i++) {
            insertDuplicate(1);
            checkDuplicates();
        }

        for (i=0; i<10; i++) {
            insertDuplicate(2);
            checkDuplicates();
        }
    }

};

class InMemorySortedDupeTest : public SortedDupeTest
{
public:
    InMemorySortedDupeTest()
        : SortedDupeTest(HAM_IN_MEMORY_DB, "InMemorySortedDupeTest")
    {
        clear_tests(); // don't inherit tests
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(InMemorySortedDupeTest, simpleInsertTest);
        BFC_REGISTER_TEST(InMemorySortedDupeTest, anotherSimpleInsertTest);
        BFC_REGISTER_TEST(InMemorySortedDupeTest, andAnotherSimpleInsertTest);
        BFC_REGISTER_TEST(InMemorySortedDupeTest, identicalRecordsTest);
    }
};

BFC_REGISTER_FIXTURE(DupeTest);
BFC_REGISTER_FIXTURE(InMemoryDupeTest);
BFC_REGISTER_FIXTURE(SortedDupeTest);
BFC_REGISTER_FIXTURE(InMemorySortedDupeTest);
