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
    CPPUNIT_TEST      (insertDuplicatesTest);
    CPPUNIT_TEST      (insertEraseTest);
    CPPUNIT_TEST      (insertTest);
    CPPUNIT_TEST      (insertSkipDuplicatesTest);
    CPPUNIT_TEST      (insertOnlyDuplicatesTest);
    CPPUNIT_TEST      (coupleUncoupleTest);
    CPPUNIT_TEST      (reopenTest);
    CPPUNIT_TEST      (moveToLastDuplicateTest);
    CPPUNIT_TEST      (invalidFlagsTest);
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
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        ::memset(&data, 0x13, sizeof(data));
        rec.data=data;
        rec.size=sizeof(data);

        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));
        ::memset(&rec2, 0, sizeof(rec2));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec2, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec2.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec2.data, sizeof(data)));

        ::memset(&data, 0x14, sizeof(data));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        ::memset(&rec2, 0, sizeof(rec2));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec2, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec2.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec2.data, sizeof(data)));

        ::memset(&data, 0x15, sizeof(data));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        ::memset(&rec2, 0, sizeof(rec2));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec2, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec2.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec2.data, sizeof(data)));

        ::memset(&data, 0x16, sizeof(data));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        ::memset(&rec2, 0, sizeof(rec2));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec2, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec2.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec2.data, sizeof(data)));
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
        
        insertData(0, "0000000000");
        insertData(0, "9999999999");
        insertData(0, "8888888888");
        insertData(0, "7777777777");
        insertData(0, "6666666666");
        insertData(0, "5555555555");
        insertData(0, "4444444444");
        insertData(0, "3333333333");
        insertData(0, "2222222222");
        insertData(0, "1111111111");

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
        insertData("111", "0000000000");
        insertData("111", "9999999999");
        insertData("111", "8888888888");
        insertData("111", "7777777777");
        insertData("111", "6666666666");
        insertData("111", "5555555555");
        insertData("111", "4444444444");
        insertData("111", "3333333333");
        insertData("111", "2222222222");
        insertData("111", "1111111111");
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
        insertData("111", "0000000000");
        insertData("111", "9999999999");
        insertData("111", "8888888888");
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
        insertData("111", "0000000000");
        insertData("111", "9999999999");
        insertData("111", "8888888888");
        insertData("111", "7777777777");
        insertData("111", "6666666666");
        insertData("111", "5555555555");
        insertData("111", "4444444444");
        insertData("111", "3333333333");
        insertData("111", "2222222222");
        insertData("111", "1111111111");
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

    void reopenTest(void)
    {
        ham_key_t key;
        ham_record_t rec, rec2;
        char data[16];
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        ::memset(&data, 0x13, sizeof(data));
        rec.data=data;
        rec.size=sizeof(data);

        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));
        ::memset(&rec2, 0, sizeof(rec2));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec2, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec2.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec2.data, sizeof(data)));

        ::memset(&data, 0x14, sizeof(data));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        ::memset(&rec2, 0, sizeof(rec2));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec2, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec2.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec2.data, sizeof(data)));

        /* reopen the database */
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
        CPPUNIT_ASSERT_EQUAL(0, ham_open(m_db, ".test", m_flags));
        CPPUNIT_ASSERT(db_get_rt_flags(m_db)&HAM_ENABLE_DUPLICATES);

        ::memset(&data, 0x15, sizeof(data));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        ::memset(&rec2, 0, sizeof(rec2));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec2, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec2.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec2.data, sizeof(data)));

        ::memset(&data, 0x16, sizeof(data));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_DUPLICATE));
        ::memset(&rec2, 0, sizeof(rec2));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec2, 0));
        CPPUNIT_ASSERT_EQUAL((ham_size_t)sizeof(data), rec2.size);
        CPPUNIT_ASSERT_EQUAL(0, ::memcmp(data, rec2.data, sizeof(data)));
    }

    void moveToLastDuplicateTest(void)
    {
        ham_cursor_t *c;

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &c));
        
        insertData(0, "0000000000");
        insertData(0, "9999999999");
        insertData(0, "8888888888");
        insertData(0, "7777777777");
        insertData(0, "6666666666");
        insertData(0, "5555555555");
        insertData(0, "4444444444");
        insertData(0, "3333333333");
        insertData(0, "2222222222");
        insertData(0, "1111111111");

        checkData(c, HAM_CURSOR_LAST,     0, "0000000000");

        ham_cursor_close(c);
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

};

class InMemoryDupeTest : public DupeTest
{
    CPPUNIT_TEST_SUITE(InMemoryDupeTest);
    CPPUNIT_TEST      (insertDuplicatesTest);
    CPPUNIT_TEST      (insertEraseTest);
    CPPUNIT_TEST      (insertTest);
    CPPUNIT_TEST      (insertSkipDuplicatesTest);
    CPPUNIT_TEST      (insertOnlyDuplicatesTest);
    CPPUNIT_TEST      (coupleUncoupleTest);
    CPPUNIT_TEST      (moveToLastDuplicateTest);
    CPPUNIT_TEST      (invalidFlagsTest);
    CPPUNIT_TEST_SUITE_END();

public:
    InMemoryDupeTest()
    :   DupeTest(HAM_IN_MEMORY_DB)
    {
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(DupeTest);
CPPUNIT_TEST_SUITE_REGISTRATION(InMemoryDupeTest);
