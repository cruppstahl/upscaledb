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
#include "memtracker.h"

class RecNoTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(RecNoTest);
    CPPUNIT_TEST      (createCloseTest);
    CPPUNIT_TEST      (createCloseOpenCloseTest);
    CPPUNIT_TEST      (createInsertCloseTest);
    CPPUNIT_TEST      (createInsertManyCloseTest);
    CPPUNIT_TEST      (createInsertCloseCursorTest);
    CPPUNIT_TEST      (createInsertCloseReopenTest);
    CPPUNIT_TEST      (createInsertCloseReopenCursorTest);
    CPPUNIT_TEST      (createInsertCloseReopenTwiceTest);
    CPPUNIT_TEST      (createInsertCloseReopenTwiceCursorTest);
    CPPUNIT_TEST      (insertBadKeyTest);
    CPPUNIT_TEST      (insertBadKeyCursorTest);
    CPPUNIT_TEST      (createBadKeysizeTest);
    CPPUNIT_TEST      (envTest);
    CPPUNIT_TEST      (endianTestOpenDatabase);
    CPPUNIT_TEST      (overwriteTest);
    CPPUNIT_TEST      (overwriteCursorTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_u32_t m_flags;
    ham_db_t *m_db;

public:
    RecNoTest(ham_u32_t flags=0)
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
    }

    void tearDown()
    {
        CPPUNIT_ASSERT_EQUAL(0, ham_delete(m_db));
    }
    
    void createCloseTest(void)
    {
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create(m_db, ".test", m_flags|HAM_RECORD_NUMBER, 0664));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
    }

    void createCloseOpenCloseTest(void)
    {
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create(m_db, ".test", m_flags|HAM_RECORD_NUMBER, 0664));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_open(m_db, ".test", m_flags));
        CPPUNIT_ASSERT(db_get_rt_flags(m_db)&HAM_RECORD_NUMBER);
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
    }

    void createInsertCloseReopenTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_u64_t recno, value=1;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        key.flags|=HAM_KEY_USER_ALLOC;
        key.data=&recno;
        key.size=sizeof(recno);

        rec.data=&value;
        rec.size=sizeof(value);

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create(m_db, ".test", m_flags|HAM_RECORD_NUMBER, 0664));

        for (int i=0; i<5; i++) {
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_insert(m_db, 0, &key, &rec, 0));
            CPPUNIT_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_open(m_db, ".test", m_flags));

        for (int i=5; i<10; i++) {
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_insert(m_db, 0, &key, &rec, 0));
            CPPUNIT_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
    }

    void createInsertCloseReopenCursorTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_cursor_t *cursor;
        ham_u64_t recno, value=1;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        key.flags|=HAM_KEY_USER_ALLOC;
        key.data=&recno;
        key.size=sizeof(recno);

        rec.data=&value;
        rec.size=sizeof(value);

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create(m_db, ".test", m_flags|HAM_RECORD_NUMBER, 0664));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_create(m_db, 0, 0, &cursor));

        for (int i=0; i<5; i++) {
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_cursor_insert(cursor, &key, &rec, 0));
            CPPUNIT_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_open(m_db, ".test", m_flags));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_create(m_db, 0, 0, &cursor));

        for (int i=5; i<10; i++) {
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_cursor_insert(cursor, &key, &rec, 0));
            CPPUNIT_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
    }

    void createInsertCloseTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_u64_t recno, value=1;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        key.flags|=HAM_KEY_USER_ALLOC;
        key.data=&recno;
        key.size=sizeof(recno);

        rec.data=&value;
        rec.size=sizeof(value);

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create(m_db, ".test", m_flags|HAM_RECORD_NUMBER, 0664));

        for (int i=0; i<5; i++) {
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_insert(m_db, 0, &key, &rec, 0));
            CPPUNIT_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
    }

    void createInsertManyCloseTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_u64_t recno, value=1;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        key.flags|=HAM_KEY_USER_ALLOC;
        key.data=&recno;
        key.size=sizeof(recno);

        rec.data=&value;
        rec.size=sizeof(value);

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create(m_db, ".test", m_flags|HAM_RECORD_NUMBER, 0664));

        for (int i=0; i<500; i++) {
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_insert(m_db, 0, &key, &rec, 0));
            CPPUNIT_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        for (int i=0; i<500; i++) {
            recno=i+1;
            memset(&key, 0, sizeof(key));
            memset(&rec, 0, sizeof(rec));
            key.data=&recno;
            key.size=sizeof(recno);
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_find(m_db, 0, &key, &rec, 0));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
    }

    void createInsertCloseCursorTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_cursor_t *cursor;
        ham_u64_t recno, value=1;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        key.flags|=HAM_KEY_USER_ALLOC;
        key.data=&recno;
        key.size=sizeof(recno);

        rec.data=&value;
        rec.size=sizeof(value);

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create(m_db, ".test", m_flags|HAM_RECORD_NUMBER, 0664));

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_create(m_db, 0, 0, &cursor));

        for (int i=0; i<5; i++) {
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_cursor_insert(cursor, &key, &rec, 0));
            CPPUNIT_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
    }

    void createInsertCloseReopenTwiceTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_u64_t recno, value=1;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        key.flags|=HAM_KEY_USER_ALLOC;
        key.data=&recno;
        key.size=sizeof(recno);

        rec.data=&value;
        rec.size=sizeof(value);

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create(m_db, ".test", m_flags|HAM_RECORD_NUMBER, 0664));

        for (int i=0; i<5; i++) {
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_insert(m_db, 0, &key, &rec, 0));
            CPPUNIT_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_open(m_db, ".test", m_flags));

        for (int i=5; i<10; i++) {
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_insert(m_db, 0, &key, &rec, 0));
            CPPUNIT_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_open(m_db, ".test", m_flags));

        for (int i=10; i<15; i++) {
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_insert(m_db, 0, &key, &rec, 0));
            CPPUNIT_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
    }

    void createInsertCloseReopenTwiceCursorTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_u64_t recno, value=1;
        ham_cursor_t *cursor;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        key.flags|=HAM_KEY_USER_ALLOC;
        key.data=&recno;
        key.size=sizeof(recno);

        rec.data=&value;
        rec.size=sizeof(value);

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create(m_db, ".test", m_flags|HAM_RECORD_NUMBER, 0664));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_create(m_db, 0, 0, &cursor));

        for (int i=0; i<5; i++) {
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_cursor_insert(cursor, &key, &rec, 0));
            CPPUNIT_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_open(m_db, ".test", m_flags));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_create(m_db, 0, 0, &cursor));

        for (int i=5; i<10; i++) {
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_cursor_insert(cursor, &key, &rec, 0));
            CPPUNIT_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_open(m_db, ".test", m_flags));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_create(m_db, 0, 0, &cursor));

        for (int i=10; i<15; i++) {
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_cursor_insert(cursor, &key, &rec, 0));
            CPPUNIT_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
    }

    void insertBadKeyTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_u64_t recno;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create(m_db, ".test", m_flags|HAM_RECORD_NUMBER, 0664));

        key.flags=0;
        key.data=&recno;
        key.size=sizeof(recno);
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, &key, &rec, 0));

        key.data=0;
        key.size=8;
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, &key, &rec, 0));

        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_insert(m_db, 0, 0, &rec, 0));

        key.data=0;
        key.size=0;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_insert(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)1ull, *(ham_u64_t *)key.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
    }

    void insertBadKeyCursorTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_cursor_t *cursor;
        ham_u64_t recno;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create(m_db, ".test", m_flags|HAM_RECORD_NUMBER, 0664));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_create(m_db, 0, 0, &cursor));

        key.flags=0;
        key.data=&recno;
        key.size=sizeof(recno);
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_insert(cursor, &key, &rec, 0));

        key.data=0;
        key.size=8;
        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_insert(cursor, &key, &rec, 0));

        CPPUNIT_ASSERT_EQUAL(HAM_INV_PARAMETER, 
                ham_cursor_insert(cursor, 0, &rec, 0));

        key.data=0;
        key.size=0;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_insert(cursor, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)1ull, *(ham_u64_t *)key.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
    }

    void createBadKeysizeTest(void)
    {
        ham_parameter_t p[]={
            {HAM_PARAM_KEYSIZE,   7}, 
            {0, 0}
        };

        CPPUNIT_ASSERT_EQUAL(HAM_INV_KEYSIZE, 
                ham_create_ex(m_db, ".test", m_flags|HAM_RECORD_NUMBER, 
                    0664, &p[0]));

        p[0].value=9;
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create_ex(m_db, ".test", m_flags|HAM_RECORD_NUMBER, 
                    0664, &p[0]));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
    }

    void envTest(void)
    {
        ham_env_t *env;
        ham_key_t key;
        ham_record_t rec;
        ham_u64_t recno;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));
        key.data=&recno;
        key.size=sizeof(recno);
        key.flags|=HAM_KEY_USER_ALLOC;

        CPPUNIT_ASSERT_EQUAL(0, ham_env_new(&env));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_create(env, ".test", m_flags, 0664));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_create_db(env, m_db, 333, HAM_RECORD_NUMBER, 0));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_insert(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)1ull, *(ham_u64_t *)key.data);
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env));

        if (!(m_flags&HAM_IN_MEMORY_DB)) {
            CPPUNIT_ASSERT_EQUAL(0, ham_env_open(env, ".test", 0));
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_env_open_db(env, m_db, 333, HAM_RECORD_NUMBER, 0));
            CPPUNIT_ASSERT_EQUAL(0, 
                    ham_insert(m_db, 0, &key, &rec, 0));
            CPPUNIT_ASSERT_EQUAL((ham_u64_t)2ull, *(ham_u64_t *)key.data);
            CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
            CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env));
        }

        CPPUNIT_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void endianTestOpenDatabase(void)
    {
        ham_key_t key;
        ham_record_t rec;
#if HAM_LITTLE_ENDIAN
        CPPUNIT_ASSERT_EQUAL(0, ham_open(m_db, 
                    "data/recno-endian-test-open-database-be.hdb", 0));
#else
        CPPUNIT_ASSERT_EQUAL(0, ham_open(m_db, 
                    "data/recno-endian-test-open-database-le.hdb", 0));
#endif
        /* generated with `cat ../COPYING | ./db4`; has 2973 entries */

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_insert(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)2974ull, *(ham_u64_t *)key.data);

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_erase(m_db, 0, &key, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
    }

    void overwriteTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_u64_t recno;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create(m_db, ".test", m_flags|HAM_RECORD_NUMBER, 0664));

        key.data=&recno;
        key.flags=HAM_KEY_USER_ALLOC;
        key.size=sizeof(recno);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

        recno=0x13ull;
        memset(&rec, 0, sizeof(rec));
        rec.data=&recno;
        rec.size=sizeof(recno);
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));

        memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));

        CPPUNIT_ASSERT_EQUAL((ham_u64_t)0x13ull, *(ham_u64_t *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
    }

    void overwriteCursorTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_u64_t recno;
        ham_cursor_t *cursor;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create(m_db, ".test", m_flags|HAM_RECORD_NUMBER, 0664));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_create(m_db, 0, 0, &cursor));

        key.data=&recno;
        key.flags=HAM_KEY_USER_ALLOC;
        key.size=sizeof(recno);
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_insert(cursor, &key, &rec, 0));

        recno=0x13ull;
        memset(&rec, 0, sizeof(rec));
        rec.data=&recno;
        rec.size=sizeof(recno);
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_cursor_insert(cursor, &key, &rec, HAM_OVERWRITE));

        memset(&rec, 0, sizeof(rec));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));

        CPPUNIT_ASSERT_EQUAL((ham_u64_t)0x13ull, *(ham_u64_t *)rec.data);

        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db));
    }
};

class InMemoryRecNoTest : public RecNoTest
{
    CPPUNIT_TEST_SUITE(InMemoryRecNoTest);
    CPPUNIT_TEST      (createCloseTest);
    CPPUNIT_TEST      (createInsertCloseTest);
    CPPUNIT_TEST      (createInsertManyCloseTest);
    CPPUNIT_TEST      (createInsertCloseCursorTest);
    CPPUNIT_TEST      (insertBadKeyTest);
    CPPUNIT_TEST      (insertBadKeyCursorTest);
    CPPUNIT_TEST      (createBadKeysizeTest);
    CPPUNIT_TEST      (envTest);
    CPPUNIT_TEST      (overwriteTest);
    CPPUNIT_TEST      (overwriteCursorTest);
    CPPUNIT_TEST_SUITE_END();

public:
    InMemoryRecNoTest()
    :   RecNoTest(HAM_IN_MEMORY_DB)
    {
    }

};

CPPUNIT_TEST_SUITE_REGISTRATION(RecNoTest);
CPPUNIT_TEST_SUITE_REGISTRATION(InMemoryRecNoTest);
