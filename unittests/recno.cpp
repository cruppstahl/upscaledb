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

#include "../src/config.h"

#include <stdexcept>
#include <string.h>
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/env.h"
#include "../src/btree.h"
#include "os.hpp"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;
using namespace ham;

class RecNoTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    RecNoTest(ham_u32_t flags=0, const char *name="RecNoTest")
    :   hamsterDB_fixture(name), m_flags(flags)
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(RecNoTest, createCloseTest);
        BFC_REGISTER_TEST(RecNoTest, createCloseOpenCloseTest);
        BFC_REGISTER_TEST(RecNoTest, createInsertCloseTest);
        BFC_REGISTER_TEST(RecNoTest, createInsertManyCloseTest);
        BFC_REGISTER_TEST(RecNoTest, createInsertCloseCursorTest);
        BFC_REGISTER_TEST(RecNoTest, createInsertCloseReopenTest);
        BFC_REGISTER_TEST(RecNoTest, createInsertCloseReopenCursorTest);
        BFC_REGISTER_TEST(RecNoTest, createInsertCloseReopenTwiceTest);
        BFC_REGISTER_TEST(RecNoTest, createInsertCloseReopenTwiceCursorTest);
        BFC_REGISTER_TEST(RecNoTest, insertBadKeyTest);
        BFC_REGISTER_TEST(RecNoTest, insertBadKeyCursorTest);
        BFC_REGISTER_TEST(RecNoTest, createBadKeysizeTest);
        BFC_REGISTER_TEST(RecNoTest, envTest);
        BFC_REGISTER_TEST(RecNoTest, endianTestOpenDatabase);
        BFC_REGISTER_TEST(RecNoTest, overwriteTest);
        BFC_REGISTER_TEST(RecNoTest, overwriteCursorTest);
        BFC_REGISTER_TEST(RecNoTest, eraseLastReopenTest);
        BFC_REGISTER_TEST(RecNoTest, uncoupleTest);
    }

protected:
    ham_u32_t m_flags;
    ham_db_t *m_db;

public:
    virtual void setup()
    {
        __super::setup();

        (void)os::unlink(BFC_OPATH(".test"));
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
    }

    virtual void teardown()
    {
        __super::teardown();

        BFC_ASSERT_EQUAL(0, ham_delete(m_db));
    }

    void createCloseTest(void)
    {
        BFC_ASSERT_EQUAL(0,
                ham_create(m_db, BFC_OPATH(".test"), m_flags|HAM_RECORD_NUMBER, 0664));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void createCloseOpenCloseTest(void)
    {
        BFC_ASSERT_EQUAL(0,
                ham_create(m_db, BFC_OPATH(".test"),
                        m_flags|HAM_RECORD_NUMBER, 0664));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0,
                ham_open(m_db, BFC_OPATH(".test"), m_flags));
        BFC_ASSERT(((Database *)m_db)->get_rt_flags()&HAM_RECORD_NUMBER);
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void createInsertCloseReopenTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_u64_t recno, value=1;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        key.flags=HAM_KEY_USER_ALLOC;
        key.data=&recno;
        key.size=sizeof(recno);

        rec.data=&value;
        rec.size=sizeof(value);

        BFC_ASSERT_EQUAL(0,
                ham_create(m_db, BFC_OPATH(".test"), m_flags|HAM_RECORD_NUMBER, 0664));

        for (int i=0; i<5; i++) {
            BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, 0, &key, &rec, 0));
            BFC_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        key.flags=HAM_KEY_USER_ALLOC;
        key.data=0;
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_insert(m_db, 0, &key, &rec, 0));
        key.data=&recno;
        key.size=4;
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_insert(m_db, 0, &key, &rec, 0));
        key.size=sizeof(recno);

        key.flags=0;
        key.size=0;
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_insert(m_db, 0, &key, &rec, 0));
        key.size=8;
        key.data=0;
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_insert(m_db, 0, &key, &rec, 0));
        key.data=&recno;
        key.flags=HAM_KEY_USER_ALLOC;

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0,
                ham_open(m_db, BFC_OPATH(".test"), m_flags));

        for (int i=5; i<10; i++) {
            BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, 0, &key, &rec, 0));
            BFC_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void createInsertCloseReopenCursorTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_cursor_t *cursor;
        ham_u64_t recno, value=1;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        key.flags=HAM_KEY_USER_ALLOC;
        key.data=&recno;
        key.size=sizeof(recno);

        rec.data=&value;
        rec.size=sizeof(value);

        BFC_ASSERT_EQUAL(0,
                ham_create(m_db, BFC_OPATH(".test"), m_flags|HAM_RECORD_NUMBER, 0664));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_create(m_db, 0, 0, &cursor));

        for (int i=0; i<5; i++) {
            BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(cursor, &key, &rec, 0));
            BFC_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0,
                ham_open(m_db, BFC_OPATH(".test"), m_flags));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_create(m_db, 0, 0, &cursor));

        for (int i=5; i<10; i++) {
            BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(cursor, &key, &rec, 0));
            BFC_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void createInsertCloseTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_u64_t recno, value=1;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        key.flags=HAM_KEY_USER_ALLOC;
        key.data=&recno;
        key.size=sizeof(recno);

        rec.data=&value;
        rec.size=sizeof(value);

        BFC_ASSERT_EQUAL(0,
                ham_create(m_db, BFC_OPATH(".test"), m_flags|HAM_RECORD_NUMBER, 0664));

        for (int i=0; i<5; i++) {
            BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, 0, &key, &rec, 0));
            BFC_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void createInsertManyCloseTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_u64_t recno, value=1;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        key.flags=HAM_KEY_USER_ALLOC;
        key.data=&recno;
        key.size=sizeof(recno);

        rec.data=&value;
        rec.size=sizeof(value);

        BFC_ASSERT_EQUAL(0,
                ham_create(m_db, BFC_OPATH(".test"), m_flags|HAM_RECORD_NUMBER, 0664));

        for (int i=0; i<500; i++) {
            BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, 0, &key, &rec, 0));
            BFC_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        key.size=4;
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_find(m_db, 0, &key, &rec, 0));
        key.size=0;
        key.data=&key;
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_find(m_db, 0, &key, &rec, 0));

        for (int i=0; i<500; i++) {
            recno=i+1;
            memset(&key, 0, sizeof(key));
            memset(&rec, 0, sizeof(rec));
            key.data=&recno;
            key.size=sizeof(recno);
            BFC_ASSERT_EQUAL(0,
                    ham_find(m_db, 0, &key, &rec, 0));
        }

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void createInsertCloseCursorTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_cursor_t *cursor;
        ham_u64_t recno, value=1;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        key.flags=HAM_KEY_USER_ALLOC;
        key.data=&recno;
        key.size=sizeof(recno);

        rec.data=&value;
        rec.size=sizeof(value);

        BFC_ASSERT_EQUAL(0,
                ham_create(m_db, BFC_OPATH(".test"), m_flags|HAM_RECORD_NUMBER, 0664));

        BFC_ASSERT_EQUAL(0,
                ham_cursor_create(m_db, 0, 0, &cursor));

        for (int i=0; i<5; i++) {
            BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(cursor, &key, &rec, 0));
            BFC_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void createInsertCloseReopenTwiceTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_u64_t recno, value=1;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        key.flags=HAM_KEY_USER_ALLOC;
        key.data=&recno;
        key.size=sizeof(recno);

        rec.data=&value;
        rec.size=sizeof(value);

        BFC_ASSERT_EQUAL(0,
                ham_create(m_db, BFC_OPATH(".test"), m_flags|HAM_RECORD_NUMBER, 0664));

        for (int i=0; i<5; i++) {
            BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, 0, &key, &rec, 0));
            BFC_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0,
                ham_open(m_db, BFC_OPATH(".test"), m_flags));

        for (int i=5; i<10; i++) {
            BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, 0, &key, &rec, 0));
            BFC_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0,
                ham_open(m_db, BFC_OPATH(".test"), m_flags));

        for (int i=10; i<15; i++) {
            BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, 0, &key, &rec, 0));
            BFC_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void createInsertCloseReopenTwiceCursorTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_u64_t recno, value=1;
        ham_cursor_t *cursor;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        key.flags=HAM_KEY_USER_ALLOC;
        key.data=&recno;
        key.size=sizeof(recno);

        rec.data=&value;
        rec.size=sizeof(value);

        BFC_ASSERT_EQUAL(0,
                ham_create(m_db, BFC_OPATH(".test"), m_flags|HAM_RECORD_NUMBER, 0664));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_create(m_db, 0, 0, &cursor));

        for (int i=0; i<5; i++) {
            BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(cursor, &key, &rec, 0));
            BFC_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0,
                ham_open(m_db, BFC_OPATH(".test"), m_flags));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_create(m_db, 0, 0, &cursor));

        for (int i=5; i<10; i++) {
            BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(cursor, &key, &rec, 0));
            BFC_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0,
                ham_open(m_db, BFC_OPATH(".test"), m_flags));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_create(m_db, 0, 0, &cursor));

        for (int i=10; i<15; i++) {
            BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(cursor, &key, &rec, 0));
            BFC_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void insertBadKeyTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_u64_t recno;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        BFC_ASSERT_EQUAL(0,
                ham_create(m_db, BFC_OPATH(".test"), m_flags|HAM_RECORD_NUMBER, 0664));

        key.flags=0;
        key.data=&recno;
        key.size=sizeof(recno);
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_insert(m_db, 0, &key, &rec, 0));

        key.data=0;
        key.size=8;
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_insert(m_db, 0, &key, &rec, 0));

        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_insert(m_db, 0, 0, &rec, 0));

        key.data=0;
        key.size=0;
        BFC_ASSERT_EQUAL(0,
                ham_insert(m_db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL((ham_u64_t)1ull, *(ham_u64_t *)key.data);

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void insertBadKeyCursorTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_cursor_t *cursor;
        ham_u64_t recno;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        BFC_ASSERT_EQUAL(0,
                ham_create(m_db, BFC_OPATH(".test"), m_flags|HAM_RECORD_NUMBER, 0664));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_create(m_db, 0, 0, &cursor));

        key.flags=0;
        key.data=&recno;
        key.size=sizeof(recno);
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_cursor_insert(cursor, &key, &rec, 0));

        key.data=0;
        key.size=8;
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_cursor_insert(cursor, &key, &rec, 0));

        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_cursor_insert(cursor, 0, &rec, 0));

        key.data=0;
        key.size=0;
        BFC_ASSERT_EQUAL(0,
                ham_cursor_insert(cursor, &key, &rec, 0));
        BFC_ASSERT_EQUAL((ham_u64_t)1ull, *(ham_u64_t *)key.data);

        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void createBadKeysizeTest(void)
    {
        ham_parameter_t p[]={
            {HAM_PARAM_KEYSIZE,   7},
            {0, 0}
        };

        BFC_ASSERT_EQUAL(HAM_INV_KEYSIZE,
                ham_create_ex(m_db, BFC_OPATH(".test"),
                        m_flags|HAM_RECORD_NUMBER, 0664, &p[0]));

        p[0].value=9;
        BFC_ASSERT_EQUAL(0,
                ham_create_ex(m_db, BFC_OPATH(".test"),
                        m_flags|HAM_RECORD_NUMBER, 0664, &p[0]));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
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
        key.flags=HAM_KEY_USER_ALLOC;

        BFC_ASSERT_EQUAL(0, ham_env_new(&env));
        BFC_ASSERT_EQUAL(0,
                ham_env_create(env, BFC_OPATH(".test"), m_flags, 0664));
        BFC_ASSERT_EQUAL(0,
                ham_env_create_db(env, m_db, 333, HAM_RECORD_NUMBER, 0));
        BFC_ASSERT_EQUAL(0,
                ham_insert(m_db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL((ham_u64_t)1ull, *(ham_u64_t *)key.data);
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));

        if (!(m_flags&HAM_IN_MEMORY_DB)) {
            BFC_ASSERT_EQUAL(0, ham_env_open(env, BFC_OPATH(".test"), 0));
            BFC_ASSERT_EQUAL(0,
                    ham_env_open_db(env, m_db, 333, HAM_RECORD_NUMBER, 0));
            BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, 0, &key, &rec, 0));
            BFC_ASSERT_EQUAL((ham_u64_t)2ull, *(ham_u64_t *)key.data);
            BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
            BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        }

        BFC_ASSERT_EQUAL(0, ham_env_delete(env));
    }

    void endianTestOpenDatabase(void)
    {
// i currently don't have access to a big-endian machine, and the existing
// files were created with a database < 1.0.9 and are no longer supported
#if 0
        ham_key_t key;
        ham_record_t rec;
        ham_offset_t recno=100;
        ham_cursor_t *cursor;

        /* generated with `cat ../COPYING.GPL2 | ./db4`; has 2973 entries */
#if defined(HAM_LITTLE_ENDIAN)
        BFC_ASSERT_EQUAL(true,
            os::copy("data/recno-endian-test-open-database-be.hdb",
                    BFC_OPATH(".test")));
#else
        BFC_ASSERT_EQUAL(true,
            os::copy("data/recno-endian-test-open-database-le.hdb",
                    BFC_OPATH(".test")));
#endif
        BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), 0));

        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &cursor));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        key.data=(void *)&recno;
        key.size=sizeof(recno);
        BFC_ASSERT_EQUAL(0,
                ham_find(m_db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, strcmp("the", (char *)rec.data));

        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_find(cursor, &key, 0));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_move(cursor, 0, &rec, 0));
        BFC_ASSERT_EQUAL(0, strcmp("the", (char *)rec.data));

        BFC_ASSERT_EQUAL(0, ham_cursor_erase(cursor, 0));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                ham_find(m_db, 0, &key, &rec, 0));

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0,
                ham_insert(m_db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL((ham_u64_t)2974ull, *(ham_u64_t *)key.data);

        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_insert(cursor, &key, &rec, 0));
        BFC_ASSERT_EQUAL((ham_u64_t)2975ull, *(ham_u64_t *)key.data);

        BFC_ASSERT_EQUAL(0,
                ham_erase(m_db, 0, &key, 0));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND,
                ham_find(m_db, 0, &key, &rec, 0));

        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
#endif
    }

    void overwriteTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_u64_t recno, value;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        BFC_ASSERT_EQUAL(0,
                ham_create(m_db, BFC_OPATH(".test"),
                        m_flags|HAM_RECORD_NUMBER, 0664));

        key.data=&recno;
        key.flags=HAM_KEY_USER_ALLOC;
        key.size=sizeof(recno);
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, 0));

        value=0x13ull;
        memset(&rec, 0, sizeof(rec));
        rec.data=&value;
        rec.size=sizeof(value);
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));

        key.size=4;
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
        key.size=8;
        key.data=0;
        BFC_ASSERT_EQUAL(HAM_INV_PARAMETER,
                ham_insert(m_db, 0, &key, &rec, HAM_OVERWRITE));
        key.data=&recno;

        memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));

        BFC_ASSERT_EQUAL(value, *(ham_u64_t *)rec.data);

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void overwriteCursorTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_u64_t recno, value;
        ham_cursor_t *cursor;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        BFC_ASSERT_EQUAL(0,
                ham_create(m_db, BFC_OPATH(".test"), m_flags|HAM_RECORD_NUMBER, 0664));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_create(m_db, 0, 0, &cursor));

        key.data=&recno;
        key.flags=HAM_KEY_USER_ALLOC;
        key.size=sizeof(recno);
        BFC_ASSERT_EQUAL(0, ham_cursor_insert(cursor, &key, &rec, 0));

        value=0x13ull;
        memset(&rec, 0, sizeof(rec));
        rec.data=&value;
        rec.size=sizeof(value);
        BFC_ASSERT_EQUAL(0,
                ham_cursor_insert(cursor, &key, &rec, HAM_OVERWRITE));

        memset(&rec, 0, sizeof(rec));
        BFC_ASSERT_EQUAL(0, ham_find(m_db, 0, &key, &rec, 0));

        BFC_ASSERT_EQUAL(value, *(ham_u64_t *)rec.data);

        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void eraseLastReopenTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_u64_t recno;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        BFC_ASSERT_EQUAL(0,
                ham_create(m_db, BFC_OPATH(".test"), m_flags|HAM_RECORD_NUMBER, 0664));

        key.data=&recno;
        key.flags=HAM_KEY_USER_ALLOC;
        key.size=sizeof(recno);

        for (int i=0; i<5; i++) {
            BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, 0, &key, &rec, 0));
            BFC_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        BFC_ASSERT_EQUAL(0, ham_erase(m_db, 0, &key, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), m_flags));

        for (int i=5; i<10; i++) {
            BFC_ASSERT_EQUAL(0,
                    ham_insert(m_db, 0, &key, &rec, 0));
            BFC_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void uncoupleTest(void)
    {
        ham_key_t key;
        ham_record_t rec;
        ham_u64_t recno;
        ham_cursor_t *cursor, *c2;

        memset(&key, 0, sizeof(key));
        memset(&rec, 0, sizeof(rec));

        key.flags=HAM_KEY_USER_ALLOC;
        key.data=&recno;
        key.size=sizeof(recno);

        BFC_ASSERT_EQUAL(0,
                ham_create(m_db, BFC_OPATH(".test"), m_flags|HAM_RECORD_NUMBER, 0664));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_create(m_db, 0, 0, &cursor));
        BFC_ASSERT_EQUAL(0,
                ham_cursor_create(m_db, 0, 0, &c2));

        for (int i=0; i<5; i++) {
            BFC_ASSERT_EQUAL(0,
                    ham_cursor_insert(cursor, &key, &rec, 0));
            BFC_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        BtreeBackend *be=(BtreeBackend *)((Database *)m_db)->get_backend();
        Page *page;
        BFC_ASSERT_EQUAL(0, db_fetch_page(&page, (Database *)m_db,
                be->get_rootpage(), 0));
        BFC_ASSERT(page!=0);
        BFC_ASSERT_EQUAL(0, page->uncouple_all_cursors());

        for (int i=0; i<5; i++) {
            BFC_ASSERT_EQUAL(0,
                ham_cursor_move(c2, &key, &rec, HAM_CURSOR_NEXT));
            BFC_ASSERT_EQUAL((ham_u64_t)i+1, recno);
        }

        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_AUTO_CLEANUP));
    }
};

class InMemoryRecNoTest : public RecNoTest
{
public:
    InMemoryRecNoTest()
    :   RecNoTest(HAM_IN_MEMORY_DB, "InMemoryRecNoTest")
    {
        clear_tests(); // don't inherit tests
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(InMemoryRecNoTest, createCloseTest);
        BFC_REGISTER_TEST(InMemoryRecNoTest, createInsertCloseTest);
        BFC_REGISTER_TEST(InMemoryRecNoTest, createInsertManyCloseTest);
        BFC_REGISTER_TEST(InMemoryRecNoTest, createInsertCloseCursorTest);
        BFC_REGISTER_TEST(InMemoryRecNoTest, insertBadKeyTest);
        BFC_REGISTER_TEST(InMemoryRecNoTest, insertBadKeyCursorTest);
        BFC_REGISTER_TEST(InMemoryRecNoTest, createBadKeysizeTest);
        BFC_REGISTER_TEST(InMemoryRecNoTest, envTest);
        BFC_REGISTER_TEST(InMemoryRecNoTest, overwriteTest);
        BFC_REGISTER_TEST(InMemoryRecNoTest, overwriteCursorTest);
        BFC_REGISTER_TEST(InMemoryRecNoTest, uncoupleTest);
    }

};

BFC_REGISTER_FIXTURE(RecNoTest);
BFC_REGISTER_FIXTURE(InMemoryRecNoTest);
