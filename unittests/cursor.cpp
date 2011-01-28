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
#include <cstdlib>
#include <cstring>
#include <vector>
#include <ham/hamsterdb.h>
#include "../src/env.h"
#include "memtracker.h"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;

class BaseCursorTest : public hamsterDB_fixture
{
    define_super(hamsterDB_fixture);

public:
    BaseCursorTest(const char *name="BaseCursorTest")
    : hamsterDB_fixture(name)
    {
    }

protected:
    ham_cursor_t *m_cursor;
    ham_db_t *m_db;
    ham_env_t *m_env;
    memtracker_t *m_alloc;

public:
    virtual void createCursor() 
    {
        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, 0, 0, &m_cursor));
    }
    
    virtual void setup() 
    { 
        __super::setup();

        BFC_ASSERT((m_alloc=memtracker_new())!=0);

        BFC_ASSERT_EQUAL(0, ham_new(&m_db));

        BFC_ASSERT_EQUAL(0, ham_env_new(&m_env));
        env_set_allocator(m_env, (mem_allocator_t *)m_alloc);

        BFC_ASSERT_EQUAL(0, 
                ham_env_create(m_env, BFC_OPATH(".test"), 
                    HAM_ENABLE_DUPLICATES
                        |HAM_ENABLE_RECOVERY
                        |HAM_ENABLE_TRANSACTIONS, 0664));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create_db(m_env, m_db, 13, 0, 0));
        createCursor();
    }

    virtual void teardown() 
    { 
        __super::teardown();

        BFC_ASSERT_EQUAL(0, ham_cursor_close(m_cursor));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_TXN_AUTO_COMMIT));
        BFC_ASSERT_EQUAL(0, ham_env_close(m_env, HAM_AUTO_CLEANUP));
        ham_delete(m_db);
        ham_env_delete(m_env);
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void insertFindTest(void)
    {
        ham_key_t key={0};
        ham_record_t rec={0};
        key.data=(void *)"12345";
        key.size=6;

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        BFC_ASSERT_EQUAL(HAM_DUPLICATE_KEY, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, HAM_OVERWRITE));
    }
};

class TempTxnCursorTest : public BaseCursorTest
{
    define_super(hamsterDB_fixture);

public:
    TempTxnCursorTest()
    : BaseCursorTest("TempTxnCursorTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(TempTxnCursorTest, insertFindTest);
    }

};

class LongTxnCursorTest : public BaseCursorTest
{
    define_super(hamsterDB_fixture);

    ham_txn_t *m_txn;
public:
    virtual void createCursor() 
    {
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&m_txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, m_txn, 0, &m_cursor));
    }
    
    LongTxnCursorTest()
    : BaseCursorTest("LongTxnCursorTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(LongTxnCursorTest, insertFindTest);
    }

};

class NoTxnCursorTest : public BaseCursorTest
{
    define_super(hamsterDB_fixture);

public:
    NoTxnCursorTest()
    : BaseCursorTest("NoTxnCursorTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(NoTxnCursorTest, insertFindTest);
    }

    virtual void setup() 
    { 
        __super::setup();

        BFC_ASSERT((m_alloc=memtracker_new())!=0);

        BFC_ASSERT_EQUAL(0, ham_new(&m_db));

        BFC_ASSERT_EQUAL(0, ham_env_new(&m_env));
        env_set_allocator(m_env, (mem_allocator_t *)m_alloc);

        BFC_ASSERT_EQUAL(0, 
                ham_env_create(m_env, BFC_OPATH(".test"), 
                    HAM_ENABLE_DUPLICATES, 0664));
        BFC_ASSERT_EQUAL(0, 
                ham_env_create_db(m_env, m_db, 13, 0, 0));
        createCursor();
    }
};

BFC_REGISTER_FIXTURE(TempTxnCursorTest);
BFC_REGISTER_FIXTURE(LongTxnCursorTest);
BFC_REGISTER_FIXTURE(NoTxnCursorTest);

