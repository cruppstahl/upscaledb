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
#include <vector>
#include <cppunit/extensions/HelperMacros.h>
#include <ham/hamsterdb.h>
#include "../src/db.h"
#include "../src/txn.h"
#include "../src/page.h"
#include "../src/error.h"
#include "memtracker.h"

class TxnTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(TxnTest);
    CPPUNIT_TEST      (beginCommitTest);
    CPPUNIT_TEST      (beginAbortTest);
    CPPUNIT_TEST      (structureTest);
    CPPUNIT_TEST      (addPageTest);
    CPPUNIT_TEST      (addPageAbortTest);
    CPPUNIT_TEST      (removePageTest);
    CPPUNIT_TEST      (onlyOneTxnAllowedTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_db_t *m_db;
    memtracker_t *m_alloc;

public:
    void setUp()
    { 
        CPPUNIT_ASSERT((m_alloc=memtracker_new())!=0);
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
    }
    
    void tearDown() 
    { 
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        CPPUNIT_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void beginCommitTest(void)
    {
        ham_txn_t txn;

        CPPUNIT_ASSERT(txn_begin(&txn, m_db, 0)==HAM_SUCCESS);
        CPPUNIT_ASSERT(txn_commit(&txn, 0)==HAM_SUCCESS);
    }

    void beginAbortTest(void)
    {
        ham_txn_t txn;

        CPPUNIT_ASSERT(txn_begin(&txn, m_db, 0)==HAM_SUCCESS);
        CPPUNIT_ASSERT(txn_abort(&txn, 0)==HAM_SUCCESS);
    }

    void structureTest(void)
    {
        ham_txn_t txn;

        CPPUNIT_ASSERT(txn_begin(&txn, m_db, 0)==HAM_SUCCESS);
        CPPUNIT_ASSERT(txn_get_db(&txn)==m_db);
        CPPUNIT_ASSERT(txn_get_pagelist(&txn)==0);
        CPPUNIT_ASSERT_EQUAL((ham_u64_t)1, txn_get_id(&txn));

        txn_set_flags(&txn, 0x99);
        CPPUNIT_ASSERT_EQUAL((ham_u32_t)0x99, txn_get_flags(&txn));

        txn_set_pagelist(&txn, (ham_page_t *)0x13);
        CPPUNIT_ASSERT(txn_get_pagelist(&txn)==(ham_page_t *)0x13);
        txn_set_pagelist(&txn, 0);

        txn_set_log_desc(&txn, 4);
        CPPUNIT_ASSERT_EQUAL(4, txn_get_log_desc(&txn));

        CPPUNIT_ASSERT(txn_get_pagelist(&txn)==0);
        CPPUNIT_ASSERT(txn_commit(&txn, 0)==HAM_SUCCESS);
    }

    void addPageTest(void)
    {
        ham_txn_t txn;
        ham_page_t *page;

        CPPUNIT_ASSERT((page=page_new(m_db))!=0);
        page_set_self(page, 0x12345);

        CPPUNIT_ASSERT(txn_begin(&txn, m_db, 0)==HAM_SUCCESS);
        CPPUNIT_ASSERT(txn_get_page(&txn, 0x12345)==0);
        CPPUNIT_ASSERT(txn_add_page(&txn, page, 0)==HAM_SUCCESS);
        CPPUNIT_ASSERT(txn_add_page(&txn, page, 1)==HAM_SUCCESS);
        CPPUNIT_ASSERT(txn_get_page(&txn, 0x12345)==page);

        CPPUNIT_ASSERT(txn_commit(&txn, 0)==HAM_SUCCESS);

        page_delete(page);
    }

    void addPageAbortTest(void)
    {
        ham_txn_t txn;
        ham_page_t *page;

        CPPUNIT_ASSERT((page=page_new(m_db))!=0);
        page_set_self(page, 0x12345);

        CPPUNIT_ASSERT(txn_begin(&txn, m_db, 0)==HAM_SUCCESS);
        CPPUNIT_ASSERT(txn_get_page(&txn, 0x12345)==0);
        CPPUNIT_ASSERT(txn_add_page(&txn, page, 0)==HAM_SUCCESS);
        CPPUNIT_ASSERT(txn_add_page(&txn, page, 1)==HAM_SUCCESS);
        CPPUNIT_ASSERT(txn_get_page(&txn, 0x12345)==page);
        CPPUNIT_ASSERT_EQUAL(0, txn_free_page(&txn, page));
        CPPUNIT_ASSERT(page_get_npers_flags(page)&PAGE_NPERS_DELETE_PENDING);

        CPPUNIT_ASSERT(txn_abort(&txn, 0)==HAM_SUCCESS);

        page_delete(page);
    }

    void removePageTest(void)
    {
        ham_txn_t txn;
        ham_page_t *page;

        CPPUNIT_ASSERT((page=page_new(m_db))!=0);
        page_set_self(page, 0x12345);

        CPPUNIT_ASSERT(txn_begin(&txn, m_db, 0)==HAM_SUCCESS);
        CPPUNIT_ASSERT(txn_add_page(&txn, page, 0)==HAM_SUCCESS);
        CPPUNIT_ASSERT(txn_get_page(&txn, page_get_self(page))==page);
        CPPUNIT_ASSERT(txn_remove_page(&txn, page)==HAM_SUCCESS);
        CPPUNIT_ASSERT(txn_get_page(&txn, page_get_self(page))==0);

        CPPUNIT_ASSERT(txn_commit(&txn, 0)==HAM_SUCCESS);

        page_delete(page);
    }

    void onlyOneTxnAllowedTest(void)
    {
        ham_txn_t *txn1, *txn2;

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create(m_db, ".test", HAM_ENABLE_TRANSACTIONS, 0644));

        CPPUNIT_ASSERT_EQUAL(0, ham_txn_begin(&txn1, m_db, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_LIMITS_REACHED, 
                ham_txn_begin(&txn2, m_db, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_LIMITS_REACHED, 
                ham_txn_begin(&txn2, m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_txn_commit(txn1, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_txn_begin(&txn2, m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_txn_commit(txn2, 0));
    }

};

class HighLevelTxnTest : public CppUnit::TestFixture
{
    CPPUNIT_TEST_SUITE(HighLevelTxnTest);
    CPPUNIT_TEST      (noPersistentDatabaseFlagTest);
    CPPUNIT_TEST      (noPersistentEnvironmentFlagTest);
    CPPUNIT_TEST      (cursorStillOpenTest);
    CPPUNIT_TEST      (clonedCursorStillOpenTest);
    CPPUNIT_TEST      (autoAbortDatabaseTest);
    CPPUNIT_TEST      (autoAbortEnvironmentTest);
    CPPUNIT_TEST      (autoAbortEnvironment2Test);
    CPPUNIT_TEST      (environmentTest);
    CPPUNIT_TEST_SUITE_END();

protected:
    ham_db_t *m_db;
    memtracker_t *m_alloc;

public:
    void setUp()
    { 
        CPPUNIT_ASSERT((m_alloc=memtracker_new())!=0);
        CPPUNIT_ASSERT_EQUAL(0, ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
    }
    
    void tearDown() 
    { 
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        CPPUNIT_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void noPersistentDatabaseFlagTest(void)
    {
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create(m_db, ".test", HAM_ENABLE_TRANSACTIONS, 0644));
        CPPUNIT_ASSERT(HAM_ENABLE_TRANSACTIONS&db_get_rt_flags(m_db));
        CPPUNIT_ASSERT(HAM_ENABLE_RECOVERY&db_get_rt_flags(m_db));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_open(m_db, ".test", 0));
        CPPUNIT_ASSERT(!(HAM_ENABLE_TRANSACTIONS&db_get_rt_flags(m_db)));
        CPPUNIT_ASSERT(!(HAM_ENABLE_RECOVERY&db_get_rt_flags(m_db)));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void noPersistentEnvironmentFlagTest(void)
    {
        ham_env_t *env;

        ham_env_new(&env);

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_create(env, ".test", HAM_ENABLE_TRANSACTIONS, 0644));
        CPPUNIT_ASSERT(HAM_ENABLE_TRANSACTIONS&env_get_rt_flags(env));
        CPPUNIT_ASSERT(HAM_ENABLE_RECOVERY&env_get_rt_flags(env));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_env_open(env, ".test", 0));
        CPPUNIT_ASSERT(!(HAM_ENABLE_TRANSACTIONS&env_get_rt_flags(env)));
        CPPUNIT_ASSERT(!(HAM_ENABLE_RECOVERY&env_get_rt_flags(env)));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));

        ham_env_delete(env);
    }

    void cursorStillOpenTest(void)
    {
        ham_txn_t *txn;
        ham_cursor_t *cursor;

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create(m_db, ".test", HAM_ENABLE_TRANSACTIONS, 0644));
        CPPUNIT_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, txn, 0, &cursor));
        CPPUNIT_ASSERT_EQUAL(HAM_CURSOR_STILL_OPEN, ham_txn_commit(txn, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_CURSOR_STILL_OPEN, ham_txn_abort(txn, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        CPPUNIT_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void clonedCursorStillOpenTest(void)
    {
        ham_txn_t *txn;
        ham_cursor_t *cursor, *clone;

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create(m_db, ".test", HAM_ENABLE_TRANSACTIONS, 0644));
        CPPUNIT_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_create(m_db, txn, 0, &cursor));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_clone(cursor, &clone));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        CPPUNIT_ASSERT_EQUAL(HAM_CURSOR_STILL_OPEN, ham_txn_commit(txn, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_CURSOR_STILL_OPEN, ham_txn_abort(txn, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_cursor_close(clone));
        CPPUNIT_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void autoAbortDatabaseTest(void)
    {
        ham_txn_t *txn;
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_create(m_db, ".test", HAM_ENABLE_TRANSACTIONS, 0644));
        CPPUNIT_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(m_db, txn, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(m_db, txn, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_open(m_db, ".test", HAM_ENABLE_TRANSACTIONS));
        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                        ham_find(m_db, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void autoAbortEnvironmentTest(void)
    {
        ham_env_t *env;
        ham_db_t *db1, *db2;
        ham_txn_t *txn;
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        ham_env_new(&env);
        ham_new(&db1);
        ham_new(&db2);

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_create(env, ".test", HAM_ENABLE_TRANSACTIONS, 0644));
        CPPUNIT_ASSERT_EQUAL(0,
                ham_env_create_db(env, db1, 1, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0,
                ham_env_create_db(env, db2, 2, 0, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_txn_begin(&txn, db1, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(db1, txn, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(db1, txn, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db1, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(db2, txn, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(db2, txn, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db2, 0));

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_open_db(env, db1, 1, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_open_db(env, db2, 2, 0, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_find(db1, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_find(db2, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
    }

    void autoAbortEnvironment2Test(void)
    {
        ham_env_t *env;
        ham_db_t *db1, *db2;
        ham_txn_t *txn;
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        ham_env_new(&env);
        ham_new(&db1);
        ham_new(&db2);

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_create(env, ".test", HAM_ENABLE_TRANSACTIONS, 0644));
        CPPUNIT_ASSERT_EQUAL(0,
                ham_env_create_db(env, db1, 1, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0,
                ham_env_create_db(env, db2, 2, 0, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_txn_begin(&txn, db1, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(db1, txn, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(db1, txn, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db1, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(db2, txn, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(db2, txn, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_open(env, ".test", HAM_ENABLE_TRANSACTIONS));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_open_db(env, db1, 1, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_open_db(env, db2, 2, 0, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_find(db1, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_find(db2, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));
    }

    void environmentTest(void)
    {
        ham_env_t *env;
        ham_db_t *db1, *db2;
        ham_txn_t *txn;
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        ham_env_new(&env);
        ham_new(&db1);
        ham_new(&db2);

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_create(env, ".test", HAM_ENABLE_TRANSACTIONS, 0644));
        CPPUNIT_ASSERT_EQUAL(0,
                ham_env_create_db(env, db1, 1, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0,
                ham_env_create_db(env, db2, 2, 0, 0));

        CPPUNIT_ASSERT_EQUAL(0, ham_txn_begin(&txn, db1, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(db1, txn, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(db1, txn, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db1, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_insert(db2, txn, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_find(db2, txn, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_close(db2, 0));

        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_open_db(env, db1, 1, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_env_open_db(env, db2, 2, 0, 0));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_find(db1, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, 
                ham_find(db2, 0, &key, &rec, 0));
        CPPUNIT_ASSERT_EQUAL(0, ham_env_close(env, 0));

        ham_delete(db1);
        ham_delete(db2);
        ham_env_delete(env);
    }
};

CPPUNIT_TEST_SUITE_REGISTRATION(TxnTest);
CPPUNIT_TEST_SUITE_REGISTRATION(HighLevelTxnTest);

