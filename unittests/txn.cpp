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
#include "../src/db.h"
#include "../src/txn.h"
#include "../src/page.h"
#include "../src/error.h"
#include "memtracker.h"

#include "bfc-testsuite.hpp"
#include "hamster_fixture.hpp"

using namespace bfc;

class TxnTest : public hamsterDB_fixture
{
	define_super(hamsterDB_fixture);

public:
    TxnTest()
    : hamsterDB_fixture("TxnTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(TxnTest, beginCommitTest);
        BFC_REGISTER_TEST(TxnTest, beginAbortTest);
        BFC_REGISTER_TEST(TxnTest, structureTest);
        BFC_REGISTER_TEST(TxnTest, addPageTest);
        BFC_REGISTER_TEST(TxnTest, addPageAbortTest);
        BFC_REGISTER_TEST(TxnTest, removePageTest);
        BFC_REGISTER_TEST(TxnTest, onlyOneTxnAllowedTest);
    }

protected:
    ham_db_t *m_db;
    memtracker_t *m_alloc;

public:
    virtual void setup() 
	{ 
		__super::setup();

        BFC_ASSERT((m_alloc=memtracker_new())!=0);
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
		db_set_rt_flags(m_db, HAM_ENABLE_TRANSACTIONS);
    }
    
    virtual void teardown() 
	{ 
		__super::teardown();

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void beginCommitTest(void)
    {
        ham_txn_t *txn;

        BFC_ASSERT(ham_txn_begin(&txn, m_db, 0)==HAM_SUCCESS);
        BFC_ASSERT(ham_txn_commit(txn, 0)==HAM_SUCCESS);
    }

    void beginAbortTest(void)
    {
        ham_txn_t *txn;

        BFC_ASSERT(ham_txn_begin(&txn, m_db, 0)==HAM_SUCCESS);
        BFC_ASSERT(ham_txn_abort(txn, 0)==HAM_SUCCESS);
    }

    void structureTest(void)
    {
        ham_txn_t *txn;

        BFC_ASSERT(ham_txn_begin(&txn, m_db, 0)==HAM_SUCCESS);
        BFC_ASSERT(txn_get_db(txn)==m_db);
        BFC_ASSERT(txn_get_pagelist(txn)==0);
        BFC_ASSERT_EQUAL((ham_u64_t)1, txn_get_id(txn));

        txn_set_flags(txn, 0x99);
        BFC_ASSERT_EQUAL((ham_u32_t)0x99, txn_get_flags(txn));

        txn_set_pagelist(txn, (ham_page_t *)0x13);
        BFC_ASSERT(txn_get_pagelist(txn)==(ham_page_t *)0x13);
        txn_set_pagelist(txn, 0);

        txn_set_log_desc(txn, 4);
        BFC_ASSERT_EQUAL(4, txn_get_log_desc(txn));

        BFC_ASSERT(txn_get_pagelist(txn)==0);
        BFC_ASSERT(ham_txn_commit(txn, 0)==HAM_SUCCESS);
    }

    void addPageTest(void)
    {
        ham_txn_t *txn;
        ham_page_t *page;

        BFC_ASSERT((page=page_new(m_db))!=0);
        page_set_self(page, 0x12345);

        BFC_ASSERT(ham_txn_begin(&txn, m_db, 0)==HAM_SUCCESS);
        BFC_ASSERT(txn_get_page(txn, 0x12345)==0);
        BFC_ASSERT(txn_add_page(txn, page, 0)==HAM_SUCCESS);
        BFC_ASSERT(txn_add_page(txn, page, 1)==HAM_SUCCESS);
        BFC_ASSERT(txn_get_page(txn, 0x12345)==page);

        BFC_ASSERT(ham_txn_commit(txn, 0)==HAM_SUCCESS);

        page_delete(page);
    }

    void addPageAbortTest(void)
    {
        ham_txn_t *txn;
        ham_page_t *page;

        BFC_ASSERT((page=page_new(m_db))!=0);
        page_set_self(page, 0x12345);

        BFC_ASSERT(ham_txn_begin(&txn, m_db, 0)==HAM_SUCCESS);
        BFC_ASSERT(txn_get_page(txn, 0x12345)==0);
        BFC_ASSERT(txn_add_page(txn, page, 0)==HAM_SUCCESS);
        BFC_ASSERT(txn_add_page(txn, page, 1)==HAM_SUCCESS);
        BFC_ASSERT(txn_get_page(txn, 0x12345)==page);
        BFC_ASSERT_EQUAL(0, txn_free_page(txn, page));
        BFC_ASSERT(page_get_npers_flags(page) & PAGE_NPERS_DELETE_PENDING);

        BFC_ASSERT(ham_txn_abort(txn, 0)==HAM_SUCCESS);

        page_delete(page);
    }

    void removePageTest(void)
    {
        ham_txn_t *txn;
        ham_page_t *page;

        BFC_ASSERT((page=page_new(m_db))!=0);
        page_set_self(page, 0x12345);

        BFC_ASSERT(ham_txn_begin(&txn, m_db, 0)==HAM_SUCCESS);
        BFC_ASSERT(txn_add_page(txn, page, 0)==HAM_SUCCESS);
        BFC_ASSERT(txn_get_page(txn, page_get_self(page))==page);
        BFC_ASSERT(txn_remove_page(txn, page)==HAM_SUCCESS);
        BFC_ASSERT(txn_get_page(txn, page_get_self(page))==0);

        BFC_ASSERT(ham_txn_commit(txn, 0)==HAM_SUCCESS);

        page_delete(page);
    }

    void onlyOneTxnAllowedTest(void)
    {
        ham_txn_t *txn1;
		ham_txn_t *txn2;

        BFC_ASSERT_EQUAL(0, 
                ham_create(m_db, BFC_OPATH(".test"), HAM_ENABLE_TRANSACTIONS, 0644));

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn1, m_db, 0));
        BFC_ASSERT_EQUAL(HAM_LIMITS_REACHED, 
                ham_txn_begin(&txn2, m_db, 0));
        BFC_ASSERT_EQUAL(HAM_LIMITS_REACHED, 
                ham_txn_begin(&txn2, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn1, 0));
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn2, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn2, 0));
    }

};

class HighLevelTxnTest : public hamsterDB_fixture
{
	define_super(hamsterDB_fixture);

public:
    HighLevelTxnTest()
    : hamsterDB_fixture("HighLevelTxnTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(HighLevelTxnTest, noPersistentDatabaseFlagTest);
        BFC_REGISTER_TEST(HighLevelTxnTest, noPersistentEnvironmentFlagTest);
        BFC_REGISTER_TEST(HighLevelTxnTest, cursorStillOpenTest);
        BFC_REGISTER_TEST(HighLevelTxnTest, clonedCursorStillOpenTest);
        BFC_REGISTER_TEST(HighLevelTxnTest, autoAbortDatabaseTest);
        BFC_REGISTER_TEST(HighLevelTxnTest, autoCommitDatabaseTest);
        BFC_REGISTER_TEST(HighLevelTxnTest, autoAbortEnvironmentTest);
        BFC_REGISTER_TEST(HighLevelTxnTest, autoAbortEnvironment2Test);
        BFC_REGISTER_TEST(HighLevelTxnTest, environmentTest);
        BFC_REGISTER_TEST(HighLevelTxnTest, rollbackBigBlobTest);
        // huge blobs are not reused if a txn is aborted @@@
        // BFC_REGISTER_TEST(HighLevelTxnTest, rollbackHugeBlobTest);
        BFC_REGISTER_TEST(HighLevelTxnTest, rollbackNormalBlobTest);
    }

protected:
    ham_db_t *m_db;
    memtracker_t *m_alloc;

public:
    virtual void setup() 
	{ 
		__super::setup();

        BFC_ASSERT((m_alloc=memtracker_new())!=0);
        BFC_ASSERT_EQUAL(0, ham_new(&m_db));
        db_set_allocator(m_db, (mem_allocator_t *)m_alloc);
    }
    
    virtual void teardown() 
	{ 
		__super::teardown();

        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
        ham_delete(m_db);
        BFC_ASSERT(!memtracker_get_leaks(m_alloc));
    }

    void noPersistentDatabaseFlagTest(void)
    {
        BFC_ASSERT_EQUAL(0, 
                ham_create(m_db, BFC_OPATH(".test"), HAM_ENABLE_TRANSACTIONS, 0644));
        BFC_ASSERT(HAM_ENABLE_TRANSACTIONS&db_get_rt_flags(m_db));
        BFC_ASSERT(HAM_ENABLE_RECOVERY&db_get_rt_flags(m_db));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        BFC_ASSERT_EQUAL(0, ham_open(m_db, BFC_OPATH(".test"), 0));
        BFC_ASSERT(!(HAM_ENABLE_TRANSACTIONS&db_get_rt_flags(m_db)));
        BFC_ASSERT(!(HAM_ENABLE_RECOVERY&db_get_rt_flags(m_db)));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void noPersistentEnvironmentFlagTest(void)
    {
        ham_env_t *env;

        ham_env_new(&env);

        BFC_ASSERT_EQUAL(0, 
                ham_env_create(env, BFC_OPATH(".test"), HAM_ENABLE_TRANSACTIONS, 0644));
        BFC_ASSERT(HAM_ENABLE_TRANSACTIONS&env_get_rt_flags(env));
        BFC_ASSERT(HAM_ENABLE_RECOVERY&env_get_rt_flags(env));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));

        BFC_ASSERT_EQUAL(0, ham_env_open(env, BFC_OPATH(".test"), 0));
        BFC_ASSERT(!(HAM_ENABLE_TRANSACTIONS&env_get_rt_flags(env)));
        BFC_ASSERT(!(HAM_ENABLE_RECOVERY&env_get_rt_flags(env)));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));

        ham_env_delete(env);
    }

    void cursorStillOpenTest(void)
    {
        ham_txn_t *txn;
        ham_cursor_t *cursor;

        BFC_ASSERT_EQUAL(0, 
                ham_create(m_db, BFC_OPATH(".test"), HAM_ENABLE_TRANSACTIONS, 0644));
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, txn, 0, &cursor));
        BFC_ASSERT_EQUAL(HAM_CURSOR_STILL_OPEN, ham_txn_commit(txn, 0));
        BFC_ASSERT_EQUAL(HAM_CURSOR_STILL_OPEN, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void clonedCursorStillOpenTest(void)
    {
        ham_txn_t *txn;
        ham_cursor_t *cursor, *clone;

        BFC_ASSERT_EQUAL(0, 
                ham_create(m_db, BFC_OPATH(".test"), HAM_ENABLE_TRANSACTIONS, 0644));
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_cursor_create(m_db, txn, 0, &cursor));
        BFC_ASSERT_EQUAL(0, ham_cursor_clone(cursor, &clone));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(cursor));
        BFC_ASSERT_EQUAL(HAM_CURSOR_STILL_OPEN, ham_txn_commit(txn, 0));
        BFC_ASSERT_EQUAL(HAM_CURSOR_STILL_OPEN, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(clone));
        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void autoAbortDatabaseTest(void)
    {
        ham_txn_t *txn;
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        BFC_ASSERT_EQUAL(0, 
                ham_create(m_db, BFC_OPATH(".test"), HAM_ENABLE_TRANSACTIONS, 0644));
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_find(m_db, txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), HAM_ENABLE_TRANSACTIONS));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                        ham_find(m_db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    void autoCommitDatabaseTest(void)
    {
        ham_txn_t *txn;
        ham_key_t key;
        ham_record_t rec;
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));

        BFC_ASSERT_EQUAL(0, 
                ham_create(m_db, BFC_OPATH(".test"), HAM_ENABLE_TRANSACTIONS, 0644));
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_find(m_db, txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, HAM_TXN_AUTO_COMMIT));

        BFC_ASSERT_EQUAL(0, 
                ham_open(m_db, BFC_OPATH(".test"), HAM_ENABLE_TRANSACTIONS));
        BFC_ASSERT_EQUAL(0, 
                        ham_find(m_db, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
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

        BFC_ASSERT_EQUAL(0, 
                ham_env_create(env, BFC_OPATH(".test"), HAM_ENABLE_TRANSACTIONS, 0644));
        BFC_ASSERT_EQUAL(0,
                ham_env_create_db(env, db1, 1, 0, 0));
        BFC_ASSERT_EQUAL(0,
                ham_env_create_db(env, db2, 2, 0, 0));

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, db1, 0));
        BFC_ASSERT_EQUAL(0, ham_insert(db1, txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_find(db1, txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_close(db1, 0));
        BFC_ASSERT_EQUAL(0, ham_insert(db2, txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_find(db2, txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_close(db2, 0));

        BFC_ASSERT_EQUAL(0, 
                ham_env_open_db(env, db1, 1, 0, 0));
        BFC_ASSERT_EQUAL(0, 
                ham_env_open_db(env, db2, 2, 0, 0));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_find(db1, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_find(db2, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));

        ham_env_delete(env);
        ham_delete(db1);
        ham_delete(db2);
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

        BFC_ASSERT_EQUAL(0, 
                ham_env_create(env, BFC_OPATH(".test"), HAM_ENABLE_TRANSACTIONS, 0644));
        BFC_ASSERT_EQUAL(0,
                ham_env_create_db(env, db1, 1, 0, 0));
        BFC_ASSERT_EQUAL(0,
                ham_env_create_db(env, db2, 2, 0, 0));

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, db1, 0));
        BFC_ASSERT_EQUAL(0, ham_insert(db1, txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_find(db1, txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_close(db1, 0));
        BFC_ASSERT_EQUAL(0, ham_insert(db2, txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_find(db2, txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));

        BFC_ASSERT_EQUAL(0, 
                ham_env_open(env, BFC_OPATH(".test"), HAM_ENABLE_TRANSACTIONS));
        BFC_ASSERT_EQUAL(0, 
                ham_env_open_db(env, db1, 1, 0, 0));
        BFC_ASSERT_EQUAL(0, 
                ham_env_open_db(env, db2, 2, 0, 0));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_find(db1, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                ham_find(db2, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));
        ham_delete(db1);
        ham_delete(db2);
        ham_env_delete(env);
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

        BFC_ASSERT_EQUAL(0, 
                ham_env_create(env, BFC_OPATH(".test"), HAM_ENABLE_TRANSACTIONS, 0644));
        BFC_ASSERT_EQUAL(0,
                ham_env_create_db(env, db1, 1, 0, 0));
        BFC_ASSERT_EQUAL(0,
                ham_env_create_db(env, db2, 2, 0, 0));

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, db1, 0));
        BFC_ASSERT_EQUAL(0, ham_insert(db1, txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_find(db1, txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_close(db1, 0));
        BFC_ASSERT_EQUAL(0, ham_insert(db2, txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_find(db2, txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));
        BFC_ASSERT_EQUAL(0, ham_close(db2, 0));

        BFC_ASSERT_EQUAL(0, 
                ham_env_open_db(env, db1, 1, 0, 0));
        BFC_ASSERT_EQUAL(0, 
                ham_env_open_db(env, db2, 2, 0, 0));
        BFC_ASSERT_EQUAL(0, 
                ham_find(db1, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, 
                ham_find(db2, 0, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_env_close(env, 0));

        ham_delete(db1);
        ham_delete(db2);
        ham_env_delete(env);
    }

    void rollbackBigBlobTest(void)
    {
        ham_txn_t *txn;
        ham_key_t key;
        ham_record_t rec;
        ham_u8_t buffer[1024*8];
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        rec.data=&buffer[0];
        rec.size=sizeof(buffer);

        BFC_ASSERT_EQUAL(0, 
                ham_create(m_db, BFC_OPATH(".test"), 
                            HAM_ENABLE_TRANSACTIONS, 0644));
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));

        ham_offset_t o=freel_alloc_area(m_db, sizeof(buffer));
        BFC_ASSERT_NOTNULL(o);
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }

    // not used...
    void rollbackHugeBlobTest(void)
    {
        ham_txn_t *txn;
        ham_key_t key;
        ham_record_t rec;
        ham_size_t ps=os_get_pagesize();
        ham_u8_t *buffer=(ham_u8_t *)malloc(ps*2);
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        rec.data=&buffer[0];
        rec.size=ps*2;

        BFC_ASSERT_EQUAL(0, 
                ham_create(m_db, BFC_OPATH(".test"), 
                    HAM_ENABLE_TRANSACTIONS, 0644));
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));

        ham_offset_t o=freel_alloc_area(m_db, ps*2);
        BFC_ASSERT_NOTNULL(o);
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));

        free(buffer);
    }

    void rollbackNormalBlobTest(void)
    {
        ham_txn_t *txn;
        ham_key_t key;
        ham_record_t rec;
        ham_u8_t buffer[64];
        ::memset(&key, 0, sizeof(key));
        ::memset(&rec, 0, sizeof(rec));
        rec.data=&buffer[0];
        rec.size=sizeof(buffer);

        BFC_ASSERT_EQUAL(0, 
                ham_create(m_db, BFC_OPATH(".test"), 
                    HAM_ENABLE_TRANSACTIONS, 0644));
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, ham_insert(m_db, txn, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, ham_txn_abort(txn, 0));

        ham_offset_t o=freel_alloc_area(m_db, sizeof(buffer));
        BFC_ASSERT_NOTNULL(o);
        BFC_ASSERT_EQUAL(0, ham_close(m_db, 0));
    }
};

BFC_REGISTER_FIXTURE(TxnTest);
BFC_REGISTER_FIXTURE(HighLevelTxnTest);

