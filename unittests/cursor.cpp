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
#include "../src/cursor.h"
#include "../src/btree_cursor.h"
#include "../src/backend.h"
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
    virtual ham_status_t createCursor(ham_cursor_t **p) 
    {
        return (ham_cursor_create(m_db, 0, 0, p));
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
        BFC_ASSERT_EQUAL(0, createCursor(&m_cursor));
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
        rec.data=(void *)"abcde";
        rec.size=6;

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        BFC_ASSERT_EQUAL(HAM_DUPLICATE_KEY, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, HAM_OVERWRITE));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, &key, &rec, 0));
    }

    void insertFindMultipleCursorsTest(void)
    {
        ham_cursor_t *c[5];
        ham_key_t key={0};
        ham_record_t rec={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;

        for (int i=0; i<5; i++)
            BFC_ASSERT_EQUAL(0, createCursor(&c[i]));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        for (int i=0; i<5; i++) {
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(c[i], &key, 0));
        }

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(m_cursor, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, strcmp("12345", (char *)key.data));
        BFC_ASSERT_EQUAL(0, strcmp("abcde", (char *)rec.data));

        for (int i=0; i<5; i++) {
            BFC_ASSERT_EQUAL(0, 
                    ham_cursor_move(c[i], &key, &rec, 0));
            BFC_ASSERT_EQUAL(0, strcmp("12345", (char *)key.data));
            BFC_ASSERT_EQUAL(0, strcmp("abcde", (char *)rec.data));
            BFC_ASSERT_EQUAL(0, ham_cursor_close(c[i]));
        }
    }

    void findInEmptyDatabaseTest(void)
    {
        ham_key_t key={0};
        ham_record_t rec={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;

        /* this looks up a key in an empty database */
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                    ham_cursor_find(m_cursor, &key, 0));
    }

    void nilCursorTest(void)
    {
        ham_key_t key={0};
        ham_record_t rec={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;

        /* cursor is nil */

        BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, 
                    ham_cursor_move(m_cursor, &key, &rec, 0));

        BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, 
                    ham_cursor_overwrite(m_cursor, &rec, 0));

        ham_cursor_t *clone;
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_clone(m_cursor, &clone));
        BFC_ASSERT_EQUAL(true, bt_cursor_is_nil(m_cursor));
        BFC_ASSERT_EQUAL(true, bt_cursor_is_nil(clone));
        BFC_ASSERT_EQUAL(true, 
                txn_cursor_is_nil(cursor_get_txn_cursor(m_cursor)));
        BFC_ASSERT_EQUAL(true, 
                txn_cursor_is_nil(cursor_get_txn_cursor(clone)));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(clone));
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
        BFC_REGISTER_TEST(TempTxnCursorTest, insertFindMultipleCursorsTest);
        BFC_REGISTER_TEST(TempTxnCursorTest, findInEmptyDatabaseTest);
        BFC_REGISTER_TEST(TempTxnCursorTest, nilCursorTest);
        BFC_REGISTER_TEST(TempTxnCursorTest, cloneCoupledBtreeCursorTest);
        BFC_REGISTER_TEST(TempTxnCursorTest, cloneUncoupledBtreeCursorTest);
    }

    void cloneCoupledBtreeCursorTest(void)
    {
        ham_key_t key={0};
        ham_record_t rec={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;

        ham_cursor_t *clone;

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_clone(m_cursor, &clone));

        BFC_ASSERT_EQUAL(false, bt_cursor_is_nil(clone));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(clone));
    }

    void cloneUncoupledBtreeCursorTest(void)
    {
        ham_key_t key={0};
        ham_record_t rec={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;

        ham_cursor_t *clone;

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, 
                    bt_cursor_uncouple((ham_bt_cursor_t *)m_cursor, 0));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_clone(m_cursor, &clone));

        BFC_ASSERT_EQUAL(false, bt_cursor_is_nil(clone));
        ham_key_t *k1=bt_cursor_get_uncoupled_key((ham_bt_cursor_t *)m_cursor);
        ham_key_t *k2=bt_cursor_get_uncoupled_key((ham_bt_cursor_t *)clone);
        BFC_ASSERT_EQUAL(0, strcmp((char *)k1->data, (char *)k2->data));
        BFC_ASSERT_EQUAL(k1->size, k2->size);
        BFC_ASSERT_EQUAL(0, ham_cursor_close(clone));
    }
};

class LongTxnCursorTest : public BaseCursorTest
{
    define_super(hamsterDB_fixture);

    ham_txn_t *m_txn;
public:
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
        BFC_ASSERT_EQUAL(0, ham_txn_begin(&m_txn, m_db, 0));
        BFC_ASSERT_EQUAL(0, createCursor(&m_cursor));
    }

    virtual ham_status_t createCursor(ham_cursor_t **p) 
    {
        return (ham_cursor_create(m_db, m_txn, 0, p));
    }
    
    LongTxnCursorTest()
    : BaseCursorTest("LongTxnCursorTest")
    {
        testrunner::get_instance()->register_fixture(this);
        BFC_REGISTER_TEST(LongTxnCursorTest, insertFindTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, insertFindMultipleCursorsTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, findInEmptyDatabaseTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, findInEmptyTransactionTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, findInBtreeOverwrittenInTxnTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, findInTxnOverwrittenInTxnTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, findInTxnOverwrittenInTxnTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, eraseInTxnKeyFromBtreeTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, eraseInTxnKeyFromTxnTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, eraseInTxnOverwrittenKeyTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, eraseInTxnOverwrittenFindKeyTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, overwriteInEmptyTransactionTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, overwriteInTransactionTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, cloneCoupledTxnCursorTest);
        BFC_REGISTER_TEST(LongTxnCursorTest, nilCursorTest);
    }

    void findInEmptyTransactionTest(void)
    {
        ham_key_t key={0};
        ham_record_t rec={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;

        /* insert a key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* this looks up a key in an empty Transaction but with the btree */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));
        BFC_ASSERT_EQUAL(0, strcmp("12345", (char *)key.data));
        BFC_ASSERT_EQUAL(0, strcmp("abcde", (char *)rec.data));
    }

    void findInBtreeOverwrittenInTxnTest(void)
    {
        ham_key_t key={0};
        ham_record_t rec={0}, rec2={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;
        rec2.data=(void *)"22222";
        rec2.size=6;

        /* insert a key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* overwrite it in the Transaction */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec2, HAM_OVERWRITE));

        /* retrieve key and compare record */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find_ex(m_cursor, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, strcmp("12345", (char *)key.data));
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)rec.data));
    }

    void findInTxnOverwrittenInTxnTest(void)
    {
        ham_key_t key={0};
        ham_record_t rec={0}, rec2={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;
        rec2.data=(void *)"22222";
        rec2.size=6;

        /* insert a key into the txn */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* overwrite it in the Transaction */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec2, HAM_OVERWRITE));

        /* retrieve key and compare record */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find_ex(m_cursor, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, strcmp("12345", (char *)key.data));
        BFC_ASSERT_EQUAL(0, strcmp("22222", (char *)rec.data));
    }

    void eraseInTxnKeyFromBtreeTest(void)
    {
        ham_key_t key={0};
        ham_record_t rec={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;

        /* insert a key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* couple the cursor to this key */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));

        /* erase it in the Transaction */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));

        /* key is now nil */
        BFC_ASSERT_EQUAL(true, bt_cursor_is_nil(m_cursor));

        /* retrieve key - must fail */
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                    ham_cursor_find(m_cursor, &key, 0));
    }

    void eraseInTxnKeyFromTxnTest(void)
    {
        ham_key_t key={0};
        ham_record_t rec={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;

        /* insert a key into the Transaction */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* erase it in the Transaction */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));

        /* retrieve key - must fail */
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                    ham_cursor_find(m_cursor, &key, 0));
    }

    void eraseInTxnOverwrittenKeyTest(void)
    {
        ham_key_t key={0};
        ham_record_t rec={0}, rec2={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;

        /* insert a key into the Transaction */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* overwrite it in the Transaction */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec2, HAM_OVERWRITE));

        /* erase it in the Transaction */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));

        /* retrieve key - must fail */
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                    ham_cursor_find(m_cursor, &key, 0));
    }

    void eraseInTxnOverwrittenFindKeyTest(void)
    {
        ham_key_t key={0};
        ham_record_t rec={0}, rec2={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;

        BFC_ASSERT_EQUAL(HAM_CURSOR_IS_NIL, 
                    ham_cursor_erase(m_cursor, 0));

        /* insert a key into the Transaction */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));

        /* overwrite it in the Transaction */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec2, HAM_OVERWRITE));

        /* once more couple the cursor to this key */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));

        /* erase it in the Transaction */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_erase(m_cursor, 0));

        /* retrieve key - must fail */
        BFC_ASSERT_EQUAL(HAM_KEY_NOT_FOUND, 
                    ham_cursor_find(m_cursor, &key, 0));
    }

    void overwriteInEmptyTransactionTest(void)
    {
        ham_key_t key={0};
        ham_record_t rec={0}, rec2={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;
        rec2.data=(void *)"aaaaa";
        rec2.size=6;

        /* insert a key into the btree */
        ham_backend_t *be=db_get_backend(m_db);
        BFC_ASSERT_EQUAL(0, be->_fun_insert(be, &key, &rec, 0));

        /* this looks up a key in an empty Transaction but with the btree */
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find(m_cursor, &key, 0));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_overwrite(m_cursor, &rec2, 0));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find_ex(m_cursor, &key, &rec, 0));

        BFC_ASSERT_EQUAL(0, strcmp("12345", (char *)key.data));
        BFC_ASSERT_EQUAL(0, strcmp("aaaaa", (char *)rec.data));
    }

    void overwriteInTransactionTest(void)
    {
        ham_key_t key={0};
        ham_record_t rec={0}, rec2={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;
        rec2.data=(void *)"aaaaa";
        rec2.size=6;


        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_overwrite(m_cursor, &rec2, 0));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_find_ex(m_cursor, &key, &rec, 0));

        BFC_ASSERT_EQUAL(0, strcmp("12345", (char *)key.data));
        BFC_ASSERT_EQUAL(0, strcmp("aaaaa", (char *)rec.data));
    }

    void cloneCoupledTxnCursorTest(void)
    {
        ham_key_t key={0};
        ham_record_t rec={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;

        ham_cursor_t *clone;

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(m_cursor, &key, &rec, 0));
        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_clone(m_cursor, &clone));

        BFC_ASSERT_EQUAL(false, bt_cursor_is_nil(clone));
        BFC_ASSERT_EQUAL(2u, txn_get_cursor_refcount(m_txn));
        BFC_ASSERT_EQUAL(
                txn_cursor_get_coupled_op(cursor_get_txn_cursor(m_cursor)), 
                txn_cursor_get_coupled_op(cursor_get_txn_cursor(clone)));
        BFC_ASSERT_EQUAL(0, ham_cursor_close(clone));
        BFC_ASSERT_EQUAL(1u, txn_get_cursor_refcount(m_txn));
                
    }

    void flushCoupledOpTest(void)
    {
        ham_key_t key={0};
        ham_record_t rec={0};
        key.data=(void *)"12345";
        key.size=6;
        rec.data=(void *)"abcde";
        rec.size=6;

        /* get rid of the existing transaction - we will take a new one */
        BFC_ASSERT_EQUAL(0, ham_txn_commit(m_txn, 0));
        m_txn=0;

        ham_txn_t *txn;
        ham_cursor_t *cursor[5]={0};
        for (int i=0; i<5; i++)
            BFC_ASSERT_EQUAL(0, createCursor(&cursor[i]));

        BFC_ASSERT_EQUAL(0, ham_txn_begin(&txn, m_db, 0));

        BFC_ASSERT_EQUAL(0, 
                    ham_cursor_insert(cursor[0], &key, &rec, 0));
        for (int i=1; i<5; i++)
            BFC_ASSERT_EQUAL(0, ham_cursor_find(cursor[i], &key, 0));

        /* all cursors are now coupled */
        for (int i=0; i<5; i++) {
            BFC_ASSERT(cursor_get_flags(cursor[i])&BT_CURSOR_FLAG_COUPLED);
        }

        /* commit the transaction and flush the key */
        BFC_ASSERT_EQUAL(0, ham_txn_commit(txn, 0));

        /* all cursors are now uncoupled */
        for (int i=0; i<5; i++) {
            BFC_ASSERT_EQUAL(0u, cursor_get_flags(cursor[i]));
            BFC_ASSERT_EQUAL(true, 
                    txn_cursor_is_nil(cursor_get_txn_cursor(cursor[i])));
        }
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
        BFC_REGISTER_TEST(TempTxnCursorTest, insertFindTest);
        BFC_REGISTER_TEST(TempTxnCursorTest, insertFindMultipleCursorsTest);
        BFC_REGISTER_TEST(TempTxnCursorTest, findInEmptyDatabaseTest);
        BFC_REGISTER_TEST(TempTxnCursorTest, nilCursorTest);
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
        BFC_ASSERT_EQUAL(0, createCursor(&m_cursor));
    }

};

BFC_REGISTER_FIXTURE(TempTxnCursorTest);
BFC_REGISTER_FIXTURE(LongTxnCursorTest);
BFC_REGISTER_FIXTURE(NoTxnCursorTest);

